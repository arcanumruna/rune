// libs/view/src/ViewTable.cpp
#include <cstring>
#include <rune/view/ViewTable.h>
#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <utility>

namespace rune::view {

namespace {
// Monotonic clock read used ONLY to measure the de-amortized rehash
// machinery's own cost (maybePrezero/migrateStep/startGrow) — never on the
// steady-state find/insert path, so a normal put()/get()/erase() that
// triggers none of that machinery pays zero clock reads for it.
inline uint64_t nowNs() noexcept {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}
} // namespace

uint64_t ViewTable::mix(uint64_t x) noexcept {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

uint8_t ViewTable::fullTag(uint64_t h) noexcept {
    uint8_t t = static_cast<uint8_t>((h >> 56) & 0x7f);
    return 0x80 | (t < 2 ? 2 : t); // 0/1 reserved for EMPTY/TOMB-style sentinels
}

ViewTable::ViewTable(RowLayout layout, size_t initialIndexSlots, size_t chunkBytes)
    : layout_(std::move(layout)) {
    assert(initialIndexSlots > 0 && std::has_single_bit(initialIndexSlots) &&
           "ViewTable: initialIndexSlots must be a power of two");

    cap_     = initialIndexSlots;
    mask_    = cap_ - 1;

    // rowsPerChunk_ = floor_pow2(chunkBytes / rowLen), clamped to >= 1 (a
    // single row wider than chunkBytes still gets its own one-row chunk).
    const size_t rowLen = static_cast<size_t>(layout_.rowLen);
    size_t rowsPerChunk = rowLen > 0 ? (chunkBytes / rowLen) : 0;
    rowsPerChunk = std::bit_floor(rowsPerChunk); // 0 stays 0 (bit_floor(0) == 0)
    if (rowsPerChunk < 1) rowsPerChunk = 1;
    rowsPerChunk_ = rowsPerChunk;
    chunkShift_   = static_cast<size_t>(std::countr_zero(rowsPerChunk_));
    chunkMask_    = rowsPerChunk_ - 1;

    tags_.assign(cap_, kEmpty);
    keys_.assign(cap_, 0);
    rid_.assign(cap_, 0);
    // free_ can never hold more than the LIVE key count, which (thanks to
    // incremental rehash) is itself unbounded over the table's lifetime —
    // but growth only ever happens via startGrow()/migrateStep(), never by
    // reallocating free_ on the hot path, so reserving the initial
    // capacity up front just gives erase()'s push_back() a warm start;
    // free_ may reallocate (amortized, standard vector growth) past that
    // if the table grows far beyond its initial capacity.
    free_.reserve(cap_);

    // Task 6: dirty-tracking side arrays. dirtyList_/deleted_ only ever
    // grow in steady state (a reserve() here just gives them a warm start
    // so the first interval's push_backs don't reallocate); dirty_/
    // dirtyKeys_ are sized to the rid space by appendChunk(), below.
    dirtyList_.reserve(cap_);
    deleted_.reserve(256);

    appendChunk(); // start with one chunk so rid 0 is always addressable
}

void ViewTable::appendChunk() {
    {
        const size_t bytes = rowsPerChunk_ * static_cast<size_t>(layout_.rowLen);
        std::unique_ptr<uint8_t[]> chunk(new uint8_t[bytes]);
        std::memset(chunk.get(), 0, bytes);
        chunks_.push_back(std::move(chunk));
    }
    // dirty_/dirtyKeys_ are indexed by rid and must cover every valid rid.
    // Grown here (alongside the row slab) rather than on index rehash:
    // rehash relocates (tag,key,rid) index entries but never changes a
    // row's rid, so dirty_[rid] must stay keyed to the SAME space the slab
    // uses, not the index's.
    dirty_.resize(chunks_.size() * rowsPerChunk_, uint8_t{0});
    dirtyKeys_.resize(chunks_.size() * rowsPerChunk_, uint64_t{0});
}

void ViewTable::markDirty(uint32_t rid) noexcept {
    if (dirty_[rid]) return; // already pending this interval -- coalesce
    dirty_[rid] = 1;
    dirtyList_.push_back(rid);
}

size_t ViewTable::findP(uint64_t key) const noexcept {
    const uint64_t h  = mix(key);
    const uint8_t  ft = fullTag(h);
    size_t i = h & mask_;
    for (size_t probes = 0; probes < cap_; ++probes) {
        const uint8_t t = tags_[i];
        if (t == kEmpty) return static_cast<size_t>(-1);
        if (t == ft && keys_[i] == key) return i;
        i = (i + 1) & mask_;
    }
    return static_cast<size_t>(-1);
}

size_t ViewTable::findO(uint64_t key) const noexcept {
    if (!migrating_) return static_cast<size_t>(-1);
    const uint64_t h  = mix(key);
    const uint8_t  ft = fullTag(h);
    size_t i = h & omask_;
    for (size_t probes = 0; probes < ocap_; ++probes) {
        const uint8_t t = ot_[i];
        if (t == kEmpty) return static_cast<size_t>(-1);
        if (t == ft && ok_[i] == key) return i;
        i = (i + 1) & omask_;
    }
    return static_cast<size_t>(-1);
}

void ViewTable::backshiftPrimary(size_t i) noexcept {
    // Backward-shift deletion (standard open-addressing linear-probing
    // delete, e.g. Wikipedia "Open addressing" / Knuth 6.4): walk forward
    // from the freed slot `hole`, pulling back any entry at `j` whose home
    // slot does NOT lie in the open-closed cyclic interval (hole, j] — such
    // an entry's probe sequence does not depend on slot `hole` being
    // occupied, so it is safe to move it into the hole, vacating `j` as the
    // new hole. Stops at the first slot that was already EMPTY (that marks
    // the end of this probe run).
    tags_[i] = kEmpty;
    size_t hole = i;
    size_t j    = i;
    for (;;) {
        j = (j + 1) & mask_;
        if (tags_[j] == kEmpty) break;

        const size_t home = mix(keys_[j]) & mask_;
        bool pinned; // entry at j must stay put (for now) relative to hole
        if (hole <= j) {
            pinned = (hole < home) && (home <= j);
        } else {
            pinned = (hole < home) || (home <= j);
        }
        if (pinned) continue;

        tags_[hole] = tags_[j];
        keys_[hole] = keys_[j];
        rid_[hole]  = rid_[j];
        tags_[j]    = kEmpty;
        hole        = j;
    }
}

void ViewTable::maybePrezero() noexcept {
    if (migrating_) return; // finish the in-flight grow before provisioning the next one
    const size_t nc = cap_ * 2;
    if (prezeroCap_ == nc && prezeroCur_ >= prezeroCap_) return; // already fully provisioned
    if (sz_ < static_cast<size_t>(cap_ * kPrezeroLoad)) return;  // not yet time

    const uint64_t t0 = nowNs();

    if (prezeroCap_ != nc) {
        // (Re)start provisioning for the NEXT grow generation. reserve()
        // only claims raw address space (a single fast, non-zeroing
        // allocation) — the expensive part, actually ZEROING nc entries,
        // happens incrementally below, kPrezero at a time, spread across
        // many put()/erase() calls instead of paid in one lump sum here.
        nextTags_.clear();    nextTags_.reserve(nc);
        nextKeys_.clear();    nextKeys_.reserve(nc);
        nextRid_buf_.clear(); nextRid_buf_.reserve(nc);
        prezeroCap_ = nc;
        prezeroCur_ = 0;
    }

    const size_t remain = prezeroCap_ - prezeroCur_;
    const size_t slice  = remain < kPrezero ? remain : kPrezero;
    nextTags_.insert(nextTags_.end(), slice, kEmpty);
    nextKeys_.insert(nextKeys_.end(), slice, uint64_t{0});
    nextRid_buf_.insert(nextRid_buf_.end(), slice, uint32_t{0});
    prezeroCur_ += slice;

    const double ns = static_cast<double>(nowNs() - t0);
    if (ns > maxRehashNs_) maxRehashNs_ = ns;
}

void ViewTable::startGrow() {
    const uint64_t t0 = nowNs();

    ot_.swap(tags_); ok_.swap(keys_); orid_.swap(rid_);
    ocap_ = cap_; omask_ = mask_; ocur_ = 0; migrating_ = true;

    const size_t nc = cap_ * 2;
    if (prezeroCap_ == nc && prezeroCur_ >= prezeroCap_) {
        // Pre-provisioning finished in time: O(1) swap, no zero-fill here.
        tags_.swap(nextTags_);
        keys_.swap(nextKeys_);
        rid_.swap(nextRid_buf_);
    } else {
        // The write burst outran pre-provisioning: finish the zero-fill
        // inline right now (the bounded fallback) and count it.
        tags_.assign(nc, kEmpty);
        keys_.assign(nc, 0);
        rid_.assign(nc, 0);
        ++fallbacks_;
    }

    // Whatever was (partially) provisioned belonged to this just-consumed
    // generation. The NEXT grow (cap_ is now nc) provisions its own next
    // buffer from scratch once load crosses kPrezeroLoad again.
    nextTags_.clear();    nextTags_.shrink_to_fit();
    nextKeys_.clear();    nextKeys_.shrink_to_fit();
    nextRid_buf_.clear(); nextRid_buf_.shrink_to_fit();
    prezeroCap_ = 0;
    prezeroCur_ = 0;

    cap_  = nc;
    mask_ = nc - 1;

    const double ns = static_cast<double>(nowNs() - t0);
    if (ns > maxRehashNs_) maxRehashNs_ = ns;
}

void ViewTable::migrateStep() noexcept {
    if (!migrating_) return;
    const uint64_t t0 = nowNs();

    // Bounded on entries SCANNED (ocur_ advanced), not just entries MOVED:
    // if the OLD index has been heavily churned (erase() during the
    // migration window tombstones OLD-only entries rather than moving
    // them), most slots here can be kEmpty/kTomb. Bounding only on "live
    // entries moved" would let a single call scan arbitrarily far through
    // a mostly-dead OLD index (up to the full ocap_) looking for kMigK
    // live ones to move — silently reintroducing an unbounded single-op
    // cost, which is exactly what incremental/de-amortized rehash exists
    // to prevent. Scanned-bound guarantees every call does O(kMigK) work,
    // and the OLD index still fully drains in exactly ceil(ocap_/kMigK)
    // calls regardless of its live/dead mix.
    size_t scanned = 0;
    while (scanned < kMigK && ocur_ < ocap_) {
        const uint8_t t = ot_[ocur_];
        if (t >= 0x80) { // live entry; kEmpty(0)/kTomb(1) are both < 0x80 (fullTag() is always >= 0x82)
            const uint64_t k = ok_[ocur_];
            size_t i = mix(k) & mask_;
            for (size_t probes = 0; probes < cap_ && tags_[i] != kEmpty; ++probes) {
                i = (i + 1) & mask_;
            }
            assert(tags_[i] == kEmpty && "ViewTable: migrateStep found no empty slot in NEW index");
            tags_[i] = t;
            keys_[i] = k;
            rid_[i]  = orid_[ocur_];
            // CRITICAL: tombstone the OLD slot now that this key lives in
            // NEW. findO() only stops its probe at a true kEmpty, so a
            // migrated-but-not-tombstoned OLD slot stays matchable for the
            // rest of the migration window — a later erase(key) (found via
            // findP, since it's in NEW) followed by put(key) while still
            // migrating would hit this stale OLD "ghost" entry in findO()
            // and treat it as an idempotent update of an existing key,
            // silently dropping the reinsertion (no write to
            // tags_/keys_/rid_, no ++sz_) and permanently losing the key
            // once OLD is later freed. kTomb does not terminate findO's
            // probe (so later, not-yet-migrated keys in this OLD probe
            // chain are still reachable), but it does stop this slot from
            // ever matching again.
            ot_[ocur_] = kTomb;
        }
        ++ocur_;
        ++scanned;
    }

    if (ocur_ >= ocap_) {
        migrating_ = false;
        ot_.clear();   ot_.shrink_to_fit();
        ok_.clear();   ok_.shrink_to_fit();
        orid_.clear(); orid_.shrink_to_fit();
        ocap_ = 0; omask_ = 0; ocur_ = 0;
    }

    const double ns = static_cast<double>(nowNs() - t0);
    if (ns > maxRehashNs_) maxRehashNs_ = ns;
}

uint8_t* ViewTable::put(uint64_t key) {
    maybePrezero();
    if (migrating_) {
        // Deferring the grow-check while already migrating is safe: NEW
        // was sized to 2x the OLD capacity that triggered this migration,
        // so even in the worst case (every op during the whole drain
        // window is a brand-new distinct key) the window closes within
        // ceil(ocap_/kMigK) ops — far too few new keys to approach NEW's
        // own 0.85 threshold before migrateStep() finishes draining OLD
        // and the next startGrow() becomes eligible again.
        migrateStep();
    } else if (sz_ + 1 > static_cast<size_t>(cap_ * kGrowLoad)) {
        startGrow();
    }

    const size_t pi = findP(key);
    if (pi != static_cast<size_t>(-1)) {
        const uint32_t rid = rid_[pi];
        dirtyKeys_[rid] = key;
        markDirty(rid);
        return rowAddr(rid); // idempotent upsert: existing key in NEW/primary, no dup
    }
    if (migrating_) {
        const size_t oi = findO(key);
        if (oi != static_cast<size_t>(-1)) {
            const uint32_t rid = orid_[oi];
            dirtyKeys_[rid] = key;
            markDirty(rid);
            return rowAddr(rid); // idempotent upsert: existing key still only in OLD
        }
    }

    uint32_t rid;
    if (!free_.empty()) {
        rid = free_.back();
        free_.pop_back();
    } else {
        // Row-id space is the chunked arena's total capacity so far:
        // append one more chunk (the deliberate growth allocation)
        // whenever the next never-used rid would fall past it. Existing
        // rids/pointers are unaffected because rows never move.
        if (nextRid_ >= chunks_.size() * rowsPerChunk_) {
            appendChunk();
        }
        rid = nextRid_++;
    }

    const uint64_t h  = mix(key);
    const uint8_t  ft = fullTag(h);
    size_t i = h & mask_;
    // Hard-bounded (never loops more than cap_ times, even in a release
    // build with asserts compiled out): the load-factor policy above
    // guarantees an empty slot exists, but this guards against ever
    // spinning forever instead of just corrupting state if that invariant
    // is somehow broken.
    for (size_t probes = 0; probes < cap_ && tags_[i] != kEmpty; ++probes) {
        i = (i + 1) & mask_;
    }
    assert(tags_[i] == kEmpty && "ViewTable: primary index unexpectedly full (load-factor invariant broken)");
    tags_[i] = ft;
    keys_[i] = key;
    rid_[i]  = rid;

    uint8_t* row = rowAddr(rid);
    std::fill(row, row + layout_.rowLen, uint8_t{0});
    ++sz_;
    dirtyKeys_[rid] = key;
    markDirty(rid);
    return row;
}

const uint8_t* ViewTable::get(uint64_t key) const noexcept {
    const size_t pi = findP(key);
    if (pi != static_cast<size_t>(-1)) return rowAddr(rid_[pi]);
    if (migrating_) {
        const size_t oi = findO(key);
        if (oi != static_cast<size_t>(-1)) return rowAddr(orid_[oi]);
    }
    return nullptr;
}

bool ViewTable::erase(uint64_t key) noexcept {
    maybePrezero();
    if (migrating_) migrateStep();

    const size_t pi = findP(key);
    if (pi != static_cast<size_t>(-1)) {
        const uint32_t rid = rid_[pi];
        free_.push_back(rid);
        backshiftPrimary(pi);
        --sz_;
        // The row at `rid` no longer exists: clear any pending dirty bit
        // (drainDirty()'s dirtyList_ loop guards on dirty_[rid] still being
        // set, so a stale dirtyList_ entry for this rid is harmlessly
        // skipped) and record the delete tombstone -- keyed, since a
        // delete has no row for the checkpoint fold to copy.
        dirty_[rid] = 0;
        deleted_.push_back({rid, key});
        return true;
    }

    if (migrating_) {
        const size_t oi = findO(key);
        if (oi != static_cast<size_t>(-1)) {
            const uint32_t rid = orid_[oi];
            free_.push_back(rid);
            // OLD is draining, not steady-state: tombstone rather than
            // backward-shift. migrateStep() skips kTomb entries (< 0x80)
            // when it later walks past this slot.
            ot_[oi] = kTomb;
            --sz_;
            dirty_[rid] = 0;
            deleted_.push_back({rid, key});
            return true;
        }
    }
    return false;
}

} // namespace rune::view
