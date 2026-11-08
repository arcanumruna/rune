// libs/view/src/ViewTable.cpp
#include <cstring>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewKeyHash.h>
#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <random>
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

// Backward-shift deletion (standard open-addressing linear-probing
// delete, e.g. Wikipedia "Open addressing" / Knuth 6.4): walk forward
// from the freed slot `hole`, pulling back any entry at `j` whose home
// slot does NOT lie in the open-closed cyclic interval (hole, j] — such
// an entry's probe sequence does not depend on slot `hole` being
// occupied, so it is safe to move it into the hole, vacating `j` as the
// new hole. Stops at the first slot that was already EMPTY (that marks
// the end of this probe run).
template <class Home>
void backshiftDelete(uint8_t* tags, uint64_t* words, uint32_t* rids, size_t mask, size_t i, Home&& home) noexcept {
    tags[i] = 0;
    size_t hole = i;
    size_t j    = i;
    for (;;) {
        j = (j + 1) & mask;
        if (tags[j] == 0) break;

        const size_t h = home(words[j]) & mask;
        bool pinned; // entry at j must stay put (for now) relative to hole
        if (hole <= j) {
            pinned = (hole < h) && (h <= j);
        } else {
            pinned = (hole < h) || (h <= j);
        }
        if (pinned) continue;

        tags[hole]  = tags[j];
        words[hole] = words[j];
        rids[hole]  = rids[j];
        tags[j]     = 0;
        hole        = j;
    }
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

void ViewTable::SlotIndex::init(size_t slots, bool wordIsHash_, uint64_t seed_, uint64_t hashMask_) {
    cap  = slots;
    mask = slots - 1;
    tags.assign(cap, kEmpty);
    keys.assign(cap, 0);
    rid.assign(cap, 0);
    wordIsHash = wordIsHash_;
    seed       = seed_;
    hashMask   = hashMask_;
}

ViewTable::ViewTable(RowLayout layout, size_t initialIndexSlots, size_t chunkBytes, ViewTableKeys keys)
    : layout_(std::move(layout)), keyWidth_(keys.keyWidth), prefixWidth_(keys.prefixWidth), hashSeed_(keys.hashSeed),
      hashMask_(keys.hashMaskForTest), headerLen_(keys.keyWidth > 8 ? keys.keyWidth : 0),
      rowStride_(headerLen_ + static_cast<size_t>(layout_.rowLen)) {
    assert(keyWidth_ <= rune::codec::kMaxViewKeyBytes && "ViewTable: keyWidth over kMaxViewKeyBytes");
    assert(initialIndexSlots > 0 && std::has_single_bit(initialIndexSlots) &&
           "ViewTable: initialIndexSlots must be a power of two");

    // rowsPerChunk_ = floor_pow2(chunkBytes / rowLen), clamped to >= 1 (a
    // single row wider than chunkBytes still gets its own one-row chunk).
    size_t rowsPerChunk = rowStride_ > 0 ? (chunkBytes / rowStride_) : 0;
    rowsPerChunk = std::bit_floor(rowsPerChunk); // 0 stays 0 (bit_floor(0) == 0)
    if (rowsPerChunk < 1) rowsPerChunk = 1;
    rowsPerChunk_ = rowsPerChunk;
    chunkShift_   = static_cast<size_t>(std::countr_zero(rowsPerChunk_));
    chunkMask_    = rowsPerChunk_ - 1;

    main_.init(initialIndexSlots, headerLen_ != 0, hashSeed_, hashMask_);
    // free_ can never hold more than the LIVE key count, which (thanks to
    // incremental rehash) is itself unbounded over the table's lifetime —
    // but growth only ever happens via startGrow()/migrateStep(), never by
    // reallocating free_ on the hot path, so reserving the initial
    // capacity up front just gives erase()'s push_back() a warm start;
    // free_ may reallocate (amortized, standard vector growth) past that
    // if the table grows far beyond its initial capacity.
    free_.reserve(main_.cap);

    // Task 6: dirty-tracking side arrays. dirtyList_/deleted_ only ever
    // grow in steady state (a reserve() here just gives them a warm start
    // so the first interval's push_backs don't reallocate); dirty_/
    // dirtyKeys_ are sized to the rid space by appendChunk(), below.
    dirtyList_.reserve(main_.cap);
    deleted_.reserve(256);
    deletedKeyBytes_.reserve(256 * headerLen_);

    if (prefixWidth_) {
        outer_.init(initialIndexSlots, true, hashSeed_, hashMask_);
        chunkBytes_ = chunkBytes;
        blockFree_.resize(32);
        std::random_device rd;
        versionClock_ = (uint64_t{rd()} << 32) | rd();
    }

    appendChunk(); // start with one chunk so rid 0 is always addressable
}

void ViewTable::appendChunk() {
    {
        const size_t bytes = rowsPerChunk_ * rowStride_;
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

ViewTable::Probe ViewTable::probeOf(const ViewKey& key) const noexcept {
    assert(key.len == keyWidth_);
    if (!headerLen_) return probeOf(key.asU64());
    const uint64_t h = viewKeyHash(key.span(), hashSeed_) & hashMask_;
    return {h, h, key.bytes.data()};
}

bool ViewTable::headerKeyEquals(uint32_t rid, const uint8_t* key) const noexcept {
    ++arenaKeyReads_;
    return std::memcmp(rowAddr(rid) - headerLen_, key, headerLen_) == 0;
}

size_t ViewTable::findP(const Probe& p) const noexcept {
    const uint8_t  ft = fullTag(p.h);
    size_t i = p.h & main_.mask;
    for (size_t probes = 0; probes < main_.cap; ++probes) {
        const uint8_t t = main_.tags[i];
        if (t == kEmpty) return static_cast<size_t>(-1);
        if (t == ft && main_.keys[i] == p.word && (!p.wideKey || headerKeyEquals(main_.rid[i], p.wideKey))) return i;
        i = (i + 1) & main_.mask;
    }
    return static_cast<size_t>(-1);
}

size_t ViewTable::findO(const Probe& p) const noexcept {
    if (!main_.migrating) return static_cast<size_t>(-1);
    const uint8_t  ft = fullTag(p.h);
    size_t i = p.h & main_.omask;
    for (size_t probes = 0; probes < main_.ocap; ++probes) {
        const uint8_t t = main_.ot[i];
        if (t == kEmpty) return static_cast<size_t>(-1);
        if (t == ft && main_.ok[i] == p.word && (!p.wideKey || headerKeyEquals(main_.orid[i], p.wideKey))) return i;
        i = (i + 1) & main_.omask;
    }
    return static_cast<size_t>(-1);
}

void ViewTable::SlotIndex::backshift(size_t i) noexcept {
    backshiftDelete(tags.data(), keys.data(), rid.data(), mask, i, [this](uint64_t w) { return home(w); });
}

void ViewTable::SlotIndex::maybePrezero(size_t sz) noexcept {
    if (migrating) return; // finish the in-flight grow before provisioning the next one
    const size_t nc = cap * 2;
    if (prezeroCap == nc && prezeroCur >= prezeroCap) return; // already fully provisioned
    if (sz < static_cast<size_t>(cap * kPrezeroLoad)) return;  // not yet time

    const uint64_t t0 = nowNs();

    if (prezeroCap != nc) {
        // (Re)start provisioning for the NEXT grow generation. reserve()
        // only claims raw address space (a single fast, non-zeroing
        // allocation) — the expensive part, actually ZEROING nc entries,
        // happens incrementally below, kPrezero at a time, spread across
        // many put()/erase() calls instead of paid in one lump sum here.
        nextTags.clear(); nextTags.reserve(nc);
        nextKeys.clear(); nextKeys.reserve(nc);
        nextRid.clear();  nextRid.reserve(nc);
        prezeroCap = nc;
        prezeroCur = 0;
    }

    const size_t remain = prezeroCap - prezeroCur;
    const size_t slice  = remain < kPrezero ? remain : kPrezero;
    nextTags.insert(nextTags.end(), slice, kEmpty);
    nextKeys.insert(nextKeys.end(), slice, uint64_t{0});
    nextRid.insert(nextRid.end(), slice, uint32_t{0});
    prezeroCur += slice;

    const double ns = static_cast<double>(nowNs() - t0);
    if (ns > maxRehashNs) maxRehashNs = ns;
}

void ViewTable::SlotIndex::startGrow() {
    const uint64_t t0 = nowNs();

    ot.swap(tags); ok.swap(keys); orid.swap(rid);
    ocap = cap; omask = mask; ocur = 0; migrating = true;

    const size_t nc = cap * 2;
    if (prezeroCap == nc && prezeroCur >= prezeroCap) {
        // Pre-provisioning finished in time: O(1) swap, no zero-fill here.
        tags.swap(nextTags);
        keys.swap(nextKeys);
        rid.swap(nextRid);
    } else {
        // The write burst outran pre-provisioning: finish the zero-fill
        // inline right now (the bounded fallback) and count it.
        tags.assign(nc, kEmpty);
        keys.assign(nc, 0);
        rid.assign(nc, 0);
        ++fallbacks;
    }

    // Whatever was (partially) provisioned belonged to this just-consumed
    // generation. The NEXT grow (cap is now nc) provisions its own next
    // buffer from scratch once load crosses kPrezeroLoad again.
    nextTags.clear(); nextTags.shrink_to_fit();
    nextKeys.clear(); nextKeys.shrink_to_fit();
    nextRid.clear();  nextRid.shrink_to_fit();
    prezeroCap = 0;
    prezeroCur = 0;

    cap  = nc;
    mask = nc - 1;

    const double ns = static_cast<double>(nowNs() - t0);
    if (ns > maxRehashNs) maxRehashNs = ns;
}

void ViewTable::SlotIndex::migrateStep() noexcept {
    if (!migrating) return;
    const uint64_t t0 = nowNs();

    // Bounded on entries SCANNED (ocur advanced), not just entries MOVED:
    // if the OLD index has been heavily churned (erase() during the
    // migration window tombstones OLD-only entries rather than moving
    // them), most slots here can be kEmpty/kTomb. Bounding only on "live
    // entries moved" would let a single call scan arbitrarily far through
    // a mostly-dead OLD index (up to the full ocap) looking for kMigK
    // live ones to move — silently reintroducing an unbounded single-op
    // cost, which is exactly what incremental/de-amortized rehash exists
    // to prevent. Scanned-bound guarantees every call does O(kMigK) work,
    // and the OLD index still fully drains in exactly ceil(ocap/kMigK)
    // calls regardless of its live/dead mix.
    size_t scanned = 0;
    while (scanned < kMigK && ocur < ocap) {
        const uint8_t t = ot[ocur];
        if (t >= 0x80) { // live entry; kEmpty(0)/kTomb(1) are both < 0x80 (fullTag() is always >= 0x82)
            const uint64_t k = ok[ocur];
            size_t i = home(k) & mask;
            for (size_t probes = 0; probes < cap && tags[i] != kEmpty; ++probes) {
                i = (i + 1) & mask;
            }
            assert(tags[i] == kEmpty && "ViewTable: migrateStep found no empty slot in NEW index");
            tags[i] = t;
            keys[i] = k;
            rid[i]  = orid[ocur];
            // CRITICAL: tombstone the OLD slot now that this key lives in
            // NEW. findO() only stops its probe at a true kEmpty, so a
            // migrated-but-not-tombstoned OLD slot stays matchable for the
            // rest of the migration window — a later erase(key) (found via
            // findP, since it's in NEW) followed by put(key) while still
            // migrating would hit this stale OLD "ghost" entry in findO()
            // and treat it as an idempotent update of an existing key,
            // silently dropping the reinsertion (no write to
            // tags/keys/rid, no ++sz_) and permanently losing the key
            // once OLD is later freed. kTomb does not terminate findO's
            // probe (so later, not-yet-migrated keys in this OLD probe
            // chain are still reachable), but it does stop this slot from
            // ever matching again.
            ot[ocur] = kTomb;
        }
        ++ocur;
        ++scanned;
    }

    if (ocur >= ocap) {
        migrating = false;
        ot.clear();   ot.shrink_to_fit();
        ok.clear();   ok.shrink_to_fit();
        orid.clear(); orid.shrink_to_fit();
        ocap = 0; omask = 0; ocur = 0;
    }

    const double ns = static_cast<double>(nowNs() - t0);
    if (ns > maxRehashNs) maxRehashNs = ns;
}

uint8_t* ViewTable::put(uint64_t key) {
    assert(keyWidth_ <= 8 && prefixWidth_ == 0);
    return putImpl(probeOf(key));
}
uint8_t* ViewTable::put(const ViewKey& key) { return putImpl(probeOf(key)); }

const uint8_t* ViewTable::get(uint64_t key) const noexcept {
    assert(keyWidth_ <= 8 && prefixWidth_ == 0);
    return getImpl(probeOf(key));
}
const uint8_t* ViewTable::get(const ViewKey& key) const noexcept { return getImpl(probeOf(key)); }

bool ViewTable::ridOf(const ViewKey& key, uint32_t& rid) const noexcept {
    const Probe p = probeOf(key);
    const size_t pi = findP(p);
    if (pi != static_cast<size_t>(-1)) { rid = main_.rid[pi]; return true; }
    if (main_.migrating) {
        const size_t oi = findO(p);
        if (oi != static_cast<size_t>(-1)) { rid = main_.orid[oi]; return true; }
    }
    return false;
}

bool ViewTable::erase(uint64_t key) noexcept {
    assert(keyWidth_ <= 8 && prefixWidth_ == 0);
    return eraseImpl(probeOf(key));
}
bool ViewTable::erase(const ViewKey& key) noexcept { return eraseImpl(probeOf(key)); }

uint8_t* ViewTable::putImpl(const Probe& p) {
    main_.maybePrezero(sz_);
    if (main_.migrating) {
        // Deferring the grow-check while already migrating is safe: NEW
        // was sized to 2x the OLD capacity that triggered this migration,
        // so even in the worst case (every op during the whole drain
        // window is a brand-new distinct key) the window closes within
        // ceil(ocap/kMigK) ops — far too few new keys to approach NEW's
        // own 0.85 threshold before migrateStep() finishes draining OLD
        // and the next startGrow() becomes eligible again.
        main_.migrateStep();
    } else if (sz_ + 1 > static_cast<size_t>(main_.cap * kGrowLoad)) {
        main_.startGrow();
    }

    const size_t pi = findP(p);
    if (pi != static_cast<size_t>(-1)) {
        const uint32_t rid = main_.rid[pi];
        dirtyKeys_[rid] = p.word;
        markDirty(rid);
        return rowAddr(rid); // idempotent upsert: existing key in NEW/primary, no dup
    }
    if (main_.migrating) {
        const size_t oi = findO(p);
        if (oi != static_cast<size_t>(-1)) {
            const uint32_t rid = main_.orid[oi];
            dirtyKeys_[rid] = p.word;
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

    const uint8_t  ft = fullTag(p.h);
    size_t i = p.h & main_.mask;
    // Hard-bounded (never loops more than cap times, even in a release
    // build with asserts compiled out): the load-factor policy above
    // guarantees an empty slot exists, but this guards against ever
    // spinning forever instead of just corrupting state if that invariant
    // is somehow broken.
    for (size_t probes = 0; probes < main_.cap && main_.tags[i] != kEmpty; ++probes) {
        i = (i + 1) & main_.mask;
    }
    assert(main_.tags[i] == kEmpty && "ViewTable: primary index unexpectedly full (load-factor invariant broken)");
    main_.tags[i] = ft;
    main_.keys[i] = p.word;
    main_.rid[i]  = rid;

    uint8_t* row = rowAddr(rid);
    if (p.wideKey) std::memcpy(row - headerLen_, p.wideKey, headerLen_);
    std::fill(row, row + layout_.rowLen, uint8_t{0});
    ++sz_;
    dirtyKeys_[rid] = p.word;
    markDirty(rid);
    if (prefixWidth_) prefixInsert(p, rid);
    return row;
}

const uint8_t* ViewTable::getImpl(const Probe& p) const noexcept {
    const size_t pi = findP(p);
    if (pi != static_cast<size_t>(-1)) return rowAddr(main_.rid[pi]);
    if (main_.migrating) {
        const size_t oi = findO(p);
        if (oi != static_cast<size_t>(-1)) return rowAddr(main_.orid[oi]);
    }
    return nullptr;
}

bool ViewTable::eraseImpl(const Probe& p) noexcept {
    main_.maybePrezero(sz_);
    if (main_.migrating) main_.migrateStep();

    const size_t pi = findP(p);
    if (pi != static_cast<size_t>(-1)) {
        const uint32_t rid = main_.rid[pi];
        free_.push_back(rid);
        main_.backshift(pi);
        --sz_;
        // The row at `rid` no longer exists: clear any pending dirty bit
        // (drainDirty()'s dirtyList_ loop guards on dirty_[rid] still being
        // set, so a stale dirtyList_ entry for this rid is harmlessly
        // skipped) and record the delete tombstone -- keyed, since a
        // delete has no row for the checkpoint fold to copy.
        dirty_[rid] = 0;
        deleted_.push_back({rid, p.word});
        if (p.wideKey) deletedKeyBytes_.insert(deletedKeyBytes_.end(), p.wideKey, p.wideKey + headerLen_);
        if (prefixWidth_) prefixErase(p, rid);
        return true;
    }

    if (main_.migrating) {
        const size_t oi = findO(p);
        if (oi != static_cast<size_t>(-1)) {
            const uint32_t rid = main_.orid[oi];
            free_.push_back(rid);
            // OLD is draining, not steady-state: tombstone rather than
            // backward-shift. migrateStep() skips kTomb entries (< 0x80)
            // when it later walks past this slot.
            main_.ot[oi] = kTomb;
            --sz_;
            dirty_[rid] = 0;
            deleted_.push_back({rid, p.word});
            if (p.wideKey) deletedKeyBytes_.insert(deletedKeyBytes_.end(), p.wideKey, p.wideKey + headerLen_);
            if (prefixWidth_) prefixErase(p, rid);
            return true;
        }
    }
    return false;
}

// --- the prefix index -----------------------------------------------------------------------------------------

uint32_t ViewTable::findSetHashed(uint64_t h, const uint8_t* prefix) const noexcept {
    const uint8_t ft = fullTag(h);
    size_t i = h & outer_.mask;
    for (size_t probes = 0; probes < outer_.cap; ++probes) {
        const uint8_t t = outer_.tags[i];
        if (t == kEmpty) break;
        if (t == ft && outer_.keys[i] == h && std::memcmp(setPrefix(outer_.rid[i]), prefix, prefixWidth_) == 0) return outer_.rid[i];
        i = (i + 1) & outer_.mask;
    }
    if (!outer_.migrating) return kNoSet;
    i = h & outer_.omask;
    for (size_t probes = 0; probes < outer_.ocap; ++probes) {
        const uint8_t t = outer_.ot[i];
        if (t == kEmpty) break;
        if (t == ft && outer_.ok[i] == h && std::memcmp(setPrefix(outer_.orid[i]), prefix, prefixWidth_) == 0) return outer_.orid[i];
        i = (i + 1) & outer_.omask;
    }
    return kNoSet;
}

uint32_t ViewTable::findSet(std::span<const uint8_t> prefix) const noexcept {
    if (prefix.size() != prefixWidth_ || outerCount_ == 0) return kNoSet;
    return findSetHashed(viewKeyHash(prefix, hashSeed_) & hashMask_, prefix.data());
}

void ViewTable::appendPoolChunk(uint8_t cls) {
    const size_t blockSlots = size_t{16} << cls;
    size_t slots = chunkBytes_ / 8 / kSlotBytes;
    slots -= slots % blockSlots;
    if (slots == 0) slots = blockSlots;
    PoolChunk chunk{std::unique_ptr<uint8_t[]>(new uint8_t[slots]()), std::unique_ptr<uint64_t[]>(new uint64_t[slots]),
                    std::unique_ptr<uint32_t[]>(new uint32_t[slots])};
    const size_t count = slots / blockSlots;
    const uint32_t first = static_cast<uint32_t>(blocks_.size());
    blocks_.reserve(first + count);
    blockClass_.reserve(first + count);
    blockFree_[cls].reserve(blockFree_[cls].size() + count);
    for (size_t b = 0; b < count; ++b) {
        blocks_.push_back({chunk.tags.get() + b * blockSlots, chunk.words.get() + b * blockSlots, chunk.rids.get() + b * blockSlots});
        blockClass_.push_back(cls);
    }
    for (size_t b = count; b-- > 0;) blockFree_[cls].push_back(first + static_cast<uint32_t>(b));
    poolBytes_ += slots * kSlotBytes;
    poolChunks_.push_back(std::move(chunk));
}

uint32_t ViewTable::allocBlock(uint8_t cls) {
    if (blockFree_[cls].empty()) appendPoolChunk(cls);
    const uint32_t id = blockFree_[cls].back();
    blockFree_[cls].pop_back();
    return id;
}

void ViewTable::startSetMigration(PrefixSet& s, uint8_t cls) {
    s.oldBlock  = s.block;
    s.block     = allocBlock(cls);
    s.cursor    = 0;
    s.migrating = true;
}

// Drains up to kSetMigrate slots of the set's old block into its new one.  The old block stays a valid hash table
// (each moved entry is backshift-deleted), so lookups can probe it until it is empty and freed.
void ViewTable::migrateSet(PrefixSet& s, size_t& budget, uint32_t& moved) noexcept {
    const SetBlock nb = blockOf(s.block);
    const SetBlock ob = blockOf(s.oldBlock);
    const auto home = [this](uint64_t w) { return hashOfWord(w); };
    for (; budget && s.cursor < ob.cap; --budget) {
        const size_t c = s.cursor;
        if (ob.tags[c] == kEmpty) {
            ++s.cursor;
            continue;
        }
        size_t i = home(ob.words[c]) & (nb.cap - 1);
        while (nb.tags[i] != kEmpty) i = (i + 1) & (nb.cap - 1);
        nb.tags[i]  = ob.tags[c];
        nb.words[i] = ob.words[c];
        nb.rids[i]  = ob.rids[c];
        ++moved;
        backshiftDelete(ob.tags, ob.words, ob.rids, ob.cap - 1, c, home);
    }
    if (s.cursor >= ob.cap) {
        freeBlock(s.oldBlock);
        s.migrating = false;
    }
}

void ViewTable::prefixInsert(const Probe& p, uint32_t rid) {
    const uint8_t* prefix = prefixBytesOf(p);
    const uint64_t ph = viewKeyHash({prefix, prefixWidth_}, hashSeed_) & hashMask_;
    uint32_t id = findSetHashed(ph, prefix);
    if (id == kNoSet) {
        outer_.maybePrezero(outerCount_);
        if (outer_.migrating) outer_.migrateStep();
        else if (outerCount_ + 1 > static_cast<size_t>(outer_.cap * kGrowLoad)) outer_.startGrow();
        if (!setFree_.empty()) {
            id = setFree_.back();
            setFree_.pop_back();
        } else {
            id = static_cast<uint32_t>(sets_.size());
            sets_.emplace_back();
            setPrefixBytes_.resize(sets_.size() * prefixWidth_);
        }
        std::memcpy(setPrefixBytes_.data() + size_t{id} * prefixWidth_, prefix, prefixWidth_);
        sets_[id] = PrefixSet{};
        sets_[id].block = allocBlock(0);
        size_t i = ph & outer_.mask;
        while (outer_.tags[i] != kEmpty) i = (i + 1) & outer_.mask;
        outer_.tags[i] = fullTag(ph);
        outer_.keys[i] = ph;
        outer_.rid[i]  = id;
        ++outerCount_;
    }

    PrefixSet& s = sets_[id];
    size_t budget = kSetMigrate;
    uint32_t moved = 0;
    if (s.migrating) {
        migrateSet(s, budget, moved);
    } else if (s.size + 1 > static_cast<size_t>(blockOf(s.block).cap * kGrowLoad)) {
        startSetMigration(s, static_cast<uint8_t>(blockClass_[s.block] + 1));
        migrateSet(s, budget, moved);
    }
    maxSetMoved_ = std::max(maxSetMoved_, moved);
    const SetBlock b = blockOf(s.block);
    size_t i = p.h & (b.cap - 1);
    while (b.tags[i] != kEmpty) i = (i + 1) & (b.cap - 1);
    b.tags[i]  = fullTag(p.h);
    b.words[i] = p.word;
    b.rids[i]  = rid;
    ++s.size;
    s.version = ++versionClock_;
}

void ViewTable::prefixErase(const Probe& p, uint32_t rid) noexcept {
    const uint8_t* prefix = prefixBytesOf(p);
    const uint64_t ph = viewKeyHash({prefix, prefixWidth_}, hashSeed_) & hashMask_;
    const uint32_t id = findSetHashed(ph, prefix);
    assert(id != kNoSet && "ViewTable: row without a prefix set");
    PrefixSet& s = sets_[id];
    size_t budget = kSetMigrate;
    uint32_t moved = 0;
    if (s.migrating) migrateSet(s, budget, moved);

    const uint8_t ft = fullTag(p.h);
    const auto home = [this](uint64_t w) { return hashOfWord(w); };
    const auto slotOf = [&](const SetBlock& b) {
        size_t i = p.h & (b.cap - 1);
        for (size_t probes = 0; probes < b.cap && b.tags[i] != kEmpty; ++probes) {
            if (b.tags[i] == ft && b.words[i] == p.word && b.rids[i] == rid) return i;
            i = (i + 1) & (b.cap - 1);
        }
        return static_cast<size_t>(-1);
    };
    const SetBlock b = blockOf(s.block);
    if (const size_t i = slotOf(b); i != static_cast<size_t>(-1)) {
        backshiftDelete(b.tags, b.words, b.rids, b.cap - 1, i, home);
    } else {
        assert(s.migrating && "ViewTable: row missing from its prefix set");
        const SetBlock ob = blockOf(s.oldBlock);
        backshiftDelete(ob.tags, ob.words, ob.rids, ob.cap - 1, slotOf(ob), home);
    }
    --s.size;
    s.version = ++versionClock_;

    if (s.size == 0) {
        // Both blocks are empty here: a deleted entry leaves no tombstone.
        freeBlock(s.block);
        if (s.migrating) freeBlock(s.oldBlock);
        outer_.maybePrezero(outerCount_);
        if (outer_.migrating) outer_.migrateStep();
        size_t i = ph & outer_.mask;
        while (outer_.tags[i] != kEmpty && outer_.rid[i] != id) i = (i + 1) & outer_.mask;
        if (outer_.tags[i] != kEmpty) {
            outer_.backshift(i);
        } else {
            i = ph & outer_.omask;
            while (outer_.ot[i] < 0x80 || outer_.orid[i] != id) i = (i + 1) & outer_.omask;
            outer_.ot[i] = kTomb;
        }
        --outerCount_;
        setFree_.push_back(id);
    } else if (!s.migrating && blockClass_[s.block] > 0 && size_t{s.size} * 8 < b.cap) {
        startSetMigration(s, static_cast<uint8_t>(blockClass_[s.block] - 1));
        migrateSet(s, budget, moved);
    }
    maxSetMoved_ = std::max(maxSetMoved_, moved);
}

} // namespace rune::view
