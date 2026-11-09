// libs/view/include/rune/view/ViewTable.h
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <vector>
#include <cassert>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewRow.h>

namespace rune::view {

// Key -> row index + chunked-arena row-payload storage for a materialized
// view's in-memory table.
//
// Phase 1 Task 4 scope: the row payload store is a chunked arena
// (`chunks_`, a `std::vector` of fixed-size `rowsPerChunk_`-row pages) that
// grows by APPENDING a new chunk when the current one fills — rows never
// move, so a `rid` (and any pointer previously returned for it by `put`/
// `get`) stays valid for the table's entire lifetime.
//
// Task 5: the open-addressing INDEX (`tags_`/`keys_`/`rid_`) also grows,
// incrementally and de-amortized, instead of asserting when full:
//
//  - When the primary index's load factor crosses `kGrowLoad` (0.85),
//    `startGrow()` makes the current (tags_/keys_/rid_) the OLD index
//    (`ot_`/`ok_`/`orid_`, `ocap_`/`omask_`, drain cursor `ocur_`) and a
//    fresh 2x-capacity buffer becomes the NEW/primary index. `migrating_`
//    is true for the rest of the migration window.
//  - Each subsequent `put`/`erase` call then performs one bounded
//    `migrateStep()`: it copies at most `kMigK` (256) still-live OLD index
//    entries into the NEW index. No single op ever moves the whole table.
//  - During the window, `get`/`put`/`erase` consult the NEW index first
//    (`findP`) then, if absent, the OLD index (`findO`) — a key is always
//    findable via exactly one of the two, never duplicated in both.
//    `erase` on an OLD-only entry marks it `kTomb` (tombstoned, discarded
//    when `migrateStep` later walks past it) rather than mutating OLD's
//    probe structure (OLD is draining, not steady-state, so no
//    backward-shift is needed there).
//  - The slab (`chunks_`) is NEVER touched by rehash: only the tiny
//    `(tag,key,rid)` index entries migrate; a `rid` means the same row
//    address whether it is reachable via the NEW or the OLD index.
//
// De-amortized pre-provision (the ONE deliberate divergence from the
// design-spike algorithm this was ported from): the spike pre-provisioned
// the next 2x index buffer on a BACKGROUND THREAD with a mutex/condition
// variable. `libs/**` forbids atomics/mutexes/threads
// (scripts/check_no_atomics.py), so instead, once the primary index's load
// crosses `kPrezeroLoad` (0.65), each `put`/`erase` calls `maybePrezero()`,
// which zero-fills a bounded slice (`kPrezero`, 1024 entries) of the next
// 2x `nextTags_`/`nextKeys_`/`nextRid_buf_` buffers — spreading the cost
// of preparing the next grow across many ordinary ops instead of paying
// for it all at once. When `startGrow()` fires, if pre-provisioning
// finished in time it swaps the ready buffers in O(1); if the write burst
// outran it, `startGrow()` finishes the zero-fill inline right there (the
// bounded fallback) and counts it via `rehashFallbacks()`. The single
// worst per-op cost paid by any of `maybePrezero`/`migrateStep`/
// `startGrow` is tracked in `maxRehashNs_` and reported via
// `worstRehashTaxUs()`.
//
// The index key is the EXACT uint64_t view key (not a hash): `get`/`erase`
// compare `keys_[slot] == key` directly. `mix()` is used only to pick the
// open-addressing probe start slot, and `fullTag()` derives a 1-byte
// Swiss/F14-style tag used as a cheap pre-filter before the exact key
// compare.
//
// Deletes are O(1) and tombstone-free in the row PAYLOAD: `erase` performs
// backward-shift deletion over the PRIMARY index arrays (tags_/keys_/rid_)
// to keep the probe sequence for every other key intact, and returns the
// freed row-id to a free-list. It never moves or clears a row's payload
// bytes.
//
// Shared-nothing: ViewTable has no cross-thread state, no atomics, no
// locks, no thread_local. It is owned and driven by exactly one worker
// thread; nothing here is safe (or intended) to touch from a second thread.
// Zero heap allocation in put()/get()/erase() steady state, EXCEPT: (a) the
// deliberate chunk allocation inside appendChunk() when `put` needs a row
// beyond the current chunk capacity, and (b) the deliberate, bounded index
// growth machinery above (`maybePrezero`/`startGrow`/`migrateStep`) — those
// ARE the growth path. get() never allocates. The row-id free-list only
// grows (via erase()) so its own push_back() never (re)allocates once
// reserve()'d.
//
// Wide keys (Part 07): a key of keyWidth > 8 bytes is stored in a header in front of its row
// (`[key][payload]`, the payload is the RowLayout row) and the index slot word is its seeded wyhash, so the
// 13-byte slot (tag, word, rid) is the same as the narrow one.  A probe matches tag, then word, then the header
// key; backshift and migration take the home from the stored word and never read the arena.  A narrow key
// (keyWidth <= 8) is the slot word itself, as before; `hashSeed` is XORed in before `mix()` (seed 0 = the
// original index).
struct ViewTableKeys {
    uint8_t  keyWidth = 8;
    uint8_t  prefixWidth = 0;
    uint64_t hashSeed = 0;
    uint64_t hashMaskForTest = ~uint64_t{0};   // AND-ed into every hash; 0 makes every key collide
    /// Optional per-row auxiliary bytes (0 = none: no allocation, no cost).  Chunked like the rows and grown by appendChunk()
    /// with them; the table only zero-fills them (the owner sets what it needs when it creates a row).
    uint32_t auxBytesPerRow = 0;
};

enum class PrefixScan : uint8_t { End, Stopped, CursorGone };
inline constexpr std::size_t kScanTokenBytes = 12;      // [pos u32][version u64], P7-11

class ViewTable {
public:
    // `initialIndexSlots` must be a power of two (capacity of the
    // open-addressing index; index growth is Task 5).
    // `chunkBytes` sizes each row-slab chunk: `rowsPerChunk_` is
    // `floor_pow2(chunkBytes / layout.rowLen)`, clamped to a minimum of 1
    // row per chunk (e.g. when a single row is wider than `chunkBytes`).
    explicit ViewTable(RowLayout layout, size_t initialIndexSlots = 4096,
                        size_t chunkBytes = 32u << 20, ViewTableKeys keys = {});

    // Returns a writable pointer to the row for `key`. Inserts a zeroed row
    // on first use. Idempotent: calling put() again for an existing key
    // returns that same row's pointer without creating a duplicate entry.
    // The uint64_t API needs keyWidth <= 8 and no prefix (a prefix table takes the prefix from the key bytes).
    uint8_t* put(uint64_t key);
    uint8_t* put(const ViewKey& key);   // key.len == keyWidth

    // Returns a pointer to the row for `key`, or nullptr if absent.
    const uint8_t* get(uint64_t key) const noexcept;
    const uint8_t* get(const ViewKey& key) const noexcept;

    // The dense row id of `key`'s row (stable while the row lives), or false when absent.  Cold-path identity for
    // consumers that track rows by id (the lease wheels).
    bool ridOf(const ViewKey& key, uint32_t& rid) const noexcept;

    // One probe instead of get() + ridOf(): the row of `key` and its row id (rid untouched when absent -> nullptr).
    const uint8_t* find(const ViewKey& key, uint32_t& rid) const noexcept;
    // put() that also reports the row id (one probe instead of put() + ridOf()).
    uint8_t* put(const ViewKey& key, uint32_t& rid);
    // The row at a live row id (from find / put): no probe.  Writing through it does NOT mark the row dirty (the owner of a
    // table whose dirty set is discarded, the lease index, uses it for in-place updates).
    uint8_t* rowOf(uint32_t rid) noexcept { return rowAddr(rid); }
    const uint8_t* rowOf(uint32_t rid) const noexcept { return rowAddr(rid); }

    // The auxiliary area of row `rid` (ViewTableKeys::auxBytesPerRow bytes, zero-filled when its chunk was allocated; a row id
    // reused after erase() keeps its old bytes, the owner resets them).  Stable address (chunked).  Requires auxBytesPerRow > 0.
    uint8_t* aux(uint32_t rid) noexcept {
        assert(auxBytes_ > 0 && rid < auxChunks_.size() * rowsPerChunk_);
        return auxChunks_[rid >> chunkShift_].get() + (rid & chunkMask_) * auxBytes_;
    }
    const uint8_t* aux(uint32_t rid) const noexcept {
        assert(auxBytes_ > 0 && rid < auxChunks_.size() * rowsPerChunk_);
        return auxChunks_[rid >> chunkShift_].get() + (rid & chunkMask_) * auxBytes_;
    }
    [[nodiscard]] uint32_t auxBytesPerRow() const noexcept { return static_cast<uint32_t>(auxBytes_); }
    // Every row id ever handed out is below this (a never-used id is bumped from it; erased ids go to the free-list and are
    // reused first).  A consumer that walks rows by id (the lease promotion pass) walks [0, ridHighWater()) and asks its own
    // liveness marker which of them hold a row.
    [[nodiscard]] uint32_t ridHighWater() const noexcept { return nextRid_; }
    // Row ids addressable without another chunk (rowsPerChunk x chunks).
    [[nodiscard]] size_t ridCapacity() const noexcept { return chunks_.size() * rowsPerChunk_; }

    // Removes `key`. Returns false if `key` was not present. O(1):
    // backward-shift in the index, the row-id returned to the free-list;
    // the row payload itself is left untouched (no tombstone, no move).
    bool erase(uint64_t key) noexcept;
    bool erase(const ViewKey& key) noexcept;

    // Prefix index (prefixWidth > 0): each prefix maps to a hashset of the keys of its rows.  Calls visit(key, payload)
    // for every row under `prefix`; a false return stops BEFORE consuming that row.  `resume` is empty (start of the
    // prefix's set) or kScanTokenBytes long: CursorGone if the prefix has no set or the token's version is not the
    // set's current one (any insert or erase under the prefix since).  On Stopped, `next` is the token to resume from.
    template <class Fn>
    PrefixScan scanPrefix(std::span<const uint8_t> prefix, std::span<const uint8_t> resume,
                          std::span<uint8_t, kScanTokenBytes> next, Fn&& visit) const noexcept {
        const uint32_t id = findSet(prefix);
        if (id == kNoSet) return resume.empty() ? PrefixScan::End : PrefixScan::CursorGone;
        const PrefixSet& s = sets_[id];
        uint32_t pos = 0;
        if (!resume.empty()) {
            uint64_t version;
            if (resume.size() != kScanTokenBytes) return PrefixScan::CursorGone;
            std::memcpy(&pos, resume.data(), 4);
            std::memcpy(&version, resume.data() + 4, 8);
            if (version != s.version) return PrefixScan::CursorGone;
        }
        const SetBlock cur = blockOf(s.block);
        const SetBlock old = s.migrating ? blockOf(s.oldBlock) : SetBlock{};
        for (size_t total = old.cap + cur.cap; pos < total; ++pos) {
            const SetBlock& b = pos < old.cap ? old : cur;
            const size_t i = pos < old.cap ? pos : pos - old.cap;
            if (b.tags[i] == kEmpty) continue;
            const uint64_t word = b.words[i];
            const uint8_t* payload = rowAddr(b.rids[i]);
            const std::span<const uint8_t> key = headerLen_ ? keyOfRow(payload)
                                                            : std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&word), keyWidth_);
            if (!visit(key, payload)) {
                std::memcpy(next.data(), &pos, 4);
                std::memcpy(next.data() + 4, &s.version, 8);
                return PrefixScan::Stopped;
            }
        }
        return PrefixScan::End;
    }

    // Tests only: the size and slot count of the set under `prefix` (0 when it has none), the most entries any one
    // op moved between a set's blocks, and the pool chunks allocated.
    static constexpr size_t kSetMigrate = 256;
    [[nodiscard]] uint32_t prefixSetSizeForTest(std::span<const uint8_t> prefix) const noexcept {
        const uint32_t id = findSet(prefix);
        return id == kNoSet ? 0 : sets_[id].size;
    }
    [[nodiscard]] uint32_t prefixSetCapacityForTest(std::span<const uint8_t> prefix) const noexcept {
        const uint32_t id = findSet(prefix);
        return id == kNoSet ? 0 : static_cast<uint32_t>(blockOf(sets_[id].block).cap);
    }
    [[nodiscard]] uint32_t maxSetEntriesMovedPerOpForTest() const noexcept { return maxSetMoved_; }
    [[nodiscard]] std::size_t prefixPoolChunksForTest() const noexcept { return poolChunks_.size(); }

    [[nodiscard]] uint8_t keyWidth() const noexcept { return keyWidth_; }
    [[nodiscard]] uint8_t prefixWidth() const noexcept { return prefixWidth_; }
    [[nodiscard]] uint64_t hashSeedForTest() const noexcept { return hashSeed_; }

    // The key stored in front of a wide row; empty for a narrow table (its key is the slot word).
    [[nodiscard]] std::span<const uint8_t> keyOfRow(const uint8_t* payload) const noexcept {
        return {payload - headerLen_, headerLen_};
    }

    /// The key bytes of live row `rid` (keyWidth bytes): a wide table's row header; a narrow table's key word as put()
    /// stored it in dirtyKeys_[rid] (the first keyWidth little-endian bytes).  Valid until the row is erased or re-put.
    [[nodiscard]] std::span<const uint8_t> keyOf(uint32_t rid) const noexcept {
        if (headerLen_) return keyOfRow(rowAddr(rid));
        return {reinterpret_cast<const uint8_t*>(&dirtyKeys_[rid]), keyWidth_};
    }

    // Header-key compares since construction, and the rids in primary-slot order.  Tests only.
    uint64_t arenaKeyReadsForTest() const noexcept { return arenaKeyReads_; }
    std::vector<uint32_t> slotOrderForTest() const {
        std::vector<uint32_t> order;
        for (size_t i = 0; i < main_.cap; ++i) if (main_.tags[i] != kEmpty) order.push_back(main_.rid[i]);
        return order;
    }

    size_t size() const noexcept { return sz_; }

    // Index slot count, doubled by every grow.  Tests only.
    size_t indexSlotsForTest() const noexcept { return main_.cap; }

    // Number of row-slab chunks currently allocated. Grows by 1 each time
    // `put` needs a row-id beyond the current total chunk capacity.
    size_t chunkCount() const noexcept { return chunks_.size(); }

    // Namespaces Phase 2 Task 12 (D26): the row-slab memory, chunkCount x bytes per chunk.  O(1) and incremental by
    // construction (chunks only ever append), so the view set charges it without scanning.
    size_t memoryBytes() const noexcept {
        return chunks_.size() * rowsPerChunk_ * (rowStride_ + auxBytes_) + poolBytes_ + (outer_.cap + outer_.ocap) * kSlotBytes;
    }

    // Task 5 test/bench hooks (Task 7's benchmark depends on these):
    //
    // Worst single-op cost (microseconds) any one put()/erase() call ever
    // paid for incremental-rehash maintenance (pre-provision zero-fill
    // slice, migrate step, or a startGrow — including its inline fallback
    // zero-fill when pre-provisioning wasn't ready in time). 0.0 if the
    // index never grew.
    double worstRehashTaxUs() const noexcept { return main_.maxRehashNs / 1000.0; }

    // Number of times startGrow() had to finish the next index buffer's
    // zero-fill inline because de-amortized pre-provisioning had not
    // completed yet (a write burst outran it). 0 means every grow swapped
    // in a fully pre-zeroed buffer in O(1).
    uint64_t rehashFallbacks() const noexcept { return main_.fallbacks; }

    // --- Task 6: row-sized dirty tracking for the differential checkpoint ---
    //
    // Checkpoint unit is row-sized (one dirty row == one {rid, rowBytes}
    // entry), per the §9.2 gate — NOT OS pages. `dirty_` is a per-rid bit
    // (grown alongside the row-slab chunks, NOT the index — a rehash moves
    // index entries, never rids, so a row's rid and its dirty bit stay
    // correct across any rehash). `put()` marks the rid it returns dirty;
    // `markDirty()` is also exposed publicly so the fold that writes
    // through the returned pointer can (redundantly/idempotently) flag it
    // too. `dirtyList_` records each rid the FIRST time it is marked dirty
    // in the current interval (coalescing repeats — a row put/written many
    // times between drains still emits exactly once).

    // Marks `rid`'s row dirty. O(1): a single bit test to avoid pushing a
    // duplicate `rid` onto dirtyList_ if it is already pending for this
    // interval. noexcept, no allocation beyond dirtyList_'s own
    // (amortized, reserve()'d) growth.
    void markDirty(uint32_t rid) noexcept;

    // Calls `emit(rid, row, deleted, key)` once for every row dirtied (an
    // upsert: `row` is the live row pointer, `deleted` is false) or deleted
    // (`row` is nullptr, `deleted` is true, `key` is the erased key — Task
    // 12's checkpoint replay needs the key since there is no row to copy)
    // since the last drainDirty() call, then clears both the dirty set and
    // the deleted list. O(dirty count); no allocation.
    //
    // ORDER: deletes are emitted BEFORE upserts. This matters for a same-key
    // delete-then-reinsert within one interval (put(A); erase(A); put(A)
    // again — erase() frees and clears A's old rid's dirty bit, then the
    // reinsert re-marks that SAME rid dirty): emitting the upsert first and
    // the delete second would let a replay-in-emit-order consumer (Task 12)
    // apply put(A)+copy-new-row then erase(A), permanently losing a key that
    // is actually still live. Delete-first is correct in every case: a
    // delete for a key NOT reinserted this interval is a safe no-op replay
    // against existing checkpoint state, and a delete immediately followed
    // by its own reinsert's upsert correctly leaves the key present with the
    // latest row. (See ViewTableDirtyTrackingRegression.
    // DeleteThenReinsertSameKeyWithinIntervalReplaysCorrectly.)
    template<class Fn>
    void drainDirty(Fn&& emit) {
        assert(keyWidth_ <= 8 && prefixWidth_ == 0);
        for (const auto& d : deleted_) {
            emit(d.rid, nullptr, /*deleted*/true, d.key);
        }
        deleted_.clear();
        deletedKeyBytes_.clear();
        for (uint32_t rid : dirtyList_) {
            // dirty_[rid] may have been cleared by an intervening erase()
            // (the row no longer exists) -- skip it then; the deleted_
            // loop above already carried that rid's one true entry.
            if (!dirty_[rid]) continue;
            dirty_[rid] = 0;
            emit(rid, rowAddr(rid), /*deleted*/false, dirtyKeys_[rid]);
        }
        dirtyList_.clear();
    }

    // As drainDirty, but `emit(rid, payload, deleted, key)` gets the keyWidth key bytes (a narrow key is the first
    // keyWidth little-endian bytes of the slot word).  Same order: deletes, then upserts.
    template<class Fn>
    void drainDirtyKeyed(Fn&& emit) {
        for (size_t n = 0; n < deleted_.size(); ++n) {
            const DeletedEntry& d = deleted_[n];
            if (headerLen_) {
                emit(d.rid, nullptr, /*deleted*/true, std::span<const uint8_t>(deletedKeyBytes_.data() + n * keyWidth_, keyWidth_));
            } else {
                emit(d.rid, nullptr, /*deleted*/true, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&d.key), keyWidth_));
            }
        }
        deleted_.clear();
        deletedKeyBytes_.clear();
        for (uint32_t rid : dirtyList_) {
            if (!dirty_[rid]) continue;
            dirty_[rid] = 0;
            const uint8_t* payload = rowAddr(rid);
            emit(rid, payload, /*deleted*/false,
                 headerLen_ ? keyOfRow(payload) : std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&dirtyKeys_[rid]), keyWidth_));
        }
        dirtyList_.clear();
    }

    // Test hook: candidate rids pending in the dirty set (NOT an exact
    // pending-dirty snapshot) as of the last mark/drain. May contain stale
    // or duplicate rids: a rid cleared by an intervening erase() (see
    // drainDirty()'s guard) or re-pushed after free-list reuse both leave
    // extra entries here that drainDirty() itself knows how to skip/resolve.
    std::span<const uint32_t> dirtyRids() const noexcept { return dirtyList_; }

    // Observability/test hook: erase()'d rows pending the next drainDirty().
    std::size_t pendingDeleteCount() const noexcept { return deleted_.size(); }

private:
    static constexpr uint8_t kEmpty = 0; // tag value for a never-used / vacated slot
    static constexpr uint8_t kTomb  = 1; // OLD-index tombstone: erased mid-migration, not re-migrated

    static constexpr double kGrowLoad    = 0.85; // primary load factor that triggers startGrow()
    static constexpr double kPrezeroLoad = 0.65; // primary load factor that starts pre-provisioning
    static constexpr size_t kMigK        = 256;  // max OLD entries migrated per migrateStep()
    static constexpr size_t kPrezero     = 1024; // max next-buffer entries zero-filled per maybePrezero()

    // Pre-provision must structurally outpace the grow cadence: between
    // crossing kPrezeroLoad and kGrowLoad there are (kGrowLoad-kPrezeroLoad)
    // * cap_ ops, each zero-filling up to kPrezero of the needed 2*cap_
    // next-buffer entries — so the margin (ops-available * kPrezero vs.
    // entries-needed) is the cap_-independent ratio below. This is WHY
    // rehashFallbacks() was observed to stay at 0 under every workload
    // tried (see task-5-report.md): with the constants above it holds with
    // ~100x headroom. If either constant above is ever changed, this catches
    // a regression to "pre-provisioning can't structurally keep up" at
    // compile time rather than as a silent, load-dependent perf cliff.
    static_assert(static_cast<double>(kPrezero) * (kGrowLoad - kPrezeroLoad) >= 2.0,
                  "pre-provision must outpace grow cadence: kPrezero * (kGrowLoad - kPrezeroLoad) must be >= 2.0 "
                  "(entries zero-filled per op * ops available in the pre-provision window >= 2x capacity needed)");

    static uint64_t mix(uint64_t x) noexcept;
    static uint8_t  fullTag(uint64_t h) noexcept;

    // The hash a stored slot word was placed by: a wide word is already the hash; a narrow word is the key.
    uint64_t hashOfWord(uint64_t word) const noexcept {
        return headerLen_ ? word : (mix(word ^ hashSeed_) & hashMask_);
    }

    // What an index probe looks for: the slot word, its hash, and (wide only) the key bytes to compare in the row header.
    struct Probe { uint64_t word; uint64_t h; const uint8_t* wideKey; };
    Probe probeOf(uint64_t narrowKey) const noexcept { return {narrowKey, hashOfWord(narrowKey), nullptr}; }
    Probe probeOf(const ViewKey& key) const noexcept;

    uint8_t*       putImpl(const Probe& p, uint32_t* ridOut = nullptr);
    const uint8_t* getImpl(const Probe& p) const noexcept;
    bool           eraseImpl(const Probe& p) noexcept;

    // Returns the PRIMARY (NEW) index slot holding the probe's key, or SIZE_MAX if absent.
    size_t findP(const Probe& p) const noexcept;

    // Returns the OLD index slot holding the probe's key, or SIZE_MAX if absent or
    // not currently migrating. Only ever non-empty while `migrating_`.
    size_t findO(const Probe& p) const noexcept;

    // True if the row `rid`'s header key equals `key`.
    bool headerKeyEquals(uint32_t rid, const uint8_t* key) const noexcept;

    // An open-addressing index of (tag, word, rid) slots with incremental, de-amortized growth: the main key index,
    // and the prefix -> set map.  `wordIsHash` says the word is already the hash (wide keys, prefixes); otherwise it is
    // the exact narrow key and `home()` hashes it.  Growth: once the load crosses kPrezeroLoad `maybePrezero()` zero-fills
    // the next 2x buffers kPrezero entries per op; at kGrowLoad `startGrow()` promotes the index to OLD (`ot`/`ok`/`orid`,
    // drain cursor `ocur`) and `migrateStep()` moves at most kMigK OLD entries per op into the 2x NEW one.  An erase of
    // an OLD-only entry tombstones it (kTomb) rather than shifting; OLD is draining, not steady-state.
    struct SlotIndex {
        std::vector<uint8_t>  tags;   // 1-byte Swiss/F14-style tag per index slot
        std::vector<uint64_t> keys;   // the slot word
        std::vector<uint32_t> rid;    // dense row-id (or prefix-set id) per index slot
        size_t cap  = 0;              // index slot count (power of two)
        size_t mask = 0;              // cap - 1

        std::vector<uint8_t>  ot;
        std::vector<uint64_t> ok;
        std::vector<uint32_t> orid;
        size_t ocap      = 0;
        size_t omask     = 0;
        size_t ocur      = 0;
        bool   migrating = false;

        // Pre-provision buffers for the NEXT 2x index.  prezeroCap == 0: nothing in flight; otherwise the target
        // capacity (cap * 2 of the generation that started it) and prezeroCur how much is zero-filled so far.
        std::vector<uint8_t>  nextTags;
        std::vector<uint64_t> nextKeys;
        std::vector<uint32_t> nextRid;
        size_t prezeroCap = 0;
        size_t prezeroCur = 0;

        double   maxRehashNs = 0.0;   // worst single-op rehash-upkeep cost (see worstRehashTaxUs())
        uint64_t fallbacks   = 0;     // startGrow() calls that had to finish the zero-fill inline

        bool     wordIsHash = false;
        uint64_t seed       = 0;
        uint64_t hashMask   = ~uint64_t{0};

        void init(size_t slots, bool wordIsHash_, uint64_t seed_, uint64_t hashMask_);
        uint64_t home(uint64_t word) const noexcept { return wordIsHash ? word : (mix(word ^ seed) & hashMask); }
        // Backward-shift deletion over the PRIMARY arrays only, starting at the already-found slot `i`.
        void backshift(size_t i) noexcept;
        // `sz` is the live entry count.
        void maybePrezero(size_t sz) noexcept;
        void startGrow();
        void migrateStep() noexcept;
    };

    // --- the prefix index -------------------------------------------------------------------------------------
    // The outer SlotIndex maps the seeded hash of a prefix to a PrefixSet id.  A set is a hashset of the keys of the
    // rows under the prefix, in one block of `16 << cls` 13-byte slots (tag | word | rid, exactly the main index's
    // slot).  Blocks come from pool chunks and never move.  A set grows or shrinks by taking a block of the next/
    // previous class and moving at most kSetMigrate entries per op out of the old block, which is a real hash table
    // (backshift deletes) until it drains; lookups probe both blocks meanwhile.
    struct SetBlock { uint8_t* tags = nullptr; uint64_t* words = nullptr; uint32_t* rids = nullptr; size_t cap = 0; };
    struct PrefixSet {
        uint64_t version   = 0;
        uint32_t block     = 0;
        uint32_t size      = 0;
        uint32_t oldBlock  = 0;   // while migrating
        uint32_t cursor    = 0;   // next slot of oldBlock to drain
        bool     migrating = false;
    };
    struct PoolChunk { std::unique_ptr<uint8_t[]> tags; std::unique_ptr<uint64_t[]> words; std::unique_ptr<uint32_t[]> rids; };
    struct BlockRef { uint8_t* tags; uint64_t* words; uint32_t* rids; };

    static constexpr uint32_t kNoSet = UINT32_MAX;
    static constexpr size_t kSlotBytes = 13;   // tag + word + rid

    SetBlock blockOf(uint32_t id) const noexcept {
        const BlockRef& r = blocks_[id];
        return {r.tags, r.words, r.rids, size_t{16} << blockClass_[id]};
    }
    const uint8_t* setPrefix(uint32_t id) const noexcept { return setPrefixBytes_.data() + size_t{id} * prefixWidth_; }
    // The id of the set under `prefix`, or kNoSet.
    uint32_t findSet(std::span<const uint8_t> prefix) const noexcept;
    uint32_t findSetHashed(uint64_t h, const uint8_t* prefix) const noexcept;
    const uint8_t* prefixBytesOf(const Probe& p) const noexcept {
        return p.wideKey ? p.wideKey : reinterpret_cast<const uint8_t*>(&p.word);   // little-endian, like ViewKey::asU64
    }
    void     appendPoolChunk(uint8_t cls);
    uint32_t allocBlock(uint8_t cls);
    void     freeBlock(uint32_t id) { blockFree_[blockClass_[id]].push_back(id); }
    void     startSetMigration(PrefixSet& s, uint8_t cls);
    // Spends from `budget` (the op's remaining kSetMigrate) and adds the entries moved to `moved`.
    void     migrateSet(PrefixSet& s, size_t& budget, uint32_t& moved) noexcept;
    void     prefixInsert(const Probe& p, uint32_t rid);
    void     prefixErase(const Probe& p, uint32_t rid) noexcept;



    // Allocates one more chunk (rowsPerChunk_ * layout_.rowLen bytes,
    // zero-initialized) and appends it. This is the ONE deliberate
    // allocation on the put() growth path; existing rid->address mappings
    // are unaffected because `chunks_` grows by push_back (prior chunk
    // vectors/pointers are never touched or moved — only a new element is
    // added) and rows live inside already-allocated chunks, never inside
    // the chunks_ vector's own backing storage.
    void appendChunk();

    // rid -> row address: shift picks the chunk, mask picks the in-chunk
    // row slot. A chunk is a single contiguous std::vector<uint8_t>, so
    // this is a shift + mask + one pointer-load + multiply-add — no
    // branching, no allocation, a few ns. Rows never move once allocated:
    // growth only ever appends a new chunk (see appendChunk()), so a
    // pointer returned for a given rid stays valid for the table's
    // lifetime.
    uint8_t* rowAddr(uint32_t rid) noexcept {
        auto& chunk = chunks_[static_cast<size_t>(rid) >> chunkShift_];
        const size_t inChunk = static_cast<size_t>(rid) & chunkMask_;
        return chunk.get() + inChunk * rowStride_ + headerLen_;
    }
    const uint8_t* rowAddr(uint32_t rid) const noexcept {
        const auto& chunk = chunks_[static_cast<size_t>(rid) >> chunkShift_];
        const size_t inChunk = static_cast<size_t>(rid) & chunkMask_;
        return chunk.get() + inChunk * rowStride_ + headerLen_;
    }

    RowLayout layout_;
    uint8_t   keyWidth_    = 8;
    uint8_t   prefixWidth_ = 0;
    uint64_t  hashSeed_    = 0;
    uint64_t  hashMask_    = ~uint64_t{0};
    uint32_t  headerLen_   = 0;  // keyWidth_ when wide (> 8), else 0
    size_t    rowStride_   = 0;  // headerLen_ + layout_.rowLen
    size_t    auxBytes_    = 0;  // ViewTableKeys::auxBytesPerRow
    std::vector<std::unique_ptr<uint8_t[]>> auxChunks_;   // parallel to chunks_ (empty when auxBytes_ == 0)
    mutable uint64_t arenaKeyReads_ = 0;

    SlotIndex main_;   // key -> rid
    // Row-payload chunked arena; rows never move.  Each chunk is a zeroed array (one memset in appendChunk),
    // not a std::vector<uint8_t>: an unoptimized (Debug / sanitizer) build constructed and destroyed a
    // vector chunk byte by byte — ~0.1 s per view table in every test that installs a view.
    std::vector<std::unique_ptr<uint8_t[]>> chunks_;
    std::vector<uint32_t> free_;   // row-id free-list (LIFO; reused before nextRid_)

    uint32_t nextRid_ = 0;  // next never-used row-id, bumped when free_ is empty
    size_t   sz_      = 0;  // live key count

    size_t rowsPerChunk_ = 0;  // power of two: floor_pow2(chunkBytes / rowLen), min 1
    size_t chunkShift_   = 0;  // log2(rowsPerChunk_); rid >> chunkShift_ == chunk index
    size_t chunkMask_    = 0;  // rowsPerChunk_ - 1; rid & chunkMask_ == in-chunk row slot

    // The prefix index (prefixWidth_ > 0 only).
    SlotIndex outer_;                          // prefix hash -> set id
    size_t outerCount_ = 0;                    // live sets
    std::vector<PrefixSet> sets_;
    std::vector<uint8_t>   setPrefixBytes_;    // prefixWidth_ bytes per set id
    std::vector<uint32_t>  setFree_;           // set ids to reuse
    std::vector<PoolChunk> poolChunks_;
    std::vector<BlockRef>  blocks_;            // block id -> its slots
    std::vector<uint8_t>   blockClass_;        // block id -> class
    std::vector<std::vector<uint32_t>> blockFree_;   // per class
    size_t   chunkBytes_ = 0;
    uint64_t versionClock_ = 0;                // set versions; seeded from std::random_device
    size_t   poolBytes_ = 0;                   // bytes of poolChunks_
    uint32_t maxSetMoved_ = 0;

    // --- Task 6: row-sized dirty tracking -------------------------------
    // `dirty_`/`dirtyKeys_` are indexed by `rid` and sized to the rid space
    // (grown in appendChunk(), i.e. alongside chunk growth — NEVER touched
    // by index rehash, since a rehash relocates (tag,key,rid) index
    // entries but a row's rid itself never changes). `dirty_[rid]` is the
    // per-row dirty bit; `dirtyKeys_[rid]` is the key last written to that
    // rid via put() (recorded there, not inside markDirty(), because
    // markDirty() only takes a rid per its public signature) so
    // drainDirty() can emit `key` for upserts too, not just deletes.
    std::vector<uint8_t>  dirty_;
    std::vector<uint64_t> dirtyKeys_;   // narrow table: the key word put() stored (keyOf reads it); wide table: the hashed slot word, never read
    std::vector<uint32_t> dirtyList_; // rids marked dirty this interval, each pushed once (coalesced)

    struct DeletedEntry { uint32_t rid; uint64_t key; };
    std::vector<DeletedEntry> deleted_; // erase()'d {rid,key} pairs pending drainDirty()
    std::vector<uint8_t> deletedKeyBytes_; // wide tables: keyWidth_ bytes per deleted_ entry, in the same order
};

} // namespace rune::view
