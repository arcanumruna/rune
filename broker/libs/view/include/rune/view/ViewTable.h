// libs/view/include/rune/view/ViewTable.h
#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>
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
class ViewTable {
public:
    // `initialIndexSlots` must be a power of two (capacity of the
    // open-addressing index; index growth is Task 5).
    // `chunkBytes` sizes each row-slab chunk: `rowsPerChunk_` is
    // `floor_pow2(chunkBytes / layout.rowLen)`, clamped to a minimum of 1
    // row per chunk (e.g. when a single row is wider than `chunkBytes`).
    explicit ViewTable(RowLayout layout, size_t initialIndexSlots = 4096,
                        size_t chunkBytes = 32u << 20);

    // Returns a writable pointer to the row for `key`. Inserts a zeroed row
    // on first use. Idempotent: calling put() again for an existing key
    // returns that same row's pointer without creating a duplicate entry.
    uint8_t* put(uint64_t key);

    // Returns a pointer to the row for `key`, or nullptr if absent.
    const uint8_t* get(uint64_t key) const noexcept;

    // Removes `key`. Returns false if `key` was not present. O(1):
    // backward-shift in the index, the row-id returned to the free-list;
    // the row payload itself is left untouched (no tombstone, no move).
    bool erase(uint64_t key) noexcept;

    size_t size() const noexcept { return sz_; }

    // Number of row-slab chunks currently allocated. Grows by 1 each time
    // `put` needs a row-id beyond the current total chunk capacity.
    size_t chunkCount() const noexcept { return chunks_.size(); }

    // Task 5 test/bench hooks (Task 7's benchmark depends on these):
    //
    // Worst single-op cost (microseconds) any one put()/erase() call ever
    // paid for incremental-rehash maintenance (pre-provision zero-fill
    // slice, migrate step, or a startGrow — including its inline fallback
    // zero-fill when pre-provisioning wasn't ready in time). 0.0 if the
    // index never grew.
    double worstRehashTaxUs() const noexcept { return maxRehashNs_ / 1000.0; }

    // Number of times startGrow() had to finish the next index buffer's
    // zero-fill inline because de-amortized pre-provisioning had not
    // completed yet (a write burst outran it). 0 means every grow swapped
    // in a fully pre-zeroed buffer in O(1).
    uint64_t rehashFallbacks() const noexcept { return fallbacks_; }

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
        for (const auto& d : deleted_) {
            emit(d.rid, nullptr, /*deleted*/true, d.key);
        }
        deleted_.clear();
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

    // Returns the PRIMARY (NEW) index slot holding `key`, or SIZE_MAX if absent.
    size_t findP(uint64_t key) const noexcept;

    // Returns the OLD index slot holding `key`, or SIZE_MAX if absent or
    // not currently migrating. Only ever non-empty while `migrating_`.
    size_t findO(uint64_t key) const noexcept;

    // Backward-shift deletion (see erase()'s doc) over the PRIMARY index
    // arrays only, starting at the already-found slot `i`.
    void backshiftPrimary(size_t i) noexcept;

    // Once the primary index's load factor crosses kPrezeroLoad (and no
    // grow is currently in flight), zero-fills up to kPrezero more entries
    // of the next 2x-capacity nextTags_/nextKeys_/nextRid_buf_ buffers
    // (reserving their backing storage, a fast non-zeroing allocation, on
    // the first call for a given target capacity). A no-op once the next
    // buffer is fully provisioned, or while `migrating_` is true.
    void maybePrezero() noexcept;

    // Promotes the current primary index to OLD and installs a fresh 2x
    // primary: swaps in the pre-provisioned next buffer in O(1) if
    // maybePrezero() finished it in time, otherwise finishes the zero-fill
    // inline right here (counted via fallbacks_). Sets migrating_ = true.
    void startGrow();

    // Moves up to kMigK still-live OLD index entries into the PRIMARY
    // index. Clears migrating_ (and frees the OLD arrays) once the OLD
    // index has been fully drained. A no-op if not currently migrating.
    void migrateStep() noexcept;

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
        return chunk.get() + inChunk * layout_.rowLen;
    }
    const uint8_t* rowAddr(uint32_t rid) const noexcept {
        const auto& chunk = chunks_[static_cast<size_t>(rid) >> chunkShift_];
        const size_t inChunk = static_cast<size_t>(rid) & chunkMask_;
        return chunk.get() + inChunk * layout_.rowLen;
    }

    RowLayout layout_;

    std::vector<uint8_t>  tags_;   // 1-byte Swiss/F14-style tag per index slot
    std::vector<uint64_t> keys_;   // exact 8-byte key per index slot
    std::vector<uint32_t> rid_;    // dense row-id per index slot
    // Row-payload chunked arena; rows never move.  Each chunk is a zeroed array (one memset in appendChunk),
    // not a std::vector<uint8_t>: an unoptimized (Debug / sanitizer) build constructed and destroyed a
    // vector chunk byte by byte — ~0.1 s per view table in every test that installs a view.
    std::vector<std::unique_ptr<uint8_t[]>> chunks_;
    std::vector<uint32_t> free_;   // row-id free-list (LIFO; reused before nextRid_)

    uint32_t nextRid_ = 0;  // next never-used row-id, bumped when free_ is empty
    size_t   sz_      = 0;  // live key count
    size_t   cap_     = 0;  // index slot count (power of two)
    size_t   mask_    = 0;  // cap_ - 1

    size_t rowsPerChunk_ = 0;  // power of two: floor_pow2(chunkBytes / rowLen), min 1
    size_t chunkShift_   = 0;  // log2(rowsPerChunk_); rid >> chunkShift_ == chunk index
    size_t chunkMask_    = 0;  // rowsPerChunk_ - 1; rid & chunkMask_ == in-chunk row slot

    // --- Task 5: incremental index rehash -----------------------------
    // OLD index: the pre-grow (tags_/keys_/rid_), kept around only for the
    // duration of the migration window (migrating_ == true). Entries with
    // tags_[.] >= 0x80 (fullTag()'s range) at ot_[ocur_.. ) are still live
    // and not yet migrated; kEmpty/kTomb entries are skipped by
    // migrateStep(). ocur_ is the drain cursor: everything before it has
    // already been visited (migrated or discarded).
    std::vector<uint8_t>  ot_;
    std::vector<uint64_t> ok_;
    std::vector<uint32_t> orid_;
    size_t ocap_      = 0;
    size_t omask_     = 0;
    size_t ocur_      = 0;
    bool   migrating_ = false;

    // De-amortized pre-provision buffers for the NEXT 2x primary index,
    // filled kPrezero entries at a time by maybePrezero(). prezeroCap_ == 0
    // means no provisioning is in flight/pending; otherwise it is the
    // target capacity (always cap_*2 for the cap_ generation that started
    // it) and prezeroCur_ (== nextTags_.size()) is how much of it has been
    // zero-filled so far. Fully provisioned once prezeroCur_ == prezeroCap_.
    std::vector<uint8_t>  nextTags_;
    std::vector<uint64_t> nextKeys_;
    std::vector<uint32_t> nextRid_buf_;
    size_t prezeroCap_ = 0;
    size_t prezeroCur_ = 0;

    // Diagnostics (Task 7 benchmark hooks): worst single-op rehash-upkeep
    // cost ever paid (ns; see worstRehashTaxUs()), and how many startGrow()
    // calls had to finish the next buffer's zero-fill inline because
    // pre-provisioning wasn't ready (see rehashFallbacks()).
    double   maxRehashNs_ = 0.0;
    uint64_t fallbacks_   = 0;

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
    std::vector<uint64_t> dirtyKeys_;
    std::vector<uint32_t> dirtyList_; // rids marked dirty this interval, each pushed once (coalesced)

    struct DeletedEntry { uint32_t rid; uint64_t key; };
    std::vector<DeletedEntry> deleted_; // erase()'d {rid,key} pairs pending drainDirty()
};

} // namespace rune::view
