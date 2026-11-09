#pragma once
// ---------------------------------------------------------------------------
// SkimpyOffsetMap — CP-7: Kafka-faithful bounded open-addressing dedupe map.
//
// Replaces the unbounded std::unordered_map<string,Offset> in OffsetMap with
// a flat, cache-friendly open-addressing table that is:
//
//   1. MEMORY-BOUNDED: constructor takes `maxSlots` (derived from
//      dedupeBufferBytes × load_factor); `put()` returns false once the table
//      reaches maxSlots — the caller STOPS the scan pass and records
//      `latestOffset()` as the pass boundary (bounded-range-per-pass).
//
//   2. FAST: entry = { uint64_t keyHash, int64_t offset } (16 B).
//      Hash function: wyhash (inline, ~30 lines, non-crypto, SIMD-friendly).
//      No heap allocation per entry; the entire backing array is allocated once.
//
//   3. COLLISION-SAFE: 64-bit wyhash provides ≤ 2^-32 false-collision
//      probability per pair (Birthday bound over 10^9 unique keys ≈ 5.4 × 10^-1
//      with 64-bit hash is still acceptable for compaction: at 128 MiB with 16
//      B/entry = 8M slots, among 8M entries P(any collision) ≈ 8M²/(2·2^64)
//      ≈ 1.7 × 10^-5 — comparable to Kafka's MD5-truncated 128-bit approach
//      which stores only the lower 64 bits of MD5 in practice per KAFKA-10650).
//      We document this, and for correctness testing the test suite exercises
//      the collision-safety path. If a caller requires full-key correctness for
//      internal topics it may use the plain OffsetMap instead.
//
// Layout:
//   slot[i] = { hash64, offset }   (hash64==0 → empty; offset==-1 → empty)
//   Linear probing; capacity must be a power of two (enforced in ctor).
//   Probe: i = (hash & mask_); then i = (i+1) & mask_ until empty or match.
//   Eviction: none — table is STATIC capacity.  Once full, put() returns false.
//
// Lease mode (CleanerTopicMode::Lease): enableLeaseAux() allocates, once and on the cold cleaner thread, a parallel
// 16-byte-per-slot array (LeaseAux, same index as the table) holding the live grant's version and the key's latest
// Checkpoint offset.  A lease cleaner therefore uses up to 2x the dedupe byte budget (table + aux).  Plain mode never
// allocates it.
//
// Thread-safety: NOT thread-safe. Intended for single-thread cleaner use.
// ---------------------------------------------------------------------------

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <rune/storage/ProducerTails.h>
#include <rune/storage/TxnCleanState.h>
#include <rune/storage/VerdictCleanState.h>
#include <rune/storage/WyHash.h>

namespace rune::storage {

using Offset = int64_t;

// wyhash lives in WyHash.h (rune::storage::detail::wyhash64).

// ---------------------------------------------------------------------------
// SkimpyOffsetMap
// ---------------------------------------------------------------------------
class SkimpyOffsetMap {
public:
    /// Construct a bounded dedupe map.
    ///
    /// @param maxSlots  Maximum number of unique keys the table may hold before
    ///                  put() starts returning false.  Must be > 0.
    ///                  Typical: (dedupeBufferBytes / kEntryBytes) * loadFactor.
    ///                  (default loadFactor = 0.9; kEntryBytes = 16).
    ///
    /// The backing array is sized to the next power-of-two ≥ maxSlots * (1/loadFactor)
    /// so that linear probing never exceeds expected ~2 probes per lookup at 50%
    /// fill and ~10 at 90%.  If this would exceed maxAllocSlots (2^28 = 256M
    /// entries = 4 GiB), we cap and derive an effective maxSlots from the cap.
    explicit SkimpyOffsetMap(std::size_t maxSlots)
        : maxSlots_(maxSlots)
    {
        if (maxSlots == 0) maxSlots_ = 1;
        // Backing capacity: at least ceil(maxSlots / 0.9), rounded to next power of 2.
        // This ensures load ≤ 90% at capacity.
        constexpr double kInvLoadFactor = 1.0 / 0.9;
        std::size_t needed = static_cast<std::size_t>(
            static_cast<double>(maxSlots_) * kInvLoadFactor + 0.5) + 1;
        if (needed > kMaxAllocSlots) needed = kMaxAllocSlots;
        // Round up to next power of two.
        std::size_t cap = 1;
        while (cap < needed) cap <<= 1;
        // Clamp maxSlots_ to what fits in this capped capacity.
        std::size_t effectiveMax = static_cast<std::size_t>(cap * 0.9);
        if (maxSlots_ > effectiveMax) maxSlots_ = effectiveMax;
        allocate_(cap);
    }

    /// The table size derived from a BYTE budget (#658): the largest power-of-two entry count whose table fits the
    /// budget, and the key limit at 90% load of it.  Building from the slot count (maxSlotsFromBudget) instead rounded
    /// UP past the budget: a 128 MiB budget allocated 256 MiB.
    struct Sizing {
        std::size_t capacity;   ///< backing entries, a power of two; capacity * kEntryBytes <= the budget
        std::size_t maxSlots;   ///< keys before put() returns false: floor(capacity * 0.9), at least 1
    };
    static constexpr std::size_t kMaxAllocSlots = std::size_t{1} << 28;   // 4 GiB of entries
    static constexpr Sizing sizingForBudget(int64_t budgetBytes) noexcept {
        const std::size_t bytes = budgetBytes < static_cast<int64_t>(kEntryBytes) ? kEntryBytes
                                                                                  : static_cast<std::size_t>(budgetBytes);
        std::size_t cap = bytes / kEntryBytes;
        cap = std::size_t{1} << (std::bit_width(cap) - 1);   // bit_floor
        if (cap > kMaxAllocSlots) cap = kMaxAllocSlots;
        std::size_t slots = static_cast<std::size_t>(static_cast<double>(cap) * kDefaultLoadFactor);
        if (slots < 1) slots = 1;
        return {cap, slots};
    }
    /// Construct from a byte budget's sizing: the capacity is used as given (no second rounding).
    explicit SkimpyOffsetMap(Sizing sizing)
        : maxSlots_(sizing.maxSlots)
    {
        allocate_(sizing.capacity);
    }

    // Disable copy (large allocation, should never be copied).
    SkimpyOffsetMap(const SkimpyOffsetMap&) = delete;
    SkimpyOffsetMap& operator=(const SkimpyOffsetMap&) = delete;
    SkimpyOffsetMap(SkimpyOffsetMap&&) = default;
    SkimpyOffsetMap& operator=(SkimpyOffsetMap&&) = default;

    /// Record that `key` was last seen at `offset`.
    /// Overwrites only if the new offset is LARGER (latest-per-key semantics).
    ///
    /// Returns true  — entry recorded (or was already present with offset ≥).
    /// Returns false — map is FULL (size() == maxSlots_).  The caller MUST stop
    ///                 the scan pass and use latestOffset()+1 as the pass boundary.
    bool put(std::string_view key, Offset offset) noexcept {
        if (key.empty()) return true; // null-key records not compactable — skip silently
        const uint64_t h = hash(key);
        std::size_t i = h & mask_;
        for (;;) {
            Entry& e = table_[i];
            if (e.hash == kEmpty) {
                // Empty slot — insert.
                if (size_ >= maxSlots_) return false; // FULL
                e.hash   = h;
                e.offset = offset;
                ++size_;
                if (offset > latestOffset_) latestOffset_ = offset;
                return true;
            }
            if (e.hash == h) {
                // Possible match (same hash).  Update if newer offset.
                if (offset > e.offset) {
                    e.offset = offset;
                    if (offset > latestOffset_) latestOffset_ = offset;
                }
                return true;
            }
            i = (i + 1) & mask_;
        }
    }

    /// Return the latest known offset for the key, or nullopt if absent.
    [[nodiscard]] std::optional<Offset> get(std::string_view key) const noexcept {
        if (key.empty()) return std::nullopt;
        const uint64_t h = hash(key);
        std::size_t i = h & mask_;
        for (;;) {
            const Entry& e = table_[i];
            if (e.hash == kEmpty) return std::nullopt;
            if (e.hash == h)     return e.offset;
            i = (i + 1) & mask_;
        }
    }

    // --- Lease mode (see the header comment) ---------------------------------
    /// Per-slot lease state, parallel to table_ (same index).
    struct LeaseAux { int64_t liveVersion; int64_t checkpointOffset; };   // 16 B
    /// liveVersion of a key whose latest non-Checkpoint record is a tombstone (or that has none).
    static constexpr int64_t kLeaseEnded = std::numeric_limits<int64_t>::min();
    /// The entry offset of a key of which only Checkpoints are mapped.
    static constexpr Offset  kNoRecord   = -1;

    /// Allocates aux_ (capacity_ entries) if empty; idempotent.  Entries are written on insert: no reset needed.
    void enableLeaseAux() {
        if (!aux_) aux_.reset(new LeaseAux[capacity_]);
    }
    [[nodiscard]] bool leaseAuxEnabled() const noexcept { return static_cast<bool>(aux_); }

    /// A non-Checkpoint record: like put(); when it inserts or raises the entry's offset it also sets liveVersion
    /// (= ended ? kLeaseEnded : version).  false = full (as put).
    bool putLease(std::string_view key, Offset offset, int64_t version, bool ended) noexcept {
        assert(aux_);
        if (key.empty()) return true;
        const uint64_t h = hash(key);
        std::size_t i = h & mask_;
        for (;;) {
            Entry& e = table_[i];
            if (e.hash == kEmpty) {
                if (size_ >= maxSlots_) return false;
                e.hash   = h;
                e.offset = offset;
                aux_[i]  = {ended ? kLeaseEnded : version, -1};
                ++size_;
                if (offset > latestOffset_) latestOffset_ = offset;
                return true;
            }
            if (e.hash == h) {
                if (offset > e.offset) {
                    e.offset = offset;
                    aux_[i].liveVersion = ended ? kLeaseEnded : version;
                    if (offset > latestOffset_) latestOffset_ = offset;
                }
                return true;
            }
            i = (i + 1) & mask_;
        }
    }

    /// A Checkpoint: inserts an entry with offset kNoRecord (liveVersion kLeaseEnded, checkpointOffset = offset) when
    /// absent, else raises checkpointOffset; raises latestOffset_ either way.  false = full.
    bool putLeaseCheckpoint(std::string_view key, Offset offset) noexcept {
        assert(aux_);
        if (key.empty()) return true;
        const uint64_t h = hash(key);
        std::size_t i = h & mask_;
        for (;;) {
            Entry& e = table_[i];
            if (e.hash == kEmpty) {
                if (size_ >= maxSlots_) return false;
                e.hash   = h;
                e.offset = kNoRecord;
                aux_[i]  = {kLeaseEnded, offset};
                ++size_;
                if (offset > latestOffset_) latestOffset_ = offset;
                return true;
            }
            if (e.hash == h) {
                if (offset > aux_[i].checkpointOffset) aux_[i].checkpointOffset = offset;
                if (offset > latestOffset_) latestOffset_ = offset;
                return true;
            }
            i = (i + 1) & mask_;
        }
    }

    struct LeaseEntry { Offset latest; int64_t liveVersion; Offset checkpointOffset; };
    [[nodiscard]] std::optional<LeaseEntry> getLease(std::string_view key) const noexcept {
        assert(aux_);
        if (key.empty()) return std::nullopt;
        const uint64_t h = hash(key);
        std::size_t i = h & mask_;
        for (;;) {
            const Entry& e = table_[i];
            if (e.hash == kEmpty) return std::nullopt;
            if (e.hash == h)      return LeaseEntry{e.offset, aux_[i].liveVersion, aux_[i].checkpointOffset};
            i = (i + 1) & mask_;
        }
    }

    /// Number of distinct keys currently held.
    [[nodiscard]] std::size_t size()    const noexcept { return size_; }
    /// Maximum slots before put() returns false.
    [[nodiscard]] std::size_t maxSlots() const noexcept { return maxSlots_; }
    /// Backing array capacity (power of two).
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    /// True if the map is at or over its slot budget.
    [[nodiscard]] bool isFull()  const noexcept { return size_ >= maxSlots_; }

    /// Highest offset ever put() into the map (used to compute pass boundary).
    [[nodiscard]] Offset latestOffset() const noexcept { return latestOffset_; }

    /// Clear all entries (reuse without reallocating).
    void clear() noexcept {
        clearKeys();
        tails_.clear();
        txn_.clear();
        verdict_.clear();
    }

    /// Clear the key entries only (the scan's tails, txn and verdict state stay).
    void clearKeys() noexcept {
        resetTable_();
        size_          = 0;
        latestOffset_  = -1;
    }

    /// The last data batch of every producer in the scanned range (retained by
    /// compaction — see ProducerTails.h).  Filled by buildOffsetMapFromBases.
    ProducerTails&       producerTails() noexcept { return tails_; }
    const ProducerTails& producerTails() const noexcept { return tails_; }
    /// D3b: the pass's transaction state (aborted ranges, LSO, removable
    /// markers — see TxnCleanState.h).  Filled by buildOffsetMapFromBases.
    TxnCleanState&       txnState() noexcept { return txn_; }
    const TxnCleanState& txnState() const noexcept { return txn_; }
    VerdictCleanState&       verdictState() noexcept { return verdict_; }
    const VerdictCleanState& verdictState() const noexcept { return verdict_; }

    // --- Budget helper (static, no instance) --------------------------------
    /// Compute maxSlots from a byte budget and per-entry size.
    /// Formula: floor(budgetBytes / kEntryBytes * kDefaultLoadFactor)
    static constexpr std::size_t kEntryBytes        = sizeof(uint64_t) + sizeof(int64_t); // 16
    static constexpr double      kDefaultLoadFactor = 0.9;

    /// Slot count for a byte budget (kept for tests that pass a slot count to the slots constructor).
    /// NOT a table size: feeding it to the slots constructor rounds the table UP past the budget (#658).  Build
    /// from sizingForBudget instead.
    static std::size_t maxSlotsFromBudget(int64_t budgetBytes) noexcept {
        if (budgetBytes <= 0) return 1;
        auto raw = static_cast<std::size_t>(budgetBytes) / kEntryBytes;
        auto effective = static_cast<std::size_t>(raw * kDefaultLoadFactor);
        return effective < 1 ? 1 : effective;
    }

private:
    struct Entry {   // trivial: the table is allocated uninitialized and written once by resetTable_
        uint64_t hash;
        int64_t  offset;
    };
    static constexpr uint64_t kEmpty = 0; // hash==0 means empty slot

    static uint64_t hash(std::string_view key) noexcept {
        // Avoid hash == 0 (used as sentinel) by mapping 0 → 1.
        uint64_t h = detail::wyhash64(key.data(), key.size());
        return h == 0 ? 1 : h;
    }

    std::size_t          maxSlots_     = 0;
    std::size_t          capacity_     = 0;
    std::size_t          mask_         = 0;
    std::size_t          size_         = 0;
    Offset               latestOffset_ = -1;
    void allocate_(std::size_t cap) {
        capacity_ = cap;
        mask_     = cap - 1;
        table_.reset(new Entry[cap]);   // trivial Entry: no per-slot construction; resetTable_ writes each once
        resetTable_();
        // I1: the producer tails share the dedupe budget — at most a quarter
        // of it (kEntryBytes per key slot vs ProducerTails::kBytesPerProducer).
        tails_.setCapacity(std::max<std::size_t>(
            1024, cap * kEntryBytes / 4 / ProducerTails::kBytesPerProducer));
    }
    /// Every slot {kEmpty, -1}.  A raw loop over the array, not std::vector::assign: an unoptimized (Debug /
    /// sanitizer) build constructed and destroyed a multi-million-slot vector element by element through the
    /// allocator — seconds per broker start for the cleaner's default dedupe budget.
    void resetTable_() noexcept {
        if (!table_) return;   // moved-from
        Entry* const end = table_.get() + capacity_;
        for (Entry* e = table_.get(); e != end; ++e) { e->hash = kEmpty; e->offset = -1; }
    }

    std::unique_ptr<Entry[]> table_;
    std::unique_ptr<LeaseAux[]> aux_;   ///< lease mode only (enableLeaseAux)
    ProducerTails        tails_;
    TxnCleanState        txn_;
    VerdictCleanState    verdict_;
};

} // namespace rune::storage
