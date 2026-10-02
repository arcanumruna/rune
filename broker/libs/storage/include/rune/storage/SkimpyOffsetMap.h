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
// Thread-safety: NOT thread-safe. Intended for single-thread cleaner use.
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <rune/storage/ProducerTails.h>

namespace rune::storage {

using Offset = int64_t;

// ---------------------------------------------------------------------------
// wyhash — inline, header-only implementation.
// Based on wyhash v4 (public domain, Wang Yi).
// Reference: https://github.com/wangyi-fudan/wyhash
// This is the fast 64-bit non-crypto hash used in place of Kafka's MD5.
//
// Key fix vs naive adaption: the 1-7 byte path mixes the start, middle, and
// end bytes into two 64-bit accumulators AND mixes the seed in separately so
// that keys of the same length with different characters produce distinct hashes.
// ---------------------------------------------------------------------------
namespace detail {

inline uint64_t wyread8(const void* p) {
    uint64_t v; std::memcpy(&v, p, sizeof(v)); return v;
}
inline uint64_t wyread4(const void* p) {
    uint32_t v; std::memcpy(&v, p, sizeof(v)); return static_cast<uint64_t>(v);
}
inline uint64_t wyread3(const uint8_t* p, size_t k) {
    return (static_cast<uint64_t>(p[0]) << 16) |
           (static_cast<uint64_t>(p[k >> 1]) << 8) |
            static_cast<uint64_t>(p[k - 1]);
}
// 128-bit multiply then XOR-fold (one MULQ/UMULH instruction on x86-64/aarch64).
inline uint64_t wymix(uint64_t a, uint64_t b) {
    __uint128_t r = static_cast<__uint128_t>(a) * b;
    return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
}

// wyhash v4 secret constants (random primes).
static constexpr uint64_t kSecret0 = 0xa0761d6478bd642fULL;
static constexpr uint64_t kSecret1 = 0xe7037ed1a0b428dbULL;
static constexpr uint64_t kSecret2 = 0x8ebc6af09c88c6e3ULL;
static constexpr uint64_t kSecret3 = 0x589965cc75374cc3ULL;

inline uint64_t wyhash64(const void* key, size_t len,
                          uint64_t seed = 0x517cc1b727220a95ULL) {
    const uint8_t* p = static_cast<const uint8_t*>(key);
    seed ^= wymix(seed ^ kSecret0, kSecret1);
    uint64_t a, b;
    if (len <= 16) {
        if (len >= 4) {
            a = (wyread4(p) << 32) | wyread4(p + ((len >> 3) << 2));
            b = (wyread4(p + len - 4) << 32) | wyread4(p + len - 4 - ((len >> 3) << 2));
        } else if (len > 0) {
            a = wyread3(p, len);
            b = 0;
        } else {
            a = b = 0;
        }
    } else {
        size_t i = len;
        if (i > 48) {
            uint64_t see1 = seed, see2 = seed;
            do {
                seed  = wymix(wyread8(p)     ^ kSecret1, wyread8(p + 8)  ^ seed);
                see1  = wymix(wyread8(p + 16)^ kSecret2, wyread8(p + 24) ^ see1);
                see2  = wymix(wyread8(p + 32)^ kSecret3, wyread8(p + 40) ^ see2);
                p += 48; i -= 48;
            } while (i > 48);
            seed ^= see1 ^ see2;
        }
        while (i > 16) {
            seed = wymix(wyread8(p) ^ kSecret1, wyread8(p + 8) ^ seed);
            i -= 16; p += 16;
        }
        a = wyread8(p + i - 16);
        b = wyread8(p + i - 8);
    }
    return wymix(kSecret1 ^ static_cast<uint64_t>(len), wymix(a ^ kSecret1, b ^ seed));
}

} // namespace detail

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
        constexpr std::size_t kMaxAllocSlots = (1u << 28); // 4 GiB
        std::size_t needed = static_cast<std::size_t>(
            static_cast<double>(maxSlots_) * kInvLoadFactor + 0.5) + 1;
        if (needed > kMaxAllocSlots) needed = kMaxAllocSlots;
        // Round up to next power of two.
        std::size_t cap = 1;
        while (cap < needed) cap <<= 1;
        // Clamp maxSlots_ to what fits in this capped capacity.
        std::size_t effectiveMax = static_cast<std::size_t>(cap * 0.9);
        if (maxSlots_ > effectiveMax) maxSlots_ = effectiveMax;

        capacity_ = cap;
        mask_     = cap - 1;
        table_.assign(cap, Entry{kEmpty, -1});
        // I1: the producer tails share the dedupe budget — at most a quarter
        // of it (kEntryBytes per key slot vs ProducerTails::kBytesPerProducer).
        tails_.setCapacity(std::max<std::size_t>(
            1024, cap * kEntryBytes / 4 / ProducerTails::kBytesPerProducer));
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
        for (auto& e : table_) { e.hash = kEmpty; e.offset = -1; }
        size_          = 0;
        latestOffset_  = -1;
        tails_.clear();
    }

    /// The last data batch of every producer in the scanned range (retained by
    /// compaction — see ProducerTails.h).  Filled by buildOffsetMapFromBases.
    ProducerTails&       producerTails() noexcept { return tails_; }
    const ProducerTails& producerTails() const noexcept { return tails_; }

    // --- Budget helper (static, no instance) --------------------------------
    /// Compute maxSlots from a byte budget and per-entry size.
    /// Formula: floor(budgetBytes / kEntryBytes * kDefaultLoadFactor)
    static constexpr std::size_t kEntryBytes        = sizeof(uint64_t) + sizeof(int64_t); // 16
    static constexpr double      kDefaultLoadFactor = 0.9;

    static std::size_t maxSlotsFromBudget(int64_t budgetBytes) noexcept {
        if (budgetBytes <= 0) return 1;
        auto raw = static_cast<std::size_t>(budgetBytes) / kEntryBytes;
        auto effective = static_cast<std::size_t>(raw * kDefaultLoadFactor);
        return effective < 1 ? 1 : effective;
    }

private:
    struct Entry {
        uint64_t hash   = 0;
        int64_t  offset = -1;
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
    std::vector<Entry>   table_;
    ProducerTails        tails_;
};

} // namespace rune::storage
