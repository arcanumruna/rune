#pragma once
// ---------------------------------------------------------------------------
// ProducerStateManager.h — Kafka-style idempotent producer dedup.
//
// Mirrors Kafka's ProducerStateManager / ProducerStateEntry (KIP-98).
//
// Per (producerId, partition) the manager tracks:
//   • producerEpoch  — current epoch (a higher epoch resets sequence tracking;
//                      a lower epoch is fenced)
//   • lastSequence   — the last sequence included in the last appended batch
//                      (= baseSequence + recordCount - 1 of the last accepted
//                      batch)
//   • retryWindow    — a small circular buffer of the last N batch descriptors
//                      used to detect and answer DUPLICATE retries (returns
//                      the original base offset, does NOT re-append).
//
// ValidationResult:
//   Accept            — new in-order batch, caller should append then call
//                       update().
//   Duplicate         — this (epoch, baseSequence..lastSequence) matches a
//                       cached batch; retryBaseOffset carries the original
//                       log base offset.  Do NOT re-append.
//   OutOfOrderSequence— gap or stale sequence that is not a known duplicate.
//   InvalidProducerEpoch — producerEpoch < tracked epoch (zombie fenced).
//
// Epoch semantics (mirror Kafka):
//   epoch == tracked   → validate sequence normally.
//   epoch  > tracked   → new producer session; reset lastSequence to -1 so
//                        the next expected baseSequence is 0.
//   epoch  < tracked   → reject (InvalidProducerEpoch).
//
// Persistence:
//   takeSnapshot(path) — writes all current state to a flat binary file and
//                        fsyncs.  recover(path) reads it back. Crash-safe:
//                        torn-tail detected by magic + CRC32C on each entry.
//
// Single-writer, no locks, no hot-path allocation.
// Namespace: rune::storage   C++23
// ---------------------------------------------------------------------------

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

// Forward-declare Log so the header does not pull in Log.h (and transitively
// the whole segment/filesystem machinery) everywhere ProducerStateManager.h
// is included.  The out-of-line rebuildFromLog() implementation in
// ProducerStateManager.cpp includes the full Log.h.
namespace rune::storage { class Log; }

namespace rune::storage {

// ---------------------------------------------------------------------------
// Number of recent batches kept per producer-partition for retry dedup.
// Mirrors Kafka's ProducerStateManager.MaxProducerIdExpirationMs / NumBatches.
// ---------------------------------------------------------------------------
inline constexpr int kRetryWindowSize = 5;

// ---------------------------------------------------------------------------
// BatchDescriptor — one entry in the per-producer retry window.
// ---------------------------------------------------------------------------
struct BatchDescriptor {
    int32_t baseSequence{-1};   // first seq in batch (-1 == empty slot)
    int32_t lastSequence{-1};   // last  seq in batch
    int64_t baseOffset{-1};     // log offset where the batch was appended
};

// ---------------------------------------------------------------------------
// ProducerStateEntry — per (producerId, partition) tracking state.
// ---------------------------------------------------------------------------
struct ProducerStateEntry {
    int16_t producerEpoch{-1};   // -1 = never seen
    int32_t lastSequence{-1};    // last seq appended (-1 = no batch yet)

    // Circular retry window (kRetryWindowSize slots).
    std::array<BatchDescriptor, kRetryWindowSize> window{};
    int     windowHead{0};       // next slot to write (oldest when full)
    int     windowCount{0};      // slots filled so far (capped at kRetryWindowSize)
};

// ---------------------------------------------------------------------------
// ValidationResult
// ---------------------------------------------------------------------------
struct ValidationResult {
    enum class Status : uint8_t {
        Accept,
        Duplicate,
        OutOfOrderSequence,
        InvalidProducerEpoch,
    };

    Status  status{Status::Accept};
    int64_t retryBaseOffset{-1};  // valid when status == Duplicate

    static ValidationResult accept()  { return {Status::Accept,  -1}; }
    static ValidationResult dup(int64_t off) { return {Status::Duplicate, off}; }
    static ValidationResult oos()     { return {Status::OutOfOrderSequence,  -1}; }
    static ValidationResult fenced()  { return {Status::InvalidProducerEpoch, -1}; }

    bool isAccept()   const { return status == Status::Accept; }
    bool isDuplicate() const { return status == Status::Duplicate; }
    bool isOos()      const { return status == Status::OutOfOrderSequence; }
    bool isFenced()   const { return status == Status::InvalidProducerEpoch; }
};

// ---------------------------------------------------------------------------
// Key: (producerId, partition)
// ---------------------------------------------------------------------------
struct ProducerPartitionKey {
    int64_t producerId;
    int32_t partition;

    bool operator==(const ProducerPartitionKey& o) const noexcept {
        return producerId == o.producerId && partition == o.partition;
    }
};

} // namespace rune::storage

// Hash specialisation in std namespace
namespace std {
template<>
struct hash<rune::storage::ProducerPartitionKey> {
    std::size_t operator()(const rune::storage::ProducerPartitionKey& k) const noexcept {
        // Combine the two halves with a mix that mirrors Kafka's hashCode idiom.
        std::size_t h = static_cast<std::size_t>(k.producerId);
        h ^= (h >> 30);
        h *= 0xbf58476d1ce4e5b9ULL;
        h ^= (h >> 27);
        h *= 0x94d049bb133111ebULL;
        h ^= (h >> 31);
        h ^= static_cast<std::size_t>(k.partition) * 0x9e3779b97f4a7c15ULL;
        return h;
    }
};
} // namespace std

namespace rune::storage {

// ---------------------------------------------------------------------------
// ProducerStateManager
// ---------------------------------------------------------------------------
class ProducerStateManager {
public:
    ProducerStateManager() = default;
    ~ProducerStateManager() = default;

    ProducerStateManager(const ProducerStateManager&)            = delete;
    ProducerStateManager& operator=(const ProducerStateManager&) = delete;
    ProducerStateManager(ProducerStateManager&&)                 = default;
    ProducerStateManager& operator=(ProducerStateManager&&)      = default;

    // -----------------------------------------------------------------------
    // validate — hot path, no allocation, no locks.
    //
    // @param producerId       — 64-bit producer id (allocated by coordinator)
    // @param partition        — partition number
    // @param producerEpoch    — epoch of this producer session (int16)
    // @param baseSequence     — first sequence number in this batch
    // @param lastSequenceInBatch — baseSequence + recordCount - 1
    //
    // Returns a ValidationResult; the caller must call update() after a
    // successful append if and only if the result is Accept.
    // -----------------------------------------------------------------------
    [[nodiscard]]
    ValidationResult validate(int64_t producerId,
                              int32_t partition,
                              int16_t producerEpoch,
                              int32_t baseSequence,
                              int32_t lastSequenceInBatch) const noexcept;

    // -----------------------------------------------------------------------
    // update — called AFTER a successful append.
    //
    // Advances lastSequence and records the batch in the retry window.
    // -----------------------------------------------------------------------
    void update(int64_t producerId,
                int32_t partition,
                int16_t producerEpoch,
                int32_t baseSequence,
                int32_t lastSequence,
                int64_t baseOffset);

    // -----------------------------------------------------------------------
    // Snapshot + recover (crash-safe, fsync'd).
    // -----------------------------------------------------------------------
    void takeSnapshot(const std::string& path) const;

    /// Recover state from a previous snapshot.  Returns the number of entries
    /// loaded.  Any torn tail is silently skipped.
    int  recover(const std::string& path);

    // -----------------------------------------------------------------------
    // rebuildFromLog — reconstruct producer state by scanning a partition Log.
    //
    // Scans all RecordBatches in [0, highWatermark) and calls update() for
    // every batch whose producerId >= 0 (idempotent producer).  Clears the
    // existing map first so the rebuild is idempotent.
    //
    // Each batch contributes:
    //   producerId    from header (LE int64 at kProducerIdOffset=43)
    //   producerEpoch from header (LE int16 at kProducerEpochOffset=51)
    //   baseSequence  from header (LE int32 at kBaseSequenceOffset=53)
    //   lastSequence  = baseSequence + lastOffsetDelta (one record per batch→delta=0)
    //   baseOffset    from header (LE int64 at kBaseOffsetOffset=0)
    //
    // The scan is last-write-wins per (pid, partition): each update() call
    // overwrites the retry window slot (oldest evicted when full, matching
    // the in-flight sequence), so scanning forward from offset 0 naturally
    // reconstructs the correct window state as of the log end.
    //
    // @param log            The partition Log to scan (non-const: Log::read is
    //                       non-const in the current implementation).
    // @param highWatermark  Scan up to (exclusive) this log offset.
    //                       MUST be the committed high-watermark (HW), NOT the
    //                       log-end-offset (LEO).  Scanning beyond HW seeds the
    //                       PSM dedup window from uncommitted batches that may be
    //                       truncated by KIP-101 divergence resolution, causing a
    //                       false-Duplicate for a genuinely new producer retry →
    //                       acknowledged data loss at a truncated-away offset.
    //                       Pass log.logEndOffset() ONLY when HW == LEO (e.g.
    //                       single-node passthrough with no uncommitted tail).
    // @param partition      The partition id to use as the key (producer state
    //                       is keyed by (producerId, partition)).
    //
    // Cold path — allocations are acceptable here.
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void rebuildFromLog(Log& log, int64_t highWatermark, int32_t partition);

    // -----------------------------------------------------------------------
    // Accessors (for tests / Phase-5 coordinator)
    // -----------------------------------------------------------------------
    const ProducerStateEntry* entry(int64_t producerId, int32_t partition) const noexcept;
    std::size_t size() const noexcept { return map_.size(); }

private:
    // Look up or create an entry.  Called from update() only (not validate).
    ProducerStateEntry& getOrCreate(int64_t producerId, int32_t partition);

    std::unordered_map<ProducerPartitionKey, ProducerStateEntry> map_;
};

} // namespace rune::storage
