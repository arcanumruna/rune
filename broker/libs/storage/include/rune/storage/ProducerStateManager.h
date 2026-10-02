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
//                       log base offset.  Do NOT re-append.  OR (known-old,
//                       Kafka DUPLICATE_SEQUENCE_NUMBER parity) a same-epoch
//                       batch entirely <= lastSequence that left the window:
//                       retryBaseOffset == -1 (offset unknown).  Either way
//                       coveredLastOffset is the log offset the answer's ack
//                       condition must cover.
//   OutOfOrderSequence— forward gap, or a batch partially overlapping
//                       lastSequence (base <= lastSequence < end).
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

#include <rune/protocol/client/Protocol.h>

// De-virtualized (Milestone D Phase 2): the former ILog seam is gone.
// rebuildFromLog() is now a TEMPLATE over the concrete log type (Log /
// InMemoryLog) so it never dispatches virtually.  The out-of-line template
// definition + explicit instantiations live in ProducerStateManager.cpp, which
// includes the full Log.h + InMemoryLog.h; callers only need the concrete type.

namespace rune::storage {

// ---------------------------------------------------------------------------
// Number of recent batches kept per producer-partition for retry dedup.
//
// EOS-OOS (2026-10-02): this MUST equal the client's idempotent in-flight bound
// (rune-protocol kIdempotentMaxInFlightBatches, 32): a retransmit of any batch
// the client may still have in flight is then answered as a duplicate with its
// original offset.  (Kafka keeps 5 = its max.in.flight cap under idempotence.)
// A same-epoch batch older than the window but entirely <= lastSequence is a
// KNOWN-OLD duplicate (Duplicate with retryBaseOffset -1), never OOS.
// ---------------------------------------------------------------------------
inline constexpr int kRetryWindowSize = rune::protocol::client::kIdempotentMaxInFlightBatches;
static_assert(kRetryWindowSize > 0 && kRetryWindowSize <= 255,
              "windowHead / windowCount are uint8_t");

// ---------------------------------------------------------------------------
// BatchDescriptor — one batch of the per-producer retry window (a VALUE view;
// the entry stores the window as parallel arrays, see ProducerStateEntry).
// ---------------------------------------------------------------------------
struct BatchDescriptor {
    int32_t baseSequence{-1};   // first seq in batch (-1 == empty slot)
    int32_t lastSequence{-1};   // last  seq in batch
    int64_t baseOffset{-1};     // log offset where the batch was appended
};

// ---------------------------------------------------------------------------
// ProducerStateEntry — per (producerId, partition) tracking state.
//
// Layout (cache-friendly, 8 + 32*8 + 32*8 = 520 bytes): a compact 8-byte header,
// then the window as parallel arrays.  The dedup scan (only for a retransmit,
// baseSequence <= lastSequence) reads just `seqs` (256 B, 4 cache lines,
// vectorisable); `baseOffsets` is touched only for the matching slot.  The
// in-order hot path (baseSequence == lastSequence + 1) touches the header only.
// Slots [0, windowCount) are filled; when full, windowHead is the oldest slot.
// ---------------------------------------------------------------------------
struct ProducerStateEntry {
    struct SeqRange {
        int32_t base{-1};        // first seq in batch (-1 == empty slot)
        int32_t last{-1};        // last  seq in batch
    };

    int16_t producerEpoch{-1};   // -1 = never seen
    uint8_t windowHead{0};       // next slot to write (oldest when full)
    uint8_t windowCount{0};      // slots filled so far (capped at kRetryWindowSize)
    int32_t lastSequence{-1};    // last seq appended (-1 = no batch yet)

    std::array<SeqRange, kRetryWindowSize> seqs{};
    std::array<int64_t,  kRetryWindowSize> baseOffsets{};

    [[nodiscard]] BatchDescriptor slot(int i) const noexcept {
        return BatchDescriptor{seqs[static_cast<std::size_t>(i)].base,
                               seqs[static_cast<std::size_t>(i)].last,
                               baseOffsets[static_cast<std::size_t>(i)]};
    }
    /// Slot of the newest batch (the one holding lastSequence); -1 when empty.
    [[nodiscard]] int newestSlot() const noexcept {
        return windowCount == 0 ? -1 : (windowHead + kRetryWindowSize - 1) % kRetryWindowSize;
    }
    void resetWindow() noexcept {
        windowHead = 0;
        windowCount = 0;
        seqs        = {};
        baseOffsets = {};
    }
    void push(int32_t baseSeq, int32_t lastSeq, int64_t baseOffset) noexcept {
        seqs[windowHead]        = SeqRange{baseSeq, lastSeq};
        baseOffsets[windowHead] = baseOffset;
        windowHead = static_cast<uint8_t>((windowHead + 1) % kRetryWindowSize);
        if (windowCount < kRetryWindowSize) ++windowCount;
    }
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
    // Duplicate only: the last log offset the duplicate's answer waits on (its
    // ack condition — HW / fsync — must cover it).  -1 = unknown.
    int64_t coveredLastOffset{-1};

    static ValidationResult accept()  { return {Status::Accept,  -1, -1}; }
    static ValidationResult dup(int64_t off, int64_t coveredLast = -1) {
        return {Status::Duplicate, off, coveredLast};
    }
    static ValidationResult oos()     { return {Status::OutOfOrderSequence,  -1, -1}; }
    static ValidationResult fenced()  { return {Status::InvalidProducerEpoch, -1, -1}; }

    bool isAccept()   const { return status == Status::Accept; }
    bool isDuplicate() const { return status == Status::Duplicate; }
    bool isOos()      const { return status == Status::OutOfOrderSequence; }
    bool isFenced()   const { return status == Status::InvalidProducerEpoch; }
};

// ---------------------------------------------------------------------------
// Key: (topicId, producerId, partition)
//
// TX-2c: topicId is included so that the same producerId producing to
// topicA:partition0 and topicB:partition0 maintains INDEPENDENT dedup state.
// Without topicId the map_ keyed only on (producerId, partition) caused
// cross-topic false OutOfOrderSequence / Duplicate for any producer that
// writes to more than one topic.
// ---------------------------------------------------------------------------
struct ProducerPartitionKey {
    int32_t topicId;
    int64_t producerId;
    int32_t partition;

    bool operator==(const ProducerPartitionKey& o) const noexcept {
        return topicId == o.topicId &&
               producerId == o.producerId &&
               partition == o.partition;
    }
};

} // namespace rune::storage

// Hash specialisation in std namespace
namespace std {
template<>
struct hash<rune::storage::ProducerPartitionKey> {
    std::size_t operator()(const rune::storage::ProducerPartitionKey& k) const noexcept {
        // Combine the three fields with a mix that mirrors Kafka's hashCode idiom.
        std::size_t h = static_cast<std::size_t>(k.producerId);
        h ^= (h >> 30);
        h *= 0xbf58476d1ce4e5b9ULL;
        h ^= (h >> 27);
        h *= 0x94d049bb133111ebULL;
        h ^= (h >> 31);
        h ^= static_cast<std::size_t>(k.partition) * 0x9e3779b97f4a7c15ULL;
        // TX-2c: mix in topicId so different topics hash differently.
        h ^= static_cast<std::size_t>(static_cast<uint32_t>(k.topicId)) * 0x517cc1b727220a95ULL;
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
    // @param topicId          — topic id (TX-2c: required for isolation across topics)
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
    ValidationResult validate(int32_t topicId,
                              int64_t producerId,
                              int32_t partition,
                              int16_t producerEpoch,
                              int32_t baseSequence,
                              int32_t lastSequenceInBatch) const noexcept;

    // -----------------------------------------------------------------------
    // update — called AFTER a successful append.
    //
    // Advances lastSequence and records the batch in the retry window.
    // F15 S5: a batch of an epoch OLDER than the tracked one is ignored (it can
    // only come from a log replay that already applied a later control-batch
    // fence — the live path never updates with a fenced epoch).
    // -----------------------------------------------------------------------
    void update(int32_t topicId,
                int64_t producerId,
                int32_t partition,
                int16_t producerEpoch,
                int32_t baseSequence,
                int32_t lastSequence,
                int64_t baseOffset);

    // -----------------------------------------------------------------------
    // fenceEpoch — F15 S5 (C1+, I3): a COMMIT/ABORT control batch of `producerId`
    // at `producerEpoch` raises the partition producer epoch (raise only, no
    // sequence: a higher epoch resets the sequence window, an equal or lower
    // one changes nothing).  A later batch of an older epoch is then fenced
    // (InvalidProducerEpoch).  Applied on receipt of the marker op and, as the
    // durable form, for every control batch the producer-state rebuild sees.
    // Cold (a marker per transaction per partition): may allocate the entry.
    // -----------------------------------------------------------------------
    void fenceEpoch(int32_t topicId, int64_t producerId, int32_t partition, int16_t producerEpoch);

    // -----------------------------------------------------------------------
    // clearPartition — F15 S5 (I3): drop every entry of (topicId, partition)
    // before its producer state is rebuilt from the log (the owning worker's
    // RC build).  Cold: O(entries).
    // -----------------------------------------------------------------------
    void clearPartition(int32_t topicId, int32_t partition);

    // -----------------------------------------------------------------------
    // Snapshot + recover (crash-safe, fsync'd).
    //
    // Format version 3 records the window size it was written with (file header
    // [12..15]); recover() accepts any recorded size and keeps the newest
    // min(recorded, kRetryWindowSize) batches.  Version 2 (implicit 5-batch
    // window, pre EOS-OOS) is still read.  Byte layout: ProducerStateManager.cpp.
    // -----------------------------------------------------------------------
    static constexpr uint32_t kSnapshotFileHeaderBytes = 16u;
    static constexpr uint32_t kSnapshotEntryBytes =
        32u + static_cast<uint32_t>(kRetryWindowSize) * 16u + 4u;   // fixed + slots + CRC32C

    void takeSnapshot(const std::string& path) const;

    /// Recover state from a previous snapshot.  Returns the number of entries
    /// loaded.  Any torn tail is silently skipped.
    int  recover(const std::string& path);

    // -----------------------------------------------------------------------
    // rebuildFromLog — reconstruct producer state by scanning a partition Log.
    //
    // Scans all RecordBatches in [0, highWatermark) and calls update() for
    // every batch whose producerId >= 0 (idempotent producer).  Clears existing
    // (topicId, partition) entries first (via erase_if, NOT the whole map) so the
    // rebuild is idempotent for this partition while accumulating other partitions'
    // state into a shared PSM across successive calls (TX-2c multi-partition fix).
    //
    // Each batch contributes:
    //   producerId    from header (LE int64 at kProducerIdOffset=43)
    //   producerEpoch from header (LE int16 at kProducerEpochOffset=51)
    //   baseSequence  from header (LE int32 at kBaseSequenceOffset=53)
    //   lastSequence  = baseSequence + lastOffsetDelta (one record per batch→delta=0)
    //   baseOffset    from header (LE int64 at kBaseOffsetOffset=0)
    //
    // The scan is last-write-wins per (topicId, pid, partition): each update() call
    // overwrites the retry window slot (oldest evicted when full, matching
    // the in-flight sequence), so scanning forward from offset 0 naturally
    // reconstructs the correct window state as of the log end.
    //
    // @param topicId        The topic id to use as part of the key (TX-2c: ensures
    //                       cross-topic isolation for the same producerId+partition).
    // @param log            The partition log to scan — a concrete rune::storage::Log
    //                       or InMemoryLog (template LogT; non-const: read() is
    //                       non-const in the current implementation).  NO virtual
    //                       dispatch — the concrete read()/segmentBaseOffsets() are
    //                       called directly (de-virtualized seam, Milestone D P2).
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
    //                       is keyed by (topicId, producerId, partition)).
    //
    // Sets rebuildHw_ = highWatermark on every call so callers can test whether
    // a subsequent truncation falls below the last rebuild bound.
    //
    // Called from ClientGateway::rebuildProducerStateFromLogs (production) and
    // from PartitionReplica::truncateLog when a divergence truncation lands below
    // the rebuild HW (Task 11 atomicity fix).
    //
    // Cold path — allocations are acceptable here.
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    template<class LogT>
    void rebuildFromLog(int32_t topicId, LogT& log, int64_t highWatermark, int32_t partition);

    // -----------------------------------------------------------------------
    // rebuildHighWatermark — the log high-watermark at which the last rebuild ran.
    //
    // Tracks rebuildHw_, which is set to the highWatermark argument on every
    // rebuildFromLog call.  Returns -1 if rebuildFromLog has never been called.
    //
    // Used by PartitionReplica::truncateLog to decide whether a divergence
    // truncation (KIP-101) falls below the PSM rebuild bound: if
    //   truncateOffset <= rebuildHighWatermark()
    // the dedup window may contain state for offsets that no longer exist, so
    // truncateLog re-rebuilds the PSM atomically with the log truncation.
    // -----------------------------------------------------------------------
    [[nodiscard]] int64_t rebuildHighWatermark() const noexcept { return rebuildHw_; }

    // -----------------------------------------------------------------------
    // Accessors (for tests / Phase-5 coordinator)
    // -----------------------------------------------------------------------
    const ProducerStateEntry* entry(int32_t topicId, int64_t producerId, int32_t partition) const noexcept;
    std::size_t size() const noexcept { return map_.size(); }

private:
    // Look up or create an entry.  Called from update() only (not validate).
    ProducerStateEntry& getOrCreate(int32_t topicId, int64_t producerId, int32_t partition);

    std::unordered_map<ProducerPartitionKey, ProducerStateEntry> map_;

    // Task 11: the log high-watermark at which the last rebuildFromLog ran.
    // -1 = never rebuilt.  Used by PartitionReplica::truncateLog to decide
    // whether a KIP-101 divergence truncation must trigger a re-rebuild.
    int64_t rebuildHw_ = -1;
};

} // namespace rune::storage
