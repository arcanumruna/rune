#pragma once
// ---------------------------------------------------------------------------
// ReadCommittedView.h — LSO tracking + aborted-txn index + read_committed
//                        filtering for Kafka-faithful read-committed isolation.
//
// Namespace: rune::transaction
// C++23. NO locks. NO busy-spin. NO hot-path alloc (beyond scan buffers).
// Single-writer: all mutating methods must be called from one thread.
//
// ## Overview (P5-4, Kafka KIP-98)
//
// read_committed isolation = a consumer reads only up to the LSO (Last Stable
// Offset) and FILTERS OUT records belonging to ABORTED transactions.
//
//   LSO = baseOffset of the FIRST still-OPEN transaction (no COMMIT/ABORT yet).
//         If no transaction is open, LSO == HW.
//         LSO never exceeds HW and is monotone non-decreasing as txns resolve.
//
//   Aborted-txn index = a list of {producerId, firstOffset, lastOffset} ranges
//         that were ABORTED (built from ABORT control markers scanned from the
//         log's control batches).  read_committed skips any data batch whose
//         producerId matches an aborted range covering the batch's base offset.
//
// ## Usage
//
//   LsoTracker tracker;
//   AbortedTxnIndex abortedIndex;
//   ReadCommittedView view(&tracker, &abortedIndex);
//
//   // As batches are produced / control markers appended:
//   tracker.onProduceBatch(producerId, baseOffset);            // txn opens
//   tracker.onControlMarker(producerId, baseOffset, commit);   // txn resolves
//   abortedIndex.onAbortMarker(producerId, firstOffset, baseOffset); // ABORT
//
//   // Consumer read:
//   auto result = view.read(log, hw, fetchOffset, maxBytes, IsolationLevel::ReadCommitted);
//   // result contains only committed, non-aborted data batches with offset < LSO.
//
// ## Key invariants
//
//   1. LSO ≤ HW (always).
//   2. LSO is the minimum first-offset of any still-open transaction,
//      or HW if none is open.
//   3. Once a txn is resolved (COMMIT or ABORT marker seen), its effect on
//      LSO is lifted; LSO can advance (but never backwards).
//   4. read_committed returns data batches with baseOffset < LSO,
//      excluding batches belonging to aborted transactions.
//   5. Control batches are NEVER returned in either isolation mode.
//
// ## Circular dep avoidance
//
//   ReadCommittedView takes a raw `rune::storage::Log*` (forward-declared-level;
//   the include is in the .cpp).  `libs/transaction` already links `rune_storage`
//   transitively via `rune_replication`? No — see CMakeLists.  We link
//   `rune_storage` directly in the transaction CMakeLists (added for P5-4).
//   `libs/replication` does NOT depend on `libs/transaction` — zero new cycle.
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>

#include <cstdint>
#include <map>
#include <optional>
#include <vector>

// Forward declaration: avoid dragging all Log headers into consumers.
namespace rune::storage { class Log; }

namespace rune::transaction {

// ---------------------------------------------------------------------------
// IsolationLevel
// ---------------------------------------------------------------------------
enum class IsolationLevel : uint8_t {
    ReadUncommitted = 0,  ///< reads all batches with offset < HW (today's default)
    ReadCommitted   = 1,  ///< reads batches with offset < LSO, minus aborted
};

// ---------------------------------------------------------------------------
// AbortedTxnRange — a single aborted transaction's offset span.
// Mirrors Kafka AbortedTransaction {producerId, firstOffset}.
// ---------------------------------------------------------------------------
struct AbortedTxnRange {
    ProducerId producerId{-1};
    int64_t    firstOffset{0};  ///< baseOffset of the txn's first DATA batch
    int64_t    lastOffset{-1};  ///< baseOffset of the ABORT control batch

    /// Returns true if the given data batch's baseOffset falls within this range.
    [[nodiscard]] bool contains(int64_t batchBaseOffset) const noexcept {
        return batchBaseOffset >= firstOffset && batchBaseOffset <= lastOffset;
    }
};

// ---------------------------------------------------------------------------
// AbortedTxnIndex
//
// Accumulates AbortedTxnRange entries from ABORT control markers.
// Provides fast "is this data batch aborted?" lookup.
//
// Single-writer. No locks. O(ranges) per lookup — in practice the number of
// aborted ranges that span any given read window is tiny (bounded by
// max-open-txns at any time).
// ---------------------------------------------------------------------------
class AbortedTxnIndex {
public:
    /// Record an aborted transaction.
    ///
    /// Called when an ABORT control marker is found in the log for `producerId`.
    /// `firstDataOffset` = baseOffset of the first DATA batch written by this
    /// producer in the aborted transaction (maintained by LsoTracker).
    /// `abortMarkerOffset` = baseOffset of the ABORT control batch.
    void onAbortMarker(ProducerId producerId,
                       int64_t    firstDataOffset,
                       int64_t    abortMarkerOffset) noexcept;

    /// Returns true if the data batch with the given producerId and baseOffset
    /// falls within any recorded aborted transaction range.
    [[nodiscard]] bool isAborted(ProducerId producerId,
                                  int64_t    batchBaseOffset) const noexcept;

    /// All recorded aborted ranges (ordered by firstOffset ascending).
    [[nodiscard]] const std::vector<AbortedTxnRange>& ranges() const noexcept {
        return ranges_;
    }

    /// Number of recorded aborted transactions.
    [[nodiscard]] std::size_t size() const noexcept { return ranges_.size(); }

    /// Clear all ranges (e.g. after log truncation in tests).
    void clear() noexcept { ranges_.clear(); }

private:
    // Sorted by firstOffset ascending for efficient range queries.
    std::vector<AbortedTxnRange> ranges_;
};

// ---------------------------------------------------------------------------
// LsoTracker
//
// Tracks the LSO (Last Stable Offset) for a single partition.
//
// The LSO is the baseOffset of the FIRST still-OPEN transaction, or HW if
// none is open.  Open transactions are those for which a DATA batch has been
// seen (via onProduceBatch) but no COMMIT/ABORT marker has yet arrived.
//
// Thread model: single-writer.  All methods called from the owning thread.
//
// ## Design note
//
// We maintain a map of open transactions:
//   openTxns_ : producerId → firstOffset (the smallest offset this producer
//                            has written in the current open transaction)
//
// Multiple producers can interleave data batches; a txn "begins" (for LSO
// purposes) at the first data batch for that producerId (since addPartitions
// does not carry an offset — the first actual data write establishes the txn's
// position).
//
// LSO = min(firstOffset) over all openTxns_, or HW if empty.
// ---------------------------------------------------------------------------
class LsoTracker {
public:
    /// Notify that a data batch was appended for this producer.
    ///
    /// If the producer is not yet in openTxns_, this is the txn's first write:
    /// record firstOffset = batchBaseOffset.  If already tracked (e.g. multiple
    /// batches in one txn) keep the earlier firstOffset.
    ///
    /// \param producerId      The batch's producerId (-1 = non-transactional; ignored).
    /// \param batchBaseOffset The batch's base offset in the log.
    void onProduceBatch(ProducerId producerId, int64_t batchBaseOffset) noexcept;

    /// Notify that a control marker (COMMIT or ABORT) was appended for this producer.
    ///
    /// Removes the producer from openTxns_ (the txn is now resolved).
    /// The LSO can advance after this call.
    ///
    /// \param producerId         The producer whose transaction is resolving.
    /// \param markerBaseOffset   The control batch's base offset (for AbortedTxnIndex use).
    /// \param commit             true=COMMIT, false=ABORT.
    ///
    /// \returns firstOffset of the now-resolved transaction (needed by
    ///          AbortedTxnIndex::onAbortMarker if commit==false), or -1 if
    ///          the producerId was not tracked (non-transactional or duplicate).
    int64_t onControlMarker(ProducerId producerId,
                             int64_t    markerBaseOffset,
                             bool       commit) noexcept;

    /// Compute the current LSO given the current high watermark.
    ///
    /// LSO = min(firstOffset over all open txns), or hw if none open.
    /// LSO is clamped to [0, hw].
    ///
    /// \param hw  The partition's current high-watermark.
    [[nodiscard]] int64_t lso(int64_t hw) const noexcept;

    /// Number of currently open transactions.
    [[nodiscard]] std::size_t openCount() const noexcept { return openTxns_.size(); }

    /// True if no transactions are open.
    [[nodiscard]] bool empty() const noexcept { return openTxns_.empty(); }

    /// First offset of the open transaction for `producerId`, or -1 if not open.
    [[nodiscard]] int64_t firstOffsetOf(ProducerId producerId) const noexcept;

    /// Clear all tracked open transactions (e.g. after a log truncation in tests).
    void clear() noexcept { openTxns_.clear(); }

private:
    // producerId → firstOffset of the current open transaction.
    // std::map gives us O(log n) lookup and automatic min via begin().
    // In practice n is tiny (max concurrent producers).
    // Key: producerId (int64_t), Value: firstOffset (int64_t).
    //
    // NOTE: We can't trivially use the map-key for min(firstOffset) because
    // two producers with different PIDs may have different firstOffsets and the
    // min is over VALUES not keys.  We maintain a separate sorted structure:
    // a std::map<firstOffset, producerId> for O(log n) min lookup.
    std::map<ProducerId, int64_t> openTxns_;      ///< pid → firstOffset
    std::map<int64_t, ProducerId> byFirstOffset_;  ///< firstOffset → pid (for min)
};

// ---------------------------------------------------------------------------
// ReadCommittedView
//
// Combines LsoTracker + AbortedTxnIndex to provide a consumer-facing read
// that implements read_committed or read_uncommitted isolation.
//
// The view does NOT own the tracker or index — callers may update them
// independently (e.g. as data and control batches are appended) and then
// call read() to serve a consumer.
//
// ## rebuild()
//
// For convenience, rebuild(log, hw) scans the log from offset 0 to hw,
// populates LsoTracker and AbortedTxnIndex from scratch, and returns the LSO.
// This is used in tests (and optionally on startup after recovery) when the
// tracker state is not maintained incrementally.
//
// ## read()
//
//   read_uncommitted: returns all data batches with baseOffset < hw,
//                     EXCLUDING control batches.
//   read_committed:   returns data batches with baseOffset < lso(hw),
//                     EXCLUDING aborted batches and control batches.
//
// maxBytes is a soft cap on the returned byte count (we stop adding batches
// once the total exceeds maxBytes, but always return at least one batch if
// any are available).
// ---------------------------------------------------------------------------
class ReadCommittedView {
public:
    /// \param tracker      Non-owning pointer; must outlive this view.
    /// \param abortedIndex Non-owning pointer; must outlive this view.
    explicit ReadCommittedView(LsoTracker*      tracker,
                               AbortedTxnIndex* abortedIndex) noexcept
        : tracker_(tracker), abortedIndex_(abortedIndex) {}

    /// Rebuild tracker + abortedIndex by scanning the log from 0 to hw.
    ///
    /// Clears both the tracker and the index before scanning.
    /// After this call, tracker_->lso(hw) and abortedIndex_->ranges() reflect
    /// the full log state.
    ///
    /// \param log   The partition log to scan.
    /// \param hw    High-watermark: scan up to (not including) offset `hw`.
    ///              Control batches ARE included in the scan (they influence LSO).
    void rebuild(rune::storage::Log* log, int64_t hw);

    /// Read batches from the log under the given isolation level.
    ///
    /// \param log          Partition log (non-owning).
    /// \param hw           High-watermark (batches with lastOffset >= hw excluded).
    /// \param fetchOffset  First offset to read.
    /// \param maxBytes     Soft cap on returned bytes.
    /// \param level        ReadUncommitted or ReadCommitted.
    ///
    /// \returns Concatenated batch bytes for all matching batches.
    ///          Control batches are stripped from the output in both modes.
    [[nodiscard]] std::vector<uint8_t> read(rune::storage::Log* log,
                                             int64_t             hw,
                                             int64_t             fetchOffset,
                                             int32_t             maxBytes,
                                             IsolationLevel      level) const;

    /// Current LSO given hw (delegates to tracker_).
    [[nodiscard]] int64_t lso(int64_t hw) const noexcept {
        return tracker_->lso(hw);
    }

private:
    LsoTracker*      tracker_;
    AbortedTxnIndex* abortedIndex_;

    // Helper: extract producerId from a raw batch buffer at offset `pos`.
    // The batch is in rune-protocol big-endian format OR ControlMarker
    // little-endian format.  We detect via the CONTROL bit in attributes.
    // For the ReadCommittedView scan we need producerId for:
    //   (a) LsoTracker::onProduceBatch / onControlMarker
    //   (b) AbortedTxnIndex::isAborted
    // The producerId field lives at the same logical position in both formats
    // (batch header offset 43..50) but the endianness differs:
    //   rune-protocol (BE): producerId at kProducerIdOffset(43) in big-endian
    //   ControlMarker  (LE): producerId at [43..50] in little-endian
    // We unify by checking the CONTROL bit first (ControlMarker batch) and
    // using ControlMarker::extract(), then falling back to rune-protocol BE
    // reading for data batches.
};

} // namespace rune::transaction
