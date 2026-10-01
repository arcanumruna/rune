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
#include <cstddef>
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
// Storage (F15 design note §2.2): ranges are kept in lastOffset order — the
// order ABORT markers are appended on a partition (single appender, L1), so an
// append is an O(1) push at the back; an out-of-order insert (tests, a
// cross-boundary replay) is placed by binary search (cold).
//
// isAborted(pid, off): binary search for the first range with lastOffset >= off,
// then a forward scan while lastOffset <= off + maxSpan_ (maxSpan_ = the widest
// range seen): any range covering `off` has lastOffset = firstOffset + span <=
// off + maxSpan_.  O(log R + k), k = ranges whose marker lies in
// [off, off + maxSpan_] — the aborted txns overlapping the batch.  It was O(R).
//
// pruneBelow(logStartOffset) drops ranges entirely below the log start (cold,
// retention).  reserve() pre-reserves; growth beyond it is cold and counted.
// Single-writer. No locks.
// ---------------------------------------------------------------------------
class AbortedTxnIndex {
public:
    /// Default per-partition reserved ranges (cold growth beyond, counted).
    static constexpr std::size_t kDefaultReservedRanges = 256;

    /// Pre-reserve room for `n` ranges (cold; never shrinks).
    void reserve(std::size_t n) { ranges_.reserve(n); }

    /// Record an aborted transaction.
    ///
    /// Called when an ABORT control marker is found in the log for `producerId`.
    /// `firstDataOffset` = baseOffset of the first DATA batch written by this
    /// producer in the aborted transaction (maintained by LsoTracker).
    /// `abortMarkerOffset` = baseOffset of the ABORT control batch.
    void onAbortMarker(ProducerId producerId,
                       int64_t    firstDataOffset,
                       int64_t    abortMarkerOffset);

    /// Returns true if the data batch with the given producerId and baseOffset
    /// falls within any recorded aborted transaction range.
    [[nodiscard]] bool isAborted(ProducerId producerId,
                                  int64_t    batchBaseOffset) const noexcept;

    /// Drop every range whose ABORT marker lies below `logStartOffset` (its
    /// data is gone with it).  Cold (retention).
    void pruneBelow(int64_t logStartOffset);

    /// All recorded aborted ranges (ordered by lastOffset — marker order).
    [[nodiscard]] const std::vector<AbortedTxnRange>& ranges() const noexcept {
        return ranges_;
    }

    /// Number of recorded aborted transactions.
    [[nodiscard]] std::size_t size() const noexcept { return ranges_.size(); }
    /// Reserved capacity (ranges).
    [[nodiscard]] std::size_t capacity() const noexcept { return ranges_.capacity(); }
    /// Cold growths beyond the reserved capacity.
    [[nodiscard]] uint64_t growths() const noexcept { return growths_; }

    /// Clear all ranges (capacity kept).
    void clear() noexcept { ranges_.clear(); maxSpan_ = 0; }

private:
    std::vector<AbortedTxnRange> ranges_;   ///< sorted by lastOffset ascending
    int64_t  maxSpan_ = 0;                  ///< max(lastOffset - firstOffset) over ranges_
    uint64_t growths_ = 0;
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
// ## Storage (F15 design note §2.2, N4)
//
// Two pre-reservable flat structures — no node allocation per insert:
//   open_ : open-addressing table  pid → FIFO position   (linear probing,
//           backward-shift deletion, load factor <= 1/2)
//   fifo_ : ring of {firstOffset, pid, live}, kept in firstOffset order.
//           Under the single-appender lemma (L1) first offsets arrive in
//           increasing order, so an insert is an O(1) push at the tail; an
//           out-of-order insert (tests, or a late registration) is placed by
//           shifting the tail — correct, O(k), counted in outOfOrderInserts().
//
//   onControlMarker : marks the FIFO entry dead (O(1)) and pops dead entries
//                     off the front (amortised O(1)), so the front is always
//                     the oldest LIVE transaction;
//   lso(hw)         : min(front.firstOffset, hw), or hw if nothing is open — O(1);
//   clear()         : resets BOTH structures in one place (N4: the two indexes
//                     can never diverge).
//
// Growth beyond the reserved capacity first compacts dead FIFO entries and
// only then doubles (cold, counted in growths()).  Steady state is
// allocation-free once reserve()d; the first insert lazily reserves
// kDefaultOpenTxnCapacity.
// ---------------------------------------------------------------------------
class LsoTracker {
public:
    /// Default per-partition open-transaction capacity (rcOpenTxnCapacity).
    static constexpr std::size_t kDefaultOpenTxnCapacity = 1024;

    LsoTracker() noexcept = default;
    explicit LsoTracker(std::size_t openTxnCapacity) { reserve(openTxnCapacity); }

    /// Pre-reserve room for `openTxnCapacity` open transactions (rounded up to
    /// a power of two).  Never shrinks.  Cold path.
    void reserve(std::size_t openTxnCapacity);

    /// Notify that a data batch was appended for this producer.
    ///
    /// If the producer is not yet open, this is the txn's first write:
    /// record firstOffset = batchBaseOffset.  If already open (e.g. multiple
    /// batches in one txn) keep the earlier firstOffset.
    ///
    /// \param producerId      The batch's producerId (-1 = non-transactional; ignored).
    /// \param batchBaseOffset The batch's base offset in the log.
    void onProduceBatch(ProducerId producerId, int64_t batchBaseOffset);

    /// Notify that a control marker (COMMIT or ABORT) was appended for this producer.
    ///
    /// Resolves the producer's open transaction; the LSO can advance after this call.
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
    [[nodiscard]] int64_t lso(int64_t hw) const noexcept;

    /// Number of currently open transactions.
    [[nodiscard]] std::size_t openCount() const noexcept { return live_; }

    /// True if no transactions are open.
    [[nodiscard]] bool empty() const noexcept { return live_ == 0; }

    /// First offset of the open transaction for `producerId`, or -1 if not open.
    [[nodiscard]] int64_t firstOffsetOf(ProducerId producerId) const noexcept;

    /// Forget every open transaction: BOTH the pid table and the first-offset
    /// FIFO (N4).  Capacity is kept.
    void clear() noexcept;

    /// Cold capacity doublings since construction (rcTrackerGrowths).
    [[nodiscard]] uint64_t growths() const noexcept { return growths_; }
    /// Inserts that arrived below the newest first offset (placed by shifting).
    [[nodiscard]] uint64_t outOfOrderInserts() const noexcept { return outOfOrderInserts_; }
    /// FIFO capacity (entries); 0 until the first reserve.
    [[nodiscard]] std::size_t capacity() const noexcept { return fifo_.size(); }

private:
    struct FifoEntry {
        int64_t    firstOffset = 0;
        ProducerId producerId  = -1;
        bool       live        = false;
    };
    struct Slot {
        ProducerId producerId = kEmptyPid;   ///< kEmptyPid = free
        uint64_t   pos        = 0;           ///< monotone FIFO position (index = pos & fifoMask_)
    };
    static constexpr ProducerId kEmptyPid = -1;   // pids < 0 are never tracked

    [[nodiscard]] std::size_t slotFor_(ProducerId pid) const noexcept;   // probe start
    [[nodiscard]] Slot*       find_(ProducerId pid) noexcept;
    [[nodiscard]] const Slot* find_(ProducerId pid) const noexcept;
    void eraseSlot_(Slot* s) noexcept;                 // backward-shift delete
    void insertSlot_(ProducerId pid, uint64_t pos) noexcept;
    void ensureRoom_();                                // cold: compact or grow
    void rebuild_(std::size_t newFifoCap);             // re-linearise live entries
    void popDeadFront_() noexcept;

    std::vector<FifoEntry> fifo_;    ///< ring; size() is the capacity (power of two)
    std::vector<Slot>      table_;   ///< open-addressing; size() == 2 * fifo_.size()
    std::size_t fifoMask_  = 0;
    std::size_t tableMask_ = 0;
    uint64_t    head_      = 0;      ///< FIFO front position (oldest entry)
    uint64_t    tail_      = 0;      ///< FIFO next-free position
    std::size_t live_      = 0;      ///< open transactions
    uint64_t    growths_   = 0;
    uint64_t    outOfOrderInserts_ = 0;
};

// ---------------------------------------------------------------------------
// RcBuilder — the ONE read_committed classification step (F15 §2.3, §2.7).
//
// Feeds ONE whole record batch into an LsoTracker + AbortedTxnIndex:
//   - a REAL control batch (CONTROL attribute bit AND a valid control-batch
//     CRC, N2) resolves its producer's open txn; an ABORT records the aborted
//     range [firstOffset, markerOffset];
//   - a data batch opens a txn ONLY if producerId >= 0 AND the TRANSACTIONAL
//     attribute (0x10) is set (N3 — an idempotent, non-transactional producer
//     never opens a transaction);
//   - anything else (pid -1 data, a corrupt CONTROL-flagged batch) is ignored.
//
// ReadCommittedView::rebuild is a loop of step() over Log::readView, and the
// owning worker's incremental build (S4) runs the same step(), so the unit
// tests exercise exactly the classification the worker runs.
//
// Zero allocation in steady state (tracker/index are pre-reserved); no virtual
// calls; non-transactional data batches cost one attribute-bit test.
// ---------------------------------------------------------------------------
class RcBuilder {
public:
    enum class Outcome : uint8_t {
        Invalid,        ///< not a whole batch (truncated / torn): nothing consumed
        Ignored,        ///< non-transactional data, or a corrupt CONTROL-flagged batch
        TxnData,        ///< transactional data batch (txn opened or already open)
        CommitMarker,   ///< COMMIT control batch applied
        AbortMarker,    ///< ABORT control batch applied
    };
    struct Result {
        Outcome outcome    = Outcome::Invalid;
        int32_t totalLength = 0;   ///< bytes of the batch (0 when Invalid)
        int64_t baseOffset  = 0;
    };

    RcBuilder(LsoTracker* tracker, AbortedTxnIndex* abortedIndex) noexcept
        : tracker_(tracker), abortedIndex_(abortedIndex) {}

    /// Classify and apply the batch at `batch` (`avail` bytes readable).
    Result step(const uint8_t* batch, std::size_t avail);

private:
    LsoTracker*      tracker_;
    AbortedTxnIndex* abortedIndex_;
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

    // Helper: extract producerId from a raw LE batch buffer at offset `pos`.
    // All-LE: both data batches and control batches use the unified
    // rune-protocol little-endian RecordBatch format.
    // producerId at kProducerIdOffset(43) is LE int64 in both cases.
    // Used for:
    //   (a) LsoTracker::onProduceBatch / onControlMarker
    //   (b) AbortedTxnIndex::isAborted
};

} // namespace rune::transaction
