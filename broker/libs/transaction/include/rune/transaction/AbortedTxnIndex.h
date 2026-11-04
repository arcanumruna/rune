#pragma once
// ---------------------------------------------------------------------------
// AbortedTxnIndex.h — the per-partition aborted-transaction index.
//
// Header-only (#591): the view fold (rune_view, which links no rune_transaction) holds a pointer to the concrete
// index and calls isAborted() inline per transactional batch — no indirect call on the fold hot path.
// ReadCommittedView.h includes this header; the semantics are unchanged.
//
// Namespace: rune::transaction.  Single-writer.  No locks.
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace rune::transaction {

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
// [off, off + maxSpan_].  Review m7: maxSpan_ is the GLOBAL widest range, so
// one long-running aborted txn widens every lookup's window (degrading toward
// O(R)); always correct, never worse than the former O(R) scan.
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
                       int64_t    abortMarkerOffset)
    {
        const AbortedTxnRange range{producerId, firstDataOffset, abortMarkerOffset};
        if (ranges_.size() == ranges_.capacity()) [[unlikely]] ++growths_;   // cold growth (counted)
        const int64_t span = abortMarkerOffset - firstDataOffset;
        if (span > maxSpan_) maxSpan_ = span;
        if (ranges_.empty() || ranges_.back().lastOffset <= abortMarkerOffset) [[likely]] {
            ranges_.push_back(range);                      // marker order (L1): O(1)
            return;
        }
        // Out of order (cold): keep lastOffset order.
        auto it = std::upper_bound(ranges_.begin(), ranges_.end(), abortMarkerOffset,
                                   [](int64_t off, const AbortedTxnRange& r) { return off < r.lastOffset; });
        ranges_.insert(it, range);
    }

    /// Returns true if the data batch with the given producerId and baseOffset
    /// falls within any recorded aborted transaction range.
    [[nodiscard]] bool isAborted(ProducerId producerId,
                                  int64_t    batchBaseOffset) const noexcept
    {
        if (ranges_.empty()) return false;
        auto it = std::lower_bound(ranges_.begin(), ranges_.end(), batchBaseOffset,
                                   [](const AbortedTxnRange& r, int64_t off) { return r.lastOffset < off; });
        const int64_t scanEnd = batchBaseOffset > std::numeric_limits<int64_t>::max() - maxSpan_
                                    ? std::numeric_limits<int64_t>::max() : batchBaseOffset + maxSpan_;
        for (; it != ranges_.end() && it->lastOffset <= scanEnd; ++it) {
            if (it->producerId == producerId && it->contains(batchBaseOffset)) return true;
        }
        return false;
    }

    /// Drop every range whose ABORT marker lies below `logStartOffset` (its
    /// data is gone with it).  Cold (retention).
    void pruneBelow(int64_t logStartOffset)
    {
        auto it = std::lower_bound(ranges_.begin(), ranges_.end(), logStartOffset,
                                   [](const AbortedTxnRange& r, int64_t off) { return r.lastOffset < off; });
        if (it == ranges_.begin()) return;
        ranges_.erase(ranges_.begin(), it);   // cold (retention); capacity kept
        int64_t span = 0;
        for (const auto& r : ranges_) span = std::max(span, r.lastOffset - r.firstOffset);
        maxSpan_ = span;
    }

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

} // namespace rune::transaction
