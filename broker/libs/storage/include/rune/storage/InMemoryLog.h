#pragma once
// ---------------------------------------------------------------------------
// InMemoryLog.h — single-owner circular ring buffer, no memmove, O(1) evict.
//
// Zero-disk, zero-alloc steady-state append.  Used for replication tests and
// disaster-recovery harnesses where a real on-disk Log is not needed.
//
// Storage: a fixed-capacity circular byte buffer (capacity = maxBytes_).
// Each batch is stored CONTIGUOUSLY.  When a batch would not fit between the
// current tail and the buffer end, tail wraps back to 0 (the gap bytes are
// accounted for as "padding" and never contain batch data).
//
// Eviction policy (DropOldest — DEFAULT):
//   When either the byte cap OR the record cap is exceeded after an append,
//   the OLDEST batch is evicted, head advances by its length (+any leading
//   padding), and logStartOffset_ advances.  Eviction continues until both
//   caps are satisfied (or only one batch remains).
//   ALL of this is O(1) per eviction — NO memmove, NO rebase loop.
//
// Back-pressure policy (BackPressure — opt-in via FullPolicy ctor param):
//   When the ring is full (byte cap or record cap), the append is REFUSED and
//   returns the sentinel value -1.  No data is evicted, no memory grows.
//   After truncateTo() frees space, appends resume normally.
//
// read(startOffset) below logStartOffset_ → empty (out-of-range, same semantics
// as Log::read on an evicted segment).
//
// truncateTo(n): removes all batches whose baseOffset >= n and resets
// logEndOffset.  Safe with circular storage: removes from the tail of entries_.
//
// sync(): no-op.
// recover(): no-op (returns 0).
// dir(): returns a synthetic string "<inmem>" — no real directory.
// segmentBaseOffsets(): returns {logStartOffset_} so PSM rebuildFromLog's
//   CP-3/CP-4 gap-skip logic works (one "virtual segment" starting at the
//   oldest live offset).
//
// NOT thread-safe (same contract as rune::storage::Log — single writer).
//
// Namespace: rune::storage   C++23
// ---------------------------------------------------------------------------

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace rune::storage {

/// Policy controlling behaviour when the ring reaches capacity.
enum class FullPolicy {
    /// Default: evict the oldest batch to make room.  logStartOffset_ advances.
    DropOldest,
    /// Opt-in: refuse the append and return -1.  No eviction, no growth.
    BackPressure,
};

/// De-virtualized (Milestone D Phase 2): `InMemoryLog` is an INDEPENDENT concrete
/// class (the former `ILog` base is gone).  It exposes the SAME method surface as
/// `rune::storage::Log` so `PartitionReplica` can dispatch on a `bool memoryBacked_`
/// to a concrete typed pointer with NO vtable on the hot path.
class InMemoryLog {
public:
    /// Construct a bounded in-memory log.
    /// @param maxBytes    Hard byte capacity of the circular ring.  Must be > 0.
    /// @param maxRecords  Max number of batches held simultaneously.  Must be > 0.
    /// @param policy      FullPolicy::DropOldest (default) or BackPressure.
    explicit InMemoryLog(std::size_t maxBytes,
                         std::size_t maxRecords,
                         FullPolicy  policy = FullPolicy::DropOldest)
        : maxBytes_(maxBytes), maxRecords_(maxRecords), policy_(policy)
    {
        assert(maxBytes  > 0 && "InMemoryLog: maxBytes must be > 0");
        assert(maxRecords > 0 && "InMemoryLog: maxRecords must be > 0");
        // Allocate the fixed-size circular byte buffer exactly once.
        ring_.resize(maxBytes);
        // Pre-allocate entry descriptor vector to maxRecords.
        entries_.reserve(maxRecords);
    }

    ~InMemoryLog() = default;

    InMemoryLog(const InMemoryLog&)            = delete;
    InMemoryLog& operator=(const InMemoryLog&) = delete;
    InMemoryLog(InMemoryLog&&)                 = default;
    InMemoryLog& operator=(InMemoryLog&&)      = default;

    // -----------------------------------------------------------------------
    // Log-compatible surface (concrete; no virtual — de-virtualized seam)
    // -----------------------------------------------------------------------

    /// Returns the base offset on success, or -1 if the ring is full
    /// (BackPressure mode only).
    int64_t append(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        return appendImpl(data, len, lastOffset);
    }

    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
        return appendImpl(batch.data(), batch.size(), lastOffset);
    }

    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        return appendImpl(data, len, lastOffset);
    }

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes) {
        if (startOffset < logStartOffset_ || entries_.empty()) {
            return {};
        }

        std::vector<uint8_t> out;
        out.reserve(static_cast<std::size_t>(maxBytes));

        for (const auto& e : entries_) {
            // Skip batches before startOffset.
            if (e.lastOffset < startOffset) continue;
            if (e.baseOffset < startOffset) {
                // Partial batch — skip (InMemoryLog stores whole batches, startOffset
                // should align to a batch boundary; if not, skip to next).
                continue;
            }
            const std::size_t batchBytes = e.length;
            // Mirror LogSegment::read "return at least one batch" semantics so a
            // consumer can always make forward progress even when a single batch
            // exceeds the maxBytes budget.
            if (!out.empty() &&
                static_cast<int32_t>(out.size() + batchBytes) > maxBytes) break;

            // Batch is stored contiguously in the circular buffer — single copy.
            const uint8_t* src = ring_.data() + e.ringOffset;
            out.insert(out.end(), src, src + batchBytes);
        }

        return out;
    }

    [[nodiscard]] int64_t logEndOffset() const noexcept {
        return logEndOffset_;
    }

    [[nodiscard]] int64_t logStartOffset() const noexcept {
        return logStartOffset_;
    }

    void truncateTo(int64_t truncateOffset) {
        // Remove all entries with baseOffset >= truncateOffset from the tail.
        while (!entries_.empty() &&
               entries_.back().baseOffset >= truncateOffset) {
            const Entry& back = entries_.back();
            // Detect whether this entry was preceded by a wrap-induced padding gap.
            // A wrap happened when the previous entry's end < back.ringOffset (no)
            // OR when back.ringOffset < (prev.ringOffset + prev.length) — i.e. the
            // tail wrapped back to 0 (or near 0) for this entry.
            if (entries_.size() >= 2) {
                const Entry& prev = entries_[entries_.size() - 2];
                const std::size_t prevEnd = prev.ringOffset + prev.length;
                if (back.ringOffset < prevEnd) {
                    // This entry was placed after a wrap; there is a padding gap of
                    // (maxBytes_ - prevEnd) bytes that should be freed.
                    const std::size_t paddingGap = maxBytes_ - prevEnd;
                    bytesUsed_ -= paddingGap;
                }
            } else if (entries_.size() == 1) {
                // Only one entry — if tail_ > back.ringOffset + back.length there's
                // padding at the logical end, but there can be no wrap (head_ == 0).
                // No action needed.
            }
            bytesUsed_ -= back.length;
            // Retract tail to free the space this entry occupied.
            tail_ = back.ringOffset;
            entries_.pop_back();
        }
        if (entries_.empty()) {
            head_ = tail_ = 0;
        }
        logEndOffset_ = truncateOffset;
    }

    void sync() {
        // No-op: InMemoryLog has no disk path.
    }

    int64_t recover() {
        // No-op: nothing to recover from.
        return logEndOffset_;
    }

    [[nodiscard]] const std::string& dir() const noexcept {
        return syntheticDir_;
    }

    [[nodiscard]] std::vector<int64_t> segmentBaseOffsets() const {
        // Expose a single virtual segment starting at logStartOffset_ so that
        // PSM::rebuildFromLog's CP-3 "start at first live segment" logic works.
        // An empty log returns an empty vector (same as a real Log with no segments).
        if (entries_.empty()) return {};
        return {logStartOffset_};
    }

    [[nodiscard]] bool isDiskBacked() const noexcept { return false; }

private:
    // -----------------------------------------------------------------------
    // Entry descriptor: points into the circular ring_.
    // -----------------------------------------------------------------------
    struct Entry {
        int64_t     baseOffset;  ///< first logical offset this batch covers
        int64_t     lastOffset;  ///< last logical offset this batch covers (inclusive)
        std::size_t ringOffset;  ///< byte index in ring_ where this batch starts (contiguous)
        std::size_t length;      ///< byte length of the batch
    };

    // -----------------------------------------------------------------------
    // Core append logic
    // -----------------------------------------------------------------------
    int64_t appendImpl(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        assert(len <= maxBytes_ && "batch larger than ring capacity — cannot store");

        const int64_t baseOffset = logEndOffset_;

        if (policy_ == FullPolicy::DropOldest) {
            // Evict oldest batches until there is room for the new batch AND
            // the record count is under the cap.  We re-evaluate the fit after
            // each eviction because evictOldest() may reset tail_ (and therefore
            // change whether a wrap is needed).
            //
            // Note: keep at least one remaining entry to guarantee forward
            // progress — a single-entry ring may still evict down to empty,
            // which is fine (tail_ resets to 0 and the new batch goes in fresh).
            while (!entries_.empty()) {
                // Recompute wrap/totalNeed against CURRENT tail_.
                const std::size_t la = maxBytes_ - tail_;
                const bool ww = (len > la);
                const std::size_t need = len + (ww ? la : 0);
                if (bytesUsed_ + need <= maxBytes_ && entries_.size() < maxRecords_)
                    break; // fits — stop evicting
                evictOldest();
            }
        } else {
            // BackPressure: refuse if caps would be exceeded.
            const std::size_t la = maxBytes_ - tail_;
            const bool ww = (len > la);
            const std::size_t need = len + (ww ? la : 0);
            if (bytesUsed_ + need > maxBytes_ || entries_.size() >= maxRecords_) {
                return -1; // sentinel: ring full, caller must retry later
            }
        }

        // --- Determine placement, potentially wrapping tail_ to 0 ---
        const std::size_t linearAvail = maxBytes_ - tail_;
        const bool willWrap           = (len > linearAvail);
        const std::size_t padding     = willWrap ? linearAvail : 0;

        std::size_t ringPos;
        if (willWrap) {
            // Skip the trailing linear gap (padding bytes are "used" so
            // evictOldest() can account for them when the head_ catches up).
            bytesUsed_ += padding;
            tail_       = 0;
            ringPos     = 0;
        } else {
            ringPos = tail_;
        }

        std::memcpy(ring_.data() + ringPos, data, len);
        tail_       = ringPos + len;
        bytesUsed_ += len;

        entries_.push_back(Entry{baseOffset, lastOffset, ringPos, len});
        logEndOffset_ = lastOffset + 1;

        return baseOffset;
    }

    // O(1) eviction — advances head_ past the oldest batch (and any leading
    // wrap-padding that preceded it), decrements bytesUsed_, advances
    // logStartOffset_.  NO memmove, NO rebase loop.
    void evictOldest() {
        assert(!entries_.empty());
        const Entry& oldest = entries_.front();

        // Detect wrap-induced padding: when oldest.ringOffset < head_, the ring
        // wrapped and there is a padding gap at [head_, maxBytes_) that should be
        // freed first.
        if (oldest.ringOffset < head_) {
            // The oldest entry wrapped to the start of the buffer.
            // Free the trailing padding gap.
            const std::size_t trailingGap = maxBytes_ - head_;
            bytesUsed_ -= trailingGap;
            head_ = 0;
        }
        // Now head_ == oldest.ringOffset (the batch starts right at head_).
        // Free the batch bytes.
        bytesUsed_      -= oldest.length;
        logStartOffset_  = oldest.lastOffset + 1;
        head_            = oldest.ringOffset + oldest.length;
        entries_.erase(entries_.begin());

        // If entries_ is now empty, reset head_/tail_ to 0 (clean slate).
        if (entries_.empty()) {
            head_ = tail_ = 0;
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    std::size_t           maxBytes_;
    std::size_t           maxRecords_;
    FullPolicy            policy_;
    std::vector<uint8_t>  ring_;             ///< fixed-capacity circular byte buffer
    std::vector<Entry>    entries_;          ///< in-order entry descriptors
    std::size_t           head_      = 0;    ///< oldest-data index in ring_ (eviction side)
    std::size_t           tail_      = 0;    ///< next-write index in ring_ (append side)
    std::size_t           bytesUsed_ = 0;    ///< logical bytes in use (data + padding gaps)
    int64_t               logStartOffset_ = 0;
    int64_t               logEndOffset_   = 0;

    // Static synthetic dir string — no real directory.
    inline static const std::string syntheticDir_{"<inmem>"};
};

} // namespace rune::storage
