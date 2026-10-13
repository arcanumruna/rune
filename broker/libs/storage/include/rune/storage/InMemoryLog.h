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
// Phase 2b E2 (TRANSIENT topics — the produce/subscribe data path):
//   appendNoSyncRestamp(): the Log-compatible zero-copy restamp append used by
//     the worker produce path (handleProduceBatch<InMemoryLog>): ONE memcpy into
//     the ring, then the assigned base offset and (when leading) the leader epoch
//     are patched in the ring copy — never in the caller's bytes.
//   Eviction floor: DropOldest never evicts a batch whose lastOffset >= the
//     floor (the owning replica keeps it at its high watermark: uncommitted
//     data).  An append that would need such an eviction is REFUSED
//     (kRefusedFull).  Default floor = INT64_MAX (unbounded: plain DropOldest).
//   kRefusedTooLarge: a batch larger than the whole ring (never storable).
//   resetTo(): restart EMPTY at an offset (follower below the leader's ring start).
//
// Namespace: rune::storage   C++23
// ---------------------------------------------------------------------------

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include <rune/storage/LogReadView.h>
#include <rune/protocol/RecordBatch.h>

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
        // Allocate the fixed-capacity circular ENTRY ring exactly once.  Unlike a
        // std::vector, evicting the oldest entry advances a head index in O(1)
        // (no front-erase memmove of the surviving descriptors).
        entries_.resize(maxRecords);
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
    /// `data` may hold SEVERAL whole RecordBatches (a follower applies a fetched
    /// span in one call): each becomes its own entry, exactly as the disk Log
    /// indexes them (Phase 2b E2 Task 11) — see appendSpan_.
    int64_t append(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        return appendSpan_(data, len, lastOffset);
    }

    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
        return appendSpan_(batch.data(), batch.size(), lastOffset);
    }

    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        return appendSpan_(data, len, lastOffset);
    }

    // -----------------------------------------------------------------------
    // Phase 2b E2 — the transient produce path.
    // -----------------------------------------------------------------------
    /// The append would have to evict a batch at/above the eviction floor (or the
    /// BackPressure policy's ring is full): nothing appended, retry later.
    static constexpr int64_t kRefusedFull     = -1;
    /// The batch is larger than the whole ring: it can never be stored.
    static constexpr int64_t kRefusedTooLarge = -2;

    /// Log::appendNoSyncRestamp parity: assigns base = logEndOffset(), copies
    /// [batch, batch+len) into the ring ONCE and patches the base offset
    /// (bytes [0..7]) and — when leaderEpochStamp() >= 0 — the partition leader
    /// epoch (bytes [12..15], outside the CRC) in the RING COPY; `batch` is never
    /// mutated.  Returns nextOffset (base + lastOffsetDelta + 1) and sets
    /// outBaseOffset, or a negative refusal code (kRefusedFull / kRefusedTooLarge)
    /// with nothing appended.  Zero allocation.
    int64_t appendNoSyncRestamp(const uint8_t* batch, std::size_t len,
                                int32_t lastOffsetDelta, int64_t& outBaseOffset) {
        // A batch shorter than its base-offset field is never storable (the
        // patch below would write past the copy).  handleProduceBatch size-checks
        // first; this guards any direct caller.
        if (len < static_cast<std::size_t>(rune::protocol::batch::kBaseOffsetOffset + 8)) [[unlikely]]
            return kRefusedTooLarge;
        const int64_t base = logEndOffset_;
        const int64_t r = appendImpl(batch, len, base + static_cast<int64_t>(lastOffsetDelta));
        if (r < 0) return r;
        uint8_t* dst = ring_.data() + backEntry().ringOffset;
        const auto ub = static_cast<uint64_t>(base);
        for (int i = 0; i < 8; ++i)
            dst[rune::protocol::batch::kBaseOffsetOffset + i] = static_cast<uint8_t>(ub >> (8 * i));
        if (leaderEpochStamp_ >= 0 &&
            len >= static_cast<std::size_t>(rune::protocol::batch::kPartitionLeaderEpochOffset + 4)) {
            const auto ue = static_cast<uint32_t>(leaderEpochStamp_);
            for (int i = 0; i < 4; ++i)
                dst[rune::protocol::batch::kPartitionLeaderEpochOffset + i] = static_cast<uint8_t>(ue >> (8 * i));
        }
        outBaseOffset = base;
        return logEndOffset_;
    }

    /// Log::setLeaderEpochStamp parity: the epoch appendNoSyncRestamp writes into
    /// every batch it stores (-1 = leave the producer's bytes).  Set by the owning
    /// PartitionReplica on leadership changes.  Owner thread only.
    void setLeaderEpochStamp(int32_t epoch) noexcept { leaderEpochStamp_ = epoch; }
    [[nodiscard]] int32_t leaderEpochStamp() const noexcept { return leaderEpochStamp_; }

    /// Eviction floor: DropOldest never evicts a batch whose lastOffset >= floor.
    /// The owning replica sets it to its high watermark before each append.
    void setEvictionFloor(int64_t offset) noexcept { evictionFloor_ = offset; }
    [[nodiscard]] int64_t evictionFloor() const noexcept { return evictionFloor_; }

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes) {
        if (startOffset < logStartOffset_ || entryCount_ == 0) {
            return {};
        }

        std::vector<uint8_t> out;
        out.reserve(static_cast<std::size_t>(maxBytes));

        // Entry offsets are strictly increasing with logical index, so binary-search
        // the FIRST live entry whose lastOffset >= startOffset — O(log n) instead of
        // an O(n) scan from the oldest entry.  This is the common case where a reader
        // resumes from a deep offset (e.g. pushSubscriptions on a large ring).
        std::size_t lo = 0, hi = entryCount_;
        while (lo < hi) {
            const std::size_t mid = lo + (hi - lo) / 2;
            if (entryAt(mid).lastOffset < startOffset) lo = mid + 1;
            else hi = mid;
        }

        // Walk from the located entry by PHYSICAL index, wrapping with a predictable
        // compare-and-reset rather than a per-element `% maxRecords_` (a real division
        // on this non-pow2 capacity).
        std::size_t phys = (entryHead_ + lo) % maxRecords_;
        for (std::size_t i = lo; i < entryCount_; ++i) {
            const Entry& e = entries_[phys];
            if (++phys == maxRecords_) phys = 0;
            // All entries from `lo` on have lastOffset >= startOffset (binary search).
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

    /// Zero-copy read (spec §3.5a.2): the CONTIGUOUS run of whole batches starting
    /// at the batch containing startOffset, capped at maxBytes — the first batch is
    /// always included (same progress guarantee as LogSegment::readView).  Stops at
    /// a ring wrap (next batch not adjacent); the caller continues from nextOffset.
    LogReadView readView(int64_t startOffset, int32_t maxBytes) noexcept {
        if (startOffset < logStartOffset_ || entryCount_ == 0) return {};
        std::size_t lo = 0, hi = entryCount_;
        while (lo < hi) {
            const std::size_t mid = lo + (hi - lo) / 2;
            if (entryAt(mid).lastOffset < startOffset) lo = mid + 1; else hi = mid;
        }
        if (lo == entryCount_) return {};
        const Entry& first = entryAt(lo);
        LogReadView v;
        v.data       = ring_.data() + first.ringOffset;
        v.len        = first.length;
        v.baseOffset = first.baseOffset;
        v.nextOffset = first.lastOffset + 1;
        const std::size_t cap = maxBytes > 0 ? static_cast<std::size_t>(maxBytes) : 0;
        std::size_t end = first.ringOffset + first.length;
        for (std::size_t i = lo + 1; i < entryCount_; ++i) {
            const Entry& e = entryAt(i);
            if (e.ringOffset != end) break;                   // wrapped: not contiguous
            if (cap == 0 || v.len + e.length > cap) break;
            v.len += e.length;
            end   += e.length;
            v.nextOffset = e.lastOffset + 1;
        }
        return v;
    }

    /// F27 R2: parity with Log::installGeneration() — an in-memory ring never
    /// swaps segments; truncation/eviction change the LEO/view the memo also keys on.
    [[nodiscard]] uint64_t installGeneration() const noexcept { return 0; }

    [[nodiscard]] int64_t logEndOffset() const noexcept {
        return logEndOffset_;
    }

    [[nodiscard]] int64_t logStartOffset() const noexcept {
        return logStartOffset_;
    }

    void truncateTo(int64_t truncateOffset) {
        // Remove all entries with baseOffset >= truncateOffset from the tail.
        while (entryCount_ > 0 &&
               backEntry().baseOffset >= truncateOffset) {
            const Entry& back = backEntry();
            // Detect whether this entry was preceded by a wrap-induced padding gap.
            // A wrap happened when the previous entry's end < back.ringOffset (no)
            // OR when back.ringOffset < (prev.ringOffset + prev.length) — i.e. the
            // tail wrapped back to 0 (or near 0) for this entry.
            if (entryCount_ >= 2) {
                const Entry& prev = entryAt(entryCount_ - 2);
                const std::size_t prevEnd = prev.ringOffset + prev.length;
                if (back.ringOffset < prevEnd) {
                    // This entry was placed after a wrap; there is a padding gap of
                    // (maxBytes_ - prevEnd) bytes that should be freed.
                    const std::size_t paddingGap = maxBytes_ - prevEnd;
                    bytesUsed_ -= paddingGap;
                }
            } else if (entryCount_ == 1) {
                // Only one entry — if tail_ > back.ringOffset + back.length there's
                // padding at the logical end, but there can be no wrap (head_ == 0).
                // No action needed.
            }
            bytesUsed_ -= back.length;
            // Retract tail to free the space this entry occupied.
            tail_ = back.ringOffset;
            popBackEntry();
        }
        if (entryCount_ == 0) {
            head_ = tail_ = 0;
            entryHead_ = 0;
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
        if (entryCount_ == 0) return {};
        return {logStartOffset_};
    }

    /// Phase 2b E2 (follower catch-up): drop every batch and restart the ring
    /// EMPTY at `offset` (logStart = logEnd = offset).  Keeps the eviction floor
    /// and the leader-epoch stamp.  Cold.
    void resetTo(int64_t offset) noexcept {
        entryHead_ = 0; entryCount_ = 0;
        head_ = 0; tail_ = 0; bytesUsed_ = 0;
        logStartOffset_ = offset;
        logEndOffset_   = offset;
    }

    /// Log parity for handleProduceBatch<LogT>: a ring never fails storage.
    [[nodiscard]] constexpr bool storageFailed() const noexcept { return false; }
    void markStorageFailed(int /*err*/) noexcept {}

    /// The ring's byte capacity (a batch larger than this is kRefusedTooLarge).
    [[nodiscard]] std::size_t capacityBytes() const noexcept { return maxBytes_; }
    /// Appends refused because the ring would have had to evict at/above the floor.
    [[nodiscard]] uint64_t fullRefusals() const noexcept { return fullRefusals_; }
    /// Records evicted (retention), summed over evicted batches.
    [[nodiscard]] uint64_t evictedRecords() const noexcept { return evictedRecords_; }

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
    // Entry-ring accessors — entries_ is a FIXED-CAPACITY CIRCULAR buffer of
    // maxRecords_ slots.  `entryHead_` is the physical index of the oldest live
    // entry; `entryCount_` is the number of live entries.  Logical index i
    // (0 == oldest) maps to physical slot (entryHead_ + i) % maxRecords_.
    //
    // This replaces the former std::vector<Entry> whose erase(begin()) on
    // eviction shifted every surviving descriptor (O(n) per append in the
    // steady-state-full hot path).  All operations below are O(1).
    // -----------------------------------------------------------------------
    [[nodiscard]] Entry& entryAt(std::size_t i) noexcept {
        return entries_[(entryHead_ + i) % maxRecords_];
    }
    [[nodiscard]] const Entry& entryAt(std::size_t i) const noexcept {
        return entries_[(entryHead_ + i) % maxRecords_];
    }
    [[nodiscard]] Entry&       frontEntry() noexcept { return entries_[entryHead_]; }
    [[nodiscard]] const Entry& frontEntry() const noexcept { return entries_[entryHead_]; }
    [[nodiscard]] Entry&       backEntry() noexcept { return entryAt(entryCount_ - 1); }
    [[nodiscard]] const Entry& backEntry() const noexcept { return entryAt(entryCount_ - 1); }

    void pushBackEntry(const Entry& e) noexcept {
        // Precondition (guaranteed by callers): entryCount_ < maxRecords_.
        entries_[(entryHead_ + entryCount_) % maxRecords_] = e;
        ++entryCount_;
    }
    void popFrontEntry() noexcept {
        // Compare-and-reset, not `% maxRecords_` (a real division on a non-pow2
        // capacity) — once per evicted batch (Task 11: per batch of a span too).
        if (++entryHead_ == maxRecords_) entryHead_ = 0;
        --entryCount_;
    }
    void popBackEntry() noexcept { --entryCount_; }

    // -----------------------------------------------------------------------
    // Phase 2b E2 Task 11 — a multi-batch span is indexed PER BATCH.
    //
    // A follower applies a fetched span of whole batches in one append.  Held as
    // ONE entry, a read from a mid-span offset re-delivered the earlier batches,
    // eviction (and the eviction floor) worked at span granularity, and
    // truncateTo(n) with n inside the span kept the stale bytes past n.  Now: the
    // span is still copied ONCE and contiguously, but each batch gets its own
    // entry (base/last from its header, rebased onto logEndOffset_).
    //
    // countSpanBatches_ returns the number of batches when [data, len) is an exact
    // sequence of whole, magic-2, offset-increasing batches whose relative extent
    // matches lastOffset; otherwise 0 — the bytes are stored as ONE opaque entry
    // (the pre-existing contract for non-batch payloads).  Header reads only; no
    // allocation.  The leader's produce path (appendNoSyncRestamp: one batch) does
    // not come here.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t countSpanBatches_(const uint8_t* data, std::size_t len,
                                                int64_t lastOffset) const noexcept {
        namespace rb = rune::protocol::batch;
        std::size_t n = 0, pos = 0;
        int64_t first = 0, prevLast = 0;
        while (pos < len) {
            const std::size_t rem = len - pos;
            if (rem < static_cast<std::size_t>(rb::kRecordBatchOverhead)) return 0;
            if (static_cast<int8_t>(data[pos + rb::kMagicOffset]) != rb::kMagicValue) return 0;
            const rune::protocol::RecordBatchReader r(data + pos, rem);
            const int32_t total = r.totalLength();
            if (total < rb::kRecordBatchOverhead || static_cast<std::size_t>(total) > rem) return 0;
            const int64_t base = r.baseOffset();
            const int32_t delta = r.lastOffsetDelta();
            if (delta < 0) return 0;
            if (n == 0) first = base;
            else if (base <= prevLast) return 0;
            prevLast = base + delta;
            pos += static_cast<std::size_t>(total);
            ++n;
        }
        if (n == 0 || logEndOffset_ + (prevLast - first) != lastOffset) return 0;
        return n;
    }

    int64_t appendSpan_(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        const std::size_t n = countSpanBatches_(data, len, lastOffset);
        if (n <= 1) return appendImpl(data, len, lastOffset);
        // More batches than the entry ring holds can never be stored whole.
        if (n > maxRecords_) [[unlikely]] return kRefusedTooLarge;
        return appendImpl(data, len, lastOffset, n);
    }

    // -----------------------------------------------------------------------
    // Core append logic.  `nEntries` > 1: [data, len) is a span of that many
    // whole batches (validated by countSpanBatches_) — one entry per batch.
    // -----------------------------------------------------------------------
    int64_t appendImpl(const uint8_t* data, std::size_t len, int64_t lastOffset,
                       std::size_t nEntries = 1) {
        // Phase 2b E2: a batch larger than the whole ring can never be stored
        // (was an assert — a release build corrupted the ring).
        if (len > maxBytes_) [[unlikely]] return kRefusedTooLarge;

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
            while (entryCount_ > 0) {
                // Recompute wrap/totalNeed against CURRENT tail_.
                const std::size_t la = maxBytes_ - tail_;
                const bool ww = (len > la);
                const std::size_t need = len + (ww ? la : 0);
                if (bytesUsed_ + need <= maxBytes_ && entryCount_ + nEntries <= maxRecords_)
                    break; // fits — stop evicting
                // Phase 2b E2: never evict at/above the floor (uncommitted data
                // of a transient partition) — refuse the append instead.
                if (frontEntry().lastOffset >= evictionFloor_) [[unlikely]] {
                    ++fullRefusals_;
                    return kRefusedFull;
                }
                evictOldest();
            }
        } else {
            // BackPressure: refuse if caps would be exceeded.
            const std::size_t la = maxBytes_ - tail_;
            const bool ww = (len > la);
            const std::size_t need = len + (ww ? la : 0);
            if (bytesUsed_ + need > maxBytes_ || entryCount_ + nEntries > maxRecords_) {
                ++fullRefusals_;
                return kRefusedFull; // sentinel: ring full, caller must retry later
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

        if (nEntries == 1) {
            pushBackEntry(Entry{baseOffset, lastOffset, ringPos, len});
        } else {
            // One entry per batch of the (validated) span, rebased onto baseOffset.
            const uint8_t* const copy = ring_.data() + ringPos;
            const int64_t first = rune::protocol::RecordBatchReader(copy, len).baseOffset();
            // Precondition (the fit loop above): entryCount_ + nEntries <= maxRecords_.
            std::size_t pos  = 0;
            std::size_t phys = (entryHead_ + entryCount_) % maxRecords_;   // one division per span
            for (std::size_t i = 0; i < nEntries; ++i) {
                const rune::protocol::RecordBatchReader r(copy + pos, len - pos);
                const auto total = static_cast<std::size_t>(r.totalLength());
                const int64_t b = baseOffset + (r.baseOffset() - first);
                entries_[phys] = Entry{b, b + r.lastOffsetDelta(), ringPos + pos, total};
                if (++phys == maxRecords_) phys = 0;
                pos += total;
            }
            entryCount_ += nEntries;
        }
        logEndOffset_ = lastOffset + 1;

        return baseOffset;
    }

    // O(1) eviction — advances head_ past the oldest batch (and any leading
    // wrap-padding that preceded it), decrements bytesUsed_, advances
    // logStartOffset_.  NO memmove, NO rebase loop.
    void evictOldest() {
        assert(entryCount_ > 0);
        const Entry& oldest = frontEntry();
        evictedRecords_ += static_cast<uint64_t>(oldest.lastOffset + 1 - oldest.baseOffset);

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
        // O(1): advance the entry-ring head past the oldest descriptor — NO
        // front-erase memmove of the surviving entries.
        popFrontEntry();

        // If the entry ring is now empty, reset head_/tail_ to 0 (clean slate).
        if (entryCount_ == 0) {
            head_ = tail_ = 0;
            entryHead_ = 0;
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    std::size_t           maxBytes_;
    std::size_t           maxRecords_;
    FullPolicy            policy_;
    std::vector<uint8_t>  ring_;             ///< fixed-capacity circular byte buffer
    std::vector<Entry>    entries_;          ///< fixed-capacity circular entry ring (size == maxRecords_)
    std::size_t           entryHead_  = 0;   ///< physical index of oldest live entry in entries_
    std::size_t           entryCount_ = 0;   ///< number of live entries in the ring
    std::size_t           head_      = 0;    ///< oldest-data index in ring_ (eviction side)
    std::size_t           tail_      = 0;    ///< next-write index in ring_ (append side)
    std::size_t           bytesUsed_ = 0;    ///< logical bytes in use (data + padding gaps)
    int64_t               logStartOffset_ = 0;
    int64_t               logEndOffset_   = 0;
    int64_t               evictionFloor_  = std::numeric_limits<int64_t>::max();   ///< E2: see setEvictionFloor
    int32_t               leaderEpochStamp_ = -1;                                   ///< E2: see setLeaderEpochStamp
    uint64_t              fullRefusals_   = 0;
    uint64_t              evictedRecords_ = 0;

    // Static synthetic dir string — no real directory.
    inline static const std::string syntheticDir_{"<inmem>"};
};

} // namespace rune::storage
