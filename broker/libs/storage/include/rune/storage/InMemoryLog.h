#pragma once
// ---------------------------------------------------------------------------
// InMemoryLog.h — bounded drop-oldest ring implementing ILog.
//
// Zero-disk, zero-alloc steady-state append.  Used for replication tests and
// disaster-recovery harnesses where a real on-disk Log is not needed.
//
// Eviction policy: when either the byte cap OR the record cap is exceeded
// after an append, the OLDEST batch is evicted and logStartOffset_ advances.
// Eviction continues until both caps are satisfied (or only one batch remains).
//
// read(startOffset) below logStartOffset_ → empty (out-of-range, same semantics
// as Log::read on an evicted segment).
//
// truncateTo(n): removes all batches whose baseOffset >= n and resets logEndOffset.
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

#include <rune/storage/ILog.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace rune::storage {

class InMemoryLog : public ILog {
public:
    /// Construct a bounded in-memory log.
    /// @param maxBytes    Soft byte cap — evict oldest batches when total stored
    ///                    bytes would exceed this.  Must be > 0.
    /// @param maxRecords  Soft record cap — evict oldest batches when the number
    ///                    of stored batches would exceed this.  Must be > 0.
    explicit InMemoryLog(std::size_t maxBytes, std::size_t maxRecords)
        : maxBytes_(maxBytes), maxRecords_(maxRecords)
    {
        assert(maxBytes  > 0 && "InMemoryLog: maxBytes must be > 0");
        assert(maxRecords > 0 && "InMemoryLog: maxRecords must be > 0");
        // Pre-allocate ring storage to maxBytes to avoid steady-state reallocation.
        ring_.reserve(maxBytes);
    }

    ~InMemoryLog() override = default;

    InMemoryLog(const InMemoryLog&)            = delete;
    InMemoryLog& operator=(const InMemoryLog&) = delete;
    InMemoryLog(InMemoryLog&&)                 = default;
    InMemoryLog& operator=(InMemoryLog&&)      = default;

    // -----------------------------------------------------------------------
    // ILog implementation
    // -----------------------------------------------------------------------

    int64_t append(const uint8_t* data, std::size_t len, int64_t lastOffset) override {
        return appendImpl(data, len, lastOffset);
    }

    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset) override {
        return appendImpl(batch.data(), batch.size(), lastOffset);
    }

    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) override {
        return appendImpl(data, len, lastOffset);
    }

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes) override {
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
            if (static_cast<int32_t>(out.size() + batchBytes) > maxBytes) break;

            const uint8_t* src = ring_.data() + e.ringOffset;
            out.insert(out.end(), src, src + batchBytes);
        }

        return out;
    }

    [[nodiscard]] int64_t logEndOffset() const noexcept override {
        return logEndOffset_;
    }

    [[nodiscard]] int64_t logStartOffset() const noexcept override {
        return logStartOffset_;
    }

    void truncateTo(int64_t truncateOffset) override {
        // Remove all entries with baseOffset >= truncateOffset.
        while (!entries_.empty() &&
               entries_.back().baseOffset >= truncateOffset) {
            bytesUsed_ -= entries_.back().length;
            // Shrink the ring storage too (ring is a simple append-only buffer;
            // truncate from the tail by resizing).
            ring_.resize(entries_.back().ringOffset);
            entries_.pop_back();
        }
        logEndOffset_ = truncateOffset;
    }

    void sync() override {
        // No-op: InMemoryLog has no disk path.
    }

    int64_t recover() override {
        // No-op: nothing to recover from.
        return logEndOffset_;
    }

    [[nodiscard]] const std::string& dir() const noexcept override {
        return syntheticDir_;
    }

    [[nodiscard]] std::vector<int64_t> segmentBaseOffsets() const override {
        // Expose a single virtual segment starting at logStartOffset_ so that
        // PSM::rebuildFromLog's CP-3 "start at first live segment" logic works.
        // An empty log returns an empty vector (same as a real Log with no segments).
        if (entries_.empty()) return {};
        return {logStartOffset_};
    }

private:
    // -----------------------------------------------------------------------
    // Entry descriptor: points into ring_.
    // -----------------------------------------------------------------------
    struct Entry {
        int64_t   baseOffset;  ///< first logical offset this batch covers
        int64_t   lastOffset;  ///< last logical offset this batch covers (inclusive)
        std::size_t ringOffset; ///< byte offset into ring_ where this batch starts
        std::size_t length;     ///< byte length of the batch
    };

    // -----------------------------------------------------------------------
    // Core append logic
    // -----------------------------------------------------------------------
    int64_t appendImpl(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        // Determine the base offset = current LEO.
        const int64_t baseOffset = logEndOffset_;

        // Append to ring (simple linear buffer — steady-state no realloc because
        // we reserved maxBytes in the constructor; eviction contracts ring_.size()
        // back toward zero by reassigning).
        const std::size_t ringPos = ring_.size();
        ring_.insert(ring_.end(), data, data + len);

        entries_.push_back(Entry{baseOffset, lastOffset, ringPos, len});
        bytesUsed_ += len;
        logEndOffset_ = lastOffset + 1;

        // Evict oldest batches until caps are satisfied.
        // Keep at least one entry (the one we just appended).
        while (entries_.size() > 1 &&
               (bytesUsed_ > maxBytes_ || entries_.size() > maxRecords_)) {
            evictOldest();
        }

        return baseOffset;
    }

    void evictOldest() {
        assert(!entries_.empty());
        const auto& oldest = entries_.front();
        bytesUsed_ -= oldest.length;
        logStartOffset_ = oldest.lastOffset + 1;
        entries_.erase(entries_.begin());

        // Compact the ring: remove the leading bytes that belonged to the
        // evicted entry.  Rebase all surviving entries' ringOffset values.
        //
        // Since the ring grows linearly (we always append) and entries are
        // evicted from the front, compacting means shifting bytes down.
        // This is O(N) but eviction is rare relative to steady-state appends
        // (it only fires when a cap is hit), so it is acceptable.
        if (!entries_.empty()) {
            const std::size_t dropBytes = entries_.front().ringOffset;
            if (dropBytes > 0) {
                // Shift surviving bytes to the front of ring_.
                const std::size_t survivingBytes = ring_.size() - dropBytes;
                std::memmove(ring_.data(), ring_.data() + dropBytes, survivingBytes);
                ring_.resize(survivingBytes);

                // Rebase all entry ring offsets.
                for (auto& e : entries_) {
                    e.ringOffset -= dropBytes;
                }
            }
        } else {
            // All entries evicted — reset ring to empty.
            ring_.clear();
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    std::size_t           maxBytes_;
    std::size_t           maxRecords_;
    std::vector<uint8_t>  ring_;            ///< preallocated byte buffer
    std::vector<Entry>    entries_;         ///< in-order entry descriptors
    std::size_t           bytesUsed_ = 0;   ///< total bytes currently in ring_
    int64_t               logStartOffset_ = 0;
    int64_t               logEndOffset_   = 0;

    // Static synthetic dir string — no real directory.
    inline static const std::string syntheticDir_{"<inmem>"};
};

} // namespace rune::storage
