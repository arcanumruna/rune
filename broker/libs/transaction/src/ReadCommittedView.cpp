// ---------------------------------------------------------------------------
// ReadCommittedView.cpp — LSO tracking + aborted-txn index + read_committed
//                          filtering. (P5-4, all-LE)
//
// Namespace: rune::transaction
// C++23. NO locks. NO busy-spin. NO hot-path alloc beyond scan buffers.
// Single-writer.
//
// All-LE: RecordBatch fixed-header fields are now little-endian.
// RecordBatchReader is the unified LE reader for both data and control batches.
// The private extractProducerId() helper now uses RecordBatchReader::producerId()
// (LE) directly — the old BE dual-path has been removed.
// ---------------------------------------------------------------------------

#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/ControlMarker.h>

#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>
// rune-protocol RecordBatch: little-endian field accessors (unified).
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cassert>
#include <limits>

namespace rune::transaction {

// ===========================================================================
// Helpers
// ===========================================================================

namespace {

// All-LE: RecordBatch fixed-header fields are now little-endian for BOTH
// data batches and control batches.  RecordBatchReader provides the unified
// LE reader; RecordBatchReader::producerId() reads the LE int64 at offset 43.
// Control batches are identified by RecordBatchReader::isControl() which checks
// the CONTROL attribute bit (0x0020) in the LE int16 at [21..22].
// ControlMarker::isControlBatch() also does CRC verification (stronger check).

// Extract producerId from a raw LE batch at position `pos`.
// Works for both data batches and LE control batches (unified header layout).
// Returns -1 if extraction fails.
inline int64_t extractProducerId(const uint8_t* buf, std::size_t pos,
                                  std::size_t avail) noexcept
{
    using namespace rune::protocol::batch;
    if (avail < static_cast<std::size_t>(kRecordBatchOverhead)) return -1;
    if (avail < static_cast<std::size_t>(kProducerIdOffset + 8)) return -1;

    // All fields are LE — use the unified RecordBatchReader.
    rune::protocol::RecordBatchReader r(buf + pos, avail);
    return r.producerId();
}

} // anonymous namespace

// ===========================================================================
// LsoTracker — flat pid table + first-offset FIFO (F15 §2.2, N4)
// ===========================================================================

namespace {
// splitmix64 finaliser: spreads sequential pids across the table.
inline uint64_t mixPid(int64_t pid) noexcept {
    uint64_t x = static_cast<uint64_t>(pid);
    x ^= x >> 33; x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}
inline std::size_t roundUpPow2(std::size_t n) noexcept {
    std::size_t c = 1;
    while (c < n) c <<= 1;
    return c;
}
} // namespace

void LsoTracker::reserve(std::size_t openTxnCapacity)
{
    const std::size_t want = roundUpPow2(openTxnCapacity == 0 ? 1 : openTxnCapacity);
    if (want <= fifo_.size()) return;
    rebuild_(want);
}

std::size_t LsoTracker::slotFor_(ProducerId pid) const noexcept
{
    return static_cast<std::size_t>(mixPid(pid)) & tableMask_;
}

const LsoTracker::Slot* LsoTracker::find_(ProducerId pid) const noexcept
{
    if (table_.empty()) return nullptr;
    // Load factor <= 1/2 (live_ <= fifo capacity == table size / 2): a free
    // slot always terminates the probe.
    for (std::size_t i = slotFor_(pid);; i = (i + 1) & tableMask_) {
        const Slot& s = table_[i];
        if (s.producerId == pid) return &s;
        if (s.producerId == kEmptyPid) return nullptr;
    }
}

LsoTracker::Slot* LsoTracker::find_(ProducerId pid) noexcept
{
    return const_cast<Slot*>(static_cast<const LsoTracker*>(this)->find_(pid));
}

void LsoTracker::insertSlot_(ProducerId pid, uint64_t pos) noexcept
{
    std::size_t i = slotFor_(pid);
    while (table_[i].producerId != kEmptyPid) i = (i + 1) & tableMask_;
    table_[i].producerId = pid;
    table_[i].pos        = pos;
}

void LsoTracker::eraseSlot_(Slot* s) noexcept
{
    // Backward-shift deletion (no tombstones): pull every later member of the
    // probe run whose home slot is not cyclically in (hole, j] into the hole.
    std::size_t hole = static_cast<std::size_t>(s - table_.data());
    std::size_t j = hole;
    for (;;) {
        j = (j + 1) & tableMask_;
        if (table_[j].producerId == kEmptyPid) break;
        const std::size_t home = slotFor_(table_[j].producerId);
        const bool stays = (hole <= j) ? (hole < home && home <= j)
                                       : (hole < home || home <= j);
        if (!stays) {
            table_[hole] = table_[j];
            hole = j;
        }
    }
    table_[hole].producerId = kEmptyPid;
}

void LsoTracker::rebuild_(std::size_t newFifoCap)
{
    // Cold: re-linearise the live entries (in FIFO order) into fresh storage.
    std::vector<FifoEntry> nf(newFifoCap);
    std::size_t n = 0;
    for (uint64_t p = head_; p != tail_; ++p) {
        const FifoEntry& e = fifo_[p & fifoMask_];
        if (e.live) nf[n++] = e;
    }
    fifo_.swap(nf);
    table_.assign(newFifoCap * 2, Slot{});
    fifoMask_  = newFifoCap - 1;
    tableMask_ = newFifoCap * 2 - 1;
    head_ = 0;
    tail_ = n;
    for (uint64_t p = 0; p < n; ++p) insertSlot_(fifo_[p].producerId, p);
}

void LsoTracker::ensureRoom_()
{
    if (fifo_.empty()) {                       // first insert: lazy default reserve
        rebuild_(kDefaultOpenTxnCapacity);
        return;
    }
    if (tail_ - head_ < fifo_.size()) return;  // room at the tail
    if (live_ * 2 <= fifo_.size()) {
        // Mostly dead entries behind a long-open front: compact IN PLACE
        // (allocation-free; amortised O(1) per insert).
        uint64_t w = head_;
        for (uint64_t r = head_; r != tail_; ++r) {
            const FifoEntry e = fifo_[r & fifoMask_];
            if (!e.live) continue;
            if (w != r) {
                fifo_[w & fifoMask_] = e;
                find_(e.producerId)->pos = w;
            }
            ++w;
        }
        tail_ = w;
        return;
    }
    rebuild_(fifo_.size() * 2);                // cold doubling
    ++growths_;
}

void LsoTracker::popDeadFront_() noexcept
{
    while (head_ != tail_ && !fifo_[head_ & fifoMask_].live) ++head_;
    if (head_ == tail_) head_ = tail_ = 0;     // empty: restart at slot 0
}

void LsoTracker::onProduceBatch(ProducerId producerId, int64_t batchBaseOffset)
{
    if (producerId < 0) return;               // non-transactional batch; no effect on LSO
    if (find_(producerId) != nullptr) return; // already open: keep the earlier firstOffset

    ensureRoom_();
    uint64_t pos = tail_;
    // Keep the FIFO in firstOffset order.  Under L1 this loop never runs.
    if (pos != head_ && fifo_[(pos - 1) & fifoMask_].firstOffset > batchBaseOffset) [[unlikely]] {
        ++outOfOrderInserts_;
        while (pos != head_ && fifo_[(pos - 1) & fifoMask_].firstOffset > batchBaseOffset) {
            const FifoEntry prev = fifo_[(pos - 1) & fifoMask_];
            fifo_[pos & fifoMask_] = prev;
            if (prev.live) find_(prev.producerId)->pos = pos;
            --pos;
        }
    }
    fifo_[pos & fifoMask_] = FifoEntry{batchBaseOffset, producerId, true};
    ++tail_;
    insertSlot_(producerId, pos);
    ++live_;
}

int64_t LsoTracker::onControlMarker(ProducerId producerId,
                                    int64_t    /*markerBaseOffset*/,
                                    bool       /*commit*/) noexcept
{
    if (producerId < 0) return -1;
    Slot* s = find_(producerId);
    if (s == nullptr) return -1;              // not tracked (non-txn or duplicate marker)
    FifoEntry& e = fifo_[s->pos & fifoMask_];
    const int64_t firstOff = e.firstOffset;
    e.live = false;
    eraseSlot_(s);
    --live_;
    popDeadFront_();
    return firstOff;
}

int64_t LsoTracker::lso(int64_t hw) const noexcept
{
    if (live_ == 0) return hw;                // no open transactions → LSO == HW
    // The front is always the oldest LIVE entry (dead ones are popped eagerly).
    const int64_t minFirst = fifo_[head_ & fifoMask_].firstOffset;
    if (minFirst > hw) return hw;
    if (minFirst < 0)  return 0;
    return minFirst;
}

int64_t LsoTracker::firstOffsetOf(ProducerId producerId) const noexcept
{
    if (producerId < 0) return -1;
    const Slot* s = find_(producerId);
    return s ? fifo_[s->pos & fifoMask_].firstOffset : -1;
}

void LsoTracker::clear() noexcept
{
    // N4: both indexes are reset here, in one place.
    for (auto& sl : table_) sl.producerId = kEmptyPid;
    head_ = tail_ = 0;
    live_ = 0;
}

// ===========================================================================
// AbortedTxnIndex
// ===========================================================================

void AbortedTxnIndex::onAbortMarker(ProducerId producerId,
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

bool AbortedTxnIndex::isAborted(ProducerId producerId,
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

void AbortedTxnIndex::pruneBelow(int64_t logStartOffset)
{
    auto it = std::lower_bound(ranges_.begin(), ranges_.end(), logStartOffset,
                               [](const AbortedTxnRange& r, int64_t off) { return r.lastOffset < off; });
    if (it == ranges_.begin()) return;
    ranges_.erase(ranges_.begin(), it);   // cold (retention); capacity kept
    int64_t span = 0;
    for (const auto& r : ranges_) span = std::max(span, r.lastOffset - r.firstOffset);
    maxSpan_ = span;
}

// ===========================================================================
// RcBuilder — step (F15 §2.3, N2 + N3)
// ===========================================================================

RcBuilder::Result RcBuilder::step(const uint8_t* batch, std::size_t avail)
{
    using namespace rune::protocol::batch;
    Result res;
    if (batch == nullptr || avail < static_cast<std::size_t>(kRecordBatchOverhead)) return res;
    rune::protocol::RecordBatchReader reader(batch, avail);
    const int32_t totalLen = reader.totalLength();
    if (totalLen <= 0 || static_cast<std::size_t>(totalLen) > avail) return res;
    res.totalLength = totalLen;
    res.baseOffset  = reader.baseOffset();
    res.outcome     = Outcome::Ignored;

    const auto attrs = static_cast<uint16_t>(reader.attributes());
    if ((attrs & static_cast<uint16_t>(kAttrControl)) != 0) [[unlikely]] {
        // Markers are recognised ONLY as real control batches (CRC-verified).  A producerId -1 control batch is a
        // checked-writes record (types 16-19): never a COMMIT or ABORT.
        if (reader.producerId() < 0) return res;
        ControlMarkerPayload payload;
        if (!ControlMarker::extract(batch, 0, payload, static_cast<std::size_t>(totalLen))) return res;
        const bool commit = (payload.controlType == ControlType::Commit);
        const int64_t firstOff = tracker_->onControlMarker(payload.producerId, res.baseOffset, commit);
        if (!commit && firstOff >= 0)
            abortedIndex_->onAbortMarker(payload.producerId, firstOff, res.baseOffset);
        res.outcome = commit ? Outcome::CommitMarker : Outcome::AbortMarker;
        return res;
    }
    // N3: only a TRANSACTIONAL batch opens a transaction.
    if ((attrs & static_cast<uint16_t>(kAttrTransactional)) != 0) {
        const int64_t producerId = reader.producerId();
        if (producerId >= 0) {
            tracker_->onProduceBatch(producerId, res.baseOffset);
            res.outcome = Outcome::TxnData;
        }
    }
    return res;
}

// ===========================================================================
// ReadCommittedView — rebuild: a loop of RcBuilder::step over Log::readView
// ===========================================================================

void ReadCommittedView::rebuild(rune::storage::Log* log, int64_t hw)
{
    assert(log != nullptr);
    tracker_->clear();
    abortedIndex_->clear();
    if (hw <= 0) return;

    // Zero-copy chunks of whole batches; the same classification step the
    // owning worker's incremental build runs (S4).
    constexpr int32_t kChunkBytes = 1 << 20;
    RcBuilder builder(tracker_, abortedIndex_);
    int64_t cursor = log->logStartOffset();
    const int64_t end = std::min(hw, log->logEndOffset());
    while (cursor < end) {
        const rune::storage::LogReadView v = log->readView(cursor, kChunkBytes);
        if (v.empty()) {
            // A gap (e.g. compaction emptied a segment): resume at the next
            // segment base above the cursor.  Cold (rebuild is not a hot path).
            int64_t next = -1;
            for (const int64_t b : log->segmentBaseOffsets())
                if (b > cursor) { next = b; break; }
            if (next < 0) break;
            cursor = next;
            continue;
        }
        std::size_t pos = 0;
        bool done = false;
        while (pos < v.len) {
            rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
            if (v.len - pos < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) ||
                r.baseOffset() >= hw) { done = true; break; }   // stop at HW
            const RcBuilder::Result res = builder.step(v.data + pos, v.len - pos);
            if (res.outcome == RcBuilder::Outcome::Invalid) { done = true; break; }
            pos += static_cast<std::size_t>(res.totalLength);
        }
        if (done || v.nextOffset <= cursor) break;
        cursor = v.nextOffset;
    }
}

// ===========================================================================
// ReadCommittedView — read
// ===========================================================================

std::vector<uint8_t> ReadCommittedView::read(rune::storage::Log* log,
                                               int64_t             hw,
                                               int64_t             fetchOffset,
                                               int32_t             maxBytes,
                                               IsolationLevel      level) const
{
    assert(log != nullptr);
    if (fetchOffset < 0) return {};

    // Determine the effective upper-offset limit.
    int64_t limit = (level == IsolationLevel::ReadCommitted)
                    ? tracker_->lso(hw)
                    : hw;

    if (fetchOffset >= limit) return {};

    // Read from the log.  The log returns raw batch bytes.
    auto raw = log->read(fetchOffset, maxBytes > 0 ? maxBytes : 1 << 20);
    if (raw.empty()) return {};

    // Walk the batch stream, collecting batches that pass the filter.
    std::vector<uint8_t> out;
    out.reserve(raw.size()); // worst case: everything passes

    std::size_t pos = 0;
    const uint8_t* data = raw.data();
    std::size_t rawSize = raw.size();
    std::size_t collected = 0;

    while (pos < rawSize) {
        std::size_t rem = rawSize - pos;
        if (rem < static_cast<std::size_t>(
                rune::protocol::batch::kRecordBatchOverhead)) break;

        // Unified little-endian reader for both data and control batches (all-LE fix).
        rune::protocol::RecordBatchReader reader(data + pos, rem);
        int32_t totalLen     = reader.totalLength();
        int64_t baseOffset   = reader.baseOffset();
        int32_t lastOffDelta = reader.lastOffsetDelta();
        int64_t producerId   = extractProducerId(data, pos, rem);

        bool ctrl = ControlMarker::isControlBatch(data, pos, rem);

        if (totalLen <= 0 || pos + static_cast<std::size_t>(totalLen) > rawSize)
            break;

        // ── Stop condition ──
        // Under read_committed: stop before any batch whose last record >= limit
        // (same NB3 logic as PartitionReplica::read()).
        // Under read_uncommitted: stop before any batch >= hw.
        int64_t lastOff = baseOffset + lastOffDelta;
        if (lastOff >= limit) break;

        // ── Filter: skip control batches (never returned to consumer) ──
        if (ctrl) {
            pos += static_cast<std::size_t>(totalLen);
            continue;
        }

        // ── Filter: skip aborted batches (read_committed only) ──
        if (level == IsolationLevel::ReadCommitted && producerId >= 0 &&
            (static_cast<uint16_t>(reader.attributes()) &
             static_cast<uint16_t>(rune::protocol::batch::kAttrTransactional)) != 0) {
            if (abortedIndex_->isAborted(producerId, baseOffset)) {
                pos += static_cast<std::size_t>(totalLen);
                continue;
            }
        }

        // ── Soft maxBytes cap: stop if we already have enough ──
        // (always include at least the first qualifying batch)
        if (collected > 0 && maxBytes > 0 &&
            static_cast<int32_t>(collected + static_cast<std::size_t>(totalLen)) > maxBytes) {
            break;
        }

        // Include this batch.
        out.insert(out.end(), data + pos, data + pos + static_cast<std::size_t>(totalLen));
        collected += static_cast<std::size_t>(totalLen);
        pos       += static_cast<std::size_t>(totalLen);
    }

    return out;
}

} // namespace rune::transaction
