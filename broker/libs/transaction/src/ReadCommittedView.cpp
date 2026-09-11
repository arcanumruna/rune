// ---------------------------------------------------------------------------
// ReadCommittedView.cpp — LSO tracking + aborted-txn index + read_committed
//                          filtering. (P5-4)
//
// Namespace: rune::transaction
// C++23. NO locks. NO busy-spin. NO hot-path alloc beyond scan buffers.
// Single-writer.
// ---------------------------------------------------------------------------

#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/ControlMarker.h>

#include <rune/storage/Log.h>
// rune-protocol RecordBatch: big-endian field accessors.
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>

namespace rune::transaction {

// ===========================================================================
// Helpers
// ===========================================================================

namespace {

// Extract producerId from a raw batch buffer at position `pos` within `buf`.
// The buffer may contain:
//   (a) a rune-protocol RecordBatch (big-endian, magic=2, CONTROL bit clear)
//   (b) a ControlMarker batch (little-endian, magic=2, CONTROL bit set)
//
// We detect (b) via ControlMarker::isControlBatch() and use ControlMarker::extract().
// For (a) we read big-endian at the kProducerIdOffset.
//
// Returns -1 if extraction fails or the batch is malformed.
inline int64_t extractProducerId(const uint8_t* buf, std::size_t pos,
                                  std::size_t avail) noexcept
{
    if (avail < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
        return -1;

    if (ControlMarker::isControlBatch(buf, pos, avail)) {
        ControlMarkerPayload payload;
        if (!ControlMarker::extract(buf, pos, payload, avail)) return -1;
        return payload.producerId;
    }

    // rune-protocol big-endian: producerId at kProducerIdOffset (43..50).
    using namespace rune::protocol;
    if (avail < static_cast<std::size_t>(batch::kProducerIdOffset + 8)) return -1;
    const uint8_t* p = buf + pos + batch::kProducerIdOffset;
    int64_t pid = 0;
    for (int i = 0; i < 8; ++i)
        pid = (pid << 8) | static_cast<uint8_t>(p[i]);
    return pid;
}

// Walk the batch stream in `raw` and invoke `fn` for each batch.
// fn(const uint8_t* batchStart, std::size_t batchBytes, int64_t baseOffset,
//    int32_t lastOffsetDelta, bool isControl, int64_t producerId)
// Returns false from fn to stop early.
template<typename Fn>
static void walkBatches(const uint8_t* raw, std::size_t rawSize, Fn&& fn)
{
    std::size_t pos = 0;
    while (pos < rawSize) {
        std::size_t rem = rawSize - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
            break;

        bool ctrl = ControlMarker::isControlBatch(raw, pos, rem);

        // Read batch length.
        int32_t totalLen = 0;
        if (ctrl) {
            // ControlMarker: fixed size.
            totalLen = static_cast<int32_t>(ControlMarker::ENCODED_SIZE);
        } else {
            // rune-protocol big-endian: length at [8..11] = batchLengthBytes, total = +12.
            using namespace rune::protocol;
            rune::protocol::RecordBatchReader reader(raw + pos, rem);
            totalLen = reader.totalLength();
        }

        if (totalLen <= 0 || pos + static_cast<std::size_t>(totalLen) > rawSize)
            break;

        // Extract baseOffset.
        int64_t baseOffset = 0;
        int32_t lastOffDelta = 0;
        if (ctrl) {
            // ControlMarker: baseOffset at [0..7] little-endian int64.
            std::memcpy(&baseOffset, raw + pos, 8);
            // lastOffsetDelta at [23..26] little-endian int32 — always 0 for control.
            lastOffDelta = 0;
        } else {
            using namespace rune::protocol;
            rune::protocol::RecordBatchReader reader(raw + pos, rem);
            baseOffset   = reader.baseOffset();
            lastOffDelta = reader.lastOffsetDelta();
        }

        int64_t pid = extractProducerId(raw, pos, rem);

        bool cont = fn(raw + pos, static_cast<std::size_t>(totalLen),
                       baseOffset, lastOffDelta, ctrl, pid);
        if (!cont) break;

        pos += static_cast<std::size_t>(totalLen);
    }
}

} // anonymous namespace

// ===========================================================================
// LsoTracker
// ===========================================================================

void LsoTracker::onProduceBatch(ProducerId producerId,
                                 int64_t    batchBaseOffset) noexcept
{
    if (producerId < 0) return; // non-transactional batch; no effect on LSO

    auto it = openTxns_.find(producerId);
    if (it == openTxns_.end()) {
        // First data batch for this producer: open a new transaction.
        openTxns_[producerId]           = batchBaseOffset;
        byFirstOffset_[batchBaseOffset] = producerId;
    }
    // If already tracked, the firstOffset stays (the earlier write).
}

int64_t LsoTracker::onControlMarker(ProducerId producerId,
                                     int64_t    /*markerBaseOffset*/,
                                     bool       /*commit*/) noexcept
{
    auto it = openTxns_.find(producerId);
    if (it == openTxns_.end()) return -1; // not tracked

    int64_t firstOff = it->second;
    openTxns_.erase(it);

    // Remove from byFirstOffset_.
    auto fit = byFirstOffset_.find(firstOff);
    if (fit != byFirstOffset_.end() && fit->second == producerId) {
        byFirstOffset_.erase(fit);
    }

    return firstOff;
}

int64_t LsoTracker::lso(int64_t hw) const noexcept
{
    if (openTxns_.empty()) {
        return hw; // no open transactions → LSO == HW
    }
    // LSO = min firstOffset over all open txns.
    assert(!byFirstOffset_.empty());
    int64_t minFirst = byFirstOffset_.begin()->first;
    // Clamp to [0, hw].
    if (minFirst > hw) return hw;
    if (minFirst < 0)  return 0;
    return minFirst;
}

int64_t LsoTracker::firstOffsetOf(ProducerId producerId) const noexcept
{
    auto it = openTxns_.find(producerId);
    return (it != openTxns_.end()) ? it->second : -1;
}

// ===========================================================================
// AbortedTxnIndex
// ===========================================================================

void AbortedTxnIndex::onAbortMarker(ProducerId producerId,
                                     int64_t    firstDataOffset,
                                     int64_t    abortMarkerOffset) noexcept
{
    // Keep sorted by firstOffset ascending for consistent ordering.
    AbortedTxnRange range{producerId, firstDataOffset, abortMarkerOffset};
    auto it = std::lower_bound(ranges_.begin(), ranges_.end(), range,
                                [](const AbortedTxnRange& a, const AbortedTxnRange& b) {
                                    return a.firstOffset < b.firstOffset;
                                });
    ranges_.insert(it, range);
}

bool AbortedTxnIndex::isAborted(ProducerId producerId,
                                  int64_t    batchBaseOffset) const noexcept
{
    for (const auto& r : ranges_) {
        if (r.producerId == producerId && r.contains(batchBaseOffset)) return true;
    }
    return false;
}

// ===========================================================================
// ReadCommittedView — rebuild
// ===========================================================================

void ReadCommittedView::rebuild(rune::storage::Log* log, int64_t hw)
{
    assert(log != nullptr);
    tracker_->clear();
    abortedIndex_->clear();

    if (hw <= 0) return;

    // Read all bytes up to hw in one shot (or in large chunks).
    // We use a large maxBytes to get the full range; the HW clamping is done
    // during the batch walk below.
    constexpr int32_t kMaxScanBytes = std::numeric_limits<int32_t>::max() / 2;
    auto raw = log->read(0, kMaxScanBytes);
    if (raw.empty()) return;

    // Single-pass scan: walk batches, handling both data and control batches
    // correctly.  A previous two-pass approach (dead first pass whose results
    // were discarded by a clear()) doubled rebuild I/O and was removed (P5-4-C1).
    {
        std::size_t pos = 0;
        const uint8_t* data = raw.data();
        std::size_t rawSize = raw.size();

        while (pos < rawSize) {
            std::size_t rem = rawSize - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;

            bool ctrl = ControlMarker::isControlBatch(data, pos, rem);

            int32_t totalLen = 0;
            int64_t baseOffset = 0;
            int64_t producerId = -1;

            if (ctrl) {
                totalLen = static_cast<int32_t>(ControlMarker::ENCODED_SIZE);
                // LE baseOffset at [0..7].
                std::memcpy(&baseOffset, data + pos, 8);
                ControlMarkerPayload payload;
                if (ControlMarker::extract(data, pos, payload, rem)) {
                    producerId = payload.producerId;

                    if (baseOffset < hw) {
                        bool commit = (payload.controlType == ControlType::Commit);
                        int64_t firstOff = tracker_->onControlMarker(producerId,
                                                                       baseOffset,
                                                                       commit);
                        if (!commit && firstOff >= 0) {
                            abortedIndex_->onAbortMarker(producerId,
                                                          firstOff,
                                                          baseOffset);
                        }
                    }
                }
            } else {
                rune::protocol::RecordBatchReader reader(data + pos, rem);
                totalLen   = reader.totalLength();
                baseOffset = reader.baseOffset();
                producerId = extractProducerId(data, pos, rem);

                if (baseOffset < hw && producerId >= 0) {
                    tracker_->onProduceBatch(producerId, baseOffset);
                }
            }

            if (totalLen <= 0 || pos + static_cast<std::size_t>(totalLen) > rawSize)
                break;

            // Stop scanning beyond HW.
            if (baseOffset >= hw) break;

            pos += static_cast<std::size_t>(totalLen);
        }
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

        bool ctrl = ControlMarker::isControlBatch(data, pos, rem);

        int32_t totalLen = 0;
        int64_t baseOffset = 0;
        int32_t lastOffDelta = 0;
        int64_t producerId = -1;

        if (ctrl) {
            totalLen     = static_cast<int32_t>(ControlMarker::ENCODED_SIZE);
            std::memcpy(&baseOffset, data + pos, 8); // LE
            lastOffDelta = 0;
            ControlMarkerPayload payload;
            if (ControlMarker::extract(data, pos, payload, rem)) {
                producerId = payload.producerId;
            }
        } else {
            rune::protocol::RecordBatchReader reader(data + pos, rem);
            totalLen     = reader.totalLength();
            baseOffset   = reader.baseOffset();
            lastOffDelta = reader.lastOffsetDelta();
            producerId   = extractProducerId(data, pos, rem);
        }

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
        if (level == IsolationLevel::ReadCommitted && producerId >= 0) {
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
