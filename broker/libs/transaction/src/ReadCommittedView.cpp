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

    // Single-pass scan using the unified little-endian RecordBatchReader for ALL batches.
    // all-LE fix: control batches share the LE record-batch format, so RecordBatchReader
    // can walk them. Control batches are identified by ControlMarker::isControlBatch()
    // which reads the CONTROL attribute bit from the LE attributes field [21..22].
    {
        std::size_t pos = 0;
        const uint8_t* data = raw.data();
        std::size_t rawSize = raw.size();

        while (pos < rawSize) {
            std::size_t rem = rawSize - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;

            // Use RecordBatchReader for ALL batches (data + control both little-endian now).
            rune::protocol::RecordBatchReader reader(data + pos, rem);
            int32_t totalLen   = reader.totalLength();
            int64_t baseOffset = reader.baseOffset();

            if (totalLen <= 0 || pos + static_cast<std::size_t>(totalLen) > rawSize)
                break;

            // Stop scanning beyond HW.
            if (baseOffset >= hw) break;

            bool ctrl = ControlMarker::isControlBatch(data, pos, rem);
            int64_t producerId = extractProducerId(data, pos, rem);

            if (ctrl) {
                // Control batch: decode the marker payload and update LSO/aborted index.
                ControlMarkerPayload payload;
                if (ControlMarker::extract(data, pos, payload, rem)) {
                    bool commit = (payload.controlType == ControlType::Commit);
                    int64_t firstOff = tracker_->onControlMarker(payload.producerId,
                                                                   baseOffset,
                                                                   commit);
                    if (!commit && firstOff >= 0) {
                        abortedIndex_->onAbortMarker(payload.producerId,
                                                      firstOff,
                                                      baseOffset);
                    }
                }
            } else {
                // Data batch: track open transactions.
                if (producerId >= 0) {
                    tracker_->onProduceBatch(producerId, baseOffset);
                }
            }

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
