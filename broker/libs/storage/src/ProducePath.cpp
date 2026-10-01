// ProducePath.cpp — Shared produce-batch helper (E5.1.2b, moved to rune_storage E5.1.2d).
//
// Contains the core produce-batch logic extracted from
// ClientGateway::handleProduce (inline path) so that BOTH the gateway and the
// per-worker ingress (E5.1.2d) can call the same implementation and share
// idempotency / CRC / restamp / durability semantics.
//
// Moved from libs/gateway/src/ to libs/storage/src/ (E5.1.2d) to eliminate the
// rune_node ↔ rune_gateway circular-dep workaround.  Both rune_gateway and
// rune_node depend on rune_storage, so this is a true leaf location.
//
// See ProducePath.h for the full contract.

#include <rune/storage/ProducePath.h>
#include <rune/protocol/RecordBatch.h>

#include <cstring>
#include <spdlog/spdlog.h>

namespace rune::storage {

namespace pc  = rune::protocol::client;
namespace rb  = rune::protocol::batch;

// ---------------------------------------------------------------------------
// handleProduceBatch — implementation.
// ---------------------------------------------------------------------------
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
    std::size_t                       batchLen,
    int32_t                           topicId,
    int32_t                           partition,
    int64_t                           producerId,
    int16_t                           producerEpoch,
    int32_t                           baseSequence,
    rune::storage::Log&               log,
    rune::storage::ProducerStateManager& producerState,
    bool                              acksAll,
    int                               isrSize,
    int                               minIsr,
    uint8_t                           durabilityTier) noexcept
{
    ProduceOutcome out;

    // -------------------------------------------------------------------------
    // 0. fsyncgate fail-stop: a log whose fsync ever failed refuses EVERY
    //    produce — including idempotent duplicates (checked before the dedup
    //    gate, so a retry of a batch whose durability failed is never acked OK).
    // -------------------------------------------------------------------------
    if (log.storageFailed()) [[unlikely]] {
        out.error = pc::ErrorCode::StorageError;
        return out;
    }

    // -------------------------------------------------------------------------
    // 1. Batch size / header sanity guards.
    //    (Caller already checked batchLen > 0; we re-check here for safety.)
    // -------------------------------------------------------------------------
    if (batchLen == 0) {
        spdlog::debug("ProducePath: empty recordBatch (batchLen=0)");
        out.error = pc::ErrorCode::Internal;
        return out;
    }

    // Minimum: we need to read fields up to kRecordCountOffset+4 bytes.
    constexpr std::size_t kMinBatchSize =
        static_cast<std::size_t>(rb::kRecordCountOffset) + 4u;
    if (batchLen < kMinBatchSize) {
        spdlog::debug("ProducePath: batch too short ({} < {}); rejected", batchLen, kMinBatchSize);
        out.error = pc::ErrorCode::Internal;
        return out;
    }

    // -------------------------------------------------------------------------
    // 2. Base-offset assignment is DEFERRED to the append (step 7).
    //    `batch` is READ-ONLY here — the base offset is patched zero-copy into
    //    the page cache by Log::appendNoSyncRestamp (pwritev), so this helper no
    //    longer needs a mutable buffer / restamp scratch.  The base-offset field
    //    [0..7] precedes the CRC region (kAttributesOffset=21), so the restamp is
    //    CRC-safe.  `base` is filled in by the append below.
    // -------------------------------------------------------------------------
    int64_t base = 0;

    // -------------------------------------------------------------------------
    // 3. Parse lastOffsetDelta (LE32 at kLastOffsetDeltaOffset=23) and
    //    recordCount (LE32 at kRecordCountOffset=57), then cross-validate.
    //
    //    Use lastOffsetDelta as the canonical source (CRC-covered) to derive
    //    lastOffset; recordCount is cross-validated and must equal delta+1.
    // -------------------------------------------------------------------------
    int32_t lastOffsetDelta = 0;
    {
        const uint8_t* p = batch + rb::kLastOffsetDeltaOffset;
        lastOffsetDelta = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) |
            (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) |
            (static_cast<uint32_t>(p[3]) << 24));
    }

    int32_t recordCount = 0;
    {
        const uint8_t* p = batch + rb::kRecordCountOffset;
        recordCount = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) |
            (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) |
            (static_cast<uint32_t>(p[3]) << 24));
    }

    if (lastOffsetDelta < 0) {
        spdlog::debug("ProducePath: lastOffsetDelta={} < 0; rejected", lastOffsetDelta);
        out.error = pc::ErrorCode::Internal;
        return out;
    }

    if (recordCount != lastOffsetDelta + 1) {
        spdlog::warn("ProducePath: recordCount={} != lastOffsetDelta+1={}; "
                     "batch inconsistent → rejected",
                     recordCount, lastOffsetDelta + 1);
        out.error = pc::ErrorCode::Internal;
        return out;
    }

    // -------------------------------------------------------------------------
    // 4. CRC validation — reject corrupt batches before they touch the log.
    //    RecordBatchReader::ensureValid() validates the CRC32C over the batch.
    //    The restamp above did NOT invalidate the CRC (base-offset precedes the
    //    CRC-covered region that starts at kAttributesOffset=21).
    // -------------------------------------------------------------------------
    {
        rune::protocol::RecordBatchReader reader(batch, batchLen);
        if (!reader.ensureValid()) {
            spdlog::warn("ProducePath: CRC mismatch (corrupt batch) → rejected");
            out.error = pc::ErrorCode::Internal;
            return out;
        }
    }

    // lastOffset is derived after the append assigns the base offset (step 7).
    int64_t lastOffset = 0;

    // Read batchAttrs (LE16 at kAttributesOffset) for caller LSO tracking.
    uint16_t batchAttrs = 0;
    constexpr std::size_t kAttrOffset = static_cast<std::size_t>(rb::kAttributesOffset);
    if (batchLen >= kAttrOffset + 2u) {
        batchAttrs = static_cast<uint16_t>(
            static_cast<uint16_t>(batch[kAttrOffset]) |
            (static_cast<uint16_t>(batch[kAttrOffset + 1u]) << 8));
    }

    // -------------------------------------------------------------------------
    // 5. Idempotent dedup gate (TX-2).
    //    Only when producerId >= 0 (producerId==-1 = legacy path, skip PSM).
    //    Mutates producerState only on the Accept path (after append).
    // -------------------------------------------------------------------------
    if (producerId >= 0) {
        const int32_t lastSeqInBatch = baseSequence + recordCount - 1;
        const auto vr = producerState.validate(
            topicId, producerId, partition,
            producerEpoch, baseSequence, lastSeqInBatch);

        if (vr.isDuplicate()) {
            spdlog::debug("ProducePath: duplicate (pid={} epoch={} baseSeq={} "
                          "partition={}) → prior baseOffset={} (no re-append)",
                          producerId, producerEpoch, baseSequence,
                          partition, vr.retryBaseOffset);
            out.error       = pc::ErrorCode::OK;
            out.isDuplicate = true;
            out.baseOffset  = vr.retryBaseOffset;
            out.recordCount = recordCount;
            out.batchAttrs  = batchAttrs;
            return out;
        }
        if (vr.isOos()) {
            spdlog::debug("ProducePath: OutOfOrderSequence "
                          "(pid={} epoch={} baseSeq={} partition={})",
                          producerId, producerEpoch, baseSequence, partition);
            out.error = pc::ErrorCode::OutOfOrderSequence;
            return out;
        }
        if (vr.isFenced()) {
            spdlog::debug("ProducePath: InvalidProducerEpoch "
                          "(pid={} epoch={} partition={})",
                          producerId, producerEpoch, partition);
            out.error = pc::ErrorCode::InvalidProducerEpoch;
            return out;
        }
        // Status::Accept — fall through to min-ISR gate and append.
    }

    // -------------------------------------------------------------------------
    // 6. Min-ISR gate for acks=All (GR-5).
    //    Checked BEFORE appendNoSync so the log is never advanced if rejected.
    //    passthrough (isrSize=0 / no replica): no ISR check — single-node / test.
    // -------------------------------------------------------------------------
    if (acksAll && isrSize > 0 && isrSize < minIsr) {
        spdlog::debug("ProducePath: acks=All rejected (ISR size={} < minIsr={}) "
                      "topicId={} partition={}",
                      isrSize, minIsr, topicId, partition);
        out.error = pc::ErrorCode::MinIsrViolation;
        return out;
    }

    // -------------------------------------------------------------------------
    // 7. Append — appendNoSyncRestamp (zero-copy page-cache write).
    //    Assigns the base offset from the log's current end offset and patches it
    //    into bytes [0..7] via pwritev WITHOUT mutating `batch` (no scratch copy).
    //    On success: log's LEO advances past lastOffset.
    // -------------------------------------------------------------------------
    try {
        const int64_t appended =
            log.appendNoSyncRestamp(batch, batchLen, lastOffsetDelta, base);
        if (appended < 0) {
            // Returns negative on back-pressure (e.g. InMemoryLog full).
            spdlog::warn("ProducePath: appendNoSyncRestamp refused (back-pressure) "
                         "topicId={} partition={}", topicId, partition);
            out.error = pc::ErrorCode::Internal;
            return out;
        }
        lastOffset = base + static_cast<int64_t>(lastOffsetDelta);
    } catch (const std::exception& ex) {
        spdlog::error("ProducePath: appendNoSyncRestamp threw: {}; Internal", ex.what());
        out.error = pc::ErrorCode::Internal;
        return out;
    } catch (...) {
        spdlog::error("ProducePath: appendNoSyncRestamp threw unknown exception; Internal");
        out.error = pc::ErrorCode::Internal;
        return out;
    }

    // -------------------------------------------------------------------------
    // 8. PSM update — called AFTER successful append, only on Accept path.
    //    Records the batch in the retry window so a duplicate retry returns the
    //    original base offset.
    // -------------------------------------------------------------------------
    if (producerId >= 0) {
        const int32_t lastSeqInBatch = baseSequence + recordCount - 1;
        producerState.update(
            topicId, producerId, partition,
            producerEpoch, baseSequence, lastSeqInBatch,
            base);
    }

    // -------------------------------------------------------------------------
    // 9. Durability: Strict tier (2) — synchronous sync() on the inline path.
    //    Only reached when durabilityTier==2 (usually this helper is called
    //    from the no-WorkerPool inline path; the WorkerPool path has its own
    //    async IoBackend and never reaches here).
    //    On any other tier the caller owns group-commit via flushDirtyLogs.
    // -------------------------------------------------------------------------
    if (durabilityTier == 2) {
        try {
            log.sync();
            out.syncDone = true;
        } catch (const std::exception& ex) {
            spdlog::error("ProducePath: Strict-tier sync() failed: {}; StorageError (log failed)", ex.what());
            out.error = pc::ErrorCode::StorageError;
            return out;
        } catch (...) {
            spdlog::error("ProducePath: Strict-tier sync() threw unknown; StorageError");
            log.markStorageFailed(5 /*EIO*/);
            out.error = pc::ErrorCode::StorageError;
            return out;
        }
    }

    // -------------------------------------------------------------------------
    // 10. Success — populate outcome fields.
    // -------------------------------------------------------------------------
    out.error      = pc::ErrorCode::OK;
    out.isDuplicate = false;
    out.baseOffset  = base;
    out.lastOffset  = lastOffset;
    out.recordCount = recordCount;
    out.batchAttrs  = batchAttrs;
    return out;
}

} // namespace rune::storage
