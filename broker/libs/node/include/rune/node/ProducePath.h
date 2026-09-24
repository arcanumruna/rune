#pragma once
// ProducePath.h — Shared produce-batch helper (E5.1.2b).
//
// Extracts the core produce-batch logic from ClientGateway::handleProduce
// (inline path) into a single function callable by both the gateway (legacy
// control-thread path) and the per-worker ingress (E5.1.2d, later task).
//
// Contract:
//   handleProduceBatch — given an ALREADY RESOLVED (log, producerState,
//   batch bytes) produce request, performs:
//     1. Batch size / header sanity guards.
//     2. Base-offset restamp IN PLACE in `batch` (CRC-safe — base-offset field
//        at [0..7] is before the CRC region that starts at kAttributesOffset=21).
//     3. lastOffsetDelta / recordCount parse & consistency check.
//     4. CRC validation (RecordBatchReader::ensureValid).
//     5. Idempotent dedup via `producerState`:
//          Duplicate     → return prior baseOffset, NO append.
//          OOS           → return OutOfOrderSequence, NO append.
//          Fenced        → return InvalidProducerEpoch, NO append.
//          Accept        → fall through.
//     6. Min-ISR gate for acks=All:
//          isrSize < minIsr → return MinIsrViolation, NO append.
//     7. appendNoSync(batch, batchLen, lastOffset) — page-cache write.
//     8. PSM update (on Accept path only).
//     9. On durabilityTier==2 (Strict, inline path): synchronous log->sync();
//        on tier!=2: no sync here (caller group-commits via flushDirtyLogs).
//    10. Returns ProduceOutcome with assigned baseOffset, lastOffset, and
//        batchAttrs (for caller LSO tracking), errorCode=None on success.
//
// What is NOT in this helper (caller responsibilities):
//   - Aeron fragment decode.
//   - Session lookup + leadership / routing-image / redirect checks.
//   - Topic registry + log lookup.
//   - Replica role check (per-partition NotLeader defence).
//   - Bootstrap-window acks=All rejection (uses gateway state committedMetadataReady_).
//   - replica->onLeaderLogAppended() (caller does this after success).
//   - Dirty-log tracking / deferred-ack queue management.
//   - TX-4 LSO tracking (getTxnState) — caller does this using batchAttrs from outcome.
//   - WorkerPool routing path (stays in gateway).
//   - Egress send of ProduceResponse (caller owns egress).
//
// Parameterized on ProducerStateManager& (caller passes its own).
// No virtual / std::function on the hot path.
// No allocation on the success path (batch is caller-owned mutable buffer).
// Little-endian throughout (kBaseOffsetOffset, kAttributesOffset, etc. are LE).
//
// Lives in libs/node so both libs/gateway (depends on node via RUNE_NODE_INCLUDE)
// and future Worker ingress (also in node) can include it without circular deps.
// libs/gateway DOES NOT link rune_node; ProducePath.cpp is compiled into rune_node.
//
// Single-thread: no locks, no atomics. Caller must ensure single-writer per
// (log, producerState) pair.

#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/client/Protocol.h>

#include <cstddef>
#include <cstdint>

namespace rune::node {

// ---------------------------------------------------------------------------
// ProduceOutcome — result of handleProduceBatch.
//
// On success (error == ErrorCode::None / OK):
//   baseOffset    — first log offset assigned to this batch.
//   lastOffset    — last  log offset of this batch (= base + lastOffsetDelta).
//   recordCount   — number of records in the batch (for PSM / ack).
//   batchAttrs    — LE16 attributes field from the batch header (for LSO tracking).
//   syncDone      — true when durabilityTier==2 and a synchronous log->sync()
//                   succeeded (caller must NOT add log to dirtyLogs_).
//
// On duplicate (error == OK with isDuplicate flag):
//   baseOffset    — the PRIOR base offset (the original append; do NOT re-ack a
//                   new offset — return this value to the client).
//   isDuplicate   — true; caller must NOT re-append or update PSM.
//
// On any other error (!isOK()):
//   baseOffset == 0, lastOffset == 0.
// ---------------------------------------------------------------------------
struct ProduceOutcome {
    rune::protocol::client::ErrorCode error = rune::protocol::client::ErrorCode::OK;
    int64_t  baseOffset   = 0;
    int64_t  lastOffset   = 0;
    int32_t  recordCount  = 0;
    uint16_t batchAttrs   = 0;   ///< LE16 attributes (bit4 = TRANSACTIONAL) for LSO tracking
    bool     isDuplicate  = false; ///< true → duplicate dedup; baseOffset = prior offset
    bool     syncDone     = false; ///< true → strict-tier inline sync() already completed

    [[nodiscard]] bool isOK() const noexcept {
        return error == rune::protocol::client::ErrorCode::OK;
    }
};

// ---------------------------------------------------------------------------
// handleProduceBatch — the shared produce-batch core.
//
// @param batch         Mutable batch buffer (base-offset is restamped in place).
// @param batchLen      Number of bytes in `batch`.
// @param topicId       Topic identifier (for PSM key).
// @param partition     Partition number (for PSM key + log).
// @param producerId    Idempotent producer id (-1 = legacy / skip PSM).
// @param producerEpoch Epoch for PSM validation.
// @param baseSequence  First sequence in this batch (PSM check).
// @param log           The partition log to append to.
// @param producerState Caller-owned ProducerStateManager (mutable).
// @param acksAll       true when the produce carries acks=All (ISR-majority gate).
// @param isrSize       Current ISR size for this partition (0 if no replica wired).
// @param minIsr        Minimum ISR required for acks=All durability.
// @param durabilityTier 1 = Majority (appendNoSync + group-commit); 2 = Strict (sync).
//
// Returns a ProduceOutcome; caller handles egress, LSO tracking, dirty-log tracking,
// onLeaderLogAppended(), and deferred-ack queue.
//
// Never throws; all exceptions are caught and returned as ErrorCode::Internal.
// Zero allocation on the success path.
// ---------------------------------------------------------------------------
ProduceOutcome handleProduceBatch(
    uint8_t*                          batch,
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
    uint8_t                           durabilityTier) noexcept;

} // namespace rune::node
