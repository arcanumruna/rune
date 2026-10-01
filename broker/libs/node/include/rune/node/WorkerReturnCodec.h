#pragma once
// WorkerReturnCodec.h — SBE wrap-in-place codecs for the worker → control
// return ring (ret_).  F15 S1 (design note 2026-10-01-f15-eos-marker-lso-design.md §7).
//
// Two templates share ret_ and are discriminated by the SBE template id:
//   - WorkerReturn (id 2): ProduceResult / PushReadResult / ... (unchanged layout;
//     the ProduceResult encode cost is pinned by BM_ProduceResultEncode and
//     ProtocolRegression.WorkerReturnLayoutUnchanged).
//   - TxnReturn (id 6): small fixed-block transactional returns
//     (CONTROL_RECORD_COMMITTED; TXN_HANGING reserved for F15-FU1).
//
// Header-only, allocation-free, no virtual calls: every function encodes or
// decodes directly in the caller's buffer (the claimed ring slot).

#include <rune/node/WorkerOp.h>

#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/ReturnType.h>
#include <rune_protocol_internal_sbe/WorkerReturn.h>

#include <cstddef>
#include <cstdint>

namespace rune::node {

// ---------------------------------------------------------------------------
// WorkerReturn (template id 2)
// ---------------------------------------------------------------------------

/// Bytes one WorkerReturn occupies in a ring slot (header + block + chunk).
[[nodiscard]] inline std::size_t workerReturnEncodedLength(const WorkerReturn& ret) noexcept {
    namespace sbe = rune::protocol::internal::sbe;
    return sbe::MessageHeader::encodedLength() + sbe::WorkerReturn::computeLength(ret.chunk.size());
}

/// SBE wrap-in-place encode of `ret` into [buf, buf+n).  `n` must be at least
/// workerReturnEncodedLength(ret).  No intermediate struct, no memcpy of scalars.
inline void encodeWorkerReturnSbe(char* buf, std::size_t n, const WorkerReturn& ret) {
    namespace sbe = rune::protocol::internal::sbe;
    sbe::WorkerReturn enc;
    enc.wrapAndApplyHeader(buf, 0, n);
    enc.returnType(static_cast<sbe::ReturnType::Value>(static_cast<std::uint8_t>(ret.type)))
       .sessionId(ret.sessionId)
       .correlationId(ret.correlationId)
       .partition(ret.partition)
       .baseOffset(ret.baseOffset)
       .lastOffset(ret.lastOffset)
       .errorCode(ret.errorCode)
       .deferAck(ret.deferAck ? 1 : 0)
       .replica(reinterpret_cast<std::uint64_t>(ret.replica))
       .producerId(ret.producerId)
       .producerEpoch(ret.producerEpoch)
       .baseSequence(ret.baseSequence)
       .recordCount(ret.recordCount)
       .topicId(ret.topicId)
       .batchAttrs(ret.batchAttrs)
       .enqueuedAtNs(ret.enqueuedAtNs)
       .subscriptionId(ret.subscriptionId)
       .readTopicId(ret.readTopicId)
       .readPartition(ret.readPartition)
       .newNextOffset(ret.newNextOffset)
       .hwBound(ret.hwBound)
       .isolation(ret.isolation)
       .isGroup(ret.isGroup ? 1 : 0)
       .groupCarrierReceived(ret.groupCarrierReceived ? 1 : 0)
       .targetReplicaId(ret.targetReplicaId)
       .incarnation(ret.incarnation);
    enc.putChunk(reinterpret_cast<const char*>(ret.chunk.data()),
                 static_cast<std::uint32_t>(ret.chunk.size()));
}

// ---------------------------------------------------------------------------
// TxnReturn (template id 6)
// ---------------------------------------------------------------------------

/// The ret_ template id of TxnReturn (1-5 are taken; 4 is FlushCompletion).
inline constexpr std::uint16_t kTxnReturnTemplateId = 6;

struct TxnReturn {
    enum class Kind : std::uint8_t {
        ControlRecordCommitted = 1,   ///< the marker at `token` is committed (HW > its offset), or errorCode
        TxnHanging             = 2,   ///< reserved for F15-FU1 (never emitted yet)
    };
    Kind         kind        = Kind::ControlRecordCommitted;
    std::int32_t topicId     = 0;
    std::int32_t partition   = 0;
    std::uint32_t incarnation = 0;
    std::int32_t leaderEpoch = 0;
    std::int64_t token       = 0;
    std::int16_t errorCode   = 0;
};

/// Encode `r` into [buf, buf+cap).  Returns the bytes written, or 0 if `cap`
/// is too small.
[[nodiscard]] inline std::size_t encodeTxnReturn(char* /*buf*/, std::size_t /*cap*/,
                                                 const TxnReturn& /*r*/) noexcept {
    return 0;   // F15 S1 RED stub: TxnReturn not in the schema yet
}

/// Decode a TxnReturn frame.  Returns false if the frame is not a TxnReturn
/// (template id / schema id mismatch) or is truncated.
[[nodiscard]] inline bool decodeTxnReturn(const char* /*buf*/, std::size_t /*len*/,
                                          TxnReturn& /*out*/) noexcept {
    return false;   // F15 S1 RED stub
}

} // namespace rune::node
