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
#include <rune_protocol_internal_sbe/TxnReturn.h>
#include <rune_protocol_internal_sbe/TxnReturnKind.h>
#include <rune_protocol_internal_sbe/WorkerReturn.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::node {

// Task 13 fix round 2 (finding 7): SessionSignal rides the retired PRODUCE_NOTIFY
// value.  Follow-up: rename RESERVED_2 → SESSION_SIGNAL in rune-protocol.
static_assert(static_cast<std::uint8_t>(WorkerReturn::Type::SessionSignal) ==
                  static_cast<std::uint8_t>(rune::protocol::internal::sbe::ReturnType::RESERVED_2),
              "WorkerReturn::SessionSignal must stay on sbe ReturnType::RESERVED_2");

// Task 20: ViewRowResult rides sbe ReturnType::VIEW_ROW_RESULT (value 7).
static_assert(static_cast<std::uint8_t>(WorkerReturn::Type::ViewRowResult) ==
                  static_cast<std::uint8_t>(rune::protocol::internal::sbe::ReturnType::VIEW_ROW_RESULT),
              "WorkerReturn::ViewRowResult must stay on sbe ReturnType::VIEW_ROW_RESULT");


// ---------------------------------------------------------------------------
// WorkerReturn (template id 2)
// ---------------------------------------------------------------------------

/// Bytes one WorkerReturn occupies in a ring slot (header + block + chunk).
[[nodiscard]] inline std::size_t workerReturnEncodedLength(const WorkerReturn& ret) noexcept {
    namespace sbe = rune::protocol::internal::sbe;
    return sbe::MessageHeader::encodedLength() + sbe::WorkerReturn::computeLength(ret.chunk.size());
}

/// Bytes one WorkerReturn with a `chunkLen`-byte chunk occupies in a ring slot.
[[nodiscard]] inline std::size_t workerReturnEncodedLength(std::size_t chunkLen) noexcept {
    namespace sbe = rune::protocol::internal::sbe;
    return sbe::MessageHeader::encodedLength() + sbe::WorkerReturn::computeLength(chunkLen);
}

/// Header + fixed block of `ret` (every scalar), wrap-in-place.  The encoder is
/// left positioned at the chunk's var-data length field.
inline void encodeWorkerReturnHead(rune::protocol::internal::sbe::WorkerReturn& enc, char* buf,
                                   std::size_t n, const WorkerReturn& ret) {
    namespace sbe = rune::protocol::internal::sbe;
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
}

/// SBE wrap-in-place encode of `ret` into [buf, buf+n).  `n` must be at least
/// workerReturnEncodedLength(ret).  No intermediate struct, no memcpy of scalars.
inline void encodeWorkerReturnSbe(char* buf, std::size_t n, const WorkerReturn& ret) {
    namespace sbe = rune::protocol::internal::sbe;
    sbe::WorkerReturn enc;
    encodeWorkerReturnHead(enc, buf, n, ret);
    enc.putChunk(reinterpret_cast<const char*>(ret.chunk.data()),
                 static_cast<std::uint32_t>(ret.chunk.size()));
}

/// One contiguous piece of a gathered chunk (a run of whole record batches in
/// a segment's read mmap).
struct ChunkRun {
    const std::uint8_t* data = nullptr;
    std::uint32_t       len  = 0;
};

/// F5 (hot-path B): encode `ret` with a chunk GATHERED from `runs` (total
/// `total` bytes) straight into the claimed slot [buf, buf+n) — the ONLY copy
/// of the bytes between the log's read mmap and the consumer thread (no
/// intermediate vector).  `ret.chunk` is ignored.  `n` must be at least
/// workerReturnEncodedLength(total).
inline void encodeWorkerReturnGather(char* buf, std::size_t n, const WorkerReturn& ret,
                                     const ChunkRun* runs, std::size_t nRuns, std::uint32_t total) {
    namespace sbe = rune::protocol::internal::sbe;
    sbe::WorkerReturn enc;
    encodeWorkerReturnHead(enc, buf, n, ret);
    const std::uint64_t lenPos = enc.sbePosition();
    const std::uint32_t lenLe  = SBE_LITTLE_ENDIAN_ENCODE_32(total);
    std::memcpy(buf + lenPos, &lenLe, sizeof lenLe);
    std::uint64_t pos = lenPos + sizeof lenLe;
    for (std::size_t i = 0; i < nRuns; ++i) {
        std::memcpy(buf + pos, runs[i].data, runs[i].len);
        pos += runs[i].len;
    }
    enc.sbePosition(pos);
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

/// Bytes one TxnReturn occupies in a ring slot (header + fixed block).
[[nodiscard]] constexpr std::size_t txnReturnEncodedLength() noexcept {
    namespace sbe = rune::protocol::internal::sbe;
    return sbe::MessageHeader::encodedLength() + sbe::TxnReturn::sbeBlockLength();
}

/// Encode `r` into [buf, buf+cap) (SBE wrap-in-place).  Returns the bytes
/// written, or 0 if `cap` is too small.
[[nodiscard]] inline std::size_t encodeTxnReturn(char* buf, std::size_t cap, const TxnReturn& r) noexcept {
    namespace sbe = rune::protocol::internal::sbe;
    const std::size_t n = txnReturnEncodedLength();
    if (buf == nullptr || cap < n) return 0;
    sbe::TxnReturn enc;
    enc.wrapAndApplyHeader(buf, 0, cap);
    enc.kind(static_cast<sbe::TxnReturnKind::Value>(static_cast<std::uint8_t>(r.kind)))
       .topicId(r.topicId)
       .partition(r.partition)
       .incarnation(r.incarnation)
       .leaderEpoch(r.leaderEpoch)
       .token(r.token)
       .errorCode(r.errorCode);
    return n;
}

/// Decode a TxnReturn frame.  Returns false if the frame is not a TxnReturn
/// (template id / schema id mismatch), carries an unknown kind, or is
/// truncated.  Never throws.
[[nodiscard]] inline bool decodeTxnReturn(const char* buf, std::size_t len, TxnReturn& out) noexcept {
    namespace sbe = rune::protocol::internal::sbe;
    const std::size_t hdrLen = sbe::MessageHeader::encodedLength();
    if (buf == nullptr || len < hdrLen) return false;
    char* frame = const_cast<char*>(buf);   // SBE flyweights take char*; decode only reads
    sbe::MessageHeader mh;
    mh.wrap(frame, 0, sbe::TxnReturn::sbeSchemaVersion(), len);
    if (mh.templateId() != sbe::TxnReturn::sbeTemplateId() ||
        mh.schemaId() != sbe::TxnReturn::sbeSchemaId()) return false;
    if (len < hdrLen + mh.blockLength() || mh.blockLength() < sbe::TxnReturn::sbeBlockLength()) return false;
    sbe::TxnReturn dec;
    dec.wrapForDecode(frame, hdrLen, mh.blockLength(), mh.version(), len);
    const std::uint8_t kind = dec.kindRaw();
    if (kind != static_cast<std::uint8_t>(TxnReturn::Kind::ControlRecordCommitted) &&
        kind != static_cast<std::uint8_t>(TxnReturn::Kind::TxnHanging)) return false;
    out.kind        = static_cast<TxnReturn::Kind>(kind);
    out.topicId     = dec.topicId();
    out.partition   = dec.partition();
    out.incarnation = dec.incarnation();
    out.leaderEpoch = dec.leaderEpoch();
    out.token       = dec.token();
    out.errorCode   = dec.errorCode();
    return true;
}

static_assert(kTxnReturnTemplateId == rune::protocol::internal::sbe::TxnReturn::sbeTemplateId(),
              "TxnReturn template id (design note §7)");

} // namespace rune::node
