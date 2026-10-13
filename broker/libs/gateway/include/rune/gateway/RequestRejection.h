#pragma once
// ---------------------------------------------------------------------------
// RequestRejection.h — no-hang wave (R2): the typed error answer for a client
// request that cannot be served.
//
// Rule: a request whose MessageHeader decodes, on a known session, ALWAYS gets
// an answer — never a silent drop that leaves the client waiting out its
// timeout.  When the body does not decode (or the broker cannot accept the
// request: service not wired, admin ring full, worker ring full), the request
// is answered with ITS OWN response type carrying an error, on the header's
// correlationId, so the client's pending op completes with that error.
//
// Requests without a response type (Keepalive, Disconnect, Unsubscribe — fire
// and forget) and Connect / HandshakeContinue (no session yet) have no answer
// here (encodeRejection returns 0).  Produce is answered by the gateway's own
// version-gated ProduceResponse encoder (ClientGateway::sendRespReliable_; the
// pre-D6 38-byte frame for old clients) — the ProduceResponse encoded here is
// the current 46-byte form, used by callers that have no session version.
//
// Cold path only: called after a decode / admission failure, never before the
// success path's own work.  Zero allocation: encodes into the caller's buffer.
// Shared by the gateway (rune_gateway) and the coordination services
// (rune_node), which answer their own decode failures with it.
// ---------------------------------------------------------------------------
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/TxnMessages.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::gateway {

/// Every rejection frame fits in this many bytes (the largest is a one-entry
/// OffsetFetchResponse: 24 + 2 + 18 = 44 B).  Callers pass at least this much.
inline constexpr std::size_t kMaxRejectionBytes = 64;

/// Encode the error answer to a request of `requestTid` into out[0..cap).
/// `code` is the value of the response type's own error field: an
/// rune::protocol::client::ErrorCode for every type except GetViewRow, whose
/// ViewRowResponse carries a ViewRowResponse::Status.  Responses without a
/// top-level error field (OffsetCommit / OffsetFetch / AddPartitionsToTxn /
/// TxnOffsetCommit) carry ONE result entry {topicId -1, partition -1, code}.
/// Ping and MetadataRequest (no error field in their response) are answered
/// with ErrorResponse{code}.  Returns the bytes written, or 0 when the request
/// type has no answer (see the file header) or cap < kMaxRejectionBytes.
[[nodiscard]] inline std::size_t encodeRejection(uint16_t requestTid, int64_t sessionId,
                                                 int64_t correlationId, uint16_t code,
                                                 uint8_t* out, std::size_t cap) noexcept {
    namespace pc = rune::protocol::client;
    using T = pc::TemplateId;
    if (cap < kMaxRejectionBytes) return 0;
    const auto ec = static_cast<int16_t>(code);
    switch (static_cast<T>(requestTid)) {
    case T::Produce: {
        pc::ProduceResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        r.errorCode = ec; r.partition = -1; r.baseOffset = 0; r.logStartOffset = -1;
        return r.encode(out);
    }
    case T::Subscribe: {
        pc::SubscribeResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        r.errorCode = ec; r.partition = -1; r.subscriptionId = 0;
        return r.encode(out);
    }
    case T::JoinGroup: {
        pc::JoinGroupResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = ec;
        return r.encode(out);
    }
    case T::SyncGroup: {
        pc::SyncGroupResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = ec;
        return r.encode(out, std::span<const pc::TopicPartition32>{});
    }
    case T::Heartbeat: {
        pc::HeartbeatResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = ec;
        return r.encode(out);
    }
    case T::LeaveGroup: {
        pc::LeaveGroupResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = ec;
        return r.encode(out);
    }
    case T::OffsetCommit: {
        pc::OffsetCommitResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        const pc::OffsetCommitResult one{-1, -1, ec};
        return r.encode(out, std::span<const pc::OffsetCommitResult>(&one, 1));
    }
    case T::OffsetFetch: {
        pc::OffsetFetchResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        const pc::OffsetFetchResult one{-1, -1, -1, ec};
        return r.encode(out, std::span<const pc::OffsetFetchResult>(&one, 1));
    }
    case T::InitProducerId: {
        pc::InitProducerIdResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        r.producerId = -1; r.producerEpoch = 0; r.errorCode = ec;
        return r.encode(out);
    }
    case T::AddPartitionsToTxn: {
        pc::AddPartitionsToTxnResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        const pc::TxnPartitionResult one{-1, -1, ec};
        return r.encode(out, std::span<const pc::TxnPartitionResult>(&one, 1));
    }
    case T::AddOffsetsToTxn: {
        pc::AddOffsetsToTxnResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = ec;
        return r.encode(out);
    }
    case T::TxnOffsetCommit: {
        pc::TxnOffsetCommitResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId;
        const pc::TxnPartitionResult one{-1, -1, ec};
        return r.encode(out, std::span<const pc::TxnPartitionResult>(&one, 1));
    }
    case T::EndTxn: {
        pc::EndTxnResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = ec;
        return r.encode(out);
    }
    case T::DefineTopic: {
        pc::DefineTopicResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = code;
        return r.encode(out);
    }
    case T::GetTopicSchema: {
        pc::TopicSchemaResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = code;
        return r.encode(out);
    }
    case T::AlterTopic: {
        pc::AlterTopicResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = code;
        return r.encode(out);
    }
    case T::CreateView: {
        pc::CreateViewResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.errorCode = code;
        return r.encode(out);
    }
    case T::GetViewRow: {
        pc::ViewRowResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.status = code;
        return r.encode(out);
    }
    case T::Ping:
    case T::MetadataRequest: {
        pc::ErrorResponse r;
        r.sessionId = sessionId; r.correlationId = correlationId; r.code = ec;
        return r.encode(out);
    }
    default:
        return 0;   // no response type (fire-and-forget / pre-session) or not a request
    }
}

/// Header-reading convenience for the coordination services: answer the
/// request in buf[0..len) whose body failed to decode with `code` (the gateway
/// already required a decodable header and a known session before handing it
/// over).  Returns 0 when the header itself does not decode.
[[nodiscard]] inline std::size_t encodeRejectionFor(const uint8_t* buf, std::size_t len, uint16_t code,
                                                    uint8_t* out, std::size_t cap) noexcept {
    const auto hdr = rune::protocol::client::MessageHeader::decode(buf, len);
    if (!hdr) return 0;
    return encodeRejection(hdr->templateId, hdr->sessionId, hdr->correlationId, code, out, cap);
}

} // namespace rune::gateway
