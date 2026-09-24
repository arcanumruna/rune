// rune/client/codec/Codec.h
//
// Thin typed adapter layer over the rune-protocol client codecs.
// Namespace: rune::client::codec
//
// Design contract:
//   - ZERO re-implementation of the wire format.  All encoding/decoding is
//     delegated to the rune-protocol headers (single source of truth).
//   - Bounds-checked: every call goes through the underlying codec that already
//     returns std::nullopt on truncation or schema/templateId mismatch.
//   - Zero-alloc-friendly: caller supplies all buffers; no heap allocation here.
//   - Header-only.
//
// Session/transport layers (CC-3/CC-4) import THIS header rather than the raw
// rune-protocol headers directly, keeping them insulated from schema details.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

// Pull in the rune-protocol client codecs (header-only, sibling include path
// injected into the build by CMakeLists.txt).
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>

// Codec.h from rune-protocol is pulled transitively via ControlMessages.h and
// PingMessages.h (both include it), so no extra include needed here.

namespace rune::client::codec {

// ---------------------------------------------------------------------------
// Namespace-level protocol constants (pass-throughs — single source of truth
// remains rune::protocol::client).
// ---------------------------------------------------------------------------
namespace rpc = rune::protocol::client;

inline constexpr uint16_t kSchemaId        = rpc::kSchemaId;        // 120
inline constexpr uint16_t kProtocolVersion = rpc::kProtocolVersion; // 1

// ---------------------------------------------------------------------------
// TemplateId — typed enum re-exported into this namespace so callers can
// switch/compare without importing rune::protocol::client directly.
// ---------------------------------------------------------------------------
using TemplateId = rpc::TemplateId;

// ---------------------------------------------------------------------------
// ErrorCode — typed enum re-exported.
// ---------------------------------------------------------------------------
using ErrorCode = rpc::ErrorCode;

// ---------------------------------------------------------------------------
// FrameStatus / FrameCheck — re-exported from rune::protocol::client.
// ---------------------------------------------------------------------------
using FrameStatus = rpc::FrameStatus;
using FrameCheck  = rpc::FrameCheck;

// ===========================================================================
// Adapter entry points — encode / decode
//
// Each message type has:
//   encodeXxx(buf, cap, fields..., sessionId, correlationId) -> size_t
//     Returns bytes written, or 0 if cap is too small.
//   decodeXxx(buf, len)  -> std::optional<XxxView>
//     Returns nullopt on any truncation or header mismatch.
//
// The XxxView structs mirror the rune-protocol message structs but live in
// this namespace so upstream code stays decoupled from the protocol namespace.
// ===========================================================================

// ---------------------------------------------------------------------------
// ConnectRequestView — decoded ConnectRequest fields
// ---------------------------------------------------------------------------
struct ConnectRequestView {
    int64_t      sessionId     = 0;
    int64_t      correlationId = 0;
    int32_t      egressStreamId = 0;
    int32_t      clientVersion  = 0;
    std::string_view egressChannel;  // view into the original buffer
};

/// Encode a ConnectRequest frame into buf[0..cap-1].
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodeConnectRequest(uint8_t*         buf,
                                        std::size_t      cap,
                                        std::string_view egressChannel,
                                        int32_t          egressStreamId,
                                        int32_t          clientVersion,
                                        int64_t          sessionId,
                                        int64_t          correlationId) noexcept {
    // Minimum bytes needed: header(24) + fixed(8) + varLen(2) + channel bytes
    const std::size_t needed = rpc::MessageHeader::ENCODED_SIZE
                             + rpc::ConnectRequest::BODY_FIXED
                             + 2u
                             + egressChannel.size();
    if (cap < needed) return 0u;

    rpc::ConnectRequest msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.egressStreamId = egressStreamId;
    msg.clientVersion  = clientVersion;
    msg.egressChannel  = egressChannel;
    return msg.encode(buf);
}

/// Decode a ConnectRequest frame from buf[0..len-1].
/// Returns nullopt on truncation, schema mismatch, or templateId mismatch.
inline std::optional<ConnectRequestView> decodeConnectRequest(const uint8_t* buf,
                                                               std::size_t    len) noexcept {
    auto opt = rpc::ConnectRequest::decode(buf, len);
    if (!opt) return std::nullopt;
    ConnectRequestView v;
    v.sessionId      = opt->sessionId;
    v.correlationId  = opt->correlationId;
    v.egressStreamId = opt->egressStreamId;
    v.clientVersion  = opt->clientVersion;
    v.egressChannel  = opt->egressChannel;
    return v;
}

// ---------------------------------------------------------------------------
// ConnectResponseView — decoded ConnectResponse fields
// ---------------------------------------------------------------------------
struct ConnectResponseView {
    int64_t sessionId         = 0;
    int64_t correlationId     = 0;
    int16_t code              = 0;   // ErrorCode value
    int32_t leaderMemberId    = -1;
    int64_t assignedSessionId = 0;
};

/// Encode a ConnectResponse frame into buf[0..cap-1].
/// Returns bytes written (always 38 on success), or 0 if cap < 38.
inline std::size_t encodeConnectResponse(uint8_t* buf,
                                          std::size_t cap,
                                          int16_t     code,
                                          int32_t     leaderMemberId,
                                          int64_t     assignedSessionId,
                                          int64_t     sessionId,
                                          int64_t     correlationId) noexcept {
    if (cap < rpc::ConnectResponse::ENCODED_SIZE) return 0u;
    rpc::ConnectResponse msg;
    msg.sessionId         = sessionId;
    msg.correlationId     = correlationId;
    msg.code              = code;
    msg.leaderMemberId    = leaderMemberId;
    msg.assignedSessionId = assignedSessionId;
    return msg.encode(buf);
}

/// Decode a ConnectResponse frame.
inline std::optional<ConnectResponseView> decodeConnectResponse(const uint8_t* buf,
                                                                  std::size_t    len) noexcept {
    auto opt = rpc::ConnectResponse::decode(buf, len);
    if (!opt) return std::nullopt;
    ConnectResponseView v;
    v.sessionId         = opt->sessionId;
    v.correlationId     = opt->correlationId;
    v.code              = opt->code;
    v.leaderMemberId    = opt->leaderMemberId;
    v.assignedSessionId = opt->assignedSessionId;
    return v;
}

// ---------------------------------------------------------------------------
// KeepaliveView — decoded Keepalive fields
// ---------------------------------------------------------------------------
struct KeepaliveView {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
};

/// Encode a Keepalive frame into buf[0..cap-1].
/// Returns bytes written (always 24 on success), or 0 if cap < 24.
inline std::size_t encodeKeepalive(uint8_t*    buf,
                                    std::size_t cap,
                                    int64_t     sessionId,
                                    int64_t     correlationId) noexcept {
    if (cap < rpc::Keepalive::ENCODED_SIZE) return 0u;
    rpc::Keepalive msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    return msg.encode(buf);
}

/// Decode a Keepalive frame.
inline std::optional<KeepaliveView> decodeKeepalive(const uint8_t* buf,
                                                      std::size_t    len) noexcept {
    auto opt = rpc::Keepalive::decode(buf, len);
    if (!opt) return std::nullopt;
    KeepaliveView v;
    v.sessionId     = opt->sessionId;
    v.correlationId = opt->correlationId;
    return v;
}

// ---------------------------------------------------------------------------
// DisconnectView — decoded Disconnect fields
// ---------------------------------------------------------------------------
struct DisconnectView {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t reason        = 0;
};

/// Encode a Disconnect frame into buf[0..cap-1].
/// Returns bytes written (always 26 on success), or 0 if cap < 26.
inline std::size_t encodeDisconnect(uint8_t*    buf,
                                     std::size_t cap,
                                     int16_t     reason,
                                     int64_t     sessionId,
                                     int64_t     correlationId) noexcept {
    if (cap < rpc::Disconnect::ENCODED_SIZE) return 0u;
    rpc::Disconnect msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.reason        = reason;
    return msg.encode(buf);
}

/// Decode a Disconnect frame.
inline std::optional<DisconnectView> decodeDisconnect(const uint8_t* buf,
                                                        std::size_t    len) noexcept {
    auto opt = rpc::Disconnect::decode(buf, len);
    if (!opt) return std::nullopt;
    DisconnectView v;
    v.sessionId     = opt->sessionId;
    v.correlationId = opt->correlationId;
    v.reason        = opt->reason;
    return v;
}

// ---------------------------------------------------------------------------
// ErrorResponseView — decoded ErrorResponse fields
// ---------------------------------------------------------------------------
struct ErrorResponseView {
    int64_t          sessionId     = 0;
    int64_t          correlationId = 0;
    int16_t          code          = 0;  // ErrorCode value
    std::string_view message;             // view into the original buffer
};

/// Encode an ErrorResponse frame into buf[0..cap-1].
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodeErrorResponse(uint8_t*         buf,
                                        std::size_t      cap,
                                        int16_t          code,
                                        std::string_view message,
                                        int64_t          sessionId,
                                        int64_t          correlationId) noexcept {
    const std::size_t needed = rpc::MessageHeader::ENCODED_SIZE
                             + rpc::ErrorResponse::BODY_FIXED
                             + 2u
                             + message.size();
    if (cap < needed) return 0u;
    rpc::ErrorResponse msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.code          = code;
    msg.message       = message;
    return msg.encode(buf);
}

/// Decode an ErrorResponse frame.
inline std::optional<ErrorResponseView> decodeErrorResponse(const uint8_t* buf,
                                                              std::size_t    len) noexcept {
    auto opt = rpc::ErrorResponse::decode(buf, len);
    if (!opt) return std::nullopt;
    ErrorResponseView v;
    v.sessionId     = opt->sessionId;
    v.correlationId = opt->correlationId;
    v.code          = opt->code;
    v.message       = opt->message;
    return v;
}

// ---------------------------------------------------------------------------
// PingView — decoded Ping fields
// ---------------------------------------------------------------------------
struct PingView {
    int64_t      sessionId     = 0;
    int64_t      correlationId = 0;
    std::string_view echo;  // opaque bytes; view into the original buffer
};

/// Encode a Ping frame into buf[0..cap-1].
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodePing(uint8_t*         buf,
                               std::size_t      cap,
                               std::string_view echo,
                               int64_t          sessionId,
                               int64_t          correlationId) noexcept {
    const std::size_t needed = rpc::MessageHeader::ENCODED_SIZE + 2u + echo.size();
    if (cap < needed) return 0u;
    rpc::Ping msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.echo          = echo;
    return msg.encode(buf);
}

/// Decode a Ping frame.
inline std::optional<PingView> decodePing(const uint8_t* buf, std::size_t len) noexcept {
    auto opt = rpc::Ping::decode(buf, len);
    if (!opt) return std::nullopt;
    PingView v;
    v.sessionId     = opt->sessionId;
    v.correlationId = opt->correlationId;
    v.echo          = opt->echo;
    return v;
}

// ---------------------------------------------------------------------------
// PongView — decoded Pong fields
// ---------------------------------------------------------------------------
struct PongView {
    int64_t      sessionId     = 0;
    int64_t      correlationId = 0;
    std::string_view echo;  // opaque bytes; verbatim copy of Ping.echo
};

/// Encode a Pong frame into buf[0..cap-1].
/// Returns total bytes written, or 0 if cap is insufficient.
inline std::size_t encodePong(uint8_t*         buf,
                               std::size_t      cap,
                               std::string_view echo,
                               int64_t          sessionId,
                               int64_t          correlationId) noexcept {
    const std::size_t needed = rpc::MessageHeader::ENCODED_SIZE + 2u + echo.size();
    if (cap < needed) return 0u;
    rpc::Pong msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.echo          = echo;
    return msg.encode(buf);
}

/// Decode a Pong frame.
inline std::optional<PongView> decodePong(const uint8_t* buf, std::size_t len) noexcept {
    auto opt = rpc::Pong::decode(buf, len);
    if (!opt) return std::nullopt;
    PongView v;
    v.sessionId     = opt->sessionId;
    v.correlationId = opt->correlationId;
    v.echo          = opt->echo;
    return v;
}

// ===========================================================================
// MD1 Task 10: MetadataRequest / MetadataResponse codec adapters
// ===========================================================================

// ---------------------------------------------------------------------------
// BrokerEntryView — a single broker entry decoded from a MetadataResponse
// ---------------------------------------------------------------------------
struct BrokerEntryView {
    int32_t          nodeId = -1;
    int32_t          port   = 0;
    std::string_view host;    // view into the original buffer
};

// ---------------------------------------------------------------------------
// PartitionEntryView — a single partition entry decoded from MetadataResponse
// ---------------------------------------------------------------------------
struct PartitionEntryView {
    int32_t topicId        = 0;
    int32_t partition      = 0;
    int32_t leaderId       = -1;
    int32_t leaderEpoch    = -1;
    int32_t ingressStreamId = 0;  ///< E5.1: per-worker Aeron ingress stream id (0 = legacy/unknown)
};

// ---------------------------------------------------------------------------
// MetadataResponseDecoded — fully decoded MetadataResponse (owns heap copies)
// ---------------------------------------------------------------------------
struct MetadataResponseDecoded {
    int64_t                        sessionId     = 0;
    int64_t                        correlationId = 0;
    // Owned copies (host strings from the response buffer are transferred here).
    // Using the protocol-layer Decoded type directly for zero re-implementation.
    std::vector<BrokerEntryView>    brokers;
    std::vector<PartitionEntryView> partitions;
};

/// Encode a MetadataRequest frame into buf[0..cap-1].
/// topicIds is the list of topic ids to query; empty span means all topics.
/// Returns bytes written, or 0 if cap is insufficient.
inline std::size_t encodeMetadataRequest(uint8_t*                     buf,
                                          std::size_t                  cap,
                                          std::span<const int32_t>     topicIds,
                                          int64_t                      sessionId,
                                          int64_t                      correlationId) noexcept
{
    const std::size_t needed = rpc::MessageHeader::ENCODED_SIZE
                             + rpc::MetadataRequest::BODY_FIXED
                             + 2u                       // uint16 topicId count
                             + topicIds.size() * 4u;    // 4 bytes per topicId
    if (cap < needed) return 0u;
    rpc::MetadataRequest msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.topicIds      = topicIds;
    return msg.encode(buf);
}

/// Decode a MetadataResponse frame.
/// Returns nullopt on truncation, version mismatch, or templateId mismatch.
inline std::optional<MetadataResponseDecoded> decodeMetadataResponse(
    const uint8_t* buf, std::size_t len) noexcept
{
    auto opt = rpc::MetadataResponse::decode(buf, len);
    if (!opt) return std::nullopt;

    MetadataResponseDecoded result;
    result.sessionId     = opt->sessionId;
    result.correlationId = opt->correlationId;

    result.brokers.reserve(opt->brokers.size());
    for (const auto& b : opt->brokers) {
        BrokerEntryView bv;
        bv.nodeId = b.nodeId;
        bv.port   = b.port;
        bv.host   = b.host;
        result.brokers.push_back(bv);
    }

    result.partitions.reserve(opt->partitions.size());
    for (const auto& p : opt->partitions) {
        PartitionEntryView pv;
        pv.topicId        = p.topicId;
        pv.partition      = p.partition;
        pv.leaderId       = p.leaderId;
        pv.leaderEpoch    = p.leaderEpoch;
        pv.ingressStreamId = p.ingressStreamId;  // E5.1: per-worker ingress stream
        result.partitions.push_back(pv);
    }

    return result;
}

// ===========================================================================
// Frame dispatch helpers — pass-throughs to rune::protocol::client::Dispatch.h
// ===========================================================================

/// Validate and classify an incoming raw frame.
/// Returns a FrameCheck with the status, templateId, and version.
/// Never reads past buf[avail-1].
[[nodiscard]] inline FrameCheck checkFrame(const uint8_t* buf, std::size_t avail) noexcept {
    return rpc::checkFrame(buf, avail);
}

/// Convenience: returns the templateId iff the frame is valid (checkFrame == Ok).
/// Returns nullopt on any validation failure.
[[nodiscard]] inline std::optional<uint16_t> peekTemplate(const uint8_t* buf,
                                                           std::size_t    avail) noexcept {
    return rpc::peekTemplateId(buf, avail);
}

} // namespace rune::client::codec
