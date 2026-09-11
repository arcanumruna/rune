#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// ConnectRequest  (templateId 1)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (8)
//     templateId  = 1
//   [24..27]  egressStreamId : int32
//   [28..31]  clientVersion  : int32
//   [32..33]  egressChannel length : uint16
//   [34..]    egressChannel bytes
//
// Total = 24 + 8 + 2 + egressChannel.size() = 34 + egressChannel.size()
// ---------------------------------------------------------------------------
struct ConnectRequest {
    int64_t      sessionId       = 0;
    int64_t      correlationId   = 0;
    int32_t      egressStreamId  = 0;
    int32_t      clientVersion   = 0;
    std::string_view egressChannel;   // e.g. "aeron:udp?endpoint=host:port"

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Connect); // 1
    static constexpr uint16_t BODY_FIXED  = 8;  // fixed body bytes (not counting header or var fields)

    /// Encode the full frame into buf.  buf must be large enough:
    ///   MessageHeader::ENCODED_SIZE + BODY_FIXED + 2 + egressChannel.size()
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        // Write header
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        // Fixed body starting at byte 24
        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE32(buf, fixedBase + 0, egressStreamId);
        detail::putLE32(buf, fixedBase + 4, clientVersion);

        // Var field starting at byte 32
        const std::size_t varOffset = fixedBase + BODY_FIXED;
        const std::size_t varWritten = putVarStr(buf + varOffset, egressChannel);

        return varOffset + varWritten;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<ConnectRequest> decode(const uint8_t* buf, std::size_t avail) noexcept {
        // Decode and validate header
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // Fixed body
        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        ConnectRequest msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.egressStreamId = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);
        msg.clientVersion  = detail::getLE32(buf, static_cast<int>(fixedBase) + 4);

        // Var field
        std::size_t pos = fixedBase + BODY_FIXED;
        if (!getVarStr(buf, avail, pos, msg.egressChannel)) return std::nullopt;

        return msg;
    }
};

// ---------------------------------------------------------------------------
// ConnectResponse  (templateId 2)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (14)
//     templateId  = 2
//   [24..25]  code              : int16
//   [26..29]  leaderMemberId    : int32
//   [30..37]  assignedSessionId : int64
//
// Total = 24 + 14 = 38 bytes (no var fields)
// ---------------------------------------------------------------------------
struct ConnectResponse {
    int64_t sessionId         = 0;
    int64_t correlationId     = 0;
    int16_t code              = 0;   // rune::protocol::client::ErrorCode value
    int32_t leaderMemberId    = -1;
    int64_t assignedSessionId = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ConnectResponse); // 2
    static constexpr uint16_t BODY_FIXED  = 14;

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 38

    /// Encode the full frame (38 bytes) into buf.  Returns 38.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16s(buf, fixedBase + 0, code);
        detail::putLE32 (buf, fixedBase + 2, leaderMemberId);
        detail::putLE64 (buf, fixedBase + 6, assignedSessionId);

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<ConnectResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        ConnectResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.code              = detail::getLE16s(buf, fixedBase + 0);
        msg.leaderMemberId    = detail::getLE32 (buf, fixedBase + 2);
        msg.assignedSessionId = detail::getLE64 (buf, fixedBase + 6);

        return msg;
    }
};

} // namespace rune::protocol::client
