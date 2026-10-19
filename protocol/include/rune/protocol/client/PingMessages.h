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
// Ping  (templateId 6, BODY_FIXED = 0)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 6
//   [24..25]  echo length : uint16  (var prefix)
//   [26..]    echo bytes  (opaque; may contain binary/NUL/0xFF)
//
// Total = 24 + 2 + echo.size()
// ---------------------------------------------------------------------------
struct Ping {
    int64_t       sessionId     = 0;
    int64_t       correlationId = 0;
    std::string_view echo;   // opaque bytes — not required to be valid UTF-8

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Ping); // 6
    static constexpr uint16_t BODY_FIXED  = 0;

    /// Encode the full frame into buf.  buf must be large enough:
    ///   MessageHeader::ENCODED_SIZE + 2 + echo.size()
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        // Var field starts immediately at byte 24 (no fixed body bytes).
        const std::size_t varOffset = MessageHeader::ENCODED_SIZE;
        const std::size_t varWritten = putVarBytes(
            buf + varOffset,
            reinterpret_cast<const uint8_t*>(echo.data()),
            static_cast<uint16_t>(echo.size()));

        return varOffset + varWritten;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation, or if schemaId/templateId don't match.
    static std::optional<Ping> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // BODY_FIXED = 0, so var field starts right after the header.
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        const uint8_t* echoData = nullptr;
        uint16_t       echoLen  = 0;
        if (!getVarBytes(buf, avail, pos, echoData, echoLen)) return std::nullopt;

        Ping msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.echo = std::string_view(reinterpret_cast<const char*>(echoData), echoLen);
        return msg;
    }
};

// ---------------------------------------------------------------------------
// Pong  (templateId 7, BODY_FIXED = 0)
//
// Frame layout identical to Ping — templateId is the only distinction.
//
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 7
//   [24..25]  echo length : uint16  (var prefix)
//   [26..]    echo bytes  (opaque; returned verbatim)
//
// Total = 24 + 2 + echo.size()
// ---------------------------------------------------------------------------
struct Pong {
    int64_t       sessionId     = 0;
    int64_t       correlationId = 0;
    std::string_view echo;   // opaque bytes — verbatim copy of Ping.echo

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Pong); // 7
    static constexpr uint16_t BODY_FIXED  = 0;

    /// Encode the full frame into buf.  buf must be large enough:
    ///   MessageHeader::ENCODED_SIZE + 2 + echo.size()
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        const std::size_t varOffset = MessageHeader::ENCODED_SIZE;
        const std::size_t varWritten = putVarBytes(
            buf + varOffset,
            reinterpret_cast<const uint8_t*>(echo.data()),
            static_cast<uint16_t>(echo.size()));

        return varOffset + varWritten;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation, or if schemaId/templateId don't match.
    static std::optional<Pong> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        std::size_t pos = MessageHeader::ENCODED_SIZE;
        const uint8_t* echoData = nullptr;
        uint16_t       echoLen  = 0;
        if (!getVarBytes(buf, avail, pos, echoData, echoLen)) return std::nullopt;

        Pong msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.echo = std::string_view(reinterpret_cast<const char*>(echoData), echoLen);
        return msg;
    }
};

} // namespace rune::protocol::client
