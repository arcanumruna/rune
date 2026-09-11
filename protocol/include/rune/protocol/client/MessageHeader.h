#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// Little-endian helpers (write/read fixed-width fields into raw byte buffers).
// Mirrors the style in rune/protocol/RecordBatch.h (post BE→LE unification).
// ---------------------------------------------------------------------------
namespace detail {

inline void putLE16(uint8_t* buf, int off, uint16_t v) noexcept {
    buf[off]     = static_cast<uint8_t>(v);
    buf[off + 1] = static_cast<uint8_t>(v >> 8);
}

inline void putLE64(uint8_t* buf, int off, int64_t v) noexcept {
    uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i)
        buf[off + i] = static_cast<uint8_t>(u >> (i * 8));
}

inline uint16_t getLE16(const uint8_t* buf, int off) noexcept {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off + 1]) << 8));
}

inline int64_t getLE64(const uint8_t* buf, int off) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

} // namespace detail

// ---------------------------------------------------------------------------
// MessageHeader — fixed little-endian layout, 24 bytes.
//
// Byte offsets:
//   blockLength   uint16  0
//   templateId    uint16  2
//   schemaId      uint16  4
//   version       uint16  6
//   sessionId     int64   8
//   correlationId int64   16
// ---------------------------------------------------------------------------
struct MessageHeader {
    uint16_t blockLength   = 0;
    uint16_t templateId    = 0;
    uint16_t schemaId      = kSchemaId;
    uint16_t version       = kProtocolVersion;
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;

    static constexpr std::size_t ENCODED_SIZE = 24;

    /// Encodes all fields into buf[0..23] as little-endian. buf must be ≥ 24 bytes.
    void encode(uint8_t* buf) const noexcept {
        detail::putLE16(buf,  0, blockLength);
        detail::putLE16(buf,  2, templateId);
        detail::putLE16(buf,  4, schemaId);
        detail::putLE16(buf,  6, version);
        detail::putLE64(buf,  8, sessionId);
        detail::putLE64(buf, 16, correlationId);
    }

    /// Decodes a header from buf[0..avail-1]. Returns nullopt if avail < 24.
    static std::optional<MessageHeader> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;
        MessageHeader h;
        h.blockLength   = detail::getLE16(buf,  0);
        h.templateId    = detail::getLE16(buf,  2);
        h.schemaId      = detail::getLE16(buf,  4);
        h.version       = detail::getLE16(buf,  6);
        h.sessionId     = detail::getLE64(buf,  8);
        h.correlationId = detail::getLE64(buf, 16);
        return h;
    }
};

static_assert(MessageHeader::ENCODED_SIZE == 24);

} // namespace rune::protocol::client
