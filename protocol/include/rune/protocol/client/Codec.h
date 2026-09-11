#pragma once
#include <cstddef>
#include <cstdint>
#include <string_view>
#include "MessageHeader.h"   // pulls in detail::putLE16/getLE16/putLE64/getLE64

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// Additional LE helpers for 32-bit and signed 16-bit fields (detail namespace).
// ---------------------------------------------------------------------------
namespace detail {

inline void putLE32(uint8_t* buf, int off, int32_t v) noexcept {
    uint32_t u = static_cast<uint32_t>(v);
    buf[off]     = static_cast<uint8_t>(u);
    buf[off + 1] = static_cast<uint8_t>(u >> 8);
    buf[off + 2] = static_cast<uint8_t>(u >> 16);
    buf[off + 3] = static_cast<uint8_t>(u >> 24);
}

inline int32_t getLE32(const uint8_t* buf, int off) noexcept {
    uint32_t v = uint32_t(buf[off])
               | (uint32_t(buf[off + 1]) << 8)
               | (uint32_t(buf[off + 2]) << 16)
               | (uint32_t(buf[off + 3]) << 24);
    return static_cast<int32_t>(v);
}

inline void putLE16s(uint8_t* buf, int off, int16_t v) noexcept {
    putLE16(buf, off, static_cast<uint16_t>(v));
}

inline int16_t getLE16s(const uint8_t* buf, int off) noexcept {
    return static_cast<int16_t>(getLE16(buf, off));
}

} // namespace detail

// ---------------------------------------------------------------------------
// Var-field helpers — little-endian, zero-alloc, noexcept.
// A "var field" is a uint16 length (LE) followed by that many bytes.
// ---------------------------------------------------------------------------

/// Write a var-bytes field into buf.  buf must have capacity for 2 + len bytes.
/// Returns bytes written (always 2 + len).
inline std::size_t putVarBytes(uint8_t* buf, const uint8_t* data, uint16_t len) noexcept {
    detail::putLE16(buf, 0, len);
    for (uint16_t i = 0; i < len; ++i)
        buf[2 + i] = data[i];
    return static_cast<std::size_t>(2) + len;
}

/// Read a var-bytes field from buf[pos..avail-1].
/// Reads the u16 length then length bytes; advances pos.
/// Returns false (no over-read) if truncated.
inline bool getVarBytes(const uint8_t* buf, std::size_t avail, std::size_t& pos,
                        const uint8_t*& outData, uint16_t& outLen) noexcept {
    // Need at least 2 bytes for the length prefix.
    if (pos + 2 > avail) return false;
    const uint16_t len = detail::getLE16(buf, static_cast<int>(pos));
    pos += 2;
    // Need len more bytes.
    if (pos + len > avail) return false;
    outData = buf + pos;
    outLen  = len;
    pos    += len;
    return true;
}

/// String convenience — encodes a string_view as a var-bytes field.
inline std::size_t putVarStr(uint8_t* buf, std::string_view s) noexcept {
    return putVarBytes(buf,
                       reinterpret_cast<const uint8_t*>(s.data()),
                       static_cast<uint16_t>(s.size()));
}

/// String convenience — decodes a var-bytes field as a string_view.
inline bool getVarStr(const uint8_t* buf, std::size_t avail, std::size_t& pos,
                      std::string_view& out) noexcept {
    const uint8_t* data = nullptr;
    uint16_t       len  = 0;
    if (!getVarBytes(buf, avail, pos, data, len)) return false;
    out = std::string_view(reinterpret_cast<const char*>(data), len);
    return true;
}

} // namespace rune::protocol::client
