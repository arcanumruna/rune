#pragma once
// ---------------------------------------------------------------------------
// OffsetMessages.h — SBE-style flat little-endian codecs for the Rune
// consumer-offset protocol (schema id = 113, version = 1).
//
// Messages:
//   OffsetCommitRequest  (templateId = 1) — consumer → coordinator
//   OffsetCommitResponse (templateId = 2) — coordinator → consumer
//   OffsetFetchRequest   (templateId = 3) — consumer → coordinator
//   OffsetFetchResponse  (templateId = 4) — coordinator → consumer
//
// Wire format: 8-byte SBE message header followed by a fixed block, then
// variable-length string fields (each prefixed by a uint16 length).
//
// All integers are little-endian (static_assert enforced).
// NO internal allocation — encode/decode work on caller-provided buffers.
// Var-length strings: uint16 length prefix + raw UTF-8 bytes (no nul).
//
// 8-byte SBE header (identical structure to schemas 111/112):
//   blockLength : uint16   (fixed-field payload bytes, excluding header)
//   templateId  : uint16
//   schemaId    : uint16   (= 113)
//   version     : uint16   (= 1)
// ---------------------------------------------------------------------------

#include <cstdint>
#include <cstring>
#include <cassert>
#include <bit>
#include <span>
#include <string_view>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// Schema constants
// ---------------------------------------------------------------------------
inline constexpr uint16_t kOffsetSchemaId      = 113;
inline constexpr uint16_t kOffsetSchemaVersion = 1;

// ---------------------------------------------------------------------------
// Template IDs
// ---------------------------------------------------------------------------
enum class OffsetTemplateId : uint16_t {
    OffsetCommitRequest  = 1,
    OffsetCommitResponse = 2,
    OffsetFetchRequest   = 3,
    OffsetFetchResponse  = 4,
};

// ---------------------------------------------------------------------------
// Internal little-endian helpers (UB-free via memcpy; optimises to mov)
// ---------------------------------------------------------------------------
namespace detail {

static_assert(std::endian::native == std::endian::little,
    "rune::coordination SBE codecs assume a little-endian host "
    "(schema 113 is little-endian SBE)");

template<typename T>
inline void write_le(uint8_t* p, T v) noexcept {
    std::memcpy(p, &v, sizeof(T));
}

template<typename T>
inline T read_le(const uint8_t* p) noexcept {
    T v; std::memcpy(&v, p, sizeof(T)); return v;
}

/// Write a uint16-prefixed string into buf at *pos; advance *pos.
inline void write_str(uint8_t* buf, std::size_t& pos, std::string_view s) noexcept {
    auto len = static_cast<uint16_t>(s.size());
    write_le<uint16_t>(buf + pos, len);
    pos += 2;
    std::memcpy(buf + pos, s.data(), len);
    pos += len;
}

/// Read a uint16-prefixed string from buf at *pos; advance *pos.
/// Returns a std::string_view into buf (zero-copy).
inline std::string_view read_str(const uint8_t* buf, std::size_t& pos) noexcept {
    auto len = read_le<uint16_t>(buf + pos);
    pos += 2;
    std::string_view sv{reinterpret_cast<const char*>(buf + pos),
                        static_cast<std::size_t>(len)};
    pos += len;
    return sv;
}

} // namespace detail

// ---------------------------------------------------------------------------
// OffsetMessageHeader — 8 bytes, little-endian (schema 113)
// ---------------------------------------------------------------------------
struct OffsetMessageHeader {
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;

    static constexpr std::size_t ENCODED_SIZE = 8;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               uint16_t blockLen,
                               uint16_t tmplId) noexcept
    {
        uint8_t* p = buf + offset;
        detail::write_le<uint16_t>(p + 0, blockLen);
        detail::write_le<uint16_t>(p + 2, tmplId);
        detail::write_le<uint16_t>(p + 4, kOffsetSchemaId);
        detail::write_le<uint16_t>(p + 6, kOffsetSchemaVersion);
        return ENCODED_SIZE;
    }

    static OffsetMessageHeader decode(const uint8_t* buf,
                                       std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset;
        OffsetMessageHeader h{};
        h.blockLength = detail::read_le<uint16_t>(p + 0);
        h.templateId  = detail::read_le<uint16_t>(p + 2);
        h.schemaId    = detail::read_le<uint16_t>(p + 4);
        h.version     = detail::read_le<uint16_t>(p + 6);
        return h;
    }
};

inline uint16_t peekOffsetTemplateId(const uint8_t* buf,
                                      std::size_t offset = 0) noexcept {
    return detail::read_le<uint16_t>(buf + offset + 2);
}

// ===========================================================================
// 1. OffsetCommitRequest  (templateId = 1)
//
// Consumer → Coordinator: commit an offset for (group, topic, partition).
//
// Fixed block (12 bytes):
//   offset              : int64  @ +0
//   partition           : int32  @ +8
// Total fixed = 12 bytes
//
// Var-length (uint16 length prefix + bytes, in order):
//   group, topic, metadata
// ===========================================================================
struct OffsetCommitRequestMsg {
    int64_t          offset{0};
    int32_t          partition{0};
    std::string_view group;    ///< zero-copy view into decode buffer
    std::string_view topic;
    std::string_view metadata;

    static constexpr uint16_t TEMPLATE_ID  = 1;
    static constexpr uint16_t BLOCK_LENGTH = 12; // int64 + int32

    /// Encode into buf[bufOffset..].
    /// Returns total bytes written (header + fixed + var strings).
    static std::size_t encode(uint8_t*         buf,
                               std::size_t      bufOffset,
                               int64_t          offset_,
                               int32_t          partition_,
                               std::string_view group_,
                               std::string_view topic_,
                               std::string_view metadata_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += OffsetMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        detail::write_le<int64_t>(buf + pos, offset_); pos += 8;
        detail::write_le<int32_t>(buf + pos, partition_); pos += 4;
        detail::write_str(buf, pos, group_);
        detail::write_str(buf, pos, topic_);
        detail::write_str(buf, pos, metadata_);
        return pos - bufOffset;
    }

    /// Decode from buf[bufOffset..]. Var fields are zero-copy views into buf.
    static OffsetCommitRequestMsg decode(const uint8_t* buf,
                                          std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + OffsetMessageHeader::ENCODED_SIZE;
        OffsetCommitRequestMsg m{};
        m.offset    = detail::read_le<int64_t>(buf + pos); pos += 8;
        m.partition = detail::read_le<int32_t>(buf + pos); pos += 4;
        m.group     = detail::read_str(buf, pos);
        m.topic     = detail::read_str(buf, pos);
        m.metadata  = detail::read_str(buf, pos);
        return m;
    }
};

// ===========================================================================
// 2. OffsetCommitResponse  (templateId = 2)
//
// Coordinator → Consumer: acknowledgement with error code.
//
// Fixed block (6 bytes):
//   errorCode           : int16  @ +0
//   partition           : int32  @ +2
// Total fixed = 6 bytes
//
// Var-length: group, topic
// ===========================================================================
struct OffsetCommitResponseMsg {
    int16_t          errorCode{0};
    int32_t          partition{0};
    std::string_view group;
    std::string_view topic;

    static constexpr uint16_t TEMPLATE_ID  = 2;
    static constexpr uint16_t BLOCK_LENGTH = 6; // int16 + int32

    static std::size_t encode(uint8_t*         buf,
                               std::size_t      bufOffset,
                               int16_t          errorCode_,
                               int32_t          partition_,
                               std::string_view group_,
                               std::string_view topic_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += OffsetMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        detail::write_le<int16_t>(buf + pos, errorCode_); pos += 2;
        detail::write_le<int32_t>(buf + pos, partition_); pos += 4;
        detail::write_str(buf, pos, group_);
        detail::write_str(buf, pos, topic_);
        return pos - bufOffset;
    }

    static OffsetCommitResponseMsg decode(const uint8_t* buf,
                                           std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + OffsetMessageHeader::ENCODED_SIZE;
        OffsetCommitResponseMsg m{};
        m.errorCode = detail::read_le<int16_t>(buf + pos); pos += 2;
        m.partition = detail::read_le<int32_t>(buf + pos); pos += 4;
        m.group     = detail::read_str(buf, pos);
        m.topic     = detail::read_str(buf, pos);
        return m;
    }
};

// ===========================================================================
// 3. OffsetFetchRequest  (templateId = 3)
//
// Consumer → Coordinator: fetch committed offset for (group, topic, partition).
//
// Fixed block (4 bytes):
//   partition           : int32  @ +0
// Total fixed = 4 bytes
//
// Var-length: group, topic
// ===========================================================================
struct OffsetFetchRequestMsg {
    int32_t          partition{0};
    std::string_view group;
    std::string_view topic;

    static constexpr uint16_t TEMPLATE_ID  = 3;
    static constexpr uint16_t BLOCK_LENGTH = 4; // int32

    static std::size_t encode(uint8_t*         buf,
                               std::size_t      bufOffset,
                               int32_t          partition_,
                               std::string_view group_,
                               std::string_view topic_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += OffsetMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        detail::write_le<int32_t>(buf + pos, partition_); pos += 4;
        detail::write_str(buf, pos, group_);
        detail::write_str(buf, pos, topic_);
        return pos - bufOffset;
    }

    static OffsetFetchRequestMsg decode(const uint8_t* buf,
                                         std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + OffsetMessageHeader::ENCODED_SIZE;
        OffsetFetchRequestMsg m{};
        m.partition = detail::read_le<int32_t>(buf + pos); pos += 4;
        m.group     = detail::read_str(buf, pos);
        m.topic     = detail::read_str(buf, pos);
        return m;
    }
};

// ===========================================================================
// 4. OffsetFetchResponse  (templateId = 4)
//
// Coordinator → Consumer: committed offset value (or -1 if not found).
//
// Fixed block (14 bytes):
//   offset              : int64  @ +0
//   commitTimestampNs   : int64  @ +8  (0 if not found)
//   errorCode           : int16  @ +16 (0 = OK, 1 = NOT_FOUND)
//   partition           : int32  @ +18 (for correlation)
// Wait — re-count: int64 + int64 + int16 + int32 = 22 bytes
// Total fixed = 22 bytes
//
// Var-length: group, topic, metadata
// ===========================================================================
struct OffsetFetchResponseMsg {
    int64_t          offset{-1};
    int64_t          commitTimestampNs{0};
    int16_t          errorCode{0};
    int32_t          partition{0};
    std::string_view group;
    std::string_view topic;
    std::string_view metadata;

    static constexpr uint16_t TEMPLATE_ID  = 4;
    // int64(offset) + int64(ts) + int16(err) + int32(partition) = 8+8+2+4 = 22
    static constexpr uint16_t BLOCK_LENGTH = 22;

    static std::size_t encode(uint8_t*         buf,
                               std::size_t      bufOffset,
                               int64_t          offset_,
                               int64_t          commitTimestampNs_,
                               int16_t          errorCode_,
                               int32_t          partition_,
                               std::string_view group_,
                               std::string_view topic_,
                               std::string_view metadata_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += OffsetMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        detail::write_le<int64_t>(buf + pos, offset_);             pos += 8;
        detail::write_le<int64_t>(buf + pos, commitTimestampNs_);  pos += 8;
        detail::write_le<int16_t>(buf + pos, errorCode_);          pos += 2;
        detail::write_le<int32_t>(buf + pos, partition_);          pos += 4;
        detail::write_str(buf, pos, group_);
        detail::write_str(buf, pos, topic_);
        detail::write_str(buf, pos, metadata_);
        return pos - bufOffset;
    }

    static OffsetFetchResponseMsg decode(const uint8_t* buf,
                                          std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + OffsetMessageHeader::ENCODED_SIZE;
        OffsetFetchResponseMsg m{};
        m.offset            = detail::read_le<int64_t>(buf + pos); pos += 8;
        m.commitTimestampNs = detail::read_le<int64_t>(buf + pos); pos += 8;
        m.errorCode         = detail::read_le<int16_t>(buf + pos); pos += 2;
        m.partition         = detail::read_le<int32_t>(buf + pos); pos += 4;
        m.group             = detail::read_str(buf, pos);
        m.topic             = detail::read_str(buf, pos);
        m.metadata          = detail::read_str(buf, pos);
        return m;
    }
};

} // namespace rune::coordination
