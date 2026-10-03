/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _RUNE_PROTOCOL_INTERNAL_SBE_WRITESYNCREQUEST_CXX_H_
#define _RUNE_PROTOCOL_INTERNAL_SBE_WRITESYNCREQUEST_CXX_H_

#if __cplusplus >= 201103L
#  define SBE_CONSTEXPR constexpr
#  define SBE_NOEXCEPT noexcept
#else
#  define SBE_CONSTEXPR
#  define SBE_NOEXCEPT
#endif

#if __cplusplus >= 201703L
#  include <string_view>
#  define SBE_NODISCARD [[nodiscard]]
#  if !defined(SBE_USE_STRING_VIEW)
#    define SBE_USE_STRING_VIEW 1
#  endif
#else
#  define SBE_NODISCARD
#endif

#if __cplusplus >= 202002L
#  include <span>
#  if !defined(SBE_USE_SPAN)
#    define SBE_USE_SPAN 1
#  endif
#endif

#if !defined(__STDC_LIMIT_MACROS)
#  define __STDC_LIMIT_MACROS 1
#endif

#include <cstdint>
#include <limits>
#include <cstring>
#include <iomanip>
#include <ostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>
#include <tuple>

#if defined(WIN32) || defined(_WIN32)
#  define SBE_BIG_ENDIAN_ENCODE_16(v) _byteswap_ushort(v)
#  define SBE_BIG_ENDIAN_ENCODE_32(v) _byteswap_ulong(v)
#  define SBE_BIG_ENDIAN_ENCODE_64(v) _byteswap_uint64(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_16(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_32(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_64(v) (v)
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#  define SBE_BIG_ENDIAN_ENCODE_16(v) __builtin_bswap16(v)
#  define SBE_BIG_ENDIAN_ENCODE_32(v) __builtin_bswap32(v)
#  define SBE_BIG_ENDIAN_ENCODE_64(v) __builtin_bswap64(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_16(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_32(v) (v)
#  define SBE_LITTLE_ENDIAN_ENCODE_64(v) (v)
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define SBE_LITTLE_ENDIAN_ENCODE_16(v) __builtin_bswap16(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_32(v) __builtin_bswap32(v)
#  define SBE_LITTLE_ENDIAN_ENCODE_64(v) __builtin_bswap64(v)
#  define SBE_BIG_ENDIAN_ENCODE_16(v) (v)
#  define SBE_BIG_ENDIAN_ENCODE_32(v) (v)
#  define SBE_BIG_ENDIAN_ENCODE_64(v) (v)
#else
#  error "Byte Ordering of platform not determined. Set __BYTE_ORDER__ manually before including this file."
#endif

#if !defined(SBE_BOUNDS_CHECK_EXPECT)
#  if defined(SBE_NO_BOUNDS_CHECK)
#    define SBE_BOUNDS_CHECK_EXPECT(exp, c) (false)
#  elif defined(_MSC_VER)
#    define SBE_BOUNDS_CHECK_EXPECT(exp, c) (exp)
#  else 
#    define SBE_BOUNDS_CHECK_EXPECT(exp, c) (__builtin_expect(exp, c))
#  endif

#endif

#define SBE_FLOAT_NAN std::numeric_limits<float>::quiet_NaN()
#define SBE_DOUBLE_NAN std::numeric_limits<double>::quiet_NaN()
#define SBE_FLOAT_INFINITY std::numeric_limits<float>::infinity()
#define SBE_DOUBLE_INFINITY std::numeric_limits<double>::infinity()
#define SBE_NULLVALUE_INT8 (std::numeric_limits<std::int8_t>::min)()
#define SBE_NULLVALUE_INT16 (std::numeric_limits<std::int16_t>::min)()
#define SBE_NULLVALUE_INT32 (std::numeric_limits<std::int32_t>::min)()
#define SBE_NULLVALUE_INT64 (std::numeric_limits<std::int64_t>::min)()
#define SBE_NULLVALUE_UINT8 (std::numeric_limits<std::uint8_t>::max)()
#define SBE_NULLVALUE_UINT16 (std::numeric_limits<std::uint16_t>::max)()
#define SBE_NULLVALUE_UINT32 (std::numeric_limits<std::uint32_t>::max)()
#define SBE_NULLVALUE_UINT64 (std::numeric_limits<std::uint64_t>::max)()


#include "MessageHeader.h"
#include "TxnReturnKind.h"
#include "TailKind.h"
#include "OpType.h"
#include "ReturnType.h"
#include "VarDataEncoding.h"

namespace rune {
namespace protocol {
namespace internal {
namespace sbe {

class WriteSyncRequest
{
private:
    char *m_buffer = nullptr;
    std::uint64_t m_bufferLength = 0;
    std::uint64_t m_offset = 0;
    std::uint64_t m_position = 0;
    std::uint64_t m_actingBlockLength = 0;
    std::uint64_t m_actingVersion = 0;

    inline std::uint64_t *sbePositionPtr() SBE_NOEXCEPT
    {
        return &m_position;
    }

public:
    static constexpr std::uint16_t SBE_BLOCK_LENGTH = static_cast<std::uint16_t>(86);
    static constexpr std::uint16_t SBE_TEMPLATE_ID = static_cast<std::uint16_t>(7);
    static constexpr std::uint16_t SBE_SCHEMA_ID = static_cast<std::uint16_t>(7);
    static constexpr std::uint16_t SBE_SCHEMA_VERSION = static_cast<std::uint16_t>(0);
    static constexpr const char* SBE_SEMANTIC_VERSION = "1.0";

    enum MetaAttribute
    {
        EPOCH, TIME_UNIT, SEMANTIC_TYPE, PRESENCE
    };

    union sbe_float_as_uint_u
    {
        float fp_value;
        std::uint32_t uint_value;
    };

    union sbe_double_as_uint_u
    {
        double fp_value;
        std::uint64_t uint_value;
    };

    using messageHeader = MessageHeader;

    WriteSyncRequest() = default;

    WriteSyncRequest(
        char *buffer,
        const std::uint64_t offset,
        const std::uint64_t bufferLength,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion) :
        m_buffer(buffer),
        m_bufferLength(bufferLength),
        m_offset(offset),
        m_position(sbeCheckPosition(offset + actingBlockLength)),
        m_actingBlockLength(actingBlockLength),
        m_actingVersion(actingVersion)
    {
    }

    WriteSyncRequest(char *buffer, const std::uint64_t bufferLength) :
        WriteSyncRequest(buffer, 0, bufferLength, sbeBlockLength(), sbeSchemaVersion())
    {
    }

    WriteSyncRequest(
        char *buffer,
        const std::uint64_t bufferLength,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion) :
        WriteSyncRequest(buffer, 0, bufferLength, actingBlockLength, actingVersion)
    {
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeBlockLength() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(86);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t sbeBlockAndHeaderLength() SBE_NOEXCEPT
    {
        return messageHeader::encodedLength() + sbeBlockLength();
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeTemplateId() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(7);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeSchemaId() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(7);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeSchemaVersion() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    SBE_NODISCARD static const char *sbeSemanticVersion() SBE_NOEXCEPT
    {
        return "1.0";
    }

    SBE_NODISCARD static SBE_CONSTEXPR const char *sbeSemanticType() SBE_NOEXCEPT
    {
        return "";
    }

    SBE_NODISCARD std::uint64_t offset() const SBE_NOEXCEPT
    {
        return m_offset;
    }

    WriteSyncRequest &wrapForEncode(char *buffer, const std::uint64_t offset, const std::uint64_t bufferLength)
    {
        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset;
        m_actingBlockLength = sbeBlockLength();
        m_actingVersion = sbeSchemaVersion();
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    WriteSyncRequest &wrapAndApplyHeader(char *buffer, const std::uint64_t offset, const std::uint64_t bufferLength)
    {
        messageHeader hdr(buffer, offset, bufferLength, sbeSchemaVersion());

        hdr
            .blockLength(sbeBlockLength())
            .templateId(sbeTemplateId())
            .schemaId(sbeSchemaId())
            .version(sbeSchemaVersion());

        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset + messageHeader::encodedLength();
        m_actingBlockLength = sbeBlockLength();
        m_actingVersion = sbeSchemaVersion();
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    WriteSyncRequest &wrapForDecode(
        char *buffer,
        const std::uint64_t offset,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion,
        const std::uint64_t bufferLength)
    {
        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset;
        m_actingBlockLength = actingBlockLength;
        m_actingVersion = actingVersion;
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    WriteSyncRequest &sbeRewind()
    {
        return wrapForDecode(m_buffer, m_offset, m_actingBlockLength, m_actingVersion, m_bufferLength);
    }

    SBE_NODISCARD std::uint64_t sbePosition() const SBE_NOEXCEPT
    {
        return m_position;
    }

    // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    std::uint64_t sbeCheckPosition(const std::uint64_t position)
    {
        if (SBE_BOUNDS_CHECK_EXPECT((position > m_bufferLength), false))
        {
            throw std::runtime_error("buffer too short [E100]");
        }
        return position;
    }

    void sbePosition(const std::uint64_t position)
    {
        m_position = sbeCheckPosition(position);
    }

    SBE_NODISCARD std::uint64_t encodedLength() const SBE_NOEXCEPT
    {
        return sbePosition() - m_offset;
    }

    SBE_NODISCARD std::uint64_t decodeLength() const
    {
        WriteSyncRequest skipper(m_buffer, m_offset, m_bufferLength, m_actingBlockLength, m_actingVersion);
        skipper.skip();
        return skipper.encodedLength();
    }

    SBE_NODISCARD const char *buffer() const SBE_NOEXCEPT
    {
        return m_buffer;
    }

    SBE_NODISCARD char *buffer() SBE_NOEXCEPT
    {
        return m_buffer;
    }

    SBE_NODISCARD std::uint64_t bufferLength() const SBE_NOEXCEPT
    {
        return m_bufferLength;
    }

    SBE_NODISCARD std::uint64_t actingVersion() const SBE_NOEXCEPT
    {
        return m_actingVersion;
    }

    SBE_NODISCARD static const char *fdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t fdId() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t fdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool fdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t fdEncodingOffset() SBE_NOEXCEPT
    {
        return 0;
    }

    static SBE_CONSTEXPR std::int32_t fdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t fdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t fdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t fdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t fd() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 0, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WriteSyncRequest &fd(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 0, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *lengthMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t lengthId() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t lengthSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool lengthInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t lengthEncodingOffset() SBE_NOEXCEPT
    {
        return 4;
    }

    static SBE_CONSTEXPR std::uint16_t lengthNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT16;
    }

    static SBE_CONSTEXPR std::uint16_t lengthMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    static SBE_CONSTEXPR std::uint16_t lengthMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(65534);
    }

    static SBE_CONSTEXPR std::size_t lengthEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::uint16_t length() const SBE_NOEXCEPT
    {
        std::uint16_t val;
        std::memcpy(&val, m_buffer + m_offset + 4, sizeof(std::uint16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    WriteSyncRequest &length(const std::uint16_t value) SBE_NOEXCEPT
    {
        std::uint16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 4, &val, sizeof(std::uint16_t));
        return *this;
    }

    SBE_NODISCARD static const char *fileOffsetMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t fileOffsetId() SBE_NOEXCEPT
    {
        return 3;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t fileOffsetSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool fileOffsetInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t fileOffsetEncodingOffset() SBE_NOEXCEPT
    {
        return 6;
    }

    static SBE_CONSTEXPR std::int64_t fileOffsetNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t fileOffsetMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t fileOffsetMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t fileOffsetEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t fileOffset() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 6, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WriteSyncRequest &fileOffset(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 6, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *tokenMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t tokenId() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t tokenSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool tokenInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t tokenEncodingOffset() SBE_NOEXCEPT
    {
        return 14;
    }

    static SBE_CONSTEXPR std::int64_t tokenNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t tokenMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t tokenMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t tokenEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t token() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 14, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WriteSyncRequest &token(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 14, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *blockMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t blockId() SBE_NOEXCEPT
    {
        return 5;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t blockSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool blockInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t blockEncodingOffset() SBE_NOEXCEPT
    {
        return 22;
    }

    static SBE_CONSTEXPR std::uint8_t blockNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT8;
    }

    static SBE_CONSTEXPR std::uint8_t blockMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(0);
    }

    static SBE_CONSTEXPR std::uint8_t blockMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(254);
    }

    static SBE_CONSTEXPR std::size_t blockEncodingLength() SBE_NOEXCEPT
    {
        return 64;
    }

    static SBE_CONSTEXPR std::uint64_t blockLength() SBE_NOEXCEPT
    {
        return 64;
    }

    SBE_NODISCARD const char *block() const SBE_NOEXCEPT
    {
        return m_buffer + m_offset + 22;
    }

    SBE_NODISCARD char *block() SBE_NOEXCEPT
    {
        return m_buffer + m_offset + 22;
    }

    SBE_NODISCARD std::uint8_t block(const std::uint64_t index) const
    {
        if (index >= 64)
        {
            throw std::runtime_error("index out of range for block [E104]");
        }

        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 22 + (index * 1), sizeof(std::uint8_t));
        return (val);
    }

    WriteSyncRequest &block(const std::uint64_t index, const std::uint8_t value)
    {
        if (index >= 64)
        {
            throw std::runtime_error("index out of range for block [E105]");
        }

        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 22 + (index * 1), &val, sizeof(std::uint8_t));
        return *this;
    }

    std::uint64_t getBlock(char *const dst, const std::uint64_t length) const
    {
        if (length > 64)
        {
            throw std::runtime_error("length too large for getBlock [E106]");
        }

        std::memcpy(dst, m_buffer + m_offset + 22, sizeof(std::uint8_t) * static_cast<std::size_t>(length));
        return length;
    }

    #ifdef SBE_USE_SPAN
    SBE_NODISCARD std::span<const std::uint8_t> getBlockAsSpan() const SBE_NOEXCEPT
    {
        const char *buffer = m_buffer + m_offset + 22;
        return std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(buffer), 64);
    }
    #endif

    #ifdef SBE_USE_SPAN
    template <std::size_t N>
    WriteSyncRequest &putBlock(std::span<const std::uint8_t, N> src) SBE_NOEXCEPT
    {
        static_assert(N <= 64, "array too large for putBlock");

        if (N > 0)
        {
            std::memcpy(m_buffer + m_offset + 22, src.data(), sizeof(std::uint8_t) * N);
        }

        for (std::size_t start = N; start < 64; ++start)
        {
            m_buffer[m_offset + 22 + start] = 0;
        }

        return *this;
    }
    #endif

    #ifdef SBE_USE_SPAN
    WriteSyncRequest &putBlock(std::span<const std::uint8_t> src)
    {
        const std::size_t srcLength = src.size();
        if (srcLength > 64)
        {
            throw std::runtime_error("array too large for putBlock [E106]");
        }

        if (srcLength > 0)
        {
            std::memcpy(m_buffer + m_offset + 22, src.data(), sizeof(std::uint8_t) * srcLength);
        }

        for (std::size_t start = srcLength; start < 64; ++start)
        {
            m_buffer[m_offset + 22 + start] = 0;
        }

        return *this;
    }
    #endif

    #ifdef SBE_USE_SPAN
    template <typename T>
    WriteSyncRequest &putBlock(T&& src)  SBE_NOEXCEPT requires
        (std::is_pointer_v<std::remove_reference_t<T>> &&
         !std::is_array_v<std::remove_reference_t<T>>)
    #else
    WriteSyncRequest &putBlock(const char *const src) SBE_NOEXCEPT
    #endif
    {
        std::memcpy(m_buffer + m_offset + 22, src, sizeof(std::uint8_t) * 64);
        return *this;
    }

template<typename CharT, typename Traits>
friend std::basic_ostream<CharT, Traits> & operator << (
    std::basic_ostream<CharT, Traits> &builder, const WriteSyncRequest &_writer)
{
    WriteSyncRequest writer(
        _writer.m_buffer,
        _writer.m_offset,
        _writer.m_bufferLength,
        _writer.m_actingBlockLength,
        _writer.m_actingVersion);

    builder << '{';
    builder << R"("Name": "WriteSyncRequest", )";
    builder << R"("sbeTemplateId": )";
    builder << writer.sbeTemplateId();
    builder << ", ";

    builder << R"("fd": )";
    builder << +writer.fd();

    builder << ", ";
    builder << R"("length": )";
    builder << +writer.length();

    builder << ", ";
    builder << R"("fileOffset": )";
    builder << +writer.fileOffset();

    builder << ", ";
    builder << R"("token": )";
    builder << +writer.token();

    builder << ", ";
    builder << R"("block": )";
    builder << '[';
    for (std::size_t i = 0, length = writer.blockLength(); i < length; i++)
    {
        if (i)
        {
            builder << ',';
        }
        builder << +writer.block(i);
    }
    builder << ']';

    builder << '}';

    return builder;
}

void skip()
{
}

SBE_NODISCARD static SBE_CONSTEXPR bool isConstLength() SBE_NOEXCEPT
{
    return true;
}

SBE_NODISCARD static std::size_t computeLength()
{
#if defined(__GNUG__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtype-limits"
#endif
    std::size_t length = sbeBlockLength();

    return length;
#if defined(__GNUG__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
}
};
}
}
}
}
#endif
