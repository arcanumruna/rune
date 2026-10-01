/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _RUNE_PROTOCOL_INTERNAL_SBE_WORKEROP_CXX_H_
#define _RUNE_PROTOCOL_INTERNAL_SBE_WORKEROP_CXX_H_

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

class WorkerOp
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
    static constexpr std::uint16_t SBE_BLOCK_LENGTH = static_cast<std::uint16_t>(239);
    static constexpr std::uint16_t SBE_TEMPLATE_ID = static_cast<std::uint16_t>(1);
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

    WorkerOp() = default;

    WorkerOp(
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

    WorkerOp(char *buffer, const std::uint64_t bufferLength) :
        WorkerOp(buffer, 0, bufferLength, sbeBlockLength(), sbeSchemaVersion())
    {
    }

    WorkerOp(
        char *buffer,
        const std::uint64_t bufferLength,
        const std::uint64_t actingBlockLength,
        const std::uint64_t actingVersion) :
        WorkerOp(buffer, 0, bufferLength, actingBlockLength, actingVersion)
    {
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeBlockLength() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(239);
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t sbeBlockAndHeaderLength() SBE_NOEXCEPT
    {
        return messageHeader::encodedLength() + sbeBlockLength();
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint16_t sbeTemplateId() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(1);
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

    WorkerOp &wrapForEncode(char *buffer, const std::uint64_t offset, const std::uint64_t bufferLength)
    {
        m_buffer = buffer;
        m_bufferLength = bufferLength;
        m_offset = offset;
        m_actingBlockLength = sbeBlockLength();
        m_actingVersion = sbeSchemaVersion();
        m_position = sbeCheckPosition(m_offset + m_actingBlockLength);
        return *this;
    }

    WorkerOp &wrapAndApplyHeader(char *buffer, const std::uint64_t offset, const std::uint64_t bufferLength)
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

    WorkerOp &wrapForDecode(
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

    WorkerOp &sbeRewind()
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
        WorkerOp skipper(m_buffer, m_offset, m_bufferLength, m_actingBlockLength, m_actingVersion);
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

    SBE_NODISCARD static const char *opTypeMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t opTypeId() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t opTypeSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool opTypeInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t opTypeEncodingOffset() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t opTypeEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t opTypeRaw() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 0, sizeof(std::uint8_t));
        return (val);
    }

    SBE_NODISCARD OpType::Value opType() const
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 0, sizeof(std::uint8_t));
        return OpType::get((val));
    }

    WorkerOp &opType(const OpType::Value value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 0, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *topicIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t topicIdId() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t topicIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool topicIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t topicIdEncodingOffset() SBE_NOEXCEPT
    {
        return 1;
    }

    static SBE_CONSTEXPR std::int32_t topicIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t topicIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t topicIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t topicIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t topicId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 1, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &topicId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 1, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *partitionMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t partitionId() SBE_NOEXCEPT
    {
        return 3;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t partitionSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool partitionInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t partitionEncodingOffset() SBE_NOEXCEPT
    {
        return 5;
    }

    static SBE_CONSTEXPR std::int32_t partitionNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t partitionMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t partitionMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t partitionEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t partition() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 5, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &partition(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 5, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *replicaMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replicaId() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replicaSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replicaInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replicaEncodingOffset() SBE_NOEXCEPT
    {
        return 9;
    }

    static SBE_CONSTEXPR std::uint64_t replicaNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT64;
    }

    static SBE_CONSTEXPR std::uint64_t replicaMinValue() SBE_NOEXCEPT
    {
        return UINT64_C(0x0);
    }

    static SBE_CONSTEXPR std::uint64_t replicaMaxValue() SBE_NOEXCEPT
    {
        return UINT64_C(0xfffffffffffffffe);
    }

    static SBE_CONSTEXPR std::size_t replicaEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::uint64_t replica() const SBE_NOEXCEPT
    {
        std::uint64_t val;
        std::memcpy(&val, m_buffer + m_offset + 9, sizeof(std::uint64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &replica(const std::uint64_t value) SBE_NOEXCEPT
    {
        std::uint64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 9, &val, sizeof(std::uint64_t));
        return *this;
    }

    SBE_NODISCARD static const char *logPtrMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t logPtrId() SBE_NOEXCEPT
    {
        return 5;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t logPtrSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool logPtrInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t logPtrEncodingOffset() SBE_NOEXCEPT
    {
        return 17;
    }

    static SBE_CONSTEXPR std::uint64_t logPtrNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT64;
    }

    static SBE_CONSTEXPR std::uint64_t logPtrMinValue() SBE_NOEXCEPT
    {
        return UINT64_C(0x0);
    }

    static SBE_CONSTEXPR std::uint64_t logPtrMaxValue() SBE_NOEXCEPT
    {
        return UINT64_C(0xfffffffffffffffe);
    }

    static SBE_CONSTEXPR std::size_t logPtrEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::uint64_t logPtr() const SBE_NOEXCEPT
    {
        std::uint64_t val;
        std::memcpy(&val, m_buffer + m_offset + 17, sizeof(std::uint64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &logPtr(const std::uint64_t value) SBE_NOEXCEPT
    {
        std::uint64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 17, &val, sizeof(std::uint64_t));
        return *this;
    }

    SBE_NODISCARD static const char *sessionIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t sessionIdId() SBE_NOEXCEPT
    {
        return 6;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t sessionIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool sessionIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t sessionIdEncodingOffset() SBE_NOEXCEPT
    {
        return 25;
    }

    static SBE_CONSTEXPR std::int64_t sessionIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t sessionIdMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t sessionIdMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t sessionIdEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t sessionId() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 25, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &sessionId(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 25, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *correlationIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t correlationIdId() SBE_NOEXCEPT
    {
        return 7;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t correlationIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool correlationIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t correlationIdEncodingOffset() SBE_NOEXCEPT
    {
        return 33;
    }

    static SBE_CONSTEXPR std::int64_t correlationIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t correlationIdMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t correlationIdMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t correlationIdEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t correlationId() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 33, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &correlationId(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 33, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *acksMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t acksId() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t acksSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool acksInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t acksEncodingOffset() SBE_NOEXCEPT
    {
        return 41;
    }

    static SBE_CONSTEXPR std::int16_t acksNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT16;
    }

    static SBE_CONSTEXPR std::int16_t acksMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::int16_t>(-32767);
    }

    static SBE_CONSTEXPR std::int16_t acksMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::int16_t>(32767);
    }

    static SBE_CONSTEXPR std::size_t acksEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::int16_t acks() const SBE_NOEXCEPT
    {
        std::int16_t val;
        std::memcpy(&val, m_buffer + m_offset + 41, sizeof(std::int16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    WorkerOp &acks(const std::int16_t value) SBE_NOEXCEPT
    {
        std::int16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 41, &val, sizeof(std::int16_t));
        return *this;
    }

    SBE_NODISCARD static const char *producerIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t producerIdId() SBE_NOEXCEPT
    {
        return 9;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t producerIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool producerIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t producerIdEncodingOffset() SBE_NOEXCEPT
    {
        return 43;
    }

    static SBE_CONSTEXPR std::int64_t producerIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t producerIdMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t producerIdMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t producerIdEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t producerId() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 43, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &producerId(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 43, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *producerEpochMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t producerEpochId() SBE_NOEXCEPT
    {
        return 10;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t producerEpochSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool producerEpochInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t producerEpochEncodingOffset() SBE_NOEXCEPT
    {
        return 51;
    }

    static SBE_CONSTEXPR std::int16_t producerEpochNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT16;
    }

    static SBE_CONSTEXPR std::int16_t producerEpochMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::int16_t>(-32767);
    }

    static SBE_CONSTEXPR std::int16_t producerEpochMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::int16_t>(32767);
    }

    static SBE_CONSTEXPR std::size_t producerEpochEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::int16_t producerEpoch() const SBE_NOEXCEPT
    {
        std::int16_t val;
        std::memcpy(&val, m_buffer + m_offset + 51, sizeof(std::int16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    WorkerOp &producerEpoch(const std::int16_t value) SBE_NOEXCEPT
    {
        std::int16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 51, &val, sizeof(std::int16_t));
        return *this;
    }

    SBE_NODISCARD static const char *baseSequenceMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t baseSequenceId() SBE_NOEXCEPT
    {
        return 11;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t baseSequenceSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool baseSequenceInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t baseSequenceEncodingOffset() SBE_NOEXCEPT
    {
        return 53;
    }

    static SBE_CONSTEXPR std::int32_t baseSequenceNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t baseSequenceMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t baseSequenceMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t baseSequenceEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t baseSequence() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 53, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &baseSequence(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 53, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *lastOffsetDeltaMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t lastOffsetDeltaId() SBE_NOEXCEPT
    {
        return 12;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t lastOffsetDeltaSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool lastOffsetDeltaInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t lastOffsetDeltaEncodingOffset() SBE_NOEXCEPT
    {
        return 57;
    }

    static SBE_CONSTEXPR std::int32_t lastOffsetDeltaNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t lastOffsetDeltaMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t lastOffsetDeltaMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t lastOffsetDeltaEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t lastOffsetDelta() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 57, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &lastOffsetDelta(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 57, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *recordCountMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t recordCountId() SBE_NOEXCEPT
    {
        return 13;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t recordCountSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool recordCountInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t recordCountEncodingOffset() SBE_NOEXCEPT
    {
        return 61;
    }

    static SBE_CONSTEXPR std::int32_t recordCountNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t recordCountMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t recordCountMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t recordCountEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t recordCount() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 61, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &recordCount(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 61, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *batchAttrsMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t batchAttrsId() SBE_NOEXCEPT
    {
        return 14;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t batchAttrsSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool batchAttrsInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t batchAttrsEncodingOffset() SBE_NOEXCEPT
    {
        return 65;
    }

    static SBE_CONSTEXPR std::uint16_t batchAttrsNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT16;
    }

    static SBE_CONSTEXPR std::uint16_t batchAttrsMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(0);
    }

    static SBE_CONSTEXPR std::uint16_t batchAttrsMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint16_t>(65534);
    }

    static SBE_CONSTEXPR std::size_t batchAttrsEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::uint16_t batchAttrs() const SBE_NOEXCEPT
    {
        std::uint16_t val;
        std::memcpy(&val, m_buffer + m_offset + 65, sizeof(std::uint16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    WorkerOp &batchAttrs(const std::uint16_t value) SBE_NOEXCEPT
    {
        std::uint16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 65, &val, sizeof(std::uint16_t));
        return *this;
    }

    SBE_NODISCARD static const char *deferAckMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t deferAckId() SBE_NOEXCEPT
    {
        return 15;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t deferAckSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool deferAckInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t deferAckEncodingOffset() SBE_NOEXCEPT
    {
        return 67;
    }

    static SBE_CONSTEXPR std::uint8_t deferAckNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT8;
    }

    static SBE_CONSTEXPR std::uint8_t deferAckMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(0);
    }

    static SBE_CONSTEXPR std::uint8_t deferAckMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(254);
    }

    static SBE_CONSTEXPR std::size_t deferAckEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t deferAck() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 67, sizeof(std::uint8_t));
        return (val);
    }

    WorkerOp &deferAck(const std::uint8_t value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 67, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *durabilityTierMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t durabilityTierId() SBE_NOEXCEPT
    {
        return 16;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t durabilityTierSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool durabilityTierInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t durabilityTierEncodingOffset() SBE_NOEXCEPT
    {
        return 68;
    }

    static SBE_CONSTEXPR std::uint8_t durabilityTierNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT8;
    }

    static SBE_CONSTEXPR std::uint8_t durabilityTierMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(0);
    }

    static SBE_CONSTEXPR std::uint8_t durabilityTierMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(254);
    }

    static SBE_CONSTEXPR std::size_t durabilityTierEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t durabilityTier() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 68, sizeof(std::uint8_t));
        return (val);
    }

    WorkerOp &durabilityTier(const std::uint8_t value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 68, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *enqueuedAtNsMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t enqueuedAtNsId() SBE_NOEXCEPT
    {
        return 17;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t enqueuedAtNsSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool enqueuedAtNsInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t enqueuedAtNsEncodingOffset() SBE_NOEXCEPT
    {
        return 69;
    }

    static SBE_CONSTEXPR std::int64_t enqueuedAtNsNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t enqueuedAtNsMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t enqueuedAtNsMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t enqueuedAtNsEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t enqueuedAtNs() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 69, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &enqueuedAtNs(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 69, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *subscriptionIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t subscriptionIdId() SBE_NOEXCEPT
    {
        return 18;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t subscriptionIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool subscriptionIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t subscriptionIdEncodingOffset() SBE_NOEXCEPT
    {
        return 77;
    }

    static SBE_CONSTEXPR std::int64_t subscriptionIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t subscriptionIdMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t subscriptionIdMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t subscriptionIdEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t subscriptionId() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 77, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &subscriptionId(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 77, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *nextOffsetMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t nextOffsetId() SBE_NOEXCEPT
    {
        return 19;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t nextOffsetSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool nextOffsetInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t nextOffsetEncodingOffset() SBE_NOEXCEPT
    {
        return 85;
    }

    static SBE_CONSTEXPR std::int64_t nextOffsetNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t nextOffsetMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t nextOffsetMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t nextOffsetEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t nextOffset() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 85, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &nextOffset(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 85, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *hwBoundMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t hwBoundId() SBE_NOEXCEPT
    {
        return 20;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t hwBoundSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool hwBoundInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t hwBoundEncodingOffset() SBE_NOEXCEPT
    {
        return 93;
    }

    static SBE_CONSTEXPR std::int64_t hwBoundNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t hwBoundMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t hwBoundMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t hwBoundEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t hwBound() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 93, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &hwBound(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 93, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *isolationMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t isolationId() SBE_NOEXCEPT
    {
        return 21;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t isolationSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool isolationInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t isolationEncodingOffset() SBE_NOEXCEPT
    {
        return 101;
    }

    static SBE_CONSTEXPR std::uint8_t isolationNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT8;
    }

    static SBE_CONSTEXPR std::uint8_t isolationMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(0);
    }

    static SBE_CONSTEXPR std::uint8_t isolationMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(254);
    }

    static SBE_CONSTEXPR std::size_t isolationEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t isolation() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 101, sizeof(std::uint8_t));
        return (val);
    }

    WorkerOp &isolation(const std::uint8_t value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 101, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *isGroupMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t isGroupId() SBE_NOEXCEPT
    {
        return 22;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t isGroupSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool isGroupInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t isGroupEncodingOffset() SBE_NOEXCEPT
    {
        return 102;
    }

    static SBE_CONSTEXPR std::uint8_t isGroupNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT8;
    }

    static SBE_CONSTEXPR std::uint8_t isGroupMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(0);
    }

    static SBE_CONSTEXPR std::uint8_t isGroupMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(254);
    }

    static SBE_CONSTEXPR std::size_t isGroupEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t isGroup() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 102, sizeof(std::uint8_t));
        return (val);
    }

    WorkerOp &isGroup(const std::uint8_t value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 102, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *groupCarrierReceivedMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t groupCarrierReceivedId() SBE_NOEXCEPT
    {
        return 23;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t groupCarrierReceivedSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool groupCarrierReceivedInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t groupCarrierReceivedEncodingOffset() SBE_NOEXCEPT
    {
        return 103;
    }

    static SBE_CONSTEXPR std::uint8_t groupCarrierReceivedNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT8;
    }

    static SBE_CONSTEXPR std::uint8_t groupCarrierReceivedMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(0);
    }

    static SBE_CONSTEXPR std::uint8_t groupCarrierReceivedMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::uint8_t>(254);
    }

    static SBE_CONSTEXPR std::size_t groupCarrierReceivedEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t groupCarrierReceived() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 103, sizeof(std::uint8_t));
        return (val);
    }

    WorkerOp &groupCarrierReceived(const std::uint8_t value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 103, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *roleEpochMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t roleEpochId() SBE_NOEXCEPT
    {
        return 24;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t roleEpochSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool roleEpochInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t roleEpochEncodingOffset() SBE_NOEXCEPT
    {
        return 104;
    }

    static SBE_CONSTEXPR std::int64_t roleEpochNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t roleEpochMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t roleEpochMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t roleEpochEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t roleEpoch() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 104, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &roleEpoch(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 104, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *roleLeaderIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t roleLeaderIdId() SBE_NOEXCEPT
    {
        return 25;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t roleLeaderIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool roleLeaderIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t roleLeaderIdEncodingOffset() SBE_NOEXCEPT
    {
        return 112;
    }

    static SBE_CONSTEXPR std::int32_t roleLeaderIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t roleLeaderIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t roleLeaderIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t roleLeaderIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t roleLeaderId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 112, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &roleLeaderId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 112, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *replLeaderEpochMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replLeaderEpochId() SBE_NOEXCEPT
    {
        return 26;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replLeaderEpochSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replLeaderEpochInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replLeaderEpochEncodingOffset() SBE_NOEXCEPT
    {
        return 116;
    }

    static SBE_CONSTEXPR std::int32_t replLeaderEpochNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t replLeaderEpochMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t replLeaderEpochMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t replLeaderEpochEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t replLeaderEpoch() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 116, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &replLeaderEpoch(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 116, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *replPartitionMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replPartitionId() SBE_NOEXCEPT
    {
        return 27;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replPartitionSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replPartitionInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replPartitionEncodingOffset() SBE_NOEXCEPT
    {
        return 120;
    }

    static SBE_CONSTEXPR std::int32_t replPartitionNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t replPartitionMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t replPartitionMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t replPartitionEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t replPartition() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 120, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &replPartition(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 120, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *replHighWatermarkMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replHighWatermarkId() SBE_NOEXCEPT
    {
        return 28;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replHighWatermarkSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replHighWatermarkInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replHighWatermarkEncodingOffset() SBE_NOEXCEPT
    {
        return 124;
    }

    static SBE_CONSTEXPR std::int64_t replHighWatermarkNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t replHighWatermarkMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t replHighWatermarkMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t replHighWatermarkEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t replHighWatermark() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 124, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &replHighWatermark(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 124, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *replLogStartOffsetMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replLogStartOffsetId() SBE_NOEXCEPT
    {
        return 29;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replLogStartOffsetSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replLogStartOffsetInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replLogStartOffsetEncodingOffset() SBE_NOEXCEPT
    {
        return 132;
    }

    static SBE_CONSTEXPR std::int64_t replLogStartOffsetNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t replLogStartOffsetMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t replLogStartOffsetMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t replLogStartOffsetEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t replLogStartOffset() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 132, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &replLogStartOffset(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 132, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *replErrorCodeMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replErrorCodeId() SBE_NOEXCEPT
    {
        return 30;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replErrorCodeSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replErrorCodeInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replErrorCodeEncodingOffset() SBE_NOEXCEPT
    {
        return 140;
    }

    static SBE_CONSTEXPR std::int16_t replErrorCodeNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT16;
    }

    static SBE_CONSTEXPR std::int16_t replErrorCodeMinValue() SBE_NOEXCEPT
    {
        return static_cast<std::int16_t>(-32767);
    }

    static SBE_CONSTEXPR std::int16_t replErrorCodeMaxValue() SBE_NOEXCEPT
    {
        return static_cast<std::int16_t>(32767);
    }

    static SBE_CONSTEXPR std::size_t replErrorCodeEncodingLength() SBE_NOEXCEPT
    {
        return 2;
    }

    SBE_NODISCARD std::int16_t replErrorCode() const SBE_NOEXCEPT
    {
        std::int16_t val;
        std::memcpy(&val, m_buffer + m_offset + 140, sizeof(std::int16_t));
        return SBE_LITTLE_ENDIAN_ENCODE_16(val);
    }

    WorkerOp &replErrorCode(const std::int16_t value) SBE_NOEXCEPT
    {
        std::int16_t val = SBE_LITTLE_ENDIAN_ENCODE_16(value);
        std::memcpy(m_buffer + m_offset + 140, &val, sizeof(std::int16_t));
        return *this;
    }

    SBE_NODISCARD static const char *replTopicIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t replTopicIdId() SBE_NOEXCEPT
    {
        return 31;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t replTopicIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool replTopicIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t replTopicIdEncodingOffset() SBE_NOEXCEPT
    {
        return 142;
    }

    static SBE_CONSTEXPR std::int32_t replTopicIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t replTopicIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t replTopicIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t replTopicIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t replTopicId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 142, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &replTopicId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 142, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *frLeaderEpochMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t frLeaderEpochId() SBE_NOEXCEPT
    {
        return 32;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t frLeaderEpochSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool frLeaderEpochInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t frLeaderEpochEncodingOffset() SBE_NOEXCEPT
    {
        return 146;
    }

    static SBE_CONSTEXPR std::int64_t frLeaderEpochNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t frLeaderEpochMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t frLeaderEpochMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t frLeaderEpochEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t frLeaderEpoch() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 146, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &frLeaderEpoch(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 146, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *frFollowerIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t frFollowerIdId() SBE_NOEXCEPT
    {
        return 33;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t frFollowerIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool frFollowerIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t frFollowerIdEncodingOffset() SBE_NOEXCEPT
    {
        return 154;
    }

    static SBE_CONSTEXPR std::int32_t frFollowerIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t frFollowerIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t frFollowerIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t frFollowerIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t frFollowerId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 154, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &frFollowerId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 154, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *frFetchOffsetMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t frFetchOffsetId() SBE_NOEXCEPT
    {
        return 34;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t frFetchOffsetSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool frFetchOffsetInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t frFetchOffsetEncodingOffset() SBE_NOEXCEPT
    {
        return 158;
    }

    static SBE_CONSTEXPR std::int64_t frFetchOffsetNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t frFetchOffsetMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t frFetchOffsetMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t frFetchOffsetEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t frFetchOffset() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 158, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &frFetchOffset(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 158, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *frMaxBytesMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t frMaxBytesId() SBE_NOEXCEPT
    {
        return 35;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t frMaxBytesSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool frMaxBytesInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t frMaxBytesEncodingOffset() SBE_NOEXCEPT
    {
        return 166;
    }

    static SBE_CONSTEXPR std::int32_t frMaxBytesNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t frMaxBytesMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t frMaxBytesMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t frMaxBytesEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t frMaxBytes() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 166, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &frMaxBytes(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 166, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *frNowNsMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t frNowNsId() SBE_NOEXCEPT
    {
        return 36;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t frNowNsSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool frNowNsInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t frNowNsEncodingOffset() SBE_NOEXCEPT
    {
        return 170;
    }

    static SBE_CONSTEXPR std::int64_t frNowNsNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t frNowNsMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t frNowNsMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t frNowNsEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t frNowNs() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 170, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &frNowNs(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 170, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *oleCurrentEpochMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t oleCurrentEpochId() SBE_NOEXCEPT
    {
        return 37;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t oleCurrentEpochSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool oleCurrentEpochInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t oleCurrentEpochEncodingOffset() SBE_NOEXCEPT
    {
        return 178;
    }

    static SBE_CONSTEXPR std::int64_t oleCurrentEpochNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t oleCurrentEpochMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t oleCurrentEpochMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t oleCurrentEpochEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t oleCurrentEpoch() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 178, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &oleCurrentEpoch(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 178, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *oleFollowerIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t oleFollowerIdId() SBE_NOEXCEPT
    {
        return 38;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t oleFollowerIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool oleFollowerIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t oleFollowerIdEncodingOffset() SBE_NOEXCEPT
    {
        return 186;
    }

    static SBE_CONSTEXPR std::int32_t oleFollowerIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t oleFollowerIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t oleFollowerIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t oleFollowerIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t oleFollowerId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 186, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &oleFollowerId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 186, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *oleRequestedEpochMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t oleRequestedEpochId() SBE_NOEXCEPT
    {
        return 39;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t oleRequestedEpochSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool oleRequestedEpochInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t oleRequestedEpochEncodingOffset() SBE_NOEXCEPT
    {
        return 190;
    }

    static SBE_CONSTEXPR std::int64_t oleRequestedEpochNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t oleRequestedEpochMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t oleRequestedEpochMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t oleRequestedEpochEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t oleRequestedEpoch() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 190, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &oleRequestedEpoch(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 190, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *oleFollowerLeoMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t oleFollowerLeoId() SBE_NOEXCEPT
    {
        return 40;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t oleFollowerLeoSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool oleFollowerLeoInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t oleFollowerLeoEncodingOffset() SBE_NOEXCEPT
    {
        return 198;
    }

    static SBE_CONSTEXPR std::int64_t oleFollowerLeoNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT64;
    }

    static SBE_CONSTEXPR std::int64_t oleFollowerLeoMinValue() SBE_NOEXCEPT
    {
        return INT64_C(-9223372036854775807);
    }

    static SBE_CONSTEXPR std::int64_t oleFollowerLeoMaxValue() SBE_NOEXCEPT
    {
        return INT64_C(9223372036854775807);
    }

    static SBE_CONSTEXPR std::size_t oleFollowerLeoEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::int64_t oleFollowerLeo() const SBE_NOEXCEPT
    {
        std::int64_t val;
        std::memcpy(&val, m_buffer + m_offset + 198, sizeof(std::int64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &oleFollowerLeo(const std::int64_t value) SBE_NOEXCEPT
    {
        std::int64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 198, &val, sizeof(std::int64_t));
        return *this;
    }

    SBE_NODISCARD static const char *egressPubMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t egressPubId() SBE_NOEXCEPT
    {
        return 42;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t egressPubSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool egressPubInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t egressPubEncodingOffset() SBE_NOEXCEPT
    {
        return 206;
    }

    static SBE_CONSTEXPR std::uint64_t egressPubNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT64;
    }

    static SBE_CONSTEXPR std::uint64_t egressPubMinValue() SBE_NOEXCEPT
    {
        return UINT64_C(0x0);
    }

    static SBE_CONSTEXPR std::uint64_t egressPubMaxValue() SBE_NOEXCEPT
    {
        return UINT64_C(0xfffffffffffffffe);
    }

    static SBE_CONSTEXPR std::size_t egressPubEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::uint64_t egressPub() const SBE_NOEXCEPT
    {
        std::uint64_t val;
        std::memcpy(&val, m_buffer + m_offset + 206, sizeof(std::uint64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &egressPub(const std::uint64_t value) SBE_NOEXCEPT
    {
        std::uint64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 206, &val, sizeof(std::uint64_t));
        return *this;
    }

    SBE_NODISCARD static const char *egressStreamIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t egressStreamIdId() SBE_NOEXCEPT
    {
        return 45;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t egressStreamIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool egressStreamIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t egressStreamIdEncodingOffset() SBE_NOEXCEPT
    {
        return 214;
    }

    static SBE_CONSTEXPR std::int32_t egressStreamIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t egressStreamIdMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t egressStreamIdMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t egressStreamIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t egressStreamId() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 214, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &egressStreamId(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 214, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *targetReplicaIdMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t targetReplicaIdId() SBE_NOEXCEPT
    {
        return 46;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t targetReplicaIdSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool targetReplicaIdInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t targetReplicaIdEncodingOffset() SBE_NOEXCEPT
    {
        return 218;
    }

    static SBE_CONSTEXPR std::uint32_t targetReplicaIdNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT32;
    }

    static SBE_CONSTEXPR std::uint32_t targetReplicaIdMinValue() SBE_NOEXCEPT
    {
        return UINT32_C(0x0);
    }

    static SBE_CONSTEXPR std::uint32_t targetReplicaIdMaxValue() SBE_NOEXCEPT
    {
        return UINT32_C(0xfffffffe);
    }

    static SBE_CONSTEXPR std::size_t targetReplicaIdEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::uint32_t targetReplicaId() const SBE_NOEXCEPT
    {
        std::uint32_t val;
        std::memcpy(&val, m_buffer + m_offset + 218, sizeof(std::uint32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &targetReplicaId(const std::uint32_t value) SBE_NOEXCEPT
    {
        std::uint32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 218, &val, sizeof(std::uint32_t));
        return *this;
    }

    SBE_NODISCARD static const char *incarnationMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t incarnationId() SBE_NOEXCEPT
    {
        return 47;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t incarnationSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool incarnationInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t incarnationEncodingOffset() SBE_NOEXCEPT
    {
        return 222;
    }

    static SBE_CONSTEXPR std::uint32_t incarnationNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT32;
    }

    static SBE_CONSTEXPR std::uint32_t incarnationMinValue() SBE_NOEXCEPT
    {
        return UINT32_C(0x0);
    }

    static SBE_CONSTEXPR std::uint32_t incarnationMaxValue() SBE_NOEXCEPT
    {
        return UINT32_C(0xfffffffe);
    }

    static SBE_CONSTEXPR std::size_t incarnationEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::uint32_t incarnation() const SBE_NOEXCEPT
    {
        std::uint32_t val;
        std::memcpy(&val, m_buffer + m_offset + 222, sizeof(std::uint32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &incarnation(const std::uint32_t value) SBE_NOEXCEPT
    {
        std::uint32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 222, &val, sizeof(std::uint32_t));
        return *this;
    }

    SBE_NODISCARD static const char *ownershipGenMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t ownershipGenId() SBE_NOEXCEPT
    {
        return 48;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t ownershipGenSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool ownershipGenInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t ownershipGenEncodingOffset() SBE_NOEXCEPT
    {
        return 226;
    }

    static SBE_CONSTEXPR std::uint64_t ownershipGenNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_UINT64;
    }

    static SBE_CONSTEXPR std::uint64_t ownershipGenMinValue() SBE_NOEXCEPT
    {
        return UINT64_C(0x0);
    }

    static SBE_CONSTEXPR std::uint64_t ownershipGenMaxValue() SBE_NOEXCEPT
    {
        return UINT64_C(0xfffffffffffffffe);
    }

    static SBE_CONSTEXPR std::size_t ownershipGenEncodingLength() SBE_NOEXCEPT
    {
        return 8;
    }

    SBE_NODISCARD std::uint64_t ownershipGen() const SBE_NOEXCEPT
    {
        std::uint64_t val;
        std::memcpy(&val, m_buffer + m_offset + 226, sizeof(std::uint64_t));
        return SBE_LITTLE_ENDIAN_ENCODE_64(val);
    }

    WorkerOp &ownershipGen(const std::uint64_t value) SBE_NOEXCEPT
    {
        std::uint64_t val = SBE_LITTLE_ENDIAN_ENCODE_64(value);
        std::memcpy(m_buffer + m_offset + 226, &val, sizeof(std::uint64_t));
        return *this;
    }

    SBE_NODISCARD static const char *minIsrMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t minIsrId() SBE_NOEXCEPT
    {
        return 49;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t minIsrSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool minIsrInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t minIsrEncodingOffset() SBE_NOEXCEPT
    {
        return 234;
    }

    static SBE_CONSTEXPR std::int32_t minIsrNullValue() SBE_NOEXCEPT
    {
        return SBE_NULLVALUE_INT32;
    }

    static SBE_CONSTEXPR std::int32_t minIsrMinValue() SBE_NOEXCEPT
    {
        return INT32_C(-2147483647);
    }

    static SBE_CONSTEXPR std::int32_t minIsrMaxValue() SBE_NOEXCEPT
    {
        return INT32_C(2147483647);
    }

    static SBE_CONSTEXPR std::size_t minIsrEncodingLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::int32_t minIsr() const SBE_NOEXCEPT
    {
        std::int32_t val;
        std::memcpy(&val, m_buffer + m_offset + 234, sizeof(std::int32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(val);
    }

    WorkerOp &minIsr(const std::int32_t value) SBE_NOEXCEPT
    {
        std::int32_t val = SBE_LITTLE_ENDIAN_ENCODE_32(value);
        std::memcpy(m_buffer + m_offset + 234, &val, sizeof(std::int32_t));
        return *this;
    }

    SBE_NODISCARD static const char *tailKindMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static SBE_CONSTEXPR std::uint16_t tailKindId() SBE_NOEXCEPT
    {
        return 43;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::uint64_t tailKindSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    SBE_NODISCARD bool tailKindInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t tailKindEncodingOffset() SBE_NOEXCEPT
    {
        return 238;
    }

    SBE_NODISCARD static SBE_CONSTEXPR std::size_t tailKindEncodingLength() SBE_NOEXCEPT
    {
        return 1;
    }

    SBE_NODISCARD std::uint8_t tailKindRaw() const SBE_NOEXCEPT
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 238, sizeof(std::uint8_t));
        return (val);
    }

    SBE_NODISCARD TailKind::Value tailKind() const
    {
        std::uint8_t val;
        std::memcpy(&val, m_buffer + m_offset + 238, sizeof(std::uint8_t));
        return TailKind::get((val));
    }

    WorkerOp &tailKind(const TailKind::Value value) SBE_NOEXCEPT
    {
        std::uint8_t val = (value);
        std::memcpy(m_buffer + m_offset + 238, &val, sizeof(std::uint8_t));
        return *this;
    }

    SBE_NODISCARD static const char *tailMetaAttribute(const MetaAttribute metaAttribute) SBE_NOEXCEPT
    {
        switch (metaAttribute)
        {
            case MetaAttribute::PRESENCE: return "required";
            default: return "";
        }
    }

    static const char *tailCharacterEncoding() SBE_NOEXCEPT
    {
        return "null";
    }

    static SBE_CONSTEXPR std::uint64_t tailSinceVersion() SBE_NOEXCEPT
    {
        return 0;
    }

    bool tailInActingVersion() SBE_NOEXCEPT
    {
        return true;
    }

    static SBE_CONSTEXPR std::uint16_t tailId() SBE_NOEXCEPT
    {
        return 44;
    }

    static SBE_CONSTEXPR std::uint64_t tailHeaderLength() SBE_NOEXCEPT
    {
        return 4;
    }

    SBE_NODISCARD std::uint32_t tailLength() const
    {
        std::uint32_t length;
        std::memcpy(&length, m_buffer + sbePosition(), sizeof(std::uint32_t));
        return SBE_LITTLE_ENDIAN_ENCODE_32(length);
    }

    std::uint64_t skipTail()
    {
        std::uint64_t lengthOfLengthField = 4;
        std::uint64_t lengthPosition = sbePosition();
        std::uint32_t lengthFieldValue;
        std::memcpy(&lengthFieldValue, m_buffer + lengthPosition, sizeof(std::uint32_t));
        std::uint64_t dataLength = SBE_LITTLE_ENDIAN_ENCODE_32(lengthFieldValue);
        sbePosition(lengthPosition + lengthOfLengthField + dataLength);
        return dataLength;
    }

    SBE_NODISCARD const char *tail()
    {
        std::uint32_t lengthFieldValue;
        std::memcpy(&lengthFieldValue, m_buffer + sbePosition(), sizeof(std::uint32_t));
        const char *fieldPtr = m_buffer + sbePosition() + 4;
        sbePosition(sbePosition() + 4 + SBE_LITTLE_ENDIAN_ENCODE_32(lengthFieldValue));
        return fieldPtr;
    }

    std::uint64_t getTail(char *dst, const std::uint64_t length)
    {
        std::uint64_t lengthOfLengthField = 4;
        std::uint64_t lengthPosition = sbePosition();
        sbePosition(lengthPosition + lengthOfLengthField);
        std::uint32_t lengthFieldValue;
        std::memcpy(&lengthFieldValue, m_buffer + lengthPosition, sizeof(std::uint32_t));
        std::uint64_t dataLength = SBE_LITTLE_ENDIAN_ENCODE_32(lengthFieldValue);
        std::uint64_t bytesToCopy = length < dataLength ? length : dataLength;
        std::uint64_t pos = sbePosition();
        sbePosition(pos + dataLength);
        std::memcpy(dst, m_buffer + pos, static_cast<std::size_t>(bytesToCopy));
        return bytesToCopy;
    }

    WorkerOp &putTail(const char *src, const std::uint32_t length)
    {
        std::uint64_t lengthOfLengthField = 4;
        std::uint64_t lengthPosition = sbePosition();
        std::uint32_t lengthFieldValue = SBE_LITTLE_ENDIAN_ENCODE_32(length);
        sbePosition(lengthPosition + lengthOfLengthField);
        std::memcpy(m_buffer + lengthPosition, &lengthFieldValue, sizeof(std::uint32_t));
        if (length != std::uint32_t(0))
        {
            std::uint64_t pos = sbePosition();
            sbePosition(pos + length);
            std::memcpy(m_buffer + pos, src, length);
        }
        return *this;
    }

    std::string getTailAsString()
    {
        std::uint64_t lengthOfLengthField = 4;
        std::uint64_t lengthPosition = sbePosition();
        sbePosition(lengthPosition + lengthOfLengthField);
        std::uint32_t lengthFieldValue;
        std::memcpy(&lengthFieldValue, m_buffer + lengthPosition, sizeof(std::uint32_t));
        std::uint64_t dataLength = SBE_LITTLE_ENDIAN_ENCODE_32(lengthFieldValue);
        std::uint64_t pos = sbePosition();
        const std::string result(m_buffer + pos, dataLength);
        sbePosition(pos + dataLength);
        return result;
    }

    std::string getTailAsJsonEscapedString()
    {
        std::ostringstream oss;
        std::string s = getTailAsString();

        for (const auto c : s)
        {
            switch (c)
            {
                case '"': oss << "\\\""; break;
                case '\\': oss << "\\\\"; break;
                case '\b': oss << "\\b"; break;
                case '\f': oss << "\\f"; break;
                case '\n': oss << "\\n"; break;
                case '\r': oss << "\\r"; break;
                case '\t': oss << "\\t"; break;

                default:
                    if ('\x00' <= c && c <= '\x1f')
                    {
                        oss << "\\u" << std::hex << std::setw(4)
                            << std::setfill('0') << (int)(c);
                    }
                    else
                    {
                        oss << c;
                    }
            }
        }

        return oss.str();
    }

    #ifdef SBE_USE_STRING_VIEW
    std::string_view getTailAsStringView()
    {
        std::uint64_t lengthOfLengthField = 4;
        std::uint64_t lengthPosition = sbePosition();
        sbePosition(lengthPosition + lengthOfLengthField);
        std::uint32_t lengthFieldValue;
        std::memcpy(&lengthFieldValue, m_buffer + lengthPosition, sizeof(std::uint32_t));
        std::uint64_t dataLength = SBE_LITTLE_ENDIAN_ENCODE_32(lengthFieldValue);
        std::uint64_t pos = sbePosition();
        const std::string_view result(m_buffer + pos, dataLength);
        sbePosition(pos + dataLength);
        return result;
    }
    #endif

    WorkerOp &putTail(const std::string &str)
    {
        if (str.length() > 1073741824)
        {
            throw std::runtime_error("std::string too long for length type [E109]");
        }
        return putTail(str.data(), static_cast<std::uint32_t>(str.length()));
    }

    #ifdef SBE_USE_STRING_VIEW
    WorkerOp &putTail(const std::string_view str)
    {
        if (str.length() > 1073741824)
        {
            throw std::runtime_error("std::string too long for length type [E109]");
        }
        return putTail(str.data(), static_cast<std::uint32_t>(str.length()));
    }
    #endif

template<typename CharT, typename Traits>
friend std::basic_ostream<CharT, Traits> & operator << (
    std::basic_ostream<CharT, Traits> &builder, const WorkerOp &_writer)
{
    WorkerOp writer(
        _writer.m_buffer,
        _writer.m_offset,
        _writer.m_bufferLength,
        _writer.m_actingBlockLength,
        _writer.m_actingVersion);

    builder << '{';
    builder << R"("Name": "WorkerOp", )";
    builder << R"("sbeTemplateId": )";
    builder << writer.sbeTemplateId();
    builder << ", ";

    builder << R"("opType": )";
    builder << '"' << writer.opType() << '"';

    builder << ", ";
    builder << R"("topicId": )";
    builder << +writer.topicId();

    builder << ", ";
    builder << R"("partition": )";
    builder << +writer.partition();

    builder << ", ";
    builder << R"("replica": )";
    builder << +writer.replica();

    builder << ", ";
    builder << R"("logPtr": )";
    builder << +writer.logPtr();

    builder << ", ";
    builder << R"("sessionId": )";
    builder << +writer.sessionId();

    builder << ", ";
    builder << R"("correlationId": )";
    builder << +writer.correlationId();

    builder << ", ";
    builder << R"("acks": )";
    builder << +writer.acks();

    builder << ", ";
    builder << R"("producerId": )";
    builder << +writer.producerId();

    builder << ", ";
    builder << R"("producerEpoch": )";
    builder << +writer.producerEpoch();

    builder << ", ";
    builder << R"("baseSequence": )";
    builder << +writer.baseSequence();

    builder << ", ";
    builder << R"("lastOffsetDelta": )";
    builder << +writer.lastOffsetDelta();

    builder << ", ";
    builder << R"("recordCount": )";
    builder << +writer.recordCount();

    builder << ", ";
    builder << R"("batchAttrs": )";
    builder << +writer.batchAttrs();

    builder << ", ";
    builder << R"("deferAck": )";
    builder << +writer.deferAck();

    builder << ", ";
    builder << R"("durabilityTier": )";
    builder << +writer.durabilityTier();

    builder << ", ";
    builder << R"("enqueuedAtNs": )";
    builder << +writer.enqueuedAtNs();

    builder << ", ";
    builder << R"("subscriptionId": )";
    builder << +writer.subscriptionId();

    builder << ", ";
    builder << R"("nextOffset": )";
    builder << +writer.nextOffset();

    builder << ", ";
    builder << R"("hwBound": )";
    builder << +writer.hwBound();

    builder << ", ";
    builder << R"("isolation": )";
    builder << +writer.isolation();

    builder << ", ";
    builder << R"("isGroup": )";
    builder << +writer.isGroup();

    builder << ", ";
    builder << R"("groupCarrierReceived": )";
    builder << +writer.groupCarrierReceived();

    builder << ", ";
    builder << R"("roleEpoch": )";
    builder << +writer.roleEpoch();

    builder << ", ";
    builder << R"("roleLeaderId": )";
    builder << +writer.roleLeaderId();

    builder << ", ";
    builder << R"("replLeaderEpoch": )";
    builder << +writer.replLeaderEpoch();

    builder << ", ";
    builder << R"("replPartition": )";
    builder << +writer.replPartition();

    builder << ", ";
    builder << R"("replHighWatermark": )";
    builder << +writer.replHighWatermark();

    builder << ", ";
    builder << R"("replLogStartOffset": )";
    builder << +writer.replLogStartOffset();

    builder << ", ";
    builder << R"("replErrorCode": )";
    builder << +writer.replErrorCode();

    builder << ", ";
    builder << R"("replTopicId": )";
    builder << +writer.replTopicId();

    builder << ", ";
    builder << R"("frLeaderEpoch": )";
    builder << +writer.frLeaderEpoch();

    builder << ", ";
    builder << R"("frFollowerId": )";
    builder << +writer.frFollowerId();

    builder << ", ";
    builder << R"("frFetchOffset": )";
    builder << +writer.frFetchOffset();

    builder << ", ";
    builder << R"("frMaxBytes": )";
    builder << +writer.frMaxBytes();

    builder << ", ";
    builder << R"("frNowNs": )";
    builder << +writer.frNowNs();

    builder << ", ";
    builder << R"("oleCurrentEpoch": )";
    builder << +writer.oleCurrentEpoch();

    builder << ", ";
    builder << R"("oleFollowerId": )";
    builder << +writer.oleFollowerId();

    builder << ", ";
    builder << R"("oleRequestedEpoch": )";
    builder << +writer.oleRequestedEpoch();

    builder << ", ";
    builder << R"("oleFollowerLeo": )";
    builder << +writer.oleFollowerLeo();

    builder << ", ";
    builder << R"("egressPub": )";
    builder << +writer.egressPub();

    builder << ", ";
    builder << R"("egressStreamId": )";
    builder << +writer.egressStreamId();

    builder << ", ";
    builder << R"("targetReplicaId": )";
    builder << +writer.targetReplicaId();

    builder << ", ";
    builder << R"("incarnation": )";
    builder << +writer.incarnation();

    builder << ", ";
    builder << R"("ownershipGen": )";
    builder << +writer.ownershipGen();

    builder << ", ";
    builder << R"("minIsr": )";
    builder << +writer.minIsr();

    builder << ", ";
    builder << R"("tailKind": )";
    builder << '"' << writer.tailKind() << '"';

    builder << ", ";
    builder << R"("tail": )";
    builder << '"' <<
        writer.skipTail() << " bytes of raw data\"";
    builder << '}';

    return builder;
}

void skip()
{
    skipTail();
}

SBE_NODISCARD static SBE_CONSTEXPR bool isConstLength() SBE_NOEXCEPT
{
    return false;
}

SBE_NODISCARD static std::size_t computeLength(std::size_t tailLength = 0)
{
#if defined(__GNUG__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtype-limits"
#endif
    std::size_t length = sbeBlockLength();

    length += tailHeaderLength();
    if (tailLength > 1073741824LL)
    {
        throw std::runtime_error("tailLength too long for length type [E109]");
    }
    length += tailLength;

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
