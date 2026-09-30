/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _RUNE_PROTOCOL_INTERNAL_SBE_TAILKIND_CXX_H_
#define _RUNE_PROTOCOL_INTERNAL_SBE_TAILKIND_CXX_H_

#if !defined(__STDC_LIMIT_MACROS)
#  define __STDC_LIMIT_MACROS 1
#endif

#include <cstdint>
#include <iomanip>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <sstream>
#include <string>

#define SBE_NULLVALUE_INT8 (std::numeric_limits<std::int8_t>::min)()
#define SBE_NULLVALUE_INT16 (std::numeric_limits<std::int16_t>::min)()
#define SBE_NULLVALUE_INT32 (std::numeric_limits<std::int32_t>::min)()
#define SBE_NULLVALUE_INT64 (std::numeric_limits<std::int64_t>::min)()
#define SBE_NULLVALUE_UINT8 (std::numeric_limits<std::uint8_t>::max)()
#define SBE_NULLVALUE_UINT16 (std::numeric_limits<std::uint16_t>::max)()
#define SBE_NULLVALUE_UINT32 (std::numeric_limits<std::uint32_t>::max)()
#define SBE_NULLVALUE_UINT64 (std::numeric_limits<std::uint64_t>::max)()

namespace rune {
namespace protocol {
namespace internal {
namespace sbe {

class TailKind
{
public:
    enum Value
    {
        NONE = static_cast<std::uint8_t>(0),
        RECORD_BATCH = static_cast<std::uint8_t>(1),
        REPLICATED_RECORDS = static_cast<std::uint8_t>(2),
        ISR_LIST = static_cast<std::uint8_t>(3),
        OWNED_SET = static_cast<std::uint8_t>(4),
        CHUNK = static_cast<std::uint8_t>(5),
        DESCRIPTOR_BLOB = static_cast<std::uint8_t>(6),
        EGRESS_CHANNEL = static_cast<std::uint8_t>(7),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static TailKind::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return NONE;
            case static_cast<std::uint8_t>(1): return RECORD_BATCH;
            case static_cast<std::uint8_t>(2): return REPLICATED_RECORDS;
            case static_cast<std::uint8_t>(3): return ISR_LIST;
            case static_cast<std::uint8_t>(4): return OWNED_SET;
            case static_cast<std::uint8_t>(5): return CHUNK;
            case static_cast<std::uint8_t>(6): return DESCRIPTOR_BLOB;
            case static_cast<std::uint8_t>(7): return EGRESS_CHANNEL;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum TailKind [E103]");
    }

    static const char *c_str(const TailKind::Value value)
    {
        switch (value)
        {
            case NONE: return "NONE";
            case RECORD_BATCH: return "RECORD_BATCH";
            case REPLICATED_RECORDS: return "REPLICATED_RECORDS";
            case ISR_LIST: return "ISR_LIST";
            case OWNED_SET: return "OWNED_SET";
            case CHUNK: return "CHUNK";
            case DESCRIPTOR_BLOB: return "DESCRIPTOR_BLOB";
            case EGRESS_CHANNEL: return "EGRESS_CHANNEL";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum TailKind [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, TailKind::Value m)
    {
        return os << TailKind::c_str(m);
    }
};

}
}
}
}

#endif
