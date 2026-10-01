/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _RUNE_PROTOCOL_INTERNAL_SBE_RETURNTYPE_CXX_H_
#define _RUNE_PROTOCOL_INTERNAL_SBE_RETURNTYPE_CXX_H_

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

class ReturnType
{
public:
    enum Value
    {
        PRODUCE_RESULT = static_cast<std::uint8_t>(0),
        PUSH_READ_RESULT = static_cast<std::uint8_t>(1),
        PRODUCE_NOTIFY = static_cast<std::uint8_t>(2),
        STORAGE_FAILURE = static_cast<std::uint8_t>(3),
        OWNERSHIP_ACK = static_cast<std::uint8_t>(4),
        CONTROL_RECORD_REFUSED = static_cast<std::uint8_t>(5),
        CONTROL_RECORD_APPENDED = static_cast<std::uint8_t>(6),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static ReturnType::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return PRODUCE_RESULT;
            case static_cast<std::uint8_t>(1): return PUSH_READ_RESULT;
            case static_cast<std::uint8_t>(2): return PRODUCE_NOTIFY;
            case static_cast<std::uint8_t>(3): return STORAGE_FAILURE;
            case static_cast<std::uint8_t>(4): return OWNERSHIP_ACK;
            case static_cast<std::uint8_t>(5): return CONTROL_RECORD_REFUSED;
            case static_cast<std::uint8_t>(6): return CONTROL_RECORD_APPENDED;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum ReturnType [E103]");
    }

    static const char *c_str(const ReturnType::Value value)
    {
        switch (value)
        {
            case PRODUCE_RESULT: return "PRODUCE_RESULT";
            case PUSH_READ_RESULT: return "PUSH_READ_RESULT";
            case PRODUCE_NOTIFY: return "PRODUCE_NOTIFY";
            case STORAGE_FAILURE: return "STORAGE_FAILURE";
            case OWNERSHIP_ACK: return "OWNERSHIP_ACK";
            case CONTROL_RECORD_REFUSED: return "CONTROL_RECORD_REFUSED";
            case CONTROL_RECORD_APPENDED: return "CONTROL_RECORD_APPENDED";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum ReturnType [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, ReturnType::Value m)
    {
        return os << ReturnType::c_str(m);
    }
};

}
}
}
}

#endif
