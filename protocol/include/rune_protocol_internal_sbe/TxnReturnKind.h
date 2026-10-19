/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _RUNE_PROTOCOL_INTERNAL_SBE_TXNRETURNKIND_CXX_H_
#define _RUNE_PROTOCOL_INTERNAL_SBE_TXNRETURNKIND_CXX_H_

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

class TxnReturnKind
{
public:
    enum Value
    {
        CONTROL_RECORD_COMMITTED = static_cast<std::uint8_t>(1),
        TXN_HANGING = static_cast<std::uint8_t>(2),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static TxnReturnKind::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(1): return CONTROL_RECORD_COMMITTED;
            case static_cast<std::uint8_t>(2): return TXN_HANGING;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum TxnReturnKind [E103]");
    }

    static const char *c_str(const TxnReturnKind::Value value)
    {
        switch (value)
        {
            case CONTROL_RECORD_COMMITTED: return "CONTROL_RECORD_COMMITTED";
            case TXN_HANGING: return "TXN_HANGING";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum TxnReturnKind [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, TxnReturnKind::Value m)
    {
        return os << TxnReturnKind::c_str(m);
    }
};

}
}
}
}

#endif
