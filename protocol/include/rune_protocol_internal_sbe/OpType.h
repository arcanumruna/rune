/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _RUNE_PROTOCOL_INTERNAL_SBE_OPTYPE_CXX_H_
#define _RUNE_PROTOCOL_INTERNAL_SBE_OPTYPE_CXX_H_

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

class OpType
{
public:
    enum Value
    {
        PRODUCE = static_cast<std::uint8_t>(0),
        PUSH_READ = static_cast<std::uint8_t>(1),
        APPLY_REPLICATED = static_cast<std::uint8_t>(2),
        BECOME_LEADER = static_cast<std::uint8_t>(3),
        BECOME_FOLLOWER = static_cast<std::uint8_t>(4),
        SET_OWNERSHIP = static_cast<std::uint8_t>(5),
        FETCH_REQUEST = static_cast<std::uint8_t>(6),
        OFFSETS_FOR_LEADER_EPOCH_REQ = static_cast<std::uint8_t>(7),
        RECONCILE_FETCH_RESP = static_cast<std::uint8_t>(8),
        APPEND_CONTROL_RECORD = static_cast<std::uint8_t>(9),
        SUBSCRIBE = static_cast<std::uint8_t>(10),
        UNSUBSCRIBE = static_cast<std::uint8_t>(11),
        SCHEMA_UPDATE = static_cast<std::uint8_t>(12),
        SESSION_ANNOUNCE = static_cast<std::uint8_t>(13),
        SESSION_CLOSE = static_cast<std::uint8_t>(14),
        OFFSETS_FOR_LEADER_EPOCH_RESP = static_cast<std::uint8_t>(15),
        RECONCILE_FETCH_REQ = static_cast<std::uint8_t>(16),
        SET_ADMISSION_POLICY = static_cast<std::uint8_t>(17),
        ALTER_ISR_RESPONSE = static_cast<std::uint8_t>(18),
        GET_VIEW_ROW = static_cast<std::uint8_t>(19),
        TOPIC_DROPPED = static_cast<std::uint8_t>(20),
        SESSION_PERMISSIONS = static_cast<std::uint8_t>(21),
        PEER_LINK_KEYS = static_cast<std::uint8_t>(22),
        ENFORCE_FENCE = static_cast<std::uint8_t>(23),
        READ_LEASE = static_cast<std::uint8_t>(24),
        FILTER_SCHEMA = static_cast<std::uint8_t>(25),
        TXN_VERIFY_RESULT = static_cast<std::uint8_t>(26),
        NS_SLOT_ASSIGN = static_cast<std::uint8_t>(27),
        NS_SLOT_RELEASE = static_cast<std::uint8_t>(28),
        NS_THROTTLE = static_cast<std::uint8_t>(29),
        NS_VIEW_MEM_OVER = static_cast<std::uint8_t>(30),
        TEST_TASK = static_cast<std::uint8_t>(200),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static OpType::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return PRODUCE;
            case static_cast<std::uint8_t>(1): return PUSH_READ;
            case static_cast<std::uint8_t>(2): return APPLY_REPLICATED;
            case static_cast<std::uint8_t>(3): return BECOME_LEADER;
            case static_cast<std::uint8_t>(4): return BECOME_FOLLOWER;
            case static_cast<std::uint8_t>(5): return SET_OWNERSHIP;
            case static_cast<std::uint8_t>(6): return FETCH_REQUEST;
            case static_cast<std::uint8_t>(7): return OFFSETS_FOR_LEADER_EPOCH_REQ;
            case static_cast<std::uint8_t>(8): return RECONCILE_FETCH_RESP;
            case static_cast<std::uint8_t>(9): return APPEND_CONTROL_RECORD;
            case static_cast<std::uint8_t>(10): return SUBSCRIBE;
            case static_cast<std::uint8_t>(11): return UNSUBSCRIBE;
            case static_cast<std::uint8_t>(12): return SCHEMA_UPDATE;
            case static_cast<std::uint8_t>(13): return SESSION_ANNOUNCE;
            case static_cast<std::uint8_t>(14): return SESSION_CLOSE;
            case static_cast<std::uint8_t>(15): return OFFSETS_FOR_LEADER_EPOCH_RESP;
            case static_cast<std::uint8_t>(16): return RECONCILE_FETCH_REQ;
            case static_cast<std::uint8_t>(17): return SET_ADMISSION_POLICY;
            case static_cast<std::uint8_t>(18): return ALTER_ISR_RESPONSE;
            case static_cast<std::uint8_t>(19): return GET_VIEW_ROW;
            case static_cast<std::uint8_t>(20): return TOPIC_DROPPED;
            case static_cast<std::uint8_t>(21): return SESSION_PERMISSIONS;
            case static_cast<std::uint8_t>(22): return PEER_LINK_KEYS;
            case static_cast<std::uint8_t>(23): return ENFORCE_FENCE;
            case static_cast<std::uint8_t>(24): return READ_LEASE;
            case static_cast<std::uint8_t>(25): return FILTER_SCHEMA;
            case static_cast<std::uint8_t>(26): return TXN_VERIFY_RESULT;
            case static_cast<std::uint8_t>(27): return NS_SLOT_ASSIGN;
            case static_cast<std::uint8_t>(28): return NS_SLOT_RELEASE;
            case static_cast<std::uint8_t>(29): return NS_THROTTLE;
            case static_cast<std::uint8_t>(30): return NS_VIEW_MEM_OVER;
            case static_cast<std::uint8_t>(200): return TEST_TASK;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum OpType [E103]");
    }

    static const char *c_str(const OpType::Value value)
    {
        switch (value)
        {
            case PRODUCE: return "PRODUCE";
            case PUSH_READ: return "PUSH_READ";
            case APPLY_REPLICATED: return "APPLY_REPLICATED";
            case BECOME_LEADER: return "BECOME_LEADER";
            case BECOME_FOLLOWER: return "BECOME_FOLLOWER";
            case SET_OWNERSHIP: return "SET_OWNERSHIP";
            case FETCH_REQUEST: return "FETCH_REQUEST";
            case OFFSETS_FOR_LEADER_EPOCH_REQ: return "OFFSETS_FOR_LEADER_EPOCH_REQ";
            case RECONCILE_FETCH_RESP: return "RECONCILE_FETCH_RESP";
            case APPEND_CONTROL_RECORD: return "APPEND_CONTROL_RECORD";
            case SUBSCRIBE: return "SUBSCRIBE";
            case UNSUBSCRIBE: return "UNSUBSCRIBE";
            case SCHEMA_UPDATE: return "SCHEMA_UPDATE";
            case SESSION_ANNOUNCE: return "SESSION_ANNOUNCE";
            case SESSION_CLOSE: return "SESSION_CLOSE";
            case OFFSETS_FOR_LEADER_EPOCH_RESP: return "OFFSETS_FOR_LEADER_EPOCH_RESP";
            case RECONCILE_FETCH_REQ: return "RECONCILE_FETCH_REQ";
            case SET_ADMISSION_POLICY: return "SET_ADMISSION_POLICY";
            case ALTER_ISR_RESPONSE: return "ALTER_ISR_RESPONSE";
            case GET_VIEW_ROW: return "GET_VIEW_ROW";
            case TOPIC_DROPPED: return "TOPIC_DROPPED";
            case SESSION_PERMISSIONS: return "SESSION_PERMISSIONS";
            case PEER_LINK_KEYS: return "PEER_LINK_KEYS";
            case ENFORCE_FENCE: return "ENFORCE_FENCE";
            case READ_LEASE: return "READ_LEASE";
            case FILTER_SCHEMA: return "FILTER_SCHEMA";
            case TXN_VERIFY_RESULT: return "TXN_VERIFY_RESULT";
            case NS_SLOT_ASSIGN: return "NS_SLOT_ASSIGN";
            case NS_SLOT_RELEASE: return "NS_SLOT_RELEASE";
            case NS_THROTTLE: return "NS_THROTTLE";
            case NS_VIEW_MEM_OVER: return "NS_VIEW_MEM_OVER";
            case TEST_TASK: return "TEST_TASK";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum OpType [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, OpType::Value m)
    {
        return os << OpType::c_str(m);
    }
};

}
}
}
}

#endif
