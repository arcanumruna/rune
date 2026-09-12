#pragma once
#include <cstdint>
namespace rune::protocol::client {

inline constexpr uint16_t kSchemaId        = 120;  // client-facing schema (distinct from internal 111-114)
inline constexpr uint16_t kProtocolVersion = 1;

enum class TemplateId : uint16_t {  // reserved ranges: control 1-15, produce/consume 16-31, groups/txn 32-63, view 64-95
    Connect         = 1,
    ConnectResponse = 2,
    Keepalive       = 3,
    Disconnect      = 4,
    ErrorResponse   = 5,
    Ping            = 6,
    Pong            = 7,
    // produce/consume range (16-31)
    Produce             = 16,
    ProduceResponse     = 17,
    Subscribe           = 18,
    SubscribeResponse   = 19,
    Records             = 20,
    Unsubscribe         = 21,
};

enum class ErrorCode : uint16_t {
    OK                   = 0,
    ProtocolVersion      = 1,
    NotLeader            = 2,
    Redirect             = 3,
    SessionExpired       = 4,
    UnknownSession       = 5,
    Internal             = 6,
    UnknownTopic         = 7,
    UnknownPartition     = 8,
    NotPartitionLeader   = 9,
};

} // namespace rune::protocol::client
