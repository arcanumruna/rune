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
    // groups/txn range (32-63)
    JoinGroup            = 32,
    JoinGroupResponse    = 33,
    SyncGroup            = 34,
    SyncGroupResponse    = 35,
    Heartbeat            = 36,
    HeartbeatResponse    = 37,
    LeaveGroup           = 38,
    LeaveGroupResponse   = 39,
    OffsetCommit         = 40,
    OffsetCommitResponse = 41,
    OffsetFetch          = 42,
    OffsetFetchResponse  = 43,
    Assign               = 44,
    // txn range (45-54, inside groups/txn 32-63)
    InitProducerId              = 45,
    InitProducerIdResponse      = 46,
    AddPartitionsToTxn          = 47,
    AddPartitionsToTxnResponse  = 48,
    AddOffsetsToTxn             = 49,
    AddOffsetsToTxnResponse     = 50,
    TxnOffsetCommit             = 51,
    TxnOffsetCommitResponse     = 52,
    EndTxn                      = 53,
    EndTxnResponse              = 54,
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
    MinIsrViolation      = 10,  // acks=All rejected — ISR below minIsr
    // consumer-group error codes (CG-1)
    StaleGeneration      = 11,
    UnknownMember        = 12,
    RebalanceInProgress  = 13,
    NotCoordinator       = 14,
    IllegalGeneration    = 15,
    GroupDead            = 16,
    // txn error codes (TX-1)
    InvalidProducerEpoch = 17,
    ProducerFenced       = 18,
    InvalidTxnState      = 19,
    OutOfOrderSequence   = 20,
    DuplicateSequence    = 21,
};

} // namespace rune::protocol::client
