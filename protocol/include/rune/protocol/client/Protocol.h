#pragma once
#include <cstdint>
namespace rune::protocol::client {

inline constexpr uint16_t kSchemaId        = 120;  // client-facing schema (distinct from internal 111-114)
inline constexpr uint16_t kProtocolVersion = 3;  // read-rework: PartitionEntry egressControlPort+egressStreamId (20→28 bytes)

// Idempotent producer bound, shared by client and broker (one constant, so the
// two can never drift apart):
//   * client: an idempotent producer keeps at most this many un-acked batches in
//     flight per partition (maxInFlight is clamped to it);
//   * broker: the per-(producer, partition) dedup window (ProducerStateManager)
//     remembers this many recent batches, so a retransmit of ANY in-flight batch
//     is answered as a duplicate with its original offset.
// Kafka caps max.in.flight at 5 for the same reason; Rune pipelines deeper.
inline constexpr int kIdempotentMaxInFlightBatches = 32;

// client<->broker TLS/AEAD (Task 4): the crypto subheader schema id, and the
// clientVersion/brokerVersion gates new additive Connect/ConnectResponse
// fields (and HandshakeContinue{,Response}) are decoded under. kProtocolVersion
// (the MessageHeader wire version) is NOT bumped — these are purely additive,
// mirroring the existing blockLength/avail-gated idiom (see DataMessages.h
// Produce BODY_FIXED_V0/V1). kClientVersion/kBrokerVersion below (in the
// "Client/Broker versions" section) are bumped to 4 for this capability:
// crypto is the v4 capability layered on top of the v3 explicit-group-
// subscriptions capability.
inline constexpr uint16_t kCryptoSchemaId      = 121;
inline constexpr int32_t  kClientVersionCrypto = 4;  // minimum ConnectRequest.clientVersion offering crypto fields
inline constexpr int32_t  kBrokerVersionCrypto = 4;  // minimum broker version choosing crypto fields

enum class TemplateId : uint16_t {  // reserved ranges: control 1-15, produce/consume 16-31, groups/txn 32-63, view 64-95
    Connect          = 1,
    ConnectResponse  = 2,
    Keepalive        = 3,
    Disconnect       = 4,
    ErrorResponse    = 5,
    Ping             = 6,
    Pong             = 7,
    // MD1 Task 10: Metadata request/response — control range 8-9
    MetadataRequest  = 8,
    MetadataResponse = 9,
    // client<->broker TLS/AEAD (Task 4): handshake continuation — control range 10-11
    HandshakeContinue         = 10,
    HandshakeContinueResponse = 11,
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
    // schema/view range (64-95) — 0b schema registry
    DefineTopic          = 64,
    DefineTopicResponse  = 65,
    GetTopicSchema       = 66,
    TopicSchemaResponse  = 67,
    AlterTopic           = 68,
    AlterTopicResponse   = 69,
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
    // produce error codes
    MessageTooLarge      = 22,  // produce batch exceeds the broker's max batch size
    // schema-registry error codes (0b)
    InvalidSchema          = 23,  // descriptor blob malformed / invariant-violating
    SchemaValidationFailed = 24,  // produced record failed validation
    TopicExists            = 25,  // DefineTopic on an existing topic name
    // storage error codes
    StorageError           = 26,  // the partition's log failed an fsync (fail-stop, KafkaStorageError analogue)
    // txn: the previous transaction's markers have not all landed yet (Kafka
    // CONCURRENT_TRANSACTIONS) — RETRIABLE: the producer retries the request.
    ConcurrentTransactions = 27,
    // D6 (Kafka UNKNOWN_PRODUCER_ID, 59): the broker's PROVEN producer state has
    // no entry for (topic, producerId, partition) and the batch's baseSequence
    // is not 0 — retention / compaction / memory-ring eviction removed every
    // batch of this producer before a rebuild (failover, restart, re-promotion).
    // The batch was NOT appended.  ProduceResponse.logStartOffset carries the
    // partition's log start so the client can tell retention (every record it
    // had acked is below it -> re-sequence, nothing fails) from loss (outcome
    // unknown).  Sent ONLY to clients whose ConnectRequest.clientVersion is
    // >= kClientVersionUnknownProducerId; older clients get OutOfOrderSequence.
    UnknownProducerId      = 28,
    // client<->broker TLS/AEAD (Task 4)
    HandshakeFailed        = 29,  // crypto handshake could not complete (cipher mismatch, bad token, decrypt failure, ...)
    // schema-evolution error codes (AlterTopic, 0c)
    SchemaIncompatible     = 30,  // proposed AlterTopic ops are incompatible with existing data/consumers
    SchemaConflict         = 31,  // AlterTopic.expectedVersion stale — topic schema changed since the caller last read it
};

// ---------------------------------------------------------------------------
// Client versions (ConnectRequest.clientVersion) — behaviour gates.  The wire
// header version (kProtocolVersion) is NOT bumped for additive changes: an old
// checkFrame rejects any newer header version.
//   1 — the original client.
//   2 — D6: understands ErrorCode::UnknownProducerId (28) and
//       ProduceResponse.logStartOffset.
//   3 — explicit consumer-group subscriptions: a group member subscribes to
//       each assigned partition's LEADER itself (after OffsetFetch, from the
//       committed offset) and demultiplexes Records by (subscriptionId,
//       correlationId); the coordinator creates no subscription at SyncGroup.
//       A broker coordinator refuses JoinGroup from a client below 3
//       (ErrorCode::ProtocolVersion, non-retriable).
//   4 — client<->broker TLS/AEAD (Task 4): offers crypto fields on Connect
//       (cipherList/handshakeBytes/authToken) and understands
//       HandshakeContinue/HandshakeContinueResponse. See kClientVersionCrypto.
// ---------------------------------------------------------------------------
inline constexpr int32_t kClientVersionUnknownProducerId          = 2;
inline constexpr int32_t kClientVersionExplicitGroupSubscriptions = 3;
inline constexpr int32_t kClientVersion                           = 4;   // what this protocol's client announces

// ---------------------------------------------------------------------------
// Broker versions (ConnectResponse.brokerVersion, an additive trailing field;
// a broker that predates it sends the 38-byte frame = version 0).
//   3 — explicit consumer-group subscriptions (see client version 3): the
//       coordinator does not auto-subscribe members, Records carry the
//       creating Subscribe's correlationId, session / subscription ids are
//       cluster-unique.  A client runs a GroupConsumer only against >= 3.
//   4 — client<->broker TLS/AEAD (Task 4): may choose a cipher / send
//       handshake bytes on ConnectResponse. See kBrokerVersionCrypto.
// ---------------------------------------------------------------------------
inline constexpr int32_t kBrokerVersionExplicitGroupSubscriptions = 3;
inline constexpr int32_t kBrokerVersion                           = 4;   // what this protocol's broker announces

/// True when a session that connected with `clientVersion` may be answered
/// UnknownProducerId (else the broker answers OutOfOrderSequence, as before).
[[nodiscard]] constexpr bool clientUnderstandsUnknownProducerId(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionUnknownProducerId;
}

/// The produce error code a session that connected with `clientVersion` is
/// answered: UnknownProducerId becomes OutOfOrderSequence (the pre-D6 answer)
/// for clients below kClientVersionUnknownProducerId; every other code passes
/// through.  The ONE definition every broker answer site uses (review L4).
[[nodiscard]] constexpr ErrorCode produceErrorForClient(ErrorCode ec, int32_t clientVersion) noexcept {
    return (ec == ErrorCode::UnknownProducerId && !clientUnderstandsUnknownProducerId(clientVersion))
               ? ErrorCode::OutOfOrderSequence
               : ec;
}

} // namespace rune::protocol::client
