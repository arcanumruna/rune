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
    // authentication (client version 9): SCRAM-SHA-512 exchange — control range 12-15
    AuthClientFirst           = 12,
    AuthServerFirst           = 13,
    AuthClientFinal           = 14,
    AuthServerFinal           = 15,
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
    // view-engine range (0c follow-on, Task 18): CreateView/GetViewRow
    CreateView           = 70,
    CreateViewResponse   = 71,
    GetViewRow           = 72,
    ViewRowResponse      = 73,
    // catalog history (client version 8): the remaining DDL ops, all answered by DdlResponse
    DropTopic            = 74,
    AlterTopicConfig     = 75,
    DropView             = 76,
    AlterView            = 77,
    DdlResponse          = 78,
    // authentication (client version 9): security DDL + one-time bootstrap, both answered by DdlResponse (78)
    SecurityDdl          = 79,
    SecurityBootstrap    = 80,
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
    // view-engine error codes (Task 18): CreateView/GetViewRow
    ViewExists             = 32,  // CreateView on an existing view name
    ViewSourceNotFound     = 33,  // CreateView's planBlob references a source topic that does not exist
    ViewFieldUnresolved    = 34,  // CreateView's planBlob references an output/key field the source schema does not have
    ViewKeyInvalid         = 35,  // GetViewRow.keyU64 does not resolve under the view's keyField type
    ViewNotCoPartitioned   = 36,  // CreateView: an aggregate view over a source with >1 partition
                                  // (Phase 2a: contributors are partitioned by wire key, which the
                                  // broker does not tie to the group-by field, so any aggregate over
                                  // a multi-partition source is rejected)
    ViewSourceTransient    = 37,  // CreateView: a persisted view over a transient source topic
                                  // (a view that reads a transient topic must itself be transient)
    ViewNotAlterable       = 38,  // AlterTopic on a VIEW's output topic (a view's schema is derived
                                  // from its plan and is never altered directly)
    InvalidRequest         = 39,  // the request's MessageHeader decoded but its body did not
                                  // (truncated / malformed / unknown enum byte) — answered on the
                                  // header's correlationId so the client never waits out a timeout.
                                  // Non-retriable: re-sending the same bytes fails the same way.
    InvalidTopicName       = 40,  // DefineTopic/CreateView: the topic/view name is empty, longer than
                                  // 249 bytes, "." or "..", has a byte outside [A-Za-z0-9._-], or
                                  // starts with the reserved "__" prefix (internal topics)
    ViewSourceIsView       = 41,  // CreateView: a source is another view's output (a view sink carries
                                  // no change events yet, so the view would stay empty; until Phase 3)
    TransientLogFull       = 42,  // Produce / txn marker (Phase 2b E2): a TRANSIENT partition's in-memory
                                  // ring is full of records not yet replicated (at/above the high
                                  // watermark) — NOTHING was appended.  RETRIABLE on the SAME leader
                                  // after a backoff, without a metadata refresh.  Answered only to
                                  // clients >= kClientVersionTransientLogFull (6); older clients get
                                  // NotLeader (their retriable code) — see produceErrorForClient.
    PartitionDataReset     = 43,  // Subscribe (Phase 2b E2, residual round 6): a RESUMED plain
                                  // subscription claimed (Subscribe.key, see kClientVersionLineageNote)
                                  // a position in a TRANSIENT partition's data lineage that did not
                                  // survive (the ring restarted empty).  NO subscription is registered;
                                  // the client resubscribes from the log start (no resume floor) and
                                  // surfaces the reset to the application.  Sent as ErrorResponse on
                                  // the Subscribe's correlationId, only to clients >= 7 (only they claim).
    AuthenticationFailed   = 44,  // client version 9: SCRAM failed / not authenticated / auth deadline
                                  // (see the version-9 note); with retryAfterMs > 0 a rate-limit.
    NotAuthorized          = 45,  // client version 9: the request is not granted (default deny)
    SessionRevoked         = 46,  // client version 9: the session's user was dropped / disabled /
                                  // terminated; terminal (corr 0 = the session-level notice)
    PasswordChangeRequired = 47,  // client version 9: a one-time-password session may only change its password
    AuthUnavailable        = 48,  // issues #15 / #111 (decision M7): this broker's `__security` state is not provably
                                  // current (catching up after a boot / wipe, partitioned from the leader or the
                                  // consensus): it serves no login, no `__security` read and no security DDL.  On an
                                  // AuthServerFinal / DdlResponse / SubscribeResponse: RETRIABLE - back off and try
                                  // another broker (rotate seeds).  As the session-level ErrorResponse (corr 0): the
                                  // broker stayed stale too long and ended the session - reconnect elsewhere.  Sent to
                                  // every client version (an older client treats it as a retriable connect failure).
    RecordEgressUnavailable = 49, // issue #118 / PR #441: an authenticating broker could not build the session's own
                                  // record-egress stream for the partition's worker (it never falls back to the shared
                                  // one).  Answered as SubscribeResponse.errorCode on the still-pending Subscribe (its
                                  // OK is held until the egress exists).  RETRIABLE a few times (the client's small
                                  // per-subscription budget, reset by a bind), then a terminal, explicit failure.
    UnstableOffsetCommit   = 50,  // client version 10 (issue #438): OffsetFetch / OffsetCommit of a (group, partition)
                                  // an unfinished transaction holds (its TxnOffsetCommit is pending until the
                                  // transaction's Complete* record commits), or while the transaction coordinator is
                                  // not loaded on the group coordinator's node (unknown = held).  RETRIABLE: the
                                  // client re-sends after its backoff; never surfaced as a failure.  Clients below 10
                                  // are answered NotCoordinator instead (their retriable code).
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
//   5 — no-hang wave: understands ErrorCode::InvalidRequest (39) on every
//       response type (non-retriable: the request fails / the group consumer
//       is refused, never re-sent), ViewRowResponse statuses 5..7, and
//       finishes a subscription whose SubscribeResponse carries an error.  A
//       broker answers a malformed JoinGroup / SyncGroup from a client below 5
//       with ProtocolVersion (which those clients already treat as
//       non-retriable) instead of InvalidRequest / Internal (which they would
//       re-join on, forever).
//   6 — Phase 2b E2: understands ErrorCode::TransientLogFull (42) on
//       ProduceResponse — retried after a backoff with the same identity and
//       no metadata refresh.  A broker answers a session below 6 NotLeader
//       instead (produceErrorForClient).
//       Also (E2 Task 13): a subscription its node can no longer serve
//       (demoted / partition moved away) is answered ErrorResponse{NotLeader}
//       on its Subscribe correlationId; a session >= 6 is first sent a
//       POSITION NOTE — a Records frame of that subscription with an EMPTY
//       recordBatch whose baseOffset is the broker's cursor — so a moved
//       subscription that delivered nothing resumes there (never re-resolves
//       Latest).  Clients below 6 ignore an empty Records batch.
//       Also (E2 Task 14): when the broker already knows ANOTHER leader for a
//       moved plain subscription (its routing image names it and its client
//       endpoint is known), a session >= 6 is answered
//       ErrorResponse{Redirect, that endpoint} instead of the bare NotLeader
//       (after the same position note); the client follows it like any
//       Subscribe Redirect.  Group-member partition subscriptions and
//       sessions below 6 keep the bare NotLeader.
//       (E2 final fix wave I2) The Rune v6 client follows a moved
//       subscription's Redirect PER PARTITION: it re-subscribes from its
//       position on the new leader's own session and never moves the session
//       its other subscriptions ride on, so later answers / pushes of that
//       subscription arrive on that node's session.  No format change.
// ---------------------------------------------------------------------------
inline constexpr int32_t kClientVersionUnknownProducerId          = 2;
inline constexpr int32_t kClientVersionExplicitGroupSubscriptions = 3;
inline constexpr int32_t kClientVersionInvalidRequest             = 5;
inline constexpr int32_t kClientVersionTransientLogFull           = 6;
inline constexpr int32_t kClientVersionMovedPositionNote          = 6;
// 7 (Phase 2b E2, residual round 6) — the transient LINEAGE NOTE and claim:
//   - broker → client: after binding a plain subscription on a TRANSIENT
//     partition (and again once its data lineage resolves, if it was still
//     unknown at bind) the broker sends a Records frame of that subscription
//     whose recordBatch is EXACTLY 8 bytes — the partition's data-lineage
//     token, little-endian (baseOffset -1).  No valid RecordBatch is 8 bytes:
//     an older client drops it as malformed.
//   - client → broker: a RESUMED Subscribe (partitionKnown = 1, from a
//     position the subscription already reached) carries the token it holds
//     in its `key` field (8 bytes LE; the key is unused when the partition is
//     known).  The broker answers PartitionDataReset (43) when the token is
//     not the partition's current lineage, the retriable NotLeader while the
//     lineage is unknown, and serves it otherwise.  No format change.
inline constexpr int32_t kClientVersionLineageNote                = 7;
// 8 — catalog history: DDL extension trailer (statement, clientName, 128-bit
//   requestId, replayedFrom), DefineTopic replicationFactor + topic config,
//   DropTopic/AlterTopicConfig/DropView/AlterView (74–77) answered by
//   DdlResponse (78).  DDL to a broker that is not the `__catalog` leader is
//   answered ErrorResponse{Redirect, leader client endpoint}; a leader change,
//   a catching-up leader, a commit that did not complete in time or
//   `__catalog` below minIsr answers NotLeader (retriable; the retry with the
//   same requestId is deduplicated); a broker whose catalog apply halted
//   answers Internal.  Any DDL may be answered ErrorResponse{InvalidRequest,
//   detail} (e.g. replicationFactor bounds, a requestId reused for a
//   different statement).  A session below 8 never receives a Redirect for
//   DDL: on a broker that is not the catalog leader its
//   DefineTopic/AlterTopic/CreateView is answered in its own response type
//   with InvalidRequest (client too old for cluster DDL routing; upgrade).
//   (A session below 8 never receives DdlResponse: it never sends 74–77.)
//   9 — authentication (spec 2026-10-13): after ConnectResponse (and TLS when negotiated) a session in
//   authMode Required runs SCRAM-SHA-512 on AuthClientFirst (12) / AuthServerFirst (13) /
//   AuthClientFinal (14) / AuthServerFinal (15); one proof per active verifier (D5).  Until it is
//   authenticated every other request is answered AuthenticationFailed (44) and the session ends.
//   NotAuthorized (45) answers any request not granted, in its own response type; SessionRevoked (46)
//   ends a session whose user was dropped / disabled / terminated (corr 0 = the session-level notice);
//   PasswordChangeRequired (47) answers every request of a one-time-password session except
//   SecurityDdl{AlterUser self SetPassword}.  SecurityDdl (79) / SecurityBootstrap (80) are answered by
//   DdlResponse (78).  An authentication-enabled broker answers a Connect below 9 ConnectResponse{ProtocolVersion}.
//   ConnectResponse v9 (blockLength 21) carries authMode and authState.
//  10 — consumer-group offsets under transactions (issues #125 / #438, KIP-447):
//   - TxnOffsetCommit may carry an optional trailing block after its entries: generationId (int32) + memberId
//     (u16 varbytes) of the consumer-group member the offsets belong to.  The coordinator then fences it exactly
//     like an OffsetCommit (UnknownMember / IllegalGeneration: the transaction must abort, the member rejoins).
//     No block = legacy (no member check).  An older broker's decoder ignores the trailing bytes (it reads only
//     `count` entries), so a v10 client may send it to any broker.
//   - OffsetFetch / OffsetCommit may be answered UnstableOffsetCommit (50), retriable (see the code).  A broker
//     answers a session below 10 NotCoordinator instead.
// 11 — Phase 2c subscription filters: `Subscribe` and `JoinGroup` may carry a trailing filter (`[u16 len][bytes]`,
//   canonical format in `rune/codec/FilterWire.h`; omitted = no filter). A broker >= 6 serves it and answers a refused
//   filter `SubscribeResponse` / `JoinGroupResponse` `{InvalidRequest, detail}`, the detail in a trailing
//   `[u16 len][utf-8]` field sent only to sessions >= 11. A broker below 6 would ignore the trailer, so a client sends
//   a filter only to a session whose `ConnectResponse.brokerVersion >= kBrokerVersionFilter`. A session below 11 never
//   has a filter trailer parsed (any bytes after the key / topic ids are ignored) and never sees a detail trailer.
// 12 — issue #116, per-receiver client->broker keys: on a session whose broker is >= 7 (kBrokerVersionReceiverKeys)
//   the client seals each encrypted client->broker frame for ONE receiver — the gateway (receiver 0) or shard worker w
//   (receiver w+1) — under rune::crypto::deriveReceiverKey(c2b, receiver), with that receiver's own seq space; the id
//   rides CryptoSubHeader.senderId (in the AAD).  A frame naming another receiver is dropped by every receiver, so a
//   frame sealed for one endpoint and replayed to another (the gateway, another worker) never opens there.
//   MetadataResponse carries each partition's ingressReceiverId (a trailer after the ingressEndpoint trailer, sent
//   only to sessions >= 12): the receiver a produce sent on that partition's advertised worker route is sealed for.
//   Either side below the gate keeps the legacy shared c2b key (senderId 0, one seq space): the replay protection
//   needs BOTH the client and the broker at the new version.
inline constexpr int32_t kClientVersionCatalog                    = 8;
inline constexpr int32_t kClientVersionAuth                       = 9;
inline constexpr int32_t kClientVersionTxnOffsetFence             = 10;
inline constexpr int32_t kClientVersionFilter                     = 11;  // subscription filters (Phase 2c)
inline constexpr int32_t kClientVersionReceiverKeys               = 12;  // per-receiver c2b keys (issue #116)
inline constexpr int32_t kClientVersion                           = 12;  // what this protocol's client announces

// Subscribe{topicId = kCatalogTopicId, partition 0, partitionKnown 1} reads
// the `__catalog` log (catalog history) from ANY broker — see DataMessages.h
// above `struct Subscribe` for the history-epoch note and the caught-up marker
// (both v8-only and format-neutral: an 8-byte recordBatch, like the lineage note).
inline constexpr int32_t kCatalogTopicId                          = -6;

// The `__security` log (client version 9) is internal partition -7; Subscribe{kSecurityTopicId}
// reads it (security admins and brokers only), exactly like kCatalogTopicId.
inline constexpr int32_t kSecurityTopicId                         = -7;

/// ConnectResponse.authMode (v9): how this broker authenticates sessions.
enum class AuthMode : uint8_t { Required = 0, DevDisabled = 1, InProcess = 2 };
/// ConnectResponse.authState (v9): where the (possibly resumed) session stands.
enum class AuthState : uint8_t { MustAuthenticate = 0, ResumedAuthenticated = 1, Anonymous = 2 };

// ---------------------------------------------------------------------------
// Broker versions (ConnectResponse.brokerVersion, an additive trailing field;
// a broker that predates it sends the 38-byte frame = version 0).
//   3 — explicit consumer-group subscriptions (see client version 3): the
//       coordinator does not auto-subscribe members, Records carry the
//       creating Subscribe's correlationId, session / subscription ids are
//       cluster-unique.  A client runs a GroupConsumer only against >= 3.
//   4 — client<->broker TLS/AEAD (Task 4): may choose a cipher / send
//       handshake bytes on ConnectResponse. See kBrokerVersionCrypto.
//   5 — authentication: ConnectResponse v9 carries authMode / authState (see client version 9).
//   6 — Phase 2c subscription filters: serves the Subscribe / JoinGroup filter trailer and answers a refused filter with
//       a detail string to sessions >= 11 (see client version 11).
//   7 — issue #116: opens client->broker frames per receiver (the gateway with the raw c2b key, shard worker w with
//       deriveReceiverKey(c2b, w+1)) for sessions >= 12, drops a frame naming another receiver, and advertises each
//       partition's ingressReceiverId to sessions >= 12 (see client version 12).
// ---------------------------------------------------------------------------
inline constexpr int32_t kBrokerVersionExplicitGroupSubscriptions = 3;
inline constexpr int32_t kBrokerVersionAuth                       = 5;   // authentication (ConnectResponse v9 fields)
inline constexpr int32_t kBrokerVersionFilter                     = 6;   // subscription filters (Phase 2c)
inline constexpr int32_t kBrokerVersionReceiverKeys               = 7;   // per-receiver c2b keys (issue #116)
inline constexpr int32_t kBrokerVersion                           = 7;   // what this protocol's broker announces

/// Issue #116: true when a session whose client announced `clientVersion` and whose broker announced
/// `brokerVersion` binds each encrypted client->broker frame to one receiver (see client version 12).
[[nodiscard]] constexpr bool sessionUsesReceiverKeys(int32_t clientVersion, int32_t brokerVersion) noexcept {
    return clientVersion >= kClientVersionReceiverKeys && brokerVersion >= kBrokerVersionReceiverKeys;
}

/// True when a session that connected with `clientVersion` handles
/// InvalidRequest (39) as non-retriable on every response type (see version 5).
[[nodiscard]] constexpr bool clientUnderstandsInvalidRequest(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionInvalidRequest;
}

/// True when a session that connected with `clientVersion` may be answered
/// UnknownProducerId (else the broker answers OutOfOrderSequence, as before).
[[nodiscard]] constexpr bool clientUnderstandsUnknownProducerId(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionUnknownProducerId;
}

/// True when a session that connected with `clientVersion` may be answered
/// TransientLogFull (42) (else the broker answers NotLeader).
[[nodiscard]] constexpr bool clientUnderstandsTransientLogFull(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionTransientLogFull;
}

/// True when a session that connected with `clientVersion` may be sent the
/// moved-subscription position note (an empty-batch Records frame; see 6).
[[nodiscard]] constexpr bool clientUnderstandsMovedPositionNote(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionMovedPositionNote;
}

/// True when a session that connected with `clientVersion` understands the
/// transient lineage note and may send a lineage claim (see version 7).
[[nodiscard]] constexpr bool clientUnderstandsLineageNote(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionLineageNote;
}

/// True when a session that connected with `clientVersion` speaks catalog
/// DDL (version 8): the DDL extension, 74–78 and a DDL Redirect.  Below 8 a
/// non-leader broker answers its DDL InvalidRequest in its own response type (F37).
[[nodiscard]] constexpr bool clientUnderstandsCatalogDdl(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionCatalog;
}

/// True when a session that connected with `clientVersion` speaks authentication
/// (version 9): templates 12-15, 79, 80 and codes 44-47.
[[nodiscard]] constexpr bool clientUnderstandsAuth(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionAuth;
}

/// True when a session that connected with `clientVersion` may be answered UnstableOffsetCommit (50) (version 10);
/// below it the broker answers NotCoordinator (retriable for every client).  A negative version (unknown: rigs) is
/// treated as current.
[[nodiscard]] constexpr bool clientUnderstandsUnstableOffsetCommit(int32_t clientVersion) noexcept {
    return clientVersion < 0 || clientVersion >= kClientVersionTxnOffsetFence;
}

/// True when a session that connected with `clientVersion` may carry a filter trailer on Subscribe / JoinGroup and
/// is sent the refusal `detail` trailer on their responses (version 11).  Below 11 the trailer is never parsed or sent.
[[nodiscard]] constexpr bool clientUnderstandsFilters(int32_t clientVersion) noexcept {
    return clientVersion >= kClientVersionFilter;
}

/// The produce error code a session that connected with `clientVersion` is
/// answered: UnknownProducerId becomes OutOfOrderSequence (the pre-D6 answer)
/// for clients below kClientVersionUnknownProducerId, and TransientLogFull becomes
/// NotLeader for clients below kClientVersionTransientLogFull; every other code
/// passes through.  The ONE definition every broker answer site uses (review L4).
[[nodiscard]] constexpr ErrorCode produceErrorForClient(ErrorCode ec, int32_t clientVersion) noexcept {
    if (ec == ErrorCode::UnknownProducerId && !clientUnderstandsUnknownProducerId(clientVersion)) [[unlikely]]
        return ErrorCode::OutOfOrderSequence;
    if (ec == ErrorCode::TransientLogFull && !clientUnderstandsTransientLogFull(clientVersion)) [[unlikely]]
        return ErrorCode::NotLeader;   // Phase 2b E2: a pre-v6 client's retriable code
    return ec;
}

} // namespace rune::protocol::client
