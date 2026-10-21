# Wire protocol

The client-to-broker protocol of `protocol/` (`rune/protocol/client/`). Every frame is a 24-byte header followed by a
body; all integers are little-endian. The source of truth is the headers:
[`Protocol.h`](../../protocol/include/rune/protocol/client/Protocol.h) (ids, error codes, versions) and one
`*Messages.h` per area. Frames travel on Aeron streams over UDP; a client never speaks TCP.

## Frame header

| Offset | Field | Type | Meaning |
|---|---|---|---|
| 0 | `blockLength` | u16 | Size of the fixed part of the body. Decoders skip what they do not know, so trailing fields are additive. |
| 2 | `templateId` | u16 | Frame type, the **Id** column below. |
| 4 | `schemaId` | u16 | `120` (`kSchemaId`). The crypto envelope uses `121`. |
| 6 | `version` | u16 | Header version, `3` (`kProtocolVersion`). A decoder refuses a newer one, so additive changes never bump it. |
| 8 | `sessionId` | i64 | Broker-assigned session id. |
| 16 | `correlationId` | i64 | Chosen by the sender; the response carries it back. `0` marks a session-level notice. |

Variable-length fields (strings, byte blobs) follow the fixed part, each with a length prefix.

Capability is negotiated with two numbers in `Connect`/`ConnectResponse`: `clientVersion` (this protocol's client
announces `10`, `kClientVersion`) and `brokerVersion` (`5`, `kBrokerVersion`). A broker answers a client below a
feature's version in the older way; an authentication-enabled broker answers a `Connect` below 9
`ConnectResponse{ProtocolVersion}`.

| Client version | Adds |
|---|---|
| 2 | `UnknownProducerId` (28) and `ProduceResponse.logStartOffset`. |
| 3 | Explicit consumer-group subscriptions (a member subscribes to each assigned partition's leader itself). |
| 4 | TLS 1.3 / AEAD: crypto fields on `Connect`, `HandshakeContinue`. |
| 5 | `InvalidRequest` (39) understood on every response; `ViewRowResponse` statuses 5 to 7. |
| 6 | `TransientLogFull` (42) retried after a backoff; moved-subscription position note and redirect. |
| 7 | Transient lineage note and claim (`PartitionDataReset`, 43). |
| 8 | Catalog DDL: the DDL extension, frames 74 to 78, DDL redirects. |
| 9 | Authentication: frames 12 to 15, 79 and 80, error codes 44 to 47. |
| 10 | `TxnOffsetCommit` member block (`generationId`, `memberId` after the entries; KIP-447); `UnstableOffsetCommit` (50). An older broker ignores the block; a broker answers a session below 10 `NotCoordinator` instead of 50. |

## Frame catalogue

Direction: **C→B** client to broker, **B→C** broker to client. Every body starts after the header; the fields listed are
the body fields in wire order (`sessionId` and `correlationId` live in the header). A `bytes` or `str` field is
length-prefixed.

Id ranges: control 1 to 15, produce and consume 16 to 31, groups and transactions 32 to 63, schema, views, DDL and
security 64 to 95.

| Id | Frame | Dir | Body fields |
|---|---|---|---|
| 1 | `ConnectRequest` | C→B | `egressStreamId`, `clientVersion`, `egressChannel`, `cipherList`, `handshakeBytes`, `authToken` |
| 2 | `ConnectResponse` | B→C | `code`, `leaderMemberId`, `assignedSessionId`, `brokerVersion`, `chosenCipher`, `handshakeBytes`, `authMode`, `authState` (v9) |
| 3 | `Keepalive` | C→B | (header only) |
| 4 | `Disconnect` | either | `reason` |
| 5 | `ErrorResponse` | B→C | `code`, `message` |
| 6 | `Ping` | C→B | `echo` |
| 7 | `Pong` | B→C | `echo` |
| 8 | `MetadataRequest` | C→B | `topicIds` |
| 9 | `MetadataResponse` | B→C | `brokers`, `partitions` |
| 10 | `HandshakeContinue` | C→B | `handshakeBytes` |
| 11 | `HandshakeContinueResponse` | B→C | `code`, `handshakeBytes` |
| 12 | `AuthClientFirst` | C→B | `mechanism`, `username`, `clientNonce` |
| 13 | `AuthServerFirst` | B→C | `combinedNonce`, `count` |
| 14 | `AuthClientFinal` | C→B | `count` |
| 15 | `AuthServerFinal` | B→C | `errorCode`, `flags`, `retryAfterMs`, `serverSignature`, `detail` |
| 16 | `Produce` | C→B | `topicId`, `partition`, `partitionKnown`, `acks`, `producerId`, `producerEpoch`, `baseSequence`, `key`, `recordBatch` |
| 17 | `ProduceResponse` | B→C | `errorCode`, `partition`, `baseOffset`, `logStartOffset` |
| 18 | `Subscribe` | C→B | `topicId`, `partition`, `partitionKnown`, `fromOffset`, `isolation`, `key` |
| 19 | `SubscribeResponse` | B→C | `errorCode`, `partition`, `subscriptionId` |
| 20 | `Records` | B→C | `subscriptionId`, `topicId`, `partition`, `baseOffset`, `recordBatch` |
| 21 | `Unsubscribe` | C→B | `subscriptionId` |
| 32 | `JoinGroup` | C→B | `sessionTimeoutMs`, `groupId`, `memberId`, `subscribedTopicIds` |
| 33 | `JoinGroupResponse` | B→C | `memberId`, `generationId`, `isLeader`, `errorCode` |
| 34 | `SyncGroup` | C→B | `groupId`, `memberId`, `generationId` |
| 35 | `SyncGroupResponse` | B→C | `errorCode`, `assignment` |
| 36 | `Heartbeat` | C→B | `groupId`, `memberId`, `generationId` |
| 37 | `HeartbeatResponse` | B→C | `errorCode` |
| 38 | `LeaveGroup` | C→B | `groupId`, `memberId` |
| 39 | `LeaveGroupResponse` | B→C | `errorCode` |
| 40 | `OffsetCommit` | C→B | `generationId`, `groupId`, `memberId`, `entries` |
| 41 | `OffsetCommitResponse` | B→C | `results` |
| 42 | `OffsetFetch` | C→B | `groupId`, `entries` |
| 43 | `OffsetFetchResponse` | B→C | `results` |
| 44 | `Assign` | B→C | `generationId`, `assignment` |
| 45 | `InitProducerId` | C→B | `transactionTimeoutMs`, `transactionalId` |
| 46 | `InitProducerIdResponse` | B→C | `producerId`, `producerEpoch`, `errorCode` |
| 47 | `AddPartitionsToTxn` | C→B | `producerId`, `producerEpoch`, `transactionalId`, `partitions` |
| 48 | `AddPartitionsToTxnResponse` | B→C | `results` |
| 49 | `AddOffsetsToTxn` | C→B | `producerId`, `producerEpoch`, `transactionalId`, `groupId` |
| 50 | `AddOffsetsToTxnResponse` | B→C | `errorCode` |
| 51 | `TxnOffsetCommit` | C→B | `producerId`, `producerEpoch`, `transactionalId`, `groupId`, `entries`, then optionally (client 10) `generationId` i32 + `memberId` |
| 52 | `TxnOffsetCommitResponse` | B→C | `results` |
| 53 | `EndTxn` | C→B | `producerId`, `producerEpoch`, `commit`, `transactionalId` |
| 54 | `EndTxnResponse` | B→C | `errorCode` |
| 64 | `DefineTopic` | C→B | `topicName`, `partitionCount`, `keyField`, `validate`, `descriptorBlob`, `persistence`, `replicationFactor`, DDL extension |
| 65 | `DefineTopicResponse` | B→C | `topicId`, `version`, `errorCode` |
| 66 | `GetTopicSchema` | C→B | `topicName`, `requestedFingerprint` (0 = latest; `~0` = the evolving lineage) |
| 67 | `TopicSchemaResponse` | B→C | `topicId`, `version`, `errorCode`, `descriptorBlob`, `partitionCount` |
| 68 | `AlterTopic` | C→B | `topicName`, `expectedVersion`, `opsBlob`, DDL extension |
| 69 | `AlterTopicResponse` | B→C | `topicId`, `newVersion`, `errorCode` |
| 70 | `CreateView` | C→B | `viewName`, `keyField`, `persistence`, `planBlob`, DDL extension |
| 71 | `CreateViewResponse` | B→C | `viewTopicId`, `outputFingerprint`, `errorCode` |
| 72 | `GetViewRow` | C→B | `viewTopicId`, `keyU64`, `requestedFingerprint` |
| 73 | `ViewRowResponse` | B→C | `status`, `rowLen`, `rowBytes`, `rowBytesLen` |
| 74 | `DropTopic` | C→B | `topicName` |
| 75 | `AlterTopicConfig` | C→B | `topicName`, `expectedVersion`, `changes` (count + `(key u8, value i64)` pairs) |
| 76 | `DropView` | C→B | `viewName` |
| 77 | `AlterView` | C→B | `viewName`, `keyField`, `persistence`, `expectedVersion`, `planBlob`, `planLen` |
| 78 | `DdlResponse` | B→C | `errorCode`, `objectId`, `newVersion`, `gateTopicId`, `detail` |
| 79 | `SecurityDdl` | C→B | `ext`, `kind`, `name`, `change`, `passwordIndex`, `terminateSessions`, `verifier`, `granteeKind`, `op`, `resourceType`, `patternKind`, `pattern`, `roleName` |
| 80 | `SecurityBootstrap` | C→B | `adminName`, `verifier` |

Notes:

- **DDL extension** (version 8, on `DefineTopic`, `AlterTopic`, `CreateView` and frames 74 to 77, 79): the statement text, a
  client name, a 128-bit request id and an optional `replayedFrom`. The request id stays the same across resends, so a
  retried DDL returns its original result. 74 to 78 and 79 and 80 are all answered by `DdlResponse` (78).
- **`SecurityDdl` `kind`**: `CreateUser`=1, `AlterUser`=2, `DropUser`=3, `CreateRole`=4, `DropRole`=5, `Grant`=6,
  `Revoke`=7, `GrantRole`=8, `RevokeRole`=9. An `AlterUser` `change` is `AddPassword`=0, `DropPassword`=1,
  `SetPassword`=2, `Disable`=3, `Enable`=4. A `verifier` is `(salt, iterations, StoredKey, ServerKey)`; the plaintext
  password never reaches a broker, and the statement text is sent with the password replaced by `'<redacted>'`.
- **`ViewRowResponse.status`**: 0 OK, 1 AbsentKey, 2 NotReady, 3 SchemaMismatch, 4 UnknownView, 5 InvalidRequest,
  6 Busy, 7 RowTooLarge, 45 NotAuthorized.
- **Internal logs** are read by subscribing to a negative topic id: `__catalog` is `-6` (`kCatalogTopicId`), `__security`
  is `-7` (`kSecurityTopicId`, security admins and brokers only). A stream ends with an 8-byte caught-up marker batch.
- `Produce.acks`: `0` leader or none (immediate ack), `1` all (deferred until the in-sync replicas hold it).
  `producerId = -1` with `baseSequence = -1` is a non-idempotent produce.
- `Subscribe.fromOffset`: `-2` earliest, `-1` latest, otherwise an absolute offset. `isolation`: `0` read uncommitted,
  `1` read committed.
- A key maps to a partition with FNV-1a 64 over the key bytes (`partitionForKey`, `PartitionHash.h`); an empty key maps
  to partition 0. Broker and client share the header, so they cannot diverge.

## Record batch (v2)

The `recordBatch` of `Produce` and `Records` is the on-disk batch, Kafka v2 layout with little-endian fields
([`RecordBatch.h`](../../protocol/include/rune/protocol/RecordBatch.h)):

| Offset | Field | Type |
|---|---|---|
| 0 | `baseOffset` | i64 |
| 8 | `length` (bytes after this field) | i32 |
| 12 | `partitionLeaderEpoch` (excluded from the CRC) | i32 |
| 16 | `magic` (always `2`) | i8 |
| 17 | `crc` (CRC-32C from offset 21 to the end) | u32 |
| 21 | `attributes` (bit 4 transactional, bit 5 control, bit 6 delete horizon in `baseTimestamp`) | i16 |
| 23 | `lastOffsetDelta` | i32 |
| 27 | `baseTimestamp` (epoch **milliseconds**) | i64 |
| 35 | `maxTimestamp` (epoch milliseconds) | i64 |
| 43 | `producerId` | i64 |
| 51 | `producerEpoch` | i16 |
| 53 | `baseSequence` | i32 |
| 57 | `recordCount` | i32 |
| 61 | records (zigzag-varint framed) | |

## Authentication flow (SCRAM-SHA-512)

Client version 9, frames 12 to 15, defined in
[`AuthMessages.h`](../../protocol/include/rune/protocol/client/AuthMessages.h). After `ConnectResponse` (and the TLS
handshake when one was negotiated) a session whose `authMode` is `Required` runs:

1. `ConnectResponse` carries `authMode` (`Required`=0, `DevDisabled`=1, `InProcess`=2) and `authState`
   (`MustAuthenticate`=0, `ResumedAuthenticated`=1, `Anonymous`=2).
2. **`AuthClientFirst`**: `mechanism` (1 = SCRAM-SHA-512), `username` (1 to 128 bytes), `clientNonce` (1 to 128 bytes).
3. **`AuthServerFirst`**: `combinedNonce`, then `count` (1 or 2) pairs of `(salt 16 to 64 bytes, iterations 4096 to
   1 000 000)`. The broker always sends two salts: a user with one password gets a deterministic fake second salt, and an
   unknown user gets a fake salt that is the same on every broker, so the answer never reveals whether the user exists.
4. **`AuthClientFinal`**: `count` proofs of 64 bytes each, one per verifier.
5. **`AuthServerFinal`**: `errorCode`, `flags` (bit 0 = the user must change the password), `retryAfterMs` (a rate-limit
   hint when `errorCode` is 44), `serverSignature` (64 bytes on success, else empty) and `detail`. The client checks the
   server signature before it trusts the broker.

Until a session is authenticated, every other request is answered `AuthenticationFailed` (44) and the session ends. See
[SECURITY.md](../../broker/docs/SECURITY.md) for the throttle, the one-time bootstrap password and revocation.

## Encrypted frames (TLS 1.3 / AEAD)

When `Connect` offers ciphers and the broker chooses one, `HandshakeContinue` (10) and `HandshakeContinueResponse` (11)
carry the TLS handshake. Afterwards each frame is
`[24-byte header][12-byte crypto subheader][ciphertext][16-byte tag]` with schema id `121`: 28 bytes of overhead, and the
36 bytes of header plus subheader are the AEAD associated data. The subheader is `version u8, cipher u8, epoch u8,
senderId u8` (0 = gateway, `i+1` = worker `i`) and a `seq u64`
([`CryptoEnvelope.h`](../../protocol/include/rune/protocol/client/CryptoEnvelope.h)).

## Error codes

`rune::protocol::client::ErrorCode` (u16), carried in `ErrorResponse.code` and in the `errorCode` field of responses.
"Client handling" is what the C++ client does, where the headers state it; an empty cell means the request completes with that error code.

| Code | Name | Meaning | Client handling |
|---|---|---|---|
| 0 | `OK` |  |  |
| 1 | `ProtocolVersion` | Client version not supported (also: an authenticated broker refusing a client below 9, or a JoinGroup from a client below 3 or 5). | not retried |
| 2 | `NotLeader` | This node does not lead the partition, or the catalog leader changed or is catching up. | retried after a metadata refresh |
| 3 | `Redirect` | Carries the leader's client endpoint; re-send there. | followed (DDL: at most 3 hops) |
| 4 | `SessionExpired` | The session timed out. |  |
| 5 | `UnknownSession` | The broker has no such session. |  |
| 6 | `Internal` | Internal error, or a DDL in flight when the leader was demoted (in doubt: it may still apply). | in doubt |
| 7 | `UnknownTopic` |  |  |
| 8 | `UnknownPartition` |  |  |
| 9 | `NotPartitionLeader` |  |  |
| 10 | `MinIsrViolation` | `acks=all` rejected: the in-sync set is below `RUNE_MIN_ISR`. | retried after a backoff |
| 11 | `StaleGeneration` | Group. |  |
| 12 | `UnknownMember` | Group. |  |
| 13 | `RebalanceInProgress` | Group. |  |
| 14 | `NotCoordinator` | Group. |  |
| 15 | `IllegalGeneration` | Group; also a commit from before a transient-lineage rebalance. |  |
| 16 | `GroupDead` | Group. |  |
| 17 | `InvalidProducerEpoch` | Transaction. | fences the producer |
| 18 | `ProducerFenced` | Transaction: a newer producer with the same transactional id took over. | fences the producer |
| 19 | `InvalidTxnState` | Transaction. |  |
| 20 | `OutOfOrderSequence` | Idempotent produce (also the answer to a client below 2 instead of 28). | re-sent once older batches resolve |
| 21 | `DuplicateSequence` | A known duplicate; counts as success. | counts as success |
| 22 | `MessageTooLarge` | Batch above the broker's limit. |  |
| 23 | `InvalidSchema` | Descriptor malformed, or partitions below 1. |  |
| 24 | `SchemaValidationFailed` | A produced record failed validation. |  |
| 25 | `TopicExists` | Name taken (also a name differing only by letter case). |  |
| 26 | `StorageError` | A log failed an fsync (fail-stop). |  |
| 27 | `ConcurrentTransactions` | The previous transaction's markers have not all landed. | retried |
| 28 | `UnknownProducerId` | Producer state evicted; `logStartOffset` tells retention from loss. | re-sequenced |
| 29 | `HandshakeFailed` | TLS handshake or crypto negotiation failed (including `RUNE_REQUIRE_TLS`). |  |
| 30 | `SchemaIncompatible` | `alter topic` incompatible with the compatibility mode. |  |
| 31 | `SchemaConflict` | `expectedVersion` is stale. | re-read the schema, then retry |
| 32 | `ViewExists` |  |  |
| 33 | `ViewSourceNotFound` |  |  |
| 34 | `ViewFieldUnresolved` | A plan names a field the source does not have. |  |
| 35 | `ViewKeyInvalid` | `keyU64` does not resolve under the view's key type. |  |
| 36 | `ViewNotCoPartitioned` | An aggregate view over a source with more than one partition. |  |
| 37 | `ViewSourceTransient` | A persisted view over a transient source. |  |
| 38 | `ViewNotAlterable` | `alter topic` on a view's output topic. |  |
| 39 | `InvalidRequest` | The body did not decode, or a DDL argument is out of range (the detail names it). | not retried |
| 40 | `InvalidTopicName` | Empty, over 249 bytes, `.` or `..`, a byte outside `[A-Za-z0-9._-]`, or the `__` prefix. |  |
| 41 | `ViewSourceIsView` | A view over another view's output. |  |
| 42 | `TransientLogFull` | A transient partition's ring is full of unreplicated records; nothing was appended. | retried after 100 ms, no metadata refresh |
| 43 | `PartitionDataReset` | A resumed subscription's data lineage did not survive. | resubscribed from the log start |
| 44 | `AuthenticationFailed` | No credentials, wrong password, unknown or disabled user, or the login deadline; with `retryAfterMs` a rate limit. Ends the session. | ends the session; a rate limit is retried after the hint |
| 45 | `NotAuthorized` | Not granted (default deny), or the object is not visible. Fails exactly that request. | never re-sent |
| 46 | `SessionRevoked` | The session's user was dropped, disabled or terminated. Terminal for the session. | terminal for the session |
| 47 | `PasswordChangeRequired` | A one-time-password session may only change its password. | fails that request |
| 48 | (reserved) | Reserved for `AuthUnavailable`. |  |
| 49 | `RecordEgressUnavailable` | Authenticating broker only: the session's own record stream for the partition's worker could not be built (there is no shared-stream fallback). Sent in `SubscribeResponse.errorCode` on the pending Subscribe. | retried a few times (a bind resets the count), then fails the subscription or the group consumer with an error |
| 50 | `UnstableOffsetCommit` | Client 10: `OffsetFetch` / `OffsetCommit` of a partition an unfinished transaction holds a pending offset on (until its completion is durable), or while the transaction coordinator is not loaded. Nothing is appended. | retried after a backoff, never surfaced |

The broker-internal storage errors (`rune::protocol::ErrorCode`, [`Errors.h`](../../protocol/include/rune/protocol/Errors.h))
are a separate, small enum used inside the log: `OffsetOutOfRange`=1, `CorruptRecord`=2, `InvalidRecordBatch`=3,
`IndexOffsetOverflow`=4. They do not cross the wire.

## Other schemas

- **Replication** (`schemaId 112`, SBE, `schema/rune-replication.xml`, generated headers in
  `include/rune_protocol_replication_sbe/`): `FetchRequest`, `FetchResponse`, `AlterIsr`, `AlterIsrResponse`,
  `InternalIsr`, `InternalIsrAck`, `OffsetsForLeaderEpochRequest`/`Response`, `ReconcileFetchRequest`/`Response`. Broker to
  broker only; inter-broker frames are sealed with the cluster key
  ([operations](../operations.md#the-cluster-key)).
- **Internal ring** (`schemaId 7`, SBE, `schema/rune-internal-ring.xml`): messages between threads of one process
  (`WorkerOp`, `WorkerReturn`, `FlushRequest`, `FlushCompletion`). They never cross a wire.
- **Conformance vectors**: `protocol/conformance/` holds generated golden vectors for the client frames; see its
  [README](../../protocol/conformance/README.md).
