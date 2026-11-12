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
announces `17`, `kClientVersion`) and `brokerVersion` (`10`, `kBrokerVersion`). A broker answers a client below a
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
| 11 | Subscription filters: the optional filter trailer on `Subscribe` and `JoinGroup`, the refusal `detail` trailer on their responses (broker version 6). |
| 17 | Checked writes (broker version 10): the `Produce` `acks` byte carries bit 1 (`View`), `ProduceResponse` carries `assignedBase` and an optional verdict trailer, the `EndTxnResponse` vote trailer, and error codes 55 to 63 exist. A session below 17 is never sent 55 to 63 (it is answered `InvalidRequest`) and keeps the 46-byte `ProduceResponse` and the 26-byte `EndTxnResponse`. |

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
| 16 | `Produce` | C→B | `topicId`, `partition`, `partitionKnown`, `acks` (bit 0 All, bit 1 View, v17), `producerId`, `producerEpoch`, `baseSequence`, `key`, `recordBatch` |
| 17 | `ProduceResponse` | B→C | `errorCode`, `partition`, `baseOffset`, `logStartOffset`, `assignedBase` (v17), `verdicts` (v17, optional trailer) |
| 18 | `Subscribe` | C→B | `topicId`, `partition`, `partitionKnown`, `fromOffset`, `isolation`, `key`, `filter` (v11, optional trailer) |
| 19 | `SubscribeResponse` | B→C | `errorCode`, `partition`, `subscriptionId`, `detail` (v11, optional trailer) |
| 20 | `Records` | B→C | `subscriptionId`, `topicId`, `partition`, `baseOffset`, `recordBatch` |
| 21 | `Unsubscribe` | C→B | `subscriptionId` |
| 32 | `JoinGroup` | C→B | `sessionTimeoutMs`, `groupId`, `memberId`, `subscribedTopicIds`, `filter` (v11, optional trailer) |
| 33 | `JoinGroupResponse` | B→C | `memberId`, `generationId`, `isLeader`, `errorCode`, `detail` (v11, optional trailer) |
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
| 54 | `EndTxnResponse` | B→C | `errorCode`, then (v17, only after a no vote) `constraintId`, `failingOffset` |
| 64 | `DefineTopic` | C→B | `topicName`, `partitionCount`, `keyField`, `validate` (sent as 1 for a typed topic; a broker of this version ignores it, typed topics always validate), `descriptorBlob`, `persistence`, `replicationFactor`, DDL extension, key features (optional; sent only when the features are not the defaults, and only to a broker of version 10 or later) |
| 65 | `DefineTopicResponse` | B→C | `topicId`, `version`, `errorCode` |
| 66 | `GetTopicSchema` | C→B | `topicName`, `requestedFingerprint` (0 = latest; `~0` = the evolving lineage) |
| 67 | `TopicSchemaResponse` | B→C | `topicId`, `version`, `errorCode`, `descriptorBlob`, `partitionCount`, key features (optional) |
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
| 80 | `SecurityBootstrap` | C→B | `adminName`, `verifier`, `fakeSaltSeed`, `nonce`, `mac` (see Bootstrap proof) |
| 88 | `GetViewRowK` | C→B | `viewTopicId`, `partition`, `requestedFingerprint`, `minOffset`, `key` (1 to 128 bytes) |
| 89 | `ScanViewRows` | C→B | `viewTopicId`, `limit`, `requestedFingerprint`, `minOffset`, `prefix` (1 to 127 bytes), `resumeToken` (0 or 12 bytes) |
| 90 | `ScanViewRowsResponse` | B→C | `status`, `rowCount`, `nextToken`, `rows` |
| 91 | `GetStats` | C→B | `scope` u8 (1 topic partition, 2 view partition), `objectId` i32, `partition` i32 |
| 92 | `StatsResponse` | B→C | `status` u16, `count` u16, `count` x `(statId u16, value i64)`, `nConstraints` u16, `nConstraints` x `(constraintId u32, rejections u64)` |
| 102 | `SecurityBootstrapChallengeRequest` | C→B | header only |
| 103 | `SecurityBootstrapChallenge` | B→C | `challenge` (32 bytes) |

Notes:

- **DDL extension** (version 8, on `DefineTopic`, `AlterTopic`, `CreateView` and frames 74 to 77, 79): the statement text, a
  client name, a 128-bit request id and an optional `replayedFrom`. The request id stays the same across resends, so a
  retried DDL returns its original result. 74 to 78 and 79 and 80 are all answered by `DdlResponse` (78).
- **`SecurityDdl` `kind`**: `CreateUser`=1, `AlterUser`=2, `DropUser`=3, `CreateRole`=4, `DropRole`=5, `Grant`=6,
  `Revoke`=7, `GrantRole`=8, `RevokeRole`=9. An `AlterUser` `change` is `AddPassword`=0, `DropPassword`=1,
  `SetPassword`=2, `Disable`=3, `Enable`=4. A `verifier` is `(salt, iterations, StoredKey, ServerKey)`; the plaintext
  password never reaches a broker, and the statement text is sent with the password replaced by `'<redacted>'`.
- **`ViewRowResponse.status`**: 0 OK, 1 AbsentKey, 2 NotReady, 3 SchemaMismatch, 4 UnknownView, 5 InvalidRequest,
  6 Busy, 7 RowTooLarge, 8 CursorInvalidated (scans, see below), 45 NotAuthorized. A `GetViewRow` on a broker that does not lead the view's source partition, or
  leads it without the controller's read lease, is answered `ErrorResponse` `Redirect` (3, the leader's endpoint) or
  `NotLeader` (2); both are retriable.
- **Internal logs** are read by subscribing to a negative topic id: `__catalog` is `-6` (`kCatalogTopicId`), `__security`
  is `-7` (`kSecurityTopicId`, security admins and brokers only). A stream ends with an 8-byte caught-up marker batch.
- `Produce.acks`: bit 0 set = all (deferred until the in-sync replicas hold it), clear = leader or none (immediate ack); bit 1 = `View` (client version 17, see Checked writes).
  `producerId = -1` with `baseSequence = -1` is a non-idempotent produce.
- `Subscribe.fromOffset`: `-2` earliest, `-1` latest, otherwise an absolute offset. `isolation`: `0` read uncommitted,
  `1` read committed.
- A key maps to a partition with FNV-1a 64 over the key bytes (`partitionForKey`, `PartitionHash.h`); an empty key maps
  to partition 0. Broker and client share the header, so they cannot diverge.
- **`OffsetFetch` with no entries (fetch-all)**: lists every committed offset of the group the caller may READ, or
  answers the single sentinel result `{topicId -1, partition -1, offset -1, errorCode MessageTooLarge (22)}` when those
  results do not all fit one answer, never a truncated list. The caller then fetches with listed entries.

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
| 21 | `attributes` (bit 4 transactional, bit 5 control, bit 6 delete horizon in `baseTimestamp`, bit 7 atomic) | i16 |
| 23 | `lastOffsetDelta` | i32 |
| 27 | `baseTimestamp` (epoch **milliseconds**) | i64 |
| 35 | `maxTimestamp` (epoch milliseconds) | i64 |
| 43 | `producerId` | i64 |
| 51 | `producerEpoch` | i16 |
| 53 | `baseSequence` | i32 |
| 57 | `recordCount` | i32 |
| 61 | records (zigzag-varint framed) | |

Each record has its own `attributes` byte. Bits 0 and 1 are the **write intent** (client version 17, typed topics with a
key index):

| Bits 0-1 | Live record (a value) | Tombstone (empty value) |
|---|---|---|
| 0 | upsert | remove (no error when the key is absent) |
| 1 | insert: `KeyExists` (56) when the key exists | refused: `InvalidRequest` |
| 2 | update: `KeyNotFound` (57) when the key is absent | removeExisting: `KeyNotFound` (57) when the key is absent |
| 3 | `InvalidRequest` | `InvalidRequest` |

The other bits are reserved and answered `InvalidRequest`. On a raw topic the bits are stored as sent and not
interpreted.

## Authentication flow (SCRAM-SHA-512)

Client version 9, frames 12 to 15, defined in
[`AuthMessages.h`](../../protocol/include/rune/protocol/client/AuthMessages.h). After `ConnectResponse` (and the TLS
handshake when one was negotiated) a session whose `authMode` is `Required` runs:

1. `ConnectResponse` carries `authMode` (`Required`=0, `DevDisabled`=1, `InProcess`=2) and `authState`
   (`MustAuthenticate`=0, `ResumedAuthenticated`=1, `Anonymous`=2).  A broker never answers `ResumedAuthenticated`
   any more (a Connect does not resume a session by its id); the value stays reserved.
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

## Bootstrap proof (client version 19)

`SecurityBootstrap` (80) proves possession of the cluster key with a challenge-response, so a captured frame replays
nowhere:

1. The client sends `SecurityBootstrapChallengeRequest` (102, a header with the session id and a correlation id) on the
   session it will bootstrap on. The gateway answers `SecurityBootstrapChallenge` (103): 32 bytes, unique per request
   (a per-process counter run through a keyed MAC under a random per-process secret) and valid for **one** bootstrap
   attempt on **that** session. A newer request replaces an older unused challenge; an attempt (accepted or refused)
   uses the challenge up. 102 is allowed and refused exactly where 80 is (the bootstrap window; the refusal is the
   same). A client below version 19 is refused both.
2. The MAC key is a subkey: `HKDF(SHA-512, ikm = cluster master, salt = empty, info = "rune-bootstrap-mac-v2")`, 64
   bytes. The master is never a MAC key.
3. The `mac` field of 80 is HMAC-SHA-512 under that subkey over
   `"rune-bootstrap-v2" | challenge 32 | adminName var16 | verifier | fakeSaltSeed 32 | nonce 16`. The challenge is not
   on the wire in 80; the gateway knows it. The layout of 80 is unchanged.
4. The proof binds no clock: the one-use challenge is the freshness. A frame signed for another challenge, another
   session, another gateway or another process lifetime does not verify; v1 proofs (raw master, `rune-bootstrap-v1`
   input) are refused.

Every resend of a bootstrap (NotLeader, Redirect, a lost session) sends a new 102 first and signs the frame again for
the new challenge; the nonce stays the same, so the broker still dedupes the bootstrap on it. The client ends the call
`BrokerTooOld` against a broker below version 12.

## Filters (client version 11)

Broker version 6 serves a subscription filter. `Subscribe` and `JoinGroup` may end with one extra variable field,
`[u16 len][filter bytes]`, after the key (`Subscribe`) or after the topic ids (`JoinGroup`). The filter bytes use the
canonical format in [`FilterWire.h`](../../codec/include/rune/codec/FilterWire.h) (at most 8,234 bytes).

- **Omitted when empty.** A frame with no filter is byte-identical to version 10.
- **A trailer that does not fit is not decodable.** A length prefix cut in half, or a length larger than the bytes left,
  fails the decode, and the broker answers `InvalidRequest`.
- **Gated on the session's version.** The broker parses the trailer only for a session that connected with
  `clientVersion >= 11`. For an older session any bytes after the key or topic ids are ignored.
- **Refusal detail.** A refused filter is answered `{InvalidRequest, detail}`. `SubscribeResponse` and
  `JoinGroupResponse` carry the detail as a trailing `[u16 len][utf-8]` field, written only when it is non-empty and
  only to a session of version 11 or higher, so an older client never sees a trailer it cannot parse. A
  `JoinGroup` whose filter differs from the group's has a detail that starts with `group filter mismatch`.
- **Broker too old.** A broker below version 6 would ignore the trailer and deliver everything, so a client sends a
  filter only to a session whose `ConnectResponse.brokerVersion` is at least 6.

On the internal worker ring the same feature appends `OpType::FILTER_SCHEMA` (25) and `TailKind::FILTER` (11).

## Checked writes (client version 17)

- **`Produce.acks`** is a bit set: bit 0 `All`, bit 1 `View`. Bits above 1 are rejected as a malformed frame. `View` makes the
  ack wait until the batch is committed (high watermark, and for `All` the in-sync replica count) and every view of the
  partition has folded it under the current leadership. An ack that cannot be given in time is `Internal`; a leader
  that lost the partition answers `NotLeader`.
- **`ProduceResponse` (client version 17)**: the body block is 30 bytes (`assignedBase` u64 at body offset 22, `0` = none),
  so the frame is 54 bytes; an optional trailer follows, `[count u16][count x {recordIndex u16, code u16, constraintId u32}]`,
  present only on a `View` ack of a verdict source (`count` 0 = every record accepted). A session below 17 is sent the
  exact 46-byte frame and never the trailer.
- **Verdict trailer entries**: `recordIndex` is the record's `offsetDelta` (the ordinal for Rune clients); `code` is 55 to 57
  (`ConstraintViolation` is 55). The ack is sent once the judge has passed the batch, without waiting for the verdict marker.
  A `View` retry of the same batch (same producer id, epoch, sequence) inside the broker's dedupe window is answered with
  the original `baseOffset` and the original verdicts once the committed watermark W has passed the batch; a retry outside
  the window on a verdict source is answered `DuplicateSequence` (21) with no trailer. An ack with more than one claim
  (thousands of rejections, up to 32,824 bytes) is offered rather than claimed; ack publications have a term of at least 512 KiB.
- **Control records 16 to 20** are control batches with producer id -1 in the log: 16 reservation, 17 verdict marker,
  18 constraint-set change, 19 transaction judge, 20 verdict plan. Consumers never receive them and the transaction readers ignore them.
- **Transaction judge record** (type 19, `kTxnJudge`): the value is `[version u8 = 1][producerId i64][producerEpoch i16][firstOffset i64]`
  (little-endian, 19 bytes; the record key is the type). The leader appends it when it has the coordinator's vote request for the transaction; the
  judge reads everything from `firstOffset` up to it as one unit, and the commit or abort marker then finalizes the verdict.
  The cleaner keeps the ones a restart or a new leader still needs to judge an undecided transaction.
- **Atomic batch** (`attributes` bit 7, `kAttrAtomic`): on a verdict source the batch is all or nothing on its partition. If any
  record is rejected, every record is, and a `View` ack carries one trailer entry (`recordIndex` 0). A session below 17 never sets it.
- **`EndTxnResponse` vote trailer** (client version 17): when a commit is voted no, the 26-byte frame grows to 38 bytes
  (`blockLength` 14). A session below 17, and every answer that is not a vote, keeps the 26-byte frame.

  | Offset (body) | Size | Field |
  |---|---|---|
  | 0 | 2 | `errorCode`: the vote, 55, 56, 57, 62 or 63 |
  | 2 | 4 | `constraintId` (`viewObjectId << 16 \| ordinal`; 0 when the code names none) |
  | 6 | 8 | `failingOffset`: the log offset of the record that failed, -1 for none |

  After a no the transaction is already aborted on every partition.
- **Verdict plan record** (type 20, `kVerdictPlan`): the plan of a `unique` or `check` view that joins a verdict source, appended just
  below its constraint-set change record so that every leader judges with the same plan. The value is little-endian, at most 65,536 bytes
  in all (a larger plan is refused at DDL):

  | Offset | Size | Field |
  |---|---|---|
  | 0 | 1 | version, 1 |
  | 1 | 8 | `viewObjectId` |
  | 9 | 8 | `planFingerprint` |
  | 17 | 1 | `kind` (2 `unique`, 3 `check`) |
  | 18 | 4 + n | `planLen`, then the serialized plan |
  | | 4 + n | `outLen`, then the output descriptor |
  | | 4 + n | `srcLen`, then the source descriptor |

  The cleaner keeps type-20 records. Consumers never receive them.
- **Reservation record** (type 16): the payload is `[version u8][reservedThroughN u64]`. An autoincrement leader appends
  it before it hands out ids above the last reserved `n`; a new leader starts above the highest one it reads. Every reader
  skips it, so it never reaches a consumer.
- **Limits and refusals**: the client sends at most 4096 records in a `View` batch (a verdict source refuses more). A `View` produce is answered `InvalidRequest` on the
  legacy gateway stream, while the cluster feature level is below checked writes, or without a replica to fold from. A
  `View` bit on a transactional batch is ignored: the transaction commit carries the verdict. An atomic batch with more records than
  `RUNE_TXN_JUDGED_RECORDS_MAX` on a verdict source is answered `InvalidRequest` (39). Error codes 55 to 63 are sent only to sessions at 17 or above.
- **Intents** (record `attributes` bits 0-1, see [Record batch](#record-batch-v2)) are judged against the topic's key
  index when the batch is folded, in offset order, each record against the accepted records before it. A rejected intent
  is a `View` trailer entry with `code` 56 or 57 and `constraintId` 0. In a transaction it is judged at the vote and a no names
  its code in the `EndTxnResponse`. An intent is refused with `InvalidRequest` on a
  topic without `key.index`, on the legacy gateway stream, below feature level 5, or without an idempotent producer and either
  the `View` bit or a transaction. While a new key index has not yet been logged in the partition's constraint set
  the broker answers `NotLeader` (retriable). Plain upserts and deletes need none of this and are never rejected by the
  key index.
- On an untyped (raw) topic the broker stores the record intent bits as sent and does not interpret them.
- **Autoincrement.** A record with intent `Insert` and a key of the key field's width in zero bytes is a key-0 create; the
  broker patches the assigned id into its key and into the key field of its value. On a topic with `autoIncrement` a record
  routes to partition `id mod partitionCount` (`Modulo`: the little-endian `uint64` of a key of 1 to 8 bytes; an empty or wider
  key is partition 0). A batch with key-0 creates holds only key-0 creates, and needs an idempotent producer id and `All`
  (and `View` when the topic's views can reject the create). `ProduceResponse.assignedBase` is the id of the batch's first
  record, record `i` having `assignedBase + i * partitionCount`. It is `0` for every other batch, an import (a batch of
  explicit-key inserts) included, and a duplicate's answer repeats the original's. A key-0 batch from a producer the leader
  cannot track yet is `NotLeader` (retriable); a mixed batch, a non-idempotent or non-`All` batch, and a plain upsert are
  `InvalidRequest`. An explicit key without the `IMPORT` grant is `NotAuthorized` (45); one above the maximum or outside
  the partition is `InvalidRequest`. The counter reaching `autoincrement.max` is `AutoIncrementExhausted` (58).
- **Key features** (`DefineTopic`, `TopicSchemaResponse`): a 14-byte block, little-endian
  ([`CatalogTypes.h`](../../protocol/include/rune/protocol/CatalogTypes.h)). On `DefineTopic` it follows the DDL
  extension and needs it; on `TopicSchemaResponse` it follows `partitionCount` and is sent only for a topic whose
  features are not the defaults (an older decoder stops before it).

  | Offset | Field | Type |
  |---|---|---|
  | 0 | `flags`: bit 0 `keyIndex`, bit 1 `autoIncrement`; other bits make the frame undecodable | u8 |
  | 1 | `partitioner`: 0 `Fnv1a`, 1 `Modulo`; above 1 is undecodable | u8 |
  | 2 | `autoIncrementMax` (default 2^53 - 1) | u64 |
  | 10 | `autoIncrementReserve` (default 4096) | u32 |

  `autoIncrement` implies `keyIndex` and `partitioner` 1; the broker stores both whatever the request carried, and the
  features never change after the topic is created. `autoIncrementMax` is the last assignable id and `autoIncrementReserve` the
  ids reserved per reservation record. `DefineTopic` with `partitioner` 1 and no `autoIncrement` is refused.

## Wide view keys and scans (client version 17)

A view can be keyed by up to 128 bytes of typed key components instead of one `uint64` (see [View keys](codec.md#view-keys)).
Frames 88 to 90 read such a view. They are sent only to a session at client version 17 or above; the C++ client ends the
call `BrokerTooOld` without sending when the seed's `brokerVersion` is below 10. A plan with key components, `lower()` or
an aggregate over key components is a plan v2 and `CreateView` refuses it with `InvalidRequest` ("plan v2 needs the checked-writes feature level")
while the cluster is below the checked-writes feature level.

- **`GetViewRowK`** (88) is a point read by the key's bytes (1 to 128, the encoding in [View keys](codec.md#view-keys)). It is
  answered with a `ViewRowResponse` (73), the same shape as `GetViewRow`. `partition` is `-1` for a sink with one partition,
  else the source partition that owns the key (the C++ `ViewKeyBuilder::partition()`). A key of a length other than the
  view's key width, or a `GetViewRow` on a view with a wide key, is `InvalidRequest`.
- **`ScanViewRows`** (89) reads one page of the rows whose key starts with `prefix`: every key component but the last,
  so a view with a one-component key cannot be scanned (`InvalidRequest`). A keyed view has one partition and a scan reads it. `limit` 0 means 256; a page holds at most 256 rows and 61,440
  row bytes. The answer is a `ScanViewRowsResponse` (90): `nextToken` is empty when the scan is complete, else 12 bytes to
  send back as `resumeToken`; `rows` is `rowCount` entries of `[length u32][view row]`, each row decoded like a `GetViewRow`
  row. A page is read in one worker step, so it is consistent.
- **Pages are in hash order, not key order.** The 12-byte token is `[r u32 LE][word u64 LE]`: the position of the last row
  returned, where `word` is the row's slot word and `r` is the bit reversal of the low 32 bits of its hash. A page returns
  the rows strictly above the token in `(r, word)` order, so a row's position depends on its key alone. A row that stays
  under the prefix for the whole scan is returned exactly once whatever is inserted, erased or reorganized between pages; a
  row inserted or erased during the scan may or may not be returned; no row is returned twice. A scan is not a snapshot
  and always finishes, whatever the write rate. Rows whose hashes are equal (a 64-bit collision of two wide keys) are
  returned together on one page; a group that does not fit an empty page is `RowTooLarge`.
- **A page may hold no rows and still carry a token.** A worker step looks at a bounded number of index slots (4096); when
  that runs out the page is `Ok` with `rowCount` 0 and a `nextToken` to continue from. Only an empty `nextToken` ends a
  scan. A token of the wrong length is `InvalidRequest`; status 8 (`CursorInvalidated`) is kept for a malformed token and
  is no longer answered for a write under the prefix. A token for a prefix with no rows left ends the scan. The C++
  `ViewRowResult::kCursorInvalidated` shares the value 8 with the client-side `kNotLeader`.
- **`minOffset`** (both frames, `-1` = none) is the read-your-writes barrier for a write acknowledged with the `ProduceResponse`
  `baseOffset`: the read waits until the view has folded that source offset. A waiting read is held for at most 100 ms and
  is then answered `NotReady`; a worker holds at most 256 reads, and one more is answered `NotReady` at once. Without it a
  read still waits for the fold to reach the committed high watermark it found on arrival.
- Statuses are those of `ViewRowResponse`. A scan whose first row would not fit the page is `RowTooLarge`. An undecodable
  body (a key of 0 or more than 128 bytes, a prefix of 0 or more than 127 bytes, a token of a length other than 0 or 12), a
  prefix of the wrong width and a token of the wrong length are `InvalidRequest`.
- Error codes 59 to 61 are `CreateView` refusals sent to sessions at 17 or above: 59 `ViewKeyExpressionInvalid` (a key
  component that does not compile: a type that is not an integer, enum, bool or `string(N)`, `lower()` on a type other than
  `string(N)`, a duplicate component, a key wider than 128 bytes), 60 `ViewConstraintInvalid` and 61 `ViewUniqueInvalid`.
- A view whose key is not exactly the source key needs a source with one partition (`ViewNotCoPartitioned`).

## Encrypted frames (TLS 1.3 / AEAD)

When `Connect` offers ciphers and the broker chooses one, `HandshakeContinue` (10) and `HandshakeContinueResponse` (11)
carry the TLS handshake. Afterwards each frame is
`[24-byte header][12-byte crypto subheader][ciphertext][16-byte tag]` with schema id `121`: 28 bytes of overhead, and the
36 bytes of header plus subheader are the AEAD associated data. The subheader is `version u8, cipher u8, epoch u8,
senderId u8` (0 = gateway, `i+1` = worker `i`) and a `seq u64`
([`CryptoEnvelope.h`](../../protocol/include/rune/protocol/client/CryptoEnvelope.h)).

## Stats (client version 17)

`GetStats` (91) reads the counters of one partition of a topic or a view from the partition's leader. It is sent only to a session at
client version 17 or above (a lower session is answered `StatsResponse` status 5), and the C++ client ends the call `BrokerTooOld`
without sending when the seed's `brokerVersion` is below `kBrokerVersionCheckedWrites` (10). The caller needs `DESCRIBE` on the
topic or view. The leader's owning worker answers, so a read costs about as much as a `GetViewRow`.

- **Request** (body 9 bytes): `scope` (1 `TopicPartition`, `objectId` is the topic id; 2 `ViewPartition`, `objectId` is the view's
  topic id), `objectId`, `partition`. A partition is served by the broker that leads the source topic's partition (for a view,
  the source's).
- **Response** (92, body 4 bytes, then the variable part): `status`, `count`, the stats in **strictly ascending** `statId`
  order (at most 64), then `nConstraints` and the per-constraint entries (at most 256). A decoder requires the exact frame length.
- **Statuses**: 0 OK, 4 unknown object (an unknown id, a view not served here), 5 invalid request (an undecodable body, a partition
  out of range, a client below 17), 6 busy (the worker ring is full), 45 not authorized. Status 2 is never sent: a broker that does
  not lead the partition, or leads a view that is not ready yet, answers an `ErrorResponse` `Redirect` (3) or `NotLeader` (2) and the
  client follows it as it does for `GetViewRow`.
- **An id is present only when the counter exists** on the partition: a topic without a judge reports none of ids 1 to 7 and 11 to 20, one
  without `autoincrement` none of 8 to 10, so a client asks `StatsResult::get(id)` and treats an absent id as "not applicable".

| Id | Name | Scope | Meaning |
|---|---|---|---|
| 1 | `rejectionsKeyExists` | topic | Records the judge rejected as `KeyExists` (56). |
| 2 | `rejectionsKeyNotFound` | topic | Records rejected as `KeyNotFound` (57). |
| 3 | `rejectionsConstraint` | topic | Records rejected as `ConstraintViolation` (55). |
| 4 | `verdictLag` | topic | Log end offset minus the offset the judge has passed. |
| 5 | `verdictWatermark` | topic | The committed watermark `W` below which every record's verdict is final. |
| 6 | `verdictMarkersWritten` | topic | Verdict markers (control record 17) the judge appended. |
| 7 | `rejectedNotCompacted` | topic | Rejected records at or after the last cleaned offset, not yet compacted away. |
| 8 | `autoIncrementCounter` | topic | The next id the partition would assign. |
| 9 | `autoIncrementReservedThrough` | topic | The highest id reserved in the log (0 = none). |
| 10 | `autoIncrementIdsBurned` | topic | Ids taken and never stored: a refused or failed batch, a reservation a failover left unused. |
| 11 | `txnVotesYes` | topic | Transaction votes answered yes. |
| 12 | `txnVotesNo` | topic | Transaction votes answered no. |
| 13 | `txnVotesNoConstraintViolation` | topic | No votes with `ConstraintViolation` (55). |
| 14 | `txnVotesNoKeyExists` | topic | No votes with `KeyExists` (56). |
| 15 | `txnVotesNoKeyNotFound` | topic | No votes with `KeyNotFound` (57). |
| 16 | `txnVotesNoTxnTooLarge` | topic | No votes with `TxnTooLarge` (62). |
| 17 | `txnVoteLatencyNsMax` | topic | The longest wait from a vote request to its answer, in nanoseconds. |
| 18 | `txnVoteLatencyNsTotal` | topic | The sum of those waits. |
| 19 | `foldPausedOnTxnLockNs` | topic | Nanoseconds the judge paused other writers' verdicts on a row a transaction had locked. |
| 20 | `txnMarkerWithoutVote` | topic | Transaction markers that arrived with no vote taken. A coordinator-bug alarm: it stays 0. |
| 32 | `uniqueConflicts` | view | Extra claimants of a `unique` view's keys: live source records that share a key with its owner. Reported for a `unique` view only. |
| 33 | `scanRows` | view | Rows returned by scans. |
| 34 | `scanPages` | view | `ScanViewRows` pages served. |
| 35 | `transientRecordsEvicted` | view | Records a transient source dropped before the view folded them. |
| 36 | `transientSourceResets` | view | Times the source ring was recreated or cut under the view's cursor. |

Ids 1 to 31 are topic scope and 32 and up are view scope; new ids are appended. The constraint entries are the per-constraint
rejection counts of a topic's verdict source, `constraintId = viewObjectId << 16 | ordinal`.

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
| 29 | `HandshakeFailed` | TLS handshake or crypto negotiation failed, or a plaintext Connect to a broker that requires TLS (every authenticating broker). |  |
| 30 | `SchemaIncompatible` | `alter topic` incompatible with the compatibility mode. |  |
| 31 | `SchemaConflict` | `expectedVersion` is stale. | re-read the schema, then retry |
| 32 | `ViewExists` |  |  |
| 33 | `ViewSourceNotFound` |  |  |
| 34 | `ViewFieldUnresolved` | A plan names a field the source does not have, or an aggregate view's non-key column is not an aggregate. |  |
| 35 | `ViewKeyInvalid` | `keyU64` does not resolve under the view's key type. |  |
| 36 | `ViewNotCoPartitioned` | An aggregate view, or a view keyed by anything but the source key, over a source with more than one partition. |  |
| 37 | `ViewSourceTransient` | Reserved; no longer sent. A persisted view may read a transient source. |  |
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
| 48 | `AuthUnavailable` | The broker's `__security` state is not current (catching up after a start, or stale: cut off its leader or the consensus): no login, `__security` read or security DDL is served. As the session-level `ErrorResponse` (correlation id 0): the broker stayed stale too long and ended the session. Sent to every client version. | retried: back off and reconnect fresh, rotating brokers; never terminal |
| 49 | `RecordEgressUnavailable` | Authenticating broker only: the session's own record stream for the partition's worker could not be built (there is no shared-stream fallback). Sent in `SubscribeResponse.errorCode` on the pending Subscribe. | retried a few times (a bind resets the count), then fails the subscription or the group consumer with an error |
| 50 | `UnstableOffsetCommit` | Client 10: `OffsetFetch` / `OffsetCommit` of a partition an unfinished transaction holds a pending offset on (until its completion is durable), or while the transaction coordinator is not loaded. Nothing is appended. | retried after a backoff, never surfaced |
| 55 | `ConstraintViolation` | Client 17: the record broke a view rule when it was folded; the constraint id is `viewObjectId << 16 \| ordinal` (ordinal 0 = `unique`). Never visible. | per-record verdict of a `View` ack |
| 56 | `KeyExists` | Client 17: `insert` on a key that exists. Never visible. | per-record verdict of a `View` ack |
| 57 | `KeyNotFound` | Client 17: `update` or delete-existing on a key that is absent. Never visible. | per-record verdict of a `View` ack |
| 58 | `AutoIncrementExhausted` | Client 17: the counter reached `autoincrement.max`. Nothing is stored and the counter is unchanged. | not retried |
| 59 | `ViewKeyExpressionInvalid` | Client 17: a view key expression does not compile (a bad type, `lower()` on a non-string, a duplicate or over-wide key, the identity rule). | DDL compile error |
| 60 | `ViewConstraintInvalid` | Client 17: a `check` that does not compile, or a constraint limit (32 verdict views, object id 65536 or above, plan record over 65,536 bytes). | DDL compile error |
| 61 | `ViewUniqueInvalid` | Client 17: a `unique` clause does not compile. | DDL compile error |
| 62 | `TxnTooLarge` | Client 17: a transaction has more records on a partition than `RUNE_TXN_JUDGED_RECORDS_MAX`. The whole transaction aborts. | vote in `EndTxnResponse`, or the `Produce` that takes it past the limit |
| 63 | `TxnConflict` | Client 17: another writer wrote one of the transaction's keys after its write and before its commit. The whole transaction aborts. | vote in `EndTxnResponse` |
| 64 | `LeaseNotFound` | Client 18: no live lease for the key, or a lease at a different version. A session below 18 is answered `InvalidRequest` (39). | per-record verdict of a `View` ack on a lease topic, or a `LeaseKeepAlive` entry |
| 65 | `LeaseLimitExceeded` | Client 18: the topic's `lease.max_keys` or the namespace's max leases is reached. | per-record verdict of a grant |
| 66 | `LeaseTtlTooLarge` | Client 18: the requested TTL is above the topic's `lease.max_ttl`. | `Produce` / `LeaseKeepAlive` error |

The broker-internal storage errors (`rune::protocol::ErrorCode`, [`Errors.h`](../../protocol/include/rune/protocol/Errors.h))
are a separate, small enum used inside the log: `OffsetOutOfRange`=1, `CorruptRecord`=2, `InvalidRecordBatch`=3,
`IndexOffsetOverflow`=4. They do not cross the wire.

## Other schemas

- **Replication** (`schemaId 112`, SBE, `schema/rune-replication.xml`, generated headers in
  `include/rune_protocol_replication_sbe/`): `FetchRequest`, `FetchResponse`, `AlterIsr`, `AlterIsrResponse`,
  `InternalIsr`, `InternalIsrAck`, `OffsetsForLeaderEpochRequest`/`Response`, `ReconcileFetchRequest`/`Response`. Broker to
  broker only; inter-broker frames are sealed with the cluster key
  ([operations](../operations.md#the-cluster-key)).
- **Transaction vote** (cluster plane, `schemaId 112`, hand-rolled little-endian blocks after the 8-byte header): template 15
  `TxnVote`, coordinator to partition leader (40-byte block: `coordinatorNodeId`, `coordinatorEpoch`, `token`, `topicId`,
  `partition`, `producerId`, `producerEpoch`, `leaderEpoch`), and template 16 `TxnVoteResponse`, leader to coordinator (48-byte
  block: `token`, `topicId`, `partition`, `producerId`, `coordinatorEpoch`, `producerEpoch`, `errorCode` for the transport,
  `code` for the vote, `constraintId`, `failingOffset`).
- **Internal ring** (`schemaId 7`, SBE, `schema/rune-internal-ring.xml`): messages between threads of one process
  (`WorkerOp`, `WorkerReturn`, `FlushRequest`, `FlushCompletion`). They never cross a wire.
- **Conformance vectors**: `protocol/conformance/` holds generated golden vectors for the client frames; see its
  [README](../../protocol/conformance/README.md).
