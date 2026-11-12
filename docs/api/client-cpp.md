# C++ client API

Namespace `rune::client`, headers under `client-cpp/include/rune/client/`; start with
[`Client.h`](../../client-cpp/include/rune/client/Client.h). Every example below was compiled against these headers.
Settings are listed in [configuration.md](../configuration.md#client-configuration); error codes in
[wire-protocol.md](wire-protocol.md#error-codes).

Contents: [model](#threading-model) · [connect](#connect) · [authentication](#authentication-and-security) ·
[topics](#topics-and-ddl) · [produce](#produce) · [subscribe](#subscribe) · [pause and resume](#pause-and-resume) · [filters](#subscription-filters) · [lease watch](#lease-watch) · [groups](#consumer-groups) ·
[transactions](#transactions) · [typed topics](#typed-topics-and-routing) · [views](#materialized-views) ·
[catalog reads](#catalog-and-security-reads) · [errors](#errors-and-failure-handling)

## Threading model

- **No locks, no futures.** App threads never touch duty-owned state. A request is a command on the client agent's ring;
  the result comes back as a **completion**.
- **`RunnerMode::Thread`** (default): a background runner thread runs the duty cycle under the configured `IdleMode`.
- **`RunnerMode::Invoker`**: no client threads at all. The constructing thread calls `Client::doWork()` in its own loop,
  for example once per server tick. Callbacks then run inside `doWork()`.
- **Two ways to issue an operation.** The token form (`connect(target, token)`) returns an `OpResult`; only `Accepted`
  is followed by exactly one `Completion`, delivered into a `Completions` queue you poll on the issuing thread, or to a
  `CompletionHandler` on the duty thread. The `*AndWait` form blocks and throws `std::runtime_error` on any failure
  other than a broker error code. Never call a `*AndWait` from a callback; it throws `std::logic_error`.
- **Exactly once.** An operation either returns a non-`Accepted` `OpResult` (no completion ever) or completes exactly
  once: with its result, its own `Timeout`, a broker error, `NotConnected`, `SessionLost` or `Closed`.
- Handles (`Producer`, `Subscription`, `GroupConsumer`) are **move-only**. A destructor never
  blocks: it equals `closeImmediately()`. Call `close()` first when delivery matters.
- The hot path allocates nothing: `send`, `trySend` and the token ack forms write into preallocated rings.

## Connect

```cpp
#include <rune/client/Client.h>
using namespace rune::client;
using namespace std::chrono_literals;

ClientConfig cfg;
cfg.gatewayChannel  = "aeron:udp?endpoint=node1:40200";   // any broker's client endpoint
cfg.gatewayStreamId = 1;
cfg.egressChannel   = "aeron:udp?endpoint=me:40400";      // where replies arrive
cfg.egressStreamId  = 2;
cfg.username        = "ws1";
cfg.passwordFile    = "/run/secrets/ws1.pw";              // read once at construction

Client client(cfg);
client.onError([](const std::string& what) { std::cerr << "rune: " << what << "\n"; });
client.connectAndWait(10s);                               // throws std::runtime_error on failure
AuthStatus a = client.authStatus();                       // Authenticated, PasswordChangeRequired or Anonymous
```

- One session goes to the seed broker; leader redirects and reconnects are handled for you. Callbacks (`onConnected`,
  `onError`, `onDisconnected`) are set before `connect`.
- TLS 1.3: set `cfg.offerCrypto = true` (and `cfg.tlsTrustPem` for a private CA). An authenticating broker serves only TLS
  sessions, and a client with credentials sends them only over TLS: without `offerCrypto` it throws at construction, and
  `requireCrypto` is forced on. Each session checks the certificate against its own endpoint's host unless
  `cfg.tlsExpectHost` names one.
- From a game loop, set `cfg.runnerMode = RunnerMode::Invoker` and call `client.doWork()` once per tick:

```cpp
while (running) {
    client.doWork();                 // one duty cycle; returns the work done (0 = idle)
    tickWorld();
}
```

- `close()` drains (every producer flushed, open transactions aborted) and then tears down, bounded by
  `closeTimeoutMs`. `closeImmediately()` and the destructor never wait for the broker.

## Authentication and security

Authentication is on by default; a client without credentials is answered `AuthenticationFailed` (44). The client logs
in with SCRAM-SHA-512 once per broker session. See [wire-protocol.md](wire-protocol.md#authentication-flow-scram-sha-512)
for the exchange and [SECURITY.md](../../broker/docs/SECURITY.md) for the model.

| Condition | What the client does |
|---|---|
| `AuthenticationFailed` (44) | Ends the session. A rate-limited login (`retryAfterMs`) is retried after the hint. |
| `NotAuthorized` (45) | Fails exactly that request, never re-sent. A subscription whose `READ` was revoked ends with `SubscriptionFailReason::NotAuthorized`; a group consumer with `GroupFailReason::NotAuthorized`. |
| `SessionRevoked` (46) | Completes everything in flight and is terminal for the session: no reconnect. |
| `PasswordChangeRequired` (47) | Fails that request. Only a self password change is accepted. |

- A client **with** credentials refuses a broker that skips the login (a dev-mode broker, an old broker) with
  `AuthenticationFailed`. Set `allowUnauthenticatedBroker = true` to accept it.
- `authErrorName(code)` returns the name of codes 44 to 48 for `Completion::errorCode` when the status is `BrokerError` (48 `AuthUnavailable` is retriable: try another broker).
- A password is never logged and never sent; only a proof derived from it. Setting both `password` and `passwordFile`
  throws `std::invalid_argument`.

### Security DDL

Users, roles and grants are changed with Rune QL statements, parsed by `rune-ql` and sent as a `SecurityPlan`. The client
builds the SCRAM verifier locally (at least `minPasswordLength` = 12 characters, else `std::invalid_argument` and nothing
is sent); only the redacted statement text reaches the broker.

```cpp
#include <rune/ql/Parser.h>
#include <rune/ql/Backends.h>

auto stmt = rune::ql::parse("GRANT READ ON TOPIC PREFIX 'world_' TO ROLE world_server;");
if (!stmt) return;                                          // stmt.error(): line, col, message
auto plan = rune::ql::applySecurityPlan(*stmt);             // SecurityPlan (only DDL statements are plans)
if (!plan) return;
SecurityDdlResult r = client.securityDdlAndWait(std::move(*plan), DdlOptions{}, 15s);
if (r.errorCode != 0) std::cerr << r.detail << "\n";
auto users = client.securityUsers(10s);                     // ADMIN on CLUSTER only
```

| Call | Purpose |
|---|---|
| `securityDdl(plan, ddl, target, token)` / `securityDdlAndWait(plan, ddl, timeout)` | Every security DDL: users, roles, grants, passwords. |
| `bootstrapSecurity(...)` / `bootstrapSecurityAndWait(adminName, oneTimePassword, clusterKeyFileContents, timeout)` | The one-time first-admin bootstrap of a fresh cluster. The MAC under the cluster key proves possession. |
| `securityHistory(filter, timeout)` | The replayed `__security` events. |
| `securityGrantsFor(kind, name, asOf, timeout)` | Effective grants of a user or role, direct and through roles. |
| `securityUsers(timeout)`, `securityRoles(timeout)` | Current users and roles. |
| `securityReader()` | The cached reader behind the four calls above. |

Reads of `__security` need `ADMIN` on `CLUSTER`; any other principal gets `Kind::Rejected` with broker code 45.
`rune::client::security::buildVerifier`, `fillSecurityDdl` and `buildBootstrap` (`security/SecurityDdl.h`) are the low-level
verifier builders.

## Topics and DDL

Topics exist only through DDL: the broker has no boot-topic setting. Every DDL is sent to any broker; a broker that is not
the catalog leader answers with a redirect that the client follows (at most `Client::kMaxDdlRedirects` = 3 hops), then
retries `NotLeader` for `ddlRetryBudget`. Every DDL carries a 128-bit request id that stays the same across resends, so a
retry returns the original result.

```cpp
TopicOptions opts;                    // replicationFactor 0 = every broker
opts.replicationFactor = 3;
DdlOptions ddl;                       // statement text, client name, request id
auto untyped = client.defineTopicAndWait("events", 3, /*keyField=*/"", {},
                                          Persistence::Disk, opts, ddl, 15s);
if (untyped.errorCode != 0) std::cerr << "defineTopic: " << untyped.errorCode << " " << untyped.detail << "\n";

auto d = rune::codec::DescriptorBuilder("player", 1)
             .add("userId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
             .addFixedStr("name", 32)
             .add("score", rune::codec::PrimType::Int64)
             .build();
auto typed = client.defineTopicAndWait("player", 4, "userId", rune::codec::serialize(d),
                                        Persistence::Disk, TopicOptions{}, DdlOptions{}, 15s);
DdlResult dropped = client.dropTopicAndWait("events", DdlOptions{}, 15s);
```

A broker error code comes back in the result (`DefineTopicResult::errorCode`, `detail`), not as an exception:
`TopicExists` (25), `InvalidSchema` (23), `InvalidTopicName` (40), and so on. `Persistence::Transient` makes a topic that
is replicated in memory and never written to disk. The usual way to create topics is Rune QL (`rune ql`, see
[ql.md](ql.md)); `Client::defineTopic*` is the same operation from code.

| Operation | Token form / blocking form |
|---|---|
| Define a topic | `defineTopic(...)` / `defineTopicAndWait(...)` (several overloads) |
| Drop a topic or view | `dropTopic`, `dropView` / `dropTopicAndWait`, `dropViewAndWait` |
| Change retention and compaction settings | `alterTopicConfig` / `alterTopicConfigAndWait(name, expectedVersion, changes, ddl, timeout)` |
| Add, drop, rename columns | `alterTopic` / `alterTopicAndWait(name, expectedVersion, opsBlob, ...)`; `opsBlob` is `rune::codec::encodeAlterOps` |
| Fetch a schema | `getTopicSchema` / `getTopicSchemaAndWait(name [, fingerprint])` |
| List topics | `listTopics(timeout)` returns `(name, topicId)` pairs in definition order |

`TopicOptions::keyFeatures.keyIndex = true` creates the topic with a **key index**, which
write intents need. The broker refuses it with `InvalidRequest` and a detail: "key.index needs every broker at feature level
5", "key.index needs a typed topic", "key.index needs a key", "key.index needs a persisted topic", "key.index needs a key of
at most 8 bytes". `autoIncrement` (with `autoIncrementMax` and `autoIncrementReserve`) creates an
[autoincrement](#autoincrement) topic and implies the key index and `partitioner = Modulo`; the broker refuses it with
`InvalidRequest` and a detail such as "autoincrement needs cleanup.policy = compact" or "autoincrement needs a new topic".
A define with key features against a broker older than version 10 completes with `OpStatus::BrokerTooOld`.
`TopicSchemaResult::keyFeatures` (valid when `hasKeyFeatures`) reports the stored features of an opened topic; `openTopicAndWait`
takes the handle's partitioner from it.

`DefineTopicResult::topicId` plus the partition count you passed make the `TopicHandle{topicId, partitionCount}` that a
producer needs. `Client::topic(name)` finds a handle only in the static `ClientConfig::topics` list.

### Raw and typed handles

A typed topic validates every write; an untyped one is a raw log. A handle binds only to its own kind:
`openTopicAndWait<T>` refuses an untyped topic ("is an untyped (raw) topic") and `rawTopicAndWait(name)` refuses a typed
one ("is a typed topic"), as it does an unknown name. A raw `send` to a typed topic must use the key field's exact bytes
(`Key(uint32_t{7})` for a `uint32` key).

## Produce

```cpp
TopicHandle events = client.topic("events");     // or TopicHandle{topicId, partitionCount}
Producer p = client.producer({.acks = Acks::All, .lingerUs = 200});

std::vector<uint8_t> bytes{1, 2, 3};
p.send(events, Key(uint64_t{42}), bytes);        // blocks only on back-pressure

SendResult r = p.trySend(events, Key(uint64_t{42}), bytes);   // never blocks
if (r == SendResult::BackPressured) { /* retry after the next duty cycle */ }
```

- **`Acks`**: `None` (no response, always non-idempotent), `Leader`, `All` (the in-sync replicas hold it). Acked producers
  are **idempotent**: the broker deduplicates a retry by `(producerId, epoch, sequence)`. The durability contract of
  each level:
  - `None`: no acknowledgement; a record can be lost without the producer knowing.
  - `Leader` (the default, like Kafka `acks=1`): acknowledged once the partition leader has the record. If the leader
    restarts or is lost before its followers replicate the record, an **acknowledged record can be lost**.
  - `All`: acknowledged once the in-sync replicas hold the record and the ISR has at least `minIsr` members
    (`RUNE_MIN_ISR`; the default is the cluster majority, 2 on a 3-node cluster). With `minIsr` at least 2, an
    acknowledged record survives the loss of any single broker. With `minIsr` 1 (a single node, or
    `RUNE_MIN_ISR=1`) an ISR shrunk to the leader alone still acknowledges, so one broker failure can lose it.

  Use `Acks::All` whenever a record must not be lost.
- **`Acks::View`** is a flag that combines with the above (`Acks::All | Acks::View`): the ack waits until the batch is
  folded into the views and carries the per-record write verdicts. It needs a broker that supports checked writes; against
  an older broker every send completes locally with `OpStatus::BrokerTooOld`. `View` batches hold at most 4096 records
  (`claimBatch` with more returns `SendResult::MessageTooLarge`, `BatchBuilder` throws `std::length_error`). On a verdict source each record of a `View` batch completes as it was judged: `Ok` with its offset, or
  `BrokerError` with `errorCode` 55 to 57 and `constraintIdOf(completion)`; a `claimBatch` or Tier-2 batch of two or more records completes once and reports every
  record: `Ok` when all were accepted, else `RecordsRejected` (15) with `value` = the base offset, the first rejection's
  `errorCode`, and a payload of every rejection (`BatchVerdicts`: `rejectedCount()`, `rejection(k)`, `accepted(i)`,
  `offsetOf(i)`, an accepted record's offset, -1 for a rejected one); the other records were stored at base + index. A batch of one record keeps the single-record form
  (`BrokerError`; `value`, like a rejected single `send`, is the rejected record's own offset). The completion reserves its full size up front (`claimBatch` above the queue's record
  limit returns `MessageTooLarge`). A retry inside the broker's window replays the original verdicts; a retry outside it completes the whole batch
  with `OpStatus::VerdictUnknown` (`errorCode` 21). `OpStatus::StoredIdUnknown` (13) is the same for a key-0 create (see [Autoincrement](#autoincrement)). A transactional
  producer never sends the `View` bit. `SendResult::WriteNotAllowed` reports a write the broker refuses for the topic. On an untyped (raw) topic the broker stores the record intent bits as sent and does not interpret them.
- **`WriteIntent`**: every `send`, `trySend`, `sendTo` and `trySendTo` takes a trailing `WriteIntent` (`Upsert`, the
  default, `Insert` or `Update`), carried in the record's attribute bits 0-1. See [Write intents](#write-intents).
- **`Key`**: up to 16 inline bytes, built from a `uint64_t`/`int64_t`, a trivially-copyable value or a byte span. The key
  picks the partition (`Key::partition(n)`, FNV-1a 64) and is stored with the record. A key is never truncated: a span of
  more than 16 bytes is an over-long key (`Key::tooLong()`), `trySend` returns `SendResult::KeyTooLong` and `send` throws
  `std::invalid_argument`; a trivially-copyable value wider than 16 bytes does not compile.
- **Record size**: key plus value at most `Producer::kMaxRecordBytes` = 16 KiB. A larger one is rejected at append:
  `trySend` returns `SendResult::MessageTooLarge`; `send` throws `std::length_error`.
- **Batching**: sends within `lingerUs` (default 5 ms) are batched per partition up to `batchBytes` (64 KiB). `lingerUs`
  of 0 defeats batching.
- **`SendResult`**: `Ok`, `BackPressured`, `ValueTooLarge`, `InvalidTopic`, `Closed`, `QueueFull`, `InvalidArgument`,
  `MessageTooLarge`, `WrongThread`, `WrongShard`, `ClaimHeld`, `NotInTransaction`, `KeyTooLong`, `WriteNotAllowed`.
  Anything but `Ok` means the record was not accepted.
- Retriable broker answers (`MinIsrViolation`, `TransientLogFull`) are re-sent with the same identity after a backoff,
  bounded by `produceTimeoutMs` and `deliveryTimeoutMs`.

### Write intents

On a typed topic with a key index an intent says what the write expects. The broker judges it when the batch is folded,
in offset order, against the records accepted before it:

| Intent | Value | Fails with |
|---|---|---|
| `Upsert` | a record | never |
| `Upsert` | empty (a delete) | never |
| `Insert` | a record | `KeyExists` (56) when the key exists |
| `Update` | a record | `KeyNotFound` (57) when the key is absent |
| `Update` | empty (removeExisting) | `KeyNotFound` (57) when the key is absent |

- **Requirements**: intents need `Acks::All | Acks::View` (or `Acks::Leader | Acks::View`) and an idempotent producer
  (every acked producer is), or a transactional producer (the transaction's commit judges them). A rejected record is never visible; its completion is `BrokerError` with `errorCode` 56 or 57
  and `constraintIdOf(completion)` 0. A topic without a key index answers an intent `InvalidRequest`.
- **Refused locally**: these sends return `SendResult::WriteNotAllowed` (nothing is queued and no completion follows), and
  the blocking forms throw `std::invalid_argument` with the text in quotes: an intent on a raw handle ("intents need a typed
  topic"), any send of a `View` producer on a raw handle ("acks View needs a typed topic"), an intent without `View` on a
  non-transactional producer ("intents need acks including View"), and an
  `Insert` with an empty value ("an insert needs a value"). Tier-2 batches and claims carry no intent.
- **Retries**: a lost ack is retried and replays the original verdict, so a retried `Insert` gets its own `Ok`, never
  `KeyExists` against itself. A retry outside the broker's window completes with `OpStatus::VerdictUnknown`.
- `Producer::sendAndWait(topic, key, value, intent, timeout)` sends one record and returns its `Completion`.
- Plain upserts and deletes to a key-index topic need no `View` and no particular client version.

### Autoincrement

On a topic created with `autoIncrement` the broker assigns the key. A record whose key field is 0, written with
`Insert`, is a **key-0 create**: `typed.insertAndWait(rec).key()` is the id the broker gave it, and a `Send` of a key-0 batch
completes `Ok` with `value` = the offset and `aux` = the id (`assignedKeyOf(completion)`; 0 for a record that was rejected).

- **Requirements**: a key-0 create needs an idempotent producer with `Acks::All`; use `Acks::All | Acks::View` where a
  constraint can reject the create, so the completion carries the verdict. A key-0 create is exempt from the intent rules
  that need `View` and from the transaction refusal.
- **Batches and routing**: the producer never puts a key-0 create in a partition batch with other records, or the reverse. Key-0
  creates go to one partition at a time and move to the next after 64 records; every other record on the topic routes by
  `id mod partitions`.
- **Refused locally** (`SendResult::WriteNotAllowed`; the blocking forms throw `std::invalid_argument` with the text in
  quotes): a key-0 create from a producer that is not idempotent ("autoincrement creates need an idempotent producer") or without
  `All` ("autoincrement creates need acks All"), and a plain upsert ("an autoincrement topic takes no plain upserts"). Against
  a broker below version 10 the batch completes `OpStatus::BrokerTooOld`.
- **Explicit ids** (an import) answer `key()` 0: the id is the one you wrote. They need the `IMPORT` grant, `Insert`, and
  `All | View` on an idempotent producer.
- **Lost acks**: a retry inside the broker's window answers the original ids. A retry the broker can no longer match
  completes the whole batch with `OpStatus::StoredIdUnknown`: it was stored, its ids are unknown.
- **Orphaned rows**: an `insertAndWait` that times out may have created the row. Look it up through a unique view before
  inserting again.

### Constraint violations

A write that breaks a `unique` view or a `check` constraint (see [Constraints](ql.md#constraints)) completes with
`OpStatus::BrokerError` and `errorCode` 55 (`ConstraintViolation`). It needs `Acks::All | Acks::View`: without `View` the
broker still rejects the record, but the writer is not told.

- **`WriteResult`**: `code()` is 55, `constraintId()` is `viewObjectId << 16 | ordinal` (ordinal 0 is the view's `unique`
  clause, `k` its `k`-th `constraint`) and `offset()` is the rejected record's own offset. A `Completion` of a single send or of
  `claimBatch(1)`, a 1-record Tier-2 batch and a 1-record `BatchAck` all carry that offset in `value`. A batch of two or more
  records completes once with `RecordsRejected` (15) and reports every rejection; a rejected record's offset is `value + rejection(k).index`
  (`BatchVerdicts::offsetOf(i)` gives an accepted record's offset and -1 for a rejected one).
- **`Client::constraintName(id)`** returns the `ConstraintName{view, constraint}` an id names (`character_name` / `unique`,
  `account_stats` / `max_characters`), or `std::nullopt` for id 0 (the key index) and for an id no view ever had. A view that
  was dropped or replaced since the write still resolves. A miss is answered from one catalog refresh, bounded by
  `kConstraintNameRefreshTimeout` (5 s; the two-argument form takes another bound), so the call blocks. Call it only from the
  thread that owns the `Client`; any other thread gets `std::logic_error`.
- **Racing writers.** The loser of two inserts of one unique key is judged after the winner, so the winner's row is already
  folded: read the unique view with `minOffset` set to the loser's `offset()`.

```cpp
// auth-server, login: two first logins race for one (provider, subject)
rp::gen::User u{.provider = provider, .subject = subject, .createdAt = now};
WriteResult r = users.insertAndWait(u);              // key 0: the broker assigns userId
uint64_t userId = r.key();
if (r.code() == 55) {                                // user_identity already holds this identity
    auto kb = v::ViewKeyBuilder::fromCatalog(state, "user_identity");
    kb->setUInt("provider", provider).setString("subject", subject);
    ViewReadOptions ro{.minOffset = r.offset(), .partition = kb->partition()};
    ViewRowResult row = client.getViewRowAndWait(identityViewTopicId, *kb->key(), ro, 5s);
    userId = /* the userId column of row */;         // the winner's id
}

// character-server, POST /character (the user id comes from the verified JWT)
WriteResult r = characters.insertAndWait(rp::gen::Character{.userId = u, .name = req.name /* ... */});
if (r.ok()) return created(r.key());                 // the new characterId
if (r.code() == 55) {
    const auto c = client.constraintName(r.constraintId());
    if (c && c->view == "character_name") return conflict();       // 409 name taken
    if (c && c->constraint == "max_characters") return badRequest(); // 400 too many characters
}

// character-server, DELETE /character/{id}
WriteResult d = characters.removeExistingAndWait(id);  // code() 57 (KeyNotFound) -> 404; a delete is never a 55
```

### Acknowledgements

Three zero-allocation ways to learn the outcome of a record:

```cpp
Completions cq(client);
if (p.trySend(events, Key(uint64_t{42}), bytes, cq, /*token=*/7) == SendResult::Ok) {
    cq.poll([](const Completion& c, std::span<const uint8_t>) {
        if (c.status == OpStatus::Ok) { /* c.value = the record's offset (-1 for acks=None) */ }
        else { /* c.status, c.errorCode */ }
    });
}
```

- `trySend(..., AckToken{t})` calls the handler you set with `onAck(fn, ctx)` on the duty thread; it must not block.
- `onAck(AckCompletionFn, ctx)` is the same handler with the full `Completion` (`value`, `aux`, `op`, `errorCode`, and
  `constraintIdOf(c)` for a rejected `Send`) plus an empty payload span; the last registration (either form) wins.
- `onBatchAck` / `batchAcksTo(cq)` deliver one `BatchAck` per batch (`baseOffset`, `baseSequence`, `recordCount`,
  `errorCode`, `producerEpoch`); a checked-writes batch also reports `rejectedCount` and `rejections` (every rejected
  record), or the list as the completion payload with `batchAcksTo`.
- Every accepted record is reported exactly once, including ones that never reached the wire (`BatchAck::kClientFailure`).

### Batches and flush

```cpp
auto batch = p.batch(events, Key(uint64_t{42}));   // one atomic append to one partition: contiguous offsets; View producers get a verdict per record. On a verdict source it is all or nothing
batch.add(bytes);
batch.send();
p.flush();
p.closeAndWait(10s);
```

A batch is built in a preallocated pool slot (`ProducerConfig::tier2Slots`, default 64; one slot holds up to
`tier2SlotBytes`, default max(`batchBytes`, `maxFrameBytes`)): no allocation per batch. A slot is taken at
`batch()` and held until the batch is encoded and offered (or fails). A record that would take the batch past a slot throws `std::length_error`.
`batch()` is single-thread per producer (the creating thread, or the thread that last called `bindBatchThread()`, with no
builder live): from another thread it returns a refused builder whose `send()` is `WrongThread`. A builder made when every
slot is in use is refused too (`send()` is `BackPressured`, nothing stored): rebuild the whole batch. `send()` is `[[nodiscard]]`.
A batch to a verdict source is all or nothing. A rejected one has one completion, `BrokerError` with the code in `errorCode`, the
constraint id in `constraintIdOf` and the batch's base offset in `value`; it carries no per-record verdicts.

### Zero-copy claims

```cpp
Claim c = p.tryClaim(events, Key(uint64_t{42}), PositionEncoder::kEncodedLength);
if (c) { PositionEncoder(c.buffer()).userId(42).zone(4).x(1.5); c.commit(); }   // refused: c.result()
auto bc = p.claimBatch(events, Key(uint64_t{42}), 3, PositionEncoder::kEncodedLength);
if (bc) { for (uint32_t i = 0; i < 3; ++i) fill(bc.record(i)); bc.commit(); }
auto w = positions.claim(userId);                  // TypedTopic: the generated encoder over the claim
if (w) { w.zone(4).x(1.5); w.commit(); }           // non-key setters only; the key is already written
```

- A claim returns the record's bytes in place (Thread mode: a producer-ring slot, 2 copies to the wire; Invoker mode: the
  Produce frame, 1 copy). It is false when refused (`result()`: `BackPressured`, `MessageTooLarge`, `Closed`, ...).
  Destruction without `commit()` aborts. Do not hold a claim across blocking work: while an Invoker claim is open, other sends on
  that producer return `SendResult::ClaimHeld`. `commit()` returns false if the claim was abandoned (close, failed partition). Writes into an abandoned
  claim's buffer are harmless (it no longer belongs to any frame); sends resume at once, and a new claim returns `ClaimHeld`
  until the abandoned one is committed, aborted or destroyed.
- `claimBatch` appends its n records contiguously and atomically: nothing foreign lands between them. An unfenced
  claim can join an open `RecordBatch` with neighbouring sends, so it is acked by the `BatchAck`(s) covering that `RecordBatch`,
  not necessarily by one `BatchAck` of n. Call `flush()` (and `doWork()` in Invoker mode) before the claim to fence it into a
  `RecordBatch` of its own.
- Steady-state claims allocate nothing (`rune_producer_claim_bench`).

`Producer::framesSent()` and `recordsSent()` are counters. `ProducerConfig::fromEnv()` builds a config from
`RUNE_LINGER_US` and friends.

## Subscribe

```cpp
Subscription sub = client.subscribe(
    events, Key(uint64_t{42}),
    {.from = From::earliest(), .isolation = Isolation::ReadCommitted},
    [](const Record& r) { /* r.offset, r.timestamp (epoch ms), r.key, r.value */ });
if (sub.finished()) { auto why = sub.failReason(); }
sub.unsubscribe();
```

- Push delivery, in order per partition. `r.key` and `r.value` are zero-copy spans valid **only inside the handler**.
- `From::earliest()`, `From::latest()` (the default) and `From::offset(n)`. `Isolation::ReadCommitted` hides open and
  aborted transactions.
- An overload takes an explicit partition instead of a key.
- `position()` is the resume point (last delivered + 1), `active()` and `finished()` the state, `failReason()` why it ended
  (`RejectedByBroker`, `TimedOut`, `SessionLost`, `NotAuthorized`, `SessionRevoked`, ...) with `brokerErrorCode()`.
- A subscription follows a leader move on its own and resumes from its position. On a transient partition, `dataResets()`
  counts how many times the data was reset under it (records are never skipped silently).
- A plain subscription to a partition another node leads follows that partition's leader on the leader's own session,
  also while a consumer group is active on the same `Client` (the seed, the group coordinator's session, is never moved).
  Only a redirect that would have to move the seed is refused while a group is active: `failReason()` is
  `LeaderElsewhereWhileGroupActive` (use a separate `Client` for it).

### Pause and resume

A consumer that cannot take a record now (its own queue is full) pauses the subscription instead of dropping the record.

```cpp
// Invoker mode: the owner thread assigns the handle first and only then drives the client.
Subscription sub;
sub = client.subscribe(events, Key(uint64_t{42}), {.from = From::earliest()},
    [&](const Record& rec) {
        // keep this record for later: it is delivered again after resume()
        if (!ring.tryPush(rec) && !sub.pause(rec.offset)) dropped.increment();
    });
for (;;) {
    client.doWork();
    // when the ring has room again:
    if (sub.paused()) sub.resume();
}
```

In Thread mode the duty thread can call the handler before `subscribe` has returned, so the handler must not use the handle
until the application has assigned it and published that to the handler. From then on `pause` inside the handler behaves
as in Invoker mode.

- `pause(resumeAt = -1)` releases the broker side of the subscription (an `Unsubscribe`) and parks it. With `resumeAt < 0`
  delivery continues after the last record the handler returned from. With `resumeAt >= 0` the next record delivered after
  `resume()` is the one at `resumeAt`; it may not be above the position (the last delivered offset + 1). Inside a handler,
  `pause(rec.offset)` therefore re-delivers `rec`.
- A pause made inside the handler takes effect at once: no further record of the batch, and none of the old broker
  subscription, reaches the handler. Pausing an already paused subscription only lowers (or keeps) its position.
- `resume()` subscribes again at the stored position. Each record is delivered exactly once: a batch the broker pushes from
  below the position is trimmed. A `dataResets()` count and a leader move are handled as for any subscription.
- `paused()` is true between a successful `pause()` and `resume()` or the end of the subscription.
  `active()` is false while the subscription is paused.
- A `From::latest()` subscription paused before its first record resolves "latest" again at `resume()`, so the records
  produced during the pause are skipped.
- Both calls return `false` when the subscription is finished or unsubscribed, when `resumeAt` is above the position, for
  `resume()` when it is not paused, and always for the subscriptions of a `LeaseWatch` (they are not pausable).
  `pause()` also returns `false` while the client holds 64 releases the transport has not yet accepted; the subscription
  keeps delivering.
- **Visibility across threads.** The calls are safe on any thread. On an application thread, `pause()` returning `true`
  means the request was queued for the client's duty thread, not that it was applied; `paused()` reads `true` on that handle
  at once and turns `false` again if the duty thread did not apply the pause (the subscription ended, or the client had no
  room to hold the release for the broker). It converges through the status the duty thread publishes back. `resumeAt` is checked
  there against the last status the handle has seen, so `pause(x)` can be refused for an `x` the duty thread has already
  delivered. A pause made from inside a message handler (the duty thread) becomes visible to the application thread
  eventually, not immediately: the duty thread publishes it on the status ring and the handle adopts it when the application
  thread next drains it, which `paused()`, `pause()`, `resume()`, `active()` and `position()` all do first. Until then
  `paused()` may still read `false` and `resume()` on the application thread may return `false`. Poll `paused()` (with a
  bounded loop) and call `resume()` once it reads `true`.
- A paused subscription keeps its place and has nothing open on the broker. A lost session or a leader move does not end
  it: `resume()` subscribes again on the current session. `unsubscribe()`, closing the client, and a revoked session end
  it. A `READ` grant revoked while it is paused is reported at `resume()`: the subscription finishes with `NotAuthorized`.

## Subscription filters

The broker can send a subscription only the records whose fields match, so a consumer that wants one world, one player
or one channel does not receive (or pay for) the rest. The filter is built from the generated `E::Filter` of a typed
topic and travels in the `Subscribe`.

```cpp
#include "player_event.gen.h"                      // rune schema generate player_event
using E = rune::gen::PlayerEvent;

SubscribeOptions o;
o.from = From::earliest();
o.filter(E::Filter{.worldId = 7u, .channel = rune::anyOf(1, 2)});   // worldId == 7 and channel in (1, 2)
SubscriptionSet world = client.subscribe(events, AllPartitions{}, o,
    [](const Record& r) { /* r.partition, r.offset, r.value: only the matches */ });
```

- **Filter by example.** `E::Filter` has one `Match` member per filterable field, in schema order. Set a member to a
  value (equal) or to `rune::anyOf(...)` (up to 64 values); unset members match anything; the terms are ANDed. At most 8
  terms. An empty `E::Filter{}` is no filter. Filterable fields: `Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`, `Enum`,
  `Bool` and `string(N)` up to 16. Not `string`, `bytes` or `float64`: those members do not exist on `E::Filter`.
- **Checked when you build it.** A value of the wrong kind does not compile; an integer literal outside the field's
  range (`.worldId = -1` on a `UInt32`) makes `o.filter(...)` throw `std::invalid_argument` before anything is sent.
- **No generated type?** `rune::codec::FilterBuilder::on(descriptor).field("worldId").eq(7u).build()` builds the same
  bytes by field name; pass the result to `o.filter(...)`.
- **Every partition.** `subscribe(topic, AllPartitions{}, opts, handler)` returns a `SubscriptionSet`: one ordinary
  `Subscription` per partition (`set[p]`, `set.position(p)`, `set.unsubscribe()`), `set.batchBytes()` for the bytes the
  broker sent, and `Record::partition` on every record. The set covers the partition count at the time of the call.
- **One entity.** `subscribe(topic, Key(k), opts, h)` picks the partition only. To read one entity's records, add the
  filter: `o.filter(E::Filter{.playerId = k})`.
- **Groups.** `GroupConfig::filter(E::Filter{...})` gives the whole group one filter. The first member fixes it; a member
  with another filter ends with `GroupFailReason::FilterMismatch` and the detail `group filter mismatch`. The group
  needs the Client's own routed fetch.
- **Isolation.** The filter applies after isolation: a `read_committed` subscription still sees only committed records.
- **Positions and notes.** A filtered subscription's `position()` moves past the records the filter skipped, so it
  reaches the log end even when nothing matches. The broker sends a position note after
  `RUNE_FILTER_NOTE_INTERVAL_MS` or `RUNE_FILTER_NOTE_RECORDS` skipped records. Records keep their original offsets; a
  rebuilt batch can have holes. Resume with `From::offset(sub.position())`; a moved subscription does this on its own with
  the same filter.
- **Refusals.** The broker answers a filter it cannot serve explicitly, never by sending everything: the subscription
  ends `RejectedByBroker` with `brokerErrorCode() == 39` (InvalidRequest), and the reason reaches `onError` as
  `subscribe topic <id> partition <p> refused: InvalidRequest (39): <detail>`:

  | Detail | Cause |
  |---|---|
  | `filter needs a typed topic` | the topic has no schema |
  | `(tag N): unknown tag` / `type mismatch` / `variable-length field` / `float field` | the filter names a field the topic does not have, retyped, or cannot be filtered |
  | `filter capacity: ...` | `RUNE_FILTER_MAX_SUBS_PER_PARTITION` or `RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION` is reached |
  | `filter needs RUNE_WORKER_MODE=shard` | the broker runs the legacy worker mode |

  A subscription refused while a topic is being altered (a tag the broker has not installed yet) is refused with
  `unknown tag`; subscribe again after the ALTER completed.
- **`BrokerTooOld`.** A filtered subscription on a broker older than broker version 6 would be answered with every
  record, so the client sends nothing and the subscription ends `SubscriptionFailReason::BrokerTooOld`. Upgrade every
  broker first.
- **Schema changes.** Tags are durable. A filter on a field that a later version dropped matches nothing in the records
  written after the drop; the broker counts them and logs the first one per subscription and version
  (`filtered field absent`). A field added later matches only the records that carry it.
- **Tombstones and short values.** A record with no value (or one too short to hold the filtered field) never matches
  and is counted.

## Lease watch

`LeaseWatch` follows the live leases of a lease topic: it scans the topic's lease view and subscribes to the changes, and keeps
the live set in memory. The owner calls `watch.doWork(nowNs)` on the thread that drives the client (the Invoker pattern), like the other lease types
in `client-cpp/include/rune/client/lease/`. The full contract is in the header,
[`LeaseWatch.h`](../../client-cpp/include/rune/client/lease/LeaseWatch.h).

### Life, close and restart

`life()` is one state, never scattered flags: `Fresh`, `Running`, `Failed`, `Closing`, `Closed`.

| Call | From | To |
|---|---|---|
| `startFromNow(prefixes)` | `Fresh` | `Running` |
| failure of the watch (definitive error, full live set, name heap, a scan over `maxPagesPerScan`, or a row-length mismatch) | `Running` | `Failed` (one `Failed` event) |
| `closeAsync()` | `Running` or `Failed` | `Closing`, then `Closed` when every subscription has finished |
| `closeAsync()` | `Fresh` | `Closed` at once |
| `closeAsync()` | `Closing`, `Closed` | no-op |
| `restart(prefixes)` | `Closed` | `Running` |

- `closeAsync()` returns at once and never calls `client.doWork()`. It cancels the subscriptions and makes outstanding scans
  stale. `doWork` then drops what is left and emits no event. Safe to call from inside a handler: the event being delivered
  still reaches the observers and the bound name. A closed watch emits nothing more, so whatever waits on one of its events (a recipe
  with a pending acquire, say) is not told: the application must tell it, or `restart()` the watch. From an application thread in Thread mode each cancel hands a command to the
  duty thread and waits (never throws) while the command ring is full.
- `isClosed()` is true for `Fresh` and `Closed`. A watch that is closed is destroyed at once, without touching the client.
- `restart(prefixes)` runs a closed watch again in place, over the same live table, names, handler, observers and buffers.
  `InvalidArgument` unless the watch is `Closed`. It allocates nothing itself (the re-subscribes that follow allocate as the
  first start does). The leases the new scan does not confirm end as `Vanished`, and `Ready` is emitted again. The handler runs
  synchronously inside `restart()`: every partition's `RerunBegin` is emitted before it returns.
- A `Failed` watch ends only by `closeAsync()`. A capacity failure (name heap or live set full) can repeat on every
  `restart()`, because the old run's entries still hold their slots: recreate the watch or size it for old plus new.

### Events

Besides `Granted`, `Updated`, `Revoked`, `Expired`, `Vanished`, `Ready` and `Failed`, a watch emits three events about
re-runs. A re-run is a scan after a partition was `Live` once, after its subscription broke or was reborn (a data reset), or
after `restart()`. Only the very first, unbroken scan has none of them.

| Event | Meaning | Fields |
|---|---|---|
| `Confirmed` | a re-run's scan found a held lease at the same version, not yet confirmed by this re-run. This is how a consumer learns of an update made while the partition was not watched. It can repeat inside one bracket, each time with the newest scan's bytes | the key, version and the scanned view row (`bytes`); `offset` = the row's `lease_granted_offset` |
| `RerunBegin` | a partition's re-run starts | `partition`; key empty, version 0, no bytes, offset -1 |
| `RerunEnd` | the partition is `Live` again, after the re-run's `Vanished` events and its buffered records | as `RerunBegin` |

- A re-run that breaks again keeps its one bracket. `Failed` and `closeAsync()` end a re-run with no `RerunEnd`: drop your
  bracket state on both. `restart()` opens a new bracket for every partition.
- A scan that breaks before the partition was ever `Live` is bracketed too: the retry reconciles the `Granted` events of the
  first attempt.
- A name registered on the watch gets `Confirmed` (by key) but never `RerunBegin` or `RerunEnd`.

### Producer guard and transactions

A producer bound to a lease with `LeaseSession::bindProducer(ref, producer)` is blocked while the lease is at risk or lost. A
record refused by the guard completes `DeliveryFailed` with `ClientErrorCode::LeaseAtRisk`. The guard also covers a
transactional producer's commit. Inside an open transaction a refusal poisons the transaction: it becomes `AbortableError`
and the commit cannot succeed.

| Moment (the guard trips, ends, or the bind is refused) | Transaction state | Result |
|---|---|---|
| any of the three | `InTransaction`, records acked or not | `AbortableError` (`DeliveryFailed` / `LeaseAtRisk`); unacked records complete `DeliveryFailed` / `LeaseAtRisk` |
| same | committing, `EndTxn` not sent yet | the commit completes `DeliveryFailed` / `LeaseAtRisk`; `AbortableError` |
| same | committing, `EndTxn` sent (also a re-send) | unchanged: the commit was sent inside the lease and its answer decides. A re-send is never blocked |
| same | aborting, `Ready`, `AbortableError`, `FatalError` | unchanged |
| `commitTransaction` issued while blocked | open | no `EndTxn(commit)`; completes `DeliveryFailed` / `LeaseAtRisk`; `AbortableError` |
| `sendOffsetsToTransaction` issued while blocked | `InTransaction` | no `AddOffsets` or `TxnOffsetCommit`; completes `DeliveryFailed` / `LeaseAtRisk`; `AbortableError` |
| `abortTransaction` while blocked | any | allowed: `EndTxn(abort)` is sent |
| `beginTransaction` while blocked | `Ready` | allowed; every send fails `LeaseAtRisk`, and the commit is refused as above |
| after `Cleared` (re-armed) | `Ready` after the abort | `beginTransaction`, sends and commit work |

A producer is bound to one lease for good. To produce under a new lease, bind a new producer.

## Consumer groups

```cpp
GroupConfig g;
g.autoCommit      = false;
GroupConsumer gc = client.subscribeGroup("matchmaking", {"events"}, g);
gc.onAssigned([](std::vector<TopicPartition> parts) { /* ... */ });
gc.onRecords([&](const RecordSet& rs) { /* handle rs, then */ gc.commit(); });
client.connectAndWait();
```

Register the group and its callbacks **before** `connectAndWait()`. Topic names are resolved against
`ClientConfig::topics`; a name not in the list is skipped. Offsets auto-commit every `autoCommitIntervalMs` unless
`autoCommit` is false; `commit()` commits on demand, `commitSync` / `commitSyncAndWait` waits for the broker.
`isolation = IsolationLevel::ReadCommitted` reads only committed transactions. A group consumer ends with a
`GroupFailReason` (`BrokerTooOld`, `RefusedByBroker`, `RetryBudgetExhausted`, `NotAuthorized`, `SessionRevoked`,
`FilterMismatch`).

At-most-once, at-least-once and exactly-once are choices of `Acks` and of when you commit; see
[Delivery guarantees](#delivery-guarantees).

## Transactions

```cpp
Producer tp = client.producer({.acks = Acks::All, .transactionalId = "trade-worker-1"});
client.connectAndWait();
tp.initTransactionsAndWait();
tp.beginTransaction();
try {
    tp.send(out, Key(uint64_t{1}), a);
    tp.send(out, Key(uint64_t{2}), b);
    const OffsetEntry offsets[] = {{topicId, partition, nextOffset}};
    tp.sendOffsetsToTransactionAndWait(offsets, "economy");   // the consumer group's input offsets
    const TxnCommitResult r = tp.commitTransactionAndWait();   // outputs and offsets, atomically
    if (!r.ok) {   // a partition voted no: the transaction is already aborted, begin the next one
        const auto name = client.constraintName(r.constraintId);   // r.code 55 / 56 / 57 / 62 / 63, r.failingOffset
    }
} catch (const std::runtime_error&) {
    if (tp.state() == TxnState::FatalError) return;   // fenced: a newer producer with this id took over
    tp.abortTransactionAndWait();              // roll back
}
```

- `TxnState`: `Uninitialized`, `Initializing`, `Ready`, `InTransaction`, `CommittingTransaction`,
  `AbortingTransaction`, `AbortableError`, `FatalError`. After a timeout in `CommittingTransaction` the outcome is in doubt:
  retry the same `commitTransactionAndWait()`.
- **A no vote.** On a topic with constraints, `commitTransactionAndWait()` returns a `TxnCommitResult` (`ok`, `code`,
  `constraintId`, `failingOffset`) instead of throwing. `code` is 55, 56, 57, 62 or 63; resolve `constraintId` with
  `Client::constraintName`. After a no the transaction is already aborted and the producer is `Ready`: call `beginTransaction()`
  for the next one. A retried commit after a coordinator failover gets the same no. Every other failure still throws.
- **`View` on a transactional producer** is ignored on sends: the verdict comes from the commit, and a commit that returns `ok`
  means the rows are final on every partition.
- Sequences continue across transactions under one epoch. After a commit lands, if any partition's sequence counter is
  above 2^30 the commit also bumps the epoch (one `InitProducerId`, every partition restarts at 0) and completes `Ok` once
  the bump is answered; if that bump fails the commit still completes `Ok` (it landed) and the producer is `FatalError`.
  A single transaction that writes more than 2^30 records to one partition can still exhaust the sequence space
  mid-transaction (no bump is possible while a transaction is open): it becomes abortable.
- Other methods: `flush`, `close`, `closeAndWait`, `producerId()`, `producerEpoch()`, `transactionActive()`. A transactional
  producer needs `WRITE` on its transactional id.
- A `ReadCommitted` consumer sees a transaction's records only after commit.
- A transaction is a plain `Producer` created with `ProducerConfig::transactionalId`; every produce path (`send`, `trySend`,
  Tier-2 batches, `tryClaim`, `claimBatch`, a typed claim through `openTopicAndWait<T>(name, producer)`) takes part in it.
  Outside a transaction a transactional producer refuses every produce path with `SendResult::NotInTransaction`; on a plain
  producer the lifecycle token calls return `OpResult::InvalidArgument`, and `beginTransaction()` and the `*AndWait` forms throw
  `std::logic_error` ("producer has no transactionalId"). `acks = None` is rejected.
  The status getters (`state()`, `producerId()`, `producerEpoch()`, `transactionActive()`) are bound to the producer's
  batch thread: the thread that created the producer, or the one that last called `bindBatchThread()`. Off that thread
  they return -1 / `TxnState::Uninitialized` / `false` and touch nothing. `producerId()` is also -1 after `FatalError`.
- Transaction call limits (all complete `InvalidArgument`, never the state error 19): at most 4 `commitTransaction` /
  `abortTransaction` calls join a pending one, at most 8 `flush` calls are pending, and one
  `sendOffsetsToTransaction` runs at a time (a second, even an empty one, completes `InvalidArgument` with "a
  sendOffsetsToTransaction is already in flight on this producer; wait for its completion"; the first completes
  normally and the transaction stays good). An empty offsets list otherwise completes `Ok` locally with no wire
  traffic.
- `UnknownProducerId` (28) inside a transaction (KIP-360): after retention, while this transaction has not yet written to
  that partition (none of its batches acked there), the producer restarts that partition's sequences at 0 under the same
  producer id and epoch and the transaction continues. When this transaction's own acked data on that partition is
  gone, the transaction is abortable (the commit completes `BrokerError(28)`) and the abort bumps the epoch, so the
  redone transaction runs under epoch+1 from sequence 0.
- A newer producer with the same `transactionalId` fences the old one: its next operation fails and its state is
  `FatalError`.
- `ProducerConfig::tier2Slots` (default 64) is the pool of Tier-2 batch slots; with every slot in use `batch()` returns a
  refused builder whose `send()` is `BackPressured`.

### Exactly-once consume-transform-produce

```cpp
GroupConfig g;  g.autoCommit = false;                  // the offsets travel in the transaction
GroupConsumer gc = client.subscribeGroup("economy", {"orders"}, g);
// ... onRecords collects the records of one batch ...
tp.beginTransaction();
try {
    for (auto& r : batch) tp.send(out, Key(r.id), transform(r));
    tp.sendOffsetsToTransactionAndWait(offsets, gc.groupMetadata());   // next offset to read, per partition
    tp.commitTransactionAndWait();
} catch (const std::runtime_error&) {
    if (tp.state() == TxnState::FatalError) return;           // fenced
    tp.abortTransactionAndWait();
    gc.seekToCommittedAndWait();                              // re-read the aborted batch, no restart
}
```

Pass `gc.groupMetadata()` (group id, generation, member id) rather than the group id alone: the coordinator then fences
the offsets by the member and its generation, so a consumer that lost its partitions in a rebalance (a zombie) cannot
commit them. A stale generation (`IllegalGeneration`, 15) or an unknown member (`UnknownMember`, 12) fails the
transaction (`AbortableError`): abort, let the consumer rejoin, re-read. The member belongs to the session that joined
it, so create the transactional producer on the GroupConsumer's own `Client`. While a transaction holds a partition's
pending offset, the group's `OffsetFetch` and `commitSync` of that partition wait (the broker answers
`UnstableOffsetCommit`, which the client retries) until the transaction completes.

`GroupConsumer::seekToCommitted()` / `seekToCommittedAndWait()` rewinds every assigned partition to its committed offset;
`seek(partition, offset)` repositions one partition (`InvalidArgument` for an unassigned one). Records handed to `onRecords`
before the rewind was applied stay delivered, so drop what you queued and accept only the next expected offset per
partition. The runnable example is `client-cpp/examples/eos_probe.cpp`; `broker/docker/eos-e2e.sh` runs it on 3 brokers
through coordinator and leader kills, injected aborts and a fenced zombie.

## Delivery guarantees

| You want | Producer | Consumer |
|---|---|---|
| At most once | `Acks::None`, or no retry of a failed send | commit **before** handling |
| At least once | `Acks::Leader` / `Acks::All`; a retry never writes a record twice (the broker assigns every acked producer an id and sequences each partition, so a resend after a reconnect is deduplicated) | commit **after** handling |
| Exactly once | `transactionalId`: outputs and consumed offsets commit atomically | `ReadCommitted` on the consumers of the outputs; `seekToCommitted()` after an abort |

`Acks::All` is durable across a leader kill (the record is on a majority before the ack). A transaction that aborts leaves
its records in the log, invisible to `ReadCommitted` readers.

## Typed topics and routing

A typed topic has a schema in the registry. Generate C++ types from it (`rune schema generate`, or
`rune::codec::generateHeader`) and bind a producer to the generated fingerprint:

```cpp
auto players = client.openTopicAndWait<game::gen::Player>("player");   // binds to Player::kFingerprint
game::gen::Player p;
p.userId = 42;
p.score  = 1200;
players.send(p);                                                       // routed by p.key()
```

- `openTopicAndWait<T>` asks the broker for the schema version whose fingerprint equals `T::kFingerprint`. If none
  matches it throws "generated type is stale"; a client compiled against an older version keeps working after the topic
  evolves, and every `send` stamps the version it is bound to.
- Routing: when `T` has a key field, `send` hashes `T::key()` (at most 16 bytes, never truncated) to the partition. A schema
  without a key sends to partition 0. The partition count comes from the broker (`typedPartitionCount`).
- **Checked typed writes**: `insert(rec)`, `update(rec)`, `upsert(rec)`, `remove(key)` and `removeExisting(key)` take
  `(…, Completions& cq, Token token)`; `upsert(rec)` (the same as `send(rec)`) and `remove(key)` also have a
  fire-and-forget form. Each has an `…AndWait(…, timeout = kDefaultWriteWait /* 30 s */)` that returns a `WriteResult`:
  `ok()`, `status()` (`Ok`, `BrokerError`, `Timeout`, `BrokerTooOld`, `VerdictUnknown`, `DeliveryFailed`, `Closed` when
  the client closes first), `code()` (55, 56, 57, 39 or 24 for a `BrokerError`; see [Constraint violations](#constraint-violations)), `constraintId()`, `offset()` and `key()`
  (the broker-assigned key of an autoincrement create, not the record's; 0 for every other write). A local refusal (see
  [Write intents](#write-intents)) or an over-long key throws `std::invalid_argument`. An oversized record throws
  `std::invalid_argument` from the `…AndWait` forms and `std::length_error` from the token forms. `remove` and
  `removeExisting` build the record key exactly as `send(rec)` does, so a delete reaches the partition of the write.
- `TypedTopic<T>` is move-only; `handle()` returns its `TopicHandle` and `boundVersion()` the schema version.
- Code generation rejects a field whose name ends in `_` (the generated code's own names end in `_`): rename the field.
- Reading: `T::decode` / `T::Decoder` place each record by its own version. `Record::op()` classifies a delivered record:
  `Delete` for an empty value, otherwise `Insert` (intent 1), `Update` (intent 2) or `Upsert`. On a raw topic the intent
  bits are whatever the producer sent. `lineageAndWait<T>(topic)` returns the
  lineage that also places records written after `T` was generated ([codec.md](codec.md#generate-c-types)).

## Materialized views

A view is created with Rune QL (`create view`, see [ql.md](ql.md#views)) or from code with a plan; both end in the same authored
`ViewPlan` and `Client::createViewAndWait`. `rune::ql::applyViewPlan` turns a parsed `create view` into that plan, and a view is read
with `select` or `getViewRow`. The code path:

```cpp
namespace v = rune::client::view;
v::ViewPlan plan;
plan.viewName    = "user_totals";
plan.key         = "userId";
plan.persistence = 0;                                             // 0 persisted, 1 transient
plan.sources.push_back(v::SourceSpec{/*topicId=*/3, "userId"});
plan.output.push_back(v::AuthoredField{"userId", PrimType::Int64, 0, "userId", v::MergeRule::Replace});
plan.output.push_back(v::AuthoredField{"n",      PrimType::Int64, 0, "",       v::MergeRule::CountAll});
plan.output.push_back(v::AuthoredField{"total",  PrimType::Int64, 0, "amount", v::MergeRule::Sum});
auto blob = v::serializeAuthored(plan);
CreateViewResult cv = client.createViewAndWait("user_totals", "userId", /*persistence=*/0, blob, DdlOptions{}, 15s);
if (cv.errorCode != 0) return;

ViewRowResult row = client.getViewRowAndWait(cv.viewTopicId, /*keyU64=*/42, cv.outputFingerprint, 5s);
if (row.status == ViewRowResult::kOk) {
    const PrimType types[] = {PrimType::Int64, PrimType::Int64, PrimType::Int64};
    v::RowLayout layout = v::buildRowLayout(types);               // same order as plan.output
    auto total = v::readSlot(row.rowBytes, layout, 2);            // total.i, total.present
}
```

- `MergeRule`: `Replace`, `Count`, `Sum`, `Avg`, `Min`, `Max`, `Last`, `CountAll` (`count(*)`), `LastNonNull` (wire value 8, appended).
  - `Min` / `Max`: numeric sources only (Int32, Int64, UInt16, UInt32, UInt64, Float64); nulls are ignored (NaN and ±Inf count as NULL); an empty or all-null group gives NULL. Exact under update, group move and delete.
  - `Last`: the value of the most recently written contributor of the group, including NULL. Any fixed-width source type.
  - `LastNonNull`: the most recently written contributor value that is not NULL, or NULL if there is none.
  - In a view that uses `Min`, `Max`, `Last` or `LastNonNull`, a contributor whose 8-byte key is `0xFFFF_FFFF_FFFF_FFFF` (int64 `-1`) is reserved as the link sentinel: the record is skipped and counted in `skippedMalformed`. Don't use `-1` as an entity id there.
  - All four keep the source field's type as the output type. Create a view that uses one only after every broker runs a build that has them.
- Rows are fixed-width and decoded positionally against a layout built from the plan's output types. Remember
  `viewTopicId` and `outputFingerprint` from `createView`: a drifted fingerprint is answered `SchemaMismatch`.
- `ViewRowResult::status`: `kOk`, `kAbsentKey`, `kNotReady` (nothing folded yet: retry), `kSchemaMismatch`, `kUnknownView`, `kInvalidRequest`, `kBusy` (retry), `kRowTooLarge`,
  `kNotAuthorized`, `kCursorInvalidated` (a scan, see [Wide view keys](#wide-view-keys)), and the client-side `kNotLeader`.
- A row is served only by the source partition's leader, under the controller's read lease, and reflects every write
  acknowledged before the read was sent. A leader that lost its lease (cut off from the controller) answers `NotLeader`;
  the client retries it within `ddlRetryBudget`.
- Rules: an aggregate view needs a **one-partition** source (`ViewNotCoPartitioned` otherwise); a view over a transient
  topic is allowed, persisted or transient (a keyless source answers `ViewKeyInvalid`); a view cannot read another view (`ViewSourceIsView`). `READ` on
  the view is needed to read a row and `READ` on each source to create it.
- Related calls: `alterView(AndWait)`, `dropView(AndWait)`.

### Wide view keys

A view keyed by up to 128 bytes of typed columns (a plan v2) is authored with `plan.keyComponents`, each `{column, keyFunction, outName}`;
`keyFunction` 1 is `lower()` (a `string(N)` column only) and then `plan.unicodeVersion` must be `rune::codec::kUnicodeVersion`.
Send the view's first key component as the `keyField`
(`v::viewKeyField(plan, keyField)`).

Aggregate views are authored two ways. A **v2** aggregate (key components) names the topic's key as
`sources[0].keyField`; its group is its key (there is no `plan.groupBy` member); the broker refuses it with `ViewKeyInvalid` if `sources[0].keyField` is not the
topic's real key. A **v1-shaped** aggregate (a single `key` field, no key components) keeps its group column as before. The key encoding is in [View keys](codec.md#view-keys).

```cpp
plan.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}};   // account_characters
// ... createViewAndWait as above, with v::viewKeyField(plan, "userId")

auto kb = v::ViewKeyBuilder::fromPlan(plan, sourceDescriptor, sourcePartitions);   // or fromCatalog(state, "account_characters")
kb->setUInt("userId", 42).setUInt("characterId", 7);
ViewReadOptions ro{.minOffset = writeOffset, .partition = kb->partition()};
ViewRowResult row = client.getViewRowAndWait(viewTopicId, *kb->key(), ro, 5s);          // point read

kb->clear();
kb->setUInt("userId", 42);
ViewScanOptions so;
for (int restarts = 0;;) {                                                               // every character of user 42
    ViewScanResult page = client.scanViewAndWait(viewTopicId, *kb->prefix(), /*limit=*/0, so, 5s);
    if (page.status == ViewRowResult::kCursorInvalidated && ++restarts <= 3) { so.resumeToken.clear(); continue; }   // 8 is also kNotLeader
    if (page.status != ViewRowResult::kOk) break;
    for (const auto& r : page.rows) { /* decode like a getViewRow row */ }
    if (page.complete()) break;
    so.resumeToken = page.nextToken;
}
```

- `ViewKeyBuilder` builds the key bytes with the same `encodeKeyComponent` the broker folds with. `setInt`, `setUInt`, `setBool`
  and `setString` name a source column; `key()` needs every component and `prefix()` the leading ones (a scan prefix is every
  component but the last). Both return an error that names the component: unset, out of range, longer than `N` bytes, invalid UTF-8.
  A `lower()` component is lowered by `key()`. `partition()` is the source partition of the key (the producer's routing of the
  source key), or `-1` for a one-partition source.
- `getViewRowK` / `getViewRowAndWait(topic, key, ViewReadOptions)` is the point read: `partition` (-1 = the sink's only
  partition), `minOffset` (-1 = none; otherwise the read waits until the view has folded that source offset, so a client
  reads its own write) and `requestedFingerprint`. `scanView` / `scanViewAndWait(topic, prefix, limit, ViewScanOptions)` reads
  one page of at most `limit` rows (0 = 256; a page also stops at 61,440 row bytes).
- `ViewScanResult`: `rows`, `nextToken` (empty on the last page; `complete()` is `status == kOk` and no token) and `status`.
  **Pages are unordered** (table order). A row added or removed under the prefix between pages invalidates the token:
  `status` is `ViewRowResult::kCursorInvalidated` (8, the same value as `kNotLeader`) and the scan restarts from the first page.
  A scan sees each row of a quiet prefix once.
- With the token form (`scanView`), a `Completions` that polls the result needs `maxResultBytes` of at least 64 KiB, else the page is `ResultTooLarge`.
  The completion's `OpKind` is `GetViewRowK` or `ScanViewRows`; decode a scan's payload with `rune::client::view::decodeScanPayload`.
- Both calls end `BrokerTooOld` without sending when the seed's `brokerVersion` is below `kBrokerVersionCheckedWrites` (10).
- The refusals at create time are error codes 59 (`ViewKeyExpressionInvalid`), 60 (`ViewConstraintInvalid`) and 61 (`ViewUniqueInvalid`);
  see [Constraints](ql.md#constraints) for what a `unique` or `check` view is refused for.
  A view whose key is not exactly the source key needs a one-partition source.

### Stats

`getStats` / `getStatsAndWait` read the counters of one topic or view partition from its leader (wire frames 91 and 92, ids in
[wire-protocol.md](wire-protocol.md#stats-client-version-17)).

```cpp
namespace rpc = rune::protocol::client;
StatsResult st = client.getStatsAndWait(rpc::StatsScope::TopicPartition, topicId, /*partition=*/0, 5s);
if (st.status == StatsResult::kOk) {
    if (auto lag = st.get(rpc::StatId::verdictLag)) { /* *lag records not yet judged */ }
    for (const auto& c : st.constraints) { /* c.constraintId, c.rejections */ }
}
```

- `StatsResult::status`: `kOk`, `kUnknownObject`, `kInvalidRequest`, `kBusy`, `kNotAuthorized`, and the client-side `kNotLeader` (the leader was not
  reached within `ddlRetryBudget`; a `Redirect` or `NotLeader` from the broker is followed as for `getViewRow`) and `kBrokerTooOld`
  (nothing was sent; see below).
- `stats` holds `(id, value)` in ascending id order; `get(id)` returns `std::nullopt` for an id the broker did not report (a stat of the other
  scope, or a counter the partition does not have). `constraints` holds the per-constraint rejection counts of a topic.
- The token form `getStats(scope, objectId, partition, target, token)` completes with `OpKind::GetStats`; `value` is the stat count and
  the payload goes to `decodeStatsPayload`.
- When the seed's `brokerVersion` is below `kBrokerVersionCheckedWrites` (10) nothing is sent: `getStatsAndWait` returns
  `StatsResult::kBrokerTooOld` and the token form completes `OpStatus::BrokerTooOld`. The gateway
  answers a session below `kClientVersionCheckedWrites` with `InvalidRequest`. `rune stats` and `show topic` / `show view` are built on it.

## Catalog and security reads

```cpp
auto history = client.catalogHistory(filter, 10s);   // SHOW HISTORY
auto state   = client.catalogAsOf(point, 10s);       // AS OF offset / time / version
auto diff    = client.catalogDiff(a, b, 10s);        // DIFF
```

The reads block, replay `__catalog` client-side with the shared verifying library, and may be called only from the thread
that constructed the `Client`; another thread constructs its own `CatalogReader(client)`. Results are
`CatalogResult<T>` (an `std::expected` whose error says `Timeout`, `Replay` with the failing offset, `Closed`, `Rejected`
with the broker code). They need `DESCRIBE` on `CLUSTER`.

## Errors and failure handling

| Where | How a failure shows |
|---|---|
| Typed `…AndWait`, `Producer::sendAndWait` | A `WriteResult` / `Completion` for every broker and transport outcome, `Timeout` included; `std::invalid_argument` for a local refusal. |
| Other `*AndWait` | `std::runtime_error` whose text is `<op>: <reason>`, for example `timed out`, `client closed`, `session lost before the answer`, `broker error code 45 (NotAuthorized): <detail>`. A *broker error code on a DDL or schema op* is **returned in the result** (`errorCode`, `detail`) instead of thrown. |
| Token form | `OpResult` at issue (`Accepted`, `Closed`, `QueueFull`, `BackPressured`, `InvalidArgument`, `WrongThread`), then a `Completion` with `OpStatus` (`Ok`, `BrokerError`, `Timeout`, `Closed`, `NotConnected`, `Superseded`, `Rejoined`, `SessionLost`, `Fenced`, `DeliveryFailed`, `ResultTooLarge`, `InvalidArgument`) and `errorCode` when `BrokerError`. |
| `Producer::trySend` | `SendResult`, never an exception. `send` throws `std::length_error` (too large), `std::invalid_argument` (bad topic), `std::logic_error` (client closed) or `std::runtime_error` (back-pressured where pumping is unsafe, or the runner stopped draining). |
| Wrong thread | `std::logic_error`: a blocking call on the runner thread or from a callback, a catalog read off the constructing thread. |
| Subscription / group | `finished()` with `failReason()`; a group consumer with `GroupFailReason`. |

Which codes are retried for you is in [wire-protocol.md](wire-protocol.md#error-codes). A DDL answered `Internal` (6) after
its record was written is **in doubt**: it may still apply, and does at the latest after a restart. Retry it with the same
`DdlOptions::requestId` (read it from `DefineTopicResult::requestId`) and the leader deduplicates.

## Building against the client

The root build embeds the client for its tests; to use it, build `client-cpp/` on its own
([wiki, Building](https://github.com/arcanumruna/rune/wiki/Building#each-library-on-its-own)) and link `rune_client_api`. The headers include rune-protocol, rune-codec, rune-ql and
rune-crypto through sibling include paths, and the examples in `client-cpp/examples/` are complete programs:
`rune_client_ping`, `rune_produce_subscribe`, `rune_produce_throughput`, `rune_consume_throughput`, `view_probe`,
`dr_client`. They read credentials from `RUNE_USER` and `RUNE_PASSWORD_FILE` (`examples/ExampleAuth.h`).
