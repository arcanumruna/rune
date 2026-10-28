# C++ client API

Namespace `rune::client`, headers under `client-cpp/include/rune/client/`; start with
[`Client.h`](../../client-cpp/include/rune/client/Client.h). Every example below was compiled against these headers.
Settings are listed in [configuration.md](../configuration.md#client-configuration); error codes in
[wire-protocol.md](wire-protocol.md#error-codes).

Contents: [model](#threading-model) · [connect](#connect) · [authentication](#authentication-and-security) ·
[topics](#topics-and-ddl) · [produce](#produce) · [subscribe](#subscribe) · [filters](#subscription-filters) · [groups](#consumer-groups) ·
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
auto untyped = client.defineTopicAndWait("events", 3, /*keyField=*/"", {}, /*validate=*/false,
                                          Persistence::Disk, opts, ddl, 15s);
if (untyped.errorCode != 0) std::cerr << "defineTopic: " << untyped.errorCode << " " << untyped.detail << "\n";

auto d = rune::codec::DescriptorBuilder("player", 1)
             .add("userId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
             .addFixedStr("name", 32)
             .add("score", rune::codec::PrimType::Int64)
             .build();
auto typed = client.defineTopicAndWait("player", 4, "userId", rune::codec::serialize(d), /*validate=*/true,
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

`DefineTopicResult::topicId` plus the partition count you passed make the `TopicHandle{topicId, partitionCount}` that a
producer needs. `Client::topic(name)` finds a handle only in the static `ClientConfig::topics` list.

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
- **`Key`**: up to 16 inline bytes, built from a `uint64_t`/`int64_t`, a trivially-copyable value or a byte span. The key
  picks the partition (`Key::partition(n)`, FNV-1a 64) and is stored with the record.
- **Record size**: key plus value at most `Producer::kMaxRecordBytes` = 16 KiB. A larger one is rejected at append:
  `trySend` returns `SendResult::MessageTooLarge`; `send` throws `std::length_error`.
- **Batching**: sends within `lingerUs` (default 5 ms) are batched per partition up to `batchBytes` (64 KiB). `lingerUs`
  of 0 defeats batching.
- **`SendResult`**: `Ok`, `BackPressured`, `ValueTooLarge`, `InvalidTopic`, `Closed`, `QueueFull`, `InvalidArgument`,
  `MessageTooLarge`, `WrongThread`, `WrongShard`. Anything but `Ok` means the record was not accepted.
- Retriable broker answers (`MinIsrViolation`, `TransientLogFull`) are re-sent with the same identity after a backoff,
  bounded by `produceTimeoutMs` and `deliveryTimeoutMs`.

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
- `onBatchAck` / `batchAcksTo(cq)` deliver one `BatchAck` per batch (`baseOffset`, `baseSequence`, `recordCount`,
  `errorCode`, `producerEpoch`).
- Every accepted record is reported exactly once, including ones that never reached the wire (`BatchAck::kClientFailure`).

### Batches and flush

```cpp
auto batch = p.batch(events, Key(uint64_t{42}));   // one atomic append to one partition: contiguous offsets, all or nothing
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
    tp.commitTransactionAndWait();             // outputs and offsets, atomically
} catch (const std::runtime_error&) {
    if (tp.state() == TxnState::FatalError) return;   // fenced: a newer producer with this id took over
    tp.abortTransactionAndWait();              // roll back
}
```

- `TxnState`: `Uninitialized`, `Initializing`, `Ready`, `InTransaction`, `CommittingTransaction`,
  `AbortingTransaction`, `AbortableError`, `FatalError`. After a timeout in `CommittingTransaction` the outcome is in doubt:
  retry the same `commitTransactionAndWait()`.
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
  `sendOffsetsToTransaction` runs at a time (a second completes `InvalidArgument` with "a sendOffsetsToTransaction is
  already in flight on this producer; wait for its completion"; the first completes normally and the transaction stays
  good). An empty offsets list completes `Ok` locally with no wire traffic.
- `UnknownProducerId` (28) inside a transaction aborts it (the commit completes `BrokerError(28)`); the app aborts and
  redoes the transaction, which runs under epoch+1 from sequence 0. KIP-360 (the coordinator keeping the transaction
  across the epoch bump) is not implemented.
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
- `TypedTopic<T>` is move-only; `handle()` returns its `TopicHandle` and `boundVersion()` the schema version.
- Reading: `T::decode` / `T::Decoder` place each record by its own version. `lineageAndWait<T>(topic)` returns the
  lineage that also places records written after `T` was generated ([codec.md](codec.md#generate-c-types)).

## Materialized views

Views are created from code with a plan and read by key. (Rune QL has no `create view` yet; `drop view` exists.)

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
  `kNotAuthorized`, and the client-side `kNotLeader`.
- A row is served only by the source partition's leader, under the controller's read lease, and reflects every write
  acknowledged before the read was sent. A leader that lost its lease (cut off from the controller) answers `NotLeader`;
  the client retries it within `ddlRetryBudget`.
- Rules: an aggregate view needs a **one-partition** source (`ViewNotCoPartitioned` otherwise); a view over a transient
  topic must itself be transient (`ViewSourceTransient`); a view cannot read another view (`ViewSourceIsView`). `READ` on
  the view is needed to read a row and `READ` on each source to create it.
- Related calls: `alterView(AndWait)`, `dropView(AndWait)`.

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
| `*AndWait` | `std::runtime_error` whose text is `<op>: <reason>`, for example `timed out`, `client closed`, `session lost before the answer`, `broker error code 45 (NotAuthorized): <detail>`. A *broker error code on a DDL or schema op* is **returned in the result** (`errorCode`, `detail`) instead of thrown. |
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
