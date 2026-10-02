# F8c: Remove std::future / std::promise from the client API (design note)

- **Status:** design note, not yet implemented. No code has changed.
- **Date:** 2026-10-01
- **Plan:** F8 slice C of `docs/superpowers/plans/2026-09-30-sharded-invoker-client.md`.
- **Inputs:** `.superpowers/sdd/2026-09-30-sharded-invoker-client/f8a-report.md` (its "Slice C candidates"), and the progress-ledger ruling of 2026-10-01.

**Binding user decision.** The API becomes "callbacks + polling":
- `std::future` is removed entirely, because it hides a mutex and a condvar.
- Results reach the app in one of two ways:
  - a callback on the owner/duty thread; or
  - an Aeron-style completion token that the app polls.
- A blocking wait becomes a pump-until-done loop in Invoker mode.
- Breaking API changes are accepted.

**Project law.**
- No atomics or mutexes anywhere except inside ring-buffer implementations.
- Every piece of state has exactly one owner thread. Cross-thread traffic goes only through rings.
- No allocation, virtual calls or `std::function` on hot paths.
- Avoid cross-thread `shared_ptr` refcounting on hot paths.

**Tree references.** `[H]` is the main repo at `feat/sharded-invoker-client` HEAD `991cffc`. `[A]` is the F8a worktree `rune-client-cpp-wt2` at `feat/f8a-no-mutex` `751df30`, which is not yet merged into `[H]`. Line numbers are from those trees.

---

## 0. Summary of the proposal

1. **`Completions` object.** Created by the app, owned by one app thread, and registered with the Client. It wraps a single-producer/single-consumer completion ring (`OneToOneRingBuffer`):
   - the duty thread is the only producer;
   - the owning app thread is the only consumer and drains it with `poll()`.
2. **Every asynchronous operation** takes a `Token` (an app-chosen 48-bit id) and a target. The target is one of:
   - a `Completions&`, polled by the app; or
   - a `CompletionHandler {fn, ctx}`, a plain function pointer called on the duty thread.

   There is no `std::function` and no allocation on this path.
3. **Exactly-once rule.** An operation is either rejected synchronously by its return code, in which case no completion ever arrives, or accepted, in which case exactly one completion arrives. Client close, handle close, timeouts, rejoins and redirects all keep this rule; at close every outstanding token completes with `Closed`.
4. **Blocking forms.** `Completions::waitFor(token, timeout, ...)` drives an idle-strategy loop:
   - in Invoker mode it pumps `Client::doWork()` itself;
   - in Thread mode it polls under Agrona's `BackoffIdleStrategy` (spin, then yield, then park).

   It never uses a condvar.
5. **Handle lifetime without shared_ptr.** Handles become move-only. The impl has a single owner at any moment: the duty registry, or the handle after the Client closes. Ownership changes hands only through ring order, using a linearizable close of the agent command ring (§3). This also removes F8a's `ClosedAfterSubmit` leak.
6. **Faster acked sends.** The per-record producer ack no longer allocates `PendingAck` (`new` plus `std::function` or `std::promise`). The 64-bit ack slot that already travels through the producer ring carries a tagged token instead, so the acked path becomes allocation-free, not only the void path.

---

## 1. Inventory

### 1.1 Public APIs that return a future or create a promise

| # | API | Declaration | Definition / promise site | Resolved where |
|---|---|---|---|---|
| P1 | `std::future<void> Client::connect()` | [H] `Client.h:328` | [H] `Client.cpp:371-414` (promise :376, held in `PendingConnect` `Client.h:532-535`) | `ConnectResponse` `Client.cpp:891`; ErrorResponse `:1262-1290`; `sweepTimeouts` `:750`; `failAllPending` `:1564` |
| P2 | `std::future<Bytes> Client::ping(Bytes)` | [H] `Client.h:340` | [H] `Client.cpp:419-493` (`PendingPing` `Client.h:537-540`) | Pong `Client.cpp:1305-1326`; ErrorResponse `:1263`; sweep; failAll |
| P3 | `std::future<DefineTopicResult> Client::defineTopic(...)` | [H] `Client.h:353` | [H] `Client.cpp:499-571` (`PendingDefine` `Client.h:543-547`) | `DefineTopicResponse` `Client.cpp:1477-1520` (promise :1482); sweep; failAll |
| P4 | `std::future<TopicSchemaResult> Client::getTopicSchema(name)` | [H] `Client.h:369` | [H] `Client.cpp:577-...` (`PendingGet` `Client.h:549-552`) | `TopicSchemaResponse` `Client.cpp:1521-1560` (promise :1526); sweep; failAll |
| P5 | `std::future<TypedTopic<T>> Client::openTopic<T>(name)` | [H] `Client.h:396-436` | `std::async(std::launch::async, ...)` `Client.h:405`. Each call spawns a thread, which takes `mtx_` (`:427`) and calls `producer()` | Same as P4, followed by a fingerprint check on the async thread |
| P6 | `std::future<int64_t> Producer::send(topic, key, value, want_future_t)` | [H] `Producer.h:292-295` (tag `:113-114`) | [H] `Producer.cpp:137-157`. The invalid-topic case makes a promise at `:143`; `ProducerImpl::registerFuture` is at `ProducerImpl.cpp:1046-1051` | `PendingAck::complete` `ProducerImpl.cpp:1011-1037`, called from `:751, :799, :817, :1637, :1841, :1851, :1960, :2099, :2115, :2120` and `failUnsubmitted` `:1053-1062` |
| P7 | `void Producer::send(topic, key, value, std::function<void(int64_t)>)` | [H] `Producer.h:283-286` | [H] `Producer.cpp:112-135`; `registerCallback` `ProducerImpl.cpp:1039-1044` | Same `PendingAck::complete` sites. No future, but **one `new PendingAck` and one `std::function` per send**: the same mechanism, and it must change with P6 |
| P8 | `std::future<int64_t> BatchBuilder::send()` (Tier-2) | [H] `BatchBuilder.h:115` | [H] `BatchBuilder.cpp:92-131` (`registerFuture` :104) | Tier-2 ack via `Tier2Entry.ackHandle` (`ProducerImpl.h:125`) → `PendingAck::complete` |
| P9 | `std::future<void> TransactionalProducer::initTransactions()` | [H] `TransactionalProducer.h:109` | [H] `TransactionalProducer.cpp:17`; `TransactionalProducerImpl.cpp:84-136` (promise :86) | `TransactionalProducerImpl.cpp:540, 638, 649` + `failAll` `:839-867`, `:1026-1034` |
| P10 | `std::future<void> sendOffsetsToTransaction(offsets, groupId)` | [H] `TransactionalProducer.h:127` | [H] `TransactionalProducer.cpp:41`; `Impl.cpp:138-158` (promise :142; `PendingOffsetOp` `Impl.h:303-309`) | `Impl.cpp:692, 702, 741, 867, 1034` |
| P11 | `std::future<void> commitTransaction()` | [H] `TransactionalProducer.h:134` | [H] `TransactionalProducer.cpp:48`; `Impl.cpp:160-176` (promise :162) | `Impl.cpp:776-813` |
| P12 | `std::future<void> abortTransaction()` | [H] `TransactionalProducer.h:138` | [H] `TransactionalProducer.cpp:53`; `Impl.cpp:178-194` (promise :180) | `Impl.cpp:776-813` |
| P13 | `std::future<void> GroupConsumer::commitSync()` | [H] `GroupConsumer.h:222`; [A] `GroupConsumer.h:262` | [H] `GroupConsumer.cpp:80-...` (promise :83, impl `GroupConsumer.h:159`); [A] `GroupConsumer.cpp:130-161` (heap promise :140, travels as `HubCommand.a` `HubCommand.h:41`) | [A] `GroupConsumerHub.cpp:170-188` (adopt), OffsetCommitResponse matched by correlation id, `failCommitSync_` on supersede/rejoin/session loss/close |

**Not futures today, but in scope:**

| # | API | Site | Note |
|---|---|---|---|
| N1 | `void Producer::flush() noexcept` | [H] `Producer.h:321`, `Producer.cpp:188` | Only sets a flag (`globalFlushPending_`, an atomic, `ProducerImpl.h:776`; F8b removes it). It has no completion. The brief listed flush; §2.4 proposes an **optional** tokenized flush. |
| N2 | `Subscription Client::subscribe(...)` | [H] `Client.h:457-466` | Returns a handle with no future. Bind state comes through F8a's `StatusRing`. An optional "bound" token is proposed in §2.6. |
| N3 | Metadata | [H] `Client.cpp` `sendMetadataRequest` / `onMetadataResponse` (`Client.h:843, 860`) | Internal and duty-thread only. **No public future exists.** Nothing changes for F8c. |
| N4 | `Producer::onBatchAck(std::function<void(const BatchAck&)>)` and Invoker `collectBatchAcks` / `drainBatchAcks` | [H] `Producer.h:300`, `ProducerImpl.h:561-574` | Already callback + poll. It gets folded into the same `Completions` and `CompletionHandler` types (§2.4). |

### 1.2 Internal promise/future uses

| # | Site | What it is |
|---|---|---|
| I1 | [H] `Client.cpp:170-178` | Runner start handshake: a `std::promise<void> go` that the runner waits on, so the write to `dutyThreadId_` happens-before the runner's reads. |
| I2 | [H] `Client.h:532-552`, `:726-730` | The `PendingConnect/Ping/Define/Get` promise structs and the `mtx_`-guarded maps. The mutex part belongs to F8b; the promise part to F8c. |
| I3 | [H] `ProducerImpl.h:210-224` | `PendingAck { Kind; std::function<void(int64_t)> callback; std::promise<int64_t> promise; }`, created by `new` per opt-in send. |
| I4 | [H] `TransactionalProducerImpl.h:74-79` (`PendingTxnOp`), `:274` (`Command::promise`), `:307` (`PendingOffsetOp::promise`) | Txn pending operations. Their command queue is behind `cmdMtx_` / `pendingSendsMtx_`; the mutexes are F8b, the promises are F8c. |
| I5 | [A] `GroupConsumer.h:169` `std::unique_ptr<std::promise<void>> commitSyncPromise` | Owned by the duty thread after F8a. |
| I6 | [H] `src/cli/Cli.cpp:185-425` (8 sites) | The tool waits with `wait_for(10s)` + `get()` on the `connect`, `defineTopic` and `getTopicSchema` futures. |
| I7 | Tests, examples, benches: 27 files, about 390 `future`/`wait_for`/`get()` sites | Mainly `tests/client/*`, `tests/producer/*`, `tests/group/*`, `bench/ClientPingBench.cpp`, `examples/{dr_client,rune_client_ping,rune_produce_subscribe,rune_consume_throughput,rune_produce_throughput}.cpp`. `dr_client` feeds the DR Docker e2e image. |
| I8 | Plan Tasks 15-18 (not yet built) | `ClientShard::connect()` returns `std::future<void>` (plan lines 5698, 6067, 6208-6211). The tests at 5831-5946, 6359-6406 and 7392-7398 use `wait_for`. **The plan text must change before Task 15 is dispatched.** |

### 1.3 Cross-thread `shared_ptr` handle refcounts

| # | Owner A (app) | Owner B (duty) | Holder in flight | Copy points |
|---|---|---|---|---|
| S1 `ProducerImpl` | `Producer::impl_` [H] `Producer.h:344` (copyable, `:168-172`); `BatchBuilder::impl_` [H] `BatchBuilder.h:118`; `TypedTopic` (copies the Producer, `TypedTopic.h:82`) | `ProducerHub` `ImplList` [H] `ProducerHub.h:47` | `new shared_ptr<ProducerImpl>` holder [H] `Client.cpp:1687`, adopted at `:129` | Every `Producer` copy, every `batch()`, `TypedTopic` copies |
| S2 `TransactionalProducerImpl` | `TransactionalProducer::impl_` [H] `TransactionalProducer.h:157` (copyable; `impl()` accessor `:152`) | `Client::txnProducers_` [H] `Client.h:773` | holder [H] `Client.cpp:1931`, adopted at `:136` | Handle copies |
| S3 `SubscriptionImpl` | `Subscription::impl_` [A] `Subscription.h:244` (move-only) | `SubscriberHub::activeById_` [A] `SubscriberHub.h:213`; `SubscribeRequest::impl` [A] `:42` | the `SubscribeRequest*` in the `HubCommand` | Adopt, bind and finish moves; [H] `SubscriberHub.cpp:186` copies under the lock (F8a removed this) |
| S4 `GroupConsumerImpl` | `GroupConsumer::impl_` [A] `GroupConsumer.h:291` (move-only) | `GroupConsumerHub::impls_` [A] `GroupConsumerHub.h:42` | `new shared_ptr<GroupConsumerImpl>` [A] `Client.cpp:1954`, adopted at `GroupConsumerHub.cpp:159` | Adopt |
| S5 Aeron objects | n/a | `AeronTransport` [H] `AeronTransport.h:284-286`; `Resolution` [H] `Resolution.h:38, 58, 81` | n/a | Created in `agent_.start()` on the constructing thread before the runner starts; released after the join. **Not concurrent, so out of scope.** Aeron's own conductor (Thread mode) holds references inside the library; we cannot change that. |

None of S1-S4 changes a refcount on the per-record hot path today: `send()` dereferences `impl_` and never copies it. The cost is twofold:
1. **Atomic refcount operations.** They run on copy, adopt and destroy, which conflicts with the law.
2. **"Last owner frees" is decided by an atomic RMW race.** The destructor can therefore run on either thread, which is the real shared-nothing violation.

---

## 2. Proposed API

### 2.1 Core types (`include/rune/client/Completion.h`, new)

```cpp
namespace rune::client {

using Token = uint64_t;                       // app-chosen; low 48 bits only (see 2.4)
inline constexpr Token kMaxToken = (Token{1} << 48) - 1;

enum class OpKind : uint8_t {
    Send = 1, Tier2Batch, Flush,              // producer
    Connect, Ping, DefineTopic, GetTopicSchema, // client control
    CommitSync,                               // group consumer
    InitTransactions, SendOffsets, CommitTransaction, AbortTransaction, // txn
    SubscribeBound,                           // optional (2.6)
};

enum class OpStatus : uint8_t {
    Ok = 0,
    BrokerError,     // errorCode = broker ErrorCode; payload = message text (truncated)
    Timeout,         // the op's own duty-side deadline expired
    Closed,          // Client closed or the owning handle closed before the result
    NotConnected,    // session not Connected when the duty thread processed it
    Superseded,      // commitSync replaced by a newer commitSync
    Rejoined,        // group generation changed under a commitSync
    SessionLost,     // session dropped after the request was sent (no answer possible)
    Fenced,          // txn: ProducerFenced / InvalidProducerEpoch
    DeliveryFailed,  // producer: delivery timeout / redirect budget / deferred-buffer full
    ResultTooLarge,  // polled form only: variable result > the reservation (value = real size)
};

// One completion record, 40 bytes, trivially copyable; any variable payload
// (ping echo, schema descriptor blob, error text) follows it inline.
struct Completion {
    Token    token;          // as passed at issue
    int64_t  value;          // Send/Tier2: baseOffset (-1 if none); Connect: sessionId;
                             // DefineTopic/GetTopicSchema: topicId; else 0
    int64_t  aux;            // GetTopicSchema/DefineTopic: version; Tier2: recordCount; else 0
    OpKind   op;
    OpStatus status;
    uint16_t errorCode;      // broker ErrorCode when status == BrokerError
    uint32_t payloadLen;     // bytes of inline payload
    uint32_t reservedBytes;  // credit returned to the Completions on consume (2.2)
    uint32_t reserved0 = 0;
};
static_assert(sizeof(Completion) == 40);

// Callback target: a plain function pointer + context. No std::function, no
// virtual call, no allocation. Called on the duty thread (see the thread table).
struct CompletionHandler {
    void (*fn)(void* ctx, const Completion&, std::span<const uint8_t> payload) noexcept;
    void* ctx;
    // Helper that binds a member function without allocating:
    template <auto Member, class C> static CompletionHandler of(C& obj) noexcept;
};

// Synchronous result of ISSUING an op (no completion will ever follow anything but Accepted).
enum class OpResult : uint8_t {
    Accepted,        // exactly one completion will follow
    Closed,          // Client (or this handle) closed; ring closed linearizably (3.2)
    QueueFull,       // the target Completions has no credit; poll() then retry
    BackPressured,   // command / producer ring full (non-blocking try* forms only)
    InvalidArgument, // invalid topic handle, token > kMaxToken, value too large, ...
    WrongThread,     // Completions used off its owner thread, or an Invoker op off the owner
};

} // namespace rune::client
```

### 2.2 `Completions`: the polled completion queue

```cpp
struct CompletionsConfig {
    uint32_t capacityBytes  = 1u << 20;   // ring capacity (pow2)
    uint32_t maxResultBytes = 16u << 10;  // reservation for a variable result (schema blob)
    uint32_t maxErrorText   = 192;        // reservation for BrokerError text (truncated)
};

class Completions {                       // move-only; owned by ONE app thread
public:
    Completions(Client& client, CompletionsConfig cfg = {});
    ~Completions();                       // release protocol, 3.3

    // Drain up to `limit` completions; h(const Completion&, std::span<const uint8_t>).
    // Payload spans are valid only during h. Returns the number delivered.
    template <class H> int poll(H&& h, int limit = INT_MAX);

    // Blocking convenience (2.3). Other completions seen while waiting are
    // stashed (copied into a preallocated stash) and returned by the next poll()
    // FIRST, so completion order is preserved.
    enum class Wait : uint8_t { Done, TimedOut };
    template <class H, class Idle = aeron::concurrent::BackoffIdleStrategy>
    Wait waitFor(Token t, std::chrono::nanoseconds timeout, H&& onDone, Idle&& idle = {});

    [[nodiscard]] uint32_t outstanding() const noexcept;  // accepted, not yet polled
    [[nodiscard]] bool     finished() const noexcept;     // Client closed AND all drained
};
```

**Layout.**
- `Completions` owns one heap block: a `detail::OneToOneRingBuffer` (the F8a port, [A] `detail/OneToOneRingBuffer.h`), its trailer, and a stash of `capacityBytes`.
- Records are typed by `msgTypeId`:
  - `kCompletion` (40 bytes plus the payload);
  - `kBatchAck` (a 32-byte `BatchAck`, §2.4).
- The duty thread keeps a registry `cqs_[index]` of raw pointers (§3).
- An op carries a `CqRef`, a 15-bit value made of a 10-bit index and a 5-bit generation, so a stale reference to a released and reused slot is dropped rather than misdelivered.

**Polling.**
- `poll()` is `ring.read(...)` with a templated handler: no `std::function`, no allocation.
- Before it returns each record to the app, it subtracts `reservedBytes` from the app-owned `reservedTotal_`.

**Credit (why the duty thread never meets a full ring).**
1. Issuing reserves first. Every op that targets a `Completions` reserves its worst-case record size *on the issuing thread*:
   - `recordBytes(40)` for a send ack;
   - `recordBytes(40 + echoLen)` for a ping;
   - `recordBytes(40 + maxResultBytes)` for a schema lookup;
   - `recordBytes(40 + maxErrorText)` for other control ops.
2. The issuing thread is the poll thread. That is the single-owner contract, enforced by a debug thread-id check. So `reservedTotal_` is a plain app-owned counter with no cross-thread traffic.
3. The reservation is bounded. Issuing fails with `QueueFull` when `reservedTotal_ + R > capacity − maxRecordBytes`. The `maxRecordBytes` slack covers the at-most-one wrap-padding record the ring can hold.
4. The duty side writes at most R. It writes each completion within the R recorded in that completion, and truncates error text to fit.

So the duty side's `write` can never fail. A failure would be a bug: it is counted in `lostCompletions_`, and the tests assert that the count is zero.

**Ownership and lifetime.** The app thread owns the `Completions` object. The duty registry holds a borrowed pointer until it applies `CqRelease` or until the Client closes (§3 protocol). A `Completions` may outlive its Client: after close it contains only `Closed` completions and is then `finished()`.

**Invoker mode.** The owner thread is both producer and consumer of the ring. The ring costs about 20-30 ns per completion and keeps one code path. A later optimisation, not in F8c, could write straight into the stash when the issuing thread is the duty thread.

### 2.3 Blocking convenience: pump-until-done, no condvar

**What Aeron and Agrona do.**
- **Aeron C client.** `aeron_async_add_publication()` returns an `aeron_async_add_publication_t*` token. The app polls `aeron_async_add_publication_poll(&pub, async)`, which returns 1 when done, 0 while pending and -1 on error, in its own loop.
- **Aeron C++ wrapper.** `addPublication()` returns a registration id; the app loops on `findPublication(id)` with `std::this_thread::yield()` or an `IdleStrategy`.
- **AeronArchive.** `pollForResponse` and `AeronCluster`'s `AsyncConnect` are `poll()` step functions that the caller drives under `idleStrategy.idle()` with a deadline check.
- **The conductor itself.** A command queue going in and polled state coming out. Waiting is always the caller running an `IdleStrategy` (`BusySpin`, `Yielding`, `Sleeping`, `Backoff`: spin, then yield, then `parkNanos` from min to max, or `NoOp`) around a non-blocking poll. There is no condvar on any client data or control path.
- **One caveat, stated for accuracy.** The Java `ClientConductor` serialises *API calls* with a `ReentrantLock`. Its *response waiting* is still polling (`awaitResponse` loops `doWork` and `sleep`). The C client has no such lock; it uses an MPSC command queue. Rune follows the C client.

**Recommendation: `Completions::waitFor`.**

| Caller | Loop | Notes |
|---|---|---|
| Invoker owner thread, outside a duty cycle | `for (;;) { w = client.doWork(); w += pollFor(t); if (done) break; if (deadline) return TimedOut; idle.idle(w); }` | This **is** pump-until-done. The thread is the duty thread, so nothing else can make progress. With `idle` work>0 there is no park; when idle it backs off. |
| Invoker, inside a duty cycle (callback) | throws `std::logic_error` | It could never complete, the same as F8a's `commitSync()` from a callback. |
| Invoker, non-owner thread | throws `std::logic_error` (`WrongThread`) | A non-owner must never run the duty cycle. |
| Thread mode, app thread | `for (;;) { n = pollFor(t); if (done) break; if (deadline) return TimedOut; idle.idle(n); }` | The runner makes progress independently. The default is `BackoffIdleStrategy(spins 10, yields 5, park 1 µs → 1 ms)`, as `ProducerImpl.cpp:834` already uses. |
| Thread mode, runner thread (callback) | throws `std::logic_error` | It would wait for itself. |
| Any mode, after the Client closed | Polls only, never dereferences `Client*` | The ring's `producerClosed()` word is checked first (§3). |

**Wake-up latency in Thread mode.** No condvar means the waiter discovers a completion at its next poll. With the default backoff the worst case is the max park (1 ms). That is fine for control operations. A latency-sensitive waiter passes `YieldingIdleStrategy` or `BusySpinIdleStrategy` as the `Idle` template argument, which costs no allocation and no virtual call.

**A wait timeout is not the op's timeout.** A `waitFor` that returns `TimedOut` only stops waiting. The op is still accepted and **will** complete later, with its result or with its own `Timeout` or `Closed`. That completion must still be polled before its credit is released.

**Sugar.** These are thin wrappers that each build a short-lived `Completions` (cold path, one allocation):
- `void Client::connectAndWait(nanoseconds)`
- `Bytes Client::pingAndWait(Bytes, nanoseconds)`
- `DefineTopicResult Client::defineTopicAndWait(...)`
- `TopicSchemaResult Client::getTopicSchemaAndWait(...)`
- `TypedTopic<T> Client::openTopicAndWait<T>(name, nanoseconds)`
- `void GroupConsumer::commitSyncAndWait(nanoseconds)`
- `void TransactionalProducer::{initTransactions,commitTransaction,abortTransaction}AndWait(nanoseconds)`

They throw `std::runtime_error` on any non-`Ok` status, or on wait timeout, which reproduces today's `.get()` behaviour for tools, the CLI and tests. Per-record sends get **no** sugar, because that would invite a wait per record on the hot path.

### 2.4 Producer

Every overload below is called on the app thread. In Invoker mode that must be the owner thread; any other thread gets `WrongThread` (unchanged from Task 14).

| Overload | Replaces | Completion delivered |
|---|---|---|
| `SendResult trySend(topic, key, value) noexcept` | unchanged | none |
| `void send(topic, key, value)` | unchanged | none |
| `SendResult trySend(topic, key, value, Completions& cq, Token t) noexcept` | `send(..., want_future)` P6 | `{op=Send, value=baseOffset}` into `cq` |
| `SendResult trySend(topic, key, value, AckToken t) noexcept` | `send(..., std::function)` P7 | to the producer's `onAck` handler (below) |
| `void send(..., Completions&, Token)` / `void send(..., AckToken)` | n/a | Blocking-on-back-pressure variants, with the void `send()` semantics: pumps in Invoker mode and backs off in Thread mode |
| `void onAck(void (*fn)(void* ctx, Token, int64_t baseOffset, OpStatus, uint16_t ec) noexcept, void* ctx)` | P7's per-send `std::function` | Setup phase. Called on the **duty thread** (the Thread-mode runner, or the Invoker owner inside `doWork()`). |
| `void onBatchAck(CompletionHandler-like fn, ctx)` / `void batchAcksTo(Completions&)` | `onBatchAck(std::function)`, `collectBatchAcks`/`drainBatchAcks` | `kBatchAck` records. One per batch: the recommended high-throughput form. |
| `OpResult BatchBuilder::send(Completions&, Token)` / `send(AckToken)` | P8 | `{op=Tier2Batch, value=baseOffset, aux=recordCount}` |
| `OpResult flush(Completions&, Token)` *(optional, see Q5)* | N1 | Completes once every record accepted before the flush has completed. Ring mode sends a marker through the producer ring; Direct mode snapshots each partition's `ackHead`. |

**Hot-path encoding (no allocation).** The 64-bit `ackHandle` that already travels in `SEND_RECORD` and the Tier-2 token, and sits in `PartitionState::ackRing` and `InFlightRing` (`ProducerImpl.h:125, 181`), changes meaning:

```
bit 63      = 1 (token ack; 0 means "no ack" exactly as today)
bit 62      = target: 0 = Completions, 1 = producer onAck handler
bits 47..61 = CqRef (15 bits) when target = Completions; 0 otherwise
bits 0..47  = Token (48 bits)
```

`PendingAck` (`ProducerImpl.h:210-224`), `registerCallback`, `registerFuture` and `failUnsubmitted`'s `delete` all disappear.

- **Callback target.** `PendingAck::complete` (`ProducerImpl.cpp:1011`) becomes a duty-thread `deliverAck(handle, offset, status)`. It decodes the tag and either calls `onAck_` (a plain function pointer under try/catch, keeping the existing exception counting) or writes 40 bytes into `cqs_[ref]`.
- **Polled target.** The credit for a `Completions` is reserved *before* the ring write in `trySend`. If the write fails, the reservation is returned on the same thread, and there is no completion (the exactly-once rule).
- **Rejected sends.** `failUnsubmitted` no longer exists. A rejected `trySend` simply returns its `SendResult`; the app learns synchronously.

### 2.5 Client control operations

| New signature | Replaces | Completion (`value`, `aux`, payload) |
|---|---|---|
| `OpResult connect(Completions&, Token)` / `connect(CompletionHandler, Token)` | P1 | `value=sessionId`. The `onConnected` callback still fires as well. |
| `OpResult ping(std::span<const uint8_t>, Completions&, Token)` / `(…, CompletionHandler, Token)` | P2 | payload = echo bytes |
| `OpResult defineTopic(name, partitions, keyField, validate, blob, target, Token)` | P3 | `value=topicId`, `aux=version`, `errorCode` |
| `OpResult getTopicSchema(name, target, Token)` | P4 | `value=topicId`, `aux=version`; payload = descriptor blob (polled form: up to `maxResultBytes`, else `ResultTooLarge`; the callback form has no cap) |
| `template<class T> static Result<TypedTopic<T>> TypedTopic<T>::fromSchema(Client&, const Completion&, std::span<const uint8_t>)` + `openTopicAndWait<T>` | P5 (`std::async` removed) | Fingerprint check on the caller's thread. The schema-cache write moves to the duty thread when it handles `TopicSchemaResponse`, which removes the `mtx_` use at `Client.h:427`. |

**Issue path.**
1. The app encodes the request frame as it does today.
2. It submits **one** agent-ring command, `kMsgControlOp {OpKind, target(kind, CqRef or fn+ctx), Token, deadlineNs, frame}`.
3. The duty thread inserts the pending entry into its own table (a plain `FlatIndexMap` by correlation id, so `PendingX` and `mtx_` disappear) and sends the frame.
4. A "not connected" check now happens on the duty thread and completes `NotConnected`. Today it is an app-thread read of an atomic snapshot (`Client.cpp:427-436`).

**Correlation ids.** How the frame gets its id belongs to F8b: today it is `nextCorrId_.fetch_add`, an atomic. This note assumes the F8b ruling, either duty-assigned ids patched at the frame's fixed SBE offset or per-thread id ranges (see Q8).

### 2.6 Group consumer, transactional producer, subscription

| New signature | Replaces | Completion / thread |
|---|---|---|
| `OpResult GroupConsumer::commitSync(Completions&, Token)` / `(CompletionHandler, Token)` | P13 | `Ok`, `BrokerError(ec)`, `Superseded`, `Rejoined`, `SessionLost`, `Closed`, `Timeout` (new: `GroupConfig::commitSyncTimeoutMs`, default 30 s, see Q6). `HubCommand` gains a `c` word so it can carry `{target word, ctx, token}`. From a callback on the duty thread it is **allowed** in the token form (it is just a command applied directly); only `commitSyncAndWait` throws there. |
| `OpResult TransactionalProducer::initTransactions(target, Token)` | P9 | `Ok`, `BrokerError`, `Fenced`, `Timeout`, `Closed` |
| `OpResult sendOffsetsToTransaction(offsets, groupId, target, Token)` | P10 | Same set as `initTransactions`. The `offsets` map is copied into the command payload (cold). |
| `OpResult commitTransaction(target, Token)` / `abortTransaction(target, Token)` | P11/P12 | Same set as `initTransactions` |
| `OpResult Client::subscribe(..., target, Token)` *(optional overload)* | n/a | `SubscribeBound {value=subscriptionId}`, or `Timeout`/`BrokerError`/`Closed`. The handle-returning overload stays the same. |

**Which thread runs each kind of callback:**

| Callback kind | Thread |
|---|---|
| `CompletionHandler`, `onAck`, batch-ack handler, Thread mode | runner thread |
| `CompletionHandler`, `onAck`, batch-ack handler, Invoker mode | owner thread, inside `doWork()` |
| `Closed` completions delivered by `Client::close()` | the thread calling `close()`, after the runner is joined (the same rule as `onDisconnected` today) |

All of these are `noexcept` at the call boundary. They are invoked under the existing `invokeUserCallback_` try/catch, and must not block or call `waitFor`.

---

## 3. Handle lifetime without cross-thread `shared_ptr`

### 3.1 Rules

1. **Handles are move-only.** This applies to `Producer`, `TransactionalProducer`, `Subscription` and `GroupConsumer` (the last two already are). `BatchBuilder` holds a borrowed `ProducerImpl*` and must not outlive its `Producer`; a debug check enforces that.
2. **Sharing a Producer across threads in Thread mode.** The app keeps one `Producer` object and lends `Producer&` to its threads. The ring path is M2O, so concurrent `trySend` calls are fine. The app must not destroy the `Producer` while any of those threads still uses it, the same contract as any C++ object.
3. **TypedTopic.** `TypedTopic<T>` becomes move-only (or holds a `Producer&`; see Q1).
4. **One owner at a time.** Each impl, together with its rings, has exactly one owner:
   - the **app** from `make` until registration is accepted;
   - then the **duty registry** (the hub's `std::vector<Impl*>` of owning raw pointers);
   - then exactly one of:
     - **(a)** the duty side, after the handle's `Release` command is applied (the duty side finishes the impl and frees it); or
     - **(b)** the handle, after Client close transfers ownership to it.

   Ownership changes **only by ring order**.

### 3.2 Linearizable close of the agent command ring (ring internals)

**The gap today.** `M2ORingBuffer::closed` is a separate word written with a release store ([H] `M2ORingBuffer.h:117-122`). A writer that checked it before the close can still claim afterwards. That is why F8a needs `HubSubmit::ClosedAfterSubmit` and accepts a leak (f8a-report concern 7).

**Proposal.** Fold *closed* into the tail position that `tryClaim` already CASes:
- close sets the top bit of the tail with a CAS loop;
- `tryClaim` fails as `Closed` when it sees that bit.

**Result.** Every claim is totally ordered with the close:
- A claim before the close is drained by the existing close drain, which already waits past uncommitted claims (fix5).
- A claim after the close fails.

`HubSubmit` loses `ClosedAfterSubmit`, and the leak disappears.

**Cost.** This changes only ring internals, which the allowlist already marks `RING`. It deviates from Aeron's `ManyToOneRingBuffer`, which has no close (see Q10). It is on the produce hot path in Thread mode, because the producer ring is the same class, so it needs a benchmark (§5, C1).

### 3.3 Release protocol (no atomics outside rings)

```
Handle destructor / close():
  if impl.status.producerClosed():           // acquire; Client close already transferred ownership
      delete impl; return                    // case (b)
  switch submit(Release{impl}):              // agent command ring (lifecycle op: guaranteed delivery,
                                             // F8a backoff-until-accepted-or-closed)
    Accepted -> return                       // case (a): duty side now owns; handle never touches impl again
    Closed   -> spin (BackoffIdle) until impl.status.producerClosed();   // close() is in progress and will
                delete impl                  // transfer: every impl not released by a DRAINED command is
                                             // transferred, and the ring close is linearized (3.2)

Duty side, applying Release{impl}:  finish(impl) (fail outstanding ops -> completions, see section 4), unlink, delete
  (if it is inside that impl's own dispatch: defer the delete to the end of the dispatch: F8a DispatchScope)

Client::close():  stopped_ -> join runner -> close command ring (3.2) -> final drain (applies every accepted
  Release, freeing those impls) -> for each remaining impl: finish(impl) -> publishFinal ->
  status.producerClose()  (release; ownership now belongs to the handle) -> forget it
```

**Why this works.** The decision "who frees" comes from two facts: whether the `Release` command was drained, and the ring's own producer-closed word. Both are ring internals. No refcount and no Dekker flag pair is involved.

**Handle on the duty thread.** A handle destroyed on the duty thread, inside a callback or as the Invoker owner, applies `Release` directly, as F8a's direct-apply path does.

**Outliving the Client.** The F8a contract stays: a handle may outlive its Client, but must not be used *concurrently* with the Client's destruction. After close, the handle reads only its own impl's ring words.

**Producer destructor semantics (proposal, see Q2).** `Release` puts the impl into a duty-owned Closing state:
1. it flushes records already accepted into its ring or builders;
2. it delivers their completions to their targets: `Completions` objects are independent of the producer, and callback contexts must outlive their ops;
3. it frees itself after the last ack, or after `deliveryTimeoutMs`.

`Producer::abort()` fails everything with `Closed` immediately. The rule "an accepted record is never silently dropped" therefore holds through destruction.

**`Completions` uses the same protocol.** Its rings are the producer-closed and consumer-closed words of the F8a `OneToOneRingBuffer`, and it is released with `CqRelease`. Completions that target a released or reused `CqRef` are dropped by the generation check, after the duty side has stopped writing.

### 3.4 Runner start handshake (I1)

`std::promise go` is replaced by a stack-local 1-record `OneToOneRingBuffer` start latch:
1. the runner writes `dutyThreadId_` (its own id) as its first action;
2. it then publishes one record, and then calls `producerClose()` as its last access to the ring;
3. the constructor spins with `YieldingIdleStrategy` until it has read that record **and** observes `producerClosed()`, then returns. There is no timeout: the runner publishes in its first statements, so the wait ends as soon as the runner is scheduled (time-independent).

The ring read (acquire on the record header, paired with the runner's release) gives the happens-before edge; the thread id is read after it. Waiting for the producer-closed word as well is required for memory safety: `write()` stores the tail after the record header and the ring lives in the constructor's frame, so returning on the record alone would let the runner's tail and closed stores land in a dead stack frame. The producer-closed word is the runner's final store to the ring, so observing it proves the ring is no longer in use.

---

## 4. Error, timeout and close semantics

**Invariant (to be tested everywhere): every `Accepted` op completes exactly once.** Nothing else ever completes.

| Situation | Completion status | Delivered by |
|---|---|---|
| Broker answers OK | `Ok` | duty thread on the response |
| Broker error code / ErrorResponse matching the correlation id | `BrokerError` (+`errorCode`, truncated text) | duty |
| Session-fatal ErrorResponse (SessionExpired/UnknownSession) | `SessionLost` for every pending control op of that session (today `Client.cpp:1258-1278` fails all pings) | duty |
| Op deadline passes (connect `connectTimeoutMs`, ping/DDL `pingTimeoutMs`, txn ops, commitSync `commitSyncTimeoutMs`) | `Timeout` | duty `sweepTimeouts` (the existing 100 ms cadence; deadlines use duty time `now()`, not `steady_clock::now()` on the app thread) |
| Producer per-record or Tier-2 delivery timeout, redirect budget exhausted, deferred-buffer full after acceptance | `DeliveryFailed` (`BatchAck::kClientFailure` stays for batch acks) | duty |
| acks=None | `Ok` with `value=-1` when handed to transport (unchanged meaning) | duty |
| commitSync superseded / rejoin / session loss after send | `Superseded` / `Rejoined` / `SessionLost` (F8a I6 semantics, now as statuses) | duty |
| Txn fenced | `Fenced` | duty |
| Second `connect()` while one is pending | the older one gets `Superseded` (today: exception "connect() called while another connect is pending") | duty |
| Handle closed (Producer `abort`, GroupConsumer close, Subscription unsubscribe) with ops outstanding | `Closed` | duty, on applying the close |
| **Client close** | **every outstanding token, of every kind, completes `Closed`:** control ops, per-record acks, Tier-2, commitSync, txn ops, flush markers | the thread calling `close()`, after the join and final drain, in this order: producers' `failAll`, then txn `failAll`, then group `closeAll`, then control tables. Each `Completions` gets its `Closed` records and then `producerClose()`, so `finished()` becomes true once drained. Credits guarantee room. |
| Issue after close | synchronous `OpResult::Closed` / `SendResult::Closed`, no completion | issuing thread |
| Issue with no credit | synchronous `QueueFull`, no completion | issuing thread |

**Callback contexts.** `ctx` must stay valid until its op's completion has been delivered, including the `Closed` delivered by `close()`. This is documented on `CompletionHandler`.

---

## 5. Migration plan (ordered, each step independently testable)

**Prerequisites.**
- F8a is merged with T14.
- F8b is either done for `Client::mtx_` and `nextCorrId_`, or folded into C3 and C5 (Q8).
- Every new test is named `*Regression` and has RED evidence first (failing on the parent commit, or by mutation).

**Benchmark rules.**
- Hot-path steps run on k3s-node-01 after `vm-clean.sh`.
- Configuration: 3-node, acks=all, 1 KiB records.
- Comparison: interleaved A/B/A/B with n ≥ 15 against the parent commit.
- Gate: no regression of the ≥100k rec/s and p99 ≤100 µs target.

| Step | Content | Hot path? | Tests (`*Regression`) | Benchmark expectation |
|---|---|---|---|---|
| **C1** | Ring primitives: (a) `M2ORingBuffer` linearizable close (tail bit); (b) `detail::CompletionRing` + `Completions` (poll, credit, stash, `waitFor` with an injected pump function), standalone with no Client | **YES (a)**: `tryClaim` on the producer ring in Thread mode | `M2OCloseLinearizesWithClaimsRegression`: N writers vs closer, 1000 iterations under TSan; every accepted claim drained, every post-close claim rejected. `CompletionRingTwoThreadExactlyOnceRegression`: 1M completions with variable payloads across wraps. `CreditNeverOverflowsRingRegression`: fuzzed op mix at the credit limit, `lostCompletions==0`. `WaitForStashesOtherTokensInOrderRegression`. `WaitForInvokerPumpsAndNeverParksWhileWorkRegression`. `FinalDrainAfterProducerCloseRegression`. | `trySend` Thread mode: ±2% throughput, p99 unchanged (it adds one bit test on a value already loaded). Microbench `ProducerAccumBench` before/after. |
| **C2** | `CompletionTarget`/`CqRef` registry in Client: `CqAdd`/`CqRelease` hub commands, close delivers `Closed` and producer-closes each CQ, §3.3 protocol for `Completions` | no | `CompletionsOutliveClientSeeClosedRegression`. `CqReleaseRacingCloseNeverLeaksNorDoubleFreesRegression` (ASan + TSan). `StaleCqRefIsDroppedByGenerationRegression`. `CompletionsUsedOffOwnerThreadIsWrongThreadRegression` (debug). | none |
| **C3** | Control ops connect/ping/defineTopic/getTopicSchema → token + handler; duty-owned pending table; `NotConnected` decided on duty; `*AndWait` sugar; `openTopicAndWait`/`TypedTopic::fromSchema` (delete `std::async`); delete `PendingX` and their `mtx_` uses; migrate the CLI | no | `ConnectCompletesOnceViaCqAndHandlerRegression`. `PingEchoPayloadDeliveredInlineRegression` (binary, empty, wrap). `ControlOpTimeoutCompletesTimeoutRegression` (frozen injected clock, no sleeps). `SessionFatalErrorCompletesAllPendingSessionLostRegression`. `SecondConnectSupersedesFirstRegression`. `CloseCompletesEveryControlTokenClosedRegression`. `SchemaLargerThanReservationIsResultTooLargeRegression`. `OpenTopicSpawnsNoThreadRegression`. | `ClientPingBench` RTT unchanged or better (no promise/condvar wake). |
| **C4** | `GroupConsumer::commitSync(target, Token)` + `commitSyncTimeoutMs`; delete `commitSyncPromise` | no | Port F8a's commitSync tests (`CommitSyncSuperseded…`, `…Rejoin…`, `…SessionLoss…`, `…NothingToCommit…`, `…ErrorCode…`, `…UnrelatedResponses…`) to statuses. Add `CommitSyncTokenFromCallbackIsAllowedRegression` and `CommitSyncTimesOutRegression`. | none |
| **C5** | Txn ops → token + handler (after, or together with, the F8b ring conversion of `cmdQueue_`/`pendingSends_`); delete `PendingTxnOp`/`Command::promise`/`PendingOffsetOp::promise` | no (txn send path untouched by F8c) | `InitTxnFencedCompletesFencedRegression`. `CommitAbortCompleteOnceRegression`. `SendOffsetsTwoPhaseCompletesOnceRegression`. `TxnCloseCompletesAllClosedRegression`. | none |
| **C6** | Producer per-record and Tier-2 acks → tagged 64-bit handle; `onAck(fn, ctx)`; `trySend(..., cq, t)`; `BatchBuilder::send(target, t)`; batch acks to handler or CQ (replaces `collectBatchAcks`); delete `PendingAck`, `registerFuture`, `registerCallback`, `failUnsubmitted`; optional `flush(target, t)` | **YES** | Extend `ProducerAllocRegressionTest`: **zero allocations on the acked path** (callback and CQ forms, Ring and Direct). `AckTokenDecodedToRightTargetRegression`. `RejectedTrySendReturnsCreditAndNoCompletionRegression`. `DeliveryTimeoutCompletesDeliveryFailedOnceRegression`. `DeferredSendAckCompletesOnceRegression` (Task 14 arena). `Tier2AckViaTokenRegression`. `CloseFailsAllInflightAcksClosedRegression`. `FlushTokenCompletesAfterPriorRecordsRegression` (if Q5 is yes). | Acked mode (`rune_produce_throughput RUNE_BENCH_MODE=acked`): expect **higher** throughput and lower p99, because one `new`, one `std::function` and one `delete` per record go away. Void mode: ±2%. Per-record CQ cost is about 25 ns/completion, roughly 0.25% of a core at 100k/s. Gate: ≥100k rec/s, p99 ≤100 µs. |
| **C6a** | *(added 2026-10-01 by the T15 design review, I2)* Non-blocking begin-close: `OpResult Producer::close(Completions& or CompletionHandler, Token)` closes the producer intake synchronously (the C1 tail-bit CAS; Direct mode: owner-local) and submits a `Close` lifecycle command. The duty side enters `Draining`: it consumes the producer M2O ring **up to the closed tail** (every claim made before the tail close is committed and read, per the fix5 close drain and its unblock bound, so a record claimed before the close but committed after it is never read after the token completes), flushes, waits for every accepted record, then completes the token `Ok` (or `Timeout` at `deliveryTimeoutMs`, or `Closed` if `closeImmediately()` intervenes). `close()` with no arguments stays the blocking name (begin-close plus wait); `closeAndWait(timeout)` is an alias that takes a timeout. The duty thread may also tail-close the intake on the transport-failure path; the CAS is idempotent across closers. Needed by T15 `ShardGroup` close step 1 (all intakes closed before any shard drains). | hot-adjacent (the tail bit is already C1) | `BeginCloseRejectsLaterSendsSynchronouslyRegression`. `BeginCloseTokenCompletesAfterLastAckRegression`. `BeginCloseThenCloseImmediatelyCompletesEachRecordOnceRegression` (TSan). `SecondBeginCloseIsClosedNoCompletionRegression`. `BeginCloseWaitsForClaimedUncommittedRecordRegression` (TSan: claim, then begin-close, then commit; the token completes only after that record's completion). `DutyTailCloseOnFailureCompletesPreCloseClaimsClosedOnceRegression` (TSan). | none beyond C1 |
| **C7** | Handles move-only; hubs hold owning raw pointers; §3.3 release protocol for Producer/Txn/Subscription/GroupConsumer; `BatchBuilder` borrows `ProducerImpl*`; delete every `shared_ptr` holder (`Client.cpp:1687, 1931`; [A] `:1954`) | hot-adjacent (`send` dereferences `impl_`: same instruction) | `HandleReleaseAcceptedDutyFreesRegression`. `HandleReleaseAfterCloseHandleFreesRegression`. `ReleaseRacingCloseExactlyOneFreeRegression` (ASan + TSan, 1000 iterations). `ProducerDtorFlushesAcceptedRecordsRegression` (after user decision 2: close() then destroy keeps delivering; the destructor itself = closeImmediately, covered by `ProducerDtorIsCloseImmediatelyRegression`). `ProducerAbortFailsClosedRegression`. `HandleDestroyedInsideOwnCallbackDefersFreeRegression`. `NoSharedPtrInPublicHandlesGuardRegression` (guard, below; renamed `NoSharedOwnershipOutsideTransportGuardRegression` in C9 fix round 1 — it scans the whole tree). | void and acked: ±2% (ABAB). |
| **C8** | Runner start latch (§3.4); remove `<future>` from every header and source | no | `RunnerSeesDutyThreadIdBeforeFirstCycleRegression` (TSan). | none |
| **C9** | Guard: extend `scripts/check_no_atomics_or_mutexes.py` to ban `std::future`, `std::promise`, `std::shared_future`, `std::packaged_task`, `std::async` and `#include <future>` in `src/**` and `include/**`, plus `std::shared_ptr` in public handle headers (allowlist: transport Aeron objects S5). Add self-test fixtures. Migrate examples, `bench/ClientPingBench.cpp`, `dr_client.cpp` (rebuild the DR e2e image with an SHA tag) and plan Tasks 15-18 text (`ClientShard::connect(target, Token)`). | no | `NoFuturesOrPromisesGuardRegression` + fixtures. `NoSharedPtrHandlesGuardRegression`. | none |
| **C10** | Validation: full Debug suite, TSan per binary, ASan for C2/C7, real end-to-end (`dr_client` default config against a real broker), final ABAB on k3s-node-01 | n/a | full suite + `ctest -R Regression` | Final gate: ≥100k rec/s, p99 ≤100 µs (3-node, acks=all, 1 KiB), no regression vs `991cffc`+F8a+F8b. |

**Ordering.**
- C1 comes first, because every later step uses `Completions` and the linearizable close.
- C2 through C5 are independent of each other once C1 and C2 are in.
- C6 and C7 touch the hot path and each gets its own ABAB.
- C6a follows C6 (it uses the ack targets and C1's tail bit) and comes before C7.
- C9 lands last, so the guard turns red the moment anything reintroduces a future.

---

## 6. Risks

1. **Wide exactly-once surface.** Every failure path (redirect, rejoin, fence, deferred arena, close drain) must complete each accepted token exactly once. Mitigation:
   - a per-test `CompletionLedger` helper that asserts every issued token completed exactly once by the end of the test;
   - running that helper in every C3-C7 test.
2. **Credit exhaustion is a new issue-time failure (`QueueFull`).** An app that never polls stalls its own ops. That is by design (back-pressure, never a drop), but it must be documented loudly. `waitFor` polls, so the sugar is immune.
3. **One `Completions` per issuing thread in Thread mode.** Multi-threaded apps need one per thread; the registry caps out at 1024. Sharing one `Completions` across threads is a debug-checked contract violation, not something the type system prevents.
4. **Thread-mode wait latency up to the max park (1 ms) with the default backoff.** Callers that care must choose a spinning `Idle`.
5. **Deviation from Aeron in `M2ORingBuffer` (tail-bit close).** This conflicts with the "Aeron is the bible" rule; see Q10. The alternative keeps `ClosedAfterSubmit` and its accepted leak, and makes the release protocol rely on a leak instead of a transfer.
6. **Breaking change ripple.** It reaches the CLI, 27 test/example/bench files (about 390 sites), the `dr_client` DR e2e image, `TypedTopic` copyability, and plan Tasks 15-18 (`ClientShard::connect`).
7. **Function-pointer callbacks are less ergonomic than lambdas.** `CompletionHandler::of<&C::method>(obj)` covers the common case. Capturing lambdas need an object that outlives the op.
8. **Polled `getTopicSchema` has a result cap** (`maxResultBytes`). Larger descriptors need the callback form or a bigger reservation.
9. **"Not connected" becomes asynchronous.** It arrives as a `NotConnected` completion rather than an immediate exception, which is a behaviour change for tests that expect a synchronous throw.

## 7. Open questions for the user

- **Q1. Move-only handles.** Is it acceptable that `Producer`, `TransactionalProducer` and `TypedTopic` become move-only? Thread-mode sharing would be by `Producer&`. The alternative is a copyable handle whose copies stay on one thread (a non-atomic local count), but that cannot be shared across threads.
- **Q2. Producer destructor.** Should it flush accepted records and deliver their completions before freeing (proposed, the Kafka-like `close()`)? Or abort immediately with `Closed`, with `abort()` and `close()` both explicit?
- **Q3. Tokens.** App-chosen 48-bit tokens (proposed: zero lookup, usable as the app's array index)? Or library-assigned ids returned at issue, as Aeron does with registration ids?
- **Q4. `std::function` on cold callbacks.** Should cold-path callbacks (connect, ping, DDL, txn, commitSync) also accept `std::function`, since allocation is irrelevant there? Or should everything be function pointer plus context, for one uniform rule (proposed)?
- **Q5. Tokenized `flush`.** Add `Producer::flush(target, Token)` with Kafka `flush()` semantics now, or later?
- **Q6. `commitSync` timeout.** Add one (proposed: `commitSyncTimeoutMs`, default 30 s; today it has none)?
- **Q7. Schema result cap.** Is `maxResultBytes` = 16 KiB right for polled `getTopicSchema`, with `ResultTooLarge` above it?
- **Q8. F8b/F8c boundary.** Fold "duty-owned pending tables + correlation-id assignment" into C3 and C5 (proposed, since the promise maps are deleted anyway)? Or land F8b's mutex removal first and then swap the promise for a token?
- **Q9. Which `*AndWait` sugar to ship.** The proposed list is in §2.3. Should any of it be dropped, so that tools must write the explicit loop?
- **Q10. Tail-bit close in `M2ORingBuffer`.** Accept it (a Rune extension beyond Aeron's `ManyToOneRingBuffer`; ring-internal and allowlisted as `RING`) to remove `ClosedAfterSubmit`? Or keep Aeron's ring untouched and accept the shutdown leak?
- **Q11. Out of scope but adjacent.** These remain `std::function` today:
  - `RecordHandler` on the subscription dispatch hot path;
  - group `onRecords`;
  - `onBatchAck`.

  F8c converts only the ack and batch-ack handlers. Should subscription and group record handlers become function pointer plus context, or a template, in a follow-up F-ticket?

## Decisions (user, 2026-10-01)

1. **Move-only handles** — approved.
2. **Producer shutdown** — two explicit paths:
   - `close()` — stops accepting new sends, then **waits until every accepted record is sent and acknowledged** (or definitively failed by the broker / delivery timeout), flushing pending batches first; completes every token with its real outcome. Blocking form follows the `waitFor` rules (Invoker owner pumps `doWork()`; Thread-mode app thread polls under an idle strategy; from a callback → `logic_error`).
   - `closeImmediately()` — stops accepting, **fails/discards** everything not yet acknowledged with `Closed` (no flush, no wait).
   - The destructor of a still-open handle performs `closeImmediately()` (a destructor must never block indefinitely); this is documented so apps call `close()` explicitly when they need delivery.
   - The same `close()`/`closeImmediately()` pair applies to `TransactionalProducer` (close = finish/abort semantics per the txn state machine, documented) and to `Client` (Client `close()` drains all producers, then tears down).
3. **Closed flag folded into the tail word of the agent command ring** — approved, subject to the C1 interleaved A/B gate (n≥15, k3s-node-01, no regression vs ≥100k rec/s & p99 ≤100µs).
4. **Blocking `*AndWait` convenience wrappers** — approved for cold-path ops (connect, ping, schema, transactions, commitSync); none for per-record sends.
