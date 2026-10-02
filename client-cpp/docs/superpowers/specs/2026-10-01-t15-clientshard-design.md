# T15: ClientShard / ShardGroup — design note (gate before implementation)

- **Status:** design note, revision 2. Revision 2 addressed the review "Approve with changes" (I1–I8; minors M1, M2, M3, M5, M6) and the user's AUTO shard-count request (§2a, D9). **Revision 3** applies the re-review's four required changes (R1 sessions never give up after Live, R2 Aeron driver timeout plus a SIGSTOP DR gate, R3 the C6a drain waits for the ring up to the closed tail, R4 AUTO with no known topics = 1) and its minors. No code has changed.
- **Base:** `feat/sharded-invoker-client` @ `5428e42`. Brief: `.superpowers/sdd/2026-09-30-sharded-invoker-client/task-15-brief.md`.
- **Inputs:**
  - spec §4.1 (`rune-broker/docs/superpowers/specs/2026-09-30-self-contained-worker-shards-design.md`);
  - the F8c note and its Decisions (`2026-10-01-f8c-no-futures-design.md`);
  - the current `Client`, `ClientAgent`, `ProducerImpl` Direct ingress (T14), `AeronTransport` and `M2ORingBuffer`;
  - the F8a `OneToOneRingBuffer` and `StatusRing` (wt2 `a7e9671`, not yet merged).
- **Binding rulings:**
  - no `std::atomic` or mutex outside ring internals;
  - no `std::future` or `std::promise`;
  - per-shard MTU defaults to 1408, with jumbo opt-in;
  - one owner thread per piece of state, and rings as the only cross-thread channel;
  - no allocation, virtual calls or `std::function` on the send/duty path.

**One-line model.** A shard is one `Client` plus one `Producer`. It has its own embedded driver in invoker mode, its own Aeron client with conductor invoker, its own sessions (a seed session plus one per leader node) and its own ack (egress) subscription. All of it is driven by exactly one thread. Shards share nothing with each other, and partitions map to shards by a pure, fixed function.

**Target configuration (I6).** For the §1 pass criterion: `shardCount = 1` **set explicitly**, `ShardMode::Invoker`, the owner thread pinned to core 0, `mtuBytes = 8192` set explicitly (jumbo works on loopback), and brokers on cores 1–3 (spec §5). The 4-core bench box shares its cores with the 3 brokers, so the automatic default (§2a) would be wrong there. Everything else in this note (N > 1, Agent mode) is the general-purpose surface and must not regress that configuration.

**Shard count defaults to AUTO (user request, §2a).** Example: a 16-core box with 16 or more partitions gets 16 shard threads.

## 1. Ownership

| State | Invoker mode | Agent (Thread) mode |
|---|---|---|
| Embedded driver (conductor, sender, receiver) | The app thread that constructed the `ClientShard` (the **owner**). It pumps the driver inside `doWork()`. | The shard's **runner** pumps it inside its duty cycle. The constructing thread pumps it only during `start()`, before the runner exists. Ownership passes to the runner through the F8c C8 start latch, which gives a happens-before edge (I3). **One thread per shard** (D1). |
| Aeron client and conductor invoker, publications, egress subscription and images, sessions and `SessionMachine`s, `MetadataCache`, route table | owner | runner |
| Partitions' batch builders, in-flight rings, deferred arena, producer id/epoch/sequences, pending-ack slots | owner (Direct ingress, T14) | runner (it drains the producer M2O ring into the builders) |
| Producer intake ring | none (Direct) | M2O ring. App threads are producers; the runner is the only consumer. The tail-bit close (F8c C1) is the only way intake ends. |
| `ShardGroup` (`vector<unique_ptr<ClientShard>>`) | not used (D5) | Built on the **control thread**, and immutable after construction. App threads only call the pure `shardFor` and the owning shard's `trySend`. Connect, close and `pollEvents` are control-thread only (debug-checked). |
| Completion/ack targets (F8c `Completions`, `CompletionHandler`) | owner (both ends of its completion ring) | Runner = producer; the issuing app thread = sole consumer. One `Completions` per app thread **per shard**. |

There is no cross-shard state.

## 2. Partition → shard map, how it relates to broker workers, and cost (I5)

- **Function.** `shardFor(topicId, partition, n) = (uint32(topicId) + uint32(partition)) % n`, and 0 when `n ≤ 1`.
  - It is pure, `constexpr`, and fixed for the life of the group.
  - It is independent of partition count, metadata and broker worker count (spec §4.1).
  - **M3:** `ShardGroup::trySend` computes `partition = key.partition(topic.partitionCount)` **once** and passes it to `ClientShard::trySendTo(topic, partition, key, value)`. That overload checks `owns()` against the given partition and then calls a `Producer` overload that takes a precomputed partition. The key is never hashed twice.
- **The map is static and deliberately not aligned to broker workers.** A partition's ordering and idempotence live in exactly one producer, which holds the producer id and sequences (`ProducerImpl.h:492-499`). Moving a partition between client shards would mean a cross-thread hand-off of in-flight batches and sequence state.
  - The broker's `ShardRouter` uses `fib(groupKey, partition) % W`, and the client cannot see `groupKey` or `W`.
  - Correctness never depends on alignment. Each shard routes per partition using its own `MetadataResponse`: a non-empty `ingressEndpoint` (broker T14) sends produce frames for that partition to that worker on its `ingressStreamId`, through `setRoute(t, p, session=node, data=worker)`; an empty value falls back to the node endpoint. Consuming the field is T16's job.
- **Acks.** Each shard announces its own egress channel in Connect.
  - UDP egress: port `base + i` (`channelWithPortOffset`).
  - IPC egress: unchanged, because the drivers are separate.
  - Worker `ProduceResponse`s (broker §3.4) are matched by batch slot and correlation id, never by publisher.
- **Cost, both sides.** Notation:
  - N = client shards; L = leader nodes; W = workers per node;
  - P_s = partitions owned by shard s;
  - T_c = shard driver term length; T_b = broker egress publication term length.

| Resource | Where | Unaligned (this design) | Aligned (rejected) |
|---|---|---|---|
| Ingress publications | client, per shard | ≤ L × min(W, P_s), each a log of 3 × T_c | L |
| Egress images (worker → shard acks) | client, per shard | ≤ L × W images, each 3 × **T_b** (image logs are sized by the publisher's term) | L |
| Session egress publications | broker, per node | O(W × N) per client: every worker that owns a partition of shard s publishes to s's session | O(max(W, N)) |
| Ingress images | broker, per worker | ≤ N per client | ≤ 1–N |

- **Shard term default.** `ShardGroupConfig` sets the shard driver's `termBufferBytes` to **1 MiB** (a plain `Client` keeps its 4 MiB default).
  - Sizing: 1 KiB × 100k rec/s is about 100 MB/s, and the publication window of term/2 = 512 KiB covers about 5 ms of that. That is far above the in-flight depth at p99 ≤ 100 µs.
  - The maximum message, term/8 = 128 KiB, is at least `maxFrameBytes` (64 KiB). This is validated: `maxFrameBytes ≤ term/8` → `invalid_argument`.
  - The Aeron receive window (128 KiB) is at most `SO_RCVBUF` and at most term/2 (the standing constraint).
  - A 1 MiB term cuts client log memory per publication from 12 MiB to 3 MiB.
  - The broker's T_b is the broker's own setting. The note documents that client image memory scales with it.
- **Why alignment is rejected for now:** metadata would have to advertise `W` and `groupKey`; it gives no correctness benefit; and at the target configuration (N = 1) both columns are equal.

## 2a. Shard count: AUTO by default; core assignment

- **Configuration.** `ShardGroupConfig::shardCount = kAutoShards` (0) is the default. Any value ≥ 1 is an explicit override and is used as given.
- **Auto derivation.** N is resolved **once**, on the control thread, in the `ShardGroup` ctor, before any driver starts, and is then fixed (`shardFor` needs a stable n). The derivation is a pure function so it can be tested: `resolveShardCount(cfg, const CpuSet& allowed)`.

  ```
  cpus   = min(|allowed|, cgroupQuotaCpus)     (Linux; cgroupQuotaCpus = ceil(quota/period) of cpu.max, or ∞ if "max"/absent)
  usable = cpus − reservedCores                (reservedCores: config, default 0)
  cap    = max(partitionCount) over cfg.base.topics
  N      = topics empty ? 1                    (R4: logged — "no topics known at construction; AUTO → 1")
                        : max(1, min(usable, cap))
  ```

  - **The allowed set comes from the platform:**
    - **Linux:** the **calling thread's** affinity mask (`sched_getaffinity(0)` from the control thread, before any shard thread exists), further bounded by the cgroup v2 `cpu.max` quota (`/sys/fs/cgroup/cpu.max`; cgroup v1 `cpu.cfs_quota_us / cpu.cfs_period_us` as a fallback). A container with a 2-CPU quota on a 64-core host therefore gets 2, not 64. The pure function receives `(CpuSet, quotaCpus)`, so both are injected in tests.
    - **macOS:** `sysctlbyname("hw.activecpu")` (there is no affinity API), treated as cores `0 … n−1`.
  - **Why the cap uses `max(partitionCount)` and not the sum:** with `shardFor`'s consecutive residues, one topic with P partitions gives every shard `< P` at least one partition. A cap of the sum can still leave shards idle; for example, topics {0: 2 partitions, 1: 2 partitions} with N = 4 leaves shard 3 empty.
  - `usable ≤ 0` clamps to 1.
  - **No known topics → 1 (R4).** Topics often arrive at runtime (`defineTopic`/`openTopic`), and N cannot change after construction. AUTO with an empty `cfg.base.topics` therefore resolves to **1** and logs that it did, rather than spinning up a thread per core for partitions that may never exist. An app that knows its partition count up front lists the topics, or sets `shardCount` explicitly.
  - The resolved N, the allowed set and the reason (mask, reserved, cap) are logged once at construction.
- **Pinning** is opt-in (`pin = false` by default). The resolved order is:
  1. **Explicit `firstCore ≥ 0`:** shard i goes to `firstCore + i`. Every one of those cores must be in the allowed set, otherwise `invalid_argument`.
  2. **`pin = true` with no `firstCore`:** shard i goes to the i-th core of the allowed set in ascending order, after skipping the reserved cores. The reserved cores are the **lowest** `reservedCores` cores of the calling thread's affinity mask (documented, e.g. for a co-located broker or the app's main thread).
  3. **macOS:** pinning is unsupported, so `pin = true` or `firstCore ≥ 0` is rejected with `invalid_argument` rather than a silent no-pin.
  4. **Invoker mode:** `pin = true` or `firstCore ≥ 0` is rejected with `invalid_argument` (T15a m12). The owner is an app thread the app pins itself; in the target configuration the app pins its owner thread to core 0.
- **Invoker mode:** AUTO is **rejected** (`invalid_argument`). Each app thread constructs its own `ClientShard(cfg, i)`, so N must equal the number of driving threads, which only the app knows. Each thread's affinity mask can also differ, so the threads could not all derive the same N.
- **Guidance (documented on `ShardGroupConfig`):**
  - **A dedicated box:** leave AUTO. A 16-core box with ≥ 16 partitions gets 16 shard threads; use `reservedCores` for cores the app or a co-located process needs.
  - **A shared box:** set `shardCount` explicitly. On the 4-core bench box with 3 co-located brokers, the bench uses `shardCount = 1`, Invoker mode, core 0.
  - In Agent mode with AUTO, the runners' idle mode decides CPU use. N BusySpin runners would peg N cores, so BusySpin under AUTO logs a warning recommending pinning.

## 3. Cross-thread flows

| Flow | Mode | Channel | Full-channel policy | Ordering |
|---|---|---|---|---|
| App → shard send | Invoker | **None.** Direct append on the owner (T14). | Builder, in-flight ring or arena full → `BackPressured`. The void `send()` pumps `doWork()`; from a callback it uses the deferred arena. | Per partition: program order |
| App → shard send | Agent | The owning shard's producer **M2O** ring (several app threads can target one shard) | `BackPressured`. The void `send()` backs off and never drops (T13). | Per (app thread, partition): send order. Across threads: the linearized order of the tail CAS. |
| Ack / completion → app | Invoker | F8c `onBatchAck(fn, ctx)` / `onAck(fn, ctx)` inside `doWork()`, or the owner's `Completions` | Credit is reserved at issue; with no credit the send returns `QueueFull` synchronously | Per partition: batch order |
| Ack / completion → app | Agent | Function pointer plus ctx on the runner. **ctx per shard**: `ShardGroup::onBatchAck(shard, fn, ctx)`. Or an O2O `Completions` per (app thread, shard). `ShardCompletions` is an owner-thread array of N of these, polled round-robin. | Same credit rule | Per partition: batch order. None across shards. |
| Metadata → shard | both | **None.** Each shard requests and applies its own metadata on its own session and duty thread. | n/a | `MetadataCache` rejects stale `leaderEpoch` |
| Shard events → control thread | Agent | One F8a `StatusRing<ShardStatus>` per shard, drained by `ShardGroup::pollEvents()`. Invoker: `ClientShard::state()` read on the owner. | Ordinary updates are latest-value (a refused one is superseded). Terminal `Closed` and `Failed` use `publishFinal` and always fit. | Latest value per shard. The state **may go back** (Live ↔ Degraded ↔ Reconnecting); it is not monotone (I1). |
| Control ops (connect, begin-close) → shard | Agent | The `Client` agent command ring | try forms return `BackPressured`. Lifecycle ops are guaranteed (F8a back-off until accepted or `Closed`). | Per shard: FIFO |
| Intake close | Agent | The producer ring's **tail-bit close** (F8c C1, a ring-internal CAS). It is set by the control thread (`Producer::close(target, token)`, §4) or by the runner on the failure path. The CAS is idempotent. | n/a | Linearized: a claim before the close is drained and completes exactly once; a claim after it returns `Closed` |

No flow uses a shared flag. The brief's `bool closed_`, read by app threads, is removed (D3).

## 4. Lifecycle (I1, I2)

**Shard state is derived, not stored separately.** The owner or runner computes it each duty cycle from its own sessions:
- the seed session (the gateway node, which carries Connect and Metadata);
- one session per routed leader node.

It is published only when it changes.

```
Starting                         ctor: transport start pumps own driver until seed pub + egress sub resolve
Idle                             started; connect not issued (or last connect failed)
Connecting{token, deadlineNs}    connect(target, token) accepted; seed session Connecting
Live{sessionId}                  seed Connected AND every routed leader's session Connected
Degraded{down: nodeMask}         seed Connected, >= 1 routed leader session not Connected
                                 (partitions on those nodes back-pressure; others flow)
Reconnecting{attempt, nextNs}    seed session lost (SessionExpired / UnknownSession / keepalive loss /
                                 retry budget exhausted); SessionMachine Backoff state, retries forever (R1)
Draining{token, deadlineNs}      begin-close accepted: intake closed, flushing, awaiting acks of accepted records
Failed{reason}                   TransportClosed{aeronError} (driver error / Aeron client closed)
                                 | Rejected{errorCode} (non-retriable broker answer: ProtocolVersion, auth)
Closed
```

- **A delivery timeout fails only a batch,** with `DeliveryFailed` exactly once. It never fails the shard (the earlier `Failed{DeliveryTimeout}` is removed).
- **A connect that fails before the shard was ever Live** (broker error, or the connect deadline) completes its token with `BrokerError`/`Timeout` and returns the shard to `Idle`. After the shard has been Live, a lost session never returns it to `Idle`: it reconnects. Convergence comes from retrying, not from a tuned deadline (spec §1a.9).
- **"Never give up" requires a `SessionMachine` change (R1, in T15a).** Today a session goes **terminal `Disconnected`** after `maxResumeRetries` = 3 resume attempts (`SessionMachine.h:440`), after `maxConnectRetries` = 3 connect timeouts (`:514`), after `maxRedirects` (`:242`, `:266`, `:306`), and on any other error code (`:253`, `:322`). The change:
  - A session gets an `everLive_` latch, set on its first `ConnectResponse{OK}`, and a new rich state `Backoff{attempt, nextAttemptNs}`.
  - With `SessionConfig::reconnectForeverAfterLive = true`, every one of those **budget-exhaustion** transitions goes to `Backoff` instead of `Disconnected` once `everLive_` is set. `Backoff` waits until `nextAttemptNs` on the injected duty clock, then does a fresh connect (a resume first if the broker still knows the session id). The delay is capped exponential: 10 ms doubling to 1 s, never a give-up.
  - This applies to the seed session and to every leader session alike.
  - `shardClientConfig` sets the flag to true. A plain `Client` keeps `false`, so today's behaviour and tests are unchanged; flipping the default is a separate decision.
  - **Non-retriable** answers stay terminal: `ProtocolVersion` and authentication rejection. On the seed session they make the shard `Failed{Rejected{ec}}`. On a leader session the shard stays `Degraded`, and that node's batches fail with `DeliveryFailed` at their deadline.
  - The budget counters stay. They now mean "attempts before entering Backoff", which keeps the fast first retries.

**Transition table.**

| From | Event | To | Effects |
|---|---|---|---|
| Starting | transport started | Idle | — |
| Starting | driver init / validate fails | (ctor throws) | Nothing is published. The group tears down 0..k−1. |
| Idle | `connect(target, t)` | Connecting | — |
| Connecting | seed Connected (+ all routed leaders) | Live | token `Ok` |
| Connecting | seed Connected, ≥ 1 routed leader not Connected | Degraded | token `Ok` (the connect op is about the seed session; leaders converge on their own) |
| Connecting | non-retriable broker answer (`ProtocolVersion`, auth) | Failed{Rejected{ec}} | token `BrokerError`; as for any `Failed` |
| Connecting | broker error / connect deadline | Idle | token `BrokerError` / `Timeout` |
| Live | a leader session lost | Degraded | that node's partitions back-pressure; their batches retry the re-offer on reconnect, or fail at delivery timeout |
| Live / Degraded | seed session lost | Reconnecting | pending control ops `SessionLost`; batches stay in their slots |
| Reconnecting | seed session's retry budget exhausted (would be `Disconnected`; **Disconnected-after-Live**) | Reconnecting (`attempt+1`) | `SessionMachine` → `Backoff`; it never gives up (R1) |
| Degraded | a leader session's retry budget exhausted (**Disconnected-after-Live**) | Degraded | that leader session → `Backoff`; it never gives up (R1) |
| Live / Degraded / Reconnecting | seed session gets a non-retriable answer | Failed{Rejected{ec}} | as for any `Failed` |
| Degraded | all routed leaders Connected | Live | in-flight re-offered (T1.4 resend path) |
| Reconnecting | seed reconnected | Degraded or Live | metadata refreshed, routes rebuilt |
| any running state | Aeron client closed | Failed | Runner (or owner) closes intake (tail bit). Every accepted, unacknowledged record **and every record claimed before the tail close** completes `Closed` exactly once. `Failed` is published with `publishFinal` (always fits, uses the reserve) **without closing the StatusRing producer**, so the later `Closed` can still be published; only the final `Closed` closes it. **M2:** the runner then switches to a parked idle (1 ms) that only drains the closed intake ring until `close()`. It does no I/O and keeps the join handshake. |
| Connecting | `close()` | Draining | The connect keeps running. Accepted records flush if it succeeds, or fail at delivery timeout. The connect token completes with its own outcome, or `Closed` at teardown. |
| Connecting | `closeImmediately()` | Closed | connect token `Closed`; all outstanding `Closed` |
| Live / Degraded / Reconnecting | `close()` | Draining | — |
| Live / Degraded / Reconnecting | `closeImmediately()` | Closed | intake tail-closed; the ring is drained to the closed tail and every accepted or claimed record completes `Closed` exactly once; pending control ops `Closed` |
| Failed | `closeImmediately()` | Closed | nothing left to complete (`Failed` already completed everything); join and teardown |
| Draining | (R3) every accepted record completed **and** the producer M2O ring consumed up to the closed tail (every claim before the tail close is committed and read; F8c fix5 close-drain, bounded by the unblock rule) | Closed | begin-close token `Ok`; join and teardown. A record claimed before the close but committed after it is therefore read, sent and completed **before** the token. |
| Draining | close deadline (`deliveryTimeoutMs`) | Closed | the ring is still drained to the closed tail first; remaining records `DeliveryFailed`; token `Timeout` |
| Draining | `closeImmediately()` | Closed | remaining records `Closed` exactly once; token `Closed`. There is no double completion: a record whose ack already completed it has had its slot freed. |
| Failed | `close()` | Closed (**immediate**) | Nothing can drain; equivalent to `closeImmediately()`. |
| Draining / Closed | `close()` again | unchanged | A no-op. The blocking form returns once Closed. A second begin-close returns `OpResult::Closed` synchronously with no completion. |
| Closed | `closeImmediately()` | Closed | no-op |
| Idle / Starting | `close()` / `closeImmediately()` | Closed | nothing outstanding |

**Begin-close: the non-blocking operation `ShardGroup` close step 1 needs (I2).**
- `OpResult Producer::close(Completions& | CompletionHandler, Token)` runs on the control (or owner) thread.
  1. It closes the producer intake **synchronously** with the tail-bit CAS, so once it returns, every later `trySend` gets `Closed`.
  2. It then submits a `Close` lifecycle command on the agent ring. The runner applies it as `Draining{token, deadline}`.
  3. The token completes `Ok`, `Timeout` or `Closed` per the table.
- **Names:** `close()` (no arguments) **stays the blocking form**: begin-close plus a wait. `closeAndWait(timeout)` is an alias that takes a timeout. The non-blocking form is the `close(target, token)` overload.
- In Invoker mode the same call is made on the owner thread, and the owner's `doWork()` drives the drain.
- This is a new F8c step, **C6a**, added to the F8c note's migration table.

**`ShardGroup` close.**
1. `producer.close(cq, base + i)` for every shard. This closes all intakes at once.
2. Wait on all N tokens (`waitFor`, capped).
3. `Client::close()` on each shard: join and teardown, which is now quick because the shards are already drained.

The wall time is about the slowest shard's drain.

**Start.**
- `ClientShard(cfg, i)` derives its config with `shardClientConfig`: `dataDir/shard-i`, egress port `+i`, `mtuBytes = base.mtuBytes` (1408 default), `termBufferBytes = shardTermBufferBytes` (1 MiB default), the runner core `firstCore + i`, `driverOnDutyThread = true`, and the liveness from I4.
- An active driver already holding the dir makes construction fail loudly (`AeronTransport.cpp:449`), so building the same index twice is caught.
- `ShardGroup` construction is all-or-nothing: if shard k throws, shards 0..k−1 are `closeImmediately()`'d and destroyed in reverse order.

**Connect** (F8c, no future):
- `OpResult ClientShard::connect(target, Token)`;
- `ShardGroup::connectAll(Completions&, Token base)`;
- the cold wrappers `connectAndWait(timeout)` and `connectAllAndWait(timeout)`. In Invoker mode the wrapper pumps; in Agent mode it polls under back-off; from a callback it throws `logic_error`.

**Handle lifetime (no `shared_ptr`).**
- `ClientShard` is non-copyable and non-movable. It owns its `Client` by value and its move-only `Producer` (F8c C7), declared Client-first so the Producer is destroyed first.
- `implForInternalUse` is dropped.
- `ShardGroup` holds a `vector<unique_ptr<ClientShard>>` that is sized once and never mutated.
- App threads hold `ShardGroup&` and must stop sending before it is destroyed.
- `TopicHandle` is a plain value.
- The destructor of a still-open shard or group does `closeImmediately()`; it never blocks indefinitely.

## 5. Failure modes

| Failure | Where detected | Outcome |
|---|---|---|
| Shard driver init fails (dir held by an active driver, bad MTU/window/`SO_RCVBUF`, `maxFrameBytes > term/8`, port in use) | `validate()` / driver init in the ctor | Throws, naming the shard index and the cause. The group is all-or-nothing. Up-front checks: `base + N − 1 ≤ 65535`, and no derived egress port collides with the gateway or another configured port. |
| Bad core layout (I6) | `ShardGroup` ctor, before any driver starts | **Linux:** each of `firstCore … firstCore + N − 1` must be in the calling thread's `sched_getaffinity` mask, otherwise `invalid_argument` listing the allowed cores. With `pin = true`, N must also be ≤ the number of allowed cores left after the reserved ones. **macOS:** pinning is unsupported, so `firstCore ≥ 0` or `pin = true` is rejected with `invalid_argument` rather than a silent no-pin. AUTO in Invoker mode is rejected (§2a). |
| A broker node or worker unreachable | publication `Resolution` / `SessionMachine` on the owner | The offer fails without blocking, the batch stays in its slot, and only that node's partitions back-pressure. The shard goes Degraded (a leader is lost) or Reconnecting (the seed is lost). Batches past the delivery timeout complete `DeliveryFailed` exactly once. Siblings are unaffected. |
| Producer M2O ring full (Agent) | app-thread `tryClaim` | `BackPressured`; the void `send()` backs off and never drops |
| `Completions` full | cannot happen on the duty side | Credit is reserved at issue; with no credit the send returns `QueueFull` synchronously. `lostCompletions_ == 0` is asserted. |
| `StatusRing` full | runner | An ordinary update is superseded on a later cycle. Terminal states always fit. |
| Metadata changes a partition's `ingressEndpoint` (**M1**, narrowed) | the owner shard's `onMetadataResponse` → `setRoute` "changed" | This can **only** happen through a leader change. Broker F7 makes a runtime replica → worker move impossible (it halts the broker), so there is no worker-to-worker move to handle. The client-shard map is unchanged; only the owning shard's data channel changes. The new publication resolves in the shard's own driver, and in-flight batches are re-offered at once (T16) with the same producer id and sequences. A late ack hits a freed slot and is ignored. |
| Aeron client closed (driver error) | owner | `Failed{TransportClosed}` (§4). No `::exit`, no hang. |
| Invoker app sends to a partition it does not own | `owns()` | `WrongShard` synchronously, no completion. The void form throws `logic_error`. |

## 6. Deviations from the brief

- **D1. A shard's driver is always in invoker mode, and in Agent mode the runner pumps it** (spec §4.1 and §4.3).
  - Today `RunnerMode::Thread` implies `DriverMode::Thread` (`Client.cpp:78`): three threads per shard.
  - **Change:** add `ClientConfig::driverOnDutyThread` (default `false` for a plain `Client`, unchanged; forced `true` by `shardClientConfig`). `toAgentConfig` (`Client.cpp:78`) then sets **`transport::DriverMode::Invoker` under `RunnerMode::Thread`**: no driver thread, `useConductorAgentInvoker(true)`, and the runner pumps both. `ClientAgent::doWork` already calls `transport_.dutyCycle()` (`ClientAgent.cpp:129`).
  - **Liveness (I4), the option chosen:** when the client and driver share one duty thread, the Aeron client-liveness / inter-service timeout is set to **effectively unbounded** (`kMaxClientLivenessTimeoutNs`, 1 h), and the timeouts derived from it follow (`publication_unblock_timeout` = 1.5 × liveness).
    - **Why:** the client and the driver can only stall together, so the timeout no longer detects a dead peer. It only measures this thread's own stalls, and turning a ≥ 1 s app stall (GC-like pause, debugger, overloaded box) into a permanent shard death is wrong. With an unbounded timeout, a stall ends in the ordinary path: the broker expires the session, and the shard goes `Reconnecting` → `Live`.
    - **Why not "pump the conductor before the driver after a gap":** the `ClientConductor` checks inter-service time against its own previous invocation (`ClientConductor.h:436`). The first conductor pass after a gap closes the client whatever the order. Ordering only stops the *driver's* client-timeout scan from firing first, so it would fix half the problem.
    - **The hidden third timeout, R2:** the Aeron C++ client also has a **driver timeout** (`aeron::Context::mediaDriverTimeout`, default 10 s). After a gap, the conductor sees a stale driver heartbeat and closes itself. In duty-thread mode the transport sets **`mediaDriverTimeout` equal to the liveness (1 h)**: the driver cannot be dead while its only pumper is alive. The driver-before-conductor order in `dutyCycle()` (`AeronTransport.cpp:72-76`) stays as a second line of defence (the driver refreshes its heartbeat before the conductor checks it). That order is asserted by a test, but nothing relies on it.
    - **The publication unblock timeout is safe at 1.5 h:** it only reclaims a claim abandoned by a dead publisher, and here the publisher is the same thread.
    - `aeronClientLivenessTimeoutNs` stays overridable, with a warning.
    - The same default applies to Invoker shards, for the same reason.
    - `InvokerTransport.StalledOwnerSurfacesClosedInsteadOfExitingRegression` keeps covering the real-stall mechanism when the timeout is configured small.
  - **Idle modes and latency.** The runner's park also delays the driver: inbound acks and status messages wait out the park.
    - **Balanced** parks for up to 1 ms.
    - **LowLatency** is Backoff(5 spins, 3 yields, 1 µs → **100 µs** park ceiling; `Client.cpp:722-727`). Its park ceiling puts the client-side p99 floor at about 100 µs, which meets the target with no margin.
    - **BusySpin on a pinned core** is the only Agent idle mode that fits p99 ≤ 100 µs.
    - All three park far below any liveness concern. The note and `ShardGroupConfig` documentation state this.
- **D2. No futures.**
  - `connect`/`connectAll` become the F8c token/handler forms plus `*AndWait` wrappers.
  - `pollBatchAcks` becomes `onBatchAck(fn, ctx)` / `batchAcksTo(Completions&)`.
- **D3. No cross-thread `closed_`.**
  - Invoker: owner-local derived state.
  - Agent: closure reaches app threads only through the tail-bit intake close, and reaches the control thread through `StatusRing`.
- **D4. No `shared_ptr` accessor;** `Producer` is move-only.
- **D5. `ShardGroup` is Agent-only, with control operations restricted to the control thread.** Ack ctx is per shard.
- **D6. Shard driver defaults:** MTU 1408 (the spec's 8 KB is overridden by the standing rule) and a 1 MiB term (I5). The bench sets 8192 explicitly.
- **D7. Tests** (M5):
  - no `std::atomic` and no `sleep_for`;
  - Agent-mode tests wait on **ring events** (`pollEvents`, completion tokens), each with a hard iteration cap;
  - the transport-closed fault seam is a **null-by-default function pointer** (`Client::testTransportFault_`), checked on a cold branch of the duty cycle. It replaces the brief's 200 ms liveness (below the 1 s floor) and its 600 ms sleep.
- **D8. Sequencing and dependencies (I3).**
  - **T15a** can start now, with no F8c dependency: `ShardMap.h`, `shardClientConfig`, `resolveShardCount` and the core assignment (§2a), port/core/term validation, `driverOnDutyThread` plus the I4/R2 liveness and driver-timeout defaults, and the R1 `SessionMachine` reconnect-forever change.
    - Until C8 lands, the handoff relies on the `std::thread` constructor's own synchronizes-with edge: everything the start thread wrote before creating the runner, including all driver and conductor state, is visible to the runner.
    - The `std::promise go` itself is replaced in C8.
  - **T15b** needs F8a merged, plus F8c:
    - **C1** (tail-bit close, `Completions`);
    - **C2** (registry);
    - **C3** (connect token);
    - **C6** (ack targets);
    - **C6a** (begin-close, §4);
    - **C7** (move-only handles);
    - **C8** (the runner start latch: the explicit happens-before edge for handing the driver from the start thread to the runner).
  - There is no interim future-based shim.
  - **M6:** Invoker shards keep `lingerUs = 0` by default (spec §4.2: end-of-pass batching), overridable through `invokerLingerUs`.
- **D9. `shardCount` defaults to AUTO, not 1 (user request).** AUTO = min(calling thread's affinity mask, cgroup `cpu.max` quota) − `reservedCores`, capped at `max(partitionCount)`, minimum 1; **1 when no topics are known** (R4). It is rejected in Invoker mode. Pinning is opt-in, onto the allowed cores after the reserved ones (§2a). The bench sets `shardCount = 1` explicitly.

## 7. Test plan (each test failing first; every name contains `Regression`; capped drive loops; injected clocks; no sleeps)

**T15a.**
- `ShardMapRegression.SingleShardOwnsEverything`
- `ShardMapRegression.DeterministicAndInRange`
- `ShardMapRegression.EveryShardOwnsAPartitionOfAnNPartitionTopic`
- `ShardMapRegression.PortOffsetRewritesOnlyEndpointPort`
- `ShardMapRegression.PortOffsetRejectsMalformedOrOverflowing`
- `ShardConfigRegression.DerivesDirPortCoreTermAndKeepsMtu1408`
- `ShardConfigRegression.DerivedPortCollisionRejectedBeforeAnyDriverStarts`
- `ShardConfigRegression.CoresOutsideAffinityMaskRejected` (Linux; pinning-unsupported platforms reject `firstCore ≥ 0`)
- AUTO derivation (pure, with an injected `CpuSet`; no dependence on the test machine):
  - `ShardCountAutoRegression.EqualsAffinityMaskSize` (e.g. mask {0, 2, 5, 7}, 64 partitions → 4)
  - `ShardCountAutoRegression.SubtractsReservedCores` (mask of 8, `reservedCores = 3` → 5)
  - `ShardCountAutoRegression.CappedAtMaxPartitionCount` (mask of 16, topics {4, 6 partitions} → 6; every shard owns ≥ 1 partition)
  - `ShardCountAutoRegression.ExplicitCountOverridesAuto` (`shardCount = 3` on a 16-core mask → 3)
  - `ShardCountAutoRegression.NeverBelowOne` (mask of 2, `reservedCores = 4` → 1; mask of 1 → 1)
  - `ShardCountAutoRegression.NoTopicsResolvesToOneAndLogs` (R4: a 64-core mask with no topics → 1)
  - `ShardCountAutoRegression.CgroupQuotaBoundsAffinityMask` (mask of 16, quota 2.5 CPUs → 3; quota "max" → 16)
  - `ShardCountAutoRegression.InvokerModeRejectsAuto`
  - `ShardPinningRegression.AutoPinsIthAllowedCoreSkippingReserved` (mask {1, 3, 4, 6}, `reservedCores = 1`, `pin = true` → shards on 3, 4, 6)
  - `ShardPinningRegression.PinOnUnsupportedPlatformRejected` (macOS)
  - `ShardCountAutoRegression.PlatformCpuSetMatchesSchedGetaffinity` (Linux, real call; on macOS, `hw.activecpu`)
- `ShardConfigRegression.MaxFrameAboveTermEighthRejected`
- `ShardConfigRegression.DriverOnDutyThreadRunsNoDriverOrConductorThread` (`usesDriverThread() == false`; the T2 dual-pumper counter = 0)
- `ShardConfigRegression.PlainClientDefaultUnchanged`
- `ShardConfigRegression.DutyThreadLivenessDefaultsUnbounded`
- `ShardConfigRegression.DutyThreadMediaDriverTimeoutEqualsLiveness` (R2: the Aeron `Context` the transport builds carries `mediaDriverTimeout == liveness`)
- `ShardConfigRegression.DutyCyclePumpsDriverBeforeConductor` (R2: order asserted through a call-recording seam)
- `ShardConfigRegression.ThreadRunnerWithDriverOnDutyThreadUsesInvokerDriver` (`DriverMode::Invoker` under `RunnerMode::Thread`)
- `SessionMachine` (R1; pure, injected clock, no transport):
  - `SessionMachineRegression.SeedSessionLostBeyondRetryBudgetStillReconnects` (after Live: more than `maxResumeRetries` and `maxConnectRetries` failures → `Backoff`, never `Disconnected`; the clock advances to each `nextAttemptNs`; the session finally reaches Connected)
  - `SessionMachineRegression.LeaderSessionLostBeyondRetryBudgetStillReconnects` (the same for a leader-endpoint session, including the `maxRedirects` exhaustion paths)
  - `SessionMachineRegression.BackoffIsCappedExponential` (10 ms → 1 s cap; no attempt fires before its deadline)
  - `SessionMachineRegression.NeverLiveSessionKeepsTerminalDisconnected` (the connect budget before the first Live is unchanged)
  - `SessionMachineRegression.NonRetriableAnswerStaysTerminalAfterLive` (`ProtocolVersion`)
  - `SessionMachineRegression.PlainClientDefaultStillGivesUp` (flag false → today's behaviour)
- `ShardConfigRegression.DriverHandoffStartThreadToRunnerIsRaceFree` (**TSan**: start, then runner start, then the first cycles touch driver state)

**T15b.**
- **Invoker shard:**
  - `ClientShardRegression.InvokerProducesAndAcksWithNoThreads` (`CompletionLedger`: exactly one completion per record; acks contiguous per partition)
  - `ClientShardRegression.OnePassIsOneFramePerPartition`
  - `ClientShardRegression.ForeignPartitionIsWrongShardNoCompletion`
- **Allocation:**
  - `ClientShardRegression.TrySendZeroAllocation` and `ShardGroupRegression.TrySendZeroAllocation` (`AllocCounter`, steady state, Invoker and Agent)
  - `ShardGroupRegression.PartitionComputedOnce` (M3: a counting key hasher sees one call per send)
- **Connect:**
  - `ClientShardRegression.ConnectViaTokenAndHandlerNoFuture`
  - `ClientShardRegression.ConnectAndWaitFromCallbackThrows`
- **Session loss and state:**
  - `ClientShardRegression.LeaderSessionLossWhileLiveIsDegradedThenLive`
  - `ClientShardRegression.SeedSessionLossWhileLiveReconnectsWithoutFailing` (pending control ops `SessionLost`; batches re-offered and acked exactly once)
  - `ClientShardRegression.DeliveryTimeoutFailsBatchNotShard`
  - `ClientShardRegression.ConnectWithLeaderDownIsDegradedTokenOk`
  - `ClientShardRegression.SeedRejectedNonRetriableIsFailedRejected`
  - `ClientShardRegression.FailedThenClosedBothPublishedOnStatusRing` (`Failed` does not close the StatusRing producer)
- **Close:**
  - `ClientShardRegression.CloseWhileConnectingDrainsOrTimesOut`
  - `ClientShardRegression.CloseImmediatelyWhileConnectingCompletesConnectClosed`
  - `ClientShardRegression.CloseImmediatelyFromLiveDegradedReconnectingFailed` (parameterized; ledger exactly once)
  - `ClientShardRegression.BeginCloseWaitsForClaimedUncommittedRecord` (**TSan**, R3: an app thread claims, the control thread begin-closes, the app commits afterwards; the token completes `Ok` only after that record's ack; 1000 iterations)
  - `ClientShardRegression.BlockingCloseAndCloseAndWaitAreEquivalent`
  - `ClientShardRegression.CloseOnFailedIsImmediate`
  - `ClientShardRegression.DoubleCloseIsNoOp`
  - `ClientShardRegression.CloseThenCloseImmediatelyAbortsDrainExactlyOnce`
  - `ClientShardRegression.CloseImmediatelyRacingInflightAcksNoDoubleCompletion` (Agent; 1000 iterations under TSan; ledger)
  - `ClientShardRegression.DtorIsCloseImmediately`
- **Failure path:**
  - `ClientShardRegression.TransportFaultFailsShardCompletesAllClosedOnce` (fault seam)
  - `ClientShardRegression.RecordsClaimedBeforeFailureTailCloseCompletedClosedOnce` (Agent: app threads claiming while the runner tail-closes on fault; TSan; ledger)
  - `ClientShardRegression.FailedRunnerParksAndDoesNoIo` (M2)
  - `ClientShardRegression.SameIndexTwiceFailsLoudOnActiveDriverDir`
- **Group:**
  - `ShardGroupRegression.AgentRoutesEachSendToOwningShard`
  - `ShardGroupRegression.ManyAppThreadsOnePartitionKeepPerThreadOrder`
  - `ShardGroupRegression.ConstructionFailureOfShardKTearsDownZeroToKMinus1`
  - `ShardGroupRegression.OneShardFailedSiblingsKeepProducing`
  - `ShardGroupRegression.CloseClosesAllIntakesBeforeDraining` (a send racing close is either acked exactly once or `Closed` synchronously; TSan)
  - `ShardGroupRegression.PerShardAckCtxNeverSharedAcrossRunners`
  - `ShardGroupRegression.NoAtomicsMutexFuturesSharedPtrInShardSources` (guard extended to `src/shard/**` and `include/rune/client/shard/**`)

**Gates (I7).**
1. Full Debug suite plus `ctest -R Regression`; TSan per binary.
2. **Real-broker e2e, default Agent mode:** `ShardGroup` with the **default AUTO shard count** (and N = 1 explicit), Balanced, default configuration (1408 MTU, legacy broker) and flag-on (shard broker).
3. **Real-broker e2e, N = 2 over UDP egress** against a shard-mode broker. It asserts, from the per-shard egress subscription counters, that every worker `ProduceResponse` for shard i arrives on port `base + i`, with zero acks on the wrong port and the ledger exactly-once.
4. **DR e2e on k3s-node-01 (R2), after `vm-clean.sh`:** a 3-node shard-mode cluster; the client is an Agent shard group (N = 2) under steady load. `SIGSTOP` the client process for **> 15 s** (past the Aeron 10 s defaults and the broker session timeout), then `SIGCONT`. Expect, per shard, from the `pollEvents` log: Live → Reconnecting → Live, with no `Failed`. The exactly-once ledger holds: every accepted record is acked once, or failed `DeliveryFailed` once; no duplicates in the topic for the idempotent producer. Docker images are tagged with the git SHA.
5. **Interleaved ABAB, n ≥ 15, on k3s-node-01** after `vm-clean.sh`: 3-node, acks=all, 1 KiB.
   - A = today's Thread-mode `Client`. B = an Agent shard (N = 1) with D1, BusySpin, pinned.
   - B must meet the absolute targets (≥ 100k rec/s **and** p99 ≤ 100 µs) and show no regression against A.
   - Run the same ABAB for the target configuration (Invoker shard on core 0) against today's Invoker `Client`.
