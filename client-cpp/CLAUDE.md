# client-cpp — architecture notes for AI sessions

The repo-wide rules are in the root [CLAUDE.md](../CLAUDE.md). This file holds client-specific detail only.

> **What this is:** the standalone, independently-releasable **C++23 Rune client
> library** — producer, subscriber, and consumer-group member — that downstream
> apps (e.g. world-server) link to talk to the broker cluster over Aeron.
> It shares the wire/record formats with the broker via the header-only
> `protocol/` library (consumed at `../protocol/include`).

The design lives in
`../broker/docs/superpowers/specs/2026-09-11-rune-client-and-broker-runtime-design.md`
and the milestone-acceptance notes under `../broker/docs/superpowers/`.

---

## 1. The one idea: a single-writer agent behind a public façade

The client is built the same way as the broker: **one background runner thread**
loops a single-writer `doWork(nowNs)` duty cycle under an `IdleStrategy`. App
threads never touch Aeron, the session, or the transport directly — they hand
work to the agent through a **lock-free `ManyToOneRingBuffer`** and get results
back via completion tokens / callbacks that fire **on the runner thread** (F8c: no futures).

```
  APP THREADS                         RUNNER (agent) THREAD  — single writer
  ───────────                         ─────────────────────────────────────
  Client::connect/ping                ClientAgent::doWork(nowNs):
  Producer::send/flush   ─┐             1. drain command ring (bounded)
  Subscription (un)sub    │  ManyToOne  2. transport_.poll() egress fragments
  GroupConsumer::commit  ─┘   ring ───► 3. route egress → session / hubs
                                        4. session_.tick(nowNs) (timeouts,
                                           keepalive, redirect/retry)
   completions/cbs    ◄────────────────  completed here (NOT app thread)
```

**Threading contract (applies everywhere):**
- Ring drain + transport poll + session tick + all record/response routing run
  on the **one** runner thread. No locks guard `session_`/`transport_` — the ring
  *is* the synchronisation primitive.
- Callbacks (`onConnected`, `onRecords`, `onAssigned`, produce acks, …) fire on
  the runner thread — **do not block them**; blocking stalls all dispatch.
- Zero-copy views (record `key`/`value` spans) point into the Aeron fragment
  buffer and are valid **only during the callback** — copy anything you retain.
- Futures never hang: the runner runs a periodic timeout sweep that fails stale
  pending connects/pings/produces.

`IdleStrategy` choice: `BackoffIdleStrategy` by default (CPU-friendly);
`YieldingIdleStrategy` for LowLatency mode (trades a core for sub-µs reaction).

---

## 2. Layered stack (`include/rune/client/`, `src/`)

Bottom to top — each layer is a separate CMake target (see §3):

| Layer | Files | Role |
|---|---|---|
| Transport | `transport/AeronTransport` | Embedded Aeron media driver + one outbound `ExclusivePublication` (→ gateway ingress) + one inbound `Subscription` + `FragmentAssembler` (← gateway egress). Single-writer `offer()`/`poll()`. |
| Session | `session/SessionMachine` | Pure, clock-injected, transport-agnostic state machine: `Disconnected→Connecting→Connected→Resuming→Closed`. Handles connect handshake, keepalive, bounded redirect/retry (NotLeader). Talks to transport only via an injected `SessionIo`. |
| Agent | `agent/ClientAgent` | The single-writer duty cycle wiring transport + session; owns the `ManyToOneRingBuffer`; decodes egress frames (`checkFrame → peekTemplate → decode → route`). |
| API | `Client` | Public entry: owns the runner thread, exposes `connect()`/`ping()` futures + lifecycle callbacks; factory for topics, producers, subscriptions, group consumers. |
| Producer | `producer/{Producer,ProducerImpl,BatchBuilder,ProducerHub}` | Auto-batched, idempotent produce. |
| Subscriber | `subscriber/{Subscription,SubscriberHub}` | Direct topic-partition subscribe + record delivery. |
| Group | `group/{GroupConsumer,GroupConsumerHub}` | Consumer-group membership, assignment, auto/sync offset commit. |
| Support | `Key`, `TopicHandle`, `codec/{Codec,DataCodec}`, `Version` | Keys, topic ids, codec adapters over `protocol/`. |

### 2.1 Producer (`producer/`)
Tier-1 producer with a **zero per-send heap allocation** fast path. App threads
call `send()`/`flush()` → write a compact `SEND_RECORD` into the ring (the
alloc-free handoff). The agent drains, accumulates records into per-
`(topicId,partition)` `BatchBuilder`s (RecordBatch v2), and flushes a batch when
**linger elapsed OR `batchBytes` reached OR `flush()` requested**.
- `Acks`: `None` (fire-and-forget; fully alloc-free end-to-end), `Leader`
  (default; awaits `ProduceResponse`), `All` (awaits commit by the in-sync replica majority),
  `View` (flag, combine with `|`: ack after the batch is folded into the views; `Acks::All | Acks::View` is the
  usual checked-write setting). Invalid combinations throw `std::invalid_argument` at creation. A `View` producer
  needs a broker of version 10 or later (`OpStatus::BrokerTooOld` locally otherwise) and sends batches of at most
  4096 records. A transactional producer never sets the `View` wire bit.
  A `View` ack on a verdict source carries per-record verdicts; no resend, no epoch bump.
- F8c C6: no per-send heap on ANY path — acked sends carry a tagged 64-bit
  token (a `Completions` queue's CqRef + Token, or the producer's `onAck`
  function pointer + Token) in the record's ack slot; begin-close
  (`close(target, token)`) / `close()` / `closeImmediately()` per F8c C6a.
- Idempotence: the producer gets its `producerId`/epoch from the broker via
  `InitProducerId` (coordinator link), and each partition stamps a monotonic
  `baseSequence` into the batch header.
- Tier-2 `batch()` is single-thread per producer (the creating thread, or `Producer::bindBatchThread()`): the
  preallocated slot pool's app side is owned by that thread; off-thread `batch()` returns a refused builder
  (`send()` = WrongThread) and never touches the pool. Thread mode: a builder destroyed elsewhere hands its slot
  back through the producer ring; Direct (Invoker) mode too (#655): the slot comes back on the owner's next duty
  pass (only a full or closed ring loses it, logged). An Invoker claim aborted off its owner thread is rolled back
  the same way (#656, kMsgDirectClaimAbort).
  Plain send/trySend/tryClaim/claimBatch stay any-thread.
  The Tier-2 slot pool is allocated by the first `batch()` / `bindBatchThread()`, never at construction: a
  producer that never batches reserves nothing.
- Phase 2b zero-copy produce: `Producer::tryClaim(topic, key, len)` -> `Claim`
  (`buffer()` / `commit()` / `abort()`, RAII; refused = false + `result()`),
  `claimBatch(topic, key, n, recLen)` -> `BatchClaim` (`record(i)`),
  `TypedTopic::claim(key)` -> `TypedClaim<XEncoder>` (generated encoder over the
  claimed bytes; all-fixed schemas; the key is written by its constructor, so use
  non-key setters only).  Thread mode claims a producer-ring slot (2 copies: ring
  -> frame -> Aeron); Invoker mode reserves the record in the partition's Produce
  frame (1 copy) under the `DirectClaimOpen` state: every other send on that
  producer returns `ClaimHeld` until it settles, and a failed/closed partition
  abandons it (`commit()` -> false) into `DirectClaimAbandoned`: the builder's
  storage (the caller's still-writable span) moves to the producer's quarantine
  buffer, so nothing appended or sealed later can contain it; sends resume at
  once, another claim stays `ClaimHeld` until the abandoned one is settled.  A `claimBatch` appends its n records
  contiguously and atomically (nothing foreign between them), but an UNFENCED one
  can join an open RecordBatch with neighbouring sends, and is acked by the
  BatchAck(s) covering that RecordBatch, not by "one BatchAck of n"; `flush()`
  (+ `doWork()` in Invoker mode) before the claim fences it into its own
  RecordBatch.  Acked `claimBatch(..., cq, token)` / `claimBatch(..., AckToken)` complete once
  with the batch's base offset; an aborted one never completes.  Zero allocation (gates: `ProducerClaimAllocRegression`,
  `TypedClaimAllocRegression`, `rune_producer_claim_bench`).
- `ProducerHub` holds the producers the duty thread owns (registered through
  the agent command ring — no lock, no atomic); `onProduceResponse` routes acks
  back by the batch id's ordinal.
- F8c C7 — handle lifetime (spec §3): `Producer` (plain or transactional),
  `Subscription`, `GroupConsumer`, `TypedTopic` are MOVE-ONLY; `BatchBuilder`
  borrows its producer.  No `shared_ptr` anywhere in the handles or hubs
  (guard: `NoSharedPtrInPublicHandlesGuardRegression`): each impl is owned
  jointly by its handle and the duty side, each side detaches its share of the
  impl's ownership word (a ring word, one RMW) exactly once, and whichever
  detaches last frees it — nobody waits, nobody refcounts.  A handle's
  destructor hands its share over with one `HandleRelease` command
  (Producer: `closeImmediately()` unless a close already
  started; Subscription: unsubscribe; GroupConsumer: close); the duty side
  frees at the end of a duty cycle once nothing references the impl (never
  under a running callback).  The duty thread (Invoker owner, runner callback)
  applies a release directly; any other thread uses the command ring, and if
  that is full the release is ORPHANED (orphan bit in the impl's ownership
  word + a fence at the ring tail + the ring's orphan hint): the duty side
  scans, applies it once the ring head passes the fence — no wait, no time
  bound.  Client close
  hands every still-held impl back to its handle.
- Shutdown pairs (user decision 2): `Producer::close()` drains /
  `closeImmediately()` discards; a transactional `Producer::close()` aborts an open
  broker transaction (EndTxn(abort)) before it reports closed /
  `closeImmediately()` discards locally; `Client::close()` drains every
  producer (transactional ones abort first), then tears down /
  `Client::closeImmediately()` (= the destructor) tears down at once.

### 2.2 Subscriber (`subscriber/`)
`Subscription` + `SubscribeOptions`. `From` selects the start offset —
`Earliest` (wire `-2`), `Latest`/LEO (`-1`), or explicit `Offset`. `Isolation`
is passed through (read-uncommitted / LEO frontier for now). The `RecordHandler`
fires on the agent thread with zero-copy spans. F8a (shared-nothing): the
`SubscriberHub` and all agent-side subscription state are owned by the duty
thread; `subscribe()`/`unsubscribe()`/RAII dtor send `HubCommand`s on the agent
command ring, and `position()`/`subscriptionId()`/`active()` drain a
per-subscription `StatusRing` the agent publishes to (once per Records frame +
lifecycle changes; eventually consistent; dirty snapshots retried every duty
cycle). Status rings are SPSC `detail::OneToOneRingBuffer` (faithful Aeron port).
Unsubscribe / GroupConsumer close have guaranteed delivery (applied directly on
the duty thread; otherwise a full ring is waited out on duty progress — never a
timeout, never a throw). `finished()` ⇒ no further callback, `position()` final:
resume = unsubscribe(), wait for finished(), then read position(). The handle is
owned by one thread at a time (calls from inside its callbacks are allowed) and
may outlive its Client.

Command-ring drain rule (F8a fix2/fix3): a `kMsgSend` control frame is offered
only while the seed session is Connected AND the frame carries its sessionId.
The transport's verdict is 3-way: Accepted; Retry (back-pressure / not connected
/ admin action) leaves it at the ring head (ABORT, re-offered next cycle — held
for exactly as long as the transport stays transiently unavailable; there is NO
fixed time bound); Fatal (publication closed / max position / Aeron threw) is
dropped, counted and logged rate-limited (first, then every 1024th). Frames over the transport's max message length are
rejected synchronously by `submitSend()`. A frame of a session that no longer
exists is dropped, counted, logged rate-limited. Subscribe (adoption + reconnect
resend), Unsubscribe, OffsetCommit and LeaveGroup bypass the ring (duty-thread
direct send on the default ingress publication): Retry = retried every duty
cycle; Fatal = dropped and the request ends (Subscribe fails the subscription,
Unsubscribe finishes locally, OffsetCommit fails commitSync, LeaveGroup is
dropped). The hubs learn the seed session's (connected, sessionId, connect
episode) AT the transition (`Client::syncSeedSession_`: ConnectResponse /
ErrorResponse handlers, before every hub command, and once per duty cycle) —
never only at the end of the cycle. Leaving a connected (sessionId, episode)
fails pending commitSyncs with the retriable `SessionLostError`.

### 2.3 Group consumer (`group/`)
`GroupConsumer` runs the full join/sync/stable/rejoin state machine, heartbeat,
record routing, and the auto-commit duty cycle **on the runner thread**. App
threads only register `onAssigned`/`onRevoked`/`onRecords` (setup) and call
`commit()`, `commitSync(target, token)` (a completion token the duty thread completes on
`OffsetCommitResponse`), and `close()` — each a `HubCommand` on the agent ring
(F8a: no atomics, no mutex). `subscribeGroup()` registers through the ring too
(`GroupAdd`); the `GroupConsumerHub` list is duty-thread owned.

The no-atomics/no-mutex law is enforced by `scripts/check_no_atomics_or_mutexes.py`
(ctest `NoAtomicsOrMutexesOutsideAllowlistRegression`): only `M2ORingBuffer`'s own
memory ordering is exempt, plus temporary capped "SLICE-B" entries.

---

## 3. Build targets & dependencies

Conan 2 + CMake ≥3.31, C++23. Depends on `aeron` (embedded C driver `aeronmd`
+ C++ client) and `spdlog`; `protocol/` is header-only via the
include path. On macOS, build **x86_64** for Aeron binary-cache matching.

CMake splits the library into targets so the header-only `rune_client`
(`INTERFACE`) can be consumed without pulling Aeron unless needed:
`rune_client_transport`, `rune_client_agent`, `rune_client_producer`,
`rune_client_subscriber`, `rune_client_group`, `rune_client_api` (each guarded by
`aeron_FOUND`; Linux links `libuuid` for the embedded driver).

```bash
# x86_64/gnu17 profile matches the Aeron binary cache on macOS
conan install . -of build -s build_type=Debug -s arch=x86_64 -s compiler.cppstd=gnu17 --build=missing
cmake -S . -B build -DBUILD_TESTING=ON \
    -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake \
    -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"
cmake --build build -j2
cd build && ctest --output-on-failure
```

- Tests: `tests/` (unit + integration; a `build_tsan/` config exists for the
  ThreadSanitizer run — the single-writer discipline must hold under TSan).
- Examples (`-DBUILD_EXAMPLES=ON`, `examples/`): `rune_client_ping`,
  `rune_produce_subscribe`, `dr_client` (used by the broker's dockerized DR/E2E
  suite). Benchmarks (`-DBUILD_BENCHMARKS=ON`): ping / produce / group.
- `docker/` holds the client's image for the cross-repo DR/E2E harness.
- **Never** run `conan create/export/upload` locally — CI publishes packages.

---

## 4. Engineering law

The client obeys the same law as the broker: single-writer agents under `doWork`/`IdleStrategy`, no hot-path locks or
allocation, Aeron ring buffers for cross-thread hand-off, injected advanceable clocks in tests (advance-and-tick, hard
iteration caps, no wall-clock waits), `IdleStrategy` back-off when idle. The full statement and the 16-point
philosophy are in the root [CLAUDE.md](../CLAUDE.md).
