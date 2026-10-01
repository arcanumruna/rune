# rune-client-cpp — architecture & working agreement

> **What this is:** the standalone, independently-releasable **C++23 Rune client
> library** — producer, subscriber, and consumer-group member — that downstream
> apps (e.g. world-server) link to talk to the `rune-broker` cluster over Aeron.
> It shares the wire/record formats with the broker via the header-only
> `rune-protocol` package (consumed at `../rune-protocol/include`).

The design lives in
`../rune-broker/docs/superpowers/specs/2026-09-11-rune-client-and-broker-runtime-design.md`
and the milestone-acceptance notes under `../rune-broker/docs/superpowers/`.

---

## 1. The one idea: a single-writer agent behind a public façade

The client is built the same way as the broker: **one background runner thread**
loops a single-writer `doWork(nowNs)` duty cycle under an `IdleStrategy`. App
threads never touch Aeron, the session, or the transport directly — they hand
work to the agent through a **lock-free `ManyToOneRingBuffer`** and get results
back via futures/callbacks that fire **on the runner thread**.

```
  APP THREADS                         RUNNER (agent) THREAD  — single writer
  ───────────                         ─────────────────────────────────────
  Client::connect/ping                ClientAgent::doWork(nowNs):
  Producer::send/flush   ─┐             1. drain command ring (bounded)
  Subscription (un)sub    │  ManyToOne  2. transport_.poll() egress fragments
  GroupConsumer::commit  ─┘   ring ───► 3. route egress → session / hubs
                                        4. session_.tick(nowNs) (timeouts,
                                           keepalive, redirect/retry)
   futures/callbacks  ◄────────────────  resolved/rejected here (NOT app thread)
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
| Support | `Key`, `TopicHandle`, `codec/{Codec,DataCodec}`, `Version` | Keys, topic ids, codec adapters over `rune-protocol`. |

### 2.1 Producer (`producer/`)
Tier-1 producer with a **zero per-send heap allocation** fast path. App threads
call `send()`/`flush()` → write a compact `SEND_RECORD` into the ring (the
alloc-free handoff). The agent drains, accumulates records into per-
`(topicId,partition)` `BatchBuilder`s (RecordBatch v2), and flushes a batch when
**linger elapsed OR `batchBytes` reached OR `flush()` requested**.
- `Acks`: `None` (fire-and-forget; fully alloc-free end-to-end), `Leader`
  (default; awaits `ProduceResponse`), `All` (treated as Leader for now).
- Per-send heap is allowed **only** on the opt-in callback/future ack paths,
  never on the void path.
- Idempotence: each partition stamps a monotonic `baseSequence` into the batch
  header (`producerId` is a local stub pending real `InitProducerId`).
- `ProducerHub` registers producers with the agent via an atomic COW snapshot —
  no lock on the runner hot path; `onProduceResponse` routes acks back.

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
dropped, logged and counted. Frames over the transport's max message length are
rejected synchronously by `submitSend()`. A frame of a session that no longer
exists is dropped, counted, logged rate-limited. Subscribe (adoption + reconnect
resend), Unsubscribe, OffsetCommit and LeaveGroup bypass the ring (duty-thread
direct send on the default ingress publication, retried every duty cycle).

### 2.3 Group consumer (`group/`)
`GroupConsumer` runs the full join/sync/stable/rejoin state machine, heartbeat,
record routing, and the auto-commit duty cycle **on the runner thread**. App
threads only register `onAssigned`/`onRevoked`/`onRecords` (setup) and call
`commit()`, `commitSync()` (returns a future the duty thread resolves on
`OffsetCommitResponse`), and `close()` — each a `HubCommand` on the agent ring
(F8a: no atomics, no mutex). `subscribeGroup()` registers through the ring too
(`GroupAdd`); the `GroupConsumerHub` list is duty-thread owned.

The no-atomics/no-mutex law is enforced by `scripts/check_no_atomics_or_mutexes.py`
(ctest `NoAtomicsOrMutexesOutsideAllowlistRegression`): only `M2ORingBuffer`'s own
memory ordering is exempt, plus temporary capped "SLICE-B" entries.

---

## 3. Build targets & dependencies

Conan 2 + CMake ≥3.31, C++23. Depends on `aeron` (embedded C driver `aeronmd`
+ C++ client) and `spdlog`; `rune-protocol` is header-only via the sibling
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

## 4. Engineering law — Aeron/Agrona is the bible

This client obeys the same non-negotiable law as the broker: single-writer
agents under `doWork`/`IdleStrategy`, no hot-path locks or allocation, lock-free
ring buffers for cross-thread handoff, injected advanceable clocks in tests
(advance-and-tick, hard iteration caps, no wall-clock waits, no `while(cond)
doWork()`), and `IdleStrategy` back-off when idle (never a bare spin). The full
statement is in `../rune-broker/CLAUDE.md` §7 — read it and mirror Aeron's C++
client exactly.

---

## 5. Session workflow — keep the live dashboard updated

Rune streams its development **live** — https://rune.regressorspath.com (fed by
the `rune-live` server). **Every session must keep it updated.**

1. **Tickets — Planka (`planka` MCP).** Board **"Rune / Roadmap"**
   (id `1861164569347490826`): Backlog · In Progress · In Review · Done. Create
   a card before starting; move it as you progress.
2. **Chat:** `scripts/rune-live.sh chat plan "..."` (kinds `plan`/`thought`/`status`).
   (The helper lives in the `rune-broker` / `rune-live` repos.)
3. **Tests/benchmarks:** push results with a *stable* suite name as first arg
   (e.g. `rune-client/produce`) so the dashboard folds re-runs by it:
   ```
   rune-live.sh test "rune-client/produce" passed \
     passed=N failed=0 total=N commit=$(git rev-parse --short HEAD)
   ```
   Override `RUNE_PUSH_TOKEN` / `RUNE_LIVE_URL` as needed; formats in
   `rune-live/README.md`.

---

## Engineering Philosophy (HARD RULES — apply everywhere, always)

These are non-negotiable across every Rune repo. Check every design and every fix against all eight:

1. **Shared-nothing.** No shared mutable state, no locks, no atomics-as-coordination, no `thread_local`. Each thread/component owns its data outright.
2. **Ring buffers are the ONLY cross-thread event passing.** All inter-thread / inter-component communication goes through ring buffers (Aeron-style) — never shared memory or cross-thread callbacks.
3. **State machines where each STATE holds exactly the variables that state needs** — not scattered booleans. e.g. `PUBLISHED(buffer, destination, publisher, counter)`, `BEHIND(...)`, `INSYNC(...)`. Every edge case = one explicit state with one code path.
4. **Zero allocations.** No heap allocation on hot paths or in steady state — preallocate, reuse buffers (capacity persists), fixed stack buffers; no per-cycle `std::vector` returns / `std::function` / dynamic containers on the fast path.
5. **Test everything, benchmark everything.** Prove features via real e2e/integration + benchmarks. No hand-waving; no "done" without the real path green.
6. **Regression test on EVERY fix**, marked as a regression suite (e.g. `*Regression*`) so they run together.
7. **On every fix/implementation, run the FULL regression suite FIRST (priority)** — then all other tests. The regression suite is the gate before anything else.
8. **Fix-on-sight.** If while reading any file you find code that violates this philosophy (shared state, cross-thread callback, scattered flags, hot-path allocation, untested/un-regressed fix), you FIX it — don't just note it.
