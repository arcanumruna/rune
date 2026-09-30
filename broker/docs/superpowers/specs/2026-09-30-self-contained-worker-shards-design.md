# Self-contained worker shards (broker) + sharded invoker client (rune-client-cpp)

**Date:** 2026-09-30
**Status:** design approved in conversation; awaiting written-spec review
**Repos:** rune-broker (primary), rune-client-cpp, rune-protocol (one additive schema field)
**Related:** `2026-09-30-per-worker-replication-transport-design.md` (#4, per-worker replication planes — this builds on it), Milestone E spec (thread-per-core, shared-nothing), memories `rune-targets-100k-100us`, `rune-idle-churn-cpu-starvation`, `aeron-is-the-bible`, `rune-shared-nothing-law`.

## 1. Goal and targets

Rune is an ultra-low-latency durable log. It must beat Kafka on latency and at least match it on throughput.

**Pass criterion (single run, measured together):** on the 4-core k3s-node-01 box, 3 brokers + 1 client co-located, 1 KB records, 3-node acks=all with the **replicated, no-fsync-before-ack** guarantee (Rune `tier=majority`, `durability=all-in-sync` — the same guarantee as Kafka acks=all with default flushing), an **open-loop producer offering 100,000 rec/s for 60 s achieves ≥ 100,000 rec/s and p99 end-to-end ≤ 100 µs** (every record measured, coordinated-omission-safe).

Out of scope: fsync-before-ack (Strict tier) latency (bounded by the disk, ~2 ms p50 here); more than one shard worker per node on this 4-core box; consumer/read path.

## 1a. Binding principles (apply to every task, broker and client)

These are standing laws for all Rune work; any task, review or plan step that violates one is defective.

1. **Shared-nothing.** Every piece of mutable state is owned by exactly one thread. No shared mutable state, no locks/mutex/condvar, no atomics as cross-thread coordination (atomics only inside ring-buffer primitives), no `thread_local` on hot paths.
2. **Ring buffers are the only cross-thread channel.** Aeron/Agrona rings (OneToOne where single-producer, ManyToOne only where genuinely multi-producer) — never shared reads or cross-thread callbacks.
3. **Aeron is the bible.** Mirror Aeron's patterns exactly: single-writer agents with `doWork(nowNs)` under an `IdleStrategy`, invoker modes, `tryClaim`/`BufferClaim` zero-copy, flat SBE messages with reused encoders, correct release/acquire ordering, one injected clock sampled once per cycle. Busy-spin only on a dedicated pinned core; an agent with no work never burns a shared core.
4. **Rich-state state machines.** Each state holds exactly the variables it needs (e.g. `Resolving(pubRegId, deadlineNs)`, `Ready(pub)`); every edge case is one explicit state with one code path — no scattered booleans. Reference: `FollowerSync`.
5. **Zero allocation, no virtual calls on hot paths.** No heap allocation, `std::function`, node-based containers (`std::map`/`unordered_map`/`std::string`) or vtable dispatch on the per-record/per-batch/per-duty-cycle path. Polymorphism via concrete types + a construction-time branch, templates, or tagged unions.
6. **Cache-friendly, compact data structures;** every optimisation proven by a before/after benchmark with no correctness or p99 regression — no speculative optimisation.
7. **Test everything; regression test on every fix** (failing repro test first, marked `*Regression*`, regression suite run first). "Done" means the real end-to-end path is green (real client ↔ real broker over real transport) **in the default configuration as well as flag-on**; shortcut/in-process tests alone never count.
8. **Tests never spin to pass time:** advance an injected clock to the next deadline; every drive loop has a hard iteration cap and fails instead of hanging.
9. **Time-independent liveness:** control-plane convergence never depends on a tuned timeout out-racing jitter.
10. **Benchmark gate:** any p99 > 100 µs is a stop-and-fix, not a result. Perf claims are proven with interleaved A/B on a verified-idle box, citing the exact code path; measurements are validated (same binary, complete captures, run succeeded, feature actually active) before any explanation is drawn from them.
11. **Never hardware to mask software:** a bad number on this box is a software bug to root-cause and fix.
12. **Fix on sight:** code found violating these while doing the work is fixed as part of the work, not noted.

## 2. Why the current design cannot meet it (measured 2026-09-30)

- Unloaded (1 record in flight) latency is **p50 4.2 ms / p99 9.8 ms**; leader append→commit alone is p50 3.0 ms.
- A record crosses ~10 threads (client agent → client driver → broker receiver → leader worker → broker sender → follower receiver → follower worker → back → leader worker → runtime/gateway → egress → client). Each hop costs a parked-thread wake-up (50 µs–1 ms with the idle shapes that were tuned for throughput).
- ~20 hot threads share 4 cores (per broker: Aeron receiver/sender/conductor, Aeron client conductor, runtime, worker; client: agent, driver, conductor). Per-thread core pinning alone changed nothing (41k rec/s, p50 4.46 ms) because pinned threads sharing a core still sleep and wake; only 4 threads can busy-spin.
- A single fused per-node thread (pre-Milestone-E) had low latency but low throughput; throughput came from sharding partitions across workers. The design must keep sharding and remove the hops inside each shard.

## 3. Architecture

### 3.1 Broker worker shard (`RUNE_WORKER_MODE=shard`, default `legacy`)

Each worker becomes a self-contained shard:

- **Owns:** its own embedded Aeron media driver in **invoker mode** (no driver threads; Aeron dir on tmpfs, `<aeronDir>/w<w>`), its own Aeron client with `useConductorAgentInvoker(true)`, and the partitions `ShardRouter` assigns it (unchanged assignment).
- **Runs:** one thread, pinned (`RUNE_WORKER_CORE_BASE + w`, existing mechanism), **busy-spin idle, never parks**. Each duty-cycle pass, in order:
  1. driver duty cycle (`aeron_driver_main_do_work`: receiver + sender + conductor);
  2. Aeron client conductor invoker;
  3. poll its client-ingress subscription → append to its partitions (existing `pollWorkerIngress_` / `handleProduceBatch`);
  4. replication on its own plane (#4): push, apply, fast-ack, HW advance;
  5. fire due produce acks directly to clients (§3.4);
  6. reap io_uring durability completions.
- **Never blocks.** Follower flush (`fsync` + `msync`, `PartitionReplica.cpp` follower flush) and the worker's time-based `syncOwnedLogs_` are submitted through the worker's existing io_uring flush backend (`UringFlushBackend`; completions are matched by token, so an unreferenced flush token can never fire a produce ack). No synchronous disk call remains on the loop.

### 3.2 Runtime placement (`RUNE_RUNTIME_PLACEMENT=thread|fused`, default `thread`)

Runtime = consensus, metadata, internal (topicId < 0) partitions, gateway control (Connect/Metadata), admin control hook.

- `thread`: own thread with its own small driver; may park; not on the produce or ack path.
- `fused`: its duty cycle is called inside worker 0's loop (one thread per node); it keeps its own driver + Aeron client, pumped by that loop. Required on this box so nothing preempts a spinning worker.
- Runtime ↔ worker traffic stays on the existing SPSC rings (ownership changes, routing image, session announcements, post-ack notifications). When fused, producer and consumer run sequentially on one thread; no path may wait for the other side to drain (§3.6).

Cleaner and admin threads stay as parked background threads (cold path).

### 3.3 Endpoints and addressing

Every per-worker port is explicit configuration, validated for collisions at startup against every port the node binds, and advertised. No node derives another node's ports.

| Traffic | Endpoint | Stream | Config |
|---|---|---|---|
| Client → worker w (produce) | `host:(RUNE_WORKER_INGRESS_BASE_PORT + w)` | `workerIngressBaseStreamId + w` (unchanged) | new `RUNE_WORKER_INGRESS_BASE_PORT` |
| Worker w ↔ worker w on peer (replication) | `peerHost:(peer.replBasePort + w)` | `replBaseStreamId + w` (#4) | new `RUNE_WORKER_REPL_BASE_PORT`; `RUNE_PEERS` entries gain optional per-peer `replBasePort` |
| Worker w → client (records, MDC) | existing `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT + w` | unchanged | unchanged |
| Worker w → client (produce acks) | client's session egress channel (§3.4) | client's egress stream | from Connect |
| Client ↔ runtime (Connect, Metadata), consensus, internal replication | node's existing ports | unchanged | unchanged |

- Shard mode with missing/invalid per-worker ports, missing peer `replBasePort`, or asymmetric worker counts → **hard startup refusal** (no silent fallback). Symmetric worker count is already enforced by #4.
- **rune-protocol:** `MetadataResponse` per-partition entry gains an additive **ingress endpoint (host:port)** field; older clients ignore it. The client already routes per partition (`setRoute`) and uses the advertised worker endpoint instead of the node endpoint.
- **Harness:** `cluster.sh` assigns node i distinct ranges (e.g. ingress `40500 + 20·i`, replication `40600 + 20·i`) and fills `RUNE_PEERS`.

### 3.4 Produce and ack path

- Today: worker appends → (acks=all: waits for HW on the worker) → `ret_` ring → runtime `drainWorkerReturns_` (PSM mirror update, LSO tracking, encode) → session publication.
- Shard mode: **the worker encodes the ProduceResponse and offers it itself** on its own exclusive publication to that client session. Acks due in the same pass for the same session are coalesced into one frame (existing #23 behaviour, moved to the worker).
- **Session announcement:** on Connect the runtime hands the session's reply destination (egress channel/stream, session id) to every worker on the node over the existing runtime→worker ring; each worker lazily opens its own exclusive publication (single writer).
- **Idempotent-producer state:** the worker's per-partition `psm_` is already authoritative on the per-worker path (`Worker.h` `handleProduceBatch(…, psm_, …)`); the gateway's `producerStateManager_` is a mirror. In shard mode the gateway mirror is not consulted for user partitions; the worker `psm_` is the single owner.
- **Off the ack path, after the ack is sent:** a fire-and-forget notification on the worker→runtime ring for transaction LSO tracking (`LsoTracker::onProduceBatch`) and node bookkeeping. If the ring is full the notification is retried next pass; it is never dropped and never delays an ack.
- **Back-pressure (G-A6, unchanged guarantee):** a back-pressured ack goes to the worker's ack-overflow queue; intake from that client pauses until it drains; no ack is ever dropped. acks=all timeouts unchanged (Internal after deadline).
- Client protocol unchanged (same ProduceResponse, same correlation ids).

### 3.5 Replication traffic fixes (permanent)

1. **No re-send of pushed data.** The follower's `FollowerSync` keeps a single-outstanding data fetch whenever its LEO < leader LEO (always under load) and the conflation guard (same fetchOffset **and** same leader LEO) never fires, so leaders sent each follower ~2× its ingest. New rule, per follower, progress-based: while that follower's fetch offset keeps advancing and the requested offset is below `lastPushedOffset_`, answer ack-only (HW, no data); once it has asked for the same offset for ≥ `kFetchResendNs` (20 ms), serve data (lost-push recovery). Measured (env-gated experiment): +10%, no stall; a global-recency variant stalled (p99 3.3 s) and is rejected.
2. **Fast-acks unicast to the leader.** `WorkerReplicationTransport::sendFetchRequest` currently broadcasts to every peer; the replica knows its leader — send to it only.
3. The per-follower progress tracker for rule 1 is a fixed-size flat array indexed by the follower's replica-set slot (no `unordered_map`; the env-gated experiment's map is replaced).

### 3.5a Fix-on-sight items found while designing (principles §1a.5)

1. **Virtual call on the hot path:** `PartitionReplica` calls `transport_->sendFetchResponse(...)` / `sendFetchRequest(...)` through the virtual `PartitionTransport` interface on every push/ack; #4's `WorkerReplicationTransport` overrides it. De-virtualise: the replica holds concrete transport pointers (per-worker vs legacy) chosen at construction and branches on a construction-time tag (both inlinable, branch perfectly predicted).
2. **Allocation + copy on every push and data fetch:** `pushToFollowers` and `onFetchRequest` build `std::vector<uint8_t> logBytes = L.read(...)` per call. Replace with a zero-copy read view over the log (`Log::readView`, already used by the read path) encoded straight into the publication claim.
3. **Worker-shard startup, route resolution, session announcement and ack overflow** are designed as rich-state state machines (e.g. publication `Resolving(regId, deadlineNs) → Ready(pub) → Failed(reason)`), not flag combinations.

### 3.6 Invoker-mode correctness rules

- Every "add publication/subscription, then sleep until found" loop (broker: `WorkerIngress.h`, `WorkerEgress.h`, `WorkerReplicationTransport.h`, `UdpMessageSink.h`; client: `AeronTransport` resolve/retarget/egress-subscription paths) becomes "add, then pump **this shard's own** driver + conductor until found or deadline".
- A driver is driven by exactly one thread. This fixes an existing race: today the startup readiness loop calls `aeron_driver_main_do_work` from the caller thread while the driver thread is already running it (broker `BrokerNode.cpp` startEmbeddedDriver readiness loop; client `AeronTransport.cpp` startup) — a plausible cause of the shared-driver stall seen 2026-09-30.
- No loop waits for another component on the same thread to make progress (e.g. worker skipping intake while `ret_` is full is a non-blocking skip, not a wait — keep it that way; when fused, the runtime drain runs in the same pass).

## 4. rune-client-cpp

### 4.1 Client shards

- `ClientShard`: one thread owning a subset of partitions, with its own embedded driver in invoker mode (tmpfs dir, **explicit MTU, default 8 KB**, configurable), its own Aeron client with conductor invoker, its partitions' batch builders, publications to whichever worker endpoints they map to, its ack subscription, and its pending-ack table. Everything inside a shard is single-threaded: no locks, no atomics, no cross-thread hops.
- **Shard count = client cores given** (config, default 1); independent of broker worker count. Deterministic partition → shard mapping.
- **Invoker mode (new, opt-in):** the app thread is the shard — `send()` then the shard duty cycle in the app's own loop. N producer threads → N shards.
- **Agent mode (existing default, kept):** a background thread per shard; app threads hand records over through a per-shard single-producer ring (one hop); any app thread may `send()` to any partition (routed to the owning shard).

### 4.2 Batching without linger

In invoker mode, records sent during one pass accumulate; at the end of the pass each partition's pending records go out as one frame, subject to its in-flight limit. Batch size tracks load (≈1 record at low rate, dozens at 100k/s); no timer. Linger remains an agent-mode option; default 0 in invoker mode.

### 4.3 Hot-path fixes (both modes)

| Today | Change |
|---|---|
| `getenv("RUNE_MN_DEBUG")`, string concat, string-keyed pub lookup on every routed offer (`ClientAgent::offerProduceToLeader_`, `AeronTransport` endpoint cache) | resolved once per route into a per-partition publication pointer |
| `std::function` SendFn rebuilt each `doWork`, copied into every `flushBatches` call | plain member call |
| heap copy of each batch frame (`BatchInfo.wireFrame`) + `std::map` batch table | fixed-size per-partition ring of in-flight batch slots, reused, no allocation |
| per-record `std::map<int64_t,PendingAck>` + `ackMtx_` fan-out on ack | per-partition flat ring indexed by batch; new per-batch callback `(baseOffset, count)`; per-record callback API kept, implemented on top |
| `void Producer::send` silently drops on a full ring | returns/propagates back-pressure; the void overload backs off and retries, never drops |
| driver `sleep-ns` idle, default 1408 MTU, startup two-thread driver pumping | invoker (app-owned loop) or configurable backoff; explicit MTU; single driving thread |

### 4.4 Acks

The shard's ack subscription receives ProduceResponses directly from workers (§3.4); each is matched to its in-flight batch slot by correlation id via direct index (O(1)) and fires the batch callback.

## 5. Benchmark

`rune_produce_throughput` rewritten on client invoker mode:

- **Open-loop fixed rate** (`--rate N`); each record's latency is measured from its **scheduled** send time (coordinated-omission-safe).
- Every record recorded in an HdrHistogram: p50/p99/p99.9/max; no sampling.
- Secondary saturation mode (max throughput, large window).
- Every run verifies the box is idle first and reports core layout + per-thread CPU split.
- VM layout for the pass run: client shard pinned to core 0, broker i (shard worker 0, runtime fused) pinned to core i+1.

## 6. Validation ladder

1. Unit/regression (marked `*Regression*`): port/config validation + hard refusal; shard routing; invoker pumping (no deadlock with no background threads); single-thread driver driving; dup-fetch gate (advancing follower → ack-only; stuck ≥ 20 ms → data); fast-ack unicast; worker-side ProduceResponse encode + coalescing + overflow; client flat ack ring; `send()` never drops.
2. In-process integration + failover with shard mode on: leader kill, epoch change, lost push, rolling restart, client reconnect.
3. Dockerized DR matrix with shard mode on, plus TSan.
4. VM runs, shard mode on and off (off must equal today's behaviour).
5. §1 pass criterion on a verified-idle box.

## 7. Rollout

Broker `RUNE_WORKER_MODE=shard` / `RUNE_RUNTIME_PLACEMENT=fused` and client invoker/shard options are **default off** until the DR matrix is green on the shard path; defaults unchanged. Already-done fixes from 2026-09-30 (broker honours `RUNE_AERON_DIR`; bench-client window wait backoff) are committed in the first plan step.

## 8. Risks

- Consensus heartbeats/elections run on worker 0's loop when fused: any blocking call on that loop becomes a liveness risk → the no-blocking rule (§3.1, §3.6) is enforced by tests and by a loop-duration FR span with an alarm threshold.
- One driver per worker increases memory (term buffers) and ports; term size is config, validated against available tmpfs.
- Loopback UDP kernel cost (~20% of CPU at 45k rec/s today) remains; if the pass criterion is missed on it, the next lever is fewer datagrams per record (per-peer multiplexing, lever #1), not hardware.
