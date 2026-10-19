# Rune Thread-Per-Core Partition-Sharded Runtime — Design

**Status:** design (approved in brainstorming 2026-09-23; next → writing-plans).
E1 + E3 landed; E5 (throughput / ingress scale-out) added 2026-09-24 — see that section.
**Milestone:** E (follows Milestone D — distributed partition leadership)
**Author dialogue:** brainstormed with the user 2026-09-23; E5 added 2026-09-24.

## Goal

Replace Rune's current **single-`AgentRunner`-per-node** runtime (one duty thread
services every partition sequentially) with a **shared-nothing, thread-per-core,
partition-sharded worker pool**: each partition is owned exclusively by one
affinitized, busy-polling worker, with **no mutexes and nothing that involves the OS
scheduler on the hot path**. This is the model behind Redpanda (Kafka-compatible, on
seastar) and ScyllaDB, and it is Rune's path to maximal throughput + lowest,
jitter-free latency (`<100µs` p99, target ~tens of µs), scaling ~linearly with cores.

## Non-goals / scope

- Milestone D (distributed partition leadership) is **done and proven** (dynamic
  rebalancing, position/lineage failover, deterministic control tick, restart-durability
  trio, memory-topic tier) except its close-out (native/Linux bench, DR-E2E, merge). This
  milestone does NOT change D's *semantics* — it changes the *runtime that executes them*.
- The **memory-topic `InMemoryLog` redo** (from a `std::vector`-memmove ring to a proper
  single-owner ring) folds into THIS milestone — its correct form (a plain single-owner
  ring, no atomics, back-pressure-when-full) is defined by this runtime model, not by a
  Phase-2 patch.
- Not changing the wire protocol semantics, the MSM/consensus design, or the
  partition-replica replication/ISR/HW logic — those are re-hosted, not rewritten.
- **Phase-2 close-out sequencing (open, to confirm before/at plan time):** either (a)
  merge Milestone D to local main now with the memmove `InMemoryLog` flagged as a
  placeholder replaced here, or (b) hold D's merge until E redoes the ring. Recommend (a):
  D's mechanism is proven; the ring is an isolated internal component.

## Core principles (locked in brainstorming)

1. **Shared-nothing thread-per-core.** A partition's hot path (`append → replicate →
   ack`, `fetch`, `subscribe-push`) has **zero cross-thread synchronization** — no locks,
   no atomics on per-partition state, cache-hot, NUMA-local. Cross-thread work happens
   ONLY at explicit, rare boundaries, always lock-free (SPSC).
2. **Ownership unit = the co-partition shard, not the individual partition.**
   `worker = f(coPartitionGroup, partitionIndex)`. Every partition sharing an aggregate-root
   key (`hash(userId) → index N` across all co-partitioned topics) is owned by the **same
   worker** → a whole aggregate root (a user + all its characters + referencing topics) is
   processed by **one core, single-threaded** → local ACID + local joins/views with zero
   cross-core sync. This extends Milestone-D co-location (group → same *node*) down to
   group → same *worker*, and is the thread-level foundation of the future DB-face.
3. **Two-level assignment, both deterministic.** MSM assigns shard → **node** (Milestone D;
   co-groups already co-locate on a node). Each node then maps shard → **worker** by
   `hash(coPartitionGroup, partitionIndex) % workerCount`. Composes cleanly with D's
   dynamic reassignment.
4. **Per-worker ingress (topology (i)).** Each worker owns its own Aeron ingress
   stream; a client is routed by `key → shard → worker` directly to the owning worker
   (extend the D metadata/REDIRECT response to carry the per-worker stream), so a keyed
   request lands on its owner with **zero cross-thread hops** on the hot path.
5. **No OS scheduler on the hot path.** Workers are pinned (CPU affinity), busy-poll,
   and **never block** — no blocking syscalls, no futex/condvar/mutex. Any blocking I/O is
   done by the kernel via `io_uring` and its completion is **polled from a shared-memory
   ring** (no userspace thread wake). Rides `isolcpus`/`nohz_full`/IRQ-affinity so nothing
   preempts a worker core.

## Durability model (the invariant that makes async I/O safe)

**Invariant: `acked ⟹ durable`. A producer is never told a write is safe until the
promised condition actually holds.** Async I/O changes only *whether the worker thread
idles* while durability is achieved — never *whether the condition is checked before the
ack*. Two tiers (per-topic knob, same shape as D's `majority`/`all-in-sync`):

- **Replication durability (default, Kafka model — what Rune does today):** ack condition
  = record in the logs of a majority of in-sync replicas + HW advanced. Survives any
  single node crash (another in-sync replica has it). Local disk `fsync` is a **background
  group-commit**, NOT on the ack path — so the worker never touches `fsync` on the hot
  path. Full-cluster power loss is the accepted residual (data was in page-cache).
- **`fsync`-before-ack (strict tier, survives power loss):** ack condition additionally
  = the record's `fsync` completion. The ack is gated on the **completion event**, not on
  the worker blocking.

The **deferred-ack queue already in Rune** (`acks=All` today enqueues a pending-ack and
fires it a cycle later once HW advances) generalizes directly: a strict-tier pending-ack
is keyed on the **flush completion token** and fired when that completion is reaped. On
`fsync` failure the completion carries the error → the pending-ack is errored (never
acked) and the shard is marked unhealthy.

## Async durability I/O — the scheduler-free hand-off

Production (Linux) uses **`io_uring`**, no extra userspace thread:
- Worker writes an `IORING_OP_WRITE` / `IORING_OP_FSYNC` **SQE into the mmap'd submission
  ring** (a memory write). With **SQPOLL**, a pinned kernel-side poller consumes
  submissions → the worker makes **zero syscalls** to submit.
- The **kernel** performs the I/O and **posts the CQE into the mmap'd completion ring**.
- The worker **reaps the CQE with a plain memory read** in its busy-poll cycle → maps the
  completion token to the pending-ack → fires the ack (or errors). No futex, no userspace
  wake, no scheduler on the worker's path. `IOPOLL` (polled completions) removes interrupt
  latency for fast NVMe.
- Group-commit: coalesce many pending flushes per file into one `fsync` SQE (throughput).
- Back-pressure, not blocking: if the submission ring is full, the worker **holds new
  durable produces** (same discipline as the memory-topic ring) rather than blocking.

Portability: `io_uring` is Linux-only (production target). Non-Linux dev (macOS) uses a
portable **dedicated-flusher-thread + SPSC** fallback — latency there is non-authoritative
(and already Rosetta-inflated), so its scheduler wake is irrelevant. The worker-facing
hand-off (SPSC submit ring + completion token) is identical either way; only the flusher's
internals differ.

## Components (isolated, well-bounded)

- **Built on Aeron `Agent`/`AgentRunner` + configurable `IdleStrategy` (LOCKED — do NOT
  hand-roll thread/loop machinery).** Rune already has `rune::consensus::Agent`
  (`virtual int doWork(int64_t nowNs)=0`, faithful Agrona port) + `rune::consensus::AgentRunner`
  (`AgentRunner(Agent&, IdleStrategy&, Clock)`) and already runs `NodeRuntimeAgent` and
  `LogCleanerAgent` each on their own `AgentRunner` with a configurable idle strategy. Workers
  reuse this exactly. **Idle strategy: default `BackoffIdleStrategy`** (spin → yield → park with
  growing backoff — does NOT peg a core); per-worker configurable (an operator may opt a
  dedicated isolated core into `BusySpinIdleStrategy`). Never a bare busy-spin default.
- **`WorkerPool`** — owns N `Worker`s; assigns shards; lifecycle. **E1:** drives the workers
  cooperatively from the existing single `AgentRunner` (no new threads — workers are Agents but
  invoked in-line). **E2+:** each `Worker` gets its OWN `rune::consensus::AgentRunner` with its
  own configurable `IdleStrategy` (default `Backoff`), pinned via affinity in E4 — mirroring how
  `LogCleanerAgent` runs today. N = (isolated cores − reserved), configurable.
- **`Worker` (partition-worker agent)** — implements `rune::consensus::Agent`
  (`int doWork(int64_t nowNs) override`); owns a set of `Shard`s.
  `doWork` run-to-completion over owned shards each cycle: poll own ingress →
  dispatch produce/fetch to the shard's `PartitionReplica` → reap `io_uring` completions →
  fire due deferred-acks → subscribe-push → poll cross-worker mailbox. Single-threaded; no
  locks; no shared mutable state with other workers. The one `Agent::doWork` dispatch is
  once-per-duty-cycle (the Aeron model) — NOT a per-record hot-path virtual, so it complies with
  [[rune-no-virtual-calls-hot-path]] (the per-record path inside stays concrete).
- **`Shard`** — `(coPartitionGroup, partitionIndex)` → the set of `PartitionReplica`s for
  that shard's partitions across the group's topics. Owned by exactly one `Worker`.
- **`ShardRouter`** — pure deterministic functions: `key → (group,index) → shardId` and
  `shardId → workerId` (`hash % workerCount`); `shardId → node` from the MSM committed map
  (Milestone D). No per-op allocation.
- **`WorkerIngress`** — per-worker Aeron subscription/stream; clients routed here by key.
- **`IoBackend`** — per-worker `io_uring` wrapper (submit write/fsync SQE, reap CQE, SQPOLL/
  IOPOLL) with the portable flusher+SPSC fallback behind the same interface.
- **`ControlAgent`** — consensus (Raft) + MSM apply + `DynamicBalancer` + membership +
  deterministic control tick + assignment/routing publish. Runs on **reserved core(s)**,
  NOT a partition worker. Publishes the shard→node/worker map via the D `RoutingImage`
  (extended with `workerId`), consumed lock-free (release/acquire swap) by workers.
- **`CrossWorkerMailbox`** — per directed worker→worker SPSC ring for the rare cross-shard
  op (cross-aggregate txn = saga; deferred/rare). Most ops are single-shard.
- **`AffinityConfig`** — core allocation: worker cores (pinned, isolated), reserved cores
  for `ControlAgent` + Aeron media driver + `io_uring` SQPOLL; `isolcpus`/`nohz_full`/IRQ
  guidance.

## Data flow (produce, disk topic, replication-durability default)

1. Client (knows leader node+worker from metadata) sends produce for key K to the owning
   worker's ingress stream (zero-hop).
2. Worker decodes, resolves shard (K → group,index), appends to the shard's
   `PartitionReplica` log (page-cache/mmap; no `fsync`), replicates (push-on-append to ISR).
3. Worker enqueues a pending-ack; submits the background write/flush SQE (off the ack path).
4. Worker continues other shards. On a later cycle, HW advances (majority replication) →
   the deferred-ack fires. (`fsync` completion drives the ack only for the strict tier.)
5. Followers' replicas for the same shard live on their nodes' corresponding workers (same
   `shard→worker` map) → replication is worker↔worker across nodes over Aeron.

## Memory-topic log under this model

A memory-topic partition is owned by exactly one worker → **no cross-thread access** → its
log is a **plain single-owner ring** (no atomics, no fences) — the fastest correct form.
Bounded, **back-pressure when full** (per the 2026-09-23 decision: full → produce
back-pressures, never overwrites un-reclaimed data), reclaim floor = `min`(replication-safe/
HW, live reader positions). This is the `InMemoryLog` redo, now trivially correct because
single-ownership removes the concurrency the memmove-vector version was (badly) coping with.

## Error handling / edge cases

- **`fsync`/write failure** → completion carries error → pending-ack errored (never acked)
  → shard marked unhealthy / retried. No false ack.
- **Submission ring / mailbox full** → back-pressure (hold new durable produces / defer the
  cross-worker send), never block.
- **Shard migration** (D dynamic reassignment, or worker-count change): ownership transfer
  is a single-writer control-plane publish — the losing worker drains in-flight for the
  shard and stops owning it; the gaining worker begins on the next committed map. No shared
  lock; the shard is never concurrently owned.
- **Core over-subscription** (workers > available isolated cores): fall back to
  non-affinitized cooperative scheduling with a logged warning — correctness holds, latency
  guarantees do not. Never silently.
- **Reserved cores**: `ControlAgent`, Aeron media driver, and `io_uring` SQPOLL run on
  housekeeping cores, never on worker cores.
- **Cross-shard op** (spans two workers): routed via the SPSC mailbox; the initiator holds
  a pending state and completes on the mailbox reply — no cross-worker locking.

## Testing

- **In-process N-worker harness** (extend the D `RebalanceFailover`/`MetadataReplication`
  patterns): all Milestone-D correctness tests must pass **unchanged** under the sharded
  runtime (parity), incl. no-committed-loss failover, co-location, dynamic rebalance.
- **TSan** on every cross-thread boundary (ingress→worker if any, cross-worker mailbox,
  routing-image publish/consume, `io_uring` completion reap) — and a structural assertion
  that per-shard state is touched by exactly one worker (shared-nothing proof).
- **`io_uring` durability tests (Linux):** ack-only-after-completion; `fsync`-failure →
  error-not-ack; group-commit coalescing; back-pressure on full submission ring; crash
  before completion → no acked-data loss.
- **Native/Linux benchmarks** (the authoritative `<100µs` p99 gate — macOS/Rosetta numbers
  are non-authoritative): produce/fetch p99 per tier (disk, memory), failover/rebalance/
  handoff, throughput scaling vs worker count, aggregate-root-local join latency.
- **Affinity/isolation smoke** (Linux): workers pinned, no cross-core migration, IRQs off
  worker cores.

## Migration from the single-`AgentRunner`

The existing code is **already single-writer per partition** (one agent touches each
partition serially) — so this is a *topology* change, not a rewrite of `PartitionReplica`/
gateway/replication. The migration re-hosts the existing per-shard `doWork` under N
affinitized agents instead of one:
1. Introduce `WorkerPool`/`Worker` wrapping the existing per-partition `doWork`; assign
   shards by the deterministic map; run N workers in-process (no affinity yet). Prove parity
   (all D tests green under N workers).
2. Add per-worker ingress + client routing-to-worker + cross-worker SPSC mailbox.
3. Add `io_uring` async durability + deferred-ack-on-completion + the memory-topic
   single-owner back-pressure ring (`InMemoryLog` redo).
4. Add CPU affinity + isolation + native/Linux benchmarks + DR-E2E under the sharded runtime.

## Phasing (for the implementation plan)

- **E1 — WorkerPool + shard→worker ownership + parity.** Shard the single agent into N
  in-process workers; deterministic `ShardRouter`; all D tests green under N workers.
- **E2 — Per-worker ingress + client worker-routing + cross-worker SPSC mailbox.**
- **E3 — `io_uring` async durability (Linux) + deferred-ack-on-completion + portable
  fallback + memory-topic single-owner back-pressure ring (`InMemoryLog` redo).**
- **E4 — CPU affinity + isolation (`isolcpus`/`nohz_full`/IRQ/SQPOLL pinning) + native/
  Linux benchmark matrix (`<100µs` gate) + DR-E2E under the sharded runtime.**
- **E5 — Throughput / ingress scale-out (added 2026-09-24).** Realize per-worker ingress
  (the unbuilt E2), add an Aeron `DEDICATED`-threading-mode knob, realize busy-spin +
  core-pinning (E4), and split consensus/coordinators off the client-gateway thread. See
  the "Throughput / ingress scale-out (E5)" section for motivation, evidence, and detail.

Each phase is independently testable and leaves the broker working.

## Throughput / ingress scale-out (E5 — added 2026-09-24)

**Motivation (evidence).** Live profiling on the 4-core Hetzner build agent (2026-09-24)
showed produce throughput capping at **~30k rec/s** (1 KiB records) vs **Kafka's 47.6k
unbatched / 79.5k batched** on the same box, while worker threads sat at **~33% CPU —
starved, not saturated.** The cause is structural, not a worker-parallelism bug: all
client ingress is serialized on a single thread *before* it reaches the parallel workers.
Workers do **not** own Aeron subscriptions — they are fed via SPSC rings by
`ClientGateway::…tryEnqueueOp` (`libs/gateway/src/ClientGateway.cpp:1477`), which runs on
the **one** `NodeRuntimeAgent` `AgentRunner` thread, fused with consensus + group/txn/
metadata coordinators (`libs/node/src/BrokerNode.cpp:1131`); and **all** network I/O
funnels through the single Aeron media-driver thread in `AERON_THREADING_MODE_SHARED`
(`libs/node/src/BrokerNode.cpp:1469`). Two single-thread funnels in series, in front of
the otherwise-parallel workers, cap throughput regardless of worker count. E5 removes
those funnels. (The excellent latency — p50 ~2 ms, 50–100× better than Kafka — comes from
the same single-writer simplicity; E5 must preserve it, not trade it away.)

1. **Per-worker ingress — realize the unbuilt E2.** Each worker owns the Aeron
   subscription/stream for *its* partitions and polls the wire directly; clients route
   `key → shard → worker` straight to the owning worker (extend the D metadata/REDIRECT
   response to carry the per-worker stream) — **no central gateway funnel, zero cross-thread
   hops on the hot path.** This is already Core Principle #4, phase **E2**, and the
   `WorkerIngress` component, but was **not built** (E1/E3 landed; ingress still goes through
   the one gateway thread). Feasible precisely because Milestone D gives per-partition
   leadership, so each worker is leader + ingress for its own partitions with **no global
   ordering thread**. **This is the single biggest structural fix.**

2. **Aeron `DEDICATED` threading mode on multi-core boxes.** Split conductor / sender /
   receiver into separate threads so network send+receive isn't one thread
   (`BrokerNode.cpp:1469` currently hard-codes `AERON_THREADING_MODE_SHARED`). SHARED is the
   correct low-resource choice for small/embedded boxes; make the mode a config knob and
   select `DEDICATED` when reserved cores are available. Pairs with reserved-core placement
   (Principle #5 / `AffinityConfig`).

3. **Busy-spin idle + core pinning, thread-count ≈ core-count — realize E4.** Replace the
   per-hop sleep/backoff latency with `BusySpinIdleStrategy` on pinned, isolated worker
   cores, and size the pool so threads do not oversubscribe cores. Already Principle #5 and
   phase E4, but only viable on right-sized hardware — the 4-core test box (6+ threads) is
   oversubscribed, a large part of its ~30k cap. Keep `Backoff` the default; busy-spin is
   opt-in per isolated core (never a bare busy-spin default — Component note stands).

4. **Split consensus off the client-gateway thread.** Today `NodeRuntimeAgent` fuses the
   client gateway (produce/fetch parse + route) with consensus + group/txn/metadata
   coordinators on one `AgentRunner` (`BrokerNode.cpp:1131`), so client request handling
   contends with replication/coordinator housekeeping. Move consensus + coordinators onto
   the reserved-core `ControlAgent` (already defined as a Component / in Principle #5) so the
   client-facing path and the control plane no longer share a thread. Composes with (1):
   once ingress is per-worker, the former gateway thread's remaining role shrinks to
   control-plane only.

**Ordering.** (1) is load-bearing; (2) + (3) unlock its headroom on multi-core hardware;
(4) removes the residual contention. **Client-side batching** (`lingerUs`/`batchBytes`) is
orthogonal — it raises throughput within the *current* model by amortizing trips through
the funnels, so it should be measured **first** to establish the batched baseline (fair
apples-to-apples vs Kafka's batched 79.5k) before restructuring ingress.

**Testing (adds to the E testing section).** Throughput-scaling vs worker count must show
near-linear improvement once ingress is per-worker (it currently does not — the E5
regression-of-record). Per-worker-ingress correctness under D dynamic reassignment (a
migrating shard's ingress stream moves with ownership, no dropped/duplicated produces).
`DEDICATED`-mode and busy-spin runs on a right-sized (≥16-core) box confirming the `<100µs`
p99 gate and throughput ≥ Kafka batched.
