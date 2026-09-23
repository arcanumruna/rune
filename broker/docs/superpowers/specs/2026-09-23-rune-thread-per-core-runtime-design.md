# Rune Thread-Per-Core Partition-Sharded Runtime — Design

**Status:** design (approved in brainstorming 2026-09-23; next → writing-plans)
**Milestone:** E (follows Milestone D — distributed partition leadership)
**Author dialogue:** brainstormed with the user 2026-09-23.

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

- **`WorkerPool`** — owns N `Worker`s; core-affinity assignment; lifecycle (start/stop/
  drain). N = (isolated cores − reserved), configurable.
- **`Worker` (partition-worker agent)** — pinned busy-poll agent owning a set of `Shard`s.
  `doWork(nowNs)` run-to-completion over owned shards each cycle: poll own ingress →
  dispatch produce/fetch to the shard's `PartitionReplica` → reap `io_uring` completions →
  fire due deferred-acks → subscribe-push → poll cross-worker mailbox. Single-threaded; no
  locks; no shared mutable state with other workers.
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

Each phase is independently testable and leaves the broker working.
