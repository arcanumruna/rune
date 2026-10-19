# Per-Worker Replication Transport (Lever #4) — Design

**Status:** design / awaiting review
**Date:** 2026-09-30
**Author:** arcanumruna + Claude
**Related:** `docs/superpowers/plans/2026-09-30-replication-throughput-optimization.md` (lever #4; and #1 multiplex which composes on top), memory `rune-replication-benchmark-kafka`, `rune-milestone-e-runtime`, `aeron-is-the-bible`, `rune-shared-nothing-law`.

## 1. Problem & goal

3-node acks=all replication throughput collapses far below Kafka (Kafka 156k rps on the same 4-core box; Rune ~6k backoff / ~43k busyspin at 256B, ~25 MB/s at 1KB). Instrumentation (rune-fr) proved the ceiling is **the single control thread**: `NodeRuntimeAgent` runs consensus + **all** partitions' replication I/O, and every worker marshals every replication frame through **one shared MPSC outbound ring** that the control thread drains and offers to a **per-node set of publications**. The commit round-trip crosses that single thread ~4× per record; broker-side copies (lever #2) are latent behind this serialization.

**Goal:** remove the single-control-thread replication funnel by giving **each worker its own Aeron pub/sub for its partitions' replication**, parallelizing replication I/O across cores while preserving Aeron's SPSC/low-latency model and all DR/failover correctness. Target: 3-node acks=all @1KB throughput rises materially toward Kafka parity on a clean 4-core box (this is the lever expected to *move* throughput, unlike #2).

## 2. Key insight — symmetric worker count → W independent replication planes

`ShardRouter::workerForPartition(topicId, partition) = hash((groupKey<<32)|partIndex) % workerCount`. It is a pure function of `(groupKey, partIndex, workerCount)`. Consensus already replicates identical topic→group metadata to every node. Therefore, **if every node runs the same `workerCount`, partition P is owned by the same worker index `w` on every node.**

Consequence: the cluster factors into **W parallel replication planes**. Plane `w` = {worker `w` on every node}. Worker `w` on the leader replicates partition P only to worker `w` on each follower (same index). **There is never cross-worker replication traffic.** Leadership failover flips *which node's* worker-`w` is leader vs follower for P; the owning worker *index* never changes for a stable `workerCount`.

This makes the transport fully shared-nothing per worker and static across failover (no pub/sub migration).

## 3. Architecture

### 3.1 Per-worker replication planes (user partitions, `topicId >= 0`)
For each node, for each worker `w` in `[0, W)`:
- **Inbound:** one `Subscription` on `(replicationChannel, replBaseStreamId + w)`, polled by worker `w` in its own `doWork()` loop.
- **Outbound:** one `ExclusivePublication` to each peer node N on stream `replBaseStreamId + w`. Worker `w` owns these (one per peer); it is the sole writer (Aeron `ExclusivePublication` = single-writer contract satisfied).

Worker `w` builds, encodes, and offers replication frames for its owned partitions **on its own thread, directly to its own publications** — no shared outbound ring, no control-thread drain. On the receive side it polls its own Image and **applies inline within the poll callback** (it owns the target log), which also removes lever #2's mandatory recv-side `records.assign` copy (the term-buffer view is consumed before the callback returns, on the owning thread).

### 3.2 Internal partitions (`topicId < 0`: consensus/metadata) stay on the control thread
Consensus and metadata replicas are control-thread-driven and must not move onto worker threads (they interleave with `NodeRuntimeAgent` consensus work). They keep the **existing** single-stream `AeronPartitionTransport` (shared outbound ring + control-thread drain) unchanged. Routing split is by `topicId` sign, which the current `onFragment` already uses for the inline-vs-worker decision.

### 3.3 Stream / port allocation
`replBaseStreamId` is a new `NodeConfig` value chosen to not collide with existing streams (client `clientStreamId`, `workerIngressBaseStreamId = clientStreamId+1 .. +W`, `workerEgressBaseStreamId`, consensus/`replicationStreamId=20`). Scheme mirrors the client path (`base + workerId`). Concrete allocation and the guard that the ranges are disjoint are specified in the plan. Peers are addressed on the same channel used today (`peer.replicationChannel()`), only the streamId becomes per-worker.

### 3.4 Symmetric-worker-count invariant (enforced, not assumed)
Per-worker planes are correct only when all nodes share `workerCount`. Today `RUNE_WORKERS` is per-node with no cross-node awareness. This design:
- Publishes each node's `workerCount` in the consensus/metadata membership it already exchanges.
- At membership resolution (startup, and when a new peer joins), **validates every peer's `workerCount == self.workerCount`**. On mismatch (when `RUNE_PER_WORKER_REPLICATION` is on): **hard-refuse** — the mismatched node fails startup with a fatal error naming the offending node and both counts (a joining node aborts rather than disrupt an established cluster; established nodes reject a peer advertising a mismatched count).
- **Why hard-refuse, not per-node fallback:** a per-node fallback to the legacy shared-stream transport would put that node on a *different* streamId scheme than its per-worker-plane peers, so they could not communicate at all — mixed transports are a split cluster, not a graceful degrade. A worker-count mismatch is a misconfiguration in a homogeneous cluster; fail loud and force the fix. (Correct cluster-wide coordinated fallback is possible but complex and unwarranted — YAGNI.)
- Asymmetric worker counts (propagate + route to each peer's own `workerForPartition`) are explicitly **out of scope** (YAGNI); homogeneous clusters are the norm.

### 3.5 Config flag & rollback (also the A/B test harness)
Gate the entire feature behind `RUNE_PER_WORKER_REPLICATION` (bool). Off → today's transport (unchanged code path). On → per-worker planes for user partitions. This gives: (a) instant rollback if DR regresses in the field; (b) a clean **before/after throughput A/B on the same binary and same clean box** by flipping the flag between interleaved runs (removes the two-binary confound entirely). Default value is decided at rollout (proposed: off until the full DR matrix is green on the flag-on path, then flip default to on).

## 4. Components / files (interfaces; detail in the plan)
- `rune-protocol` — none for #4 (wire format unchanged; #1 adds the repeating group later).
- `NodeConfig` — add `perWorkerReplication` (bool), `replBaseStreamId` (int32), publish `workerCount` in membership.
- New `WorkerReplicationTransport` (per-worker) implementing the `PartitionTransport` interface the replica already depends on — owns worker `w`'s inbound Subscription + per-peer ExclusivePublications; `poll()` drives receive+inline-apply; send methods offer directly. Mirrors `WorkerIngress`/`WorkerEgress` lifecycle.
- `BrokerNode` — construct per-worker replication subs/pubs at startup (behind the flag), hand each to its worker; validate symmetric worker count.
- `Worker` — drive its replication transport `poll()` + the owned replicas' `doWork()` (already loops owned replicas); route each replica's send through its own transport instead of the shared one.
- `PartitionReplica` — inject the per-worker transport; no logic change to becomeLeader/Follower/onFetchRequest/applyReplicated/OLE/reconcile beyond which transport instance they call.
- Legacy `AeronPartitionTransport` — retained for internal (`topicId<0`) partitions and for flag-off.

## 5. What composes on top (not in this spec)
- **#1 per-(peer,worker) multiplex:** coalesce worker `w`'s partitions-to-peer-N into one repeating-group FetchRequest/FetchResponse. Rides directly on plane `w`. Separate spec.
- **#2c leader-read `readView`:** zero-copy log read on the send path. Separate follow-on.

## 6. Validation & Testing Strategy

Nothing merges to main until **every** layer below is green on the **flag-on** path, and the flag-off path is proven unchanged. Order follows `feedback_integration_before_e2e`: fast deterministic layers first, Docker E2E last, throughput on a **clean box** last.

### 6.1 Correctness — unit & regression (fastest gate, build-fast on Mac)
- `rune_replication_tests` (currently 340/340), `rune_storage_tests`, transport unit tests — all green, flag-agnostic.
- **New unit tests** for the plane model: (a) `workerForPartition(P)` identical across two ShardRouters with equal `workerCount` (the invariant the design rests on); (b) symmetric-worker-count validator accepts equal, rejects mismatch (→ fall back + fatal log); (c) `WorkerReplicationTransport` send→receive round-trips a FetchResponse to the correct worker plane in an in-process harness (InMemory/loopback transport), including inline-apply with no `records.assign`.
- **Regression tests** for any bug found, marked `*Regression*` (per `mark-regression-tests-as-suite`), runnable as a suite.

### 6.2 Correctness — in-process integration (green smoke before Docker)
- Per-worker ownership integration tests (existing `rune_worker_pool_*`, `rune_gateway_*_replication`, `rune_gateway_failover_replication`, acks=all deferred-ack) run **with the flag on**, all green — leader→follower over the real per-worker planes, including epoch change / truncation / reconcile / ISR shrink-expand.
- **Failover-specific in-process tests** (the correctness-critical set): rolling leader kill, flapping leader, minority-leader fence, OLE epoch probe, reverse-catch-up reconcile — assert **no committed-data loss** and ISR re-expansion, flag on. Reproduce any E2E bug here first (`feedback_integration_before_e2e`).

### 6.3 DR / E2E (dockerized, on the VM)
- Full dockerized DR matrix (`rune-dr-e2e` style: multi-client, netem latency/loss, node crashes, rolling restart, partition/heal) run with **flag on**, images tagged by git SHA (`docker-image-tag-policy`). Acceptance = **the same matrix that is green on main today is green flag-on**, with zero committed-loss and correct ISR/epoch convergence (time-independent, per `time-independent-liveness-convergence`).
- Run the **flag-off** path through a smoke subset to prove the legacy path is byte-for-byte unaffected (no accidental shared-code regression).
- If TSan-relevant (worker threads now own Aeron resources): a TSan build of the in-process failover suite must be clean (`rune-shared-nothing-law`).

### 6.4 Smoke (VM, clean box)
- 3-node cluster smoke (`replbench.sh` / `cluster.sh`) forms ISR=3, produces + consumes acks=all at 1KB, no NO-LEADER / no errors in broker logs — flag on and flag off.

### 6.5 Throughput — before/after (the headline; clean box, no hardware crutch)
Per `perf-ab-never-blame-server`, `never-hardware-to-mask-bad-software`, and the clean-box protocol in `k3s-node-01-repurposed-dev-box`:
- **Box hygiene FIRST:** kill leftover `rune_broker`/producer/bench procs and the host game backend; verify `uptime` load < ~1 and `top` ≥ 90% idle before any run. Bogus numbers from a dirty box are not results.
- **Same clean box, same binary, flag flipped** — `RUNE_PER_WORKER_REPLICATION=off` (A) vs `on` (B) — **interleaved ABAB, n ≥ 15 pairs.** No binary swap, no config drift.
- **Scenario:** 3-node acks=all (tier=majority/all-in-sync), **1 KB** records (the Kafka-comparison payload), 4 partitions / 2 workers (and a higher partition:worker ratio, e.g. 16p/4w, where per-worker parallelism should show more), 16 MiB Aeron buffers (64 MiB thrashes this box — a documented failure mode, not a hardware limit), backoff idle.
- **Primary metric:** records/sec and MB/s (median + IQR across the 15 pairs), plus p50/p99 produce-ack latency. **Secondary:** rune-fr per-stage spans (`ReplOutboundDrain` should shrink/vanish for user partitions; per-worker `poll`/apply spans appear), and broker CPU distribution across worker threads (was: one hot control thread → should be: work spread across workers).
- **Acceptance gate:** a **statistically clear** throughput increase (paired, the ABAB pairs consistently favor B) with **no latency regression** and DR/correctness green. If the cluster metric is too noisy on 4 cores to resolve (as it was for #2), fall back to the rune-fr span deltas + a pinned micro-bench of the send/poll path (box-independent) — but #4 removes a whole serialization stage, so a cluster-level effect is expected here, unlike #2.
- **Honesty clause:** if #4 does **not** move throughput, that is a finding to root-cause in software (what is the next serialization point?), never an excuse to move to bigger hardware.

### 6.6 Exit criteria (all required to merge)
1. Unit + regression green (flag-agnostic). 2. In-process integration + failover green, flag on. 3. Dockerized DR matrix green flag-on; flag-off smoke unaffected; TSan clean. 4. VM smoke clean, flag on & off. 5. Throughput ABAB shows a clear, paired improvement with no latency regression — on a verified-clean box. 6. Rollback proven (flag off = legacy path).

## 7. Risks & mitigations
- **Aeron resource/memory blowup:** W subs + W×(N-1) pubs per node, each with term buffers. At W=4/N=3 that's 4 subs + 8 pubs. Term-buffer sizing must fit the box (64 MiB × that count thrashes 8 GB) → size per-plane term buffers modestly (e.g. 16 MiB) and document. Mitigation validated in 6.4/6.5.
- **OLE / reconcile / epoch paths on worker threads:** these must be verified worker-safe on plane `w` (6.2). Internal partitions staying on the control thread (3.2) limits blast radius to user-partition replication.
- **Symmetric-worker-count invariant violated in the field:** enforced validation with **hard-refuse** at membership resolution (3.4) prevents both silent mis-routing and a split cluster from mixed transports.
- **Regression in DR:** flag gives instant rollback; full DR matrix gates merge (6.3).

## 8. Out of scope
Asymmetric worker counts; #1 multiplex (repeating-group wire change — separate spec); #2c readView; worker-count rescale without restart.
