# Activity-driven worker pass (T23 I3) — design

Status: DRAFT 2026-10-19. Design only. Branch read: `feat/auth` @ 9bff81b1.
Companion: `2026-10-19-replica-facts-ring-design.md` removes the per-pass `publishCleanerState`.

## 1. Problem

Finding T23 I3: the worker's idle pass walks every owned replica. That costs about 43 µs per pass at
1024 replicas. A produce that arrives just after the ingress poll waits for a whole pass before it is
polled. Its push and its acks=all fire then need one or two more passes. p50 50 µs is out of reach at
that scale.

The shard pass is `Worker::doWorkShard_` (Worker.h:1706). These full scans run on every pass, busy or
idle:

| # | Loop | Per-entry work | Code |
|---|---|---|---|
| L1 | `owned_` → `replica->doWork(now)` | `retryOwedDrSends_`, 4 pending-report checks, `recomputeHw`, the owed-fetch retry, push-window min over the ISR, the ISR-check timer, `FollowerSync::decide`, the follower flush timer, `publishCleanerState` | Worker.h:1730; PartitionReplica.cpp:2424-2666 |
| L2 | `rcSlots_` → `rcAdvance_` | RC build step / ring budget | Worker.h:5555 |
| L3 | `owned_` → `driveViews_` (if views) | `interestedIn`, role, rc, HW | Worker.h:5413 |
| L4 | `slots_` + `owned_` → `submitBackgroundDiskWork_` | `reconcileTruncation_`, checkpoint kick, pending seals, epoch-state due, follower flush request | Worker.h:5928 |
| L5 | `slots_` cold tail | `drainCleanerSwapsOnOwner`, `drainRetentionDeletesOnOwner`, `drainStagingRenamesDone`, `checkStorageFailure_`. Each one acquire-loads an SPSC index, so 4 × N cache lines are touched | Worker.h:1756-1761 |
| L6 | `subs_` push | budgeted, per subscription | Worker.h:2839 |
| L7 | `slots_` sync | time-gated (interval 0 by default) | Worker.h:6174 |

The classic `doWork` (Worker.h:1521) has the same loops. Every loop costs O(owned) whether or not
anything happened.

## 2. Measurement first

### 2.1 Bench `WorkerScaleLatencyBench` (new, benchmark/)

- **Rig.** The rig follows DutyPassLatencyGateTest: a real `Worker` in shard mode with fused runtime
  placement, real disk `Log`s, and a real `NodeRuntimeAgent` pass. The bench thread *is* the worker
  thread. A second in-process worker is the RF=2 follower, and the two are connected by the
  in-process replication fake from `replication/testing/`. Produce frames are injected through the
  worker ingress frame handler, each stamped with `t0`. Acks are captured at `SessionAckSink::flush`
  (`t1`), and PushRecords are captured at the egress (`t2`).
- **Sweep.**
  - `owned ∈ {1, 16, 64, 256, 1024, 4096}`
  - `active ∈ {1 partition, 10 %, 100 %}`
  - `acks ∈ {1, all}`
  - offered rate ∈ {closed-loop 1 in flight, 50 k/s open-loop}
- **Output.**
  - produce→ack and produce→deliver p50, p99 and p99.9 (fixed-bucket histogram, no allocation);
  - the pass-duration distribution;
  - the idle pass cost;
  - `ready entries serviced / pass`.

  `RUNE_BENCH_JSON=1` writes the numbers into `docs/benchmarks/worker-scale.json`.
- **Baseline expectation.** Idle pass ≈ 43 µs at 1024. Produce p50 grows linearly with `owned`.

### 2.2 Gate test

`DutyPassLatencyGateTest` gains the scenario `ScaleIdleOwned` (1024 idle plus 1 hot partition),
first in the `kKnownFailures` list with the measured number. It flips to a hard gate in step 7.

## 3. Approach

**Chosen: an event-driven scheduler per worker, with ready lists fed by the events that create work,
a hashed timer wheel for deadlines, and a bounded fair share per pass.**

Rejected:
- **Shard the replicas over more workers.** This only divides the constant: 4096 replicas still
  mean 1024 per worker.
- **Round-robin K replicas per pass (budgeted scan).** It bounds pass time, but a produce on replica
  *i* waits for the cursor to reach *i*: O(N/K) passes of latency. Wrong for p50.
- **Per-replica threads or coroutines.** They break shared-nothing ownership per worker and the
  zero-alloc rule.
- **Keep the scan and make it cheaper.** It is still O(N) cache misses. 1024 × ~40 ns is about the
  observed cost, so the floor does not move.

### 3.1 Primitives (header-only, zero-alloc, worker-thread-only)

- **`ReadyQueue`.** Two intrusive FIFOs of slot indices, `hot` and `maint`. Each slot carries
  `uint8_t queuedMask` and `uint16_t reasons`, and an enqueue is O(1) and idempotent. The capacity is
  the slot-array size, fixed at ownership install. No allocation in steady state: the arrays are
  resized only in `executeSetOwnershipWire_`, which is cold, under the ownership generation.
- **`TimerWheel`.** A 2-level hashed wheel: 256 × 64 µs buckets, then 256 × 16 ms. Each slot has one
  intrusive node per timer kind: `kFollowerFetch`, `kIsrCheck`, `kResend`, `kFlush`, `kFactsDue`.
  `arm(slot, kind, dueNs)` replaces the earlier arming. `advance(now)` moves expired nodes into
  `maint` (or `hot` for `kFollowerFetch` when Behind). There is no per-entry heap: nodes live in the
  slot.

### 3.2 Event sources, and the reason each marks a slot ready

| Event | Where it is observed (worker thread) | Queue / reason |
|---|---|---|
| Client produce appended | `pollWorkerIngress_` / `drainIngress_` append | hot / `Appended` (push, HW) |
| Follower fetch arrives (leader) | `pollReplTransport_` → `onFetchRequest` | hot / `FollowerAck` (HW, push window, owed acks) |
| Push / fetch response applied (follower) | `pollReplTransport_` apply | hot / `Applied` (ack owed, keepalive re-arm) |
| Role / ownership / epoch op | `drainIngress_` op ring | maint / `RoleChange` |
| io completion (seal, flush, epoch-state fsync, recovery checkpoint) | `reapDurableCompletions_` CQ entries carry the slot index | maint / `IoDone` |
| Cleaner posted swap / retention delete / rename done / checkpoint failure | **new** `CleanerNotifyRing` (cleaner → owner, O2O, `{slot, kind}`). It replaces L5's 4×N index polls with one poll. | maint / `CleanerPosted` |
| Segment provisioner ready segment | the provisioner ring already exists. The ring now carries the slot. | maint / `Provisioned` |
| RC change (txn batch / marker applied, rebuild budget left) | at the apply site | maint / `RcWork` |
| View interest (source append on a view-source leader) | at the append site, when `viewSet_->interestedIn(topic)` | hot / `ViewFold` |
| Transport refused a send (F27 owed fetch / DR / answer) | at the refusal site | `deferred` list: retried next pass, does not count as work, so the idle strategy still backs off (today's semantics) |

**Post-service rule.** After any call into slot *i*'s replica or log (append, apply, doWork, cleaner
drain, completion), the worker calls `postService_(i)`. It checks the replica/log-side flags that the
call could have raised, enqueues the slot with `maint` / `DiskWork`, and re-arms the timers:
- the flags: `epochStateWriteDue`, `takeFollowerFlushRequest`, `pendingSealCount`,
  `takeCheckpointKick`, `truncatePending`, `storageFailed`, `report_.pending`, …;
- the timers come from `replica.nextDueNs(kind)`.

Every such flag is raised only synchronously inside a worker call on that slot. The audit is part of
step 3, and a test pins it: an `ActivityAudit` debug mode re-runs the old full scan and asserts it
finds no work the scheduler missed.

### 3.3 `PartitionReplica` changes

`doWork(now)` stays the single place that performs duties. It is called only for a ready or due
slot. New pure accessors:
- `nextDueNs(TimerKind)` returns the follower's next fetch, keepalive or resend from `FollowerSync`,
  the leader's next ISR check (`lastIsrCheckNs_ + isrCheckIntervalNs`), the AlterIsr / report
  resend, and the follower flush.
- `hasImmediateWork()` covers owed DR sends, owed fetch answers, pending reports with a deadline
  passed, and `ackOwed`.

Nothing else in doWork changes. The ISR check becomes a timer instead of a per-pass compare. Timers
are staggered by slot index, `hash(slot) mod interval`, so 1024 leaders do not all expire in one
bucket.

### 3.4 The pass (shard order kept)

```
drainIngress_ (ops) → flushRetFifo_ → pollWorkerIngress_ (appends: mark hot)
→ submitCoalescedFlushes_ → pollReplTransport_ (fetch/apply: mark hot)
→ wheel.advance(now) (due → maint/hot)
→ serve(hot, budget Kh)  then  serve(maint, budget Km)   // each: doWork + postService_
→ rcAdvance (only RcWork-ready slots) → driveViews_ (only ViewFold-ready slots)
→ serveHeldPushReads_ → submitBackgroundDiskWork_ (only DiskWork-ready) → flushOwedAcksAll_
→ reapDurableCompletions_ → pushOwnedSubscriptions_ → poll CleanerNotifyRing → worker-wide timers
→ facts flush (companion design) → ack sink flush → prepareIdle_
```

- **Budgets.** `Kh = 256` and `Km = 32` slots per pass, named constants. The hot work per pass is
  already bounded by the ingress and replication poll limits (`kReplFetchLimit`, the ingress drain
  cap), so `Kh` is a backstop.
- **Fairness.** A served slot leaves its queue. A slot re-marked during service goes to the **back**.
- **Owed acks** (`flushOwedAcksAll_`) become event-driven. Only replicas whose HW moved this pass,
  carried in the `HwMoved` reason, are checked against their waiters. `pendingAcksAll_` becomes
  a per-slot, offset-ordered intrusive list, the same shape as `OrderedWaitQueue` (Worker.h:4507),
  so a check fires from the front in O(1). Ack timeouts are
  one worker-wide wheel timer at the earliest deadline.
- **`prepareIdle_`** parks only when hot, maint and deferred are all empty and the next wheel
  deadline is later than the park. The runner's park is bounded by `wheel.nextDueNs()` (the
  existing ParkUntilWorkIdle bound).

### 3.5 Duties that stay periodic (timers)

| Duty | Timer | Period / deadline |
|---|---|---|
| Follower fetch cadence (Behind), keepalive (InSync, ≤ 20 ms), resend (`kFetchResendNs` 20 ms) | per slot `kFollowerFetch` | from `FollowerSync` |
| Leader ISR lag check | per slot `kIsrCheck` | `isrCheckIntervalNs` (10 ms), staggered |
| AlterIsr resend, leader-epoch bump / leaderless / lineage / refused-report resend | per slot `kResend` | `kAlterIsrResendNs` / report cadence |
| Follower flush | per slot `kFlush` | `followerFlushIntervalNs` (off by default) |
| Facts progress conflation | per slot `kFactsDue` | 1 ms (companion design) |
| Owned-log async sync | worker-wide | `syncIntervalNs_` (0 = off) |
| acks=all / Strict ack timeout | worker-wide (earliest waiter) | ack timeout (30 s) |
| No-hang stats log, RATE diag | worker-wide | 10 s / 1 s (`fsLog` only) |

## 4. Starvation and timing analysis

- **Hot latency.** A produce on any replica is serviced in the pass that polls it: the append marks
  it hot, and hot is served before maint in the same pass. The pass length is O(events this pass +
  Km + expired timers), independent of `owned`.
- **No starvation.** Each queue is a FIFO with dedup, so length ≤ slots. A slot marked ready is
  served within ⌈len/K⌉ passes of its queue. Maint always gets `Km ≥ 1` per pass even under a hot
  flood, so maint latency is ≤ ⌈N/Km⌉ passes: 32 passes at N = 1024, which is ≪ 10 ms at ≤ 10 µs
  passes. A timer is late by at most one bucket (64 µs) plus the maint bound.
- **Liveness duties at the bound.** The worst case is 1024 leaders whose ISR checks all expire at
  once; staggering prevents that by construction. That gives ≤ 32 passes ≈ 0.3 ms of extra lateness
  on a 10 ms check, and on a `replicaLagMaxMs` of 10 s that is irrelevant. Follower keepalive
  lateness is ≤ 0.3 ms on 20 ms, far below the eviction window.
- **Missed event = hang risk.** This is the main risk. Three mitigations:
  - the post-service audit (§3.2);
  - a debug-build `ActivityAudit` that runs the old scan every pass and fails on found work;
  - a release-build **safety sweep**: one slot per pass is round-robined through `doWork` anyway.
    It costs 1 call per pass, guarantees every slot is visited every N passes, and counts
    `sweepFoundWork` (must stay 0; the counter is logged on change). This is defence in depth, not a
    correctness dependency.
- **Ownership change.** `executeSetOwnershipWire_` rebuilds the slot arrays and queues at the new
  generation, and marks every new slot maint/`RoleChange`. Stale queue entries carry an incarnation
  and are dropped.

## 5. Test plan

Regression tests (named `*Regression*`):
- `ActivitySchedulerEquivalenceRegression`, the safety proof. A differential test: the same
  randomized event trace (appends, fetches, applies, role ops, io completions, cleaner posts, timer
  advances) is driven through the old every-pass driver and through the scheduler, with 10⁵ seeds.
  The emitted messages, acks and HW/LEO sequences must be identical per replica (time-shifted by
  at most the stated lateness bound).
- `SchedulerNoStarvationRegression`: a hot flood on 1 partition plus 1023 idle partitions with due
  timers. Every timer fires within its bound, and every maint slot is served within ⌈N/Km⌉ passes.
- `SchedulerIdleParksRegression`: 1024 idle replicas. The pass does 0 doWork calls besides the
  sweep, and the runner parks.
- `CleanerNotifyRingRegression`: a swap, delete or rename-done is posted while the owner is idle.
  It is installed in ≤ 1 pass, and a full notify ring falls back to the sweep, never to a lost swap.
- `SchedulerOwnershipChangeRegression`: a SetOwnership while slots are queued and timers armed. No
  stale slot is served, and every new slot is serviced.
- `SchedulerZeroAllocRegression`: steady state at 1024 owned under load. The alloc hook counts 0.

### Interaction matrix (op × entity × timing)

- **Ops:** produce acks=0/1/all/Strict; follower fetch; push apply; keepalive; ISR shrink; ISR
  expand; AlterIsr resend; becomeLeader; becomeFollower; divergence truncation; roll / seal;
  compaction swap; retention delete; staging rename; RC txn marker; view fold; PushRead held; ack
  timeout; ownership change.
- **Entities:** disk leader; disk follower; transient; view source; RC partition; 1 / 1024 / 4096
  owned; single-node; RF=3.
- **Timings:** event in the same pass as its timer expiry; event during park; ring full (ret_,
  notify, transport refused); burst of 1024 simultaneous timer expiries; ownership change mid-queue;
  io completion for a retired slot.

Each cell is a parameterized case of the equivalence harness. The leader/follower and retire
columns also run as integration tests in a real 3-broker in-process cluster.

E2E (k3s VM, under `flock /root/vm.lock`): a 3-node cluster with 1024 partitions, RF=3, and one hot
partition. Measure produce→deliver from the real client, then kill and return a node. Assert
failover, ISR re-expansion and no stall.

## 6. Bench gates

| Bench | Gate |
|---|---|
| `WorkerScaleLatencyBench` idle pass, owned = 1024 | ≤ 2 µs (from ~43 µs) |
| produce→ack acks=1, 1 hot of 1024 owned | p50 within 10 % of the owned = 1 p50; p99 ≤ 200 µs |
| produce→deliver acks=all RF=2 in-process, 1 hot of 1024 | p50 ≤ 50 µs, p99 ≤ 200 µs |
| 100 % active, 1024 owned, 50 k/s | p99 no worse than baseline (the scheduler must not lose to the scan under full load) |
| DutyPassLatencyGateTest | every existing budget kept; `ScaleIdleOwned` meets the hot budget |
| e2e k3s 1024 partitions | p50 ≤ 50 µs, p99 ≤ 200 µs produce→deliver (the NFR) |

## 7. Implementation steps (for Sonnet)

1. Bench + gate scenario: `WorkerScaleLatencyBench` (§2.1) and `ScaleIdleOwned` in
   `kKnownFailures`. Record the baseline JSON.
2. `ReadyQueue` + `TimerWheel` primitives. Unit, model-check (vs a brute-force oracle) and
   zero-alloc tests.
3. `PartitionReplica::nextDueNs` / `hasImmediateWork`, plus the flag audit: list every flag raised
   outside doWork and prove it is raised synchronously in a worker call. Unit tests per timer kind.
4. Worker scheduler: event marking at every source in §3.2, `postService_`, the hot/maint serve
   loops replacing L1/L3/L4, RC only for ready slots, safety sweep, `ActivityAudit` debug mode. The
   equivalence regression goes in **with** this step.
5. `CleanerNotifyRing` (cleaner/provisioner → owner) replacing L5's per-log polls. Notify-ring
   regression.
6. Event-driven owed acks (`HwMoved`) + worker-wide timers + `prepareIdle_` bound by the wheel.
   Starvation, idle-park and ownership-change regressions.
7. Gates: flip `ScaleIdleOwned` to hard, run the full `*Regression*` under ASan+UBSan and TSan, the
   bench gates, k3s e2e, and docs/benchmarks numbers.

## 8. Decisions made

- Keep `PartitionReplica::doWork` as the one duty function, and change only *when* it is called.
  This avoids a duty rewrite and makes the differential test possible.
- Two priority classes (hot/maint), FIFO within each class. `Kh = 256`, `Km = 32`.
- 2-level hashed wheel: 64 µs / 16 ms buckets.
- A one-slot-per-pass safety sweep in release, as defence in depth with a must-be-zero counter.
- The ISR check stays a 10 ms timer (staggered), not per-follower lag deadlines. That is simpler and
  keeps the current semantics.
