# View read lease (N-I1) and view-sink ownership — design

Status: DRAFT 2026-10-19. Design only. Branch read: `feat/auth` @ 9bff81b1.
Part (b) has one point that needs a user decision (§B.6).

---------------------------------------------------------------------------------------------------

## Part A — GetViewRow read lease

### A.1 Problem

`Worker::executeGetViewRowWire_` (Worker.h:3163) serves a row when its *local* source replica says
`role() == Leader` and the instance was folded under this leadership generation (Worker.h:3203-3225).

Take a leader that is partitioned away from the controller. It has not applied its demotion, so it
keeps `role_ == Leader` and keeps answering from its frozen table. The controller fails the partition
over after `brokerLivenessTimeoutMs` (NodeConfig.h:440, default 3 × the leader-heartbeat timeout).
The new leader then accepts writes, and those writes are acked acks=all. The old broker still serves
pre-failover rows to any client that reaches it. Gateway-side gating does not help: `viewSourceLedElsewhere_`
(ClientGateway.cpp:7953) reads the old broker's own stale image.

The result is a stale read after a write was acknowledged. Views are sold as a DB face, so this is a
linearizability bug.

Two smaller gaps are fixed here as well:
- The fold may lag HW within a pass. A read that arrives right after an ack could miss the write
  even on the true leader.
- A client that has already seen a newer epoch can be routed back to the zombie.

### A.2 Options

| Option | Verdict |
|---|---|
| **Read-index via the controller.** Each read (or batch) confirms leadership with a controller round trip. | Rejected for the hot path. It adds an RTT plus a controller duty cycle to every read, so p50 50 µs is impossible, and it puts read load on the controller. |
| **Read-index / lease via partition followers** (quorum of follower fetches). | **Unsound in the ISR model.** The controller may elect *any* ISR member. With RF=3 and a split {A, B} \| {C, controller}, A still gets B's fetches, a "quorum of 2", while C is elected leader. |
| **KRaft-style fencing alone.** The controller fences a broker whose heartbeats stop. | Fencing only stops *assigning* leadership. The zombie serves until it learns. Kafka itself bounds zombie reads only through client-side leader epochs (KIP-320), which help clients that already know the newer epoch. Kept as a complement (A.5.4), not the fix. |
| **Controller-granted, quorum-backed broker read lease, with a demotion proof shortcut.** | **Chosen.** It costs one compare per read on the hot path. It is safe under a bounded-drift assumption only, with no clock synchronisation. Grants ride the existing 20 ms consensus heartbeat cadence. |

### A.3 Clock assumptions

- **C1. Bounded drift.** Each node's lease clock advances at real rate within `[1/(1+ρ), 1+ρ]`.
  `ρ = 0.01` (1 %) is the named constant `kLeaseDriftBudget`. That is two orders of magnitude above
  disciplined crystal and kvmclock drift, and it costs only about 2 % of lease length.
- **C2. The lease clock counts suspend.** It is `CLOCK_BOOTTIME` on Linux and
  `mach_continuous_time` on macOS. It is **not** the duty clock: `MonotonicNanoClock` is
  `CLOCK_MONOTONIC_RAW` (NanoClock.h:32), which stops during suspend, and a stopped lease clock is
  unsafe because the node would believe less time has passed. A VM pause is covered as long as the
  guest BOOTTIME catches up after the pause, which kvmclock does. This is documented as an
  operational requirement.
- **C3. No synchronisation.** No two clocks are ever compared. Every bound is a local duration
  measured on one node.
- **C4. PreVote stickiness** (`2026-10-08-prevote-design.md`). A voter that heard the live
  controller within `H = leaderHeartbeatTimeoutNs` denies pre-votes, so no new controller can be
  elected until a majority has gone `H` without hearing it. Lease grants are added to the "heard"
  sources (`leaderHeardNs_`).

### A.4 Protocol

There are two new consensus templates, hand-encoded like PreVote:

- 62 `ReadLeaseRequest{term, memberId, reqSeq, heardGrantSeq}` (broker → controller). It is sent
  with every AppendPosition, from FollowerLiveness `sendAppendPosition` (20 ms).
- 63 `ReadLeaseGrant{term, memberId, reqSeq, grantSeq, commitPos, validForNs}` (controller →
  broker).

**Controller quorum lease `QL`.** The controller stamps each grant `grantSeq` with its send time
`g`, kept in a 64-entry ring per controller. Requests echo the grant seq that member last heard. Let
`t*` be the largest `g` such that a majority, self included, has echoed a seq sent at or after `t*`.
By C4, no new controller exists before real `t* + H/(1+ρ)`. So `QL = t* + H/(1+ρ)²` on the
controller clock.

**Grant.** On a request:

```
validForNs = clamp(min(L, QL − now), 0, L)
```

Here `L = kReadLeaseNs = 200 ms`. In practice `QL` caps the grant at about 120 ms. The grant carries
`commitPos`, the controller's committed metadata position. The controller records per broker
`leaseExpiry[b] = max(leaseExpiry[b], now + validForNs)`.

**Broker.** The broker records the request send time `s[reqSeq]` on its lease clock, in a 64-entry
ring. On a grant it computes `leaseEnd = s[reqSeq] + validForNs/(1+ρ)²`.

The proof that the broker's real end comes before the controller's real end:

- broker real end ≤ τ_s + V/(1+ρ)
- τ_s ≤ τ_grant
- controller real end ≥ τ_grant + V/(1+ρ)

**Applied-position condition.** The broker hands the lease to its workers only once every role op
derived from metadata `≤ commitPos` has been **enqueued on that worker's op ring**. The lease travels
on the same FIFO ring as the role ops (A.5.1). So a grant issued after a demotion was committed can
never extend the lease of a worker that has not yet applied the demotion.

### A.5 State machines and messages

**A.5.1 Broker control thread: `ReadLease` per node.**

```
NoLease{}                                   – boot, term change, lease expired, or grant from stale term
Leased{leaseEnd, commitPos, term}           – valid grant; forwarded to workers
```

The control thread posts `WorkerOp::ReadLease{leaseEnd}` to each worker through
`WorkerPool::tryEnqueueOp`. The op enters that worker's FIFO behind any held role ops, and a
per-worker `deliveredMetaPos[w] ≥ commitPos` gate holds it until they are delivered. A full ring
holds the newest lease only (conflated). A newer grant replaces the pending one, which is safe
because a newer lease is always ≥ the older one.

**A.5.2 Worker: lease view.** The worker keeps a plain `int64_t leaseEndNs_` (monotone max). The
check in `executeGetViewRowWire_`, after the role check, is:

```
if (leaseClock.now() >= leaseEndNs_) → kNotReady (retriable)
```

That is one clock read and one compare. On a single-node or `memberCount == 1` cluster the lease is
infinite: the controller is self, and there is no other leader.

**A.5.3 Worker: new-leader serve gate.** This is per source-partition leadership, held in the view
instance, next to `instanceLeadership`:

```
Fenced{since, waitUntil, prevLeader, epoch}  – just became leader; predecessor may still serve
Serving{epoch}                               – predecessor proven out
```

- `Fenced → Serving` happens on one of three events:
  - **(proof)** a fetch from `prevLeader` at `leaderEpoch ≥ epoch`. The ex-leader fetches as a
    follower only after it applied the demotion.
  - **(time)** `leaseClock.now() ≥ waitUntil`, where `waitUntil = since + Lmax·(1+ρ)²` and
    `Lmax = H` (≈ 153 ms).
  - **(controller)** the LeaderChange record carries `predecessorFenced = true`.
- The controller sets `predecessorFenced = true` only for a dead-broker failover where
  `now ≥ leaseExpiry[oldLeader]`. With `brokerLivenessTimeout` (450 ms) > L, that is the normal
  failover case, so failover adds **zero** view-read unavailability.
- A new controller has no `leaseExpiry` for grants it did not issue. For `Lmax·(1+ρ)²` after its
  election, it emits every LeaderChange with `predecessorFenced = false`.
- Reads while `Fenced` answer `kNotReady`.

**A.5.4 Complementary epoch fence (client).** `GetViewRow` gains `knownLeaderEpoch`, a new trailing
field that is versioned (D6 encodeFor). If it exceeds the local epoch, the broker answers NotLeader
and the client refreshes metadata. This is the KIP-320 analogue. It needs a rune-client change, and
it is cheap.

**A.5.5 Local fold barrier.** The read records `hwAtArrival = replica.highWatermark()`. If
`instance.foldedTo < hwAtArrival`, the read is parked on a fixed-capacity held-view-read list. It is
served after `driveViews_` the same pass, or the next pass. If the list is full, it answers
`kNotReady`. The barrier is bounded: the target is fixed at arrival, and the fold reaches it within
a pass. This gives read-your-writes on the true leader.

### A.6 Failure / timing analysis

- **Zombie leader partitioned with a stale controller (minority).** The deposed controller cannot
  collect majority echoes, so `QL` stops advancing. Every grant it issues is capped at `QL`, which is
  ≤ the new controller's election time. The zombie's lease therefore ends before any new partition
  leader can exist, whatever `predecessorFenced` says.
- **Zombie partitioned from everyone.** It receives no grants, and its lease ends within
  `≤ validFor` (about 120 ms). The failover happens after 450 ms.
- **Live rebalance.** LeaderChange is committed at P. Any grant after it has `commitPos ≥ P`, so it
  cannot reach a worker before the demotion op, by FIFO. Grants sent before P end by
  `t_P + L(1+ρ)`. The new leader serves on the demotion proof, typically one fetch RTT, or after
  `W` at the latest.
- **Controller StandDown** (D24 TermFloorNotice). Followers drop stickiness at once, so `QL` no
  longer bounds a new election. The new controller's first-`W` rule (A.5.3) still makes the new
  partition leader wait out every old grant.
- **Availability cost.** View reads answer NotReady when:
  - lease renewal stalls for more than about 120 ms (a controller hiccup);
  - in the first `W` after a controller change, for a non-failover leader change without proof;
  - during a rolling upgrade while the controller lacks templates 62/63. Mixed-version clusters
    degrade to "no lease, so view reads are NotReady" until the controller is upgraded. This is the
    same policy as PreVote: documented, and acceptable before release.
- **Process pause after the check.** The linearization point is the check. The ex-leader's table
  only moves forward with its own committed data, so the row returned reflects a state at or after
  the check.
- **Hot-path cost.** One `CLOCK_BOOTTIME` vDSO read (~20 ns) and a compare. Zero allocation. The
  fold barrier is a compare in the common case.

### A.7 Tests (Part A)

Regression tests (named `*Regression*`):
- `ViewReadLeaseZombieRegression`: a 3-broker in-process cluster. Partition the view-source leader
  from the controller and peers, fail over, write and ack on the new leader, then read from the
  zombie. Expected: NotReady or NotLeader, never the old row. This is RED today.
- `ViewReadLeaseStaleControllerRegression`: the controller and the leader are cut off together on
  the minority side. The zombie's lease expires before the new controller exists.
- `ViewReadLeaseRebalanceRegression`: a preferred-leader move. The new leader serves only after the
  ex-leader's fetch at the new epoch (proof path) or after `W`.
- `ViewReadLeaseFifoRegression`: the worker op ring is full when a demotion and a newer lease
  arrive. The lease never overtakes the demotion.
- `ViewReadYourWriteRegression`: ack a produce, then immediately GetViewRow. The read sees the
  write.
- `LeaseClockSuspendRegression` (unit, injected clock): a lease-clock jump forward expires the lease.
  The duty clock is not used.

Interaction matrix (op × entity × timing):
- **Ops:** GetViewRow; produce; failover; rebalance; controller election; StandDown; demotion op;
  lease grant; client epoch refresh.
- **Entities:** view-source leader; ex-leader; new leader; follower; single-node; RF=3; transient
  source.
- **Timings:** grant in flight during demotion; ring full; lease renewal stalls 150 ms; within `W`;
  after `W`; proof before `W`; mixed-version controller.

The partition cases also run as an e2e on k3s with iptables partitions.

### A.8 Implementation notes (issues #82, #486, #491; 2026-10-22)

Implemented: A.3, A.4, A.5.1, A.5.2 and A.5.5; A.5.3 is replaced by a controller-side move fence (below).  Where the code
differs from the text above:

- **Templates 63 / 64.** 62 is `TermFloorNotice`; `ReadLeaseRequest` is 63 and `ReadLeaseGrant` 64.
- **A broker without a lease answers NotLeader, not NotReady** (the worker's not-leader sentinel: the gateway
  redirects or answers a bare NotLeader).  The client retries NotLeader by itself within `ddlRetryBudget`, while
  NotReady reaches the application.  A zombie cannot vouch for its own leadership, so NotLeader is the honest answer.
- **commitPos is the controller's metadata log END**, not its committed position: a new controller's HW can lag what
  its predecessor committed, and its log never does.
- **W is a constant, `L(1+rho)^2` (about 204 ms), not `H(1+rho)^2`** (#491).  A holder caps every grant at `L`, so W
  bounds every lease whatever the grantor's `H`: no per-node config skew can make a lease outlive it.
- **The move fence (#486), instead of the new-leader gate (A.5.3) and `predecessorFenced`.** A metadata record that
  takes leadership off broker A and leaves A out of the new ISR is appended only once no lease A holds can run:
  - the controller is past W since its own leadership began (a predecessor's grants have ended), and no lease this
    controller granted A may still run (`Election::readLeaseMoveFenceOpen`);
  - meanwhile A's lease is no longer renewed (its requests are answered `validFor = 0`, the echo still counts, so QL
    is unaffected; a controller A stops leasing itself), so the wait ends within one lease, about 120 ms;
  - A keeps leading and acking writes during the wait; a dead A passes at once (it was judged dead only after its
    lease ended);
  - the record waits in `MetadataService::drainDynamicQueue`, the one point every decision path goes through
    (controlled shutdown, untrusted storage, epoch-bump exclusions, LostCommitted and storage resets, failover, the
    coordinator-partition bump), and the records behind it wait in order;
  - grants after the append carry a commitPos above it, so FIFO keeps them behind the demotion.
  A move that keeps A in the ISR (preferred-leader move, `electionExcluded`, coordinator steer) never waits: the new
  leader's HW needs A's fetch at the new epoch, which A sends only after it applied the demotion.  A LATER record that
  removes A from the ISR (the new leader's ISR shrink, a controlled shutdown of A, the election after a leaderless step)
  goes through the same fence: the controller remembers per partition every real leader it replaced within the last W
  (a bounded set, at most 4 per partition, pruned on expiry; on overflow every removal from that partition's ISR is
  fenced until W after the overflowing move), and fences any removal of one of them.  A controller inside W of its own
  takeover knows none of the moves before it, so it fences every removal from any ISR (cleared on step-down for the same
  reason).  A fenced record waits only with the later records of its own partition: it never delays another
  partition's records (a dead broker's coordinator-partition failover queued behind it included).
- **ISR right after a takeover (#493).** A new leader's offset-lag gate counts only fetches made at its own
  leadership, and a member with no fetch state is timed from the takeover: a leader change never collapses the ISR to
  the new leader (which also made the case above reachable on the default config).  With every leader
  change thus ordered after the predecessor's lease (or its demotion), the new leader cannot serve state the old one
  lacks while the old one still serves, so the new-leader wait W of A.5.3 is redundant and is not implemented (#487).
  Cost: a controlled shutdown or exclusion move commits up to about 120 ms later (within W of a controller's
  election, up to W), and the moving broker's view reads answer NotLeader for that wait.
- **A.5.4 (the client epoch fence) is not implemented**: it needs a client and wire change, and the lease already
  bounds every stale read: #488.
- **Fold barrier**: it holds a read only on a Ready table (a table still catching up answers NotReady at once), and a
  held read is answered NotReady after 100 ms if the fold cannot reach its target.
- **Rolling upgrade**: a lease needs the controller and a majority of the members on a build that has it.

---------------------------------------------------------------------------------------------------

## Part B — View-sink ownership and the explicit refusal

### B.1 Problem

A view's sink is applied as a real topic: compacted, keyed, `rf 0` = a replica on every broker
(BrokerNode.h:1081-1087, boot BrokerNode.cpp:3489). `createRuntimeUserReplicas` builds a
`PartitionReplica` per sink partition and binds its replication plane **at creation**. The plane
index is `workerIdForShard({gk = sinkTopicId, p})` (BrokerNode.cpp:1501, 1562).

Later, the plan install (`fanoutViewPlan_`, BrokerNode.cpp:4293) calls
`WorkerPool::coLocateViewWithSource` (WorkerPool.h:238). That re-maps the sink's group key to the
source's. Then:

1. **"would MOVE".** Once runners have started, the next `computeAssignment_` sees the sink replica's
   router worker change and throws `std::logic_error` (WorkerPool.h:318-327), which halts the broker.
2. **Plane mismatch.** If the re-map lands before the first assignment, in the same control cycle,
   the replica is owned by worker `W_src` but bound to plane `W_sink`. Peers place it on `W_sink`. Its
   frames arrive on a plane whose worker does not own it.
3. **Silent semantics.** Nothing ever writes the sink log. Rows live in `WorkerViewSet`, and
   checkpoints live in `ViewSnapshotIoAgent`'s own dir. So `Subscribe` on a sink id is silently
   empty forever, and `Produce` to it is silently *accepted* into a log nobody reads.
4. **Dead weight.** Each sink partition is a leader/follower set with fetch keepalives on every
   broker. It adds to the idle-pass cost of `2026-10-19-activity-driven-worker-pass-design.md`, and
   it has an independent leadership that no read uses: GetViewRow follows the *source* leader,
   ClientGateway.cpp:7953.

### B.2 Chosen ownership

**A view sink is a replica-less, catalog-only "view namespace" topic.** The view instance for
`(view, p)` is owned exactly as the source partition `(source, p)`:
- the source's owner worker;
- the source's replication plane, because the sink has none;
- the source's leader and epoch.

Concretely:
- `TopicRegistry` gets a `TopicKind::ViewSink` entry, with no logs and no dirs. It keeps id, name,
  partition count, key field, `TopicState` (Pending/Serving/Dropped) and the C1 gates.
- `sinkApplied` (live + boot) builds **no** `PartitionReplica` and no `MetadataService` assignment.
  There is no `coLocateViewWithSource` (deleted). GetViewRow is enqueued with the **source** topic
  id for routing: `tryEnqueueOp(viewSourceOf_(view), p, op)` instead of ClientGateway.cpp:7967. The
  router never sees sink ids, so no re-map exists to race the assignment.
- Invariant, checked at `sinkApplied`: `sink.partitions == source.partitions` and the same
  partitioner. A mismatch is a catalog-apply halt, never a silent mis-route.
- The C1 gate (`viewRowGate_`, ClientGateway.cpp:7880) uses the sink's TopicState plus the
  plan-delivered gate plus the **source's** placement (`placementCommitted_(source, p)`).
- Metadata responses synthesise the sink's per-partition leaders from the source, so clients that
  route GetViewRow by the view's metadata keep working unchanged.
- Upgrade: old sink dirs, which hold empty logs only, are removed by `TopicDirReconcile` as
  `ViewSink` (no log expected), and the removal is logged.

### B.3 Explicit refusal (permanent `InvalidRequest`)

Data-plane ops on a sink id are refused with `InvalidRequest` (non-retriable), logged once per
(session, topic):
- Produce (gateway and per-worker ingress), Subscribe, group subscribe, Fetch / PushRead,
  ListOffsets, OffsetCommit;
- transactional AddPartitions on a sink id.

Where:
1. The gateway front: `TopicRegistry::kind(topic) == ViewSink`. This is an O(1) lookup on a
   path that already resolves the topic.
2. The worker ingress path: produce frames bypass the gateway in shard mode, so the worker checks
   `viewSet_->isViewSink(topic)`. The plan install ring already carries every view id.

The gap between sinkApplied and the plan install is covered too. The gateway's
`viewSinksAwaitingPlan_` already knows the id, and the worker sees no replica for it, so it answers
the existing NotLeader. Once the plan lands, the answer becomes `InvalidRequest`. That is eventually
explicit, and it never accepts the write.

### B.4 Alternative rejected: keep sink replicas, co-locate them from birth

The fix would be to put `colocatedWithTopicId` in `SinkSpec` and install the source group key before
`createRuntimeUserReplicas`, the same as catalog co-partition groups (BrokerNode.cpp Task 17 C1). It
fixes 1 and 2, but it keeps 3 and 4: inert replicated logs, a second leadership that can diverge from
the source's, and per-pass cost. It stays the **fallback** if the user wants the sink log kept for a
future view changelog (§B.6). In that case the changelog would be written by the source owner on the
source's plane, and §B.3 would allow Subscribe only.

### B.5 Tests (Part B)

Regression tests (named `*Regression*`):
- `ViewSinkNoMoveRegression`: CreateView at runtime with `workerCount = 4` and runners started.
  No logic_error, and GetViewRow returns OK on every partition. This is RED today when the hash
  differs.
- `ViewSinkPlaneRegression`: a 3-broker in-process cluster. There is no sink replica on any plane,
  and the source plane carries everything.
- `ViewSinkRefusalRegression`: Produce (gateway and worker ingress), Subscribe, Fetch, ListOffsets
  and AddPartitions on a sink id each answer `InvalidRequest`. Nothing is appended anywhere.
- `ViewSinkBootRegression`: restart with sinks in the catalog. No sink replica or log, and old sink
  dirs are removed.
- `ViewSinkPartitionMismatchRegression`: a crafted catalog with mismatched partitions halts the
  apply.

Interaction matrix (op × entity × timing):
- **Ops:** CreateView; AlterView (fresh sink id); DropView; GetViewRow; Produce; Subscribe;
  Fetch; ListOffsets; AddPartitions; boot replay.
- **Entities:** sink; source; view on a co-partition-group source; transient source; single and
  multi worker; single and multi node.
- **Timings:** before plan delivery; after; during AlterView swap; after drop; across restart.

---------------------------------------------------------------------------------------------------

## Bench gates (both parts)

| Bench | Gate |
|---|---|
| `ViewTableBench` / new `BM_GetViewRowServe` with lease and barrier checks | ≤ +30 ns vs baseline; 0 allocations |
| e2e GetViewRow (k3s) | p50 ≤ 50 µs, p99 ≤ 200 µs |
| produce→deliver (MultiLeaderProduceBench) | no regression > 2 % (the lease adds no produce-path work) |
| Worker idle pass with N views | sink partitions add 0 replica services (Part B) |
| consensus duty pass (ConsensusAgentBenchmark) | + lease request/grant ≤ 1 µs per 20 ms tick |

## Implementation steps (for Sonnet)

1. **B core.** `TopicKind::ViewSink` with no replicas, logs or assignment. Delete
   `coLocateViewWithSource`. Route GetViewRow by source. Add the partition invariant, the
   metadata leader synthesis and TopicDirReconcile handling. No-move, plane, boot and mismatch
   regressions.
2. **B refusal.** The gateway and worker-ingress `InvalidRequest` for every data-plane op on a sink
   id. Refusal regression.
3. **A clock + protocol.** `LeaseClock` (BOOTTIME), templates 62/63 (encode, adapter, handler), the
   controller `QL` computation, and grants added to `leaderHeardNs_`. Unit tests with injected clocks
   and drift extremes.
4. **A broker side.** The `ReadLease` state machine, the `deliveredMetaPos` gate and
   `WorkerOp::ReadLease` on the FIFO op ring. The worker lease check in `executeGetViewRowWire_`.
   FIFO and suspend regressions.
5. **A new-leader gate.** `Fenced/Serving` in the view instance, the proof via the ex-leader fetch
   epoch, `waitUntil`, LeaderChange `predecessorFenced`, and the new-controller first-`W` rule.
   Zombie, stale-controller and rebalance regressions.
6. **A fold barrier + client epoch.** The held-view-read list (fixed capacity) and `knownLeaderEpoch`
   on GetViewRow (broker plus rune-client). Read-your-write regression.
7. **Checkpoint.** The full `*Regression*` under ASan+UBSan and TSan, the bench gates, and the k3s
   e2e with iptables partitions (zombie, minority controller, rebalance).

## Decisions made

- Lease over read-index. The lease is controller-granted and quorum-capped. Read-index via
  followers is unsound under ISR election.
- `ρ = 1 %`, `L = 200 ms`, effective about 120 ms through `QL`. `W = H·(1+ρ)²` ≈ 153 ms.
- The lease clock is `CLOCK_BOOTTIME`, not the duty clock.
- Failover is marked `predecessorFenced`, so failover adds no view-read downtime. Rebalance uses the
  demotion proof.
- The fold barrier and the client epoch fence are included, for read-your-writes and to stop
  zombie revisits.
- A sink partition must mirror its source's partition count and partitioner (halt on mismatch).

## B.6 Needs a user decision

The **replica-less sink** removes the sink's log and replicas entirely. It is safe today, because
nothing writes or reads the sink log. It does, however, close the door on a future "subscribe to a
view's change stream" built on the sink topic. If that feature is planned, the fallback is §B.4:
keep sink replicas, co-locate them from birth, and refuse Produce only. **Default if unanswered:
replica-less.**
