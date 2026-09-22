# Milestone D — Kafka (KRaft) Reference & Edge-Case Parity

> **STATUS: REFERENCE.** Distilled from a source read of Apache Kafka
> **v4.5 (KRaft era)** at `/Users/vradu/Projects/kafka`. Companion to the D design
> spec. Purpose: mirror Kafka's proven controller-assigns mechanics and, critically,
> its **edge-case handling**, so the Rune implementation does not reintroduce bugs
> Kafka already solved. Every rule carries a Kafka `file:line` and the bug it
> prevents. Abbreviations: `RCM` = `ReplicationControlManager.java`, `PCB` =
> `PartitionChangeBuilder.java`, `CCM` = `ClusterControlManager.java`, `BHM` =
> `BrokerHeartbeatManager.java`, `BHT` = `BrokerHeartbeatTracker.java`, all under
> `metadata/src/main/java/org/apache/kafka/controller/`; `PR` =
> `metadata/.../metadata/PartitionRegistration.java`.

## 0. The one architectural fact

All election/ISR/assignment decisions are made **centrally in the controller** and
materialized as a single record; **no broker ever mutates its own leadership/ISR
state directly.** The controller is the sole authority; brokers derive their role
from committed metadata. Rune must replicate this: the MSM/controller is the only
writer; the bridge only *applies* committed records. (`PCB`, `RCM` throughout.)

---

## 1. Write → commit → apply lifecycle (controller + MSM)

- **Single-writer via controller epoch.** The active controller checks
  `curClaimEpoch` before every write; a standby has `-1` and rejects writes;
  Raft `prepareAppend(controllerEpoch, records)` fences a stale controller at the
  log (`QuorumController.java:788-791, 822`). → *Rune:* fence assignment writes by
  the consensus/controller epoch; a demoted controller must be unable to append.
- **Apply only committed (stable-offset) state.** The active controller applies
  records in-memory *optimistically before* commit, but brokers/standbys act only
  on records ≤ `lastStableOffset`; in-flight/transactional records are invisible
  (`QuorumController.java:827, 990-1002`; `OffsetControlManager` stable-offset cap
  `:319-333`). → *Rune:* the bridge applies only **committed** assignment records —
  never optimistic ones — so a rolled-back assignment never reaches a node.
- **Deterministic replay + snapshots.** Every controller applies the same records
  in the same order; recovery = load snapshot (reset state, replay from
  `lastContainedOffset+1`) then tail (`QuorumController.java:1023-1065`;
  `MetadataProvenance`). → *Rune:* MSM apply is a pure function of the record
  stream; snapshot at stable offset bounds recovery.
- **Broker apply contract (`LocalReplicaChanges`).** From the metadata delta each
  broker derives, per partition: **delete** (was a replica, no longer),
  **electedLeader** (is leader **and `leaderEpoch` changed**), **leader-update**
  (is leader **and `partitionEpoch` changed** → ISR/replica change),
  **follower** (not leader **and `partitionEpoch` changed**)
  (`image/TopicDelta.java:176-226`). → *Rune:* the bridge computes the same four
  cases from the assignment delta, keyed on which epoch moved.
- **Bug this prevents:** a node acting on an assignment that later rolls back;
  divergence between controllers; unnecessary follower-state resets.

---

## 2. TWO epochs — leaderEpoch vs partitionEpoch (the highest-value rule)

Applied atomically in one place: `PartitionRegistration.merge()` (`PR:238-277`).
- **`partitionEpoch` — bumped on EVERY applied change, unconditionally** (`PR:274`).
  The version / optimistic-concurrency token.
- **`leaderEpoch` — bumped ONLY when the record sets the `leader` field**
  (`PR:254-260`), i.e. on an actual leader change *or* a forced bump (below).
  Sentinels matter: `NO_LEADER_CHANGE = -2` skips the bump; `NO_LEADER = -1`
  (going leaderless) **is** a leader change and **does** bump (`LeaderConstants`).
- **Forced leaderEpoch bump without a leader change** in two cases: (a) a replica
  was *removed* from the replica set during reassignment
  (`PCB:364-374`); (b) *legacy* pre-3.6 ISR-shrink workaround only — **post-3.6 an
  ISR shrink does NOT bump leaderEpoch** (`PCB:382-403`,
  `MetadataVersion.isLeaderEpochBumpRequiredOnIsrShrink`).
- **Invariant:** if `leaderEpoch` increases, `partitionEpoch` also increases;
  `partitionEpoch` never lags (`PCB:359-363`).
- **Bug this prevents:** stale followers/consumers reading from a deposed leader
  because leaderEpoch didn't advance; AlterPartition races because partitionEpoch
  didn't advance on every mutation.
- **Rune:** adopt BOTH epochs. `leaderEpoch` fences the data plane (followers/
  clients reject a stale leader); `partitionEpoch` is the optimistic-concurrency
  version for ISR-change requests. Our draft named only one — this is corrected in
  the spec.

---

## 3. Leader election priority (don't churn)

`electPreferredLeader` (`PCB:227-251`) / `electAnyLeader` (`PCB:256-284`), core
predicate `isValidNewLeader` = `(targetIsr.contains(r) || (isr.empty && elr.contains(r))) && isAcceptableLeader(r)` (`PCB:318-322`):
1. Preferred: `replicas[0]` if valid; else keep current leader if still valid;
   else first other valid replica; else last-known-leader (ELR); else `NO_LEADER`.
2. Online/unclean: current leader if valid; else first valid replica; else
   last-known-leader; else (UNCLEAN only) first *acceptable* replica ignoring ISR
   (data-loss path); else `NO_LEADER`.
3. **Keep the current leader if still valid** — only write a new leader when it
   actually differs (`PCB:324-356`).
- **Bug this prevents:** needlessly bumping leaderEpoch (invalidating every
  follower's fetch state) when the existing leader was fine.
- **Rune:** re-run election over the *proposed* ISR; never trust a leader that
  isn't in ISR (or ELR when ISR empty); prefer keeping the current valid leader.

---

## 4. AlterPartition (ISR shrink/expand) fencing — exact order

`validateAlterPartitionData` (`RCM:1272-1376`) checks, **in this order**:
1. partition null → `UNKNOWN_TOPIC_OR_PARTITION`.
2. `req.leaderEpoch > cur` **or** `req.partitionEpoch > cur` → `NOT_CONTROLLER`
   (this controller is behind).
3. `req.leaderEpoch < cur` → `FENCED_LEADER_EPOCH` (request from a **deposed** leader).
4. `brokerId != partition.leader` → `INVALID_REQUEST` (only the leader alters ISR).
5. `req.partitionEpoch < cur` → `INVALID_UPDATE_VERSION` (lost the optimistic race).
6. ISR ⊄ replicas / dups / unsorted → `INVALID_REQUEST`.
7. **`newIsr` must contain the leader** (`RCM:1337-1345`).
8. any ISR member not registered / fenced / in controlled-shutdown / epoch-mismatch
   → `INELIGIBLE_REPLICA` (`RCM:1388-1408`).
- Ordering subtlety: epoch checks come **before** the not-the-leader check, so an
  old leader gets `FENCED_LEADER_EPOCH`, not `INVALID_REQUEST`.
- **Bug this prevents:** a partitioned-away old leader corrupting the ISR after a
  new leader was elected (split-brain ISR).
- **Rune:** replicate this precedence exactly; **ISR always contains the leader**;
  reject ineligible ISR members.

---

## 5. Unclean election — opt-in, marked, refused if unnecessary

- Gated per topic by `unclean.leader.election.enable`; only then does the builder
  use `Election.UNCLEAN` (`RCM:1155-1157`).
- The unclean pick forcibly sets **`ISR = [newLeader]`** and
  `leaderRecoveryState = RECOVERING` — *"This can result in data loss!"*
  (`PCB:344-352`).
- Admin `electLeader` returns `ELECTION_NOT_NEEDED` if the partition already has a
  leader (`RCM:1657-1660`).
- **Bug this prevents:** silent data loss from electing a stale out-of-ISR replica
  without an explicit opt-in; and losing the "log may be truncated" signal.
- **Rune:** unclean election is a per-topic opt-in, sets a recovery flag, resets ISR
  to the single new leader. Default off.

---

## 6. ELR — Eligible Leader Replicas (KIP-966) [optional, recommended later]

Lets ISR shrink below `min.insync.replicas` without discarding the identity of the
last-in-sync replicas, so a later election can be **clean** instead of unclean.
- Populate ELR only while under-replicated; clear once `ISR ≥ minISR`; demote
  ex-ISR (minus unclean-shutdown replicas) into ELR (`PCB:532-563`).
- ELR members are electable **only when ISR is empty**, and such an election is
  treated as clean (`PCB:320, 330-335`).
- `lastKnownElr` (single slot) stores the last leader on the first leaderless
  transition, consumed by `canElectLastKnownLeader` for balanced recovery
  (`PCB:286-316, 491-500`).
- **Bug this prevents:** an unnecessary lossy unclean election when a replica that
  *was* in sync could be re-elected cleanly.
- **Rune:** ELR is **not** required for Phase 1–3; note it as the principled
  data-loss-avoidance upgrade. Without it, "under-min then all-ISR-lost" forces
  either unclean election or unavailability — an acceptable Phase-1 stance.

---

## 7. No-ISR-survivor path

Election returns `NO_LEADER` (`-1`); `tryElection` writes it (counts as a leader
change → leaderEpoch bumps), partition goes offline/unavailable; with ELR, ISR may
persist empty and `lastKnownElr := [oldLeader]`. Recovery re-runs election over
`partitionsWithNoLeader()` when a broker unfences (`RCM:1464-1471`).
- **Bug this prevents:** a phantom leader, or losing the last-leader identity so the
  partition can never recover cleanly.
- **Rune:** on no survivor, mark the partition leaderless (bump leaderEpoch),
  preserve last-known-leader, retry election on relevant broker unfence. Never risk
  committed loss to restore availability unless unclean is explicitly enabled.

---

## 8. Membership, liveness & fencing

- **Split soft vs hard state.** Heartbeats/timeouts are **soft** state on the active
  controller, **never in the log**; registration/fencing are **hard** log records
  (`BHM:44-57`). → *Rune:* keep heartbeats out of consensus; rebuild liveness on
  controller failover and accept one session-timeout of detection latency
  (`CCM:314-326`).
- **Broker epoch = registration log offset** → cluster-wide monotonic; epoch-check
  every state-mutating request (`QuorumController.java:2044-2046`; `CCM:797-807`).
- **Zombie guard:** same incarnation-id re-register = idempotent amend (keep epoch);
  different incarnation while the incumbent session is valid = **rejected**
  (`CCM:369-378`). → *prevents two processes acting as one broker id.*
- **BrokerControlState:** FENCED / UNFENCED / CONTROLLED_SHUTDOWN / SHUTDOWN_NOW;
  **unfence only after metadata catch-up** (`BHM:402-479`, catch-up gate `:414-428`).
- **Fence one broker at a time**, serialized so each fence's effects propagate
  before the next (`RCM:1759-1787`) — the correlated-failure throttle.
- **Controlled (graceful) shutdown moves leadership off while the broker is still
  alive & in-ISR**, and only grants "you may stop" once every live broker consumed
  the moves (`controlledShutdownOffset ≤ lowestActiveOffset`) (`RCM:1484-1493`;
  `BHM:456-474`). Distinct from hard-failure fencing.
- **Overload safety valve:** under controller backlog, still record liveness but
  suppress state transitions (`RCM:1738-1746`) — avoids mass false-positive fencing
  when already stressed.
- **Clock-rollover guard** in expiry math (`BHT:131-133`) — never `now-last>timeout`
  naively.
- **`isAcceptableLeader`** = registered ∧ ¬fenced ∧ ¬controlled-shutdown ∧
  online-log-dir, minus brokerToRemove/uncleanShutdown, plus a brokerToAdd
  unfence-race override; **strip removed brokers from targetIsr**, not just from
  acceptability (`RCM:2099-2101, 2133-2134, 2549-2572`).

---

## 9. The multiplied-concurrent-failover answer (our Phase-4 worry)

When one node dies, many partitions reassign at once. Kafka's mechanisms to copy:
- **Reverse index `broker → partitions-it-leads`** (`BrokersToIsrs`), a bit-packed
  int (leader flag in the high bit) so a death costs **O(partitions-on-that-broker),
  not O(all partitions)** (`BrokersToIsrs.java:105, 188-191`). *Also a
  ULL/zero-alloc-friendly structure — fits decision #8.*
- **Fence one broker at a time** (`RCM:1779-1781`).
- **Batch + bound** the resulting partition-change records (`BoundedList` /
  `maxRecordsPerBatch`), and **skip no-op changes** (`build().ifPresent`,
  `RCM:2137`) — a single failure can produce thousands of reassignments; cap and
  paginate so it can't stall the log.
- **Rune:** build the reverse index; fence-one-at-a-time; bounded batched
  assignment records; emit records only where leadership/ISR actually moved.

---

## 10. Placement & the co-location gap (validates our co-partition groups)

- **StripedReplicaPlacer** spreads replicas by rack-first striped round-robin with
  per-topic randomization + shuffle-on-cycle (`StripedReplicaPlacer.java:337-388`).
- **Preferred leader = `replicas[0]`;** an `imbalancedPartitions` set; a periodic
  balance task **capped at 1000 elections/cycle**; preferred election only if the
  preferred replica is **in ISR** (`RCM:150, 1805-1846, 1657-1680`). Min-churn, no
  forced moves — anti-storm.
- **Reassignment is staged (add → wait-for-ISR → remove), not atomic**, with a
  guarded revert (unclean only if configured) (`RCM:2256-2320`;
  `PartitionReassignmentReplicas.java`, `PartitionReassignmentRevert.java`).
- **CONFIRMED: Kafka has NO cross-topic co-location.** Each topic calls the placer
  independently with a fresh `RackList` and fresh random offset; `PlacementSpec`
  carries no topic/group context (`RCM:794-798`;
  `StripedReplicaPlacer.java:430`; `PlacementSpec.java:26`). So `topicA-p5` and
  `topicB-p5` are *not* co-located even with identical counts.
- **Rune:** our **co-partition group + co-location-aware Balancer is a deliberate
  extension Kafka lacks** — justified. Assign same-index partitions of a group to
  one node (seed placement by group, not per-topic); move a group as a unit on
  rebalance; reject a group config with mismatched partition counts or without live
  brokers to satisfy RF; borrow Kafka's 1000/cycle cap + preferred-in-ISR + staged
  reassignment discipline.

---

## 11. Client metadata & routing

- **Per-partition metadata** returns `leaderId`, **`leaderEpoch` (v7+)**,
  replicas, ISR, offlineReplicas (`MetadataResponse.json`).
- **The client staleness rule (ground truth):** accept a metadata update for a
  partition **iff `newEpoch ≥ lastSeenEpoch`**; a lower epoch is *ignored*, keeping
  cached info — this is what prevents routing to a demoted leader
  (`clients/.../Metadata.java:584-596, 277-287`). Epochs reliable only at metadata
  **v9+** (`:279-281`).
- **Error → refresh → reroute:** `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` /
  `UNKNOWN_LEADER_EPOCH` → `metadata.requestUpdate()` → full refresh → re-enqueue
  batch → route to new leader (`Sender.java:722-755`).
- **Zero-RTT reroute:** the error response carries the **`currentLeader`
  {id, epoch}** so the client updates its cache without waiting for a full metadata
  round-trip (`Sender.java:724-728`).
- **Backoff** on equivalent (no-progress) responses via exponential backoff —
  prevents refresh storms (`Metadata.java:154-169`). Proactive refresh at
  `metadata.max.age.ms` (default 5 min).
- **Producer partitioner:** `murmur2(key) % numPartitions` computed **client-side**
  (`BuiltInPartitioner.java:413-415`); keyless = sticky adaptive.
- **Rune:** our Metadata response carries `leaderEpoch`; client caches
  `lastSeenLeaderEpochs` and **rejects lower-epoch updates**; our NotLeader/REDIRECT
  response carries `{newLeaderId, leaderEpoch}` for zero-RTT reroute; exponential
  backoff on no-progress; keyed hash partitioning client-side (zero-alloc flat-array
  lookup per decision #8).

---

## 12. Parity checklist (what Rune MUST implement to match Kafka correctness)

1. Controller = sole writer, fenced by controller epoch; bridge applies **committed
   only**.
2. **Two epochs:** partitionEpoch every change; leaderEpoch only on leader-field set
   (incl. `NO_LEADER=-1`; excl. `NO_LEADER_CHANGE=-2`); forced bump on replica
   removal.
3. AlterPartition fencing precedence: `NOT_CONTROLLER` (too-high) → `FENCED_LEADER_EPOCH`
   (leaderEpoch too-low) → `INVALID_REQUEST` (not leader) → `INVALID_UPDATE_VERSION`
   (partitionEpoch too-low) → ISR validity → **ISR-contains-leader** → `INELIGIBLE_REPLICA`.
4. Election: keep current valid leader (don't churn); clean leader must be in ISR
   (or ELR when ISR empty) and acceptable; re-run election on the proposed ISR.
5. Unclean = per-topic opt-in; sets `ISR=[leader]` + RECOVERING; refused if a leader
   already exists.
6. No-survivor → `NO_LEADER` + leaderEpoch bump + preserve last-known-leader; retry
   on unfence.
7. Membership: soft liveness off-log; broker epoch = registration offset;
   incarnation zombie guard; unfence only after catch-up; **fence one at a time**;
   graceful ≠ hard shutdown; overload safety valve; clock-rollover guard.
8. Multiplied failover: reverse index (bit-packed), bounded batched records, skip
   no-ops.
9. Placement/Balancer: preferred=`replicas[0]`; imbalance set; **1000-election/cycle
   cap**; preferred-in-ISR only; staged reassignment; **co-partition-group
   co-location (our addition)**.
10. Client routing: `leaderEpoch` in metadata; client **rejects lower-epoch
    updates**; error carries `currentLeader` for zero-RTT reroute; exponential
    backoff; client-side keyed hashing.

**Deliberate divergences from Kafka:** (a) co-partition-group co-location (Kafka has
none); (b) ELR deferred (accept unclean-or-unavailable in early phases); (c)
faithful-port constraint — reuse Aeron's encoders/ring buffers, and hold the 100µs
p99 + near-zero-allocation bar (decision #8) on the hot path, which Kafka (JVM) does
not target.
