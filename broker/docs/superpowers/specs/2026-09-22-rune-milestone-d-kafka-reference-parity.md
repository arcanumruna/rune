# Milestone D — Kafka (KRaft) Reference & Edge-Case Parity

> **STATUS: REFERENCE.** Distilled from a source read of Apache Kafka
> **v4.5 (KRaft era)** at `~/Projects/kafka`. Companion to the D design
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

---

# Part 2 — Second-pass deep dive (what the first pass missed)

A deeper source read (metadata apply pipeline, broker hot path, and the correctness
that a leadership *move* triggers) surfaced items the first pass didn't cover.
**Highest-value theme: because D moves leadership routinely, the machinery that runs
on every handoff — producer-state rebuild, follower truncation, HW/maximalIsr — must
be first-class, not incidental.**

## 13. Leadership-move correctness (D triggers this on every handoff)

### 13a. Producer/EOS state is LOG-DERIVED, not shipped
Producer `.snapshot` files are **not** replicated. Followers update
`ProducerStateManager` from replicated batches/markers via `appendAsFollower→update`
(`UnifiedLog.java:1266`); on becoming leader / after truncation the new leader
**rebuilds producer state by replaying the committed log** (`rebuildProducerState`→
`loadProducersFromRecords`, `UnifiedLog.java:2540-2616`). Snapshots are a local
recovery optimization only. Dedup window = last 5 batches
(`ProducerStateEntry.NUM_BATCHES_TO_RETAIN=5`), matching `max.in.flight=5`; dup
detection by exact `(firstSeq,lastSeq)` returns the original offsets (silent re-ack,
no exception); `OutOfOrderSequence` only for `AppendOrigin.CLIENT` (replication is
not seq-validated). **Truncation + rebuild is one atomic step** under lock:
`truncateTo` → `deleteProducerSnapshots` → `rebuildProducerState(targetOffset)`
(`UnifiedLog.java:2366-2383`).
- **bug prevented:** duplicate accepted or valid sequence rejected after failover;
  wrong LSO for read_committed; zombie writer un-fenced on the new leader.
- **Rune:** *already has this* — Milestone C-B TX-2b: PSM failover, HW-bounded
  rebuild-from-log on Follower→Leader; LeaderEpochCache (KIP-101). D just exercises
  it **per-partition on every handoff**. Requirement: leadership move must rebuild
  the dedup window (≥ max in-flight), last-seq, and producer/coordinator epochs from
  the committed prefix the new leader already holds, atomically with truncation.

### 13b. Follower truncation = epoch-boundary protocol (KIP-101/279/595)
`LeaderEpochFileCache` = `TreeMap<epoch, startOffset>`; `endOffsetFor(epoch)` =
**start offset of the next-higher epoch** (`LeaderEpochFileCache.java:284-328`) — an
epoch's *end* is the successor's *start*, which is what makes "same offset, different
data" detectable. On becoming follower, truncate to `min(followerEpochEndOffset,
leaderEndOffset, followerLEO)` (`AbstractFetcherThread.getOffsetTruncationState:604-655`);
if the leader answers with a *lower* epoch than asked (it was itself a truncated
follower), do an **intermediate truncation and re-query** — the KIP-279 multi-epoch
walk-back. **Re-check under lock that the leader epoch didn't change during the RPC**
(`:213-224`). Modern path (KIP-595) folds this into Fetch: follower sends
`lastFetchedEpoch`; leader returns an empty batch + `divergingEpoch` when
`epochEndOffset.leaderEpoch < fetchEpoch || endOffset < fetchOffset`
(`Partition.scala:1403`); follower truncates from the response **without advancing
watermarks** — zero extra round-trips, no window to build on divergent data.
- **bug prevented:** KIP-101 divergent-tail retention; KIP-279 mistruncation when the
  new leader was a truncated follower; the append-past-divergence race.
- **Rune:** the truncation-on-handoff protocol is **mandatory**; HW-only truncation
  silently keeps divergent tails. Reuse the existing LeaderEpochCache; add the
  epoch-boundary query + `min(...)` + multi-epoch loop + leader-epoch-recheck.

### 13c. HW / LEO / maximalIsr invariants across handoff
- HW advances to **min LEO over `maximalIsr`**, and **not at all while under
  min-ISR** (`Partition.maybeIncrementLeaderHW:1012-1055`).
- **`maximalIsr`** is the *safe superset*: pending **expand** = committed ISR ∪
  {new replica} (so acks/HW already require the joiner); pending **shrink** = the
  **old committed ISR**, NOT the shrunk target (HW stays gated on the full set until
  the controller confirms). Using the target ISR early would ack data that a later
  leader might not have (`PendingExpandIsr`/`PendingShrinkIsr`).
- **A new leader keeps its checkpointed HW and only ever increments it**; it sets
  `leaderEpochStartOffset = LEO` on election (gating ISR rejoin until a follower
  catches up *within the new epoch*) and refuses client reads while
  `leaderEpochStart > HW` (`OFFSET_NOT_AVAILABLE`) (`makeLeader:590-688`). A follower
  clamps HW to `min(leaderHW, ownLEO)` and appends records *before* applying HW.
- **bug prevented:** exposing uncommitted post-election tail; losing acked data when
  a pending shrink is rejected and a shrunk-out replica is later elected.
- **Rune:** adopt the two-state ISR (committed vs pending) + maximalIsr for HW/acks;
  new-leader HW discipline; the `leaderEpochStart>HW` read fence.

## 14. Metadata apply pipeline & controller performance (MSM engineering)

- **Apply off the request path:** a single-threaded loader queue applies committed
  deltas; the read cache is **one `volatile` immutable image swapped with a single
  store** (`KRaftMetadataCache.currentImage`, `:74/:486`); readers capture the ref
  once (`MetadataImage image = currentImage;`) — no lock shared with the applier.
- **Publisher fault isolation:** each publisher call is wrapped per-publisher and
  routed to a FaultHandler, *continuing* to the next (`MetadataLoader.java:362-369`);
  deliberately choose fatal (controller write-side `replay`,
  `QuorumController.java:828-834`) vs non-fatal (broker apply-side).
- **MVCC/timeline reads:** `TimelineHashMap`/`SnapshotRegistry` give O(1)
  current-version reads + per-entry deltas + `revert(epoch)` (discard uncommitted
  speculation) + `deleteSnapshotsUpTo` (bound memory) — the controller mutates
  speculatively yet serves consistent reads at a committed offset without locking.
- **Structural sharing:** images are immutable, backed by persistent maps; applying a
  delta reuses untouched topics/partitions by reference → next image is O(changed),
  not O(total) (`TopicsDelta.apply`).
- **Batching bounds:** `maxRecordsPerBatch≈10k`; atomic results over the cap **throw**
  (never sliced); non-atomic are split into per-batch-snapshotted chunks
  (`QuorumController.appendRecords:889-942`); `BoundedList` caps records/op; also a
  hard **byte** ceiling (`MAX_BATCH_SIZE_BYTES≈8MB`). A mass reassignment must be
  bounded, not one giant batch.
- **Version-gate assignment records:** metadata-version-derived wire version +
  `handleLoss` on unsupported fields; refuse to finalize the version until **every**
  broker+controller advertises support; downgrade refuses if it would drop metadata
  (`FeatureControlManager`, `MetadataVersion.partitionChangeRecordVersion`).
- **Snapshots are FULL + self-throttled** (20 MiB / 1 h defaults, own queue,
  single-in-flight debounce); load **prunes** state absent from the snapshot
  (`finishSnapshot`) — else removed assignments resurrect.

## 15. Broker hot-path techniques (latency)

- **acks=all via purgatory + hierarchical timing wheel** (O(1) insert, one reaper),
  completion *signaled* by the HW-advance event calling `checkAndComplete(partitionKey)`
  — not thread-per-request; watcher map sharded (512) to cut lock contention;
  completion guarded by volatile+lock so timeout/data-arrival race safely.
- **Network↔work decoupling:** bounded request queue; network threads only do I/O and
  mute the connection while in-flight (per-connection ordering); slow work parks in
  purgatory; continuations re-enter via a priority callback queue + selector wakeup.
- **Incremental fetch sessions (KIP-227):** send only changed-partition deltas;
  broker caches per-partition cursor; cap+shard the session cache, prioritize
  internal (follower) sessions on eviction. Essential at high partition counts.
- **Fetch-from-follower (KIP-392):** route a reader to a follower only if it is
  **in-ISR and already holds the offset**, serve **capped at committed HW**, with an
  `(leaderEpoch, endOffsetForEpoch)` probe so the reader detects truncation after a
  handoff. **Directly the read path for materialized-view read-replicas.**
- **Zero-copy egress (`transferTo`/sendfile) + page-cache ingress**, fsync decoupled
  (durability from replication). TLS forfeits sendfile (user-space encrypt copy).

## 16. Other correctness edges

- **Topic deletion** = one atomic `RemoveTopicRecord`; brokers process **deletes
  before creates** (safe same-name recreate); an offline broker's stale log becomes a
  **stray replica** quarantined to `-stray` (topic ids never reused). Rune: same
  ordering + stray-quarantine + non-reusable topic/partition ids.
- **Reassignment cancel/revert** reverts to `originalReplicas` (strip *adding*, keep
  *removing*), ISR minus adding; `unclean` is computed from **set membership only,
  not liveness** — so a "clean" cancel can still yield a *transiently leaderless*
  partition if surviving originals are down. Rune: decide the policy explicitly
  (reject / allow-leaderless / allow-unclean) and where liveness is checked.
- **Controlled shutdown is availability-first:** min-ISR is a **produce-time** gate,
  **not** an election/shutdown gate — the controller will shrink ISR below min-ISR or
  go `NO_LEADER` and still let the broker leave, but **never** does a silent unclean
  election. Rune: match this (rely on produce-side min-ISR) or explicitly choose to
  block moving the last in-sync leader — do not assume min-ISR gates election.
- **Multi-record atomicity:** a multi-record op (create-topic + N partition records)
  is one atomic batch that must fit one replication batch or it **fails hard** — no
  partial apply across batch boundaries; watch the byte ceiling separately from count.

## 17. Second-pass parity additions checklist
1. Producer/EOS state **rebuilt from the replicated log** on every leadership move,
   atomic with truncation (reuse Rune's PSM rebuild-from-log).
2. Follower **epoch-boundary truncation** protocol mandatory on every handoff
   (endOffsetFor + min(3) + multi-epoch walk-back + leader-epoch recheck; ideally the
   diverging-epoch-in-fetch form).
3. **maximalIsr** two-state ISR + HW=min-LEO-over-maximalIsr + under-min-ISR gate;
   new-leader keeps checkpointed HW, only increments; `leaderEpochStart>HW` read fence.
4. MSM apply **off the hot path** (single-thread loader + volatile immutable-image
   swap); **publisher fault isolation**; **MVCC/timeline** consistent reads + revert +
   bound; **structural sharing**; **batching bounds**; **version-gated** records;
   **full self-throttled snapshots that prune on load**.
5. Hot path: **purgatory + timing wheel** for acks=all; **incremental fetch**;
   **fetch-from-follower** for view read-replicas; zero-copy + page-cache.
6. Topic-delete ordering + stray quarantine; reassignment-cancel liveness policy;
   controlled-shutdown vs min-ISR policy; multi-record atomic-batch bound.
