# Milestone C-A — Consumer Groups + Offset Commit/Fetch (design)

> **STATUS: proposed design, ready for review.** First sub-project of the rest of
> Milestone C (A = consumer groups, B = exactly-once/transactions, C = log
> compaction). Sequencing chosen: A → B → C.

## 1. Purpose & motivation

A consumer group is a **horizontally-scalable, fault-tolerant, resumable logical
consumer**. It provides what a bare `Subscribe` cannot:

1. **Scale-out consumption (work distribution).** A topic's partitions are divided
   among group members so each partition is consumed by exactly one member. More
   members → more parallelism, up to the partition count (competing-consumers /
   queue semantics).
2. **Automatic failover.** A member crash reassigns its partitions to survivors
   (rebalance) — the workload self-heals with no manual intervention.
3. **Resumable progress.** The group's committed offsets are durable; after
   crash / restart / rebalance, consumption resumes where the group left off.
   Progress is tracked *per group*.
4. **Pub/sub + queue in one model.** Different groups each get the full stream
   independently (pub/sub fan-out); within a group, members share the partitions
   (queue). One topic feeds many independent, internally load-balanced subscribers.

This also lays the groundwork for exactly-once read-process-write (sub-project B)
and, later, workers-per-partition scale-out (Milestone D).

## 2. Scope & non-goals

**In scope (v1):**
- Group membership + eager rebalance (join / sync / heartbeat / leave).
- Broker-side partition assignment (range / round-robin).
- Push-based delivery to group members (extends existing push-subscribe).
- Durable offset commit / fetch on a replicated `__consumer_offsets` internal topic.
- Auto-commit (configurable interval) + explicit `commit()`; at-least-once delivery.
- Client consumer-group API + DR-E2E extension + benchmarks.

**Non-goals (deferred):**
- Cooperative-sticky (incremental) rebalance — eager only in v1.
- Exactly-once / read_committed / transactional offset commit — sub-project B.
- `__consumer_offsets` log compaction — sub-project C (simple retention until then).
- Hash-based multi-node coordinator placement — Milestone D (see §7 seam).
- Client-leader-computed assignment (Kafka's SyncGroup assignor upload) — broker
  computes assignment in v1.

## 3. Locked decisions (from brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Delivery model | **Push-based** (extend push-subscribe) | Keeps Rune's low-latency push ethos; reuses tested HW-bounded push + offset tracking |
| Coordinator placement | **Cluster leader = sole coordinator**; REDIRECT elsewhere | Matches current "leader owns all partitions"; behind a seam for Milestone D |
| Assignment | **Broker-side** (`GroupCoordinator::autoAssignAndSync`) | Broker must know assignments to push; avoids client-leader dance |
| Failover | **Durable offsets + rebuild membership on rejoin** | Kafka-faithful, lighter; offsets ride hardened acks=All replication |
| Rebalance protocol | **Eager (stop-the-world)** | Simplest correct protocol; cooperative-sticky is a clean later enhancement |
| Offset commit | **Auto-commit + manual**, at-least-once | Covers both common usage patterns |

## 4. Architecture: components & boundaries

### 4.1 Protocol (`rune-protocol`, templateIds 32–63)
New client request/response pairs (LE SBE, schema-versioned, faithful to existing
codec style + conformance vectors):
- `JoinGroup{groupId, memberId?, sessionTimeoutMs, subscribedTopics}` /
  `JoinGroupResponse{memberId, generationId, errorCode}`
- `SyncGroup{groupId, memberId, generationId}` /
  `SyncGroupResponse{assignment:[{topicId,partition}], errorCode}`
- `Heartbeat{groupId, memberId, generationId}` /
  `HeartbeatResponse{errorCode}`
- `LeaveGroup{groupId, memberId}` / `LeaveGroupResponse{errorCode}`
- `OffsetCommit{groupId, memberId, generationId, [{topicId,partition,offset,metadata?}]}` /
  `OffsetCommitResponse{[{topicId,partition,errorCode}]}`
- `OffsetFetch{groupId, [{topicId,partition}]?}` /
  `OffsetFetchResponse{[{topicId,partition,offset,errorCode}]}`
- `Assign`/`Revoke` egress control frame (broker → member) to signal an
  assignment change out-of-band from `SyncGroup` (used on rebalance/revoke).

Error codes reuse the `GroupCoordinator` set (kErrStaleGeneration,
kErrUnknownMember, kErrRebalanceInProgress, kErrDead) + a `kErrNotCoordinator`
(REDIRECT to the coordinator node).

### 4.2 `GroupCoordinationService` (broker, new thin layer)
- Owns per-group `GroupCoordinator` instances (already a full state machine:
  `join`, `syncGroup`, `heartbeat`, `leave`, `autoAssignAndSync`, `doWork`).
- Drives each coordinator's `doWork(nowNs)` on the duty cycle (single-writer,
  injected clock — same discipline as the gateway/consensus).
- Implements the six handlers (§4.1) by dispatching to the coordinator + the
  offset store.
- On rebalance-complete, **translates each member's assignment into push
  subscriptions** (§4.4).
- Runs only on the cluster leader; a request to a non-coordinator returns
  `kErrNotCoordinator` + the coordinator endpoint (reuse GB-4 REDIRECT).

### 4.3 `OffsetStore` re-backed on `__consumer_offsets`
- Today `OffsetStore` is a standalone flat-file log (replay-on-construction),
  **not replicated** → would lose offsets on coordinator failover.
- Re-back it onto a **replicated internal-topic partition Log** (`__consumer_offsets`),
  so each `OffsetCommit` appends via `PartitionReplica` and is replicated to the
  followers over Aeron. Kafka semantics preserved: commit overwrites `(group, topic,
  partition)`; a later lower offset is allowed.
  - **AS-BUILT (correction, 2026-09-16):** the offsets log runs at **`minIsr=1`
    (acks=Leader + async replication)**, NOT majority-durable acks=All. Rationale:
    avoids ISR-establishment latency on every autocommit and on a new leader. Trade:
    a hard leader-kill within the replication window rebuilds from a slightly stale
    committed offset → **bounded re-delivery (duplicate), never loss** — at-least-once
    holds (DR failover matrix 10/10). Making offset commits majority-durable (acks=All)
    is a future option if stronger "every acked commit is majority-durable" is wanted.
- On coordinator failover, the new leader **replays `__consumer_offsets`** to
  rebuild the committed-offset map (membership is rebuilt via rejoin).
- Compaction (keep latest per `(group, tp)`) deferred to sub-project C; simple
  retention until then.
- Internal topic bootstrap: created at cluster init (single partition in v1,
  co-located with the leader/coordinator; multi-partition + hashed placement is
  Milestone D).

### 4.4 Gateway push path (reused)
- A member's assigned partition becomes an **internal HW-bounded push-subscription
  registry entry** keyed by `(group, memberSession, topicId, partition)`, seeded
  from `OffsetStore.fetch(group, tp)` (or `auto.offset.reset = earliest|latest`
  when no commit exists).
- Delivery reuses the existing push machinery (HW-bound, offset tracking,
  back-pressure, reap-on-disconnect).
- **Eager rebalance** = revoke ALL of the group's push entries (stop pushes + send
  `Revoke` frames) → recompute → create new entries → resume each from its
  committed offset.

### 4.5 Client (`rune-client-cpp`)
- New API:
  ```
  client.subscribeGroup(groupId, {topics}, {autoCommit, autoCommitIntervalMs,
                                            sessionTimeoutMs, autoOffsetReset})
      -> GroupConsumer
  GroupConsumer: onAssigned(parts) / onRevoked(parts) / onRecords(recs)
                 commit() / commitSync()
  ```
- Background heartbeat cadence (< sessionTimeout / 3, Kafka-style).
- Auto-rejoin on `kErrRebalanceInProgress` / `kErrNotCoordinator` (follow REDIRECT).
- Auto-commit timer + manual commit; at-least-once (commit after `onRecords`
  returns in the auto case, or when the app calls `commit()`).
- Zero-alloc discipline on the heartbeat / record-dispatch / commit hot paths
  (reuse the wait-free patterns from DR-BUG-1).

### 4.6 DR-E2E extension (`dr-e2e/`)
New scenarios (parametrized matrices, same harness): multi-member
join→assign→consume→commit; rebalance on member join/leave; commit/fetch resume;
**coordinator-failover offset survival**; member-crash→reassignment;
at-least-once invariants (no committed-offset loss, bounded duplicate delivery).

## 5. Data flow (happy path)
1. Client → `JoinGroup`. Coordinator admits member → `PreparingRebalance` →
   returns `memberId` + `generationId`.
2. After the rebalance window, `autoAssignAndSync` computes assignment → `Stable`.
3. Client → `SyncGroup` → receives assigned partitions.
4. Broker creates push-subscription entries per assigned partition, seeded from
   committed offsets (§4.4).
5. Broker pushes HW-bounded `Records` per partition (existing push path).
6. App processes → auto-commit at interval or `commit()` → `OffsetCommit` →
   append to `__consumer_offsets` (acks=All) → `OffsetCommitResponse`.
7. Heartbeats sustain membership; `OffsetFetch` on (re)join resumes position.

## 6. Rebalance & failover
- **Membership change** (join / leave / heartbeat-timeout): eager rebalance —
  `PreparingRebalance` → revoke all (stop pushes + `Revoke`) → members rejoin →
  recompute → resume from committed offsets. Consumption pauses for the rebalance
  window; clients see `onRevoked` then `onAssigned`.
- **Coordinator (leader) failover:** consensus elects a new leader → new
  coordinator starts with empty in-memory group state but **replays
  `__consumer_offsets`** → members' heartbeats/pushes to the dead node fail →
  clients get `kErrNotCoordinator` / connection loss → rejoin new coordinator →
  rebalance → resume from committed offsets. Brief pause; **no committed-offset
  loss** (offsets rode acks=All replication).

## 7. Milestone-D seam
Coordinator placement is **leader-only in v1**, isolated behind a single
`coordinatorFor(groupId) -> nodeId` lookup (returns the cluster leader today).
When Milestone D lands, this becomes `hash(groupId) → owning node`, and
`__consumer_offsets` becomes a multi-partition distributed internal topic. No
other component encodes the "leader owns all" assumption.

## 8. Error handling / edge cases
- Stale-generation / unknown-member / rebalance-in-progress → typed error codes →
  client rejoins.
- At-least-once bound: after rebalance/failover, redelivery from last commit →
  **bounded duplicate, no loss**. Apps must be idempotent or use EOS (sub-project B).
- Session-timeout eviction → rebalance. `OffsetCommit` with wrong generation →
  rejected. `auto.offset.reset` (earliest|latest) applies only when no commit
  exists for a `(group, tp)`.
- Commit for a partition the member no longer owns (post-rebalance) → rejected
  with stale-generation.
- Empty group / last member leaves → group goes `Empty` (retains offsets), `Dead`
  after retention.

## 9. Testing & benchmarks
- **Unit:** handlers; `OffsetStore`-on-replicated-log (commit→replicate→replay
  rebuild); assignment→subscription translation; revoke/resume. (GroupCoordinator
  state machine already covered by existing tests.)
- **Integration:** multi-member join→assign→consume→commit; rebalance on
  join/leave; commit/fetch resume after restart; coordinator-failover offset
  survival.
- **DR-E2E:** rebalance-under-failover, member-crash→reassignment,
  committed-offset resume across leader-kill, at-least-once invariants.
- **Benchmarks (100µs p99 gate on hot paths):** join/rebalance latency,
  `OffsetCommit`/`OffsetFetch` p99, zero per-op alloc on heartbeat / record
  dispatch / commit paths.

## 10. Ticket breakdown (proposed)
- **CG-1** — protocol messages + codecs + conformance vectors (templateIds 32–63).
- **CG-2** — `OffsetStore` re-backed on replicated `__consumer_offsets` (commit /
  fetch / fetchAll over `PartitionReplica`, acks=All, replay-rebuild).
- **CG-3** — `GroupCoordinationService` + six handlers, driven on the duty cycle,
  leader-only + REDIRECT.
- **CG-4** — assignment→push-subscription translation + resume-from-committed +
  eager revoke/reassign.
- **CG-5** — client consumer-group API (`subscribeGroup`, callbacks, heartbeat,
  auto/manual commit, auto-rejoin).
- **CG-6** — DR-E2E consumer-group scenarios (rebalance-under-failover, etc.).
- **CG-7** — benchmarks + full-area Opus review to CLEAN.

## 11. Open questions (resolve during planning)
- `__consumer_offsets` bootstrap: create eagerly at cluster init vs lazily on
  first commit? (Lean eager, single partition v1.)
- `Assign/Revoke` as a dedicated control frame vs reusing `SyncGroupResponse`
  round-trips — pick one for the push model.
- Auto-commit exactly-when semantics (before vs after `onRecords`) to keep the
  at-least-once guarantee crisp.
- Heartbeat transport: dedicated `Heartbeat` message vs piggyback on the existing
  keepalive — keep separate for generation/rebalance signaling.
