# Milestone D — Distributed Partition Leadership — Design

**Status:** Approved for planning (brainstormed 2026-09-22)
**Repos:** rune-broker (primary), rune-client-cpp (client routing)
**Supersedes:** `2026-09-15-rune-milestone-d-distributed-partition-leadership-DRAFT.md` (idea capture)
**Rationale:** `2026-09-22-rune-milestone-d-rationale-tradeoffs.md` (why we do this)
**Kafka parity:** `2026-09-22-rune-milestone-d-kafka-reference-parity.md` (edge-case rules mirrored from Kafka KRaft v4.5)
**Companion (downstream):** `2026-09-15-rune-db-face-materialized-views-transactions-DRAFT.md` (views ride on this)

## Goal

Let different partitions be led by different nodes — assigned by a controller,
durably recorded, and rebalanced on cluster churn — so Rune scales **write**
throughput across the cluster, shrinks the blast radius of a node failure, and
provides the co-partitioned placement substrate the materialized-view / DB-face
roadmap requires. The per-partition data path (single-writer log, ISR ack,
KIP-101 leader-epoch/truncation) is **unchanged**; this is a control-plane
milestone.

## Background — current state

- **Single-leader simplification (Milestone B/C):** the cluster (consensus)
  leader owns *every* partition. All produce funnels through one node; the rest
  are hot-standbys / read-replicas. Blast radius of that node's failure = 100% of
  writes for the failover window.
- **Per-partition machinery already exists and is reused as-is:**
  `PartitionReplica::becomeLeader(epoch, isr)` takes a *per-partition* epoch +
  ISR; `IsrManager`, `LeaderEpochCache` (KIP-101), and `ReplicaRegistry`
  (`(topicId,partition)→replica`) are per-partition. The DR-hardened
  epoch-lineage / no-committed-loss failover machinery is partition-local and
  works unchanged once per-partition leadership is wired in.
- **The single-leader assumption is concentrated in two spots:**
  `ReplicationMetadataBridge::doWork()` (broadcasts one epoch/ISR to *all*
  replicas — `libs/node/include/rune/node/ReplicationMetadataBridge.h:104-109`)
  and the single-boolean `leaderViewFn` (`libs/node/src/BrokerNode.cpp:521-527`).
- **What is missing (the load-bearing new build):** there is **no replicated
  metadata state machine**. Phase-2 Aeron-Raft consensus is *elections-only* — it
  elects one cluster leader and broadcasts a single `leadershipTermId`; there is
  no durable topology/assignment log.
- **Coordinator seams already stubbed:** `GroupCoordinationService` /
  `TransactionCoordinationService` carry explicit `// Milestone-D: hash(...)→node`
  comments; today they return `isLeader_()` / `leaderId_()`.
- **Client routing:** there is **no Metadata request**; clients discover the
  leader via the REDIRECT loop (`ClientGateway.cpp:895-920` cluster-level check,
  then per-partition `NotLeader` defense at `:997-1019`).

## Decisions (locked during brainstorming)

1. **Model = controller-assigns via a replicated metadata state machine.** Promote
   the *single* existing Aeron-Raft consensus group from elections-only into a
   deterministic replicated state machine holding the durable, versioned
   `partition → {leaderNode, ISR, leaderEpoch, partitionEpoch, replicas}` map
   (two epochs — see §D). One consensus group, N partitions. (Not per-partition
   Raft.)
2. **Replication is orthogonal and retained.** Every partition stays RF-way
   ISR-replicated across machines; D distributes only the *leader role*. A leader
   is only ever assigned from a partition's current ISR (no committed loss).
3. **Co-partition groups are first-class from Phase 1.** A topic may declare a
   co-partition group; the controller co-locates *same-index* partitions of all
   group members on one leader node. This is a deliberate addition over Kafka's
   independent per-topic balancing and is what delivers aggregate-root locality.
4. **Client routing = Metadata request + REDIRECT fallback.** Add a versioned
   `Metadata` request (partition→leader map); keep the existing REDIRECT loop as
   the always-correct fallback so a naive/stale client never breaks.
5. **Rebalancing policy = preferred-leader + even-balance + min-churn**;
   rack/AZ-awareness deferred to a later knob.
6. **Transaction scope = Tier-1 (scales for free) + generalize Tier-2 EOS
   coordinator placement. Tier-3 cross-shard 2PC is deferred** to its own opt-in
   spec, built only if a saga proves insufficient. Tier-4 refused.
7. **Full milestone, phased** (4 phases); Phase 1 is the first buildable slice.
8. **Ultra-low-latency, near-zero-allocation, benchmark-validated.** The
   per-partition hot path (produce/append/ISR-ack) and the routing lookups on it
   (partition→leader on the client, per-partition role on the gateway) must be
   **zero-allocation** and stay under the 100µs p99 gate. The control plane
   (assignment compute/commit/apply) is rare and off the hot path — it may
   allocate within bounds, but even there records use flyweight encoders (no
   per-record heap). Every optimization is benchmark-validated, not speculative.

## Non-goals

- **Not** per-partition Raft consensus (one metadata consensus group only).
- **Not** Tier-3 distributed (cross-shard read-modify-write) transactions — saga
  over escrow aggregates covers cross-user cases; deferred spec if 2PC ever needed.
- **Not** rack/AZ-aware or load-adaptive placement in this milestone (min-churn
  preferred-leader/even-balance only; smarter policy is a later knob).
- **Not** the materialized-view layer itself — D provides the placement substrate;
  views are a downstream spec.
- **No** change to the per-partition hot path (append/ISR/epoch/truncation).

## Architecture

### A. Metadata State Machine (MSM) + Controller — the new core

A deterministic replicated state machine layered on the existing Aeron-Raft
consensus log (faithful to Aeron Cluster: ordered commands → committed log →
apply to state machine). The **controller** is the metadata-consensus leader and
is the *sole writer* of assignment records (single-writer by consensus);
followers apply committed records.

State it owns (durable + versioned; monotonic `assignmentVersion`):
- **Topics:** `topicId`, name, `partitionCount`, replication factor,
  `partitionKey`/partitioner, `placementMode` (`local`/`partitioned`/`distributed`),
  `coPartitionGroup` (nullable).
- **Cluster membership:** node set + client endpoints + liveness.
- **Assignment map:** per `(topicId, partition)` →
  `{leaderNode, ISR[], leaderEpoch, partitionEpoch, replicas[], (optional) ELR[]}`.

Command records (applied deterministically): `CreateTopic`, `UpdateTopicConfig`,
`NodeJoin`, `NodeLeave`, `UpdatePartitionAssignment(...)`. Recovery = snapshot + log
replay.

**Apply committed only (Kafka parity §1).** The controller may compute assignments
optimistically, but **nodes/bridge apply only records at or below the committed
(stable) offset** — never in-flight ones — so a rolled-back assignment never
reaches a node. The bridge derives per-partition actions from the delta exactly as
Kafka's `LocalReplicaChanges`: **delete** (was a replica, no longer), **elected
leader** (is leader and `leaderEpoch` changed), **leader-update** (is leader and
`partitionEpoch` changed → ISR/replica change), **follower** (not leader and
`partitionEpoch` changed).

### B. Balancer (controller-side, co-location-aware)

Computes assignment deltas on membership/topology change:
- **Even-balance** partition leadership across live nodes; **preferred-leader**
  to minimize churn; **min-churn** (move as few leaderships as possible on a delta).
- **Co-location constraint:** for every co-partition group, assign
  `topicX-pN`, `topicY-pN`, … (same index N) to the **same** leader node.
  Rebalancing moves a group's same-index partitions **as a unit** — never
  splitting a group across nodes.
- **ISR-only leaders:** a node not in a partition's current ISR is never assigned
  its leader (guarantees no committed loss on handoff). Preferred leader =
  `replicas[0]`; preferred election fires only if that replica is **in ISR**.
- **Anti-storm & efficiency (Kafka parity §9, §10):** keep an `imbalancedPartitions`
  set and **cap preferred-leader elections per balance cycle** (Kafka: 1000);
  never force a leader move. On a node death, use a **reverse index
  `node → partitions-it-leads`** (bit-packed, cache-friendly — fits decision #8) so
  the cost is O(partitions-on-that-node), not O(all partitions); **fence one node at
  a time** so each fence propagates before the next; emit assignment records only
  where leadership/ISR actually changed (**skip no-ops**); **batch and bound** the
  records so one failure can't stall the metadata log.
- **Staged reassignment (Kafka parity §10):** replica-set changes stage as add →
  wait-for-ISR → remove (not atomic), with a guarded revert. A co-partition group's
  same-index partitions move **as a unit**; reject a reassignment that would break a
  group's co-location or leave it without live brokers to satisfy RF.

### C. Per-node multi-role runtime

Each `BrokerNode` runs `PartitionReplica` as **leader** for its assigned
partitions and **follower** for the rest. The change is concentrated in
`ReplicationMetadataBridge::doWork()`: replace the "broadcast one epoch/ISR to
all replicas" loop with a **per-partition lookup** from the committed assignment
map → `becomeLeader(perPartitionEpoch, perPartitionISR)` or
`becomeFollower(leaderNode)`. `BrokerNode` initializes per-partition roles from
the map.

### D. Epoch & fencing (two epochs — Kafka parity §2, §4)

D adopts Kafka's **two distinct per-partition epochs**, applied atomically in one
merge:
- **`leaderEpoch`** — bumped **only when the leader changes** (or on a forced bump:
  a replica removed from the replica set; going leaderless `NO_LEADER` counts as a
  change and bumps). This fences the **data plane**: a deposed leader's
  appends/replication are rejected by higher-epoch followers/clients (KIP-101,
  reused unchanged). **Do not** bump it on a plain ISR shrink/expand (post-Kafka-3.6
  rule) — a `NO_LEADER_CHANGE` sentinel skips the bump.
- **`partitionEpoch`** — bumped on **every** applied partition change; the
  **optimistic-concurrency version** for ISR-change requests. Invariant: if
  `leaderEpoch` rises, `partitionEpoch` rises too; it never lags.

**Election rule:** keep the current leader if it's still valid (don't churn); a
clean leader must be **in ISR** (or ELR when ISR is empty) and acceptable; re-run
election over the *proposed* ISR rather than trusting it.

**ISR-change (AlterPartition) fencing precedence** (mirror exactly): too-high
epoch → `NOT_CONTROLLER`; `leaderEpoch` too-low → `FENCED_LEADER_EPOCH` (deposed
leader); requester ≠ current leader → `INVALID_REQUEST`; `partitionEpoch` too-low →
`INVALID_UPDATE_VERSION`; ISR must be a valid subset of replicas **and must contain
the leader**; ineligible (unregistered/fenced/shutting-down/epoch-mismatch) members
→ rejected. Two-writers is prevented by this fencing + single-writer assignment via
consensus. The mechanism is per-partition and already present; D exercises it
concurrently across many partitions.

### E. Client routing

- New **`handleMetadata()`** in `ClientGateway` returns the versioned
  partition→leader map + endpoints.
- Produce/subscribe routing: replace the cluster-level `!lv.isLeader` check with a
  **per-partition** leader lookup; REDIRECT to the *partition's* leader endpoint
  (not the cluster leader). The per-partition `NotLeader` defense already present
  at `ClientGateway.cpp:997-1019` becomes the enforcement point.
- Client caches the map, routes `hash(key)→partition→node`, and refreshes on
  REDIRECT / epoch change. A stale map costs one REDIRECT round-trip then
  re-caches — self-correcting, no steady-state tax.
- **`leaderEpoch` staleness rule (Kafka parity §11 — ground truth):** the Metadata
  response carries `leaderEpoch` per partition; the client caches
  `lastSeenLeaderEpoch` and **accepts an update only if `newEpoch ≥ lastSeen`**,
  ignoring lower-epoch data. This is the only reliable defense against routing to a
  demoted leader when topology changes rapidly.
- **Zero-RTT reroute:** the `NotLeader`/REDIRECT error response carries the new
  `{leaderId, leaderEpoch}` so the client updates its cache and re-routes without
  waiting for a full metadata round-trip; it still schedules a background full
  refresh. Refresh uses **exponential backoff on no-progress (equivalent)
  responses** to prevent refresh storms.

### F. Coordinator placement (Tier-2 EOS generalization)

`GroupCoordinationService` / `TransactionCoordinationService`: replace the
cluster-leader checks with `hash(groupId)` / `hash(txnId)` → node via the
assignment map. `__consumer_offsets` / `__transaction_state` become multi-partition
and distributed (fills the existing `// Milestone-D` seams). This makes the
already-built Tier-2 EOS work in a distributed-leadership world — it is *placement
generalization*, not new transaction machinery.

## Data model — per-topic sharding config

Each topic carries (in the MSM topic record):
- `partitionCount` — fixed per topic.
- `partitionKey` / partitioner — the field that decides `hash(key)→partition`.
- `placementMode` — `local` (single node) / `partitioned` (co-partitioned, the
  default) / `distributed` (explicitly spread).
- `coPartitionGroup` — optional group name. **Constraint:** all members of a group
  MUST share the same `partitionCount` and be keyed by the same aggregate root, or
  co-location is impossible (`hash(key)` lands on different indexes). The
  controller rejects a group join that violates this.

## Partitioning & co-location guidance (aggregate-root modeling)

The locality guarantee ("a user + all its characters + every topic referencing
those characters on one node") is delivered by modeling, not magic:

- **Aggregate root = `userId`/`accountId`.** Key *every* topic in the user's world
  by userId — including character-scoped topics. `characterId` is a payload field
  (or the composite key `{userId, charLocalId}` so the routing key travels with
  the record), never the partition key. A stable `characterId → userId` mapping (a
  character belongs to one account for life) lets producers resolve the key.
- Put those topics in **one co-partition group** with **equal partition counts**.
  Then `hash(userId)` → same index → same node for the whole aggregate → Tier-1
  local ACID + local joins/views across all of it, with zero distributed
  transactions.
- **Global/shared entities are NOT part of a user aggregate:** world zones, global
  chat, auction house, leaderboards get their own topics/keys and their own
  partitioning (higher fan-out and/or broadcast "global views").
- **Cross-user operations** (player↔player trade, AH, mail, guild bank) span two
  aggregates → **saga over an escrow/trade-session aggregate** (remove→escrow→grant,
  each step Tier-1 + idempotent; a mid-failure leaves the item safely in escrow —
  never duplicated, never lost). Not co-location, not 2PC.
- **In-app purchases** = **idempotent Tier-1 grant + external reconciliation**
  (dedup by receipt/txn id; grant premium currency/items in one single-aggregate
  transaction; reconcile charged-but-not-granted via saga against the provider).
  No cross-shard distributed transaction — you cannot 2PC with Apple/Google/Stripe.
- **Skew caveat:** userId keying confines an account to one partition/node. For
  per-human workloads this is fine (bounded throughput, even hashing across
  accounts). Watch only for pathological single-entity hotspots (which belong in
  the global-entity bucket, not a user aggregate).

## Data flow

**Assignment / rebalance**
```
membership/topology change
  → Controller (Balancer) computes deltas (even-balance + preferred-leader
    + co-location + ISR-only + min-churn)
  → appends UpdatePartitionAssignment record(s) to the metadata consensus log
  → committed + replicated to all nodes (assignmentVersion++)
  → each node's ReplicationMetadataBridge applies per-partition roles
    (becomeLeader(epoch,isr) / becomeFollower(leaderNode))
  → gateway leaderView reflects per-partition leadership
  → clients refresh Metadata / follow REDIRECT
```

**Produce**
```
client: hash(key) → partition → Metadata map → partition leader node
  → append → ISR ack → ack to client
stale map → one REDIRECT → re-cache → retry
```

**Failover**
```
node dies → consensus membership change (NodeLeave)
  → Controller reassigns its partitions to ISR survivors (epoch bump,
    co-partition groups moved as a unit)
  → records committed → survivors becomeLeader per-partition
  → clients redirect to new leaders
(rides DR-hardened epoch-lineage / no-committed-loss machinery, now N-way)
```

## Performance & allocation constraints (cross-cutting)

The milestone splits cleanly into a **hot path** (must be zero-alloc, <100µs p99)
and a **control plane** (rare, bounded allocation allowed). The design keeps them
apart deliberately.

**Hot path — zero allocation, no locks on the fast route:**
- **Client partition→leader lookup:** the routing map is a **preallocated flat
  array indexed by `partitionId`** (`partition → {leaderNodeId, leaderEpoch}`),
  not a per-produce hashmap allocation. `hash(key)→partition` then an O(1) array
  index. Endpoints are interned (id→endpoint resolved once, cached), so the
  produce path carries a small integer node id, never a `std::string`.
- **Gateway per-partition role check:** the "am I the leader of partition P?"
  test on every produce/subscribe is an O(1) lookup into a preallocated
  per-partition role array (mirrors `ReplicaRegistry`), no allocation, no map
  rehash. Reject/redirect uses a preallocated error response.
- **The append/ISR path is untouched** — D adds no allocation to it; that is the
  whole point of keeping this a control-plane milestone.

**Control plane — bounded allocation, still disciplined:**
- Assignment records use **flyweight/zero-copy encoders** (reuse Aeron's own
  encoders per the faithful-port rule) — no per-record heap churn when appending
  to or replaying the metadata log.
- The MSM apply and the metadata bridge **reuse scratch buffers** across apply
  cycles (the compaction-cleaner pattern already in the codebase: reusable
  vectors/maps `.clear()`ed, not reallocated per cycle).
- Assignment deltas are computed into **reused, preallocated** delta buffers;
  a rebalance touches only changed partitions (min-churn), so steady state does
  near-zero allocation.
- The metadata image is a **compact, cache-friendly** structure (contiguous
  per-partition records, integer node ids), sized once and updated in place where
  possible — consistent with Rune's ULL data-structure standard.

**Validation:** every claim above is benchmark-validated — the Phase-4 matrix
includes an allocation-profile assertion on the produce path (zero steady-state
heap allocations under load) alongside the p99 gate, and a control-plane
apply-latency benchmark so a rebalance cannot stall the hot path.

## Phasing

**Phase 1 — buildable slice: MSM + static assignment + multi-leader produce.**
Metadata state machine (records, apply, snapshot/recovery); **static** assignment
computed by the controller at startup (co-location-aware, no dynamic rebalancing
yet); per-partition bridge wiring; gateway per-partition routing + REDIRECT;
client `Metadata` request + per-partition routing. Co-partition groups honored in
the static assignment.
*Exit:* a client concurrently produces to partitions led by *different* nodes; a
co-partition group's same-index partitions land on one node; integration test +
a >1-node aggregate-throughput benchmark.

**Phase 2 — dynamic rebalancing.** Controller reassigns on node
join/leave/failure (preferred-leader / even-balance / min-churn; groups moved as
a unit); epoch bumps; follower catch-up. *Exit:* DR-E2E rebalance under churn, no
committed loss on handoff, co-location preserved across rebalance.

**Phase 3 — coordinator placement generalization.** Group/txn coordinators placed
by `hash(id)→node`; `__consumer_offsets` / `__transaction_state` distributed.
*Exit:* Tier-2 EOS + consumer groups verified across distributed leaders.

**Phase 4 — DR / benchmark matrix.** Many concurrent partition failovers,
split-brain/fencing at scale, balanced + skew throughput benchmarks. *Exit:* the
multiplied-failover DR-E2E matrix green; benchmarks under the 100µs p99 hot-path
gate.

## Error handling / edge cases

- **Stale client map** → REDIRECT → refresh (always correct; no data risk).
- **Assignment not yet applied on a node** → brief `NotLeader`; client retries /
  redirects.
- **Controller (metadata-leader) failure** → metadata consensus re-elects; the
  assignment map is durable in the consensus log; an in-flight reassignment
  resumes from the committed prefix.
- **No ISR survivor for a partition** (all replicas lost) → mark leaderless
  (`NO_LEADER`, bump `leaderEpoch`), **preserve last-known-leader**, and re-run
  election over no-leader partitions when a broker unfences (Kafka parity §7).
  Partition is unavailable rather than risking committed loss.
- **Unclean leader election** (electing a replica not in ISR — data loss) is a
  **per-topic opt-in, default off**; when enabled it forces `ISR=[newLeader]` and
  sets a `RECOVERING` recovery flag; admin-triggered election is refused if a leader
  already exists (Kafka parity §5). **ELR (KIP-966)** — the clean-recovery
  alternative that remembers last-in-sync replicas — is **deferred** (Phase-1..3
  accept unclean-or-unavailable); noted as the principled later upgrade (§6).
- **Membership edge cases (Kafka parity §8):** broker epoch = registration log
  offset (monotonic); incarnation-id zombie guard (same id, different process,
  rejected while incumbent session valid); **unfence only after metadata
  catch-up**; **fence one node at a time**; graceful controlled-shutdown moves
  leadership off while the node is alive and waits until all live nodes consumed the
  moves (distinct from hard-failure fencing); **overload safety valve** (record
  liveness but suppress state transitions under controller backlog); **clock-rollover
  guard** in expiry math. Liveness/heartbeats are soft state off the consensus log.
- **Co-partition group config violation** (mismatched partition counts/keys) →
  controller rejects the group membership at `CreateTopic`/`UpdateTopicConfig`.
- **Rebalance thrash** → min-churn + preferred-leader damp it; groups move as a
  unit to avoid partial-group churn.
- **Hot partition / skew** → observable per-partition; mitigation is modeling
  (global entities out of user aggregates), not automatic in this milestone.

## Testing

- **Integration-first (in-process), before Docker E2E:** MSM apply/recovery,
  static + dynamic assignment, co-location correctness, per-partition failover,
  coordinator placement, client Metadata/REDIRECT routing.
- **TSan** on the bridge/assignment-apply concurrency path (mandatory for this
  concurrency-sensitive change).
- **DR-E2E Docker matrix (Phases 2 & 4):** churn, rebalance, multiplied
  concurrent failover, split-brain/fencing, no committed loss on handoff.
- **Benchmarks:** multi-leader aggregate throughput (the scale story), skew, and
  rebalance impact — all under the 100µs p99 hot-path gate where applicable.
- **Edge-case tests derived from the Kafka parity checklist** (§12 of the parity
  doc): two-epoch bump rules (partitionEpoch every change; leaderEpoch only on
  leader change / replica removal / `NO_LEADER`; not on ISR shrink); AlterPartition
  fencing precedence (`NOT_CONTROLLER` → `FENCED_LEADER_EPOCH` → `INVALID_REQUEST` →
  `INVALID_UPDATE_VERSION`); ISR-must-contain-leader; no-ISR-survivor →
  `NO_LEADER`; unclean-election opt-in gating; incarnation zombie guard;
  unfence-after-catch-up; fence-one-at-a-time under multiplied failover; client
  lower-epoch-rejection routing.

## Rollout / ownership

- Implementation produces broker + client-routing changes across the 4 phases;
  the user runs benchmarks/deploys and cuts any releases.
- Commit to LOCAL main per standing rule; push only on explicit request.

## Open questions (resolve during planning)

- Exact MSM record encoding + snapshot cadence vs the existing consensus log
  format (reuse Aeron's encoders per the faithful-port rule).
- How aggressively to co-locate a coordinator with the offsets/txn partition it
  manages (Phase 3) vs. pure `hash(id)`.
- Preferred-leader restoration policy after a recovered node rejoins (auto vs
  operator-triggered) to avoid rebalance storms.
- Metadata request push vs. pull (client polls vs. server pushes assignment-version
  bumps) — start pull + REDIRECT, evaluate push later.
- Interaction sequencing with in-flight Milestone C work (coordinator seams must
  not calcify before Phase 3 fills them).
