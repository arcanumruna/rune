# Milestone D — Phase 1 Implementation Plan (MSM + Static Assignment + Multi-Leader Produce)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let different partitions be led by different nodes via a controller-assigned, durably-replicated metadata state machine, so a client concurrently produces to partitions led by *different* nodes — the first buildable slice of distributed partition leadership.

**Architecture:** Promote the existing single Aeron-Raft consensus group (elections-only) with a new **Metadata State Machine (MSM)** — a replicated, deterministically-applied assignment log built from the *existing* offsets/txn service template (`Log` + `PartitionReplica` + `ReplicatedStateStore` + service, driven from `NodeRuntimeAgent`). A **static Balancer** computes a co-location-aware `partition → {leaderNode, ISR, leaderEpoch, partitionEpoch}` map at cluster form; the generalized `ReplicationMetadataBridge` applies per-partition roles; the gateway routes per-partition via an immutable routing image; a new client `Metadata` request + REDIRECT fallback lets clients reach the right node. No dynamic rebalancing, no failover election, no coordinator-placement changes (those are Phases 2–3).

**Tech Stack:** C++23, Aeron (consensus/counters/rings), rune-protocol (SBE-style flyweight codecs, schemaId=120, all little-endian), GoogleTest, x86_64 (Rosetta) build via CLion `conan_host_profile`, `build-tsan/` for ThreadSanitizer, the `benchmark/` harness.

**Spec:** `docs/superpowers/specs/2026-09-22-rune-milestone-d-distributed-partition-leadership-design.md`
**Companion refs (executors read these):** `…-rune-milestone-d-kafka-reference-parity.md` (edge-case rules + §-anchors this plan cites), `…-rune-milestone-d-aeron-ull-reference.md` (ULL primitives), and the **As-built reconciliation** table inside the design spec (what's reuse vs net-new, with `file:line` anchors).

## Global Constraints

Every task's requirements implicitly include this section.

- **Latency gate (HARD, benchmark-enforced):** **every operation, including end-to-end produce latency, must be < 100µs p99; target ~50µs p99.** A task that adds/changes an operation is **not done** until a benchmark proves its p99. Any p99 > 100µs = STOP-and-fix (see [[benchmark-latency-gate]]).
- **Zero-allocation hot path:** produce/append/routing-lookup do **zero** steady-state heap allocation. Control-plane (MSM apply, balancer, assignment records) may allocate within bounds but reuses scratch buffers + flyweight encoders (Aeron's own — faithful-port rule).
- **Test ordering (HARD — this is the point of this plan):** for **every** feature: **(1) exhaustive unit tests covering ALL edge cases → (2) in-process integration tests proving it works end-to-end in the broker → (3) benchmarks proving the p99 gate → only THEN (4) e2e (smoke / availability / resiliency; the exhaustive stress + disaster-recovery matrix is Phase 4).** Never write an e2e test for behavior not already proven by unit + integration. Reproduce any e2e bug in an in-process integration test first ([[feedback_integration_before_e2e]]).
- **MANDATORY ThreadSanitizer** on every task that touches a concurrency path (MSM apply loop, the metadata bridge, SPSC rings, counter publish/read): build in `build-tsan/` and run the task's tests under TSan; **0 data races** required before the task is done.
- **Durability model (decision #2, locked):** leader-owned ISR; majority-quorum acks by default; per-topic `all-in-sync` durability knob; failover = position/lineage election (Phase 2). **No** controller-owned ISR, **no** AlterPartition, **no** two-state ISR.
- **Two epochs:** `partitionEpoch` bumps on **every** assignment change; `leaderEpoch` bumps **only** on a leader change / replica removal / going-leaderless (`NO_LEADER`), **never** on a plain ISR shrink. Invariant: `leaderEpoch` rises ⇒ `partitionEpoch` rises.
- **Apply committed-only:** the bridge/MSM apply act only on records at/below the committed (high-watermark) position — never optimistic/in-flight ones.
- **Aeron-faithful:** reuse Aeron encoders/ring buffers/counters and the `doWork()→idle(workCount)` agent cycle; mirror the `enqueueCleanedSegment`/`drainCleanerSwaps` SPSC pattern for any cross-thread handoff.
- **Git:** commit to **LOCAL `main` only, NEVER push** (push only on explicit user request). Frequent commits (one per task). End messages with the Co-Authored-By trailer.
- **Model policy:** Sonnet implements, Opus reviews; escalate any Sonnet-introduced regression to Opus ([[rune-subagent-model-policy]]). Verifiers must **not** mutate git ([[rune-subagent-zombie-reactivation]]).
- **Live dashboard:** each task = a Planka ticket; move it to the in-progress lane when taken, push plan + test/bench results to the live board ([[rune-session-live-protocol]]).
- **Build/test commands:**
  - Configure+build (Debug, x86_64): `cmake --build build -j8` (uses the CLion `conan_host_profile`; see [[sibling-cpp-build-profile]]).
  - TSan build: `cmake --build build-tsan -j8`.
  - Run one test target: `./build/libs/<lib>/rune_<name>_tests --gtest_filter='<Suite>.*'`.
  - Run a benchmark: `./build/benchmark/<Name>Bench` (or the target added by the task).

## Scope

**This plan = Phase 1 only.** Exit criterion: on a 3-node cluster, a client concurrently produces to partitions whose leaders are on *different* nodes; a co-partition group's same-index partitions land on one node; all Phase-1 unit + integration + benchmark + smoke/availability gates green; produce e2e p99 < 100µs (target ~50µs). Phases 2 (dynamic rebalancing + position/lineage failover), 3 (coordinator hash placement), and 4 (multiplied-failover DR + full stress/resiliency + benchmark matrix) get their own plans (outlined at the end).

## Testing & benchmark discipline (how each task is structured)

Each task follows: **failing unit test(s) → run(fail) → minimal impl → run(pass) → enumerate & cover ALL edge cases → [TSan if concurrent] → [benchmark + assert p99 if it touches an operation] → integration test if it crosses components → commit.** The "edge cases" list in each task is the *minimum* coverage bar — add any you discover. Benchmarks are first-class steps, not an afterthought.

## File Structure

**rune-protocol** (`../rune-protocol/include/rune/`):
- `protocol/MetadataRecords.h` (NEW) — MSM log record codecs: `CreateTopicRecord`, `UpdatePartitionAssignmentRecord`, `NodeJoinRecord`, `NodeLeaveRecord`; each carries a `recordVersion` byte for version gating. Little-endian, fixed-block + optional var arrays via the existing `ArrayView` pattern.
- `protocol/ClientMessages.h` (MODIFY) — add `MetadataRequest` / `MetadataResponse` client codecs (per-partition `{leaderId, leaderEpoch, endpoint}`), templateId in the **control range 1–15** (verify next free id; cluster metadata is answerable by any node).

**rune-broker** `libs/metadata/` (NEW lib, mirrors `libs/coordination` layout):
- `include/rune/metadata/MetadataStateMachine.h` / `src/MetadataStateMachine.cpp` — the deterministic assignment state + `apply(record, committedPos)` + delta derivation + snapshot/restore.
- `include/rune/metadata/AssignmentMap.h` — compact, cache-friendly `partition → PartitionAssignment{leaderNode, isr[], leaderEpoch, partitionEpoch, replicas[]}` (open-addressing, no per-op alloc).
- `include/rune/metadata/StaticBalancer.h` / `src/StaticBalancer.cpp` — pure function: `(topics, coPartitionGroups, liveNodes, rf) → assignment records`.
- `include/rune/metadata/RoutingImage.h` — immutable flat array `partitionId → {leaderNodeId, leaderEpoch}`, published by release-store, read by acquire-load.
- `tests/…` — one gtest target per unit (`rune_metadata_state_machine_tests`, `rune_static_balancer_tests`, `rune_routing_image_tests`, `rune_metadata_records_tests`).

**rune-broker** existing files (MODIFY, anchors from the reconciliation table):
- `libs/node/include/rune/node/ReplicationMetadataBridge.h` (`:76-130`) — per-partition apply from the committed map.
- `libs/node/include/rune/node/NodeRuntimeAgent.h` (`:115-183`) — add `metadataService_->doWork(nowNs)` after the bridge (step 2b).
- `libs/node/include/rune/node/BrokerNode.h` (`:279-298`) — add `metadataLog_/metadataReplica_/metadataStore_/metadataService_`; register `metadataReplica_` in `ReplicaRegistry`; add MSM commit/append-position counters.
- `libs/gateway/src/ClientGateway.cpp` (`:521-527` leaderView, `:895-920`/`:997-1019` produce routing, `:1401-1430` subscribe, dispatch `:310-356`) — per-partition routing via `RoutingImage` + `handleMetadata`.

**rune-client-cpp** (`../rune-client-cpp/`):
- client `Metadata` request send + partition→leader cache (`lastSeenLeaderEpoch`, lower-epoch rejection) + `hash(key)→partition→node` routing + zero-RTT reroute on `NotLeader`/`Redirect` carrying `{leaderId, leaderEpoch}`.

**benchmark/** (NEW): `MetadataApplyBench.cpp`, `RoutingImageBench.cpp`, `MultiLeaderProduceBench.cpp`.

---

### Task 1: MSM log record codecs (rune-protocol)

**Files:**
- Create: `../rune-protocol/include/rune/protocol/MetadataRecords.h`
- Test: `../rune-protocol/tests/MetadataRecordsTest.cpp` (+ CMake target `rune_metadata_records_tests`)

**Interfaces:**
- Produces: `CreateTopicRecord{topicId:int32, name:var, partitionCount:int32, rf:int16, partitionKey:var, placementMode:uint8, coPartitionGroup:var, recordVersion:uint8}`; `UpdatePartitionAssignmentRecord{topicId:int32, partition:int32, leaderNode:int32, isr:ArrayView<int32>, leaderEpoch:int32, partitionEpoch:int32, replicas:ArrayView<int32>, recordVersion:uint8}`; `NodeJoinRecord{nodeId, endpoint:var, recordVersion}`; `NodeLeaveRecord{nodeId, recordVersion}`. Each: `encode(buf,len)`, `decode(buf,len)`, `ENCODED_SIZE`/`TOTAL_SIZE`, `peekTemplateId`.

- [ ] **Step 1: Write failing round-trip + edge tests.** Cases (all must be present): round-trip each of the 4 records with representative values; empty ISR / empty replicas arrays; empty var strings (name, coPartitionGroup null); max-partition-count boundary; **little-endian byte-order assertion** (encode a known value, assert exact bytes); **truncated buffer → decode rejects** (len < TOTAL_SIZE returns error, never mis-parses); **unknown `recordVersion` → clean reject** (not a crash); `peekTemplateId` returns the assigned ids; templateIds fall in the control/metadata reserved range (assert range).
- [ ] **Step 2: Run → FAIL** (`rune_metadata_records_tests` — types undefined).
- [ ] **Step 3: Implement** the flyweight codecs in `MetadataRecords.h` mirroring the existing `ConsensusMessages.h`/`ClientMessages.h` style (fixed BLOCK_LENGTH + `ArrayView` for var arrays; `recordVersion` guarded like the existing schema-version guard).
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Commit** (`git add … && git commit -m "feat(protocol): MSM metadata log record codecs (Milestone D Phase 1)"`).

---

### Task 2: Two-epoch model (`leaderEpoch` + `partitionEpoch`)

**Files:**
- Create: `libs/metadata/include/rune/metadata/PartitionAssignment.h`
- Test: `libs/metadata/tests/PartitionAssignmentTest.cpp` (target `rune_partition_assignment_tests`)

**Interfaces:**
- Produces: `struct PartitionAssignment{int32 leaderNode; std::vector<int32> isr; std::vector<int32> replicas; int32 leaderEpoch; int32 partitionEpoch;}` and a pure `PartitionAssignment mergeChange(const PartitionAssignment& cur, const AssignmentChange& c)` implementing the bump rules.

- [ ] **Step 1: Write failing bump-rule tests (exhaustive).** Cases: leader change ⇒ both epochs bump; replica *removed* from replica set ⇒ both bump (forced leaderEpoch bump even if leader unchanged); going leaderless (`leaderNode = NO_LEADER = -1`) ⇒ both bump; plain ISR shrink ⇒ **only** `partitionEpoch` bumps, `leaderEpoch` unchanged; plain ISR expand ⇒ only `partitionEpoch`; no-op change ⇒ neither bumps (or rejected as no-op); invariant property test: after any sequence of merges, `leaderEpoch` monotonic AND every `leaderEpoch` bump coincided with a `partitionEpoch` bump (`partitionEpoch` never lags).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** `mergeChange` per the bump rules (parity §2). `NO_LEADER = -1` counts as a leader change; a `NO_LEADER_CHANGE = -2` sentinel skips the leaderEpoch bump.
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Commit** (`feat(metadata): two-epoch partition assignment + bump rules`).

---

### Task 3: MetadataStateMachine apply core

**Files:**
- Create: `libs/metadata/include/rune/metadata/AssignmentMap.h`, `include/rune/metadata/MetadataStateMachine.h`, `src/MetadataStateMachine.cpp`
- Test: `libs/metadata/tests/MetadataStateMachineTest.cpp` (target `rune_metadata_state_machine_tests`)

**Interfaces:**
- Consumes: Task 1 record codecs, Task 2 `PartitionAssignment`/`mergeChange`.
- Produces: `class MetadataStateMachine { void apply(const RecordView&, int64 recordPos); const PartitionAssignment* assignment(int32 topicId,int32 partition) const noexcept; LocalRoleDelta deriveLocalDelta(int32 selfNode, const AssignmentMap& prev) const; void snapshot(SnapshotWriter&) const; void restore(SnapshotReader&); };` where `LocalRoleDelta = { deletes[], electedLeaders[], leaderUpdates[], followers[] }`.

- [ ] **Step 1: Write failing tests (ALL edge cases).** Cases: apply a stream of records → assignment map matches expected; **deterministic replay** (apply same stream twice from empty ⇒ identical map; hash the map); **apply-committed-only** (records above `committedPos` are not visible via `assignment()`); **delta derivation** — for a partition where self becomes leader with a new `leaderEpoch` ⇒ appears in `electedLeaders`; self stays leader but `partitionEpoch` changed (ISR change) ⇒ `leaderUpdates`; self not leader but `partitionEpoch` changed ⇒ `followers`; self was a replica, now absent ⇒ `deletes`; no change ⇒ appears in none; **snapshot→restore round-trip** produces an identical map (hash equality); **restore prunes** assignments absent from the snapshot (create A+B, snapshot with only A, restore ⇒ B gone); **structural sharing** (applying a change to one topic does not rebuild others — assert via a cheap identity/allocation probe); unknown record version ⇒ skipped-or-rejected deterministically.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** `AssignmentMap` (open-addressing, no per-op alloc; power-of-2 + mask per the Aeron-ULL collections pattern), `apply` (dispatch by templateId → `mergeChange`), `deriveLocalDelta` (the 4-case derivation keyed on epoch deltas, parity §1), `snapshot`/`restore` (full image; restore resets then replays; prune-on-load).
- [ ] **Step 4: Run → PASS; cover every case above.**
- [ ] **Step 5: Commit** (`feat(metadata): deterministic MSM apply + delta derivation + snapshot`).

---

### Task 4: Wire MSM as a replicated service into the node runtime

**Files:**
- Modify: `libs/node/include/rune/node/BrokerNode.h` (`:279-298` service members; add `metadataLog_/metadataReplica_/metadataStore_/metadataService_`; register `metadataReplica_` in `ReplicaRegistry`), `src/BrokerNode.cpp` (build them, mirroring `offsetLog_`/`groupCoordService_` wiring); `libs/node/include/rune/node/NodeRuntimeAgent.h` (`:115-183`, add `metadataService_->doWork(nowNs)` as step 2b, right after the bridge)
- Create: `libs/node/include/rune/node/MetadataService.h` (thin driver: on leader edge builds the MSM from its replica's committed log; exposes `assignment()` + publishes the `RoutingImage`)
- Test: `libs/node/tests/integration/MetadataReplicationTest.cpp` (target `rune_metadata_replication_tests`)

**Interfaces:**
- Consumes: Task 3 `MetadataStateMachine`; the existing `Log`/`PartitionReplica`/`ReplicaRegistry`/`ReplicationMetadataBridge`/`NodeRuntimeAgent` template (reconciliation: offsets/txn services are the pattern).
- Produces: `MetadataService::assignment(topicId,partition)`, `MetadataService::routingImage()`.

- [ ] **Step 1: Write failing integration tests.** Cases: **single-node** — append assignment records via the metadata replica, drive `NodeRuntimeAgent::doWork`, assert `metadataService_->assignment(...)` reflects them **only after commit** (HW advance); **3-node in-process** — records committed on the leader become visible on all followers' MSM after replication (majority HW); **restart** — a node restarts, loads the MSM snapshot + replays the tail, assignment map identical (hash equality); **apply is off the request path** (assert a produce/read served while an assignment applies — no shared lock stall); the metadata replica's role/epoch is driven by the **existing bridge** for free (assert leader/follower role on the metadata partition tracks cluster leadership).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the wiring: a dedicated internal topic (sentinel `topicId`, e.g. `-3`) for `__cluster_metadata`, `metadataReplica_` in the registry (so the bridge drives its role/epoch), `metadataStore_` rebuilding the MSM from the committed log (mirror `ReplicatedOffsetStore::rebuildFromLog`), `metadataService_->doWork` applying committed records + republishing the routing image via an SPSC swap if produced off-thread (mirror `enqueueCleanedSegment`/`drainCleanerSwaps`).
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: TSan.** Build `build-tsan`, run `rune_metadata_replication_tests` under TSan → **0 races** (apply loop vs routing-image read vs bridge).
- [ ] **Step 6: Benchmark.** Add `benchmark/MetadataApplyBench.cpp`: apply-record latency + routing-image republish; assert control-plane apply p99 well within budget and **does not perturb** a concurrent produce bench (append p99 unchanged). Record numbers.
- [ ] **Step 7: Commit** (`feat(node): replicated metadata state machine wired into node runtime`).

---

### Task 5: Static Balancer (pure, co-location-aware)

**Files:**
- Create: `libs/metadata/include/rune/metadata/StaticBalancer.h`, `src/StaticBalancer.cpp`
- Test: `libs/metadata/tests/StaticBalancerTest.cpp` (target `rune_static_balancer_tests`)

**Interfaces:**
- Produces: `std::vector<UpdatePartitionAssignmentRecord> StaticBalancer::assign(const std::vector<TopicSpec>& topics, const std::vector<NodeId>& liveNodes)` where `TopicSpec{topicId, partitionCount, rf, coPartitionGroup, partitionKey}`.

- [ ] **Step 1: Write failing tests (exhaustive edge cases).** Cases: even leadership distribution across nodes (counts differ by ≤1); **co-location** — two topics in the same group with equal `partitionCount` get same-index partitions on the **same** leader node (assert for every index); ISR = the RF replicas chosen, leader ∈ ISR; **RF > liveNodes ⇒ rejected** (error, no assignment); **group partition-count mismatch ⇒ rejected** (parity §10, spec data-model constraint); **group key mismatch ⇒ rejected**; single node (all partitions on it); determinism (same inputs ⇒ byte-identical records); leaderEpoch/partitionEpoch initialized to 0.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the striped even-balance with the co-partition-group constraint (seed placement by group so same-index partitions align — the deliberate extension Kafka lacks, parity §10); validation up front.
- [ ] **Step 4: Run → PASS; cover every case.**
- [ ] **Step 5: Commit** (`feat(metadata): static co-location-aware balancer`).

---

### Task 6: Controller emits the static assignment on leader claim

**Files:**
- Modify: `libs/node/src/BrokerNode.cpp` / `MetadataService.h` (on becoming the metadata-consensus leader, run `StaticBalancer::assign` over configured topics and append the records to the metadata replica in bounded batches)
- Test: `libs/node/tests/integration/StaticAssignmentEmitTest.cpp` (target `rune_static_assignment_emit_tests`)

**Interfaces:**
- Consumes: Task 4 `MetadataService`, Task 5 `StaticBalancer`.

- [ ] **Step 1: Write failing integration tests.** Cases: on a freshly-formed 3-node cluster with configured topics, the leader emits assignment records, they commit, and **all** nodes' MSM shows the balanced+co-located map; **idempotent** (re-running on the same membership produces no duplicate/divergent assignment — guard by "already assigned" / assignmentVersion); **bounded batching** (a large topic set is split into ≤maxRecords batches, never one giant atomic batch — parity §14); only the metadata leader emits (a follower does not).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the emit-on-leader-claim path (guard on leader edge; bounded batches; version/idempotency guard).
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Commit** (`feat(node): emit static partition assignment on metadata-leader claim`).

---

### Task 7: Generalize the ReplicationMetadataBridge to per-partition roles

**Files:**
- Modify: `libs/node/include/rune/node/ReplicationMetadataBridge.h` (`:76-130` — replace the "broadcast one epoch/ISR to all replicas" loop with per-partition lookup from the committed assignment map)
- Test: `libs/node/tests/node/ReplicationMetadataBridgePerPartitionTest.cpp` (target `rune_bridge_per_partition_tests`)

**Interfaces:**
- Consumes: Task 3 `deriveLocalDelta`; the existing `PartitionReplica::becomeLeader(epoch, isr)` / `becomeFollower(epoch, leaderId)`.

- [ ] **Step 1: Write failing unit tests (all delta cases).** Cases: given a committed assignment map and this node id, the bridge calls `becomeLeader(perPartitionEpoch, perPartitionISR)` for partitions it leads and `becomeFollower(epoch, leaderNode)` for the rest — **per-partition**, not uniform; a partition removed from this node ⇒ replica stopped/deleted; **apply-committed-only** (an uncommitted assignment above HW is NOT applied); no-op fast path (unchanged map ⇒ returns 0 work, no `becomeX` calls); epoch source is the **per-partition** `leaderEpoch` from the map, not the cluster term.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the per-partition apply, driven by `deriveLocalDelta(selfNode, prevMap)` against the committed map; keep the monotonic-epoch clamp; preserve the existing `observedIsLeader_` release/acquire mirror.
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: TSan** on `rune_bridge_per_partition_tests` → 0 races.
- [ ] **Step 6: Commit** (`feat(node): per-partition role application in the metadata bridge`).

---

### Task 8: Immutable routing image (zero-alloc, O(1))

**Files:**
- Create: `libs/metadata/include/rune/metadata/RoutingImage.h`
- Test: `libs/metadata/tests/RoutingImageTest.cpp` (target `rune_routing_image_tests`)
- Bench: `benchmark/RoutingImageBench.cpp`

**Interfaces:**
- Produces: `class RoutingImage { struct Entry{int32 leaderNodeId; int32 leaderEpoch;}; const Entry& lookup(int32 globalPartitionId) const noexcept; }` built as an immutable flat array; a publisher swaps `std::shared_ptr<const RoutingImage>`/atomic pointer with a **release-store**; readers **acquire-load** once.

- [ ] **Step 1: Write failing tests.** Cases: build from an assignment map, `lookup(pid)` returns the right `{leader, epoch}`; out-of-range/unassigned pid ⇒ sentinel `{-1,-1}`; **atomic swap** — a reader holding the old image sees a consistent snapshot while a new one is published (no torn read); interned endpoint id→endpoint resolves once; the image is a contiguous flat array (no per-lookup allocation).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the flat-array image + release/acquire swap (Aeron-ULL §6/§1 discipline; C++ `std::atomic<std::shared_ptr<const RoutingImage>>` with acquire/release).
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Benchmark** `RoutingImageBench` — `lookup` latency (expect single-digit ns), **assert zero allocation** in the lookup loop; p99 recorded.
- [ ] **Step 6: Commit** (`feat(metadata): immutable zero-alloc routing image`).

---

### Task 9: Gateway per-partition routing + REDIRECT

**Files:**
- Modify: `libs/gateway/src/ClientGateway.cpp` (`:521-527` leaderView → per-partition; `:895-920`/`:997-1019` produce; `:1401-1430` subscribe) and `BrokerNode.cpp` to feed the `RoutingImage` to the gateway
- Test: `libs/gateway/tests/integration/GatewayPerPartitionRoutingTest.cpp` (target `rune_gateway_per_partition_routing_tests`)
- Bench: extend `benchmark/GatewayDataBench.cpp`

**Interfaces:**
- Consumes: Task 8 `RoutingImage`; existing `ReplicaRegistry` per-partition `role()` (the defense-in-depth check stays).

- [ ] **Step 1: Write failing tests.** Cases: a produce to a partition this node leads is accepted (append happens); a produce to a partition led **elsewhere** is **rejected with REDIRECT to that partition's leader endpoint** (not the cluster leader) and the engine is untouched; same for subscribe; a **stale routing image** (client targets an old leader) ⇒ REDIRECT with the current `{leaderId, leaderEpoch}`; the per-partition `NotLeader` defense at `:997` still fires if the image and replica role disagree.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — replace the single `!lv.isLeader` cluster check with a `RoutingImage::lookup(pid)` → if leader ≠ self, REDIRECT to that leader's endpoint carrying `{leaderId, leaderEpoch}`; keep the `:997-1019` per-partition guard.
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Benchmark (THE gate).** `GatewayDataBench` produce path with per-partition routing on: **assert e2e produce p99 < 100µs, target ~50µs**; assert **zero steady-state allocation** on the produce path; compare against pre-change baseline (routing adds no measurable p99). p99 > 100µs ⇒ STOP-and-fix.
- [ ] **Step 6: Commit** (`feat(gateway): per-partition routing + partition-leader REDIRECT`).

---

### Task 10: Client `Metadata` request + partition routing (rune-protocol + broker + client)

**Files:**
- Modify: `../rune-protocol/include/rune/protocol/ClientMessages.h` (add `MetadataRequest`/`MetadataResponse`, control-range templateId), `libs/gateway/src/ClientGateway.cpp` (dispatch `:310-356` + `handleMetadata`), `../rune-client-cpp/…` (send Metadata, cache, route, reroute)
- Test: `../rune-protocol/tests/…` (codec round-trip), `libs/gateway/tests/integration/MetadataRequestTest.cpp` (target `rune_metadata_request_tests`), client-side unit tests

**Interfaces:**
- Produces: `MetadataResponse{ brokers:[{nodeId,endpoint}], partitions:[{topicId,partition,leaderId,leaderEpoch}] }`.

- [ ] **Step 1: Write failing tests.** Protocol: round-trip Metadata req/resp (empty topic list = all; specific topics), LE + truncation reject + unknown-version reject. Broker: `handleMetadata` returns the current routing image contents (per-partition leader + epoch + endpoints); answerable by **any** node (not just leader). Client (unit): caches `lastSeenLeaderEpoch`; **rejects a lower-epoch update** (parity §11 staleness rule); `hash(key)→partition→node` routing; **zero-RTT reroute** — on `NotLeader`/`Redirect` carrying `{leaderId, leaderEpoch}` the client updates its cache and re-routes without a full refresh; exponential backoff on no-progress refresh.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** codecs, `handleMetadata`, client cache/route/reroute.
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: Integration** — a client with no prior map issues Metadata, then produces to the correct node for each partition; after a (simulated) leader change it reroutes on REDIRECT and succeeds.
- [ ] **Step 6: Commit** (`feat(client): Metadata request + per-partition routing with epoch-staleness rule`).

---

### Task 11: Multi-leader produce — the Phase-1 integration headline

**Files:**
- Test: `libs/node/tests/integration/MultiLeaderProduceTest.cpp` (target `rune_multi_leader_produce_tests`)

- [ ] **Step 1: Write the integration tests (the exit criterion).** Cases: bring up a **3-node in-process cluster**, configure topics so static assignment places partitions' leaders on different nodes; a client produces **concurrently** to partitions led by different nodes → **all records land, HW advances per-partition, no cross-node produce**; a **co-partition group's** same-index partitions are all led by one node (assert co-location end-to-end); majority-quorum ack works (a lagging non-majority follower does not stall acks); records are readable per-partition (HW-bounded).
- [ ] **Step 2: Run → FAIL** (until Tasks 4–10 are wired).
- [ ] **Step 3: Fix wiring gaps** surfaced (no new features — only integration glue).
- [ ] **Step 4: Run → PASS.**
- [ ] **Step 5: TSan** on the full multi-leader run → 0 races.
- [ ] **Step 6: Commit** (`test(node): multi-leader produce integration (Phase-1 exit)`).

---

### Task 12: Benchmark gate (Phase-1, benchmark-prevalent)

**Files:**
- Create: `benchmark/MultiLeaderProduceBench.cpp` (+ CMake targets)
- Modify: aggregate any per-task benches into a Phase-1 report

- [ ] **Step 1: Write the benchmarks.** Metrics (each with p99): **end-to-end produce latency** through client routing → partition leader → majority ack (**< 100µs p99, target ~50µs**); routing-image `lookup` (ns); MSM apply latency (control-plane); **multi-leader aggregate throughput** on 3 nodes vs single-node baseline (assert > single-node — the scale story); **allocation profile** — zero steady-state heap allocations on the produce path under load.
- [ ] **Step 2: Run all benches; record numbers** to the plan + live board.
- [ ] **Step 3: Gate.** Any operation p99 > 100µs ⇒ STOP-and-fix before proceeding; document the achieved p99 (target ~50µs). Confirm append p99 with routing ON vs OFF is indistinguishable (isolation).
- [ ] **Step 4: Commit** (`bench(node): Milestone D Phase-1 latency + throughput + allocation gate`).

---

### Task 13: Phase-1 e2e — smoke + availability (integration-proven first)

**Files:**
- Create: `dr-e2e/tests/test_multi_leader_smoke.py` (+ availability scenario), reusing the `dr-e2e` py-testinfra + Docker harness

- [ ] **Step 1: Write e2e tests (only behavior already proven by unit+integration).** Cases: **smoke** — dockerized 3-node cluster starts, static assignment distributes leadership, a client produces+consumes across partitions led by different nodes; **availability** — take down a node that leads *some* partitions' work but is a *follower* for others → partitions led *elsewhere* keep serving produce with no interruption (Phase-1 has no failover election yet, so a partition whose *leader* dies is expected unavailable until Phase 2 — assert exactly that boundary, don't over-claim).
- [ ] **Step 2: Run** (`DR_E2E_BUILD=1 pytest -m milestone_d_p1` when Docker is up; start Docker if down — [[docker-start-when-down]]).
- [ ] **Step 3: Push results to the live board.** Record pass/fail + the boundary (leader-death recovery is Phase 2).
- [ ] **Step 4: Commit** (`test(e2e): Milestone D Phase-1 smoke + availability`).

> **Exhaustive stress / disaster-recovery / resiliency matrix is Phase 4** (multiplied concurrent failover, split-brain/fencing at scale, netem+crashes, skew) — deferred by design because Phase 1 has no dynamic failover. Each later phase runs its relevant e2e slice **after** its unit+integration+bench gates are green.

---

## Phases 2–4 (subsequent plans — outline only)

Each gets its own detailed plan with the **same discipline** (exhaustive unit → integration → benchmark p99 gate → e2e).

- **Phase 2 — dynamic rebalancing + position/lineage failover.** Commit/append-position counters (net-new); controller re-elects the most-complete survivor on the epoch lineage (`compareLog`) on node join/leave/failure; min-churn preferred-leader + even-balance; co-partition groups move as a unit; `LeaderEpochCache` persistence + ProducerId-watermark recovery (net-new, for restart); producer-state re-rebuild if a divergence truncation lands below the rebuild HW. e2e: rebalance-under-churn, no-committed-loss handoff.
- **Phase 3 — coordinator hash placement.** Fill the `GroupCoordinationService`/`TransactionCoordinationService` `coordinatorFor()` stubs with `hash(id)→node`; distribute `__consumer_offsets`/`__transaction_state`; per-topic durability-quorum knob (`all-in-sync`) surfaced. e2e: Tier-2 EOS + consumer groups across distributed leaders.
- **Phase 4 — DR / stress / resiliency / benchmark matrix.** Multiplied-concurrent-failover (reverse index, fence-one-at-a-time), split-brain/fencing at scale, balanced + skew throughput, full DR-E2E (netem + crashes), all under the 100µs p99 gate.

## Self-Review

**Spec coverage (Phase-1 scope):** MSM (design §A/§H) → Tasks 1,3,4; two epochs (§D) → Task 2; static co-location Balancer (§B, data-model) → Task 5; emit-on-claim → Task 6; per-partition bridge (§C) → Task 7; routing image + gateway routing + REDIRECT (§E, perf §) → Tasks 8,9; client Metadata + staleness/reroute (§E, parity §11) → Task 10; multi-leader produce exit → Task 11; benchmark gate (decision #8, §perf) → Tasks 4/8/9/12; e2e smoke+availability (§testing) → Task 13. Leadership-move correctness (§G) and coordinator placement (§F) are Phases 2–3 (outlined). Durability decision #2 honored (leader-owned ISR, majority quorum, no two-state ISR). ✅ no Phase-1 gaps.

**Placeholder scan:** every task has concrete files, interfaces, enumerated edge-case test lists, exact commands, and TSan/benchmark gates. Where exact internal C++ signatures depend on code the executor will read, the reconciliation table's `file:line` anchors are cited (not fabricated). No "TBD"/"add error handling"/"write tests for the above".

**Type consistency:** `PartitionAssignment{leaderNode,isr,replicas,leaderEpoch,partitionEpoch}` (Task 2) is consumed identically in Tasks 3,5,7,8; `deriveLocalDelta`/`LocalRoleDelta` (Task 3) consumed in Task 7; `RoutingImage::Entry{leaderNodeId,leaderEpoch}` (Task 8) consumed in Task 9; `MetadataResponse` partition tuple `{topicId,partition,leaderId,leaderEpoch}` consistent across Tasks 9,10. ✅
