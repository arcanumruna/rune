# Milestone C-A — Consumer Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing `GroupCoordinator` + `OffsetStore` engines into the gateway, protocol, and client so clients can consume a topic as a fault-tolerant, resumable, load-balanced consumer group.

**Architecture:** Push-based delivery (extends the existing push-subscribe registry); the cluster leader is the sole group coordinator (behind a `coordinatorFor(groupId)` seam for Milestone D); partition assignment is computed broker-side (`autoAssignAndSync`); committed offsets are durable on a replicated `__consumer_offsets` internal topic (rides `PartitionReplica` acks=All) and rebuilt by replay on coordinator failover; membership is rebuilt by member rejoin. Eager (stop-the-world) rebalance; auto+manual commit; at-least-once.

**Tech Stack:** C++23, GoogleTest, Google Benchmark, Aeron (aeron:udp transport), SBE-style flat LE codecs (schema 120), pytest + Docker (dr-e2e). Repos: `rune-protocol` (codecs), `rune-broker` (engine/gateway/node), `rune-client-cpp` (client).

**Spec:** `docs/superpowers/specs/2026-09-15-rune-milestone-c-consumer-groups-design.md`

## Global Constraints

- Commit to **local `main` only — never push** (unless the user explicitly asks).
- **Never** `conan create` / `export` / `upload` — packages are CI-owned (a PreToolUse hook enforces this).
- Any benchmark **p99 > 100µs on a hot path = STOP and fix** before proceeding (or prove pure-harness-artifact + produce a corrected < 100µs measurement).
- **Single-writer duty-cycle discipline:** all coordinator/offset/push state is mutated only on the duty-cycle thread; inject the clock (`nowNs`) sampled once per cycle; **no locks, no per-op heap allocation, no blocking** on the hot path.
- Client protocol schema id = **120**; consumer-group templateIds occupy the reserved **groups/txn range 32–63**.
- Every task ends green (unit/integration), is committed, and — where it maps to a CG ticket — updates Planka (move card, comment) and pushes its test run to live via `scripts/rune-live.sh test <suite> passed passed=.. failed=.. total=.. commit=..`.
- Build with CLion's bundled cmake + `conan_host_profile` (x86_64 / cppstd=23 / Debug): `export PATH="/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin:$PATH"` then build in `rune-broker/build/` (Unix Makefiles, `BUILD_TESTING=ON`).
- **Faithful-Aeron / Kafka-faithful** discipline: mirror existing codec/handler/test patterns exactly; Opus review gates on allocation + latency + correctness.

---

## File Structure

**rune-protocol** (`include/rune/protocol/client/`)
- `GroupMessages.h` — CREATE: JoinGroup/Response, SyncGroup/Response, Heartbeat/Response, LeaveGroup/Response, Assign/Revoke control frame codecs.
- `OffsetMessages.h` — CREATE: OffsetCommit/Response, OffsetFetch/Response codecs.
- `Protocol.h` — MODIFY: add templateIds 32–39 + group error codes.
- `Dispatch.h` — MODIFY: register new templateIds as known.
- `tests/` — conformance vectors + round-trip tests for every new message.

**rune-broker**
- `libs/coordination/include/rune/coordination/ReplicatedOffsetStore.h` (+ `src/...cpp`) — CREATE: offset store backed by a `PartitionReplica`-replicated `__consumer_offsets` Log.
- `libs/node/include/rune/node/GroupCoordinationService.h` (+ `src/...cpp`) — CREATE: owns per-group `GroupCoordinator`s, drives `doWork`, implements the six handlers, translates assignment→push subscriptions.
- `libs/gateway/include/rune/gateway/ClientGateway.h` + `src/ClientGateway.cpp` — MODIFY: dispatch group templateIds to the service; expose an internal API to create/revoke group push-subscriptions.
- `libs/node/src/NodeRuntimeAgent.*` — MODIFY: construct + wire the service into the duty cycle; `coordinatorFor(groupId)` seam.
- `libs/*/tests/…` — unit + integration tests per component.

**rune-client-cpp** (`include/rune/client/`)
- `GroupConsumer.h` (+ `src/...cpp`) — CREATE: group-consumer state machine (join→sync→consume→heartbeat→commit→rejoin).
- `Client.h` — MODIFY: `subscribeGroup(...) -> GroupConsumer`.
- `tests/…` — unit + integration tests.

**dr-e2e**
- `harness/driver.py` / `dr_client` — MODIFY: group-consumer mode.
- `tests/test_cg_*.py` — CREATE: rebalance / failover / at-least-once scenarios.

---

## CG-1 — Protocol messages + codecs + conformance

### Task 1: Group templateIds + error codes

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/Protocol.h`
- Test: `rune-protocol/tests/` (existing protocol test target)

**Interfaces:**
- Produces: `TemplateId::{JoinGroup=32, JoinGroupResponse=33, SyncGroup=34, SyncGroupResponse=35, Heartbeat=36, HeartbeatResponse=37, LeaveGroup=38, LeaveGroupResponse=39, OffsetCommit=40, OffsetCommitResponse=41, OffsetFetch=42, OffsetFetchResponse=43, Assign=44}` and `ErrorCode::{StaleGeneration=11, UnknownMember=12, RebalanceInProgress=13, NotCoordinator=14, IllegalGeneration=15, GroupDead=16}`.

- [ ] **Step 1: Write the failing test** (`rune-protocol/tests/GroupProtocolTest.cpp`)
```cpp
#include <gtest/gtest.h>
#include <rune/protocol/client/Protocol.h>
using namespace rune::protocol::client;
TEST(GroupProtocol, TemplateIdsInReservedRange) {
    EXPECT_EQ(static_cast<uint16_t>(TemplateId::JoinGroup), 32);
    EXPECT_EQ(static_cast<uint16_t>(TemplateId::Assign),    44);
    // 32..63 is the reserved groups/txn range.
    EXPECT_GE(static_cast<uint16_t>(TemplateId::JoinGroup), 32);
    EXPECT_LE(static_cast<uint16_t>(TemplateId::Assign),    63);
}
TEST(GroupProtocol, GroupErrorCodes) {
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::StaleGeneration),    11);
    EXPECT_EQ(static_cast<uint16_t>(ErrorCode::NotCoordinator),     14);
}
```
- [ ] **Step 2: Run to verify it fails** — `ctest -R GroupProtocol` → FAIL (enumerators undefined).
- [ ] **Step 3: Add enumerators** to `TemplateId` (32–44) and `ErrorCode` (11–16) in `Protocol.h`, keeping the existing `// groups/txn 32-63` comment.
- [ ] **Step 4: Run to verify pass** — `ctest -R GroupProtocol` → PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(protocol): [CG-1] group + offset templateIds (32-44) and group error codes"`.

### Task 2: JoinGroup / SyncGroup codecs

**Files:**
- Create: `rune-protocol/include/rune/protocol/client/GroupMessages.h`
- Test: `rune-protocol/tests/GroupProtocolTest.cpp`

**Interfaces (mirror the `Subscribe` codec pattern in `DataMessages.h`):**
- Produces:
  - `struct JoinGroup { int64_t sessionId, correlationId; std::span<const uint8_t> groupId; std::span<const uint8_t> memberId; int32_t sessionTimeoutMs; std::vector<int32_t> subscribedTopicIds; encode(uint8_t*)->size_t; encodedSize(); static decode(buf,avail)->optional<JoinGroup>; TEMPLATE_ID=32; };`
  - `struct JoinGroupResponse { ...; std::span<const uint8_t> memberId; int32_t generationId; uint8_t isLeader; int16_t errorCode; TEMPLATE_ID=33; };`
  - `struct SyncGroup { ...; std::span<const uint8_t> groupId, memberId; int32_t generationId; TEMPLATE_ID=34; }` (followers send empty assignment; broker-side assignment ⇒ no assignment payload from client in v1).
  - `struct SyncGroupResponse { ...; int16_t errorCode; std::vector<TopicPartition32> assignment; TEMPLATE_ID=35; }` where `TopicPartition32{int32_t topicId; int32_t partition;}`.

- [ ] **Step 1: Write the failing round-trip test**
```cpp
TEST(GroupProtocol, JoinGroupRoundTrip) {
    const char* g = "grp"; const char* m = "";  // empty memberId → new id
    JoinGroup in;
    in.sessionId = 7; in.correlationId = 9;
    in.groupId = {reinterpret_cast<const uint8_t*>(g), 3};
    in.memberId = {reinterpret_cast<const uint8_t*>(m), 0};
    in.sessionTimeoutMs = 30000;
    in.subscribedTopicIds = {1, 2};
    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    auto out = JoinGroup::decode(buf.data(), buf.size());
    ASSERT_TRUE(out);
    EXPECT_EQ(out->sessionId, 7);
    EXPECT_EQ(std::string(out->groupId.begin(), out->groupId.end()), "grp");
    EXPECT_EQ(out->sessionTimeoutMs, 30000);
    ASSERT_EQ(out->subscribedTopicIds.size(), 2u);
    EXPECT_EQ(out->subscribedTopicIds[1], 2);
}
TEST(GroupProtocol, SyncGroupResponseCarriesAssignment) {
    SyncGroupResponse in; in.errorCode = 0;
    in.assignment = {{1,0},{1,2}};
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    auto out = SyncGroupResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(out); ASSERT_EQ(out->assignment.size(), 2u);
    EXPECT_EQ(out->assignment[1].partition, 2);
}
```
- [ ] **Step 2: Run to verify it fails** — header does not exist yet → compile FAIL.
- [ ] **Step 3: Implement `GroupMessages.h`** following `DataMessages.h::Subscribe` exactly: `MessageHeader` prefix, `BODY_FIXED` for fixed ints, `putVarBytes`/`getVarBytes` for `groupId`/`memberId`, and a `uint16` count prefix + fixed-stride array for `subscribedTopicIds` / `assignment`. Decode returns `nullopt` on schema/template mismatch or truncation. Include `#include <rune/protocol/client/DataMessages.h>` for the shared `detail::` helpers + `putVarBytes`.
- [ ] **Step 4: Run to verify pass** — `ctest -R GroupProtocol` → PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(protocol): [CG-1] JoinGroup/SyncGroup codecs"`.

### Task 3: Heartbeat / LeaveGroup codecs
**Files:** Modify `GroupMessages.h`; test in `GroupProtocolTest.cpp`.
**Interfaces:** `Heartbeat{groupId,memberId,generationId}` (36) / `HeartbeatResponse{errorCode}` (37); `LeaveGroup{groupId,memberId}` (38) / `LeaveGroupResponse{errorCode}` (39).
- [ ] **Step 1:** Write round-trip tests for `Heartbeat` (fixed `generationId`, var `groupId`/`memberId`) and `LeaveGroup`, plus a truncation test (`decode(buf, buf.size()-1)` → `nullopt`).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the four structs (same pattern).
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit — `feat(protocol): [CG-1] Heartbeat/LeaveGroup codecs`.

### Task 4: OffsetCommit / OffsetFetch codecs + Assign frame + Dispatch + conformance
**Files:** Create `OffsetMessages.h`; modify `GroupMessages.h` (Assign), `Dispatch.h`; tests + conformance vectors.
**Interfaces:**
- `OffsetCommit{groupId,memberId,generationId, entries:[{topicId,partition,offset,metadata}]}` (40) / `OffsetCommitResponse{[{topicId,partition,errorCode}]}` (41).
- `OffsetFetch{groupId, entries:[{topicId,partition}] (empty ⇒ all)}` (42) / `OffsetFetchResponse{[{topicId,partition,offset,errorCode}]}` (43).
- `Assign{ generationId, assignment:[{topicId,partition}] }` (44) — broker→client egress control frame (revoke = empty assignment).
- [ ] **Step 1:** Write round-trip tests: multi-entry `OffsetCommit` (with metadata var field per entry), `OffsetFetch` all-partitions (empty entries), `Assign` empty (revoke). Add a `Dispatch` test asserting `peekTemplateId` returns each new id and `classify` does not report `UnknownTemplate` for 32–44.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `OffsetMessages.h` + `Assign` in `GroupMessages.h`; extend `Dispatch.h`’s known-template set to include 32–44.
- [ ] **Step 4:** Run → PASS. Add encoded-frame **conformance vectors** (fixed byte arrays + expected decode) mirroring the existing conformance harness for at least JoinGroup, OffsetCommit, Assign.
- [ ] **Step 5:** Commit — `feat(protocol): [CG-1] OffsetCommit/OffsetFetch/Assign codecs + dispatch + conformance`.
- [ ] **Step 6:** Full protocol suite green; **Planka** move CG-1 → Done + comment; **live** push `rune-protocol/groups passed`.

---

## CG-2 — ReplicatedOffsetStore on `__consumer_offsets`

**Files:**
- Create: `rune-broker/libs/coordination/include/rune/coordination/ReplicatedOffsetStore.h` + `src/ReplicatedOffsetStore.cpp`
- Test: `libs/coordination/tests/coordination/ReplicatedOffsetStoreTest.cpp` (unit) + `libs/node/tests/integration/OffsetReplicationTest.cpp` (integration)

**Interfaces:**
- Consumes: `rune::replication::PartitionReplica::append(std::span<const uint8_t>, bool acksAll) -> Offset`; `PartitionReplica::highWatermark() -> Offset`; existing `OffsetStore` record codec (reuse the LE record layout from `OffsetStore.h`).
- Produces:
  ```cpp
  class ReplicatedOffsetStore {
  public:
    // replica: the __consumer_offsets partition replica (leader on the coordinator node).
    explicit ReplicatedOffsetStore(rune::replication::PartitionReplica* replica);
    // Append an offset-commit record via replica.append(acksAll=true); returns false if gated (minIsr).
    bool commit(const GroupId&, const TopicPartition&, Offset, std::string_view metadata, int64_t tsNs);
    std::optional<CommittedOffset> fetch(const GroupId&, const TopicPartition&) const;
    std::vector<std::pair<TopicPartition,CommittedOffset>> fetchAll(const GroupId&) const;
    // Replay all committed records up to replica.highWatermark() into the in-memory map.
    void rebuildFromLog();
  };
  ```

**Architecture note:** reuse the exact on-disk record layout already defined in `OffsetStore.h` (magic `0x4F464653`, CRC32C tail) as the *record body* appended to the partition Log, so a single decoder serves both. `commit` builds one record → `replica->append(record, /*acksAll=*/true)`; the in-memory map is updated only after the append is accepted (offset ≥ 0). `rebuildFromLog` reads the replicated Log `[0, highWatermark())` and folds each record (last write wins per `(group,tp)`).

- [ ] **Step 1: Write the failing unit test** (uses an in-memory/loopback `PartitionReplica`, single replica so HW advances on append)
```cpp
TEST(ReplicatedOffsetStore, CommitThenFetchRoundTrip) {
    auto replica = makeSingleNodeReplica(/*topicId=*/0, /*partition=*/0); // test helper
    rune::coordination::ReplicatedOffsetStore store(replica.get());
    ASSERT_TRUE(store.commit("g", {1,0}, 42, "", 1000));
    driveHwAdvance(*replica);                        // pump until HW covers the append
    auto c = store.fetch("g", {1,0});
    ASSERT_TRUE(c); EXPECT_EQ(c->offset, 42);
}
TEST(ReplicatedOffsetStore, RebuildFromLogAfterReopen) {
    auto replica = makeSingleNodeReplica(0,0);
    { rune::coordination::ReplicatedOffsetStore s(replica.get());
      s.commit("g", {1,0}, 42, "", 1); s.commit("g", {1,0}, 99, "", 2);   // overwrite
      driveHwAdvance(*replica); }
    rune::coordination::ReplicatedOffsetStore s2(replica.get());          // fresh in-memory map
    s2.rebuildFromLog();
    auto c = s2.fetch("g", {1,0});
    ASSERT_TRUE(c); EXPECT_EQ(c->offset, 99);                              // last write wins
}
```
- [ ] **Step 2: Run** → FAIL (class undefined).
- [ ] **Step 3: Implement** `ReplicatedOffsetStore` (record encode reused from OffsetStore layout; `commit`→`replica->append`; `rebuildFromLog`→fold `[0,HW)`). Add the `makeSingleNodeReplica`/`driveHwAdvance` test helpers next to existing replica test helpers.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(coordination): [CG-2] ReplicatedOffsetStore over __consumer_offsets (acks=All)`.
- [ ] **Step 6: Integration test** (`OffsetReplicationTest.cpp`, 3-in-process-node harness like `GatewayProduceReplicationTest`): commit on leader → kill leader → new leader `rebuildFromLog()` → `fetch` returns the committed offset (no loss). Run → PASS. Commit. **Planka** CG-2 → Done; **live** push.

---

## CG-3 — GroupCoordinationService + handlers

### Task 5: Service skeleton + duty-cycle drive + coordinatorFor seam
**Files:** Create `libs/node/include/rune/node/GroupCoordinationService.h` + `src/GroupCoordinationService.cpp`; modify `NodeRuntimeAgent` to construct + `doWork`-drive it; test `libs/node/tests/GroupCoordinationServiceTest.cpp`.
**Interfaces:**
- Consumes: `GroupCoordinator` (join/leave/heartbeat/syncGroup/autoAssignAndSync/doWork/state/assignmentFor); `ReplicatedOffsetStore`; `bool isCoordinatorLeader()` from the metadata bridge.
- Produces:
  ```cpp
  class GroupCoordinationService {
  public:
    GroupCoordinationService(ReplicatedOffsetStore* offsets, TopicRegistry* topics);
    int  doWork(int64_t nowNs);                     // drives each GroupCoordinator::doWork; returns work count
    NodeId coordinatorFor(const GroupId&) const;    // v1: returns cluster-leader nodeId (Milestone-D seam)
    bool isLocalCoordinator() const;                // == this node is cluster leader
    GroupCoordinator& coordinator(const GroupId&);  // get-or-create per-group
  };
  ```
- [ ] **Step 1:** Failing test: `doWork` on an empty service returns 0; `coordinator("g")` lazily creates and is stable across calls; when `isLocalCoordinator()==false`, `coordinatorFor` still returns the leader nodeId.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the service (map `GroupId→unique_ptr<GroupCoordinator>`; `doWork` iterates and sums; `coordinatorFor` returns the bridge’s current leader nodeId). Wire `service_.doWork(nowNs)` into `NodeRuntimeAgent::doWork` after consensus/replication, before/after gateway (single-writer, same thread).
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit — `feat(node): [CG-3] GroupCoordinationService skeleton + duty-cycle drive + coordinatorFor seam`.

### Task 6: Membership handlers (Join/Sync/Heartbeat/Leave)
**Files:** Modify `GroupCoordinationService.*`, `ClientGateway.cpp` (dispatch), tests.
**Interfaces:** Produces `handleJoinGroup/handleSyncGroup/handleHeartbeat/handleLeaveGroup(const uint8_t* buf, size_t len, int64_t nowNs) -> std::vector<uint8_t> response` (or writes to the session egress). Non-coordinator ⇒ `ErrorCode::NotCoordinator`.
- [ ] **Step 1:** Failing tests: Join returns a memberId + generationId and moves state to `PreparingRebalance`; after `setRebalanceTimeoutMs` + `doWork` past the timeout, `autoAssignAndSync` is invoked and Sync returns a non-empty assignment; Heartbeat during rebalance returns `RebalanceInProgress`; stale-generation Heartbeat returns `StaleGeneration`; Leave shrinks membership + triggers rebalance; a request when `isLocalCoordinator()==false` returns `NotCoordinator`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement handlers: decode (CG-1 codecs) → call the `GroupCoordinator` → encode response. On the join-timeout transition in `doWork`, call `autoAssignAndSync(topicPartitionCounts, AssignorType::Range, nowNs)` using `TopicRegistry` partition counts. Route dispatch in `ClientGateway.cpp` for templateIds 32–39 to the service.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit — `feat(node+gateway): [CG-3] group membership handlers (join/sync/heartbeat/leave)`.

### Task 7: Offset handlers (OffsetCommit/OffsetFetch)
**Files:** Modify `GroupCoordinationService.*`, `ClientGateway.cpp`; tests.
**Interfaces:** `handleOffsetCommit/handleOffsetFetch(...)`; commit is generation-fenced (reject wrong generation with `IllegalGeneration`); commit writes via `ReplicatedOffsetStore::commit` (acks=All), fetch reads `ReplicatedOffsetStore::fetch`.
- [ ] **Step 1:** Failing tests: OffsetCommit with correct generation persists (fetch returns it); wrong generation → `IllegalGeneration`; OffsetFetch of an uncommitted `(group,tp)` returns the “no offset” sentinel; commit that is minIsr-gated (replica returns false) → `MinIsrViolation`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement both handlers; dispatch templateIds 40–43 in the gateway.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit — `feat(node+gateway): [CG-3] offset commit/fetch handlers`. **Planka** CG-3 → Done; **live** push.

---

## CG-4 — Assignment → push-subscription translation

**Files:** Modify `ClientGateway.h/.cpp` (expose internal group-subscription create/revoke over the existing `Subscription` registry), `GroupCoordinationService.*` (drive create/revoke on rebalance-complete), tests `libs/node/tests/integration/GroupPushDeliveryTest.cpp`.

**Interfaces:**
- Consumes: existing `ClientGateway` `Subscription{sessionId, topicId, partition, nextOffset, isolation}` registry + `pushSubscriptions()`; `GroupCoordinator::assignmentFor(memberId)`; `ReplicatedOffsetStore::fetch`.
- Produces on `ClientGateway`:
  ```cpp
  // Create an internal push subscription for a group member, seeded from `fromOffset`.
  int64_t addGroupSubscription(int64_t memberSessionId, int32_t topicId, int32_t partition,
                               int64_t fromOffset, uint8_t isolation);
  // Revoke ALL push subscriptions for a member session (eager rebalance revoke).
  void revokeGroupSubscriptions(int64_t memberSessionId);
  ```

**Behavior:** when a group transitions to `Stable` (in `doWork` after `autoAssignAndSync`), for each member: `revokeGroupSubscriptions(session)` then, for each assigned `(topicId,partition)`, `addGroupSubscription(session, topicId, partition, committedOffsetOr(auto.offset.reset), isolation)`. On entering `PreparingRebalance`: `revokeGroupSubscriptions` for all members + send `Assign{generationId, {}}` (revoke) to each. On `Stable`: send `Assign{generationId, assignment}` to each member. Delivery then flows through the existing `pushSubscriptions()` (HW-bounded).

- [ ] **Step 1: Failing integration test** (in-process gateway + coordinator + a 1-partition topic with N appended records): two members join group over topic T(2 partitions) → after rebalance each member’s egress receives records for exactly one partition, resumed from committed offset; no partition delivered to both (single-owner).
```cpp
TEST(GroupPushDelivery, EachPartitionToExactlyOneMember) {
    // ... bring up gateway + service; append 10 records to T:p0 and T:p1 ...
    joinMember(svc, "g", "m1"); joinMember(svc, "g", "m2");
    driveRebalance(svc);              // doWork past rebalance timeout → Stable → push subs created
    pumpPush(gw, /*cycles=*/50);
    auto p0 = recordsDeliveredTo("m1"); auto p1 = recordsDeliveredTo("m2");
    EXPECT_EQ(ownersOfPartition(0).size(), 1u);
    EXPECT_EQ(ownersOfPartition(1).size(), 1u);
    EXPECT_EQ(p0.size() + p1.size(), 20u);   // all records, no dup in steady state
}
TEST(GroupPushDelivery, ResumeFromCommittedOffset) {
    // commit offset 5 for (g, T:p0) before join → member starts at 5, receives 5..9 only
}
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `addGroupSubscription`/`revokeGroupSubscriptions` on the gateway (reuse the `Subscription` struct + registry; key by memberSessionId) and the on-rebalance create/revoke + `Assign` egress in the service.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(gateway+node): [CG-4] assignment→push-subscription translation + resume-from-committed + eager revoke`.
- [ ] **Step 6:** Add a **rebalance-revoke test** (a third member joins → all revoked, reassigned to 3 members, each partition still single-owner, resumed from last commit). Green → commit. **Planka** CG-4 → Done; **live** push.

---

## CG-5 — Client consumer-group API

**Files:** Create `rune-client-cpp/include/rune/client/GroupConsumer.h` + `src/GroupConsumer.cpp`; modify `Client.h`; tests `rune-client-cpp/tests/GroupConsumerTest.cpp`.

**Interfaces:**
```cpp
struct GroupConfig { bool autoCommit=true; int32_t autoCommitIntervalMs=5000;
                     int32_t sessionTimeoutMs=30000; OffsetReset autoOffsetReset=OffsetReset::Latest; };
class GroupConsumer {
public:
  void onAssigned(std::function<void(std::vector<TopicPartition>)>);
  void onRevoked (std::function<void(std::vector<TopicPartition>)>);
  void onRecords (std::function<void(RecordSet)>);
  void commit();                 // async manual commit of current positions
  std::future<void> commitSync();
  void close();
};
// Client.h:
GroupConsumer& subscribeGroup(std::string groupId, std::vector<std::string> topics, GroupConfig = {});
```

**State machine (runs on the client agent duty cycle — reuse the CC-5 duty-cycle + wait-free snapshot pattern from DR-BUG-1; no locks on the hot path):** `Unjoined → Joining(JoinGroup) → Syncing(SyncGroup) → Stable(consume + heartbeat + auto-commit) → Rejoining` on `RebalanceInProgress`/`NotCoordinator`(→follow REDIRECT)/heartbeat-timeout. Records for assigned partitions arrive on the existing egress `Records` path and are surfaced via `onRecords`; positions advance; auto-commit timer fires `OffsetCommit`; on `Assign` control frame, fire `onRevoked(old)` then `onAssigned(new)`.

- [ ] **Step 1: Failing unit tests** (against a mock/loopback gateway): join→sync yields `onAssigned` with the assigned partitions; a delivered `Records` frame surfaces via `onRecords` and advances position; `commit()` sends an `OffsetCommit` with the current position; a `RebalanceInProgress` heartbeat response drives a rejoin; an `Assign{{}}` (revoke) frame fires `onRevoked`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `GroupConsumer` + `Client::subscribeGroup`, reusing the client transport/codec adapters and the wait-free producer/subscriber snapshot patterns. Heartbeat cadence = `sessionTimeoutMs/3`.
- [ ] **Step 4: Run** → PASS (add a TSan build run for the consume/heartbeat/commit hot path — must be clean).
- [ ] **Step 5: Commit** — `feat(client): [CG-5] consumer-group API (subscribeGroup, callbacks, heartbeat, auto/manual commit, auto-rejoin)`.
- [ ] **Step 6: Integration test** vs the dockerized/in-process cluster: two `GroupConsumer`s over a 2-partition topic → balanced consumption, commit, restart one → resumes from commit. Green → commit. **Planka** CG-5 → Done; **live** push.

---

## CG-6 — DR-E2E consumer-group scenarios

**Files:** Modify `dr-e2e/harness/driver.py` + `dr_client` (group-consumer mode: `RUNE_GROUP_ID`, `RUNE_GROUP_MEMBERS`, `RUNE_AUTOCOMMIT_MS`); create `dr-e2e/tests/test_cg_rebalance.py`, `test_cg_failover.py`, `test_cg_atleastonce.py`.

**Interfaces:** Consumes the existing `cluster`/`fresh_cluster` fixtures + `wait_data_plane_ready` (DR-BUG-8) + `faults.py` (kill/pause/netem) + `invariants.py`.

- [ ] **Step 1: Add group mode to `dr_client`** — join a group, consume assigned partitions, auto-commit, print `{member, partition, offset, count}` records for the parser. Add `harness/driver.py::run_group(...)` returning per-member consumption.
- [ ] **Step 2: `test_cg_rebalance.py`** (matrix: members ∈ {2,3,4}, partitions ∈ {2,4,8}): all members join → assert **every partition owned by exactly one member** + **union of consumed = all produced** (no gap). Member leaves → reassignment → still single-owner, no committed loss. Run → PASS.
- [ ] **Step 3: `test_cg_failover.py`**: produce+consume+commit → **kill the coordinator (cluster leader)** → members rejoin the new coordinator → assert **resume from committed offset, no committed-offset loss**, bounded re-delivery. Include a netem-loss variant (mirrors DR-8). Run → PASS.
- [ ] **Step 4: `test_cg_atleastonce.py`**: kill a member mid-consume → its partitions reassigned → assert **no produced record is missed by the group** (at-least-once), duplicates bounded to the uncommitted window. Run → PASS.
- [ ] **Step 5: Commit** each test file as it goes green — `test(dr-e2e): [CG-6] consumer-group <scenario>`.
- [ ] **Step 6:** Run the full CG DR-E2E set on a **fresh `--no-cache` build** (guard against the stale-binary trap); all green. **Planka** CG-6 → Done; **live** push `dr-e2e passed`.

---

## CG-7 — Benchmarks + full-area review

**Files:** Create `rune-broker/bench/GroupCoordinationBench.cpp`; extend client bench; no product code unless a gate fails.

- [ ] **Step 1: Benchmarks** (Release): JoinGroup→assignment latency; rebalance completion time vs member count; `OffsetCommit` and `OffsetFetch` p99; per-cycle `GroupCoordinationService::doWork` cost with N groups. Assert **no per-op heap alloc** on the heartbeat/commit/push hot paths (allocation counter or manual review).
- [ ] **Step 2: Gate check** — any hot-path p99 > 100µs → STOP, root-cause, fix, re-measure < 100µs (or prove harness artifact). Record numbers.
- [ ] **Step 3: Full-area Opus review** of all CG changes (protocol + coordination + node + gateway + client) against: single-writer, zero hot-path alloc/locks/blocking, generation fencing correctness, at-least-once (no loss / bounded dup), offset durability + failover replay, REDIRECT/coordinator-move correctness, no phantom delivery beyond HW. Fix findings to **CLEAN** (escalate any Sonnet-introduced regression to Opus).
- [ ] **Step 4: Commit** — `bench(coordination): [CG-7] consumer-group benchmarks + full-area review CLEAN`.
- [ ] **Step 5:** **Planka** CG-7 → Done; **live** push bench numbers (`kind=benchmark`).

---

## Self-review (author checklist — completed)

- **Spec coverage:** every spec §4 component maps to a task — protocol §4.1→CG-1; OffsetStore §4.3→CG-2; service+handlers §4.2→CG-3; push translation §4.4→CG-4; client §4.5→CG-5; DR-E2E §4.6→CG-6; benchmarks/review §9→CG-7. Failover (§6) covered by CG-2 integration + CG-6 failover test. Milestone-D seam (§7) = `coordinatorFor` in CG-3 Task 5. Error/edge cases (§8) covered across CG-3/CG-4/CG-6.
- **Placeholder scan:** no TBD/TODO; each code step carries real test/impl content or an exact interface signature.
- **Type consistency:** `TopicPartition32{topicId,partition}` used consistently in codecs; `ReplicatedOffsetStore::{commit,fetch,fetchAll,rebuildFromLog}` names match across CG-2/CG-3; `addGroupSubscription/revokeGroupSubscriptions` match across CG-4; `GroupConsumer::{onAssigned,onRevoked,onRecords,commit,commitSync}` match across CG-5/CG-6.

## Open items to confirm during execution (from spec §11)
- `__consumer_offsets` bootstrap: eager single-partition create at cluster init (default) vs lazy on first commit.
- `Assign`/`Revoke` as the dedicated control frame (chosen) vs SyncGroup round-trips.
- Auto-commit timing: fire **after** `onRecords` returns (chosen) to keep at-least-once crisp.
- Heartbeat as a dedicated message (chosen) vs keepalive piggyback.
