# Per-Worker Replication Transport (Lever #4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each worker its own Aeron subscription + per-peer publications for its partitions' replication, removing the single-control-thread MPSC-drain funnel that is the proven 3-node throughput ceiling.

**Architecture:** With a symmetric cluster `workerCount`, partition P is owned by the same worker index `w` on every node, so the cluster factors into W independent shared-nothing replication planes (worker `w` ↔ worker `w` on every peer) on stream `replBaseStreamId + w`. User partitions (`topicId≥0`) move to per-worker planes; internal partitions (`topicId<0`, consensus/metadata) stay on the legacy control-thread `AeronPartitionTransport`. Gated by `RUNE_PER_WORKER_REPLICATION`, which also serves as instant rollback and the same-binary A/B throughput harness.

**Tech Stack:** C++23, Aeron (embedded media driver, ExclusivePublication/Subscription), SBE (rune-protocol codecs, unchanged for #4), ninja/ctest, rune-fr flight recorder, dockerized DR harness, `replbench.sh`.

**Spec:** `docs/superpowers/specs/2026-09-30-per-worker-replication-transport-design.md`

## Global Constraints

- Aeron contract: one `ExclusivePublication` has exactly one writer thread; one `Image` is polled by exactly one thread. Every per-worker resource is owned and touched only by its worker thread (`rune-shared-nothing-law`: no shared state/atomics/locks across threads; all cross-thread via ring buffers).
- Internal partitions (`topicId < 0`) MUST remain on the legacy control-thread `AeronPartitionTransport`; only `topicId ≥ 0` move to per-worker planes.
- `RUNE_PER_WORKER_REPLICATION` default = **off** until the full dockerized DR matrix is green flag-on; then a final task flips the default to on.
- Symmetric worker count is a hard invariant when the flag is on: validate every peer's `workerCount == self.workerCount` at membership resolution; on mismatch **hard-refuse** (fatal, name the offending node + both counts). No per-node fallback (mixed streamId schemes = split cluster).
- No wire-format change in #4 (FetchRequest/FetchResponse SBE unchanged; #1 multiplex is a separate later plan).
- Correctness gate: the DR/failover matrix that is green on `main` today MUST be green flag-on (no committed-data loss; time-independent ISR/epoch convergence). Flag-off path must be byte-for-byte the current behavior.
- Every bug fix ships a regression test marked `*Regression*` (`mark-regression-tests-as-suite`). Never report done without the real e2e path green (`never-hand-over-untested-code`).
- Throughput measured only on a **verified-clean box** (kill leftover procs + host game backend; load<1, ≥90% idle); interleaved ABAB n≥15; 16 MiB Aeron buffers on the 4-core box; never use bigger hardware to mask a result (`never-hardware-to-mask-bad-software`).

## Review Focus

- **Worker-count mismatch mid-life (peer rejoins with different `RUNE_WORKERS`):** a node that was valid at startup must still hard-refuse the mismatched peer on membership change, not mis-route. → Task 2 tests the validator on a membership-change event, not just startup.
- **Flag-off regression:** the legacy `AeronPartitionTransport` path must be entirely unchanged when the flag is off. → Task 5 test asserts flag-off constructs zero per-worker planes and routes all partitions through the legacy transport.
- **Internal-partition isolation:** a `topicId<0` consensus replica must never be placed on a per-worker plane even with the flag on. → Task 5 test asserts internal replicas stay on the legacy transport.
- **Term-buffer view lifetime on inline apply:** the receiving worker applies records straight from the Aeron term-buffer view; storing that view past the poll callback corrupts data. → Task 4 test drives two back-to-back polls and asserts the first frame's records are fully consumed (applied) before the second poll overwrites the buffer region.
- **Plane resolution failure (a per-worker sub/pub fails to resolve within the bounded window):** startup must fail loudly, not run with a half-wired plane silently dropping a partition's replication. → Task 5 test asserts a plane that fails to resolve aborts startup.

---

## File Structure

- `libs/node/include/rune/node/NodeConfig.h` / `libs/node/src/NodeConfig.cpp` — add `perWorkerReplication` (bool), `replBaseStreamId` (int32); publish/parse peer `workerCount`.
- `libs/node/include/rune/node/WorkerReplicationValidator.h` (new) — pure function `validateSymmetricWorkerCount(self, peers)` → result; hard-refuse decision isolated + unit-testable.
- `libs/node/include/rune/node/WorkerReplicationTransport.h` (new) — per-worker `PartitionTransport` impl: owns one inbound `WorkerIngress`-style subscription (stream `replBase+w`) + one `ExclusivePublication` per peer; send methods offer directly; `poll(dispatch)` receives + inline-applies.
- `libs/node/src/BrokerNode.cpp` — construct per-worker replication subs/pubs behind the flag; validate symmetric worker count; route `topicId≥0` replicas to per-worker transports, `topicId<0` to legacy.
- `libs/node/include/rune/node/Worker.h` — drive the worker's replication transport `poll()` in `doWork()`; construct owned replicas with the per-worker transport.
- `libs/replication/include/rune/replication/PartitionReplica.h` — no interface change (already takes a `PartitionTransport*`); confirm injection point.
- Tests: `libs/node/tests/node/WorkerReplicationValidatorTest.cpp`, `libs/node/tests/node/WorkerReplicationTransportTest.cpp`, `libs/node/tests/integration/PerWorkerReplicationTest.cpp`; extend existing `rune_gateway_failover_replication_tests`.
- `bench/kafka-compare/replbench.sh` — honor `RUNE_PER_WORKER_REPLICATION` for the ABAB harness (env already passes through to broker).

---

## Task 1: NodeConfig — flag, stream base, peer workerCount

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (add fields near `workerCount`, `replicationStreamId`)
- Modify: `libs/node/src/NodeConfig.cpp` (parse `RUNE_PER_WORKER_REPLICATION`, `RUNE_REPL_BASE_STREAM_ID`; include `workerCount` in the membership/peer descriptor)
- Test: `libs/node/tests/node/NodeConfigTest.cpp` (existing config-parse test file)

**Interfaces:**
- Produces: `bool NodeConfig::perWorkerReplication` (default `false`); `int32_t NodeConfig::replBaseStreamId` (default chosen disjoint from client/consensus streams, e.g. `2000`); `int32_t PeerConfig::workerCount` (advertised per peer, default `0` = unknown).

- [ ] **Step 1: Write the failing test**
```cpp
TEST(NodeConfig, ParsesPerWorkerReplicationFlagAndStreamBase) {
    setenv("RUNE_PER_WORKER_REPLICATION", "true", 1);
    setenv("RUNE_REPL_BASE_STREAM_ID", "2000", 1);
    setenv("RUNE_WORKERS", "4", 1);
    auto cfg = parseNodeConfig();   // existing entry point used by other cases
    EXPECT_TRUE(cfg.perWorkerReplication);
    EXPECT_EQ(cfg.replBaseStreamId, 2000);
    EXPECT_EQ(cfg.workerCount, 4);
    unsetenv("RUNE_PER_WORKER_REPLICATION"); unsetenv("RUNE_REPL_BASE_STREAM_ID");
}
```
- [ ] **Step 2: Run to verify it fails**
Run: `cd build-fast && ninja rune_node_config_tests && ./libs/node/rune_node_config_tests --gtest_filter='NodeConfig.ParsesPerWorkerReplicationFlagAndStreamBase'`
Expected: FAIL (no member `perWorkerReplication`).
- [ ] **Step 3: Add the fields + parse**
In `NodeConfig.h` add `bool perWorkerReplication = false;` and `int32_t replBaseStreamId = 2000;` near `replicationStreamId`; add `int32_t workerCount = 0;` to `PeerConfig`. In `NodeConfig.cpp` parse the two env vars (mirror an existing bool/int parse such as the `RUNE_AERON_THREADING_MODE`/`replicationStreamId` handling) and copy `workerCount` into the self peer descriptor.
- [ ] **Step 4: Run to verify it passes**
Run: same as Step 2. Expected: PASS.
- [ ] **Step 5: Verify the disjoint-stream guard**
Add assertion test that `replBaseStreamId .. replBaseStreamId+workerCount-1` does not overlap `clientStreamId`, `workerIngressBaseStreamId..+wc`, `workerEgressBaseStreamId..+wc`, `replicationStreamId`. Implement a `NodeConfig::validateStreamRanges()` returning `std::optional<std::string>` (error text) and call it in the parse path; test asserts a deliberately-overlapping base returns an error.
- [ ] **Step 6: Commit**
```bash
git add libs/node/include/rune/node/NodeConfig.h libs/node/src/NodeConfig.cpp libs/node/tests/node/NodeConfigTest.cpp
git commit -m "feat(node): NodeConfig perWorkerReplication flag + replBaseStreamId + peer workerCount"
```

---

## Task 2: Symmetric-worker-count validator (hard-refuse)

**Files:**
- Create: `libs/node/include/rune/node/WorkerReplicationValidator.h`
- Test: `libs/node/tests/node/WorkerReplicationValidatorTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (register the new test target)

**Interfaces:**
- Produces:
```cpp
namespace rune::node {
struct WorkerCountCheck { bool ok; std::string error; };
// Pure function: returns ok=true iff every peer's advertised workerCount equals self.
// A peer with workerCount==0 (unknown/not-yet-advertised) is tolerated (skipped),
// so a node can start before all peers have advertised; re-checked on membership change.
inline WorkerCountCheck validateSymmetricWorkerCount(
    int32_t selfWorkerCount,
    const std::vector<std::pair<int32_t /*nodeId*/, int32_t /*workerCount*/>>& peers) noexcept;
}
```

- [ ] **Step 1: Write the failing tests**
```cpp
using rune::node::validateSymmetricWorkerCount;
TEST(WorkerReplicationValidator, AcceptsEqualCounts) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,4}});
    EXPECT_TRUE(r.ok);
}
TEST(WorkerReplicationValidator, ToleratesUnknownPeerCount) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,0}});   // peer 2 not yet advertised
    EXPECT_TRUE(r.ok);
}
TEST(WorkerReplicationValidatorRegression, RejectsMismatchNamingNode) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,2}});
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.error.find("node 2"), std::string::npos);
    EXPECT_NE(r.error.find("2"), std::string::npos);   // the offending count
}
TEST(WorkerReplicationValidatorRegression, RejectsOnMembershipChange) {
    // simulate a peer rejoining with a new count (same function, new peer list)
    auto r = validateSymmetricWorkerCount(4, {{1,4},{3,8}});
    EXPECT_FALSE(r.ok);
}
```
- [ ] **Step 2: Run to verify it fails**
Run: `cd build-fast && ninja rune_worker_replication_validator_tests && ./libs/node/rune_worker_replication_validator_tests`
Expected: FAIL to compile (header missing).
- [ ] **Step 3: Implement the validator**
```cpp
inline WorkerCountCheck validateSymmetricWorkerCount(
    int32_t self, const std::vector<std::pair<int32_t,int32_t>>& peers) noexcept {
    for (auto& [nodeId, wc] : peers) {
        if (wc == 0) continue;                 // not yet advertised — tolerate
        if (wc != self)
            return { false, "per-worker replication requires equal RUNE_WORKERS: node "
                     + std::to_string(nodeId) + " advertises " + std::to_string(wc)
                     + " but self has " + std::to_string(self) };
    }
    return { true, {} };
}
```
- [ ] **Step 4: Run to verify it passes**
Run: same as Step 2. Expected: PASS (4 tests).
- [ ] **Step 5: Commit**
```bash
git add libs/node/include/rune/node/WorkerReplicationValidator.h libs/node/tests/node/WorkerReplicationValidatorTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(node): symmetric worker-count validator for per-worker replication (hard-refuse)"
```

---

## Task 3: WorkerReplicationTransport — send path (per-worker sub + per-peer pubs)

**Files:**
- Create: `libs/node/include/rune/node/WorkerReplicationTransport.h`
- Test: `libs/node/tests/node/WorkerReplicationTransportTest.cpp`
- Modify: `libs/node/CMakeLists.txt`

**Interfaces:**
- Consumes: `rune::replication::PartitionTransport` (6 virtuals: `sendFetchRequest`, `sendFetchResponse`, `sendOffsetsForLeaderEpochRequest`, `sendOffsetsForLeaderEpochResponse`, `sendReconcileRequest`, `sendReconcileResponse`), the SBE encoders in `ReplicationMessages.h` (`FetchRequestMsg::encode`, `FetchResponseMsg::encodedSize/encode`, `stampTemplateId`, `kReconcileFetch*TemplateId`), and the `enqueueFetchResponse`-style direct-encode pattern from `AeronPartitionTransport.cpp` (#2b).
- Produces:
```cpp
class WorkerReplicationTransport final : public rune::replication::PartitionTransport {
public:
    WorkerReplicationTransport(std::shared_ptr<aeron::Aeron> aeron,
                               int32_t workerId,
                               std::string inboundChannel, int32_t inboundStreamId,     // replBase + w
                               std::vector<std::pair<rune::replication::NodeId,std::string>> peerChannels, // per peer send channel
                               int32_t peerStreamId /* replBase + w on the peer */) noexcept;
    bool resolved() const noexcept;                         // all pubs + sub live
    void sendFetchRequest(const FetchRequestMsg&) override; // offers directly to the peer pub
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override; // direct-encode-into-claim (#2b)
    void sendOffsetsForLeaderEpochRequest(NodeId, const OffsetsForLeaderEpochReqMsg&) override;
    void sendOffsetsForLeaderEpochResponse(NodeId, const OffsetsForLeaderEpochRespMsg&) override;
    void sendReconcileRequest(NodeId, const FetchRequestMsg&) override;
    void sendReconcileResponse(NodeId, const FetchResponseMsg&) override;
    // poll() added in Task 4.
};
```
  Send target: `sendFetchRequest` addresses the partition's leader — but on a per-worker plane the leader is worker `w` on the leader node, so the transport sends to the peer identified by `req`/`resp` NodeId on `peerStreamId`. Peer→pub lookup is a small `unordered_map<NodeId, ExclusivePublication>` owned by this worker.

- [ ] **Step 1: Write the failing test (send encodes correct frame to the right peer pub)**
Use a loopback/in-memory Aeron double OR (preferred, no Aeron) a seam: extract the frame-encode into a `encodeFetchResponseFrame(buf, resp)` free function reused from #2b and test that `WorkerReplicationTransport::sendFetchResponse` selects the peer pub for `toFollower` and encodes a byte-identical frame to the `AeronPartitionTransport` path.
```cpp
TEST(WorkerReplicationTransport, SendFetchResponseSelectsPeerPubAndEncodesFrame) {
    FakePubRegistry pubs;                       // records offers per NodeId
    auto t = makeTestTransport(/*workerId*/1, /*peers*/{2,3}, pubs);
    FetchResponseMsg resp = makeResp(/*part*/5, /*records*/blob(1024));
    t.sendFetchResponse(/*toFollower*/3, resp);
    ASSERT_EQ(pubs.offersTo(3).size(), 1u);
    EXPECT_TRUE(frameEquals(pubs.offersTo(3)[0], encodeExpected(resp)));
    EXPECT_TRUE(pubs.offersTo(2).empty());
}
```
- [ ] **Step 2: Run to verify it fails**
Run: `cd build-fast && ninja rune_worker_replication_transport_tests && ./libs/node/rune_worker_replication_transport_tests`
Expected: FAIL to compile (class missing).
- [ ] **Step 3: Implement construction + send methods**
Mirror `WorkerIngress` ctor (bounded resolve loop `addSubscription`/`findSubscription`; and per-peer `addExclusivePublication`/`findExclusivePublication`). Each send method encodes directly into a `BufferClaim` via `ExclusivePublication::tryClaim` and the #2b in-claim encode (no intermediate wire buffer, no outbound ring); on back-pressure (`tryClaim<0`) drop (retriable — the follower re-fetches). `sendReconcile*` re-stamp the templateId in place (as `enqueueFetchResponse(reconcile=true)` does).
- [ ] **Step 4: Run to verify it passes**
Run: same as Step 2. Expected: PASS.
- [ ] **Step 5: Commit**
```bash
git add libs/node/include/rune/node/WorkerReplicationTransport.h libs/node/tests/node/WorkerReplicationTransportTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(node): WorkerReplicationTransport send path (per-worker sub + per-peer pubs, direct-claim encode)"
```

---

## Task 4: WorkerReplicationTransport — receive + inline apply (kills #2 recv copy)

**Files:**
- Modify: `libs/node/include/rune/node/WorkerReplicationTransport.h` (add `poll`)
- Modify: `libs/node/tests/node/WorkerReplicationTransportTest.cpp`

**Interfaces:**
- Produces:
```cpp
// dispatch is called INSIDE the poll callback with a zero-copy view into the term
// buffer (valid only for the call). The owning worker applies straight to its replica
// (onFetchRequest / applyReplicated / OLE / reconcile) — no records.assign copy.
int poll(int fragmentLimit,
         const std::function<void(int32_t topicId, int32_t partition,
                                  uint16_t templateId,
                                  const uint8_t* frame, std::size_t len)>& dispatch);
```
(Match the no-`std::function`-on-hot-path convention if the existing code uses a trampoline; mirror `WorkerIngress::poll`'s trampoline exactly rather than a `std::function` if that is the established pattern.)

- [ ] **Step 1: Write the failing test (round-trip + lifetime)**
```cpp
TEST(WorkerReplicationTransport, PollDispatchesFrameToOwningReplicaInline) {
    // loopback: worker-1 plane sends to itself; poll must surface the decoded frame
    auto t = makeLoopbackTransport(/*workerId*/1);
    FetchResponseMsg resp = makeResp(/*part*/5, blob(1024));
    t.sendFetchResponse(/*self*/1, resp);
    std::vector<int32_t> seenParts;
    int n = t.poll(10, [&](int32_t tid,int32_t part,uint16_t tmpl,const uint8_t* f,std::size_t l){
        seenParts.push_back(part);
        EXPECT_EQ(FetchResponseMsg::decode(f,0,l).records.size(), 1024u); // consumed in-callback
    });
    EXPECT_EQ(n, 1);
    ASSERT_EQ(seenParts, (std::vector<int32_t>{5}));
}
TEST(WorkerReplicationTransportRegression, BackToBackPollsDoNotAliasRecords) {
    auto t = makeLoopbackTransport(1);
    t.sendFetchResponse(1, makeResp(5, blobPattern(0xAA,1024)));
    t.sendFetchResponse(1, makeResp(6, blobPattern(0xBB,1024)));
    std::vector<uint8_t> firstByteSeen;
    t.poll(1, [&](auto,auto,auto,const uint8_t* f,std::size_t l){
        firstByteSeen.push_back(FetchResponseMsg::decode(f,0,l).records[0]); });
    // first poll must have fully consumed frame 1 before frame 2's buffer is touched
    EXPECT_EQ(firstByteSeen.front(), 0xAA);
}
```
- [ ] **Step 2: Run to verify it fails**
Run: `./libs/node/rune_worker_replication_transport_tests` — FAIL (no `poll`).
- [ ] **Step 3: Implement poll**
Mirror `WorkerIngress::poll` (FragmentAssembler + trampoline). In the fragment callback: read the SBE header templateId, decode enough to get `(topicId, partition)`, and invoke `dispatch(...)` with the raw frame view. No copy out of the term buffer.
- [ ] **Step 4: Run to verify it passes** — Expected PASS.
- [ ] **Step 5: Commit**
```bash
git add libs/node/include/rune/node/WorkerReplicationTransport.h libs/node/tests/node/WorkerReplicationTransportTest.cpp
git commit -m "feat(node): WorkerReplicationTransport poll + inline dispatch (zero-copy recv, no records.assign)"
```

---

## Task 5: BrokerNode wiring behind the flag

**Files:**
- Modify: `libs/node/src/BrokerNode.cpp` (replication build path, ~378-394 peer setup; ~768-847 per-worker construction pattern)
- Test: `libs/node/tests/node/BrokerNodeTest.cpp` (or a focused new `PerWorkerReplicationWiringTest.cpp`)

**Interfaces:**
- Consumes: Task 1 config, Task 2 validator, Task 3/4 transport.
- Produces: when `perWorkerReplication && topicId≥0`, the replica is constructed with a `WorkerReplicationTransport*` for `workerForPartition(topicId,part)`; otherwise the legacy `AeronPartitionTransport*`.

- [ ] **Step 1: Write the failing tests (flag-off unchanged; internal stays legacy; resolve-fail aborts)**
```cpp
TEST(PerWorkerReplicationWiring, FlagOffUsesLegacyTransportForAll) {
    auto node = buildTestNode({.perWorkerReplication=false, .workers=2, .userParts=4});
    EXPECT_EQ(node.perWorkerPlanesConstructed(), 0);
    EXPECT_TRUE(node.allReplicasOnLegacyTransport());
}
TEST(PerWorkerReplicationWiring, FlagOnUserPartsOnPlanesInternalOnLegacy) {
    auto node = buildTestNode({.perWorkerReplication=true, .workers=2, .userParts=4});
    EXPECT_EQ(node.perWorkerPlanesConstructed(), 2);           // W planes
    EXPECT_TRUE(node.userReplicasOnPerWorkerTransport());      // topicId>=0
    EXPECT_TRUE(node.internalReplicasOnLegacyTransport());     // topicId<0
}
TEST(PerWorkerReplicationWiringRegression, MismatchedPeerWorkerCountRefusesStart) {
    EXPECT_THROW(buildTestNode({.perWorkerReplication=true, .workers=4,
                                .peerWorkerCounts={{2,2}}}), std::runtime_error);
}
TEST(PerWorkerReplicationWiringRegression, UnresolvedPlaneAbortsStart) {
    EXPECT_THROW(buildTestNodeWithUnresolvablePlane(), std::runtime_error);
}
```
- [ ] **Step 2: Run to verify it fails** — `ninja rune_broker_node_tests && ./libs/node/rune_broker_node_tests --gtest_filter='PerWorkerReplication*'` → FAIL.
- [ ] **Step 3: Implement wiring**
In the replication build path: if `perWorkerReplication`, call `validateSymmetricWorkerCount`; on `!ok` throw `std::runtime_error(r.error)` (hard-refuse). Construct W `WorkerReplicationTransport` instances (mirror the per-worker ingress/egress construction at BrokerNode.cpp:768-847), each with inbound stream `replBaseStreamId+w` and per-peer send channel + `replBaseStreamId+w`. If any transport `!resolved()` within the bounded window, throw. Keep the legacy `AeronPartitionTransport` for internal partitions and for flag-off. When creating each replica, choose its transport by `topicId<0 ? legacy : perWorker[workerForPartition(...)]`.
- [ ] **Step 4: Run to verify it passes** — Expected PASS.
- [ ] **Step 5: Commit**
```bash
git add libs/node/src/BrokerNode.cpp libs/node/tests/node/*.cpp
git commit -m "feat(node): wire per-worker replication planes behind RUNE_PER_WORKER_REPLICATION (internal stays legacy)"
```

---

## Task 6: Worker drives its replication transport poll

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h` (`doWork()` loop ~616; store the worker's `WorkerReplicationTransport*`)
- Test: `libs/node/tests/node/WorkerTest.cpp`

**Interfaces:**
- Consumes: Task 4 `poll`; the worker already loops its owned replicas' `doWork()`.
- Produces: each `doWork()` cycle calls `replTransport_->poll(limit, dispatch)` where `dispatch` routes to the owned replica's `onFetchRequest`/`applyReplicated`/OLE/reconcile by templateId, inline (no WorkerOp hop).

- [ ] **Step 1: Write the failing test**
```cpp
TEST(Worker, DoWorkPollsReplicationTransportAndAppliesInline) {
    auto w = makeWorkerWithReplica(/*part*/5, /*role*/Follower);
    injectInboundFetchResponse(w, /*part*/5, blob(1024));   // via loopback transport
    w.doWork();
    EXPECT_EQ(replicaLeo(w, 5), 1);                         // applied inline, no ring hop
}
```
- [ ] **Step 2: Run to verify it fails** — `ninja rune_worker_pool_tests && ./libs/node/rune_worker_pool_tests --gtest_filter='Worker.DoWorkPolls*'` → FAIL.
- [ ] **Step 3: Implement**
In `doWork()`, after driving owned replicas, call `replTransport_->poll(...)`; the dispatch switches on templateId (FetchRequest→`onFetchRequest`, FetchResponse→`applyReplicated`, OLE req/resp, reconcile req/resp) and calls the replica found via the worker's local owned-replica map by `(topicId,partition)`. Add the poll fragment count to the doWork work-count so idle detection stays correct.
- [ ] **Step 4: Run to verify it passes** — Expected PASS.
- [ ] **Step 5: Commit**
```bash
git add libs/node/include/rune/node/Worker.h libs/node/tests/node/WorkerTest.cpp
git commit -m "feat(node): worker drives per-worker replication poll + inline apply in doWork"
```

---

## Task 7: In-process integration + failover (flag on)

**Files:**
- Create: `libs/node/tests/integration/PerWorkerReplicationTest.cpp`
- Modify: `libs/node/tests/integration/GatewayFailoverReplicationTest.cpp` (run its scenarios with the flag on)
- Modify: `libs/node/CMakeLists.txt`

**Interfaces:**
- Consumes: the whole stack wired flag-on.

- [ ] **Step 1: Write failing integration tests**
```cpp
TEST(PerWorkerReplication, AcksAllThreeNodeReplicatesAndCommits) { /* 3 in-proc nodes, flag on, produce N, all followers reach LEO=N, HW=N */ }
TEST(PerWorkerReplication, RollingLeaderKillNoCommittedLoss) { /* kill leader mid-produce, new leader, assert no committed record lost, ISR re-expands */ }
TEST(PerWorkerReplication, EpochChangeTruncatesDivergentTailOnly) { /* OLE probe + reconcile path over per-worker plane */ }
```
- [ ] **Step 2: Run to verify they fail** (or are skipped without flag) — build + run the new target.
- [ ] **Step 3: Make them pass** — fix any per-worker-plane routing/lifetime bugs surfaced (this is where real defects appear; reproduce any later Docker failure here first per `feedback_integration_before_e2e`).
- [ ] **Step 4: Run existing suites flag-on**
Run: `./libs/replication/rune_replication_tests` (340/340) + `./libs/node/rune_gateway_failover_replication_tests` + `rune_gateway_produce_replication_tests` + `rune_gateway_acks_all_deferred_ack_tests`, with `RUNE_PER_WORKER_REPLICATION=true`. Expected: all green.
- [ ] **Step 5: Commit**
```bash
git add libs/node/tests/integration/PerWorkerReplicationTest.cpp libs/node/tests/integration/GatewayFailoverReplicationTest.cpp libs/node/CMakeLists.txt
git commit -m "test(node): in-process per-worker replication + failover suite (flag on) green"
```

---

## Task 8: TSan + dockerized DR matrix (flag on) + flag-off smoke

**Files:**
- Modify: DR harness config to pass `RUNE_PER_WORKER_REPLICATION=true` (env plumbs through `cluster.sh`/compose).
- No product code unless a defect is found (then add a `*Regression*` test in Task 7's file first).

- [ ] **Step 1: TSan build of the in-process failover suite**
Run: `cd build-tsan && ninja rune_gateway_failover_replication_tests && RUNE_PER_WORKER_REPLICATION=true ./libs/node/rune_gateway_failover_replication_tests`
Expected: no TSan data-race reports (workers own their Aeron resources; shared-nothing).
- [ ] **Step 2: Build SHA-tagged images** (`docker-image-tag-policy`) and run the full DR matrix flag-on on the VM (netem latency/loss, node crash, rolling restart, partition/heal, multi-client).
Expected: the matrix green on `main` today is green flag-on; zero committed-loss; ISR/epoch converge time-independently.
- [ ] **Step 3: Flag-off smoke subset** — run a DR subset with the flag off; assert behavior identical to current main (legacy path unaffected).
- [ ] **Step 4: If any failure** — reproduce in the Task 7 in-process suite first, fix, add a `*Regression*` test, re-run the matrix.
- [ ] **Step 5: Commit** (harness/config + any regression tests)
```bash
git add bench/kafka-compare/*.sh docs/... 2>/dev/null; git commit -m "test(dr): per-worker replication DR matrix green flag-on; flag-off smoke unchanged; TSan clean"
```

---

## Task 9: VM smoke (flag on & off)

**Files:** none (operational).

- [ ] **Step 1: Clean box** — kill leftover `rune_broker`/producer/bench procs and any host services; verify `uptime` load < ~1 and `top` ≥ 90% idle (`k3s-node-01-repurposed-dev-box` protocol).
- [ ] **Step 2: Smoke flag-off** — `RUNE_PER_WORKER_REPLICATION=false ... replbench.sh` at 16 MiB buffers; ISR=3 forms, produces/consumes, no errors.
- [ ] **Step 3: Smoke flag-on** — `RUNE_PER_WORKER_REPLICATION=true ...`; ISR=3 forms, produces/consumes, no NO-LEADER, no errors in broker logs; rune-fr shows per-worker poll spans and `ReplOutboundDrain` absent for user partitions.
- [ ] **Step 4: Record** results to memory `rune-replication-benchmark-kafka`.

---

## Task 10: Clean-box before/after throughput ABAB (via the flag)

**Files:** none (measurement); may add a small parser to `bench/kafka-compare/`.

- [ ] **Step 1: Clean box** (as Task 9 Step 1) — verify idle before every run.
- [ ] **Step 2: ABAB, same binary, flag flipped** — A=`RUNE_PER_WORKER_REPLICATION=false`, B=`true`; interleaved **n≥15 pairs**; 3-node acks=all, **1 KB**, 16 MiB buffers, backoff; two partition:worker ratios (4p/2w and 16p/4w).
- [ ] **Step 3: Metrics** — median + IQR of rps/MB/s and p50/p99 ack latency per variant; rune-fr per-stage spans; broker CPU spread across worker threads vs one control thread.
- [ ] **Step 4: Acceptance** — B shows a clear, paired throughput increase with no latency regression. If the cluster metric is too noisy on 4 cores, fall back to rune-fr span deltas + a pinned micro-bench of the send/poll path (box-independent). If B does NOT beat A, root-cause the next serialization point in software (never bigger hardware).
- [ ] **Step 5: Record** the ABAB result + methodology to memory.

---

## Task 11: Flip flag default to on

**Files:**
- Modify: `libs/node/src/NodeConfig.cpp` (default `perWorkerReplication = true`)
- Test: update `NodeConfig` default test.

- [ ] **Step 1: Precondition** — Tasks 7–10 all green (DR matrix green flag-on; throughput ABAB favorable; smoke clean).
- [ ] **Step 2: Change the default + update the default-value test.**
- [ ] **Step 3: Re-run** the flag-default DR smoke to confirm the on-by-default cluster forms and replicates.
- [ ] **Step 4: Commit**
```bash
git add libs/node/src/NodeConfig.cpp libs/node/tests/node/NodeConfigTest.cpp
git commit -m "feat(node): default RUNE_PER_WORKER_REPLICATION=on (DR matrix + throughput validated)"
```

---

## Self-Review

**Spec coverage:** §3.1 planes → Tasks 3/4/5/6; §3.2 internal-on-legacy → Task 5; §3.3 stream scheme → Task 1; §3.4 hard-refuse → Tasks 2/5; §3.5 flag + A/B → Tasks 1/10/11; §5 composes (#1/#2c) → out of scope (noted); §6.1 unit → Tasks 1–4; §6.2 integration+failover → Task 7; §6.3 DR+TSan → Task 8; §6.4 smoke → Task 9; §6.5 throughput → Task 10; §6.6 exit criteria → gated across Tasks 7–11. No gaps.

**Placeholder scan:** no TBD/TODO; each code/test step carries concrete signatures or test bodies. The DR-matrix step references the existing harness rather than re-listing scenarios (operational, not code).

**Type consistency:** `validateSymmetricWorkerCount`, `WorkerReplicationTransport` (6 overrides + `poll`), `perWorkerReplication`/`replBaseStreamId`/`PeerConfig::workerCount` used consistently across Tasks 1–6.

**Review Focus:** all five lines have owning-task tests (mismatch-on-membership-change → T2; flag-off regression → T5; internal isolation → T5; term-buffer lifetime → T4; plane resolve-fail → T5).
