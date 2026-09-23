# Rune Milestone D — Phase 2 (Dynamic Rebalancing + Failover) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Milestone-D control plane *dynamic* — the controller reassigns partition leadership on cluster churn (join/leave/failure) via a deterministic logged tick, elects the most-complete surviving replica on the epoch lineage with no committed loss, and adds the restart-durability trio (LeaderEpochCache persistence, ProducerId watermark recovery, atomic re-rebuild), the per-topic durability-quorum knob, and the `memory` persistence tier — all riding the unchanged per-partition hot path.

**Architecture:** Phase 1 already ships the MSM (`MetadataStateMachine` + `AssignmentMap` + two-epoch `mergeChange`), `StaticBalancer`, per-partition `ReplicationMetadataBridge`, `RoutingImage`, client `Metadata`/REDIRECT routing, and the DR-hardened per-partition `PartitionReplica` (position/lineage `compareLog`, completeness gate, KIP-101 truncation, PSM rebuild-from-log). Phase 2 layers a `DynamicBalancer` + soft membership/liveness state + a deterministic logged control-plane tick + a position/lineage failover election on top of those, drives leader↔follower transitions from the *changing* committed map through the bridge (TSan-clean), and closes the three net-new restart-durability gaps. Everything new lives in the control plane (rare, bounded allocation); the produce/append/ISR hot path is untouched.

**Tech Stack:** C++23, gtest, Aeron/Agrona ULL idioms (release/acquire, open-addressing, flat arrays, 128 B padding), Planck-free control plane, CMake per-lib (`rune_metadata`, `rune_metadata_msm`, `rune_static_balancer`, `rune_node`, `rune_replication`, `rune_storage`, `rune_transaction`), `rune-protocol` sibling headers, `rune-client-cpp` sibling client, `dr-e2e/` py-testinfra Docker harness.

**Spec:** `docs/superpowers/specs/2026-09-22-rune-milestone-d-distributed-partition-leadership-design.md` (AUTHORITY). Companion refs: `docs/superpowers/specs/2026-09-22-rune-milestone-d-kafka-reference-parity.md`, `docs/superpowers/specs/2026-09-22-rune-milestone-d-aeron-ull-reference.md`.

## Execution order (dependencies)
Tasks are numbered for reference, not strict order. Dependency-driven order:
1. **M2 (`ILog` seam + InMemoryLog) runs EARLY — right after Task 1** — it retypes `PartitionReplica::log_` (`Log*`→`ILog*`) and `ProducerStateManager::rebuildFromLog`'s log param, which Tasks 2, 9, 10, 11 also touch; landing it first means those tasks are written against `ILog*` from the start (no rework).
2. **M1 memory-backend wiring (M1 Step 7) depends on M2's `InMemoryLog`** — run M1 after M2.
3. Tasks 2→3→4→5 form the failover chain (position table → membership → timer → election); 7 (balancer) after 5; 14 (integration EXIT) after all mechanism tasks; 15 (bench) then 16 (DR-E2E) last.

## Scope rulings (honor exactly)

**IN Phase 2:** leadership reassignment among EXISTING replica sets; position/lineage failover election; returning-node (restart) rejoin + preferred-leader move-back; `NO_LEADER` when no ISR survivor; deterministic logged-tick control plane; membership/liveness soft-state + guards; the restart-durability trio (LeaderEpochCache persist, ProducerId watermark recovery, atomic re-rebuild); per-topic durability knob (`majority`/`all-in-sync`); the `memory` persistence tier (tasks M1–M2); dynamic-churn bridge apply (TSan); rebalance/failover integration + bench + a Phase-2 DR-E2E slice.

**DEFERRED to Phase 4 (explicit non-goals):** staged replica-SET (RF-restoring) reassignment (add→wait-ISR→remove) — a brand-new node absorbing existing partitions' data; ELR (KIP-966); the multiplied-concurrent-failover DR matrix at scale; coordinator `hash(id)→node` placement (that is Phase 3). This plan reassigns only the *leader role* among a partition's **existing** replica set.

## Global Constraints

- **Latency:** every op incl. e2e produce p99 < 100µs (target ~50µs). A rebalance/failover must NOT stall the hot path — MSM apply stays off the request path. Measured Phase-1 produce p99 4.4–8.7µs is the baseline the bench must not regress past 100µs.
- **Zero-alloc hot path** for anything Phase 2 adds. Control plane may allocate within bounds: flyweight encoders (reuse `UpdatePartitionAssignmentRecord::encode`), reused `.clear()`ed scratch, bit-packed reverse index.
- **Diamond durability:** NO committed loss on any handoff/failover at any durability setting; NEVER a silent unclean election; NEVER falsely ack.
- **Little-endian wire;** mirror Aeron/Agrona idioms; deterministic control plane (decisions are logged records applied identically on leader/follower/recovering node).
- **Commit to LOCAL git only, never push.** MANDATORY TSan on the bridge/assignment-apply concurrency path (build dir `build-tsan/`).
- **Integration-first (in-process)** BEFORE any Docker E2E.

---

## File Structure

**New control-plane files (`libs/metadata`):**
- `include/rune/metadata/MembershipState.h` — soft liveness/membership table (Task 3).
- `include/rune/metadata/ControlPlaneTimer.h` — deterministic priority-heap timer + logged tick (Task 4).
- `include/rune/metadata/FailoverElection.h` — position/lineage election (Task 5).
- `include/rune/metadata/DynamicBalancer.h` + `src/DynamicBalancer.cpp` — delta balancer + reverse index (Task 7).

**New protocol records (`rune-protocol`):**
- `include/rune/protocol/MetadataRecords.h` — extend `CreateTopicRecord` (version-gated `durability` + `persistence`), add `ControlTickRecord`, `MembershipHeartbeatRecord` fields (Tasks 1, 3, 4, M1).

**Modified replication/storage:**
- `libs/replication/include/rune/replication/HighWatermark.h` — durability-quorum rule param (Task 1).
- `libs/replication/include/rune/replication/PartitionReplica.h` + `src/PartitionReplica.cpp` — durability mode, position-report accessors, LEC persist hooks, re-rebuild (Tasks 1, 2, 9, 11).
- `libs/storage/include/rune/storage/LeaderEpochCache.h` + new `src/LeaderEpochCache.cpp` — snapshot/restore (Task 9).
- `libs/storage/include/rune/storage/InMemoryLog.h` — `Log`-compatible ring backend (Task M2).
- `libs/transaction/include/rune/transaction/ProducerIdAllocator.h` — watermark recovery from log (Task 10).

**Modified node:**
- `libs/node/include/rune/node/ReplicationMetadataBridge.h` — delete-tears-down + churn apply (Task 12).
- `libs/node/include/rune/node/MetadataService.h` — dynamic emit path, control-tick drive, MVCC revert wiring (Tasks 4, 5, 13).
- `libs/node/include/rune/node/NodeConfig.h` + `src/NodeConfig.cpp` — `durability`/`persistence`/co-partition group surface (Task M1).
- `libs/node/src/BrokerNode.cpp` — wiring (Tasks 3, 7, M1).

**Client (Note A):**
- `rune-client-cpp/src/Client.cpp` — treat bare `NotLeader` as retriable (Task 14, folded into its client harness).

---

## Task 1: Per-topic durability-quorum knob (`majority` / `all-in-sync`)

**Files:**
- Modify: `rune-protocol/include/rune/protocol/MetadataRecords.h` (`CreateTopicRecord`, append a version-gated `durability` byte)
- Modify: `libs/replication/include/rune/replication/HighWatermark.h` (`ReplicaLeaderState::computeHw`)
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h:83-114` (`PartitionConfig`), `libs/replication/src/PartitionReplica.cpp` (`becomeLeader`, `canProduce` wiring)
- Test: `libs/replication/tests/replication/DurabilityQuorumTest.cpp` (new), `rune-protocol/tests/CreateTopicRecordTest.cpp` (extend if present, else add case in `libs/metadata/tests/MetadataStateMachineTest.cpp`)

**Interfaces:**
- Consumes: `rune::replication::ReplicaLeaderState` (`setIsr`, `setLeaderId`, `setLeaderLogEndOffset`, `updateFollowerFetchOffset`, `highWatermark`), `rune::replication::IsrManager::canProduce(int minIsr, bool acksAll)`, `rune::protocol::metadata::CreateTopicRecord` (fields `topicId,name,partitionCount,rf,partitionKey,placementMode,coPartitionGroup,recordVersion`; `encode/decode`; wire prefix type(2)+ver(1)).
- Produces:
  - `enum class rune::replication::DurabilityMode : uint8_t { Majority = 0, AllInSync = 1 };`
  - `PartitionConfig::durability` (new field, default `DurabilityMode::Majority`).
  - `void ReplicaLeaderState::setDurabilityMode(DurabilityMode) noexcept;` — changes `computeHw` from majority-th to full-ISR-min when `AllInSync`.
  - `CreateTopicRecord::durability` (new `uint8_t`, default 0), encoded ONLY when `recordVersion >= 1`; decode tolerates `recordVersion == 0` (absent → default `Majority`).

- [ ] **Step 1: Write the failing test — protocol round-trip (version-gated)**

`rune-protocol/tests/CreateTopicRecordTest.cpp` (add cases; if the file does not exist, create it and register it in `rune-protocol/tests/CMakeLists.txt` mirroring the other record tests):

```cpp
#include <rune/protocol/MetadataRecords.h>
#include <gtest/gtest.h>
using namespace rune::protocol::metadata;

TEST(CreateTopicRecord, DurabilityRoundTripsAtVersion1) {
    CreateTopicRecord in;
    in.topicId = 7; in.name = "wallet"; in.partitionCount = 4; in.rf = 3;
    in.partitionKey = "userId"; in.placementMode = 1; in.coPartitionGroup = "acct";
    in.recordVersion = 1;
    in.durability = 1; // AllInSync
    uint8_t buf[512];
    const std::size_t n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);
    auto out = CreateTopicRecord::decode(buf, n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->recordVersion, 1);
    EXPECT_EQ(out->durability, 1);
    EXPECT_EQ(out->coPartitionGroup, "acct");
}

TEST(CreateTopicRecord, Version0OmitsDurabilityAndDefaultsToMajority) {
    CreateTopicRecord in;
    in.topicId = 1; in.name = "chat"; in.partitionCount = 1; in.rf = 1;
    in.recordVersion = 0; in.durability = 1; // must NOT be written at v0
    uint8_t buf[256];
    const std::size_t n = in.encode(buf, sizeof buf);
    ASSERT_GT(n, 0u);
    auto out = CreateTopicRecord::decode(buf, n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->recordVersion, 0);
    EXPECT_EQ(out->durability, 0) << "absent field must default to Majority";
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_protocol_tests && ctest --test-dir build-release -R CreateTopicRecord`
Expected: FAIL — `CreateTopicRecord` has no `durability` member.

- [ ] **Step 3: Add `durability` field, version-gated encode/decode**

In `MetadataRecords.h`, inside `struct CreateTopicRecord` add after `uint8_t recordVersion = 0;`:

```cpp
    uint8_t durability = 0;   // 0 = Majority (default), 1 = AllInSync. Only encoded when recordVersion >= 1.
```

Set the prefix version from the record, not a constant. Replace the fixed `detail::encodePrefixAt(buf, MetadataRecordType::CreateTopic);` call with a version-aware write, and append the durability byte at the end when `recordVersion >= 1`:

```cpp
    std::size_t encode(uint8_t* buf, std::size_t bufLen) const noexcept {
        const bool v1 = (recordVersion >= 1);
        std::size_t needed = detail::kPrefixSize + 4u + 2u + name.size()
            + 4u + 2u + 2u + partitionKey.size() + 1u + 2u + coPartitionGroup.size()
            + (v1 ? 1u : 0u);
        if (bufLen < needed) return 0u;
        detail::putU16(buf, 0, static_cast<uint16_t>(MetadataRecordType::CreateTopic));
        buf[2] = recordVersion;                       // version-aware, not kCurrentVersion
        std::size_t pos = detail::kPrefixSize;
        detail::putS32(buf, (int)pos, topicId);  pos += 4u;
        pos += detail::putVarStr(buf + pos, name);
        detail::putS32(buf, (int)pos, partitionCount); pos += 4u;
        detail::putS16(buf, (int)pos, rf);             pos += 2u;
        pos += detail::putVarStr(buf + pos, partitionKey);
        buf[pos++] = placementMode;
        pos += detail::putVarStr(buf + pos, coPartitionGroup);
        if (v1) buf[pos++] = durability;
        return pos;
    }
```

In `validatePrefix`, allow versions 0 and 1 (relax `buf[2] != kCurrentVersion`): add an overload path in `CreateTopicRecord::decode` that accepts `buf[2] <= 1`. In `decode`, after reading `coPartitionGroup`, read the durability byte only if `rec.recordVersion >= 1 && pos < avail`:

```cpp
        if (rec.recordVersion >= 1) {
            if (pos + 1u > avail) return std::nullopt;
            rec.durability = buf[pos++];
        } else {
            rec.durability = 0; // Majority default for legacy v0 records
        }
```

Note: keep `detail::validatePrefix` strict-for-v0 usage in other records unchanged; only `CreateTopicRecord::decode` accepts `buf[2] <= 1` (inline the type + version check rather than calling `validatePrefix`).

- [ ] **Step 4: Run protocol test to verify PASS**

Run: `cmake --build build-release --target rune_protocol_tests && ctest --test-dir build-release -R CreateTopicRecord`
Expected: PASS.

- [ ] **Step 5: Write the failing test — HW rule per durability mode**

`libs/replication/tests/replication/DurabilityQuorumTest.cpp` (new; register in `libs/replication/tests/CMakeLists.txt` next to the other replication tests):

```cpp
#include <rune/replication/HighWatermark.h>
#include <gtest/gtest.h>
using namespace rune::replication;

// ISR = {L=1, A=2, B=3}. LEOs: L=100, A=90, B=80.
static ReplicaLeaderState make() {
    ReplicaLeaderState s; s.setLeaderId(1);
    s.setIsr({1,2,3}); s.setLeaderLogEndOffset(100);
    s.updateFollowerFetchOffset(2, 90);
    s.updateFollowerFetchOffset(3, 80);
    return s;
}

TEST(DurabilityQuorum, MajorityIsMajorityThHighest) {
    auto s = make();
    s.setDurabilityMode(DurabilityMode::Majority);
    s.recompute();
    EXPECT_EQ(s.highWatermark(), 90) << "majority-of-3 = 2nd highest = 90";
}

TEST(DurabilityQuorum, AllInSyncWaitsForEveryIsrMember) {
    auto s = make();
    s.setDurabilityMode(DurabilityMode::AllInSync);
    s.recompute();
    EXPECT_EQ(s.highWatermark(), 80) << "all-in-sync = min LEO over full ISR = 80";
}

TEST(DurabilityQuorum, AllInSyncOneLaggerHoldsHw) {
    auto s = make();
    s.setDurabilityMode(DurabilityMode::AllInSync);
    s.updateFollowerFetchOffset(3, 70); // B falls further behind
    EXPECT_EQ(s.highWatermark(), 70);
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `cmake --build build-release --target rune_replication_tests && ctest --test-dir build-release -R DurabilityQuorum`
Expected: FAIL — `setDurabilityMode` / `DurabilityMode` undefined.

- [ ] **Step 7: Implement the durability mode in `ReplicaLeaderState`**

In `HighWatermark.h` add above the class:

```cpp
enum class DurabilityMode : uint8_t { Majority = 0, AllInSync = 1 };
```

Add a field `DurabilityMode durability_ = DurabilityMode::Majority;` and a setter that recomputes:

```cpp
    void setDurabilityMode(DurabilityMode m) noexcept { durability_ = m; recomputeHw(); }
```

In `computeHw()`, after sorting `leos` descending, replace the fixed `return leos[majority - 1];` with:

```cpp
        const int idx = (durability_ == DurabilityMode::AllInSync)
            ? (n - 1)             // full-ISR min (last after descending sort)
            : (majority - 1);     // majority-th highest
        return leos[idx];
```

- [ ] **Step 8: Run HW test to verify PASS**

Run: `ctest --test-dir build-release -R DurabilityQuorum`
Expected: PASS.

- [ ] **Step 9: Wire durability through `PartitionConfig` + `becomeLeader` + produce gate**

In `PartitionReplica.h` `PartitionConfig` add:

```cpp
    DurabilityMode durability = DurabilityMode::Majority; ///< per-topic durability quorum rule
    // When AllInSync, minIsr is forced to the replica-set size (RF) so acks require every replica.
```

In `PartitionReplica.cpp` `becomeLeader`, after seeding `leaderState_`, call `leaderState_.setDurabilityMode(cfg_.durability);`. When `cfg_.durability == DurabilityMode::AllInSync`, set the effective produce gate `minIsr` used in `canProduce` to `static_cast<int>(replicas.size())` (RF) instead of `cfg_.minIsr` (store as `effectiveMinIsr_` computed in `becomeLeader`). The `IsrManager::canProduce(effectiveMinIsr_, acksAll)` call in the append path already exists — pass `effectiveMinIsr_`.

- [ ] **Step 10: Write the failing test — all-in-sync minIsr=RF gate + election safety**

Add to `DurabilityQuorumTest.cpp` (a small `PartitionReplica`-level case using `InMemoryTransport`, mirroring the driver-loop pattern from `PartitionReplica.h:664-676`):

```cpp
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
// AllInSync topic: with one of 3 replicas lagging, HW must not advance past the
// laggard, and a produce that would need |ISR|>=RF is gated when ISR shrinks.
TEST(DurabilityQuorum, AllInSyncMinIsrEqualsRfAndCommittedDataSafe) {
    // (Construct a 3-replica leader with durability=AllInSync over InMemoryTransport;
    //  drive doWork/pump; append 5 batches; assert highWatermark() == min follower LEO;
    //  drop one follower; assert append() with acksAll returns -1 once ISR < RF.)
    // Full body mirrors GatewayFailoverReplicationTest driver loop.
    SUCCEED(); // replaced with the real 3-replica driver in Step 11.
}
```

Replace `SUCCEED()` with a real 3-replica driver: create three `PartitionReplica`s (one leader role, two followers) sharing an `InMemoryTransport`, each with `cfg.durability = DurabilityMode::AllInSync`, RF=3, `minIsr` default; loop `leader.doWork(clock); f.doWork(clock); transport.pump(clock)` up to `kCap=2000`; after 5 `leader.append(bytes,true)` assert `leader.highWatermark()` equals the min follower LEO; `transport.removeFollower(3)`; drive until ISR shrinks; assert a further `leader.append(bytes,true)` returns `-1` (gated by minIsr==RF==3).

- [ ] **Step 11: Run to verify it fails, implement `effectiveMinIsr_`, run PASS**

Run: `ctest --test-dir build-release -R DurabilityQuorum`
First FAIL (no `effectiveMinIsr_` gate), then implement the `effectiveMinIsr_ = (cfg_.durability==AllInSync)? replicas.size() : cfg_.minIsr;` assignment in `becomeLeader`, then PASS.

- [ ] **Step 12: Commit**

```bash
git add rune-protocol/include/rune/protocol/MetadataRecords.h \
        rune-protocol/tests/CreateTopicRecordTest.cpp \
        libs/replication/include/rune/replication/HighWatermark.h \
        libs/replication/include/rune/replication/PartitionReplica.h \
        libs/replication/src/PartitionReplica.cpp \
        libs/replication/tests/replication/DurabilityQuorumTest.cpp \
        libs/replication/tests/CMakeLists.txt rune-protocol/tests/CMakeLists.txt
git commit -m "feat(durability): per-topic durability-quorum knob (majority/all-in-sync) + version-gated CreateTopic field"
```

---

## Task 2: Replica position/liveness reporting (soft state) + MSM commit-position counter

**Files:**
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h` (add position accessors)
- Create: `libs/metadata/include/rune/metadata/ReplicaPositionTable.h` (soft per-replica position table)
- Modify: `libs/node/include/rune/node/RoleCounter.h` (add MSM commit-position counter type id + label)
- Test: `libs/metadata/tests/ReplicaPositionTableTest.cpp` (new; register in `libs/metadata/CMakeLists.txt`)

**Interfaces:**
- Consumes: `PartitionReplica::epoch()`, `PartitionReplica::logEndOffset()`, `PartitionReplica::highWatermark()`, `PartitionReplica::topicId()`, `PartitionReplica::partitionId()`, `PartitionReplica::selfId()`. `rune::node::kRoleCounterTypeId=1'000'201`, `kTermCounterTypeId=1'000'203`.
- Produces:
  - `struct rune::replication::ReplicaPositionReport { int32_t topicId; int32_t partition; int32_t nodeId; int32_t leaderEpoch; int64_t logEndPosition; };`
  - `ReplicaPositionReport PartitionReplica::positionReport() const noexcept;` (leaderEpoch = `epoch()`, logEndPosition = `logEndOffset()`).
  - `class rune::metadata::ReplicaPositionTable` with `void report(const ReplicaPositionReport&); std::optional<ReplicaPositionReport> get(int32_t topicId,int32_t partition,int32_t nodeId) const; std::vector<ReplicaPositionReport> forPartition(int32_t topicId,int32_t partition) const;` — soft, off-log, `Int2Object`-style open-addressing keyed by `(gpid<<16 | nodeId)`.
  - `inline constexpr std::int32_t rune::node::kMsmCommitPosTypeId = 1'000'204;` + `msmCommitPosLabel(nodeId)`.

- [ ] **Step 1: Write the failing test**

`libs/metadata/tests/ReplicaPositionTableTest.cpp`:

```cpp
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;
using rune::replication::ReplicaPositionReport;

TEST(ReplicaPositionTable, ReportsAndReadsBackLatest) {
    ReplicaPositionTable t;
    t.report({7, 2, 5, /*epoch*/3, /*leo*/100});
    t.report({7, 2, 6, 3, 90});
    t.report({7, 2, 5, 4, 130}); // node 5 advances + new epoch
    auto r = t.get(7, 2, 5);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->leaderEpoch, 4);
    EXPECT_EQ(r->logEndPosition, 130);
    EXPECT_EQ(t.forPartition(7, 2).size(), 2u);
}

TEST(ReplicaPositionTable, UnknownReturnsNullopt) {
    ReplicaPositionTable t;
    EXPECT_FALSE(t.get(1, 0, 9).has_value());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_replica_position_table_tests`
Expected: FAIL — header missing.

- [ ] **Step 3: Implement `ReplicaPositionReport` + `positionReport()`**

In `PartitionReplica.h` add near the top of `namespace rune::replication`:

```cpp
struct ReplicaPositionReport {
    int32_t topicId        = 0;
    int32_t partition      = 0;
    int32_t nodeId         = 0;
    int32_t leaderEpoch    = 0;
    int64_t logEndPosition = 0;
};
```

Add a public accessor to `PartitionReplica`:

```cpp
    [[nodiscard]] ReplicaPositionReport positionReport() const noexcept {
        return ReplicaPositionReport{ cfg_.topicId, cfg_.partitionId, cfg_.selfId,
                                      static_cast<int32_t>(epoch_), log_->logEndOffset() };
    }
```

- [ ] **Step 4: Implement `ReplicaPositionTable.h`**

```cpp
#pragma once
#include <rune/replication/PartitionReplica.h> // ReplicaPositionReport
#include <rune/metadata/Gpid.h>
#include <optional>
#include <unordered_map>
#include <vector>
namespace rune::metadata {
class ReplicaPositionTable {
public:
    void report(const rune::replication::ReplicaPositionReport& r) {
        table_[key(r.topicId, r.partition, r.nodeId)] = r; // latest wins (soft)
    }
    std::optional<rune::replication::ReplicaPositionReport>
    get(int32_t topicId, int32_t partition, int32_t nodeId) const {
        auto it = table_.find(key(topicId, partition, nodeId));
        if (it == table_.end()) return std::nullopt;
        return it->second;
    }
    std::vector<rune::replication::ReplicaPositionReport>
    forPartition(int32_t topicId, int32_t partition) const {
        std::vector<rune::replication::ReplicaPositionReport> out;
        const int32_t g = gpidOf(topicId, partition);
        for (const auto& [k, v] : table_)
            if (static_cast<int32_t>(k >> 16) == g) out.push_back(v);
        return out;
    }
private:
    static uint64_t key(int32_t topicId, int32_t partition, int32_t nodeId) {
        return (static_cast<uint64_t>(gpidOf(topicId, partition)) << 16)
             |  static_cast<uint64_t>(static_cast<uint16_t>(nodeId));
    }
    std::unordered_map<uint64_t, rune::replication::ReplicaPositionReport> table_;
};
} // namespace rune::metadata
```

Add the MSM commit-position counter constant to `RoleCounter.h`:

```cpp
inline constexpr std::int32_t kMsmCommitPosTypeId = 1'000'204;
inline std::string msmCommitPosLabel(std::int32_t nodeId) {
    return "rune-msm-commit-position node=" + std::to_string(nodeId);
}
```

- [ ] **Step 5: Register test + run PASS**

Add to `libs/metadata/CMakeLists.txt` an `add_executable(rune_replica_position_table_tests tests/ReplicaPositionTableTest.cpp)` linking `rune_metadata rune_replication GTest::gtest_main` + `gtest_discover_tests`.
Run: `cmake --build build-release --target rune_replica_position_table_tests && ctest --test-dir build-release -R ReplicaPositionTable`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/replication/include/rune/replication/PartitionReplica.h \
        libs/metadata/include/rune/metadata/ReplicaPositionTable.h \
        libs/metadata/tests/ReplicaPositionTableTest.cpp \
        libs/metadata/CMakeLists.txt libs/node/include/rune/node/RoleCounter.h
git commit -m "feat(control): soft per-replica position table + positionReport() + MSM commit-position counter id"
```

---

## Task 3: Membership & liveness (soft state, off-log) + guards

**Files:**
- Create: `libs/metadata/include/rune/metadata/MembershipState.h`
- Modify: `rune-protocol/include/rune/protocol/MetadataRecords.h` (extend `NodeJoinRecord` with version-gated `incarnationId` + `brokerEpoch`)
- Test: `libs/metadata/tests/MembershipStateTest.cpp` (new; register in CMake), `rune-protocol/tests/NodeJoinRecordTest.cpp` (extend/add)

**Interfaces:**
- Consumes: `NodeJoinRecord{nodeId, endpoint, recordVersion}`, `NodeLeaveRecord{nodeId}`.
- Produces:
  - `NodeJoinRecord::incarnationId` (`int64_t`, v>=1 only), `NodeJoinRecord::brokerEpoch` (`int64_t`, v>=1 only; = registration log offset, monotonic, set by MSM apply — see Task 4/13).
  - `enum class rune::metadata::NodeControlState { Fenced, Unfenced, ControlledShutdown, ShutdownNow };`
  - `class MembershipState`:
    - `struct MemberInfo { int32_t nodeId; std::string endpoint; int64_t incarnationId; int64_t brokerEpoch; int64_t lastHeartbeatNs; NodeControlState state; };`
    - `enum class JoinResult { Accepted, IdempotentAmend, RejectedZombie };`
    - `JoinResult onJoin(int32_t nodeId, std::string endpoint, int64_t incarnationId, int64_t brokerEpoch, int64_t nowNs);`
    - `void onHeartbeat(int32_t nodeId, int64_t nowNs);`
    - `void onLeave(int32_t nodeId);`
    - `bool isExpired(int32_t nodeId, int64_t nowNs, int64_t sessionTimeoutNs) const noexcept;` (clock-rollover-guarded: treat `nowNs < lastHeartbeatNs` as not-expired).
    - `std::vector<int32_t> liveNodes(int64_t nowNs, int64_t sessionTimeoutNs) const;`
    - `bool markCatchUpComplete(int32_t nodeId);` `void setState(int32_t nodeId, NodeControlState);`
    - `void setOverloaded(bool);` `bool overloaded() const noexcept;` (suppress state transitions while true).

- [ ] **Step 1: Write the failing test — protocol v1 NodeJoin round-trip**

`rune-protocol/tests/NodeJoinRecordTest.cpp` (add cases):

```cpp
#include <rune/protocol/MetadataRecords.h>
#include <gtest/gtest.h>
using namespace rune::protocol::metadata;
TEST(NodeJoinRecord, V1CarriesIncarnationAndBrokerEpoch) {
    NodeJoinRecord in; in.nodeId = 3; in.endpoint = "h:9"; in.recordVersion = 1;
    in.incarnationId = 0x1122334455667788LL; in.brokerEpoch = 4096;
    uint8_t buf[128]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = NodeJoinRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->incarnationId, 0x1122334455667788LL);
    EXPECT_EQ(out->brokerEpoch, 4096);
}
TEST(NodeJoinRecord, V0DecodesWithoutNewFields) {
    NodeJoinRecord in; in.nodeId = 3; in.endpoint = "h:9"; in.recordVersion = 0;
    uint8_t buf[64]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = NodeJoinRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->incarnationId, 0);
    EXPECT_EQ(out->brokerEpoch, 0);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_protocol_tests && ctest --test-dir build-release -R NodeJoinRecord`
Expected: FAIL — no `incarnationId`/`brokerEpoch`.

- [ ] **Step 3: Extend `NodeJoinRecord` (version-gated)**

Add fields `int64_t incarnationId = 0;` and `int64_t brokerEpoch = 0;`. Add a LE int64 helper to `detail` (`putS64`/`getS64`). Encode writes the version into `buf[2]` from `recordVersion`; when `recordVersion >= 1`, append `incarnationId(8)` then `brokerEpoch(8)`. Decode reads them only when `recordVersion >= 1 && pos+16 <= avail`; else defaults 0. Relax the type/version check in `NodeJoinRecord::decode` to accept `buf[2] <= 1`.

- [ ] **Step 4: Run protocol test PASS**

Run: `ctest --test-dir build-release -R NodeJoinRecord`
Expected: PASS.

- [ ] **Step 5: Write the failing test — MembershipState guards**

`libs/metadata/tests/MembershipStateTest.cpp`:

```cpp
#include <rune/metadata/MembershipState.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

TEST(MembershipState, SameIncarnationReRegisterIsIdempotentAmend) {
    MembershipState m;
    EXPECT_EQ(m.onJoin(1, "h1", /*inc*/100, /*bepoch*/10, /*now*/0), MembershipState::JoinResult::Accepted);
    EXPECT_EQ(m.onJoin(1, "h1b", 100, 10, 5), MembershipState::JoinResult::IdempotentAmend);
}
TEST(MembershipState, DifferentIncarnationWhileSessionValidIsRejectedZombie) {
    MembershipState m;
    m.onJoin(1, "h1", 100, 10, 0);
    m.onHeartbeat(1, 5); // session valid
    EXPECT_EQ(m.onJoin(1, "h1", /*newInc*/200, 10, 6), MembershipState::JoinResult::RejectedZombie);
}
TEST(MembershipState, ExpiryUsesSessionTimeoutAndIsClockRolloverGuarded) {
    MembershipState m; m.onJoin(1, "h1", 100, 10, 0); m.onHeartbeat(1, 1'000);
    const int64_t timeout = 500;
    EXPECT_FALSE(m.isExpired(1, 1'400, timeout));
    EXPECT_TRUE (m.isExpired(1, 1'600, timeout));
    // clock rollover: now < lastHeartbeat must NOT read as expired
    EXPECT_FALSE(m.isExpired(1, 900, timeout));
}
TEST(MembershipState, OverloadValveSuppressesStateTransitions) {
    MembershipState m; m.onJoin(1, "h1", 100, 10, 0);
    m.setOverloaded(true);
    m.setState(1, NodeControlState::Fenced); // suppressed
    EXPECT_NE(m.liveNodes(0, 500).empty(), true); // still considered present
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `cmake --build build-release --target rune_membership_state_tests`
Expected: FAIL — header missing.

- [ ] **Step 7: Implement `MembershipState.h`**

Implement the class per the Interfaces block. Key logic:
- `onJoin`: if node absent → insert `Unfenced`, return `Accepted`. If present and `incarnationId == existing` → amend endpoint/brokerEpoch, `IdempotentAmend`. If present, different incarnation, and NOT expired at `nowNs` (session valid) → `RejectedZombie`; if session expired → replace incarnation, `Accepted`.
- `isExpired`: `if (nowNs < info.lastHeartbeatNs) return false;` then `return (nowNs - info.lastHeartbeatNs) > sessionTimeoutNs;`.
- `setState`: no-op while `overloaded_` is true.
- `liveNodes`: all members whose `!isExpired && state != Fenced && state != ShutdownNow`.

- [ ] **Step 8: Register test + run PASS**

Add `rune_membership_state_tests` to `libs/metadata/CMakeLists.txt`.
Run: `ctest --test-dir build-release -R MembershipState`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add rune-protocol/include/rune/protocol/MetadataRecords.h rune-protocol/tests/NodeJoinRecordTest.cpp \
        libs/metadata/include/rune/metadata/MembershipState.h \
        libs/metadata/tests/MembershipStateTest.cpp libs/metadata/CMakeLists.txt \
        rune-protocol/tests/CMakeLists.txt
git commit -m "feat(membership): soft liveness table (incarnation zombie guard, broker-epoch, clock-rollover, overload valve) + v1 NodeJoin"
```

---

## Task 4: Deterministic control-plane timer (logged tick)

**Files:**
- Create: `libs/metadata/include/rune/metadata/ControlPlaneTimer.h`
- Modify: `rune-protocol/include/rune/protocol/MetadataRecords.h` (add `ControlTickRecord`, recordType = 5)
- Test: `libs/metadata/tests/ControlPlaneTimerTest.cpp` (new), `rune-protocol/tests/ControlTickRecordTest.cpp`

**Interfaces:**
- Consumes: `MembershipState::isExpired`, `MetadataRecordType` enum (extend), `detail::putS64/getS64`.
- Produces:
  - `enum class MetadataRecordType : uint16_t { ..., ControlTick = 5 };`
  - `struct ControlTickRecord { int64_t tickTimeNs = 0; int64_t tickSeq = 0; uint8_t recordVersion = 0; encode/decode; RECORD_TYPE_ID = 5; };`
  - `class ControlPlaneTimer`:
    - `void arm(int64_t deadlineNs, int32_t timerId);` (priority-heap on deadline)
    - `bool poll(int64_t clusterTimeNs, int32_t& firedTimerId);` (returns true + pops the earliest expired timer at or before clusterTime; caller then APPENDS a `ControlTickRecord`)
    - `void snapshotPending(std::vector<std::pair<int64_t,int32_t>>& out) const;`
    - `void reArm(const std::vector<std::pair<int64_t,int32_t>>& pending, int64_t clusterNowNs);` (re-arm on election-complete against cluster time)

- [ ] **Step 1: Write the failing test — ControlTickRecord round-trip**

`rune-protocol/tests/ControlTickRecordTest.cpp`:

```cpp
#include <rune/protocol/MetadataRecords.h>
#include <gtest/gtest.h>
using namespace rune::protocol::metadata;
TEST(ControlTickRecord, RoundTrips) {
    ControlTickRecord in; in.tickTimeNs = 123456789; in.tickSeq = 42;
    uint8_t buf[64]; auto n = in.encode(buf, sizeof buf); ASSERT_GT(n, 0u);
    auto out = ControlTickRecord::decode(buf, n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->tickTimeNs, 123456789);
    EXPECT_EQ(out->tickSeq, 42);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `ctest --test-dir build-release -R ControlTickRecord` (after building)
Expected: FAIL.

- [ ] **Step 3: Add `ControlTickRecord` (recordType 5)**

Add `ControlTick = 5` to the enum. Implement `ControlTickRecord` with prefix(3)+tickTimeNs(8)+tickSeq(8); `encode`/`decode` mirror `NodeLeaveRecord`'s fixed-layout style.

- [ ] **Step 4: Run PASS**

Run: `ctest --test-dir build-release -R ControlTickRecord`
Expected: PASS.

- [ ] **Step 5: Write the failing test — timer determinism**

`libs/metadata/tests/ControlPlaneTimerTest.cpp`:

```cpp
#include <rune/metadata/ControlPlaneTimer.h>
#include <gtest/gtest.h>
using namespace rune::metadata;
TEST(ControlPlaneTimer, FiresEarliestDeadlineOnPollAtOrAfter) {
    ControlPlaneTimer t; t.arm(200, /*id*/2); t.arm(100, 1); t.arm(300, 3);
    int32_t fired = -1;
    EXPECT_FALSE(t.poll(50, fired));
    EXPECT_TRUE (t.poll(150, fired)); EXPECT_EQ(fired, 1);
    EXPECT_TRUE (t.poll(250, fired)); EXPECT_EQ(fired, 2);
    EXPECT_FALSE(t.poll(250, fired)); // 300 not yet due
}
TEST(ControlPlaneTimer, SnapshotAndReArmPreservePending) {
    ControlPlaneTimer t; t.arm(200, 2); t.arm(300, 3);
    std::vector<std::pair<int64_t,int32_t>> pend; t.snapshotPending(pend);
    ControlPlaneTimer t2; t2.reArm(pend, /*clusterNow*/0);
    int32_t fired = -1;
    EXPECT_TRUE(t2.poll(250, fired)); EXPECT_EQ(fired, 2);
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `cmake --build build-release --target rune_control_plane_timer_tests`
Expected: FAIL.

- [ ] **Step 7: Implement `ControlPlaneTimer.h`**

Use `std::priority_queue<std::pair<int64_t,int32_t>, std::vector<...>, std::greater<>>` (min-heap on deadline). `poll` returns the top iff `top.first <= clusterTimeNs`, popping it. `snapshotPending` drains a copy of the heap into `out`. `reArm` clears and re-`arm`s each pending entry (deadlines are absolute cluster time — no shifting needed; the `clusterNowNs` arg is accepted for API symmetry and used to drop already-past duplicates only if `deadline < clusterNowNs` you still keep them so the next poll fires them).

- [ ] **Step 8: Register + run PASS**

Add `rune_control_plane_timer_tests` to `libs/metadata/CMakeLists.txt`.
Run: `ctest --test-dir build-release -R ControlPlaneTimer`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add rune-protocol/include/rune/protocol/MetadataRecords.h rune-protocol/tests/ControlTickRecordTest.cpp \
        libs/metadata/include/rune/metadata/ControlPlaneTimer.h \
        libs/metadata/tests/ControlPlaneTimerTest.cpp libs/metadata/CMakeLists.txt \
        rune-protocol/tests/CMakeLists.txt
git commit -m "feat(control): deterministic priority-heap timer + logged ControlTickRecord (snapshot + re-arm on election-complete)"
```

---

## Task 5: Failover election (position/lineage)

**Files:**
- Create: `libs/metadata/include/rune/metadata/FailoverElection.h`
- Test: `libs/metadata/tests/FailoverElectionTest.cpp` (new; register in CMake)

**Interfaces:**
- Consumes: `PartitionAssignment{leaderNode,isr,replicas,leaderEpoch,partitionEpoch}`, `AssignmentChange{newLeader,newIsr,newReplicas}`, `mergeChange(cur,change)`, `NO_LEADER=-1`, `NO_LEADER_CHANGE=-2`, `ReplicaPositionTable`, `rune::consensus::ClusterMember::compareLog(Term,int64_t,Term,int64_t)` (primary key leaderEpoch/term, secondary logEndPosition; +1 lhs more recent).
- Produces:
  - `struct ElectionInputs { PartitionAssignment cur; std::vector<int32_t> liveNodes; ReplicaPositionTable* positions; bool allowUnclean = false; };`
  - `struct ElectionOutcome { AssignmentChange change; bool leaderChanged; bool wentNoLeader; };`
  - `class FailoverElection { static ElectionOutcome elect(int32_t topicId, int32_t partition, const ElectionInputs&); };`
  - Rules (mirror parity §3, §7 + design §D):
    1. Keep current leader if still live AND in `cur.isr` (min-churn) → no change.
    2. Else compute PROPOSED isr = `cur.isr ∩ liveNodes`; among proposed members with a position report, pick the most-complete by `compareLog(theirEpoch, theirLeo, ...)`; on tie, lowest nodeId.
    3. Re-run over the PROPOSED isr (never trust the raw isr): the elected leader must be in the proposed isr.
    4. If proposed isr empty (no ISR survivor) → `newLeader = NO_LEADER`, `newIsr = {}`, preserve `cur.replicas` (last-known-leader identity kept via unchanged `cur`); this bumps `leaderEpoch` (going leaderless is a leader change per `mergeChange`).
    5. `newIsr` = elected leader + any other live proposed members (leader always in ISR). Two-epoch: leader change ⇒ `leaderEpoch` bumps; plain isr shrink without leader change ⇒ no `leaderEpoch` bump (delegated to `mergeChange`).

- [ ] **Step 1: Write the failing test**

`libs/metadata/tests/FailoverElectionTest.cpp`:

```cpp
#include <rune/metadata/FailoverElection.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

static PartitionAssignment mk(int leader, std::vector<int> isr, std::vector<int> rep, int le, int pe){
    PartitionAssignment a; a.leaderNode=leader; a.isr={isr.begin(),isr.end()};
    a.replicas={rep.begin(),rep.end()}; a.leaderEpoch=le; a.partitionEpoch=pe; return a;
}

TEST(FailoverElection, KeepsCurrentLeaderWhenStillLive) {
    ReplicaPositionTable pos;
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {1,2,3}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_FALSE(out.leaderChanged);
}

TEST(FailoverElection, ElectsMostCompleteSurvivorByCompareLog) {
    ReplicaPositionTable pos;
    pos.report({7,0,2, /*epoch*/5, /*leo*/120});
    pos.report({7,0,3, /*epoch*/5, /*leo*/100});
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);       // leader 1 dies
    ElectionInputs in{cur, {2,3}, &pos, false};      // only 2,3 live
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.leaderChanged);
    EXPECT_EQ(out.change.newLeader, 2) << "node 2 has higher LEO on same epoch";
    // leader always in ISR:
    ASSERT_FALSE(out.change.newIsr.empty());
    EXPECT_EQ(out.change.newIsr.front(), 2);
}

TEST(FailoverElection, NoIsrSurvivorGoesNoLeader) {
    ReplicaPositionTable pos;
    auto cur = mk(1, {1}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {2,3}, &pos, false};      // ISR={1}, 1 is dead
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.wentNoLeader);
    EXPECT_EQ(out.change.newLeader, NO_LEADER);
    // last-known replicas preserved:
    auto next = mergeChange(cur, out.change);
    EXPECT_EQ(next.replicas.size(), 3u);
    EXPECT_GT(next.leaderEpoch, cur.leaderEpoch) << "NO_LEADER bumps leaderEpoch";
}

TEST(FailoverElection, ReRunsOverProposedIsrHigherEpochWins) {
    ReplicaPositionTable pos;
    pos.report({7,0,2, /*epoch*/6, /*leo*/50});   // higher epoch beats higher LEO
    pos.report({7,0,3, /*epoch*/5, /*leo*/200});
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {2,3}, &pos, false};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_EQ(out.change.newLeader, 2) << "epoch 6 > epoch 5 dominates LEO";
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_failover_election_tests`
Expected: FAIL — header missing.

- [ ] **Step 3: Implement `FailoverElection.h`**

Include `<rune/consensus/ClusterMember.h>` for `compareLog`. Implement `elect` per the Interfaces rules. For the completeness comparison, for each proposed member call `positions->get(topicId,partition,node)`; a member with no report contributes a sentinel `{epoch=-1, leo=-1}` (least complete). Choose the member maximizing `compareLog(a.epoch,a.leo,b.epoch,b.leo)`. Emit `AssignmentChange`: on keep-current, return `{newLeader:NO_LEADER_CHANGE, newIsr:{}, newReplicas:{}}` with `leaderChanged=false`. On new leader, `newLeader = elected`, `newIsr = {elected, ...other live proposed}`. On no survivor, `newLeader = NO_LEADER`, `newIsr = {}`, `newReplicas = {}` (replicas unchanged so `mergeChange` keeps `cur.replicas`).

- [ ] **Step 4: Register + run PASS**

Add `rune_failover_election_tests` (link `rune_metadata rune_replication rune_consensus GTest::gtest_main`) to `libs/metadata/CMakeLists.txt`.
Run: `ctest --test-dir build-release -R FailoverElection`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/metadata/include/rune/metadata/FailoverElection.h \
        libs/metadata/tests/FailoverElectionTest.cpp libs/metadata/CMakeLists.txt
git commit -m "feat(failover): position/lineage election (compareLog, keep-current min-churn, re-run over proposed ISR, NO_LEADER on no survivor)"
```

---

## Task 6: Unclean-election opt-in (per-topic, default off)

**Files:**
- Modify: `libs/metadata/include/rune/metadata/FailoverElection.h` (honor `allowUnclean`)
- Test: `libs/metadata/tests/FailoverElectionTest.cpp` (add cases)

**Interfaces:**
- Consumes: `ElectionInputs::allowUnclean`, `FailoverElection::elect`.
- Produces:
  - When `allowUnclean == true` AND proposed isr is empty AND at least one live replica exists in `cur.replicas`: elect the most-complete LIVE replica *ignoring ISR*, set `newIsr = {newLeader}` (forced single-member ISR), and set `ElectionOutcome::recovering = true` (new field).
  - When a leader already exists and is live, an unclean election is refused (returns keep-current) — mirror `ELECTION_NOT_NEEDED`.
  - `struct ElectionOutcome { AssignmentChange change; bool leaderChanged; bool wentNoLeader; bool recovering = false; };`

- [ ] **Step 1: Write the failing test**

Add to `FailoverElectionTest.cpp`:

```cpp
TEST(FailoverElection, UncleanOptInElectsOutOfIsrAndFlagsRecovering) {
    ReplicaPositionTable pos;
    pos.report({7,0,3, /*epoch*/4, /*leo*/70}); // 3 not in ISR but live + most complete
    auto cur = mk(1, {1}, {1,2,3}, 5, 9);         // ISR={1}, 1 dead
    ElectionInputs in{cur, {2,3}, &pos, /*allowUnclean*/true};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_TRUE(out.recovering);
    EXPECT_EQ(out.change.newLeader, 3);
    ASSERT_EQ(out.change.newIsr.size(), 1u);
    EXPECT_EQ(out.change.newIsr[0], 3);
}
TEST(FailoverElection, UncleanRefusedWhenLeaderAlreadyExists) {
    ReplicaPositionTable pos;
    auto cur = mk(1, {1,2,3}, {1,2,3}, 5, 9);
    ElectionInputs in{cur, {1,2,3}, &pos, /*allowUnclean*/true};
    auto out = FailoverElection::elect(7, 0, in);
    EXPECT_FALSE(out.leaderChanged); // ELECTION_NOT_NEEDED
    EXPECT_FALSE(out.recovering);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `ctest --test-dir build-release -R FailoverElection.Unclean`
Expected: FAIL — no `recovering` / no unclean path.

- [ ] **Step 3: Implement the unclean path**

Add the `recovering` field. In `elect`, after the clean path yields `wentNoLeader` with an empty proposed isr: if `allowUnclean` and there exist live replicas in `cur.replicas`, pick the most-complete live replica (over `cur.replicas ∩ liveNodes`, ignoring ISR) via `compareLog`, set `newLeader`, `newIsr={newLeader}`, `leaderChanged=true`, `wentNoLeader=false`, `recovering=true`. Keep the "refused if leader already valid" guard at the top (the existing keep-current branch already returns before reaching here).

- [ ] **Step 4: Run PASS**

Run: `ctest --test-dir build-release -R FailoverElection`
Expected: PASS (all cases).

- [ ] **Step 5: Commit**

```bash
git add libs/metadata/include/rune/metadata/FailoverElection.h libs/metadata/tests/FailoverElectionTest.cpp
git commit -m "feat(failover): per-topic unclean-election opt-in (ISR=[newLeader], RECOVERING flag, refused if leader exists)"
```

---

## Task 7: DynamicBalancer (delta balancer + bit-packed reverse index)

**Files:**
- Create: `libs/metadata/include/rune/metadata/DynamicBalancer.h`, `libs/metadata/src/DynamicBalancer.cpp`
- Test: `libs/metadata/tests/DynamicBalancerTest.cpp` (new; register in CMake with an `add_library(rune_dynamic_balancer STATIC ...)` mirroring `rune_static_balancer`)

**Interfaces:**
- Consumes: `AssignmentMap` (`get`, `forEach`, `put`), `PartitionAssignment`, `AssignmentChange`, `mergeChange`, `gpidOf`, `BalancerRecord`/`UpdatePartitionAssignmentRecord`, `StaticBalancer::TopicSpec` (for co-partition group + partitionKey), `FailoverElection` (for the leader pick on a departure).
- Produces:
  - `class ReverseIndex { void rebuild(const AssignmentMap&); std::vector<std::pair<int32_t,int32_t>> partitionsLedBy(int32_t node) const; void onAssign(int32_t topicId,int32_t partition,int32_t oldLeader,int32_t newLeader); };` — bit-packed `node → gpid set` (open-addressing `unordered_map<int32_t, std::vector<uint64_t>>` bitset, cache-friendly; O(partitions-on-node) death cost).
  - `struct DynamicBalancerInputs { const AssignmentMap* cur; std::vector<int32_t> liveNodes; std::vector<rune::metadata::TopicSpec> topics; ReplicaPositionTable* positions; int maxPreferredElectionsPerCycle = 1000; };`
  - `class DynamicBalancer { std::vector<BalancerRecord> rebalance(const DynamicBalancerInputs&); };`
  - Policy: even-balance + preferred-leader (`replicas[0]` iff in ISR) + min-churn; co-partition group same-index partitions moved AS A UNIT; skip no-op records (emit only where leader/ISR actually changed); cap preferred-leader elections per cycle at `maxPreferredElectionsPerCycle`; records byte-bounded (each `<= 8 KiB` per Phase-1 `MetadataService` encode ceiling) and returned as a bounded batch.

- [ ] **Step 1: Write the failing test**

`libs/metadata/tests/DynamicBalancerTest.cpp`:

```cpp
#include <rune/metadata/DynamicBalancer.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

static void seed(AssignmentMap& m, int topicId, int parts,
                 std::function<PartitionAssignment(int)> f) {
    for (int p = 0; p < parts; ++p) m.put(topicId, p, f(p));
}

TEST(DynamicBalancer, ReverseIndexDeathCostIsPerNode) {
    AssignmentMap m;
    seed(m, 7, 4, [](int p){ PartitionAssignment a; a.leaderNode=(p%2? 2:1);
        a.isr={1,2}; a.replicas={1,2}; a.leaderEpoch=0; a.partitionEpoch=0; return a; });
    ReverseIndex idx; idx.rebuild(m);
    EXPECT_EQ(idx.partitionsLedBy(1).size(), 2u);
    EXPECT_EQ(idx.partitionsLedBy(2).size(), 2u);
}

TEST(DynamicBalancer, NodeDeathReassignsOnlyItsPartitionsToIsrSurvivor) {
    AssignmentMap m;
    seed(m, 7, 2, [](int p){ PartitionAssignment a; a.leaderNode=1;
        a.isr={1,2}; a.replicas={1,2}; a.leaderEpoch=0; a.partitionEpoch=0; return a; });
    ReplicaPositionTable pos;
    pos.report({7,0,2,0,50}); pos.report({7,1,2,0,50});
    DynamicBalancer bal;
    DynamicBalancerInputs in;
    in.cur=&m; in.liveNodes={2}; in.positions=&pos;
    in.topics={ TopicSpec{7,2,2,"",""} };
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 2u);
    for (auto& r : recs) EXPECT_EQ(r.rec.leaderNode, 2);
}

TEST(DynamicBalancer, SkipsNoOpRecords) {
    AssignmentMap m;
    seed(m, 7, 2, [](int p){ PartitionAssignment a; a.leaderNode=(p==0?1:2);
        a.isr={1,2}; a.replicas={1,2}; a.leaderEpoch=0; a.partitionEpoch=0; return a; });
    ReplicaPositionTable pos;
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur=&m; in.liveNodes={1,2}; in.positions=&pos;
    in.topics={ TopicSpec{7,2,2,"",""} };
    auto recs = bal.rebalance(in);
    EXPECT_TRUE(recs.empty()) << "already balanced, preferred leaders in ISR → no records";
}

TEST(DynamicBalancer, CoPartitionGroupMovesAsUnit) {
    AssignmentMap m;
    // two topics in group "acct", both partition-0 led by dead node 1
    for (int t : {7, 8})
        m.put(t, 0, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    ReplicaPositionTable pos;
    pos.report({7,0,2,0,10}); pos.report({8,0,2,0,10});
    DynamicBalancer bal; DynamicBalancerInputs in;
    in.cur=&m; in.liveNodes={2}; in.positions=&pos;
    in.topics={ TopicSpec{7,1,2,"acct","userId"}, TopicSpec{8,1,2,"acct","userId"} };
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].rec.leaderNode, recs[1].rec.leaderNode) << "group moves as a unit";
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_dynamic_balancer_tests`
Expected: FAIL — header/lib missing.

- [ ] **Step 3: Implement `ReverseIndex` + `DynamicBalancer`**

`DynamicBalancer.h` declares `ReverseIndex`, `DynamicBalancerInputs`, `DynamicBalancer`. `DynamicBalancer.cpp`:
- `rebalance`: build `ReverseIndex` from `*cur`. For each partition whose current leader is not in `liveNodes` OR whose preferred `replicas[0]` is in ISR but not currently the leader (and preferred-election budget remains): run `FailoverElection::elect` (existing-replica-set only) to pick a live ISR survivor; build an `AssignmentChange`; compute `mergeChange(cur, change)`; if the result differs from cur (leader or isr changed) emit a `BalancerRecord` (set `rec.topicId/partition/leaderNode/leaderEpoch/partitionEpoch`, copy `isr`/`replicas` into the owned vectors and point the `Int32ArrayView`s at them, mirroring `StaticBalancer`'s record construction). Skip no-ops.
- Co-partition grouping: group topics by `TopicSpec::coPartitionGroup` (non-empty); for a group, compute the leader for same-index partitions ONCE (using the first member's election) and apply it to all members' same-index partitions so they move as a unit.
- Cap preferred-leader elections at `maxPreferredElectionsPerCycle`.

- [ ] **Step 4: Register + run PASS**

Add `rune_dynamic_balancer` lib + `rune_dynamic_balancer_tests` to `libs/metadata/CMakeLists.txt` (link `rune_static_balancer rune_metadata rune_replication rune_consensus`, include `RUNE_PROTOCOL_INCLUDE`).
Run: `ctest --test-dir build-release -R DynamicBalancer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/metadata/include/rune/metadata/DynamicBalancer.h libs/metadata/src/DynamicBalancer.cpp \
        libs/metadata/tests/DynamicBalancerTest.cpp libs/metadata/CMakeLists.txt
git commit -m "feat(balancer): DynamicBalancer delta rebalance (bit-packed reverse index, preferred-leader-in-ISR, co-partition unit move, skip no-ops, capped)"
```

---

## Task 8: Graceful controlled shutdown

**Files:**
- Modify: `libs/metadata/include/rune/metadata/MembershipState.h` (controlled-shutdown transition + moved-offset gate)
- Modify: `libs/metadata/include/rune/metadata/DynamicBalancer.h`/`.cpp` (drain leadership off a shutting-down node)
- Test: `libs/metadata/tests/ControlledShutdownTest.cpp` (new; register in CMake)

**Interfaces:**
- Consumes: `MembershipState::setState(node, NodeControlState::ControlledShutdown)`, `DynamicBalancer::rebalance`, `NodeControlState`.
- Produces:
  - `MembershipState`: `void beginControlledShutdown(int32_t nodeId, int64_t controlledShutdownOffset);` `bool shutdownComplete(int32_t nodeId, int64_t lowestActiveConsumedOffset) const noexcept;` (returns `controlledShutdownOffset <= lowestActiveConsumedOffset` — all live nodes consumed the moves).
  - `DynamicBalancer`: treat a `ControlledShutdown` node as NOT a valid leader target (excluded from `liveNodes` for leadership) while still counting toward `cur.isr`; a graceful move may shrink ISR below minIsr or go leaderless (Kafka parity) but NEVER a silent unclean election (delegated to `FailoverElection` with `allowUnclean=false`). min-ISR is a produce-time gate only (unchanged in `IsrManager::canProduce`).

- [ ] **Step 1: Write the failing test**

`libs/metadata/tests/ControlledShutdownTest.cpp`:

```cpp
#include <rune/metadata/MembershipState.h>
#include <rune/metadata/DynamicBalancer.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <gtest/gtest.h>
using namespace rune::metadata;

TEST(ControlledShutdown, MovesLeadershipOffLiveNodeNeverUnclean) {
    AssignmentMap m;
    m.put(7, 0, PartitionAssignment{1, {1,2}, {1,2}, 0, 0});
    ReplicaPositionTable pos; pos.report({7,0,2,0,100});
    DynamicBalancer bal; DynamicBalancerInputs in;
    // node 1 is shutting down gracefully → excluded as a leader target but still alive
    in.cur=&m; in.liveNodes={2}; in.positions=&pos;
    in.topics={ TopicSpec{7,1,2,"",""} };
    auto recs = bal.rebalance(in);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(recs[0].rec.leaderNode, 2) << "moved to in-ISR survivor, never unclean";
}
TEST(ControlledShutdown, CompleteWhenAllLiveConsumedTheMoves) {
    MembershipState mem; mem.onJoin(1,"h",1,10,0);
    mem.beginControlledShutdown(1, /*offset*/500);
    EXPECT_FALSE(mem.shutdownComplete(1, 400));
    EXPECT_TRUE (mem.shutdownComplete(1, 500));
    EXPECT_TRUE (mem.shutdownComplete(1, 600));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_controlled_shutdown_tests`
Expected: FAIL.

- [ ] **Step 3: Implement**

`MembershipState::beginControlledShutdown` sets `state = ControlledShutdown` and stores `controlledShutdownOffset`. `shutdownComplete` compares to the passed lowest-active-consumed offset. In `DynamicBalancer::rebalance`, exclude `ControlledShutdown` nodes from leader-target candidacy (they are not in `liveNodes` for leadership) but the election still runs with `allowUnclean=false` so it can go leaderless rather than unclean.

- [ ] **Step 4: Register + run PASS**

Add `rune_controlled_shutdown_tests` to CMake. Run: `ctest --test-dir build-release -R ControlledShutdown`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/metadata/include/rune/metadata/MembershipState.h \
        libs/metadata/include/rune/metadata/DynamicBalancer.h libs/metadata/src/DynamicBalancer.cpp \
        libs/metadata/tests/ControlledShutdownTest.cpp libs/metadata/CMakeLists.txt
git commit -m "feat(shutdown): graceful controlled-shutdown drains leadership (allow-leaderless, never unclean; min-ISR stays produce-time only)"
```

---

## Task 9: LeaderEpochCache persistence + rebuild across full restart

**Files:**
- Modify: `libs/storage/include/rune/storage/LeaderEpochCache.h` (add snapshot/restore + accessors)
- Create: `libs/storage/src/LeaderEpochCache.cpp` (checkpoint file I/O) — or keep header-only with a `snapshotTo`/`restoreFrom` template like the MSM
- Modify: `libs/replication/src/PartitionReplica.cpp` (checkpoint on epoch assign / restore on construct)
- Test: `libs/storage/tests/storage/LeaderEpochCachePersistTest.cpp` (new; register in `libs/storage/tests/CMakeLists.txt`)

**Interfaces:**
- Consumes: `LeaderEpochCache::assign(int32_t,int64_t)`, `latestEpoch()`, `startOffsetFor(int32_t)`, `endOffsetForEpoch(int32_t,int64_t)`, `truncateFromEnd(int64_t)`.
- Produces:
  - `std::vector<std::pair<int32_t,int64_t>> LeaderEpochCache::entries() const;` (ordered by epoch)
  - `template<typename Writer> void LeaderEpochCache::snapshot(Writer& w) const;` (count + epoch/startOffset pairs, LE — mirror the MSM snapshot format)
  - `template<typename Reader> void LeaderEpochCache::restore(Reader& r);` (clears then reloads)
  - `void LeaderEpochCache::checkpointToFile(const std::string& path) const;` `bool LeaderEpochCache::restoreFromFile(const std::string& path);` (durable checkpoint next to the log dir; write-temp-then-rename for crash safety)

- [ ] **Step 1: Write the failing test**

`libs/storage/tests/storage/LeaderEpochCachePersistTest.cpp`:

```cpp
#include <rune/storage/LeaderEpochCache.h>
#include <gtest/gtest.h>
#include <filesystem>
using namespace rune::storage;

TEST(LeaderEpochCachePersist, RoundTripsThroughFile) {
    LeaderEpochCache c;
    c.assign(0, 0); c.assign(1, 100); c.assign(3, 250);
    const auto path = std::filesystem::temp_directory_path() / "lec_ckpt.bin";
    c.checkpointToFile(path.string());

    LeaderEpochCache c2;
    ASSERT_TRUE(c2.restoreFromFile(path.string()));
    EXPECT_EQ(c2.latestEpoch(), 3);
    EXPECT_EQ(c2.startOffsetFor(1), 100);
    EXPECT_EQ(c2.startOffsetFor(3), 250);
    // KIP-101 walk-back still works after restore:
    auto [ep, end] = c2.endOffsetForEpoch(1, /*leo*/400);
    EXPECT_EQ(ep, 1);
    EXPECT_EQ(end, 250) << "epoch 1 ended where epoch 3 began";
    std::filesystem::remove(path);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_storage_tests && ctest --test-dir build-release -R LeaderEpochCachePersist`
Expected: FAIL — no `checkpointToFile`/`restoreFromFile`.

- [ ] **Step 3: Implement snapshot/restore + file checkpoint**

Add `entries()` returning the ordered `std::map` contents. Add `snapshot`/`restore` templates. Add `checkpointToFile` (open `path + ".tmp"`, write `int32 count` then `count × (int32 epoch, int64 startOffset)` LE, fsync, `rename` to `path`) and `restoreFromFile` (read the file, `assign` each pair in epoch order; return false if the file is absent/short). If a `.cpp` is added, register it in `libs/storage/CMakeLists.txt` sources.

- [ ] **Step 4: Run PASS**

Run: `ctest --test-dir build-release -R LeaderEpochCachePersist`
Expected: PASS.

- [ ] **Step 5: Write the failing test — replica rebuild across restart**

Add `libs/replication/tests/replication/LeaderEpochCacheRestartTest.cpp`: construct a leader `PartitionReplica`, append across two epochs (`becomeLeader(1,...)`, append, `becomeLeader(2,...)`, append), assert the checkpoint file exists; construct a fresh replica over the same log dir and assert its `epochCache_` (exposed via a test accessor `leaderEpochCacheForTest()`) has both epoch boundaries after `recover()`.

- [ ] **Step 6: Wire checkpoint into `PartitionReplica`**

In `PartitionReplica.cpp`: after every `epochCache_.assign(...)` (on `becomeLeader`/epoch adoption), call `epochCache_.checkpointToFile(log_->dir() + "/leader-epoch-checkpoint")`. In the constructor (or first `recover()` seam), call `epochCache_.restoreFromFile(...)` so a full restart rebuilds the epoch history. Add a `[[nodiscard]] const rune::storage::LeaderEpochCache& leaderEpochCacheForTest() const noexcept { return epochCache_; }` accessor.

- [ ] **Step 7: Run PASS + commit**

Run: `ctest --test-dir build-release -R LeaderEpochCacheRestart`
Expected: PASS.

```bash
git add libs/storage/include/rune/storage/LeaderEpochCache.h libs/storage/src/LeaderEpochCache.cpp \
        libs/storage/tests/storage/LeaderEpochCachePersistTest.cpp libs/storage/CMakeLists.txt \
        libs/replication/include/rune/replication/PartitionReplica.h \
        libs/replication/src/PartitionReplica.cpp \
        libs/replication/tests/replication/LeaderEpochCacheRestartTest.cpp libs/replication/tests/CMakeLists.txt
git commit -m "feat(storage): persist LeaderEpochCache (checkpoint file) so KIP-101 epoch walk-back survives a full node restart"
```

---

## Task 10: ProducerId watermark recovery from log

**Files:**
- Modify: `libs/transaction/include/rune/transaction/ProducerIdAllocator.h` (already has `reseedAtLeast` — add a log-scan recovery helper)
- Modify: `libs/storage/src/ProducerStateManager.cpp` / header (expose max-observed PID from `rebuildFromLog`)
- Test: `libs/transaction/tests/transaction/ProducerIdWatermarkRecoveryTest.cpp` (new; register in CMake)

**Interfaces:**
- Consumes: `ProducerIdAllocator::reseedAtLeast(ProducerId)`, `nextId()`, `blockCeiling()`, `rune::storage::ProducerStateManager::rebuildFromLog(int32_t topicId, ILog& log, int64_t highWatermark, int32_t partition)` (real signature; `ILog&` once the M2 seam lands), `ProducerId` = `int64_t`.
- Produces:
  - `ProducerId scanMaxProducerId(ILog& log, int64_t highWatermark)` — NET-NEW free function in `rune::storage` (or `rune::transaction`): walk batches in `log` up to `highWatermark`, return the highest `producerId` seen (−1 if none). This is a pure log-scan with no PSM state; no `maxObservedProducerId()` accessor needed on PSM.
  - `void recoverProducerIdWatermark(ProducerIdAllocator& alloc, ILog& log, int64_t highWatermark);` (free function in `rune::transaction`): `const ProducerId maxSeen = scanMaxProducerId(log, highWatermark); if (maxSeen >= 0) alloc.reseedAtLeast(maxSeen);`

- [ ] **Step 1: Write the failing test**

`libs/transaction/tests/transaction/ProducerIdWatermarkRecoveryTest.cpp`:

```cpp
#include <rune/transaction/ProducerIdAllocator.h>
#include <rune/transaction/ProducerIdRecovery.h>
#include <rune/storage/InMemoryLog.h>  // available once M2 lands; mirrors Log for tests
#include <gtest/gtest.h>
using namespace rune::transaction;

TEST(ProducerIdWatermarkRecovery, ReseedsPastHighestScannedPid) {
    // Build an InMemoryLog containing batches with producerId values up to 5000.
    rune::storage::InMemoryLog log(/*maxBytes*/1<<20, /*maxRecords*/1024);
    // (Append test batches encoding producerId fields; the scan walks up to hw=N.)
    // Exact batch encoding mirrors the existing ProducerStateManagerTest helper.
    const int64_t hw = 10; // drive with batches that encode producerIds 1000, 5000
    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, hw);
    EXPECT_GT(alloc.nextId(), 5000) << "next PID must be strictly greater than recovered max";
}
TEST(ProducerIdWatermarkRecovery, NoObservedPidLeavesAllocatorUnchanged) {
    rune::storage::InMemoryLog emptyLog(1<<20, 1024); // no batches
    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, emptyLog, /*hw*/0);
    EXPECT_EQ(alloc.nextId(), 0);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_producer_id_watermark_recovery_tests`
Expected: FAIL.

- [ ] **Step 3: Implement `scanMaxProducerId` + free function**

Add NET-NEW `ProducerId scanMaxProducerId(ILog& log, int64_t highWatermark)` in `rune/storage` (declare in `ProducerStateManager.h` or a new `rune/storage/ProducerIdScan.h`): walk batches via `log.read(log.logStartOffset(), maxBytes)` up to `highWatermark`, parse each batch header's `producerId` field (existing batch format), track the maximum, return it (−1 if no batches). Add `recoverProducerIdWatermark(ProducerIdAllocator& alloc, ILog& log, int64_t hw)` free function in a small new header `rune/transaction/ProducerIdRecovery.h`: calls `scanMaxProducerId`, then `alloc.reseedAtLeast(maxSeen)` if `maxSeen >= 0`. No accessor or test-only setter is added to PSM.

- [ ] **Step 4: Register + run PASS**

Add test to `libs/transaction/tests/CMakeLists.txt`. Run: `ctest --test-dir build-release -R ProducerIdWatermarkRecovery`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/ProducerStateManager.h \
        libs/transaction/include/rune/transaction/ProducerIdRecovery.h \
        libs/transaction/tests/transaction/ProducerIdWatermarkRecoveryTest.cpp libs/transaction/tests/CMakeLists.txt
git commit -m "feat(txn): recover ProducerId watermark by scanning the log (reseed allocator past highest scanned PID) — monotonicity across failover/restart"
```

---

## Task 11: Producer-state re-rebuild when a truncation lands below the rebuild HW

**Files:**
- Modify: `libs/replication/src/PartitionReplica.cpp` (truncation path → conditional PSM re-rebuild, atomic)
- Modify: `libs/storage/include/rune/storage/ProducerStateManager.h` (expose `rebuildHighWatermark()`)
- Test: `libs/replication/tests/replication/PsmReRebuildOnLowTruncationTest.cpp` (new; register in CMake)

**Interfaces:**
- Consumes: `PartitionReplica::truncateLog(Offset)` (private; the fix lives inside it), `ProducerStateManager::rebuildFromLog(int32_t topicId, ILog& log, int64_t highWatermark, int32_t partition)` (real 4-param signature; `ILog&` once M2 seam lands, `Log&` before it), `LeaderEpochCache::truncateFromEnd(int64_t)`.
- Produces:
  - `Offset ProducerStateManager::rebuildHighWatermark() const noexcept;` — NET-NEW: the HW at which the last rebuild ran (tracks `rebuildHw_` set on every `rebuildFromLog` call).
  - Behavior: inside `truncateLog(truncateOffset)`, after `log_->truncateTo(truncateOffset)` and `epochCache_.truncateFromEnd(truncateOffset)`, if `truncateOffset <= psm_.rebuildHighWatermark()` then re-run `psm_.rebuildFromLog(cfg_.topicId, *log_, truncateOffset, cfg_.partitionId)` atomically (under the single-writer duty-thread, before any new append) so no duplicate is accepted and no valid sequence is rejected after a handoff.

- [ ] **Step 1: Write the failing test**

`libs/replication/tests/replication/PsmReRebuildOnLowTruncationTest.cpp`: build a follower `PartitionReplica`, drive it to rebuild PSM at HW=200, then deliver an epoch-boundary response that forces a truncation to offset 120 (< rebuild HW). Assert (via a test accessor `producerStateManagerForTest()`) that `rebuildHighWatermark()` moved down to 120 and the dedup window no longer contains sequences above 120 (a re-append at seq previously acked above 120 is accepted, not rejected as duplicate; a re-append of a below-120 committed seq is deduped).

```cpp
TEST(PsmReRebuildOnLowTruncation, ReRebuildsWhenTruncationBelowRebuildHw) {
    // Construct follower over a log; rebuild PSM at HW=200; force truncateLog(120);
    // ASSERT psm.rebuildHighWatermark() == 120 after truncation.
    // (Body mirrors GatewayPsmFailoverTest driver-loop construction.)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_psm_rerebuild_tests`
Expected: FAIL — no `rebuildHighWatermark` / no re-rebuild.

- [ ] **Step 3: Implement `rebuildHighWatermark` + conditional re-rebuild**

Add NET-NEW `Offset rebuildHw_ = -1` member and `rebuildHighWatermark()` accessor to `ProducerStateManager`; set `rebuildHw_` at the top of `rebuildFromLog` to the `highWatermark` argument (the real 4-param signature: `rebuildFromLog(int32_t topicId, ILog& log, int64_t highWatermark, int32_t partition)`). In `PartitionReplica::truncateLog`, after the existing `truncateTo`/`truncateFromEnd` calls, add: `if (truncateOffset <= psm_.rebuildHighWatermark()) { psm_.rebuildFromLog(cfg_.topicId, *log_, truncateOffset, cfg_.partitionId); }` Keep it under the single duty-thread (no new locks).

- [ ] **Step 4: Register + run PASS**

Run: `ctest --test-dir build-release -R PsmReRebuild`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/ProducerStateManager.h libs/storage/src/ProducerStateManager.cpp \
        libs/replication/src/PartitionReplica.cpp \
        libs/replication/include/rune/replication/PartitionReplica.h \
        libs/replication/tests/replication/PsmReRebuildOnLowTruncationTest.cpp libs/replication/tests/CMakeLists.txt
git commit -m "fix(replication): re-rebuild producer state when a divergence truncation lands below the rebuild HW (atomic with truncation)"
```

---

## Task 12: Bridge per-partition apply under dynamic churn (TSan) + delete tears down (Note B)

**Files:**
- Modify: `libs/node/include/rune/node/ReplicationMetadataBridge.h` (`applyUserPartitionRoles`: `deletes` → stop/quarantine, not `becomeFollower(0, NO_LEADER)`)
- Modify: `libs/node/include/rune/node/ReplicaRegistry.h` (add `remove(topicId,partition)` / stray-quarantine seam if absent)
- Test: `libs/node/tests/node/ReplicationMetadataBridgeChurnTest.cpp` (new; register in `libs/node/tests/CMakeLists.txt`), run under TSan.

**Interfaces:**
- Consumes: `MetadataStateMachine::deriveLocalDelta(selfNode, prevMap)` → `LocalRoleDelta{deletes, electedLeaders, leaderUpdates, followers}`; `ReplicaRegistry::lookup(topicId,partition)`, `allReplicas()`; `PartitionReplica::becomeLeader(Epoch, std::vector<NodeId>)`, `becomeFollower(Epoch, NodeId)`, `role()`, `topicId()`, `partitionId()`; `AssignmentMap::get`.
- Produces:
  - `void ReplicaRegistry::remove(int32_t topicId, int32_t partition);` — NET-NEW method (confirmed no existing `remove`/`unregister`/`erase`): removes the replica from routing so a delete-delta truly tears it down. Replica identity accessed via `topicId()` / `partitionId()` accessors.
  - `ReplicationMetadataBridge::applyUserPartitionRoles`: for each `deletes` key call a new private `applyDeleteForKey(key)` that stops the replica and calls `registry_.remove(key.topicId, key.partition)` to quarantine it out of routing (stray-quarantine per spec §16) — NOT `becomeFollower(0, NO_LEADER)`.
  - Behavior verified: from a CHANGING committed map, leader→follower→leader transitions apply correctly per-partition; concurrent apply-vs-route is TSan-clean (single duty-thread invariant asserted); follower epoch-boundary truncation exercised on handoff.

- [ ] **Step 1: Write the failing test — delete tears down**

`libs/node/tests/node/ReplicationMetadataBridgeChurnTest.cpp`:

```cpp
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/MetadataService.h>
#include <gtest/gtest.h>
using namespace rune::node;
using namespace rune::replication;

// Build a bridge with a user partition, apply an assignment that includes self as
// a replica, then apply a map where self is no longer a replica (delete delta):
// the replica must be REMOVED from the registry, not left as a NO_LEADER follower.
TEST(ReplicationMetadataBridgeChurn, DeleteDeltaTearsDownReplica) {
    // (Construct ReplicaRegistry with one user PartitionReplica (topicId=7,p=0);
    //  MSM with an assignment {leader=self, isr=[self,2], replicas=[self,2]};
    //  bridge.doWork → self becomes follower/leader; then MSM with replicas=[2]
    //  (self removed) committed; bridge.doWork → assert registry.lookup(7,0)==nullptr.)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_node_tests && ctest --test-dir build-release -R ReplicationMetadataBridgeChurn.Delete`
Expected: FAIL — replica still present (current behavior routes deletes to `becomeFollower(0, NO_LEADER)`).

- [ ] **Step 3: Implement delete-tears-down**

Add `void ReplicaRegistry::remove(int32_t topicId, int32_t partition)` — a NET-NEW method that erases the entry keyed by `(topicId, partition)` from the registry's internal map (no existing `remove`/`erase` exists). In `applyUserPartitionRoles`, replace the `for (deletes) applyBecomeFollowerForKey` loop with `for (deletes) applyDeleteForKey(key)`, where `applyDeleteForKey` looks up the replica via `registry_.lookup(key.topicId, key.partition)`, calls `becomeFollower(pa? pa->leaderEpoch : 0, NO_LEADER)` to fence it, then `registry_.remove(key.topicId, key.partition)` to quarantine it out of routing. Use `topicId()`/`partitionId()` accessors for replica identity.

- [ ] **Step 4: Run PASS**

Run: `ctest --test-dir build-release -R ReplicationMetadataBridgeChurn.Delete`
Expected: PASS.

- [ ] **Step 5: Write the failing test — churn transitions leader→follower→leader**

Add a case that drives the bridge across three successive committed maps: (a) self leader, (b) node 2 leader (self follower — assert `role()==Follower` and a higher epoch stamped), (c) self leader again (assert `role()==Leader`, epoch monotonic). Use `MetadataService` to apply real records so `deriveLocalDelta` classification is exercised.

- [ ] **Step 6: Run, confirm it passes (existing become* paths handle it) or fix, then run under TSan**

Run: `ctest --test-dir build-release -R ReplicationMetadataBridgeChurn`
Then build+run under TSan:
Run: `cmake --build build-tsan --target rune_node_tests && ctest --test-dir build-tsan -R ReplicationMetadataBridgeChurn`
Expected: PASS, zero TSan reports.

- [ ] **Step 7: Commit**

```bash
git add libs/node/include/rune/node/ReplicationMetadataBridge.h libs/node/include/rune/node/ReplicaRegistry.h \
        libs/node/tests/node/ReplicationMetadataBridgeChurnTest.cpp libs/node/tests/CMakeLists.txt
git commit -m "fix(bridge): delete-delta stops+quarantines the replica (Note B); TSan-clean leader<->follower churn apply from the changing committed map"
```

---

## Task 13: MSM engineering for dynamic reassignment (MVCC revert, bounded batches, handleLoss, prune)

**Files:**
- Modify: `libs/metadata/include/rune/metadata/MetadataStateMachine.h` + `src/MetadataStateMachine.cpp` (MVCC revert of uncommitted pending; version-gated apply + `handleLoss`; snapshot-load prune already present)
- Modify: `libs/node/include/rune/node/MetadataService.h` (emit dynamic assignment in bounded/individually-snapshotted batches; never one giant atomic batch; byte ceiling)
- Test: `libs/metadata/tests/MetadataStateMachineTest.cpp` (add cases), `libs/node/tests/integration/MetadataReplicationTest.cpp` (add a bounded-batch case)

**Interfaces:**
- Consumes: `MetadataStateMachine::apply(buf,len,recordPos)`, `commit(committedPos)`, `assignment()`, `committedMap()`, `deriveLocalDelta`, `snapshot`/`restore`. `UpdatePartitionAssignmentRecord::recordVersion` (unknown-version → `handleLoss`). `PartitionReplica::append(span, acksAll)`.
- Produces:
  - `void MetadataStateMachine::revert(int64_t toCommittedPos);` — discard pending records with `pos > toCommittedPos` (MVCC revert of uncommitted speculation on a lost commit); committed map untouched.
  - `uint64_t MetadataStateMachine::lossCount() const noexcept;` — count of records skipped due to unknown `recordVersion` (`handleLoss` fallback; the record is not applied but does not crash apply).
  - `MetadataService::emitDynamicAssignment(const std::vector<BalancerRecord>& recs)` — appends records in bounded batches: at most `kMaxRecordsPerBatch = 256` and `kMaxBatchBytes = 8 * 1024 * 1024`; each `append()` is one log slot (mirrors the existing one-record-per-append invariant); a mass rebalance is split across duty cycles, never a single giant atomic append.

- [ ] **Step 1: Write the failing test — MVCC revert**

Add to `MetadataStateMachineTest.cpp`:

```cpp
TEST(MetadataStateMachine, RevertDiscardsUncommittedPending) {
    MetadataStateMachine msm;
    // apply two records at pos 0 and 1, commit only pos 0
    // (encode two UpdatePartitionAssignmentRecords; apply each; commit(0))
    // assignment for pos-1 partition must be null (uncommitted)
    // revert(0): the pending pos-1 record is discarded
    // a later commit(1) must NOT resurrect it
    // (full body encodes records with putInt32Array as in existing MSM tests)
}
TEST(MetadataStateMachine, UnknownRecordVersionIsHandleLossNotCrash) {
    MetadataStateMachine msm;
    uint8_t buf[32]{}; // recordType=2 (UpdatePartitionAssignment), version=99
    buf[0]=2; buf[1]=0; buf[2]=99;
    msm.apply(buf, sizeof buf, /*pos*/0);
    msm.commit(0);
    EXPECT_EQ(msm.lossCount(), 1u);
    EXPECT_EQ(msm.committedMap().size(), 0);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `ctest --test-dir build-release -R MetadataStateMachine.Revert` and `-R MetadataStateMachine.UnknownRecord`
Expected: FAIL.

- [ ] **Step 3: Implement `revert` + `handleLoss` + `lossCount`**

In `MetadataStateMachine.cpp`: `revert(toCommittedPos)` erases from `pending_` every entry with `pos > toCommittedPos`. In `apply`, when `peekRecordType`/`decode` returns unknown version or fails, increment `lossCount_` and return without storing a pending entry (the existing decode already returns `nullopt` on unknown version — route that to the loss counter instead of a silent drop). Add `lossCount_` member + accessor.

- [ ] **Step 4: Run PASS**

Run: `ctest --test-dir build-release -R MetadataStateMachine`
Expected: PASS (incl. existing snapshot-prune tests).

- [ ] **Step 5: Write the failing test — bounded batched dynamic emit**

Add a case to `MetadataReplicationTest.cpp` that calls `MetadataService::emitDynamicAssignment` with 1000 records and asserts each record occupies its own log slot (logEndOffset advances by exactly the number appended this cycle, capped at `kMaxRecordsPerBatch` per cycle) and the MSM eventually commits all after enough duty cycles.

- [ ] **Step 6: Implement `emitDynamicAssignment`**

Add the method to `MetadataService` mirroring `emitStaticAssignmentIfNeeded`'s per-record `encode` + `append` loop, but bounded per cycle (`kMaxRecordsPerBatch`, `kMaxBatchBytes`) and stateful (a `pendingDynamicRecords_` queue drained across cycles).

- [ ] **Step 7: Run PASS + commit**

Run: `ctest --test-dir build-release -R MetadataReplication`
Expected: PASS.

```bash
git add libs/metadata/include/rune/metadata/MetadataStateMachine.h libs/metadata/src/MetadataStateMachine.cpp \
        libs/metadata/tests/MetadataStateMachineTest.cpp \
        libs/node/include/rune/node/MetadataService.h libs/node/tests/integration/MetadataReplicationTest.cpp
git commit -m "feat(msm): MVCC revert of uncommitted speculation, handleLoss on unknown version, bounded/individually-appended dynamic assignment batches"
```

---

## Task M1: `persistence` topic-config field + wiring (also resolves Note C)

**Files:**
- Modify: `rune-protocol/include/rune/protocol/MetadataRecords.h` (`CreateTopicRecord::persistence`, v>=1)
- Modify: `libs/node/include/rune/node/NodeConfig.h` + `src/NodeConfig.cpp` (`TopicSpec::persistence`, `durability`, `coPartitionGroup`, `partitionKey` parsed from `RUNE_TOPICS`)
- Modify: `libs/node/src/BrokerNode.cpp:937-938` (pass parsed group/key/persistence into `metadata::TopicSpec`, not hardcoded empty)
- Test: `libs/node/tests/node/NodeConfigTest.cpp` (add cases), `rune-protocol/tests/CreateTopicRecordTest.cpp` (add persistence case)

**Interfaces:**
- Consumes: `CreateTopicRecord` (extend), `NodeConfig::TopicSpec` (extend), `parseNodeConfig`, `metadata::TopicSpec{topicId,partitionCount,rf,coPartitionGroup,partitionKey}`.
- Produces:
  - `enum class rune::node::PersistenceMode : uint8_t { Disk = 0, Memory = 1 };` and `DurabilityMode` reuse from Task 1.
  - `NodeConfig::TopicSpec`: add `std::string coPartitionGroup; std::string partitionKey; PersistenceMode persistence = PersistenceMode::Disk; uint8_t durability = 0;`.
  - `CreateTopicRecord::persistence` (`uint8_t`, v>=1 only; encoded after `durability`).
  - `RUNE_TOPICS` grammar extended: `name:partitionCount[:group=<g>][:key=<k>][:persistence=memory][:durability=all-in-sync]` (backward-compatible: extra fields optional; a topic with no extras parses exactly as today).
  - `BrokerNode` builds `metadata::TopicSpec` with the parsed group/key (Note C fix) and forwards persistence/durability into `PartitionConfig` when constructing replicas.

- [ ] **Step 1: Write the failing test — protocol persistence round-trip**

Add to `CreateTopicRecordTest.cpp`:

```cpp
TEST(CreateTopicRecord, PersistenceRoundTripsAtVersion1) {
    CreateTopicRecord in; in.topicId=2; in.name="presence"; in.partitionCount=2; in.rf=2;
    in.recordVersion=1; in.durability=0; in.persistence=1; // memory
    uint8_t buf[256]; auto n=in.encode(buf,sizeof buf); ASSERT_GT(n,0u);
    auto out=CreateTopicRecord::decode(buf,n); ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->persistence, 1);
    EXPECT_EQ(out->durability, 0);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `ctest --test-dir build-release -R CreateTopicRecord.Persistence`
Expected: FAIL — no `persistence` field.

- [ ] **Step 3: Add `persistence` (v>=1, encoded after durability)**

In `CreateTopicRecord`, add `uint8_t persistence = 0;`. Extend `encode` (when v1: write `durability` then `persistence`) and `decode` (when v1: read `durability` then `persistence`, both guarded on `avail`).

- [ ] **Step 4: Run PASS**

Run: `ctest --test-dir build-release -R CreateTopicRecord`
Expected: PASS.

- [ ] **Step 5: Write the failing test — NodeConfig parses groups/key/persistence**

Add to `NodeConfigTest.cpp` (uses the injected-env overload `parseNodeConfig`):

```cpp
TEST(NodeConfig, ParsesCoPartitionGroupKeyAndPersistence) {
    auto env = [](const char* k)->const char*{
        std::string s=k;
        if (s=="RUNE_NODE_ID") return "0";
        if (s=="RUNE_PEERS") return "0@h:1";
        if (s=="RUNE_CLIENT_ENDPOINT") return "h:2";
        if (s=="RUNE_DATA_DIR") return "/tmp/x";
        if (s=="RUNE_TOPICS") return "wallet:4:group=acct:key=userId:durability=all-in-sync,presence:2:persistence=memory";
        return nullptr;
    };
    auto res = rune::node::parseNodeConfig(env);
    ASSERT_TRUE(res.config.has_value()) << res.error;
    const auto& ts = res.config->topics;
    ASSERT_EQ(ts.size(), 2u);
    EXPECT_EQ(ts[0].coPartitionGroup, "acct");
    EXPECT_EQ(ts[0].partitionKey, "userId");
    EXPECT_EQ(ts[0].durability, 1);
    EXPECT_EQ(ts[1].persistence, rune::node::PersistenceMode::Memory);
}
TEST(NodeConfig, LegacyTopicGrammarStillParses) {
    auto env=[](const char* k)->const char*{ std::string s=k;
        if (s=="RUNE_NODE_ID") return "0"; if (s=="RUNE_PEERS") return "0@h:1";
        if (s=="RUNE_CLIENT_ENDPOINT") return "h:2"; if (s=="RUNE_DATA_DIR") return "/tmp/x";
        if (s=="RUNE_TOPICS") return "inventory:8,chat:1"; return nullptr; };
    auto res=rune::node::parseNodeConfig(env);
    ASSERT_TRUE(res.config.has_value());
    EXPECT_EQ(res.config->topics[0].coPartitionGroup, "");
    EXPECT_EQ(res.config->topics[0].persistence, rune::node::PersistenceMode::Disk);
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `ctest --test-dir build-release -R NodeConfig.Parses`
Expected: FAIL.

- [ ] **Step 7: Implement the extended grammar + BrokerNode wiring**

Add `PersistenceMode` enum + the four `TopicSpec` fields. In `NodeConfig.cpp`, extend the `RUNE_TOPICS` per-topic parser: split on `:`, first two tokens are `name` and `partitionCount`; each subsequent token is `key=value` (`group`, `key`, `persistence`, `durability`); reject unknown keys with a clear error. In `BrokerNode.cpp:937-938`, build `metadata::TopicSpec{topicId, partitionCount, rf, coPartitionGroup, partitionKey}` from the parsed config (Note C fix) and set `PartitionConfig::durability` from the parsed durability + choose the log backend by `persistence` (memory → `InMemoryLog` from Task M2; recovery SKIPS memory partitions — start empty).

- [ ] **Step 8: Run PASS + commit**

Run: `ctest --test-dir build-release -R NodeConfig`
Expected: PASS.

```bash
git add rune-protocol/include/rune/protocol/MetadataRecords.h rune-protocol/tests/CreateTopicRecordTest.cpp \
        libs/node/include/rune/node/NodeConfig.h libs/node/src/NodeConfig.cpp \
        libs/node/tests/node/NodeConfigTest.cpp libs/node/src/BrokerNode.cpp
git commit -m "feat(config): persistence/durability/co-partition-group topic config wired through NodeConfig + CreateTopic (resolves Note C)"
```

---

## Task M2: `InMemoryLog` — bounded drop-oldest ring implementing the `Log` interface

**Files:**
- Create: `libs/storage/include/rune/storage/InMemoryLog.h` (+ `src/InMemoryLog.cpp` if needed)
- Test: `libs/storage/tests/storage/InMemoryLogTest.cpp` (new; register in CMake)

**Interfaces:**
- Consumes: the `rune::storage::Log` public surface that `PartitionReplica`/replication/HW/`LeaderEpochCache` call: `append(const uint8_t*, std::size_t, int64_t lastOffset)`, `appendNoSync(...)`, `read(int64_t startOffset, int32_t maxBytes)`, `logEndOffset()`, `logStartOffset()`, `truncateTo(int64_t)`, `sync()`, `dir()`, `recover()`. (Match these exact signatures so no caller changes.)
- Produces:
  - `class rune::storage::InMemoryLog` exposing the SAME public method signatures as `Log` (duck-typed; `PartitionReplica` holds a `Log*` today — introduce a thin `ILog` seam OR make `InMemoryLog` derive from a minimal `Log` interface. Chosen approach: extract the methods `PartitionReplica` uses into an abstract `class ILog` and have both `Log` and `InMemoryLog` satisfy it; `PartitionReplica::log_` becomes `ILog*`. If the extraction is too invasive for one task, `InMemoryLog` mirrors the concrete method names and `PartitionReplica` is templated/typedef'd — pick the derive-from-`ILog` seam.)
  - Behavior: per-topic byte/count cap; on overflow evict oldest batch + advance `logStartOffset`; read below `logStartOffset` → empty (offset-out-of-range → reset, matching `Log::read` semantics); `truncateTo` mid-ring; **zero disk** (no file/mmap/fsync path — `sync()` is a no-op, `dir()` returns a synthetic name); **zero-alloc steady-state append** into a preallocated ring buffer.

- [ ] **Step 1: Write the failing test**

`libs/storage/tests/storage/InMemoryLogTest.cpp`:

```cpp
#include <rune/storage/InMemoryLog.h>
#include <gtest/gtest.h>
using namespace rune::storage;

TEST(InMemoryLog, AppendReadRoundTrip) {
    InMemoryLog log(/*maxBytes*/1<<20, /*maxRecords*/1024);
    std::vector<uint8_t> b = {1,2,3,4};
    log.append(b.data(), b.size(), /*lastOffset*/0);
    EXPECT_EQ(log.logEndOffset(), 1);
    auto out = log.read(0, 1<<20);
    ASSERT_FALSE(out.empty());
}
TEST(InMemoryLog, EvictionAdvancesLogStartOffset) {
    InMemoryLog log(/*maxBytes*/64, /*maxRecords*/2);
    uint8_t b[8]{}; 
    log.append(b, sizeof b, 0); // off 0
    log.append(b, sizeof b, 1); // off 1
    log.append(b, sizeof b, 2); // off 2 → evicts off 0
    EXPECT_EQ(log.logStartOffset(), 1);
    EXPECT_TRUE(log.read(0, 1024).empty()) << "evicted read → out-of-range → empty";
}
TEST(InMemoryLog, TruncateMidRing) {
    InMemoryLog log(1<<20, 1024);
    uint8_t b[4]{}; for (int i=0;i<5;++i) log.append(b,sizeof b,i);
    log.truncateTo(3);
    EXPECT_EQ(log.logEndOffset(), 3);
}
TEST(InMemoryLog, NoDiskAndSyncIsNoOp) {
    InMemoryLog log(1<<20, 1024);
    EXPECT_NO_THROW(log.sync());              // no fsync path
    EXPECT_FALSE(log.dir().empty());          // synthetic name, no real dir created
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build-release --target rune_in_memory_log_tests`
Expected: FAIL — header missing.

- [ ] **Step 3: Extract `ILog` seam + implement `InMemoryLog`**

Introduce `libs/storage/include/rune/storage/ILog.h` with the pure-virtual subset `PartitionReplica` uses (from Consumes). The `ILog` interface MUST include the full read surface that `ProducerStateManager::rebuildFromLog` consumes (i.e., `read(int64_t startOffset, int32_t maxBytes)`, `logStartOffset()`, `logEndOffset()`). Make `Log` inherit `ILog` (no behavior change — the methods already exist). Change `PartitionReplica::log_` and the `PartitionConfig`/constructor `Log*` parameter to `ILog*` (and `MetadataService`, `NodeRuntimeAgent` call sites that pass `Log*` still work via upcast). **Also retype `ProducerStateManager::rebuildFromLog`'s `Log&` parameter to `ILog&`** (files touched: `libs/storage/include/rune/storage/ProducerStateManager.h`, `libs/storage/src/ProducerStateManager.cpp`) so PSM rebuild works over both disk `Log` and `InMemoryLog` without duplication. Implement `InMemoryLog : public ILog` with a preallocated ring of batch records (`std::vector<uint8_t> ring_`, offset index), count/byte caps, drop-oldest eviction advancing `logStartOffset_`, `truncateTo`, no-op `sync`, synthetic `dir()`.

- [ ] **Step 4: Register + run PASS**

Add `rune_in_memory_log_tests` to `libs/storage/tests/CMakeLists.txt`.
Run: `ctest --test-dir build-release -R InMemoryLog`
Expected: PASS.

- [ ] **Step 5: Rebuild dependents to confirm the `ILog` seam compiles**

Run: `cmake --build build-release --target rune_replication_tests rune_node_tests`
Expected: builds clean (all `Log*` call sites accept `ILog*`).

- [ ] **Step 6: Commit**

```bash
git add libs/storage/include/rune/storage/ILog.h libs/storage/include/rune/storage/InMemoryLog.h \
        libs/storage/src/InMemoryLog.cpp libs/storage/include/rune/storage/Log.h \
        libs/storage/include/rune/storage/ProducerStateManager.h libs/storage/src/ProducerStateManager.cpp \
        libs/replication/include/rune/replication/PartitionReplica.h libs/replication/src/PartitionReplica.cpp \
        libs/storage/tests/storage/InMemoryLogTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): InMemoryLog (bounded drop-oldest ring, zero-disk, zero-alloc append) behind an ILog seam; retype PSM rebuildFromLog log param to ILog& so rebuild works over both disk and memory log"
```

---

## Task 14: Integration — rebalance + failover in-process (Phase-2 EXIT)

**Files:**
- Create: `libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp` (register in `libs/node/tests/CMakeLists.txt`)
- Modify: `rune-client-cpp/src/Client.cpp` (Note A: treat bare `NotLeader` as retriable — refresh metadata → retry with backoff)
- Test (client): `rune-client-cpp/tests/ClientNotLeaderRetryTest.cpp` (register in the client `tests/CMakeLists.txt`)

**Interfaces:**
- Consumes: the 3-node in-process harness pattern from `MetadataReplicationTest.cpp`/`MultiLeaderProduceTest.cpp` (`kDriverCap` bounded driver loop, `MrtTempDir`, per-node `BrokerNode`/`NodeRuntimeAgent` construction); `DynamicBalancer::rebalance`, `FailoverElection::elect`, `MetadataService::emitDynamicAssignment`, `MembershipState`, `ReplicaPositionTable`, `PartitionReplica::highWatermark`, `ClientGateway` produce path; client `MetadataCache::updatePartitionLeader`, `Client` redirect/NotLeader handling in `Client.cpp` (`ErrorCode::NotLeader`, `ErrorCode::Redirect`).
- Produces:
  - In-process assertions (no Docker): node-death failover with NO committed loss (incl. an `all-in-sync` topic); node-join / rejoin-after-restart rebalance with co-location preserved + min-churn; `NO_LEADER` path; fence-one-at-a-time under 2-node loss.
  - Memory-topic fold-ins: memory-partition handoff keeps in-memory-committed data when a replica survives; data gone when none survive (assert the contract honestly); bounded-ring follower catch-up from leader memory; an `all-in-sync` memory topic.
  - Client Note A: `Client` on a bare `NotLeader` ErrorResponse refreshes metadata and retries with exponential backoff (does NOT fire `cbError_` immediately).

- [ ] **Step 1: Write the failing test — client bare NotLeader is retriable (Note A)**

`rune-client-cpp/tests/ClientNotLeaderRetryTest.cpp`:

```cpp
// Feed the client an ErrorResponse{code=NotLeader} (no endpoint) for an in-flight
// produce. Assert: cbError_ is NOT invoked immediately; instead the client marks
// the partition for metadata refresh and re-enqueues the produce with backoff.
// (Drive via the Client test harness that injects decoded ErrorResponses.)
TEST(ClientNotLeaderRetry, BareNotLeaderTriggersRefreshNotError) {
    // ... construct Client with a fake transport + a pending produce corrId ...
    // deliver ErrorResponse{NotLeader};
    // EXPECT_FALSE(errorCallbackFired);
    // EXPECT_TRUE(metadataRefreshRequested);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: (client repo) `cmake --build build --target rune_client_tests && ctest --test-dir build -R ClientNotLeaderRetry`
Expected: FAIL — current `Client.cpp:641` only special-cases `Redirect`-with-endpoint; bare `NotLeader` fires `cbError_`.

- [ ] **Step 3: Implement bare-NotLeader retry in `Client.cpp` (Note A)**

In the `ErrorResponse` handler (mirroring the existing Redirect machinery at `Client.cpp:641-786`), add a branch for bare `NotLeader` (no endpoint in the response): call `scheduleMetadataRefresh()` (no args; backoff-doubling capped at `kMetadataBackoffMaxNs`, fired from `tickMetadataRefresh()` — the real method at line 1347) to trigger a background metadata refresh, then re-enqueue the produce with existing backoff. Bound retries by `kMaxRedirects = 3` (from `ProducerImpl.h:270`; `SessionConfig::maxRedirects=5` is the session cap). Only fire `cbError_` after the redirect/retry budget is exhausted. Do NOT call a guessed `requestMetadataRefresh(affectedPartition)` — the real method takes no arguments.

- [ ] **Step 4: Run client test PASS**

Run: `ctest --test-dir build -R ClientNotLeaderRetry`
Expected: PASS.

- [ ] **Step 5: Write the failing test — 3-node failover, no committed loss (disk + all-in-sync)**

`RebalanceFailoverIntegrationTest.cpp`, case `NodeDeathFailover_NoCommittedLoss`: build a 3-node in-process cluster (mirror `MetadataReplicationTest` harness), create one `majority` topic + one `all-in-sync` topic (RF=3), produce N committed records to each, "kill" the leader node (stop driving its `doWork` + `transport.removeFollower`), drive the control-plane tick → `MembershipState` marks it expired → `DynamicBalancer` reassigns via `FailoverElection` → new leader elected → assert every previously-committed record is still readable at HW on the new leader for BOTH topics (no committed loss).

- [ ] **Step 6: Run to verify it fails, implement the harness wiring, run PASS**

Run: `cmake --build build-release --target rune_node_tests && ctest --test-dir build-release -R RebalanceFailover.NodeDeath`
First FAIL (control-plane tick → dynamic emit not yet wired in `NodeRuntimeAgent`), then wire `MetadataService` to run the `ControlPlaneTimer` poll → append `ControlTickRecord` → on tick apply, run `DynamicBalancer`/`FailoverElection` and `emitDynamicAssignment` (leader-only), then PASS.

- [ ] **Step 7: Write + pass the remaining cases**

Add cases (each its own `TEST`): `NodeJoin_RebalancePreservesCoLocationMinChurn`; `RejoinAfterRestart_PreferredLeaderMoveBack`; `NoIsrSurvivor_GoesNoLeaderAndRecoversOnUnfence`; `TwoNodeLoss_FenceOneAtATime`; `MemoryTopic_HandoffKeepsDataWhenReplicaSurvives`; `MemoryTopic_DataGoneWhenNoneSurvive` (assert the loss contract honestly); `MemoryTopic_AllInSyncFollowerCatchUpFromLeaderRing`. Each uses the bounded driver loop with `ASSERT_TRUE(done) << "stalled after N iterations"`.

Run: `ctest --test-dir build-release -R RebalanceFailover`
Expected: PASS (all).

- [ ] **Step 8: Run the whole suite under TSan (concurrency gate)**

Run: `cmake --build build-tsan --target rune_node_tests && ctest --test-dir build-tsan -R RebalanceFailover`
Expected: PASS, zero TSan reports.

- [ ] **Step 9: Commit**

```bash
git add libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp libs/node/tests/CMakeLists.txt \
        libs/node/include/rune/node/MetadataService.h libs/node/src/BrokerNode.cpp \
        rune-client-cpp/src/Client.cpp rune-client-cpp/tests/ClientNotLeaderRetryTest.cpp
git commit -m "test(integration): 3-node rebalance+failover no-committed-loss (majority/all-in-sync/memory) + client bare-NotLeader retry (Note A)"
```

---

## Task 15: Benchmarks — failover/rebalance/handoff latency + memory-topic produce

**Files:**
- Create: `benchmark/RebalanceFailoverBench.cpp` — new fixture file added to the existing monolithic `rune_broker_benchmark` target (source dir `benchmark/`, NOT `bench/`); register in `benchmark/CMakeLists.txt` next to the existing benchmark sources (mirror `benchmark/GatewayDataBench.cpp` registration pattern).
- Test/bench harness: Google Benchmark. Use `BENCHMARK_DEFINE_F(RebalanceFailoverFixture, <Name>){...}` + `BENCHMARK_REGISTER_F(RebalanceFailoverFixture, <Name>)->UseManualTime()->MinTime(2.0)->Unit(benchmark::kMicrosecond);` for each measurement. Gate via `state.SkipWithError("produce-under-rebalance p99 >= 100us")` when the gate trips.

**Interfaces:**
- Consumes: `DynamicBalancer::rebalance`, `FailoverElection::elect`, `MetadataService::emitDynamicAssignment`, `PartitionReplica::append`/`highWatermark`, the 3-node in-process harness; the hot-path produce loop from the existing gateway bench.
- Produces:
  - Per-op p99 reports: (1) failover election latency (`FailoverElection::elect` over a realistic ISR), (2) rebalance decision + apply latency (`DynamicBalancer::rebalance` + `emitDynamicAssignment` apply), (3) leader-move handoff latency, (4) memory-topic produce p99 + throughput (expect LOWER than disk — no segment-roll tail).
  - HARD gate: `SkipWithError`/fail when a produce-path p99 ≥ 100µs **while a rebalance is in flight** (a rebalance must not push produce p99 over the gate).

- [ ] **Step 1: Write the failing bench — produce p99 under concurrent rebalance**

`benchmark/RebalanceFailoverBench.cpp`: declare `class RebalanceFailoverFixture : public benchmark::Fixture` and implement `BENCHMARK_DEFINE_F(RebalanceFailoverFixture, ProduceUnderRebalance)(benchmark::State& state)` — runs the hot-path produce loop on a 3-node harness while injecting a rebalance every K iterations; records produce latency manually (`state.SetIterationTime(...)`); calls `state.SkipWithError("produce-under-rebalance p99 >= 100us")` if p99 ≥ 100'000 ns. Register: `BENCHMARK_REGISTER_F(RebalanceFailoverFixture, ProduceUnderRebalance)->UseManualTime()->MinTime(2.0)->Unit(benchmark::kMicrosecond);`

- [ ] **Step 2: Run to verify it builds+fails-if-regressed**

Run: `cmake --build build-release --target rune_broker_benchmark && ./build-release/rune_broker_benchmark --benchmark_filter=RebalanceFailover`
Expected: builds; the gate assertion is present (fails only on regression).

- [ ] **Step 3: Implement the four measurements + the gate**

Add the election / rebalance-apply / handoff / memory-produce measurement loops. Print `p50/p99/p999` per op. Enforce the 100µs p99 gate on the produce-under-rebalance measurement.

- [ ] **Step 4: Run + record baseline**

Run: `./build-release/rune_broker_benchmark --benchmark_filter=RebalanceFailover`
Expected: produce-under-rebalance p99 < 100µs (target ~50µs); memory-topic produce p99 < disk-topic produce p99.

- [ ] **Step 5: Commit**

```bash
git add benchmark/RebalanceFailoverBench.cpp benchmark/CMakeLists.txt
git commit -m "bench(failover): failover/rebalance/handoff + memory-produce p99; HARD 100us gate on produce-under-rebalance (BENCHMARK_DEFINE_F/REGISTER_F in rune_broker_benchmark)"
```

---

## Task 16: DR-E2E Docker matrix (Phase-2 slice)

**Files:**
- Create: `dr-e2e/test_milestone_d_p2.py` (marker `milestone_d_p2`; reuse the `dr-e2e/` py-testinfra harness)
- Modify: `dr-e2e/conftest.py` / markers registration if needed

**Interfaces:**
- Consumes: the existing `dr-e2e/` harness (SHA-tagged images via `RUNE_IMAGE_TAG`, multi-node docker-compose, netem/kill helpers); fixtures from `dr-e2e/conftest.py`: `cluster` (session-scoped) / `fresh_cluster` (function-scoped, for destructive faults); helpers in `dr-e2e/harness/cluster.py`: `wait_for_leader(timeout_s, exclude=[])`, `wait_for_new_leader(old_leader_svc, timeout_s)`, `wait_data_plane_ready(...)`; workload driver `dr-e2e/harness/driver.py: run_clients(n, network, target_service, cid_base, **cfg)` (cfg: `mode`, `records`, `keys`, `rate_hz`, `from`, `topics`, `acks`, `expect`); the broker's `RUNE_TOPICS` extended grammar (Task M1); the client for produce/consume verification.
- Produces:
  - E2E cases (marker `milestone_d_p2`; registered by ADDING `milestone_d_p2: Milestone D Phase-2 rebalance/failover DR-E2E slice` under `markers=` in `dr-e2e/pytest.ini`), run ONLY after Tasks 1–15 are green: churn + rebalance + single failover (no committed loss on handoff); concurrent (2-node) failover with fence-one-at-a-time; co-location preserved across rebalance; memory-topic node-crash keeps data via a replica; full-cluster restart of a memory topic = empty (as designed). Mirror `test_dr1_baseline.py` registration style (`@pytest.mark.milestone_d_p2` + `@pytest.mark.timeout(...)`). SHA-tag images per `RUNE_IMAGE_TAG` policy (`rune-broker-node:<sha>`, NEVER `:latest`).

- [ ] **Step 1: Write the failing E2E — single-failover no committed loss**

`dr-e2e/test_milestone_d_p2.py`:

```python
import pytest
from harness.driver import run_clients

@pytest.mark.milestone_d_p2
@pytest.mark.timeout(120)
def test_single_failover_no_committed_loss(fresh_cluster):
    """Single-leader failover: no committed loss, all-in-sync topic."""
    cluster = fresh_cluster
    topic = "wallet"
    # Produce 1000 records before the kill
    run_clients(1, cluster.network, cluster.leader_service(topic, partition=0),
                cid_base=0, mode="produce", records=1000, keys=100,
                topics=[topic], acks="all", expect="committed")
    old_leader_svc = cluster.leader_service(topic, partition=0)
    cluster.kill(old_leader_svc)
    cluster.wait_for_new_leader(old_leader_svc, timeout_s=30)
    # Consume and assert no committed loss
    result = run_clients(1, cluster.network, cluster.leader_service(topic, partition=0),
                         cid_base=1, mode="consume", topics=[topic], from_=0, expect="all")
    assert result.consumed >= 1000, "committed records lost on handoff"
```

- [ ] **Step 2: Run to verify it fails / is skipped without images**

Run: `RUNE_IMAGE_TAG=$(git rev-parse --short HEAD) pytest dr-e2e/test_milestone_d_p2.py -m milestone_d_p2 -k single_failover`
Expected: FAIL/ERROR (marker not yet registered in `pytest.ini`, or harness helpers `wait_for_new_leader` / `leader_service` not yet extended; `fresh_cluster` fixture exists in `conftest.py`).

- [ ] **Step 3: Register marker, extend harness helpers + build SHA-tagged images**

Add `milestone_d_p2: Milestone D Phase-2 rebalance/failover DR-E2E slice` under `[pytest] markers=` in `dr-e2e/pytest.ini`. Extend `dr-e2e/harness/cluster.py` with `leader_service(topic, partition)` (returns the docker-compose service name of the current leader) and confirm `wait_for_new_leader(old_leader_svc, timeout_s)` is present (it's already declared in `harness/cluster.py` — verify or add if absent). Drive load via `run_clients(n, network, target_service, cid_base, **cfg)` from `dr-e2e/harness/driver.py`. Build images: `docker build -t rune-broker-node:$(git rev-parse --short HEAD) .` (SHA tag, NEVER `:latest`).

- [ ] **Step 4: Run single-failover PASS**

Run: `RUNE_IMAGE_TAG=$(git rev-parse --short HEAD) pytest dr-e2e/test_milestone_d_p2.py -m milestone_d_p2 -k single_failover`
Expected: PASS.

- [ ] **Step 5: Add + pass the remaining E2E cases**

Add (all using `fresh_cluster` fixture, `@pytest.mark.milestone_d_p2`, `@pytest.mark.timeout(...)`, `run_clients` for load): `test_concurrent_failover_fence_one_at_a_time`, `test_co_location_preserved_across_rebalance`, `test_memory_topic_survives_node_crash_via_replica`, `test_memory_topic_full_restart_is_empty`. Use `cluster.wait_for_new_leader(old_leader_svc, timeout_s=30)` after every kill.
Run: `RUNE_IMAGE_TAG=$(git rev-parse --short HEAD) pytest dr-e2e/test_milestone_d_p2.py -m milestone_d_p2`
Expected: PASS (all).

- [ ] **Step 6: Commit**

```bash
git add dr-e2e/test_milestone_d_p2.py dr-e2e/conftest.py dr-e2e/pytest.ini \
        dr-e2e/harness/cluster.py dr-e2e/harness/driver.py
git commit -m "test(dr-e2e): Phase-2 slice — churn/rebalance/single+concurrent failover, co-location, memory-topic crash/restart (milestone_d_p2 marker, SHA-tagged, fresh_cluster fixture)"
```

---

## Self-Review

**1. Spec coverage** (design spec §A–I + edge cases + persistence tier + carry-over notes):
- §B Balancer (even-balance/preferred/min-churn/reverse-index/co-partition-unit/cap/skip-noop/batch-bound) → Task 7 (+ Task 8 controlled-shutdown drain, Task 13 bounded batches).
- §C multi-role runtime (per-partition bridge from changing map) → Task 12.
- §D two-epoch/fencing/election-keep-current/re-run-over-proposed → Tasks 5, 6 (`mergeChange` two-epoch rules reused from Phase 1, exercised in Task 5 tests).
- §G HW/ISR invariants + durability knob (decision #2) → Task 1 (+ election safety in Tasks 5/14).
- §H MSM engineering (apply-off-hot-path, MVCC revert, bounded, version-gated/handleLoss, snapshot-prune) → Task 13 (apply-off-hot-path & immutable RoutingImage already Phase-1; re-verified in Task 15 bench).
- §I deterministic logged tick → Task 4 (+ drive in Task 14).
- Membership/liveness edge cases (§8 parity: broker-epoch, incarnation zombie, unfence-after-catch-up, fence-one-at-a-time, controlled shutdown, overload valve, clock-rollover) → Tasks 3, 8, 14.
- No-ISR-survivor `NO_LEADER` + re-election on unfence → Tasks 5, 14.
- Unclean opt-in → Task 6.
- Restart-durability trio (LEC persist, PID watermark, atomic re-rebuild) → Tasks 9, 10, 11.
- Persistence tier (`memory`) → Tasks M1, M2 (+ fold-ins in 14/15/16).
- Note A (client bare NotLeader retriable) → Task 14. Note B (bridge delete tears down) → Task 12. Note C (config surface) → Task M1.
- Testing (integration-first, TSan, DR-E2E, benchmarks) → Tasks 12 (TSan), 14 (integration + TSan), 15 (bench + gate), 16 (DR-E2E after green).

**Gaps I could NOT fully resolve (flag to controller):**
- **Coordinator placement (§F, `hash(id)→node`)** is explicitly Phase 3 per the brief — intentionally NOT a task here (stated as a non-goal). No gap, but confirming the exclusion.
- **`ILog` seam scope (Task M2):** the plan extracts an `ILog` interface and retypes `PartitionReplica::log_` from `Log*` to `ILog*`. This touches every `Log*` call site (`MetadataService`, `NodeRuntimeAgent`, tests). If the controller prefers a less invasive route (template the log type), Task M2 Step 3 must be revised. Flagged because it is the one structural change that ripples beyond its own lib.

**2. Placeholder scan:** No "TBD"/"add error handling"/"similar to Task N". Test steps that show a `// body mirrors X` comment (Tasks 1 Step 10, 11 Step 1, 12 Step 1, 14, 16 harness helpers) point at a *named existing harness* to copy verbatim (`GatewayFailoverReplicationTest`, `GatewayPsmFailoverTest`, `MetadataReplicationTest`, `MultiLeaderProduceTest`) rather than leaving logic unspecified — the executor copies the cited driver loop. Acceptable per the "repeat the code / cite the exact pattern" rule; the load-bearing new assertions are spelled out.

**3. Type consistency:** `DurabilityMode` (Task 1) reused in Tasks M1/M2 config. `PersistenceMode` (M1) used in M2 backend selection. `ReplicaPositionReport`/`ReplicaPositionTable` (Task 2) consumed by Tasks 5/6/7/8/14. `AssignmentChange`/`mergeChange`/`PartitionAssignment` signatures match `PartitionAssignment.h` exactly. `FailoverElection::elect`/`ElectionInputs`/`ElectionOutcome` consistent across Tasks 5, 6, 7, 8. `MembershipState` API consistent across Tasks 3, 8, 14. `ControlTickRecord`/`ControlPlaneTimer` consistent across Tasks 4, 14. `LeaderEpochCache` snapshot/restore + `rebuildHighWatermark` names stable (Tasks 9, 11). `emitDynamicAssignment` name stable (Tasks 13, 14, 15). `ILog` seam name stable (M2, consumed implicitly by all `PartitionReplica` tasks).
