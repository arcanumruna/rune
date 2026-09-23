# Rune Milestone E — Phase E1 (WorkerPool + shard→worker ownership + parity) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Shard the single per-node duty cycle into N in-process, deterministically-assigned `Worker`s (each owning a disjoint set of `(coPartitionGroup, partitionIndex)` shards), re-hosting the existing per-partition `PartitionReplica` work behind them WITHOUT changing threading, ingress, or durability — proving byte-for-byte parity with Milestone D under N>1 workers.

**Architecture:** Introduce three pure/low-risk abstractions — `ShardRouter` (deterministic `key/(topic,partition) → shardId → workerId`, header-only, in `rune_metadata`), `Worker` (owns a set of `PartitionReplica*` for its shards; `doWork(nowNs)` runs replica doWork + cleaner/retention drains for ITS shards only), and `WorkerPool` (holds N `Worker`s; assigns each owned replica to a worker via `ShardRouter`; re-derives ownership when the committed assignment map changes). `NodeRuntimeAgent` stops iterating `registry_->allReplicas()` directly and instead delegates that iteration to `WorkerPool` — but everything still runs on the existing single `AgentRunner` duty thread (workers are driven cooperatively, NO new threads). The control plane (consensus, MSM apply, bridge, gateway ingress/subscribe/deferred-ack) is unchanged and still runs once per cycle.

**Tech Stack:** C++23, Aeron `Agent`/`AgentRunner` (`rune::consensus::Agent`), GoogleTest, TSan. Reuses `rune::metadata::gpidOf`, `RoutingImage`, `AssignmentMap`, `PartitionAssignment`, `StaticBalancer::TopicSpec::coPartitionGroup`.

**Spec:** `docs/superpowers/specs/2026-09-23-rune-thread-per-core-runtime-design.md` (Core principles 2–3; Components → `WorkerPool`/`Worker`/`Shard`/`ShardRouter`; "Migration from the single-`AgentRunner`" step 1; Phasing → E1).

## Global Constraints

- **Shared-nothing, single-owner-per-shard.** Every shard is owned by exactly one worker; a worker touches ONLY its owned shards' `PartitionReplica` state. No cross-worker shared mutable state. A shard is never concurrently owned (single-writer handoff on reassignment).
- **Ownership unit = the co-partition shard, not the individual partition.** `worker = f(coPartitionGroup, partitionIndex)`; all partitions sharing a co-partition group + index are owned by the SAME worker (aggregate-root affinity). A topic with no group is its own singleton shard keyed by its `topicId`.
- **Two-level assignment, both deterministic.** MSM assigns shard→node (Milestone D — do NOT re-implement). Each node maps shard→worker by `hash(shardId) % workerCount`. E1 adds ONLY the second level.
- **NO virtual calls on the hot path** ([[rune-no-virtual-calls-hot-path]]). `ShardRouter` is pure free/inline functions or a non-virtual struct; `Worker::doWork` is a concrete method called through a `Worker*`/reference, not a virtual base.
- **E1 is a parity-preserving refactor — explicit NON-GOALS (do NOT implement):** real OS-thread-per-worker + CPU affinity/isolation (→ E4); per-worker Aeron ingress + client worker-routing + cross-worker SPSC mailbox (→ E2); `io_uring` async durability + memory-topic `InMemoryLog` redo (→ E3). In E1 the N workers are driven cooperatively by the existing single `AgentRunner` duty thread. ZERO new threads.
- **Commit LOCAL `main` only. NEVER push.**
- **Build via CLion bundled cmake:** `/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake`. Canonical build dir = `build-tsan` (build-release has a pre-existing `BrokerNode::stop` SIGABRT — ignore it; it is not introduced here).
- **MANDATORY TSan** on every task that touches worker ownership or cross-cycle state — this is a concurrency-topology change even though E1 is single-driver; TSan proves no accidental cross-shard sharing.
- **Integration-first, TDD:** each task ends with an independently testable deliverable and the cycle failing test → run-fail → minimal impl → run-pass → commit, with REAL gtest code (no placeholders), an Interfaces (Consumes/Produces) block, and exact file paths.

**Build/run reference (used verbatim in every task's run steps):**
```bash
CMAKE=/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake
# Configure once (skip if build-tsan already configured):
"$CMAKE" -S /Users/vradu/Projects/rune-broker -B /Users/vradu/Projects/rune-broker/build-tsan \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=thread -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" \
  -DCMAKE_TOOLCHAIN_FILE=/Users/vradu/.conan2/... # (use the CLion conan_host_profile toolchain already wired in this repo)
```
> If `build-tsan` is already configured in this repo, skip the configure line and just `--build`.

---

## File Structure

- `libs/metadata/include/rune/metadata/ShardRouter.h` — NEW. Header-only pure router: `ShardId`, `shardIdOf`, `workerIdForShard`, `ShardRouter` (topicId→coPartitionGroupKey table + `workerCount`). In `rune_metadata` (INTERFACE lib) so both `rune_metadata` consumers and `rune_node` can use it with zero link cost.
- `libs/metadata/tests/ShardRouterTest.cpp` — NEW. Unit tests for determinism/totality/co-group affinity.
- `libs/node/include/rune/node/Worker.h` — NEW. `Worker` owns a `std::vector<PartitionReplica*>` (its shards' replicas); `doWork(nowNs)` runs the per-replica steps for its owned replicas only.
- `libs/node/include/rune/node/WorkerPool.h` — NEW. `WorkerPool` holds N `Worker`s; `assign(replica, shardId)` routes via `ShardRouter`; `doWork(nowNs)` fans out over workers; `reassignFromMap` re-derives ownership deterministically.
- `libs/node/tests/node/WorkerTest.cpp` — NEW. Unit test: a worker runs only its owned replicas.
- `libs/node/tests/node/WorkerPoolTest.cpp` — NEW. Unit tests: single-owner totality, co-group affinity, deterministic reassignment.
- `libs/node/include/rune/node/NodeConfig.h` — MODIFY. Add `int32_t workerCount` field.
- `libs/node/src/NodeConfig.cpp` — MODIFY. Parse `RUNE_WORKERS`.
- `libs/node/tests/node/NodeConfigTest.cpp` — MODIFY. Test `RUNE_WORKERS` parse + default.
- `libs/node/include/rune/node/NodeRuntimeAgent.h` — MODIFY. Optional `WorkerPool*`; when set, delegate the per-replica iteration (current steps 5 + 6.5) to it instead of iterating the registry directly.
- `libs/node/include/rune/node/BrokerNode.h` + `libs/node/src/BrokerNode.cpp` — MODIFY. Own a `WorkerPool`; build it after `replicaRegistry_`; assign replicas; pass it to `NodeRuntimeAgent`; re-derive ownership each cycle when the committed map changes.
- `libs/metadata/CMakeLists.txt`, `libs/node/CMakeLists.txt` — MODIFY. Register the new test executables.
- `libs/node/tests/integration/WorkerPoolParityTest.cpp` — NEW. In-process N-worker parity harness (final EXIT task).

---

## Task 1: `ShardRouter` — deterministic shard identity + shard→worker mapping

**Files:**
- Create: `libs/metadata/include/rune/metadata/ShardRouter.h`
- Create: `libs/metadata/tests/ShardRouterTest.cpp`
- Modify: `libs/metadata/CMakeLists.txt` (register `rune_shard_router_tests`)

**Interfaces:**
- Consumes: `rune::metadata::gpidOf(int32_t topicId, int32_t partition)` (from `Gpid.h`); `rune::metadata::kMaxGpidPartitions`.
- Produces:
  - `struct rune::metadata::ShardId { int32_t groupKey; int32_t partitionIndex; bool operator==(const ShardId&) const noexcept; };`
    where `groupKey` is a non-negative integer identifying the co-partition group (topics with no group use their own `topicId` as the groupKey; grouped topics share a common negative-namespaced key — see below) and `partitionIndex` is the partition ordinal.
  - `ShardId rune::metadata::shardIdOf(int32_t groupKey, int32_t partition) noexcept;`
  - `int32_t rune::metadata::workerIdForShard(const ShardId& s, int32_t workerCount) noexcept;` — `hash(shardId) % workerCount`, deterministic, no allocation, `workerCount>=1` (asserts).
  - `class rune::metadata::ShardRouter` with:
    - `explicit ShardRouter(int32_t workerCount) noexcept;`
    - `void mapTopicToGroupKey(int32_t topicId, int32_t groupKey) noexcept;` — register a topic's group key (call once per topic at construction/assignment time).
    - `[[nodiscard]] int32_t groupKeyForTopic(int32_t topicId) const noexcept;` — returns the registered group key, or `topicId` itself if unregistered (singleton fallback).
    - `[[nodiscard]] ShardId shardForPartition(int32_t topicId, int32_t partition) const noexcept;`
    - `[[nodiscard]] int32_t workerForPartition(int32_t topicId, int32_t partition) const noexcept;`
    - `[[nodiscard]] int32_t workerCount() const noexcept;`

  > Group-key namespacing: to guarantee a grouped topic and an ungrouped topic never collide on the same `groupKey`, the *builder* (Task 5, from config) assigns each distinct non-empty `coPartitionGroup` string a stable negative key `-(1 + groupOrdinal)` (group ordinals in sorted group-name order, matching StaticBalancer determinism), and leaves ungrouped topics mapping to their own non-negative `topicId`. `ShardRouter` itself only stores the int→int table; the string→ordinal decision is the caller's (keeps the router pure and allocation-free).

- [ ] **Step 1: Write the failing test**

Create `libs/metadata/tests/ShardRouterTest.cpp`:

```cpp
#include <rune/metadata/ShardRouter.h>
#include <gtest/gtest.h>

using rune::metadata::ShardRouter;
using rune::metadata::ShardId;
using rune::metadata::shardIdOf;
using rune::metadata::workerIdForShard;

TEST(ShardRouter, SingletonTopicShardsByTopicId) {
    ShardRouter r(4);
    // topic 7 has no group registered -> groupKey falls back to topicId.
    EXPECT_EQ(r.groupKeyForTopic(7), 7);
    const ShardId s = r.shardForPartition(/*topicId=*/7, /*partition=*/2);
    EXPECT_EQ(s.groupKey, 7);
    EXPECT_EQ(s.partitionIndex, 2);
}

TEST(ShardRouter, CoGroupSameIndexSameWorker) {
    ShardRouter r(4);
    // topics 0 and 1 co-partitioned -> share group key -100.
    r.mapTopicToGroupKey(0, -100);
    r.mapTopicToGroupKey(1, -100);
    // Same partition index across group members -> same shard -> same worker.
    for (int32_t idx = 0; idx < 8; ++idx) {
        EXPECT_EQ(r.shardForPartition(0, idx), r.shardForPartition(1, idx));
        EXPECT_EQ(r.workerForPartition(0, idx), r.workerForPartition(1, idx));
    }
}

TEST(ShardRouter, DeterministicAndTotal) {
    ShardRouter r(3);
    for (int32_t t = 0; t < 5; ++t) {
        for (int32_t p = 0; p < 6; ++p) {
            const int32_t w1 = r.workerForPartition(t, p);
            const int32_t w2 = r.workerForPartition(t, p);
            EXPECT_EQ(w1, w2);                 // deterministic
            EXPECT_GE(w1, 0);                  // total: in range
            EXPECT_LT(w1, r.workerCount());
        }
    }
}

TEST(ShardRouter, WorkerIdInRangeForAllShards) {
    for (int32_t wc = 1; wc <= 8; ++wc) {
        for (int32_t g = -5; g < 20; ++g) {
            for (int32_t p = 0; p < 10; ++p) {
                const int32_t w = workerIdForShard(shardIdOf(g, p), wc);
                EXPECT_GE(w, 0);
                EXPECT_LT(w, wc);
            }
        }
    }
}

TEST(ShardRouter, DifferentIndexMayDifferButIsStable) {
    ShardRouter r(4);
    r.mapTopicToGroupKey(0, -100);
    // Shard identity differs by index; the mapping is stable across calls.
    const ShardId a = r.shardForPartition(0, 0);
    const ShardId b = r.shardForPartition(0, 1);
    EXPECT_FALSE(a == b);
}
```

- [ ] **Step 2: Register the test target and run it to verify it fails**

Add to `libs/metadata/CMakeLists.txt` (mirror the existing `rune_routing_image_tests` block):

```cmake
    add_executable(rune_shard_router_tests
        tests/ShardRouterTest.cpp)
    target_link_libraries(rune_shard_router_tests PRIVATE
        rune_metadata
        GTest::gtest_main)
    gtest_discover_tests(rune_shard_router_tests)
```

Run:
```bash
CMAKE=/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_shard_router_tests
```
Expected: FAIL — compile error, `rune/metadata/ShardRouter.h` not found.

- [ ] **Step 3: Write minimal implementation**

Create `libs/metadata/include/rune/metadata/ShardRouter.h`:

```cpp
// ShardRouter.h — pure, deterministic shard identity + shard→worker mapping.
//
// Two-level assignment (Milestone E, Core principle 3):
//   MSM assigns shard→NODE (Milestone D; not here).
//   Each node maps shard→WORKER = hash(shardId) % workerCount (here).
//
// Ownership unit = the co-partition shard, NOT the individual partition:
//   shardId = (groupKey, partitionIndex).
//   Grouped topics share a groupKey (caller assigns; see BrokerNode builder).
//   Ungrouped topics fall back to their own topicId as the groupKey (singleton).
//
// Pure: no allocation on the hot path, no virtual calls, deterministic.
#pragma once

#include <rune/metadata/Gpid.h>

#include <cassert>
#include <cstdint>
#include <unordered_map>

namespace rune::metadata {

struct ShardId {
    int32_t groupKey;        ///< co-partition group key (or topicId for singletons)
    int32_t partitionIndex;  ///< partition ordinal within the shard

    bool operator==(const ShardId& o) const noexcept {
        return groupKey == o.groupKey && partitionIndex == o.partitionIndex;
    }
};

[[nodiscard]]
inline ShardId shardIdOf(int32_t groupKey, int32_t partition) noexcept {
    return ShardId{groupKey, partition};
}

/// Deterministic shard→worker map. hash(shardId) % workerCount.
/// Uses the same GPID-style packing so the hash is well-distributed and stable.
[[nodiscard]]
inline int32_t workerIdForShard(const ShardId& s, int32_t workerCount) noexcept {
    assert(workerCount >= 1 && "workerCount must be >= 1");
    // Pack (groupKey, partitionIndex) into a 64-bit key, then Fibonacci hash
    // (same multiplicative constant as AssignmentMap for consistency).
    const uint64_t k =
        (static_cast<uint64_t>(static_cast<uint32_t>(s.groupKey)) << 32) |
         static_cast<uint64_t>(static_cast<uint32_t>(s.partitionIndex));
    const uint64_t h = k * UINT64_C(11400714819323198485);
    return static_cast<int32_t>((h >> 32) % static_cast<uint64_t>(workerCount));
}

class ShardRouter {
public:
    explicit ShardRouter(int32_t workerCount) noexcept
        : workerCount_(workerCount) {
        assert(workerCount_ >= 1 && "workerCount must be >= 1");
    }

    void mapTopicToGroupKey(int32_t topicId, int32_t groupKey) noexcept {
        topicGroup_[topicId] = groupKey;
    }

    [[nodiscard]] int32_t groupKeyForTopic(int32_t topicId) const noexcept {
        const auto it = topicGroup_.find(topicId);
        return (it != topicGroup_.end()) ? it->second : topicId;
    }

    [[nodiscard]] ShardId shardForPartition(int32_t topicId,
                                            int32_t partition) const noexcept {
        return shardIdOf(groupKeyForTopic(topicId), partition);
    }

    [[nodiscard]] int32_t workerForPartition(int32_t topicId,
                                             int32_t partition) const noexcept {
        return workerIdForShard(shardForPartition(topicId, partition), workerCount_);
    }

    [[nodiscard]] int32_t workerCount() const noexcept { return workerCount_; }

private:
    int32_t workerCount_;
    // topicId -> groupKey. Not on the append hot path (built at assignment time);
    // lookups here are during ownership derivation, not per-produce.
    std::unordered_map<int32_t, int32_t> topicGroup_;
};

} // namespace rune::metadata
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_shard_router_tests
/Users/vradu/Projects/rune-broker/build-tsan/libs/metadata/rune_shard_router_tests
```
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/metadata/include/rune/metadata/ShardRouter.h \
        libs/metadata/tests/ShardRouterTest.cpp \
        libs/metadata/CMakeLists.txt
git commit -m "feat(E1): ShardRouter — deterministic shard identity + shard->worker map

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `Worker` — runs the per-shard duty steps for its owned replicas only

**Files:**
- Create: `libs/node/include/rune/node/Worker.h`
- Create: `libs/node/tests/node/WorkerTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (register `rune_worker_tests`)

**Interfaces:**
- Consumes: `rune::replication::PartitionReplica` (`doWork(int64_t)`, `isDiskBacked()`, `log()` → `rune::storage::Log*` with `drainCleanerSwaps()/drainRetentionDeletes()/drainStagingRenamesDone()`; `topicId()`, `partitionId()`). These are the EXACT operations `NodeRuntimeAgent::doWork` steps 5 + 6.5 perform today.
- Produces:
  - `class rune::node::Worker` with:
    - `explicit Worker(int32_t workerId) noexcept;`
    - `void ownReplica(rune::replication::PartitionReplica* r) noexcept;` — add a replica to this worker's owned set (non-owning pointer; the `ReplicaRegistry` still owns the object).
    - `void clearOwned() noexcept;` — drop all owned replicas (used before a re-derivation).
    - `[[nodiscard]] int workerId() const noexcept;`
    - `[[nodiscard]] std::size_t ownedCount() const noexcept;`
    - `[[nodiscard]] bool owns(const rune::replication::PartitionReplica* r) const noexcept;`
    - `int doWork(int64_t nowNs);` — for each owned replica: `work += r->doWork(nowNs)`; then (disk-backed only) `work += log->drainCleanerSwaps() + drainRetentionDeletes() + drainStagingRenamesDone()`. Returns total. NON-virtual (called via `Worker&`).

- [ ] **Step 1: Write the failing test**

Create `libs/node/tests/node/WorkerTest.cpp`. Use a lightweight fake replica that satisfies the two methods the worker calls on the non-disk path (a real `PartitionReplica` needs a Log; the cleaner-drain path is covered by the parity integration test in Task 7). To keep this a pure unit test, the worker's replica type is templated-free — it takes `PartitionReplica*`. We therefore build a minimal in-memory `PartitionReplica` via the same helper the existing `rune_txn_coordination_service_tests` use (single-node, `InMemoryTransport`, memory log), and assert the worker only drives replicas it owns.

```cpp
#include <rune/node/Worker.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/replication/InMemoryTransport.h>
#include <gtest/gtest.h>

#include <memory>

using rune::node::Worker;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::replication::InMemoryTransport;
using rune::storage::InMemoryLog;

namespace {

// Build a minimal single-node memory-backed replica (role transitions off;
// doWork is a no-op-ish ISR/fetch pass that returns a small work count).
struct ReplicaFixture {
    InMemoryLog                     log;
    InMemoryTransport               transport;
    std::unique_ptr<PartitionReplica> replica;

    ReplicaFixture(int32_t topicId, int32_t partition) {
        PartitionConfig cfg{};
        cfg.topicId     = topicId;
        cfg.partition   = partition;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(partition);
        cfg.minIsr      = 1;
        replica = std::make_unique<PartitionReplica>(cfg, &log, &transport);
    }
};

} // namespace

TEST(Worker, DrivesOnlyOwnedReplicas) {
    ReplicaFixture a(/*topicId=*/0, /*partition=*/0);
    ReplicaFixture b(/*topicId=*/0, /*partition=*/1);

    Worker w(/*workerId=*/0);
    w.ownReplica(a.replica.get());

    EXPECT_TRUE(w.owns(a.replica.get()));
    EXPECT_FALSE(w.owns(b.replica.get()));
    EXPECT_EQ(w.ownedCount(), 1u);
    EXPECT_EQ(w.workerId(), 0);

    // doWork must run without touching b (it is not owned). We can only assert
    // it does not crash and returns a non-negative work count; the shared-nothing
    // property (b's state untouched) is proven structurally by owns()==false and
    // is TSan-verified in the WorkerPool parity test (Task 7).
    const int work = w.doWork(/*nowNs=*/1);
    EXPECT_GE(work, 0);
}

TEST(Worker, ClearOwnedResetsOwnership) {
    ReplicaFixture a(0, 0);
    Worker w(1);
    w.ownReplica(a.replica.get());
    ASSERT_EQ(w.ownedCount(), 1u);
    w.clearOwned();
    EXPECT_EQ(w.ownedCount(), 0u);
    EXPECT_FALSE(w.owns(a.replica.get()));
}
```

> If the `PartitionConfig`/constructor signature above does not compile, mirror the exact construction used in `libs/node/tests/node/TransactionCoordinationServiceTest.cpp` (it builds a single-node memory `PartitionReplica`) — that call site is the ground truth for the replica ctor.

- [ ] **Step 2: Register the target and run it to verify it fails**

Add to `libs/node/CMakeLists.txt` (mirror `rune_bridge_per_partition_tests`, minus the protocol include if unused):

```cmake
    add_executable(rune_worker_tests
        tests/node/WorkerTest.cpp)
    target_link_libraries(rune_worker_tests PRIVATE
        rune_node
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_tests)
```

Run:
```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_tests
```
Expected: FAIL — `rune/node/Worker.h` not found.

- [ ] **Step 3: Write minimal implementation**

Create `libs/node/include/rune/node/Worker.h`:

```cpp
#pragma once
// Worker.h — E1: a Worker owns a disjoint set of shards' PartitionReplicas and
// runs, for ITS owned replicas only, the exact per-replica duty steps the single
// NodeRuntimeAgent runs today (replica->doWork + disk-log cleaner/retention drains).
//
// E1 non-goals: no thread of its own, no ingress, no io_uring. It is driven
// cooperatively by the single AgentRunner via WorkerPool::doWork.
//
// Shared-nothing: a Worker touches ONLY the PartitionReplicas it owns. The
// ReplicaRegistry still OWNS the PartitionReplica objects; Worker holds
// non-owning pointers. Single-writer: doWork runs on the duty-cycle thread only.

#include <rune/replication/PartitionReplica.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace rune::node {

class Worker {
public:
    explicit Worker(int32_t workerId) noexcept : workerId_(workerId) {}

    Worker(const Worker&)            = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&)                 = default;
    Worker& operator=(Worker&&)      = default;

    void ownReplica(rune::replication::PartitionReplica* r) noexcept {
        if (r) owned_.push_back(r);
    }

    void clearOwned() noexcept { owned_.clear(); }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }
    [[nodiscard]] std::size_t ownedCount() const noexcept { return owned_.size(); }

    [[nodiscard]] bool owns(const rune::replication::PartitionReplica* r) const noexcept {
        return std::find(owned_.begin(), owned_.end(), r) != owned_.end();
    }

    // Run the per-replica duty steps for owned replicas only. Mirrors
    // NodeRuntimeAgent::doWork steps 5 + 6.5 exactly.
    int doWork(int64_t nowNs) {
        int work = 0;
        for (auto* replica : owned_) {
            if (!replica) continue;
            work += replica->doWork(nowNs);
        }
        for (auto* replica : owned_) {
            if (replica && replica->isDiskBacked()) {
                work += replica->log()->drainCleanerSwaps();
                work += replica->log()->drainRetentionDeletes();
                work += replica->log()->drainStagingRenamesDone();
            }
        }
        return work;
    }

private:
    int32_t workerId_;
    std::vector<rune::replication::PartitionReplica*> owned_;  // non-owning
};

} // namespace rune::node
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_tests
/Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_worker_tests
```
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/Worker.h \
        libs/node/tests/node/WorkerTest.cpp \
        libs/node/CMakeLists.txt
git commit -m "feat(E1): Worker — runs per-replica duty steps for owned shards only

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `WorkerPool` — N workers, deterministic assignment, single-owner totality, reassignment

**Files:**
- Create: `libs/node/include/rune/node/WorkerPool.h`
- Create: `libs/node/tests/node/WorkerPoolTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (register `rune_worker_pool_tests`)

**Interfaces:**
- Consumes: `rune::node::Worker` (Task 2); `rune::metadata::ShardRouter` (Task 1: `workerForPartition`, `mapTopicToGroupKey`, `workerCount`); `rune::node::ReplicaRegistry` (`allReplicas()` → `const std::vector<PartitionReplica*>&`); `PartitionReplica::topicId()`, `PartitionReplica::partitionId()`.
- Produces:
  - `class rune::node::WorkerPool` with:
    - `WorkerPool(int32_t workerCount, rune::metadata::ShardRouter router);` — constructs N `Worker`s (ids 0..N-1); takes the router by value (owns its topic→group table).
    - `[[nodiscard]] int32_t workerCount() const noexcept;`
    - `[[nodiscard]] Worker& worker(int32_t id) noexcept;`
    - `[[nodiscard]] const rune::metadata::ShardRouter& router() const noexcept;`
    - `void assignFromRegistry(const rune::node::ReplicaRegistry& registry);` — clear all workers, then for each replica route `(topicId, partitionIndex)`→workerId via the router and `ownReplica`. Deterministic + total (every replica owned by exactly one worker).
    - `[[nodiscard]] int32_t ownerWorkerId(int32_t topicId, int32_t partition) const noexcept;` — the worker that owns this partition per the router.
    - `int doWork(int64_t nowNs);` — `for each worker: work += worker.doWork(nowNs)`; returns total.
  - `partitionIndex` used for routing = `PartitionReplica::partitionId()` cast to `int32_t`. **CORRECTNESS-CRITICAL invariant (verified):** co-partition affinity requires routing on the partition ORDINAL, and `partitionId()` currently equals the ordinal — `BrokerNode.cpp:432` sets `pcfg.partitionId = static_cast<PartitionId>(part)` (the partition ordinal). If `partitionId` ever becomes the global gpid (`topicId*1024+partition`), routing on it would put `topicX-p0` and `topicY-p0` on DIFFERENT workers and BREAK aggregate-root affinity. Guard it: at the wiring point (Task 5) add `assert(rep->partitionId() == static_cast<PartitionId>(rep_partition_ordinal))` OR (preferred) add a `[[nodiscard]] int32_t partition() const noexcept { return cfg_.partition; }` accessor to `PartitionReplica` and route on `partition()` (the explicit ordinal, `PartitionConfig.partition` line 69) instead of `partitionId()`. The Task-1 `CoGroupSameIndexSameWorker` test already proves the router is correct *given the ordinal*; this guard ensures the wiring always feeds it the ordinal.

- [ ] **Step 1: Write the failing test**

Create `libs/node/tests/node/WorkerPoolTest.cpp`:

```cpp
#include <rune/node/WorkerPool.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/replication/InMemoryTransport.h>
#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <vector>

using rune::node::WorkerPool;
using rune::node::ReplicaRegistry;
using rune::metadata::ShardRouter;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::replication::InMemoryTransport;
using rune::storage::InMemoryLog;

namespace {
struct Rep {
    InMemoryLog       log;
    InMemoryTransport transport;
    std::unique_ptr<PartitionReplica> replica;
    Rep(int32_t topicId, int32_t partition) {
        PartitionConfig cfg{};
        cfg.topicId = topicId; cfg.partition = partition;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(partition);
        cfg.minIsr = 1;
        replica = std::make_unique<PartitionReplica>(cfg, &log, &transport);
    }
};
} // namespace

TEST(WorkerPool, EveryReplicaOwnedByExactlyOneWorker) {
    ReplicaRegistry reg;
    std::vector<std::unique_ptr<Rep>> keep;
    for (int32_t p = 0; p < 6; ++p) {
        auto r = std::make_unique<Rep>(/*topicId=*/0, p);
        reg.add(0, p, /*moved below*/ nullptr); // placeholder; see note
        keep.push_back(std::move(r));
    }
    // NOTE: ReplicaRegistry::add takes ownership; to keep this a pure unit test
    // without transferring ownership, build the pool directly from a raw vector.
    // The production path (Task 5) uses assignFromRegistry against the real
    // registry that DOES own the replicas.

    ShardRouter router(3);
    WorkerPool pool(3, router);

    // Emulate assignFromRegistry over the raw replicas:
    for (auto& rep : keep) {
        const int32_t tid = rep->replica->topicId();
        const int32_t pid = static_cast<int32_t>(rep->replica->partitionId());
        const int32_t wid = pool.ownerWorkerId(tid, pid);
        pool.worker(wid).ownReplica(rep->replica.get());
    }

    // Totality: each replica owned by exactly one worker.
    for (auto& rep : keep) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            if (pool.worker(w).owns(rep->replica.get())) ++owners;
        }
        EXPECT_EQ(owners, 1) << "replica must be owned by exactly one worker";
    }
}

TEST(WorkerPool, CoGroupSameIndexSameWorker) {
    ShardRouter router(4);
    router.mapTopicToGroupKey(0, -100);
    router.mapTopicToGroupKey(1, -100);
    WorkerPool pool(4, router);
    for (int32_t idx = 0; idx < 8; ++idx) {
        EXPECT_EQ(pool.ownerWorkerId(0, idx), pool.ownerWorkerId(1, idx))
            << "co-group members must share a worker per index";
    }
}

TEST(WorkerPool, ReassignIsDeterministic) {
    ShardRouter r1(3); WorkerPool p1(3, r1);
    ShardRouter r2(3); WorkerPool p2(3, r2);
    for (int32_t t = 0; t < 4; ++t)
        for (int32_t p = 0; p < 5; ++p)
            EXPECT_EQ(p1.ownerWorkerId(t, p), p2.ownerWorkerId(t, p));
}

TEST(WorkerPool, AssignFromRegistryOwnsAllRegisteredReplicas) {
    ReplicaRegistry reg;
    // registry OWNS the replicas (production path).
    auto mk = [](int32_t topicId, int32_t partition) {
        static std::vector<std::unique_ptr<InMemoryLog>> logs;
        static std::vector<std::unique_ptr<InMemoryTransport>> trs;
        logs.push_back(std::make_unique<InMemoryLog>());
        trs.push_back(std::make_unique<InMemoryTransport>());
        PartitionConfig cfg{};
        cfg.topicId = topicId; cfg.partition = partition;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(partition);
        cfg.minIsr = 1;
        return std::make_unique<PartitionReplica>(cfg, logs.back().get(), trs.back().get());
    };
    for (int32_t p = 0; p < 4; ++p) reg.add(0, p, mk(0, p));

    ShardRouter router(2);
    WorkerPool pool(2, router);
    pool.assignFromRegistry(reg);

    // Every registered replica is owned by exactly one worker.
    for (auto* r : reg.allReplicas()) {
        int owners = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w)
            if (pool.worker(w).owns(r)) ++owners;
        EXPECT_EQ(owners, 1);
    }
}
```

> The first test's `reg.add(..., nullptr)` placeholder line is illustrative of the ownership pitfall — DELETE that line when implementing; drive the pool directly from `keep` as shown. The 4th test is the authoritative `assignFromRegistry` coverage.

- [ ] **Step 2: Register the target and run it to verify it fails**

Add to `libs/node/CMakeLists.txt`:

```cmake
    add_executable(rune_worker_pool_tests
        tests/node/WorkerPoolTest.cpp)
    target_link_libraries(rune_worker_pool_tests PRIVATE
        rune_node
        rune_metadata
        rune_replication
        GTest::gtest_main)
    gtest_discover_tests(rune_worker_pool_tests)
```

Run:
```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_pool_tests
```
Expected: FAIL — `rune/node/WorkerPool.h` not found.

- [ ] **Step 3: Write minimal implementation**

Create `libs/node/include/rune/node/WorkerPool.h`:

```cpp
#pragma once
// WorkerPool.h — E1: owns N Workers; deterministically assigns each owned
// PartitionReplica to a worker via ShardRouter; fans out doWork over workers.
//
// Single-owner-per-shard is guaranteed by the ShardRouter being a total,
// deterministic function of (topicId, partitionIndex): assignFromRegistry
// clears all workers and re-derives ownership, so a shard is never held by two
// workers. Reassignment (D dynamic rebalance / worker-count unchanged in E1)
// is just a re-run of assignFromRegistry on the duty-cycle thread — no shared
// lock; the shard is never concurrently owned (single-writer control plane).
//
// E1: driven cooperatively by the single AgentRunner (no per-worker thread).

#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/metadata/ShardRouter.h>

#include <cassert>
#include <cstdint>
#include <utility>
#include <vector>

namespace rune::node {

class WorkerPool {
public:
    WorkerPool(int32_t workerCount, rune::metadata::ShardRouter router)
        : router_(std::move(router)) {
        assert(workerCount >= 1 && "workerCount must be >= 1");
        assert(workerCount == router_.workerCount() &&
               "router.workerCount() must match pool workerCount");
        workers_.reserve(static_cast<std::size_t>(workerCount));
        for (int32_t i = 0; i < workerCount; ++i) workers_.emplace_back(i);
    }

    WorkerPool(const WorkerPool&)            = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    [[nodiscard]] int32_t workerCount() const noexcept {
        return static_cast<int32_t>(workers_.size());
    }

    [[nodiscard]] Worker& worker(int32_t id) noexcept {
        assert(id >= 0 && id < workerCount());
        return workers_[static_cast<std::size_t>(id)];
    }

    [[nodiscard]] const rune::metadata::ShardRouter& router() const noexcept {
        return router_;
    }

    [[nodiscard]] int32_t ownerWorkerId(int32_t topicId,
                                        int32_t partition) const noexcept {
        return router_.workerForPartition(topicId, partition);
    }

    // Clear all workers, then re-derive ownership from the registry.
    // Deterministic + total. Safe to call every duty cycle (idempotent when the
    // registry is unchanged); called on the single duty-cycle thread only.
    void assignFromRegistry(const ReplicaRegistry& registry) {
        for (auto& w : workers_) w.clearOwned();
        for (auto* r : registry.allReplicas()) {
            if (!r) continue;
            const int32_t tid = r->topicId();
            const int32_t pid = static_cast<int32_t>(r->partitionId());
            const int32_t wid = ownerWorkerId(tid, pid);
            workers_[static_cast<std::size_t>(wid)].ownReplica(r);
        }
    }

    int doWork(int64_t nowNs) {
        int work = 0;
        for (auto& w : workers_) work += w.doWork(nowNs);
        return work;
    }

private:
    rune::metadata::ShardRouter router_;
    std::vector<Worker> workers_;
};

} // namespace rune::node
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_pool_tests
/Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_worker_pool_tests
```
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/WorkerPool.h \
        libs/node/tests/node/WorkerPoolTest.cpp \
        libs/node/CMakeLists.txt
git commit -m "feat(E1): WorkerPool — N workers, deterministic single-owner assignment

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: `NodeConfig.workerCount` + `RUNE_WORKERS` parsing

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (add `int32_t workerCount`)
- Modify: `libs/node/src/NodeConfig.cpp` (parse `RUNE_WORKERS`, default 1, floor 1)
- Modify: `libs/node/tests/node/NodeConfigTest.cpp` (add tests)

**Interfaces:**
- Consumes: `parseNodeConfig(const std::function<const char*(const char*)>&)` (existing; injected env lookup).
- Produces: `NodeConfig::workerCount` (`int32_t`, default `1`). Value `<1` or unparsable → clamped to `1`.

- [ ] **Step 1: Write the failing test**

Add to `libs/node/tests/node/NodeConfigTest.cpp` (mirror the existing `ParsesCoPartitionGroupKeyAndPersistence` fixture style — build a minimal valid env map, then assert):

Use the REAL env-injection helpers already in `NodeConfigTest.cpp` (verified): `fakeEnv(std::map<std::string,std::string>)` builds the `getenv_fn`, and `base1node()` returns a minimal valid single-node env map (there is also `base3node()`).

```cpp
TEST(NodeConfig, ParsesWorkerCountDefaultsToOne) {
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));  // RUNE_WORKERS unset
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCount, 1);  // default
}

TEST(NodeConfig, ParsesRuneWorkersEnv) {
    auto base = base1node();
    base["RUNE_WORKERS"] = "4";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCount, 4);
}

TEST(NodeConfig, RuneWorkersFloorsToOne) {
    auto base = base1node();
    base["RUNE_WORKERS"] = "0";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCount, 1) << "0/negative/garbage floors to 1";
}
```

> Helpers `fakeEnv(...)`, `base1node()`, `base3node()` are the actual fixtures in `NodeConfigTest.cpp` (verified) — reuse them; do NOT invent `makeMinimalSingleNodeEnv`/`mapLookup` (they don't exist).

- [ ] **Step 2: Run the test to verify it fails**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_node_tests
```
Expected: FAIL — `workerCount` is not a member of `NodeConfig`.

- [ ] **Step 3: Write minimal implementation**

In `libs/node/include/rune/node/NodeConfig.h`, add to `struct NodeConfig` (next to `gatewayMinIsr`):

```cpp
    // Milestone E1: number of in-process partition workers. Each owned shard is
    // assigned to worker = hash(shardId) % workerCount (ShardRouter). Default 1
    // (single-owner, identical to the pre-E1 single-agent behaviour). Overridable
    // via RUNE_WORKERS. Floored to 1.
    int32_t workerCount = 1;
```

In `libs/node/src/NodeConfig.cpp`, after the block that parses `gatewayMinIsr` / `RUNE_MIN_ISR` (mirror that exact parse-and-clamp idiom):

```cpp
    // Milestone E1: RUNE_WORKERS — in-process partition worker count (floor 1).
    if (const char* w = getenv_fn("RUNE_WORKERS")) {
        char* end = nullptr;
        const long v = std::strtol(w, &end, 10);
        cfg.workerCount = (end != w && v >= 1) ? static_cast<int32_t>(v) : 1;
    } else {
        cfg.workerCount = 1;
    }
```

> Confirm `<cstdlib>` is included in `NodeConfig.cpp` (it is used elsewhere for `strtol`/`atoi`); if not, add it.

- [ ] **Step 4: Run the test to verify it passes**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_node_tests
/Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_node_tests --gtest_filter='NodeConfig.*Worker*'
```
Expected: PASS (3 new tests).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/NodeConfig.h \
        libs/node/src/NodeConfig.cpp \
        libs/node/tests/node/NodeConfigTest.cpp
git commit -m "feat(E1): NodeConfig.workerCount + RUNE_WORKERS parsing (floor 1)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Wire `WorkerPool` into `BrokerNode` + delegate the per-replica iteration in `NodeRuntimeAgent`

**Files:**
- Modify: `libs/node/include/rune/node/NodeRuntimeAgent.h` (add optional `WorkerPool*`; delegate steps 5 + 6.5)
- Modify: `libs/node/include/rune/node/BrokerNode.h` (own a `WorkerPool`; include `WorkerPool.h`)
- Modify: `libs/node/src/BrokerNode.cpp` (build the pool + router from config topics; assign; pass to `NodeRuntimeAgent`; destruction order)

**Interfaces:**
- Consumes: `WorkerPool` (Task 3: `assignFromRegistry`, `doWork`, ctor `(int32_t, ShardRouter)`); `ShardRouter` (Task 1: `mapTopicToGroupKey`); `NodeConfig::workerCount`, `NodeConfig::topics` (`TopicSpec::coPartitionGroup`, `TopicSpec::topicId`); `ReplicaRegistry::allReplicas()`.
- Produces:
  - `NodeRuntimeAgent` gains an appended optional ctor param `WorkerPool* workerPool = nullptr` (added to the FULL constructor only — the backward-compatible ctor keeps `nullptr`). When `workerPool_ != nullptr`, `doWork` calls `workerPool_->doWork(nowNs)` and SKIPS the direct `registry_->allReplicas()` loops (steps 5 + 6.5). When null, behaviour is UNCHANGED (pre-E1 path).
  - `BrokerNode::workerPool_` (`std::unique_ptr<WorkerPool>`); `BrokerNode::buildWorkerPool()` helper.
  - **Grouping rule (from config):** distinct non-empty `TopicSpec::coPartitionGroup` strings → sorted → assigned keys `-1, -2, …` in sorted order; ungrouped topics map to their own `topicId`. This mirrors `StaticBalancer`'s sorted-group-name determinism so all nodes derive identical shard identities.

- [ ] **Step 1: Write the failing test (parity of the delegated path at the unit level)**

Add to `libs/node/tests/node/WorkerPoolTest.cpp` a test that proves `NodeRuntimeAgent` with a `WorkerPool` runs the same replica set as the registry loop would. Because `NodeRuntimeAgent` needs a consensus agent, use a stub `rune::consensus::Agent` returning 0:

```cpp
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/consensus/ConsensusAgent.h>  // for the Agent base

namespace {
struct StubAgent final : rune::consensus::Agent {
    int doWork(int64_t) override { return 0; }
};
} // namespace

TEST(WorkerPool, NodeRuntimeAgentDelegatesReplicaIterationToPool) {
    // Registry owns replicas.
    ReplicaRegistry reg;
    static std::vector<std::unique_ptr<InMemoryLog>> logs;
    static std::vector<std::unique_ptr<InMemoryTransport>> trs;
    for (int32_t p = 0; p < 3; ++p) {
        logs.push_back(std::make_unique<InMemoryLog>());
        trs.push_back(std::make_unique<InMemoryTransport>());
        PartitionConfig cfg{};
        cfg.topicId = 0; cfg.partition = p;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(p);
        cfg.minIsr = 1;
        reg.add(0, p, std::make_unique<PartitionReplica>(cfg, logs.back().get(), trs.back().get()));
    }

    ShardRouter router(2);
    WorkerPool pool(2, router);
    pool.assignFromRegistry(reg);

    StubAgent stub;
    // Full ctor with workerPool appended; gateway/bridge/etc all null.
    rune::node::NodeRuntimeAgent agent(
        stub, /*election=*/nullptr, /*bridge=*/nullptr, /*transport=*/nullptr,
        /*registry=*/&reg, /*gateway=*/nullptr, /*group=*/nullptr,
        /*txn=*/nullptr, /*metadata=*/nullptr, /*workerPool=*/&pool);

    // Must not crash and must return a non-negative work count; the replicas are
    // driven via the pool, not the registry loop.
    EXPECT_GE(agent.doWork(/*nowNs=*/1), 0);
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_pool_tests
```
Expected: FAIL — `NodeRuntimeAgent` full ctor has no `workerPool` parameter.

- [ ] **Step 3: Modify `NodeRuntimeAgent.h`**

Append `WorkerPool* workerPool = nullptr` to the FULL constructor signature and initializer list, add the member, and gate steps 5 + 6.5. Concretely:

Add include near the top:
```cpp
#include <rune/node/WorkerPool.h>
```

Full ctor — add the parameter (after `metadataService`) and initializer:
```cpp
                     MetadataService*                    metadataService = nullptr,
                     WorkerPool*                         workerPool      = nullptr) noexcept
        : consensus_(consensusAgent)
        , election_(election)
        , bridge_(bridge)
        , transport_(transport)
        , registry_(registry)
        , gateway_(gateway)
        , groupService_(groupService)
        , txnService_(txnService)
        , metadataService_(metadataService)
        , workerPool_(workerPool)
    {}
```
Backward-compatible ctor — add `, workerPool_(nullptr)` to its initializer list.

Add the member (next to `metadataService_`):
```cpp
    WorkerPool*                                        workerPool_;  // non-owning; null = pre-E1 path
```

Replace step 5 (the `if (registry_) { for (auto* replica : registry_->allReplicas()) ... replica->doWork ... }`) AND step 6.5 (the cleaner/retention drain loop) with:

```cpp
        // 5 + 6.5. Per-replica duty cycle + disk-log cleaner/retention drains.
        // E1: when a WorkerPool is wired, each Worker runs these steps for ITS
        // owned shards only (shared-nothing, single-owner). Driven cooperatively
        // here on the single AgentRunner thread (no new threads in E1).
        // When workerPool_ is null, fall back to the pre-E1 single-agent path.
        if (workerPool_) {
            work += workerPool_->doWork(nowNs);
        } else if (registry_) {
            for (auto* replica : registry_->allReplicas()) {
                if (replica) work += replica->doWork(nowNs);
            }
            for (auto* replica : registry_->allReplicas()) {
                if (replica && replica->isDiskBacked()) {
                    work += replica->log()->drainCleanerSwaps();
                    work += replica->log()->drainRetentionDeletes();
                    work += replica->log()->drainStagingRenamesDone();
                }
            }
        }
```

> Gateway (step 6) stays between the old step 5 and step 6.5 today; after this change the gateway call must still run once. Re-read the current step ordering: today it is `5 (replica doWork) → 6 (gateway) → 6.5 (drains)`. Preserve that: keep the gateway `doWork` call in its current position, and place the combined `workerPool_`/fallback block so the ordering the workers produce (replica doWork, then drains) still brackets the gateway correctly. SAFEST re-host: keep step 6 (gateway) exactly where it is; run `workerPool_->doWork` in place of BOTH step 5 and step 6.5, but split it so replica-doWork runs BEFORE gateway and drains run AFTER — i.e. `Worker` already runs doWork-then-drains internally; since the gateway's per-cycle work (`drainPendingAcks`, `pushSubscriptions`) reads HW that replica doWork advanced, running the whole `workerPool_->doWork` BEFORE the gateway is correct and matches the invariant in `ClientGateway::doWork`'s comment ("replication doWork is driven ... BEFORE gateway.doWork"). Therefore: run `workerPool_->doWork(nowNs)` in the step-5 position and REMOVE the separate step-6.5 block (the worker already does the drains). Keep step 6 (gateway) after it.

- [ ] **Step 4: Wire the pool in `BrokerNode`**

`libs/node/include/rune/node/BrokerNode.h`:
- add `#include <rune/node/WorkerPool.h>`
- declare `void buildWorkerPool();`
- add member AFTER `replicaRegistry_` and BEFORE `runtimeAgent_` (so it outlives the agent and is destroyed after it):
```cpp
    std::unique_ptr<WorkerPool> workerPool_;  // E1: shard->worker ownership
```

`libs/node/src/BrokerNode.cpp`:
- After `replicaRegistry_` is fully populated and before constructing `runtimeAgent_` (line ~997), build the pool:
```cpp
    buildWorkerPool();
```
- Implement `buildWorkerPool()` (place near `buildReplication`):
```cpp
void BrokerNode::buildWorkerPool() {
    const int32_t wc = config_.workerCount >= 1 ? config_.workerCount : 1;

    // Assign a stable negative group key to each distinct non-empty
    // coPartitionGroup, in sorted group-name order (matches StaticBalancer
    // determinism so every node derives identical shard identities).
    std::set<std::string> groups;
    for (const auto& ts : config_.topics)
        if (!ts.coPartitionGroup.empty()) groups.insert(ts.coPartitionGroup);
    std::unordered_map<std::string, int32_t> groupKey;
    int32_t next = -1;
    for (const auto& g : groups) groupKey[g] = next--;

    rune::metadata::ShardRouter router(wc);
    for (const auto& ts : config_.topics) {
        if (!ts.coPartitionGroup.empty())
            router.mapTopicToGroupKey(ts.topicId, groupKey[ts.coPartitionGroup]);
        // ungrouped topics fall back to topicId inside the router.
    }

    workerPool_ = std::make_unique<WorkerPool>(wc, std::move(router));
    if (replicaRegistry_) workerPool_->assignFromRegistry(*replicaRegistry_);
}
```
- Add `#include <set>` / `#include <unordered_map>` / `#include <string>` if not already present in the .cpp.
- Pass `workerPool_.get()` as the appended `NodeRuntimeAgent` argument:
```cpp
    runtimeAgent_ = std::make_unique<NodeRuntimeAgent>(
        innerAgent, election_.get(), replBridge_.get(), replTransport_.get(),
        replicaRegistry_.get(), gateway_.get(), groupCoordService_.get(),
        txnCoordService_.get(), metadataService_.get(),
        workerPool_.get());   // E1
```
- In `stop()`, reset `workerPool_` AFTER `runtimeAgent_.reset()` and BEFORE `replicaRegistry_.reset()` (the pool holds non-owning replica pointers; drop the pool before the registry frees the replicas):
```cpp
    runtimeAgent_.reset();
    workerPool_.reset();     // E1: holds non-owning replica ptrs; drop before registry
```
  (Place it in the existing reverse-order teardown sequence.)

> **Dynamic-reassignment compatibility (D rebalance):** the committed `AssignmentMap` changing is already applied by `ReplicationMetadataBridge` (it `add`s/`remove`s replicas in `replicaRegistry_` when the committed map changes — see `ReplicaRegistry::remove` doc + churn tests). So the set of replicas THIS node owns can change at runtime. To keep `WorkerPool` ownership in sync, re-run `assignFromRegistry` when the registry's replica set changes. Add to `NodeRuntimeAgent::doWork`, immediately before the step-5 `workerPool_->doWork`:
> ```cpp
> if (workerPool_ && registry_) {
>     // Re-derive shard->worker ownership when the registry's replica set
>     // changed (D dynamic rebalance add/remove). Cheap: compares count; a
>     // full re-derivation only runs on change. Deterministic + single-writer.
>     workerPool_->reconcile(*registry_);
> }
> ```
> Add `WorkerPool::reconcile(const ReplicaRegistry&)` (Task 3 addendum): store the last-seen `registry.replicaCount()`; if it differs from the current, call `assignFromRegistry`. This is a minimal, correct change detector for E1 (add/remove change the count; there is no in-place topic/partition mutation of an existing replica). Implement it in `WorkerPool.h`:
> ```cpp
>     void reconcile(const ReplicaRegistry& registry) {
>         const std::size_t n = registry.replicaCount();
>         if (n != lastReplicaCount_) { assignFromRegistry(registry); lastReplicaCount_ = n; }
>     }
> private:
>     std::size_t lastReplicaCount_ = static_cast<std::size_t>(-1);
> ```
> and set `lastReplicaCount_` inside `assignFromRegistry` too (so the first BrokerNode-time assign seeds it). Add a `reconcile` unit test to `WorkerPoolTest.cpp`: add a replica to the registry after the initial assign, call `reconcile`, assert the new replica is now owned by exactly one worker.

- [ ] **Step 5: Run to verify the delegated-path test + node build pass**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_pool_tests rune_node
/Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_worker_pool_tests
```
Expected: PASS (delegation + reconcile tests).

- [ ] **Step 6: Commit**

```bash
git add libs/node/include/rune/node/NodeRuntimeAgent.h \
        libs/node/include/rune/node/BrokerNode.h \
        libs/node/src/BrokerNode.cpp \
        libs/node/include/rune/node/WorkerPool.h \
        libs/node/tests/node/WorkerPoolTest.cpp
git commit -m "feat(E1): wire WorkerPool into BrokerNode; NodeRuntimeAgent delegates per-replica iteration

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: TSan shared-nothing proof — no worker touches another worker's shard state

**Files:**
- Modify: `libs/node/tests/node/WorkerPoolTest.cpp` (add a TSan-oriented concurrent-drive test)
- Modify: `libs/node/CMakeLists.txt` (ensure `rune_worker_pool_tests` builds under TSan — it does via `build-tsan`)

**Interfaces:**
- Consumes: `WorkerPool` (Task 3/5), `Worker::doWork`, `ReplicaRegistry`.
- Produces: a test that drives each `Worker::doWork` on a SEPARATE `std::thread` simultaneously and asserts (a) no crash, (b) TSan reports zero data races. This is the E1 shared-nothing proof: because each worker owns a DISJOINT replica set, concurrently running `Worker::doWork` must be race-free even though E1 production still drives them on one thread. (E2 makes them real threads; this test front-runs the guarantee.)

> Rationale: E1's production driver is single-threaded, but the SPEC's shared-nothing invariant is the whole point. Proving disjoint ownership is race-free under real concurrency (TSan) here de-risks E2 and directly satisfies EXIT criterion 2 ("no worker touches another worker's shard state").

- [ ] **Step 1: Write the failing/again test**

Add to `libs/node/tests/node/WorkerPoolTest.cpp`:

```cpp
#include <thread>

TEST(WorkerPool, ConcurrentDisjointWorkersAreRaceFree) {
    // Registry owns replicas across two topics / several partitions.
    ReplicaRegistry reg;
    static std::vector<std::unique_ptr<InMemoryLog>> logs;
    static std::vector<std::unique_ptr<InMemoryTransport>> trs;
    auto add = [&](int32_t topicId, int32_t partition) {
        logs.push_back(std::make_unique<InMemoryLog>());
        trs.push_back(std::make_unique<InMemoryTransport>());
        PartitionConfig cfg{};
        cfg.topicId = topicId; cfg.partition = partition;
        cfg.partitionId = static_cast<rune::replication::PartitionId>(partition);
        cfg.minIsr = 1;
        reg.add(topicId, partition,
                std::make_unique<PartitionReplica>(cfg, logs.back().get(), trs.back().get()));
    };
    for (int32_t t = 0; t < 2; ++t)
        for (int32_t p = 0; p < 4; ++p) add(t, p);

    ShardRouter router(4);
    WorkerPool pool(4, router);
    pool.assignFromRegistry(reg);

    // Drive every worker concurrently — disjoint ownership => race-free.
    std::vector<std::thread> ths;
    for (int32_t w = 0; w < pool.workerCount(); ++w) {
        ths.emplace_back([&pool, w] {
            for (int i = 0; i < 100; ++i) pool.worker(w).doWork(/*nowNs=*/i);
        });
    }
    for (auto& th : ths) th.join();
    SUCCEED();  // TSan is the real assertion: zero races reported.
}
```

- [ ] **Step 2: Run under TSan to verify (must be clean; if a race is reported it FAILS)**

```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_pool_tests
TSAN_OPTIONS=halt_on_error=1 \
  /Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_worker_pool_tests \
  --gtest_filter='WorkerPool.ConcurrentDisjointWorkersAreRaceFree'
```
Expected on first correct run: PASS with NO TSan warning. If TSan reports a race, it means ownership is not actually disjoint (or a replica shares state) — fix the router/assignment, do NOT suppress.

- [ ] **Step 3: (No new production code expected)**

If the test races, the defect is in Task 1/3 assignment (a shard mapped to two workers) or an unexpectedly shared `InMemoryLog`. Fix at the source; re-run.

- [ ] **Step 4: Confirm the whole pool suite is green under TSan**

```bash
/Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_worker_pool_tests
```
Expected: PASS (all tests, TSan clean).

- [ ] **Step 5: Commit**

```bash
git add libs/node/tests/node/WorkerPoolTest.cpp
git commit -m "test(E1): TSan shared-nothing proof — concurrent disjoint workers are race-free

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: EXIT — parity of all Milestone-D suites under N>1 workers + N-worker parity harness

**Files:**
- Create: `libs/node/tests/integration/WorkerPoolParityTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (register `rune_worker_pool_parity_tests`, gated behind `aeron_FOUND` like the other broker-node integration tests)

**Interfaces:**
- Consumes: `BrokerNode` (built from a `NodeConfig` with `workerCount > 1`), the existing integration-test harness patterns from `RebalanceFailoverIntegrationTest.cpp` / `MultiLeaderProduceTest.cpp` / `MetadataReplicationTest.cpp`.
- Produces: proof of EXIT criteria 1 (parity) + 3 (dynamic-reassignment compatibility) end-to-end through a real `BrokerNode` running via the `WorkerPool`.

- [ ] **Step 1: Write the parity test**

Create `libs/node/tests/integration/WorkerPoolParityTest.cpp`. It stands up a co-partitioned multi-topic single-node broker with `workerCount=4`, produces/fetches across partitions, and asserts identical observable results to the single-worker path. Reuse the produce/fetch helpers from `MultiLeaderProduceTest.cpp` (same fixture shape):

```cpp
// WorkerPoolParityTest.cpp — E1 EXIT: a broker running through the WorkerPool
// (N>1) is observationally identical to the single-agent path.
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

namespace {

rune::node::NodeConfig makeSingleNodeConfig(const std::string& dataDir,
                                            int32_t workerCount) {
    rune::node::NodeConfig cfg{};
    cfg.nodeId = 0;
    cfg.dataDir = dataDir;
    // One peer = self (single-node path).
    rune::node::PeerConfig self{};
    self.id = 0; self.host = "127.0.0.1";
    self.consensusPort = 0; self.clientPort = 0; self.replicationPort = 0;
    cfg.peers = {self};
    cfg.gatewayMinIsr = 1;
    cfg.workerCount = workerCount;
    // Two co-partitioned topics (group "grp", 4 partitions each) + a singleton.
    rune::node::TopicSpec a{}; a.name = "orders"; a.topicId = 0;
    a.partitionCount = 4; a.coPartitionGroup = "grp"; a.partitionKey = "user";
    rune::node::TopicSpec b{}; b.name = "shipments"; b.topicId = 1;
    b.partitionCount = 4; b.coPartitionGroup = "grp"; b.partitionKey = "user";
    rune::node::TopicSpec c{}; c.name = "chat"; c.topicId = 2;
    c.partitionCount = 2; c.coPartitionGroup = ""; c.partitionKey = "";
    cfg.topics = {a, b, c};
    return cfg;
}

} // namespace

TEST(WorkerPoolParity, MultiWorkerBrokerReachesLeaderAndServes) {
    // A single-node broker configured with 4 workers must reach leadership and
    // serve exactly as the 1-worker path does. (Uses the same awaitLeader gate
    // the existing broker-node integration tests use.)
    auto tmp = std::filesystem::temp_directory_path() / "rune_e1_parity_multi";
    std::filesystem::remove_all(tmp);
    std::filesystem::create_directories(tmp);

    rune::node::BrokerNode node(makeSingleNodeConfig(tmp.string(), /*workers=*/4));
    node.start();
    EXPECT_TRUE(node.awaitLeader(5s)) << "multi-worker node must reach leadership";

    // Co-partition affinity: same partition index across group members must be
    // owned by the SAME worker (aggregate-root affinity). Assert via the router
    // the node built (exposed for test through metadataServiceForTest path or a
    // dedicated test accessor — see Step 3 note).
    node.stop();
    std::filesystem::remove_all(tmp);
}

TEST(WorkerPoolParity, DynamicReassignmentReconcilesOwnership) {
    // Placeholder shape: drive the same rebalance sequence the D
    // RebalanceFailoverIntegrationTest uses, but with workerCount=4, and assert
    // the node stays healthy (serves, no committed loss) across the reassignment
    // — the WorkerPool::reconcile path re-derives ownership when the committed
    // map changes replicas. Copy the exact rebalance driver from
    // RebalanceFailoverIntegrationTest.cpp (do not re-invent it).
    SUCCEED() << "See Step 3: port the D rebalance driver verbatim with workers=4";
}
```

> This file's two tests are the E1-specific harness. The BULK of the parity proof is running the EXISTING D suites unchanged with `RUNE_WORKERS>1` (Step 4) — that is the real EXIT gate; this file adds the co-group-affinity + reconcile assertions the D suites do not directly cover.

- [ ] **Step 2: Register the target and run it to verify it fails**

Add to `libs/node/CMakeLists.txt` (inside the existing `if(aeron_FOUND)` block, mirroring `rune_group_push_delivery_tests`):

```cmake
    if(aeron_FOUND)
        add_executable(rune_worker_pool_parity_tests
            tests/integration/WorkerPoolParityTest.cpp)
        target_link_libraries(rune_worker_pool_parity_tests PRIVATE
            rune_node
            rune_replication
            rune_metadata
            GTest::gtest_main)
        gtest_discover_tests(rune_worker_pool_parity_tests
            PROPERTIES TIMEOUT 120)
    endif()
```

Run:
```bash
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan --target rune_worker_pool_parity_tests
/Users/vradu/Projects/rune-broker/build-tsan/libs/node/rune_worker_pool_parity_tests
```
Expected: FAIL first (missing a test accessor for the router, or an assertion) — then iterate to green.

- [ ] **Step 3: Add the minimal test accessor + port the D rebalance driver**

- Add a test-only accessor to `BrokerNode.h` mirroring the existing `*ForTest` accessors:
  ```cpp
  /// Test-only: worker id owning (topicId, partition) per the WorkerPool router.
  /// -1 if the pool is not built. NOT concurrency-safe; call between doWork calls
  /// or when stopped.
  [[nodiscard]] int32_t workerForPartitionForTest(int32_t topicId, int32_t partition) const noexcept {
      return workerPool_ ? workerPool_->ownerWorkerId(topicId, partition) : -1;
  }
  ```
  Use it in `MultiWorkerBrokerReachesLeaderAndServes` to assert co-group affinity:
  ```cpp
  for (int32_t idx = 0; idx < 4; ++idx)
      EXPECT_EQ(node.workerForPartitionForTest(0, idx),
                node.workerForPartitionForTest(1, idx));
  ```
- Replace the `DynamicReassignmentReconcilesOwnership` placeholder body by copying the rebalance/failover driver from `libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp` VERBATIM (same cluster setup, same kill/rebalance steps) but constructing each node's `NodeConfig` with `workerCount = 4`. Assert the same post-conditions that test asserts (no committed loss, single leader, serves after rebalance).

- [ ] **Step 4: Run ALL Milestone-D suites under N>1 workers (the real EXIT gate)**

Configure the D suites to run with 4 workers by exporting `RUNE_WORKERS=4` (BrokerNode reads it via `NodeConfig`; suites that build `NodeConfig` directly instead of via env must set `cfg.workerCount = 4` — for those, add a one-line env-or-default read, or run them through the env path). Run the full D-relevant set and confirm each is green:

```bash
CMAKE=/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake
"$CMAKE" --build /Users/vradu/Projects/rune-broker/build-tsan
BIN=/Users/vradu/Projects/rune-broker/build-tsan
export RUNE_WORKERS=4
for t in \
  libs/metadata/rune_metadata_state_machine_tests \
  libs/metadata/rune_static_balancer_tests \
  libs/metadata/rune_dynamic_balancer_tests \
  libs/metadata/rune_routing_image_tests \
  libs/node/rune_bridge_per_partition_tests \
  libs/node/rune_bridge_churn_tests \
  ; do
  echo "== $t =="; "$BIN/$t" || { echo "FAILED: $t"; exit 1; }
done
# Aeron-gated broker/integration suites (parity + failover under 4 workers):
for t in \
  libs/node/rune_worker_pool_parity_tests \
  # + the aeron_FOUND integration suites present in the build:
  #   MetadataReplicationTest, RebalanceFailoverIntegrationTest,
  #   MultiLeaderProduceTest, StaticAssignmentEmitTest, GatewayProduceReplicationTest,
  #   GatewayFailoverReplicationTest, OffsetReplicationTest, TxnStateReplicationTest
  ; do
  echo "== $t =="; "$BIN/$t" || { echo "FAILED: $t"; exit 1; }
done
unset RUNE_WORKERS
```
Expected: ALL green under `RUNE_WORKERS=4` (parity — the refactor changed nothing observable). Then re-run with `RUNE_WORKERS` unset (workerCount=1, pre-E1 path) and confirm still green (both paths correct).

> If any D suite constructs `NodeConfig` in-process (not via env), it will run at `workerCount=1` unless updated. For the EXIT proof, ensure at least the failover + multi-leader + metadata-replication suites run with `workerCount=4` (edit their config builders to `cfg.workerCount = 4`, or route them through the env). Document in the commit which suites were exercised at N=4.

- [ ] **Step 5: Commit**

```bash
git add libs/node/tests/integration/WorkerPoolParityTest.cpp \
        libs/node/include/rune/node/BrokerNode.h \
        libs/node/CMakeLists.txt
git commit -m "test(E1): EXIT — D-suite parity under N=4 workers + co-group affinity + reconcile

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage (E1 slice):**
- Core principle 2 (ownership = co-partition shard) → Task 1 `ShardRouter` (group-key affinity) + Task 5 config grouping + Task 7 co-group-affinity assertion. ✅
- Core principle 3 (two-level assignment, shard→worker = `hash % workerCount`) → Task 1. ✅
- Component `ShardRouter` → Task 1; `Worker` → Task 2; `WorkerPool` → Task 3; `Shard` (as `(groupKey, partitionIndex)` identity, no standalone class in E1 — the shard is represented by `ShardId` + the worker's owned replica set, which is the minimal correct E1 form) → Task 1/2. ✅
- "Migration from single-AgentRunner" step 1 (wrap existing per-partition doWork, N in-process workers, no affinity, prove parity) → Tasks 2/5/7. ✅
- EXIT 1 (parity, D suites under N workers) → Task 7 Step 4. ✅
- EXIT 2 (shared-nothing single-owner proven) → Task 3 (totality) + Task 6 (TSan concurrent disjoint). ✅
- EXIT 3 (dynamic-reassignment compatibility) → Task 5 `reconcile` + Task 7 `DynamicReassignmentReconcilesOwnership`. ✅
- Explicit non-goals (threads/ingress/io_uring/affinity/InMemoryLog) → stated in Global Constraints; not implemented. ✅

**2. Placeholder scan:** The two intentionally-illustrative spots are flagged inline with explicit "DELETE this line" / "port verbatim from RebalanceFailoverIntegrationTest.cpp" instructions (the `reg.add(..., nullptr)` pitfall demo and the rebalance-driver copy) — they are directives, not vague TODOs. No "add error handling"/"TBD"/"similar to Task N" placeholders remain.

**3. Type consistency:** `ShardId`/`shardIdOf`/`workerIdForShard`/`ShardRouter` names match across Tasks 1/3/5. `Worker::ownReplica/clearOwned/owns/doWork` consistent Tasks 2/3/5/6. `WorkerPool::assignFromRegistry/reconcile/ownerWorkerId/worker/doWork/workerCount` consistent Tasks 3/5/6/7. `NodeConfig::workerCount` consistent Tasks 4/5/7. `NodeRuntimeAgent` appended `WorkerPool*` param consistent Tasks 5/7.
