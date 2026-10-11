# Rune Phase 2b — Part E2: Transient Topics Data Path (produce + subscribe) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A transient topic (`PersistenceMode::Transient`, `InMemoryLog`) is fully usable: produce in every ack mode (acks=all replicated to the ISR before the ack), push subscriptions from earliest / latest / an offset, consumer groups and read_committed, in BOTH worker modes (shard = default, legacy), with zero steady-state allocation and disk-class latency — plus a defined rule for a full ring.

**Architecture:** Nothing new on the wire. The worker — the single owner of a partition's log — gets ONE extra read source and ONE extra append target: where a disk partition uses its `Log*`, a transient partition uses its replica's `InMemoryLog*` (zero-copy `readView` into the ring for reads; `handleProduceBatch` templated over the log type, with a new `InMemoryLog::appendNoSyncRestamp` that copies the batch once into the ring and patches base offset + leader epoch there). The gateway stops treating "no disk `Log`" as "no partition": a `servedPartition_` helper recognises transient partitions and routes them to the owning worker (never touching the ring cross-thread). The ring IS the topic's retention: it evicts the oldest batches only below the replica's high watermark (an "eviction floor"); a ring full of uncommitted data refuses the append (retriable `NotLeader`, nothing appended); a reader whose cursor was evicted resumes at the ring start (counted); a follower below the leader's ring start restarts its ring there.

**Tech Stack:** C++23, GoogleTest, Google Benchmark, CMake/Ninja (`build-fast`), real Aeron UDP for the in-process BrokerNode integration tests, docker e2e (single node).

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` — section **E. Transient topics** ("replicated to a majority before ack", "produce + subscribe work", "restart → topic exists and is empty") and "Global constraints". The data path itself is the user decision recorded in Part E's plan (`docs/superpowers/plans/2026-10-11-rune-phase2b-e-transient-topics.md`, Spec question 1): *transient topics must fully work — produce (all ack modes), push subscribe from earliest/latest/offset, consumer groups, read-committed, in both shard and legacy worker modes — zero steady-state allocation, same latency class as disk.*

**Builds on / order:** E2 executes **after Part E** (it uses E's names: `PersistenceMode::Transient` in `libs/node/include/rune/node/PersistenceMode.h`, `TopicSpec.persistence`, `DefineTopic` persistence byte, `Client::defineTopicAndWait(..., Persistence::Transient, ...)`). E's **Task 10 (e2e) was written BLOCKED on this part — it is executed here as Task 8** (code reproduced in full). Line numbers below are from `feature/materialized-views` (broker HEAD `7a3fdf7`); Part E shifts some of them — every edit is anchored by quoted code, use the anchor, not the number.

## Execution context

- **Repos and branches.** E2 touches the broker and (Task 8 only) the client. Cut from Part E's branch (or from the integration branch once E is merged into it):
  ```bash
  git -C /Users/vradu/Projects/rune-broker-matviews switch -c feat/phase2b-e2-transient-data-path feat/phase2b-e-transient-topics
  git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp switch -c feat/phase2b-e2-transient-data-path feat/phase2b-e-transient-topics
  ```
- **Builds (canonical sibling layout).** Broker `build-fast` (Ninja), configured once exactly as Part E does it:
  ```bash
  cd /Users/vradu/Projects/rune-broker-matviews
  cmake -S . -B build-fast -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DBUILD_BENCHMARKS=ON \
    -DCMAKE_TOOLCHAIN_FILE=/Users/vradu/Projects/rune-broker/build/Debug/generators/conan_toolchain.cmake \
    -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/se-worktrees/rune-protocol/include \
    -DRUNE_CODEC_ROOT=/Users/vradu/Projects/se-worktrees/rune-codec
  ```
  Client: existing build dir `build` (`cmake --build build -j --target view_probe`).
- **Regression gate before EVERY commit (CLAUDE.md #7), every repo the commit touches:**
  - broker: `ctest --test-dir build-fast -R Regression --output-on-failure -j8`
  - client: `ctest --test-dir build -R Regression --output-on-failure -j8`
  then the touched targets' full `ctest -R` filters given in each task.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- HARD RULES (CLAUDE.md): shared-nothing (the gateway NEVER reads an `InMemoryLog`; only the owning worker does); ring buffers are the only cross-thread passing (no new channel is needed: produce/read/marker ops already ride the op/return rings); state machines where each state holds only its variables; zero heap allocation on hot paths / in steady state; test + benchmark everything; a `*Regression*`-named test on every fix; full `*Regression*` suite first on every change; fix-on-sight.
- User decision (verbatim intent): transient topics **fully work** — produce in all ack modes incl. **acks=all replicated to a majority (the ISR) before the ack**; push subscribe from **earliest / latest / offset**; **consumer groups**; **read_committed**; in **both shard (default) and legacy** worker modes; **zero steady-state allocation**; **same latency class as disk topics**.
- Spec E: "replicated to a majority before ack, never written to disk"; "metadata survives, data does not … after a full-cluster restart a transient topic is empty".
- **Full-ring rule (this plan's decision, Spec question 1):** the ring is the transient topic's retention (`RUNE_MEMORY_LOG_RING_BYTES` per partition, default 8 MiB, ≤ 1 000 000 batches). It evicts oldest-first **only below the replica's high watermark**. An append that would have to evict a batch at/above the HW is **refused** — retriable `NotLeader` (Spec question 2), nothing appended. A batch larger than the whole ring is refused `MessageTooLarge` (non-retriable). A reader (subscription / PushRead / view) whose cursor is below the ring start resumes **at** the ring start (counted, rate-limited log) — never a stall. A follower whose fetch position is below the leader's ring start restarts its ring **empty at** the leader's ring start.
- No wire change: no new error code, no new message field. `FetchResponse.logStartOffset` (an existing field, always 0 today) carries the leader's ring start for transient partitions only; disk partitions keep sending 0.
- Disk partitions: byte-for-byte the same behaviour and the same hot path (the transient branch is per call, one predicted pointer test; the disk path's calls are unchanged).
- A transient topic configured `tier=strict` acks like `majority` (there is no fsync to wait for) — documented, Spec question 3.

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **A consumer slower than the ring must keep consuming** (resume at the ring start), never stall on an evicted offset → `TransientDataPathRegression.PushReadBelowRingStartResumesAtRingStartRegression` (Task 4).
2. **A full ring under a slow follower must not lose not-yet-replicated data** — it refuses (retriable) instead of evicting above the HW, and accepts again once the follower catches up → `TransientDataPathRegression.FullRingRefusesRetriablyAndNeverEvictsUncommittedRegression` (Task 3) and `TransientReplicaRegression.LeaderRingNeverEvictsAboveTheHighWatermarkRegression` (Task 2).
3. **A restarted (empty) node must catch up a transient partition whose leader already evicted offset 0** → `TransientReplicaRegression.FollowerBehindTheLeaderRingStartResetsAndCatchesUpRegression` (Task 2).
4. **A deferred/held read must never read a ring after its partition moved to another worker** (a cross-thread read of a single-owner ring) → `TransientDataPathRegression.DeferredPushReadAfterOwnershipMovedNeverReadsTheRingRegression` (Task 4).
5. **`From::latest()` on a transient topic must not replay old records; an explicit offset beyond the HW clamps to it** → `TransientGatewayRegression.SubscribeLatestAndExplicitOffsetClampToTheHighWatermarkRegression` (Task 6) and `TransientTopicDataPathRegression.ShardProduceAndSubscribeEarliestLatestOffsetRegression` (Task 7).

Also pinned: read_committed withholds an open transaction until its commit marker (`TransientDataPathRegression.ReadCommittedHoldsTheOpenTransactionUntilItsCommitRegression`, Task 4); steady-state produce + evict + read never allocates (`TransientDataPathRegression.SteadyStateProduceEvictAndPushReadNeverAllocateRegression`, Task 5); a replicated apply never allocates (`ZeroAllocReplicaRegression.TransientFollowerApplyAllocatesNothing`, Task 2 — fixes a hot-path `std::vector` found on sight).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `libs/storage/include/rune/storage/InMemoryLog.h` | `appendNoSyncRestamp`, leader-epoch stamp, eviction floor, refusal codes, `resetTo`, counters | 1 |
| `libs/storage/include/rune/storage/ProducePath.h`, `libs/storage/src/ProducePath.cpp` | `handleProduceBatch` templated over the log type; `ProduceOutcome.logFull` | 1 |
| `libs/storage/tests/storage/InMemoryLogTransientTest.cpp` (new), `libs/storage/CMakeLists.txt` | storage tests | 1 |
| `libs/replication/include/rune/replication/PartitionReplica.h`, `libs/replication/src/PartitionReplica.cpp` | epoch stamp on the ring, HW eviction floor, refusals, follower ring reset, fetch answer below the ring start, apply zero-alloc fix | 2 |
| `libs/replication/tests/replication/TransientReplicaTest.cpp` (new), `ZeroAllocReplicaTest.cpp`, `libs/replication/CMakeLists.txt` | replication tests | 2 |
| `libs/node/include/rune/node/Worker.h` | produce core over the ring, marker back-pressure mapping, push reads / owned-subscription pushes from the ring, evicted-cursor skip, counters | 3, 4 |
| `libs/node/tests/integration/TransientRig.h` (new) | one-worker transient rig (shared) | 3 |
| `libs/node/tests/integration/TransientDataPathTest.cpp` (new), `TransientDataPathAllocTest.cpp` (new), `libs/node/CMakeLists.txt` | worker regressions + allocation regression | 3, 4, 5 |
| `benchmark/WorkerProduceLatencyBench.cpp` | produce→PushRead latency, transient vs disk | 5 |
| `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp` | `servedPartition_`; produce / subscribe / push / group / marker sites | 6 |
| `libs/gateway/tests/gateway/TransientGatewayTest.cpp` (new), `libs/gateway/CMakeLists.txt` | gateway regressions (legacy worker-pool mode) | 6 |
| `libs/node/tests/integration/TransientTopicDataPathTest.cpp` (new), `libs/node/CMakeLists.txt` | BrokerNode over real Aeron: shard, legacy, 3-node acks=all, restart | 7 |
| `docs/SELF-HOSTING.md` | data-path + full-ring rule | 7 |
| `rune-client-cpp/examples/view_probe.cpp`, `docker/view-engine-e2e.sh` | e2e transient leg (Part E Task 10, executed here) | 8 |

---

### Task 1: Storage — `InMemoryLog` data-path surface + `handleProduceBatch` over either log

**Files:**
- Modify: `libs/storage/include/rune/storage/InMemoryLog.h` (header comment, includes, public surface, `appendImpl`, `evictOldest`, fields)
- Modify: `libs/storage/include/rune/storage/ProducePath.h:73-110` (`ProduceOutcome`), `:132-174` (declaration)
- Modify: `libs/storage/src/ProducePath.cpp:29-44` (definition head), `:270-292` (append step), end of file (instantiations)
- Create: `libs/storage/tests/storage/InMemoryLogTransientTest.cpp`
- Modify: `libs/storage/CMakeLists.txt:23-24` (`rune_in_memory_log_tests` sources)

**Interfaces:**
- Produces (namespace `rune::storage`):
  ```cpp
  class InMemoryLog {
    static constexpr int64_t kRefusedFull     = -1;   // would evict at/above the eviction floor (or BackPressure policy full)
    static constexpr int64_t kRefusedTooLarge = -2;   // batch larger than the whole ring
    int64_t appendNoSyncRestamp(const uint8_t* batch, std::size_t len, int32_t lastOffsetDelta, int64_t& outBaseOffset);
    void    setLeaderEpochStamp(int32_t epoch) noexcept;   [[nodiscard]] int32_t leaderEpochStamp() const noexcept;
    void    setEvictionFloor(int64_t offset) noexcept;     [[nodiscard]] int64_t evictionFloor() const noexcept;
    void    resetTo(int64_t offset) noexcept;
    [[nodiscard]] constexpr bool storageFailed() const noexcept;  void markStorageFailed(int) noexcept;
    [[nodiscard]] uint64_t fullRefusals() const noexcept;  [[nodiscard]] uint64_t evictedRecords() const noexcept;
  };
  struct ProduceOutcome { /* … */ bool logFull = false; };
  template <class LogT> ProduceOutcome handleProduceBatch(/* same params */, LogT& log, /* … */) noexcept;  // Log, InMemoryLog
  ```
  `appendNoSyncRestamp` returns `nextOffset` (= base + delta + 1) or a negative refusal code; the default eviction floor is `INT64_MAX` (unbounded — today's `DropOldest` behaviour for every existing caller).

- [ ] **Step 1: Write the failing tests** — create `libs/storage/tests/storage/InMemoryLogTransientTest.cpp`:

```cpp
// InMemoryLogTransientTest.cpp — Phase 2b E2: the InMemoryLog surface a
// TRANSIENT topic's data path needs: the zero-copy restamp append (base offset
// + leader epoch patched in the ring copy), the eviction floor (never evict
// uncommitted data — refuse instead), the refusal codes, resetTo (follower
// catch-up below the leader's ring start), and handleProduceBatch over the ring.
#include <gtest/gtest.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <cstdint>
#include <string>
#include <vector>

using rune::storage::InMemoryLog;
namespace pc = rune::protocol::client;

namespace {
std::vector<uint8_t> batchOf(int records, std::size_t valueBytes = 100) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < records; ++i) b.append("k", v, 1000 + i);
    return b.build();
}
int32_t deltaOf(const std::vector<uint8_t>& b) {
    return rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
}
} // namespace

TEST(InMemoryLogTransientRegression, RestampAppendPatchesBaseOffsetAndEpochInTheRingCopyOnly) {
    InMemoryLog log(1 << 20, 1024);
    log.setLeaderEpochStamp(3);
    const auto a = batchOf(2), b = batchOf(3);
    int64_t base = -1;
    ASSERT_EQ(log.appendNoSyncRestamp(a.data(), a.size(), deltaOf(a), base), 2);
    EXPECT_EQ(base, 0);
    const std::vector<uint8_t> bBefore = b;
    ASSERT_EQ(log.appendNoSyncRestamp(b.data(), b.size(), deltaOf(b), base), 5);
    EXPECT_EQ(base, 2);
    EXPECT_EQ(b, bBefore) << "the caller's batch is never mutated (it may alias a read-only fragment)";
    const auto v = log.readView(2, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 2);
    EXPECT_EQ(r.partitionLeaderEpoch(), 3) << "the leader stamps its epoch (Kafka parity; KIP-320 lineage)";
    EXPECT_TRUE(r.ensureValid()) << "base offset and leader epoch are outside the CRC";
}

TEST(InMemoryLogTransientRegression, EvictionFloorRefusesInsteadOfEvictingUncommittedData) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);   // room for exactly 4 batches
    log.setEvictionFloor(0);                // nothing committed yet
    int64_t base = -1;
    for (int i = 0; i < 4; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0) << "an uncommitted batch was evicted";
    EXPECT_EQ(log.logEndOffset(), 4);
    EXPECT_EQ(log.fullRefusals(), 1u);
    log.setEvictionFloor(2);                // offsets 0 and 1 are committed (HW = 2)
    ASSERT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 5);
    EXPECT_EQ(log.logStartOffset(), 1) << "exactly one committed batch made room";
    EXPECT_EQ(log.evictedRecords(), 1u);
}

TEST(InMemoryLogTransientRegression, RecordCapEvictionHonoursTheFloorToo) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 20, /*maxRecords=*/2);
    log.setEvictionFloor(0);
    int64_t base = -1;
    ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0);
}

TEST(InMemoryLogTransient, DefaultFloorKeepsDropOldestBehaviour) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);
    int64_t base = -1;
    for (int i = 0; i < 10; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.logEndOffset(), 10);
    EXPECT_EQ(log.logStartOffset(), 6) << "4 batches retained, oldest evicted (unbounded floor)";
    EXPECT_EQ(log.fullRefusals(), 0u);
    EXPECT_EQ(log.evictedRecords(), 6u);
}

TEST(InMemoryLogTransientRegression, BatchLargerThanTheRingIsRefusedTooLarge) {
    const auto big = batchOf(1, 4096);
    InMemoryLog log(1024, 16);
    int64_t base = -1;
    EXPECT_EQ(log.appendNoSyncRestamp(big.data(), big.size(), 0, base), InMemoryLog::kRefusedTooLarge);
    EXPECT_EQ(log.logEndOffset(), 0);
    EXPECT_EQ(log.appendNoSync(big.data(), big.size(), 0), InMemoryLog::kRefusedTooLarge)
        << "the plain append refuses too (was an assert)";
}

TEST(InMemoryLogTransient, ResetToRestartsTheRingEmptyAtAnOffset) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 16, 64);
    int64_t base = -1;
    for (int i = 0; i < 3; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    log.resetTo(42);
    EXPECT_EQ(log.logStartOffset(), 42);
    EXPECT_EQ(log.logEndOffset(), 42);
    EXPECT_TRUE(log.readView(42, 1 << 16).empty());
    ASSERT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 43);
    EXPECT_EQ(base, 42);
    const auto v = log.readView(42, 1 << 16);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(rune::protocol::RecordBatchReader(v.data, v.len).baseOffset(), 42);
}

TEST(InMemoryLogTransientRegression, ProducePathOverTheRingAppendsAndRefusesFullAsRetriable) {
    const auto x = batchOf(1);
    InMemoryLog log(2 * x.size(), 1024);
    log.setEvictionFloor(0);
    rune::storage::ProducerStateManager psm;
    auto produce = [&](InMemoryLog& l) {
        return rune::storage::handleProduceBatch(x.data(), x.size(), /*topicId=*/1, /*partition=*/0,
                                                 /*producerId=*/-1, /*epoch=*/0, /*baseSeq=*/-1, l, psm,
                                                 /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1, /*tier=*/1);
    };
    const auto o1 = produce(log);
    ASSERT_TRUE(o1.isOK());
    EXPECT_EQ(o1.baseOffset, 0);
    ASSERT_TRUE(produce(log).isOK());
    const auto full = produce(log);
    EXPECT_EQ(full.error, pc::ErrorCode::NotLeader) << "retriable: nothing was appended";
    EXPECT_TRUE(full.logFull);
    EXPECT_EQ(log.logEndOffset(), 2);

    InMemoryLog tiny(64, 4);
    const auto tooLarge = produce(tiny);
    EXPECT_EQ(tooLarge.error, pc::ErrorCode::MessageTooLarge) << "a batch larger than the ring can never fit";
    EXPECT_TRUE(tooLarge.logFull);
}
```

In `libs/storage/CMakeLists.txt` change

```cmake
    add_executable(rune_in_memory_log_tests tests/storage/InMemoryLogTest.cpp
        tests/storage/InMemoryLogReadViewTest.cpp)
```
to
```cmake
    add_executable(rune_in_memory_log_tests tests/storage/InMemoryLogTest.cpp
        tests/storage/InMemoryLogReadViewTest.cpp
        tests/storage/InMemoryLogTransientTest.cpp)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_in_memory_log_tests`
Expected: compile errors `no member named 'setLeaderEpochStamp' in 'rune::storage::InMemoryLog'`, `no member named 'appendNoSyncRestamp'`, `no member named 'logFull' in 'rune::storage::ProduceOutcome'`.

- [ ] **Step 3: Implement — `InMemoryLog.h`**

Header comment: after the paragraph that ends `// NOT thread-safe (same contract as rune::storage::Log — single writer).` insert:

```cpp
//
// Phase 2b E2 (TRANSIENT topics — the produce/subscribe data path):
//   appendNoSyncRestamp(): the Log-compatible zero-copy restamp append used by
//     the worker produce path (handleProduceBatch<InMemoryLog>): ONE memcpy into
//     the ring, then the assigned base offset and (when leading) the leader epoch
//     are patched in the ring copy — never in the caller's bytes.
//   Eviction floor: DropOldest never evicts a batch whose lastOffset >= the
//     floor (the owning replica keeps it at its high watermark: uncommitted
//     data).  An append that would need such an eviction is REFUSED
//     (kRefusedFull).  Default floor = INT64_MAX (unbounded: plain DropOldest).
//   kRefusedTooLarge: a batch larger than the whole ring (never storable).
//   resetTo(): restart EMPTY at an offset (follower below the leader's ring start).
```

Includes: after `#include <cstring>` add `#include <limits>`; after `#include <rune/storage/LogReadView.h>` add `#include <rune/protocol/RecordBatch.h>`.

In the public section, right after

```cpp
    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) {
        return appendImpl(data, len, lastOffset);
    }
```
insert:

```cpp
    // -----------------------------------------------------------------------
    // Phase 2b E2 — the transient produce path.
    // -----------------------------------------------------------------------
    /// The append would have to evict a batch at/above the eviction floor (or the
    /// BackPressure policy's ring is full): nothing appended, retry later.
    static constexpr int64_t kRefusedFull     = -1;
    /// The batch is larger than the whole ring: it can never be stored.
    static constexpr int64_t kRefusedTooLarge = -2;

    /// Log::appendNoSyncRestamp parity: assigns base = logEndOffset(), copies
    /// [batch, batch+len) into the ring ONCE and patches the base offset
    /// (bytes [0..7]) and — when leaderEpochStamp() >= 0 — the partition leader
    /// epoch (bytes [12..15], outside the CRC) in the RING COPY; `batch` is never
    /// mutated.  Returns nextOffset (base + lastOffsetDelta + 1) and sets
    /// outBaseOffset, or a negative refusal code (kRefusedFull / kRefusedTooLarge)
    /// with nothing appended.  Zero allocation.
    int64_t appendNoSyncRestamp(const uint8_t* batch, std::size_t len,
                                int32_t lastOffsetDelta, int64_t& outBaseOffset) {
        const int64_t base = logEndOffset_;
        const int64_t r = appendImpl(batch, len, base + static_cast<int64_t>(lastOffsetDelta));
        if (r < 0) return r;
        uint8_t* dst = ring_.data() + backEntry().ringOffset;
        const auto ub = static_cast<uint64_t>(base);
        for (int i = 0; i < 8; ++i)
            dst[rune::protocol::batch::kBaseOffsetOffset + i] = static_cast<uint8_t>(ub >> (8 * i));
        if (leaderEpochStamp_ >= 0 &&
            len >= static_cast<std::size_t>(rune::protocol::batch::kPartitionLeaderEpochOffset + 4)) {
            const auto ue = static_cast<uint32_t>(leaderEpochStamp_);
            for (int i = 0; i < 4; ++i)
                dst[rune::protocol::batch::kPartitionLeaderEpochOffset + i] = static_cast<uint8_t>(ue >> (8 * i));
        }
        outBaseOffset = base;
        return logEndOffset_;
    }

    /// Log::setLeaderEpochStamp parity: the epoch appendNoSyncRestamp writes into
    /// every batch it stores (-1 = leave the producer's bytes).  Set by the owning
    /// PartitionReplica on leadership changes.  Owner thread only.
    void setLeaderEpochStamp(int32_t epoch) noexcept { leaderEpochStamp_ = epoch; }
    [[nodiscard]] int32_t leaderEpochStamp() const noexcept { return leaderEpochStamp_; }

    /// Eviction floor: DropOldest never evicts a batch whose lastOffset >= floor.
    /// The owning replica sets it to its high watermark before each append.
    void setEvictionFloor(int64_t offset) noexcept { evictionFloor_ = offset; }
    [[nodiscard]] int64_t evictionFloor() const noexcept { return evictionFloor_; }
```

Right before `[[nodiscard]] bool isDiskBacked() const noexcept { return false; }` insert:

```cpp
    /// Phase 2b E2 (follower catch-up): drop every batch and restart the ring
    /// EMPTY at `offset` (logStart = logEnd = offset).  Keeps the eviction floor
    /// and the leader-epoch stamp.  Cold.
    void resetTo(int64_t offset) noexcept {
        entryHead_ = 0; entryCount_ = 0;
        head_ = 0; tail_ = 0; bytesUsed_ = 0;
        logStartOffset_ = offset;
        logEndOffset_   = offset;
    }

    /// Log parity for handleProduceBatch<LogT>: a ring never fails storage.
    [[nodiscard]] constexpr bool storageFailed() const noexcept { return false; }
    void markStorageFailed(int /*err*/) noexcept {}

    /// Appends refused because the ring would have had to evict at/above the floor.
    [[nodiscard]] uint64_t fullRefusals() const noexcept { return fullRefusals_; }
    /// Records evicted (retention), summed over evicted batches.
    [[nodiscard]] uint64_t evictedRecords() const noexcept { return evictedRecords_; }
```

In `appendImpl`, replace

```cpp
        assert(len <= maxBytes_ && "batch larger than ring capacity — cannot store");

        const int64_t baseOffset = logEndOffset_;
```
with
```cpp
        // Phase 2b E2: a batch larger than the whole ring can never be stored
        // (was an assert — a release build corrupted the ring).
        if (len > maxBytes_) [[unlikely]] return kRefusedTooLarge;

        const int64_t baseOffset = logEndOffset_;
```
and inside the `DropOldest` loop replace
```cpp
                if (bytesUsed_ + need <= maxBytes_ && entryCount_ < maxRecords_)
                    break; // fits — stop evicting
                evictOldest();
```
with
```cpp
                if (bytesUsed_ + need <= maxBytes_ && entryCount_ < maxRecords_)
                    break; // fits — stop evicting
                // Phase 2b E2: never evict at/above the floor (uncommitted data
                // of a transient partition) — refuse the append instead.
                if (frontEntry().lastOffset >= evictionFloor_) [[unlikely]] {
                    ++fullRefusals_;
                    return kRefusedFull;
                }
                evictOldest();
```
and in the `BackPressure` branch replace
```cpp
                return -1; // sentinel: ring full, caller must retry later
```
with
```cpp
                ++fullRefusals_;
                return kRefusedFull; // sentinel: ring full, caller must retry later
```

In `evictOldest()`, right after `const Entry& oldest = frontEntry();` add:
```cpp
        evictedRecords_ += static_cast<uint64_t>(oldest.lastOffset + 1 - oldest.baseOffset);
```

Fields: after `int64_t               logEndOffset_   = 0;` add:
```cpp
    int64_t               evictionFloor_  = std::numeric_limits<int64_t>::max();   ///< E2: see setEvictionFloor
    int32_t               leaderEpochStamp_ = -1;                                   ///< E2: see setLeaderEpochStamp
    uint64_t              fullRefusals_   = 0;
    uint64_t              evictedRecords_ = 0;
```

- [ ] **Step 4: Implement — `ProducePath.h` / `ProducePath.cpp`**

`ProducePath.h`: add `#include <rune/storage/InMemoryLog.h>` after `#include <rune/storage/Log.h>`. In `struct ProduceOutcome`, after `int64_t  logStartOffset = -1;` add:

```cpp
    /// Phase 2b E2: the append was refused by an InMemoryLog (a TRANSIENT
    /// partition's ring): error NotLeader (retriable — the ring is full of
    /// uncommitted data) or MessageTooLarge (larger than the ring).  Nothing
    /// was appended.  Never set for a disk Log.
    bool     logFull = false;
```

Replace the declaration (from `ProduceOutcome handleProduceBatch(` through `    bool                              crcPreverified = false) noexcept;`) with:

```cpp
// Phase 2b E2: templated over the concrete log type — rune::storage::Log (disk)
// or rune::storage::InMemoryLog (a TRANSIENT partition's ring).  No virtual
// dispatch; the two instantiations live in ProducePath.cpp.
template <class LogT>
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
    std::size_t                       batchLen,
    int32_t                           topicId,
    int32_t                           partition,
    int64_t                           producerId,
    int16_t                           producerEpoch,
    int32_t                           baseSequence,
    LogT&                             log,
    rune::storage::ProducerStateManager& producerState,
    bool                              acksAll,
    int                               isrSize,
    int                               minIsr,
    uint8_t                           durabilityTier,
    bool                              crcPreverified = false) noexcept;

extern template ProduceOutcome handleProduceBatch<Log>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
extern template ProduceOutcome handleProduceBatch<InMemoryLog>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
```

`ProducePath.cpp`: replace the definition head

```cpp
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
```
…through…
```cpp
    rune::storage::Log&               log,
```
by
```cpp
template <class LogT>
ProduceOutcome handleProduceBatch(
    const uint8_t*                    batch,
    std::size_t                       batchLen,
    int32_t                           topicId,
    int32_t                           partition,
    int64_t                           producerId,
    int16_t                           producerEpoch,
    int32_t                           baseSequence,
    LogT&                             log,
```
(the remaining parameters and the body stay). In step 7 replace

```cpp
        if (appended < 0) {
            // Returns negative on back-pressure (e.g. InMemoryLog full).
            spdlog::warn("ProducePath: appendNoSyncRestamp refused (back-pressure) "
                         "topicId={} partition={}", topicId, partition);
            out.error = pc::ErrorCode::Internal;
            return out;
        }
```
with
```cpp
        if (appended < 0) [[unlikely]] {
            // Phase 2b E2: only an InMemoryLog refuses (a disk Log throws).
            // TRANSIENT_FULL: the ring cannot take the batch without evicting
            // uncommitted data (at/above the HW) — retriable NotLeader, the only
            // "nothing appended, retry" code without a protocol change.  A batch
            // larger than the whole ring can never fit: MessageTooLarge.  The
            // caller counts + rate-limits the log line (hot path: no log here).
            out.logFull = true;
            out.error   = appended == InMemoryLog::kRefusedTooLarge ? pc::ErrorCode::MessageTooLarge
                                                                    : pc::ErrorCode::NotLeader;
            return out;
        }
```
At the end of the file, before `} // namespace rune::storage`, add:

```cpp
template ProduceOutcome handleProduceBatch<Log>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
template ProduceOutcome handleProduceBatch<InMemoryLog>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool) noexcept;
```

(The `rune::node::handleProduceBatch` shim at the bottom of `ProducePath.h` takes `rune::storage::Log&` and deduces `Log` — unchanged. Existing callers in `Worker.h`, `ClientGateway.cpp`, `DrHarness.h`, `ProducePathTest.cpp`, `ProducePathBenchmark.cpp` pass a `Log` and deduce it.)

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'InMemoryLog|ProducePath' --output-on-failure`
Expected: all PASS (the existing `InMemoryLogRingTest` / `InMemoryLogTest` keep passing: the default floor is unbounded).

- [ ] **Step 6: Commit**

```bash
git add libs/storage/include/rune/storage/InMemoryLog.h libs/storage/include/rune/storage/ProducePath.h libs/storage/src/ProducePath.cpp libs/storage/tests/storage/InMemoryLogTransientTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): InMemoryLog restamp append, HW eviction floor, refusal codes; produce path over either log

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Replication — transient replica: epoch stamp, HW floor, refusals, follower ring reset, zero-alloc apply

**Files:**
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h:1192-1202` (new public/private block), `:1273-1284` (`makeFetchResponse`)
- Modify: `libs/replication/src/PartitionReplica.cpp:274-279` (ctor), `:455-459` (`becomeLeader`), `:514` (`becomeFollower`), `:779` (self-demotion), `:562-577` (`append`), `:628-636` (`appendControlBatch`), `:679` (`appendKeyed`), `:1033-1034` (`onFetchRequest` empty-view skip), `:1294-1297` + `:1361-1395` (`applyReplicated`), new `resetTransientLogTo_`
- Create: `libs/replication/tests/replication/TransientReplicaTest.cpp`
- Modify: `libs/replication/tests/replication/ZeroAllocReplicaTest.cpp` (one test), `libs/replication/CMakeLists.txt:82-101` (source list)

**Interfaces:**
- Consumes (Task 1): `InMemoryLog::setLeaderEpochStamp`, `setEvictionFloor`, `resetTo`, `kRefusedFull`, `fullRefusals()`, `logStartOffset()`.
- Produces (namespace `rune::replication`, class `PartitionReplica`):
  ```cpp
  void prepareTransientAppend() noexcept;                         // floor := HW (leader) / followerHw_ (follower); no-op on disk
  [[nodiscard]] uint64_t transientLogResetsForTest() const noexcept;
  [[nodiscard]] uint64_t transientApplyRefusalsForTest() const noexcept;
  // append() / appendKeyed() / appendControlBatch() on a transient replica return kAppendBackPressure (-4) when the ring refuses.
  // FetchResponseMsg::logStartOffset = the leader ring start for a transient partition (0 for disk).
  ```

- [ ] **Step 1: Write the failing tests** — create `libs/replication/tests/replication/TransientReplicaTest.cpp`:

```cpp
// TransientReplicaTest.cpp — Phase 2b E2: replication of a TRANSIENT
// (InMemoryLog) partition.  The leader's ring never evicts at/above its high
// watermark (refuses instead), and a follower whose fetch position is below the
// leader's ring start (a restarted node rejoining empty) restarts its ring there
// and catches up — before the fix it fetched an evicted offset forever.
#include <gtest/gtest.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <cstdint>
#include <span>
#include <vector>

using namespace rune::replication;

namespace {
PartitionConfig tcfg(NodeId self) {
    PartitionConfig c;
    c.topicId = 7; c.partitionId = 0; c.selfId = self; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000'000'000LL;
    c.replicaLagMaxMs = 600'000; c.replicaLagMaxOffset = 1'000'000'000;
    c.pushOnAppend = false; c.maxFetchBytes = 1 << 20;
    return c;
}
constexpr int kCap = 20'000;   // hard cap on every drive loop
} // namespace

TEST(TransientReplicaRegression, LeaderRingNeverEvictsAboveTheHighWatermarkRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    Offset last = 0;
    int appended = 0;
    for (int i = 0; i < 64; ++i) {   // follower 2 (in the ISR) never fetched: HW stays 0
        last = r1.append(std::span<const uint8_t>(payload), /*acksAll=*/false);
        if (last < 0) break;
        ++appended;
    }
    ASSERT_EQ(last, PartitionReplica::kAppendBackPressure) << "a full ring must refuse, not evict unreplicated data";
    EXPECT_GT(appended, 0);
    EXPECT_EQ(mem1.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(mem1.fullRefusals(), 1u);

    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r1.highWatermark() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    ASSERT_EQ(r1.highWatermark(), r1.logEndOffset()) << "follower never caught up";
    EXPECT_GE(r1.append(std::span<const uint8_t>(payload), false), 0) << "committed records make room";
    EXPECT_GT(mem1.logStartOffset(), 0);
}

TEST(TransientReplicaRegression, FollowerBehindTheLeaderRingStartResetsAndCatchesUpRegression) {
    rune::storage::InMemoryLog mem1(8 * 1024, 1000), mem2(1 << 20, 100'000);
    InMemoryTransport transport;
    const std::vector<NodeId> all = {1, 2};
    const std::vector<NodeId> leaderOnly = {1};
    // Follower 2 is OUT of the ISR (a restarted node rejoining empty): the HW
    // follows the leader alone, so the leader's ring evicts committed records.
    PartitionReplica r1(tcfg(1), Role::Leader,   1, all, leaderOnly, &mem1, &transport);
    PartitionReplica r2(tcfg(2), Role::Follower, 1, all, all,        &mem2, &transport);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 40; ++i) ASSERT_GE(r1.append(std::span<const uint8_t>(payload), false), 0);
    const Offset leaderStart = mem1.logStartOffset();
    ASSERT_GT(leaderStart, 0) << "precondition: the leader's ring evicted its oldest records";

    transport.setLeader(&r1);
    transport.addFollower(2, &r2);
    int64_t clock = 0;
    for (int i = 0; i < kCap && r2.logEndOffset() < r1.logEndOffset(); ++i) {
        clock += 1'000'000;
        r1.doWork(clock); r2.doWork(clock); transport.pump(clock);
    }
    EXPECT_EQ(r2.logEndOffset(), r1.logEndOffset())
        << "stalled: a follower below the leader's ring start is never served from its LEO";
    EXPECT_EQ(r2.transientLogResetsForTest(), 1u);
    EXPECT_EQ(mem2.logStartOffset(), leaderStart);
}

TEST(TransientReplicaRegression, LeaderAppendStampsItsEpochIntoTheRing) {
    rune::storage::InMemoryLog mem(1 << 20, 1000);
    InMemoryTransport transport;
    PartitionReplica r1(tcfg(1), Role::Leader, 4, {1}, {1}, &mem, &transport);
    EXPECT_EQ(mem.leaderEpochStamp(), 4) << "a transient leader stamps restamp-appends with its epoch";
    r1.becomeFollower(5, 2);
    EXPECT_EQ(mem.leaderEpochStamp(), -1) << "a follower keeps the producer's bytes";
}
```

Append to `libs/replication/tests/replication/ZeroAllocReplicaTest.cpp` (add `#include <rune/storage/InMemoryLog.h>` next to `#include <rune/storage/Log.h>`):

```cpp
// Phase 2b E2 (fix-on-sight): applyReplicated copied the new suffix of every
// replicated response into a fresh std::vector before appending — a heap
// allocation per apply on the follower's hot path (disk AND transient).
TEST(ZeroAllocReplicaRegression, TransientFollowerApplyAllocatesNothing) {
    rune::storage::InMemoryLog mem(1 << 20, 4096);
    SinkTransport t;
    PartitionConfig c = cfg(/*push=*/false);
    c.selfId = 2;
    PartitionReplica follower(c, Role::Follower, 1, {1, 2}, {1, 2}, &mem, &t);
    auto batchAt = [](int64_t base) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(512, 'v'), 1000);
        auto x = b.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(x.data(), 1);
        return x;
    };
    const auto b0 = batchAt(0), b1 = batchAt(1);
    FetchResponseMsg r0{};
    r0.leaderEpoch = 1; r0.partition = 0; r0.highWatermark = 1; r0.logStartOffset = 0;
    r0.errorCode = 0; r0.topicId = 0; r0.records = std::span<const uint8_t>(b0);
    follower.applyReplicated(r0);   // warm-up: the epoch-cache entry for epoch 1 is created here
    ASSERT_EQ(follower.logEndOffset(), 1);
    FetchResponseMsg r1 = r0;
    r1.highWatermark = 2;
    r1.records = std::span<const uint8_t>(b1);
    uint64_t n = 0;
    {
        Armed a;
        follower.applyReplicated(r1);
        n = a.count();
    }
    EXPECT_EQ(follower.logEndOffset(), 2);
    EXPECT_EQ(n, 0u) << "the follower's replicated apply must not allocate";
}
```

In `libs/replication/CMakeLists.txt`, in the `add_executable(rune_replication_tests …)` list, change `        tests/replication/LeaderEpochFenceTest.cpp)` to:

```cmake
        tests/replication/LeaderEpochFenceTest.cpp
        tests/replication/TransientReplicaTest.cpp)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_replication_tests rune_replication_zero_alloc_tests`
Expected: compile error `no member named 'transientLogResetsForTest'`. After adding only the accessors: `LeaderRingNeverEvictsAboveTheHighWatermark` FAILS (`last` is a non-negative offset: the ring evicted unreplicated data), `FollowerBehindTheLeaderRingStart…` FAILS (stalled), `LeaderAppendStampsItsEpoch…` FAILS (`-1`), `TransientFollowerApplyAllocatesNothing` FAILS (`n >= 1`).

- [ ] **Step 3: Implement — `PartitionReplica.h`**

In `makeFetchResponse`, replace `        r.logStartOffset = 0;` with:

```cpp
        // Phase 2b E2: a TRANSIENT leader advertises its ring start — a follower
        // below it restarts its ring there (applyReplicated).  Disk: 0, unchanged.
        r.logStartOffset = memoryBacked_ ? memLog_->logStartOffset() : 0;
```

After the block

```cpp
private:
    uint64_t truncatesDeferred_ = 0;
    uint64_t appliesDroppedTruncatePending_ = 0;
```
insert:

```cpp
public:
    /// Phase 2b E2 (TRANSIENT partitions): refresh the ring's eviction floor to
    /// the current high watermark before an append that bypasses this replica
    /// (the worker produce path appends to memoryLog() directly).  No-op on a
    /// disk partition.  Owner thread.
    void prepareTransientAppend() noexcept { refreshTransientEvictionFloor_(); }
    /// Follower ring restarts at the leader's ring start / replicated applies the ring refused.
    [[nodiscard]] uint64_t transientLogResetsForTest() const noexcept { return transientLogResets_; }
    [[nodiscard]] uint64_t transientApplyRefusalsForTest() const noexcept { return transientApplyRefusals_; }
private:
    uint64_t transientLogResets_     = 0;
    uint64_t transientApplyRefusals_ = 0;
    /// The ring never evicts at/above what is not committed: the leader's HW
    /// (min-ISR-gated served HW), a follower's followerHw_.
    void refreshTransientEvictionFloor_() noexcept {
        if (memoryBacked_) memLog_->setEvictionFloor(role_ == Role::Leader ? highWatermark() : followerHw_);
    }
    /// Kafka leader-epoch stamp of the ACTIVE log (disk Log or ring).
    void stampEpochOnLog_(int32_t epoch) noexcept {
        if (memoryBacked_) { if (memLog_ != nullptr) memLog_->setLeaderEpochStamp(epoch); }
        else if (diskLog_ != nullptr) diskLog_->setLeaderEpochStamp(epoch);
    }
    /// A transient follower below the leader's ring start restarts its ring there.  Cold.
    void resetTransientLogTo_(Offset start);
```

- [ ] **Step 4: Implement — `PartitionReplica.cpp`**

Constructor (leader seed): replace

```cpp
            if (!memoryBacked_ && diskLog_ != nullptr)
                diskLog_->setLeaderEpochStamp(static_cast<int32_t>(epoch_));
```
with
```cpp
            stampEpochOnLog_(static_cast<int32_t>(epoch_));
```

`becomeLeader`: replace

```cpp
    if (!memoryBacked_ && diskLog_ != nullptr) {
        diskLog_->setLeaderEpochStamp(static_cast<int32_t>(epoch_));
        checkpointEpochs_();
    }
```
with
```cpp
    stampEpochOnLog_(static_cast<int32_t>(epoch_));
    if (!memoryBacked_ && diskLog_ != nullptr) checkpointEpochs_();
```

`becomeFollower` and the self-demotion path (two occurrences — use replace-all on the exact text): replace
`if (!memoryBacked_ && diskLog_ != nullptr) diskLog_->setLeaderEpochStamp(-1);` with `stampEpochOnLog_(-1);`.

`append()`: replace

```cpp
    if (externalFollowerFlush_) {
        // Worker-driven replica (spec §3.1, every placement — F30): no inline
        // fsync on the worker (txn markers come through here).  Page-cache
        // append + a flush request the owning worker submits to its io backend
        // in the same pass.
        onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
        if (!memoryBacked_) followerFlushRequested_ = true;
    } else {
        onLog([&](auto& L){ L.append(batch, baseOffset); });
    }
```
with
```cpp
    if (memoryBacked_) {
        // Phase 2b E2: the ring never evicts at/above the HW — a full ring
        // refuses (retriable back-pressure), nothing appended.
        refreshTransientEvictionFloor_();
        if (memLog_->appendNoSync(batch.data(), batch.size(), baseOffset) < 0) [[unlikely]]
            return kAppendBackPressure;
    } else if (externalFollowerFlush_) {
        // Worker-driven replica (spec §3.1, every placement — F30): no inline
        // fsync on the worker (txn markers come through here).  Page-cache
        // append + a flush request the owning worker submits to its io backend
        // in the same pass.
        diskLog_->appendNoSync(batch.data(), batch.size(), baseOffset);
        followerFlushRequested_ = true;
    } else {
        diskLog_->append(batch, baseOffset);
    }
```

`appendControlBatch()`: replace

```cpp
        if (memLog_->appendNoSync(batchScratch_.data(), batchScratch_.size(),
                                  baseOffset + lastOffsetDelta) < 0) return kAppendBackPressure;
```
with
```cpp
        refreshTransientEvictionFloor_();   // Phase 2b E2: never evict uncommitted data
        if (memLog_->appendNoSync(batchScratch_.data(), batchScratch_.size(),
                                  baseOffset + lastOffsetDelta) < 0) return kAppendBackPressure;
```

`appendKeyed()`: replace `    onLog([&](auto& L){ L.append(batch, baseOffset); });` (the one right after `stampLeaderEpoch(batch.data(), …)` in `appendKeyed`) with:

```cpp
    refreshTransientEvictionFloor_();   // Phase 2b E2 (no-op on disk)
    if (onLog([&](auto& L){ return L.append(batch, baseOffset); }) < 0) [[unlikely]]
        return kAppendBackPressure;
```

`onFetchRequest()`: replace

```cpp
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_() && view.empty())
        [[unlikely]] return kErrNone;
```
with
```cpp
    // Phase 2b E2: a TRANSIENT follower fetching BELOW our ring start asks for
    // records the ring evicted (committed; retention) — answer it (no records;
    // makeFetchResponse carries our ring start) so it restarts its ring there,
    // instead of the read-map-failure skip below (which would never answer it).
    const bool belowRingStart = memoryBacked_ && req.fetchOffset < memLog_->logStartOffset();
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_() && view.empty() && !belowRingStart)
        [[unlikely]] return kErrNone;
```

`applyReplicated()`: right after

```cpp
    const Offset preLeo = logEndOffset_();
    const Offset preHw  = followerHw_;
```
insert:
```cpp
    // Phase 2b E2 — TRANSIENT follower behind the leader's ring start: the
    // leader no longer holds [our LEO, its ring start) (evicted committed
    // records), so a fetch from our LEO can never be served.  Restart our ring
    // EMPTY at the leader's ring start (Kafka: a follower out of range below the
    // leader's log start truncates fully and resumes there).  After preLeo is
    // captured, so the fast-ack below fetches from the new position at once.
    if (memoryBacked_ && role_ == Role::Follower && resp.logStartOffset > logEndOffset_()) [[unlikely]]
        resetTransientLogTo_(resp.logStartOffset);
```

In the record-batch branch replace

```cpp
            if (newStart < resp.records.size() && lastOff >= logEndOffset_()) {
                // Append only the strictly-new suffix.
                std::vector<uint8_t> raw(resp.records.begin() + static_cast<std::ptrdiff_t>(newStart),
                                          resp.records.end());
```
…through…
```cpp
                if (assignEpochsFromBatches_(raw.data(), raw.size()) &&
                    !memoryBacked_ && diskLog_ != nullptr)
                    checkpointEpochs_();
            }
```
with
```cpp
            if (newStart < resp.records.size() && lastOff >= logEndOffset_()) {
                // Append only the strictly-new suffix — straight from the
                // response view (Phase 2b E2 fix-on-sight: was copied into a
                // fresh std::vector per apply — a hot-path heap allocation).
                // THROUGHPUT FIX: appendNoSync — do NOT fsync per apply.  The syncing
                // append() cost ~693µs mean (disk fsync) PER batch on the follower,
                // capping acks=all at ~1.5k rec/s.  acks=all crash-durability comes
                // from ISR REPLICATION (multiple page-cache copies), NOT per-record
                // fsync on each follower (same as Kafka's default + rune's documented
                // appendNoSync contract).  A single coalesced sync() runs once per
                // duty cycle below (followerApplyDirty_), amortising the fsync.
                const uint8_t* const newData = resp.records.data() + newStart;
                const std::size_t    newLen  = resp.records.size() - newStart;
                refreshTransientEvictionFloor_();   // E2: a transient ring never evicts above followerHw_
                if (onLog([&](auto& L){ return L.appendNoSync(newData, newLen, lastOff); }) < 0) [[unlikely]] {
                    // E2: the ring is full of records above our HW — nothing applied;
                    // the leader re-serves them once our HW (from its answers) moves.
                    ++transientApplyRefusals_;
                } else {
                    followerApplyDirty_ = true;
                    // Kafka LeaderEpochFileCache.assign on append: the cache is built
                    // from the epochs the leader STAMPED on the batches.
                    if (assignEpochsFromBatches_(newData, newLen) &&
                        !memoryBacked_ && diskLog_ != nullptr)
                        checkpointEpochs_();
                }
            }
```

In the raw-bytes fallback branch of `applyReplicated` replace

```cpp
            onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
            followerApplyDirty_ = true;
            if (assignEpochsFromBatches_(batch.data(), batch.size()) &&
                !memoryBacked_ && diskLog_ != nullptr)
                checkpointEpochs_();
```
with
```cpp
            refreshTransientEvictionFloor_();
            if (onLog([&](auto& L){ return L.appendNoSync(batch.data(), batch.size(), baseOffset); }) < 0) [[unlikely]] {
                ++transientApplyRefusals_;
            } else {
                followerApplyDirty_ = true;
                if (assignEpochsFromBatches_(batch.data(), batch.size()) &&
                    !memoryBacked_ && diskLog_ != nullptr)
                    checkpointEpochs_();
            }
```

Add the definition right after `void PartitionReplica::syncIsrToLeaderState()`'s body:

```cpp
// Phase 2b E2: a TRANSIENT follower whose position is below the leader's ring
// start.  Cold (once per catch-up episode).
void PartitionReplica::resetTransientLogTo_(Offset start)
{
    const Offset was = logEndOffset_();
    memLog_->resetTo(start);
    // Every epoch entry named offsets this ring no longer holds; the next
    // fetch carries lastFetchedEpoch -1 (an empty log: no lineage to validate)
    // and the cache is rebuilt from the stamped batches the leader sends.
    epochCache_.truncateFromEnd(std::numeric_limits<int64_t>::min());
    followerSync_.forceBehind();   // fetch on at once (the leader holds [start, LEO))
    ++transientLogResets_;
    spdlog::warn("PartitionReplica[node={} topic={} part={}]: transient follower at offset {} is below the "
                 "leader's ring start {} — ring restarted empty at {} (resets={})",
                 cfg_.selfId, cfg_.topicId, cfg_.partitionId, was, start, start, transientLogResets_);
}
```

(`<limits>` and `<spdlog/spdlog.h>` are already included in `PartitionReplica.cpp`.)

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'TransientReplica|ZeroAllocReplica|PartitionReplica|EpochFencing|HighWatermark|LeaderEpoch|TransientTopic_|RebalanceFailover' --output-on-failure`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/replication/include/rune/replication/PartitionReplica.h libs/replication/src/PartitionReplica.cpp libs/replication/tests/replication/TransientReplicaTest.cpp libs/replication/tests/replication/ZeroAllocReplicaTest.cpp libs/replication/CMakeLists.txt
git commit -m "feat(replication): transient ring never evicts above the HW; follower below the leader's ring start resets; zero-alloc apply

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 3: Worker — produce to a transient partition (shard ingress + op ring), marker back-pressure

**Files:**
- Create: `libs/node/tests/integration/TransientRig.h`
- Create: `libs/node/tests/integration/TransientDataPathTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target after `rune_hotpath_b_alloc_regression_tests`, ~`:874`)
- Modify: `libs/node/include/rune/node/Worker.h` — `produceCore_` (the `if (!log) { … }` block, the `asyncStrict` line, the `handleProduceBatch` call), `executeAppendControlRecordWire_` (the `kAppendMinIsr` branch), `getLogAndReplicaForPartition_` (comment), the push-read test accessors (`deferredPushReadsForTest`, ~`:4602`) and the private push fields (after `pushScratch_`, ~`:4610`), a new `noteTransientFull_` next to `notePushReadOversize_`

**Interfaces:**
- Consumes: `handleProduceBatch<InMemoryLog>`, `ProduceOutcome::logFull` (Task 1); `PartitionReplica::prepareTransientAppend()`, `kAppendBackPressure` from transient `appendControlBatch` (Task 2).
- Produces (class `rune::node::Worker`, test accessors used by Tasks 4-5):
  ```cpp
  [[nodiscard]] uint64_t transientFullRefusalsForTest() const noexcept;
  [[nodiscard]] uint64_t transientCursorSkipsForTest() const noexcept;
  [[nodiscard]] uint64_t transientRecordsSkippedForTest() const noexcept;
  ```
  and the shared test rig `transient_rig::Rig` (`TransientRig.h`): `Rig(std::size_t ringBytes = 4u << 20, std::size_t maxBatches = 100'000, bool withFollower = false)`, members `mem`, `rep`, `pool`, `now`; `pass(frame*)`, `produce(batch, acks, int64_t* baseOut, pid, epoch, seq) → int16_t` (error code, `-1` = unanswered), `pushRead(subId, from, isolation)`, `readOnce(subId, from, isolation) → std::optional<WorkerReturn>`, `fetch(from)`, `marker(token, pid, commit)`; free functions `batchOf(recs, valueBytes, pid, epoch, seq, txn)`, `produceFrame(corr, batch, acks, pid, epoch, seq)`.

- [ ] **Step 1: Write the rig** — create `libs/node/tests/integration/TransientRig.h`:

```cpp
#pragma once
// TransientRig.h — Phase 2b E2 test rig: ONE worker (shard mode, registry-
// routed ops, worker ingress active) owning ONE TRANSIENT leader partition —
// topic 1, partition 0 — on an InMemoryLog.  Optional in-sync follower 2 that
// never fetches unless the test sends its FetchRequest (fetch()).  Test-only;
// shared by TransientDataPathTest.cpp and TransientDataPathAllocTest.cpp.
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace transient_rig {

namespace pc = rune::protocol::client;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerPool;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

constexpr int32_t kTopic = 1;

inline std::vector<uint8_t> batchOf(int recs, std::size_t valueBytes, int64_t pid = -1, int16_t epoch = 0,
                                    int32_t seq = -1, bool txn = false) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq); }
    if (txn) b.setTransactional(true);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < recs; ++i) b.append("k" + std::to_string(i), v, 1000 + i);
    return b.build();
}

/// One client Produce wire frame (what the worker's ingress delivers in shard mode).
inline std::vector<uint8_t> produceFrame(int64_t corr, const std::vector<uint8_t>& batch, uint8_t acks,
                                         int64_t pid = -1, int16_t epoch = 0, int32_t seq = -1) {
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = kTopic; msg.partition = 0;
    msg.partitionKnown = 1; msg.acks = acks; msg.producerId = pid; msg.producerEpoch = epoch;
    msg.baseSequence = seq; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}

struct Rig {
    rune::storage::InMemoryLog                mem;          // outlives the replica (declared first)
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    WorkerPool                                pool{1, rune::metadata::ShardRouter(1)};
    PartitionReplica*                         rep  = nullptr;
    int64_t                                   now  = 1'000'000'000;
    int64_t                                   corr = 0;

    explicit Rig(std::size_t ringBytes = 4u << 20, std::size_t maxBatches = 100'000, bool withFollower = false)
        : mem(ringBytes, maxBatches) {
        PartitionConfig c;
        c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.replicaLagMaxMs = 600'000;
        c.followerFlushIntervalNs = 0;
        const std::vector<int32_t> set = withFollower ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, set, set, &mem, &transport);
        r->setExternalFollowerFlush(true);
        rep = r.get();
        reg.add(kTopic, 0, std::move(r));
        pool.setRegistry(&reg);
        Worker& w = pool.worker(0);
        w.setWorkerIngress(nullptr, /*active=*/true);
        w.setShardMode(true);
        pool.assignFromRegistry(reg);
        pass();
    }
    Worker& worker() { return pool.worker(0); }

    void pass(const std::vector<uint8_t>* frame = nullptr) {
        now += 1'000'000;
        if (frame) (void)worker().handleIngressFrameForTest(frame->data(), frame->size(), now);
        (void)worker().doWork(now);
    }

    /// One produce over the shard ingress; passes until ITS ProduceResult (bounded).
    /// Returns its error code (0 = OK, -1 = never answered); *baseOut = its base offset.
    /// Drops every other return drained meanwhile (tests keep phases separate).
    int16_t produce(const std::vector<uint8_t>& batch, uint8_t acks, int64_t* baseOut = nullptr,
                    int64_t pid = -1, int16_t epoch = 0, int32_t seq = -1) {
        const int64_t c = ++corr;
        const auto frame = produceFrame(c, batch, acks, pid, epoch, seq);
        pass(&frame);
        for (int i = 0; i < 64; ++i) {
            int16_t code = -1;
            pool.drainAllReturns([&](WorkerReturn r) {
                if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == c) {
                    code = r.errorCode;
                    if (baseOut != nullptr) *baseOut = r.baseOffset;
                }
            });
            if (code != -1) return code;
            pass();
        }
        return -1;
    }

    /// A gateway PushRead (legacy-mode subscription read) for the transient partition.
    void pushRead(int64_t subId, int64_t from, uint8_t isolation = 0) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead; op.topicId = kTopic; op.partition = 0;
        op.subscriptionId = subId; op.sessionId = 1; op.nextOffset = from;
        op.hwBound = INT64_MAX; op.isolation = isolation;
        op.logPtr = nullptr;   // the gateway holds no disk Log for a transient partition
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    }
    /// pushRead + passes until ITS PushReadResult (bounded).
    std::optional<WorkerReturn> readOnce(int64_t subId, int64_t from, uint8_t isolation = 0) {
        pushRead(subId, from, isolation);
        for (int i = 0; i < 64; ++i) {
            pass();
            std::optional<WorkerReturn> got;
            pool.drainAllReturns([&](WorkerReturn r) {
                if (r.type == WorkerReturn::Type::PushReadResult && r.subscriptionId == subId) got = std::move(r);
            });
            if (got) return got;
        }
        return std::nullopt;
    }

    /// Follower 2's FetchRequest, as the replication transport hands it to the owning worker.
    void fetch(int64_t from) {
        WorkerOp op;
        op.type = WorkerOp::Type::FetchRequest; op.topicId = kTopic; op.partition = 0; op.replica = rep;
        op.fetchRequestPayload.leaderEpoch = 1;
        op.fetchRequestPayload.partition   = 0;
        op.fetchRequestPayload.followerId  = 2;
        op.fetchRequestPayload.fetchOffset = from;
        op.fetchRequestPayload.maxBytes    = 512 * 1024;
        op.fetchRequestPayload.topicId     = kTopic;
        op.fetchRequestPayload.nowNs       = now;
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    }

    /// A transaction coordinator's control marker for the transient partition.
    void marker(int64_t token, int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kTopic; op.partition = 0;
        op.correlationId = token; op.producerId = pid;
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    }
};

} // namespace transient_rig
```

- [ ] **Step 2: Write the failing tests** — create `libs/node/tests/integration/TransientDataPathTest.cpp`:

```cpp
// TransientDataPathTest.cpp — Phase 2b E2: the worker data path of a TRANSIENT
// (InMemoryLog) partition — produce over shard ingress and the op ring, push
// reads served from the ring, read_committed, the full-ring rule, the evicted
// cursor, ownership moves.  Deterministic: one worker driven by hand, no Aeron.
#include "TransientRig.h"

using namespace transient_rig;

namespace {
constexpr int16_t kOk        = 0;
constexpr int16_t kNotLeader = static_cast<int16_t>(pc::ErrorCode::NotLeader);
} // namespace

TEST(TransientDataPathRegression, ShardIngressProduceAppendsToTheRingRegression) {
    Rig rig;
    const auto b = batchOf(3, 100);
    int64_t base = -1;
    ASSERT_EQ(rig.produce(b, /*acks=*/1, &base), kOk) << "a transient produce over shard ingress was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    ASSERT_EQ(rig.produce(b, 1, &base), kOk);
    EXPECT_EQ(base, 3);
    EXPECT_EQ(rig.mem.logEndOffset(), 6);
    EXPECT_EQ(rig.rep->highWatermark(), 6) << "single node: acks=all commits at once";
}

TEST(TransientDataPathRegression, ProducedBatchCarriesTheLeaderEpochRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(2, 50), /*acks=*/0), kOk);
    const auto v = rig.mem.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 0);
    EXPECT_EQ(r.partitionLeaderEpoch(), 1) << "followers build their epoch cache from the stamped epoch (KIP-320)";
    EXPECT_TRUE(r.ensureValid());
}

TEST(TransientDataPathRegression, GatewayRoutedProduceOpAppendsToTheRingRegression) {
    Rig rig;
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = kTopic; op.partition = 0;
    op.logPtr = nullptr;   // the gateway has no disk Log for a transient partition (legacy mode)
    op.sessionId = 1; op.correlationId = 77;
    op.lastOffsetDelta = 2; op.recordCount = 3; op.durabilityTier = 1;
    op.recordBatchBytes = batchOf(3, 100);
    ASSERT_TRUE(rig.pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == 77) { code = r.errorCode; base = r.baseOffset; }
        });
    }
    ASSERT_EQ(code, kOk) << "a gateway-routed (legacy mode) transient produce was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    EXPECT_EQ(rig.mem.logEndOffset(), 3);
}

TEST(TransientDataPathRegression, FullRingRefusesRetriablyAndNeverEvictsUncommittedRegression) {
    Rig rig(/*ringBytes=*/16 * 1024, /*maxBatches=*/1000, /*withFollower=*/true);   // ISR {1,2}: HW waits for 2
    const auto b = batchOf(1, 1000);
    int ok = 0;
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) {
        code = rig.produce(b, /*acks=*/0);
        if (code == kOk) ++ok;
    }
    ASSERT_EQ(code, kNotLeader) << "a full ring must refuse (retriable), not evict unreplicated data";
    EXPECT_GT(ok, 0);
    EXPECT_EQ(rig.mem.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(rig.mem.fullRefusals(), 1u);
    EXPECT_GE(rig.worker().transientFullRefusalsForTest(), 1u);
    const int64_t leo = rig.mem.logEndOffset();
    EXPECT_EQ(leo, ok) << "a refused produce appended nothing";

    rig.fetch(leo);   // follower 2 now holds everything: the HW reaches the LEO
    for (int i = 0; i < 8; ++i) rig.pass();
    ASSERT_EQ(rig.rep->highWatermark(), leo);
    EXPECT_EQ(rig.produce(b, 0), kOk) << "committed records make room";
    EXPECT_GT(rig.mem.logStartOffset(), 0);
}

TEST(TransientDataPathRegression, FullRingMarkerIsRefusedRetriablyRegression) {
    Rig rig(16 * 1024, 1000, /*withFollower=*/true);
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) code = rig.produce(batchOf(1, 1000), 0);
    ASSERT_EQ(code, kNotLeader) << "precondition: the ring is full of uncommitted data";
    rig.marker(/*token=*/5, /*pid=*/9, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 5) mcode = r.errorCode;
        });
    }
    EXPECT_EQ(mcode, kNotLeader) << "the coordinator must retry a refused marker (was: Internal — dropped)";
}
```

In `libs/node/CMakeLists.txt`, right after the `gtest_discover_tests(rune_hotpath_b_alloc_regression_tests)` line add:

```cmake
    # Phase 2b E2: the worker data path of TRANSIENT (InMemoryLog) partitions —
    # produce, push reads, read_committed, full ring, evicted cursors.  No Aeron.
    add_executable(rune_transient_data_path_tests
        tests/integration/TransientDataPathTest.cpp)
    target_link_libraries(rune_transient_data_path_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_data_path_tests)
```

- [ ] **Step 3: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_transient_data_path_tests`
Expected: compile error `no member named 'transientFullRefusalsForTest' in 'rune::node::Worker'`. With only the accessors added: `ShardIngressProduce…`, `ProducedBatchCarries…`, `GatewayRoutedProduceOp…` FAIL with errorCode 6 (`Internal`: "owned but … log"), the full-ring tests FAIL the same way.

- [ ] **Step 4: Implement — `Worker.h`**

Accessors: right after `[[nodiscard]] std::size_t deferredPushReadsForTest() const noexcept { return deferredPushCount_; }` add:

```cpp
    /// Phase 2b E2 (TRANSIENT partitions): produces/markers the ring refused;
    /// reads whose cursor was below the ring start, and the records they skipped.
    [[nodiscard]] uint64_t transientFullRefusalsForTest() const noexcept { return transientFullRefusals_; }
    [[nodiscard]] uint64_t transientCursorSkipsForTest() const noexcept { return transientCursorSkips_; }
    [[nodiscard]] uint64_t transientRecordsSkippedForTest() const noexcept { return transientRecordsSkipped_; }
```

Fields: right after the `std::vector<uint8_t>                  pushScratch_ = …;` line add:

```cpp
    uint64_t transientFullRefusals_   = 0;   ///< E2: ring refused a produce / marker (full or too large)
    uint64_t transientCursorSkips_    = 0;   ///< E2: reads whose cursor was below the ring start
    uint64_t transientRecordsSkipped_ = 0;   ///< E2: records those reads skipped (evicted unread)
```

Right before `[[gnu::cold]] void notePushReadOversize_(` add:

```cpp
    // Phase 2b E2: a TRANSIENT ring refused an append (full of unreplicated
    // records, or a batch larger than the ring).  Counted; logged first + every 1024th.
    [[gnu::cold]] void noteTransientFull_(int32_t topicId, int32_t partition,
                                          rune::protocol::client::ErrorCode ec) noexcept {
        ++transientFullRefusals_;
        if (transientFullRefusals_ == 1 || (transientFullRefusals_ & 1023u) == 0)
            spdlog::warn("Worker {}: transient topicId={} partition={}: ring refused an append ({}) — {} "
                         "(total={}); raise RUNE_MEMORY_LOG_RING_BYTES or let the followers catch up",
                         workerId_, topicId, partition, static_cast<int>(ec),
                         ec == rune::protocol::client::ErrorCode::MessageTooLarge
                             ? "batch larger than the ring" : "ring full of unreplicated records",
                         transientFullRefusals_);
    }
```

`produceCore_`: replace the whole `if (!log) { … }` block — it begins with the comment `// E5.1.5: distinguish "partition not owned by this worker" (retriable` and ends with `enqueueReturn_(ret);` / `return 1;` / `}` right before the comment `// F15 §3.1 (I1): the owning worker is the role authority` — with:

```cpp
        // Phase 2b E2: a TRANSIENT partition (owned replica, no disk Log) appends
        // to the replica's own InMemoryLog through this same produce core.
        rune::storage::InMemoryLog* const mem =
            (log == nullptr && replica != nullptr) ? replica->memoryLog() : nullptr;
        if (log == nullptr && mem == nullptr) {
            // Not owned by this worker (getLogAndReplicaForPartition_ → {nullptr,
            // nullptr}): retriable NotLeader so the client refreshes metadata and
            // re-routes.  Shared-nothing: no cross-worker forwarding per
            // [[rune-shared-nothing-law]].
            spdlog::debug("Worker {}: produce for a partition this worker does not own (topicId={} "
                          "partition={}); returning NotLeader (retriable)",
                          workerId_, f.topicId, f.partition);
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return 1;
        }
```

Replace `        const bool asyncStrict = durabilityTier == 2 && log->isDiskBacked();` with:

```cpp
        // Phase 2b E2: a transient ring has no fd — tier=strict acks as Majority.
        const bool asyncStrict = durabilityTier == 2 && log != nullptr && log->isDiskBacked();
```

Replace

```cpp
        // Call the shared produce helper.
        rune::storage::ProduceOutcome outcome = rune::storage::handleProduceBatch(
            rbSpan.data(), rbSpan.size(),
            f.topicId, f.partition,
            f.producerId, f.producerEpoch, f.baseSequence,
            *log, psm_,
            acksAll, isrSize, minIsr, helperTier,
            // M-3: skip the repeat CRC pass ONLY when the op's producer says it
            // validated these exact bytes (the gateway, right after ensureValid).
            /*crcPreverified=*/f.fromOp && f.crcValidated);
```
with
```cpp
        // Call the shared produce helper — over the disk Log, or (Phase 2b E2) a
        // TRANSIENT partition's ring: one memcpy into the ring, base offset +
        // leader epoch patched there; the ring never evicts at/above the HW
        // (prepareTransientAppend) — full → NotLeader (retriable), nothing appended.
        // M-3: skip the repeat CRC pass ONLY when the op's producer says it
        // validated these exact bytes (the gateway, right after ensureValid).
        const bool crcPreverified = f.fromOp && f.crcValidated;
        rune::storage::ProduceOutcome outcome;
        if (log != nullptr) {
            outcome = rune::storage::handleProduceBatch(
                rbSpan.data(), rbSpan.size(), f.topicId, f.partition,
                f.producerId, f.producerEpoch, f.baseSequence,
                *log, psm_, acksAll, isrSize, minIsr, helperTier, crcPreverified);
        } else {
            replica->prepareTransientAppend();
            outcome = rune::storage::handleProduceBatch(
                rbSpan.data(), rbSpan.size(), f.topicId, f.partition,
                f.producerId, f.producerEpoch, f.baseSequence,
                *mem, psm_, acksAll, isrSize, minIsr, helperTier, crcPreverified);
            if (outcome.logFull) [[unlikely]] noteTransientFull_(f.topicId, f.partition, outcome.error);
        }
```

`executeAppendControlRecordWire_`: replace

```cpp
            } else if (base == RepT::kAppendMinIsr) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — min-ISR gate refused the marker "
                             "topicId={} partition={}", workerId_, topicId, partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
            } else {
```
with
```cpp
            } else if (base == RepT::kAppendMinIsr) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — min-ISR gate refused the marker "
                             "topicId={} partition={}", workerId_, topicId, partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
            } else if (base == RepT::kAppendBackPressure) {
                // Phase 2b E2: a TRANSIENT ring full of unreplicated records —
                // retriable: the coordinator re-sends the marker (never dropped).
                noteTransientFull_(topicId, partition, pc::ErrorCode::NotLeader);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            } else {
```

`getLogAndReplicaForPartition_`: replace the comment block

```cpp
                // Only disk-backed replicas expose a rune::storage::Log*.
                // Memory-backed (InMemoryLog) replicas are NOT supported on the
                // WorkerIngress produce path — the gateway fanout handles them
                // via the existing WorkerOp ring (which carries a logPtr).
                // Return nullptr for memory-backed so the caller sends an
                // Internal error rather than crash.
```
with
```cpp
                // Only disk-backed replicas expose a rune::storage::Log*.  A
                // TRANSIENT (InMemoryLog) replica returns {nullptr, replica}:
                // produceCore_ appends to replica->memoryLog() (Phase 2b E2).
```

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'TransientDataPath|Worker|ProducePath|F15|HotPathB|ShardNonBlocking' --output-on-failure`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/node/include/rune/node/Worker.h libs/node/tests/integration/TransientRig.h libs/node/tests/integration/TransientDataPathTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(worker): produce to transient partitions (shard ingress + op ring); full ring refuses retriably

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Worker — push reads and owned-subscription pushes from the ring; evicted cursor; ownership moves

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h` — `pushOwnedSubscriptions_` (~`:2756-2995`), `servePushRead_` (~`:3193-3305`), `readPushChunk_` (~`:3315-3328`), `beginPushPass_` (~`:3425-3434`), new `skipEvictedCursor_` before `readNoFault_` (~`:3454`), `serveHeldPushReads_` (~`:5045-5051`)
- Test: `libs/node/tests/integration/TransientDataPathTest.cpp` (append)

**Interfaces:**
- Consumes: Task 3's counters/accessors and rig.
- Produces: `int64_t Worker::skipEvictedCursor_(const rune::storage::InMemoryLog& mem, int64_t cursor) noexcept` (private; returns `max(cursor, mem.logStartOffset())`, counting a skip). `PushReadArgs` whose partition moved away now carries `replica == nullptr` (never only `logPtr == nullptr`).

- [ ] **Step 1: Write the failing tests** — append to `TransientDataPathTest.cpp`:

```cpp
TEST(TransientDataPathRegression, PushReadServesTheRingRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(3, 100), 0), kOk);
    ASSERT_EQ(rig.produce(batchOf(3, 100), 0), kOk);
    const auto r = rig.readOnce(/*subId=*/7, /*from=*/0);
    ASSERT_TRUE(r.has_value()) << "no PushReadResult";
    ASSERT_FALSE(r->chunk.empty()) << "a transient PushRead was left unanswered (no disk Log)";
    EXPECT_EQ(r->pushEffectiveBound(), 6);
    EXPECT_EQ(r->newNextOffset, 6);
    std::size_t pos = 0;
    int64_t expect = 0;
    while (pos < r->chunk.size()) {
        rune::protocol::RecordBatchReader b(r->chunk.data() + pos, r->chunk.size() - pos);
        ASSERT_GT(b.totalLength(), 0);
        EXPECT_EQ(b.baseOffset(), expect);
        expect = b.baseOffset() + b.lastOffsetDelta() + 1;
        pos += static_cast<std::size_t>(b.totalLength());
    }
    EXPECT_EQ(expect, 6) << "whole contiguous batches from the cursor";
}

TEST(TransientDataPathRegression, PushReadBelowRingStartResumesAtRingStartRegression) {
    Rig rig(/*ringBytes=*/8 * 1024, /*maxBatches=*/1000);
    for (int i = 0; i < 20; ++i) ASSERT_EQ(rig.produce(batchOf(1, 1000), 0), kOk);
    const int64_t start = rig.mem.logStartOffset();
    ASSERT_GT(start, 0) << "precondition: the ring evicted committed records (retention)";
    const auto r = rig.readOnce(9, /*from=*/0);   // a consumer that fell behind the ring
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->chunk.empty()) << "a cursor below the ring start stalled";
    EXPECT_EQ(rune::protocol::RecordBatchReader(r->chunk.data(), r->chunk.size()).baseOffset(), start);
    EXPECT_EQ(rig.worker().transientCursorSkipsForTest(), 1u);
    EXPECT_EQ(rig.worker().transientRecordsSkippedForTest(), static_cast<uint64_t>(start));
}

TEST(TransientDataPathRegression, ReadCommittedHoldsTheOpenTransactionUntilItsCommitRegression) {
    Rig rig;
    constexpr int64_t kPid = 7;
    const auto txn = batchOf(1, 50, kPid, /*epoch=*/0, /*seq=*/0, /*txn=*/true);
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code != kOk; ++i)   // NotLeader while the RC state builds (retriable)
        code = rig.produce(txn, /*acks=*/0, &base, kPid, 0, 0);
    ASSERT_EQ(code, kOk);
    ASSERT_EQ(base, 0);

    const auto open = rig.readOnce(21, 0, /*read_committed=*/1);
    ASSERT_TRUE(open.has_value());
    EXPECT_TRUE(open->chunk.empty()) << "an OPEN transaction leaked to read_committed";
    EXPECT_EQ(open->pushEffectiveBound(), 0) << "the LSO pins at the open transaction's first offset";

    rig.marker(/*token=*/3, kPid, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 3) mcode = r.errorCode;
        });
    }
    ASSERT_EQ(mcode, kOk);

    const auto committed = rig.readOnce(22, 0, 1);
    ASSERT_TRUE(committed.has_value());
    ASSERT_FALSE(committed->chunk.empty()) << "the committed transaction was never delivered";
    rune::protocol::RecordBatchReader b(committed->chunk.data(), committed->chunk.size());
    EXPECT_EQ(b.baseOffset(), 0);
    EXPECT_EQ(static_cast<std::size_t>(b.totalLength()), committed->chunk.size()) << "a marker was delivered";
    EXPECT_EQ(committed->newNextOffset, 2) << "the cursor passes the marker";
}

TEST(TransientDataPathRegression, DeferredPushReadAfterOwnershipMovedNeverReadsTheRingRegression) {
    Rig rig;   // 4 MiB ring
    const auto b = batchOf(4, 1000);
    for (int i = 0; i < 300; ++i) ASSERT_EQ(rig.produce(b, 0), kOk);   // ~1.2 MiB: several passes of push budget
    for (int s = 0; s < 4; ++s) rig.pushRead(200 + s, 0);
    rig.pass();
    ASSERT_GT(rig.worker().deferredPushReadsForTest(), 0u) << "precondition: reads deferred";
    int answeredWithData = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::PushReadResult && !r.chunk.empty()) ++answeredWithData;
    });
    rig.worker().setOwnedDirect({});   // the partition moved to another worker
    for (int k = 0; k < 4; ++k) rig.pass();
    int unanswered = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::PushReadResult) return;
        EXPECT_TRUE(r.chunk.empty()) << "read a ring this worker no longer owns (a cross-thread read)";
        EXPECT_EQ(r.pushEffectiveBound(), -1);
        ++unanswered;
    });
    EXPECT_EQ(answeredWithData + unanswered, 4);
    EXPECT_EQ(rig.worker().deferredPushReadsForTest(), 0u);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_transient_data_path_tests && ctest --test-dir build-fast -R 'TransientDataPathRegression\.(PushRead|ReadCommitted|DeferredPushRead)' --output-on-failure`
Expected: `PushReadServesTheRing` FAILS ("left unanswered": no `Log*` → bound -1), `PushReadBelowRingStart…` FAILS (empty chunk), `ReadCommitted…` FAILS (committed read empty), `DeferredPushRead…` PASSES today (reads are unanswered for every transient partition) — it pins the ownership rule the next step must keep.

- [ ] **Step 3: Implement — `Worker.h`**

Add before `rune::storage::LogReadView readNoFault_(rune::storage::Log* log, int64_t offset, std::size_t maxBytes,`:

```cpp
    // Phase 2b E2 — TRANSIENT_EVICTED: the ring evicted committed records this
    // reader had not read yet (the in-memory retention).  The cursor resumes AT
    // the ring start — counted, logged first + every power of two — never a
    // stall on an offset the ring no longer holds.  Disk logs never take this.
    [[nodiscard]] int64_t skipEvictedCursor_(const rune::storage::InMemoryLog& mem, int64_t cursor) noexcept {
        const int64_t start = mem.logStartOffset();
        if (cursor >= start) [[likely]] return cursor;
        ++transientCursorSkips_;
        transientRecordsSkipped_ += static_cast<uint64_t>(start - cursor);
        if ((transientCursorSkips_ & (transientCursorSkips_ - 1)) == 0)
            spdlog::warn("Worker {}: a reader of a transient partition fell behind its ring: cursor {} < ring "
                         "start {} — resumed at {} (skips={}, records skipped={})", workerId_, cursor, start,
                         start, transientCursorSkips_, transientRecordsSkipped_);
        return start;
    }
```

`servePushRead_`: replace from `    void servePushRead_(const PushReadArgs& a, bool mayHold) {` through the end of the

```cpp
        rune::storage::Log* log = a.logPtr;
        if (!log) {
            // No log: an UNANSWERED read (review I-1) — the gateway re-issues it
            // and never parks the subscription on it.
            ret.setPushEffectiveBound(-1);
            enqueueReturn_(ret);
            return;
        }
```
block with:

```cpp
    void servePushRead_(const PushReadArgs& in, bool mayHold) {
        PushReadArgs a = in;   // Phase 2b E2: the cursor may move up (TRANSIENT_EVICTED) — a stack copy
        rune::storage::Log* log = a.logPtr;
        // Phase 2b E2: a TRANSIENT partition has no disk Log — its source is the
        // owning replica's InMemoryLog.  a.replica is THIS worker's replica here:
        // PushRead is ownership-gated, and a deferred / held read whose partition
        // moved away is re-served with replica == nullptr (beginPushPass_,
        // serveHeldPushReads_) — never a ring this worker does not own.
        rune::storage::InMemoryLog* const mem =
            (log == nullptr && a.replica != nullptr) ? a.replica->memoryLog() : nullptr;
        if (mem != nullptr) a.nextOffset = skipEvictedCursor_(*mem, a.nextOffset);
        const int32_t topicId    = a.topicId;
        const int32_t partition  = a.partition;
        const int64_t nextOffset = a.nextOffset;
        const int64_t hwBound    = a.hwBound;
        WorkerReturn ret;
        ret.type                 = WorkerReturn::Type::PushReadResult;
        ret.subscriptionId       = a.subscriptionId;
        ret.sessionId            = a.sessionId; // gateway needs this to find the session
        ret.readTopicId          = topicId;
        ret.readPartition        = partition;
        ret.newNextOffset        = nextOffset; // will be advanced by gateway on success
        ret.hwBound              = hwBound;
        ret.isolation            = a.isolation;
        ret.isGroup              = a.isGroup;
        ret.groupCarrierReceived = a.carrier;
        ret.setPushEffectiveBound(-1);   // N5: unanswered unless a path below answers explicitly

        if (!log && !mem) {
            // No source: an UNANSWERED read (review I-1) — the gateway re-issues it
            // and never parks the subscription on it.
            ret.setPushEffectiveBound(-1);
            enqueueReturn_(ret);
            return;
        }
```

(The rest of `servePushRead_` keeps using `a`, `nextOffset`, `log` — now the clamped copy; a held / deferred read stores the clamped cursor, so a skip is counted once.) In it, replace `            switch (readPushChunk_(a, ret, log, bound, rc)) {` with `            switch (readPushChunk_(a, ret, log, mem, bound, rc)) {`.

`readPushChunk_`: replace its signature line

```cpp
    PushChunk readPushChunk_(const PushReadArgs& a, WorkerReturn& ret, rune::storage::Log* log, int64_t bound,
                             const RcState* rc) {
```
with
```cpp
    PushChunk readPushChunk_(const PushReadArgs& a, WorkerReturn& ret, rune::storage::Log* log,
                             rune::storage::InMemoryLog* mem, int64_t bound, const RcState* rc) {
```
and replace `        const rune::storage::LogReadView v = readNoFault_(log, a.nextOffset, cap, bound);` with:

```cpp
        // Phase 2b E2: a TRANSIENT partition reads a zero-copy view of its ring
        // (same thread, no syscall); the bound clamp below applies unchanged.
        const rune::storage::LogReadView v =
            log != nullptr ? readNoFault_(log, a.nextOffset, cap, bound)
                           : mem->readView(a.nextOffset, static_cast<int32_t>(cap));
```

`beginPushPass_`: replace

```cpp
            } else {
                a.logPtr = nullptr;   // never read a log this worker no longer owns → unanswered
            }
```
with
```cpp
            } else {
                // Never read a log — or (E2) a transient ring, found through the
                // replica — this worker no longer owns → unanswered.
                a.logPtr  = nullptr;
                a.replica = nullptr;
            }
```

`serveHeldPushReads_`: replace

```cpp
            } else {
                a.logPtr = nullptr;   // never read a log this worker no longer owns
                servePushRead_(a, false);   // → unanswered: the gateway re-issues to the new owner
            }
```
with
```cpp
            } else {
                a.logPtr  = nullptr;   // never read a log this worker no longer owns
                a.replica = nullptr;   // E2: nor its ring (a transient read finds it through the replica)
                servePushRead_(a, false);   // → unanswered: the gateway re-issues to the new owner
            }
```

`pushOwnedSubscriptions_` (shard mode — the worker pushes on its own egress): replace
`            if (!s.logPtr || !s.active) continue;   // parked: partition moved away` with
`            if (!s.active) continue;   // parked: partition moved away (E2: the source is resolved below)`.

Replace

```cpp
            auto* rep = subReplica_(s);
            if (s.incarnation != 0 && !rep) continue;   // not owned (defensive: active implies owned)
            int64_t bound = rep ? rep->highWatermark()
                                : s.logPtr->logEndOffset();
```
with
```cpp
            auto* rep = subReplica_(s);
            if (s.incarnation != 0 && !rep) continue;   // not owned (defensive: active implies owned)
            // Phase 2b E2: a TRANSIENT partition's source is the owned replica's
            // ring (the Subscribe op carried no disk Log); no source → nothing to push.
            rune::storage::InMemoryLog* const mem =
                (s.logPtr == nullptr && rep != nullptr) ? rep->memoryLog() : nullptr;
            if (s.logPtr == nullptr && mem == nullptr) continue;
            if (mem != nullptr) s.nextOffset = skipEvictedCursor_(*mem, s.nextOffset);   // TRANSIENT_EVICTED
            int64_t bound = rep ? rep->highWatermark()
                                : s.logPtr->logEndOffset();
```

Replace

```cpp
            const rune::storage::LogReadView view = readNoFault_(
                s.logPtr, s.nextOffset, std::min<std::size_t>(kPushMaxChunkBytes, pushBudgetLeft_), bound);
```
with
```cpp
            const std::size_t readCap = std::min<std::size_t>(kPushMaxChunkBytes, pushBudgetLeft_);
            const rune::storage::LogReadView view =
                s.logPtr != nullptr ? readNoFault_(s.logPtr, s.nextOffset, readCap, bound)
                                    : mem->readView(s.nextOffset, static_cast<int32_t>(readCap));   // E2: zero-copy ring view
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'TransientDataPath|HotPathB|Worker|SubscribeDedupe|GroupPush|ReadCommittedPush|GatewaySubscribe' --output-on-failure`
Expected: all PASS (`WorkerPoolTest`'s InMemoryLog PushReads with `hwBound = 0` are now answered at bound 0 instead of unanswered — that test only counts results).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/Worker.h libs/node/tests/integration/TransientDataPathTest.cpp
git commit -m "feat(worker): push reads + owned-subscription pushes from transient rings; evicted cursors resume at the ring start

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Zero-allocation regression + produce→subscribe latency benchmark (transient vs disk)

**Files:**
- Create: `libs/node/tests/integration/TransientDataPathAllocTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target after `rune_transient_data_path_tests`)
- Modify: `benchmark/WorkerProduceLatencyBench.cpp` (fixture + runner inside the anonymous namespace, before `} // namespace`; registrations at the end)

**Interfaces:**
- Consumes: `transient_rig::Rig` (Task 3), the Task 3/4 worker paths.
- Produces: benchmarks `BM_WorkerProduceToPushRead_{Disk,Transient}_{256B,1KiB}` with counters `p50Us`, `p99Us`.

- [ ] **Step 1: Write the allocation regression** — create `libs/node/tests/integration/TransientDataPathAllocTest.cpp`:

```cpp
// TransientDataPathAllocTest.cpp — Phase 2b E2: a steady-state worker pass on a
// TRANSIENT partition — produce intake (the ring wraps and evicts), push reads
// served from the ring — never allocates.  Replaces the global operator new
// (binary-wide), so it lives in its own binary (rune_transient_alloc_regression_tests).
#include "TransientRig.h"

#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <new>

// Heap allocations made by THIS thread while armed; the first few print a
// backtrace so the culprit is named.
thread_local bool     gAllocArmed  = false;
thread_local uint64_t gArmedAllocs = 0;
void* operator new(std::size_t n) {
    if (gAllocArmed) [[unlikely]] {
        if (++gArmedAllocs <= 3) {
            gAllocArmed = false;   // backtrace itself may allocate
            void* frames[32];
            const int k = ::backtrace(frames, 32);
            std::fprintf(stderr, "E2 transient hot-path allocation of %zu bytes:\n", n);
            ::backtrace_symbols_fd(frames, k, 2);
            gAllocArmed = true;
        }
    }
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace transient_rig;

TEST(TransientDataPathRegression, SteadyStateProduceEvictAndPushReadNeverAllocateRegression) {
    Rig rig(/*ringBytes=*/256u << 10, /*maxBatches=*/100'000);
    const auto batch = batchOf(4, 200);
    const auto frame = produceFrame(1, batch, /*acks=*/0);
    int64_t next = 0;
    int64_t sub  = 1;
    auto drainReads = [&] {   // test side, disarmed (the copy-mode drain allocates the chunk)
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::PushReadResult && r.newNextOffset > next) next = r.newNextOffset;
        });
    };
    // Warm-up: every container reaches its steady size; the ring wraps and evicts many times.
    for (int i = 0; i < 2048; ++i) {
        rig.pass(&frame);
        if ((i & 3) == 3) { rig.pushRead(sub++, next); rig.pass(); }
        drainReads();
    }
    ASSERT_GT(rig.mem.logStartOffset(), 0) << "precondition: the ring evicts in steady state";
    ASSERT_GT(next, 0) << "precondition: the reads are answered from the ring";
    gArmedAllocs = 0;
    for (int i = 0; i < 512; ++i) {
        if ((i & 3) == 0) rig.pushRead(sub++, next);   // enqueue: test side, disarmed
        rig.now += 1'000'000;
        gAllocArmed = true;
        (void)rig.worker().handleIngressFrameForTest(frame.data(), frame.size(), rig.now);
        (void)rig.worker().doWork(rig.now);
        gAllocArmed = false;
        drainReads();
    }
    EXPECT_EQ(gArmedAllocs, 0u) << "a steady-state transient pass allocated (see the backtraces above)";
}
```

In `libs/node/CMakeLists.txt`, after `gtest_discover_tests(rune_transient_data_path_tests)` add:

```cmake
    # Phase 2b E2: replaces the global operator new (binary-wide) — own binary.
    add_executable(rune_transient_alloc_regression_tests
        tests/integration/TransientDataPathAllocTest.cpp)
    target_link_libraries(rune_transient_alloc_regression_tests PRIVATE
        rune_node
        rune_replication
        rune_storage
        rune_transaction
        GTest::gtest_main)
    gtest_discover_tests(rune_transient_alloc_regression_tests)
```

- [ ] **Step 2: Run it**

Run: `cmake --build build-fast -j --target rune_transient_alloc_regression_tests && ctest --test-dir build-fast -R 'SteadyStateProduceEvictAndPushRead' --output-on-failure`
Expected: PASS (Tasks 1-4 were written allocation-free: restamp append = one memcpy into the preallocated ring, `readView` = a pointer view, eviction = an index move). If it FAILS, the printed backtrace names the allocation — fix it in the named function (HARD RULE 4), never by loosening the test.

- [ ] **Step 3: Write the benchmark** — in `benchmark/WorkerProduceLatencyBench.cpp`, insert before the closing `} // namespace` of the anonymous namespace (right after `BM_WorkerRoleGate_Baseline`):

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2: produce → subscribe latency, TRANSIENT vs DISK, on the REAL
// worker path: a Produce op (append + ack) then a PushRead op at the produced
// offset (the read a subscription's next push cycle makes) → the
// PushReadResult carrying the batch.  One sample = "a produced record reaches
// a subscriber" on one worker.  Gate (E2): transient p50 and p99 <= disk's.
// ---------------------------------------------------------------------------
struct TransientWorkerFixture {
    rune::storage::InMemoryLog        mem{64u << 20, 1'000'000};
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker>           worker;

    TransientWorkerFixture() {
        PartitionConfig cfg{};
        cfg.topicId = 1; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(
            cfg, rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1}, &mem, &transport);
        worker = std::make_unique<Worker>(0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
    }
};

template <class Fix>
static void runProduceToPushRead(benchmark::State& state, Fix& fix, Log* diskLog, std::size_t payload) {
    std::vector<int64_t> samples;
    samples.reserve(1 << 16);
    int64_t nowNs = 1, corr = 0, sub = 0;
    const auto proto = makeBatch(payload);
    for (auto _ : state) {
        WorkerOp prod;
        prod.type = WorkerOp::Type::Produce;
        prod.topicId = 1; prod.partition = 0;
        prod.replica = fix.replica.get(); prod.logPtr = diskLog;   // transient: nullptr
        prod.sessionId = 1; prod.correlationId = ++corr;
        prod.lastOffsetDelta = 0; prod.recordCount = 1; prod.durabilityTier = 1;
        prod.recordBatchBytes = proto;
        WorkerOp read;
        read.type = WorkerOp::Type::PushRead;
        read.topicId = 1; read.partition = 0;
        read.replica = fix.replica.get(); read.logPtr = diskLog;
        read.subscriptionId = ++sub; read.sessionId = 1;
        read.hwBound = INT64_MAX; read.isolation = 0;

        const auto t0 = std::chrono::steady_clock::now();
        while (!fix.worker->tryEnqueueOp(prod)) fix.worker->doWork(nowNs++);
        int64_t base = -1;
        for (int i = 0; i < 5'000'000 && base < 0; ++i) {
            fix.worker->doWork(nowNs++);
            fix.worker->drainReturn([&](WorkerReturn&& r) {
                if (r.type == WorkerReturn::Type::ProduceResult && r.errorCode == 0) base = r.baseOffset;
            });
        }
        read.nextOffset = base;
        while (!fix.worker->tryEnqueueOp(read)) fix.worker->doWork(nowNs++);
        bool delivered = false;
        for (int i = 0; i < 5'000'000 && !delivered; ++i) {
            fix.worker->doWork(nowNs++);
            fix.worker->drainReturn([&](WorkerReturn&& r) {
                if (r.type == WorkerReturn::Type::PushReadResult && !r.chunk.empty()) delivered = true;
            });
        }
        const auto t1 = std::chrono::steady_clock::now();
        if (!delivered) { state.SkipWithError("the produced record never reached the PushRead"); break; }
        samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    state.SetItemsProcessed(state.iterations());
    reportPercentiles(state, samples);
}

static void BM_WorkerProduceToPushRead_Disk_256B(benchmark::State& s) {
    RealWorkerFixture f; runProduceToPushRead(s, f, f.log.get(), 256);
}
static void BM_WorkerProduceToPushRead_Transient_256B(benchmark::State& s) {
    TransientWorkerFixture f; runProduceToPushRead(s, f, nullptr, 256);
}
static void BM_WorkerProduceToPushRead_Disk_1KiB(benchmark::State& s) {
    RealWorkerFixture f; runProduceToPushRead(s, f, f.log.get(), 1024);
}
static void BM_WorkerProduceToPushRead_Transient_1KiB(benchmark::State& s) {
    TransientWorkerFixture f; runProduceToPushRead(s, f, nullptr, 1024);
}
```

and at the end of the file (after the last `BENCHMARK(…)` line):

```cpp
BENCHMARK(BM_WorkerProduceToPushRead_Disk_256B)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduceToPushRead_Transient_256B)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduceToPushRead_Disk_1KiB)->Unit(benchmark::kNanosecond)->MinTime(0.2);
BENCHMARK(BM_WorkerProduceToPushRead_Transient_1KiB)->Unit(benchmark::kNanosecond)->MinTime(0.2);
```

- [ ] **Step 4: Run the benchmark (gate)**

Run: `cmake --build build-fast -j --target rune_broker_benchmark && $(find build-fast -name rune_broker_benchmark -type f | head -1) --benchmark_filter='WorkerProduceToPushRead|WorkerProduce_Majority' --benchmark_repetitions=3 --benchmark_report_aggregates_only=true`
Expected: all four `WorkerProduceToPushRead` benchmarks complete without `SkipWithError`; for each payload, **Transient `p50Us` and `p99Us` ≤ Disk's** (the transient path has no pwrite/pread syscall). `BM_WorkerProduce_Majority_*` within noise of the pre-E2 base (run it once on the base commit first). Record the four p50/p99 pairs and the Majority numbers in the commit body. If transient is slower than disk, stop and profile — that is a gate failure, not a tolerance to widen.

- [ ] **Step 5: Full gate + commit**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure -j8`
Expected: all PASS.

Commit (replace the text of the second `-m` with the measured Step 4 lines, e.g. `Transient_256B p50=… p99=… | Disk_256B p50=… p99=…`):

```bash
git add libs/node/tests/integration/TransientDataPathAllocTest.cpp libs/node/CMakeLists.txt benchmark/WorkerProduceLatencyBench.cpp
git commit -m "test(worker): transient steady state allocates nothing; bench produce->subscribe transient vs disk" \
  -m "Bench (Step 4, p50Us/p99Us): one line per BM_WorkerProduceToPushRead_* and BM_WorkerProduce_Majority_* (before/after)." \
  -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 6: Gateway — serve transient partitions through their owning worker

**Files:**
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h` (declaration next to `routesToWorker_`, ~`:1924`)
- Modify: `libs/gateway/src/ClientGateway.cpp` — new `servedPartition_` before `bool ClientGateway::handleProduce(` (~`:2443`); `handleProduce` (~`:2623-2628` and before the INLINE path, ~`:2985`); `handleSubscribe` (~`:3629-3634`, `:3667`, `:3687`); `pushSubscriptions` (~`:3935-3948`); `appendRemoteControlRecord` (~`:4607-4611`, `:4664`); `addGroupSubscription` (~`:5869-5885`); `appendControlRecord` (~`:6040-6045`, `:6119`)
- Create: `libs/gateway/tests/gateway/TransientGatewayTest.cpp`
- Modify: `libs/gateway/CMakeLists.txt` (new target after `rune_gateway_subscribe_dedupe_tests`, ~`:213`)

**Interfaces:**
- Consumes: Part E's `rune::node::TopicSpec::persistence` / `rune::node::PersistenceMode::Transient`; `PartitionReplica::gatewayHighWatermark()` (existing acquire-load); the worker paths of Tasks 3-4.
- Produces (private, `rune::gateway::ClientGateway`):
  ```cpp
  struct ServedPartition {
      rune::storage::Log* log = nullptr;   // the disk Log, or nullptr
      bool transient = false;              // a TRANSIENT partition: no disk Log; its owning worker serves it
      explicit operator bool() const noexcept;
  };
  [[nodiscard]] ServedPartition servedPartition_(int32_t topicId, int32_t partition) const noexcept;
  ```
  `transient` is true only when the topic is `persistence=transient`, the partition is in range and its replica is registered (its HW snapshot is the subscribe clamp). The gateway never reads a ring: a transient Earliest resolves to 0 and the owning worker skips the cursor up to its ring start (Task 4).

- [ ] **Step 1: Write the failing tests** — create `libs/gateway/tests/gateway/TransientGatewayTest.cpp`:

```cpp
// TransientGatewayTest.cpp — Phase 2b E2: the gateway serves a TRANSIENT
// partition (no disk Log) through its owning worker — produce routed to the
// worker, Subscribe accepted and clamped to the HW snapshot, PushReads issued
// (legacy worker-pool mode), group subscriptions, txn markers routed — and
// refuses it (UnknownPartition) without a worker pool: no gateway-inline path
// may touch a worker-owned ring.  Deterministic: no Aeron, driven by hand.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-transient-gw-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

std::vector<uint8_t> batchOf(int recs) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < recs; ++i) b.append("k", "v", 1000 + i);
    return b.build();
}

// Gateway + topic 0 "presence" (1 partition, TRANSIENT) + its leader replica on
// an InMemoryLog; optionally a 1-worker pool (legacy mode: no per-worker egress,
// the gateway issues PushReads).
struct Rig {
    TempDir                                   dir;
    rune::storage::InMemoryLog                mem{1 << 20, 10'000};   // outlives the replica
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    std::unique_ptr<TopicRegistry>            registry;
    std::unique_ptr<rune::node::WorkerPool>   pool;
    ClientGateway                             gw;
    PartitionReplica*                         rep = nullptr;
    std::map<int64_t, pc::ProduceResponse>    produced;     // by correlationId
    std::map<int64_t, pc::SubscribeResponse>  subscribed;   // by correlationId
    std::vector<pc::Records>                  records;      // baseOffset / subscriptionId only
    int64_t                                   now  = 1'000'000'000;
    int64_t                                   corr = 0;

    explicit Rig(bool workers)
        : gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
              onFrame(b, n, /*sessionEgress=*/false);
              return 1;
          }, 1)) {
        rune::node::TopicSpec ts;
        ts.name = "presence"; ts.partitionCount = 1; ts.topicId = 0;
        ts.persistence = rune::node::PersistenceMode::Transient;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        PartitionConfig c;
        c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                    std::vector<int32_t>{1}, &mem, &transport);
        r->setExternalFollowerFlush(true);
        rep = r.get();
        reg.add(0, 0, std::move(r));
        gw.setTopicRegistry(registry.get());
        gw.setReplicaRegistry(&reg);
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n, true); });
        if (workers) {
            pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1));
            pool->setRegistry(&reg);
            pool->assignFromRegistry(reg);
            gw.setWorkerPool(pool.get());
        }
    }
    void onFrame(const uint8_t* b, std::size_t n, bool sessionEgress) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
        case pc::TemplateId::ProduceResponse:
            if (auto r = pc::ProduceResponse::decode(b, n)) produced[r->correlationId] = *r;
            break;
        case pc::TemplateId::SubscribeResponse:
            if (auto r = pc::SubscribeResponse::decode(b, n)) subscribed[r->correlationId] = *r;
            break;
        case pc::TemplateId::Records:
            if (sessionEgress) if (auto r = pc::Records::decode(b, n)) records.push_back(*r);
            break;
        default: break;
        }
    }
    void pass() {
        now += 1'000'000;
        if (pool) (void)pool->worker(0).doWork(now);
        gw.drainWorkerReturnsForTest(now);
    }
    int64_t produce(int64_t sid, int recs, uint8_t acks = 1) {
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = ++corr; msg.topicId = 0; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = acks; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
        const auto batch = batchOf(recs);
        msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        gw.applyProduceForTest(frame.data(), frame.size());
        return corr;
    }
    std::optional<pc::ProduceResponse> awaitProduced(int64_t c) {
        for (int i = 0; i < 64 && !produced.count(c); ++i) pass();
        const auto it = produced.find(c);
        return it == produced.end() ? std::nullopt : std::optional<pc::ProduceResponse>(it->second);
    }
    void subscribe(int64_t sid, int64_t fromOffset, int64_t subCorr) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = subCorr; s.topicId = 0; s.partition = 0;
        s.partitionKnown = 1; s.fromOffset = fromOffset; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
    }
    /// Push cycles (PushRead → worker → PushReadResult → Records) until n frames (bounded).
    bool pushUntil(std::size_t n) {
        for (int i = 0; i < 64 && records.size() < n; ++i) { gw.pushSubscriptionsForTest(); pass(); }
        return records.size() >= n;
    }
};

constexpr int16_t kOk               = 0;
constexpr int16_t kUnknownPartition = static_cast<int16_t>(pc::ErrorCode::UnknownPartition);

} // namespace

TEST(TransientGatewayRegression, SubscribeToATransientPartitionIsAcceptedRegression) {
    Rig r(/*workers=*/true);
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, /*Earliest=*/-2, 41);
    ASSERT_TRUE(r.subscribed.count(41));
    EXPECT_EQ(r.subscribed[41].errorCode, kOk) << "was UnknownPartition: the topic has no disk Log";
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[41].subscriptionId), 0);
}

TEST(TransientGatewayRegression, ProduceThroughTheWorkerAndPushToTheSubscriberRegression) {
    Rig r(true);
    const int64_t sid = r.gw.injectTestSession(0);
    const auto p1 = r.awaitProduced(r.produce(sid, 3, /*acks=all*/1));
    ASSERT_TRUE(p1.has_value());
    ASSERT_EQ(p1->errorCode, kOk) << "was UnknownPartition: the topic has no disk Log";
    EXPECT_EQ(p1->baseOffset, 0);
    const auto p2 = r.awaitProduced(r.produce(sid, 2, 1));
    ASSERT_TRUE(p2.has_value());
    ASSERT_EQ(p2->errorCode, kOk);
    EXPECT_EQ(p2->baseOffset, 3);
    EXPECT_EQ(r.mem.logEndOffset(), 5) << "the owning worker appended to the ring";

    r.subscribe(sid, -2, 42);
    ASSERT_TRUE(r.pushUntil(2)) << "no Records pushed for a transient subscription";
    EXPECT_EQ(r.records[0].baseOffset, 0);
    EXPECT_EQ(r.records[1].baseOffset, 3);
    EXPECT_EQ(r.records[0].subscriptionId, r.subscribed[42].subscriptionId);
}

TEST(TransientGatewayRegression, SubscribeLatestAndExplicitOffsetClampToTheHighWatermarkRegression) {
    Rig r(true);
    const int64_t sid = r.gw.injectTestSession(0);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 1)).value().errorCode, kOk);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 1)).value().errorCode, kOk);
    for (int i = 0; i < 4; ++i) r.pass();   // the worker published its HW
    ASSERT_EQ(r.rep->gatewayHighWatermark(), 2);
    r.subscribe(sid, /*Latest=*/-1, 101);
    r.subscribe(sid, 1, 102);
    r.subscribe(sid, 99, 103);
    for (int64_t c : {101, 102, 103}) ASSERT_EQ(r.subscribed.at(c).errorCode, kOk);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[101].subscriptionId), 2) << "Latest replays old records";
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[102].subscriptionId), 1);
    EXPECT_EQ(r.gw.subscriptionNextOffsetForTest(r.subscribed[103].subscriptionId), 2) << "beyond the HW clamps to it";
}

TEST(TransientGatewayRegression, GroupSubscriptionOnATransientPartitionIsPushedRegression) {
    Rig r(true);
    const int64_t sid = r.gw.injectTestSession(0);
    ASSERT_EQ(r.awaitProduced(r.produce(sid, 2)).value().errorCode, kOk);
    const int64_t subId = r.gw.addGroupSubscription(sid, 0, 0, /*fromOffset=*/0, /*isolation=*/0,
                                                    /*pendingCarrier=*/false);
    ASSERT_GE(subId, 0) << "a group member could not be assigned a transient partition (was: -1)";
    ASSERT_TRUE(r.pushUntil(1));
    EXPECT_EQ(r.records[0].subscriptionId, subId);
    EXPECT_EQ(r.records[0].baseOffset, 0);
}

TEST(TransientGatewayRegression, TxnMarkerForATransientPartitionIsRoutedToTheWorkerRegression) {
    Rig r(true);
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    EXPECT_EQ(r.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), /*producerId=*/7, /*commit=*/true,
                                       /*coordinatorEpoch=*/0),
              ClientGateway::kControlRecordAsync) << "was -1 (dropped): no disk Log";
    for (int i = 0; i < 8; ++i) r.pass();
    EXPECT_EQ(r.mem.logEndOffset(), 1) << "the owning worker appended the marker to the ring";
}

// Pins "never inline": without a worker pool nothing may touch a worker-owned
// ring — every path refuses the transient partition explicitly.
TEST(TransientGateway, WithoutAWorkerPoolTransientIsRefusedNeverInline) {
    Rig r(/*workers=*/false);
    const int64_t sid = r.gw.injectTestSession(0);
    const int64_t c = r.produce(sid, 1);
    ASSERT_TRUE(r.produced.count(c));
    EXPECT_EQ(r.produced[c].errorCode, kUnknownPartition);
    EXPECT_EQ(r.mem.logEndOffset(), 0);
    r.subscribe(sid, -2, 9);
    ASSERT_TRUE(r.subscribed.count(9));
    EXPECT_EQ(r.subscribed[9].errorCode, kUnknownPartition);
    EXPECT_EQ(r.gw.addGroupSubscription(sid, 0, 0, 0, 0, false), -1);
}
```

In `libs/gateway/CMakeLists.txt`, after `gtest_discover_tests(rune_gateway_subscribe_dedupe_tests)` add:

```cmake

    # Phase 2b E2: TRANSIENT partitions served through their owning worker — no Aeron.
    add_executable(rune_gateway_transient_tests tests/gateway/TransientGatewayTest.cpp)
    target_link_libraries(rune_gateway_transient_tests PRIVATE rune_gateway rune_node rune_replication rune_storage
                          rune_transaction spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_transient_tests)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_gateway_transient_tests && ctest --test-dir build-fast -R 'TransientGateway' --output-on-failure`
Expected: every `TransientGatewayRegression.*` FAILS (Subscribe/Produce answered `UnknownPartition` = 8; `addGroupSubscription` returns -1; `appendControlRecord` returns -1). `TransientGateway.WithoutAWorkerPoolTransientIsRefusedNeverInline` PASSES (pins the rule the fix must keep).

- [ ] **Step 3: Implement — `ClientGateway.h`**

Right after

```cpp
    [[nodiscard]] bool routesToWorker_(int32_t topicId) const noexcept {
        return workerPool_ != nullptr && (!perWorkerIngressActive_ || topicId >= 0);
    }
```
add:

```cpp
    // Phase 2b E2: a partition this gateway can serve — a disk Log, or a
    // TRANSIENT partition (no disk Log; its InMemoryLog is owned by the
    // replica's worker and is NEVER touched here).  `transient` is set only
    // when the topic is persistence=transient, the partition is in range and
    // its replica is registered (its published HW is the subscribe clamp).
    struct ServedPartition {
        rune::storage::Log* log       = nullptr;
        bool                transient = false;
        [[nodiscard]] explicit operator bool() const noexcept { return log != nullptr || transient; }
    };
    [[nodiscard]] ServedPartition servedPartition_(int32_t topicId, int32_t partition) const noexcept;
```

- [ ] **Step 4: Implement — `ClientGateway.cpp`**

Insert right before `bool ClientGateway::handleProduce(const uint8_t* buf, std::size_t len)`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E2: servedPartition_ — a disk partition (its Log) or a TRANSIENT
// partition (no disk Log: served only by its owning worker).  One registry
// probe for a disk partition (unchanged cost); the topic + replica probes run
// only when the partition has no disk Log.  Control thread.
// ---------------------------------------------------------------------------
ClientGateway::ServedPartition ClientGateway::servedPartition_(int32_t topicId, int32_t partition) const noexcept
{
    ServedPartition s;
    if (!topicRegistry_) return s;
    s.log = topicRegistry_->log(topicId, partition);
    if (s.log != nullptr) return s;
    const rune::node::TopicSpec* spec = topicRegistry_->topic(topicId);
    if (spec == nullptr || spec->persistence != rune::node::PersistenceMode::Transient) return s;
    if (partition < 0 || partition >= spec->partitionCount) return s;
    s.transient = replicaRegistry_ != nullptr && replicaRegistry_->lookup(topicId, partition) != nullptr;
    return s;
}

```

`handleProduce` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->log(msg.topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }
```
with
```cpp
    // Phase 2b E2: a TRANSIENT partition has no disk Log — routed to its owning
    // worker below (op.logPtr = nullptr: the worker appends to the replica's ring).
    const ServedPartition served = servedPartition_(msg.topicId, partition);
    if (!served) {
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }
    rune::storage::Log* log = served.log;   // nullptr ⟺ transient
```
and right before the comment block that starts `    // ---------------------------------------------------------------------------` / `    // INLINE path (pre-E2 / no WorkerPool):` (just after the `if (routeToWorker) { … return true; }` block) insert:

```cpp
    // Phase 2b E2: a TRANSIENT partition is appended only by its owning worker —
    // never inline (the gateway never touches a worker-owned ring).  Reached
    // only without a worker pool (unit rigs).
    if (log == nullptr) [[unlikely]] {
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }

```

`handleSubscribe` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->log(msg.topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }
```
with
```cpp
    // Phase 2b E2: a TRANSIENT partition (no disk Log) is pushed by its owning
    // worker only — refused without a worker pool (no inline push path).
    const ServedPartition served = servedPartition_(msg.topicId, partition);
    if (!served || (served.transient && !workerPool_)) {
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ transient
```
replace `    const int64_t leo = log->logEndOffset();` with
```cpp
    // E2: a transient partition's replica is registered (servedPartition_), so
    // its clamp is the published HW below — the ring itself is never read here.
    const int64_t leo = log != nullptr ? log->logEndOffset() : 0;
```
and replace `    const int64_t logStart = log->logStartOffset();` with
```cpp
    // E2: a transient ring's start is worker-owned state (never read cross-
    // thread): Earliest resolves to 0 and the owning worker moves a cursor below
    // its ring start up to it (TRANSIENT_EVICTED, Worker::skipEvictedCursor_).
    const int64_t logStart = log != nullptr ? log->logStartOffset() : 0;
```

`pushSubscriptions` — replace

```cpp
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        if (!log) continue; // registry removed or not set — skip
```
with
```cpp
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        // Phase 2b E2: a TRANSIENT partition (no disk Log) is read by its owning
        // worker only (the PushRead below) — never by the inline path.
        if (!log && (!workerPool_ || !servedPartition_(sub.topicId, sub.partition).transient))
            continue; // registry removed or not set — skip
```
then replace
```cpp
            hw = replica ? replica->gatewayHighWatermark() : log->logEndOffset();
```
with
```cpp
            hw = replica ? replica->gatewayHighWatermark() : (log ? log->logEndOffset() : 0);
```
and in the `else` branch below it replace `            hw = log->logEndOffset();` with `            hw = log ? log->logEndOffset() : 0;`.

`appendRemoteControlRecord` — replace

```cpp
    rune::storage::Log* log = topicRegistry_ ? topicRegistry_->log(w.topicId, w.partition) : nullptr;
    if (log == nullptr || w.topicId < 0 || w.marker.empty()) {
```
with
```cpp
    const ServedPartition served = servedPartition_(w.topicId, w.partition);   // E2: transient partitions too
    rune::storage::Log* const log = served.log;
    if (!served || w.topicId < 0 || w.marker.empty()) {
```
and replace
```cpp
    const int64_t base = appendControlRecordInline_(w.topicId, w.partition, w.marker, log);
```
with
```cpp
    // E2: a transient partition has no inline append (worker-owned ring) — NotLeader, the coordinator retries.
    const int64_t base = log != nullptr ? appendControlRecordInline_(w.topicId, w.partition, w.marker, log) : -1;
```

`addGroupSubscription` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->log(topicId, partition);
    if (!log) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown topicId={} partition={}",
                     topicId, partition);
        return -1;
    }

    // Clamp fromOffset to [0, bound] where bound is HW when a replica is
    // wired (GR-6), else LEO.  Negative fromOffset is clamped to 0 (log start).
    const int64_t leo = log->logEndOffset();
```
with
```cpp
    // Phase 2b E2: a TRANSIENT partition (no disk Log) is pushed by its owning
    // worker only — refused without a worker pool.
    const ServedPartition served = servedPartition_(topicId, partition);
    if (!served || (served.transient && !workerPool_)) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown topicId={} partition={}",
                     topicId, partition);
        return -1;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ transient

    // Clamp fromOffset to [0, bound] where bound is HW when a replica is
    // wired (GR-6), else LEO.  Negative fromOffset is clamped to 0 (log start;
    // E2: a transient cursor below the ring start is moved up by the worker).
    const int64_t leo = log != nullptr ? log->logEndOffset() : 0;
```

`appendControlRecord` — replace

```cpp
    rune::storage::Log* log = topicRegistry_->log(topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway::appendControlRecord: unknown topicId={} partition={} — dropped",
                      topicId, partition);
        return -1;
    }
```
with
```cpp
    // Phase 2b E2: a TRANSIENT partition's markers ride the same worker / remote
    // paths below (the worker appends the control batch to the replica's ring).
    const ServedPartition served = servedPartition_(topicId, partition);
    if (!served) {
        spdlog::debug("ClientGateway::appendControlRecord: unknown topicId={} partition={} — dropped",
                      topicId, partition);
        return -1;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ transient
```
and replace the function's last statement `    return appendControlRecordInline_(topicId, partition, marker, log);` with:

```cpp
    if (log == nullptr) [[unlikely]] {   // E2: transient without a worker pool — no inline append
        spdlog::debug("ClientGateway::appendControlRecord: transient topicId={} partition={} without a "
                      "worker pool — dropped", topicId, partition);
        return -1;
    }
    return appendControlRecordInline_(topicId, partition, marker, log);
```

- [ ] **Step 5: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'TransientGateway|ClientGateway|SubscribeDedupe|GatewaySubscribe|GatewayProduce|ReadCommittedPush|GroupPush|F15|D9' --output-on-failure`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add libs/gateway/include/rune/gateway/ClientGateway.h libs/gateway/src/ClientGateway.cpp libs/gateway/tests/gateway/TransientGatewayTest.cpp libs/gateway/CMakeLists.txt
git commit -m "feat(gateway): serve transient partitions through their owning worker (produce, subscribe, groups, markers)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: BrokerNode over real Aeron — shard, legacy, 3-node acks=all, restart; docs

**Files:**
- Create: `libs/node/tests/integration/TransientTopicDataPathTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (new target right after `gtest_discover_tests(rune_shard_mode_tests …)`, ~`:1347`, inside the same `if(aeron_FOUND)` block)
- Modify: `docs/SELF-HOSTING.md` (the "Transient topics" section Part E Task 9 added)

**Interfaces:**
- Consumes: everything above; `rune::node::test::ShardTestClient`, `freeUdpPortBlocks`, `freeUdpPort`, `makeBatch` (`libs/node/tests/support/ShardTestClient.h`), `leoOnOwner`, `hasReplica` (`support/OwnerThreadTestHelpers.h`); `TopicSpec.persistence = PersistenceMode::Transient` (Part E).
- Produces: nothing consumed later (end-to-end proof in-process).

- [ ] **Step 1: Write the tests** — create `libs/node/tests/integration/TransientTopicDataPathTest.cpp`:

```cpp
// TransientTopicDataPathTest.cpp — Phase 2b E2, in-process BrokerNodes over
// REAL Aeron UDP: a TRANSIENT topic (static RUNE_TOPICS persistence=transient,
// topic 0 "tt") produces and is consumed in shard mode (the default: produce on
// the worker's ingress, Records pushed by the worker) and in legacy mode
// (produce + PushRead through the gateway); acks=all is replicated to a
// majority before the ack (3 nodes); a restart keeps the topic and empties it.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PersistenceMode.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_tt_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;

struct Ports {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static Ports discover(int nodes) {
        Ports p;
        auto blocks = freeUdpPortBlocks(3 * nodes, 1);
        std::vector<std::pair<int, int>> taken;
        for (uint16_t b : blocks) taken.push_back({b, b + 1});
        auto single = [&taken] {
            for (int attempt = 0; attempt < 2000; ++attempt) {
                const int q = freeUdpPort();
                bool clash = q == 0;
                for (const auto& [lo, hi] : taken) clash = clash || (q >= lo && q < hi);
                if (!clash) { taken.push_back({q, q + 1}); return static_cast<uint16_t>(q); }
            }
            throw std::runtime_error("Ports: no collision-free UDP port");
        };
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(single()); p.client.push_back(single()); p.repl.push_back(single());
            p.ib.push_back(blocks[3 * i]); p.rb.push_back(blocks[3 * i + 1]); p.eg.push_back(blocks[3 * i + 2]);
        }
        return p;
    }
};

NodeConfig makeConfig(int nodeId, const fs::path& dataDir, const Ports& p, int nodes, bool shard) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = shard ? NodeConfig::WorkerMode::Shard : NodeConfig::WorkerMode::Legacy;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = shard;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;   // tests never auto-pin busy-spinning workers (ctest -j)
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = 1;
        pc.egressControlBasePort = p.eg[i];
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t;
    t.name = "tt"; t.partitionCount = 1; t.topicId = 0;
    t.persistence = PersistenceMode::Transient;
    cfg.topics.push_back(t);
    return cfg;
}

std::string gatewayChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[node]);
}
std::string workerIngressChannel(const Ports& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.ib[node]);
}
std::size_t pushedAt(const ShardTestClient& c, int64_t base) {
    std::size_t n = 0;
    for (const auto& r : c.records()) n += r.baseOffset == base ? 1u : 0u;
    return n;
}

} // namespace

TEST(TransientTopicDataPathRegression, ShardProduceAndSubscribeEarliestLatestOffsetRegression) {
    TmpDir dir{"shard"}, cdir{"shard_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        for (int64_t want : {0, 3, 6}) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0,
                                        /*acks=*/1, makeBatch(3));
            ASSERT_TRUE(ack.has_value()) << "a transient produce on the worker shard was not acked";
            EXPECT_EQ(ack->baseOffset, want);
        }
        // Earliest: the whole ring.
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 6) >= 1; })) << "an earliest subscription got nothing";
        EXPECT_EQ(pushedAt(c, 0), 1u);
        EXPECT_EQ(pushedAt(c, 3), 1u);
        // An explicit offset.
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*fromOffset=*/6));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 6) >= 2; })) << "an offset subscription got nothing";
        EXPECT_EQ(pushedAt(c, 0), 1u) << "the offset subscription replayed below its offset";
        // Latest: never replays — produce until its subscription delivers; its FIRST batch is new.
        std::set<int64_t> before;
        for (const auto& r : c.records()) before.insert(r.subscriptionId);
        ASSERT_EQ(before.size(), 2u);
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Latest=*/-1));
        int64_t latestFirstBase = -1;
        for (int i = 0; i < 20 && latestFirstBase < 0; ++i) {
            ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                         makeBatch(3)).has_value());
            (void)c.pollUntil([&] {
                for (const auto& r : c.records())
                    if (!before.count(r.subscriptionId)) { latestFirstBase = r.baseOffset; return true; }
                return false;
            });
        }
        ASSERT_GE(latestFirstBase, 0) << "the latest subscription never delivered";
        EXPECT_GE(latestFirstBase, 9) << "a Latest subscription replayed old records";
    }
    node.stop();
}

TEST(TransientTopicDataPathRegression, LegacyModeProduceAndSubscribeRegression) {
    TmpDir dir{"legacy"}, cdir{"legacy_c"};
    const auto ports = Ports::discover(1);
    BrokerNode node{makeConfig(0, dir.path, ports, 1, /*shard=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        for (int64_t want : {0, 2}) {
            auto ack = c.produceUntilOk(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*acks=*/1, makeBatch(2));
            ASSERT_TRUE(ack.has_value()) << "a transient produce through the gateway was not acked";
            EXPECT_EQ(ack->baseOffset, want);
        }
        ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
        ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 2) >= 1; })) << "the gateway never pushed the transient topic";
        EXPECT_EQ(pushedAt(c, 0), 1u);
    }
    node.stop();
}

TEST(TransientTopicDataPathRegression, ThreeNodeAcksAllIsOnAMajorityBeforeTheAckRegression) {
    TmpDir d0{"m0"}, d1{"m1"}, d2{"m2"}, cdir{"mc"};
    const auto ports = Ports::discover(3);
    BrokerNode n0{makeConfig(0, d0.path, ports, 3, true)};
    BrokerNode n1{makeConfig(1, d1.path, ports, 3, true)};
    BrokerNode n2{makeConfig(2, d2.path, ports, 3, true)};
    n0.start(); n1.start(); n2.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms));
    int32_t leader = -1;
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        for (int i = 0; i < 200 && leader < 0; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            if (leader < 0) std::this_thread::sleep_for(50ms);   // metadata settling (poll interval)
        }
        ASSERT_GE(leader, 0) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        for (int b = 0; b < 5; ++b) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader), kClientStream, sid, 0, 0,
                                        /*acks=all*/1, makeBatch(4));
            ASSERT_TRUE(ack.has_value()) << "acks=all batch " << b << " on a transient topic not committed";
        }
    }
    // While the nodes run (the data is in memory only): every acked record is on a majority.
    int holders = 0;
    for (BrokerNode* n : {&n0, &n1, &n2})
        holders += (hasReplica(*n, 0, 0) && leoOnOwner(*n, 0, 0) >= 20) ? 1 : 0;
    EXPECT_GE(holders, 2) << "an acks=all ack on a transient topic before a majority held the records";
    n0.stop(); n1.stop(); n2.stop();
}

TEST(TransientTopicDataPathRegression, RestartKeepsTheTopicAndEmptiesItRegression) {
    TmpDir dir{"restart"}, c1{"restart_c1"}, c2{"restart_c2"};
    const auto ports = Ports::discover(1);
    const auto cfg = makeConfig(0, dir.path, ports, 1, /*shard=*/true);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        {
            ShardTestClient c(c1.path);
            const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
            ASSERT_GT(sid, 0);
            for (int i = 0; i < 2; ++i)
                ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1,
                                             makeBatch(3)).has_value());
        }
        node.stop();
    }
    EXPECT_FALSE(fs::exists(dir.path / "topics" / "tt")) << "a transient topic never writes a partition directory";
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(10'000ms));
        {
            ShardTestClient c(c2.path);
            const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
            ASSERT_GT(sid, 0);
            auto ack = c.produceUntilOk(workerIngressChannel(ports, 0), kClientStream, sid, 0, 0, 1, makeBatch(3));
            ASSERT_TRUE(ack.has_value()) << "the transient topic must exist after a restart";
            EXPECT_EQ(ack->baseOffset, 0) << "a transient topic restarts EMPTY";
            ASSERT_TRUE(c.subscribe(gatewayChannel(ports, 0), kClientStream, sid, 0, 0, /*Earliest=*/-2));
            ASSERT_TRUE(c.pollUntil([&] { return pushedAt(c, 0) >= 1; }));
            EXPECT_EQ(c.records().size(), 1u) << "pre-restart records came back";
        }
        node.stop();
    }
}
```

In `libs/node/CMakeLists.txt`, right after

```cmake
        gtest_discover_tests(rune_shard_mode_tests PROPERTIES TIMEOUT 360 RESOURCE_LOCK rune_udp_ports)
```
add:

```cmake

        # Phase 2b E2: TRANSIENT topics end to end in-process over real Aeron —
        # shard + legacy produce/subscribe, 3-node acks=all, restart (empty).
        add_executable(rune_transient_topic_data_path_tests tests/integration/TransientTopicDataPathTest.cpp)
        target_include_directories(rune_transient_topic_data_path_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_transient_topic_data_path_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_transient_topic_data_path_tests PROPERTIES TIMEOUT 360 RESOURCE_LOCK rune_udp_ports)
```

- [ ] **Step 2: Run them**

Run: `cmake --build build-fast -j --target rune_transient_topic_data_path_tests && ctest --test-dir build-fast -R 'TransientTopicDataPath' --output-on-failure`
Expected: all four PASS on top of Tasks 1-6. (Against `feature/materialized-views` + Part E only, every one FAILS: shard produce answers Internal, the legacy produce/subscribe answer UnknownPartition.) A failure here is a real integration gap: fix it in the owning layer with its own `*Regression*` test before continuing.

- [ ] **Step 3: Docs** — in `docs/SELF-HOSTING.md`, in the "### Transient topics" section, replace the line

```markdown
- Use `durability=majority` (default) or `all-in-sync` if a transient topic must survive a node death.
```
with
```markdown
- Use `durability=majority` (default) or `all-in-sync` if a transient topic must survive a node death.
- Produce (every `acks` mode, idempotent and transactional producers), push subscriptions from
  earliest / latest / an offset, consumer groups and `read_committed` work exactly as on disk topics,
  in both worker modes (`RUNE_WORKER_MODE=shard`, the default, and `legacy`). `tier=strict` acks like
  `majority` (there is nothing to fsync).
- **Retention is the in-memory ring:** `RUNE_MEMORY_LOG_RING_BYTES` per partition (default 8 MiB, at
  most 1 000 000 batches). When it is full the oldest **committed** records (below the high watermark)
  are evicted; a consumer whose position was evicted resumes at the oldest record still held (counted
  and logged). Records not yet replicated are never evicted: while the ring is full of them a produce
  is refused with a retriable `NotLeader` (the client retries) until the followers catch up. A batch
  larger than the whole ring is refused `MessageTooLarge`.
- A node that rejoins a transient partition whose leader already evicted the oldest records restarts
  its copy at the leader's oldest record and catches up from there.
```

- [ ] **Step 4: Full gate + commit**

Run: `ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'TransientTopicDataPath|ShardMode|RuntimeTopicRecovery|TransientTopic_' --output-on-failure`
Expected: all PASS.

```bash
git add libs/node/tests/integration/TransientTopicDataPathTest.cpp libs/node/CMakeLists.txt docs/SELF-HOSTING.md
git commit -m "test(node): transient topics end to end over Aeron (shard, legacy, 3-node acks=all, restart); docs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 8: e2e — define a transient topic, produce + subscribe, restart → exists and empty (Part E Task 10, executed here)

Part E wrote this leg and marked it BLOCKED on the data path (E's Spec question 1); Tasks 1-7 are that data path. The code below is E's Task 10, reproduced in full so this task is self-contained. **If E's Task 10 commits already exist on the branch, skip Steps 1-3 and run Step 4.** The spec's e2e says "console defines a transient topic"; the console is Part D (after E/E2): this leg defines it through `Client::defineTopicAndWait(..., Persistence::Transient, …)` — exactly what D's console calls — and D re-runs it through `rune console`. The docker broker runs the default `RUNE_WORKER_MODE=shard`, so this leg proves the shard data path end to end over a real client.

**Files:**
- Modify: `rune-client-cpp/examples/view_probe.cpp` (setup step 6; verify_restart step (e); header comment)
- Modify: `docker/view-engine-e2e.sh:283-306` (extract/handoff), `:343-355` (verify env), `:396-405` (summary)

**Interfaces:**
- Consumes: Part E `rune::client::Persistence::Transient` + `Client::defineTopicAndWait(name, partitions, keyField, validate, blob, Persistence, wait)`; existing probe helpers `sendAndWaitAck`, `encodeCharacter`, `characterDescriptor`, `getEnvI64`, locals `blob`, `wait`, `timeoutMs`, `boundMs`, `c` (all in `examples/view_probe.cpp`); client `Subscription::position()`, `unsubscribe()`, `finished()`.
- Produces: handoff variable `VIEW_TT_TOPIC_ID` / env `RUNE_VIEW_TT_TOPIC_ID`.

- [ ] **Step 1: Probe — setup step 6** — in `examples/view_probe.cpp` add `#include <rune/client/subscriber/Subscription.h>`; add these helpers above `main`:

```cpp
// Subscribe from EARLIEST and wait (bounded) until the subscription's published
// position reaches `atLeast`; returns the final position after unsubscribe +
// finished().  position() comes off the subscription's own status ring — no
// state is shared with the handler (which runs on the client's duty thread).
static int64_t subscribeEarliestUntil(Client& c, int32_t topicId, int64_t atLeast, int boundMs) {
    SubscribeOptions opts;
    opts.from = From::earliest();
    auto sub = c.subscribe(TopicHandle{topicId, 1}, /*partition=*/0, opts, [](const Record&) {});
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (sub.position() < atLeast && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(5ms);   // poll interval, not correctness
    sub.unsubscribe();
    const auto fin = std::chrono::steady_clock::now() + 5s;
    while (!sub.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
    return sub.position();
}

static bool produceOne(Client& c, int32_t topicId, int64_t key, int timeoutMs, int64_t& offsetOut) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::All;   // transient: replicated before ack
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    auto p = c.producer(pcfg);
    Completions cq(c);
    Completion  done;
    const auto value = encodeCharacter(key, 1, /*version=*/1);
    const bool ok = sendAndWaitAck(cq, p, TopicHandle{topicId, 1}, Key(static_cast<uint64_t>(key)),
                                   std::span<const uint8_t>(value), 1, timeoutMs, done);
    offsetOut = done.value;
    return ok && done.status == OpStatus::Ok;
}
```

In `mode == "setup"`, before the handoff block insert:

```cpp
            // ---- 6. Phase 2b E/E2: a TRANSIENT topic — define, produce, subscribe ----
            DefineTopicResult tt = c.defineTopicAndWait(
                "presence_t", /*partitions=*/1, /*keyField=*/"id", /*validate=*/false,
                std::span<const uint8_t>(blob), Persistence::Transient, wait);
            if (tt.errorCode != 0) { std::cerr << "PROBE: FAIL defineTopic(presence_t transient) errorCode=" << tt.errorCode << "\n"; return 50; }
            for (int64_t k = 1; k <= 3; ++k) {
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, k, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t)\n"; return 51; }
            }
            const int64_t pos = subscribeEarliestUntil(c, tt.topicId, 3, boundMs);
            if (pos < 3) { std::cerr << "PROBE: FAIL subscribe(presence_t) position=" << pos << " (want >= 3)\n"; return 52; }
            std::cerr << "PROBE: transient topic presence_t topicId=" << tt.topicId << " produced+consumed 3\n";
```

and add to the handoff lines `std::cout << "VIEW_TT_TOPIC_ID=" << tt.topicId << "\n";`.

- [ ] **Step 2: Probe — verify_restart step (e)** — read `const int32_t ttTopic = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TT_TOPIC_ID", -1));` with the other env reads, and before the final `VIEW-ENGINE-E2E: OK` line insert:

```cpp
            // ---- (e) Phase 2b E/E2: the transient topic EXISTS and is EMPTY ------
            if (ttTopic < 0) { std::cerr << "PROBE: FAIL missing RUNE_VIEW_TT_TOPIC_ID\n"; return 60; }
            {
                // Metadata survived: re-defining answers TopicExists with the SAME id.
                const auto blob = rune::codec::serialize(characterDescriptor());
                DefineTopicResult again = c.defineTopicAndWait(
                    "presence_t", 1, "id", false, std::span<const uint8_t>(blob),
                    Persistence::Transient, wait);
                if (again.errorCode != 25 || again.topicId != ttTopic) {
                    std::cerr << "PROBE: FAIL presence_t after restart errorCode=" << again.errorCode
                              << " topicId=" << again.topicId << " (want TopicExists, id " << ttTopic << ")\n";
                    return 61;
                }
                // Data did not: the first post-restart record lands at offset 0.
                int64_t off = -1;
                if (!produceOne(c, ttTopic, 9, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t) after restart\n"; return 62; }
                if (off != 0) { std::cerr << "PROBE: FAIL presence_t not empty after restart: first offset=" << off << "\n"; return 63; }
                const int64_t pos = subscribeEarliestUntil(c, ttTopic, 1, boundMs);
                if (pos != 1) { std::cerr << "PROBE: FAIL presence_t subscribe after restart position=" << pos << " (want 1)\n"; return 64; }
                std::cerr << "PROBE: presence_t exists after restart, empty, produce+subscribe OK\n";
            }
```

Add to the file header's mode description: `6. (Phase 2b E/E2) defineTopicAndWait("presence_t", Persistence::Transient), produce 3 (acks=All), subscribe from earliest → position 3; after restart: TopicExists with the same id, first produce at offset 0, subscribe → position 1.`

- [ ] **Step 3: Script** — in `docker/view-engine-e2e.sh` add `VIEW_TT_TOPIC_ID="$(extract VIEW_TT_TOPIC_ID)"` after `VIEW_FP_AGG=…`; add `VIEW_TT_TOPIC_ID` to the `for v in …` required list; append ` transientTopicId=${VIEW_TT_TOPIC_ID}` to the "Setup OK" log; add `-e RUNE_VIEW_TT_TOPIC_ID="${VIEW_TT_TOPIC_ID}" \` to the `run_client verify_restart` call; add `log "  -> transient topic presence_t: produce+subscribe, restart -> exists (TopicExists, same id) and empty (first offset 0)"` to the final summary.

- [ ] **Step 4: Run**

Run (from the broker repo root, sibling layout `../rune-protocol ../rune-codec ../rune-crypto ../rune-fr ../rune-ql ../rune-client-cpp` on the `feat/phase2b-e2-transient-data-path` branches, E's branches beneath them): `bash docker/view-engine-e2e.sh`
Expected: `VIEW-ENGINE-E2E: OK, exit 0.` with both `presence_t` lines in the probe log (`produced+consumed 3`, `exists after restart, empty, produce+subscribe OK`).

- [ ] **Step 5: Commit** (two repos; client regression gate first: `ctest --test-dir build -R Regression --output-on-failure -j8`)

```bash
git -C ../rune-client-cpp add examples/view_probe.cpp
git -C ../rune-client-cpp commit -m "test(e2e): transient topic leg in view_probe (define, produce, subscribe, restart empty)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git add docker/view-engine-e2e.sh
git commit -m "test(e2e): view-engine e2e carries the transient-topic leg (produce + subscribe + restart)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Self-review

- **Coverage of the user decision / spec E:** produce in all ack modes — shard ingress (Task 3 `ShardIngressProduce…`, acks=all), op ring / legacy (Task 3 `GatewayRoutedProduceOp…`, Task 6 gateway produce, Task 7 legacy), acks=0 (Task 3 full-ring tests); **acks=all replicated to a majority before the ack** — Task 2 (ring never evicts above the HW), Task 7 `ThreeNodeAcksAll…` (majority holds every acked record, measured while running), plus Part E's replica-level gate; push subscribe from **earliest / latest / offset** — Task 6 (clamps), Task 7 shard (all three, real Aeron), Task 7 legacy (earliest); **consumer groups** — Task 6 `GroupSubscription…` (the coordinator's `addGroupSubscription` path) — shard-mode group pushes ride the same owned-subscription path Task 4 fixes and Task 7 exercises for plain subscriptions; **read_committed** — Task 4 (open txn withheld until the commit marker), markers routed by the gateway (Task 6) and refused retriably on a full ring (Task 3); **both worker modes** — Tasks 3/4/7; **zero steady-state allocation** — Task 5 (`SteadyStateProduceEvictAndPushRead…`), Task 2 (`TransientFollowerApplyAllocatesNothing`); **same latency class** — Task 5 benchmark gate; **full-ring behaviour** — Tasks 1-4, docs Task 7, Spec question 1; **e2e** — Task 8; **restart → exists and empty** — Task 7 (static topic, in-process) and Task 8 (runtime topic via the client).
- **Placeholders:** none in code steps; every edit is an exact before/after anchored on code. The Task 5 commit body records the numbers measured in Step 4 (runtime data, not plan content).
- **Type consistency:** `InMemoryLog::kRefusedFull` / `kRefusedTooLarge` (`int64_t`), `appendNoSyncRestamp(const uint8_t*, std::size_t, int32_t, int64_t&) → int64_t`, `ProduceOutcome::logFull` (Task 1) are what Tasks 2-3 use; `PartitionReplica::prepareTransientAppend()` / `kAppendBackPressure` (Task 2) are what Task 3 uses; `Worker::transient{FullRefusals,CursorSkips,RecordsSkipped}ForTest()` are added in Task 3 and asserted in Tasks 3-4; `transient_rig::Rig` members/methods are defined once (Task 3) and used unchanged in Tasks 4-5; `ClientGateway::ServedPartition` / `servedPartition_` are Task 6 only.
- **Review Focus:** each of the five has its named test in the owning task (Tasks 4, 3+2, 2, 4, 6+7).
- **Philosophy:** shared-nothing — the gateway never reads an `InMemoryLog` (only its replica's published atomic HW), the worker reads only rings it owns (Task 4 clears `replica` on every not-owned re-serve); rings only — no new channel; no new flags-as-state (the transient source is derived per call from the owned replica; the evicted-cursor rule is one code path); zero allocation — proven (Task 5, Task 2); fix-on-sight — `applyReplicated`'s per-apply `std::vector` (Task 2), `InMemoryLog`'s release-mode over-capacity corruption (Task 1, was an `assert`), the dropped marker on a refused ring append (Task 3, was `Internal`).

## Spec questions

1. **Full-ring rule (decided here, please confirm).** `InMemoryLog` is a fixed ring (`RUNE_MEMORY_LOG_RING_BYTES`, default 8 MiB per partition, ≤ 1 000 000 batches) whose only policy today is `DropOldest` — which would evict records not yet replicated (losing acks=all data under a slow follower) and leave readers/followers stalled on evicted offsets. The plan's rule, Kafka-retention-consistent: the ring is the topic's **retention**; it evicts oldest-first **only below the high watermark**; an append needing an eviction at/above the HW is **refused (retriable)**; a reader behind the ring start **resumes at the ring start** (counted, logged); a follower behind the leader's ring start **restarts its ring there**. Alternatives considered: refuse-only (never evict — a transient topic would fill and block forever); evict-anything (loses unacked data, breaks "replicated before ack"); floor at the LSO instead of the HW (also never evict an open transaction's records — would let one long transaction block all produce; Kafka retention is HW-bounded, not LSO-bounded). Also: should transient topics get a per-topic ring size (e.g. `RUNE_TOPICS … ring=64m`, Rune QL option) instead of the node-wide env var?
2. **Refusal code.** A full ring answers `NotLeader` — the only retriable "nothing appended, retry" code the client protocol has (the same choice the worker already makes for a full return ring, review m11). It makes the client refresh metadata on every refusal. A dedicated retriable code (e.g. append-only `ErrorCode::TransientLogFull = 37`, client-retriable, version-gated like `UnknownProducerId`) is cleaner — protocol + client change; deferred unless requested.
3. **`tier=strict` on a transient topic** acks like `majority` (no fsync exists). Alternative: reject `tier=strict` together with `persistence=transient` at config / DefineTopic time. Plan documents the former.
4. **Disk followers behind the leader's retention start have the same stall** this plan fixes for transient rings (a disk leader advertises `logStartOffset = 0` and skips answering an empty read below its first segment). Out of E2's scope (disk path unchanged by design); it needs Kafka-style "truncate fully and resume at the leader's log start" for `Log`. Track separately?
5. **Gateway cross-thread reads of disk `Log::logEndOffset()` / `logStartOffset()`** (subscribe clamp, group subscribe, inline push) are pre-existing reads of worker-owned state on the control thread (a HARD RULE 1 violation, not introduced here; E2 adds none for transient partitions — it uses only the published atomic HW). Fixing it means publishing the disk log start/end like the HW (`publishCleanerState`). Out of E2's scope — confirm a follow-up.
6. **No worker pool (unit-rig gateway inline path)** refuses transient produce / subscribe / group subscribe with `UnknownPartition` and drops markers (no inline append to a worker-owned ring). Production always has a worker pool; confirm the refusal is acceptable rather than an inline transient path.

## Controller rulings on the spec questions (2026-10-11)
1. **Full ring:** accepted as written — the ring is the transient topic's retention; evict only records below the high watermark; refuse a produce that would evict unreplicated records (retriable); lagging consumers/followers resume at the oldest held record. Ring size stays node-wide (`RUNE_TRANSIENT_LOG_RING_BYTES`, `RUNE_MEMORY_LOG_RING_BYTES` alias per Part E); per-topic sizing is a follow-up.
2. **Refusal code:** do NOT reuse `NotLeader` (it triggers a client metadata refresh on every refusal). Append a dedicated retriable error code `TransientLogFull` to the protocol `ErrorCode` enum (append-only), map it in the client as `BackPressured`-class retriable without a metadata refresh. Executors add this to the protocol and client in the task that introduces the refusal.
3. **`tier=strict` on a transient topic:** acks like `majority`, documented (there is no fsync to wait for).
4. **Disk followers stalling below the leader's retention start:** out of scope for 2b; recorded as a follow-up.
5. **Gateway cross-thread reads of disk log offsets:** a real shared-nothing violation (CLAUDE.md rule 1), pre-existing and outside 2b; recorded as a follow-up plan to raise with the user. E2 adds no new cross-thread access.
6. **No-worker-pool unit-test path refusing transient topics with `UnknownPartition`:** accepted.
