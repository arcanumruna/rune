// TransientFailoverLatencyGateTest.cpp — E2 final fix wave (final review I3):
// the K-partition failover latency gate for TRANSIENT partitions.
//
// The bug: the read_committed (and producer-state) rebuild of a memory-backed
// partition walked its WHOLE ring in one worker pass, on the edge into
// leadership / ownership — up to RUNE_TRANSIENT_LOG_RING_BYTES (8 MiB, ~10^5
// minimum-size batches) per partition, for every partition that changed in that
// pass.  A failover that moves K transient partitions onto one worker stalled
// that worker for tens to hundreds of ms: every partition it serves breached
// the p99 200 µs target.  The rebuild is now the budgeted RingScanning state
// (RcState::kDefaultScanBytesPerPass per partition per pass).
//
// The gate: ONE shard-mode worker owns K transient FOLLOWER partitions whose
// 8 MiB rings are full of minimum-size batches (transactions interleaved);
// BecomeLeader for all K lands in one pass (a failover / controlled shutdown
// moving them here).  Every worker duty pass (Worker::doWork) is timed until all
// K read_committed states are Ready.  It fails when the slowest pass exceeds the
// per-build-type budget.  It also reports the time until all K are Ready (the
// read_committed availability after the failover) and the pass count.
//
// E2 final fix wave (residual, ruled): the ring-rebuild budget is WORKER-WIDE —
// at most kDefaultScanBytesPerPass (32 KiB) of rebuild per pass, shared
// round-robin by every rebuilding partition — so the slowest pass with K
// partitions stays near the single-partition figure (one 32 KiB step) instead
// of K steps; the rebuild window grows K-fold instead (same total work).
//
// Budgets (named, per build type — the test binary may be built -O0), on the
// p99 pass (the steady cost of the rebuild window) and the max pass (a stall):
//   Release: kP99PassUs 100 / kMaxPassUs 1 000 — one ~25 µs step per pass;
//   Debug:   kP99PassUs 250 / kMaxPassUs 2 000 — the same work unoptimised.
// RED: per-partition budget (K steps per pass): p99 ~271 µs Release / ~704 µs
// Debug; the original one-pass rebuild: 64.6 ms / 227 ms.  Best of kAttempts
// (a scheduler preemption does not reproduce; a stall in the pass does).
// Benchmark mode: RUNE_LATENCY_GATE_BENCH=1 prints, never fails.
// ctest runs the binary RUN_SERIAL (a latency measurement must not share the host).
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace {

using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerPool;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

constexpr int32_t     kTopic    = 1;
constexpr int         kK        = 10;                 // transient partitions moved in one failover
constexpr std::size_t kRingB    = 8u << 20;           // RUNE_TRANSIENT_LOG_RING_BYTES default
constexpr std::size_t kMaxBatch = 1'000'000;          // the ring's batch cap
constexpr int         kAttempts = 3;
#if defined(NDEBUG)
constexpr int64_t kMaxPassUs = 1'000;    // Release
constexpr int64_t kP99PassUs = 100;
#else
constexpr int64_t kMaxPassUs = 2'000;    // Debug
constexpr int64_t kP99PassUs = 250;
#endif

bool benchMode() {
    const char* v = std::getenv("RUNE_LATENCY_GATE_BENCH");
    return v != nullptr && v[0] == '1';
}

std::vector<uint8_t> minBatch(int64_t pid = -1, int32_t seq = -1, bool txn = false) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    if (txn) b.setTransactional(true);
    b.append("", "", 1000);
    return b.build();
}

void put(rune::storage::InMemoryLog& mem, const std::vector<uint8_t>& b) {
    int64_t base = -1;
    const int32_t d = rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
    ASSERT_GE(mem.appendNoSyncRestamp(b.data(), b.size(), d, base), 0);
}

// A full ring: minimum-size batches until it wrapped, transactions interleaved
// (committed / aborted / still open per producer), so the RC build does real work.
void fillRing(rune::storage::InMemoryLog& mem, int64_t pidBase) {
    const auto plain = minBatch();
    int32_t seq[8] = {};
    for (int64_t i = 0; mem.logStartOffset() == 0 || i < 150'000; ++i) {
        if (i % 1000 == 0) {
            const int k = static_cast<int>((i / 1000) % 8);
            put(mem, minBatch(pidBase + k, seq[k]++, /*txn=*/true));
            if ((i / 1000) % 3 != 2)
                put(mem, rune::transaction::ControlMarker::build(
                             pidBase + k, 0,
                             ((i / 1000) % 3) == 0 ? rune::transaction::ControlType::Commit
                                                   : rune::transaction::ControlType::Abort,
                             0));
        } else {
            put(mem, plain);
        }
        if (i > 2'000'000) break;   // bounded
    }
}

struct Result {
    int64_t maxPassUs   = 0;
    int64_t p99PassUs   = 0;
    int64_t allReadyUs  = 0;
    int     passes      = 0;
    bool    allReady    = false;
};

Result runOnce() {
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> mems;
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1)};
    for (int p = 0; p < kK; ++p) {
        mems.push_back(std::make_unique<rune::storage::InMemoryLog>(kRingB, kMaxBatch));
        fillRing(*mems.back(), 1000 + 10 * p);
        PartitionConfig c;
        c.topicId = kTopic; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
        c.followerFlushIntervalNs = 0;
        auto r = std::make_unique<PartitionReplica>(c, Role::Follower, 1, std::vector<int32_t>{1, 2},
                                                    std::vector<int32_t>{1, 2}, mems.back().get(), &transport);
        r->setExternalFollowerFlush(true);
        reg.add(kTopic, p, std::move(r));
    }
    pool.setRegistry(&reg);
    Worker& w = pool.worker(0);
    w.setWorkerIngress(nullptr, /*active=*/true);
    w.setShardMode(true);
    pool.assignFromRegistry(reg);
    int64_t now = 1'000'000'000;
    for (int i = 0; i < 8; ++i) { now += 1'000'000; (void)w.doWork(now); pool.drainAllReturns([](WorkerReturn) {}); }

    // The failover: all K become leader in one pass.
    for (int p = 0; p < kK; ++p) {
        WorkerOp op;
        op.type = WorkerOp::Type::BecomeLeader;
        op.topicId = kTopic; op.partition = p; op.roleEpoch = 2; op.isr = {1};
        EXPECT_TRUE(pool.tryEnqueueOp(kTopic, p, std::move(op)));
    }
    std::vector<int64_t> passUs;
    passUs.reserve(1u << 16);
    Result res;
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < 200'000; ++i) {
        now += 1'000;
        const auto a = std::chrono::steady_clock::now();
        (void)w.doWork(now);
        const auto b = std::chrono::steady_clock::now();
        passUs.push_back(std::chrono::duration_cast<std::chrono::microseconds>(b - a).count());
        pool.drainAllReturns([](WorkerReturn) {});
        bool all = true;
        for (int p = 0; p < kK && all; ++p) all = w.rcStatusForTest(kTopic, p) == 2;
        if (all) {
            res.allReady   = true;
            res.allReadyUs = std::chrono::duration_cast<std::chrono::microseconds>(b - t0).count();
            break;
        }
    }
    res.passes = static_cast<int>(passUs.size());
    std::sort(passUs.begin(), passUs.end());
    if (!passUs.empty()) {
        res.maxPassUs = passUs.back();
        res.p99PassUs = passUs[std::min(passUs.size() - 1, (passUs.size() * 99) / 100)];
    }
    return res;
}

}  // namespace

TEST(TransientFailoverLatencyGateRegression, KTransientPartitionsBecomingLeaderNeverStallAWorkerPass) {
    Result best;
    best.maxPassUs = INT64_MAX;
    best.p99PassUs = INT64_MAX;
    for (int a = 0; a < kAttempts; ++a) {
        const Result r = runOnce();
        ASSERT_TRUE(r.allReady) << "the K read_committed states never became Ready";
        std::printf("[transient failover gate] attempt %d: K=%d rings of %zu MiB: max pass %lld us, p99 pass "
                    "%lld us, %d passes, all Ready after %lld us (budget max %lld / p99 %lld us)\n",
                    a + 1, kK, kRingB >> 20, static_cast<long long>(r.maxPassUs),
                    static_cast<long long>(r.p99PassUs), r.passes, static_cast<long long>(r.allReadyUs),
                    static_cast<long long>(kMaxPassUs), static_cast<long long>(kP99PassUs));
        if (r.p99PassUs < best.p99PassUs || (r.p99PassUs == best.p99PassUs && r.maxPassUs < best.maxPassUs)) best = r;
        if (best.maxPassUs <= kMaxPassUs && best.p99PassUs <= kP99PassUs && !benchMode()) break;
    }
    if (benchMode()) return;
    EXPECT_LE(best.p99PassUs, kP99PassUs)
        << "the p99 worker pass was " << best.p99PassUs << " us while K=" << kK
        << " transient partitions rebuilt (a per-partition budget: K steps per pass)";
    EXPECT_LE(best.maxPassUs, kMaxPassUs)
        << "a worker pass stalled " << best.maxPassUs << " us while K=" << kK
        << " transient partitions rebuilt their read_committed state (one pass rebuilding whole rings)";
    EXPECT_GT(best.passes, kK) << "the K rebuilds were not spread over passes";
}

// ---------------------------------------------------------------------------
// E2 final fix wave (residual): the worker-wide ring budget is SHARED FAIRLY.
// K partitions rebuild at once (smaller 1 MiB rings: a functional check, not a
// latency one).  Per pass: the ring-rebuild bytes of ALL partitions together
// stay within one budget (+ one batch: a step always takes whole batches).
// Per partition: it gets its first slice within K passes of the failover and,
// while it rebuilds, never waits more than K passes for its next one; every
// partition reaches Ready.  RED (a per-partition budget again): a pass rebuilt
// ~K budgets.
// ---------------------------------------------------------------------------
TEST(TransientFailoverLatencyGateRegression, WorkerWideRingBudgetIsSharedFairlyRegression) {
    constexpr int         K     = 10;
    constexpr std::size_t kRing = 1u << 20;
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> mems;
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1)};
    for (int p = 0; p < K; ++p) {
        mems.push_back(std::make_unique<rune::storage::InMemoryLog>(kRing, kMaxBatch));
        fillRing(*mems.back(), 1000 + 10 * p);
        PartitionConfig c;
        c.topicId = kTopic; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000; c.followerFlushIntervalNs = 0;
        auto r = std::make_unique<PartitionReplica>(c, Role::Follower, 1, std::vector<int32_t>{1, 2},
                                                    std::vector<int32_t>{1, 2}, mems.back().get(), &transport);
        r->setExternalFollowerFlush(true);
        reg.add(kTopic, p, std::move(r));
    }
    pool.setRegistry(&reg);
    Worker& w = pool.worker(0);
    w.setWorkerIngress(nullptr, /*active=*/true);
    w.setShardMode(true);
    pool.assignFromRegistry(reg);
    int64_t now = 1'000'000'000;
    for (int i = 0; i < 8; ++i) { now += 1'000'000; (void)w.doWork(now); pool.drainAllReturns([](WorkerReturn) {}); }
    for (int p = 0; p < K; ++p) {
        WorkerOp op;
        op.type = WorkerOp::Type::BecomeLeader;
        op.topicId = kTopic; op.partition = p; op.roleEpoch = 2; op.isr = {1};
        ASSERT_TRUE(pool.tryEnqueueOp(kTopic, p, std::move(op)));
    }
    constexpr uint64_t kBudget = rune::node::RcState::kDefaultScanBytesPerPass;
    constexpr uint64_t kSlack  = 256;   // one minimum-size batch past the budget
    std::vector<uint64_t> bytes(K, 0);
    std::vector<int> firstPass(K, -1), lastPass(K, -1), maxGap(K, 0);
    std::vector<bool> ready(K, false);
    uint64_t maxPassBytes = 0;
    int pass = 0;
    for (; pass < 100'000; ++pass) {
        now += 1'000;
        (void)w.doWork(now);
        pool.drainAllReturns([](WorkerReturn) {});
        uint64_t passBytes = 0;
        bool all = true;
        for (int p = 0; p < K; ++p) {
            const uint64_t b = w.rcScannedBytesForTest(kTopic, p);
            if (b > bytes[static_cast<std::size_t>(p)]) {
                passBytes += b - bytes[static_cast<std::size_t>(p)];
                bytes[static_cast<std::size_t>(p)] = b;
                if (firstPass[static_cast<std::size_t>(p)] < 0) firstPass[static_cast<std::size_t>(p)] = pass;
                if (lastPass[static_cast<std::size_t>(p)] >= 0)
                    maxGap[static_cast<std::size_t>(p)] =
                        std::max(maxGap[static_cast<std::size_t>(p)], pass - lastPass[static_cast<std::size_t>(p)]);
                lastPass[static_cast<std::size_t>(p)] = pass;
            }
            ready[static_cast<std::size_t>(p)] = w.rcStatusForTest(kTopic, p) == 2;
            all = all && ready[static_cast<std::size_t>(p)];
        }
        maxPassBytes = std::max(maxPassBytes, passBytes);
        if (all) break;
    }
    for (int p = 0; p < K; ++p) {
        EXPECT_TRUE(ready[static_cast<std::size_t>(p)]) << "partition " << p << " never reached Ready (starved)";
        EXPECT_GE(firstPass[static_cast<std::size_t>(p)], 0) << "partition " << p << " never got a slice";
        EXPECT_LE(firstPass[static_cast<std::size_t>(p)], K) << "partition " << p << " waited "
                                                             << firstPass[static_cast<std::size_t>(p)] << " passes for its first slice";
        EXPECT_LE(maxGap[static_cast<std::size_t>(p)], K) << "partition " << p << " waited "
                                                          << maxGap[static_cast<std::size_t>(p)] << " passes between slices";
    }
    EXPECT_LE(maxPassBytes, kBudget + kSlack)
        << "one pass rebuilt " << maxPassBytes << " B across the partitions (the budget is " << kBudget
        << " B per WORKER pass, not per partition)";
    std::printf("[ring budget fairness] K=%d rings of %zu KiB: Ready after %d passes, max %llu B per pass\n", K,
                kRing >> 10, pass + 1, static_cast<unsigned long long>(maxPassBytes));
}

// ---------------------------------------------------------------------------
// E2 final fix wave, residual round 3 — N5: the worker-wide ring budget costs
// NOTHING on a worker that owns no memory-backed partition: rcAdvanceAll_
// never enters the ring walk there (the count of memory-backed slots is kept on
// ownership apply).  A worker that owns one does walk.  RED (no skip): the
// disk-only worker walked the slots for ring work on every pass.
// ---------------------------------------------------------------------------
TEST(TransientFailoverLatencyGateRegression, DiskOnlyWorkerSkipsTheRingBudgetWalkRegression) {
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / ("rune-n5-" + std::to_string(::getpid()));
    fs::remove_all(dir);
    std::vector<std::unique_ptr<rune::storage::Log>> logs;
    std::unique_ptr<rune::storage::InMemoryLog> mem;
    rune::replication::NullPartitionTransport transport;
    auto run = [&](bool withMemory) -> uint64_t {
        rune::node::ReplicaRegistry reg;
        WorkerPool pool{1, rune::metadata::ShardRouter(1)};
        constexpr int kDisk = 4;
        for (int p = 0; p < kDisk + (withMemory ? 1 : 0); ++p) {
            PartitionConfig c;
            c.topicId = kTopic; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
            c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000; c.followerFlushIntervalNs = 0;
            std::unique_ptr<PartitionReplica> r;
            if (p < kDisk) {
                const fs::path pd = dir / (std::string(withMemory ? "m" : "d") + std::to_string(p));
                fs::create_directories(pd);
                logs.push_back(std::make_unique<rune::storage::Log>(pd.string()));
                logs.back()->recover();
                r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                       std::vector<int32_t>{1}, logs.back().get(), &transport);
            } else {
                mem = std::make_unique<rune::storage::InMemoryLog>(1u << 20, kMaxBatch);
                r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                       std::vector<int32_t>{1}, mem.get(), &transport);
            }
            reg.add(kTopic, p, std::move(r));
        }
        pool.setRegistry(&reg);
        Worker& w = pool.worker(0);
        w.setWorkerIngress(nullptr, /*active=*/true);
        w.setShardMode(true);
        pool.assignFromRegistry(reg);
        int64_t now = 1'000'000'000;
        for (int i = 0; i < 64; ++i) { now += 1'000'000; (void)w.doWork(now); pool.drainAllReturns([](WorkerReturn) {}); }
        EXPECT_EQ(w.rcStatusForTest(kTopic, 0), 2) << "precondition: the disk partitions' RC states are driven";
        return w.ringWalksForTest();
    };
    EXPECT_EQ(run(false), 0u) << "a worker with no memory-backed partition walked its slots for ring work";
    EXPECT_GT(run(true), 0u) << "a worker owning a memory-backed partition never walked for ring work";
    logs.clear();
    fs::remove_all(dir);
}
