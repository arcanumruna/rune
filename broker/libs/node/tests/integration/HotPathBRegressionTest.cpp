// HotPathBRegressionTest.cpp — worker hot-path latency fixes (brief
// 2026-10-05-hotpath-B): deterministic regressions for the behaviour behind the
// latency-gate findings, single-threaded (the test thread is the worker owner).
//
//   F5  PushRead answers are gathered from the segment read mmap straight into
//       the return ring, bounded per worker pass (Worker::kPushPassBudgetBytes)
//       and fair (budget-deferred reads are served first next pass).
//   F7  role changes never fsync on the worker: the leader-epoch fence +
//       epoch-cache image go to the two-slot leader-epoch-state file through
//       the worker's io backend, and a controller-managed leadership acts only
//       once that write completed (event-driven).
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/SegmentProvisionerAgent.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/LeaderEpochState.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerPool;
using rune::node::WorkerReturn;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;

namespace {

struct TmpDir {
    fs::path p;
    TmpDir() {
        static int seq = 0;
        p = fs::temp_directory_path() / ("rune_hpb_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::remove_all(p);
        fs::create_directories(p);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(p, ec); }
};

std::vector<uint8_t> produceFrame(int64_t corr, int32_t topicId, int32_t part, std::size_t valueBytes, int recs) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < recs; ++i) b.append("k" + std::to_string(i), v, 1000 + i);
    const auto batch = b.build();
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = topicId; msg.partition = part;
    msg.partitionKnown = 1; msg.acks = 1; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
    msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}

/// One worker (shard mode, registry-routed ops) owning one leader partition.
struct Rig {
    TmpDir                                    ownDir;
    struct { fs::path p; } dir;
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    WorkerPool                                pool{1, rune::metadata::ShardRouter(1)};
    std::unique_ptr<rune::storage::Log>       log;
    PartitionReplica*                         rep = nullptr;
    int64_t                                   now = 1'000'000'000;
    std::unique_ptr<rune::node::SegmentProvisionerAgent> prov;   ///< driven on the test thread (its own "thread")

    explicit Rig(bool controllerManaged = false, int64_t rollBytes = 64 * 1024 * 1024, const fs::path* at = nullptr,
                 bool provisioned = false) {
        dir.p = at ? *at : ownDir.p;
        log = std::make_unique<rune::storage::Log>(dir.p.string(), rollBytes);
        log->recover();
        if (provisioned) {
            prov = std::make_unique<rune::node::SegmentProvisionerAgent>(1);
            prov->addToViewForTest(log.get());
            log->setProvisioning(prov->hook(), 50);
        }
        PartitionConfig c;
        c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.followerFlushIntervalNs = 0;
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                    std::vector<int32_t>{1}, log.get(), &transport);
        r->setExternalFollowerFlush(true);
        if (controllerManaged) r->setIsrChannel(&transport);
        rep = r.get();
        reg.add(1, 0, std::move(r));
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
        if (prov) (void)prov->doWork(now);
    }
    void roleOp(bool leader, int64_t epoch, int32_t leaderId = 2) {
        WorkerOp op;
        op.type = leader ? WorkerOp::Type::BecomeLeader : WorkerOp::Type::BecomeFollower;
        op.topicId = 1; op.partition = 0; op.roleEpoch = epoch;
        if (leader) op.isr = {1}; else op.roleLeaderId = leaderId;
        ASSERT_TRUE(pool.tryEnqueueOp(1, 0, std::move(op)));
    }
    /// Passes until `done` (the io backend completes on its own thread) —
    /// bounded, never a sleep-based wait for an outcome.
    template <class F> bool passUntil(F&& done, int maxMs = 5000) {
        const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(maxMs);
        while (!done()) {
            if (std::chrono::steady_clock::now() > end) return false;
            pass();
            std::this_thread::yield();
        }
        return true;
    }
    void pushRead(int64_t subId, int64_t from) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead; op.topicId = 1; op.partition = 0;
        op.subscriptionId = subId; op.sessionId = 1; op.nextOffset = from;
        op.hwBound = INT64_MAX; op.isolation = 0; op.logPtr = log.get();
        ASSERT_TRUE(pool.tryEnqueueOp(1, 0, std::move(op)));
    }
};

} // namespace

// F5: N concurrent PushReads, each with 512 KiB of data available, must not be
// served in one worker pass (old: N x (512 KiB heap vector + copy + ring copy)
// in ONE pass — 870 us passes).  A pass copies at most the push budget; the
// rest is deferred, served first on later passes, and every read is answered
// with whole, contiguous batches from its offset.
TEST(HotPathBRegression, PushReadsBoundedPerPassAndFairRegression) {
    Rig rig;
    // ~2 MiB of 4 KiB batches.
    const auto frame = produceFrame(1, 1, 0, 1000, 4);
    for (int i = 0; i < 520; ++i) rig.pass(&frame);
    rig.pool.drainAllReturns([](WorkerReturn) {});
    ASSERT_GT(rig.log->logEndOffset(), 2000);
    ASSERT_EQ(rig.rep->highWatermark(), rig.log->logEndOffset());

    constexpr int kReads = 8;
    for (int s = 0; s < kReads; ++s) rig.pushRead(100 + s, 0);
    std::map<int64_t, std::size_t> answered;   // subId -> chunk bytes
    std::size_t firstPassBytes = 0, maxBatch = 0;
    int passes = 0;
    while (answered.size() < static_cast<std::size_t>(kReads) && passes < 64) {
        rig.pass();
        std::size_t passBytes = 0;
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type != WorkerReturn::Type::PushReadResult) return;
            ASSERT_EQ(r.errorCode, 0);
            ASSERT_FALSE(answered.count(r.subscriptionId)) << "answered twice";
            answered[r.subscriptionId] = r.chunk.size();
            passBytes += r.chunk.size();
            // Whole contiguous batches from offset 0, cursor past the last one.
            std::size_t pos = 0;
            int64_t expect = 0;
            while (pos < r.chunk.size()) {
                rune::protocol::RecordBatchReader b(r.chunk.data() + pos, r.chunk.size() - pos);
                ASSERT_GT(b.totalLength(), 0);
                ASSERT_EQ(b.baseOffset(), expect);
                expect = b.baseOffset() + b.lastOffsetDelta() + 1;
                maxBatch = std::max<std::size_t>(maxBatch, static_cast<std::size_t>(b.totalLength()));
                pos += static_cast<std::size_t>(b.totalLength());
            }
            ASSERT_EQ(pos, r.chunk.size());
            EXPECT_EQ(r.newNextOffset, expect);
            EXPECT_GT(r.chunk.size(), 0u);
        });
        if (passes == 0) firstPassBytes = passBytes;
        EXPECT_LE(passBytes, Worker::kPushPassBudgetBytes + maxBatch) << "pass " << passes << " copied too much";
        ++passes;
    }
    EXPECT_EQ(answered.size(), static_cast<std::size_t>(kReads)) << "every deferred read is answered";
    EXPECT_LE(firstPassBytes, Worker::kPushPassBudgetBytes + maxBatch);
    EXPECT_GT(passes, 1) << "8 x 512 KiB must not fit one pass";
    EXPECT_GT(rig.worker().pushReadsDeferredForTest(), 0u);
    EXPECT_EQ(rig.worker().deferredPushReadsForTest(), 0u);
}

// F5: a deferred read whose partition was given away meanwhile is answered
// UNANSWERED (bound -1, no data) — never read from a log the worker no longer owns.
TEST(HotPathBRegression, DeferredPushReadRevalidatesOwnershipRegression) {
    Rig rig;
    const auto frame = produceFrame(1, 1, 0, 1000, 4);
    for (int i = 0; i < 300; ++i) rig.pass(&frame);
    rig.pool.drainAllReturns([](WorkerReturn) {});
    for (int s = 0; s < 4; ++s) rig.pushRead(200 + s, 0);
    rig.pass();
    ASSERT_GT(rig.worker().deferredPushReadsForTest(), 0u) << "precondition: reads deferred";
    int answeredWithData = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) { if (!r.chunk.empty()) ++answeredWithData; });
    // Give the partition away (empty ownership), then let the deferred reads run.
    rig.worker().setOwnedDirect({});
    for (int k = 0; k < 4; ++k) rig.pass();
    int unanswered = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::PushReadResult) return;
        EXPECT_TRUE(r.chunk.empty()) << "read a log it no longer owns";
        EXPECT_EQ(r.pushEffectiveBound(), -1);
        ++unanswered;
    });
    EXPECT_EQ(answeredWithData + unanswered, 4);
    EXPECT_EQ(rig.worker().deferredPushReadsForTest(), 0u);
}

// F7: the pass that executes a controller-managed BecomeLeader at a new epoch
// must not fsync the leader-epoch fence / checkpoint inline (old: write + fsync
// + rename + directory fsync on the worker — 2-13 ms passes for every
// partition the worker owns).  The leadership is deferred until the owning
// worker's io backend completed the leader-epoch-state write that carries the
// fence AND the new epoch's cache entry; then it leads, and both are durable.
TEST(HotPathBRegression, RoleChangeEpochDurabilityIsAsyncAndGatesLeadershipRegression) {
    Rig rig(/*controllerManaged=*/true);
    const auto frame = produceFrame(1, 1, 0, 100, 2);
    for (int i = 0; i < 4; ++i) rig.pass(&frame);
    const int64_t leo = rig.log->logEndOffset();
    ASSERT_GT(leo, 0);

    rig.roleOp(/*leader=*/false, 2);
    rig.pass();
    ASSERT_EQ(rig.rep->role(), Role::Follower);

    rig.roleOp(/*leader=*/true, 3);
    rig.pass();   // the role-op pass: submits the write, never waits for it
    EXPECT_NE(rig.rep->role(), Role::Leader)
        << "led at epoch 3 inside the role-op pass: its fence was fsynced on the worker";
    EXPECT_TRUE(rig.rep->leadershipPendingForTest());
    EXPECT_EQ(rig.rep->epochStateInlineWritesForTest(), 0u) << "a synchronous epoch write on the worker";

    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->role() == Role::Leader; })) << "leadership never completed";
    EXPECT_EQ(rig.rep->epoch(), 3);
    EXPECT_FALSE(rig.rep->leadershipPendingForTest());
    EXPECT_EQ(rig.rep->epochStateInlineWritesForTest(), 0u);
    EXPECT_GE(rig.rep->epochStateWritesForTest(), 1u);

    // Durable: the fence and the epoch entry, exactly as a restart reads them.
    const auto st = rune::storage::LeaderEpochState::read(rig.dir.p.string());
    ASSERT_EQ(st.state, rune::storage::LeaderEpochState::FileState::Valid);
    EXPECT_GE(st.fence, 3);
    rune::storage::LeaderEpochCache c;
    ASSERT_TRUE(PartitionReplica::readDurableEpochCache(rig.dir.p.string(), c));
    EXPECT_EQ(c.latestEpoch(), 3);
    EXPECT_EQ(c.startOffsetFor(3), leo);

    // It is writable under the new epoch.
    std::size_t errs = 0, oks = 0;
    rig.pool.drainAllReturns([](WorkerReturn) {});
    rig.pass(&frame);
    rig.passUntil([&] {
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult) {
                if (r.errorCode == 0) ++oks; else ++errs;
            }
        });
        return oks + errs > 0;
    });
    EXPECT_EQ(errs, 0u);
}

// F7: a newer role while the leadership awaits its durable fence supersedes it
// (never leads at the superseded epoch), and a later leadership still works.
TEST(HotPathBRegression, DeferredLeadershipSupersededByNewerRoleRegression) {
    Rig rig(/*controllerManaged=*/true);
    rig.roleOp(false, 2);
    rig.pass();
    rig.roleOp(true, 3);
    rig.roleOp(false, 4);    // same pass: demoted before the fence completed
    rig.pass();
    EXPECT_FALSE(rig.rep->leadershipPendingForTest());
    // Let the in-flight write complete: it must not resurrect the leadership.
    rig.passUntil([&] { return rig.rep->epochStateDurable(); });
    for (int i = 0; i < 8; ++i) rig.pass();
    EXPECT_EQ(rig.rep->role(), Role::Follower);
    EXPECT_EQ(rig.rep->epoch(), 4);

    rig.roleOp(true, 5);
    rig.pass();
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->role() == Role::Leader; }));
    EXPECT_EQ(rig.rep->epoch(), 5);
    rune::storage::LeaderEpochCache c;
    ASSERT_TRUE(PartitionReplica::readDurableEpochCache(rig.dir.p.string(), c));
    EXPECT_EQ(c.latestEpoch(), 5);
    EXPECT_GE(rune::storage::LeaderEpochState::read(rig.dir.p.string()).fence, 5);
}

namespace {
std::vector<uint8_t> stampedBatch(int64_t base, int n, int32_t epoch) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < n; ++i) b.append("k", "v", 1000 + i);
    auto v = b.build();
    rune::protocol::RecordBatchReader::stampLeaderEpoch(v.data(), epoch);
    return v;
}
} // namespace

// F7 restore: leader-epoch-state is authoritative (the legacy checkpoint is no
// longer written once it exists), its fence feeds the restart fence, and a
// state file with no valid slot is REBUILT from the batches' stamps — never
// restored from a stale legacy checkpoint.
TEST(HotPathBRegression, EpochStateRestoreAndCorruptionRegression) {
    TmpDir d;
    rune::replication::NullPartitionTransport t;
    PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    {
        rune::storage::Log log(d.p.string());
        log.recover();
        const auto a = stampedBatch(0, 3, 1);
        log.append(a, 2);
        PartitionReplica r(c, Role::Leader, 1, {1}, {1}, &log, &t);
        r.setExternalFollowerFlush(true);      // creates leader-epoch-state
        ASSERT_TRUE(r.epochStateEnabledForTest());
        r.setExternalFollowerFlush(false);     // control-thread mode: written inline
        r.setIsrChannel(&t);
        r.becomeLeader(7, {1});                // fence 7 + entry (7, 3), synchronously
        ASSERT_EQ(r.role(), Role::Leader);
    }
    ASSERT_FALSE(fs::exists(d.p / "leader-epoch-checkpoint") &&
                 fs::exists(d.p / "leader-epoch-fence")) << "legacy files still written";
    {
        rune::storage::Log log(d.p.string());
        log.recover();
        PartitionReplica r(c, Role::Follower, 7, {1}, {1}, &log, &t);
        EXPECT_GE(r.restartFenceEpochForTest(), 7);
        EXPECT_EQ(r.leaderEpochCacheForTest().latestEpoch(), 1)   // DUR-1: (7, 3) names no record
            << "restored cache";
        EXPECT_EQ(r.leaderEpochCacheForTest().startOffsetFor(1), 0);
    }
    // Corrupt both slots; drop in a stale legacy checkpoint claiming epoch 9.
    {
        std::fstream f(d.p / rune::storage::LeaderEpochState::kFileName, std::ios::in | std::ios::out | std::ios::binary);
        const auto sz = fs::file_size(d.p / rune::storage::LeaderEpochState::kFileName);
        f.seekp(0); f.write("XXXX", 4);
        f.seekp(static_cast<std::streamoff>(sz / 2)); f.write("XXXX", 4);
    }
    {
        rune::storage::LeaderEpochCache stale;
        stale.assign(9, 0);
        stale.checkpointToFile((d.p / "leader-epoch-checkpoint").string());
    }
    {
        rune::storage::Log log(d.p.string());
        log.recover();
        PartitionReplica r(c, Role::Follower, 7, {1}, {1}, &log, &t);
        EXPECT_EQ(r.leaderEpochCacheForTest().latestEpoch(), 1) << "trusted the stale legacy checkpoint";
        EXPECT_EQ(r.leaderEpochCacheForTest().startOffsetFor(1), 0);
    }
}

// F7 (truncation completion): a worker-driven follower's divergence truncation
// below its durable recovery point must not fsync on the worker (old: an inline
// recovery-point write + fdatasync, then the rebuilt index's msync + directory
// fsync — 3-5 ms in the truncating pass).  The lowered recovery point is made
// durable through the io backend FIRST (the truncation waits in
// TruncatePending, event-driven), so no file is touched before it (E1d F6).
TEST(HotPathBRegression, FollowerTruncationNeverSyncsOnTheWorkerRegression) {
    Rig rig(/*controllerManaged=*/false, /*rollBytes=*/4096);
    rig.roleOp(true, 2);
    rig.pass();
    ASSERT_EQ(rig.rep->role(), Role::Leader);
    const auto frame = produceFrame(1, 1, 0, 200, 4);
    for (int i = 0; i < 64; ++i) rig.pass(&frame);
    rig.pool.drainAllReturns([](WorkerReturn) {});
    ASSERT_TRUE(rig.passUntil([&] { return rig.log->persistedRecoveryPoint() > 40; }))
        << "precondition: a durable recovery point above the cut (R=" << rig.log->persistedRecoveryPoint() << ")";

    rig.roleOp(false, 3, 2);
    rig.pass();
    ASSERT_EQ(rig.rep->role(), Role::Follower);
    const uint64_t syncsBefore = rig.log->roleChangeSyncCallsForTest();
    const uint64_t inlineBefore = rig.log->syncCallsForTest();

    rune::replication::ErrorPayloadBuf epb;
    WorkerOp op;
    op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
    op.replicatedPayload.leaderEpoch = 3;
    op.replicatedPayload.partition = 0;
    op.replicatedPayload.topicId = 1;
    op.replicatedPayload.errorCode = rune::replication::kErrDivergingEpoch;
    const auto pl = epb.set(2, 10);
    op.replicatedPayload.records.assign(pl.begin(), pl.end());
    ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));

    ASSERT_TRUE(rig.passUntil([&] { return rig.log->logEndOffset() <= 10; })) << "truncation never completed";
    EXPECT_EQ(rig.log->roleChangeSyncCallsForTest(), syncsBefore)
        << "the truncation wrote the recovery point / index / directory synchronously on the worker";
    EXPECT_EQ(rig.log->syncCallsForTest(), inlineBefore) << "an inline fsync on the worker";
    EXPECT_GE(rig.log->asyncTruncationLoweringsForTest(), 1u);
    EXPECT_LE(rig.log->persistedRecoveryPoint(), rig.log->logEndOffset())
        << "a durable recovery point above the truncated log end";
    EXPECT_FALSE(rig.log->storageFailed());
    // The follower carries on (the log stays usable; R advances again once the
    // async directory fsync covered the rebuilt index).
    for (int i = 0; i < 8; ++i) rig.pass();
    EXPECT_FALSE(rig.rep->truncatePendingForTest());
}

// F7 guard: a worker-driven log truncated mid-segment (index rebuilt without
// msync / directory fsync) and then CRASHED before any async durability ran
// recovers to exactly the truncated content, with R never above it.
TEST(HotPathBRegression, UnsyncedTruncationRebuildRecoversAfterCrashRegression) {
    TmpDir d;
    int64_t cut = 0;
    {
        rune::storage::Log log(d.p.string(), 1 << 20);
        log.recover();
        log.setDeferSealSync(true);
        for (int i = 0; i < 40; ++i) { const auto b = stampedBatch(i * 2, 2, 1); log.append(b, i * 2 + 1); }
        ASSERT_EQ(log.logEndOffset(), 80);
        ASSERT_EQ(log.checkpointRecoveryPoint(), 0);
        cut = 31;   // inside batch [30, 31] → the log ends at 30
        ASSERT_TRUE(log.truncateTo(cut));
        ASSERT_EQ(log.logEndOffset(), 30);
        EXPECT_LE(log.persistedRecoveryPoint(), 30);
        log.crashForTest();
    }
    rune::storage::Log log(d.p.string(), 1 << 20);
    log.recover();
    EXPECT_EQ(log.logEndOffset(), 30);
    EXPECT_LE(log.persistedRecoveryPoint(), 30);
    const auto v = log.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.nextOffset, 30);
    const auto b = stampedBatch(30, 2, 2);
    log.append(b, 31);
    EXPECT_EQ(log.logEndOffset(), 32);
}

// F7: a sitting leader re-elected at a newer epoch (controller epoch bump) never
// appends while its new fence is not durable — produce is refused retriably
// (NotLeader) instead of landing under the superseded epoch, then accepted
// under the new one.
TEST(HotPathBRegression, EpochBumpOfSittingLeaderRefusesProduceUntilDurableRegression) {
    Rig rig(/*controllerManaged=*/true);
    ASSERT_EQ(rig.rep->role(), Role::Leader);
    const auto frame = produceFrame(1, 1, 0, 100, 2);
    rig.roleOp(true, 2);
    rig.pass();                                   // the role-op pass (write submitted)
    const int64_t leo = rig.log->logEndOffset();
    rig.pass(&frame);                             // intake before this pass reaps anything
    int16_t err = 0;
    int answers = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) { ++answers; err = r.errorCode; }
    });
    ASSERT_EQ(answers, 1);
    EXPECT_EQ(err, static_cast<int16_t>(pc::ErrorCode::NotLeader))
        << "appended under the superseded epoch while the new fence was not durable";
    EXPECT_EQ(rig.log->logEndOffset(), leo);

    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->acceptsProduce() && rig.rep->epoch() == 2; }));
    rig.pass(&frame);
    err = -1;
    rig.pool.drainAllReturns([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ProduceResult) err = r.errorCode; });
    EXPECT_EQ(err, 0);
    rune::storage::LeaderEpochCache c;
    ASSERT_TRUE(PartitionReplica::readDurableEpochCache(rig.dir.p.string(), c));
    EXPECT_EQ(c.latestEpoch(), 2);
    EXPECT_EQ(c.startOffsetFor(2), leo);
}

// F5: consumer reads of the growing ACTIVE segment never create / remap its
// read mapping on the worker (old: a 1 MiB first map grown geometrically —
// munmap + mmap of up to 32 MiB inside the read pass, 100-260 us measured
// under the cleaner's / provisioner's concurrent mmap traffic).
TEST(HotPathBRegression, ActiveSegmentReadsNeverRemapOnTheWorkerRegression) {
    Rig rig(/*controllerManaged=*/false, /*rollBytes=*/16 * 1024 * 1024, nullptr, /*provisioned=*/true);
    const auto frame = produceFrame(1, 1, 0, 4000, 8);   // ~32 KiB batches
    int64_t next = 0;
    for (int i = 0; i < 300; ++i) {                     // ~9.6 MiB into one segment
        rig.pass(&frame);
        if (i % 4 == 3) {
            rig.pushRead(500 + i, next);
            rig.pass();
            rig.pool.drainAllReturns([&](WorkerReturn r) {
                if (r.type == WorkerReturn::Type::PushReadResult && r.errorCode == 0 && r.newNextOffset > next)
                    next = r.newNextOffset;
            });
        }
    }
    ASSERT_GT(rig.log->logEndOffset(), 2000);
    ASSERT_GT(next, 1000) << "precondition: the reads consumed the segment as it grew";
    EXPECT_EQ(rig.log->segmentBaseOffsets().size(), 1u) << "precondition: one (active) segment";
    EXPECT_EQ(rig.log->activeReadMapRemapsForTest(), 0u) << "the read map was created / grown on the worker";
}

// ===========================================================================
// Review round 1 (2026-10-06 hot-path B review): C1 / M1 / Md1-Md4, Md6.
// ===========================================================================
namespace {

/// Apply `n` replicated batches stamped `stamp` (2 records each) at the
/// follower's LEO, under leader epoch `leaderEpoch`, HW = the new end.
void applyStamped(Rig& rig, int64_t leaderEpoch, int32_t stamp, int n) {
    for (int i = 0; i < n; ++i) {
        const int64_t leo = rig.log->logEndOffset();
        rune::protocol::RecordBatchBuilder b(leo, 1000);
        for (int k = 0; k < 2; ++k) b.append("k", std::string(100, 'x'), 1000 + k);
        auto v = b.build();
        rune::protocol::RecordBatchReader::stampLeaderEpoch(v.data(), stamp);
        WorkerOp op;
        op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
        op.replicatedPayload.leaderEpoch = leaderEpoch;
        op.replicatedPayload.partition = 0;
        op.replicatedPayload.topicId = 1;
        op.replicatedPayload.highWatermark = leo + 2;
        op.replicatedPayload.records.assign(v.begin(), v.end());
        ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));
        rig.pass();
    }
}

std::string readFile(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}
void writeFile(const fs::path& p, const std::string& b) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(b.data(), static_cast<std::streamsize>(b.size()));
}

/// Every batch of the log is owned (cache.epochForOffset) by the epoch stamped
/// on it — the KIP-101/279 lineage a restarted replica answers with.
int lineageMismatches(rune::storage::Log& log, const rune::storage::LeaderEpochCache& c, std::string* first) {
    int bad = 0;
    int64_t at = log.logStartOffset();
    while (at < log.logEndOffset()) {
        const auto buf = log.read(at, 1 << 20);
        if (buf.empty()) break;
        std::size_t pos = 0;
        int64_t next = at;
        while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            const int32_t t = r.totalLength();
            if (t <= 0 || pos + static_cast<std::size_t>(t) > buf.size()) break;
            if (c.epochForOffset(r.baseOffset()) != r.partitionLeaderEpoch()) {
                if (bad++ == 0 && first)
                    *first = "offset " + std::to_string(r.baseOffset()) + " stamped " +
                             std::to_string(r.partitionLeaderEpoch()) + " but history says " +
                             std::to_string(c.epochForOffset(r.baseOffset()));
            }
            next = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += static_cast<std::size_t>(t);
        }
        if (next <= at) break;
        at = next;
    }
    return bad;
}

struct Restarted {
    std::unique_ptr<rune::storage::Log> log;
    rune::replication::NullPartitionTransport t;
    std::unique_ptr<PartitionReplica> rep;
    explicit Restarted(const fs::path& d, int64_t roll) {
        log = std::make_unique<rune::storage::Log>(d.string(), roll);
        log->recover();
        PartitionConfig c;
        c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        rep = std::make_unique<PartitionReplica>(c, Role::Follower, 9, std::vector<int32_t>{1, 2},
                                                 std::vector<int32_t>{1, 2}, log.get(), &t);
    }
};

} // namespace

// C1 (assign variant): a follower appends epoch-6 data; the data becomes
// durable (seals → recovery point) while the epoch-state image carrying 6 is
// still in flight; power loss reverts the file to its last confirmed image.
// The restarted replica must still own every batch by its stamped epoch.
TEST(HotPathBRegression, EpochHistoryNeverLagsRecoveryPointAcrossPowerLossRegression) {
    TmpDir d;
    constexpr int64_t kRoll = 4096;
    int64_t at6 = -1;
    {
        Rig rig(false, kRoll, &d.p);
        rig.roleOp(false, 3, 2);
        rig.pass();
        applyStamped(rig, 3, 3, 10);
        ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
        const std::string confirmed = readFile(d.p / "leader-epoch-state");
        rig.rep->holdEpochStateCompletionsForTest(true);   // the image carrying epoch 6 never confirms
        rig.roleOp(false, 6, 2);
        rig.pass();
        at6 = rig.log->logEndOffset();
        applyStamped(rig, 6, 6, 120);                      // ~30 rolls: seals make data durable
        (void)rig.passUntil([&] { return rig.log->persistedRecoveryPoint() > at6 + 20; }, 1500);
        EXPECT_LE(rig.log->persistedRecoveryPoint(), at6)
            << "the recovery point passed epoch history that is not durable";
        writeFile(d.p / "leader-epoch-state", confirmed);  // power loss: the unconfirmed slot is gone
        rig.log->crashForTest();
    }
    Restarted r(d.p, kRoll);
    std::string why;
    EXPECT_EQ(lineageMismatches(*r.log, r.rep->leaderEpochCacheForTest(), &why), 0) << why;
    EXPECT_EQ(r.rep->leaderEpochCacheForTest().startOffsetFor(6), at6);
}

// C1 (truncation variant): a divergence truncation's history must be durable
// before the follower re-fetches; a power loss reverting the image never
// leaves a stale epoch owning re-fetched records.
TEST(HotPathBRegression, DivergenceTruncationRefetchWaitsForDurableHistoryRegression) {
    TmpDir d;
    constexpr int64_t kRoll = 4096;
    {
        Rig rig(false, kRoll, &d.p);
        rig.roleOp(false, 3, 2);
        rig.pass();
        applyStamped(rig, 3, 3, 20);
        const int64_t x = rig.log->logEndOffset();
        rig.roleOp(false, 5, 2);
        rig.pass();
        applyStamped(rig, 5, 5, 20);                       // divergent epoch-5 tail [x, ...)
        ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
        const std::string confirmed = readFile(d.p / "leader-epoch-state");
        rig.rep->holdEpochStateCompletionsForTest(true);
        rig.roleOp(false, 6, 2);
        rig.pass();
        rune::replication::ErrorPayloadBuf epb;
        WorkerOp op;
        op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
        op.replicatedPayload.leaderEpoch = 6;
        op.replicatedPayload.partition = 0;
        op.replicatedPayload.topicId = 1;
        op.replicatedPayload.errorCode = rune::replication::kErrDivergingEpoch;
        const auto pl = epb.set(3, x);
        op.replicatedPayload.records.assign(pl.begin(), pl.end());
        ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));
        ASSERT_TRUE(rig.passUntil([&] { return rig.log->logEndOffset() == x; })) << "truncation never ran";
        EXPECT_TRUE(rig.rep->refetchAwaitsEpochStateForTest()) << "re-fetch did not wait for the durable history";
        applyStamped(rig, 6, 6, 60);                       // the new leader's records
        (void)rig.passUntil([&] { return rig.log->persistedRecoveryPoint() > x + 20; }, 1500);
        writeFile(d.p / "leader-epoch-state", confirmed);
        rig.log->crashForTest();
    }
    Restarted r(d.p, kRoll);
    std::string why;
    EXPECT_EQ(lineageMismatches(*r.log, r.rep->leaderEpochCacheForTest(), &why), 0) << why;
    EXPECT_LT(r.rep->leaderEpochCacheForTest().startOffsetFor(5), 0) << "the truncated epoch 5 survived";
}

// C1: once the truncated history is durable the re-fetch goes by itself
// (event-driven on the completion) and replication resumes.
TEST(HotPathBRegression, RefetchResumesOnDurableHistoryRegression) {
    Rig rig(false, 4096);
    rig.roleOp(false, 3, 2);
    rig.pass();
    applyStamped(rig, 3, 3, 4);
    const int64_t x = rig.log->logEndOffset();
    rig.roleOp(false, 5, 2);
    rig.pass();
    applyStamped(rig, 5, 5, 4);
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
    rig.rep->holdEpochStateCompletionsForTest(true);
    rig.roleOp(false, 6, 2);
    rig.pass();
    rune::replication::ErrorPayloadBuf epb;
    WorkerOp op;
    op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
    op.replicatedPayload.leaderEpoch = 6; op.replicatedPayload.partition = 0; op.replicatedPayload.topicId = 1;
    op.replicatedPayload.errorCode = rune::replication::kErrDivergingEpoch;
    const auto pl = epb.set(3, x);
    op.replicatedPayload.records.assign(pl.begin(), pl.end());
    ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));
    ASSERT_TRUE(rig.passUntil([&] { return rig.log->logEndOffset() == x; }));
    ASSERT_TRUE(rig.rep->refetchAwaitsEpochStateForTest());
    rig.rep->releaseHeldEpochStateForTest();
    ASSERT_TRUE(rig.passUntil([&] { return !rig.rep->refetchAwaitsEpochStateForTest(); }));
    applyStamped(rig, 6, 6, 4);
    EXPECT_GT(rig.log->logEndOffset(), x) << "replication did not resume";
}

// Md1: a deferred leadership cancelled while its image (carrying the new
// epoch's entry) is in flight never leaves a phantom epoch after a crash.
TEST(HotPathBRegression, CancelledLeadershipLeavesNoPhantomEpochAfterCrashRegression) {
    TmpDir d;
    constexpr int64_t kRoll = 4096;
    {
        Rig rig(/*controllerManaged=*/true, kRoll, &d.p);
        rig.roleOp(false, 2, 2);
        rig.pass();
        applyStamped(rig, 2, 2, 10);
        ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
        rig.rep->holdEpochStateCompletionsForTest(true);
        rig.roleOp(true, 3);                               // deferred: image (fence 3, 3:leo) lands, never confirmed
        rig.pass();
        ASSERT_TRUE(rig.rep->leadershipPendingForTest());
        rig.roleOp(false, 4, 2);                           // cancelled
        rig.pass();
        applyStamped(rig, 4, 4, 120);
        (void)rig.passUntil([&] { return rig.log->persistedRecoveryPoint() > rig.log->logEndOffset() / 2; }, 1500);
        rig.log->crashForTest();
    }
    Restarted r(d.p, kRoll);
    std::string why;
    EXPECT_EQ(lineageMismatches(*r.log, r.rep->leaderEpochCacheForTest(), &why), 0) << why;
    EXPECT_LT(r.rep->leaderEpochCacheForTest().startOffsetFor(3), 0) << "phantom epoch 3 (never led) survived";
}

// Md2: a failed (non-leadership) epoch-state write is retried and becomes
// durable; kMaxEpochStateFailures failures in a row fail the log (fail-stop).
TEST(HotPathBRegression, FailedEpochStateWriteRetriesThenFailsStopRegression) {
    {
        Rig rig(false, 4096);
        rig.roleOp(false, 3, 2);
        rig.pass();
        ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
        rig.rep->failNextEpochStateCompletionsForTest(1, EIO);
        applyStamped(rig, 3, 3, 2);                         // assign 3 → write → EIO
        ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); })) << "never retried";
        EXPECT_EQ(rig.rep->epochStateErrorsForTest(), 1u);
        EXPECT_FALSE(rig.log->storageFailed());
        rune::storage::LeaderEpochCache c;
        ASSERT_TRUE(PartitionReplica::readDurableEpochCache(rig.dir.p.string(), c));
        EXPECT_EQ(c.latestEpoch(), 3);
    }
    {
        Rig rig(false, 4096);
        rig.roleOp(false, 3, 2);
        rig.pass();
        ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
        rig.rep->failNextEpochStateCompletionsForTest(100, EIO);
        applyStamped(rig, 3, 3, 2);
        EXPECT_TRUE(rig.passUntil([&] { return rig.log->storageFailed(); }, 3000))
            << "repeated epoch-state write failures never failed the log";
        EXPECT_EQ(rig.rep->epochStateErrorsForTest(), static_cast<uint64_t>(PartitionReplica::kMaxEpochStateFailures));
    }
}

// Md3: an epoch-state completion that cannot be routed never wedges a
// deferred promotion — it is rewritten and the leadership completes.
TEST(HotPathBRegression, UnroutableEpochStateCompletionNeverWedgesPromotionRegression) {
    Rig rig(/*controllerManaged=*/true);
    rig.roleOp(false, 2, 2);
    rig.pass();
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
    rig.worker().forceRouteFailForTest(1);
    rig.roleOp(true, 3);
    rig.pass();
    EXPECT_TRUE(rig.passUntil([&] { return rig.rep->role() == Role::Leader; }, 3000)) << "promotion wedged";
    EXPECT_EQ(rig.worker().epochStateNotRoutedForTest(), 1u);
}

// Md4: an idle partition keeps no epoch-state fd open and its file is small.
TEST(HotPathBRegression, IdlePartitionHoldsNoEpochStateFdRegression) {
    Rig rig(/*controllerManaged=*/true);
    rig.roleOp(false, 2, 2);
    rig.pass();
    rig.roleOp(true, 3);
    rig.pass();
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->role() == Role::Leader && rig.rep->epochStateDurable(); }));
    EXPECT_FALSE(rig.rep->epochStateFdOpenForTest()) << "an fd per idle partition";
    EXPECT_LE(fs::file_size(rig.dir.p / "leader-epoch-state"), 8192u);
}

// Md6: the writer is never switched while a write is in flight.
TEST(HotPathBRegression, EpochStateWriterToggleWhileInFlightRefusedRegression) {
    Rig rig(/*controllerManaged=*/true);
    rig.roleOp(false, 2, 2);
    rig.pass();
    rig.rep->holdEpochStateCompletionsForTest(true);
    rig.roleOp(true, 3);
    rig.pass();
    ASSERT_FALSE(rig.rep->epochStateDurable());
#if defined(NDEBUG)
    rig.rep->setExternalFollowerFlush(false);
    EXPECT_TRUE(rig.rep->externalFollowerFlush()) << "switched to the inline writer with a write in flight";
#else
    EXPECT_DEATH(rig.rep->setExternalFollowerFlush(false), "");
#endif
    rig.rep->releaseHeldEpochStateForTest();
}

// M1 ruling (no downgrade support): legacy-format directories migrate; once
// leader-epoch-state holds a durable image the legacy files are gone; a stale
// legacy file never wins over the state file.
TEST(HotPathBRegression, LegacyEpochFilesMigrateAndAreRemovedRegression) {
    PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    rune::replication::NullPartitionTransport t;
    auto populate = [](const fs::path& p) {
        rune::storage::Log log(p.string());
        log.recover();
        log.append(stampedBatch(0, 3, 1), 2);
        log.append(stampedBatch(3, 2, 2), 4);
    };
    // (a) stale legacy-only directory: the legacy checkpoint misses epoch 2.
    {
        TmpDir d;
        populate(d.p);
        rune::storage::LeaderEpochCache stale;
        stale.assign(1, 0);
        stale.checkpointToFile((d.p / "leader-epoch-checkpoint").string());
        writeFile(d.p / "leader-epoch-fence", "2\n");
        {
            rune::storage::Log log(d.p.string());
            log.recover();
            PartitionReplica r(c, Role::Follower, 2, {1}, {1}, &log, &t);
            r.setExternalFollowerFlush(true);
            EXPECT_EQ(r.leaderEpochCacheForTest().startOffsetFor(2), 3) << "stale legacy history kept";
            EXPECT_GE(r.restartFenceEpochForTest(), 2);
        }
        EXPECT_FALSE(fs::exists(d.p / "leader-epoch-checkpoint"));
        EXPECT_FALSE(fs::exists(d.p / "leader-epoch-fence"));
        rune::storage::LeaderEpochCache k;
        ASSERT_TRUE(PartitionReplica::readDurableEpochCache(d.p.string(), k));
        EXPECT_EQ(k.startOffsetFor(2), 3);
        EXPECT_GE(rune::storage::LeaderEpochState::read(d.p.string()).fence, 2);
    }
    // (b) both present (a stale legacy checkpoint next to the state file): the
    // state file wins, the legacy files are removed.
    {
        TmpDir d;
        populate(d.p);
        {
            rune::storage::Log log(d.p.string());
            log.recover();
            PartitionReplica r(c, Role::Follower, 2, {1}, {1}, &log, &t);
            r.setExternalFollowerFlush(true);
        }
        rune::storage::LeaderEpochCache stale;
        stale.assign(9, 0);
        stale.checkpointToFile((d.p / "leader-epoch-checkpoint").string());
        {
            rune::storage::Log log(d.p.string());
            log.recover();
            PartitionReplica r(c, Role::Follower, 2, {1}, {1}, &log, &t);
            EXPECT_EQ(r.leaderEpochCacheForTest().latestEpoch(), 2) << "the stale legacy checkpoint won";
        }
        EXPECT_FALSE(fs::exists(d.p / "leader-epoch-checkpoint"));
    }
    // (c) corrupt state file + legacy: rebuilt from the stamps (never legacy),
    // re-created, legacy removed.
    {
        TmpDir d;
        populate(d.p);
        {
            rune::storage::Log log(d.p.string());
            log.recover();
            PartitionReplica r(c, Role::Follower, 2, {1}, {1}, &log, &t);
            r.setExternalFollowerFlush(true);
        }
        writeFile(d.p / "leader-epoch-state", std::string(8192, 'X'));
        rune::storage::LeaderEpochCache stale;
        stale.assign(9, 0);
        stale.checkpointToFile((d.p / "leader-epoch-checkpoint").string());
        {
            rune::storage::Log log(d.p.string());
            log.recover();
            PartitionReplica r(c, Role::Follower, 2, {1}, {1}, &log, &t);
            EXPECT_EQ(r.leaderEpochCacheForTest().latestEpoch(), 2);
            EXPECT_EQ(r.leaderEpochCacheForTest().startOffsetFor(2), 3);
        }
        EXPECT_FALSE(fs::exists(d.p / "leader-epoch-checkpoint"));
        EXPECT_EQ(rune::storage::LeaderEpochState::read(d.p.string()).state,
                  rune::storage::LeaderEpochState::FileState::Valid);
    }
}

// M2: a divergence truncation inside the active segment changes no directory
// entry (in-place ftruncate, index entries zeroed in its mapping) — so it
// costs no directory fsync (old: a rebuilt segment + index rename + an async
// directory fsync per truncation, a journal commit overlapping intake).
TEST(HotPathBRegression, InSegmentTruncationNeedsNoDirectoryFsyncRegression) {
    Rig rig(false, 64 * 1024 * 1024);
    rig.roleOp(false, 3, 2);
    rig.pass();
    applyStamped(rig, 3, 3, 10);
    const int64_t x = rig.log->logEndOffset();
    rig.roleOp(false, 5, 2);
    rig.pass();
    applyStamped(rig, 5, 5, 10);
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
    const uint64_t dirMut = rig.log->directoryMutationsForTest();
    const uint64_t dirSyncs = rig.worker().dirSyncSubmitsForTest();
    rig.roleOp(false, 6, 2);
    rig.pass();
    rune::replication::ErrorPayloadBuf epb;
    WorkerOp op;
    op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
    op.replicatedPayload.leaderEpoch = 6; op.replicatedPayload.partition = 0; op.replicatedPayload.topicId = 1;
    op.replicatedPayload.errorCode = rune::replication::kErrDivergingEpoch;
    const auto pl = epb.set(3, x);
    op.replicatedPayload.records.assign(pl.begin(), pl.end());
    ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));
    ASSERT_TRUE(rig.passUntil([&] { return rig.log->logEndOffset() == x && rig.rep->epochStateDurable(); }));
    for (int i = 0; i < 16; ++i) rig.pass();
    EXPECT_EQ(rig.log->directoryMutationsForTest(), dirMut) << "the truncation changed the directory";
    EXPECT_EQ(rig.worker().dirSyncSubmitsForTest(), dirSyncs) << "a directory fsync per truncation";
    EXPECT_EQ(rig.log->inPlaceTruncationsForTest(), 1u);
    // Readable and appendable at the cut; the index serves lookups in range.
    applyStamped(rig, 6, 6, 4);
    const auto v = rig.log->readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    std::string why;
    EXPECT_EQ(lineageMismatches(*rig.log, rig.rep->leaderEpochCacheForTest(), &why), 0) << why;
    EXPECT_EQ(rig.log->readView(x, 1 << 20).baseOffset, x);
}

// M2 guard: an in-place truncated active segment recovers to exactly the
// truncated content after a crash (the stale index tail never survives).
TEST(HotPathBRegression, InPlaceTruncationRecoversAfterCrashRegression) {
    TmpDir d;
    {
        rune::storage::Log log(d.p.string(), 1 << 20);
        log.recover();
        log.setDeferSealSync(true);
        for (int i = 0; i < 400; ++i) { const auto b = stampedBatch(i * 2, 2, 1); log.append(b, i * 2 + 1); }
        ASSERT_EQ(log.checkpointRecoveryPoint(), 0);
        // No owner here: truncateTo lowers the recovery point inline (persistAtMost_).
        ASSERT_TRUE(log.truncateTo(301));
        ASSERT_EQ(log.logEndOffset(), 300);
        EXPECT_EQ(log.inPlaceTruncationsForTest(), 1u);
        const auto b = stampedBatch(300, 2, 2);
        log.append(b, 301);
        log.crashForTest();
    }
    rune::storage::Log log(d.p.string(), 1 << 20);
    log.recover();
    EXPECT_EQ(log.logEndOffset(), 302);
    for (int64_t o : {0, 100, 298, 300}) {
        const auto v = log.readView(o, 1);
        ASSERT_FALSE(v.empty()) << o;
        EXPECT_LE(v.baseOffset, o);
        EXPECT_GT(v.nextOffset, o);
    }
}


// Md5: address space is bounded — an idle partition's active segment maps
// 1 MiB; a filling one grows (off the owner, once) to page-rounded
// rollBytes + one maximal batch — never a power-of-two 128 MiB per segment.
TEST(HotPathBRegression, ReadMapAddressSpaceIsBoundedAndGrownOffOwnerRegression) {
    constexpr int64_t kRoll = 6 * 1024 * 1024 + 4096;   // not a power of two
    Rig rig(false, kRoll, nullptr, /*provisioned=*/true);
    EXPECT_LE(rig.log->readView(0, 1).len + 0u, 0u);
    const auto frame = produceFrame(1, 1, 0, 4000, 8);  // ~32 KiB batches
    for (int i = 0; i < 4; ++i) rig.pass(&frame);
    (void)rig.log->readView(0, 1);
    // Idle-ish: the mapping is the 1 MiB minimum.
    // (sizes observed through the growth counter / final size below)
    for (int i = 0; i < 110; ++i) {                     // ~3.5 MiB: past half of the roll
        rig.pass(&frame);
        (void)rig.log->readView(0, 1);
    }
    ASSERT_TRUE(rig.passUntil([&] { (void)rig.log->readView(0, 1); return rig.log->readMapGrowthsForTest() > 0; }));
    EXPECT_EQ(rig.log->activeReadMapRemapsForTest(), 0u) << "a read-map remap on the owner";
    EXPECT_EQ(rig.log->segmentBaseOffsets().size(), 1u);
    EXPECT_EQ(rune::storage::LogSegment::readMapBytesFor(kRoll), static_cast<std::size_t>(kRoll) + (1u << 20));
    EXPECT_EQ(rune::storage::LogSegment::readMapBytesFor(64LL << 20), (64u << 20) + (1u << 20))
        << "64 MiB roll: 65 MiB per active segment (was 128 MiB)";
    // FREE-tier sizing: 10k partitions, ~1% actively filling a segment:
    // 9.9k x 1 MiB + 100 x 65 MiB ≈ 16 GiB of address space (was 10k x 128 MiB = 1.25 TiB).
}

// ===========================================================================
// Re-review (2026-10-06, rereview of bfc92af): N1-N6.
// ===========================================================================
namespace {
/// A worker-driven (deferred-seal) provisioned log whose provisioner is driven
/// by hand (or not at all).
struct ProvLog {
    TmpDir d;
    rune::node::SegmentProvisionerAgent prov{1};
    std::unique_ptr<rune::storage::Log> log;
    int64_t next = 0;
    explicit ProvLog(int64_t roll = 64LL << 20) {
        log = std::make_unique<rune::storage::Log>(d.p.string(), roll);
        log->recover();
        log->setDeferSealSync(true);
        prov.addToViewForTest(log.get());
        log->setProvisioning(prov.hook(), 50);
    }
    void appendBytes(std::size_t bytes) {   // ~16 KiB batches
        for (std::size_t done = 0; done < bytes;) {
            rune::protocol::RecordBatchBuilder b(next, 1000);
            for (int k = 0; k < 2; ++k) b.append("k", std::string(8000, 'x'), 1000 + k);
            auto v = b.build();
            log->appendNoSync(v.data(), v.size(), next + 1);
            next += 2;
            done += v.size();
        }
    }
};
} // namespace

// N1: an in-place truncation over a PARTIAL read map (growth requested, never
// installed) cuts exactly at the requested offset — never at the mapped prefix.
TEST(HotPathBRegression, InPlaceTruncationOverPartialReadMapCutsAtTheOffsetRegression) {
    ProvLog p;
    p.appendBytes(200 * 1024);
    (void)p.log->readView(0, 1);           // served: the owner no longer remaps this segment
    p.appendBytes(1400 * 1024);            // ~1.6 MiB: past the 1 MiB mapping (growth never served)
    const int64_t x = (p.next / 2) * 2 - 20;   // a batch boundary well past 1 MiB
    ASSERT_GT(x * 8100, int64_t{1} << 20);
    ASSERT_TRUE(p.log->truncateTo(x));
    EXPECT_EQ(p.log->logEndOffset(), x) << "the cut landed below the requested offset (records deleted)";
    EXPECT_FALSE(p.log->storageFailed());
}

// N2: a late grown mapping answering a superseded request is never installed
// into another segment.
TEST(HotPathBRegression, LateReadMapForSupersededRequestIsDiscardedRegression) {
    ProvLog p(2LL << 20);
    p.appendBytes(600 * 1024);              // past half of the 1 MiB map: request #1 (segment A)
    p.prov.holdGrowsForTest(true);
    (void)p.prov.doWork(0);                 // request #1 held
    p.appendBytes(1600 * 1024);             // past the roll size
    for (int i = 0; i < 64 && p.log->segmentBaseOffsets().size() < 2; ++i) { (void)p.prov.doWork(0); p.appendBytes(16 * 1024); }
    ASSERT_GE(p.log->segmentBaseOffsets().size(), 2u) << "precondition: rolled to segment B";
    const int64_t bBase = p.log->segmentBaseOffsets().back();
    p.appendBytes(1200 * 1024);             // B past its 1 MiB map
    p.log->expireReadMapRequestForTest();
    (void)p.log->readView(bBase, 1);        // re-request for B (request #1 superseded)
    p.prov.holdGrowsForTest(false);         // A's late mapping arrives
    (void)p.log->readView(bBase, 1);
    EXPECT_GE(p.log->readMapsDiscardedForTest(), 1u) << "the superseded mapping was not discarded";
    const auto v = p.log->readView(bBase, 1);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.baseOffset, bBase) << "segment B serves another file's bytes";
    (void)p.prov.doWork(0);
    (void)p.log->readView(bBase, 1);
    const auto w = p.log->readView(p.next - 2, 1);
    ASSERT_FALSE(w.empty());
    EXPECT_EQ(w.baseOffset, p.next - 2);
}

// N3: readers never stall on the provisioner — data past the mapping is served.
TEST(HotPathBRegression, ReadsPastTheMappingNeverStallOnTheProvisionerRegression) {
    ProvLog p;
    p.appendBytes(200 * 1024);
    (void)p.log->readView(0, 1);
    p.appendBytes(1400 * 1024);             // provisioner never runs
    const int64_t at = p.next - 2;
    const auto v = p.log->readView(at, 1);
    ASSERT_FALSE(v.empty()) << "a follower / consumer read past the mapping stalled on the provisioner";
    EXPECT_EQ(v.baseOffset, at);
    EXPECT_GE(p.log->ownerFallbackRemapsForTest(), 1u);
}

// N4: the re-fetch wait is released by ANY path that made the history durable
// (here: an inline write after a failed async one) — never wedged.
TEST(HotPathBRegression, RefetchWaitReleasedByInlineDurabilityRegression) {
    Rig rig(false, 4096);
    rig.roleOp(false, 3, 2);
    rig.pass();
    applyStamped(rig, 3, 3, 4);
    const int64_t x = rig.log->logEndOffset();
    rig.roleOp(false, 5, 2);
    rig.pass();
    applyStamped(rig, 5, 5, 4);
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateDurable(); }));
    rig.rep->holdEpochStateCompletionsForTest(true);
    rig.roleOp(false, 6, 2);
    rig.pass();
    rune::replication::ErrorPayloadBuf epb;
    WorkerOp op;
    op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
    op.replicatedPayload.leaderEpoch = 6; op.replicatedPayload.partition = 0; op.replicatedPayload.topicId = 1;
    op.replicatedPayload.errorCode = rune::replication::kErrDivergingEpoch;
    const auto pl = epb.set(3, x);
    op.replicatedPayload.records.assign(pl.begin(), pl.end());
    ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));
    ASSERT_TRUE(rig.passUntil([&] { return rig.log->logEndOffset() == x; }));
    ASSERT_TRUE(rig.rep->refetchAwaitsEpochStateForTest());
    ASSERT_TRUE(rig.passUntil([&] { return rig.rep->epochStateCompletionHeldForTest(); }));
    rig.rep->failNextEpochStateCompletionsForTest(1, EIO);
    rig.rep->releaseHeldEpochStateForTest();          // async write failed: still due (backoff)
    ASSERT_FALSE(rig.rep->epochStateDurable());
    ASSERT_TRUE(rig.rep->writeEpochStateInlineForTest());   // durable through the inline path
    EXPECT_FALSE(rig.rep->refetchAwaitsEpochStateForTest()) << "the re-fetch wait wedged";
}

// N5: a missing recovery-point file next to epoch history is created at the
// held (startup) value — never at the log end.
TEST(HotPathBRegression, MissingRecoveryPointFileHonoursTheEpochGuardRegression) {
    TmpDir d;
    {
        rune::storage::Log log(d.p.string(), 4096);
        log.recover();
        for (int i = 0; i < 60; ++i) log.append(stampedBatch(i * 2, 2, 1), i * 2 + 1);
    }
    rune::storage::LeaderEpochCache c;
    c.assign(1, 0);
    c.checkpointToFile((d.p / "leader-epoch-checkpoint").string());
    fs::remove(d.p / rune::storage::Log::kRecoveryPointFile);
    rune::storage::Log log(d.p.string(), 4096);
    log.recover();
    ASSERT_LT(log.startupRecoveryPoint(), log.logEndOffset()) << "precondition: several segments";
    EXPECT_EQ(log.persistedRecoveryPoint(), log.startupRecoveryPoint())
        << "the new recovery-point file trusts epoch history that was never reconciled";
}

// N6: a failed update of a valid state file never falls back to a second
// (legacy) authority — the log fails instead.
TEST(HotPathBRegression, FailedEpochStateUpdateNeverFallsBackToLegacyRegression) {
    TmpDir d;
    {
        rune::storage::Log log(d.p.string());
        log.recover();
        log.append(stampedBatch(0, 3, 1), 2);
        log.append(stampedBatch(3, 2, 2), 4);
    }
    {   // a valid but stale state file (misses epoch 2)
        const std::vector<std::pair<int32_t, int64_t>> e{{1, 0}};
        const int fd = rune::storage::LeaderEpochState::create(d.p.string(), 4096, 1, 1, e.begin(), e.end());
        ASSERT_GE(fd, 0);
        ::close(fd);
    }
    PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    rune::replication::NullPartitionTransport t;
    rune::storage::Log log(d.p.string());
    log.recover();
    PartitionReplica::failInlineEpochStateWritesForTest(1);
    PartitionReplica r(c, Role::Follower, 2, {1}, {1}, &log, &t);
    PartitionReplica::failInlineEpochStateWritesForTest(0);
    EXPECT_FALSE(fs::exists(d.p / "leader-epoch-checkpoint")) << "a second (legacy) epoch authority was written";
    EXPECT_TRUE(log.storageFailed());
}


// D15 re-review CRITICAL (m5): a MEMORY-backed user replica's restart fence
// goes through the owning Worker's async epoch-state writer — the real Worker
// path, not a hand-driven one.  The Worker submitted epoch-state writes for
// disk-backed replicas only, so a memory replica's deferred leadership waited
// forever (memory partitions never led in a multi-node cluster).  It must lead
// once its fence is durable — and not before.
TEST(HotPathBRegression, MemoryReplicaFenceWrittenByTheWorkerLeadsWhenDurableRegression) {
    TmpDir d;
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1)};
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    c.replicaLagMaxOffset = 1'000'000'000;
    auto r = std::make_unique<PartitionReplica>(c, Role::Follower, 1, std::vector<int32_t>{1},
                                                std::vector<int32_t>{1}, &log, &transport);
    r->setExternalFollowerFlush(true);
    r->setIsrChannel(&transport);
    const fs::path stateDir = d.p / "1-0.state";
    r->setMemoryEpochStateDir(stateDir.string(), (d.p / "1-0").string());
    PartitionReplica* rep = r.get();
    reg.add(1, 0, std::move(r));
    pool.setRegistry(&reg);
    Worker& w = pool.worker(0);
    w.setWorkerIngress(nullptr, /*active=*/true);
    w.setShardMode(true);
    pool.assignFromRegistry(reg);
    int64_t now = 1'000'000'000;
    auto pass = [&] { now += 1'000'000; (void)w.doWork(now); };
    pass();
    WorkerOp op;
    op.type = WorkerOp::Type::BecomeLeader;
    op.topicId = 1; op.partition = 0; op.roleEpoch = 3; op.isr = {1};
    ASSERT_TRUE(pool.tryEnqueueOp(1, 0, std::move(op)));
    pass();   // the role-op pass: submits the write, never waits for it
    EXPECT_NE(rep->role(), Role::Leader) << "led at epoch 3 before its fence was durable";
    EXPECT_TRUE(rep->leadershipPendingForTest());
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (rep->role() != Role::Leader && std::chrono::steady_clock::now() < end) { pass(); std::this_thread::yield(); }
    ASSERT_EQ(rep->role(), Role::Leader) << "the memory replica's fence write was never submitted: it never leads";
    EXPECT_EQ(rep->epoch(), 3);
    EXPECT_EQ(rep->epochStateInlineWritesForTest(), 0u) << "a synchronous epoch write on the worker";
    const auto st = rune::storage::LeaderEpochState::read(stateDir.string());
    ASSERT_EQ(st.state, rune::storage::LeaderEpochState::FileState::Valid);
    EXPECT_GE(st.fence, 3);
}
