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
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
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
    TmpDir                                    dir;
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry               reg;
    WorkerPool                                pool{1, rune::metadata::ShardRouter(1)};
    std::unique_ptr<rune::storage::Log>       log;
    PartitionReplica*                         rep = nullptr;
    int64_t                                   now = 1'000'000'000;

    explicit Rig(bool controllerManaged = false, int64_t rollBytes = 64 * 1024 * 1024) {
        log = std::make_unique<rune::storage::Log>(dir.p.string(), rollBytes);
        log->recover();
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
    Rig rig(/*controllerManaged=*/false, /*rollBytes=*/16 * 1024 * 1024);
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
