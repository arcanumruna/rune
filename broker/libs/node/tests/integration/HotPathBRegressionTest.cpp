// HotPathBRegressionTest.cpp — worker hot-path latency fixes (brief
// 2026-10-05-hotpath-B): deterministic regressions for the behaviour behind the
// latency-gate findings, single-threaded (the test thread is the worker owner).
//
//   F5  PushRead answers are gathered from the segment read mmap straight into
//       the return ring, bounded per worker pass (Worker::kPushPassBudgetBytes)
//       and fair (budget-deferred reads are served first next pass).
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <filesystem>
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

    Rig() {
        log = std::make_unique<rune::storage::Log>(dir.p.string(), 64 * 1024 * 1024);
        log->recover();
        PartitionConfig c;
        c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.followerFlushIntervalNs = 0;
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                    std::vector<int32_t>{1}, log.get(), &transport);
        r->setExternalFollowerFlush(true);
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
