#pragma once
// HotPathBRig.h — the shared rig of the hot-path B regression tests
// (HotPathBRegressionTest.cpp, HotPathBAllocRegressionTest.cpp).  Test-only.
#include <sys/stat.h>
#include <fcntl.h>
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
#include <execinfo.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <map>
#include <memory>
#include <cstdlib>
#include <new>
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

}  // namespace

namespace {

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
                 bool provisioned = false, bool withFollower = false) {
        dir.p = at ? *at : ownDir.p;
        log = std::make_unique<rune::storage::Log>(dir.p.string(), rollBytes);
        log->recover();
        if (provisioned) {
            prov = std::make_unique<rune::node::SegmentProvisionerAgent>(1);
            prov->addToViewForTest(log.get());
            log->setProvisioning(prov->channel(), 50);
        }
        PartitionConfig c;
        c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.followerFlushIntervalNs = 0;
        const std::vector<int32_t> set = withFollower ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, set, set, log.get(), &transport);
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
    /// D26: follower 2's FetchRequest, as the replication transport hands it to the owning worker.
    void fetch(int64_t from) {
        WorkerOp op;
        op.type = WorkerOp::Type::FetchRequest; op.topicId = 1; op.partition = 0; op.replica = rep;
        op.fetchRequestPayload.leaderEpoch = 1;
        op.fetchRequestPayload.partition   = 0;
        op.fetchRequestPayload.followerId  = 2;
        op.fetchRequestPayload.fetchOffset = from;
        op.fetchRequestPayload.maxBytes    = 512 * 1024;
        op.fetchRequestPayload.topicId     = 1;
        op.fetchRequestPayload.nowNs       = now;
        ASSERT_TRUE(pool.tryEnqueueOp(1, 0, std::move(op)));
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
