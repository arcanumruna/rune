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

    /// `leaderTransport` (optional): receives the leader's FetchResponses / pushes
    /// (the seeded stress drives a real follower from them); default: dropped.
    explicit Rig(std::size_t ringBytes = 4u << 20, std::size_t maxBatches = 100'000, bool withFollower = false,
                 rune::replication::PartitionTransport* leaderTransport = nullptr)
        : mem(ringBytes, maxBatches) {
        PartitionConfig c;
        c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.replicaLagMaxMs = 600'000;
        c.followerFlushIntervalNs = 0;
        const std::vector<int32_t> set = withFollower ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, set, set, &mem,
                                                    leaderTransport != nullptr ? leaderTransport : &transport);
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
