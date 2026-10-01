// ShardWorkerAckTest.cpp — the worker acks the client directly on its own session
// publication (REAL invoker driver, aeron:ipc), notifies the runtime after the ack,
// and fails parked acks=all promptly on demotion.
//
// F15 reconciliation: the post-ack notification is the existing facts-only
// ProduceResult (deferAck == true: PSM-mirror facts + session liveness, never an
// answer) — WorkerReturn PRODUCE_NOTIFY(2) was retired by F15 §7 (RESERVED_2).
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <span>
#include <unistd.h>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int kCap = 200'000;
constexpr int32_t kIngress = 7301, kEgress = 7302;
class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {}
};

struct Fixture {
    fs::path dir, adir;
    std::unique_ptr<rune::storage::Log> log;
    NullTransport t;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::unique_ptr<WorkerIngress> ing;
    // unique_ptr: the worker (its session publications) must die BEFORE the Aeron client.
    std::unique_ptr<Worker> worker = std::make_unique<Worker>(0, 1 << 16, 1 << 16, 64, 64);
    std::shared_ptr<aeron::ExclusivePublication> producePub;
    std::shared_ptr<aeron::Subscription> egress;
    std::map<int64_t, pc::ProduceResponse> acks;
    aeron::FragmentAssembler asmb{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                         aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o),
                                                 static_cast<std::size_t>(l)))
            acks[r->correlationId] = *r;
    }};
    int64_t now = 1'000;

    explicit Fixture(std::vector<rune::replication::NodeId> replicas, int minIsr) {
        static std::atomic<int> seq{0};
        const std::string tag = std::to_string(::getpid()) + "_" + std::to_string(seq++);
        dir  = fs::temp_directory_path() / ("rune_swa_log_" + tag);
        adir = fs::temp_directory_path() / ("rune_swa_aeron_" + tag);
        fs::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir.string(), 64LL * 1024 * 1024);
        log->recover();
        PartitionConfig c; c.topicId = 1; c.selfId = 1; c.minIsr = minIsr;
        c.fetchIntervalNs = 1'000'000'000; c.isrCheckIntervalNs = 1'000'000'000'000LL;
        c.replicaLagMaxMs = 600'000; c.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(c, Role::Leader, 1, replicas, replicas, log.get(), &t);
        EmbeddedDriverConfig dc; dc.dir = adir.string(); dc.termBufferBytes = 64 * 1024;
        drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(adir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        ing = std::make_unique<WorkerIngress>(client, "aeron:ipc", kIngress, drv->pumpWith(client.get()));
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setWorkerIngress(ing.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(client.get());
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress);
        const int64_t preg = client->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress && producePub && producePub->isConnected()); ++i) {
            pump();
            if (!egress) egress = client->findSubscription(sreg);
            if (!producePub) producePub = client->findExclusivePublication(preg);
        }
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 5;
        a.egressStreamId = kEgress; a.egressChannel = "aeron:ipc";
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    ~Fixture() {
        worker.reset();
        producePub.reset(); egress.reset(); ing.reset(); client.reset(); drv.reset();
        std::error_code ec; fs::remove_all(dir, ec); fs::remove_all(adir, ec);
    }
    void pump() { drv->doWork(); client->conductorAgentInvoker().invoke(); }
    void step() { pump(); worker->doWork(now += 10'000); if (egress) egress->poll(asmb.handler(), 64); }
    void produce(int64_t corr, uint8_t acksAll, int64_t pid = -1, int32_t seq = -1, int64_t sid = 5) {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        pc::Produce m; m.sessionId = sid; m.correlationId = corr; m.topicId = 1; m.partition = 0;
        m.partitionKnown = 1; m.acks = acksAll; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        for (int i = 0; i < kCap && producePub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0; ++i) pump();
    }
    bool awaitAck(int64_t corr) {
        for (int i = 0; i < kCap && !acks.count(corr); ++i) step();
        return acks.count(corr) == 1;
    }
};
} // namespace

TEST(ShardWorkerAckRegression, AcksDirectlyOnTheSessionPublicationNotTheReturnRing) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/42, /*acks=*/0);
    ASSERT_TRUE(f.awaitAck(42));
    EXPECT_EQ(f.acks[42].errorCode, 0);
    int answers = 0, notifies = 0;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::ProduceResult) return;
        if (r.deferAck) ++notifies; else ++answers;
    });
    EXPECT_EQ(answers, 0) << "shard mode: the runtime never relays produce acks";
    EXPECT_EQ(notifies, 1) << "one post-ack notification per ack (facts + session liveness)";
}

TEST(ShardWorkerAckRegression, DemotedLeaderFiresNotLeaderPromptly) {
    Fixture f({1, 2}, /*minIsr=*/2);                          // HW cannot advance without node 2
    f.produce(/*corr=*/7, /*acks=all*/1);
    for (int i = 0; i < 2'000; ++i) f.step();                 // appended + parked
    ASSERT_EQ(f.acks.count(7), 0u);
    f.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    ASSERT_TRUE(f.awaitAck(7));
    EXPECT_EQ(f.acks[7].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(f.now, 1'000 + Worker::kDefaultAckTimeoutNs) << "must not wait for the 30 s ack timeout";
}

TEST(ShardWorkerAckRegression, ProduceNotifyFollowsTheAckOnTheReturnRing) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/9, /*acks=*/0, /*pid=*/11, /*seq=*/0);
    ASSERT_TRUE(f.awaitAck(9));
    int notifies = 0, answers = 0; int64_t pid = -1; int64_t sid = -1; int16_t err = -1;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::ProduceResult) return;
        if (!r.deferAck) { ++answers; return; }
        ++notifies; pid = r.producerId; sid = r.sessionId; err = r.errorCode;
    });
    EXPECT_EQ(answers, 0);
    EXPECT_EQ(notifies, 1);
    EXPECT_EQ(pid, 11) << "the notification carries the PSM facts of the acked append";
    EXPECT_EQ(sid, 5) << "and the session (runtime liveness)";
    EXPECT_EQ(err, 0) << "a notification is never an answer (facts-only invariant)";
}

TEST(ShardWorkerAckRegression, ErrorAckNotifiesLivenessWithoutFacts) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/3, /*acks=*/0, /*pid=*/11, /*seq=*/5);   // OutOfOrderSequence: seq must start at 0
    ASSERT_TRUE(f.awaitAck(3));
    EXPECT_EQ(f.acks[3].errorCode, static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence));
    int notifies = 0; int64_t pid = 0;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult && r.deferAck) { ++notifies; pid = r.producerId; }
    });
    EXPECT_EQ(notifies, 1);
    EXPECT_EQ(pid, -1) << "an error carries no facts — the mirror must never learn a refused batch";
}

TEST(ShardWorkerAckRegression, GatewayRoutedProduceIsDedupedByTheWorkerPsm) {
    // Spec §3.4: in shard mode the worker psm_ is the single owner — a
    // gateway-received user produce (WorkerOp::Produce) runs the SAME produce
    // core as worker ingress, so a retried idempotent batch is appended once.
    Fixture f({1}, 1);
    rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
    const auto batch = b.build();
    auto routed = [&](int64_t corr) {
        WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
        op.replica = f.replica.get(); op.logPtr = f.log.get();
        op.sessionId = 5; op.correlationId = corr; op.acks = 0;
        op.producerId = 11; op.producerEpoch = 0; op.baseSequence = 0;
        op.lastOffsetDelta = 0; op.recordCount = 1;
        op.recordBatchView = std::span<const uint8_t>(batch);
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(op)));
    };
    routed(21);
    ASSERT_TRUE(f.awaitAck(21));
    routed(22);
    ASSERT_TRUE(f.awaitAck(22));
    EXPECT_EQ(f.acks[21].errorCode, 0);
    EXPECT_EQ(f.acks[22].errorCode, 0);
    EXPECT_EQ(f.acks[22].baseOffset, f.acks[21].baseOffset) << "duplicate answered with the original offset";
    EXPECT_EQ(f.log->logEndOffset(), 1) << "the retried batch must not be appended twice";
}

TEST(ShardWorkerAckRegression, ClosedSessionGetsNoAckAndReannounceRestoresIt) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/1, /*acks=*/0);
    ASSERT_TRUE(f.awaitAck(1));
    WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = 5;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    f.produce(/*corr=*/2, /*acks=*/0);
    for (int i = 0; i < 2'000; ++i) f.step();
    EXPECT_EQ(f.acks.count(2), 0u) << "a closed session's acks are dropped (the client is gone)";
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 5;
    a.egressStreamId = kEgress; a.egressChannel = "aeron:ipc";
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    f.produce(/*corr=*/3, /*acks=*/0);
    EXPECT_TRUE(f.awaitAck(3));
}

TEST(ShardWorkerAckRegression, ProduceIsNotPolledBeforeItsSessionAnnouncementIsApplied) {
    // Announce-before-accept end to end on the worker: the runtime queued the
    // announcement BEFORE the client could produce, but the worker cannot drain
    // it this pass (drain limit 1, ops ahead of it).  Polling the produce now
    // would ack an unknown session — dropped, the client hangs.  The shard must
    // apply every queued op first.
    Fixture f({1}, 1);
    for (int i = 0; i < 4; ++i) f.step();                      // session 5 resolved
    f.worker->setIngressDrainLimit(1);
    for (int64_t id = 100; id < 104; ++id) {
        WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = id;
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(c)));
    }
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 6;
    a.egressStreamId = kEgress; a.egressChannel = "aeron:ipc";
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    f.produce(/*corr=*/61, /*acks=*/0, -1, -1, /*sid=*/6);
    ASSERT_TRUE(f.awaitAck(61)) << "the produce of a just-announced session must be acked";
    EXPECT_EQ(f.acks[61].sessionId, 6);
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}
