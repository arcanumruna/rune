// ShardWorkerAckTest.cpp — the worker acks the client directly on its own session
// publication (REAL invoker driver, aeron:ipc), notifies the runtime after the ack,
// and fails parked acks=all promptly on demotion.
//
// F15 reconciliation: the post-ack notification is the existing facts-only
// ProduceResult (deferAck == true: PSM-mirror facts + session liveness, never an
// answer) — WorkerReturn PRODUCE_NOTIFY(2) was retired by F15 §7 (RESERVED_2).
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/OwnerThreadTestTask.h>
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
#include <functional>
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
constexpr int32_t kIngress = 7301, kEgress = 7302, kEgress2 = 7303;
// The ack publications a worker opens to its sessions: a small IPC term (the
// driver default is 64 MiB) so a client that stops reading back-pressures them.
constexpr const char* kEgressChannel = "aeron:ipc?term-length=65536";
bool isFailedSignal(const WorkerReturn& r) {
    return r.type == WorkerReturn::Type::SessionSignal &&
           r.lastOffset == static_cast<int64_t>(SessionSignalKind::Failed);
}
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
    int egressLimit = 64;                                      // session 5's client reads this many acks / step
    bool runtimeDrainsReturns = false;                         // the runtime's ret_ drain (notifications)
    std::vector<WorkerReturn> sessionFailures;
    std::function<void(const WorkerReturn&)> onReturn;         // the emulated runtime's return handler
    // A second client: its own ingress publication (own image) + its own egress.
    std::shared_ptr<aeron::ExclusivePublication> pub2;
    std::shared_ptr<aeron::Subscription> egress2;
    aeron::FragmentAssembler asmb2{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                          aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o),
                                                 static_cast<std::size_t>(l)))
            acks[r->correlationId] = *r;
    }};

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
        a.egressStreamId = kEgress; a.egressChannel = kEgressChannel;
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    ~Fixture() {
        worker.reset();
        pub2.reset(); egress2.reset();
        producePub.reset(); egress.reset(); ing.reset(); client.reset(); drv.reset();
        std::error_code ec; fs::remove_all(dir, ec); fs::remove_all(adir, ec);
    }
    void pump() { drv->doWork(); client->conductorAgentInvoker().invoke(); }
    void step() {
        pump();
        worker->doWork(now += 10'000);
        if (egress && egressLimit > 0) egress->poll(asmb.handler(), egressLimit);
        if (egress2) egress2->poll(asmb2.handler(), 64);
        if (runtimeDrainsReturns)
            worker->drainReturn([this](WorkerReturn r) {
                if (isFailedSignal(r)) sessionFailures.push_back(r);
                if (onReturn) onReturn(r);
            });
    }
    // The second client: a separate ingress publication (a separate image) and
    // egress stream kEgress2; announced as session `sid` on the lifecycle ring.
    void addSecondClient(int64_t sid) {
        connectSecondClient();
        announce(sid, kEgress2);
    }
    void connectSecondClient() {
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress2);
        const int64_t preg = client->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress2 && pub2 && pub2->isConnected()); ++i) {
            pump();
            if (!egress2) egress2 = client->findSubscription(sreg);
            if (!pub2) pub2 = client->findExclusivePublication(preg);
        }
        ASSERT_TRUE(egress2 && pub2 && pub2->isConnected());
    }
    void announce(int64_t sid, int32_t egressStream) {
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = sid;
        a.egressStreamId = egressStream; a.egressChannel = kEgressChannel;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    static std::vector<uint8_t> produceFrame(int64_t sid, int64_t corr, uint8_t acks, int32_t topicId = 1,
                                             std::string_view value = "v") {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", value, 1000);
        const auto batch = b.build();
        pc::Produce m; m.sessionId = sid; m.correlationId = corr; m.topicId = topicId; m.partition = 0;
        m.partitionKnown = 1; m.acks = acks; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        return f;
    }
    /// A gateway-routed produce (WorkerOp::Produce on the op ring), as the
    /// runtime enqueues it for a client that ignores ingressEndpoint.
    bool routedProduce(int64_t sid, int64_t corr, uint8_t acks = 0) {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
        op.replica = replica.get(); op.logPtr = log.get();
        op.sessionId = sid; op.correlationId = corr; op.acks = acks;
        op.producerId = -1; op.producerEpoch = 0; op.baseSequence = -1;
        op.lastOffsetDelta = 0; op.recordCount = 1;
        op.recordBatchView = std::span<const uint8_t>(batch);
        return worker->tryEnqueueOp(std::move(op));
    }
    /// ONE offer attempt (never loops: a paused client's offers back-pressure).
    static bool offerOnce(aeron::ExclusivePublication& pub, std::vector<uint8_t>& f) {
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        return pub.offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }
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

// F15 S5 (Kafka parity, re-review 1 I1 residual): a retry of an acks=all
// batch whose original is still below the HW is answered only when the
// original commits — on shard ingress AND on the gateway-routed op path.
TEST(ShardWorkerAckRegression, AcksAllDuplicateAnsweredOnlyWhenTheOriginalCommits) {
    for (bool routed : {false, true}) {
        Fixture f({1, 2}, /*minIsr=*/1);                       // follower 2 in the ISR: the HW waits for it
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        auto send = [&](int64_t corr) {
            if (!routed) { f.produce(corr, /*acks=all*/1, /*pid=*/11, /*seq=*/0); return; }
            WorkerOp op; op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
            op.replica = f.replica.get(); op.logPtr = f.log.get();
            op.sessionId = 5; op.correlationId = corr; op.acks = 1; op.deferAck = true;
            op.producerId = 11; op.producerEpoch = 0; op.baseSequence = 0;
            op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 1;
            op.recordBatchView = std::span<const uint8_t>(batch);
            ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(op)));
        };
        send(31);
        for (int i = 0; i < 2'000; ++i) f.step();
        ASSERT_EQ(f.acks.count(31), 0u) << "precondition: the original is parked on the HW (routed=" << routed << ")";
        send(32);                                              // the client's retry
        for (int i = 0; i < 2'000; ++i) f.step();
        EXPECT_EQ(f.log->logEndOffset(), 1) << "one copy (routed=" << routed << ")";
        EXPECT_EQ(f.acks.count(32), 0u)
            << "the duplicate was acked before its original reached the HW (routed=" << routed << ")";
        rune::replication::FetchRequestMsg fr{1, 0, 2, f.log->logEndOffset(), 1 << 20, 0};
        (void)f.replica->onFetchRequest(fr, f.now);
        ASSERT_TRUE(f.awaitAck(31));
        ASSERT_TRUE(f.awaitAck(32));
        EXPECT_EQ(f.acks[31].errorCode, 0);
        EXPECT_EQ(f.acks[32].errorCode, 0);
        EXPECT_EQ(f.acks[32].baseOffset, f.acks[31].baseOffset);
    }
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
    a.egressStreamId = kEgress; a.egressChannel = kEgressChannel;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    f.produce(/*corr=*/3, /*acks=*/0);
    EXPECT_TRUE(f.awaitAck(3));
}

TEST(ShardWorkerAckRegression, GatewayRoutedProduceDrainedWithItsJustCommittedAnnouncementIsAcked) {
    // Fix round 2 (finding 5, replaces a test the lifecycle ring made vacuous):
    // the runtime commits a session's announcement and, right after, a gateway-
    // routed Produce of that session — both AFTER this pass drained the
    // lifecycle ring, so the produce op runs in this pass's op drain with its
    // session still unknown.  Its ack must be held until the announcement is
    // applied (next pass), never dropped.  A test task on the op ring commits
    // both exactly there.
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    f.connectSecondClient();
    struct Task : OwnerThreadTestTask {
        Fixture* f; bool routed = false;
        explicit Task(Fixture* fx)
            : OwnerThreadTestTask(Target::Replica, 1, 0, &run, Clock::now() + std::chrono::hours(1)), f(fx) {}
        static void run(OwnerThreadTestTask& t, PartitionReplica*) noexcept {
            auto& self = static_cast<Task&>(t);
            WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 6;
            a.egressStreamId = kEgress2; a.egressChannel = kEgressChannel;
            if (!self.f->worker->tryEnqueueOp(std::move(a))) return;
            self.routed = self.f->routedProduce(6, 61);
        }
    };
    auto task = std::make_shared<Task>(&f);
    WorkerOp t; t.type = WorkerOp::Type::TestTask;
    t.egressPub = new OwnerThreadTestTaskRef(task);
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(t)));
    ASSERT_TRUE(f.awaitAck(61)) << "the produce of a just-announced session must be acked";
    ASSERT_TRUE(task->routed);
    EXPECT_EQ(f.acks[61].sessionId, 6);
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// ===========================================================================
// Task 13 fix round 1.
// ===========================================================================
namespace {
bool allAcked(const Fixture& f, int64_t from, int64_t to) {
    for (int64_t c = from; c < to; ++c) if (!f.acks.count(c)) return false;
    return true;
}
} // namespace

// C1 + m7: a client that stops reading its acks (its publication back-
// pressures) must not stall the shard: a role change through the REAL
// BecomeFollower op and a new session's announce + produce are applied and
// answered within a bounded number of passes while it still holds acks.  (Was:
// >= 1024 held acks paused the whole op ring — role ops, announcements and
// closes stuck behind one slow client; only the 5 s stall timer could free it.)
TEST(ShardWorkerAckRegression, SlowSessionBacklogNeverStallsRoleOpsOrNewSessions) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 0;                                      // session 5's client stops reading acks
    const int64_t c0 = 1'000'000;
    int64_t corr = c0;
    int refused = 0;
    for (int i = 0; i < 20'000 && f.worker->sessionAcksPendingForTest() < 1'100 && refused < 1'000; ++i) {
        auto fr = Fixture::produceFrame(5, corr, 0);
        if (Fixture::offerOnce(*f.producePub, fr)) { ++corr; refused = 0; } else ++refused;
        f.step();
    }
    ASSERT_GT(corr - c0, 400) << "session 5 must have out-run its ack publication";
    ASSERT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "precondition: session 5 holds acks";

    f.addSecondClient(6);
    auto fr6 = Fixture::produceFrame(6, 6'000'001, 0);
    ASSERT_TRUE(Fixture::offerOnce(*f.pub2, fr6));
    for (int i = 0; i < 2'000 && !f.acks.count(6'000'001); ++i) f.step();
    ASSERT_EQ(f.acks.count(6'000'001), 1u)
        << "a new session's announce + produce must not wait behind another client's ack backlog";

    WorkerOp bf; bf.type = WorkerOp::Type::BecomeFollower; bf.topicId = 1; bf.partition = 0;
    bf.replica = f.replica.get(); bf.roleEpoch = 2; bf.roleLeaderId = 2;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(bf)));
    for (int i = 0; i < 2'000 && f.replica->role() != Role::Follower; ++i) f.step();
    EXPECT_EQ(f.replica->role(), Role::Follower) << "a role op must not wait behind a slow client's acks";
    EXPECT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "session 5 still held acks during all of it";
    EXPECT_GT(f.worker->intakePausedFramesForTest(), 0u) << "G-A6: session 5's intake was paused, not the shard's";

    f.egressLimit = 64;                                     // session 5 reads again: nothing was lost
    for (int i = 0; i < 50'000 && !allAcked(f, c0, corr); ++i) f.step();
    EXPECT_TRUE(allAcked(f, c0, corr));
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// C1: a legal client that consumes ONE ack per pass while producing faster
// must never stall another session's intake.  (Was: its held acks grew to the
// queue's capacity and the poll budget of the whole shard reached 0; the stall
// timer never fired because every claim reset it.)
TEST(ShardWorkerAckRegression, TrickleConsumerNeverStallsOtherSessionsIntake) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.addSecondClient(6);
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 1;
    int64_t c5 = 1'000'000;
    const int64_t c6 = 6'000'000;
    int64_t sent6 = 0, late6 = 0;
    for (int i = 0; i < 30'000; ++i) {
        for (int k = 0; k < 8; ++k) {
            auto fr = Fixture::produceFrame(5, c5, 0);
            if (!Fixture::offerOnce(*f.producePub, fr)) break;
            ++c5;
        }
        if (i % 50 == 0) {
            // Session 6's previous produce must have been acked within these 50
            // passes — WHILE session 5 keeps out-producing its consumption.
            if (sent6 > 0 && !f.acks.count(c6 + sent6 - 1)) ++late6;
            auto fr = Fixture::produceFrame(6, c6 + sent6, 0);
            if (Fixture::offerOnce(*f.pub2, fr)) ++sent6;
        }
        f.step();
    }
    EXPECT_GT(c5 - 1'000'000, 30'000) << "precondition: session 5 produced far more than it read";
    EXPECT_EQ(late6, 0) << "session 6 waited behind session 5's held acks";
    for (int i = 0; i < 2'000 && !allAcked(f, c6, c6 + sent6); ++i) f.step();
    EXPECT_EQ(sent6, 600) << "session 6's offers were never back-pressured";
    EXPECT_TRUE(allAcked(f, c6, c6 + sent6)) << "every produce of session 6 must be acked";
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// C2: no ack of an OPEN session is dropped on time.  A client that stalls for
// longer than any window (6 s of worker clock here) still gets every ack once
// it reads again.  (Was: dropped after kStallDropNs = 5 s, silently.)
TEST(ShardWorkerAckRegression, StalledClientGetsEveryAckAfterAnyStall) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 0;
    const int64_t c0 = 2'000'000;
    int64_t corr = c0;
    int refused = 0;
    for (int i = 0; i < 20'000 && corr - c0 < 600 && refused < 1'000; ++i) {
        auto fr = Fixture::produceFrame(5, corr, 0);
        if (Fixture::offerOnce(*f.producePub, fr)) { ++corr; refused = 0; } else ++refused;
        f.step();
    }
    ASSERT_GT(f.worker->sessionAcksPendingForTest(), 0u) << "precondition: acks held for the stalled client";
    f.now += 6'000'000'000LL;                               // the client stays stalled for 6 s
    for (int i = 0; i < 100; ++i) f.step();
    f.egressLimit = 64;
    for (int i = 0; i < 50'000 && !allAcked(f, c0, corr); ++i) f.step();
    EXPECT_TRUE(allAcked(f, c0, corr)) << "a live session's acks are never dropped on a timer";
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// C2: a session whose ack publication can never deliver is REPORTED to the
// runtime (WorkerReturn::SessionFailed → the runtime closes it; the client
// reconnects) — never a silent black hole.
TEST(ShardWorkerAckRegression, UndeliverableSessionPublicationIsReportedToTheRuntime) {
    Fixture f({1}, 1);
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 8;
    a.egressStreamId = kEgress2; a.egressChannel = "aeron:udp?endpoint=127.0.0.1:notaport";
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    int failed = 0; int64_t failedSid = 0;
    for (int i = 0; i < 5'000 && failed == 0; ++i) {
        f.step();
        f.worker->drainReturn([&](WorkerReturn r) {
            if (isFailedSignal(r)) { ++failed; failedSid = r.sessionId; }
        });
    }
    ASSERT_EQ(failed, 1) << "an undeliverable session must be reported";
    EXPECT_EQ(failedSid, 8);
    for (int i = 0; i < 200; ++i) {
        f.step();
        f.worker->drainReturn([&](WorkerReturn r) { if (isFailedSignal(r)) ++failed; });
    }
    EXPECT_EQ(failed, 1) << "reported exactly once";
    EXPECT_EQ(f.worker->sessionRegistrationsDrainingForTest(), 0u)
        << "an errored registration is already gone (real Aeron) — never on the drain list";
}

// I1: never skip the client poll for pending ops — continuous runtime op
// traffic (drain limit below the arrival rate) must not starve shard ingress.
// (Was: the poll ran only with the op ring empty after two drains.)
TEST(ShardWorkerAckRegression, ContinuousOpTrafficNeverSkipsTheIngressPoll) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.worker->setIngressDrainLimit(1);
    auto unsub = [&](int64_t id) {
        WorkerOp u; u.type = WorkerOp::Type::Unsubscribe; u.subscriptionId = id;
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(u)));
    };
    for (int64_t k = 0; k < 3; ++k) unsub(900'000 + k);      // a standing backlog
    auto fr = Fixture::produceFrame(5, 77, 0);
    ASSERT_TRUE(Fixture::offerOnce(*f.producePub, fr));
    for (int i = 0; i < 2'000 && !f.acks.count(77); ++i) {
        unsub(910'000 + 2 * i); unsub(910'001 + 2 * i);     // ops keep arriving
        f.step();
    }
    EXPECT_EQ(f.acks.count(77), 1u) << "shard ingress must be polled every pass";
}

// I1 (aeron:ipc TOCTOU): a client frame can become visible in the same pass as
// its session's announcement is enqueued — AFTER the lifecycle ring was drained
// and BEFORE the poll.  Its ack must be HELD until the announcement is applied
// (next pass), never dropped as "unknown session".  A test task on the op ring
// runs exactly there (op drain → poll).
namespace {
struct MidPassAnnounce : OwnerThreadTestTask {
    Fixture* f;
    bool offered = false;
    explicit MidPassAnnounce(Fixture* fx)
        : OwnerThreadTestTask(Target::Replica, 1, 0, &run, Clock::now() + std::chrono::hours(1)), f(fx) {}
    static void run(OwnerThreadTestTask& t, PartitionReplica*) noexcept {
        auto& self = static_cast<MidPassAnnounce&>(t);
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 7;
        a.egressStreamId = kEgress2; a.egressChannel = kEgressChannel;
        if (!self.f->worker->tryEnqueueOp(std::move(a))) return;   // the runtime announces...
        auto fr = Fixture::produceFrame(7, 7'000'001, 0);           // ...the client's frame lands
        self.offered = Fixture::offerOnce(*self.f->pub2, fr);
    }
};
} // namespace

TEST(ShardWorkerAckRegression, FrameVisibleBeforeItsAnnouncementIsAppliedIsStillAcked) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;                          // as the runtime does every cycle
    for (int i = 0; i < 4; ++i) f.step();
    f.connectSecondClient();
    auto task = std::make_shared<MidPassAnnounce>(&f);
    WorkerOp t; t.type = WorkerOp::Type::TestTask;
    t.egressPub = new OwnerThreadTestTaskRef(task);
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(t)));
    for (int i = 0; i < 2'000 && !f.acks.count(7'000'001); ++i) f.step();
    ASSERT_TRUE(task->offered);
    EXPECT_EQ(f.acks.count(7'000'001), 1u) << "the ack of a frame seen before its announcement must be held, not dropped";
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// I3 (fix on sight (d) of Task 13): shard-ingress produces run the Task 7
// per-topic schema validation — a batch failing it is refused with
// SchemaValidationFailed and never appended.  (Base ede91e8 appended it.)
TEST(ShardWorkerAckRegression, ShardIngressEnforcesTopicSchema) {
    Fixture f({1}, 1);
    const auto desc = rune::codec::DescriptorBuilder("characters")
                          .add("id", rune::codec::PrimType::Int64)
                          .add("type", rune::codec::PrimType::Enum, 1, false, {0, 1, 2})
                          .add("name", rune::codec::PrimType::String)
                          .build();
    WorkerOp su; su.type = WorkerOp::Type::SchemaUpdate; su.topicId = 1;
    su.schemaUpdateBlob = rune::codec::serialize(desc);
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(su)));
    for (int i = 0; i < 4; ++i) f.step();
    auto bad = Fixture::produceFrame(5, 31, 0, /*topicId=*/1, /*value=*/"v");   // not a 'characters' record
    ASSERT_TRUE(Fixture::offerOnce(*f.producePub, bad));
    ASSERT_TRUE(f.awaitAck(31));
    EXPECT_EQ(f.acks[31].errorCode, static_cast<int16_t>(pc::ErrorCode::SchemaValidationFailed));
    EXPECT_EQ(f.log->logEndOffset(), 0) << "a schema-invalid batch is never appended";
}

// Fix round 2 (finding 1, critical): C1 on the GATEWAY-ROUTED path — a client
// that ignores ingressEndpoint (every Produce through the node endpoint → op
// ring) and stops reading its acks must not fill the shard's ack queue.  The
// worker signals Held for the session; the runtime (emulated here exactly as
// the gateway does it: no more routed frames for a Held session until
// Released) pauses only that client.  Role ops and other sessions keep flowing
// and the op drain never pauses for want of ack room.
TEST(ShardWorkerAckRegression, GatewayRoutedSlowSessionIsPausedNotTheShard) {
    Fixture f({1}, 1);
    f.runtimeDrainsReturns = true;
    for (int i = 0; i < 4; ++i) f.step();
    f.egressLimit = 0;                                      // session 5 stops reading acks
    bool held5 = false;
    int heldEdges = 0, releasedEdges = 0;
    f.onReturn = [&](const WorkerReturn& r) {
        if (r.type != WorkerReturn::Type::SessionSignal || r.sessionId != 5) return;
        if (r.lastOffset == static_cast<int64_t>(SessionSignalKind::Held)) { held5 = true; ++heldEdges; }
        if (r.lastOffset == static_cast<int64_t>(SessionSignalKind::Released)) { held5 = false; ++releasedEdges; }
    };
    const int64_t c0 = 4'000'000;
    int64_t corr = c0;
    int pausedPasses = 0;
    for (int i = 0; i < 40'000 && corr - c0 < 17'000 && pausedPasses < 200; ++i) {
        if (!held5) {
            pausedPasses = 0;
            for (int k = 0; k < 8 && f.routedProduce(5, corr); ++k) ++corr;
        } else {
            ++pausedPasses;                                 // the gateway ABORTs session 5's frames
        }
        f.step();
    }
    ASSERT_GT(corr - c0, 300) << "session 5 out-ran its ack publication";
    EXPECT_TRUE(held5) << "the worker must report the slow session Held";

    f.addSecondClient(6);
    ASSERT_TRUE(f.routedProduce(6, 6'000'001));
    for (int i = 0; i < 2'000 && !f.acks.count(6'000'001); ++i) f.step();
    EXPECT_EQ(f.acks.count(6'000'001), 1u) << "another session's routed produce is acked";
    WorkerOp bf; bf.type = WorkerOp::Type::BecomeFollower; bf.topicId = 1; bf.partition = 0;
    bf.replica = f.replica.get(); bf.roleEpoch = 2; bf.roleLeaderId = 2;
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(bf)));
    for (int i = 0; i < 2'000 && f.replica->role() != Role::Follower; ++i) f.step();
    EXPECT_EQ(f.replica->role(), Role::Follower) << "a role op is never stuck behind a slow client's acks";
    EXPECT_EQ(f.worker->ackRoomOpPausesForTest(), 0u) << "the op drain never ran out of ack room";

    f.egressLimit = 64;                                     // session 5 reads again: nothing lost
    for (int i = 0; i < 50'000 && !allAcked(f, c0, corr); ++i) f.step();
    EXPECT_TRUE(allAcked(f, c0, corr));
    for (int i = 0; i < 100 && held5; ++i) f.step();
    EXPECT_FALSE(held5) << "Released once its acks drained";
    EXPECT_EQ(heldEdges, releasedEdges);
    EXPECT_EQ(f.worker->sessionAcksDroppedForTest(), 0u);
}

// Fix round 2 (finding 9): a session signal the return ring cannot take now is
// deferred (counted), never lost — delivered exactly once when there is room.
TEST(ShardWorkerAckRegression, DeferredSessionSignalIsCountedAndStillDeliveredOnce) {
    Fixture f({1}, 1);
    for (int i = 0; i < 4; ++i) f.step();
    WorkerReturn filler; filler.type = WorkerReturn::Type::OwnershipAck;
    while (f.worker->injectReturnForTest(filler)) {}        // the runtime is slow: ret_ full
    ASSERT_TRUE(f.routedProduce(5, 71));                    // its notification goes to the FIFO
    f.step();
    WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 8;
    a.egressStreamId = kEgress2; a.egressChannel = "aeron:udp?endpoint=127.0.0.1:notaport";
    ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    for (int i = 0; i < 50; ++i) f.step();
    EXPECT_GT(f.worker->sessionSignalsDeferredForTest(), 0u) << "deferred while the FIFO is non-empty";
    f.runtimeDrainsReturns = true;                          // the runtime catches up
    for (int i = 0; i < 2'000 && f.sessionFailures.empty(); ++i) f.step();
    for (int i = 0; i < 50; ++i) f.step();
    ASSERT_EQ(f.sessionFailures.size(), 1u);
    EXPECT_EQ(f.sessionFailures[0].sessionId, 8);
}
