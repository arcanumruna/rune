// WorkerDoorbellTest.cpp — A9: a shard worker's cross-thread rings ring its doorbell, a held-back op ring
// does not keep it awake, ret_ back-pressure arms the space wait, a parked shard runner takes a cross-thread
// op at once (Linux: driver wake fds), and a term rotation never surfaces ADMIN_ACTION to a worker claim.
// Deterministic where possible (single test thread driving doWork); waits are condition waits with deadlines.
#include <rune/node/ShardAgent.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/ParkUntilWorkIdle.h>
#include <rune/node/SessionAckSink.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace rune::node;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
using rune::storage::Log;
namespace fs = std::filesystem;

namespace {
struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() /
               ("rune_wdb_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" + std::to_string(t));
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
PartitionConfig cfg1() {
    PartitionConfig c; c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
WorkerOp produceOp(PartitionReplica* r, int64_t corr) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = r->topicId(); op.partition = static_cast<int32_t>(r->partitionId());
    op.replica = r; op.logPtr = r->log(); op.sessionId = 1; op.correlationId = corr;
    op.recordCount = 1; op.durabilityTier = 1; op.recordBatchBytes = b.build();
    op.targetReplicaId = r->registryId(); op.incarnation = r->incarnation();
    return op;
}
WorkerOp setOwnershipOp(Worker::OwnedSet set, uint64_t gen) {
    WorkerOp op;
    op.type = WorkerOp::Type::SetOwnership;
    op.ownedSet = std::move(set);
    op.ownershipGen = gen;
    return op;
}
std::size_t drainReturns(Worker& w, int* ownershipAcks = nullptr) {
    return w.drainReturn([&](WorkerReturn r) {
        if (ownershipAcks && r.type == WorkerReturn::Type::OwnershipAck) ++*ownershipAcks;
    });
}
} // namespace

// The worker's op ring and session ring ring its doorbell while its runner parks (producer = another thread in
// production; here the test thread plays both roles, step by step).
TEST(WorkerDoorbell, OpAndSessionRingsRingTheParkedWorker) {
    Worker w(0, 1 << 16, 1 << 16, 16, 16);
    w.setShardMode(true);
    w.enableDoorbell(/*retSpaceWait=*/true);
    auto& bell = w.doorbell();
    ASSERT_TRUE(bell.valid());
    EXPECT_GE(bell.watchCount(), 2) << "op ring + session ring (+ the portable backend's completion ring)";
    EXPECT_TRUE(bell.armForPark());
    ASSERT_TRUE(w.tryEnqueueOp(setOwnershipOp({}, 1)));
    EXPECT_TRUE(bell.firedForTest()) << "an op committed while the worker parks must wake it";
    bell.disarm(true);
    EXPECT_FALSE(bell.armForPark()) << "the op is still queued: no park until it is drained";
    (void)w.doWork(1);
    EXPECT_TRUE(bell.armForPark()) << "drained: park";

    WorkerOp announce;
    announce.type = WorkerOp::Type::SessionAnnounce;
    announce.sessionId = 7;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(announce)));
    EXPECT_TRUE(bell.firedForTest()) << "a session lifecycle op wakes the parked worker";
    bell.disarm(true);
}

// A worker that holds its op ring back (return FIFO at high water: nobody drains ret_) must not be kept awake
// by the ops it will not take — its op-ring watch goes off — and it waits on ret_ space instead: the runtime's
// drain rings its doorbell.  Without the gate the doorbell's re-check would see the queued op on every arm and
// the runner would never park (a busy loop).
TEST(WorkerDoorbellRegression, HeldBackOpRingDoesNotKeepTheWorkerAwakeRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg1(), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 23, 1 << 16, 64, 64);
    w.setShardMode(true);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    w.enableDoorbell(/*retSpaceWait=*/true);
    auto& bell = w.doorbell();
    int64_t now = 1;
    constexpr int kN = 2000;
    for (int i = 0; i < kN; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, i + 1)));
    for (int p = 0; p < 8 && w.retFifoSizeForTest() < Worker::kOverflowHighWater; ++p) (void)w.doWork(++now);
    ASSERT_GE(w.retFifoSizeForTest(), Worker::kOverflowHighWater) << "precondition: FIFO at high water";
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, kN + 1)));   // an op the worker will hold back
    int work = 1;
    for (int p = 0; p < 4 && work != 0; ++p) work = w.doWork(++now);
    ASSERT_EQ(work, 0) << "a held-back worker has nothing to do";
    EXPECT_FALSE(w.ingressWatchableForTest()) << "the held-back op ring's watch is off";
    EXPECT_TRUE(bell.armForPark()) << "the worker parks although its op ring holds records";
    bell.disarm(false);
    EXPECT_FALSE(bell.firedForTest());
    // The runtime drains ret_: the worker waited for that space — its doorbell rings.
    EXPECT_GT(drainReturns(w), 0u);
    EXPECT_TRUE(bell.firedForTest()) << "freed ret_ space must wake the worker that waits for it";
    bell.disarm(true);
    // It resumes: everything is answered, the watch is back on.
    std::size_t answered = 0;
    for (int p = 0; p < 200 && answered < static_cast<std::size_t>(kN + 1); ++p) {
        (void)w.doWork(++now);
        answered += drainReturns(w);
    }
    (void)w.doWork(++now);
    EXPECT_TRUE(w.ingressWatchableForTest());
    EXPECT_EQ(log.logEndOffset(), kN + 1);
}

// Review M-1: a worker whose runner does not park on its doorbell (RUNE_IDLE_DOORBELL=0, no wake fds,
// busy-spin) has no doorbell wired: commits pay no doorbell fence, a held-back worker arms no space wait and
// the runtime's drain writes no fd — exactly A7.
TEST(WorkerDoorbellRegression, DoorbellOffWiresNothingAndWritesNoFdRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg1(), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 23, 1 << 16, 64, 64);   // never enableDoorbell()
    w.setShardMode(true);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    EXPECT_FALSE(w.doorbellEnabled());
    EXPECT_FALSE(w.opRingConsumerDoorbellForTest());
    EXPECT_EQ(w.doorbell().watchCount(), 0);
    int64_t now = 1;
    for (int i = 0; i < 2000; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, i + 1)));
    for (int p = 0; p < 8 && w.retFifoSizeForTest() < Worker::kOverflowHighWater; ++p) (void)w.doWork(++now);
    ASSERT_GE(w.retFifoSizeForTest(), Worker::kOverflowHighWater);
    for (int p = 0; p < 4; ++p) (void)w.doWork(++now);
    EXPECT_FALSE(w.retSpaceWaitArmedForTest()) << "no doorbell: no space wait";
    EXPECT_GT(drainReturns(w), 0u);
    EXPECT_FALSE(w.doorbell().firedForTest()) << "no doorbell: the drain wrote no fd";
}

TEST(WorkerDoorbellRegression, EnableShardDoorbellsFollowsTheKillSwitchRegression) {
    for (const bool on : {false, true}) {
        SCOPED_TRACE(on ? "RUNE_IDLE_DOORBELL=1" : "RUNE_IDLE_DOORBELL=0");
        TmpDir dir;
        EmbeddedDriverConfig c; c.dir = (dir.path / "aeron").string(); c.termBufferBytes = 64 * 1024;
        EmbeddedDriver drv(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(c.dir); ctx.useConductorAgentInvoker(true);
        auto client = aeron::Aeron::connect(ctx);
        rune::metadata::ShardRouter router(2);
        WorkerPool pool(2, router, 1 << 16, 1 << 16, 16, 16);
        rune::consensus::MonotonicNanoClock clk;
        std::vector<std::unique_ptr<ShardAgent>> agents;
        for (int i = 0; i < 2; ++i) agents.push_back(std::make_unique<ShardAgent>(pool.worker(i), drv, *client, clk));
        NodeConfig cfg;
        cfg.idleDoorbell = on;
        int fds[ParkUntilWorkIdle::kMaxWakeFds];
        const bool haveFds = agents[0]->wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds) > 0;   // Linux epoll-first
        const int n = pool.enableShardDoorbells(agents, cfg, /*fused=*/true);
        EXPECT_EQ(n, on && haveFds ? 2 : 0);
        for (int i = 0; i < 2; ++i) {
            EXPECT_EQ(pool.worker(i).doorbellEnabled(), on && haveFds);
            EXPECT_EQ(pool.worker(i).opRingConsumerDoorbellForTest(), on && haveFds);
        }
        client.reset();
    }
}

namespace {
struct Shard {
    TmpDir dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    Worker worker{0, 1 << 16, 1 << 16, 16, 16};
    Shard() {
        EmbeddedDriverConfig c; c.dir = (dir.path / "aeron").string(); c.termBufferBytes = 64 * 1024;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(c.dir); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        worker.setShardMode(true);
    }
    ~Shard() { client.reset(); driver.reset(); }
};
template <class Pred>
bool waitFor(std::chrono::milliseconds limit, Pred&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::yield();
    }
    return pred();
}
} // namespace

#if defined(__linux__)
namespace {
// A parked shard runner (real invoker driver + its wake fds, a 2 s park) and an op from another thread
// (the runtime in thread placement / fused W>1): ms from the enqueue to the worker's OwnershipAck.
int64_t parkedShardRunnerOpLatencyNs(bool doorbell) {
    Shard s;
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent agent(s.worker, *s.driver, *s.client, clk);
    int fds[ParkUntilWorkIdle::kMaxWakeFds];
    const int n = agent.wakeFds(fds, ParkUntilWorkIdle::kMaxWakeFds - 1);
    EXPECT_GT(n, 0);
    NodeConfig cfg;
    cfg.idleDoorbell = doorbell;
    cfg.idleDoorbellMinParkUs = cfg.idleDoorbellParkUs = 2'000'000;   // 2 s: the park cap must not be what wakes it
    cfg.idleSpins = 0; cfg.idleYields = 0; cfg.idleMaxParkUs = 2'000'000;
    if (doorbell) agent.enableDoorbell(true);
    auto idle = makeShardIdle(cfg, fds, n, agent.doorbellEnabled() ? &agent.doorbell() : nullptr);
    auto* p = dynamic_cast<ParkUntilWorkIdle*>(idle.get());
    EXPECT_NE(p, nullptr);
    rune::consensus::AgentRunner runner(agent, *idle, clk);
    runner.start();
    // Let the runner settle into its long park: its early passes may find startup work.
    for (int i = 0; i < 20'000; ++i) std::this_thread::yield();
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_TRUE(s.worker.tryEnqueueOp(setOwnershipOp({}, 1)));
    int acks = 0;
    EXPECT_TRUE(waitFor(std::chrono::seconds(5), [&] { drainReturns(s.worker, &acks); return acks > 0; }));
    const int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
    runner.stop();
    s.worker.doorbell().ring();   // end the park so the join does not wait it out
    (void)s.worker.tryEnqueueOp(setOwnershipOp({}, 2));
    runner.join();
    s.driver->bindPumpThread();   // pump ownership back to this thread for the close
    return ns;
}
} // namespace

TEST(WorkerDoorbellRegression, ParkedShardRunnerTakesACrossThreadOpAtOnceRegression) {
    const int64_t ns = parkedShardRunnerOpLatencyNs(/*doorbell=*/true);
    EXPECT_LT(ns, 500'000'000) << "the parked shard runner slept out its 2 s park: no doorbell wake";
}
#endif

// Aeron contract: ADMIN_ACTION (the claim rotated the term) means "retry now".  A worker claim that surfaced it
// waited for the next pass — after a park, up to the park cap (1 ms with the doorbell shape).  Every
// worker-path claim retries once in place, so a rotation never reaches the caller.
TEST(WorkerDoorbellRegression, SessionAckClaimNeverSurfacesAdminActionRegression) {
    Shard s;
    auto pump = [&] { s.driver->doWork(); s.client->conductorAgentInvoker().invoke(); };
    const int64_t subReg = s.client->addSubscription("aeron:ipc", 4242);
    const int64_t pubReg = s.client->addExclusivePublication("aeron:ipc?term-length=65536", 4242);
    std::shared_ptr<aeron::Subscription> sub;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    for (int i = 0; i < 200'000 && !(sub && pub && pub->isConnected()); ++i) {
        pump();
        if (!sub) sub = s.client->findSubscription(subReg);
        if (!pub) pub = s.client->findExclusivePublication(pubReg);
    }
    ASSERT_TRUE(sub && pub && pub->isConnected());
    AeronSessionPubPolicy policy;
    policy.client = s.client.get();
    int adminActions = 0, sent = 0;
    for (int i = 0; i < 2'000; ++i) {   // 2000 x 1 KiB = ~31 rotations of a 64 KiB term
        const int64_t pos = policy.claim(pub, 1000, [](uint8_t* dst) { std::memset(dst, 0x5a, 1000); });
        if (pos == aeron::ADMIN_ACTION) ++adminActions;
        if (pos > 0) ++sent;
        pump();
        (void)sub->poll([](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                           aeron::Header&) {}, 100);
    }
    EXPECT_EQ(adminActions, 0) << "a term rotation surfaced to the caller (it would wait a pass / a park)";
    EXPECT_GT(sent, 1'000);
}
