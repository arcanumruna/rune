// GatewayProduceCreditTest.cpp — D8c: the gateway's pause of a slow gateway-routed
// client is CREDIT based and so independent of pipeline depth (burst size, MTU,
// socket buffers).  A real ClientGateway in shard mode in front of a real
// WorkerPool worker with worker-side acks (its own SessionAckSink on a REAL
// invoker media driver, aeron:ipc).  Single test thread: the gateway's client
// poll is driven through controlledPollFramesForTest (the real per-frame
// admit/ABORT decision and dispatch of the shard-mode controlled poll), the
// worker by explicit doWork passes whose returns feed the gateway — every
// ordering is deterministic.  No sleeps; every wait has a hard iteration cap.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/SessionAckSink.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <unistd.h>

using namespace rune::node;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int kCap = 200'000;                       // hard cap on any condition-wait loop
constexpr int32_t kEgressA = 7411, kEgressB = 7412;
// The ack publications the worker opens to its sessions: a small IPC term so a
// client that stops reading back-pressures them after a few hundred acks.
constexpr const char* kAckChannel = "aeron:ipc?term-length=65536";

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() / (std::string("rune_d8c_") + tag + "_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(seq++) + "_" + std::to_string(t));
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
PartitionConfig cfg() {
    PartitionConfig c; c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
std::vector<uint8_t> produceFrame(int64_t sid, int64_t corr, uint8_t acks) {
    static const std::vector<uint8_t> batch = [] {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", "v", 1000);
        return b.build();
    }();
    pc::Produce m;
    m.sessionId = sid; m.correlationId = corr; m.topicId = 0; m.partition = 0; m.partitionKnown = 1;
    m.acks = acks; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}
rune::node::WorkerReturn creditSignal(int64_t sid, int64_t n) {
    rune::node::WorkerReturn r;
    r.type = rune::node::WorkerReturn::Type::SessionSignal;
    r.sessionId = sid; r.partition = 0;
    r.lastOffset = static_cast<int64_t>(SessionSignalKind::Credit);
    r.baseOffset = n;
    return r;
}

// One user partition (topic 0 / partition 0, leader, ISR {self}) owned by worker 0
// of a real WorkerPool (32 MiB rings, like the default), behind a real gateway in
// shard mode.  withAcks: the worker gets its own invoker driver + client and acks
// sessions itself (SessionAckSink), exactly as BrokerNode wires a shard.
struct Rig {
    TmpDir d{"log"}, ad{"aeron"};
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 32u << 20, 32u << 20, 16, 16};
    std::vector<int16_t> gatewayAnswers;            // ProduceResponses the GATEWAY sent (not forwarded)
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) gatewayAnswers.push_back(r->errorCode);
            return 1;
        });
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::shared_ptr<aeron::Subscription> egA, egB;    // the clients' ack subscriptions
    std::set<int64_t> acks;                           // correlation ids acked by the worker
    aeron::FragmentAssembler asmb{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                         aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o),
                                                 static_cast<std::size_t>(l)))
            acks.insert(r->correlationId);
    }};
    int readA = 0;                                    // acks client A reads per pass (0 = stopped reading)
    int64_t now = 1'000;

    explicit Rig(bool withAcks) {
        auto rep = std::make_unique<PartitionReplica>(cfg(), Role::Leader, 1, std::vector<NodeId>{1},
                                                      std::vector<NodeId>{1}, topics.log(0, 0), &t);
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setPerWorkerIngressActive(true);
        gw.setCommittedMetadataReady(true);
        gw.setShardMode(true);
        w().setShardMode(true);
        w().setWorkerIngress(nullptr, true);
        if (withAcks) {
            EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
            drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
            aeron::Context ctx; ctx.aeronDir(ad.path.string()); ctx.useConductorAgentInvoker(true);
            client = aeron::Aeron::connect(ctx);
            w().setShardClient(client.get());
            const int64_t ra = client->addSubscription("aeron:ipc", kEgressA);
            const int64_t rb = client->addSubscription("aeron:ipc", kEgressB);
            for (int i = 0; i < kCap && !(egA && egB); ++i) {
                pump();
                if (!egA) egA = client->findSubscription(ra);
                if (!egB) egB = client->findSubscription(rb);
            }
        }
        runWorker();                                  // ownership applied
    }
    ~Rig() {
        // The worker's session publications release through the client: drop them before it.
        if (client) w().releaseShardClient();
        egA.reset(); egB.reset(); client.reset(); drv.reset();
    }
    Worker& w() { return pool.worker(0); }
    void pump() { if (drv) { drv->doWork(); client->conductorAgentInvoker().invoke(); } }
    // A gateway session the worker knows (announce-before-accept, as the gateway's announcer does).
    int64_t session(int32_t egressStream) {
        const int64_t sid = gw.injectTestSession(now);
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = sid;
        a.egressStreamId = egressStream; a.egressChannel = kAckChannel;
        EXPECT_TRUE(w().tryEnqueueOp(std::move(a)));
        for (int i = 0; i < 8; ++i) runWorker();
        return sid;
    }
    // One worker pass + the runtime's return drain + the clients reading their acks.
    void runWorker() {
        pump();
        w().doWork(now += 10'000);
        w().drainReturn([&](WorkerReturn r) { gw.handleWorkerReturnForTest(r); });
        if (egA && readA > 0) egA->poll(asmb.handler(), readA);
        if (egB) egB->poll(asmb.handler(), 64);
    }
    bool acked(int64_t from, int64_t to) const {
        for (int64_t c = from; c < to; ++c) if (!acks.count(c)) return false;
        return true;
    }
};
} // namespace

// The defining case.  A burst far larger than the worker's whole ack queue
// (kAckCapacity) arrives in ONE poll of a client that never reads its acks.  The
// gateway forwards exactly its credit budget and ABORTs the next frame — in that
// same poll, before the worker has run once — so the worker's ack queue never
// holds more than the budget and its op drain never runs out of ack room.  (Was:
// the whole burst was forwarded; the Held signal could only land a ret_ hop
// later, by which time ~27k ops were queued behind ack-room pauses.)  Another
// session is served meanwhile; once A reads, every frame is forwarded and acked
// and all credits come home.
TEST(GatewayProduceCreditRegression, BurstLargerThanTheAckRoomIsPausedBeforeTheAckQueueFillsRegression) {
    Rig r(/*withAcks=*/true);
    ASSERT_TRUE(r.egA && r.egB);
    const int64_t a = r.session(kEgressA);
    const uint32_t C = r.gw.sessionProduceCredits();
    ASSERT_EQ(C, rune::gateway::ClientGateway::kDefaultSessionProduceCredits);
    const std::size_t N = SessionAckSink::kAckCapacity + 2048;   // > the worker's entire ack queue
    const int64_t c0 = 1'000'000;
    std::vector<std::vector<uint8_t>> burst;
    burst.reserve(N);
    for (std::size_t i = 0; i < N; ++i) burst.push_back(produceFrame(a, c0 + static_cast<int64_t>(i), /*acks=*/0));

    std::size_t consumed = r.gw.controlledPollFramesForTest(burst);   // ONE poll, the worker has not run
    ASSERT_LE(consumed, C) << "the gateway forwarded " << consumed << " of a " << N
                           << "-frame burst in one poll: no pause before the worker's ack queue can fill";
    EXPECT_EQ(consumed, C) << "exactly the credit budget is forwarded";
    EXPECT_EQ(r.gw.produceInFlightForTest(a), static_cast<int64_t>(C));
    EXPECT_EQ(r.gw.creditProduceAbortsForTest(), 1u) << "the next frame is ABORTed (paused) in the same poll";

    // The worker runs; A never reads.  Credits come back only for the acks its
    // window takes (claimed), so the gateway forwards only that many more.
    int idle = 0;
    for (int i = 0; i < 4'000 && idle < 64; ++i) {
        r.runWorker();
        const std::size_t n = r.gw.controlledPollFramesForTest(burst, consumed);
        consumed += n;
        idle = n == 0 ? idle + 1 : 0;
        ASSERT_LE(r.w().sessionAcksPendingForTest(), C) << "the worker never holds more than A's budget";
    }
    EXPECT_LT(consumed, N) << "A is paused";
    // Paused by credit first, then (once a worker flush holds its acks) also Held:
    // the credits its window's claims returned are not reused while it is Held.
    // Conservation: every forwarded, unclaimed produce holds one credit and one
    // queued ack — none dropped, none invented.
    const int64_t inFlight = r.gw.produceInFlightForTest(a);
    EXPECT_LE(inFlight, static_cast<int64_t>(C));
    EXPECT_GT(inFlight, 0);
    EXPECT_EQ(static_cast<int64_t>(r.w().sessionAcksPendingForTest()), inFlight)
        << "in-flight credits == the worker's held acks of A";
    EXPECT_TRUE(r.gw.clientFrameHeldForTest(burst[consumed].data(), burst[consumed].size())) << "still paused";
    EXPECT_EQ(r.w().ackRoomOpPausesForTest(), 0u) << "the worker's op drain never ran out of ack room";
    EXPECT_TRUE(r.acks.empty()) << "A read nothing";

    // Another session is not paused: its produce is admitted and acked while A is.
    const int64_t b = r.session(kEgressB);
    std::vector<std::vector<uint8_t>> fb{produceFrame(b, 7, 0)};
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fb), 1u) << "session B is not paused";
    for (int i = 0; i < 2'000 && !r.acks.count(7); ++i) r.runWorker();
    EXPECT_EQ(r.acks.count(7), 1u) << "session B's routed produce is acked while A is paused";

    // A reads again: the rest of the burst is forwarded and every frame is acked once.
    r.readA = 256;
    for (int i = 0; i < kCap && !(consumed == N && r.acked(c0, c0 + static_cast<int64_t>(N))); ++i) {
        r.runWorker();
        consumed += r.gw.controlledPollFramesForTest(burst, consumed);
    }
    ASSERT_EQ(consumed, N);
    EXPECT_TRUE(r.acked(c0, c0 + static_cast<int64_t>(N))) << "nothing lost";
    for (int i = 0; i < 8; ++i) r.runWorker();
    EXPECT_EQ(r.gw.produceInFlightForTest(a), 0) << "every credit returned once its ack was claimed";
    EXPECT_EQ(r.gw.produceInFlightForTest(b), 0);
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 0u);
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
    EXPECT_EQ(r.w().ackRoomOpPausesForTest(), 0u);
    EXPECT_EQ(r.w().sessionAcksDroppedForTest(), 0u);
}

// Accounting: a forwarded produce holds one credit until its ack is claimed —
// acks=0 (answered at append) and acks=all (parked until the HW passes it) alike;
// a produce the GATEWAY answers itself (never forwarded) takes none.
TEST(GatewayProduceCreditRegression, ForwardedProduceHoldsOneCreditUntilItsAckIsClaimedRegression) {
    Rig r(/*withAcks=*/true);
    ASSERT_TRUE(r.egA);
    const int64_t a = r.session(kEgressA);
    r.readA = 64;
    for (const uint8_t acks : {uint8_t{0}, uint8_t{1}}) {
        const int64_t corr = 100 + acks;
        std::vector<std::vector<uint8_t>> f{produceFrame(a, corr, acks)};
        ASSERT_EQ(r.gw.controlledPollFramesForTest(f), 1u);
        EXPECT_EQ(r.gw.produceInFlightForTest(a), 1) << "acks=" << int(acks) << ": one credit held from forward";
        for (int i = 0; i < 2'000 && !r.acks.count(corr); ++i) r.runWorker();
        ASSERT_EQ(r.acks.count(corr), 1u) << "acks=" << int(acks);
        for (int i = 0; i < 4 && r.gw.produceInFlightForTest(a) != 0; ++i) r.runWorker();
        EXPECT_EQ(r.gw.produceInFlightForTest(a), 0) << "acks=" << int(acks) << ": returned once claimed";
    }
    // Answered by the gateway itself (unknown topic): nothing forwarded, no credit.
    pc::Produce m;
    m.sessionId = a; m.correlationId = 200; m.topicId = 99; m.partition = 0; m.partitionKnown = 1; m.acks = 0;
    m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
    rune::protocol::RecordBatchBuilder bb(0, 1000);
    bb.append("k", "v", 1000);
    const std::vector<uint8_t> batch = bb.build();
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<std::vector<uint8_t>> f{std::vector<uint8_t>(m.encodedSize())};
    m.encode(f[0].data());
    ASSERT_EQ(r.gw.controlledPollFramesForTest(f), 1u);
    ASSERT_EQ(r.gatewayAnswers.size(), 1u) << "the gateway answered it";
    EXPECT_EQ(r.gw.produceInFlightForTest(a), 0) << "a produce the gateway answers takes no credit";
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
}

// Close frees the budget: a session paused for want of credit that disconnects
// leaves no paused-session count behind (the fast path is restored for everyone),
// and a late Credit for it is ignored — never credited to anyone, never an underflow.
TEST(GatewayProduceCreditRegression, CloseFreesThePausedSessionsCreditsRegression) {
    Rig r(/*withAcks=*/false);
    r.gw.setSessionProduceCredits(4);
    const int64_t a = r.gw.injectTestSession(r.now), b = r.gw.injectTestSession(r.now);
    std::vector<std::vector<uint8_t>> fa;
    for (int i = 0; i < 10; ++i) fa.push_back(produceFrame(a, i, 0));
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fa), 4u) << "budget 4: the 5th frame is paused";
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 1u);
    EXPECT_TRUE(r.gw.clientFrameHeldForTest(fa[4].data(), fa[4].size()));
    r.gw.handleWorkerReturnForTest(creditSignal(a, 3));
    EXPECT_EQ(r.gw.produceInFlightForTest(a), 1);
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 0u) << "credits returned: no longer paused";
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fa, 4), 3u) << "exactly the returned credits are reused";
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 1u);

    pc::Disconnect d; d.sessionId = a; d.correlationId = 1;
    std::vector<uint8_t> buf(pc::Disconnect::ENCODED_SIZE);
    d.encode(buf.data());
    r.gw.applyDisconnectForTest(buf.data(), buf.size());
    EXPECT_EQ(r.gw.produceInFlightForTest(a), -1) << "the session is gone";
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 0u) << "its budget died with it";
    r.gw.handleWorkerReturnForTest(creditSignal(a, 4));   // the workers' late claims
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
    EXPECT_EQ(r.gw.produceInFlightForTest(b), 0) << "never credited to another session";
    std::vector<std::vector<uint8_t>> fb{produceFrame(b, 50, 0)};
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fb), 1u);
}

// Lease: a session paused for want of credit has its image stopped by the gateway
// (its Keepalives wait behind the aborted Produce), so it is leased like a held
// session (maxHeldIdleNs_), not reaped on the 10 s idle lease; leaving the pause
// is progress and refreshes its lease.
TEST(GatewayProduceCreditRegression, CreditPausedSessionKeepsItsLeaseUntilCreditReturnsRegression) {
    Rig r(/*withAcks=*/false);
    r.gw.setSessionProduceCredits(2);
    const int64_t a = r.gw.injectTestSession(0);
    r.gw.setNowForTest(0);
    std::vector<std::vector<uint8_t>> fa{produceFrame(a, 1, 0), produceFrame(a, 2, 0), produceFrame(a, 3, 0)};
    ASSERT_EQ(r.gw.controlledPollFramesForTest(fa), 2u);
    constexpr int64_t T = rune::gateway::ClientGateway::kDefaultSessionTimeoutNs;
    r.gw.reapExpiredForTest(T + 1);
    ASSERT_EQ(r.gw.produceInFlightForTest(a), 2) << "a credit-paused session is not reaped on the idle lease";
    r.gw.setNowForTest(3 * T);
    r.gw.handleWorkerReturnForTest(creditSignal(a, 1));   // a claim at 3T
    EXPECT_EQ(r.gw.sessionLastSeenNsForTest(a), 3 * T) << "leaving the pause refreshes the lease";
    r.gw.reapExpiredForTest(3 * T + T / 2);
    EXPECT_EQ(r.gw.produceInFlightForTest(a), 1) << "unpaused: the normal lease runs from the refresh";
    r.gw.reapExpiredForTest(4 * T + 1);
    EXPECT_EQ(r.gw.produceInFlightForTest(a), -1) << "and expires on it";
    const int64_t h = r.gw.injectTestSession(3 * T);
    std::vector<std::vector<uint8_t>> fh{produceFrame(h, 1, 0), produceFrame(h, 2, 0)};
    ASSERT_EQ(r.gw.controlledPollFramesForTest(fh), 2u);   // paused at 3T
    r.gw.reapExpiredForTest(3 * T + rune::gateway::ClientGateway::kDefaultMaxHeldIdleNs - 1);
    ASSERT_EQ(r.gw.produceInFlightForTest(h), 2);
    r.gw.reapExpiredForTest(3 * T + rune::gateway::ClientGateway::kDefaultMaxHeldIdleNs + 1);
    EXPECT_EQ(r.gw.produceInFlightForTest(h), -1) << "a credit-paused session is still bounded by maxHeldIdle";
}
