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
#include <algorithm>
#include <set>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

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
// A free loopback UDP port (bind to 0, read it back, release it).
int freeUdpPort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t n = sizeof a;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &n);
    ::close(fd);
    return ntohs(a.sin_port);
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
        extra.clear(); egA.reset(); egB.reset(); client.reset(); drv.reset();
    }
    Worker& w() { return pool.worker(0); }
    std::vector<std::shared_ptr<aeron::Subscription>> extra;   // stalled clients' ack subscriptions (never polled)
    void subscribe(int32_t stream, const std::string& channel = "aeron:ipc") {
        const int64_t reg = client->addSubscription(channel, stream);
        std::shared_ptr<aeron::Subscription> sub;
        for (int i = 0; i < kCap && !sub; ++i) { pump(); sub = client->findSubscription(reg); }
        ASSERT_TRUE(sub);
        extra.push_back(std::move(sub));
    }
    void pump() { if (drv) { drv->doWork(); client->conductorAgentInvoker().invoke(); } }
    // A gateway session the worker knows (announce-before-accept, as the gateway's announcer does).
    int64_t session(int32_t egressStream, const std::string& ackChannel = kAckChannel) {
        const int64_t sid = gw.injectTestSession(now);
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = sid;
        a.egressStreamId = egressStream; a.egressChannel = ackChannel;
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

// D8c fix round 1 (MAJOR-1): MANY stalled gateway sessions — 20 at the MAXIMUM per-session budget (4096), i.e.
// 5x a worker's routed ack share — still cannot fill the worker's ack queue: the gateway's per-worker cap
// (Worker::kRoutedAckBudget) bounds the sum, so the worker's op drain never runs out of ack room and a new
// session is still served.  (Was: every session held its whole budget — 16 x 1024 / 4 x 4096 filled the queue
// and stalled the whole shard until the stalled sessions were reaped.)
TEST(GatewayProduceCreditRegression, ManyStalledSessionsAtTheMaxBudgetCannotFillTheWorkersAckQueueRegression) {
    Rig r(/*withAcks=*/true);
    ASSERT_TRUE(r.egB);
    r.gw.setSessionProduceCredits(rune::gateway::ClientGateway::kMaxSessionProduceCredits);   // RUNE_GATEWAY_PRODUCE_CREDITS max
    constexpr int kStalled = 20;
    const std::size_t perSession = rune::gateway::ClientGateway::kMaxSessionProduceCredits + 512;
    std::vector<int64_t> sids;
    std::vector<std::vector<std::vector<uint8_t>>> bursts(kStalled);
    std::vector<std::size_t> consumed(kStalled, 0);
    for (int k = 0; k < kStalled; ++k) {
        r.subscribe(7500 + k);                                       // a client that never reads its acks
        sids.push_back(r.session(7500 + k));
        for (std::size_t i = 0; i < perSession; ++i)
            bursts[k].push_back(produceFrame(sids[k], (k + 1) * 10'000'000LL + static_cast<int64_t>(i), 0));
    }
    int idle = 0;
    for (int i = 0; i < 4'000 && idle < 64; ++i) {
        std::size_t n = 0;
        for (int k = 0; k < kStalled; ++k) {                         // every stalled client offers its whole burst
            const std::size_t c = r.gw.controlledPollFramesForTest(bursts[k], consumed[k]);
            consumed[k] += c;
            n += c;
        }
        r.runWorker();
        idle = n == 0 ? idle + 1 : 0;
        ASSERT_LE(r.gw.routedInFlightForTest(0), rune::node::Worker::kRoutedAckBudget);
        ASSERT_LE(r.w().sessionAcksPendingForTest(), rune::node::Worker::kRoutedAckBudget);
    }
    EXPECT_GT(r.gw.workerCapProduceAbortsForTest(), 0u) << "the per-worker cap engaged";
    EXPECT_EQ(r.w().ackRoomOpPausesForTest(), 0u) << "the worker's op drain never ran out of ack room";
    EXPECT_LT(r.gw.routedInFlightForTest(0), rune::node::Worker::kRoutedAckBudget)
        << "stalled (heavy) sessions stop in the congested band; light sessions keep the rest";
    const int64_t b = r.session(kEgressB);
    std::vector<std::vector<uint8_t>> fb{produceFrame(b, 7, 0)};
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fb), 1u) << "a new (light) session is admitted";
    for (int i = 0; i < 2'000 && !r.acks.count(7); ++i) r.runWorker();
    EXPECT_EQ(r.acks.count(7), 1u) << "and served while 20 sessions are stalled";
    EXPECT_EQ(r.w().ackRoomOpPausesForTest(), 0u);
    // Closing the stalled sessions hands their share of the worker's cap back at once.
    for (int64_t sid : sids) {
        pc::Disconnect d; d.sessionId = sid; d.correlationId = 1;
        std::vector<uint8_t> buf(pc::Disconnect::ENCODED_SIZE);
        d.encode(buf.data());
        r.gw.applyDisconnectForTest(buf.data(), buf.size());
    }
    for (int i = 0; i < 8; ++i) r.runWorker();
    EXPECT_EQ(r.gw.routedInFlightForTest(0), 0u) << "a close returns the session's share of every worker's cap";
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
}

// D8c fix round 1 (MAJOR-1 fairness): in the per-worker cap's last quarter only LIGHT sessions (< 64 in flight to
// that worker) are admitted — the heavy (stalled) session runs out first; the hard cap is never exceeded; a deferred
// frame is not consumed (redelivered next poll); credits and closes give the share back.
TEST(GatewayProduceCreditRegression, CongestedWorkerAdmitsLightSessionsAndPausesTheHeavyOneFirstRegression) {
    Rig r(/*withAcks=*/false);
    r.gw.setWorkerRoutedCapForTest(400);                                  // congested band: [300, 400)
    const int64_t h = r.gw.injectTestSession(r.now), l1 = r.gw.injectTestSession(r.now),
                  l2 = r.gw.injectTestSession(r.now);
    auto frames = [](int64_t sid, int n) {
        std::vector<std::vector<uint8_t>> f;
        for (int i = 0; i < n; ++i) f.push_back(produceFrame(sid, i, 0));
        return f;
    };
    const auto fh = frames(h, 600), f1 = frames(l1, 100), f2 = frames(l2, 100);
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fh), 300u) << "the heavy session stops at the congested band";
    EXPECT_EQ(r.gw.workerCapProduceAbortsForTest(), 1u);
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 0u) << "not its own budget (1024): the worker's cap";
    EXPECT_EQ(r.gw.controlledPollFramesForTest(f1), 64u) << "a light session is admitted up to 64 in the band";
    EXPECT_EQ(r.gw.controlledPollFramesForTest(f2), 36u) << "the hard cap is never exceeded";
    EXPECT_EQ(r.gw.routedInFlightForTest(0), 400u);
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fh, 300), 0u) << "deferred, not consumed";
    r.gw.handleWorkerReturnForTest(creditSignal(h, 150));                 // 250 in flight: below the band
    EXPECT_EQ(r.gw.routedInFlightForTest(0), 250u);
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fh, 300), 50u) << "the heavy session resumes up to the band";
    pc::Disconnect d; d.sessionId = h; d.correlationId = 1;
    std::vector<uint8_t> buf(pc::Disconnect::ENCODED_SIZE);
    d.encode(buf.data());
    r.gw.applyDisconnectForTest(buf.data(), buf.size());
    EXPECT_EQ(r.gw.routedInFlightForTest(0), 100u) << "the close returned the heavy session's 200";
}

// D8c fix round 1: LOW-1 — only LEAVING a credit pause refreshes the lease (other claims are not client liveness);
// LOW-2 — changing the budget with sessions in flight recomputes the paused-session count.
TEST(GatewayProduceCreditRegression, OnlyLeavingThePauseRefreshesTheLeaseAndBudgetChangesRecountRegression) {
    Rig r(/*withAcks=*/false);
    const int64_t a = r.gw.injectTestSession(0);
    r.gw.setNowForTest(0);
    std::vector<std::vector<uint8_t>> fa{produceFrame(a, 1, 0), produceFrame(a, 2, 0), produceFrame(a, 3, 0)};
    ASSERT_EQ(r.gw.controlledPollFramesForTest(fa), 3u);
    r.gw.setNowForTest(5'000);
    r.gw.handleWorkerReturnForTest(creditSignal(a, 1));                   // not paused: no refresh
    EXPECT_EQ(r.gw.sessionLastSeenNsForTest(a), 0);
    r.gw.setSessionProduceCredits(2);                                     // 2 in flight >= 2: paused
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 1u);
    EXPECT_TRUE(r.gw.clientFrameHeldForTest(fa[0].data(), fa[0].size()));
    r.gw.setSessionProduceCredits(8);
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 0u) << "raising the budget unpauses it (no stale count)";
    r.gw.setSessionProduceCredits(2);
    r.gw.setNowForTest(9'000);
    r.gw.handleWorkerReturnForTest(creditSignal(a, 1));                   // leaves the pause: refresh
    EXPECT_EQ(r.gw.creditBlockedSessionsForTest(), 0u);
    EXPECT_EQ(r.gw.sessionLastSeenNsForTest(a), 9'000);
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
}

// D8c fix round 2 (re-review MEDIUM-R1): enough stalled sessions to fill a worker's WHOLE routed share — 12 at the
// full budget below the congested band + 64 "light" ones that stall with 64 unclaimed inside it (80 in all; one
// process can open them).  The share must
// not stay full: once it blocks a frame, the gateway expires the HELD session with the largest share on that worker
// (SessionExpired, the client reconnects), and a new session is served.  (Was: every gateway-routed produce to the
// worker waited for the stalled sessions' 10-minute reap.)
TEST(GatewayProduceCreditRegression, FullWorkerShareExpiresTheHeldLargestShareSessionAndServesANewOneRegression) {
    Rig r(/*withAcks=*/true);
    ASSERT_TRUE(r.egB);
    constexpr int kStalled = 80;                                     // 12 heavy (1024 held) + 64 light (64 held) + spare
    const std::size_t perSession = rune::gateway::ClientGateway::kDefaultSessionProduceCredits + 256;
    std::vector<int64_t> sids;
    std::vector<std::vector<std::vector<uint8_t>>> bursts(kStalled);
    std::vector<std::size_t> consumed(kStalled, 0);
    // The stalled clients' ack publications never connect (nobody listens at their egress endpoint — a dead or
    // unreachable client): no ack is ever claimed, so a "light" one holds all of its 64 inside the congested band.
    // (A client that merely stops reading still lets its publication window — >= term/2 = 32 KiB, ~340 acks —
    // take its band share, so it cannot stall there; an unreachable one can.)
    const std::string ackCh = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort()) + "|term-length=65536";
    for (int k = 0; k < kStalled; ++k) {
        sids.push_back(r.session(7600 + k, ackCh));
        for (std::size_t i = 0; i < perSession; ++i)
            bursts[k].push_back(produceFrame(sids[k], (k + 1) * 10'000'000LL + static_cast<int64_t>(i), 0));
    }
    int idle = 0;
    for (int i = 0; i < 8'000 && idle < 64; ++i) {
        std::size_t n = 0;
        for (int k = 0; k < kStalled; ++k) {
            if (r.gw.produceInFlightForTest(sids[k]) < 0) continue;  // expired
            const std::size_t c = r.gw.controlledPollFramesForTest(bursts[k], consumed[k]);
            consumed[k] += c;
            n += c;
        }
        r.runWorker();
        idle = n == 0 ? idle + 1 : 0;
    }
    ASSERT_GT(r.gw.workerCapProduceAbortsForTest(), 0u);
    // Precondition (what the review found): the stalled sessions alone fill the worker's routed share.
    const uint64_t evictionsWhileFilling = r.gw.workerCapEvictionsForTest();
    ASSERT_TRUE(r.gw.routedInFlightForTest(0) == rune::node::Worker::kRoutedAckBudget || evictionsWhileFilling > 0)
        << "the stalled sessions fill the share (" << r.gw.routedInFlightForTest(0) << ")";

    if (evictionsWhileFilling == 1) {                                // the full share already expired one
        const int64_t first = r.gw.lastCapEvictedSessionForTest();
        EXPECT_NE(std::find(sids.begin(), sids.begin() + 12, first), sids.begin() + 12)
            << "the first expiry hits a held session with the largest share (a full-budget one)";
    }
    // The largest-share held session before B arrives.
    int64_t heaviest = 0, heaviestShare = -1;
    for (int64_t sid : sids) {
        const int64_t f = r.gw.produceInFlightForTest(sid);
        if (f > heaviestShare) { heaviestShare = f; heaviest = sid; }
    }
    const int64_t b = r.session(kEgressB);
    std::vector<std::vector<uint8_t>> fb{produceFrame(b, 7, 0)};
    std::size_t sent = 0;
    for (int i = 0; i < 4'000 && !r.acks.count(7); ++i) {
        if (sent == 0) sent = r.gw.controlledPollFramesForTest(fb);
        for (int k = 0; k < kStalled; ++k)                           // the stalled clients keep offering
            if (r.gw.produceInFlightForTest(sids[k]) >= 0)
                consumed[k] += r.gw.controlledPollFramesForTest(bursts[k], consumed[k]);
        r.runWorker();
    }
    EXPECT_EQ(r.acks.count(7), 1u) << "a new session's gateway-routed produce is served (not deferred for 10 min)";
    EXPECT_GT(r.gw.workerCapEvictionsForTest(), 0u) << "a held session was expired";
    if (evictionsWhileFilling == 0)
        EXPECT_EQ(r.gw.lastCapEvictedSessionForTest(), heaviest) << "the held session with the largest share";
    EXPECT_EQ(r.gw.produceInFlightForTest(r.gw.lastCapEvictedSessionForTest()), -1) << "it was closed";
    EXPECT_GE(r.gw.produceInFlightForTest(b), 0) << "the reader B is never expired";
    EXPECT_EQ(r.w().ackRoomOpPausesForTest(), 0u);
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
}

// D8c fix round 2: the expiry never hits a session that is not Held by that worker (a busy reader), takes at most one
// session per cap-full episode, and waits kCapEvictAfterDeferredPolls deferred polls; a session that returned a Credit
// during the episode is skipped.
TEST(GatewayProduceCreditRegression, CapExpiryOnlyHitsAHeldSessionOncePerEpisodeRegression) {
    Rig r(/*withAcks=*/false);
    r.gw.setWorkerRoutedCapForTest(400);                                  // band [300, 400)
    const int64_t h = r.gw.injectTestSession(r.now), l1 = r.gw.injectTestSession(r.now),
                  l2 = r.gw.injectTestSession(r.now), n = r.gw.injectTestSession(r.now);
    auto frames = [](int64_t sid, int k) {
        std::vector<std::vector<uint8_t>> f;
        for (int i = 0; i < k; ++i) f.push_back(produceFrame(sid, i, 0));
        return f;
    };
    const auto fh = frames(h, 300), f1 = frames(l1, 64), f2 = frames(l2, 36), fn = frames(n, 4);
    ASSERT_EQ(r.gw.controlledPollFramesForTest(fh), 300u);
    ASSERT_EQ(r.gw.controlledPollFramesForTest(f1), 64u);
    ASSERT_EQ(r.gw.controlledPollFramesForTest(f2), 36u);
    ASSERT_EQ(r.gw.routedInFlightForTest(0), 400u);
    constexpr uint32_t N = 64;                                            // kCapEvictAfterDeferredPolls
    for (uint32_t i = 0; i < 3 * N; ++i) EXPECT_EQ(r.gw.controlledPollFramesForTest(fn), 0u);
    EXPECT_EQ(r.gw.workerCapEvictionsForTest(), 0u) << "nobody is Held: a full share of readers is never evicted";
    auto held = [&](int64_t sid) {
        rune::node::WorkerReturn s;
        s.type = rune::node::WorkerReturn::Type::SessionSignal; s.sessionId = sid; s.partition = 0;
        s.lastOffset = static_cast<int64_t>(SessionSignalKind::Held);
        r.gw.handleWorkerReturnForTest(s);
    };
    held(l1); held(l2);                                                   // h (the largest share) is not held
    for (uint32_t i = 0; i + 1 < N; ++i) ASSERT_EQ(r.gw.controlledPollFramesForTest(fn), 0u);
    EXPECT_EQ(r.gw.workerCapEvictionsForTest(), 0u) << "waits kCapEvictAfterDeferredPolls deferred polls";
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fn), 0u);
    EXPECT_EQ(r.gw.workerCapEvictionsForTest(), 1u);
    EXPECT_EQ(r.gw.lastCapEvictedSessionForTest(), l1) << "the HELD session with the largest share (64 > 36), not h";
    EXPECT_EQ(r.gw.routedInFlightForTest(0), 336u) << "its share is freed";
    EXPECT_EQ(r.gw.produceInFlightForTest(h), 300) << "the unheld heavy session is untouched";
    EXPECT_EQ(r.gw.controlledPollFramesForTest(fn), 4u) << "the new session is served";
    // A new episode (refill to the cap).  n becomes Held and its share is the largest, but it returns a Credit
    // during the episode (inside the band: the episode goes on) — a slow reader — so the held l2 is expired instead.
    const int64_t m = r.gw.injectTestSession(r.now);
    const auto f3 = frames(n, 64);
    EXPECT_EQ(r.gw.controlledPollFramesForTest(f3), 60u) << "n: 4 -> 64 in flight, share 400";
    ASSERT_EQ(r.gw.routedInFlightForTest(0), 400u);                       // its 61st frame was deferred: episode 2
    held(n);
    r.gw.handleWorkerReturnForTest(creditSignal(n, 1));                   // 399, still in the band
    const auto fm = frames(m, 1);
    ASSERT_EQ(r.gw.controlledPollFramesForTest(fm), 1u);                  // back to 400
    const auto one = frames(h, 1);
    for (uint32_t i = 0; i < N && r.gw.workerCapEvictionsForTest() < 2; ++i)
        EXPECT_EQ(r.gw.controlledPollFramesForTest(one), 0u);
    EXPECT_EQ(r.gw.workerCapEvictionsForTest(), 2u) << "one more expiry in the new episode";
    EXPECT_EQ(r.gw.lastCapEvictedSessionForTest(), l2) << "n credited in this episode: skipped although larger";
    EXPECT_GE(r.gw.produceInFlightForTest(n), 0);
    EXPECT_EQ(r.gw.produceCreditUnderflowsForTest(), 0u);
}
