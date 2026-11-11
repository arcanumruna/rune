// tests/shard/ClientShardTest.cpp — T15b: ClientShard in Invoker mode, single-
// threaded against the InvokerGateway (the test thread is the owner: it drives
// shard.doWork() and gw.pump()), with an injected duty clock.
//
// Rules (F8c): no std::atomic / mutex / future in tests, no sleeps.  Drive loops
// wait on progress with a 10 s failure deadline.  The ONLY real-time wait is the
// stalled-owner test, which yields on Aeron's own conductor clock until
// ClientConductor.h:436's condition holds (Aeron's clock cannot be injected).
#include "../support/AllocCounter.h"
#include "../support/FreePort.h"
#include "../support/InvokerGateway.h"
#include "../support/OwnDriverGateway.h"

#include <rune/client/shard/ClientShard.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <arpa/inet.h>
#include <fcntl.h>
#include <fstream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using namespace std::chrono_literals;

namespace {
int     g_ctr = 0;  // test thread only
int64_t g_now = 0;  // injected duty clock (Invoker: the test thread is the only thread)
int64_t fakeNow() noexcept { return g_now; }

const std::vector<uint8_t> kVal(32, 0x21);

// Every group directory is removed at process exit (the drivers delete their
// aeron/ subdirectories; the parents would accumulate otherwise).
struct DirSweeper {
    std::vector<std::string> dirs;
    ~DirSweeper() {
        for (const auto& d : dirs) {
            std::error_code ec;
            std::filesystem::remove_all(d, ec);
        }
    }
};
DirSweeper g_sweeper;  // test thread only

rs::ShardGroupConfig group(int32_t in, int32_t eg, uint32_t count, rs::ShardMode mode) {
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = "aeron:ipc";
    g.base.gatewayStreamId = in;
    g.base.egressChannel   = "aeron:ipc";
    g.base.egressStreamId  = eg;
    g.base.dataDir = std::filesystem::temp_directory_path().string() + "/shard_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    g_sweeper.dirs.push_back(g.base.dataDir);
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 8;
    g.base.topics.push_back(ts);
    g.shardCount        = count;
    g.mode              = mode;
    g.producer.acks     = rc::Acks::Leader;
    g.producer.lingerUs = 0;
    return g;
}

// Invoker group config on the injected clock (starts at the real steady time so
// sessions opened lazily with a steady_clock stamp agree with it).
rs::ShardGroupConfig invokerGroup(int32_t in, int32_t eg) {
    auto g = group(in, eg, 1, rs::ShardMode::Invoker);
    g.base.nanoClock = &fakeNow;
    g_now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
    return g;
}

rc::Key keyFor(int32_t partition, int32_t partitions = 8) {
    for (uint64_t k = 0; k < 1'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to the partition");
}

template <class Step, class P>
bool driveUntil(Step&& step, P&& pred) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        step();
        if (pred()) return true;
    }
    return false;
}

// Exactly-once ledger of batch acks (records per outcome).
struct BatchLedger {
    int64_t                    ok = 0, failed = 0, records = 0;
    int                        batches = 0;
    std::map<int32_t, int32_t> nextSeq;
    bool                       contiguous = true;
    void operator()(const rc::BatchAck& a) {
        ++batches;
        records += a.recordCount;
        if (a.errorCode == 0) {
            ok += a.recordCount;
            if (a.baseSequence != nextSeq[a.partition]) contiguous = false;
            nextSeq[a.partition] = a.baseSequence + a.recordCount;
        } else {
            failed += a.recordCount;
        }
    }
};

struct ShardRig {
    std::unique_ptr<rs::ClientShard> shard;  // owns the driver: constructed first
    test_support::InvokerGateway     gw;
    BatchLedger                      ledger;
    std::vector<rs::ShardStatus>     events;
    ShardRig(const rs::ShardGroupConfig& g, uint32_t i)
        : shard(std::make_unique<rs::ClientShard>(g, i)),
          gw(rs::shardClientConfig(g, i).dataDir + "/aeron", g.base.gatewayChannel,
             g.base.gatewayStreamId, rs::shardClientConfig(g, i).egressChannel, g.base.egressStreamId) {}
    ~ShardRig() {
        if (shard) shard->closeImmediately();
    }
    // One owner pass: shard, gateway, batch acks, status events.
    void step(int64_t advanceNs = 0) {
        g_now += advanceNs;
        if (!shard) {
            gw.pump();
            return;
        }
        shard->doWork();
        gw.pump();
        shard->pollBatchAcks([&](const rc::BatchAck& a) { ledger(a); });
        shard->pollEvents([&](const rs::ShardStatus& s) { events.push_back(s); });
    }
    template <class P>
    bool drive(P&& pred, int64_t advanceNs = 0) {
        return driveUntil([&] { step(advanceNs); }, pred);
    }
    [[nodiscard]] rs::ShardState state() const { return shard->status().state; }
    [[nodiscard]] bool saw(rs::ShardState s) const {
        for (const auto& e : events) {
            if (e.state == s) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        rc::Completions cq(shard->client());
        ASSERT_EQ(shard->connect(cq, 1), rc::OpResult::Accepted);
        bool         done = false;
        rc::OpStatus st   = rc::OpStatus::Closed;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 1) {
                    done = true;
                    st   = c.status;
                }
            });
            return done;
        }));
        ASSERT_EQ(st, rc::OpStatus::Ok);
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
    // Send n records to `partition` and wait until the gateway received them.
    void sendTo(int32_t partition, int n) {
        const auto        topic  = shard->topic("t");
        const std::size_t before = gw.produced.size();
        for (int i = 0; i < n; ++i) ASSERT_EQ(shard->trySend(topic, keyFor(partition), kVal), rc::SendResult::Ok);
        ASSERT_TRUE(drive([&] { return gw.produced.size() > before; }));
    }
};

// The fake broker on its OWN driver (BrokerHost): a transport rebuild replaces the client's driver, never the broker's.
// The shard reaches it over UDP.
rs::ShardGroupConfig ownDriverGroup(int32_t in, int32_t eg) {
    auto g          = invokerGroup(in, eg);
    const auto port = rune::client::testing::allocFreeUdpPortRange(2);
    g.base.gatewayChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port);
    g.base.egressChannel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port + 1);
    return g;
}

struct OwnDriverRig {
    test_support::BrokerHost         broker;
    std::unique_ptr<rs::ClientShard> shard;
    BatchLedger                      ledger;
    std::vector<rs::ShardStatus>     events;
    explicit OwnDriverRig(const rs::ShardGroupConfig& g)
        : broker(test_support::BrokerHost::uniqueDir(), g.base.gatewayChannel, g.base.gatewayStreamId,
                 rs::shardClientConfig(g, 0).egressChannel, g.base.egressStreamId),
          shard(std::make_unique<rs::ClientShard>(g, 0)) {}
    ~OwnDriverRig() {
        if (shard) shard->closeImmediately();
    }
    [[nodiscard]] test_support::InvokerGateway& gw() { return broker.gw; }
    void step(int64_t advanceNs = 0) {
        g_now += advanceNs;
        shard->doWork();
        gw().pump();
        shard->pollBatchAcks([&](const rc::BatchAck& a) { ledger(a); });
        shard->pollEvents([&](const rs::ShardStatus& s) { events.push_back(s); });
    }
    template <class P>
    bool drive(P&& pred, int64_t advanceNs = 0) {
        return driveUntil([&] { step(advanceNs); }, pred);
    }
    [[nodiscard]] rs::ShardState state() const { return shard->status().state; }
    [[nodiscard]] bool saw(rs::ShardState s) const {
        for (const auto& e : events) {
            if (e.state == s) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw().ready(); }));
        rc::Completions cq(shard->client());
        ASSERT_EQ(shard->connect(cq, 1), rc::OpResult::Accepted);
        bool         done = false;
        rc::OpStatus st   = rc::OpStatus::Closed;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 1) {
                    done = true;
                    st   = c.status;
                }
            });
            return done;
        }));
        ASSERT_EQ(st, rc::OpStatus::Ok);
        ASSERT_TRUE(drive([&] { return state() == rs::ShardState::Live && gw().metadataRequests > 0; }));
    }
    // Accept n records for `partition`.
    void accept(int32_t partition, int n) {
        const auto topic = shard->topic("t");
        for (int i = 0; i < n; ++i) ASSERT_EQ(shard->trySend(topic, keyFor(partition), kVal), rc::SendResult::Ok);
    }
    // Send n records to `partition` and wait until the gateway received them.
    void sendTo(int32_t partition, int n) {
        const std::size_t before = gw().produced.size();
        accept(partition, n);
        ASSERT_TRUE(drive([&] { return gw().produced.size() > before; }));
    }
    // The owner stalls past the Aeron client liveness while only the broker runs; Aeron's clock cannot be injected, so
    // this waits on it (ClientConductor.h:436: nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs).  No tuned sleep.
    void stallPastAeronLiveness(int64_t livenessNs) {
        const long long livenessMs = livenessNs / 1'000'000;
        const long long t0         = aeron::currentTimeMillis();
        while (aeron::currentTimeMillis() <= t0 + livenessMs) {
            gw().pump();
            std::this_thread::yield();
        }
    }
};

// The blocking close() pumps the shard itself; the single-threaded fake broker
// is pumped from the Client's duty-cycle test hook (Invoker owner = this thread).
test_support::InvokerGateway* g_pumpGw = nullptr;
void pumpGw() {
    if (g_pumpGw != nullptr) g_pumpGw->pump();
}
struct PumpGwDuring {
    explicit PumpGwDuring(ShardRig& r) {
        g_pumpGw = &r.gw;
        r.shard->client().testSetDutyCycleHook(&pumpGw);
    }
    ~PumpGwDuring() { g_pumpGw = nullptr; }
};

// Captures one control-op completion through a CompletionHandler (duty thread).
struct HandlerCapture {
    int          calls  = 0;
    rc::OpStatus status = rc::OpStatus::Ok;
    static void on(void* ctx, const rc::Completion& c, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<HandlerCapture*>(ctx);
        ++self->calls;
        self->status = c.status;
    }
    rc::CompletionHandler handler() { return rc::CompletionHandler{&HandlerCapture::on, this}; }
};
} // namespace

// ---- produce / route -----------------------------------------------------------

TEST(ClientShardRegression, InvokerProducesAndAcksWithNoThreads) {
    ShardRig r(group(3800, 3801, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto topic = r.shard->topic("t");
    for (uint64_t i = 0; i < 100; ++i) ASSERT_EQ(r.shard->trySend(topic, rc::Key(i), kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.ledger.records >= 100; }));
    EXPECT_EQ(r.ledger.records, 100) << "exactly one batch ack per record";
    EXPECT_EQ(r.ledger.ok, 100);
    EXPECT_TRUE(r.ledger.contiguous);
    EXPECT_EQ(r.state(), rs::ShardState::Live);
}

TEST(ClientShardRegression, OnePassIsOneFramePerPartition) {
    ShardRig r(group(3802, 3803, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto    topic = r.shard->topic("t");
    const rc::Key k     = keyFor(3);
    for (int i = 0; i < 10; ++i) ASSERT_EQ(r.shard->trySend(topic, k, kVal), rc::SendResult::Ok);
    const std::size_t before = r.gw.produced.size();
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() > before; }));
    EXPECT_EQ(r.gw.produced.back().records, 10) << "one pass = one frame per partition (no linger timer)";
}

TEST(ClientShardRegression, ForeignPartitionIsWrongShardNoCompletion) {
    ShardRig   r(group(3804, 3805, 2, rs::ShardMode::Invoker), 0);
    const auto topic   = r.shard->topic("t");
    int32_t    foreign = -1;
    for (int32_t p = 0; p < 8 && foreign < 0; ++p) {
        if (!r.shard->owns(1, p)) foreign = p;
    }
    ASSERT_GE(foreign, 0);
    EXPECT_EQ(r.shard->trySend(topic, keyFor(foreign), kVal), rc::SendResult::WrongShard);
    EXPECT_EQ(r.shard->trySendTo(topic, foreign, keyFor(foreign), kVal), rc::SendResult::WrongShard);
    EXPECT_THROW(r.shard->send(topic, keyFor(foreign), kVal), std::logic_error);
    for (int i = 0; i < 100; ++i) r.step();
    EXPECT_EQ(r.ledger.batches, 0) << "nothing accepted: no batch ack ever follows";
}

// Review finding 3 (M3): the partition is computed once and passed down — the
// producer routes by the partition it is given, it never re-hashes the key.
TEST(ClientShardRegression, PartitionComputedOnceIsTheOneRouted) {
    ShardRig r(group(3820, 3821, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto    topic = r.shard->topic("t");
    const rc::Key k     = keyFor(2);
    ASSERT_EQ(r.shard->trySendTo(topic, 5, k, kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return !r.gw.produced.empty(); }));
    EXPECT_EQ(r.gw.produced.back().partition, 5) << "routed by the given partition, not hash(key) = 2";
    EXPECT_EQ(r.shard->trySendTo(topic, 8, k, kVal), rc::SendResult::InvalidArgument);
    EXPECT_EQ(r.shard->trySendTo(topic, -1, k, kVal), rc::SendResult::InvalidArgument);
}

TEST(ClientShardRegression, TrySendZeroAllocation) {
    ShardRig r(group(3822, 3823, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    const auto topic = r.shard->topic("t");
    // Warm up every partition (builders, in-flight slots) and drain.
    for (uint64_t i = 0; i < 64; ++i) ASSERT_EQ(r.shard->trySend(topic, rc::Key(i), kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.ledger.records >= 64; }));
    alloc_counter::start();
    int ok = 0;
    for (uint64_t i = 0; i < 64; ++i) ok += r.shard->trySend(topic, rc::Key(i), kVal) == rc::SendResult::Ok;
    const long allocs = alloc_counter::stop();
    EXPECT_EQ(ok, 64);
    EXPECT_EQ(allocs, 0) << "ClientShard::trySend must not allocate";
}

TEST(ClientShardRegression, DoWorkIsInvokerOnly) {
    auto            g = group(3808, 3809, 1, rs::ShardMode::Agent);
    rs::ClientShard s(g, 0);
    EXPECT_THROW((void)s.doWork(), std::logic_error);
    EXPECT_EQ(s.pollBatchAcks([](const rc::BatchAck&) {}), 0) << "Agent: no batch-ack queue";
}

TEST(ClientShardRegression, SameIndexTwiceFailsLoudOnActiveDriverDir) {
    auto            g = group(3824, 3825, 1, rs::ShardMode::Agent);
    rs::ClientShard first(g, 0);
    EXPECT_ANY_THROW(rs::ClientShard(g, 0));
}

// ---- connect -----------------------------------------------------------------------

TEST(ClientShardRegression, ConnectViaTokenAndHandlerNoFuture) {
    // Token into a Completions: ShardRig::connect().  Here: a CompletionHandler.
    ShardRig r(group(3826, 3827, 1, rs::ShardMode::Invoker), 0);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    HandlerCapture h;
    ASSERT_EQ(r.shard->connect(h.handler(), 7), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return h.calls > 0; }));
    EXPECT_EQ(h.calls, 1);
    EXPECT_EQ(h.status, rc::OpStatus::Ok);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live; }));
    EXPECT_TRUE(r.saw(rs::ShardState::Live));
}

namespace {
struct CallbackWait {
    rs::ClientShard* shard   = nullptr;
    int              threw   = 0;
    int              calls   = 0;
    static void on(void* ctx, const rc::Completion&, std::span<const uint8_t>) noexcept {
        auto* self = static_cast<CallbackWait*>(ctx);
        ++self->calls;
        try {
            self->shard->connectAndWait(1s);
        } catch (const std::logic_error&) {
            ++self->threw;
        } catch (...) {
        }
    }
};
} // namespace

TEST(ClientShardRegression, ConnectAndWaitFromCallbackThrows) {
    ShardRig r(group(3828, 3829, 1, rs::ShardMode::Invoker), 0);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    CallbackWait cw;
    cw.shard = r.shard.get();
    ASSERT_EQ(r.shard->connect(rc::CompletionHandler{&CallbackWait::on, &cw}, 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return cw.calls > 0; }));
    EXPECT_EQ(cw.threw, 1) << "a blocking wait inside a duty-cycle callback is a logic_error, never a hang";
}

// ---- session loss and state ------------------------------------------------------

// T15a m8: a non-retriable answer to the seed's connect completes the connect
// token at once (BrokerError, no deadline wait — the clock is frozen) and the
// shard is Failed{Rejected}; sends then report Closed.
TEST(ClientShardRegression, SeedRejectedNonRetriableIsFailedRejected) {
    ShardRig r(invokerGroup(3830, 3831), 0);
    r.gw.connectErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::ProtocolVersion);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    HandlerCapture h;
    ASSERT_EQ(r.shard->connect(h.handler(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return h.calls > 0; }));
    EXPECT_EQ(h.status, rc::OpStatus::BrokerError);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Failed; }));
    EXPECT_EQ(r.shard->status().failure, rs::ShardFailure::Rejected);
    EXPECT_EQ(r.shard->status().rejectCode, 1);
    EXPECT_TRUE(r.shard->closed());
    EXPECT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(0), kVal), rc::SendResult::Closed);
}

// The seed session is lost while Live (SessionExpired): Reconnecting, then Live
// again — never Failed, never Idle.  A pending control op completes SessionLost;
// in-flight batches are re-offered and every record is acked exactly once.
TEST(ClientShardRegression, SeedSessionLossWhileLiveReconnectsWithoutFailing) {
    ShardRig r(invokerGroup(3832, 3833), 0);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live; }));
    r.gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    for (int32_t p = 0; p < 4; ++p) r.sendTo(p, 5);
    rc::Completions cq(r.shard->client());
    const uint8_t   echo[] = {1};
    ASSERT_EQ(r.shard->client().ping(echo, cq, 9), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.gw.pings > 0; }));
    r.gw.holdPartitions.clear();
    r.gw.held.clear();  // the broker lost them with the session: only the re-offer is acked
    r.events.clear();   // from Live on (Idle / Connecting preceded the first connect)
    r.gw.expireSession(r.gw.assignedSessionId);
    rc::OpStatus pingSt = rc::OpStatus::Ok;
    bool         pinged = false;
    ASSERT_TRUE(r.drive([&] {
        cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            pinged = true;
            pingSt = c.status;
        });
        return pinged && r.state() == rs::ShardState::Live && r.ledger.records >= 20;
    }, 1'000'000));
    EXPECT_EQ(pingSt, rc::OpStatus::SessionLost);
    EXPECT_TRUE(r.saw(rs::ShardState::Reconnecting));
    EXPECT_FALSE(r.saw(rs::ShardState::Failed));
    EXPECT_FALSE(r.saw(rs::ShardState::Idle));
    for (int i = 0; i < 200; ++i) r.step(1'000'000);
    EXPECT_EQ(r.ledger.records, 20) << "every record acked exactly once";
    EXPECT_EQ(r.ledger.ok, 20);
    EXPECT_TRUE(r.ledger.contiguous);
}

namespace {
// Routes every partition of topic 1 to a leader node on a UDP endpoint the
// gateway also serves (endpoint 1: leader control, endpoint 2: its data stream).
void addLeader(test_support::InvokerGateway& gw, int32_t port, int32_t ctlStream, int32_t dataStream) {
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port);
    gw.brokerHost        = "127.0.0.1";
    gw.brokerPort        = port;
    for (int32_t p = 0; p < 8; ++p) {
        test_support::InvokerGateway::PartitionAd ad;
        ad.topicId         = 1;
        ad.partition       = p;
        ad.leaderId        = 0;
        ad.ingressStreamId = dataStream;
        gw.partitions.push_back(ad);
    }
    gw.addEndpoint(ch, ctlStream);
    gw.addEndpoint(ch, dataStream);
}
} // namespace

// A routed leader node is down at connect: the connect token completes Ok (the
// connect op is about the seed session) and the shard is Degraded; that node's
// partitions wait; once the leader answers the shard is Live and the parked
// record is acked.
TEST(ClientShardRegression, ConnectWithLeaderDownIsDegradedTokenOk) {
    auto g = invokerGroup(3834, 3835);
    ShardRig r(g, 0);
    addLeader(r.gw, 24850, 3834, 3836);
    r.gw.silentEndpoints = {1};
    r.connect();  // token Ok
    ASSERT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(3), kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Degraded; }, 1'000'000));
    EXPECT_EQ(r.shard->status().downMask, 1u) << "leader session 0 (the only routed node) is down";
    EXPECT_EQ(r.ledger.records, 0);
    r.gw.silentEndpoints.clear();
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live && r.ledger.records == 1; },
                        10'000'000));
    EXPECT_EQ(r.ledger.ok, 1);
    ASSERT_FALSE(r.gw.produced.empty());
    EXPECT_EQ(r.gw.produced.back().endpoint, 2) << "the record went to the leader's data endpoint";
}

// A routed leader session is lost while Live: it stops answering; every session
// goes stale (responseTimeout on the duty clock); the seed is answered again,
// the leader is not -> Degraded; then it answers -> Live.  Never Failed.
TEST(ClientShardRegression, LeaderSessionLossWhileLiveIsDegradedThenLive) {
    auto g = invokerGroup(3870, 3871);
    ShardRig r(g, 0);
    addLeader(r.gw, 24854, 3870, 3872);
    r.connect();
    // The leader session opens on the first record to its partitions (1 ms of
    // duty time per pass: its connect retries run on the duty clock).
    ASSERT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(3), kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live && r.ledger.ok == 1; }, 1'000'000));
    r.events.clear();
    r.gw.silentEndpoints = {1};
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Degraded; }, 50'000'000));
    r.gw.silentEndpoints.clear();
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live; }, 10'000'000));
    EXPECT_TRUE(r.saw(rs::ShardState::Degraded));
    EXPECT_FALSE(r.saw(rs::ShardState::Failed));
    ASSERT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(5), kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.ledger.ok == 2; }, 1'000'000));
}

// A leader session opened lazily (first record to its node) runs on the
// client's duty clock (ClientConfig::nanoClock), never on steady_clock.  RED:
// openSession_ stamped its connect with steady_clock::now(), so on an injected
// clock its connect timers sat in another clock domain — here (duty clock far
// behind steady time) the ConnectRequest's fast retry and connect timeout never
// came due, and a Connect sent before the leader's UDP publication connected
// was never re-sent: the leader never came up.  (With the duty clock behind by
// the rig's set-up wall time — invokerGroup() — it only delayed the leader by
// that much: LeaderSessionLossWhileLiveIsDegradedThenLive's first phase lasted
// set-up-wall-time + ~300 ms of duty time.)
TEST(ClientShardRegression, LeaderSessionOpensOnTheDutyClockNotSteadyTimeRegression) {
    auto g = invokerGroup(3876, 3877);
    g_now  = 1'000'000'000;   // the duty clock: 1 s — unrelated to steady_clock
    ShardRig r(g, 0);
    addLeader(r.gw, 24856, 3876, 3878);
    r.connect();
    const int64_t start = g_now;
    ASSERT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(3), kVal), rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live && r.ledger.ok == 1; }, 1'000'000))
        << "the leader session never connected on the duty clock (state " << rs::toString(r.state()) << ")";
    EXPECT_LT(g_now - start, int64_t{5'000'000'000}) << "the leader's connect retries ran on another clock";
    EXPECT_EQ(r.ledger.failed, 0);
}

TEST(ClientShardRegression, DeliveryTimeoutFailsBatchNotShard) {
    auto g                       = invokerGroup(3838, 3839);
    g.producer.deliveryTimeoutMs = 2000;
    ShardRig r(g, 0);
    r.connect();
    r.gw.holdPartitions = {3};
    r.sendTo(3, 2);
    ASSERT_TRUE(r.drive([&] { return r.ledger.failed == 2; }, 10'000'000));  // 10 ms per pass
    EXPECT_EQ(r.ledger.records, 2);
    EXPECT_EQ(r.state(), rs::ShardState::Live) << "a delivery timeout fails the batch, never the shard";
    r.sendTo(4, 1);
    ASSERT_TRUE(r.drive([&] { return r.ledger.ok == 1; }));
    EXPECT_FALSE(r.shard->closed());
}

// A terminal seed Rejected: the broker expires the session and answers every connect with a non-retriable code.
void rejectSeed(test_support::InvokerGateway& gw) {
    gw.connectErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::ProtocolVersion);
    gw.expireSession(gw.assignedSessionId);
}

// ---- failure path ------------------------------------------------------------------

// The Aeron client closes (fault seam) with records in flight and others accepted but not yet sent: the transport is
// rebuilt, the session resumes, and all 28 records are acked Ok exactly once (the gateway's sequences per partition stay
// contiguous).  Was: Failed{TransportClosed} with every record Closed.
TEST(ClientShardRegression, TransportFaultRebuildsInFlightAckedOnceRegression) {
    OwnDriverRig r(ownDriverGroup(3840, 3841));
    r.connect();
    r.gw().holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    for (int32_t p = 0; p < 8; ++p) r.sendTo(p, 3);
    // Accepted but not yet sent (no doWork between them and the fault).
    for (int i = 0; i < 4; ++i) r.accept(i, 1);
    r.events.clear();
    r.shard->client().testInjectTransportFault();
    ASSERT_TRUE(r.drive([&] { return r.shard->client().testTransportRebuilds() == 1 && r.state() == rs::ShardState::Live; }, 100'000));
    r.gw().holdPartitions.clear();
    r.gw().releaseHeld();
    ASSERT_TRUE(r.drive([&] { return r.ledger.records >= 28; }));
    for (int i = 0; i < 100; ++i) r.step();
    EXPECT_EQ(r.ledger.records, 28) << "every accepted record completes exactly once";
    EXPECT_EQ(r.ledger.ok, 28);
    EXPECT_TRUE(r.ledger.contiguous);
    EXPECT_EQ(r.shard->client().testTransportRebuilds(), 1u);
    EXPECT_TRUE(r.saw(rs::ShardState::Reconnecting));
    EXPECT_FALSE(r.saw(rs::ShardState::Failed));
    std::map<int32_t, std::set<int32_t>> seqs;
    for (const auto& pr : r.gw().produced) {
        for (int32_t q = 0; q < pr.records; ++q) seqs[pr.partition].insert(pr.baseSequence + q);
    }
    int64_t distinct = 0;
    for (const auto& [part, set] : seqs) {
        distinct += static_cast<int64_t>(set.size());
        EXPECT_EQ(*set.begin(), 0) << part;
        EXPECT_EQ(*set.rbegin(), static_cast<int32_t>(set.size()) - 1) << "partition " << part << ": a sequence gap";
    }
    EXPECT_EQ(distinct, 28);
    EXPECT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(0), kVal), rc::SendResult::Ok);
}

// M2: a failed shard does no I/O — no keepalives, no metadata, nothing — while
// the owner keeps driving it.
TEST(ClientShardRegression, FailedShardDoesNoIo) {
    ShardRig r(invokerGroup(3842, 3843), 0);
    r.connect();
    for (int i = 0; i < 50; ++i) r.step(100'000'000);  // 5 s: keepalives flow (1 s cadence)
    const int liveFrames = r.gw.framesReceived;
    ASSERT_GT(liveFrames, 0);
    rejectSeed(r.gw);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Failed; }));
    const int atFailure = r.gw.framesReceived;
    for (int i = 0; i < 100; ++i) r.step(100'000'000);  // 10 s of duty time
    EXPECT_EQ(r.gw.framesReceived, atFailure) << "a failed shard sends nothing";
}

TEST(ClientShardRegression, FailedThenClosedBothPublishedOnStatusRing) {
    ShardRig r(invokerGroup(3844, 3845), 0);
    r.connect();
    rejectSeed(r.gw);
    ASSERT_TRUE(r.drive([&] { return r.saw(rs::ShardState::Failed); }));
    r.shard->closeImmediately();
    r.shard->pollEvents([&](const rs::ShardStatus& s) { r.events.push_back(s); });
    ASSERT_GE(r.events.size(), 2u);
    EXPECT_EQ(r.events[r.events.size() - 2].state, rs::ShardState::Failed);
    EXPECT_EQ(r.events.back().state, rs::ShardState::Closed);
    EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
}

// An owner stall past the Aeron client liveness timeout closes the Aeron client.  The shard rebuilds the transport and
// reconnects: Live -> Reconnecting -> Live, never Failed.  The records in flight at the stall and the ones accepted
// during it are each acked Ok exactly once.  Aeron's conductor clock cannot be injected (Aeron.cpp:43 hard-wires
// currentTimeMillis), so the stall yields on it (no tuned sleep).  RED on main: the shard went Failed{TransportClosed}
// and every record completed Closed.
TEST(ClientShardRegression, AeronClientClosedByOwnerStallRebuildsAndAcksExactlyOnceRegression) {
    auto g = ownDriverGroup(3806, 3807);
    // T15a I4: shards take the liveness from the group (default 1 h); an explicit override to the smallest value
    // validate() accepts (1 s = 2x the Aeron client's 500 ms keepalive) is the shortest real close there is.
    g.aeronClientLivenessTimeoutNs = rune::client::transport::kMinClientLivenessTimeoutNs;
    OwnDriverRig r(g);
    r.connect();
    r.gw().holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    r.sendTo(2, 5);
    r.events.clear();
    r.accept(3, 5);
    r.stallPastAeronLiveness(g.aeronClientLivenessTimeoutNs);
    ASSERT_TRUE(r.drive([&] { return r.saw(rs::ShardState::Reconnecting) || r.state() == rs::ShardState::Failed; }));
    ASSERT_NE(r.state(), rs::ShardState::Failed) << "the closed Aeron client must be rebuilt, not fatal";
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live; }, 100'000));
    r.gw().holdPartitions.clear();
    r.gw().releaseHeld();
    ASSERT_TRUE(r.drive([&] { return r.ledger.records >= 10; }, 1'000'000));
    for (int i = 0; i < 50; ++i) r.step(1'000'000);
    EXPECT_EQ(r.ledger.records, 10) << "every accepted record acked exactly once";
    EXPECT_EQ(r.ledger.ok, 10);
    EXPECT_TRUE(r.ledger.contiguous);
    EXPECT_TRUE(r.saw(rs::ShardState::Reconnecting));
    EXPECT_FALSE(r.saw(rs::ShardState::Failed));
    EXPECT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(1), kVal), rc::SendResult::Ok);
}

namespace {
// Injects the Aeron-client-closed fault and drives (slow fake clock: the rebuilt UDP link needs real time) until
// the transport was rebuilt `n` times in all and the shard is Live again.
bool faultAndRecover(OwnDriverRig& r, uint64_t n) {
    r.shard->client().testInjectTransportFault();
    return r.drive([&] { return r.shard->client().testTransportRebuilds() == n && r.state() == rs::ShardState::Live; }, 100'000);
}
// A seed channel Aeron rejects while parsing the port (no DNS lookup): every rebuild attempt fails, so the transport stays Down across duty cycles.
// The test restores the good channel to let it recover.
const char* const kUnreachableSeed = "aeron:udp?endpoint=127.0.0.1:65536";

// Injects the fault and drives until the first rebuild attempt failed (the shard shows Reconnecting, still Down).
bool faultIntoDown(OwnDriverRig& r) {
    r.shard->client().testSetSeedIngressChannel(kUnreachableSeed);
    r.shard->client().testInjectTransportFault();
    return r.drive([&] { return r.state() == rs::ShardState::Reconnecting && r.shard->status().attempt >= 1; }, 1'000'000);
}
}  // namespace

// Records accepted while the transport is Down (rebuilds failing) are held, never refused as Closed: the ring fills up
// to BackPressured, and once the rebuild works everything is delivered, in order, after the records in flight.
TEST(ClientShardRegression, RecordsAcceptedWhileTransportDownAreDeliveredInOrderRegression) {
    auto              g = ownDriverGroup(3808, 3809);
    const std::string goodChannel = g.base.gatewayChannel;
    OwnDriverRig r(g);
    r.connect();
    r.sendTo(1, 3);
    ASSERT_TRUE(faultIntoDown(r));
    EXPECT_EQ(r.shard->client().testTransportRebuilds(), 0u);
    int              accepted = 0;
    rc::SendResult   last     = rc::SendResult::Ok;
    const auto       topic    = r.shard->topic("t");
    for (int i = 0; i < 200'000 && last == rc::SendResult::Ok; ++i) {
        last = r.shard->trySend(topic, keyFor(1), kVal);
        if (last == rc::SendResult::Ok) ++accepted;
        if (i % 32 == 0) r.step(10'000);
    }
    EXPECT_GT(accepted, 0);
    EXPECT_TRUE(last == rc::SendResult::BackPressured || last == rc::SendResult::QueueFull)
        << "a full ring while Down is back-pressure, got " << static_cast<int>(last);
    EXPECT_NE(r.state(), rs::ShardState::Failed);
    EXPECT_EQ(r.ledger.failed, 0);
    r.shard->client().testSetSeedIngressChannel(goodChannel);
    ASSERT_TRUE(r.drive([&] { return r.ledger.records >= 3 + accepted; }, 1'000'000));
    for (int i = 0; i < 50; ++i) r.step(100'000);
    EXPECT_EQ(r.ledger.records, 3 + accepted);
    EXPECT_EQ(r.ledger.ok, 3 + accepted);
    EXPECT_TRUE(r.ledger.contiguous) << "partition 1's sequences arrive in order";
    EXPECT_EQ(r.shard->client().testTransportRebuilds(), 1u);
}

// A control op sent before the close completes SessionLost; one issued while the transport is Down is held and completes
// Ok after the reconnect; a connect pending at the close completes too.  Each exactly once.
TEST(ClientShardRegression, ControlOpsAcrossRebuildRegression) {
    {
        auto              g = ownDriverGroup(3810, 3811);
        g.base.pingTimeoutMs = 600'000;   // the fake duty clock must not burn the ping's deadline while the real-time reconnect runs
        const std::string goodChannel = g.base.gatewayChannel;
        OwnDriverRig r(g);
        r.connect();
        rc::Completions     cq(r.shard->client());
        std::map<rc::Token, int>          times;
        std::map<rc::Token, rc::OpStatus> status;
        const auto collect = [&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                ++times[c.token];
                status[c.token] = c.status;
            });
        };
        const uint8_t echo[] = {1};
        r.gw().replyPongs = false;
        ASSERT_EQ(r.shard->client().ping(echo, cq, 5), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return r.gw().pings > 0; }));
        ASSERT_TRUE(faultIntoDown(r));
        r.gw().replyPongs = true;
        ASSERT_TRUE(r.drive([&] { collect(); return times[5] > 0; }, 1'000'000));
        EXPECT_EQ(status[5], rc::OpStatus::SessionLost) << "the ping sent before the close completes at the close";
        EXPECT_EQ(r.shard->client().testTransportRebuilds(), 0u) << "still Down";
        ASSERT_EQ(r.shard->client().ping(echo, cq, 6), rc::OpResult::Accepted) << "an op issued while Down is accepted";
        for (int i = 0; i < 20; ++i) { r.step(1'000'000); collect(); }
        EXPECT_EQ(times[6], 0) << "held while Down: nothing is sent on a closed transport";
        r.shard->client().testSetSeedIngressChannel(goodChannel);
        ASSERT_TRUE(r.drive([&] { collect(); return times[6] > 0; }, 100'000));
        EXPECT_EQ(status[6], rc::OpStatus::Ok);
        ASSERT_EQ(r.shard->client().ping(echo, cq, 7), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { collect(); return times[7] > 0; }, 100'000));
        EXPECT_EQ(status[7], rc::OpStatus::Ok);
        for (int i = 0; i < 20; ++i) { r.step(1'000'000); collect(); }
        for (const rc::Token t : {5, 6, 7}) EXPECT_EQ(times[t], 1) << "token " << t << " completes exactly once";
    }
    {   // a connect pending when the Aeron client closes
        OwnDriverRig r(ownDriverGroup(3830, 3831));
        r.gw().replyConnects = false;
        ASSERT_TRUE(r.drive([&] { return r.gw().ready(); }));
        rc::Completions cq(r.shard->client());
        ASSERT_EQ(r.shard->connect(cq, 1), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return r.gw().connectsArrived >= 1; }));
        r.shard->client().testInjectTransportFault();
        ASSERT_TRUE(r.drive([&] { return r.shard->client().testTransportRebuilds() == 1; }, 100'000));
        r.gw().replyConnects = true;
        int          times = 0;
        rc::OpStatus st    = rc::OpStatus::Closed;
        ASSERT_TRUE(r.drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { ++times; st = c.status; });
            return times > 0 && r.state() == rs::ShardState::Live;
        }, 100'000)) << "a connect pending at the close never completed";
        for (int i = 0; i < 20; ++i) {
            r.step(1'000'000);
            cq.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++times; });
        }
        EXPECT_EQ(times, 1) << "the pending connect completes exactly once";
        EXPECT_TRUE(st == rc::OpStatus::Ok || st == rc::OpStatus::Timeout || st == rc::OpStatus::SessionLost)
            << "status " << static_cast<int>(st);
    }
}

// Storm control.  The first close rebuilds at once.  A second close before the seed reconnected (the Resuming window)
// waits the doubled gap on the duty clock; only a Connected edge resets it, so a close after that rebuilds at once
// again.  Every cycle leaves the pub and sub slot tables as they were (no leak), and the shard never goes Failed.
TEST(ClientShardRegression, RepeatedTransportClosesRecoverEachTimeRegression) {
    OwnDriverRig r(ownDriverGroup(3812, 3813));
    r.connect();
    auto&       client = r.shard->client();
    const auto& tr     = client.testTransport();
    r.sendTo(1, 2);
    const std::size_t pubSlots = tr.pubSlotCountForTest(), subSlots = tr.subSlotCountForTest();
#ifdef __linux__
    const auto openFds = [] { return std::distance(std::filesystem::directory_iterator("/proc/self/fd"), std::filesystem::directory_iterator{}); };
    const auto fds     = openFds();
#endif
    // 1: back to back with the live session: rebuilt at once.
    client.testInjectTransportFault();
    ASSERT_TRUE(r.drive([&] { return client.testTransportRebuilds() == 1; }));
    ASSERT_EQ(r.state(), rs::ShardState::Reconnecting) << "the session is Resuming: no Connected edge yet";
    // 2: a close during Resuming: the doubled gap applies (100 ms on the fake clock), nothing is rebuilt inside it.
    client.testInjectTransportFault();
    const int64_t t0 = g_now;
    for (int i = 0; i < 90; ++i) r.step(1'000'000);
    EXPECT_EQ(client.testTransportRebuilds(), 1u) << "no rebuild inside the gap";
    ASSERT_TRUE(r.drive([&] { return client.testTransportRebuilds() == 2; }, 1'000'000));
    EXPECT_GE(g_now - t0, 100'000'000) << "rebuilt only after the gap";
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live; }, 100'000)) << "the second rebuild resumes";
    // 3: after the Connected edge the gap is reset: a close rebuilds at once, on the same clock reading.
    const int64_t before = g_now;
    client.testInjectTransportFault();
    ASSERT_TRUE(r.drive([&] { return client.testTransportRebuilds() == 3; }));
    EXPECT_EQ(g_now, before) << "a Connected edge resets the gap";
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Live; }, 100'000));
    for (uint64_t n = 4; n <= 5; ++n) {
        r.sendTo(static_cast<int32_t>(n), 2);
        ASSERT_TRUE(faultAndRecover(r, n)) << "close " << n;
    }
    ASSERT_TRUE(r.drive([&] { return r.ledger.records >= 6; }, 100'000));
    EXPECT_EQ(r.ledger.ok, 6);
    EXPECT_FALSE(r.saw(rs::ShardState::Failed));
    EXPECT_EQ(tr.pubSlotCountForTest(), pubSlots) << "publication slots are reused by every rebuild";
    EXPECT_EQ(tr.subSlotCountForTest(), subSlots) << "subscription slots are reused by every rebuild";
#ifdef __linux__
    EXPECT_EQ(openFds(), fds) << "no fd leaks across rebuilds";
#endif
}

// A rebuild that keeps failing is retried on a doubling gap (100 ms, 200 ms, ... capped at 10 s plus jitter): never one
// attempt per duty cycle, and doWork() does no work between attempts.  The shard stays Reconnecting with a growing attempt
// and recovers once rebuilding works.
TEST(ClientShardRegression, RebuildFailureBacksOffWithoutSpinningRegression) {
    auto g = ownDriverGroup(3814, 3815);
    const std::string goodChannel = g.base.gatewayChannel;
    OwnDriverRig r(g);
    r.connect();
    r.shard->client().testSetSeedIngressChannel(kUnreachableSeed);
    r.shard->client().testInjectTransportFault();
    std::vector<int64_t> attemptAt;   // fake clock reading of every failed attempt
    int32_t              seen = 0;
    int                  workSteps = 0;
    for (int i = 0; i < 6000 && attemptAt.size() < 10; ++i) {
        g_now += 10'000'000;   // 10 ms
        const int work = r.shard->doWork();
        r.gw().pump();
        r.shard->pollEvents([&](const rs::ShardStatus& st) { r.events.push_back(st); });
        workSteps += work != 0 ? 1 : 0;
        const int32_t attempt = r.shard->status().attempt;
        if (attempt > seen) {
            attemptAt.push_back(g_now);
            seen = attempt;
        }
    }
    EXPECT_EQ(r.shard->client().testTransportRebuilds(), 0u);
    EXPECT_EQ(r.state(), rs::ShardState::Reconnecting);
    ASSERT_GE(attemptAt.size(), 10u);
    // The paced steps return 0: only the attempts themselves and the Client's own 5 s metadata timer (two steps each) do work.
    EXPECT_LE(workSteps, 2 * static_cast<int>(attemptAt.size()) + 8) << "doWork() returns 0 between attempts";
    const int64_t gap = attemptAt[1] - attemptAt[0];
    EXPECT_GE(gap, 100'000'000) << "the first retry waits at least 100 ms";
    EXPECT_LE(gap, 140'000'000);   // the gap starts on the pass after the failed attempt: one 10 ms step later
    for (std::size_t i = 2; i < attemptAt.size(); ++i) {
        const int64_t expected = std::min<int64_t>(100'000'000LL << (i - 1), 10'000'000'000LL);
        const int64_t actual   = attemptAt[i] - attemptAt[i - 1];
        EXPECT_GE(actual, expected - 10'000'000) << "gap " << i;
        EXPECT_LE(actual, expected + expected / 4 + 30'000'000) << "gap " << i << " (jitter is at most a quarter)";
    }
    EXPECT_GE(attemptAt.back() - attemptAt[attemptAt.size() - 2], 9'990'000'000LL) << "the gap saturates at 10 s";
    r.shard->client().testSetSeedIngressChannel(goodChannel);
    ASSERT_TRUE(r.drive([&] { return r.shard->client().testTransportRebuilds() == 1 && r.state() == rs::ShardState::Live; }, 10'000'000));
}

// A failed attempt that itself takes longer than the gap (a slow DNS lookup) must not eat the gap: it runs from the first
// duty pass after the attempt.  Every duty pass advances the fake clock by 1 s, so the pass that ran an attempt "took" 1 s, longer than any
// gap below the 10 s cap; attempts must still be at most every second pass.
TEST(ClientShardRegression, RebuildGapStartsAfterASlowFailedAttemptRegression) {
    auto g = ownDriverGroup(3818, 3819);
    OwnDriverRig r(g);
    r.connect();
    r.shard->client().testSetSeedIngressChannel(kUnreachableSeed);
    r.shard->client().testInjectTransportFault();
    for (int i = 0; i < 100 && r.shard->client().testRebuildAttempt() == 0; ++i) r.step(1'000'000'000);
    ASSERT_GE(r.shard->client().testRebuildAttempt(), 1u);
    const uint32_t first = r.shard->client().testRebuildAttempt();
    for (int i = 0; i < 6; ++i) r.step(1'000'000'000);
    EXPECT_EQ(r.shard->client().testRebuildAttempt() - first, 3u) << "a 1 s attempt does not consume the gap that follows it";
}

// The seed moved to another broker channel (a completed failover): the rebuild keeps it there, and the resume goes to
// that broker, not back to the configured gateway.
TEST(ClientShardRegression, RebuildKeepsTheFailedOverSeedChannelRegression) {
    auto g = ownDriverGroup(3824, 3825);
    const std::string ch2 = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::client::testing::allocFreeUdpPort());
    OwnDriverRig r(g);
    const int ep2 = r.gw().addEndpoint(ch2, g.base.gatewayStreamId);
    r.connect();
    ASSERT_TRUE(r.shard->client().testRetargetSeed(ch2));
    ASSERT_TRUE(r.drive([&] { return r.gw().connectsAt[ep2] >= 1 && r.state() == rs::ShardState::Live; }, 100'000));
    const int connectsAt2 = r.gw().connectsAt[ep2];
    const int connectsAt1 = r.gw().connectsAt[0];
    ASSERT_TRUE(faultAndRecover(r, 1));
    EXPECT_EQ(r.shard->client().testSeedChannel(), ch2);
    EXPECT_GT(r.gw().connectsAt[ep2], connectsAt2) << "the resume reached the failed-over broker";
    EXPECT_EQ(r.gw().connectsAt[0], connectsAt1) << "and not the configured gateway";
}

// A seed Rejected stays terminal even when a transport fault follows: no rebuild, no I/O.
TEST(ClientShardRegression, SeedRejectedStaysTerminalEvenAfterTransportFaultRegression) {
    OwnDriverRig r(ownDriverGroup(3816, 3817));
    r.connect();
    rejectSeed(r.gw());
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Failed; }, 100'000));
    EXPECT_EQ(r.shard->status().failure, rs::ShardFailure::Rejected);
    const int frames = r.gw().framesReceived;
    r.shard->client().testInjectTransportFault();
    for (int i = 0; i < 200; ++i) r.step(1'000'000);
    EXPECT_EQ(r.shard->client().testTransportRebuilds(), 0u) << "a Rejected seed never rebuilds";
    EXPECT_EQ(r.state(), rs::ShardState::Failed);
    EXPECT_EQ(r.shard->status().failure, rs::ShardFailure::Rejected);
    EXPECT_EQ(r.gw().framesReceived, frames) << "a failed shard does no I/O";
}

// Closing while the transport is Down and every rebuild fails is bounded, and every accepted record completes exactly
// once: closeImmediately(), and close with a timeout.  A Thread-runner client closes promptly around a fault too.
void closeDuringTransportDown(bool graceful) {
    OwnDriverRig r(ownDriverGroup(graceful ? 3818 : 3832, graceful ? 3819 : 3833));
    r.connect();
    r.gw().holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    r.sendTo(1, 3);
    r.accept(2, 2);
    ASSERT_TRUE(faultIntoDown(r));
    EXPECT_EQ(r.shard->client().testTransportRebuilds(), 0u);
    const auto t0 = std::chrono::steady_clock::now();
    if (graceful) {
        EXPECT_ANY_THROW((void)r.shard->closeAndWait(300ms)) << "nothing can drain while Down: the deadline expires";
    }
    r.shard->closeImmediately();
    r.shard->pollBatchAcks([&](const rc::BatchAck& a) { r.ledger(a); });
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s);
    EXPECT_EQ(r.ledger.records, 5) << "each accepted record completes exactly once";
    EXPECT_EQ(r.ledger.failed, 5);
    EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
}

TEST(ClientShardRegression, CloseImmediatelyDuringTransportDownIsBoundedExactlyOnceRegression) { closeDuringTransportDown(false); }

TEST(ClientShardRegression, CloseAndWaitDuringTransportDownIsBoundedExactlyOnceRegression) { closeDuringTransportDown(true); }

// Thread runner: close() from the test thread joins promptly right after a fault.
TEST(ClientShardRegression, ThreadRunnerCloseRightAfterTransportFaultJoinsPromptlyRegression) {
    auto       g    = group(3834, 3835, 1, rs::ShardMode::Agent);
    const auto port = rune::client::testing::allocFreeUdpPortRange(2);
    g.base.gatewayChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port);
    g.base.egressChannel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port + 1);
    g.base.runnerMode     = rc::RunnerMode::Thread;
    g.base.shardStatus    = true;
    test_support::BrokerHost broker(test_support::BrokerHost::uniqueDir(), g.base.gatewayChannel, g.base.gatewayStreamId,
                                    g.base.egressChannel, g.base.egressStreamId);
    rc::Client      client(g.base);
    rc::Completions cq(client);
    bool            connected = false;
    const auto      pumpUntil = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (broker.gw.pump() == 0) std::this_thread::yield();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(pumpUntil([&] { return broker.gw.ready(); }));
    ASSERT_EQ(client.connect(cq, 1), rc::OpResult::Accepted);
    ASSERT_TRUE(pumpUntil([&] {
        cq.poll([&](const rc::Completion&, std::span<const uint8_t>) { connected = true; });
        return connected;
    }));
    client.testSetSeedIngressChannel(kUnreachableSeed);   // posted to the runner: every rebuild fails from here on
    client.testInjectTransportFault();
    int32_t seen = 0;   // two failed attempts have run: the runner is now in a gap of at least 200 ms
    ASSERT_TRUE(pumpUntil([&] {
        client.pollShardStatus([&](const rs::ShardStatus& s) {
            if (s.state == rs::ShardState::Reconnecting) seen = s.attempt;
        });
        return seen >= 2;
    }));
    client.close();
    // driveRebuild_ runs before the ring drain in doWork: a close() that waited for the gap would see attempt seen + 1 run first.
    EXPECT_EQ(client.testRebuildAttempt(), static_cast<uint32_t>(seen)) << "close() ran no further rebuild attempt";
    EXPECT_EQ(client.testTransportRebuilds(), 0u) << "no rebuild succeeded while the seed was unreachable";
}

// A Thread-runner client (its own duty thread and driver thread, real clock) rebuilds after the transport fault too: it
// resumes and the records accepted around the fault are acked exactly once.  The broker runs on its own driver, pumped
// from this thread.  Progress is observed through the completion queue only; duty-thread state is read after close().
TEST(ClientShardRegression, ThreadModeClientRebuildsAfterTransportFaultRegression) {
    auto            g    = group(3820, 3821, 1, rs::ShardMode::Agent);
    const auto      port = rune::client::testing::allocFreeUdpPortRange(2);
    g.base.gatewayChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port);
    g.base.egressChannel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port + 1);
    g.base.runnerMode     = rc::RunnerMode::Thread;
    g.base.connectTimeoutMs = 300;   // the first resume is offered before the new UDP link is warm: retried at this timeout
    test_support::BrokerHost broker(test_support::BrokerHost::uniqueDir(), g.base.gatewayChannel, g.base.gatewayStreamId,
                                    g.base.egressChannel, g.base.egressStreamId);
    rc::Client client(g.base);
    auto       producer = client.producer(g.producer);
    rc::Completions cq(client);
    constexpr rc::Token kConnect = 1, kBeforeFault = 10, kAfterFault = 20;
    int                 acked = 0, notOk = 0;
    auto pumpUntil = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (broker.gw.pump() == 0) std::this_thread::yield();
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == kConnect) return;
                ++acked;
                if (c.status != rc::OpStatus::Ok) ++notOk;
            });
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(pumpUntil([&] { return broker.gw.ready(); }));
    ASSERT_EQ(client.connect(cq, kConnect), rc::OpResult::Accepted);
    const auto topic = client.topic("t");
    const auto send  = [&](rc::Token first, int partition, int n) {
        for (int i = 0; i < n; ++i) {
            rc::SendResult r = rc::SendResult::BackPressured;
            while (r == rc::SendResult::BackPressured) r = producer.trySend(topic, keyFor(partition), std::span<const uint8_t>(kVal), cq, first + i);
            ASSERT_EQ(r, rc::SendResult::Ok);
        }
    };
    send(kBeforeFault, 1, 5);
    ASSERT_TRUE(pumpUntil([&] { return acked == 5; }));
    EXPECT_TRUE(client.testTransport().usesDriverThread()) << "Thread mode: the transport's driver runs on its own thread";
    client.testInjectTransportFault();
    send(kAfterFault, 2, 5);
    ASSERT_TRUE(pumpUntil([&] { return acked == 10; })) << "records accepted around the fault are acked after the rebuild";
    EXPECT_EQ(notOk, 0);
    producer.closeAndWait(5s);
    client.close();
    EXPECT_EQ(client.testTransportRebuilds(), 1u);
    EXPECT_EQ(client.testTransport().callerPumpViolations(), 0);
}

#ifdef __linux__
// The Aeron client's sockets are new after a rebuild: no fds are offered while the transport is Down, and the park-until-work
// fds re-queried afterwards are open and wake on an ack.
TEST(ClientShardRegression, WakeFdsAreReplacedAfterRebuildRegression) {
    auto              g = ownDriverGroup(3822, 3823);
    const std::string goodChannel = g.base.gatewayChannel;
    OwnDriverRig r(g);
    r.connect();
    int before[8], after[8];
    const int nb = r.shard->wakeFds(before, 8);
    ASSERT_GT(nb, 0);
    ASSERT_TRUE(faultIntoDown(r));
    EXPECT_EQ(r.shard->wakeFds(after, 8), 0) << "no fds to park on while the transport is Down";
    r.shard->client().testSetSeedIngressChannel(goodChannel);
    ASSERT_TRUE(r.drive([&] { return r.shard->client().testTransportRebuilds() == 1 && r.state() == rs::ShardState::Live; }, 100'000));
    const int na = r.shard->wakeFds(after, 8);
    ASSERT_GT(na, 0);
    // The old fds were closed with the old driver; whatever the kernel reused, every fd now returned is open.
    for (int i = 0; i < na; ++i) EXPECT_NE(::fcntl(after[i], F_GETFD), -1) << after[i];
    // The rebuilt client's egress receive socket (the only one bound to the egress endpoint) is registered in one of
    // the re-queried epoll fds, so an ack on it wakes the park: /proc/self/fdinfo/<epfd> lists each target as "tfd: <fd>".
    const std::string egress = rs::shardClientConfig(g, 0).egressChannel;
    const int egressPort = std::stoi(egress.substr(egress.rfind(':') + 1));
    int egressFd = -1;
    for (const auto& e : std::filesystem::directory_iterator("/proc/self/fd")) {
        const int fd = std::stoi(e.path().filename().string());
        sockaddr_in a{};
        socklen_t   len = sizeof a;
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len) == 0 && a.sin_family == AF_INET && ntohs(a.sin_port) == egressPort) egressFd = fd;
    }
    ASSERT_GE(egressFd, 0) << "no socket bound to the egress endpoint";
    bool registered = false;
    for (int i = 0; i < na; ++i) {
        std::ifstream info("/proc/self/fdinfo/" + std::to_string(after[i]));
        for (std::string line; std::getline(info, line);) registered = registered || (line.rfind("tfd:", 0) == 0 && std::stoi(line.substr(4)) == egressFd);
    }
    EXPECT_TRUE(registered) << "the egress socket " << egressFd << " is in none of the re-queried wake fds";
    // An owner that parks on the re-queried fds between duty cycles gets its ack.
    r.accept(1, 1);
    r.step();   // the record goes out and the broker acks it after this duty cycle: the ack is not read yet
    ASSERT_EQ(r.ledger.ok, 0);
    for (int parks = 0; r.ledger.ok == 0 && parks < 20; ++parks) {
        r.gw().pump();
        pollfd fds[8];
        const int n = r.shard->wakeFds(after, 8);
        for (int i = 0; i < n; ++i) fds[i] = pollfd{after[i], POLLIN, 0};
        ASSERT_GT(::poll(fds, static_cast<nfds_t>(n), 1000), 0) << "nothing woke a park on the re-queried fds";
        r.step();
    }
    EXPECT_EQ(r.ledger.ok, 1) << "the ack arrived through the parked owner loop";
}
#endif

// ---- close -------------------------------------------------------------------------

TEST(ClientShardRegression, BlockingCloseAndCloseAndWaitAreEquivalent) {
    for (const bool withTimeout : {false, true}) {
        SCOPED_TRACE(withTimeout ? "closeAndWait" : "close");
        ShardRig r(group(withTimeout ? 3846 : 3848, withTimeout ? 3847 : 3849, 1, rs::ShardMode::Invoker), 0);
        r.connect();
        const auto topic = r.shard->topic("t");
        for (uint64_t i = 0; i < 20; ++i) ASSERT_EQ(r.shard->trySend(topic, rc::Key(i), kVal), rc::SendResult::Ok);
        PumpGwDuring       pump(r);
        const rc::OpStatus st = withTimeout ? r.shard->closeAndWait(10s) : r.shard->close();
        EXPECT_EQ(st, rc::OpStatus::Ok);
        r.shard->pollBatchAcks([&](const rc::BatchAck& a) { r.ledger(a); });
        EXPECT_EQ(r.ledger.ok, 20) << "close drains: every accepted record acked before Ok";
        EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
        EXPECT_EQ(r.shard->trySend(topic, rc::Key(uint64_t{1}), kVal), rc::SendResult::Closed);
    }
}

TEST(ClientShardRegression, DoubleCloseIsNoOp) {
    ShardRig r(group(3850, 3851, 1, rs::ShardMode::Invoker), 0);
    r.connect();
    PumpGwDuring pump(r);
    EXPECT_EQ(r.shard->close(), rc::OpStatus::Ok);
    EXPECT_EQ(r.shard->close(), rc::OpStatus::Closed) << "second close: no drain, no hang";
    r.shard->closeImmediately();
    r.shard->closeImmediately();
    int closedEvents = 0;
    r.shard->pollEvents([&](const rs::ShardStatus& s) { closedEvents += s.state == rs::ShardState::Closed; });
    EXPECT_EQ(closedEvents, 1) << "Closed is published once";
}

TEST(ClientShardRegression, CloseOnFailedIsImmediate) {
    ShardRig r(invokerGroup(3852, 3853), 0);
    r.connect();
    r.gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    r.sendTo(1, 3);
    rejectSeed(r.gw);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Failed; }));
    // The clock is frozen: a close that waited for a drain would time out here.
    EXPECT_EQ(r.shard->closeAndWait(5s), rc::OpStatus::Closed);
    EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
    r.shard->pollBatchAcks([&](const rc::BatchAck& a) { r.ledger(a); });
    EXPECT_EQ(r.ledger.records, 3);
    EXPECT_EQ(r.ledger.failed, 3);
}

TEST(ClientShardRegression, CloseWhileConnectingDrainsOrTimesOut) {
    auto g                       = invokerGroup(3854, 3855);
    g.producer.deliveryTimeoutMs = 1000;
    ShardRig r(g, 0);
    r.gw.replyConnects = false;
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    HandlerCapture connect;
    ASSERT_EQ(r.shard->connect(connect.handler(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Connecting; }));
    for (int i = 0; i < 3; ++i) ASSERT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(i), kVal), rc::SendResult::Ok);
    HandlerCapture beginClose;
    ASSERT_EQ(r.shard->close(beginClose.handler(), 2), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Draining; }));
    EXPECT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(0), kVal), rc::SendResult::Closed)
        << "begin-close closes the intake synchronously";
    ASSERT_TRUE(r.drive([&] { return beginClose.calls > 0; }, 10'000'000));
    EXPECT_EQ(beginClose.status, rc::OpStatus::Timeout);
    EXPECT_EQ(r.ledger.failed, 3) << "the remaining records still complete, each once";
    ASSERT_TRUE(r.drive([&] { return connect.calls > 0; }, 10'000'000));
    EXPECT_EQ(connect.calls, 1);
    r.shard->closeImmediately();
    EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
}

TEST(ClientShardRegression, CloseImmediatelyWhileConnectingCompletesConnectClosed) {
    ShardRig r(invokerGroup(3856, 3857), 0);
    r.gw.replyConnects = false;
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    HandlerCapture connect;
    ASSERT_EQ(r.shard->connect(connect.handler(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Connecting; }));
    r.shard->closeImmediately();
    EXPECT_EQ(connect.calls, 1);
    EXPECT_EQ(connect.status, rc::OpStatus::Closed);
    EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
}

// closeImmediately from every running state (Live, Degraded, Reconnecting,
// Failed): each accepted record completes exactly once (Closed, or the outcome
// it already had).
TEST(ClientShardRegression, CloseImmediatelyFromLiveDegradedReconnectingFailed) {
    for (const rs::ShardState from : {rs::ShardState::Live, rs::ShardState::Degraded, rs::ShardState::Reconnecting,
                                      rs::ShardState::Failed}) {
        SCOPED_TRACE(rs::toString(from));
        ShardRig r(invokerGroup(3858, 3859), 0);
        if (from == rs::ShardState::Degraded) {
            addLeader(r.gw, 24852, 3858, 3866);
            r.gw.silentEndpoints = {1};  // the leader node is down: records park
        }
        r.connect();
        r.gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
        if (from == rs::ShardState::Degraded) {
            for (int32_t p = 0; p < 4; ++p) {
                for (int i = 0; i < 2; ++i) ASSERT_EQ(r.shard->trySend(r.shard->topic("t"), keyFor(p), kVal), rc::SendResult::Ok);
            }
        } else {
            for (int32_t p = 0; p < 4; ++p) r.sendTo(p, 2);
        }
        if (from == rs::ShardState::Reconnecting) {
            r.gw.replyConnects = false;
            r.gw.expireSession(r.gw.assignedSessionId);
        } else if (from == rs::ShardState::Failed) {
            rejectSeed(r.gw);
        }
        ASSERT_TRUE(r.drive([&] { return r.state() == from; }, from == rs::ShardState::Degraded ? 1'000'000 : 0));
        r.shard->closeImmediately();
        r.shard->pollBatchAcks([&](const rc::BatchAck& a) { r.ledger(a); });
        EXPECT_EQ(r.ledger.records, 8) << "every accepted record completed exactly once";
        EXPECT_EQ(r.ledger.ok, 0);
        EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
        r.shard.reset();
    }
}

TEST(ClientShardRegression, CloseThenCloseImmediatelyAbortsDrainExactlyOnce) {
    ShardRig r(invokerGroup(3860, 3861), 0);
    r.connect();
    r.gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    for (int32_t p = 0; p < 3; ++p) r.sendTo(p, 2);
    HandlerCapture beginClose;
    ASSERT_EQ(r.shard->close(beginClose.handler(), 5), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Draining; }));
    r.shard->closeImmediately();
    EXPECT_EQ(beginClose.calls, 1);
    EXPECT_EQ(beginClose.status, rc::OpStatus::Closed);
    r.shard->pollBatchAcks([&](const rc::BatchAck& a) { r.ledger(a); });
    EXPECT_EQ(r.ledger.records, 6);
    EXPECT_EQ(r.ledger.failed, 6);
}

TEST(ClientShardRegression, DtorIsCloseImmediately) {
    ShardRig r(invokerGroup(3862, 3863), 0);
    r.gw.replyConnects = false;
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    HandlerCapture connect;
    ASSERT_EQ(r.shard->connect(connect.handler(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Connecting; }));
    r.shard.reset();  // no close(): the destructor never blocks
    EXPECT_EQ(connect.calls, 1);
    EXPECT_EQ(connect.status, rc::OpStatus::Closed);
}
