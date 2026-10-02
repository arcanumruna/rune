// tests/shard/ClientShardTest.cpp — T15b: ClientShard in Invoker mode, single-
// threaded against the InvokerGateway (the test thread is the owner: it drives
// shard.doWork() and gw.pump()), with an injected duty clock.
//
// Rules (F8c): no std::atomic / mutex / future in tests, no sleeps.  Drive loops
// wait on progress with a 10 s failure deadline.  The ONLY real-time wait is the
// stalled-owner test, which yields on Aeron's own conductor clock until
// ClientConductor.h:436's condition holds (Aeron's clock cannot be injected).
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"

#include <rune/client/shard/ClientShard.h>

#include <Aeron.h>  // aeron::currentTimeMillis — the conductor's own clock (stall test)
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
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
    g.base.dataDir = "/tmp/shard_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
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

// ---- failure path ------------------------------------------------------------------

// Review finding 2: the Aeron client closed (fault seam) -> Failed{TransportClosed};
// every record accepted before the failure completes Closed exactly once; later
// sends are Closed at once.
TEST(ClientShardRegression, TransportFaultFailsShardCompletesAllClosedOnce) {
    ShardRig r(invokerGroup(3840, 3841), 0);
    r.connect();
    r.gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    for (int32_t p = 0; p < 8; ++p) r.sendTo(p, 3);
    const auto topic = r.shard->topic("t");
    // Accepted but not yet sent (no doWork between them and the fault).
    for (int i = 0; i < 4; ++i) ASSERT_EQ(r.shard->trySend(topic, keyFor(i), kVal), rc::SendResult::Ok);
    r.shard->client().testInjectTransportFault();
    ASSERT_TRUE(r.drive([&] { return r.shard->closed() && r.ledger.records >= 28; }));
    for (int i = 0; i < 100; ++i) r.step(1'000'000);
    EXPECT_EQ(r.ledger.records, 28) << "every accepted record completes exactly once";
    EXPECT_EQ(r.ledger.failed, 28);
    EXPECT_EQ(r.state(), rs::ShardState::Failed);
    EXPECT_EQ(r.shard->status().failure, rs::ShardFailure::TransportClosed);
    EXPECT_EQ(r.shard->trySend(topic, keyFor(0), kVal), rc::SendResult::Closed);
    EXPECT_THROW(r.shard->send(topic, keyFor(0), kVal), std::runtime_error);
}

// M2: a failed shard does no I/O — no keepalives, no metadata, nothing — while
// the owner keeps driving it.
TEST(ClientShardRegression, FailedShardDoesNoIo) {
    ShardRig r(invokerGroup(3842, 3843), 0);
    r.connect();
    for (int i = 0; i < 50; ++i) r.step(100'000'000);  // 5 s: keepalives flow (1 s cadence)
    const int liveFrames = r.gw.framesReceived;
    ASSERT_GT(liveFrames, 0);
    r.shard->client().testInjectTransportFault();
    ASSERT_TRUE(r.drive([&] { return r.state() == rs::ShardState::Failed; }));
    const int atFailure = r.gw.framesReceived;
    for (int i = 0; i < 100; ++i) r.step(100'000'000);  // 10 s of duty time
    EXPECT_EQ(r.gw.framesReceived, atFailure) << "a failed shard sends nothing";
}

TEST(ClientShardRegression, FailedThenClosedBothPublishedOnStatusRing) {
    ShardRig r(invokerGroup(3844, 3845), 0);
    r.connect();
    r.shard->client().testInjectTransportFault();
    ASSERT_TRUE(r.drive([&] { return r.saw(rs::ShardState::Failed); }));
    r.shard->closeImmediately();
    r.shard->pollEvents([&](const rs::ShardStatus& s) { r.events.push_back(s); });
    ASSERT_GE(r.events.size(), 2u);
    EXPECT_EQ(r.events[r.events.size() - 2].state, rs::ShardState::Failed);
    EXPECT_EQ(r.events.back().state, rs::ShardState::Closed);
    EXPECT_EQ(r.shard->status().state, rs::ShardState::Closed);
}

// Review Focus 1: an owner stall past the Aeron client liveness timeout turns
// sends into Closed — no ::exit, no hang — and the records accepted before the
// stall complete Closed exactly once.  Aeron's conductor clock cannot be
// injected (Aeron.cpp:43 hard-wires currentTimeMillis), so the test yields on
// that clock until ClientConductor.h:436's condition holds — no tuned sleep.
TEST(ClientShardRegression, StalledOwnerTurnsSendsIntoClosed) {
    auto g = group(3806, 3807, 1, rs::ShardMode::Invoker);
    // T15a I4: shards take the liveness from the group (default 1 h); this is
    // an explicit override to the smallest value validate() accepts (1 s = 2x
    // the Aeron client's 500 ms keepalive; Task 6's stall test uses the same).
    g.aeronClientLivenessTimeoutNs = rune::client::transport::kMinClientLivenessTimeoutNs;
    ShardRig r(g, 0);
    r.connect();
    r.gw.holdPartitions = {0, 1, 2, 3, 4, 5, 6, 7};
    r.sendTo(2, 5);
    // Stall exactly past Aeron's own condition (same clock, same comparison as
    // ClientConductor.h:436: nowMs > timeOfLastDoWorkMs + interServiceTimeoutMs);
    // t0 is taken after the last doWork.  No tuned sleep.
    const long long livenessMs = g.aeronClientLivenessTimeoutNs / 1'000'000;
    const long long t0         = aeron::currentTimeMillis();
    while (aeron::currentTimeMillis() <= t0 + livenessMs) std::this_thread::yield();
    ASSERT_TRUE(r.drive([&] { return r.shard->closed() && r.ledger.records >= 5; }));
    EXPECT_EQ(r.ledger.records, 5);
    EXPECT_EQ(r.ledger.failed, 5);
    EXPECT_EQ(r.shard->trySend(r.shard->topic("t"), rc::Key(uint64_t{1}), kVal), rc::SendResult::Closed);
    EXPECT_THROW(r.shard->send(r.shard->topic("t"), rc::Key(uint64_t{1}), kVal), std::runtime_error);
}

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
    r.shard->client().testInjectTransportFault();
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
            r.shard->client().testInjectTransportFault();
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
