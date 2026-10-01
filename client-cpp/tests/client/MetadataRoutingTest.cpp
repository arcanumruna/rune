// tests/client/MetadataRoutingTest.cpp
//
// MD1 Task 10 review fixes — end-to-end tests against a REAL Aeron gateway that
// speaks the wire protocol (Connect / Metadata / Produce / Redirect).  These
// exercise the client's live egress-frame handling, not bare cache calls.
//
// Coverage:
//   MR1  (IMPORTANT-1): a REAL ErrorResponse{Redirect} frame updates the
//        MetadataCache in place (zero-RTT) so the next route targets the new
//        node — WITHOUT a full MetadataRequest round-trip driving the reroute.
//   MR2  (IMPORTANT-2): the produce path consults the PartitionRouter; a keyed
//        produce resolves to the partition leader's endpoint from cached
//        metadata (route-selection, not the single connected session).
//   MR3  (IMPORTANT-3): on no-progress the client issues a BACKGROUND metadata
//        refresh with exponential backoff (a second MetadataRequest arrives
//        without the app doing anything, and the client does not storm).
//
// PHASE-1 LIMITATION (documented in the task report): the client transport keeps
// a SINGLE session, so IMPORTANT-2 routing is proven at the route-selection
// level (endpoint resolved from cached metadata) plus the retarget/redirect
// machinery to reach it; N simultaneous leader connections are out of scope.

#include "../support/TestCompletions.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/codec/Codec.h>

#include <rune/protocol/client/MetadataMessages.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;
namespace rp  = rune::protocol::client;

static constexpr int  kPumpMax   = 6'000;
static constexpr auto kPumpSleep = 2ms;

namespace {
std::atomic<int> g_ctr{0};
std::string uniqueDir(const char* prefix) {
    const int c = g_ctr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/") + prefix + "_" +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}
} // namespace

// ---------------------------------------------------------------------------
// MetaGateway — real Aeron gateway that answers Connect + Metadata + Produce.
//
// The MetadataResponse advertises two brokers:
//   node 0 → "aeron:ipc"     (this test channel — the initially-connected node)
//   node 1 → the leaderEndpoint string passed to the ctor (a distinct UDP
//            endpoint that nobody actually listens on — MR1 only needs the
//            client to UPDATE ITS CACHE from the redirect frame, not to reach it)
// and partition (topicId=1, partition=0) leader = node 0, epoch = 1.
//
// On the first Produce it replies ErrorResponse{Redirect, leaderEndpoint} so
// the client must (zero-RTT) flip the cached leader for (1,0) to node 1.
// ---------------------------------------------------------------------------
struct MetaGateway {
    std::atomic<int> connectsSeen{0};
    std::atomic<int> metadataReqSeen{0};
    std::atomic<int> producesSeen{0};

    std::string leaderEndpoint;         // node 1's "host:port"
    static constexpr int64_t kSessionId = 55;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;
    uint8_t replyBuf[4096]{};

    // F8c fix round 1 (I-2): invoker mode — the gateway's Aeron client runs in
    // invoker mode and is driven ONLY by pump(), on the test thread, which also
    // drives the RunnerMode::Invoker Client's doWork() (and so its embedded
    // driver).  Registrations resolve inside pump(); replies wait in `outbox`
    // until the publication accepts them.  No sleeps, no other thread.
    bool invoker = false;
    int64_t subReg = -1;
    int64_t pubReg = -1;
    std::vector<std::vector<uint8_t>> outbox;

    struct InvokerTag {};
    MetaGateway(InvokerTag, const std::string& aeronDir, int32_t inStr, int32_t egStr,
                std::string leaderEp)
        : leaderEndpoint(std::move(leaderEp)), invoker(true)
    {
        aeron::Context ctx;
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        ctx.useConductorAgentInvoker(true);
        gwAeron = aeron::Aeron::connect(ctx);
        subReg = gwAeron->addSubscription("aeron:ipc", inStr);
        pubReg = gwAeron->addExclusivePublication("aeron:ipc", egStr);
        makeAssembler();
    }

    /// Invoker mode: one gateway duty cycle (conductor, registrations, outbox, poll).
    int pump() {
        int work = gwAeron->conductorAgentInvoker().invoke();
        if (!gwSub) gwSub = gwAeron->findSubscription(subReg);
        if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubReg);
        while (!outbox.empty() && gwPub) {
            const auto& f = outbox.front();
            aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(f.data()),
                                               static_cast<aeron::util::index_t>(f.size()));
            if (gwPub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0) break;
            outbox.erase(outbox.begin());
            ++work;
        }
        return work + poll(16);
    }

    void makeAssembler() {
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    MetaGateway(const std::string& aeronDir, int32_t inStr, int32_t egStr,
                std::string leaderEp)
        : leaderEndpoint(std::move(leaderEp))
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        const int64_t subId = gwAeron->addSubscription("aeron:ipc", inStr);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egStr);
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(1ms);
        }
        if (!gwSub || !gwPub)
            throw std::runtime_error("MetaGateway: sub/pub timed out");
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }
    ~MetaGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    int poll(int lim = 16) {
        return gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), lim) : 0;
    }

    bool gwSend(const uint8_t* b, std::size_t l) {
        if (invoker) {  // offered by pump(), in order
            outbox.emplace_back(b, b + l);
            return true;
        }
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 500; ++i) {
            if (gwPub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    // Build + send a MetadataResponse advertising the two brokers + one partition.
    void sendMetadataResponse(int64_t sessionId, int64_t correlationId) {
        // rp::BrokerEntry::host is a std::string_view (zero-copy into the source).
        // The backing string MUST outlive msg.encode() below, so keep it in a
        // function-scope local (an inner-block temporary would dangle — the bug this
        // fixes: b1.host previously viewed a destroyed substr → garbage host on the
        // wire → the client's nodeIdForEndpoint never matched the redirect endpoint).
        const auto colon = leaderEndpoint.rfind(':');
        const std::string b1Host = leaderEndpoint.substr(0, colon);
        std::vector<rp::BrokerEntry> brokers;
        {
            rp::BrokerEntry b0; b0.nodeId = 0; b0.host = "aeron:ipc"; b0.port = 0;
            rp::BrokerEntry b1;
            b1.nodeId = 1;
            b1.host   = b1Host;  // view into the function-scope string (valid through encode)
            b1.port   = std::stoi(leaderEndpoint.substr(colon + 1));
            brokers.push_back(std::move(b0));
            brokers.push_back(std::move(b1));
        }
        std::vector<rp::PartitionEntry> parts;
        {
            rp::PartitionEntry p;
            p.topicId = 1; p.partition = 0; p.leaderId = 0; p.leaderEpoch = 1;
            parts.push_back(p);
        }
        rp::MetadataResponse msg;
        msg.sessionId     = sessionId;
        msg.correlationId = correlationId;
        msg.brokers       = std::span<const rp::BrokerEntry>(brokers);
        msg.partitions    = std::span<const rp::PartitionEntry>(parts);
        const std::size_t n = msg.encode(replyBuf);
        if (n > 0) gwSend(replyBuf, n);
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            connectsSeen.fetch_add(1);
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK), 0, kSessionId,
                opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid == rcc::TemplateId::MetadataRequest) {
            const auto opt = rp::MetadataRequest::decode(buf, len);
            if (!opt) return;
            metadataReqSeen.fetch_add(1);
            sendMetadataResponse(opt->sessionId, opt->correlationId);
            return;
        }

        if (tid == rcc::TemplateId::Produce) {
            const auto opt = rcc::decodeProduce(buf, len);
            if (!opt) return;
            producesSeen.fetch_add(1);
            // Only topic 1 (which the MetadataResponse advertises) is redirected
            // to node 1's endpoint (a REAL redirect frame — drives the zero-RTT
            // cache update).  Produces to the never-advertised topic 2 are DROPPED
            // so the client stays Connected and exercises the "unknown leader"
            // background-refresh path (MR3) instead of a redirect.
            if (opt->topicId != 1) return;
            const std::string_view ep = leaderEndpoint;
            const std::size_t n = rcc::encodeErrorResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::Redirect),
                ep, opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }
    }
};

static Client* makeConnectedClient(std::unique_ptr<Client>& holder,
                                   const std::string& agentDir,
                                   int32_t inStr, int32_t egStr,
                                   std::unique_ptr<MetaGateway>& gw,
                                   const std::string& leaderEp)
{
    ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:ipc";
    cfg.gatewayStreamId  = inStr;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = egStr;
    cfg.dataDir          = agentDir;
    cfg.connectTimeoutMs = 8000;
    cfg.idleMode         = IdleMode::LowLatency;

    TopicSpec ts; ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);
    // A second topic the gateway's MetadataResponse never advertises — used by
    // MR3 to drive the "unknown leader" no-progress background-refresh path.
    TopicSpec ts2; ts2.name = "t2"; ts2.topicId = 2; ts2.partitionCount = 1;
    cfg.topics.push_back(ts2);

    holder = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);
    gw = std::make_unique<MetaGateway>(agentDir + "/aeron", inStr, egStr, leaderEp);

    test_support::TestCq connFutCq(*holder);

    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return holder->connect(q, t); });
    for (int i = 0; i < kPumpMax && !connFut.ready(); ++i) {
        gw->poll(16);
        std::this_thread::sleep_for(kPumpSleep);
    }
    connFut.check();
    return holder.get();
}

// F8c fix round 1 (I-2): MR1/MR2 run the Client in RunnerMode::Invoker.  The
// test thread is the owner: every step is `client->doWork(); gw->pump();`, and
// the duty-owned metadata cache is read through the test hooks only BETWEEN
// those pumps — on the duty thread, race-free by construction, deterministic,
// no sleeps.  (They used to read it from the test thread while a Thread-mode
// runner wrote it: a TSan data race.)
struct InvokerRig {
    std::unique_ptr<Client>      client;  // constructed first: owns the driver
    std::unique_ptr<MetaGateway> gw;      // destroyed first
    template <class P>
    bool drive(P&& pred, int cap = 400'000) {
        for (int i = 0; i < cap; ++i) {
            client->doWork();
            gw->pump();
            if (pred()) return true;
        }
        return false;
    }
    ~InvokerRig() { gw.reset(); client.reset(); }
};

static void makeInvokerConnectedClient(InvokerRig& rig, const std::string& agentDir,
                                       int32_t inStr, int32_t egStr, const std::string& leaderEp) {
    ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:ipc";
    cfg.gatewayStreamId  = inStr;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = egStr;
    cfg.dataDir          = agentDir;
    cfg.connectTimeoutMs = 8000;
    cfg.runnerMode       = RunnerMode::Invoker;
    TopicSpec ts; ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);
    rig.client = std::make_unique<Client>(cfg);
    rig.gw = std::make_unique<MetaGateway>(MetaGateway::InvokerTag{}, agentDir + "/aeron",
                                           inStr, egStr, leaderEp);
    test_support::TestCq connFutCq(*rig.client);
    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return rig.client->connect(q, t); });
    ASSERT_TRUE(rig.drive([&] { return connFut.ready(); }))
        << "invoker client never connected";
    connFut.check();
}

// ===========================================================================
// MR1 (IMPORTANT-1): REAL redirect frame → zero-RTT cache update, no full
// metadata round-trip needed for the reroute.
// ===========================================================================
TEST(MetadataRouting, RealRedirectFrameUpdatesCacheZeroRtt) {
    static constexpr int32_t kInStr = 4500, kEgStr = 4501;
    const std::string agentDir = uniqueDir("md_mr1");
    // node1 endpoint the redirect will point at (nobody listens — we only assert
    // the cache flip, which happens on the redirect frame BEFORE any reconnect).
    const std::string leaderEp = "127.0.0.1:24591";

    InvokerRig rig;
    makeInvokerConnectedClient(rig, agentDir, kInStr, kEgStr, leaderEp);
    if (HasFatalFailure()) return;
    Client* client = rig.client.get();
    MetaGateway* gw = rig.gw.get();

    // Drive until the on-connect MetadataResponse populates the cache: leader
    // for (1,0) must become node 0 (epoch 1).
    ASSERT_TRUE(rig.drive([&] {
        return client->testLeaderFor(1, 0) == 0 && client->testEpochFor(1, 0) == 1;
    })) << "on-connect MetadataResponse must populate cache (leader node 0, epoch 1)";
    ASSERT_EQ(gw->metadataReqSeen.load(), 1)
        << "exactly one MetadataRequest so far (the on-connect one)";

    // Produce a keyed record — the gateway replies with a REAL Redirect frame to
    // node 1's endpoint.  The client must flip the cached leader for (1,0) to
    // node 1 with a HIGHER epoch, driven by the redirect frame alone.
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 20000;
    auto p  = client->producer(pcfg);
    auto th = client->topic("t");
    Key key(uint64_t{7});
    const std::vector<uint8_t> val = {0x01};
    auto fut = p.send(th, key, std::span<const uint8_t>(val), want_future);

    // Drive until the redirect is processed and the cache flips to node 1.
    int metaReqAtFlip = -1;
    const bool flipped = rig.drive([&] {
        if (client->testLeaderFor(1, 0) != 1) return false;
        metaReqAtFlip = gw->metadataReqSeen.load();
        return true;
    });

    ASSERT_TRUE(flipped)
        << "cache leader for (1,0) must flip to node 1 after the redirect frame";
    EXPECT_GT(client->testEpochFor(1, 0), 1)
        << "zero-RTT update must bump the epoch above the metadata epoch";

    // KEY zero-RTT assertion: the reroute happened from the redirect FRAME, not
    // from a full metadata round-trip — no NEW MetadataRequest was required for
    // the flip (still 1, the on-connect one, at the moment of the flip).
    EXPECT_EQ(metaReqAtFlip, 1)
        << "reroute must NOT require a full metadata refresh round-trip";
    EXPECT_GE(gw->producesSeen.load(), 1) << "gateway must have seen the produce";

    // Best-effort: fail/close (node 1 endpoint is dead so the produce won't ack).
    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// MR2 (IMPORTANT-2): the produce path consults the PartitionRouter — a keyed
// produce resolves to the partition leader's endpoint from cached metadata.
// ===========================================================================
TEST(MetadataRouting, KeyedProduceResolvesPartitionLeaderEndpoint) {
    static constexpr int32_t kInStr = 4510, kEgStr = 4511;
    const std::string agentDir = uniqueDir("md_mr2");
    const std::string leaderEp = "127.0.0.1:24601";

    InvokerRig rig;
    makeInvokerConnectedClient(rig, agentDir, kInStr, kEgStr, leaderEp);
    if (HasFatalFailure()) return;
    Client* client = rig.client.get();

    // Drive until the cache is populated by the on-connect MetadataResponse.
    ASSERT_TRUE(rig.drive([&] { return client->testLeaderFor(1, 0) == 0; }));

    // The produce path uses PartitionRouter::routeKey(topic,key) to resolve the
    // target.  For key→partition 0 (single partition), the leader is node 0 whose
    // cached endpoint is "aeron:ipc:0" (host "aeron:ipc" + port 0 from the
    // MetadataResponse) — i.e. routing resolves to the PARTITION LEADER's
    // endpoint from cached metadata, not merely "wherever the session points".
    Key key0(uint64_t{7});
    EXPECT_EQ(client->testRouteEndpoint(1, key0), std::string("aeron:ipc:0"))
        << "keyed produce must resolve to partition 0's leader (node 0)";

    // After a REAL redirect flips the cached leader to node 1, the SAME keyed
    // produce must now resolve to node 1's endpoint — proving the route follows
    // the cached partition leader, not a fixed connection.
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 20000;
    auto p  = client->producer(pcfg);
    auto th = client->topic("t");
    const std::vector<uint8_t> val = {0x02};
    auto fut = p.send(th, key0, std::span<const uint8_t>(val), want_future);

    EXPECT_TRUE(rig.drive([&] { return client->testRouteEndpoint(1, key0) == leaderEp; }))
        << "after redirect, keyed produce must route to the new partition leader's endpoint";

    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// MR3 (IMPORTANT-3): background metadata refresh with exponential backoff fires
// on no-progress WITHOUT app action, and does not storm.
//
// A keyed produce to topic "t2" — whose partition the gateway's MetadataResponse
// never advertises — hits the "unknown leader" no-progress path.  The client
// stays Connected, arms a background metadata refresh (backoff), and fires at
// least one ADDITIONAL MetadataRequest beyond the on-connect one, without the
// app doing anything — while exponential backoff bounds the request rate.
// ===========================================================================
TEST(MetadataRouting, BackgroundRefreshWithBackoffOnNoProgress) {
    static constexpr int32_t kInStr = 4520, kEgStr = 4521;
    const std::string agentDir = uniqueDir("md_mr3");
    const std::string leaderEp = "127.0.0.1:24611";

    std::unique_ptr<Client> client;
    std::unique_ptr<MetaGateway> gw;
    makeConnectedClient(client, agentDir, kInStr, kEgStr, gw, leaderEp);

    // Wait for the on-connect MetadataResponse (baseline = 1 request).
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (gw->metadataReqSeen.load() >= 1) break;
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_EQ(gw->metadataReqSeen.load(), 1);

    // Produce to "t2" (topicId 2) — its partition leader is UNKNOWN in the cache
    // (never advertised).  routeProduceToLeader() detects no-progress and arms a
    // backoff refresh WHILE STAYING CONNECTED; tickMetadataRefresh() (sweep)
    // fires it once the 100 ms window elapses.
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 20000;
    auto p  = client->producer(pcfg);
    auto th = client->topic("t2");
    ASSERT_TRUE(th.valid());
    Key key(uint64_t{9});
    const std::vector<uint8_t> val = {0x03};
    (void)p.send(th, key, std::span<const uint8_t>(val), want_future);

    // Pump up to ~4 s; a background refresh (2nd MetadataRequest) must appear.
    bool refreshed = false;
    const auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < 4s) {
        gw->poll(16);
        if (gw->metadataReqSeen.load() >= 2) { refreshed = true; break; }
        std::this_thread::sleep_for(kPumpSleep);
    }
    EXPECT_TRUE(refreshed)
        << "a background metadata refresh (2nd MetadataRequest) must fire on no-progress";

    // No-storm: within a short window the client must NOT have emitted a large
    // burst of MetadataRequests (exponential backoff caps the rate).  Over ~4 s
    // with 100ms→200ms→…backoff a handful is expected, never dozens.
    EXPECT_LT(gw->metadataReqSeen.load(), 30)
        << "exponential backoff must prevent a metadata-request storm";

    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// F8c fix round 1 (I-2): the duty-owned test hooks enforce their precondition.
// Reading them from the test thread while a Thread-mode runner is live (what
// MR1/MR2 used to do — a TSan-only data race) aborts deterministically in debug
// builds; after close() (runner joined) the same read is allowed.
// ===========================================================================
TEST(MetadataRoutingDeathTest, TestHookOffDutyThreadAbortsRegression) {
#ifdef NDEBUG
    GTEST_SKIP() << "the hook owner check is a debug-build assertion";
#else
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::string agentDir = uniqueDir("md_hook");
    ClientConfig cfg;
    cfg.gatewayChannel  = "aeron:ipc";
    cfg.gatewayStreamId = 4530;
    cfg.egressChannel   = "aeron:ipc";
    cfg.egressStreamId  = 4531;
    cfg.dataDir         = agentDir;
    EXPECT_DEATH(
        {
            Client c(cfg);                    // Thread mode: the runner is live
            (void)c.testLeaderFor(1, 0);      // off the duty thread
        },
        "off the duty thread");
    {
        Client c(cfg);
        c.close();                            // runner joined
        EXPECT_EQ(c.testLeaderFor(1, 0), c.testLeaderFor(1, 0)) << "allowed after close()";
    }
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
#endif
}
