// NatEgressMatrixRegressionTest.cpp - NAT-safe egress Task 8 (spec 2026-10-26-rune-nat-egress-design, D2..D7): the REAL
// client (rune::client::Client, Thread runner) against REAL in-process BrokerNodes over real Aeron UDP, behind a
// SIMULATED NAT.
//
// The proof idea.  A client behind NAT declares an egress address the broker cannot reach.  In-process that is the
// client test seam ClientConfig::declaredEgressChannelForTest: the client SUBSCRIBES on its real local egress channel
// but DECLARES kUnroutable (TEST-NET-1) in its ConnectRequest.  Whatever the broker sends on the declared path is
// lost, so every ack, Record, Pong, group and DDL answer the client receives proves the response path (the replies
// ride the client's own flow back: gateway response publication, worker response publication).  An "old" client
// keeps a reachable declared channel and gatewayResponseControlPort = 0.
//
// Target rune_nat_egress_matrix_tests (libs/node/CMakeLists.txt, RUN_SERIAL).  Every wait is a bounded deadline loop
// (20 ms poll); the deadline is the failure, never a hang.
#include "client/ClusterClient.h"
#include "client/OneBroker.h"
#include "support/AuthBroker.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/cli/Cli.h>
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/HostDefaults.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/Worker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
namespace rc = rune::client;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

const std::string kUnroutable = "aeron:udp?endpoint=192.0.2.1:40200";   // TEST-NET-1: never reachable

// ---------------------------------------------------------------------------------------------------------------
// Response ports and client configs
// ---------------------------------------------------------------------------------------------------------------

/// 2n collision-free UDP ports: the gateway response-control port and the (single) worker response port per node.
struct RespPorts {
    std::vector<uint16_t> gw, w;
    static RespPorts discover(int n) {
        RespPorts r;
        for (int i = 0; i < n; ++i) {
            r.gw.push_back(rune::aeronutil::testing::allocFreeUdpPort());
            r.w.push_back(rune::aeronutil::testing::allocFreeUdpPort());
        }
        return r;
    }
};

/// Sets, for the node itself (cfg.nodeId 1..n -> index id-1) and for EVERY peer entry (peer id i+1), the response ports.
std::function<void(NodeConfig&)> withResponsePorts(const RespPorts& r) {
    return [r](NodeConfig& cfg) {
        const auto self = static_cast<std::size_t>(cfg.nodeId - 1);
        cfg.gatewayResponseControlPort = r.gw[self];
        cfg.responseControlBasePort    = r.w[self];
        for (std::size_t i = 0; i < cfg.peers.size(); ++i) {
            cfg.peers[i].gatewayResponseControlPort = r.gw[i];
            cfg.peers[i].responseControlBasePort    = r.w[i];
        }
    };
}

/// A NAT'd client: subscribes on its real egress channel, DECLARES an unreachable one, and knows the seed's response port.
rc::ClientConfig natClient(ThreeNodes& c, int id, const RespPorts& r) {
    auto cfg = clientConfigFor(c, id);
    cfg.gatewayResponseControlPort = r.gw[static_cast<std::size_t>(id - 1)];
    cfg.declaredEgressChannelForTest = kUnroutable;
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    return cfg;
}
/// A pre-NAT-egress client: port 0, a reachable declared channel.
rc::ClientConfig oldClient(ThreeNodes& c, int id) {
    auto cfg = clientConfigFor(c, id);
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    return cfg;
}

// ---------------------------------------------------------------------------------------------------------------
// Bounded waits and the produce / consume idioms
// ---------------------------------------------------------------------------------------------------------------

template <class Pred>
bool waitUntil(Pred&& pred, std::chrono::milliseconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline; std::this_thread::sleep_for(20ms))
        if (pred()) return true;
    return pred();
}

rc::TopicSpec defineTopic(rc::Client& admin, const std::string& name, uint32_t parts, int16_t rf) {
    rc::TopicOptions to;
    to.replicationFactor = rf;
    const auto def = admin.defineTopicAndWait(name, parts, "", {}, false, rc::Persistence::Disk, to, {}, 60s);
    EXPECT_EQ(def.errorCode, 0) << name << ": " << def.detail;
    return rc::TopicSpec{name, def.topicId, static_cast<int32_t>(parts)};
}

rc::TopicHandle handleOf(const rc::TopicSpec& s) { return rc::TopicHandle{s.topicId, s.partitionCount}; }

rc::ProducerConfig producerConfig(rc::Acks acks) {
    rc::ProducerConfig p;
    p.acks              = acks;
    p.lingerUs          = 1;
    p.batchBytes        = 1;
    p.maxInFlight       = 16;
    p.produceTimeoutMs  = 30'000;
    p.deliveryTimeoutMs = 40'000;
    return p;
}

struct Sent {
    int                      ok = 0, bad = 0, undone = 0;
    std::vector<std::string> okValues;   ///< the values whose completion was Ok
    std::string              firstBad;
};

/// Sends "<tag>-<i>" for i in [from, from + n) through `producer` (keys spread over the partitions) and waits for
/// every completion (bounded).  Records per-value outcome.
Sent sendN(rc::Client& cl, rc::Producer& producer, rc::TopicHandle th, const std::string& tag, int from, int n) {
    Sent out;
    rc::Completions cq(cl);
    std::vector<std::string> values(static_cast<std::size_t>(n));
    std::vector<char> done(static_cast<std::size_t>(n), 0);
    int completed = 0;
    auto drain = [&] {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
            const auto ix = static_cast<std::size_t>(d.token - 1);
            if (ix >= done.size() || done[ix]) return;
            done[ix] = 1;
            ++completed;
            if (d.status == rc::OpStatus::Ok) {
                ++out.ok;
                out.okValues.push_back(values[ix]);
            } else {
                ++out.bad;
                if (out.firstBad.empty())
                    out.firstBad = "status " + std::to_string(static_cast<int>(d.status)) + " code " + std::to_string(d.errorCode);
            }
        });
    };
    const auto deadline = Clock::now() + 90s;
    for (int k = 0; k < n; ++k) {
        values[static_cast<std::size_t>(k)] = tag + "-" + std::to_string(from + k);
        const std::string& v = values[static_cast<std::size_t>(k)];
        const auto bytes = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size());
        for (;;) {
            const rc::SendResult r = producer.trySend(th, rc::Key(static_cast<uint64_t>(from + k)), bytes, cq,
                                                      static_cast<rc::Token>(k) + 1);
            if (r == rc::SendResult::Ok) break;
            if ((r == rc::SendResult::QueueFull || r == rc::SendResult::BackPressured) && Clock::now() < deadline) {
                drain();
                std::this_thread::sleep_for(1ms);
                continue;
            }
            ADD_FAILURE() << "trySend refused " << v << ": " << static_cast<int>(r);
            ++out.bad;
            done[static_cast<std::size_t>(k)] = 1;
            ++completed;
            break;
        }
        drain();
    }
    while (completed < n && Clock::now() < deadline) {
        drain();
        std::this_thread::sleep_for(2ms);
    }
    out.undone = n - completed;
    return out;
}

Sent produceN(rc::Client& cl, rc::TopicHandle th, const std::string& tag, int from, int n, rc::Acks acks) {
    rc::Producer p = cl.producer(producerConfig(acks));
    Sent s = sendN(cl, p, th, tag, from, n);
    p.close();
    return s;
}

/// What a subscription delivered: value -> times seen (partition / offset kept for diagnostics).
struct Collector {
    std::mutex                    mu;
    std::map<std::string, int>    seen;
    std::size_t                   total = 0;
    std::map<int32_t, int>        perPartition;
    void add(const rc::Record& r) {
        std::lock_guard lk(mu);
        ++perPartition[r.partition];
        ++seen[std::string(reinterpret_cast<const char*>(r.value.data()), r.value.size())];
        ++total;
    }
    std::size_t count(const std::set<std::string>& want) {
        std::lock_guard lk(mu);
        std::size_t n = 0;
        for (const auto& v : want) n += seen.count(v) != 0;
        return n;
    }
    std::size_t distinct() { std::lock_guard lk(mu); return seen.size(); }
    /// Number of values delivered more than once, or not in `want`.
    std::size_t strays(const std::set<std::string>& want) {
        std::lock_guard lk(mu);
        std::size_t n = 0;
        for (const auto& [v, c] : seen) n += (want.count(v) == 0 || c != 1);
        return n;
    }
    std::size_t withPrefix(const std::string& prefix) {
        std::lock_guard lk(mu);
        std::size_t n = 0;
        for (const auto& [v, c] : seen) n += v.rfind(prefix, 0) == 0;
        return n;
    }
};

std::vector<rc::Subscription> subscribeAll(rc::Client& cl, const rc::TopicSpec& spec, Collector& col,
                                           rc::Isolation iso = rc::Isolation::ReadUncommitted) {
    std::vector<rc::Subscription> subs;
    rc::SubscribeOptions o;
    o.from      = rc::From::earliest();
    o.isolation = iso;
    for (int32_t p = 0; p < spec.partitionCount; ++p)
        subs.push_back(cl.subscribe(handleOf(spec), p, o, [&col](const rc::Record& r) { col.add(r); }));
    return subs;
}

std::set<std::string> asSet(const std::vector<std::string>& v) { return {v.begin(), v.end()}; }

/// A client of `cfg` that has connected (bounded); a failure to connect is a test failure with the reason.
std::unique_ptr<rc::Client> connectOrFail(const rc::ClientConfig& cfg) {
    auto cl = std::make_unique<rc::Client>(cfg);
    try {
        cl->connectAndWait(60s);
    } catch (const std::exception& e) {
        ADD_FAILURE() << "connect failed: " << e.what();
        return nullptr;
    }
    return cl;
}

// ---------------------------------------------------------------------------------------------------------------
// Broker observations
// ---------------------------------------------------------------------------------------------------------------

std::set<int64_t> sessionIds(BrokerNode& n) {
    const auto v = n.onControlThreadForTest([&n] { return n.gatewayForTest()->sessionIdsForTest(); });
    return {v.begin(), v.end()};
}
std::set<int64_t> minus(const std::set<int64_t>& a, const std::set<int64_t>& b) {
    std::set<int64_t> out;
    for (int64_t x : a) if (b.count(x) == 0) out.insert(x);
    return out;
}
std::string sourceIdentityOf(BrokerNode& n, int64_t sid) {
    return n.onControlThreadForTest([&n, sid] { return n.gatewayForTest()->sessionSourceIdentityForTest(sid); });
}
uint64_t bindsAccepted(BrokerNode& n) {
    return n.onWorkerThreadForTest(0, [](Worker& w) { return w.workerBindsAcceptedForTest(); });
}
uint64_t spoofed(BrokerNode& n) {
    return n.onWorkerThreadForTest(0, [](Worker& w) { return w.spoofedIngressDroppedForTest(); });
}
bool responsePubReady(BrokerNode& n, int64_t sid) {
    return n.onWorkerThreadForTest(0, [sid](Worker& w) { return w.responsePubReadyForTest(sid); });
}

// ---------------------------------------------------------------------------------------------------------------
// The "every operation" run (tests 1 and 2)
// ---------------------------------------------------------------------------------------------------------------

struct GroupTap {
    std::mutex            mu;
    std::set<int32_t>     partitions;
    Collector             records;
};

/// Drives every client operation class through `cfg`'s client (plain produce at the three acks levels, subscribe, a
/// group consumer with commitSync, a transactional producer, DDL define + drop) and checks every outcome.  `scratch`
/// (created beforehand by an old client) is dropped over the NAT'd client's own session.  The client is returned
/// closed (its duty thread gone), so its duty-owned observers can be read race-free.
void everyOperation(std::unique_ptr<rc::Client>& out, rc::ClientConfig cfg, const rc::TopicSpec& spec,
                    const std::string& scratch, const std::string& tag,
                    const std::function<void(rc::Client&, rc::Client&)>& beforeClose) {
    // The GROUP leg runs on a second NAT client (same declared address, own data dir): while a group is active on a
    // Client a plain subscribe() to a partition another node leads is failed by design (documented contract of
    // Client::subscribe(), LeaderElsewhereWhileGroupActive; issue #749).
    rc::ClientConfig gcfgClient = cfg;
    gcfgClient.dataDir = cfg.dataDir + "_g";
    gcfgClient.egressChannel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());   // its own local subscription
    gcfgClient.topics  = {spec};
    auto groupClient = std::make_unique<rc::Client>(gcfgClient);
    out = std::make_unique<rc::Client>(cfg);
    rc::Client& cl = *out;
    GroupTap tap;
    rc::GroupConfig gcfg;
    gcfg.autoCommit          = false;
    gcfg.sessionTimeoutMs    = 30'000;
    gcfg.commitSyncTimeoutMs = 30'000;
    auto gc = groupClient->subscribeGroup("g-" + tag, {spec.name}, gcfg);
    gc.onAssigned([&tap](std::vector<rc::TopicPartition> tps) {
        std::lock_guard lk(tap.mu);
        for (const auto& tp : tps) tap.partitions.insert(tp.partition);
    });
    gc.onRecords([&tap](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
            std::lock_guard lk(tap.records.mu);
            ++tap.records.seen[std::string(v)];
            ++tap.records.total;
        });
    });
    ASSERT_NO_THROW(cl.connectAndWait(60s)) << "connect over the response path";
    ASSERT_NO_THROW(groupClient->connectAndWait(60s)) << "group client connect over the response path";

    // The group joins and is assigned every partition before the data is produced.
    ASSERT_TRUE(waitUntil([&] { std::lock_guard lk(tap.mu); return tap.partitions.size() == static_cast<std::size_t>(spec.partitionCount); }, 60s))
        << "the group consumer was assigned every partition";

    Collector main;
    auto subs = subscribeAll(cl, spec, main);

    const rc::TopicHandle th = handleOf(spec);
    const Sent all    = produceN(cl, th, tag + "-all", 0, 200, rc::Acks::All);
    const Sent leader = produceN(cl, th, tag + "-ldr", 0, 50, rc::Acks::Leader);
    const Sent none   = produceN(cl, th, tag + "-none", 0, 20, rc::Acks::None);
    const Sent last   = produceN(cl, th, tag + "-last", 0, 1, rc::Acks::All);
    EXPECT_EQ(all.ok, 200)    << "acks=All acks over the response path: " << all.firstBad << " undone " << all.undone;
    EXPECT_EQ(leader.ok, 50)  << "acks=Leader acks over the response path: " << leader.firstBad << " undone " << leader.undone;
    EXPECT_EQ(none.ok, 20)    << "acks=None handed to the transport: " << none.firstBad << " undone " << none.undone;
    EXPECT_EQ(last.ok, 1)     << last.firstBad;
    EXPECT_EQ(all.bad + leader.bad + none.bad + last.bad, 0);
    EXPECT_EQ(all.undone + leader.undone + none.undone + last.undone, 0);

    std::set<std::string> acked = asSet(all.okValues);
    for (const auto* s : {&leader, &last}) for (const auto& v : s->okValues) acked.insert(v);
    EXPECT_EQ(acked.size(), 251u);

    // Subscribe: every acked value exactly once.
    EXPECT_TRUE(waitUntil([&] { return main.count(acked) == acked.size(); }, 60s))
        << "subscription Records over the response path: " << main.count(acked) << "/" << acked.size()
        << " p0=" << main.perPartition[0] << " p1=" << main.perPartition[1] << " p2=" << main.perPartition[2];
    EXPECT_EQ(main.strays(acked) , main.distinct() - main.count(acked)) << "no acked value was delivered twice";
    {
        std::lock_guard lk(main.mu);
        for (const auto& v : acked) EXPECT_LE(main.seen[v], 1) << v << " delivered twice";
    }

    // Group: the same values through the group consumer's own fetch path, and a commitSync answered.
    EXPECT_TRUE(waitUntil([&] { return tap.records.count(acked) == acked.size(); }, 60s))
        << "group Records over the response path: " << tap.records.count(acked) << "/" << acked.size();
    EXPECT_NO_THROW(gc.commitSyncAndWait(30s)) << "group commitSync answered over the response path";

    // Transactions: begin, 10 records, commit; visible to a read_committed subscription.
    {
        rc::ProducerConfig tp = producerConfig(rc::Acks::All);
        tp.transactionOpTimeoutMs = 25'000;
        tp.transactionalId        = "tx-" + tag;
        rc::Producer p = cl.producer(tp);
        EXPECT_NO_THROW(p.initTransactionsAndWait(30s));
        p.beginTransaction();
        const Sent tx = sendN(cl, p, th, tag + "-txn", 0, 10);
        EXPECT_EQ(tx.ok, 10) << tx.firstBad << " undone " << tx.undone;
        EXPECT_NO_THROW(p.commitTransactionAndWait(30s)) << "transaction commit answered over the response path";
        p.close();
        Collector committed;
        auto csubs = subscribeAll(cl, spec, committed, rc::Isolation::ReadCommitted);
        const std::set<std::string> txnValues = asSet(tx.okValues);
        EXPECT_TRUE(waitUntil([&] { return committed.count(txnValues) == txnValues.size(); }, 60s))
            << "the 10 committed transaction records are visible: " << committed.count(txnValues);
        for (auto& s : csubs) s.unsubscribe();
    }

    // DDL over the response path: define a topic, drop the scratch one.
    {
        const auto def = cl.defineTopicAndWait(tag + "-ddl", 1, "", false, {}, 30s);
        EXPECT_EQ(def.errorCode, 0) << def.detail;
        const auto drop = cl.dropTopicAndWait(scratch, {}, 30s);
        EXPECT_EQ(drop.errorCode, 0) << drop.detail;
    }

    // Duty-owned observers are read here, with every operation finished (close() resets the transport's pairing).
    beforeClose(cl, *groupClient);
    for (auto& s : subs) s.unsubscribe();
    gc.close();
    groupClient->close();
    cl.close();
}

}  // namespace

// =================================================================================================================
// 1. One node, shard mode: every operation of a NAT'd client rides the response path.
// =================================================================================================================
TEST(NatEgressMatrixRegression, NewClientEveryOperationOverResponsePathOneNodeRegression) {
    const RespPorts r = RespPorts::discover(1);
    OneBroker b("nat1", OneBroker::Runtime::Shard, withResponsePorts(r));
    rc::Client* admin = b.connectClient();
    const rc::TopicSpec spec = defineTopic(*admin, "orders", 3, 1);
    (void)defineTopic(*admin, "scratch", 1, 1);

    auto cfg = b.clientConfig();
    cfg.gatewayResponseControlPort   = r.gw[0];
    cfg.declaredEgressChannelForTest = kUnroutable;
    cfg.connectTimeoutMs = 30'000;
    cfg.pingTimeoutMs    = 30'000;
    std::unique_ptr<rc::Client> client;
    everyOperation(client, cfg, spec, "scratch", "n1", [](rc::Client& cl, rc::Client& gcl) {
        EXPECT_TRUE(cl.testSeedResponseCorrelated()) << "the seed session is response-correlated";
        EXPECT_TRUE(gcl.testSeedResponseCorrelated()) << "the group client's seed session is response-correlated";
    });
    ASSERT_NE(client, nullptr);
    EXPECT_GE(bindsAccepted(b.node()), 1u) << "the worker accepted the client's WorkerBind";
    EXPECT_EQ(spoofed(b.node()), 0u);
}

// =================================================================================================================
// 2. Three nodes: the same, with partition leaders spread; the client learnt every node's response ports.
// =================================================================================================================
TEST(NatEgressMatrixRegression, NewClientEveryOperationOverResponsePathThreeNodesRegression) {
    const RespPorts r = RespPorts::discover(3);
    ThreeNodes c = ThreeNodes::startWith("nat3", withResponsePorts(r));
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto admin = connectClient(c, 1);
    const rc::TopicSpec spec = defineTopic(*admin, "orders", 3, 3);
    (void)defineTopic(*admin, "scratch", 1, 3);
    admin->close();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, spec.topicId, 3, leaders, 30s));

    std::unique_ptr<rc::Client> client;
    everyOperation(client, natClient(c, 1, r), spec, "scratch", "n3", [&](rc::Client& cl, rc::Client& gcl) {
        EXPECT_TRUE(cl.testSeedResponseCorrelated());
        EXPECT_TRUE(gcl.testSeedResponseCorrelated());
        // The gateway response port of every node was learnt (Metadata brokers entries / the seed's own config).
        for (int k = 1; k <= 3; ++k)
            EXPECT_EQ(cl.testKnownResponsePort(c.clientChannel(k)), r.gw[static_cast<std::size_t>(k - 1)])
                << "node " << k << "'s gateway response port";
    });
    ASSERT_NE(client, nullptr);
    // Metadata partition entries carry the owning worker's response port (Task 5 finding 1): there is no client
    // accessor for MetadataCache partitions, so this is asserted through its consequence - every node that leads a
    // partition accepted a WorkerBind, which the client can only have sent to w[leader-1] (the advertised port).
    for (int id : std::set<int32_t>(leaders.begin(), leaders.end()))
        EXPECT_GE(bindsAccepted(c.node(id)), 1u) << "leader " << id << "'s worker never accepted a bind";
    for (int id = 1; id <= 3; ++id) EXPECT_EQ(spoofed(c.node(id)), 0u);
}

// =================================================================================================================
// 3. An old and a new client share a topic on a cluster that has response ports.
// =================================================================================================================
TEST(NatEgressMatrixRegression, MixedOldAndNewClientsShareATopicThreeNodesRegression) {
    const RespPorts r = RespPorts::discover(3);
    ThreeNodes c = ThreeNodes::startWith("natmix", withResponsePorts(r));
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto admin = connectClient(c, 1);
    const rc::TopicSpec spec = defineTopic(*admin, "shared", 3, 3);
    admin->close();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, spec.topicId, 3, leaders, 30s));

    auto ncfg = natClient(c, 1, r);
    ncfg.topics = {spec};
    auto ocfg = oldClient(c, 2);
    ocfg.topics = {spec};
    auto nat = connectOrFail(ncfg);
    auto old = connectOrFail(ocfg);
    ASSERT_NE(nat, nullptr);
    ASSERT_NE(old, nullptr);

    const Sent sn = produceN(*nat, handleOf(spec), "new", 0, 200, rc::Acks::All);
    const Sent so = produceN(*old, handleOf(spec), "old", 0, 200, rc::Acks::All);
    EXPECT_EQ(sn.ok, 200) << sn.firstBad << " undone " << sn.undone;
    EXPECT_EQ(so.ok, 200) << so.firstBad << " undone " << so.undone;
    std::set<std::string> want = asSet(sn.okValues);
    for (const auto& v : so.okValues) want.insert(v);
    ASSERT_EQ(want.size(), 400u);

    Collector cn, co;
    auto subsN = subscribeAll(*nat, spec, cn);
    auto subsO = subscribeAll(*old, spec, co);
    EXPECT_TRUE(waitUntil([&] { return cn.count(want) == want.size(); }, 60s)) << "new client: " << cn.count(want) << "/400";
    EXPECT_TRUE(waitUntil([&] { return co.count(want) == want.size(); }, 60s)) << "old client: " << co.count(want) << "/400";
    EXPECT_EQ(cn.strays(want), 0u);
    EXPECT_EQ(co.strays(want), 0u);
    for (auto& s : subsN) s.unsubscribe();
    for (auto& s : subsO) s.unsubscribe();
    EXPECT_TRUE(nat->testSeedResponseCorrelated());
    EXPECT_FALSE(old->testSeedResponseCorrelated()) << "an old client is never put on the response path";
    nat->close();
    old->close();
}

// =================================================================================================================
// 4. Two NAT'd clients on one "host" (same declared channel): neither sees the other's replies.
// =================================================================================================================
TEST(NatEgressMatrixRegression, TwoNatClientsOneHostNeverSeeEachOthersRepliesRegression) {
    const RespPorts r = RespPorts::discover(3);
    ThreeNodes c = ThreeNodes::startWith("nat2c", withResponsePorts(r));
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto admin = connectClient(c, 1);
    const rc::TopicSpec specA = defineTopic(*admin, "topic-a", 3, 3);
    const rc::TopicSpec specB = defineTopic(*admin, "topic-b", 3, 3);
    admin->close();
    std::vector<int32_t> la, lb;
    ASSERT_TRUE(awaitLeaders(c, specA.topicId, 3, la, 30s));
    ASSERT_TRUE(awaitLeaders(c, specB.topicId, 3, lb, 30s));

    const std::set<int64_t> before = sessionIds(c.node(2));
    auto cfgA = natClient(c, 2, r);
    auto cfgB = natClient(c, 2, r);
    ASSERT_EQ(cfgA.declaredEgressChannelForTest, cfgB.declaredEgressChannelForTest) << "both declare the same address";
    auto a = connectOrFail(cfgA);
    ASSERT_NE(a, nullptr);
    const std::set<int64_t> afterA = sessionIds(c.node(2));
    auto b = connectOrFail(cfgB);
    ASSERT_NE(b, nullptr);
    const std::set<int64_t> afterB = sessionIds(c.node(2));
    const std::set<int64_t> sidsA = minus(afterA, before), sidsB = minus(afterB, afterA);
    EXPECT_FALSE(sidsA.empty());
    EXPECT_FALSE(sidsB.empty());
    for (int64_t s : sidsA) EXPECT_EQ(sidsB.count(s), 0u) << "session id " << s << " belongs to both clients";

    Sent sa, sb;
    std::thread tb([&] { sb = produceN(*b, handleOf(specB), "bee", 0, 200, rc::Acks::All); });
    sa = produceN(*a, handleOf(specA), "aye", 0, 200, rc::Acks::All);
    tb.join();
    EXPECT_EQ(sa.ok, 200) << sa.firstBad << " undone " << sa.undone;
    EXPECT_EQ(sb.ok, 200) << sb.firstBad << " undone " << sb.undone;
    const std::set<std::string> wantA = asSet(sa.okValues), wantB = asSet(sb.okValues);
    Collector ca, cb;
    auto subsA = subscribeAll(*a, specA, ca);
    auto subsB = subscribeAll(*b, specB, cb);
    EXPECT_TRUE(waitUntil([&] { return ca.count(wantA) == 200; }, 60s)) << "A: " << ca.count(wantA) << "/200";
    EXPECT_TRUE(waitUntil([&] { return cb.count(wantB) == 200; }, 60s)) << "B: " << cb.count(wantB) << "/200";
    EXPECT_EQ(ca.strays(wantA), 0u);
    EXPECT_EQ(cb.strays(wantB), 0u);
    EXPECT_EQ(ca.withPrefix("bee-"), 0u) << "A saw B's values";
    EXPECT_EQ(cb.withPrefix("aye-"), 0u) << "B saw A's values";
    for (auto& s : subsA) s.unsubscribe();
    for (auto& s : subsB) s.unsubscribe();
    EXPECT_TRUE(a->testSeedResponseCorrelated());
    EXPECT_TRUE(b->testSeedResponseCorrelated());
    a->close();
    b->close();
}

// =================================================================================================================
// 5 / 6. Authentication over TLS on a shard broker: the new client's session source identity is its real flow (the
// image), never the declared channel (Task 5 finding 2, v16 leg); the old client's is the image too (v15 leg).
// =================================================================================================================
namespace {

constexpr const char* kOtp     = "one-time-password-nat-1";
constexpr const char* kAdminPw = "admin-password-over-nat-1";
constexpr const char* kUserPw  = "user-password-over-nat-1";

fs::path scratchDir() {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_nat_egress_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    return d;
}
std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = scratchDir() / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);
    return p.string();
}
int runCli(std::vector<std::string> args, std::string* err = nullptr) {
    std::stringstream o, e;
    const int code = rune::cli::run(std::move(args), o, e);
    if (err) *err = e.str();
    return code;
}

/// `rune security bootstrap` + the forced first password change + `script`, all with the CLI over TLS.
void bootstrapOverTls(AuthBroker& b, const std::string& script) {
    const std::string otp = writeTemp("otp", std::string(kOtp) + "\n");
    const std::string ca  = AuthBroker::tlsCaFile().string();
    std::string err;
    ASSERT_EQ(runCli({"security", "bootstrap", "--admin", "admin", "--cluster-key-file", b.keyFile().string(),
                      "--password-file", otp, "--broker", b.endpoint(), "--tls-ca", ca}, &err), 0) << err;
    const std::string change = writeTemp("change.rql", std::string("alter user admin set password '") + kAdminPw + "';\n");
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", otp, "-f", change, "--tls-ca", ca},
                     &err), 0) << err;
    const std::string pw   = writeTemp("admin.pw", std::string(kAdminPw) + "\n");
    const std::string file = writeTemp("script.rql", script);
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw, "-f", file, "--tls-ca", ca},
                     &err), 0) << err;
}

rc::ClientConfig tlsConfig(AuthBroker& b, const std::string& user, const std::string& pw) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = pw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    return c;
}

/// One TLS leg: a user with write / read on "orders" connects as a NAT'd (nat) or old client and produces + consumes 100.
void tlsLeg(bool nat, const char* tag) {
    const RespPorts r = RespPorts::discover(1);
    AuthBroker::Options o;
    o.shard   = true;
    o.tlsOnly = true;
    o.configure = [&r](NodeConfig& cfg) {
        cfg.gatewayResponseControlPort = r.gw[0];
        cfg.responseControlBasePort    = r.w[0];
        cfg.peers[0].gatewayResponseControlPort = r.gw[0];
        cfg.peers[0].responseControlBasePort    = r.w[0];
    };
    AuthBroker b(tag, o);
    bootstrapOverTls(b, std::string("create user ws1 with password '") + kUserPw + "';\n"
                        "grant create on topic prefix '' to user admin;\n"
                        "grant write on topic 'orders' to user ws1;\n"
                        "grant read on topic 'orders' to user ws1;\n"
                        "grant describe on cluster to user ws1;\n");
    if (::testing::Test::HasFatalFailure()) return;
    rc::TopicSpec spec;
    {
        rc::Client admin(tlsConfig(b, "admin", kAdminPw));
        ASSERT_NO_THROW(admin.connectAndWait(30s));
        const auto def = admin.defineTopicAndWait("orders", 1, "", false, {}, 30s);
        ASSERT_EQ(def.errorCode, 0) << def.detail;
        spec = rc::TopicSpec{"orders", def.topicId, 1};
        admin.closeImmediately();
    }

    const std::set<int64_t> before = sessionIds(b.node());
    auto cfg = tlsConfig(b, "ws1", kUserPw);
    if (nat) {
        cfg.gatewayResponseControlPort   = r.gw[0];
        cfg.declaredEgressChannelForTest = kUnroutable;
    }
    const std::string declaredEndpoint = cfg.egressChannel.substr(cfg.egressChannel.rfind('=') + 1);
    auto client = connectOrFail(cfg);
    ASSERT_NE(client, nullptr) << "login over TLS";
    EXPECT_EQ(client->authStatus().kind, rc::AuthStatus::Kind::Authenticated);
    const std::set<int64_t> fresh = minus(sessionIds(b.node()), before);
    ASSERT_FALSE(fresh.empty());
    for (int64_t sid : fresh) {
        const std::string src = sourceIdentityOf(b.node(), sid);
        EXPECT_FALSE(src.empty()) << "session " << sid << " has a source identity";
        EXPECT_EQ(src.find("192.0.2.1"), std::string::npos) << "the declared channel is never the source: " << src;
        EXPECT_EQ(src.rfind("127.0.0.1:", 0), 0u) << "the source is the Connect image's host:port: " << src;
        EXPECT_NE(src, declaredEndpoint) << "the source is the flow's port, not the declared egress endpoint";
    }

    const Sent sent = produceN(*client, handleOf(spec), "tls", 0, 100, rc::Acks::All);
    EXPECT_EQ(sent.ok, 100) << sent.firstBad << " undone " << sent.undone;
    Collector col;
    auto subs = subscribeAll(*client, spec, col);
    const auto want = asSet(sent.okValues);
    EXPECT_TRUE(waitUntil([&] { return col.count(want) == want.size(); }, 60s)) << col.count(want) << "/100";
    EXPECT_EQ(col.strays(want), 0u);
    for (auto& s : subs) s.unsubscribe();
    EXPECT_EQ(client->testSeedResponseCorrelated(), nat);
    client->close();
    if (nat) EXPECT_GE(bindsAccepted(b.node()), 1u) << "the worker accepted the client's WorkerBind";
    else     EXPECT_EQ(bindsAccepted(b.node()), 0u) << "an old client never binds";
}

}  // namespace

TEST(NatEgressMatrixRegression, TlsAuthNewClientOverResponsePathRegression) { tlsLeg(/*nat=*/true, "natauthnew"); }

TEST(NatEgressMatrixRegression, TlsAuthOldClientDeclaredPathRegression) { tlsLeg(/*nat=*/false, "natauthold"); }

// =================================================================================================================
// 8. A connect racing a consensus-leader stop converges.
// =================================================================================================================
TEST(NatEgressMatrixRegression, ConnectRacingALeaderChangeConvergesRegression) {
    const RespPorts r = RespPorts::discover(3);
    ThreeNodes c = ThreeNodes::startWith("natrace", withResponsePorts(r));
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = awaitController(c);
    ASSERT_GT(leader, 0);
    const int32_t follower = aFollowerOfTheCatalog(c) == leader ? -1 : aFollowerOfTheCatalog(c);
    int seed = follower;
    for (int id : c.runningNodes()) if (seed <= 0 && id != leader) seed = id;
    ASSERT_GT(seed, 0);
    ASSERT_NE(seed, leader);

    auto admin = connectClient(c, seed);
    const rc::TopicSpec spec = defineTopic(*admin, "raced", 3, 3);
    admin->close();
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, spec.topicId, 3, leaders, 30s));

    auto cfg = natClient(c, seed, r);
    cfg.topics = {spec};
    rc::Client client(cfg);
    std::thread killer([&] { c.stop(leader); });
    bool connected = true;
    try {
        client.connectAndWait(60s);
    } catch (const std::exception& e) {
        connected = false;
        ADD_FAILURE() << "connect racing the leader stop failed: " << e.what();
    }
    killer.join();
    ASSERT_TRUE(connected);

    std::vector<int32_t> survivors;
    ASSERT_TRUE(awaitLeaders(c, spec.topicId, 3, survivors, 60s)) << "every partition re-led on the two survivors";
    const Sent sent = produceN(client, handleOf(spec), "race", 0, 200, rc::Acks::All);
    EXPECT_EQ(sent.ok, 200) << sent.firstBad << " undone " << sent.undone;
    Collector col;
    auto subs = subscribeAll(client, spec, col);
    const auto want = asSet(sent.okValues);
    EXPECT_TRUE(waitUntil([&] { return col.count(want) == want.size(); }, 60s)) << col.count(want) << "/200";
    EXPECT_EQ(col.strays(want), 0u);
    for (auto& s : subs) s.unsubscribe();
    EXPECT_TRUE(client.testSeedResponseCorrelated());
    client.close();
}

// =================================================================================================================
// 9. A source-port change (a new NAT mapping = a new driver): the old session ends, nothing reuses it.
// =================================================================================================================
TEST(NatEgressMatrixRegression, SourcePortChangeEndsTheOldSessionAndNothingReusesItRegression) {
    const RespPorts r = RespPorts::discover(1);
    OneBroker b("natport", OneBroker::Runtime::Shard, withResponsePorts(r));
    rc::Client* admin = b.connectClient();
    const rc::TopicSpec spec = defineTopic(*admin, "moving", 1, 1);

    auto natCfg = [&] {
        auto cfg = b.clientConfig();
        cfg.gatewayResponseControlPort   = r.gw[0];
        cfg.declaredEgressChannelForTest = kUnroutable;
        cfg.connectTimeoutMs = 30'000;
        cfg.pingTimeoutMs    = 30'000;
        cfg.topics           = {spec};
        return cfg;
    };

    const std::set<int64_t> s0 = sessionIds(b.node());
    auto a = connectOrFail(natCfg());
    ASSERT_NE(a, nullptr);
    const std::set<int64_t> sidsA = minus(sessionIds(b.node()), s0);
    ASSERT_EQ(sidsA.size(), 1u);
    const int64_t sidA = *sidsA.begin();
    const Sent sa = produceN(*a, handleOf(spec), "first", 0, 50, rc::Acks::All);
    EXPECT_EQ(sa.ok, 50) << sa.firstBad << " undone " << sa.undone;
    Collector ca;
    auto subsA = subscribeAll(*a, spec, ca);
    const auto wantA = asSet(sa.okValues);
    EXPECT_TRUE(waitUntil([&] { return ca.count(wantA) == 50; }, 60s));
    EXPECT_TRUE(responsePubReady(b.node(), sidA)) << "A's session has a worker response publication";
    // The old flow vanishes: closeImmediately() sends no Disconnect frame and waits for nothing, which is what a NAT
    // remap looks like to the broker (the 5-tuple simply stops).
    for (auto& s : subsA) s.unsubscribe();
    a->closeImmediately();

    EXPECT_TRUE(waitUntil([&] { return sessionIds(b.node()).count(sidA) == 0; }, 30s))
        << "the gateway forgot A's session within 30 s";
    EXPECT_TRUE(waitUntil([&] { return !responsePubReady(b.node(), sidA); }, 30s))
        << "the worker dropped A's response publication within 30 s";

    const uint64_t bindsBefore = bindsAccepted(b.node());
    const std::set<int64_t> s1 = sessionIds(b.node());
    auto nb = connectOrFail(natCfg());
    ASSERT_NE(nb, nullptr);
    const std::set<int64_t> sidsB = minus(sessionIds(b.node()), s1);
    ASSERT_EQ(sidsB.size(), 1u);
    EXPECT_NE(*sidsB.begin(), sidA) << "nothing reuses the old session id";
    const Sent sb = produceN(*nb, handleOf(spec), "second", 0, 50, rc::Acks::All);
    EXPECT_EQ(sb.ok, 50) << sb.firstBad << " undone " << sb.undone;
    Collector cb;
    auto subsB = subscribeAll(*nb, spec, cb);
    std::set<std::string> both = wantA;
    for (const auto& v : sb.okValues) both.insert(v);
    EXPECT_TRUE(waitUntil([&] { return cb.count(both) == both.size(); }, 60s)) << cb.count(both) << "/100";
    EXPECT_EQ(cb.strays(both), 0u);
    EXPECT_EQ(bindsAccepted(b.node()), bindsBefore + 1) << "exactly B's bind";
    for (auto& s : subsB) s.unsubscribe();
    nb->close();
}

// =================================================================================================================
// 10. DEFAULT (derived) ports carry the response path: no explicit shard or response port on the node.
// =================================================================================================================
TEST(NatEgressMatrixRegression, DerivedDefaultPortsCarryTheResponsePathRegression) {
    constexpr int kNodeId = 113;
    const fs::path dir = scratchDir() / ("derived_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    const Ports ports = Ports::discover(1);
    NodeConfig cfg = makeConfig(1, dir / "broker", ports, 1, Mode::Shard);
    cfg.nodeId = kNodeId;
    cfg.peers[0].id = kNodeId;
    cfg.workerIngressBasePort.reset();
    cfg.workerReplBasePort.reset();
    cfg.workerEgressControlBasePort.reset();
    cfg.responseControlBasePort.reset();
    cfg.gatewayResponseControlPort.reset();
    cfg.peers[0].ingressBasePort = 0;
    cfg.peers[0].replBasePort = 0;
    cfg.peers[0].egressControlBasePort = 0;
    cfg.peers[0].responseControlBasePort = 0;
    cfg.peers[0].gatewayResponseControlPort = 0;

    const auto base = deriveShardBasePorts(kNodeId, 1);
    ASSERT_TRUE(base.has_value());
    const auto resp = deriveResponseControlPorts(base->egressControl, 1);
    ASSERT_TRUE(resp.has_value());

    BrokerNode node(cfg);
    node.start();
    struct Stop { BrokerNode& n; ~Stop() { n.stop(); } } stop{node};
    ASSERT_TRUE(node.awaitLeader(30s));

    rc::ClientConfig ccfg;
    ccfg.gatewayChannel  = cfg.clientEndpoint.channel;
    ccfg.gatewayStreamId = kClientStream;
    ccfg.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    ccfg.egressStreamId  = 2;
    ccfg.dataDir         = (dir / "client").string();
    ccfg.gatewayResponseControlPort   = resp->gateway;
    ccfg.declaredEgressChannelForTest = kUnroutable;
    ccfg.connectTimeoutMs = 30'000;
    ccfg.pingTimeoutMs    = 30'000;

    // The topic is created over the same NAT'd client's DDL.
    auto client = connectOrFail(ccfg);
    ASSERT_NE(client, nullptr) << "connect over the derived response ports";
    rc::TopicSpec spec;
    {
        const auto def = client->defineTopicAndWait("derived", 1, "", false, {}, 60s);
        ASSERT_EQ(def.errorCode, 0) << def.detail;
        spec = rc::TopicSpec{"derived", def.topicId, 1};
    }
    const Sent sent = produceN(*client, handleOf(spec), "drv", 0, 50, rc::Acks::All);
    EXPECT_EQ(sent.ok, 50) << sent.firstBad << " undone " << sent.undone;
    Collector col;
    auto subs = subscribeAll(*client, spec, col);
    const auto want = asSet(sent.okValues);
    EXPECT_TRUE(waitUntil([&] { return col.count(want) == want.size(); }, 60s)) << col.count(want) << "/50";
    EXPECT_EQ(col.strays(want), 0u);
    for (auto& s : subs) s.unsubscribe();
    EXPECT_TRUE(client->testSeedResponseCorrelated());
    client->close();
    EXPECT_GE(bindsAccepted(node), 1u);
    std::error_code ec;
    fs::remove_all(dir, ec);
}
