// tests/client/GroupRoutingRegressionTest.cpp — consumer-group fetch routing
// across broker nodes (distributed per-partition leadership).
//
// The bug (dockerized 3-node DR suite: test_cg_rebalance / _atleastonce /
// _failover): GroupConsumerHub sent EVERY assigned partition's Subscribe on the
// seed (group coordinator) session.  A broker answers a Subscribe for a
// partition it does not lead with ErrorResponse{Redirect | NotLeader} and
// registers nothing, and the group consumer never handled that answer — the
// partitions led by other nodes were never consumed by any member.
//
// Rig: one Invoker client + one InvokerGateway that models several broker nodes
// (endpoint 0 = node 0 = the seed / group coordinator, more UDP endpoints =
// more nodes).  Subscribe leadership is modelled by InvokerGateway::
// subscribeLeader.  Everything is single-threaded and condition-driven: no
// sleeps; every wait is a bounded pump loop.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/TransactionalProducer.h>

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <chrono>
#include <map>
#include <memory>
#include <tuple>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;

namespace {
int g_ctr = 0;  // test thread only

std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

// MEDIUM-6: never a fixed port — the kernel picks a free one.
int freePort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    (void)::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    (void)::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}

struct RigOptions {
    // Each fake node sends from its own UDP socket (a distinct
    // Image::sourceIdentity), like real broker nodes on distinct hosts; the
    // client's egress channel is UDP then.  Default: IPC egress (one source).
    bool    udpSources        = false;
    int64_t keepaliveNs       = 0;  // 0 = agent defaults
    int64_t responseTimeoutNs = 0;
    int64_t connectTimeoutMs  = 0;  // 0 = the client default
    bool    reconnectForever  = false;
};

struct GroupRig {
    RigOptions                   opt;
    std::string                  egress;
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    rc::Completions              cq{client};
    rc::Token                    nextToken = 1;

    // Node 0 (the seed + group coordinator) on a free port.  A real clock: UDP
    // sessions need the SessionMachine's connect retry (duty clock).
    GroupRig(int32_t in, int32_t eg, RigOptions o = {})
        : opt(o),
          egress(o.udpSources ? udp(freePort()) : std::string("aeron:ipc")),
          c(makeCfg(in, eg, udp(freePort()), egress, o)),
          client(c),
          gw(c.dataDir + "/aeron", c.gatewayChannel, in, egress, eg, pubChannel()) {
        const std::string& seed = c.gatewayChannel;
        gw.brokerHost       = "127.0.0.1";
        gw.brokerPort       = std::stoi(seed.substr(seed.rfind(':') + 1));
        gw.uniqueSessionIds = true;   // each node session gets its own id
        gw.groupProtocol    = true;
        gw.holdJoins        = true;   // the test releases the join once routes are known
        gw.replyPongs       = true;   // keepalives answered (each node answers its own)
        gw.replyFromArrival = true;   // each node answers / pushes from its own publication
    }
    // The reply publication of the next fake node (a distinct source when udpSources).
    std::string pubChannel() const {
        return opt.udpSources ? egress + "|interface=127.0.0.1:" + std::to_string(freePort()) : std::string();
    }
    /// Adds a fake broker node on a free port; returns (endpoint index, port).
    std::pair<int, int> addNode(int32_t stream) {
        const int port = freePort();
        gw.nextPubChannel = pubChannel();
        return {gw.addEndpoint(udp(port), stream), port};
    }
    static rc::ClientConfig makeCfg(int32_t in, int32_t eg, const std::string& seed, const std::string& egress,
                                    const RigOptions& o) {
        rc::ClientConfig c;
        c.gatewayChannel  = seed;
        c.gatewayStreamId = in;
        c.egressChannel   = egress;
        c.egressStreamId  = eg;
        c.dataDir = "/tmp/group_routing_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        c.keepaliveIntervalNs = o.keepaliveNs;
        c.responseTimeoutNs   = o.responseTimeoutNs;
        if (o.connectTimeoutMs > 0) c.connectTimeoutMs = o.connectTimeoutMs;
        c.reconnectForeverAfterLive = o.reconnectForever;
        rc::TopicSpec ts;
        ts.name           = "t";
        ts.topicId        = 1;
        ts.partitionCount = 2;
        c.topics.push_back(ts);
        rc::TopicSpec tb;
        tb.name           = "b";
        tb.topicId        = 2;
        tb.partitionCount = 1;
        c.topics.push_back(tb);
        return c;
    }
    template <class P>
    bool drive(P&& pred, int cap = 400'000) {
        for (int i = 0; i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        const rc::Token t = nextToken++;
        ASSERT_EQ(client.connect(cq, t), rc::OpResult::Accepted);
        bool ok = false;
        ASSERT_TRUE(drive([&] {
            cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
                if (cc.token == t) {
                    ok      = cc.status == rc::OpStatus::Ok;
                    seedSid = cc.value;   // the seed session's broker id
                }
            });
            return ok;
        }));
    }
    int64_t seedSid = 0;  // set by connect()
    /// A subscription for partition `p` registered on endpoint `ep`, or nullptr.
    [[nodiscard]] const test_support::InvokerGateway::Subscribed* subAt(int32_t p, int ep) const {
        const auto* s = gw.latestSubscription(1, p);
        return (s != nullptr && s->endpoint == ep) ? s : nullptr;
    }
};

// Per-partition consumption ledger (runs on the duty thread = this thread).
struct Consumed {
    std::map<int32_t, int64_t> records;   // partition -> records delivered
    std::map<int32_t, int64_t> next;      // partition -> baseOffset + recordCount of the last batch
};

void wire(rc::GroupConsumer& gc, Consumed& led) {
    gc.onRecords([&led](const rc::RecordSet& rs) {
        led.records[rs.partition] += rs.recordCount;
        led.next[rs.partition] = rs.baseOffset + rs.recordCount;
    });
}
} // namespace

// RED on main: partition 1 is led by node 1, the member is assigned both
// partitions — its Subscribe for partition 1 went to the seed (node 0), which
// refused it, so partition 1 was never consumed.  The fix routes each group
// Subscribe to the partition's LEADER session, stamped with that session's id.
TEST(GroupRoutingRegression, MemberConsumesPartitionsLedByTwoNodesRegression) {
    GroupRig r(4101, 4102);
    const auto [n1, p1port] = r.addNode(4101);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, /*leader*/ 0, 1, 4101, ""}, {1, 1, /*leader*/ 1, 1, 4101, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};

    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty(); }))
        << "precondition: the metadata routes partition 1 to node 1";
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();

    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }))
        << "partition 1's Subscribe never reached its leader (node 1)";
    const auto s0 = *r.subAt(0, 0);
    const auto s1 = *r.subAt(1, n1);
    ASSERT_FALSE(r.gw.groupSessionIds.empty());
    const int64_t seedSid = r.gw.groupSessionIds.front();
    EXPECT_EQ(s0.sessionId, seedSid) << "partition 0 (led by the coordinator) on the seed session";
    EXPECT_NE(s1.sessionId, seedSid) << "partition 1 must carry node 1's session id, not the seed's";
    EXPECT_EQ(r.gw.subscribeRefusals, 0) << "every Subscribe went straight to its leader";

    r.gw.pushRecords(s0, 0, 3);
    r.gw.pushRecords(s1, 0, 4);
    ASSERT_TRUE(r.drive([&] { return led.records[0] == 3 && led.records[1] == 4; }))
        << "records of both partitions must be consumed (p0=" << led.records[0] << " p1=" << led.records[1] << ")";
    for (const int64_t sid : r.gw.groupSessionIds) {
        EXPECT_EQ(sid, seedSid) << "group coordinator frames stay on the seed session";
    }
}

// The metadata is stale (partition 1 still advertised on node 0): the seed
// answers the group Subscribe with ErrorResponse{Redirect, node 1}.  The member
// must re-route THAT partition to node 1 and consume it — without moving the
// seed (coordinator) session off node 0.  RED on main: the Redirect was taken as
// a seed-level redirect (transport retarget + reconnect of the coordinator
// session) and partition 1 was never subscribed on node 1.
TEST(GroupRoutingRegression, RedirectedGroupSubscribeReroutesToTheLeaderRegression) {
    GroupRig r(4103, 4104);
    const auto [n1, p1port] = r.addNode(4103);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4103, ""}, {1, 1, /*stale leader*/ 0, 1, 4103, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};

    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0 && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeRefusals >= 1; }));
    // A real broker's metadata agrees with its redirects: later answers name node 1.
    r.gw.partitions = {{1, 0, 0, 1, 4103, ""}, {1, 1, 1, 2, 4103, ""}};

    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }))
        << "the redirected partition never re-subscribed on its leader (refusals=" << r.gw.subscribeRefusals << ")";
    EXPECT_GE(r.gw.subscribeRefusals, 1) << "precondition: the seed redirected partition 1";
    const auto s0 = *r.subAt(0, 0);
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s0, 0, 2);
    r.gw.pushRecords(s1, 0, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[0] == 2 && led.records[1] == 5; }))
        << "p0=" << led.records[0] << " p1=" << led.records[1];
    EXPECT_EQ(r.client.testTransport().ingressChannel(), r.c.gatewayChannel)
        << "a group Subscribe redirect must not move the seed (coordinator) session";
    const int64_t seedSid = r.gw.groupSessionIds.front();
    for (const int64_t sid : r.gw.groupSessionIds) EXPECT_EQ(sid, seedSid);
}

// A bare ErrorResponse{NotLeader} (the refusing node does not know the leader)
// parks the partition until a metadata answer names the leader; the member
// then subscribes there.
TEST(GroupRoutingRegression, NotLeaderGroupSubscribeConvergesOnMetadataRegression) {
    GroupRig r(4105, 4106);
    const auto [n1, p1port] = r.addNode(4105);
    r.gw.moreBrokers        = {{1, "127.0.0.1", p1port}};
    r.gw.partitions         = {{1, 0, 0, 1, 4105, ""}, {1, 1, /*stale*/ 0, 1, 4105, ""}};
    r.gw.subscribeLeader    = {{0, 0}, {1, n1}};
    r.gw.subscribeNotLeader = true;
    r.gw.groupAssignment    = {{1, 0}, {1, 1}};

    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0 && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeRefusals >= 1; }));

    // The cluster view catches up: partition 1 is led by node 1 at epoch 2.
    r.gw.partitions = {{1, 0, 0, 1, 4105, ""}, {1, 1, 1, 2, 4105, ""}};
    r.gw.sendMetadata(r.gw.groupSessionIds.front(), /*correlationId=*/0);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }))
        << "the NotLeader-parked partition never subscribed on its leader";
    r.gw.pushRecords(*r.subAt(1, n1), 0, 3);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 3; }));
}

// Leader change: partition 1 moves from node 1 to node 2 (metadata at a newer
// epoch).  The member must re-subscribe on node 2 FROM ITS POSITION (no gap, no
// replay of what it already consumed) and unsubscribe the old node-1
// subscription.
TEST(GroupRoutingRegression, LeaderChangeResubscribesOnTheNewLeaderRegression) {
    GroupRig r(4107, 4108);
    const auto [n1, p1port] = r.addNode(4107);
    const auto [n2, p2port] = r.addNode(4107);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4107, ""}, {1, 1, 1, 1, 4107, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    r.gw.committedOffsets[{1, 1}] = 10;

    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    EXPECT_EQ(s1.fromOffset, 10) << "a leader-node subscription starts at the committed offset";
    r.gw.pushRecords(s1, 10, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 5; }));

    // Failover: node 2 now leads partition 1.
    r.gw.subscribeLeader[1] = n2;
    r.gw.partitions = {{1, 0, 0, 1, 4107, ""}, {1, 1, 2, 2, 4107, ""}};
    r.gw.sendMetadata(r.gw.groupSessionIds.front(), /*correlationId=*/0);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }))
        << "no re-subscribe on the new leader after the leader change";
    const auto s2 = *r.subAt(1, n2);
    EXPECT_EQ(s2.fromOffset, 15) << "the new leader subscription resumes at the member's position";
    EXPECT_NE(s2.sessionId, s1.sessionId) << "node 2's own session";
    ASSERT_TRUE(r.drive([&] {
        for (std::size_t i = 0; i < r.gw.unsubscribed.size(); ++i) {
            if (r.gw.unsubscribed[i] == s1.subscriptionId && r.gw.unsubscribedAt[i] == n1) return true;
        }
        return false;
    })) << "the old leader's subscription was not unsubscribed";
    r.gw.pushRecords(s2, 15, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 7; }));
    EXPECT_EQ(led.next[1], 17);
}

// Defect 2 (DR test_cg_rebalance_stable[2]: one member never got an assignment
// in the final generation, the other got all partitions).  Mechanism: the
// Redirect answering a member's partition Subscribe was taken as a SEED-level
// redirect — the seed transport (= the group coordinator's session) moved to
// the partition leader and reconnected there.  From then on the member's
// Heartbeat / JoinGroup went to a node that is not the coordinator, every
// answer was NotCoordinator, so at the next rebalance it never re-joined (its
// coordinator member expired and the survivor took everything).  Here: the
// member is Stable at generation 1 with partition 1 redirected to node 1; the
// coordinator (node 0) starts generation 2 — the member must re-join on node 0
// and be assigned again.  RED on main: every group frame after the redirect
// arrives on node 1 (NotCoordinator), generation 2 never assigns.
TEST(GroupRoutingRegression, MemberRejoinsTheCoordinatorAfterARedirectedSubscribeRegression) {
    GroupRig r(4109, 4110);
    const auto [n1, p1port] = r.addNode(4109);
    r.gw.moreBrokers         = {{1, "127.0.0.1", p1port}};
    r.gw.partitions          = {{1, 0, 0, 1, 4109, ""}, {1, 1, /*stale*/ 0, 1, 4109, ""}};
    r.gw.subscribeLeader     = {{0, 0}, {1, n1}};
    r.gw.groupAssignment     = {{1, 0}, {1, 1}};
    r.gw.coordinatorEndpoint = 0;

    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 600;  // a heartbeat every 200 ms
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    Consumed led;
    wire(gc, led);
    std::vector<int32_t> assignedGens;  // generation of every onAssigned (duty thread = this thread)
    gc.onAssigned([&](std::vector<rc::TopicPartition>) { assignedGens.push_back(gc.generation()); });
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0 && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeRefusals >= 1 && !assignedGens.empty(); }))
        << "precondition: generation 1 assigned and partition 1's Subscribe redirected";
    r.gw.partitions = {{1, 0, 0, 1, 4109, ""}, {1, 1, 1, 2, 4109, ""}};  // metadata agrees from now on

    const int64_t seedSid = r.gw.groupSessionIds.front();
    r.gw.holdJoins = false;  // the coordinator answers re-joins at once from now on
    r.gw.startRebalance(/*gen=*/2, seedSid);
    ASSERT_TRUE(r.drive([&] { return !assignedGens.empty() && assignedGens.back() == 2; }, 2'000'000))
        << "the member never re-joined the coordinator for generation 2 (NotCoordinator answers: "
        << r.gw.notCoordinatorAnswers << ")";
    EXPECT_EQ(r.gw.notCoordinatorAnswers, 0) << "a group frame reached a node that is not the coordinator";
    for (std::size_t i = 0; i < r.gw.groupEndpoints.size(); ++i) {
        EXPECT_EQ(r.gw.groupEndpoints[i], 0) << "group frame " << i << " left the coordinator node";
    }
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }));
    r.gw.pushRecords(*r.subAt(1, n1), 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] >= 2; }));
}

// ===========================================================================
// Fix round 1 (review 2026-10-04: C1, M1, M3, MEDIUM-2/3/4/7)
// ===========================================================================

namespace {
// Committed offsets of (topic 1, partition) in every OffsetCommit the gateway received.
std::vector<int64_t> committedOf(const test_support::InvokerGateway& gw, int32_t partition) {
    std::vector<int64_t> v;
    for (const auto& f : gw.offsetCommits) {
        const auto oc = rune::protocol::client::OffsetCommit::decode(f.data(), f.size());
        if (!oc) continue;
        for (const auto& e : oc->entries) {
            if (e.topicId == 1 && e.partition == partition) v.push_back(e.offset);
        }
    }
    return v;
}
// The newest subscription of (topic 1, p) on endpoint ep made after `after` (by corrId), or nullptr.
const test_support::InvokerGateway::Subscribed* newSubAt(const test_support::InvokerGateway& gw, int32_t p, int ep,
                                                          int64_t afterCorr) {
    const auto* s = gw.latestSubscription(1, p);
    return (s != nullptr && s->endpoint == ep && s->corrId != afterCorr) ? s : nullptr;
}
} // namespace

// C1: a retired leader-node subscription's records never seed or advance the
// position.  The member is Stable at 5 on node 1, the coordinator starts a
// rebalance (heartbeats answer RebalanceInProgress), node 1 keeps pushing the
// OLD subscription while the member re-joins, and one more old-subscription
// batch (10) lands after the member is Stable again but before its OffsetFetch
// answer.  RED (before the fix): that stray batch seeded the position (11), the
// new node-1 subscription started at 11 and auto-commits skipped 5..10 — silent
// loss.  Now: the new subscription starts at the committed offset (5) and the
// commits never pass what was consumed contiguously.
TEST(GroupRoutingRegression, StaleRecordsAfterRejoinNeverMoveThePositionRegression) {
    GroupRig r(4111, 4112);
    const auto [n1, p1port] = r.addNode(4111);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4111, ""}, {1, 1, 1, 1, 4111, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};

    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs     = 600;  // a heartbeat every 200 ms
    gcfg.autoCommitIntervalMs = 100;
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    Consumed led;
    int64_t maxContiguousEnd = 0;  // end of the contiguous prefix consumed on p1
    gc.onRecords([&](const rc::RecordSet& rs) {
        led.records[rs.partition] += rs.recordCount;
        if (rs.partition == 1 && rs.baseOffset <= maxContiguousEnd) {
            maxContiguousEnd = std::max<int64_t>(maxContiguousEnd, rs.baseOffset + rs.recordCount);
        }
    });
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s1, 0, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 5; }));

    // The coordinator rebalances: the member learns it from a heartbeat.
    r.gw.rebalancing = true;
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }, 2'000'000)) << "the member never re-joined";
    r.gw.pushRecords(s1, 5, 5);  // node 1 still pushes the old subscription
    r.gw.committedOffsets[{1, 1}] = 5;
    r.gw.holdOffsetFetch          = true;
    const int fetchesBefore = r.gw.offsetFetches;
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.gw.offsetFetches > fetchesBefore; })) << "never Stable again";
    // Stable, OffsetFetch unanswered: a stray old-subscription batch arrives.
    r.gw.pushRecords(s1, 10, 1);
    EXPECT_FALSE(r.drive([&] { return led.records[1] > 5; }, 20'000)) << "a retired subscription's batch was delivered";
    r.gw.releaseOffsetFetches();

    ASSERT_TRUE(r.drive([&] { return newSubAt(r.gw, 1, n1, s1.corrId) != nullptr; }))
        << "no node-1 subscription after the rebalance";
    const auto s2 = *newSubAt(r.gw, 1, n1, s1.corrId);
    EXPECT_EQ(s2.fromOffset, 5) << "the new subscription must start at the committed offset";
    r.gw.pushRecords(s2, 5, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 10; }));
    ASSERT_TRUE(r.drive([&] { return !committedOf(r.gw, 1).empty() && committedOf(r.gw, 1).back() == 10; }));
    for (const int64_t c : committedOf(r.gw, 1)) {
        EXPECT_LE(c, maxContiguousEnd) << "committed past what was consumed contiguously";
    }
}

// MEDIUM-3: a batch entirely below the position (a re-delivery) is skipped,
// never handed to the application again.  RED: it was delivered.
TEST(GroupRoutingRegression, BatchBelowThePositionIsNotRedeliveredRegression) {
    GroupRig r(4117, 4118);
    const auto [n1, p1port] = r.addNode(4117);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4117, ""}, {1, 1, 1, 1, 4117, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s1, 0, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 5; }));
    r.gw.pushRecords(s1, 1, 3);  // entirely below the position 5
    r.gw.pushRecords(s1, 5, 1);
    ASSERT_TRUE(r.drive([&] { return led.next[1] == 6; }));
    EXPECT_EQ(led.records[1], 6) << "a batch below the position was delivered again";
}

// M1 (scenario A): node 1, leader of p1, dies.  The client gets no Redirect
// (it only consumes) and nobody feeds it metadata: losing the node-1 session
// must itself ask for metadata, and the answer (node 2 leads) re-routes p1,
// which resumes on node 2 from the member's position.  RED: p1 stalled forever.
TEST(GroupRoutingRegression, DeadLeaderNodeSessionReroutesItsPartitionsRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;   // 50 ms
    o.responseTimeoutNs = 400'000'000;  // the session machine's own staleness rule
    GroupRig r(4119, 4120, o);
    const auto [n1, p1port] = r.addNode(4119);
    const auto [n2, p2port] = r.addNode(4119);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4119, ""}, {1, 1, 1, 1, 4119, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    r.gw.pushRecords(*r.subAt(1, n1), 0, 3);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 3; }));

    // Node 1 dies; the cluster elects node 2 for p1 (what metadata now says).
    r.gw.deadEndpoints.insert(n1);
    r.gw.partitions      = {{1, 0, 0, 1, 4119, ""}, {1, 1, 2, 2, 4119, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }, 20'000'000))
        << "p1 never re-routed to node 2 after node 1 died";
    EXPECT_EQ(r.subAt(1, n2)->fromOffset, 3) << "resumes at the member's position";
    r.gw.pushRecords(*r.subAt(1, n2), 3, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 5; }));
}

// M1 (scenario B): node 1 is unreachable when the member starts — its session
// exhausts its first-connect budget (terminal Disconnected).  When node 1 comes
// back (still the leader) the session is re-driven and p1 is consumed.  RED: the
// dead session was re-used forever and p1 never consumed.
TEST(GroupRoutingRegression, NeverConnectedLeaderSessionIsRevivedRegression) {
    RigOptions o;
    o.connectTimeoutMs = 300;  // a short first-connect budget
    GroupRig r(4121, 4122, o);
    const auto [n1, p1port] = r.addNode(4121);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4121, ""}, {1, 1, 1, 1, 4121, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.silentEndpoints.insert(n1);  // node 1 does not answer connects
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    // One connect episode is (1 + maxConnectRetries) attempts with a fast retry
    // each (~8 ConnectRequests); 12 means the session was re-driven after its
    // budget ran out.
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[n1] >= 12; }, 40'000'000))
        << "the node-1 session gave up for good (" << r.gw.connectsAt[n1] << " connects)";
    r.gw.silentEndpoints.clear();  // node 1 is back
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 20'000'000));
    r.gw.pushRecords(*r.subAt(1, n1), 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 2; }));
}

// MEDIUM-2: the OffsetFetch answer carries an error for the partition (here
// NotCoordinator): the start offset is fetched again, and the leader-node
// subscription is made.  RED: the partition was never subscribed.
TEST(GroupRoutingRegression, ErroredOffsetFetchIsRetriedRegression) {
    GroupRig r(4123, 4124);
    const auto [n1, p1port] = r.addNode(4123);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4123, ""}, {1, 1, 1, 1, 4123, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.offsetFetchErrorsNext = 1;
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 4'000'000))
        << "an errored OffsetFetch left the partition unsubscribed (" << r.gw.offsetFetches << " fetches)";
    EXPECT_GE(r.gw.offsetFetches, 2);
}

// M3 / redesign: broker session ids collide across nodes (both nodes assign
// 42) but subscription ids are cluster-unique (node prefix, like the broker
// now allocates them): a plain subscription on topic b (seed) and the group
// member's p1 subscription on node 1 each get only their own records.  RED
// (main / fix round 0): node 1's group records also reached the topic-b handler
// when the per-node subscription ids collided.
TEST(GroupRoutingRegression, CollidingSessionIdsNeverCrossDeliverRegression) {
    GroupRig r(4125, 4126);
    const auto [n1, p1port] = r.addNode(4125);
    r.gw.uniqueSessionIds  = false;  // every node assigns session id 42
    r.gw.perEndpointSubIds = true;   // per-node counters, node-prefixed (cluster-unique)
    r.gw.moreBrokers       = {{1, "127.0.0.1", p1port}};
    r.gw.partitions        = {{1, 0, 0, 1, 4125, ""}, {1, 1, 1, 1, 4125, ""}, {2, 0, 0, 1, 4125, ""}};
    r.gw.subscribeLeader   = {{1, n1}};
    r.gw.groupAssignment   = {{1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    int plain = 0;
    auto sub = r.client.subscribe(r.client.topic("b"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record&) { ++plain; });
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] {
        const auto* b = r.gw.latestSubscription(2, 0);
        return b != nullptr && sub.subscriptionId() > 0 && r.subAt(1, n1) != nullptr;
    }));
    const auto sb = *r.gw.latestSubscription(2, 0);
    const auto s1 = *r.subAt(1, n1);
    ASSERT_EQ(sb.sessionId, s1.sessionId) << "precondition: colliding session ids";
    ASSERT_NE(sb.subscriptionId, s1.subscriptionId);
    r.gw.pushRecords(s1, 0, 3);
    r.gw.pushRecords(sb, 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 3 && plain >= 2; }));
    r.gw.pushRecords(sb, 2, 1);  // a marker: once it arrived, everything before it did
    ASSERT_TRUE(r.drive([&] { return plain >= 3; }));
    EXPECT_EQ(plain, 3) << "node 1's group records reached the plain topic-b subscription";
    EXPECT_EQ(led.records[1], 3);
}

// MEDIUM-4: while a consumer group is active the seed session is the group
// coordinator's.  A produce Redirect arriving before any metadata route exists
// must follow the partition (route + leader session), never move the seed.
// RED: the seed transport was retargeted to the redirect target.
TEST(GroupRoutingRegression, ProduceRedirectNeverMovesTheCoordinatorSessionRegression) {
    GroupRig r(4127, 4128);
    const auto [n1, p1port] = r.addNode(4127);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {};  // no routes yet
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    rc::ProducerConfig pc;
    pc.lingerUs = 0;
    auto p = r.client.producer(pc);
    r.gw.redirectNext = 1;
    r.gw.redirectTo   = udp(p1port);
    const std::vector<uint8_t> val{1, 2, 3};
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{7}), std::span<const uint8_t>(val), r.cq, t),
              rc::SendResult::Ok);
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }));
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_EQ(r.client.testTransport().ingressChannel(), r.c.gatewayChannel)
        << "a produce redirect moved the coordinator's (seed) session";
    ASSERT_FALSE(r.gw.produced.empty());
    EXPECT_EQ(r.gw.produced.back().endpoint, n1) << "the redirected batch follows its partition to node 1";
}

// MEDIUM-7: a group consumer created AFTER connect() joins at once.  RED: it
// stayed Unjoined until the next seed (re)connect.
TEST(GroupRoutingRegression, GroupConsumerCreatedAfterConnectJoinsRegression) {
    GroupRig r(4129, 4130);
    r.gw.holdJoins       = false;
    r.gw.groupAssignment = {{1, 0}};
    r.connect();
    auto gc = r.client.subscribeGroup("g", {"t"});
    int assigned = 0;
    gc.onAssigned([&](std::vector<rc::TopicPartition>) { ++assigned; });
    ASSERT_TRUE(r.drive([&] { return assigned == 1; }, 2'000'000))
        << "a consumer created after connect() never joined (" << r.gw.joinGroups << " JoinGroups)";
}

// ===========================================================================
// Fix round 2 (redesign: cluster-unique ids, explicit leader subscriptions,
// legitimate gaps)
// ===========================================================================

namespace {
// A rig with p0 led by node 0 (seed) and p1 by node 1, both assigned, both
// subscribed (start offsets known: nothing committed = 0).
struct TwoLeaders {
    GroupRig r;
    int      n1 = 0, p1port = 0;
    Consumed led;
    std::unique_ptr<rc::GroupConsumer> gc;
    explicit TwoLeaders(int32_t in, int32_t eg, RigOptions o = {}, rc::GroupConfig gcfg = {})
        : r(in, eg, o) {
        std::tie(n1, p1port) = r.addNode(in);
        r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
        r.gw.partitions      = {{1, 0, 0, 1, in, ""}, {1, 1, 1, 1, in, ""}};
        r.gw.subscribeLeader = {{0, 0}, {1, n1}};
        r.gw.groupAssignment = {{1, 0}, {1, 1}};
        gc = std::make_unique<rc::GroupConsumer>(r.client.subscribeGroup("g", {"t"}, gcfg));
        wire(*gc, led);
    }
    void start() {
        r.connect();
        ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
        r.gw.releaseJoins();
        ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }));
    }
};
} // namespace

// CRITICAL-1 (re-review): offsets the broker never pushes (a transaction's
// COMMIT/ABORT marker, aborted batches, compaction holes) are legitimate gaps:
// consumption continues past them on a leader-node partition AND on the seed's.
// RED (fix round 1): the gap re-subscribed, then parked — never advanced.
TEST(GroupRoutingRegression, TransactionMarkerGapIsConsumedPastRegression) {
    TwoLeaders t(4131, 4132);
    t.start();
    for (int p = 0; p < 2; ++p) {
        const auto s = p == 0 ? *t.r.subAt(0, 0) : *t.r.subAt(1, t.n1);
        t.r.gw.pushRecords(s, 0, 5);   // 0..4 (committed txn)
        t.r.gw.pushRecords(s, 6, 3);   // 5 = its COMMIT marker (never pushed); 6..8
        t.r.gw.pushRecords(s, 12, 2);  // 9..11 aborted + marker (read_committed skips); 12..13
    }
    ASSERT_TRUE(t.r.drive([&] { return t.led.records[0] == 10 && t.led.records[1] == 10; }))
        << "p0=" << t.led.records[0] << " p1=" << t.led.records[1];
    EXPECT_EQ(t.led.next[0], 14);
    EXPECT_EQ(t.led.next[1], 14);
}

// MAJOR-1 (re-review): Records that come from a socket no session's
// ConnectResponse came from (the broker's MDC fallback: a worker's own
// publication), with broker session ids colliding across nodes, are still
// consumed — demultiplexed by the (cluster-unique) subscriptionId.  RED (fix
// round 1): the unknown source + colliding ids dropped every batch.
TEST(GroupRoutingRegression, RecordsFromANonSessionSourceAreConsumedRegression) {
    GroupRig r(4133, 4134);
    const auto [n1, p1port] = r.addNode(4133);
    const auto [w1, w1port] = r.addNode(4133);  // node 1's worker publication (no session)
    (void)w1port;
    r.gw.uniqueSessionIds  = false;  // both nodes assign session id 42
    r.gw.perEndpointSubIds = true;
    r.gw.moreBrokers       = {{1, "127.0.0.1", p1port}};
    r.gw.partitions        = {{1, 0, 0, 1, 4133, ""}, {1, 1, 1, 1, 4133, ""}};
    r.gw.subscribeLeader   = {{0, 0}, {1, n1}};
    r.gw.groupAssignment   = {{1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    r.gw.pushRecordsFrom(*r.subAt(1, n1), 0, 4, w1);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 4; })) << "MDC-fallback records were dropped";
}

// MAJOR-2 (re-review): a ConnectResponse no session was waiting for (a stale /
// duplicate answer) changes nothing: no new connect episode, no re-subscribe,
// the live subscriptions keep delivering.  RED (fix round 1): it bumped the
// seed's episode — every seed subscription was re-made and its records dropped.
TEST(GroupRoutingRegression, StaleConnectResponseChangesNothingRegression) {
    TwoLeaders t(4135, 4136);
    t.start();
    const auto s0 = *t.r.subAt(0, 0);
    const std::size_t subsBefore = t.r.gw.subscribed.size();
    // A duplicate OK for the seed's (long answered) first connect correlationId.
    uint8_t out[64];
    const std::size_t n = rune::client::codec::encodeConnectResponse(
        out, sizeof out, 0, 0, s0.sessionId, 0, /*correlationId=*/1);
    t.r.gw.send(0, out, n);
    t.r.gw.pushRecords(s0, 0, 3);  // after the stale frame, on the same publication
    ASSERT_TRUE(t.r.drive([&] { return t.led.records[0] == 3; }))
        << "the live seed subscription stopped delivering after a stale ConnectResponse";
    // A positive marker (LOW-8): a Ping sent now is answered only after every
    // frame the client sent before it on the seed reached the gateway.
    const uint8_t echo[1] = {7};
    const rc::Token pt = t.r.nextToken++;
    ASSERT_EQ(t.r.client.ping(std::span<const uint8_t>(echo), t.r.cq, pt), rc::OpResult::Accepted);
    bool pong = false;
    ASSERT_TRUE(t.r.drive([&] {
        t.r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) { if (cc.token == pt) pong = true; });
        return pong;
    }));
    EXPECT_EQ(t.r.gw.subscribed.size(), subsBefore) << "a stale ConnectResponse re-made subscriptions";
}

// MEDIUM-1a (re-review): the first metadata answer after node 1 dies still
// names node 1 (the client noticed before the cluster re-elected); with
// reconnect-forever sessions node 1's session never becomes Disconnected.  The
// client keeps asking (backoff-paced) and moves p1 once node 2 is named.  RED
// (fix round 1): one metadata request per loss — p1 stalled.
TEST(GroupRoutingRegression, StaleMetadataAfterALeaderLossIsAskedAgainRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 400'000'000;
    o.reconnectForever  = true;
    GroupRig r(4137, 4138, o);
    const auto [n1, p1port] = r.addNode(4137);
    const auto [n2, p2port] = r.addNode(4137);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4137, ""}, {1, 1, 1, 1, 4137, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const int mdBefore = r.gw.metadataRequests;
    r.gw.deadEndpoints.insert(n1);  // metadata still names node 1 (stale)
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > mdBefore; }, 20'000'000))
        << "losing node 1 never asked for metadata";
    // That answer was stale; the cluster now elects node 2.
    r.gw.partitions      = {{1, 0, 0, 1, 4137, ""}, {1, 1, 2, 2, 4137, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }, 40'000'000))
        << "p1 never moved: the client stopped asking after one stale answer (" << r.gw.metadataRequests
        << " requests)";
}

// MEDIUM-1b (re-review): node 1's session became terminally Disconnected BEFORE
// the group used it (a producer opened it while node 1 was unreachable).  Once
// p1 is assigned there (node 1 reachable again), the session is re-driven.  RED
// (fix round 1): only a NEWLY Disconnected session was revived — p1 stalled.
TEST(GroupRoutingRegression, AlreadyDisconnectedLeaderSessionIsRevivedRegression) {
    RigOptions o;
    o.connectTimeoutMs = 300;
    GroupRig r(4139, 4140, o);
    const auto [n1, p1port] = r.addNode(4139);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4139, ""}, {1, 1, 1, 1, 4139, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.silentEndpoints.insert(n1);
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    // A producer opens node 1's session (p1's route) — node 1 never answers.
    rc::ProducerConfig pc;
    pc.lingerUs = 0;
    auto p = r.client.producer(pc);
    const std::vector<uint8_t> val{1};
    ASSERT_EQ(p.trySendTo(r.client.topic("t"), 1, rc::Key(uint64_t{1}), std::span<const uint8_t>(val)),
              rc::SendResult::Ok);
    // DR-7 review M1: a routed session that spends its first-connect budget is
    // re-driven at once (never left terminally Disconnected) — the precondition is
    // that node 1's session went through that give-up (first attempt + 3 retries).
    ASSERT_TRUE(r.drive([&] {
        return r.client.testSessionCount() > 1 && r.client.testSessionConnectFailuresAt(1) >= 4;
    }, 40'000'000)) << "precondition: node 1's session spent its first-connect budget";
    // Node 1 is reachable again; the group now assigns p1 there.
    r.gw.silentEndpoints.clear();
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 20'000'000))
        << "the already-Disconnected node-1 session was never re-driven for the group";
    r.gw.pushRecords(*r.subAt(1, n1), 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 2; }));
}

// MEDIUM-3 (re-review): while a group is active, a plain subscription to a
// partition another node leads finishes with a distinct, documented reason
// (never a seed redirect).
TEST(GroupRoutingRegression, PlainSubscribeRedirectedWhileGroupActiveReportsWhyRegression) {
    GroupRig r(4141, 4142);
    const auto [n1, p1port] = r.addNode(4141);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4141, ""}, {2, 0, 1, 1, 4141, ""}};
    r.gw.subscribeLeader = {{0, n1}};  // topic b / partition 0: node 1 (the fake keys by partition)
    r.gw.groupAssignment = {{1, 0}};
    r.gw.holdJoins       = false;
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("b"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::LeaderElsewhereWhileGroupActive);
    EXPECT_EQ(r.client.testTransport().ingressChannel(), r.c.gatewayChannel) << "the seed moved";
}


// ===========================================================================
// Fix round 3 (exact instance identity by correlationId, version gate,
// unanswered Subscribes re-sent on events)
// ===========================================================================

// MAJOR-1 (round 3): a bound PLAIN subscription of this Client tails p0 on the
// seed from far ahead while the group member's own p0 Subscribe is still
// unanswered: the plain subscription's pushes never feed the member.  RED
// (round 2): the member accepted them pre-bind (same session, unretired id),
// jumped to the log end and skipped its own records.
TEST(GroupRoutingRegression, PlainSubscriptionNeverFeedsAPendingGroupSubscriptionRegression) {
    GroupRig r(4143, 4144);
    r.gw.partitions      = {{1, 0, 0, 1, 4143, ""}};
    r.gw.groupAssignment = {{1, 0}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    int plain = 0;
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::latest()},
                                  [&](const rc::Record&) { ++plain; });
    ASSERT_TRUE(r.drive([&] { return sub.subscriptionId() > 0 && !r.gw.pendingJoins.empty(); }));
    const auto plainSub = *r.gw.latestSubscription(1, 0);
    r.gw.dropSubscribeResponsesNext = 1;  // the member's Subscribe stays unanswered
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] {
        const auto* s = r.gw.latestSubscription(1, 0);
        return s != nullptr && s->corrId != plainSub.corrId;
    }));
    const auto groupSub = *r.gw.latestSubscription(1, 0);
    ASSERT_EQ(groupSub.fromOffset, 0);
    r.gw.pushRecords(plainSub, 900, 3);  // the plain subscription, far ahead
    ASSERT_TRUE(r.drive([&] { return plain == 3; }));
    r.gw.pushRecords(groupSub, 0, 2);     // the member's own records
    ASSERT_TRUE(r.drive([&] { return led.records[0] >= 2; }));
    EXPECT_EQ(led.records[0], 2) << "the member consumed the plain subscription's batch";
    EXPECT_EQ(led.next[0], 2);
}

// MEDIUM-2 (round 3): the SubscribeResponse for p1 (node 1) is lost and p1 is
// quiet; the coordinator then revokes and re-assigns p1.  The member must
// subscribe p1 again (on the next routing event) and consume.  RED (round 2):
// the abandoned unanswered instance blocked every later Subscribe on that
// session — p1 stalled forever.
TEST(GroupRoutingRegression, LostSubscribeResponseNeverBlocksAReassignRegression) {
    GroupRig r(4145, 4146);
    const auto [n1, p1port] = r.addNode(4145);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4145, ""}, {1, 1, 1, 1, 4145, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.dropSubscribeResponsesNext = 2;  // both answers lost (the subscriptions exist)
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }));
    const auto first = *r.subAt(1, n1);
    const int64_t sid = r.gw.groupSessionIds.front();
    r.gw.sendAssign(sid, 2, {{1, 0}});          // p1 revoked
    r.gw.sendAssign(sid, 3, {{1, 0}, {1, 1}});  // and assigned back
    ASSERT_TRUE(r.drive([&] { return newSubAt(r.gw, 1, n1, first.corrId) != nullptr; }, 2'000'000))
        << "p1 was never subscribed again after a lost SubscribeResponse";
    r.gw.pushRecords(*newSubAt(r.gw, 1, n1, first.corrId), 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 2; }));
}

// MAJOR-2 (round 3): against a broker that predates explicit group
// subscriptions (no ConnectResponse.brokerVersion) a GroupConsumer never
// joins: it finishes with GroupFailReason::BrokerTooOld.  RED (round 2): it
// joined (and would lose records on the old broker's auto-subscriptions).
TEST(GroupRoutingRegression, GroupConsumerRefusesABrokerTooOldRegression) {
    GroupRig r(4147, 4148);
    r.gw.advertisedBrokerVersion = -1;  // an older broker's 38-byte ConnectResponse
    r.gw.groupAssignment         = {{1, 0}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 400'000)) << "the consumer ran against an old broker";
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::BrokerTooOld);
    EXPECT_EQ(r.gw.joinGroups, 0);
}

// MAJOR-2 (round 3): a broker coordinator refuses this client's JoinGroup
// (ProtocolVersion): non-retriable — the consumer finishes with
// GroupFailReason::RefusedByBroker, no JoinGroup storm.  RED (round 2): it
// re-joined forever.
TEST(GroupRoutingRegression, JoinGroupRefusedForVersionFinishesTheConsumerRegression) {
    GroupRig r(4149, 4150);
    r.gw.holdJoins     = false;
    r.gw.joinErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::ProtocolVersion);
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 400'000)) << "joins: " << r.gw.joinGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker);
    EXPECT_EQ(r.gw.joinGroups, 1);
}


// CG round 4 (A-MEDIUM-3): a rolling upgrade — the seed / coordinator runs v3
// but p1's leader node still runs an older broker (its pushes would carry no
// Subscribe correlationId: every batch dropped, a silent stall).  The member
// never subscribes p1 there (parked, logged) and keeps consuming p0.  RED
// (round 3): it subscribed p1 on the old node.
TEST(GroupRoutingRegression, PartitionLedByAnOldNodeIsParkedNotSilentlyStalledRegression) {
    GroupRig r(4151, 4152);
    const auto [n1, p1port] = r.addNode(4151);
    r.gw.advertisedBrokerVersionAt[n1] = -1;  // node 1: an older broker's ConnectResponse
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4151, ""}, {1, 1, 1, 1, 4151, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.gw.connectsAt[n1] > 0; }));
    r.gw.pushRecords(*r.subAt(0, 0), 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[0] == 2; }));
    // Marker: a Ping answered after every frame sent before it reached the gateway.
    const uint8_t echo[1] = {1};
    const rc::Token pt = r.nextToken++;
    ASSERT_EQ(r.client.ping(std::span<const uint8_t>(echo), r.cq, pt), rc::OpResult::Accepted);
    bool pong = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) { if (cc.token == pt) pong = true; });
        return pong;
    }));
    EXPECT_EQ(r.subAt(1, n1), nullptr) << "subscribed a partition on a node that cannot tag its pushes";
    EXPECT_FALSE(gc.finished()) << "the rest of the assignment keeps running";
}

// R4-MEDIUM-1: a partition whose leader node is pre-v3 is skipped WITHOUT
// polling metadata (nothing to ask — the node itself must change) — the
// metadata request count stays flat while it is assigned — and it is
// subscribed once that node is upgraded and its session reconnects
// (advertising v3).  RED (round 4): parking set metadataWanted and every
// metadata answer unparked it — ~5 requests per second, a log line each.
TEST(GroupRoutingRegression, OldLeaderNodeNeverPollsMetadataAndResumesAfterUpgradeRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 400'000'000;
    GroupRig r(4153, 4154, o);
    const auto [n1, p1port] = r.addNode(4153);
    r.gw.advertisedBrokerVersionAt[n1] = -1;  // node 1: pre-v3
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4153, ""}, {1, 1, 1, 1, 4153, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.gw.connectsAt[n1] > 0; }));
    // Settle (bounded) until the old node's session is connected, then measure.
    ASSERT_TRUE(r.drive([&] { return r.client.testSessionCount() > 1 &&
                                     r.client.testSessionStateAt(1) == rc::session::SessionState::Connected; }));
    const int mdBefore = r.gw.metadataRequests;
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);  // a bounded window, not a sleep
    while (std::chrono::steady_clock::now() < until) {
        r.client.doWork();
        r.gw.pump();
    }
    EXPECT_EQ(r.gw.metadataRequests, mdBefore) << "a pre-v3 leader made the client poll metadata";
    EXPECT_EQ(r.subAt(1, n1), nullptr);
    EXPECT_FALSE(gc.finished());

    // Node 1 is upgraded: it restarts (its session is lost) and comes back v3.
    r.gw.advertisedBrokerVersionAt[n1] = rune::protocol::client::kBrokerVersion;
    r.gw.deadEndpoints.insert(n1);
    ASSERT_TRUE(r.drive([&] { return r.client.testSessionStateAt(1) != rc::session::SessionState::Connected; },
                        20'000'000));
    r.gw.deadEndpoints.clear();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 40'000'000))
        << "the upgraded node's partition was never subscribed";
    r.gw.pushRecords(*r.subAt(1, n1), 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 2; }));
}

// DR-7 (dockerized test_dr7_leader_crash_restart_rejoins_follower[1], 2026-10-05):
// the client fetched metadata just before the broker failed the partition over, so
// its route named the KILLED leader.  That node's session never connects — nothing
// reaches any broker (no NotLeader / Redirect) and nothing asked for metadata again:
// every batch of the partition failed DeliveryFailed after the delivery timeout
// while the cluster had long elected a new leader.  A failed connect attempt of a
// session a partition routes to asks for metadata (Kafka: a failed connection
// requests a metadata update); the answer re-routes the held batch.
TEST(GroupRoutingRegression, ProduceRoutedToADeadLeaderFollowsTheFailoverRegression) {
    RigOptions o;
    o.connectTimeoutMs = 300;
    GroupRig r(4141, 4142, o);
    const auto [n1, p1port] = r.addNode(4141);
    r.gw.silentEndpoints.insert(n1);                // node 1 was killed: never answers
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {{2, 0, 1, 1, 4141, ""}};    // b/0 led by node 1 (about to be stale)
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(2, 0).empty(); }));
    rc::ProducerConfig pc;
    pc.lingerUs          = 0;
    pc.deliveryTimeoutMs = 600'000;                 // the delivery timeout never decides this test
    auto p = r.client.producer(pc);
    // The cluster failed b/0 over to node 0 (epoch 2); nobody tells the client.
    r.gw.partitions = {{2, 0, 0, 2, 4141, ""}};
    const int md = r.gw.metadataRequests;
    const std::vector<uint8_t> val{1};
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("b"), rc::Key(uint64_t{1}), std::span<const uint8_t>(val), r.cq, t),
              rc::SendResult::Ok);
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, 4'000'000)) << "the batch stayed held on the dead leader's session (no metadata refresh)";
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_GT(r.gw.metadataRequests, md) << "a failed connect to the partition's leader asked for no metadata";
    ASSERT_FALSE(r.gw.produced.empty());
    EXPECT_EQ(r.gw.produced.back().endpoint, 0) << "the batch follows the failover to node 0";
}

// Review MAJOR-1: the election outlasts the dead ex-leader session's first-connect
// budget (it gives up after its retries).  The client must keep asking — the route
// converges whenever the failover completes.  RED (9eccba0): the given-up session
// raised no more events, the stale route stuck, every batch hit the delivery timeout.
TEST(GroupRoutingRegression, ProduceFollowsAFailoverThatOutlastsTheConnectBudgetRegression) {
    RigOptions o;
    o.connectTimeoutMs = 300;
    GroupRig r(4143, 4144, o);
    const auto [n1, p1port] = r.addNode(4143);
    r.gw.deadEndpoints.insert(n1);                  // node 1 was killed
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {{2, 0, 1, 1, 4143, ""}};    // b/0 still advertised on node 1 (election pending)
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(2, 0).empty(); }));
    rc::ProducerConfig pc;
    pc.lingerUs          = 0;
    pc.deliveryTimeoutMs = 600'000;
    auto p = r.client.producer(pc);
    const std::vector<uint8_t> val{1};
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("b"), rc::Key(uint64_t{1}), std::span<const uint8_t>(val), r.cq, t),
              rc::SendResult::Ok);
    // Many failed attempts later (well past the first-connect budget) the client
    // is still asking — only then does the cluster finish the failover.
    const int md0 = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests >= md0 + 8; }, 40'000'000))
        << "the client stopped asking once node 1's session gave up";
    r.gw.partitions = {{2, 0, 0, 2, 4143, ""}};
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, 8'000'000));
    EXPECT_EQ(st, rc::OpStatus::Ok);
    ASSERT_FALSE(r.gw.produced.empty());
    EXPECT_EQ(r.gw.produced.back().endpoint, 0);
}

// Review MEDIUM-1/2: the leader was Connected and took the batch, then died
// silently (no answer, no NotLeader).  The attempt timeout / the lost session asks
// for metadata and the stored batch is re-offered on the new leader at once (same
// identity).  RED (9eccba0): no refresh and a node-route move re-offered nothing.
TEST(GroupRoutingRegression, BatchSentToALeaderThatDiesFollowsTheFailoverRegression) {
    RigOptions o;
    o.connectTimeoutMs  = 300;
    o.keepaliveNs       = 600'000'000'000;   // the session never notices the death by itself:
    o.responseTimeoutNs = 600'000'000'000;   // only the attempt timeout can trigger the refresh
    GroupRig r(4145, 4146, o);
    const auto [n1, p1port] = r.addNode(4145);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {{2, 0, 1, 1, 4145, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(2, 0).empty(); }));
    rc::ProducerConfig pc;
    pc.lingerUs          = 0;
    pc.deliveryTimeoutMs = 600'000;
    pc.produceTimeoutMs  = 500;
    auto p = r.client.producer(pc);
    r.gw.holdPartitions = {0};                      // node 1 takes the batch, never acks
    const std::vector<uint8_t> val{1};
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("b"), rc::Key(uint64_t{1}), std::span<const uint8_t>(val), r.cq, t),
              rc::SendResult::Ok);
    ASSERT_TRUE(r.drive([&] { return !r.gw.held.empty() && r.gw.held.back().endpoint == n1; }, 8'000'000));
    // Node 1 dies; the cluster fails b/0 over to node 0.
    r.gw.deadEndpoints.insert(n1);
    r.gw.holdPartitions.clear();
    r.gw.partitions = {{2, 0, 0, 2, 4145, ""}};
    const int md = r.gw.metadataRequests;
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, 8'000'000)) << "the batch stayed on the dead leader";
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_GT(r.gw.metadataRequests, md);
    EXPECT_EQ(r.gw.produced.back().endpoint, 0);
}

// DR-7 (rerun, 2026-10-05): the DR client's connect timeout (40 s) is longer than
// its delivery timeout (30 s), so a connect FAILURE never comes in time — the batch
// held on the dead ex-leader's still-connecting session must itself ask for
// metadata (Kafka: a leader that is not ready requests a metadata update, paced by
// the retry backoff).  RED (9bcc6e6): no refresh before the delivery timeout.
TEST(GroupRoutingRegression, BatchHeldOnAConnectingDeadLeaderAsksForMetadataRegression) {
    RigOptions o;
    o.connectTimeoutMs = 600'000;                   // no connect failure within this test
    GroupRig r(4147, 4148, o);
    const auto [n1, p1port] = r.addNode(4147);
    r.gw.deadEndpoints.insert(n1);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {{2, 0, 1, 1, 4147, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(2, 0).empty(); }));
    rc::ProducerConfig pc;
    pc.lingerUs          = 0;
    pc.deliveryTimeoutMs = 600'000;
    auto p = r.client.producer(pc);
    r.gw.partitions = {{2, 0, 0, 2, 4147, ""}};    // already failed over; the client is not told
    const std::vector<uint8_t> val{1};
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(p.trySend(r.client.topic("b"), rc::Key(uint64_t{1}), std::span<const uint8_t>(val), r.cq, t),
              rc::SendResult::Ok);
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, 4'000'000)) << "the held batch never asked for metadata";
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_EQ(r.gw.produced.back().endpoint, 0);
}

// ===========================================================================
// D4 (broker backlog "group subscribe redirect", 2026-10-08): a member
// bootstrapped against a FOLLOWER.  The follower answers the coordinator
// request ErrorResponse{NotCoordinator, coordinator endpoint}; the seed session
// must move to the coordinator and the member join there at once — before the
// fix the group-active rule kept the seed on the follower and the member
// re-joined the follower forever (dockerized: 0 of 80 records consumed).
// ===========================================================================
TEST(GroupRoutingRegression, MemberOnAFollowerJoinsTheCoordinatorRegression) {
    GroupRig r(4131, 4132);
    const auto [n1, p1port] = r.addNode(4131);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    // Partition 0 is led by the coordinator (node 1); partition 1 by the
    // bootstrap follower (node 0) — after the seed moves to node 1, partition 1
    // still needs a session to node 0.
    r.gw.partitions            = {{1, 0, /*leader*/ n1, 1, 4131, ""}, {1, 1, /*leader*/ 0, 1, 4131, ""}};
    r.gw.subscribeLeader       = {{0, n1}, {1, 0}};
    r.gw.groupAssignment       = {{1, 0}, {1, 1}};
    r.gw.coordinatorEndpoint   = n1;                                     // node 1 is the coordinator
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.holdJoins             = false;

    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 30'000;   // the join deadline (30 s) must NOT be what un-sticks the member
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    Consumed got;
    wire(gc, got);
    bool assigned = false;
    gc.onAssigned([&](std::vector<rc::TopicPartition>) { assigned = true; });
    r.connect();
    const auto t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(r.drive([&] { return assigned; }, 4'000'000))
        << "the member never joined the coordinator (NotCoordinator answers: " << r.gw.notCoordinatorAnswers << ")";
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_LT(ms, 5'000) << "joined only after " << ms << " ms (a join deadline, not the redirect)";
    EXPECT_GE(r.gw.notCoordinatorAnswers, 1) << "precondition: the follower answered NotCoordinator";
    EXPECT_EQ(r.gw.groupEndpoints.back(), n1) << "the last group frame did not reach the coordinator";
    // Both partitions are consumed from their own leaders — including the one the
    // old seed node leads (the moved seed must not still claim that node).
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && r.subAt(1, 0) != nullptr; }, 4'000'000))
        << "a partition never subscribed on its leader (refusals=" << r.gw.subscribeRefusals << ")";
    r.gw.pushRecords(*r.subAt(0, n1), 0, 2);
    r.gw.pushRecords(*r.subAt(1, 0), 0, 3);
    ASSERT_TRUE(r.drive([&] { return got.records[0] >= 2 && got.records[1] >= 3; }, 4'000'000))
        << "records: p0=" << got.records[0] << " p1=" << got.records[1];

    // D4 coordinator session (review MAJOR-1/2/3): the seed never moves for a
    // coordinator answer — the coordinator gets its own (one-per-node) session.
    EXPECT_EQ(r.client.testSessionStateAt(0), rc::session::SessionState::Connected);
    EXPECT_EQ(r.gw.connectsAt[0], 1) << "the seed session was reconnected (it must not move)";
    EXPECT_EQ(r.gw.connectsAt[n1], 1) << "a second session was opened to the coordinator node";
    EXPECT_EQ(r.client.testSessionCount(), 2u) << "one session per node: the seed (node 0) + node 1";
    EXPECT_EQ(r.client.testTransport().ingressChannel(), r.c.gatewayChannel) << "the seed was retargeted";
    std::size_t first = r.gw.groupEndpoints.size();
    for (std::size_t i = 0; i < r.gw.groupEndpoints.size(); ++i) {
        if (r.gw.groupEndpoints[i] == n1) { first = i; break; }
    }
    ASSERT_LT(first, r.gw.groupEndpoints.size());
    const int64_t coordSid = r.gw.groupSessionIds[first];
    EXPECT_NE(coordSid, r.seedSid) << "coordinator frames carry the coordinator session's own id";
    for (std::size_t i = first; i < r.gw.groupEndpoints.size(); ++i) {
        EXPECT_EQ(r.gw.groupEndpoints[i], n1) << "group frame " << i << " after the join left the coordinator";
        EXPECT_EQ(r.gw.groupSessionIds[i], coordSid) << "group frame " << i << " carries another session's id";
    }
    EXPECT_EQ(r.subAt(1, 0)->sessionId, r.seedSid) << "the old seed node's partition rides the seed session";
}

// D4: a transactional producer bootstrapped on a FOLLOWER.  The follower answers
// InitProducerId ErrorResponse{NotCoordinator, coordinator endpoint}; the
// request must be re-sent on the coordinator's session (not wait out its op
// deadline, not move the seed).  RED without the coordinator route: the
// InitProducerId in flight on the follower is never answered.
TEST(GroupRoutingRegression, TxnProducerOnAFollowerInitsOnTheCoordinatorRegression) {
    GroupRig r(4133, 4134);
    const auto [n1, p1port] = r.addNode(4133);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, n1, 1, 4133, ""}, {1, 1, 0, 1, 4133, ""}};
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.initPidReply          = true;
    auto tp = r.client.transactionalProducer("tx-d4");
    r.connect();
    const auto t0 = std::chrono::steady_clock::now();
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(tp.initTransactions(r.cq, t), rc::OpResult::Accepted);
    bool done = false;
    rc::OpStatus st = rc::OpStatus::Closed;
    int64_t pid = 0;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; pid = cc.value; }
        });
        return done;
    }, 4'000'000)) << "InitProducerId never completed (NotCoordinator answers: " << r.gw.notCoordinatorAnswers << ")";
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_EQ(pid, 4242);
    EXPECT_LT(ms, 5'000) << "completed only after " << ms << " ms (an op deadline, not the redirect)";
    ASSERT_GE(r.gw.coordinatorEndpoints.size(), 2u);
    EXPECT_EQ(r.gw.coordinatorEndpoints.front(), 0) << "precondition: the first InitProducerId went to the follower";
    EXPECT_GE(r.gw.notCoordinatorAnswers, 1);
    EXPECT_EQ(r.gw.coordinatorEndpoints.back(), n1) << "the InitProducerId was not re-sent to the coordinator";
    EXPECT_NE(r.gw.coordinatorSessionIds.back(), r.seedSid) << "re-sent on the coordinator's own session";
    EXPECT_EQ(r.gw.connectsAt[0], 1) << "the seed session must not move / reconnect";
    EXPECT_EQ(r.client.testSessionStateAt(0), rc::session::SessionState::Connected);
}

// D4: coordinator failover while the member is Stable.  The old coordinator
// answers a Heartbeat ErrorResponse{NotCoordinator, new coordinator}; the
// member re-joins on the new coordinator's session — first seed -> node 1,
// then node 1 -> node 2 (a coordinator session to a coordinator session).
TEST(GroupRoutingRegression, CoordinatorFailoverWhileStableRejoinsTheNewCoordinatorRegression) {
    GroupRig r(4135, 4136);
    const auto [n1, p1port] = r.addNode(4135);
    const auto [n2, p2port] = r.addNode(4135);
    r.gw.moreBrokers         = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions          = {{1, 0, 0, 1, 4135, ""}, {1, 1, 0, 1, 4135, ""}};
    r.gw.groupAssignment     = {{1, 0}, {1, 1}};
    r.gw.coordinatorEndpoint = 0;   // the seed node is the coordinator at first
    r.gw.holdJoins           = false;

    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 600;   // a heartbeat every 200 ms
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    Consumed got;
    wire(gc, got);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 1 && r.gw.heartbeats >= 1; }, 4'000'000))
        << "precondition: Stable at generation 1 on the seed";

    // Failover 1: node 1 is the coordinator now; the seed node knows it.
    const int joinsBefore = r.gw.joinGroups;
    r.gw.groupGeneration       = 2;
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 2; }, 4'000'000))
        << "the member never re-joined the new coordinator (NotCoordinator answers: "
        << r.gw.notCoordinatorAnswers << ")";
    EXPECT_GT(r.gw.joinGroups, joinsBefore);
    EXPECT_EQ(r.gw.groupEndpoints.back(), n1);
    const int hb1 = r.gw.heartbeats;
    ASSERT_TRUE(r.drive([&] { return r.gw.heartbeats > hb1 && r.gw.groupEndpoints.back() == n1; }, 4'000'000))
        << "precondition: Stable on node 1 (heartbeating there)";

    // Failover 2: node 2 takes over; node 1 (the old coordinator) knows it.
    r.gw.groupGeneration       = 3;
    r.gw.coordinatorEndpoint   = n2;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p2port);
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 3; }, 4'000'000))
        << "the member never followed the coordinator to node 2";
    EXPECT_EQ(r.gw.groupEndpoints.back(), n2);
    EXPECT_EQ(r.gw.connectsAt[0], 1) << "the seed session must never move";
    EXPECT_EQ(r.gw.connectsAt[n1], 1);
    EXPECT_EQ(r.gw.connectsAt[n2], 1);
}

// D4 (review MEDIUM-1): a late NotCoordinator naming the coordinator the member
// already joined (an answer to a frame sent to the follower before the move)
// changes nothing — no rebalance, no new session, no reconnect.
TEST(GroupRoutingRegression, LateNotCoordinatorNamingTheCurrentCoordinatorIsANoOpRegression) {
    GroupRig r(4137, 4138);
    const auto [n1, p1port] = r.addNode(4137);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, n1, 1, 4137, ""}, {1, 1, n1, 1, 4137, ""}};
    r.gw.groupAssignment       = {{1, 0}, {1, 1}};
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.holdJoins             = false;

    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 600;   // a heartbeat every 200 ms
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    Consumed got;
    wire(gc, got);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 1 && r.gw.heartbeats >= 1; }, 4'000'000))
        << "precondition: Stable on the coordinator";
    const int joins    = r.gw.joinGroups;
    const int connects = r.gw.connects;
    const std::size_t sessions = r.client.testSessionCount();

    // The follower's late answer, naming the coordinator the member is on.
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator),
                   r.gw.coordinatorRedirectTo, r.seedSid, 0);
    const int hb = r.gw.heartbeats;
    ASSERT_TRUE(r.drive([&] { return r.gw.heartbeats >= hb + 2; }, 4'000'000));
    EXPECT_EQ(r.gw.joinGroups, joins) << "a late answer naming the current coordinator forced a rebalance";
    EXPECT_EQ(r.gw.connects, connects) << "a late answer naming the current coordinator reconnected a session";
    EXPECT_EQ(r.client.testSessionCount(), sessions);
    EXPECT_EQ(gc.generation(), 1);
    EXPECT_EQ(r.gw.groupEndpoints.back(), n1);
}

// D4 (review MEDIUM-3, ghost member): a JoinGroup the coordinator HOLDS (a
// rebalance) on a session that then resumes with the SAME id is not re-sent —
// the held join is answered on it; a second JoinGroup with an empty memberId
// would register a ghost member.  RED with the unconditional re-join of
// Joining members on every connect (a0f61a1): two JoinGroups.
TEST(GroupRoutingRegression, ResumeKeepingTheSessionIdKeepsTheHeldJoinRegression) {
    GroupRig r(4139, 4140);
    r.gw.partitions      = {{1, 0, 0, 1, 4139, ""}, {1, 1, 0, 1, 4139, ""}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    r.gw.holdJoins       = true;
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed got;
    wire(gc, got);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    ASSERT_EQ(r.gw.joinGroups, 1);
    const int connects = r.gw.connects;

    // The broker lost track of the session for a moment: the seed resumes and
    // the gateway hands the SAME session id back.
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "", r.seedSid, 0);
    ASSERT_TRUE(r.drive([&] {
        return r.gw.connects > connects &&
               r.client.testSessionStateAt(0) == rc::session::SessionState::Connected;
    })) << "precondition: the seed resumed";
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests >= 2; })) << "the resume's ConnectResponse was handled";
    EXPECT_EQ(r.gw.joinGroups, 1) << "the held JoinGroup was re-sent on the same session (a ghost member)";

    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 1; })) << "the held join was never completed";
    EXPECT_EQ(r.gw.joinGroups, 1);
}

// D4 (review MAJOR-3): a NotCoordinator answer whose correlationId equals the
// seed's in-flight connect corrId (another counter's namespace) must not fail
// that connect.  RED before: the seed machine took it as a connect error ->
// terminal Disconnected.
TEST(GroupRoutingRegression, NotCoordinatorWithAConnectCorrIdNeverFailsTheConnectRegression) {
    GroupRig r(4141, 4142);
    const auto [n1, p1port] = r.addNode(4141);
    r.gw.replyConnects = false;
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(r.client.connect(r.cq, t), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return !r.gw.connectCorrIds.empty(); }));
    ASSERT_EQ(r.client.testSessionStateAt(0), rc::session::SessionState::Connecting);
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator),
                   "127.0.0.1:" + std::to_string(p1port), 0, r.gw.connectCorrIds.back());
    ASSERT_TRUE(r.drive([&] { return r.client.testCoordinatorRedirects() == 1; }))
        << "precondition: the NotCoordinator answer was handled";
    EXPECT_NE(r.client.testSessionStateAt(0), rc::session::SessionState::Disconnected)
        << "a NotCoordinator answer failed the seed's connect";
    r.gw.replyConnects = true;
    bool ok = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) ok = cc.status == rc::OpStatus::Ok;
        });
        return ok;
    }, 4'000'000)) << "the seed never connected";
}

// D4 fallback: the named coordinator is unreachable — its session gives up
// (connect budget spent -> Disconnected) and the coordinator route falls back
// to the seed, whose node answers again with the coordinator; once that node is
// reachable the member joins there.  Paced by the session's own connect budget
// (no loop at round-trip speed).
TEST(GroupRoutingRegression, UnreachableCoordinatorFallsBackToTheSeedAndRetriesRegression) {
    RigOptions o;
    o.connectTimeoutMs = 300;   // the coordinator session gives up after 4 x 300 ms
    GroupRig r(4143, 4144, o);
    const auto [n1, p1port] = r.addNode(4143);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4143, ""}, {1, 1, 0, 1, 4143, ""}};
    r.gw.groupAssignment       = {{1, 0}, {1, 1}};
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.holdJoins             = false;
    r.gw.deadEndpoints.insert(n1);
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed got;
    wire(gc, got);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testCoordinatorFallbacks() >= 1; }, 8'000'000))
        << "the coordinator session never gave up / fell back (state "
        << static_cast<int>(r.client.testSessionStateAt(1)) << ")";
    // Bounded by the connect budget: one NotCoordinator answer per give-up (+1 in flight).
    EXPECT_LE(r.gw.notCoordinatorAnswers, static_cast<int>(r.client.testCoordinatorFallbacks()) + 2)
        << "the redirect loop ran at round-trip speed";
    r.gw.deadEndpoints.erase(n1);
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 1; }, 8'000'000))
        << "the member never joined the coordinator once reachable";
    EXPECT_EQ(r.gw.groupEndpoints.back(), n1);
    EXPECT_EQ(r.gw.connectsAt[0], 1) << "the seed session must never move";
}
