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

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;

namespace {
int g_ctr = 0;  // test thread only

std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

struct GroupRig {
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    rc::Completions              cq{client};
    rc::Token                    nextToken = 1;

    // `seedPort`: node 0 (the seed + group coordinator).  A real clock: UDP
    // sessions need the SessionMachine's connect retry (duty clock).
    GroupRig(int32_t in, int32_t eg, int seedPort)
        : c(makeCfg(in, eg, udp(seedPort))), client(c), gw(c.dataDir + "/aeron", udp(seedPort), in, "aeron:ipc", eg) {
        gw.brokerHost       = "127.0.0.1";
        gw.brokerPort       = seedPort;
        gw.uniqueSessionIds = true;   // each node session gets its own id
        gw.groupProtocol    = true;
        gw.holdJoins        = true;   // the test releases the join once routes are known
    }
    static rc::ClientConfig makeCfg(int32_t in, int32_t eg, const std::string& seed) {
        rc::ClientConfig c;
        c.gatewayChannel  = seed;
        c.gatewayStreamId = in;
        c.egressChannel   = "aeron:ipc";
        c.egressStreamId  = eg;
        c.dataDir = "/tmp/group_routing_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        rc::TopicSpec ts;
        ts.name           = "t";
        ts.topicId        = 1;
        ts.partitionCount = 2;
        c.topics.push_back(ts);
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
                if (cc.token == t) ok = cc.status == rc::OpStatus::Ok;
            });
            return ok;
        }));
    }
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
    GroupRig r(4101, 4102, 24871);
    const int n1 = r.gw.addEndpoint(udp(24872), 4101);
    r.gw.moreBrokers     = {{1, "127.0.0.1", 24872}};
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
    GroupRig r(4103, 4104, 24873);
    const int n1 = r.gw.addEndpoint(udp(24874), 4103);
    r.gw.moreBrokers     = {{1, "127.0.0.1", 24874}};
    r.gw.partitions      = {{1, 0, 0, 1, 4103, ""}, {1, 1, /*stale leader*/ 0, 1, 4103, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};

    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0 && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();

    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }))
        << "the redirected partition never re-subscribed on its leader (refusals=" << r.gw.subscribeRefusals << ")";
    EXPECT_GE(r.gw.subscribeRefusals, 1) << "precondition: the seed redirected partition 1";
    const auto s0 = *r.subAt(0, 0);
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s0, 0, 2);
    r.gw.pushRecords(s1, 0, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[0] == 2 && led.records[1] == 5; }))
        << "p0=" << led.records[0] << " p1=" << led.records[1];
    EXPECT_EQ(r.client.testTransport().ingressChannel(), udp(24873))
        << "a group Subscribe redirect must not move the seed (coordinator) session";
    const int64_t seedSid = r.gw.groupSessionIds.front();
    for (const int64_t sid : r.gw.groupSessionIds) EXPECT_EQ(sid, seedSid);
}

// A bare ErrorResponse{NotLeader} (the refusing node does not know the leader)
// parks the partition until a metadata answer names the leader; the member
// then subscribes there.
TEST(GroupRoutingRegression, NotLeaderGroupSubscribeConvergesOnMetadataRegression) {
    GroupRig r(4105, 4106, 24875);
    const int n1 = r.gw.addEndpoint(udp(24876), 4105);
    r.gw.moreBrokers        = {{1, "127.0.0.1", 24876}};
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
    GroupRig r(4107, 4108, 24877);
    const int n1 = r.gw.addEndpoint(udp(24878), 4107);
    const int n2 = r.gw.addEndpoint(udp(24879), 4107);
    r.gw.moreBrokers     = {{1, "127.0.0.1", 24878}, {2, "127.0.0.1", 24879}};
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
    GroupRig r(4109, 4110, 24880);
    const int n1 = r.gw.addEndpoint(udp(24881), 4109);
    r.gw.moreBrokers         = {{1, "127.0.0.1", 24881}};
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
