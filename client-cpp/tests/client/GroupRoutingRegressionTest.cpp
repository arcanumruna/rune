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
#include <optional>
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/client/Completions.h>
#include <rune/client/group/GroupConsumer.h>

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

#include "../support/GroupRig.h"

namespace {
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

// PR #441 F4, real timing (the client's own metadata pacing, wall clock): partition 1 has no reachable leader for ~5 s
// - every metadata answer keeps naming the refusing node, which answers each re-subscribe NotLeader.  The member must
// survive it (routing refusals are unbounded, however many the pacing produces) and bind once a leader appears.
TEST(GroupRoutingRegression, AFiveSecondLeaderlessPartitionStillBindsWhenALeaderAppearsRegression) {
    GroupRig r(4195, 4196);
    const auto [n1, p1port] = r.addNode(4195);
    r.gw.moreBrokers        = {{1, "127.0.0.1", p1port}};
    r.gw.partitions         = {{1, 0, 0, 1, 4195, ""}, {1, 1, /*stale*/ 0, 1, 4195, ""}};
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
    const auto t0 = std::chrono::steady_clock::now();
    (void)r.drive([&] { return std::chrono::steady_clock::now() - t0 >= std::chrono::seconds(5); }, 50'000'000);
    const int refusals = r.gw.subscribeRefusals;
    std::fprintf(stderr, "leaderless 5 s: %d NotLeader refusals of the partition-1 fetch\n", refusals);
    EXPECT_GT(refusals, 1) << "the client kept re-subscribing during the outage";
    EXPECT_FALSE(gc.finished()) << "a leaderless partition ended the group consumer";

    r.gw.partitions = {{1, 0, 0, 1, 4195, ""}, {1, 1, 1, 2, 4195, ""}};   // a leader appears: node 1, epoch 2
    r.gw.sendMetadata(r.gw.groupSessionIds.front(), /*correlationId=*/0);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; })) << "never bound on the new leader";
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
    r.gw.holdJoins = false;   // fix round 4: the NotCoordinator re-joins — answered at once
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

// No-hang wave: a JoinGroup the broker could not decode is answered
// InvalidRequest (39) — non-retriable, like ProtocolVersion: the consumer
// finishes RefusedByBroker instead of re-joining with the same bytes forever.
TEST(GroupRoutingRegression, JoinGroupInvalidRequestFinishesTheConsumerRegression) {
    GroupRig r(4181, 4182);
    r.gw.holdJoins     = false;
    r.gw.joinErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 400'000)) << "joins: " << r.gw.joinGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker);
    EXPECT_EQ(r.gw.joinGroups, 1);
}

// Authentication (Task 7): a coordinator answering JoinGroup NotAuthorized (45) / SessionRevoked (46) is a
// grant / login problem, not a transient one: the consumer finishes with the matching reason after ONE join —
// it used to fall into the Backoff verdict and re-join until its retry budget ran out.
TEST(GroupRoutingRegression, JoinGroupNotAuthorizedFinishesTheConsumerWithoutRejoinRegression) {
    GroupRig r(4183, 4184);
    r.gw.holdJoins     = false;
    r.gw.joinErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotAuthorized);
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 400'000)) << "joins: " << r.gw.joinGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::NotAuthorized);
    EXPECT_EQ(r.gw.joinGroups, 1);
}

TEST(GroupRoutingRegression, JoinGroupSessionRevokedFinishesTheConsumerRegression) {
    GroupRig r(4185, 4186);
    r.gw.holdJoins     = false;
    r.gw.joinErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionRevoked);
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 400'000)) << "joins: " << r.gw.joinGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::SessionRevoked);
    EXPECT_EQ(r.gw.joinGroups, 1);
}

// ===========================================================================
// No-hang fix round 2 (C1 / I2): the group state machine never loops forever,
// never storms, never stays Stable on an error the broker could not serve.
// ===========================================================================
namespace {
// Reach Stable with one assigned partition (single-node routing).
void reachStable(GroupRig& r, rc::GroupConsumer& gc) {
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 1; }, 4'000'000)) << "precondition: Stable";
}
} // namespace

TEST(GroupRoutingRegression, SyncGroupInvalidRequestFinishesTheConsumerRegression) {
    GroupRig r(4183, 4184);
    r.gw.holdJoins     = false;
    r.gw.syncErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 400'000)) << "syncs: " << r.gw.syncGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker);
    EXPECT_EQ(r.gw.syncGroups, 1) << "re-synced a request the broker cannot decode";
}

// Internal on JoinGroup: re-joined with an exponential backoff, and refused
// once the budget is spent — never a tight JoinGroup <-> Internal storm.
TEST(GroupRoutingRegression, JoinGroupInternalBacksOffThenRefusesRegression) {
    GroupRig r(4185, 4186);
    r.gw.holdJoins     = false;
    r.gw.joinErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    auto gc = r.client.subscribeGroup("g", {"t"});
    const auto t0 = std::chrono::steady_clock::now();
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 40'000'000)) << "joins: " << r.gw.joinGroups;
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RetryBudgetExhausted);
    EXPECT_EQ(r.gw.joinGroups, 9) << "1 join + kMaxBackoffRejoins (8) paced re-joins, then refused";
    EXPECT_GE(took, std::chrono::milliseconds(2000)) << "the re-joins were not paced (a storm)";
}

TEST(GroupRoutingRegression, SyncGroupInternalBacksOffThenRefusesRegression) {
    GroupRig r(4187, 4188);
    r.gw.holdJoins     = false;
    r.gw.syncErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 40'000'000)) << "syncs: " << r.gw.syncGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RetryBudgetExhausted);
    EXPECT_EQ(r.gw.syncGroups, 9);
}

// A Heartbeat the broker could not serve (Internal): the member re-joins (the
// broker may have evicted it) — it used to stay Stable (silent split-brain).
TEST(GroupRoutingRegression, HeartbeatInternalRejoinsNeverStaysStableRegression) {
    GroupRig r(4189, 4190);
    r.gw.holdJoins       = false;
    r.gw.groupAssignment = {{1, 0}};
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 600;   // a heartbeat every 200 ms
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    reachStable(r, gc);
    const int joins = r.gw.joinGroups;
    r.gw.heartbeatErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    ASSERT_TRUE(r.drive([&] { return r.gw.joinGroups > joins; }, 4'000'000))
        << "an Internal heartbeat answer left the member Stable";
}

TEST(GroupRoutingRegression, HeartbeatInvalidRequestFinishesTheConsumerRegression) {
    GroupRig r(4191, 4192);
    r.gw.holdJoins       = false;
    r.gw.groupAssignment = {{1, 0}};
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 600;
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    reachStable(r, gc);
    r.gw.heartbeatErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 4'000'000));
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker);
}

// I2: the broker could not decode the OffsetFetch ({-1, -1, InvalidRequest}):
// the consumer is refused — it used to skip the entry and wait forever for a
// start offset that never came.
TEST(GroupRoutingRegression, OffsetFetchRejectedFinishesTheConsumerRegression) {
    GroupRig r(4193, 4194);
    r.gw.holdJoins             = false;
    r.gw.groupAssignment       = {{1, 0}};
    r.gw.offsetFetchRejectNext = 1;
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 4'000'000)) << "fetches: " << r.gw.offsetFetches;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker);
}

// I2: a retriable per-entry error is re-fetched (paced) — but within a budget:
// a broker that never serves it finishes the consumer instead of looping forever.
TEST(GroupRoutingRegression, OffsetFetchRetriableErrorIsBoundedRegression) {
    GroupRig r(4195, 4196);
    const auto [n1, p1port] = r.addNode(4195);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4195, ""}, {1, 1, 1, 1, 4195, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.offsetFetchErrorCode  = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    r.gw.offsetFetchErrorsNext = 1'000;
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 80'000'000)) << "fetches: " << r.gw.offsetFetches;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RetryBudgetExhausted);
    EXPECT_EQ(r.gw.offsetFetches, 9) << "1 fetch + kMaxOffsetFetchAttempts (8) re-fetches";
}

// C4 review (#607 class): AuthUnavailable (48) on an OffsetFetch - whole-request {-1, -1, 48} (#603's
// answerSecurityBehind_) or per entry - is the broker catching up on security, never a refusal: it is re-fetched
// (paced) and the consumer recovers once the broker answers.  RED before: the first 48 refused the consumer for good.
TEST(GroupRoutingRegression, OffsetFetchAnswered48IsRefetchedAndRecoversRegression) {
    for (const bool wholeRequest : {true, false}) {
        GroupRig r(wholeRequest ? 4930 : 4932, wholeRequest ? 4931 : 4933);
        const auto [n1, p1port] = r.addNode(wholeRequest ? 4930 : 4932);
        r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
        const int32_t gwPort = wholeRequest ? 4930 : 4932;
        r.gw.partitions      = {{1, 0, 0, 1, gwPort, ""}, {1, 1, 1, 1, gwPort, ""}};
        r.gw.subscribeLeader = {{0, 0}, {1, n1}};
        r.gw.groupAssignment = {{1, 1}};
        const auto k48 = static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthUnavailable);
        if (wholeRequest) {
            r.gw.offsetFetchRejectCode = k48;
            r.gw.offsetFetchRejectNext = 2;
        } else {
            r.gw.offsetFetchErrorCode  = k48;
            r.gw.offsetFetchErrorsNext = 2;
        }
        auto gc = r.client.subscribeGroup("g", {"t"});
        r.connect();
        ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
        r.gw.releaseJoins();
        ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr || gc.finished(); }, 80'000'000))
            << "wholeRequest=" << wholeRequest << " fetches: " << r.gw.offsetFetches;
        EXPECT_FALSE(gc.finished()) << "wholeRequest=" << wholeRequest << ": a transient 48 ended the consumer";
        EXPECT_EQ(r.gw.offsetFetches, 3) << "wholeRequest=" << wholeRequest << ": two 48s, then the served fetch";
    }
}

// C4 review (#607 class): a broker that answers every OffsetFetch 48 ends the consumer within the same budget as any
// other retriable OffsetFetch error (RetryBudgetExhausted), never a fast refusal and never a loop.
TEST(GroupRoutingRegression, OffsetFetchAnswered48ForeverIsBoundedRegression) {
    GroupRig r(4934, 4935);
    const auto [n1, p1port] = r.addNode(4934);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4934, ""}, {1, 1, 1, 1, 4934, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.offsetFetchRejectCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthUnavailable);
    r.gw.offsetFetchRejectNext = 1'000;
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 80'000'000)) << "fetches: " << r.gw.offsetFetches;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RetryBudgetExhausted);
    EXPECT_EQ(r.gw.offsetFetches, 9) << "1 fetch + kMaxOffsetFetchAttempts (8) re-fetches";
}

// No-hang fix round 2: an OffsetCommit the broker rejects (InvalidRequest /
// Internal as its {-1, -1, code} entry) completes the pending commitSync with
// BrokerError and that code — never left pending.
TEST(GroupRoutingRegression, OffsetCommitRejectionCompletesCommitSyncRegression) {
    for (const auto code : {rune::protocol::client::ErrorCode::InvalidRequest,
                            rune::protocol::client::ErrorCode::Internal}) {
        const int32_t base = code == rune::protocol::client::ErrorCode::InvalidRequest ? 4211 : 4213;
        GroupRig r(base, base + 1);
        r.gw.holdJoins             = false;
        r.gw.groupAssignment       = {{1, 0}};
        r.gw.offsetCommitErrorCode = static_cast<int16_t>(code);
        auto gc = r.client.subscribeGroup("g", {"t"});
        Consumed got0;
        wire(gc, got0);
        reachStable(r, gc);
        ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr; }, 4'000'000));
        r.gw.pushRecords(*r.subAt(0, 0), 0, 2);
        ASSERT_TRUE(r.drive([&] { return got0.records[0] >= 2; }, 4'000'000));   // a position to commit
        const std::size_t before = r.gw.offsetCommits.size();
        const rc::Token t = r.nextToken++;
        ASSERT_EQ(gc.commitSync(r.cq, t), rc::OpResult::Accepted);
        rc::Completion got{};
        bool done = false;
        ASSERT_TRUE(r.drive([&] {
            r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
                if (cc.token == t) { done = true; got = cc; }
            });
            return done;
        }, 4'000'000)) << "a rejected OffsetCommit left commitSync pending";
        EXPECT_GT(r.gw.offsetCommits.size(), before);
        EXPECT_EQ(got.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(got.errorCode, static_cast<uint16_t>(code));
    }
}

// Fix round 3 (C1): a coordinator that answers EVERY Heartbeat Internal no
// longer cycles Stable → rejoin → Stable forever — only a Heartbeat OK resets
// the backoff budget, so the consumer finishes RetryBudgetExhausted after a
// bounded number of rejoins.
TEST(GroupRoutingRegression, HeartbeatInternalForeverIsRefusedAfterTheBudgetRegression) {
    GroupRig r(4215, 4216);
    r.gw.holdJoins          = false;
    r.gw.groupAssignment    = {{1, 0}};
    r.gw.heartbeatErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 300;   // a heartbeat every 100 ms
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 80'000'000)) << "joins: " << r.gw.joinGroups;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RetryBudgetExhausted);
    EXPECT_LE(r.gw.joinGroups, 9) << "more than 1 + kMaxBackoffRejoins joins";
}

// Fix round 4 (item 2a): a node that answers every Heartbeat OK but every
// OffsetFetch NotCoordinator can no longer keep the member Stable and
// unsubscribed forever: each NotCoordinator re-joins (rediscovery, paced) and
// kMaxOffsetFetchRejoins consecutive ones finish the consumer explicitly
// (RetryBudgetExhausted).  RED (round 3): re-fetched every 2 s forever, Stable,
// never finished, no signal.
TEST(GroupRoutingRegression, OffsetFetchNotCoordinatorForeverWithHeartbeatOkTerminatesRegression) {
    GroupRig r(4217, 4218);
    const auto [n1, p1port] = r.addNode(4217);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4217, ""}, {1, 1, 1, 1, 4217, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.offsetFetchErrorCode  = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator);
    r.gw.offsetFetchErrorsNext = 1'000'000;   // forever; heartbeats answer OK
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 300;   // a heartbeat every 100 ms (they stay OK)
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.holdJoins = false;   // every re-join is answered at once from now on
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); }, 80'000'000))
        << "fetches: " << r.gw.offsetFetches << " joins: " << r.gw.joinGroups
        << " — OffsetFetch NotCoordinator kept the member Stable forever";
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RetryBudgetExhausted);
    EXPECT_EQ(r.gw.offsetFetches, 9) << "1 fetch + kMaxOffsetFetchRejoins (8) re-joined fetches";
    EXPECT_EQ(r.gw.joinGroups, 9) << "each NotCoordinator must re-join (rediscovery), exactly once";
    EXPECT_EQ(r.subAt(1, n1), nullptr);
}

// Fix round 4 (item 2b): ONE failover — OffsetFetch NotCoordinator, then OK at
// the rediscovered coordinator — recovers through a single re-join, spending
// neither the OffsetFetch budget nor the Internal backoff budget: a second
// failover later recovers the same way.
TEST(GroupRoutingRegression, OffsetFetchNotCoordinatorFailoverRejoinsWithoutSpendingTheBudgetRegression) {
    GroupRig r(4219, 4220);
    const auto [n1, p1port] = r.addNode(4219);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4219, ""}, {1, 1, 1, 1, 4219, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.offsetFetchErrorCode  = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator);
    r.gw.offsetFetchErrorsNext = 1;
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 300;   // a heartbeat every 100 ms
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.holdJoins = false;
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 80'000'000))
        << "fetches: " << r.gw.offsetFetches << (gc.finished() ? " (the consumer was refused)" : "");
    EXPECT_FALSE(gc.finished());
    EXPECT_EQ(r.gw.joinGroups, 2) << "the NotCoordinator must trigger exactly one re-join";
    EXPECT_EQ(r.gw.offsetFetches, 2);
    // A later failover: the streak was reset by the served fetch.  Eight more
    // single failovers in a row each recover — none is charged to a budget.
    for (int k = 0; k < 8; ++k) {
        const int joins = r.gw.joinGroups;
        const int fetches = r.gw.offsetFetches;
        r.gw.offsetFetchErrorsNext = 1;
        r.gw.heartbeatErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::RebalanceInProgress);
        ASSERT_TRUE(r.drive([&] { return r.gw.joinGroups > joins; }, 80'000'000));
        r.gw.heartbeatErrorCode = 0;
        ASSERT_TRUE(r.drive([&] { return r.gw.offsetFetches >= fetches + 2 && r.subAt(1, n1) != nullptr; },
                            80'000'000)) << "failover " << k;
        EXPECT_FALSE(gc.finished()) << "failover " << k << " spent a budget";
    }
}

// Flake fix (OffsetFetchNotCoordinatorForever ~2/10: 10 fetches, not 9): the
// MEDIUM-2 retry pass re-sent the eager Stable-entry OffsetFetch while it was
// still in flight (its pacing clock was 0 / left over from an earlier
// generation), superseding the first one's correlationId — one duplicate
// fetch whenever a duty sweep fell between the fetch and its answer.  Now a
// fetch in flight is never duplicated; only one unanswered past
// sessionTimeoutMs (a lost answer) is re-sent — exactly once per deadline.
TEST(GroupRoutingRegression, OffsetFetchInFlightIsNeverDuplicatedByTheRetryPassRegression) {
    GroupRig r(4221, 4222);
    const auto [n1, p1port] = r.addNode(4221);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4221, ""}, {1, 1, 1, 1, 4221, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.holdOffsetFetch = true;   // the eager fetch stays in flight
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 1'500;  // its answer deadline
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.gw.offsetFetches == 1; }, 4'000'000));
    const auto sentAt = std::chrono::steady_clock::now();
    // Many duty sweeps (100 ms each) while it is in flight: no duplicate.
    (void)r.drive([&] { return std::chrono::steady_clock::now() - sentAt > std::chrono::milliseconds(700); },
                  80'000'000);
    EXPECT_EQ(r.gw.offsetFetches, 1) << "the retry pass duplicated an OffsetFetch still in flight";
    // Unanswered past sessionTimeoutMs: a lost answer — re-sent, once.
    ASSERT_TRUE(r.drive([&] { return r.gw.offsetFetches >= 2; }, 80'000'000)) << "a lost answer was never re-sent";
    EXPECT_LT(std::chrono::steady_clock::now() - sentAt, std::chrono::milliseconds(1'500 + 1'000));
    EXPECT_EQ(r.gw.offsetFetches, 2);
    // The (latest) answer seeds the start offset: the leader-node subscription goes out.
    r.gw.holdOffsetFetch = false;
    r.gw.releaseOffsetFetches();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 4'000'000));
    EXPECT_EQ(r.gw.offsetFetches, 2);
    EXPECT_FALSE(gc.finished());
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
// request ErrorResponse{NotCoordinator, coordinator endpoint}; the member must
// join the coordinator at once on that node's own session (the coordinator
// route) while the seed session stays where it is — before the fix the member
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
    rc::ProducerConfig tcfg;
    tcfg.acks            = rc::Acks::All;
    tcfg.transactionalId = "tx-d4";
    auto tp = r.client.producer(tcfg);
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
    // r3 MEDIUM-2: the property is that the seed CONNECTS (it was never failed);
    // the app's connect() op itself may race its own deadline (the seed's retry
    // and connect() both fire at 5 s), so it is not the oracle.
    // Bounded by wall time, not a pump count: the seed's connect retry is a
    // 5 s timer, which a fast host's pump loop can outrun.
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool connected = false;
    while (!connected && std::chrono::steady_clock::now() < until) {
        r.client.doWork();
        r.gw.pump();
        r.cq.poll([](const rc::Completion&, std::span<const uint8_t>) {});
        connected = r.client.testSessionStateAt(0) == rc::session::SessionState::Connected;
    }
    ASSERT_TRUE(connected) << "the seed never connected";
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

// ===========================================================================
// D4 fix round 1 (re-review r2)
// ===========================================================================

namespace {
// The seed was retargeted off its bootstrap (node 0) to node 1 by a plain
// Subscribe's data-op Redirect: the seed's identity is node 0, it is
// physically on node 1.
// The seed is moved by a DATA-OP Redirect of a Produce (no partition route
// installed, no group active: the CB-5 B seed retarget).  E2 residual round 5
// (R3): a plain Subscribe's Redirect never moves the seed any more (it follows
// per partition, or is held until metadata places its target).
void moveSeedToNode1(GroupRig& r, int n1, int p1port, int32_t topicIdx, std::optional<rc::Producer>& keep) {
    r.gw.redirectNext = 1;
    r.gw.redirectTo   = "127.0.0.1:" + std::to_string(p1port);
    rc::ProducerConfig pc;
    pc.lingerUs = 0;
    keep.emplace(r.client.producer(pc));   // the caller keeps it alive (the redirected batch is re-sent)
    const std::vector<uint8_t> val{1};
    (void)keep->trySendTo(r.client.topic(topicIdx == 1 ? "b" : "t"), 0, rc::Key(uint64_t{1}),
                                 std::span<const uint8_t>(val));
    ASSERT_TRUE(r.drive([&] {
        return r.client.testTransport().ingressChannel() == udp(p1port) && r.gw.connectsAt[n1] >= 1 &&
               r.client.testSessionStateAt(0) == rc::session::SessionState::Connected;
    }, 4'000'000)) << "precondition: the seed was retargeted to node 1";
}
} // namespace

// MAJOR-1 (r2): the txn coordinator is the BOOTSTRAP node while the seed sits
// on node 1 (moved there by a data-op Redirect).  Node 1 answers InitProducerId
// NotCoordinator{bootstrap}; the request must reach the bootstrap node.  RED at
// d22a757: the answer was taken for "the coordinator is already the seed" (a
// no-op) and the InitProducerId was never re-sent.
TEST(GroupRoutingRegression, TxnCoordinatorOnTheBootstrapAfterASeedRetargetRegression) {
    GroupRig r(4171, 4172);
    const auto [n1, p1port] = r.addNode(4171);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.holdJoins   = false;
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    std::optional<rc::Producer> seedMover;
    moveSeedToNode1(r, n1, p1port, 0, seedMover);
    r.gw.coordinatorEndpoint   = 0;   // the bootstrap node is the coordinator
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(r.gw.brokerPort);
    r.gw.initPidReply          = true;
    r.gw.coordinatorEndpoints.clear();   // FD4: the seed mover's own (plain) InitProducerId is not under test
    const int seedConnects = r.gw.connectsAt[n1];
    rc::ProducerConfig tcfg;
    tcfg.acks            = rc::Acks::All;
    tcfg.transactionalId = "tx-d4-boot";
    auto tp = r.client.producer(tcfg);
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(tp.initTransactions(r.cq, t), rc::OpResult::Accepted);
    bool done = false;
    rc::OpStatus st = rc::OpStatus::Closed;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, 3'000'000)) << "InitProducerId never reached the bootstrap coordinator (NotCoordinator answers: "
                   << r.gw.notCoordinatorAnswers << ")";
    EXPECT_EQ(st, rc::OpStatus::Ok);
    ASSERT_FALSE(r.gw.coordinatorEndpoints.empty());
    EXPECT_EQ(r.gw.coordinatorEndpoints.front(), n1) << "precondition: the first InitProducerId rode the seed";
    EXPECT_EQ(r.gw.coordinatorEndpoints.back(), 0) << "re-sent to the bootstrap coordinator";
    EXPECT_EQ(r.client.testTransport().ingressChannel(), udp(p1port)) << "the seed must not move";
    EXPECT_EQ(r.gw.connectsAt[n1], seedConnects) << "the seed must not reconnect";
}

// MAJOR-1 (r2), group flavour: the member's coordinator is the bootstrap node
// while the seed sits on node 1.  RED at d22a757: one JoinGroup to node 1,
// NotCoordinator{bootstrap} ignored, never joined.
TEST(GroupRoutingRegression, GroupCoordinatorOnTheBootstrapAfterASeedRetargetRegression) {
    GroupRig r(4173, 4174);
    const auto [n1, p1port] = r.addNode(4173);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4173, ""}, {1, 1, 0, 1, 4173, ""}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    r.gw.holdJoins       = false;
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("b"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    std::optional<rc::Producer> seedMover;
    moveSeedToNode1(r, n1, p1port, 1, seedMover);
    r.gw.coordinatorEndpoint   = 0;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(r.gw.brokerPort);
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 30'000;   // the join deadline must not be what un-sticks it
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    bool assigned = false;
    gc.onAssigned([&](std::vector<rc::TopicPartition>) { assigned = true; });
    ASSERT_TRUE(r.drive([&] { return assigned; }, 3'000'000))
        << "the member never joined the bootstrap coordinator (NotCoordinator answers: "
        << r.gw.notCoordinatorAnswers << ", joins " << r.gw.joinGroups << ")";
    EXPECT_EQ(r.gw.groupEndpoints.back(), 0);
    EXPECT_EQ(r.client.testTransport().ingressChannel(), udp(p1port)) << "the seed must not move";
}

// MEDIUM-1 (r2): a SessionExpired for the COORDINATOR session (header sessionId
// = its cluster-unique id, correlationId 0 — the broker's sendSessionExpired_)
// must reach that session, never the healthy seed.  RED at d22a757: the seed
// went Resuming and reconnected; the coordinator session never learned.
TEST(GroupRoutingRegression, SessionExpiredForTheCoordinatorSessionNeverHitsTheSeedRegression) {
    GroupRig r(4175, 4176);
    const auto [n1, p1port] = r.addNode(4175);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, n1, 1, 4175, ""}, {1, 1, n1, 1, 4175, ""}};
    r.gw.groupAssignment       = {{1, 0}, {1, 1}};
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.holdJoins             = false;
    rc::GroupConfig gcfg;
    gcfg.sessionTimeoutMs = 600;
    auto gc = r.client.subscribeGroup("g", {"t"}, gcfg);
    int appErrors = 0;   // r3 MEDIUM-1: the app sees no error for the coordinator session's expiry
    r.client.onError([&](const std::string&) { ++appErrors; });
    r.connect();
    ASSERT_TRUE(r.drive([&] { return gc.generation() == 1 && r.gw.heartbeats >= 1; }, 4'000'000));
    const int64_t coordSid = r.gw.groupSessionIds.back();
    const int errorsBefore = appErrors;
    ASSERT_NE(coordSid, r.seedSid);
    ASSERT_EQ(r.gw.connectsAt[0], 1);
    ASSERT_EQ(r.gw.connectsAt[n1], 1);
    // Fix round 5 (N14): a control op pending on the SEED (unanswered: the rig
    // does not reply to DefineTopic) must survive the coordinator's expiry.
    const std::vector<uint8_t> blob{9};
    const rc::Token defTok = r.nextToken++;
    ASSERT_EQ(r.client.defineTopic("x", 1, "", false, blob, r.cq, defTok), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.client.testPendingControlOps() == 1; }, 400'000));
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "", coordSid, 0);
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[n1] >= 2 || r.gw.connectsAt[0] >= 2; }, 4'000'000))
        << "nobody reacted to the SessionExpired";
    EXPECT_EQ(r.gw.connectsAt[n1], 2) << "the coordinator session did not resume";
    EXPECT_EQ(r.gw.connectsAt[0], 1) << "the SessionExpired of the coordinator session hit the seed";
    EXPECT_EQ(r.client.testSessionStateAt(0), rc::session::SessionState::Connected);
    EXPECT_EQ(appErrors, errorsBefore) << "the seed's ops / onError were failed for another session's expiry";
    bool defCompleted = false;
    r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) { defCompleted |= cc.token == defTok; });
    EXPECT_FALSE(defCompleted) << "a seed control op was completed by the coordinator session's expiry";
    EXPECT_EQ(r.client.testPendingControlOps(), 1u);
}

// r2 LOW-5: a commitSync of a member whose coordinator is a session of its own
// goes there (coordDirectSendTrampoline_), stamped with that session's id.
TEST(GroupRoutingRegression, CommitSyncRidesTheCoordinatorSessionRegression) {
    GroupRig r(4177, 4178);
    const auto [n1, p1port] = r.addNode(4177);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, n1, 1, 4177, ""}, {1, 1, n1, 1, 4177, ""}};
    r.gw.groupAssignment       = {{1, 0}, {1, 1}};
    r.gw.subscribeLeader       = {{0, n1}, {1, n1}};
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.holdJoins             = false;
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed got;
    wire(gc, got);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && r.subAt(1, n1) != nullptr; }, 4'000'000));
    r.gw.pushRecords(*r.subAt(0, n1), 0, 2);
    ASSERT_TRUE(r.drive([&] { return got.records[0] >= 2; }, 4'000'000));
    const int64_t coordSid = r.gw.groupSessionIds.back();
    const std::size_t before = r.gw.offsetCommits.size();
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(gc.commitSync(r.cq, t), rc::OpResult::Accepted);
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, 4'000'000));
    EXPECT_EQ(st, rc::OpStatus::Ok);
    ASSERT_GT(r.gw.offsetCommits.size(), before);
    const auto& f = r.gw.offsetCommits.back();
    const auto oc = rune::protocol::client::OffsetCommit::decode(f.data(), f.size());
    ASSERT_TRUE(oc.has_value());
    EXPECT_EQ(oc->sessionId, coordSid) << "the OffsetCommit did not ride the coordinator session";
}

// Rune E2 Task 13 fix round 1: a BOUND group fetch subscription whose node
// stops leading the partition (demoted: controlled shutdown, failover, a
// higher epoch).  The broker answers the fetch Subscribe's correlation id with
// a bare NotLeader and drops it.  A bound fetch subscription had already
// forgotten that id (corrId is cleared on binding), so the answer was taken as
// stale: no metadata asked, no re-route — the partition stalled silently until
// some unrelated refresh.  It now matches the instance id, asks for metadata
// and re-subscribes on the new leader FROM ITS POSITION.
TEST(GroupRoutingRegression, BoundFetchSubscriptionAnsweredNotLeaderMovesToTheNewLeaderRegression) {
    GroupRig r(4223, 4224);
    const auto [n1, p1port] = r.addNode(4223);
    const auto [n2, p2port] = r.addNode(4223);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4223, ""}, {1, 1, 1, 1, 4223, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.committedOffsets[{1, 1}] = 10;

    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s1, 10, 5);   // binds it by push (its corrId is forgotten locally)
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 5; }));

    // Node 1 is demoted: the cluster now routes partition 1 to node 2 (no metadata
    // is pushed to the client), and node 1 answers the subscription NotLeader.
    r.gw.subscribeLeader[1] = n2;
    r.gw.partitions = {{1, 0, 0, 1, 4223, ""}, {1, 1, 2, 2, 4223, ""}};
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), "", s1.sessionId,
                   s1.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }, 200'000))
        << "SILENT STALL: the group partition never moved off the node that stopped leading it";
    const auto s2 = *r.subAt(1, n2);
    EXPECT_EQ(s2.fromOffset, 15) << "the moved fetch subscription resumes at the member's position";
    r.gw.pushRecords(s2, 15, 2);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 7; }));
    EXPECT_EQ(led.next[1], 17);
    // M4 (fix round 2): the old instance is released on node 1, as the plain path does.
    int released = 0;
    for (std::size_t i = 0; i < r.gw.unsubscribed.size(); ++i)
        released += (r.gw.unsubscribed[i] == s1.subscriptionId && r.gw.unsubscribedAt[i] == n1) ? 1 : 0;
    EXPECT_EQ(released, 1) << "the moved fetch subscription's old instance was not unsubscribed (exactly once) on its node";
}

// I1 (fix round 2): after the move the new leader pushes the WHOLE batch that
// contains the member's position (brokers never split a batch): 12..16 for
// position 15.  12..14 were consumed before the move: the RecordSet says so
// (deliverFrom == 15) and a consumer iterating it from deliverFrom sees every
// record exactly once.  RED: deliverFrom stayed 0 — 12..14 handed over twice.
TEST(GroupRoutingRegression, MovedFetchSubscriptionStraddlingBatchSkipsConsumedRecordsRegression) {
    GroupRig r(4225, 4226);
    const auto [n1, p1port] = r.addNode(4225);
    const auto [n2, p2port] = r.addNode(4225);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4225, ""}, {1, 1, 1, 1, 4225, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.committedOffsets[{1, 1}] = 10;

    auto gc = r.client.subscribeGroup("g", {"t"});
    std::map<int64_t, int> consumed;   // offset -> times handed to the application (duty thread = this thread)
    std::vector<int64_t>   deliverFrom;
    gc.onRecords([&](const rc::RecordSet& rs) {
        deliverFrom.push_back(rs.deliverFrom);
        rune::protocol::RecordBatchReader rd(rs.recordBatch.data(), rs.recordBatch.size());
        if (!rd.ensureValid()) return;
        rd.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
            if (off >= rs.deliverFrom) ++consumed[off];
        });
    });
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s1, 10, 5);   // 10..14
    ASSERT_TRUE(r.drive([&] { return consumed.size() == 5; }));

    r.gw.subscribeLeader[1] = n2;
    r.gw.partitions = {{1, 0, 0, 1, 4225, ""}, {1, 1, 2, 2, 4225, ""}};
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), "", s1.sessionId,
                   s1.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }, 200'000));
    const auto s2 = *r.subAt(1, n2);
    ASSERT_EQ(s2.fromOffset, 15);
    r.gw.pushRecords(s2, 12, 5);   // the whole batch containing 15: 12..16
    ASSERT_TRUE(r.drive([&] { return deliverFrom.size() == 2; }));
    EXPECT_EQ(deliverFrom.back(), 15) << "the straddling batch did not mark its consumed records";
    for (const auto& [off, n] : consumed) EXPECT_EQ(n, 1) << "offset " << off << " handed over " << n << " times";
    EXPECT_EQ(consumed.size(), 7u);   // 10..16
}

// E2 final fix wave (I4): the same straddling re-delivery, but the application
// does NOT cooperate — it iterates with RecordSet::forEachRecord and never
// looks at deliverFrom.  The library's iteration skips the consumed records:
// every record is handed over exactly once.  RED (forEachRecord without the
// deliverFrom filter): 12..14 handed over twice.
TEST(GroupRoutingRegression, RecordSetIterationSkipsAStraddlingReDeliveryWithoutAppCooperationRegression) {
    GroupRig r(4229, 4230);
    const auto [n1, p1port] = r.addNode(4229);
    const auto [n2, p2port] = r.addNode(4229);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4229, ""}, {1, 1, 1, 1, 4229, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.committedOffsets[{1, 1}] = 10;

    auto gc = r.client.subscribeGroup("g", {"t"});
    std::map<int64_t, int> consumed;   // offset -> times handed to the application
    int                    batches = 0;
    gc.onRecords([&](const rc::RecordSet& rs) {
        ++batches;
        rs.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) { ++consumed[off]; });
    });
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s1, 10, 5);   // 10..14
    ASSERT_TRUE(r.drive([&] { return consumed.size() == 5; }));

    r.gw.subscribeLeader[1] = n2;
    r.gw.partitions = {{1, 0, 0, 1, 4229, ""}, {1, 1, 2, 2, 4229, ""}};
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), "", s1.sessionId,
                   s1.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }, 200'000));
    const auto s2 = *r.subAt(1, n2);
    ASSERT_EQ(s2.fromOffset, 15);
    r.gw.pushRecords(s2, 12, 5);   // the whole batch containing 15: 12..16
    ASSERT_TRUE(r.drive([&] { return batches == 2; }));
    for (const auto& [off, n] : consumed) EXPECT_EQ(n, 1) << "offset " << off << " handed over " << n << " times";
    EXPECT_EQ(consumed.size(), 7u);   // 10..16
}

// E2 Task 14 (Task 13 re-review N4): a group fetch instance NEVER acts on a
// broker position note (an empty-batch Records frame, client version 6) — the
// note concerns plain subscriptions; a member resumes from its own position.
// Should one reach the group hub, an empty view would count as one record:
// the member's position would jump to the note's base + 1 (21 here), the next
// real batch (15..16) would be dropped as "below the position" and the move
// would resume from 21.  Expected: no onRecords for the note, the position
// stays 15, the move resumes there and every record arrives exactly once.
TEST(GroupRoutingRegression, GroupFetchInstanceNeverActsOnAPositionNoteRegression) {
    GroupRig r(4227, 4228);
    const auto [n1, p1port] = r.addNode(4227);
    const auto [n2, p2port] = r.addNode(4227);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4227, ""}, {1, 1, 1, 1, 4227, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};
    r.gw.committedOffsets[{1, 1}] = 10;

    auto gc = r.client.subscribeGroup("g", {"t"});
    int                     callbacks = 0;
    std::map<int64_t, int>  consumed;   // offset -> times handed over (from deliverFrom)
    int64_t                 next = -1;
    gc.onRecords([&](const rc::RecordSet& rs) {
        ++callbacks;
        next = rs.baseOffset + rs.recordCount;
        rune::protocol::RecordBatchReader rd(rs.recordBatch.data(), rs.recordBatch.size());
        if (!rd.ensureValid()) return;
        rd.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
            if (off >= rs.deliverFrom) ++consumed[off];
        });
    });
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }));
    const auto s1 = *r.subAt(1, n1);
    r.gw.pushRecords(s1, 10, 5);   // 10..14 -> position 15
    ASSERT_TRUE(r.drive([&] { return consumed.size() == 5; }));

    // The demoted node sends a position note on the fetch instance (exactly the
    // instance the member is bound to: its id, session and correlation id) ...
    {
        namespace rcc = rune::client::codec;
        uint8_t buf[256]{};
        const std::size_t n = rcc::encodeRecords(buf, sizeof(buf), s1.subscriptionId, s1.topicId, s1.partition,
                                                 /*baseOffset=*/20, std::span<const uint8_t>{}, s1.sessionId,
                                                 s1.corrId);
        ASSERT_GT(n, 0u);
        r.gw.send(n1, buf, n);
    }
    // ... followed by a real batch (still node 1), then NotLeader.
    r.gw.pushRecords(s1, 15, 1);   // 15 -> position 16
    ASSERT_TRUE(r.drive([&] { return consumed.size() == 6; }, 200'000))
        << "the batch after the note was dropped: the note moved the member's position";
    EXPECT_EQ(callbacks, 2) << "onRecords fired for the position note";
    EXPECT_EQ(next, 16);

    r.gw.subscribeLeader[1] = n2;
    r.gw.partitions = {{1, 0, 0, 1, 4227, ""}, {1, 1, 2, 2, 4227, ""}};
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), "", s1.sessionId,
                   s1.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr; }, 200'000));
    const auto s2 = *r.subAt(1, n2);
    EXPECT_EQ(s2.fromOffset, 16) << "the moved fetch subscription resumed from the note, not its position";
    r.gw.pushRecords(s2, 16, 2);
    ASSERT_TRUE(r.drive([&] { return consumed.size() == 8; }));
    EXPECT_EQ(callbacks, 3);
    for (const auto& [off, n] : consumed) EXPECT_EQ(n, 1) << "offset " << off << " handed over " << n << " times";
}

// ===========================================================================
// E2 final fix wave (I2): a MOVED plain subscription answered Redirect follows
// it PER PARTITION, on the new leader node's own session — the seed is never
// retargeted, so the other subscriptions bound on it stay alive.
// ===========================================================================
namespace {
struct PlainLedger {
    std::map<int32_t, std::map<int64_t, int>> seen;   // partition -> offset -> times delivered
    rc::RecordHandler handler(int32_t p) {
        return [this, p](const rc::Record& rec) { ++seen[p][rec.offset]; };
    }
    [[nodiscard]] int count(int32_t p) const {
        const auto it = seen.find(p);
        return it == seen.end() ? 0 : static_cast<int>(it->second.size());
    }
    [[nodiscard]] int dupes() const {
        int d = 0;
        for (const auto& [p, m] : seen)
            for (const auto& [o, n] : m) d += n > 1 ? n - 1 : 0;
        return d;
    }
};
int subscribesFor(const test_support::InvokerGateway& gw, int32_t p) {
    int n = 0;
    for (const auto& s : gw.subscribed) n += (s.topicId == 1 && s.partition == p) ? 1 : 0;
    return n;
}
}  // namespace

// Two subscriptions bound on the seed (node 0 leads partitions 0 and 1); only
// partition 0's leadership moves to node 1, whose Redirect answers its bound
// Subscribe.  Expected: partition 0 re-subscribes on node 1's own session from
// its position; partition 1's subscription is untouched (no re-subscribe, the
// seed stays on node 0) and keeps delivering; every record exactly once.
// RED (Task 14 behaviour): the client retargeted the SEED to node 1 — the
// seed channel changed, and partition 1's subscription was left on the
// abandoned session (a real broker drops it when that session expires: a
// silent stall).
TEST(GroupRoutingRegression, TwoBoundSubscriptionsOnOneSeedOnlyOneMovesRegression) {
    GroupRig r(4231, 4232);
    const auto [n1, p1port] = r.addNode(4231);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4231, ""}, {1, 1, 0, 1, 4231, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, 0}};
    r.gw.groupProtocol   = false;
    r.connect();
    const std::string seedBefore = r.client.testTransport().ingressChannel();

    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    auto s1 = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()}, led.handler(1));
    ASSERT_TRUE(r.drive([&] { return s0.subscriptionId() != 0 && s1.subscriptionId() != 0; }));
    const auto a0 = *r.subAt(0, 0);
    const auto a1 = *r.subAt(1, 0);
    r.gw.pushRecords(a0, 0, 3);   // p0: 0..2
    r.gw.pushRecords(a1, 0, 2);   // p1: 0..1
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 3 && led.count(1) == 2; }));

    // Partition 0 alone moves to node 1; node 0 answers its bound Subscribe Redirect.
    r.gw.subscribeLeader[0] = n1;
    r.gw.partitions         = {{1, 0, 1, 2, 4231, ""}, {1, 1, 0, 1, 4231, ""}};
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(p1port), a0.sessionId, a0.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && s0.subscriptionId() == r.subAt(0, n1)->subscriptionId; }))
        << "SILENT STALL: the moved subscription never re-subscribed on its new leader";
    const auto b0 = *r.subAt(0, n1);
    EXPECT_EQ(b0.fromOffset, 3) << "not resumed at the moved subscription's position";
    EXPECT_NE(b0.sessionId, a0.sessionId) << "re-subscribed on the seed session instead of node 1's own";
    EXPECT_EQ(r.client.testTransport().ingressChannel(), seedBefore) << "the moved subscription retargeted the SEED";
    EXPECT_EQ(subscribesFor(r.gw, 1), 1) << "the untouched subscription was re-subscribed";

    // Both keep delivering: p1 on the seed (node 0), p0 on node 1.
    r.gw.pushRecords(a1, 2, 2);   // p1: 2..3
    r.gw.pushRecords(b0, 3, 2);   // p0: 3..4
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 5 && led.count(1) == 4; }))
        << "p0 " << led.count(0) << "/5, p1 " << led.count(1) << "/4";
    EXPECT_EQ(led.dupes(), 0);
    EXPECT_TRUE(s1.active());
    EXPECT_EQ(s1.subscriptionId(), a1.subscriptionId) << "the untouched subscription was re-bound";
    // The old instance was released on node 0 (its own home session).
    bool released = false;
    for (std::size_t i = 0; i < r.gw.unsubscribed.size(); ++i)
        released = released || (r.gw.unsubscribed[i] == a0.subscriptionId && r.gw.unsubscribedAt[i] == 0);
    EXPECT_TRUE(released) << "the moved subscription's old instance was not unsubscribed on its node";
    s0.unsubscribe();
    s1.unsubscribe();
}

// I2: a subscription already homed on node 1 moves AGAIN — back to the seed's
// node.  The answer arrives on node 1's session (its home), is accepted there,
// and the subscription follows it back to the seed (the seed's node leads):
// no retarget, no stall, exactly once; its Unsubscribe on leaving goes to
// node 1.  RED: an answer on a non-seed session was ignored (the subscription
// stayed bound to a node that no longer serves it).
TEST(GroupRoutingRegression, HomedMovedSubscriptionMovesAgainFromItsNodeSessionRegression) {
    GroupRig r(4233, 4234);
    const auto [n1, p1port] = r.addNode(4233);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4233, ""}};
    r.gw.subscribeLeader = {{0, 0}};
    r.gw.groupProtocol   = false;
    r.connect();
    const std::string seedBefore = r.client.testTransport().ingressChannel();
    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    ASSERT_TRUE(r.drive([&] { return s0.subscriptionId() != 0; }));
    const auto a0 = *r.subAt(0, 0);
    r.gw.pushRecords(a0, 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 2; }));

    r.gw.subscribeLeader[0] = n1;   // to node 1
    r.gw.partitions         = {{1, 0, 1, 2, 4233, ""}};
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(p1port), a0.sessionId, a0.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && s0.subscriptionId() == r.subAt(0, n1)->subscriptionId; }));
    const auto b0 = *r.subAt(0, n1);
    r.gw.pushRecords(b0, 2, 2);   // 2..3 from node 1
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 4; }));

    // ... and back to node 0: node 1 answers on ITS session.
    r.gw.subscribeLeader[0] = 0;
    r.gw.partitions         = {{1, 0, 0, 3, 4233, ""}};
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   r.gw.brokerHost + ":" + std::to_string(r.gw.brokerPort), b0.sessionId, b0.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(0, 0)->corrId != a0.corrId &&
                                     s0.subscriptionId() == r.subAt(0, 0)->subscriptionId; }))
        << "SILENT STALL: an answer on the subscription's node session did not move it";
    const auto c0 = *r.subAt(0, 0);
    EXPECT_EQ(c0.fromOffset, 4);
    EXPECT_EQ(c0.sessionId, a0.sessionId) << "not back on the seed session";
    EXPECT_EQ(r.client.testTransport().ingressChannel(), seedBefore);
    r.gw.pushRecords(c0, 4, 1);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 5; }));
    EXPECT_EQ(led.dupes(), 0);
    bool releasedOnN1 = false;
    for (std::size_t i = 0; i < r.gw.unsubscribed.size(); ++i)
        releasedOnN1 = releasedOnN1 || (r.gw.unsubscribed[i] == b0.subscriptionId && r.gw.unsubscribedAt[i] == n1);
    EXPECT_TRUE(releasedOnN1) << "the node-1 instance was not unsubscribed on node 1";
    s0.unsubscribe();
}

// I2: the node session a moved subscription lives on is LOST (node 1 dies; the
// cluster elects node 2, as metadata now says).  The broker ended the
// subscription with that session: the client re-arms it from its position and
// routes it to the new leader — never a silent stall.  RED (mutation: the
// lost-session re-arm in SubscriberHub::driveHomed disabled): never
// re-subscribed on node 2.
TEST(GroupRoutingRegression, MovedSubscriptionWhoseNodeSessionIsLostIsReArmedRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;   // 50 ms
    o.responseTimeoutNs = 400'000'000;  // the session machine's own staleness rule
    GroupRig r(4235, 4236, o);
    const auto [n1, p1port] = r.addNode(4235);
    const auto [n2, p2port] = r.addNode(4235);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4235, ""}};
    r.gw.subscribeLeader = {{0, 0}};
    r.gw.groupProtocol   = false;
    r.connect();
    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    ASSERT_TRUE(r.drive([&] { return s0.subscriptionId() != 0; }));
    const auto a0 = *r.subAt(0, 0);
    r.gw.pushRecords(a0, 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 2; }));

    r.gw.subscribeLeader[0] = n1;   // moves to node 1 (its own session)
    r.gw.partitions         = {{1, 0, 1, 2, 4235, ""}};
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(p1port), a0.sessionId, a0.corrId);
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && s0.subscriptionId() == r.subAt(0, n1)->subscriptionId; }));
    r.gw.pushRecords(*r.subAt(0, n1), 2, 2);   // 2..3
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 4; }));

    // Node 1 dies; the cluster elects node 2.
    r.gw.deadEndpoints.insert(n1);
    r.gw.partitions      = {{1, 0, 2, 3, 4235, ""}};
    r.gw.subscribeLeader = {{0, n2}};
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n2) != nullptr && s0.subscriptionId() == r.subAt(0, n2)->subscriptionId; },
                        20'000'000))
        << "SILENT STALL: the moved subscription never left the lost node session";
    EXPECT_EQ(r.subAt(0, n2)->fromOffset, 4) << "not resumed at its position";
    r.gw.pushRecords(*r.subAt(0, n2), 4, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 6; }));
    EXPECT_EQ(led.dupes(), 0);
    EXPECT_TRUE(s0.active());
    s0.unsubscribe();
}

// I2 (found by the 3-node transient failover e2e, seed-follows-redirect
// variant): the seed session sits on the node being stopped.  It redirects the
// moved subscription to node 1, but node 1 has not taken over yet and answers
// a bare NotLeader; the seed's node is gone meanwhile.  The moved subscription
// must retry on node 1's own session after a backoff and bind once node 1
// leads — never wait for the seed.  RED (the retry went through the seed's
// redirected-subscribe path, gated on the dead seed): never re-subscribed.
TEST(GroupRoutingRegression, MovedSubscriptionAnsweredNotLeaderByItsNewLeaderRetriesThereWithoutTheSeedRegression) {
    GroupRig r(4237, 4238);
    const auto [n1, p1port] = r.addNode(4237);
    const auto [n2, p2port] = r.addNode(4237);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4237, ""}};
    r.gw.subscribeLeader = {{0, 0}};
    r.gw.groupProtocol   = false;
    r.connect();
    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    ASSERT_TRUE(r.drive([&] { return s0.subscriptionId() != 0; }));
    const auto a0 = *r.subAt(0, 0);
    r.gw.pushRecords(a0, 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 2; }));

    // Node 0 (the seed) stops: it redirects the subscription to node 1 and dies.
    // Node 1 does not lead yet (the fake's routing still names node 2): bare NotLeader.
    r.gw.subscribeLeader[0]  = n2;
    r.gw.subscribeNotLeader  = true;
    r.gw.partitions          = {{1, 0, 1, 2, 4237, ""}};
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(p1port), a0.sessionId, a0.corrId);
    r.gw.deadEndpoints.insert(0);
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeRefusals >= 1; })) << "node 1 never saw the moved Subscribe";
    // Node 1 takes over.
    r.gw.subscribeLeader[0] = n1;
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && s0.subscriptionId() == r.subAt(0, n1)->subscriptionId; },
                        20'000'000))
        << "SILENT STALL: the moved subscription waited for the dead seed instead of retrying on node 1";
    EXPECT_EQ(r.subAt(0, n1)->fromOffset, 2);
    r.gw.pushRecords(*r.subAt(0, n1), 2, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 4; }));
    EXPECT_EQ(led.dupes(), 0);
    EXPECT_TRUE(s0.active());
    s0.unsubscribe();
}

// E2 final fix wave (C1 multi-node, broker option b): right after a full-cluster
// restart a transient partition's data lineage is not confirmed yet and the
// coordinator answers that OffsetFetch entry NotLeader (an existing retriable
// code) — several times while the cluster settles.  The member must retry it
// (paced, within its budget), never be refused and never seed a guessed
// position; once answered it seeds and consumes from the answer (-1: the reset
// policy, the log start).  Pins the client half of the broker's contract.
TEST(GroupRoutingRegression, OffsetFetchAnsweredNotLeaderWhileTheLineageSettlesIsRetriedRegression) {
    GroupRig r(4239, 4240);
    const auto [n1, p1port] = r.addNode(4239);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4239, ""}, {1, 1, 1, 1, 4239, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.committedOffsets[{1, 1}] = -1;   // the answer once settled: no committed offset (a re-minted lineage)
    r.gw.offsetFetchErrorCode  = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
    r.gw.offsetFetchErrorsNext = 4;
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 80'000'000))
        << "the member never subscribed after " << r.gw.offsetFetches << " fetches (refused: " << gc.finished() << ")";
    EXPECT_FALSE(gc.finished()) << "a retriable NotLeader answer refused the consumer";
    EXPECT_GE(r.gw.offsetFetches, 5) << "the NotLeader answers were not retried";
    EXPECT_EQ(r.subAt(1, n1)->fromOffset, 0) << "not the reset policy (the log start) after 'no committed offset'";
    r.gw.pushRecords(*r.subAt(1, n1), 0, 3);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 3; }));
}

// E2 final fix wave, residual round 3 — N6: a TRANSIENT partition whose
// committed offset did not survive (a new lineage) is answered -1 ("no
// committed offset").  The member starts it at the LOG START — the oldest
// record the ring still holds — so it reads every refilled record; it never
// applies Latest (the GroupConfig default), which would skip the refill
// (silent loss of the first records after a restart).  RED (the seed followed a
// Latest default): the subscription did not start at the log start.
TEST(GroupRoutingRegression, TransientNoCommittedOffsetSeedsTheLogStartNeverLatestRegression) {
    GroupRig r(4241, 4242);
    const auto [n1, p1port] = r.addNode(4241);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, 0, 1, 4241, ""}, {1, 1, 1, 1, 4241, ""}};
    r.gw.subscribeLeader       = {{0, 0}, {1, n1}};
    r.gw.groupAssignment       = {{1, 1}};
    r.gw.committedOffsets[{1, 1}] = -1;   // a re-minted lineage: the old offset answered "none"
    rc::GroupConfig cfg;
    auto gc = r.client.subscribeGroup("g", {"t"}, cfg);
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 1).empty() && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; }, 40'000'000))
        << "the member never subscribed after a -1 answer";
    EXPECT_EQ(r.subAt(1, n1)->fromOffset, 0)
        << "SILENT LOSS: a 'no committed offset' answer did not start at the log start (Latest skips the refill)";
    r.gw.pushRecords(*r.subAt(1, n1), 0, 5);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 5; })) << "the refilled records were not all consumed";
    EXPECT_FALSE(gc.finished());
}

// E2 final fix wave, residual round 3 — O1: a NEW (never-bound) plain
// Subscribe answered Redirect follows it PER PARTITION on the leader node's own
// session, exactly as a moved subscription does (I2) — never a seed retarget.
// Two subscriptions are bound on the seed (node 0 leads partitions 0 and 1); a
// third, new one for partition 2 (node 1 leads it) goes to the seed and is
// answered Redirect.  Expected: it binds on node 1's own session; the seed
// channel is unchanged; the first two keep receiving on the seed, every record
// exactly once.  RED (the historical Client-level retarget): the seed moved to
// node 1 and the two bound subscriptions were stranded (silent stall).
TEST(GroupRoutingRegression, NewSubscribeRedirectedFollowsPerPartitionAndNeverMovesTheSeedRegression) {
    GroupRig r(4243, 4244);
    const auto [n1, p1port] = r.addNode(4243);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4243, ""}, {1, 1, 0, 1, 4243, ""}, {1, 2, 1, 1, 4243, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, 0}, {2, n1}};
    r.gw.groupProtocol   = false;
    r.connect();
    const std::string seedBefore = r.client.testTransport().ingressChannel();

    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    auto s1 = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()}, led.handler(1));
    ASSERT_TRUE(r.drive([&] { return s0.subscriptionId() != 0 && s1.subscriptionId() != 0; }));
    const auto a0 = *r.subAt(0, 0);
    const auto a1 = *r.subAt(1, 0);
    r.gw.pushRecords(a0, 0, 2);
    r.gw.pushRecords(a1, 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 2 && led.count(1) == 2; }));

    // The new subscription: sent on the seed, answered Redirect to node 1.
    auto s2 = r.client.subscribe(r.client.topic("t"), 2, rc::SubscribeOptions{rc::From::earliest()}, led.handler(2));
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeRefusals >= 1; })) << "precondition: the seed redirected it";
    ASSERT_TRUE(r.drive([&] { return r.subAt(2, n1) != nullptr && s2.subscriptionId() == r.subAt(2, n1)->subscriptionId; },
                        20'000'000))
        << "SILENT STALL: the redirected new subscription never bound on its leader";
    const auto b2 = *r.subAt(2, n1);
    EXPECT_NE(b2.sessionId, a0.sessionId) << "bound on the seed session instead of node 1's own";
    EXPECT_EQ(r.client.testTransport().ingressChannel(), seedBefore) << "the new subscription's Redirect moved the SEED";
    EXPECT_EQ(subscribesFor(r.gw, 0), 1) << "a bound subscription was re-subscribed";
    EXPECT_EQ(subscribesFor(r.gw, 1), 1) << "a bound subscription was re-subscribed";

    // All three deliver: p0 / p1 on the seed, p2 on node 1.
    r.gw.pushRecords(a0, 2, 2);
    r.gw.pushRecords(a1, 2, 2);
    r.gw.pushRecords(b2, 0, 3);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 4 && led.count(1) == 4 && led.count(2) == 3; }))
        << "SILENT STALL: p0 " << led.count(0) << "/4, p1 " << led.count(1) << "/4, p2 " << led.count(2) << "/3";
    EXPECT_EQ(led.dupes(), 0);
    EXPECT_TRUE(s0.active());
    EXPECT_TRUE(s1.active());
    EXPECT_EQ(s0.subscriptionId(), a0.subscriptionId);
    EXPECT_EQ(s1.subscriptionId(), a1.subscriptionId);
    s0.unsubscribe();
    s1.unsubscribe();
    s2.unsubscribe();
}

// E2 final fix wave, residual round 5 — R3: the O1 per-partition follow must
// work BEFORE the first metadata answer installed any route (multiNodeRouting()
// is still false).  With nothing known about the target the follow is HELD —
// never skipped to a seed retarget (which would strand the subscriptions bound
// on the seed: a silent stall); the first metadata answer places the target
// and the subscription binds on that node's own session.  RED (gated on
// multiNodeRouting()): the seed moved.
TEST(GroupRoutingRegression, NewSubscribeRedirectedBeforeAnyMetadataStillFollowsPerPartitionRegression) {
    GroupRig r(4245, 4246);
    const auto [n1, p1port] = r.addNode(4245);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4245, ""}, {1, 1, 0, 1, 4245, ""}, {1, 2, 1, 1, 4245, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, 0}, {2, n1}};
    r.gw.groupProtocol   = false;
    r.gw.holdMetadata    = true;   // no metadata answer: no route is installed
    r.connect();
    const std::string seedBefore = r.client.testTransport().ingressChannel();

    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    auto s1 = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()}, led.handler(1));
    ASSERT_TRUE(r.drive([&] { return s0.subscriptionId() != 0 && s1.subscriptionId() != 0; }));
    const auto a0 = *r.subAt(0, 0);
    const auto a1 = *r.subAt(1, 0);
    auto s2 = r.client.subscribe(r.client.topic("t"), 2, rc::SubscribeOptions{rc::From::earliest()}, led.handler(2));
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeRefusals >= 1; })) << "precondition: the seed redirected it";
    // No metadata answer yet: the follow is HELD — the seed never moves.
    ASSERT_TRUE(r.drive([&] { return r.client.testRedirectFollowsHeld() >= 1; }))
        << "the follow was neither held nor done before any metadata (skipped)";
    (void)r.drive([&] { return false; }, 200'000);
    EXPECT_EQ(r.client.testTransport().ingressChannel(), seedBefore) << "the held follow moved the SEED";
    // The first metadata answer arrives: the follow goes per partition.
    ASSERT_FALSE(r.gw.metadataCorrIds.empty());
    r.gw.holdMetadata = false;
    r.gw.sendMetadata(a0.sessionId, r.gw.metadataCorrIds.back());
    ASSERT_TRUE(r.drive([&] { return r.subAt(2, n1) != nullptr && s2.subscriptionId() == r.subAt(2, n1)->subscriptionId; },
                        20'000'000))
        << "SILENT STALL: the held follow never bound on its leader after the first metadata answer";
    EXPECT_EQ(r.client.testTransport().ingressChannel(), seedBefore)
        << "the new subscription's Redirect moved the SEED (no route yet: the follow was skipped)";
    const auto b2 = *r.subAt(2, n1);
    r.gw.pushRecords(a0, 0, 2);
    r.gw.pushRecords(a1, 0, 2);
    r.gw.pushRecords(b2, 0, 2);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 2 && led.count(1) == 2 && led.count(2) == 2; }))
        << "SILENT STALL: p0 " << led.count(0) << "/2, p1 " << led.count(1) << "/2, p2 " << led.count(2) << "/2";
    EXPECT_EQ(led.dupes(), 0);
    s0.unsubscribe();
    s1.unsubscribe();
    s2.unsubscribe();
}

// E2 final fix wave, residual round 5 — R4: a Redirect counts only from the
// session its pending Subscribe is outstanding on.  The Subscribe was sent on
// seed session S1 (its answer lost); the seed reconnected (S2) and re-sent it.
// A LATE Redirect from S1 then arrives for the same correlationId: following
// it would bind the subscription a second time, on node 1, while the copy
// re-sent on S2 binds on the seed.  It is ignored and counted; the
// subscription binds exactly once.  RED (no session check): node 1 got the
// Subscribe too.
TEST(GroupRoutingRegression, RedirectFromAnEarlierSessionNeverBindsASubscriptionTwiceRegression) {
    GroupRig r(4247, 4248);
    const auto [n1, p1port] = r.addNode(4247);
    r.gw.moreBrokers   = {{1, "127.0.0.1", p1port}};
    r.gw.partitions    = {{1, 0, 0, 1, 4247, ""}};
    r.gw.groupProtocol = false;
    r.connect();
    const std::string seedBefore = r.client.testTransport().ingressChannel();
    r.gw.dropSubscribeResponsesNext = 2;   // the answers of the first send AND the reconnect's re-send are lost
    PlainLedger led;
    auto s = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    ASSERT_TRUE(r.drive([&] { return r.gw.subscribeCorrIds.size() == 1; }));
    const int64_t corr = r.gw.subscribeCorrIds[0];
    // F-A C1: the rig hands out unique session ids, so the expiry names the seed's REAL session (a notice for any other
    // id is stale and moves nothing), and the resume is answered with a NEW id (S2), as the scenario requires.
    r.gw.newIdOnResume = true;
    const int64_t s1   = r.client.testSeedSessionId();
    r.gw.expireSession(s1);   // reconnect: S2, the pending Subscribe re-sent there
    ASSERT_TRUE(r.drive([&] {
        return r.gw.subscribeCorrIds.size() >= 2 && r.client.testSeedSessionId() != s1 && r.client.testSeedSessionId() != 0;
    })) << "precondition: re-sent on a new seed session";
    ASSERT_EQ(r.gw.subscribeCorrIds.back(), corr);

    // The late Redirect of the FIRST send, from S1.
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::Redirect),
                   "127.0.0.1:" + std::to_string(p1port), s1, corr);
    ASSERT_TRUE(r.drive([&] { return r.client.testStaleSubscribeRedirects() >= 1 || r.subAt(0, n1) != nullptr; }))
        << "the late Redirect was neither ignored nor followed";
    (void)r.drive([&] { return r.subAt(0, n1) != nullptr; }, 300'000);
    EXPECT_EQ(r.subAt(0, n1), nullptr) << "DUPLICATE: a Redirect from an earlier session bound the subscription on node 1";
    EXPECT_EQ(r.client.testStaleSubscribeRedirects(), 1u);
    EXPECT_EQ(r.client.testTransport().ingressChannel(), seedBefore) << "the stale Redirect moved the seed";
    EXPECT_FALSE(s.finished());

    // The copy outstanding on the seed binds (one more re-send after a reconnect): exactly once.
    // F-A C1: the expiry must name the seed's CURRENT session (the rig hands out unique ids) - a notice for any other
    // id is a stale one and, correctly, moves nothing.
    const int64_t s2 = r.client.testSeedSessionId();
    r.gw.expireSession(s2);
    ASSERT_TRUE(r.drive([&] { return s.subscriptionId() != 0; })) << "the subscription never bound on the seed";
    EXPECT_EQ(r.subAt(0, n1), nullptr);
    r.gw.pushRecords(*r.subAt(0, 0), 0, 3);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 3; }));
    EXPECT_EQ(led.dupes(), 0);
    s.unsubscribe();
}

// E2 final fix wave, residual round 6 — F1 (plain subscriptions): a bound
// plain subscription on a TRANSIENT partition lives on its leader node's own
// session (homed).  The node crashes and comes back with an EMPTY ring (a new
// data lineage); the client re-arms the subscription from its position (20)
// after the lost session.  It must never carry that position across the
// lineage change: the resume CLAIMS the lineage the broker noted at bind, the
// broker answers PartitionDataReset, and the subscription restarts at the log
// start with no resume floor — the application is told (dataResets() == 1) and
// receives every post-restart record exactly once.  RED (no claim): the resume
// bound at 20 and the floor dropped the refilled records 0..5 (silent loss).
TEST(GroupRoutingRegression, PlainSubscriptionNeverCarriesAPositionAcrossALineageChangeRegression) {
    GroupRig r(4249, 4250);
    const auto [n1, p1port] = r.addNode(4249);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4249, ""}, {1, 2, 1, 1, 4249, ""}};
    r.gw.subscribeLeader = {{0, 0}, {2, n1}};
    r.gw.groupProtocol   = false;
    r.connect();
    PlainLedger led;
    auto s2 = r.client.subscribe(r.client.topic("t"), 2, rc::SubscribeOptions{rc::From::earliest()}, led.handler(2));
    ASSERT_TRUE(r.drive([&] { return r.subAt(2, n1) != nullptr && s2.subscriptionId() == r.subAt(2, n1)->subscriptionId; }))
        << "precondition: bound on node 1's own session";
    const auto b1 = *r.subAt(2, n1);
    r.gw.sendLineageNote(b1, 0x1111);   // the data lineage at bind
    r.gw.pushRecords(b1, 0, 20);
    ASSERT_TRUE(r.drive([&] { return led.count(2) == 20; }));

    // Node 1 restarts with an empty ring: a new lineage; its session is lost.
    r.gw.partitionLineage[2] = 0x2222;
    r.gw.expireSession(b1.sessionId);
    ASSERT_TRUE(r.drive([&] {
        const auto* s = r.subAt(2, n1);
        return r.gw.dataResetAnswers >= 1 && s != nullptr && s->fromOffset == -2 &&
               s2.subscriptionId() == s->subscriptionId;
    }, 20'000'000)) << "the resume never claimed its lineage / never restarted at the log start (claims: "
                    << r.gw.lineageClaims.size() << ", resets " << r.gw.dataResetAnswers << ")";
    EXPECT_NE(std::find(r.gw.lineageClaims.begin(), r.gw.lineageClaims.end(), uint64_t{0x1111}),
              r.gw.lineageClaims.end()) << "the resume did not claim the noted lineage";
    EXPECT_EQ(s2.dataResets(), 1u) << "the reset was not surfaced to the application";
    EXPECT_TRUE(s2.active());

    // Every post-restart record exactly once, from the log start (no floor).
    led.seen[2].clear();
    const auto b2 = *r.subAt(2, n1);
    r.gw.pushRecords(b2, 0, 6);
    ASSERT_TRUE(r.drive([&] { return led.count(2) == 6; }))
        << "SILENT LOSS: " << led.count(2) << "/6 post-restart records delivered (a resume floor dropped them)";
    EXPECT_EQ(led.dupes(), 0);
    s2.unsubscribe();
}

// E2 final fix wave — FU1 seed-session check: a plain subscription BOUND on the
// SEED session is never re-armed when that session is lost (every way back is
// a new broker session, which ended the old one's subscriptions).  It must end
// EXPLICITLY (SessionLost) — never stay active() and silent.  A subscription
// homed on a node's own session is untouched (driveHomed owns it).  RED (no
// end on seed loss): s0 stayed active with no subscription broker-side.
TEST(GroupRoutingRegression, SeedBoundPlainSubscriptionEndsExplicitlyWhenTheSeedSessionIsLostRegression) {
    GroupRig r(4251, 4252);
    const auto [n1, p1port] = r.addNode(4251);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4251, ""}, {1, 2, 1, 1, 4251, ""}};
    r.gw.subscribeLeader = {{0, 0}, {2, n1}};
    r.gw.groupProtocol   = false;
    r.connect();
    PlainLedger led;
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()}, led.handler(0));
    auto s2 = r.client.subscribe(r.client.topic("t"), 2, rc::SubscribeOptions{rc::From::earliest()}, led.handler(2));
    ASSERT_TRUE(r.drive([&] {
        return r.subAt(0, 0) != nullptr && s0.subscriptionId() == r.subAt(0, 0)->subscriptionId &&
               r.subAt(2, n1) != nullptr && s2.subscriptionId() == r.subAt(2, n1)->subscriptionId;
    })) << "precondition: p0 bound on the seed, p2 on node 1's own session";
    const auto b0 = *r.subAt(0, 0);
    r.gw.pushRecords(b0, 0, 5);
    ASSERT_TRUE(r.drive([&] { return led.count(0) == 5; }));

    const int connectsBefore = r.gw.connects;
    r.gw.expireSession(b0.sessionId);   // the seed session is lost
    ASSERT_TRUE(r.drive([&] { return !s0.active(); }, 20'000'000))
        << "SILENT STALL: the seed-bound subscription stayed active after its session was lost";
    EXPECT_EQ(s0.failReason(), rc::SubscriptionFailReason::SessionLost);
    EXPECT_EQ(s0.position(), 5) << "the position to resubscribe from";
    EXPECT_TRUE(s2.active()) << "a node-homed subscription is not the seed's";
    ASSERT_TRUE(r.drive([&] { return r.gw.connects > connectsBefore; }, 20'000'000)) << "the seed reconnects";
    s2.unsubscribe();
}

// DR client bug 1 (dr3 / dr13: no re-route after a leader SIGKILL): every
// MetadataRequest rode the SEED session — when the seed's broker is the one that
// died, nothing could ever learn the new leaders (Client::requestMetadataNow_ /
// tickMetadataRefresh asked only while the seed was Connected).  The new leader
// must be learnt from ANY live broker.  Isolated from the seed failover: the seed's
// node goes dead AND the live node never answers a new Connect (so the seed's
// failover attempt to it cannot complete) — yet that node's EXISTING session must
// carry the MetadataRequest.
// RED (before the fix): no MetadataRequest ever reaches node 1 after the seed died.
TEST(GroupRoutingRegression, MetadataIsAskedThroughALiveNodeSessionWhileTheSeedIsDownRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000LL;    // 50 ms
    o.responseTimeoutNs = 400'000'000LL;   // the seed goes stale 400 ms after its node died
    GroupRig r(4261, 4262, o);
    const auto [n1, p1port] = r.addNode(4261);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4261, ""}, {1, 1, 1, 1, 4261, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupProtocol   = false;
    r.connect();
    PlainLedger led;
    auto s1 = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()}, led.handler(1));
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && s1.subscriptionId() == r.subAt(1, n1)->subscriptionId; }))
        << "precondition: node 1 has its own Connected session (p1 homed there)";
    const auto askedAt = [&](int ep) {
        int n = 0;
        for (int e : r.gw.metadataEndpoints) n += e == ep ? 1 : 0;
        return n;
    };
    const int beforeN1 = askedAt(n1);
    r.gw.deadEndpoints.insert(0);      // the seed's broker dies (hears nothing more)
    r.gw.silentEndpoints.insert(n1);   // node 1 answers no NEW connect (the seed cannot fail over to it)
    const auto t0 = std::chrono::steady_clock::now();
    bool asked = false;
    for (const auto deadline = t0 + std::chrono::seconds(5); std::chrono::steady_clock::now() < deadline && !asked;) {
        r.client.doWork();
        r.gw.pump();
        asked = askedAt(n1) > beforeN1;
    }
    EXPECT_TRUE(asked) << "the new leader must be learnt from a live broker's session while the seed is down";
    EXPECT_NE(r.client.testSessionStateAt(0), rc::session::SessionState::Connected) << "the seed is down";
    EXPECT_GE(r.client.testMetadataViaOtherSession(), 1u);
    s1.unsubscribe();
}

// ===========================================================================
// DR client bugs — fix round 1 (client-dr-bugs-review.md recipes C1, I1-I4, M1)
// ===========================================================================
using namespace std::chrono_literals;
namespace {
/// Send one acked record and drive until it completes (bounded); the status, or
/// Closed when it never completed within `cycles`.
rc::OpStatus sendOne(GroupRig& r, rc::Producer& p, const std::string& topic, uint64_t key, int cycles = 4'000'000) {
    const std::vector<uint8_t> val{1};
    const rc::Token t = r.nextToken++;
    if (p.trySend(r.client.topic(topic), rc::Key(key), std::span<const uint8_t>(val), r.cq, t) != rc::SendResult::Ok)
        return rc::OpStatus::Closed;
    rc::OpStatus st = rc::OpStatus::Closed;
    bool done = false;
    (void)r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == t) { done = true; st = cc.status; }
        });
        return done;
    }, cycles);
    return done ? st : rc::OpStatus::Closed;
}
template <class P>
bool driveFor(GroupRig& r, std::chrono::milliseconds budget, P&& pred) {
    for (const auto deadline = std::chrono::steady_clock::now() + budget; std::chrono::steady_clock::now() < deadline;) {
        r.client.doWork();
        r.gw.pump();
        if (pred()) return true;
    }
    return false;
}
RigOptions failoverOpts() {
    RigOptions o;
    o.keepaliveNs       = 50'000'000LL;    // 50 ms
    o.responseTimeoutNs = 400'000'000LL;   // a dead node is noticed 400 ms later
    o.connectTimeoutMs  = 300;
    return o;
}
}  // namespace

// C1: after the seed moved off its bootstrap node, "no route entry = the seed's
// broker" must mean where the seed PHYSICALLY is.  The bootstrap node regains the
// partition's leadership; its records must reach it.
// RED (setRoute compares with the seed's bootstrap identity): the metadata naming
// node 0 ERASES the route — the records ride the seed (now on node 1), which
// answers NotLeader, forever; none reaches node 0.
TEST(GroupRoutingRegression, SeedMovedThenTheBootstrapLeadsAgainRoutesToTheBootstrapRegression) {
    GroupRig r(4301, 4302, failoverOpts());
    const auto [n1, p1port] = r.addNode(4301);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {{2, 0, 0, 1, 4301, ""}};   // b/0 led by node 0 (the seed's bootstrap)
    r.gw.groupProtocol = false;
    r.connect();
    rc::ProducerConfig pc;
    pc.lingerUs = 0;
    pc.deliveryTimeoutMs = 600'000;
    auto p = r.client.producer(pc);
    ASSERT_EQ(sendOne(r, p, "b", 1), rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, 0);

    // Node 0 dies; the cluster moves b/0 to node 1; the seed fails over to node 1.
    r.gw.deadEndpoints.insert(0);
    r.gw.partitions = {{2, 0, 1, 2, 4301, ""}};
    ASSERT_TRUE(driveFor(r, 10s, [&] {
        return r.client.testSeedFailovers() >= 1 && r.client.testSessionStateAt(0) == rc::session::SessionState::Connected;
    })) << "precondition: the seed failed over";
    ASSERT_TRUE(rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p1port)));
    ASSERT_EQ(sendOne(r, p, "b", 1), rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, n1) << "precondition: b/0 served by node 1";

    // Node 0 is back and leads b/0 again; node 1 no longer owns it.
    r.gw.deadEndpoints.erase(0);
    r.gw.partitions = {{2, 0, 0, 3, 4301, ""}};
    r.gw.notLeaderEndpoints = {n1};
    int okOnNode0 = 0;
    for (int i = 0; i < 10; ++i) {
        const auto st = sendOne(r, p, "b", 1, 2'000'000);
        if (st == rc::OpStatus::Ok && r.gw.produced.back().endpoint == 0) ++okOnNode0;
    }
    EXPECT_EQ(okOnNode0, 10) << "every b/0 record reaches its leader (node 0, the seed's old bootstrap)";
    EXPECT_TRUE(rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p1port)))
        << "the seed stays where it is (it is not moved 'home')";
}

// C1, the group-fetch sibling: a member's fetch of a partition led by the seed's
// old bootstrap node must reach that node.  RED: the route is erased and the
// fetch stays on the seed (node 1).
TEST(GroupRoutingRegression, SeedMovedGroupFetchOfABootstrapLedPartitionIsServedRegression) {
    GroupRig r(4303, 4304, failoverOpts());
    const auto [n1, p1port] = r.addNode(4303);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4303, ""}, {1, 1, 1, 1, 4303, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    r.gw.holdJoins       = false;
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }));

    r.gw.deadEndpoints.insert(0);   // node 0 (seed + coordinator) dies; t/0 moves to node 1
    r.gw.partitions      = {{1, 0, 1, 2, 4303, ""}, {1, 1, 1, 1, 4303, ""}};
    r.gw.subscribeLeader = {{0, n1}, {1, n1}};
    ASSERT_TRUE(driveFor(r, 15s, [&] { return r.client.testSeedFailovers() >= 1 && r.subAt(0, n1) != nullptr; }))
        << "precondition: the member fetches t/0 from node 1 after the failover";

    r.gw.deadEndpoints.erase(0);    // node 0 is back and leads t/0 again
    r.gw.partitions      = {{1, 0, 0, 3, 4303, ""}, {1, 1, 1, 1, 4303, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.client.testRequestMetadataNow();
    EXPECT_TRUE(driveFor(r, 5s, [&] { return r.subAt(0, 0) != nullptr; }))
        << "the t/0 fetch reaches its leader (node 0), not the seed's broker";
    gc.close();
}

// I1: every partition is led by the seed's broker (the route table is EMPTY), so
// the seed's reconnect raises no per-partition connect event.  After the failover
// the in-flight batches must be re-offered at once.
// RED (no re-offer on the failover's Connected edge): they wait for the 30 s attempt timer.
TEST(GroupRoutingRegression, SingleRouteTopologySeedFailoverReOffersInFlightAtOnceRegression) {
    GroupRig r(4305, 4306, failoverOpts());
    const auto [n1, p1port] = r.addNode(4305);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.partitions  = {{2, 0, 0, 1, 4305, ""}};
    r.gw.groupProtocol = false;
    r.connect();
    rc::ProducerConfig pc;
    pc.lingerUs = 0; pc.batchBytes = 1;
    pc.produceTimeoutMs = 30'000; pc.deliveryTimeoutMs = 40'000;
    auto p = r.client.producer(pc);
    r.gw.holdPartitions = {0};   // node 0 takes them, never answers
    const std::vector<uint8_t> val{1};
    std::vector<rc::Token> toks;
    for (int i = 0; i < 3; ++i) {
        toks.push_back(r.nextToken++);
        ASSERT_TRUE(r.drive([&] {
            return p.trySend(r.client.topic("b"), rc::Key(uint64_t{1}), std::span<const uint8_t>(val), r.cq,
                             toks.back()) == rc::SendResult::Ok;
        }));
    }
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() >= 3; })) << "precondition: 3 batches in flight on node 0";
    r.gw.deadEndpoints.insert(0);
    r.gw.partitions = {{2, 0, 1, 2, 4305, ""}};   // b/0 moved to node 1 (where the seed fails over)
    r.gw.holdPartitions.clear();
    ASSERT_TRUE(driveFor(r, 10s, [&] { return r.client.testSeedFailovers() >= 1; }));
    std::map<rc::Token, rc::OpStatus> done;
    EXPECT_TRUE(driveFor(r, 3s, [&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) { done[cc.token] = cc.status; });
        return done.size() == 3;
    })) << "the in-flight batches were not re-offered after the failover (only " << done.size() << "/3)";
    for (const rc::Token t : toks) EXPECT_EQ(done[t], rc::OpStatus::Ok);
    int onN1 = 0;
    for (const auto& pr : r.gw.produced) onN1 += pr.endpoint == n1 ? 1 : 0;
    EXPECT_EQ(onN1, 3) << "each batch reached the new leader exactly once";
    // Fix round 2 (C-1): the re-offered frames carry the seed's NEW session id (the strict fake
    // drops a Produce for an unknown session unanswered, as the broker does).
    EXPECT_EQ(r.gw.droppedUnknownSessionProduce, 0);
    for (const auto& pr : r.gw.produced)
        if (pr.endpoint == n1) EXPECT_EQ(pr.sessionId, r.client.testSeedSessionId());
}

// Fix round 2 (C-1): a single node; the seed's session is expired and the resume is answered
// with a NEW id.  The in-flight batches are re-offered at once on the seed's new session.
// RED (no Connected-edge re-offer): they wait for the 30 s attempt timer; with the default
// branch's restamp also gone they are dropped as unknown-session frames even then.
TEST(GroupRoutingRegression, SeedResumedUnderANewSessionIdReOffersInFlightWithTheNewIdRegression) {
    GroupRig r(4321, 4322, failoverOpts());
    r.gw.newIdOnResume = true;
    r.gw.partitions  = {{2, 0, 0, 1, 4321, ""}};
    r.gw.groupProtocol = false;
    r.connect();
    rc::ProducerConfig pc;
    pc.lingerUs = 0; pc.batchBytes = 1;
    pc.produceTimeoutMs = 30'000; pc.deliveryTimeoutMs = 40'000;
    auto p = r.client.producer(pc);
    r.gw.holdPartitions = {0};
    const std::vector<uint8_t> val{1};
    std::vector<rc::Token> toks;
    for (int i = 0; i < 2; ++i) {
        toks.push_back(r.nextToken++);
        ASSERT_TRUE(r.drive([&] {
            return p.trySend(r.client.topic("b"), rc::Key(uint64_t{1}), std::span<const uint8_t>(val), r.cq,
                             toks.back()) == rc::SendResult::Ok;
        }));
    }
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() >= 2; }));
    const int64_t s1 = r.client.testSeedSessionId();
    r.gw.held.clear();          // the old session's in-flight frames are gone with it
    r.gw.holdPartitions.clear();
    r.gw.expireSession(s1);
    std::map<rc::Token, rc::OpStatus> done;
    EXPECT_TRUE(driveFor(r, 3s, [&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) { done[cc.token] = cc.status; });
        return done.size() == 2;
    })) << "the in-flight batches were not re-offered on the new session (" << done.size() << "/2)";
    for (const rc::Token t : toks) EXPECT_EQ(done[t], rc::OpStatus::Ok);
    EXPECT_NE(r.client.testSeedSessionId(), s1);
    EXPECT_EQ(r.gw.droppedUnknownSessionProduce, 0);
    EXPECT_EQ(r.client.testSeedFailovers(), 0u);
}

// I2(a): the seed's node died and the only other broker refuses every Connect
// (retriable code): attempts back off exponentially.
// RED (a fixed 100 ms gap): a refusal every ~200 ms.
TEST(GroupRoutingRegression, SeedFailoverBacksOffExponentiallyAgainstRefusingBrokersRegression) {
    GroupRig r(4307, 4308, failoverOpts());
    const auto [n1, p1port] = r.addNode(4307);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints.insert(0);
    r.gw.refuseConnectsWith[n1] = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    ASSERT_TRUE(driveFor(r, 5s, [&] { return r.gw.connectsAt[n1] >= 1; })) << "precondition: the first attempt";
    const int first = r.gw.connectsAt[n1];
    (void)driveFor(r, 3s, [] { return false; });
    const int refused = r.gw.connectsAt[n1] - first;
    EXPECT_LE(refused, 3) << "refused Connects at node 1 in 3 s (exponential gaps 100 ms .. 10 s; fixed 100 ms: >= 5)";
    std::fprintf(stderr, "[I2a] refused connects in 3 s: %d\n", refused);
}

// I2(b): a non-retriable connect answer ends the failover — the seed stays down,
// explicitly; no further attempts.  RED: attempts continue forever.
TEST(GroupRoutingRegression, SeedFailoverStopsOnANonRetriableConnectErrorRegression) {
    GroupRig r(4309, 4310, failoverOpts());
    const auto [n1, p1port] = r.addNode(4309);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints.insert(0);
    r.gw.refuseConnectsWith[n1] = static_cast<int16_t>(rune::protocol::client::ErrorCode::ProtocolVersion);
    ASSERT_TRUE(driveFor(r, 5s, [&] { return r.gw.connectsAt[n1] >= 1; }));
    (void)driveFor(r, 200ms, [] { return false; });
    const int after = r.gw.connectsAt[n1];
    (void)driveFor(r, 2s, [] { return false; });
    EXPECT_EQ(r.gw.connectsAt[n1], after) << "no Connect after a non-retriable refusal";
    EXPECT_EQ(r.client.testSeedFailovers(), 1u);
    EXPECT_NE(r.client.testSessionStateAt(0), rc::session::SessionState::Connected);
}

// I2(c): a reconnectForeverAfterLive (shard) client — every broker dead: the
// failover never connects by itself, it only re-points the seed between the
// machine's own Backoff attempts (connect rate == the Backoff schedule).
// RED (the failover forceReconnects on its own deadlines): 16-18 Connects in 4 s vs 10, and
// testSeedFailoverConnects() > 0.
TEST(GroupRoutingRegression, ShardClientSeedFailoverNeverUndercutsBackoffRegression) {
    RigOptions o = failoverOpts();
    o.reconnectForever = true;
    GroupRig r(4311, 4312, o);
    const auto [n1, p1port] = r.addNode(4311);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints = {0, n1};   // every broker dead (their Connects still counted: connectsArrived)
    ASSERT_TRUE(driveFor(r, 5s, [&] { return r.client.testSessionStateAt(0) != rc::session::SessionState::Connected; }));
    const int c0 = r.gw.connectsArrived;
    (void)driveFor(r, 4s, [] { return false; });
    const int c = r.gw.connectsArrived - c0;
    std::fprintf(stderr, "[I2c] connects in 4 s: %d (failovers %llu)\n", c,
                 static_cast<unsigned long long>(r.client.testSeedFailovers()));
    // Every Connect is the machine's own Backoff attempt: the failover only re-points the publication.
    // (Measured on an idle box: 10 Connects in these 4 s with the failover off, 10 with it, 16-18 RED.)
    EXPECT_EQ(r.client.testSeedFailoverConnects(), 0u) << "the failover issued Connects of its own";
    EXPECT_LE(c, 12) << "connects in 4 s above the Backoff schedule (failover off: 10)";   // load only lowers it
    EXPECT_GE(r.client.testSeedFailovers(), 1u) << "the failover still re-points the seed between attempts";
}

// I3: candidates rotate.  Node 1 keeps an old Connected session but answers no new
// Connect; node 2 is healthy.  RED ("a live session's broker always wins"): the
// seed retries node 1 forever.
TEST(GroupRoutingRegression, SeedFailoverTriesTheNextBrokerWhenALiveSessionsBrokerRefusesConnectsRegression) {
    GroupRig r(4313, 4314, failoverOpts());
    const auto [n1, p1port] = r.addNode(4313);
    const auto [n2, p2port] = r.addNode(4313);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions  = {{2, 0, 1, 1, 4313, ""}};   // b/0 on node 1: its session is Connected
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(2, 0).empty(); }));
    rc::ProducerConfig pc;
    pc.lingerUs = 0;
    pc.deliveryTimeoutMs = 600'000;
    auto p = r.client.producer(pc);
    ASSERT_EQ(sendOne(r, p, "b", 1), rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, n1);
    r.gw.silentEndpoints.insert(n1);   // node 1 answers no NEW Connect (its old session lives)
    r.gw.deadEndpoints.insert(0);      // the seed's node dies
    EXPECT_TRUE(driveFor(r, 6s, [&] {
        return r.client.testSessionStateAt(0) == rc::session::SessionState::Connected &&
               rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p2port));
    })) << "the seed must reach the healthy node 2 (seed on " << r.client.testSeedChannel() << ")";
    (void)n2;
}

// I4: a route failure asks EVERY Connected broker.  The seed's broker (alive) lags
// and keeps naming the dead node 2 as b/0's leader; node 1 knows the new leader.
// RED (one broker asked): the route stays on the dead node forever.
TEST(GroupRoutingRegression, StaleMetadataFromOneBrokerDoesNotPinTheRouteToTheDeadLeaderRegression) {
    GroupRig r(4315, 4316, failoverOpts());
    const auto [n1, p1port] = r.addNode(4315);
    const auto [n2, p2port] = r.addNode(4315);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions  = {{1, 1, 1, 1, 4315, ""}, {2, 0, 2, 1, 4315, ""}};   // t/1 on node 1, b/0 on node 2
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] {
        return !r.client.testDataChannelFor(2, 0).empty() && !r.client.testDataChannelFor(1, 1).empty();
    }));
    rc::ProducerConfig pc;
    pc.lingerUs = 0;
    pc.deliveryTimeoutMs = 600'000;
    auto p = r.client.producer(pc);
    ASSERT_EQ(sendOne(r, p, "b", 1), rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, n2);
    uint64_t k1 = 0;   // a key of t/1, so node 1's session is Connected
    while (rc::Key(k1).partition(2) != 1) ++k1;
    ASSERT_EQ(sendOne(r, p, "t", k1), rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.back().endpoint, n1);

    r.gw.deadEndpoints.insert(n2);   // b/0's leader dies; node 1 leads it now (epoch 2)
    r.gw.partitionsAt[0]  = {{1, 1, 1, 1, 4315, ""}, {2, 0, 2, 1, 4315, ""}};   // the seed's broker lags
    r.gw.partitionsAt[n1] = {{1, 1, 1, 1, 4315, ""}, {2, 0, 1, 2, 4315, ""}};
    const auto st = sendOne(r, p, "b", 1, 6'000'000);
    EXPECT_EQ(st, rc::OpStatus::Ok) << "b/0's record never reached its new leader";
    EXPECT_EQ(r.gw.produced.back().endpoint, n1);
}

// M1: the CLIENT stalls past its response timeout (no doWork for 600 ms) while its
// broker is healthy: the seed resumes there — never a failover away from it.
// RED (an own-stall loss counts as a dead broker): the seed moves to node 1.
TEST(GroupRoutingRegression, ClientStallDoesNotMoveTheSeedRegression) {
    GroupRig r(4317, 4318, failoverOpts());
    const auto [n1, p1port] = r.addNode(4317);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    std::this_thread::sleep_for(600ms);   // the client stalls (nothing pumped)
    EXPECT_TRUE(driveFor(r, 5s, [&] {
        return r.client.testSessionStateAt(0) == rc::session::SessionState::Connected;
    }));
    (void)driveFor(r, 300ms, [] { return false; });
    EXPECT_EQ(r.client.testSeedFailovers(), 0u) << "a client stall is not a dead broker";
    EXPECT_FALSE(rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p1port)));
}

// I7 (shard sibling): a reconnectForeverAfterLive client whose seed broker dies
// reaches a live broker — the failover re-points the machine's own Backoff
// attempts at the other broker.  RED (no failover): the machine alternates its
// bootstrap and last-known leader — both the dead node — forever.
TEST(GroupRoutingRegression, ShardClientSeedFailsOverToALiveBrokerRegression) {
    RigOptions o = failoverOpts();
    o.reconnectForever = true;
    GroupRig r(4319, 4320, o);
    const auto [n1, p1port] = r.addNode(4319);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints.insert(0);
    EXPECT_TRUE(driveFor(r, 10s, [&] {
        return r.client.testSessionStateAt(0) == rc::session::SessionState::Connected &&
               rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p1port));
    })) << "the shard client's seed never reached the live broker (seed on " << r.client.testSeedChannel() << ")";
    (void)n1;
}

// F-A C1, the seed-failover route: the seed failed over from node 0 to node 1 (a new session there).  When node 0 is
// reachable again its lease reap tells the ABANDONED session SessionExpired on the client's single egress channel:
// the healthy session on node 1 stays Connected.
TEST(ClientSessionRegression, StaleNoticeAfterASeedFailoverLeavesTheNewSeedConnectedRegression) {
    GroupRig r(4311, 4312, failoverOpts());
    const auto [n1, p1port] = r.addNode(4311);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    const int64_t oldSid = r.gw.lastUniqueSessionId();
    r.gw.deadEndpoints.insert(0);
    ASSERT_TRUE(driveFor(r, 10s, [&] {
        return r.client.testSeedFailovers() >= 1 && r.client.testSessionStateAt(0) == rc::session::SessionState::Connected;
    })) << "precondition: the seed failed over and is Connected on node 1";
    ASSERT_NE(r.gw.lastUniqueSessionId(), oldSid);
    r.gw.deadEndpoints.erase(0);   // node 0 is reachable again: its reap notifies the abandoned session
    const int connects = r.gw.connectsAt[n1];
    int appErrors = 0;   // fix round 2 (M-7)
    r.client.onError([&](const std::string&) { ++appErrors; });
    test_support::TestCq cq(r.client);
    // A pending op (an unanswered schema read: keepalive pongs must keep flowing).
    auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("t", q, t); });
    (void)driveFor(r, 100ms, [] { return false; });
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "", oldSid, 0);
    ASSERT_TRUE(driveFor(r, 3s, [&] { return r.client.testStaleSessionNotices() == 1; }));
    (void)driveFor(r, 300ms, [] { return false; });
    EXPECT_EQ(r.client.testSessionStateAt(0), rc::session::SessionState::Connected)
        << "the abandoned node-0 session's notice tore down the node-1 seed";
    EXPECT_EQ(r.gw.connectsAt[n1], connects);
    EXPECT_FALSE(ping.ready()) << ping.describe();
    EXPECT_EQ(appErrors, 0);
}


// ===========================================================================
// DR client bugs — fix round 2 (client-failover-review.md recipes I-1 .. I-4)
// ===========================================================================
namespace pcl = rune::protocol::client;
namespace {
/// Two nodes; the seed's node dies and its failover connect to node 1 is held open
/// (delayed connect replies): returns once the seed is mid-failover (not Connected).
RigOptions slowConnectOpts() {   // the failover's connect is held open (delayed reply) but inside its timeout
    RigOptions o = failoverOpts();
    o.connectTimeoutMs = 3000;
    return o;
}
void startFailoverWindow(GroupRig& r, std::chrono::milliseconds connectDelay) {
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.connectReplyDelay = connectDelay;
    r.gw.deadEndpoints.insert(0);
    ASSERT_TRUE(driveFor(r, 10s, [&] {
        return r.client.testSeedFailovers() >= 1 &&
               r.client.testSessionStateAt(0) != rc::session::SessionState::Connected;
    })) << "precondition: the seed is failing over";
}
}  // namespace

// I-1: a DDL issued while the seed fails over WAITS for it (inside ddlTimeoutMs) and succeeds on the
// new session.  RED (the old gate): it completes NotConnected at once.
TEST(GroupRoutingRegression, DdlIssuedWhileTheSeedFailsOverWaitsAndSucceedsRegression) {
    GroupRig r(4323, 4324, slowConnectOpts());
    const auto [n1, p1port] = r.addNode(4323);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    std::vector<std::pair<int, int64_t>> arrivals;
    r.gw.templateHandlers[pcl::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pcl::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        arrivals.emplace_back(ep, m->sessionId);
        pcl::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.objectId = 41; resp.newVersion = 1;
        r.gw.sendDdlResponse(ep, resp);
    };
    startFailoverWindow(r, 600ms);
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    (void)driveFor(r, 300ms, [] { return false; });
    EXPECT_FALSE(f.ready()) << "the DDL waits for the seed (" << (f.ready() ? f.describe() : "") << ")";
    EXPECT_TRUE(driveFor(r, 5s, [&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 41);
    ASSERT_EQ(arrivals.size(), 1u);
    EXPECT_EQ(arrivals[0].first, n1);
    EXPECT_EQ(arrivals[0].second, r.client.testSeedSessionId());
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
}

// I-1: Ping and GetTopicSchema issued mid-failover wait and are answered on the new session.
// RED (the old gate): NotConnected at once.
TEST(GroupRoutingRegression, PingAndSchemaIssuedWhileTheSeedFailsOverWaitRegression) {
    GroupRig r(4325, 4326, slowConnectOpts());
    const auto [n1, p1port] = r.addNode(4325);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.replySchemas = true;
    r.gw.schemaBlob = {1, 2, 3, 4};
    startFailoverWindow(r, 600ms);
    test_support::TestCq cq(r.client);
    const std::vector<uint8_t> echo{9, 9};
    auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(echo, q, t); });
    auto sch  = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getTopicSchema("t", q, t); });
    ASSERT_TRUE(ping.accepted() && sch.accepted());
    EXPECT_TRUE(driveFor(r, 5s, [&] { return ping.ready() && sch.ready(); }));
    EXPECT_TRUE(ping.ok()) << ping.describe();
    EXPECT_TRUE(sch.ok()) << sch.describe();
    EXPECT_EQ(cq.timesCompleted(ping.token()), 1);
    EXPECT_EQ(cq.timesCompleted(sch.token()), 1);
    (void)n1;
}

// I-1: GetViewRow (routed like a DDL) issued mid-failover waits and is answered.  RED: NotConnected.
TEST(GroupRoutingRegression, GetViewRowIssuedWhileTheSeedFailsOverWaitsRegression) {
    GroupRig r(4327, 4328, slowConnectOpts());
    const auto [n1, p1port] = r.addNode(4327);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    const std::vector<uint8_t> row{1, 2, 3, 4, 5, 6, 7, 8};
    r.gw.templateHandlers[pcl::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pcl::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        pcl::ViewRowResponse resp;
        resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.status = pcl::ViewRowResponse::kOk; resp.rowLen = static_cast<uint32_t>(row.size());
        resp.rowBytes = row.data(); resp.rowBytesLen = static_cast<uint32_t>(row.size());
        std::vector<uint8_t> out(resp.encodedSize());
        const std::size_t n = resp.encode(out.data());
        r.gw.send(ep, out.data(), n);
    };
    startFailoverWindow(r, 600ms);
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    EXPECT_TRUE(driveFor(r, 5s, [&] { return f.ready(); }));
    EXPECT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    (void)n1;
}

// I-1: a control op ends NotConnected only when the seed can never reconnect.
//  (a) never connected: at once;  (b) after a stopped failover: at once, never Timeout;
//  (c) a ping waiting mid-failover when the failover stops: NotConnected promptly.
// RED (the stopped failover not counted as "cannot reconnect"): (b) / (c) end Timeout at pingTimeoutMs.
TEST(GroupRoutingRegression, ControlOpEndsNotConnectedOnlyWhenTheSeedCannotReconnectRegression) {
    {   // (a)
        GroupRig r(4329, 4330, failoverOpts());
        ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
        test_support::TestCq cq(r.client);
        auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(std::vector<uint8_t>{1}, q, t); });
        r.client.doWork();
        ASSERT_TRUE(ping.ready());
        EXPECT_EQ(ping.status(), rc::OpStatus::NotConnected);
    }
    {   // (c) then (b)
        RigOptions o = failoverOpts();
        o.pingTimeoutMs = 20'000;
        GroupRig r(4331, 4332, o);
        const auto [n1, p1port] = r.addNode(4331);
        r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
        r.gw.refuseConnectsWith[n1] = static_cast<int16_t>(rune::protocol::client::ErrorCode::ProtocolVersion);
        r.gw.groupProtocol = false;
        r.connect();
        ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
        test_support::TestCq cq(r.client);
        r.gw.deadEndpoints.insert(0);
        ASSERT_TRUE(driveFor(r, 5s, [&] { return r.client.testSessionStateAt(0) != rc::session::SessionState::Connected; }));
        auto waiting = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(std::vector<uint8_t>{1}, q, t); });
        ASSERT_TRUE(driveFor(r, 5s, [&] { return r.gw.connectsAt[n1] >= 1; })) << "the failover reaches node 1";
        const auto tStop = std::chrono::steady_clock::now();
        EXPECT_TRUE(driveFor(r, 6s, [&] { return waiting.ready(); }));
        EXPECT_LT(std::chrono::steady_clock::now() - tStop, 5s) << "N-M2: RED is the 20 s ping Timeout";
        EXPECT_EQ(waiting.status(), rc::OpStatus::NotConnected) << waiting.describe();
        auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(std::vector<uint8_t>{1}, q, t); });
        auto ddl  = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
        EXPECT_TRUE(driveFor(r, 100ms, [&] { return ping.ready() && ddl.ready(); }));
        EXPECT_EQ(ping.status(), rc::OpStatus::NotConnected);
        EXPECT_EQ(ddl.status(), rc::OpStatus::NotConnected);
    }
}

// I-1: a control op waiting for the seed ends at its OWN deadline (Timeout, exactly once) — every
// broker dead, a failover that keeps trying.
TEST(GroupRoutingRegression, WaitingControlOpTimesOutWithinItsOwnBudgetRegression) {
    RigOptions o = failoverOpts();
    o.pingTimeoutMs = 800;
    GroupRig r(4333, 4334, o);
    const auto [n1, p1port] = r.addNode(4333);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints = {0, n1};
    ASSERT_TRUE(driveFor(r, 5s, [&] { return r.client.testSessionStateAt(0) != rc::session::SessionState::Connected; }));
    test_support::TestCq cq(r.client);
    const auto t0 = std::chrono::steady_clock::now();
    auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(std::vector<uint8_t>{1}, q, t); });
    ASSERT_TRUE(driveFor(r, 5s, [&] { return ping.ready(); }));
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(ping.status(), rc::OpStatus::Timeout) << ping.describe();
    EXPECT_GE(took, 600ms);
    EXPECT_LT(took, 5s) << "fix round 3 (N-M2): a wall bound load cannot break (RED, no deadline, is a hang)";
    EXPECT_EQ(cq.timesCompleted(ping.token()), 1);
}

// I-1: close() while a control op waits for the seed completes it Closed, exactly once.
TEST(GroupRoutingRegression, CloseWhileAControlOpWaitsForTheSeedCompletesItClosedRegression) {
    GroupRig r(4335, 4336, slowConnectOpts());
    const auto [n1, p1port] = r.addNode(4335);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    startFailoverWindow(r, 2500ms);
    test_support::TestCq cq(r.client);
    auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(std::vector<uint8_t>{1}, q, t); });
    r.client.doWork();
    ASSERT_EQ(r.client.testCtlAwaitingSeed(), 1u);
    r.client.close();
    ASSERT_TRUE(ping.ready());
    EXPECT_EQ(ping.status(), rc::OpStatus::Closed);
    EXPECT_EQ(cq.timesCompleted(ping.token()), 1);
    EXPECT_EQ(r.client.testCtlAwaitingSeed(), 0u);
    (void)n1;
}

// I-2: a late SessionExpired for the ABANDONED seed session arrives while the seed's id is 0 (its
// failover connect pending): nothing of the seed's fails — no onError, the pending connect() and a
// waiting ping stay pending, then both complete Ok.  RED (`seedSid != 0` guard): onError fires and
// the connect completes BrokerError.
TEST(GroupRoutingRegression, StaleNoticeWhileTheSeedReconnectsAfterAFailoverFailsNothingRegression) {
    GroupRig r(4337, 4338, slowConnectOpts());
    const auto [n1, p1port] = r.addNode(4337);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    const int64_t oldSid = r.client.testSeedSessionId();
    int appErrors = 0;
    r.client.onError([&](const std::string&) { ++appErrors; });
    r.client.doWork();
    r.gw.connectReplyDelay = 800ms;
    r.gw.deadEndpoints.insert(0);
    ASSERT_TRUE(driveFor(r, 10s, [&] { return r.client.testSeedFailovers() >= 1 && r.client.testSeedSessionId() == 0; }));
    test_support::TestCq cq(r.client);
    auto conn = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping(std::vector<uint8_t>{1}, q, t); });
    const uint64_t noticesBefore = r.client.testStaleSessionNotices();
    r.gw.sendError(n1, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "", oldSid, 0);
    (void)driveFor(r, 200ms, [] { return false; });
    EXPECT_EQ(appErrors, 0) << "onError fired for another session's expiry";
    EXPECT_FALSE(conn.ready()) << conn.describe();
    EXPECT_FALSE(ping.ready()) << ping.describe();
    EXPECT_EQ(r.client.testStaleSessionNotices(), noticesBefore + 1);
    EXPECT_TRUE(driveFor(r, 5s, [&] { return conn.ready() && ping.ready(); }));
    EXPECT_TRUE(conn.ok()) << conn.describe();
    EXPECT_TRUE(ping.ok()) << ping.describe();
}

// I-3: the plain failover retries every broker in turn.  n1 and n2 are silent at first; the first
// attempt reaches n1; then n1 recovers — it must be retried.  RED (list[attempt % size] over a
// list excluding the current broker): attempts go n2, n0, n2, n0 — n1 never again.
TEST(GroupRoutingRegression, SeedFailoverRetriesEveryBrokerInTurnRegression) {
    GroupRig r(4339, 4340, failoverOpts());
    const auto [n1, p1port] = r.addNode(4339);
    const auto [n2, p2port] = r.addNode(4339);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints = {0};
    r.gw.silentEndpoints = {n1, n2};
    ASSERT_TRUE(driveFor(r, 5s, [&] { return r.gw.connectsAt[n1] >= 1; })) << "the first attempt reaches node 1";
    r.gw.silentEndpoints.erase(n1);
    EXPECT_TRUE(driveFor(r, 6s, [&] {
        return r.client.testSessionStateAt(0) == rc::session::SessionState::Connected &&
               rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p1port));
    })) << "node 1 was never retried (seed on " << r.client.testSeedChannel() << ")";
    (void)n2; (void)p2port;
}

// I-3: a shard (reconnectForever) client reaches EVERY broker — n1 refuses connects (retriable), n2
// is healthy.  RED (re-points on even Backoff entries undone by tickBackoff_'s bootstrap retarget;
// the old list): it alternates n1 and n0 forever, n2 never tried.
TEST(GroupRoutingRegression, ShardSeedFailoverReachesEveryBrokerRegression) {
    RigOptions o = failoverOpts();
    o.reconnectForever = true;
    GroupRig r(4341, 4342, o);
    const auto [n1, p1port] = r.addNode(4341);
    const auto [n2, p2port] = r.addNode(4341);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints = {0};
    r.gw.refuseConnectsWith[n1] = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    EXPECT_TRUE(driveFor(r, 15s, [&] {
        return r.client.testSessionStateAt(0) == rc::session::SessionState::Connected &&
               rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p2port));
    })) << "node 2 was never reached (seed on " << r.client.testSeedChannel() << ")";
    EXPECT_EQ(r.client.testSeedFailoverConnects(), 0u);
    (void)n1; (void)n2;
}

// I-4: a shard client with the DEFAULT connect timeout moves at the stale-loss edge (~ the response
// timeout), not after its resume + connect budget at the first Backoff entry.
// RED (no stale-loss re-point): the first move waits ~400 ms + 4 x 5 s.
TEST(GroupRoutingRegression, ShardClientFailsOverWithinTheStaleThresholdWithDefaultConnectTimeoutsRegression) {
    RigOptions o = failoverOpts();
    o.reconnectForever = true;
    o.connectTimeoutMs = 5000;
    GroupRig r(4343, 4344, o);
    const auto [n1, p1port] = r.addNode(4343);
    r.gw.moreBrokers = {{1, "127.0.0.1", p1port}};
    r.gw.groupProtocol = false;
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.deadEndpoints.insert(0);
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_TRUE(driveFor(r, 10s, [&] {
        return r.client.testSessionStateAt(0) == rc::session::SessionState::Connected &&
               rc::agent::ClientAgent::sameEndpoint(r.client.testSeedChannel(), udp(p1port));
    })) << "the shard client did not fail over within the stale threshold";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 8s) << "N-M2: RED is >= 20.4 s (400 ms + 4 x 5 s)";
    (void)n1;
}

// ===========================================================================
// #365: plain subscriptions homed on a node that crashed
// ===========================================================================
namespace {
int subscribesAt(const test_support::InvokerGateway& gw, int32_t p, int ep) {
    int n = 0;
    for (const auto& s : gw.subscribed) n += (s.partition == p && s.endpoint == ep) ? 1 : 0;
    return n;
}
} // namespace

// A reader created right after p1's leader (node 1) crashed: its Subscribe is
// sent to node 1's session, which never answers.  When fresh metadata names the
// new leader (node 2), the pending Subscribe moves there and binds within its
// deadline, and so does the reader that was already bound on node 1 — both
// while node 1's session still looks alive.  RED: both stayed homed on node 1's
// session until it was seen stale (responseTimeoutNs, 10 s: only a session
// EPOCH change re-routed them), so a reader created within that window of the
// crash ended TimedOut (subscribeTimeoutMs, 10 s).
TEST(GroupRoutingRegression, PlainReaderCreatedRightAfterALeaderCrashBindsOnTheNewLeaderRegression) {
    GroupRig r(4501, 4502);   // default session timeouts: node 1's session is not yet seen stale
    const auto [n1, p1port] = r.addNode(4501);
    const auto [n2, p2port] = r.addNode(4501);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4501, ""}, {1, 1, 1, 1, 4501, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    auto bound = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                    [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && bound.active(); }));

    // Node 1 crashes; a fresh reader subscribes at once (stale metadata).
    r.gw.deadEndpoints.insert(n1);
    auto fresh = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                    [](const rc::Record&) {});
    // Its Subscribe is redirected by the seed (stale view) to node 1, which
    // never answers (a dead node records nothing: bounded drive, no predicate).
    (void)r.drive([&] { return fresh.finished(); }, 50'000);
    ASSERT_FALSE(fresh.finished());
    ASSERT_EQ(subscribesAt(r.gw, 1, n2), 0);

    // The cluster elects node 2 for p1.
    r.gw.partitions      = {{1, 0, 0, 1, 4501, ""}, {1, 1, 2, 2, 4501, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    ASSERT_TRUE(r.drive([&] {
        return subscribesAt(r.gw, 1, n2) >= 2 || fresh.finished() ||
               r.client.testSessionStateAt(1) != rc::session::SessionState::Connected;
    }, 40'000'000))
        << "p1's subscriptions never moved to node 2 (" << subscribesAt(r.gw, 1, n2) << " there)";
    EXPECT_FALSE(fresh.finished()) << "the fresh reader ended " << static_cast<int>(fresh.failReason());
    EXPECT_FALSE(bound.finished());
    // Moved on the fresh metadata, not on node 1's staleness (10 s): its session
    // still looks Connected.  (RED without the fix: nothing moved until then.)
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);
}

// ---- PR #513 adversarial review (#365 area) ---------------------------------------------------------------------

// Data integrity: a BOUND reader whose partition fresh metadata moves to node 2 while node 1 is still alive (a
// clean leadership move) resumes on node 2 from exactly its next offset, releases node 1's instance, and drops the
// old instance's late records — every offset delivered once, in order, none lost.
TEST(GroupRoutingRegression, ABoundReaderFollowsALiveLeaderMoveWithoutLossOrDuplicateAdversarialRegression) {
    GroupRig r(4511, 4512);
    const auto [n1, p1port] = r.addNode(4511);
    const auto [n2, p2port] = r.addNode(4511);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4511, ""}, {1, 1, 1, 1, 4511, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    std::vector<int64_t> offsets;
    auto sub = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record& rec) { offsets.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && sub.active(); }));
    const auto old = *r.subAt(1, n1);
    r.gw.pushRecordsFrom(old, 0, 5, n1);
    ASSERT_TRUE(r.drive([&] { return offsets.size() == 5; }));

    // p1 moves to node 2 (node 1 alive); a metadata answer names it.
    r.gw.partitions      = {{1, 0, 0, 1, 4511, ""}, {1, 1, 2, 2, 4511, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    r.client.testRequestMetadataNow();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr && sub.active(); })) << "never moved to node 2";
    EXPECT_EQ(r.subAt(1, n2)->fromOffset, 5) << "resumed from the wrong offset";
    ASSERT_TRUE(r.drive([&] {
        for (std::size_t i = 0; i < r.gw.unsubscribed.size(); ++i)
            if (r.gw.unsubscribed[i] == old.subscriptionId && r.gw.unsubscribedAt[i] == n1) return true;
        return false;
    })) << "node 1's instance was never released";
    // Node 1's late records for the released instance, then node 2's.
    r.gw.pushRecordsFrom(old, 5, 3, n1);
    r.gw.pushRecordsFrom(*r.subAt(1, n2), 5, 3, n2);
    ASSERT_TRUE(r.drive([&] { return offsets.size() >= 8; }));
    for (int i = 0; i < 2000; ++i) { r.client.doWork(); r.gw.pump(); }
    const std::vector<int64_t> want{0, 1, 2, 3, 4, 5, 6, 7};
    EXPECT_EQ(offsets, want);
    EXPECT_FALSE(sub.finished());
}

// Route moves to the SEED (routeMovedPending_'s session-0 branch): a reader created right after node 1 crashed,
// whose Subscribe went to node 1, binds on the seed once fresh metadata names it — before node 1's session is
// seen stale.
TEST(GroupRoutingRegression, APendingReaderOnACrashedLeaderFollowsTheRouteToTheSeedAdversarialRegression) {
    GroupRig r(4513, 4514);
    const auto [n1, p1port] = r.addNode(4513);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4513, ""}, {1, 1, 1, 1, 4513, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    auto bound = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                    [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && bound.active(); }));
    r.gw.deadEndpoints.insert(n1);   // node 1 crashes; its session still looks Connected
    auto fresh = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                    [](const rc::Record&) {});
    (void)r.drive([&] { return fresh.finished(); }, 50'000);
    ASSERT_FALSE(fresh.finished());
    ASSERT_EQ(subscribesAt(r.gw, 1, 0), 0);
    // The seed's node is elected for p1.
    r.gw.partitions      = {{1, 0, 0, 1, 4513, ""}, {1, 1, 0, 2, 4513, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, 0}};
    ASSERT_TRUE(r.drive([&] {
        return subscribesAt(r.gw, 1, 0) >= 2 || fresh.finished() || bound.finished() ||
               r.client.testSessionStateAt(1) != rc::session::SessionState::Connected;
    }, 40'000'000)) << "p1's subscriptions never moved to the seed (" << subscribesAt(r.gw, 1, 0) << " there)";
    ASSERT_TRUE(r.drive([&] { return (fresh.active() && bound.active()) || fresh.finished() || bound.finished(); }));
    EXPECT_TRUE(fresh.active() && bound.active());
    EXPECT_FALSE(fresh.finished()) << "ended " << static_cast<int>(fresh.failReason());
    EXPECT_FALSE(bound.finished());
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected)
        << "moved only once node 1 was seen stale (RED without the fix)";
}

// Resources / no churn: a homed Subscribe a HEALTHY node registered but whose answer is still out keeps a
// backoff-paced metadata refresh armed (#365) — and the refreshes, naming the same leader, never re-send it.
TEST(GroupRoutingRegression, AnUnansweredHealthySubscribeIsNeverResentByTheRefreshItArmsAdversarialRegression) {
    GroupRig r(4515, 4516);
    const auto [n1, p1port] = r.addNode(4515);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4515, ""}, {1, 1, 1, 1, 4515, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 1) == 1; }));
    r.gw.dropSubscribeResponsesNext = 1;   // registered on node 1, its answer lost
    auto sub = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return subscribesAt(r.gw, 1, n1) == 1; }));
    const int metaBefore = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests >= metaBefore + 2 || sub.finished(); }, 40'000'000))
        << "no refresh was armed by the unanswered Subscribe";
    EXPECT_FALSE(sub.finished());
    EXPECT_EQ(subscribesAt(r.gw, 1, n1), 1) << "the refresh re-sent a Subscribe whose route did not move";
    EXPECT_EQ(subscribesAt(r.gw, 1, 0), 0);
}

// ===========================================================================
// #514: a leader node session that carries a bound reader / group fetch and has gone silent
// ===========================================================================
namespace {
/// Lets the one metadata refresh the subscribe flow itself arms (a redirect's provisional guess) fire and quiet
/// down, so the tests below start with NO refresh armed: ten keepalive rounds on a live rig.
bool settleRefreshes(GroupRig& r) {
    const int pingsBefore = r.gw.pings;
    return r.drive([&] { return r.gw.pings >= pingsBefore + 10; }, 40'000'000);
}
} // namespace

// A BOUND reader on a crashed leader arms nothing by itself (a metadata refresh was armed only while a Subscribe
// was unanswered): its node session was only declared lost at responseTimeoutNs (10 s), so delivery stopped for
// that long.  A live node answers every keepalive Ping, so silence longer than two keepalive intervals keeps the
// backoff-paced refresh armed; fresh metadata then moves the reader through the #365 path.
// responseTimeoutNs is set to 60 s: the stale path cannot be what moves it.
// RED (before): nothing moves within the drive cap (the session stays Connected, no Subscribe reaches node 2).
TEST(GroupRoutingRegression, ABoundReaderAloneOnACrashedLeaderMovesBeforeTheSessionIsStaleRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4551, 4552, o);
    const auto [n1, p1port] = r.addNode(4551);
    const auto [n2, p2port] = r.addNode(4551);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4551, ""}, {1, 1, 1, 1, 4551, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    std::vector<int64_t> offsets;
    auto sub = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record& rec) { offsets.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && sub.active(); }));
    ASSERT_TRUE(settleRefreshes(r));
    ASSERT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);

    // Node 1 crashes; the cluster elects node 2 for p1.  The reader is the only subscription anywhere.
    r.gw.deadEndpoints.insert(n1);
    r.gw.partitions      = {{1, 0, 0, 1, 4551, ""}, {1, 1, 2, 2, 4551, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    ASSERT_TRUE(r.drive([&] {
        return subscribesAt(r.gw, 1, n2) >= 1 || sub.finished() ||
               r.client.testSessionStateAt(1) != rc::session::SessionState::Connected;
    }, 40'000'000)) << "the bound reader never moved off the crashed leader";
    EXPECT_GE(subscribesAt(r.gw, 1, n2), 1);
    EXPECT_FALSE(sub.finished()) << "ended " << static_cast<int>(sub.failReason());
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected)
        << "moved by fresh metadata, not by node 1's staleness (RED without the fix)";

    // Node 2 answers and its records arrive.
    ASSERT_TRUE(r.drive([&] { return (r.subAt(1, n2) != nullptr && sub.active()) || sub.finished(); }));
    ASSERT_NE(r.subAt(1, n2), nullptr);
    const auto fresh = *r.subAt(1, n2);
    r.gw.pushRecordsFrom(fresh, 0, 3, n2);
    ASSERT_TRUE(r.drive([&] { return offsets.size() == 3; })) << "records from node 2 never arrived";
    EXPECT_FALSE(sub.finished());
}

// The group twin: a member whose only assigned partition is led by the crashed node keeps its fetch armed the
// same way - the fetch moves to node 2 before node 1's session is seen stale.
TEST(GroupRoutingRegression, AGroupMemberAloneOnACrashedLeaderMovesItsFetchBeforeTheSessionIsStaleRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4553, 4554, o);
    const auto [n1, p1port] = r.addNode(4553);
    const auto [n2, p2port] = r.addNode(4553);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4553, ""}, {1, 1, 1, 1, 4553, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 1}};                 // only p1, led by node 1
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 1) == 1 && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr; })) << "the fetch never reached node 1";
    r.gw.pushRecordsFrom(*r.subAt(1, n1), 0, 3, n1);
    ASSERT_TRUE(r.drive([&] { return led.records[1] == 3; })) << "the fetch on node 1 never delivered";
    ASSERT_TRUE(settleRefreshes(r));
    ASSERT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);

    r.gw.deadEndpoints.insert(n1);
    r.gw.partitions      = {{1, 0, 0, 1, 4553, ""}, {1, 1, 2, 2, 4553, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    ASSERT_TRUE(r.drive([&] {
        return subscribesAt(r.gw, 1, n2) >= 1 ||
               r.client.testSessionStateAt(1) != rc::session::SessionState::Connected;
    }, 40'000'000)) << "the fetch never moved off the crashed leader";
    EXPECT_GE(subscribesAt(r.gw, 1, n2), 1);
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected)
        << "moved by fresh metadata, not by node 1's staleness (RED without the fix)";
    EXPECT_FALSE(gc.finished());
}

// No refresh storm: a live but idle leader answers every keepalive Ping, so it is never silent and the bound
// reader on it never arms a metadata refresh.
TEST(GroupRoutingRegression, AnIdleButLiveLeaderDoesNotTriggerRefreshStormRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4555, 4556, o);
    const auto [n1, p1port] = r.addNode(4555);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4555, ""}, {1, 1, 1, 1, 4555, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && sub.active(); }));
    ASSERT_TRUE(settleRefreshes(r));
    const int pingsBefore = r.gw.pings;
    const int metaBefore  = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.pings >= pingsBefore + 20; }, 40'000'000))
        << "keepalives never ran (" << (r.gw.pings - pingsBefore) << " pings)";
    EXPECT_EQ(r.gw.metadataRequests, metaBefore) << "an idle live leader armed metadata refreshes";
    EXPECT_FALSE(sub.finished());
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);
}

// ===========================================================================
// #514 adversarial (M31+ review): which silent sessions arm a refresh, how often, and when it stops.
// ===========================================================================

// A silent node session that carries NO reader or fetch (its reader was unsubscribed; the session stays open)
// arms nothing, even while another homed reader keeps the silent check running.  Only a session a reader or
// fetch depends on may pace metadata refreshes.
// RED by mutation: drop the usesSession / fetchUses filter in watchSilentLeaders_ (every silent bit refreshes).
TEST(GroupRoutingRegression, ASilentNodeCarryingNoReaderArmsNoRefreshAdversarialRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4561, 4562, o);
    const auto [n1, p1port] = r.addNode(4561);
    const auto [n2, p2port] = r.addNode(4561);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 1, 1, 4561, ""}, {1, 1, 2, 1, 4561, ""}};   // p0 on node 1, p1 on node 2
    r.gw.subscribeLeader = {{0, n1}, {1, n2}};
    r.connect();
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                 [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && s0.active(); }));
    s0.unsubscribe();                                // node 1's session stays open, carrying nothing
    ASSERT_TRUE(r.drive([&] { return !r.gw.unsubscribed.empty(); })) << "the Unsubscribe never reached node 1";
    std::vector<int64_t> offsets;
    auto s1 = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                 [&](const rc::Record& rec) { offsets.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr && s1.active(); }));
    // Confirm both redirect guesses now (an urgent ask fanned out after them), so no backoff refresh of the
    // subscribe flow is still armed when the silence starts.
    const int answers = r.client.testMetadataResponses();
    r.client.testRequestMetadataNow();
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() >= answers + 3; }));
    ASSERT_TRUE(settleRefreshes(r));
    ASSERT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);

    r.gw.deadEndpoints.insert(n1);                   // node 1 goes silent; nothing depends on it
    const int pingsBefore = r.gw.pings;
    const int metaBefore  = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.pings >= pingsBefore + 20; }, 40'000'000));
    EXPECT_EQ(r.gw.metadataRequests, metaBefore) << "a silent session nobody reads from armed refreshes";
    EXPECT_EQ(subscribesAt(r.gw, 1, n2), 1) << "the live reader was re-sent";
    r.gw.pushRecordsFrom(*r.subAt(1, n2), 0, 2, n2);
    ASSERT_TRUE(r.drive([&] { return offsets.size() == 2; })) << "the reader on the live node stopped delivering";
    EXPECT_FALSE(s1.finished());
}

// A silent leader that fresh metadata STILL names (no election yet): refreshes keep coming - at most one per
// keepalive interval, never a busy storm - the reader is never re-sent to the same route, and the moment the
// metadata names a new leader the reader moves there before the old session is declared stale.
// RED by mutation: drop the scheduleMetadataRefresh() call in watchSilentLeaders_ (no refresh, never moves).
TEST(GroupRoutingRegression, ASilentLeaderStillNamedKeepsABoundedRefreshPaceThenMovesOnElectionAdversarialRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4563, 4564, o);
    const auto [n1, p1port] = r.addNode(4563);
    const auto [n2, p2port] = r.addNode(4563);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4563, ""}, {1, 1, 1, 1, 4563, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    auto sub = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && sub.active(); }));
    ASSERT_TRUE(settleRefreshes(r));

    r.gw.deadEndpoints.insert(n1);                   // silent, but still the named leader
    const int pingsBefore = r.gw.pings;
    const int metaBefore  = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.pings >= pingsBefore + 20; }, 40'000'000));
    const int pings = r.gw.pings - pingsBefore;
    const int metas = r.gw.metadataRequests - metaBefore;
    EXPECT_GE(metas, 2) << "refreshing stopped while the leader was still silent";
    EXPECT_LE(metas, pings) << "a refresh storm: " << metas << " refreshes in " << pings << " keepalive Pings";
    EXPECT_EQ(subscribesAt(r.gw, 1, n1), 1) << "the reader was re-sent on an unchanged route";
    EXPECT_FALSE(sub.finished());

    r.gw.partitions      = {{1, 0, 0, 1, 4563, ""}, {1, 1, 2, 2, 4563, ""}};   // the election
    r.gw.subscribeLeader = {{0, 0}, {1, n2}};
    ASSERT_TRUE(r.drive([&] {
        return subscribesAt(r.gw, 1, n2) >= 1 || sub.finished() ||
               r.client.testSessionStateAt(1) != rc::session::SessionState::Connected;
    }, 40'000'000)) << "the reader never followed the election";
    EXPECT_GE(subscribesAt(r.gw, 1, n2), 1);
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);
    EXPECT_FALSE(sub.finished());
}

// A leader that went silent (a long pause) and then comes back: once it answers again the refreshes stop, the
// reader was never moved or re-sent, and its pushes still arrive on the original subscription.
// RED by mutation: report every Connected session as silent in leaderSessionsSilentMask (refreshes never stop).
TEST(GroupRoutingRegression, ALeaderSilentThenBackStopsRefreshingAndKeepsItsReaderAdversarialRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4565, 4566, o);
    const auto [n1, p1port] = r.addNode(4565);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4565, ""}, {1, 1, 1, 1, 4565, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    std::vector<int64_t> offsets;
    auto sub = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record& rec) { offsets.push_back(rec.offset); });
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && sub.active(); }));
    const auto bound = *r.subAt(1, n1);
    ASSERT_TRUE(settleRefreshes(r));

    r.gw.deadEndpoints.insert(n1);
    const int metaSilent = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests >= metaSilent + 2; }, 40'000'000))
        << "the silence armed no refresh";
    r.gw.deadEndpoints.erase(n1);                    // the pause ends: node 1 answers its next Ping
    const int n1Pings = r.gw.pings;
    ASSERT_TRUE(r.drive([&] { return r.gw.pings >= n1Pings + 6; }, 40'000'000));   // node 1 heard again
    const int pingsBefore = r.gw.pings;
    const int metaBefore  = r.gw.metadataRequests;
    ASSERT_TRUE(r.drive([&] { return r.gw.pings >= pingsBefore + 20; }, 40'000'000));
    EXPECT_LE(r.gw.metadataRequests - metaBefore, 1) << "refreshes went on after the leader answered again";
    EXPECT_EQ(subscribesAt(r.gw, 1, n1), 1) << "the reader was re-sent although its route never moved";
    EXPECT_EQ(subscribesAt(r.gw, 1, 0), 0);
    EXPECT_EQ(r.client.testSessionStateAt(1), rc::session::SessionState::Connected);
    r.gw.pushRecordsFrom(bound, 0, 3, n1);
    ASSERT_TRUE(r.drive([&] { return offsets.size() == 3; })) << "the original subscription stopped delivering";
    EXPECT_FALSE(sub.finished());
}

// Sibling of #514 on the SEED session (M31+ review: a gap in the #514 fix, closed in fix round 1):
// a reader bound on the seed's broker when the seed crashes and the cluster elects node 1.  The silent watch
// skips session 0 and a refresh would be asked on the dead seed first, so the reader waits for the seed
// session's stale timeout (responseTimeoutNs) before anything moves it.
TEST(GroupRoutingRegression, ABoundReaderOnACrashedSeedMovesBeforeTheSeedIsStaleAdversarialRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4567, 4568, o);
    const auto [n1, p1port] = r.addNode(4567);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4567, ""}, {1, 1, 1, 1, 4567, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.connect();
    auto other = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                    [](const rc::Record&) {});   // keeps a live node session open
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n1) != nullptr && other.active(); }));
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && sub.active(); }));
    ASSERT_TRUE(settleRefreshes(r));

    r.gw.deadEndpoints.insert(0);                    // the seed crashes; node 1 now leads p0
    r.gw.partitions      = {{1, 0, 1, 2, 4567, ""}, {1, 1, 1, 1, 4567, ""}};
    r.gw.subscribeLeader = {{0, n1}, {1, n1}};
    const int pingsBefore = r.gw.pings;              // node 1's keepalives are the clock (~20 per second)
    EXPECT_TRUE(r.drive([&] { return subscribesAt(r.gw, 0, n1) >= 1 || r.gw.pings >= pingsBefore + 100; },
                        40'000'000));
    EXPECT_GE(subscribesAt(r.gw, 0, n1), 1) << "the reader on the crashed seed did not move within ~5 s";
    EXPECT_FALSE(sub.finished());
}

namespace {
/// The #514 (seed) rig: a node-1 reader on p1 keeps node 1's session open, a reader on p0 is bound at the seed.
struct SeedRig {
    RigOptions    o;
    GroupRig      r;
    int           n1{0};
    int           base_{0};
    std::optional<rc::Subscription> other_;
    std::optional<rc::Subscription> sub_;
    static RigOptions opts() {
        RigOptions o;
        o.keepaliveNs       = 50'000'000;
        o.responseTimeoutNs = 60'000'000'000;
        return o;
    }
    SeedRig(int base) : o(opts()), r(base, base + 1, o), base_(base) {
        const auto [node1, p1port] = r.addNode(base);
        n1 = node1;
        r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
        r.gw.partitions      = {{1, 0, 0, 1, base, ""}, {1, 1, 1, 1, base, ""}};
        r.gw.subscribeLeader = {{0, 0}, {1, n1}};
        r.connect();
        other_.emplace(r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                          [](const rc::Record&) {}));
    }
    bool bind() {
        if (!r.drive([&] { return r.subAt(1, n1) != nullptr && other_->active(); })) return false;
        sub_.emplace(r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                        [](const rc::Record&) {}));
        if (!r.drive([&] { return r.subAt(0, 0) != nullptr && sub_->active(); })) return false;
        return settleRefreshes(r);
    }
    void moveP0ToNode1() {
        r.gw.partitions      = {{1, 0, 1, 2, base_, ""}, {1, 1, 1, 1, base_, ""}};
        r.gw.subscribeLeader = {{0, n1}, {1, n1}};
    }
};
int unsubscribesAt(const test_support::InvokerGateway& gw, int64_t subId, int ep) {
    int n = 0;
    for (std::size_t i = 0; i < gw.unsubscribed.size(); ++i)
        n += (gw.unsubscribed[i] == subId && gw.unsubscribedAt[i] == ep) ? 1 : 0;
    return n;
}
} // namespace

// Seed alive, p0's leader moves to node 1: the seed-homed reader follows fresh metadata (reconcile no longer skips
// homeSession == 0), releases its old instance on the seed exactly once, and is not ended.
// RED by mutation: restore the `homeSession == 0 -> continue` skip, or drop the seed release block.
TEST(GroupRoutingRegression, ASeedHomedReaderFollowsAMovedRouteWhileTheSeedIsUpRegression) {
    SeedRig s(4571);
    ASSERT_TRUE(s.bind());
    const int64_t oldId = s.sub_->subscriptionId();
    s.moveP0ToNode1();
    s.r.client.testRequestMetadataNow();
    const int pingsBefore = s.r.gw.pings;
    EXPECT_TRUE(s.r.drive([&] { return subscribesAt(s.r.gw, 0, s.n1) >= 1 || s.r.gw.pings >= pingsBefore + 100; },
                          40'000'000));
    EXPECT_GE(subscribesAt(s.r.gw, 0, s.n1), 1);
    EXPECT_TRUE(s.r.drive([&] { return unsubscribesAt(s.r.gw, oldId, 0) >= 1 || s.r.gw.pings >= pingsBefore + 100; },
                          40'000'000));
    const int pingsAfter = s.r.gw.pings;
    EXPECT_TRUE(s.r.drive([&] { return s.r.gw.pings >= pingsAfter + 20; }, 40'000'000));
    EXPECT_EQ(unsubscribesAt(s.r.gw, oldId, 0), 1) << "the old seed instance must be released exactly once";
    EXPECT_FALSE(s.sub_->finished());
}

// Seed alive and p0 stays on the seed: refreshes never re-arm or move the seed-homed reader.
// RED by mutation: drop the `leaderSession == 0 -> continue` guard in the reconcile sweep.
TEST(GroupRoutingRegression, ASeedHomedReaderWhoseRouteStaysOnTheSeedIsNeverReArmedRegression) {
    SeedRig s(4573);
    ASSERT_TRUE(s.bind());
    const int pingsBefore = s.r.gw.pings;
    int round = 0;
    EXPECT_TRUE(s.r.drive([&] {
        if (s.r.gw.pings >= pingsBefore + 40 * (round + 1) && round < 5) {
            ++round;
            s.r.client.testRequestMetadataNow();
        }
        return s.r.gw.pings >= pingsBefore + 200;
    }, 40'000'000));
    EXPECT_GE(round, 4);
    EXPECT_EQ(subscribesAt(s.r.gw, 0, 0), 1);
    EXPECT_EQ(subscribesAt(s.r.gw, 0, s.n1), 0);
    EXPECT_FALSE(s.sub_->finished());
}

// The seed crashes, p0 moves to node 1, then the seed comes back: p0 is subscribed on node 1 once and never
// re-sent to the returned seed.
TEST(GroupRoutingRegression, AReaderMovedOffACrashedSeedIsNotSubscribedTwiceWhenTheSeedReturnsRegression) {
    SeedRig s(4575);
    ASSERT_TRUE(s.bind());
    s.r.gw.deadEndpoints.insert(0);
    s.moveP0ToNode1();
    const int pingsBefore = s.r.gw.pings;
    ASSERT_TRUE(s.r.drive([&] { return subscribesAt(s.r.gw, 0, s.n1) >= 1 || s.r.gw.pings >= pingsBefore + 100; },
                          40'000'000));
    ASSERT_GE(subscribesAt(s.r.gw, 0, s.n1), 1) << "the reader never moved";
    const int atSeed = subscribesAt(s.r.gw, 0, 0);
    s.r.gw.deadEndpoints.erase(0);
    const int pingsReturn = s.r.gw.pings;
    EXPECT_TRUE(s.r.drive([&] { return s.r.gw.pings >= pingsReturn + 200; }, 80'000'000));
    EXPECT_EQ(subscribesAt(s.r.gw, 0, s.n1), 1);
    EXPECT_EQ(subscribesAt(s.r.gw, 0, 0), atSeed) << "a new Subscribe for p0 reached the returned seed";
    EXPECT_FALSE(s.sub_->finished());
}

// The group twin: a member whose p0 fetch lives on the seed moves it to node 1 before the seed session is stale.
// RED by mutation: remove the seed branch in watchSilentLeaders_, or ask the seed first in
// sendMetadataRequestAnywhere_.
TEST(GroupRoutingRegression, AGroupFetchOnACrashedSeedMovesBeforeTheSeedIsStaleRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4577, 4578, o);
    const auto [n1, p1port] = r.addNode(4577);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, 0, 1, 4577, ""}, {1, 1, 1, 1, 4577, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    Consumed led;
    wire(gc, led);
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 1) == 1 && !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }))
        << "the fetches never reached the seed and node 1";
    ASSERT_TRUE(settleRefreshes(r));

    r.gw.deadEndpoints.insert(0);
    r.gw.partitions      = {{1, 0, 1, 2, 4577, ""}, {1, 1, 1, 1, 4577, ""}};
    r.gw.subscribeLeader = {{0, n1}, {1, n1}};
    const int pingsBefore = r.gw.pings;
    EXPECT_TRUE(r.drive([&] { return subscribesAt(r.gw, 0, n1) >= 1 || r.gw.pings >= pingsBefore + 100; },
                        40'000'000));
    EXPECT_GE(subscribesAt(r.gw, 0, n1), 1) << "the group fetch on the crashed seed did not move within ~5 s";
    EXPECT_FALSE(gc.finished());
}

// Out-of-batch finding (M31+ review, issue #651 — out of the suite until it is fixed): two redirected
// Subscribes, the second right after the first binds.  The second redirect's guess stays provisional; each LAGGING
// MetadataResponse (requested before that redirect, so it cannot confirm the guess) disarms the armed refresh in
// Client::onMetadataResponse and re-arms it at now + a doubled backoff, pushing the confirming request out (39
// Pings here, ~0.65 s, instead of the first backoff step).  Fix: keep an armed deadline while a guess is provisional.
TEST(GroupRoutingRegression, DISABLED_LaggingMetadataAnswersNeverPostponeTheArmedRefreshAdversarialRegression) {
    RigOptions o;
    o.keepaliveNs       = 50'000'000;
    o.responseTimeoutNs = 60'000'000'000;
    GroupRig r(4569, 4570, o);
    const auto [n1, p1port] = r.addNode(4569);
    const auto [n2, p2port] = r.addNode(4569);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}, {2, "127.0.0.1", p2port}};
    r.gw.partitions      = {{1, 0, 1, 1, 4569, ""}, {1, 1, 2, 1, 4569, ""}};
    r.gw.subscribeLeader = {{0, n1}, {1, n2}};
    r.connect();
    auto s0 = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                 [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, n1) != nullptr && s0.active(); }));
    auto s1 = r.client.subscribe(r.client.topic("t"), 1, rc::SubscribeOptions{rc::From::earliest()},
                                 [](const rc::Record&) {});
    ASSERT_TRUE(r.drive([&] { return r.subAt(1, n2) != nullptr && s1.active(); }));
    const std::size_t asked = r.gw.metadataCorrIds.size();
    const int64_t lastAsked = asked == 0 ? 0 : r.gw.metadataCorrIds.back();
    const int pingsBefore = r.gw.pings;              // three live sessions: ~60 Pings per second
    ASSERT_TRUE(r.drive([&] {
        for (std::size_t i = asked; i < r.gw.metadataCorrIds.size(); ++i)
            if (r.gw.metadataCorrIds[i] > lastAsked) return true;
        return r.gw.pings >= pingsBefore + 200;
    }, 40'000'000));
    EXPECT_LE(r.gw.pings - pingsBefore, 30) << "the refresh that confirms the second redirect was postponed by "
                                               "lagging answers (each re-armed it with a doubled backoff)";
}

// #440: the coordinator's seed session reconnects onto a node that was rolled back below what the consumer needs
// while the cluster once announced more (a rolling upgrade in flight).  The consumer waits (it is not ended
// BrokerTooOld) and keeps running once the coordinator answers with a current version.  RED: it ended BrokerTooOld.
// A rolling upgrade: node 1 (a partition leader) is already current and stays connected while the coordinator (the
// seed) is rolled back to an older broker: the consumer's join waits, it does not end.
TEST(GroupRoutingRegression, AConsumerDefersWhenItsCoordinatorReconnectsOntoAnOlderNodeRegression) {
    GroupRig r(4590, 4591);
    const auto [n1, p1port] = r.addNode(4590);
    r.gw.moreBrokers     = {{1, "127.0.0.1", p1port}};
    r.gw.partitions      = {{1, 0, /*leader*/ 0, 1, 4590, ""}, {1, 1, /*leader*/ 1, 1, 4590, ""}};
    r.gw.subscribeLeader = {{0, 0}, {1, n1}};
    r.gw.groupAssignment = {{1, 0}, {1, 1}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr && r.subAt(1, n1) != nullptr; }))
        << "precondition: the consumer is Stable, node 1's session connected";
    const int joins = r.gw.joinGroups;

    r.gw.advertisedBrokerVersionAt[0] = rune::protocol::client::kBrokerVersionExplicitGroupSubscriptions - 1;
    int connects = r.gw.connectsAt[0];
    r.gw.expireSession(r.seedSid);
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[0] > connects; })) << "precondition: the seed reconnected";
    for (int i = 0; i < 200; ++i) { r.client.doWork(); r.gw.pump(); }
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::None) << "ended on a rolling upgrade";
    EXPECT_TRUE(gc.active());
    EXPECT_EQ(r.gw.joinGroups, joins) << "no join against the older coordinator";

    r.gw.advertisedBrokerVersionAt[0] = rune::protocol::client::kBrokerVersion;
    connects = r.gw.connectsAt[0];
    r.gw.expireSession(r.seedSid);
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[0] > connects; })) << "precondition: the seed reconnected again";
    for (int i = 0; i < 200; ++i) { r.client.doWork(); r.gw.pump(); }
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::None);
    EXPECT_TRUE(gc.active()) << "still running on the upgraded coordinator";
}

// ===========================================================================
// C3 adversarial review (#440): liveness of a deferred join.
// ===========================================================================

// #440 liveness: the cluster announced a current version ONCE (the seed itself, before it was rolled back) and then
// never again: the rollback is permanent, no node of the cluster can serve the consumer.  The consumer must end
// explicitly (BrokerTooOld) once no CONNECTED session announces the needed version — never wait silently forever (a
// permanent hang that only one stderr line reveals).  Time-independent: it is decided by the live sessions'
// versions, no timer.  RED before the fix: the gate used the highest version EVER seen (sticky), so the consumer
// stayed active, unjoined, failReason None, through any number of reconnects onto the old node.
TEST(GroupRoutingRegression, ADeferredJoinWithNoLiveNewerNodeEndsBrokerTooOldNeverHangsAdversarialRegression) {
    GroupRig r(4594, 4595);
    r.gw.groupAssignment = {{1, 0}};
    auto gc = r.client.subscribeGroup("g", {"t"});
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr; })) << "precondition: the consumer is Stable";
    const int joins = r.gw.joinGroups;

    // Permanent rollback: every later connect announces an old version; no other node exists.
    r.gw.advertisedBrokerVersionAt[0] = rune::protocol::client::kBrokerVersionExplicitGroupSubscriptions - 1;
    for (int round = 0; round < 3; ++round) {
        const int connects = r.gw.connectsAt[0];
        r.gw.expireSession(r.seedSid);
        ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[0] > connects; })) << "the seed never reconnected";
        for (int i = 0; i < 200; ++i) { r.client.doWork(); r.gw.pump(); }
    }
    EXPECT_EQ(r.gw.joinGroups, joins) << "joined an old coordinator";
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::BrokerTooOld)
        << "no connected node can serve the consumer, yet it waits silently forever (deferred on a stale max)";
}
