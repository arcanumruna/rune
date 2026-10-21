// tests/client/GroupFilterClientTest.cpp — Phase 2c Task 12 (fix round 1): the Client.cpp wiring of filtered
// consumer groups, end to end over the InvokerGateway: the filter on JoinGroup and the fetch Subscribe, the
// position note reaching the hub, and refusal details reaching onError.  Single-threaded; bounded pump loops.
#include "../support/GroupRig.h"

#include "../../examples/filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/client/Client.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace {
using E = rune::gen::PlayerEvent;

struct FilteredGroup : GroupRig {
    std::vector<std::string> errors;
    explicit FilteredGroup(int32_t in) : GroupRig(in, in + 1) {
        gw.groupAssignment = {{1, 0}};
        client.onError([this](const std::string& m) { errors.push_back(m); });
    }
    void passes(int n) { for (int i = 0; i < n; ++i) { client.doWork(); gw.pump(); } }
    [[nodiscard]] bool errorMentions(const std::string& needle) const {
        return std::any_of(errors.begin(), errors.end(),
                           [&](const std::string& e) { return e.find(needle) != std::string::npos; });
    }
    [[nodiscard]] std::vector<int64_t> committed(int32_t partition) const {
        std::vector<int64_t> v;
        for (const auto& f : gw.offsetCommits) {
            const auto oc = rune::protocol::client::OffsetCommit::decode(f.data(), f.size());
            if (!oc) continue;
            for (const auto& e : oc->entries)
                if (e.topicId == 1 && e.partition == partition) v.push_back(e.offset);
        }
        return v;
    }
};

rc::GroupConfig filteredCfg() {
    rc::GroupConfig c;
    c.filter(E::Filter{.worldId = 7u});
    return c;
}
std::vector<uint8_t> wantBytes() {
    const auto b = E::Filter{.worldId = 7u}.encode();
    return std::vector<uint8_t>(b.span().begin(), b.span().end());
}
} // namespace

TEST(GroupFilterClient, FilterRidesJoinAndFetchAndNotesMoveTheCommitRegression) {
    FilteredGroup r(4500);
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr; }));
    ASSERT_FALSE(r.gw.joinFilters.empty());
    EXPECT_EQ(r.gw.joinFilters.back(), wantBytes()) << "JoinGroup carries the config's filter";
    const auto s0 = *r.subAt(0, 0);
    EXPECT_EQ(s0.filter, wantBytes()) << "the fetch Subscribe carries it too";
    // The Client routes an empty-batch Records frame (a position note) to the filtered group.
    r.gw.pushNote(s0, 5000);
    int tick = 0;
    ASSERT_TRUE(r.drive([&] {
        if (++tick % 50 == 1) gc.commit();   // a few commits, never a flood of commands
        const auto c = r.committed(0);
        return std::find(c.begin(), c.end(), 5000) != c.end();
    })) << "the note never moved the group's position";
}

TEST(GroupFilterClient, JoinMismatchDetailReachesOnErrorAndEndsTheGroupRegression) {
    FilteredGroup r(4502);
    r.gw.joinErrorCode   = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    r.gw.joinErrorDetail = "group filter mismatch: the group uses filter 00ff";
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.failReason() == rc::GroupFailReason::FilterMismatch; }));
    EXPECT_TRUE(r.errorMentions("group filter mismatch")) << "the broker's detail reaches onError";
    r.passes(300);
    EXPECT_EQ(r.gw.joinGroups, 1) << "no rejoin storm";
}

TEST(GroupFilterClient, LeaderFilterRefusalSurfacesAndEndsTheGroupNoRetryLoopRegression) {
    FilteredGroup r(4504);
    r.gw.subscribeErrorCode   = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    r.gw.subscribeErrorDetail = "filter needs a typed topic";
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return gc.failReason() == rc::GroupFailReason::RefusedByBroker; }));
    EXPECT_TRUE(r.errorMentions("filter needs a typed topic")) << "the leader's detail reaches onError";
    const auto frames = r.gw.subscribeFrames.size();
    r.passes(400);   // metadata cycles would re-send a parked partition
    EXPECT_EQ(r.gw.subscribeFrames.size(), frames) << "the refused Subscribe is never retried";
    EXPECT_EQ(frames, 1u);
}

// N1: the leader drops a BOUND filtered fetch instance with ErrorResponse{InvalidRequest} on its Subscribe
// correlation id: the consumer ends RefusedByBroker and the text reaches onError (no silent stall).
TEST(GroupFilterClient, BoundInstanceRefusedByTheLeaderEndsTheGroupRegression) {
    FilteredGroup r(4506);
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr; }));
    const auto s0 = *r.subAt(0, 0);
    ASSERT_TRUE(r.drive([&] { return gc.active() && s0.subscriptionId != 0; }));
    r.passes(20);
    ASSERT_EQ(gc.failReason(), rc::GroupFailReason::None);
    r.gw.sendError(0, static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest),
                   "filter no longer servable", s0.sessionId, s0.corrId);
    ASSERT_TRUE(r.drive([&] { return gc.failReason() == rc::GroupFailReason::RefusedByBroker; }));
    EXPECT_TRUE(r.errorMentions("filter no longer servable"));
    const auto frames = r.gw.subscribeFrames.size();
    r.passes(300);
    EXPECT_EQ(r.gw.subscribeFrames.size(), frames) << "never re-armed";
}

// N2: a seek re-arms the partition with the group's filter bytes.
TEST(GroupFilterClient, SeekReArmsWithTheFilterBytesRegression) {
    FilteredGroup r(4508);
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.gw.pendingJoins.empty(); }));
    r.gw.releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.subAt(0, 0) != nullptr; }));
    const auto first = *r.subAt(0, 0);
    EXPECT_EQ(first.filter, wantBytes());
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(gc.seek({1, 0}, 5, rc::CompletionTarget(r.cq), t), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] {
        const auto* s = r.gw.latestSubscription(1, 0);
        return s != nullptr && s->corrId != first.corrId;
    })) << "the seek never re-subscribed";
    const auto* again = r.gw.latestSubscription(1, 0);
    EXPECT_EQ(again->fromOffset, 5);
    EXPECT_EQ(again->filter, wantBytes()) << "a seek re-arm keeps the filter";
}

// M3: the seed reconnects onto an OLDER node while the coordinator lives on a node of its own at the current
// version.  A filtered consumer created afterwards is judged by the COORDINATOR's version, not the seed's.
// (One consumer per Client: the brief's unfiltered consumer A cannot coexist with B; the group-less
// coordinator move is made by a transactional producer's InitProducerId.)
TEST(GroupFilterClient, SeedOnAnOlderNodeDoesNotJudgeAFilteredConsumerOnAnotherCoordinatorRegression) {
    FilteredGroup r(4510);
    const auto [n1, p1port] = r.addNode(4510);
    r.gw.moreBrokers           = {{1, "127.0.0.1", p1port}};
    r.gw.partitions            = {{1, 0, n1, 1, 4510, ""}, {1, 1, n1, 1, 4510, ""}};
    r.gw.subscribeLeader       = {{0, n1}, {1, n1}};
    r.gw.groupAssignment       = {{1, 0}};
    r.gw.coordinatorEndpoint   = n1;
    r.gw.coordinatorRedirectTo = "127.0.0.1:" + std::to_string(p1port);
    r.gw.initPidReply          = true;
    r.gw.holdJoins             = false;
    rc::ProducerConfig pcfg;
    pcfg.acks            = rc::Acks::All;
    pcfg.transactionalId = "tx-m3";
    auto tp = r.client.producer(pcfg);
    r.connect();
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(tp.initTransactions(r.cq, t), rc::OpResult::Accepted);
    bool done = false;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) { if (cc.token == t) done = true; });
        return done;
    })) << "precondition: the coordinator moved to node 1";
    // The seed's node is rolled back below the filter version and the seed reconnects.
    r.gw.advertisedBrokerVersionAt[0] = rune::protocol::client::kBrokerVersionFilter - 1;
    const int connects = r.gw.connectsAt[0];
    r.gw.expireSession(r.seedSid);
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[0] > connects; })) << "precondition: the seed reconnected";
    r.passes(50);
    auto gc = r.client.subscribeGroup("g", {"t"}, filteredCfg());
    ASSERT_TRUE(r.drive([&] { return r.gw.joinGroups > 0; })) << "the filtered consumer never joined";
    EXPECT_EQ(r.gw.groupEndpoints.back(), n1) << "its JoinGroup reaches the coordinator";
    EXPECT_EQ(r.gw.joinFilters.back(), wantBytes());
    r.passes(100);
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::None) << "judged by the seed's older node";
    EXPECT_TRUE(gc.active());
}
