// CatalogLeadershipBrokerTest.cpp — catalog history Task 11 on THREE in-process
// BrokerNodes over REAL Aeron UDP, driven by the wire client ShardTestClient.
//
//   * the DDL route (CatalogRoute, published by the control thread from the
//     committed (-6, 0) assignment): a follower answers EVERY DDL template (64,
//     68, 70, 74–77) Redirect to the catalog leader's client endpoint — never
//     Internal (fix-on-sight) — and a pre-v8 session InvalidRequest in its own
//     response type (F37); reads are served locally;
//   * a halted broker (its CatalogApplier halted) answers DDL Internal naming the
//     offset, never Redirect; a halted CONTROLLER resigns, so another broker
//     places the topics created after the halt (binding, Task 8 → 11).
#include "support/CatalogBrokerCluster.h"
#include "support/DdlHarness.h"          // charBlob / viewPlanBlob / alterOpsFor
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
using Clock = std::chrono::steady_clock;

namespace {

constexpr int16_t kLdOk             = 0;
constexpr int16_t kLdNotLeader      = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kLdRedirect       = static_cast<int16_t>(pc::ErrorCode::Redirect);
constexpr int16_t kLdInternal       = static_cast<int16_t>(pc::ErrorCode::Internal);
constexpr int16_t kLdInvalidRequest = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
constexpr uint16_t kErrorResponse = static_cast<uint16_t>(pc::TemplateId::ErrorResponse);

std::string clientChannelOf(const ThreeNodes& c, int id) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(id - 1)]);
}

/// One DDL of each template on session `sid` of `ch`; returns its correlation id.
int64_t sendDdlOf(ShardTestClient& cl, const std::string& ch, int64_t sid, pc::TemplateId t, int seq) {
    const std::string name = "r" + std::to_string(seq);
    switch (t) {
    case pc::TemplateId::DefineTopic:      return cl.sendDefineTopic(ch, kClientStream, sid, name, 1, charBlob());
    case pc::TemplateId::AlterTopic:
        return cl.sendAlterTopic(ch, kClientStream, sid, name, 0, alterOpsFor(Op::AlterAdd, static_cast<uint64_t>(seq)));
    case pc::TemplateId::CreateView:
        return cl.sendCreateView(ch, kClientStream, sid, name, "id", 0, viewPlanBlob(name, 0, 0));
    case pc::TemplateId::DropTopic:        return cl.sendDropTopic(ch, kClientStream, sid, name);
    case pc::TemplateId::AlterTopicConfig: return cl.sendAlterTopicConfig(ch, kClientStream, sid, name, 0, {{1, 60'000}});
    case pc::TemplateId::DropView:         return cl.sendDropView(ch, kClientStream, sid, name);
    case pc::TemplateId::AlterView:
        return cl.sendAlterView(ch, kClientStream, sid, name, "id", 0, 0, viewPlanBlob(name, 0, 0));
    default: return 0;
    }
}

const std::vector<pc::TemplateId>& allDdlTemplates() {
    static const std::vector<pc::TemplateId> all{pc::TemplateId::DefineTopic, pc::TemplateId::AlterTopic,
                                                 pc::TemplateId::CreateView, pc::TemplateId::DropTopic,
                                                 pc::TemplateId::AlterTopicConfig, pc::TemplateId::DropView,
                                                 pc::TemplateId::AlterView};
    return all;
}

/// A define on `ch` retried (bounded) while the answer is the retriable NotLeader
/// — the documented settle while leadership / the route converge.
std::optional<ShardTestClient::AnyAnswer> defineSettled(ShardTestClient& cl, const std::string& ch, int64_t sid,
                                                        const std::string& name, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    std::optional<ShardTestClient::AnyAnswer> a;
    while (Clock::now() < deadline) {
        a = cl.awaitAnyDdlAnswer(cl.sendDefineTopic(ch, kClientStream, sid, name, 1, charBlob()), Clock::now() + 10s);
        if (!a || a->code != kLdNotLeader) return a;
        std::this_thread::sleep_for(50ms);
    }
    return a;
}

int32_t consensusLeader(ThreeNodes& c) {
    for (BrokerNode* n : c.live())
        if (n->isLeader()) return n->nodeIdForTest();
    return -1;
}

bool awaitConsensusLeaderOtherThan(ThreeNodes& c, int32_t old, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        const int32_t l = consensusLeader(c);
        if (l > 0 && l != old) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

/// The controller (consensus leader) and the catalog leader agree — the
/// controller prefers itself as the catalog leader (ruling 7); returns that id.
int32_t awaitControllerLeadsTheCatalog(ThreeNodes& c, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        const int32_t ctl = consensusLeader(c);
        if (ctl > 0 && c.settledLeader() == ctl) return ctl;
        std::this_thread::sleep_for(20ms);
    }
    return -1;
}

/// A session on broker `id`.  Until a user topic is committed, a broker that is
/// not the consensus leader redirects connects (pre-MD1 bootstrap gate): retried.
int64_t connectSettled(ShardTestClient& cl, const ThreeNodes& c, int id, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        const int64_t sid = cl.connect(clientChannelOf(c, id), kClientStream);
        if (sid > 0) return sid;
        std::this_thread::sleep_for(50ms);
    }
    return -1;
}

} // namespace

// Fix-on-sight (Task 11): a non-leader admin answered DDL Internal (non-retriable).
// Over real Aeron a follower answers every DDL template Redirect with the catalog
// leader's client endpoint, exactly once; the leader serves it.  RED: Internal /
// NotLeader forever on the follower.
TEST(CatalogLeadershipBrokerRegression, FollowerRedirectsEveryDdlTemplateToTheCatalogLeaderOverAeronRegression) {
    ThreeNodes c = ThreeNodes::start("ld_redirect");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = awaitControllerLeadsTheCatalog(c, 30s);
    ASSERT_GT(leader, 0);
    const int32_t follower = leader == 1 ? 2 : 1;
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);   // a v8 session follows a DDL Redirect
    const std::string leaderCh = clientChannelOf(c, leader), followerCh = clientChannelOf(c, follower);
    const int64_t onLeader = connectSettled(cl, c, leader, 20s);
    ASSERT_GT(onLeader, 0);

    // Settle: the leader serves DDL (its catch-up gate opened).
    const auto ready = defineSettled(cl, leaderCh, onLeader, "ready", 30s);
    ASSERT_TRUE(ready.has_value());
    ASSERT_EQ(ready->code, kLdOk);
    const int64_t onFollower = connectSettled(cl, c, follower, 20s);
    ASSERT_GT(onFollower, 0);

    int seq = 0;
    for (pc::TemplateId t : allDdlTemplates()) {
        SCOPED_TRACE(static_cast<int>(t));
        // The route is published on the follower's control pass: bounded settle on NotLeader.
        std::optional<ShardTestClient::AnyAnswer> a;
        int64_t corr = 0;
        for (const auto deadline = Clock::now() + 20s; Clock::now() < deadline;) {
            corr = sendDdlOf(cl, followerCh, onFollower, t, ++seq);
            ASSERT_GT(corr, 0) << "sent";
            a = cl.awaitAnyDdlAnswer(corr, Clock::now() + 10s);
            if (!a || a->code != kLdNotLeader) break;
            std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(a.has_value()) << "every DDL is answered";
        EXPECT_EQ(a->responseTemplate, kErrorResponse);
        EXPECT_EQ(a->code, kLdRedirect) << "never Internal on a follower";
        EXPECT_EQ(a->message, leaderCh) << "the catalog leader's client endpoint";
        std::this_thread::sleep_for(20ms);
        EXPECT_EQ(cl.answersFor(corr), 1u) << "answered exactly once";
    }
    // Reads are served locally on the follower (never redirected).
    const auto schema = cl.awaitSchema(cl.sendGetTopicSchema(followerCh, kClientStream, onFollower, "ready"),
                                       Clock::now() + 10s);
    ASSERT_TRUE(schema.has_value());
    EXPECT_NE(schema->errorCode, static_cast<uint16_t>(kLdRedirect));

    // F37: a pre-v8 session on the follower is answered InvalidRequest in its own type.
    ShardTestClient old(c.dirs[1]->path / "client-aeron-v7");
    old.keepSessionsAlive(true);
    old.setClientVersion(7);
    const int64_t oldSid = connectSettled(old, c, follower, 20s);
    ASSERT_GT(oldSid, 0);
    const auto refused = old.awaitAnyDdlAnswer(old.sendDefineTopic(followerCh, kClientStream, oldSid, "old", 1, charBlob()),
                                               Clock::now() + 10s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->responseTemplate, static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse));
    EXPECT_EQ(refused->code, kLdInvalidRequest);
}

// Binding (Task 8 → 11) + review focus #5: a broker whose applier HALTED never
// drives placement — the halted controller resigns, another broker becomes the
// controller and places a topic created after the halt; the halted broker
// answers DDL Internal naming its offset (never a Redirect).  RED (no
// resignation): the halted controller stays the controller, never applies the
// new topic, never places it — the DDL's OK (held until the topic serves) is
// never given.
TEST(CatalogLeadershipBrokerRegression, HaltedControllerResignsAndAnotherBrokerPlacesANewTopicRegression) {
    ThreeNodes c = ThreeNodes::start("ld_halted_ctl");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitControllerLeadsTheCatalog(c, 30s);
    ASSERT_GT(ctl, 0);
    const int32_t leader0 = ctl;
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);
    std::vector<int64_t> sid(4, 0);
    sid[static_cast<std::size_t>(ctl)] = connectSettled(cl, c, ctl, 20s);
    ASSERT_GT(sid[static_cast<std::size_t>(ctl)], 0);
    const auto ready = defineSettled(cl, clientChannelOf(c, leader0), sid[static_cast<std::size_t>(leader0)], "ready", 30s);
    ASSERT_TRUE(ready.has_value());
    ASSERT_EQ(ready->code, kLdOk);
    for (int id = 1; id <= 3; ++id) {
        if (id == ctl) continue;
        sid[static_cast<std::size_t>(id)] = connectSettled(cl, c, id, 20s);
        ASSERT_GT(sid[static_cast<std::size_t>(id)], 0) << id;
    }

    // The controller's applier halts at the next event (only ITS apply is tampered).
    BrokerNode& halted = c.node(ctl);
    halted.onAdminThreadForTest([&halted] {
        halted.adminWorkerAgentForTest()->worker().applierForTest().tamperNextFingerprintForTest();
        return 0;
    });
    const auto trigger = cl.awaitAnyDdlAnswer(
        cl.sendDefineTopic(clientChannelOf(c, leader0), kClientStream, sid[static_cast<std::size_t>(leader0)], "trigger",
                           1, charBlob()),
        Clock::now() + 15s);
    ASSERT_TRUE(trigger.has_value()) << "answered (OK, or Internal when the halted broker led the catalog)";
    EXPECT_TRUE(trigger->code == kLdOk || trigger->code == kLdInternal) << trigger->code;
    const bool haltedNow = [&] {
        for (const auto deadline = Clock::now() + 15s; Clock::now() < deadline;) {
            if (halted.onAdminThreadForTest([&halted] {
                    return halted.adminWorkerAgentForTest()->worker().roleName() == "Halted";
                }))
                return true;
            std::this_thread::sleep_for(20ms);
        }
        return false;
    }();
    ASSERT_TRUE(haltedNow);

    // It resigns the controllership; another broker leads the consensus AND the catalog.
    ASSERT_TRUE(awaitConsensusLeaderOtherThan(c, ctl, 30s)) << "the halted controller resigned";
    EXPECT_TRUE(halted.onControlThreadForTest([&halted] { return halted.electionForTest()->resigned(); }));
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(ctl, 30s));
    const int32_t newCtl = consensusLeader(c);
    const int32_t newLeader = c.catalogLeaderId();
    ASSERT_NE(newCtl, ctl);
    ASSERT_NE(newLeader, ctl);

    // A topic created after the halt is placed by the new controller and serves.
    const auto fresh = defineSettled(cl, clientChannelOf(c, newLeader), sid[static_cast<std::size_t>(newLeader)],
                                     "fresh", 30s);
    ASSERT_TRUE(fresh.has_value());
    ASSERT_EQ(fresh->code, kLdOk) << "the OK is held until the topic serves — it was placed";
    const auto schema = cl.awaitSchema(cl.sendGetTopicSchema(clientChannelOf(c, newLeader), kClientStream,
                                                             sid[static_cast<std::size_t>(newLeader)], "fresh"),
                                       Clock::now() + 10s);
    ASSERT_TRUE(schema.has_value());
    ASSERT_EQ(schema->errorCode, 0u);
    BrokerNode& placer = c.node(newCtl);
    const bool placed = placer.onControlThreadForTest([&placer, id = schema->topicId] {
        return placer.metadataServiceForTest()->assignment(id, 0) != nullptr;
    });
    EXPECT_TRUE(placed) << "the new controller placed the topic created after the halt";
    EXPECT_FALSE(c.node(ctl).isLeader()) << "the halted broker never leads the consensus again";

    // The halted broker answers DDL Internal with its offset — never a Redirect.
    const auto refused = cl.awaitAnyDdlAnswer(
        cl.sendDropTopic(clientChannelOf(c, ctl), kClientStream, sid[static_cast<std::size_t>(ctl)], "fresh"),
        Clock::now() + 10s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->responseTemplate, kErrorResponse);
    EXPECT_EQ(refused->code, kLdInternal);
    EXPECT_NE(refused->message.find("catalog apply halted at offset"), std::string::npos) << refused->message;
}
