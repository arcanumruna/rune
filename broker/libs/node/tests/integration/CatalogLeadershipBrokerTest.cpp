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
#include <rune/protocol/RecordBatch.h>
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

/// A session on broker `id` (every broker hosts sessions in every role — the
/// follower-connect hang fix; retried only while its client endpoint warms up).
int64_t connectSettled(ShardTestClient& cl, const ThreeNodes& c, int id, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        const int64_t sid = cl.connect(clientChannelOf(c, id), kClientStream);
        if (sid > 0) return sid;
        std::this_thread::sleep_for(50ms);
    }
    return -1;
}

/// Halt `id`'s applier at its NEXT event (only ITS apply is tampered).
void tamperNextEvent(BrokerNode& n) {
    n.onAdminThreadForTest([&n] {
        n.adminWorkerAgentForTest()->worker().applierForTest().tamperNextFingerprintForTest();
        return 0;
    });
}
bool awaitHalted(BrokerNode& n, std::chrono::seconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
        if (n.onAdminThreadForTest([&n] { return n.adminWorkerAgentForTest()->worker().roleName() == "Halted"; }))
            return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}
std::optional<rune::metadata::PartitionAssignment> assignmentOn(BrokerNode& n, int32_t topicId, int32_t p) {
    return n.onControlThreadForTest([&n, topicId, p]() -> std::optional<rune::metadata::PartitionAssignment> {
        const auto* pa = n.metadataServiceForTest()->assignment(topicId, p);
        return pa ? std::optional(*pa) : std::nullopt;
    });
}
/// `name` is applied on `n` (its catalog has it).
bool awaitApplied(BrokerNode& n, const std::string& name, std::chrono::seconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
        if (n.onAdminThreadForTest([&n, &name] {
                return n.adminWorkerAgentForTest()->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, name) != nullptr;
            }))
            return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}
std::vector<uint8_t> oneRecordBatch(std::string_view value) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, value, 0);
    return b.build();
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
    EXPECT_GE(halted.onControlThreadForTest([&halted] { return halted.electionForTest()->standDowns(); }), 1u)
        << "it stood down (never a permanent withdrawal: ruling 1)";
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
    // Review M5/M1: steady state — sampled over 3 s (beyond the stand-down hold),
    // the halted broker never leads again and stood down exactly once.
    for (const auto until = Clock::now() + 3s; Clock::now() < until;) {
        EXPECT_FALSE(c.node(ctl).isLeader()) << "the halted broker led the consensus again";
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_EQ(halted.onControlThreadForTest([&halted] { return halted.electionForTest()->standDowns(); }), 1u)
        << "one stand-down, no flapping";
    EXPECT_EQ(consensusLeader(c), newCtl) << "the healthy controller stayed";

    // The halted broker answers DDL Internal with its offset — never a Redirect.
    const auto refused = cl.awaitAnyDdlAnswer(
        cl.sendDropTopic(clientChannelOf(c, ctl), kClientStream, sid[static_cast<std::size_t>(ctl)], "fresh"),
        Clock::now() + 10s);
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(refused->responseTemplate, kErrorResponse);
    EXPECT_EQ(refused->code, kLdInternal);
    EXPECT_NE(refused->message.find("catalog apply halted at offset"), std::string::npos) << refused->message;
}

// Fix round 0, ruling 2: a halted broker reports its halt (node-control
// register, kNodeCatalogHalted) and the controller never places a NEW partition
// on it.  RED: the fresh topic's replicas name the halted broker (it never
// applies the topic, so those replicas never serve).
TEST(CatalogLeadershipBrokerRegression, ControllerNeverPlacesANewPartitionOnAHaltedBrokerRegression) {
    ThreeNodes c = ThreeNodes::start("ld_halt_place");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitControllerLeadsTheCatalog(c, 30s);
    ASSERT_GT(ctl, 0);
    const int32_t h = ctl == 3 ? 2 : 3;                 // a healthy follower that halts
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);
    const int64_t sid = connectSettled(cl, c, ctl, 20s);
    ASSERT_GT(sid, 0);
    const std::string ch = clientChannelOf(c, ctl);
    const auto ready = defineSettled(cl, ch, sid, "ready", 30s);
    ASSERT_TRUE(ready.has_value());
    ASSERT_EQ(ready->code, kLdOk);

    tamperNextEvent(c.node(h));
    const auto trigger = defineSettled(cl, ch, sid, "trigger", 30s);
    ASSERT_TRUE(trigger.has_value());
    ASSERT_EQ(trigger->code, kLdOk) << "the healthy leader serves; only broker " << h << " halts";
    ASSERT_TRUE(awaitHalted(c.node(h), 15s));
    BrokerNode& controller = c.node(ctl);
    bool known = false;
    for (const auto deadline = Clock::now() + 15s; Clock::now() < deadline && !known;) {
        known = controller.onControlThreadForTest([&controller, h] {
            return controller.metadataServiceForTest()->catalogHalted(h);
        });
        if (!known) std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(known) << "the controller learned the halt";
    EXPECT_EQ(consensusLeader(c), ctl) << "a halted FOLLOWER changes no controllership";

    // Fix round 1 (I2): the placement view reached the catalog leader's admin
    // thread (control → admin ring), and its dry-run refuses an rf it cannot place.
    bool viewArrived = false;
    for (const auto deadline = Clock::now() + 15s; Clock::now() < deadline && !viewArrived;) {
        viewArrived = controller.onAdminThreadForTest([&controller] {
            return controller.adminWorkerAgentForTest()->worker().placeableBrokers() == 2;
        });
        if (!viewArrived) std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(viewArrived);
    const auto rf3 = cl.awaitAnyDdlAnswer(
        cl.sendDefineTopic(ch, kClientStream, sid, "rf3", 1, charBlob(), 0, {}, /*replicationFactor=*/3),
        Clock::now() + 10s);
    ASSERT_TRUE(rf3.has_value());
    EXPECT_EQ(rf3->code, kLdInvalidRequest);
    EXPECT_EQ(rf3->message, "replicationFactor 3 > placeable brokers 2 (broker " + std::to_string(h) +
                                ": catalog apply halted)");

    const auto fresh = defineSettled(cl, ch, sid, "fresh", 30s);   // rf 0 = every PLACEABLE broker
    ASSERT_TRUE(fresh.has_value());
    ASSERT_EQ(fresh->code, kLdOk);
    const auto schema = cl.awaitSchema(cl.sendGetTopicSchema(ch, kClientStream, sid, "fresh"), Clock::now() + 10s);
    ASSERT_TRUE(schema.has_value());
    const auto pa = assignmentOn(controller, schema->topicId, 0);
    ASSERT_TRUE(pa.has_value());
    EXPECT_EQ(pa->replicas.size(), 2u) << "every placeable broker";
    EXPECT_EQ(std::find(pa->replicas.begin(), pa->replicas.end(), h), pa->replicas.end())
        << "never placed on the halted broker " << h;
    EXPECT_NE(pa->leaderNode, h);
}

// Fix round 0, ruling 1: with EVERY broker halted no broker stands down — a
// halted controller stays the controller for the data plane, so a data
// partition still fails over when its leader dies.  RED (the first fix's
// permanent withdrawal): no controller is left and the partition stays led by
// the dead broker.
TEST(CatalogLeadershipBrokerRegression, AllBrokersHaltedKeepAControllerAndADataPartitionStillFailsOverRegression) {
    ThreeNodes c = ThreeNodes::start("ld_all_halted");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitControllerLeadsTheCatalog(c, 30s);
    ASSERT_GT(ctl, 0);
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);
    std::vector<int64_t> sid(4, 0);
    sid[static_cast<std::size_t>(ctl)] = connectSettled(cl, c, ctl, 20s);
    ASSERT_GT(sid[static_cast<std::size_t>(ctl)], 0);
    const auto data = defineSettled(cl, clientChannelOf(c, ctl), sid[static_cast<std::size_t>(ctl)], "data", 30s);
    ASSERT_TRUE(data.has_value());
    ASSERT_EQ(data->code, kLdOk);
    for (int id = 1; id <= 3; ++id)
        if (id != ctl) {
            sid[static_cast<std::size_t>(id)] = connectSettled(cl, c, id, 20s);
            ASSERT_GT(sid[static_cast<std::size_t>(id)], 0) << id;
        }
    const auto schema = cl.awaitSchema(cl.sendGetTopicSchema(clientChannelOf(c, ctl), kClientStream,
                                                             sid[static_cast<std::size_t>(ctl)], "data"),
                                       Clock::now() + 10s);
    ASSERT_TRUE(schema.has_value());
    const int32_t dataId = schema->topicId;

    // Fix round 2 (re-review O2): arm the tamper only once EVERY broker applied
    // 'data' — under load a broker could otherwise halt on 'data' itself.
    for (int id = 1; id <= 3; ++id) ASSERT_TRUE(awaitApplied(c.node(id), "data", 15s)) << id;
    // Every broker's applier halts at the same next event.
    for (int id = 1; id <= 3; ++id) tamperNextEvent(c.node(id));
    const auto trigger = cl.awaitAnyDdlAnswer(
        cl.sendDefineTopic(clientChannelOf(c, ctl), kClientStream, sid[static_cast<std::size_t>(ctl)], "trigger", 1,
                           charBlob()),
        Clock::now() + 15s);
    ASSERT_TRUE(trigger.has_value());
    EXPECT_EQ(trigger->code, kLdInternal) << "the halted catalog leader";
    for (int id = 1; id <= 3; ++id) ASSERT_TRUE(awaitHalted(c.node(id), 15s)) << id;

    // A controller exists and STAYS (nobody healthy to hand it to) — review M5:
    // sampled over 2 s, and no broker ever stood down.
    int32_t stayed = -1;
    for (const auto deadline = Clock::now() + 10s; Clock::now() < deadline && stayed < 0;) {
        stayed = consensusLeader(c);
        if (stayed < 0) std::this_thread::sleep_for(20ms);
    }
    ASSERT_GT(stayed, 0) << "never a cluster without a controller";
    for (const auto until = Clock::now() + 2s; Clock::now() < until;) {
        EXPECT_EQ(consensusLeader(c), stayed) << "the controller is stable (no flap)";
        std::this_thread::sleep_for(100ms);
    }
    for (int id = 1; id <= 3; ++id)
        EXPECT_EQ(c.node(id).onControlThreadForTest([&c, id] { return c.node(id).electionForTest()->standDowns(); }),
                  0u) << "broker " << id << " stood down although no healthy peer exists";

    // DDL is Internal everywhere.
    for (int id = 1; id <= 3; ++id) {
        const auto a = cl.awaitAnyDdlAnswer(cl.sendDropTopic(clientChannelOf(c, id), kClientStream,
                                                             sid[static_cast<std::size_t>(id)], "data"),
                                            Clock::now() + 10s);
        ASSERT_TRUE(a.has_value()) << id;
        EXPECT_EQ(a->code, kLdInternal) << "broker " << id;
    }

    // The data partition's leader dies; a survivor takes it over.
    const auto before = assignmentOn(c.node(stayed), dataId, 0);
    ASSERT_TRUE(before.has_value());
    const int32_t dead = before->leaderNode;
    ASSERT_GT(dead, 0);
    c.stop(dead);
    std::optional<rune::metadata::PartitionAssignment> after;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline;) {
        const int32_t now = consensusLeader(c);
        if (now > 0 && now != dead) {
            after = assignmentOn(c.node(now), dataId, 0);
            if (after && after->leaderNode > 0 && after->leaderNode != dead) break;
        }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(after.has_value());
    ASSERT_NE(after->leaderNode, dead) << "failover still works with every broker halted";
    const int32_t newLeader = after->leaderNode;
    const auto ack = cl.produceUntilOk(clientChannelOf(c, newLeader), kClientStream,
                                       sid[static_cast<std::size_t>(newLeader)], dataId, 0, /*acks=*/1,
                                       oneRecordBatch("after-failover"));
    ASSERT_TRUE(ack.has_value()) << "the new partition leader serves produce";
    EXPECT_EQ(ack->errorCode, 0);
}

// Fix rounds 2/4 (re-review BLOCK): the create event that halts broker h ALSO
// places the topic on h — h's halt broadcast is held so no member knows yet
// (the race, forced).  h cannot serve the topic and REPORTS it, live; the
// controller only excludes h from those partitions' leader election (round 4:
// nothing else — h stays in the replicas, no metadata record).  h never leads;
// the topic serves produce from the others.  RED (no report): h stays the
// static leader of ~1/3 of the partitions of a topic it does not host.
TEST(CatalogLeadershipBrokerRegression, TopicPlacedOnABrokerThatHaltsOnItsCreateIsRePlacedAndServesRegression) {
    ThreeNodes c = ThreeNodes::start("ld_halt_race");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitControllerLeadsTheCatalog(c, 30s);
    ASSERT_GT(ctl, 0);
    const int32_t h = ctl == 3 ? 2 : 3;
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);
    std::vector<int64_t> sid(4, 0);
    sid[static_cast<std::size_t>(ctl)] = connectSettled(cl, c, ctl, 20s);
    ASSERT_GT(sid[static_cast<std::size_t>(ctl)], 0);
    const std::string ch = clientChannelOf(c, ctl);
    const auto ready = defineSettled(cl, ch, sid[static_cast<std::size_t>(ctl)], "ready", 30s);
    ASSERT_TRUE(ready.has_value());
    ASSERT_EQ(ready->code, kLdOk);
    for (int id = 1; id <= 3; ++id) {
        if (id == ctl) continue;
        sid[static_cast<std::size_t>(id)] = connectSettled(cl, c, id, 20s);
        ASSERT_GT(sid[static_cast<std::size_t>(id)], 0) << id;
    }
    for (int id = 1; id <= 3; ++id) ASSERT_TRUE(awaitApplied(c.node(id), "ready", 15s)) << id;

    // The race: h halts on the NEXT event, and nobody hears of it.
    c.node(h).holdCatalogHaltReportForTest(true);
    tamperNextEvent(c.node(h));
    const auto racy = cl.awaitAnyDdlAnswer(
        cl.sendDefineTopic(ch, kClientStream, sid[static_cast<std::size_t>(ctl)], "racy", 3, charBlob()),
        Clock::now() + 15s);
    ASSERT_TRUE(racy.has_value()) << "answered explicitly";
    ASSERT_TRUE(awaitHalted(c.node(h), 15s));
    const auto schema = cl.awaitSchema(cl.sendGetTopicSchema(ch, kClientStream, sid[static_cast<std::size_t>(ctl)],
                                                             "racy"),
                                       Clock::now() + 10s);
    ASSERT_TRUE(schema.has_value());
    ASSERT_EQ(schema->errorCode, 0u);
    const int32_t topicId = schema->topicId;
    BrokerNode& controller = c.node(ctl);
    EXPECT_FALSE(controller.onControlThreadForTest([&controller, h] {
        return controller.metadataServiceForTest()->catalogHalted(h);
    })) << "precondition: the halt mark is held — the placement could not know";

    // Within a bound: h leads none of the partitions, and the controller holds
    // h's live reports for them.
    bool clean = false;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && !clean;) {
        clean = true;
        for (int32_t p = 0; p < 3; ++p) {
            const auto pa = assignmentOn(controller, topicId, p);
            clean = clean && pa && pa->leaderNode > 0 && pa->leaderNode != h;
        }
        if (!clean) std::this_thread::sleep_for(50ms);
    }
    ASSERT_TRUE(clean) << "h never leads a partition it cannot serve";
    const bool reported = controller.onControlThreadForTest([&controller, topicId, h] {
        const MetadataService* ms = controller.metadataServiceForTest();
        return ms->reportedUnservable(topicId, 0, h) && ms->reportedUnservable(topicId, 1, h) &&
               ms->reportedUnservable(topicId, 2, h);
    });
    EXPECT_TRUE(reported) << "the race really placed the topic on h, and h reports it, live";
    bool stillReplica = false;
    for (int32_t p = 0; p < 3; ++p) {
        const auto pa = assignmentOn(controller, topicId, p);
        stillReplica = stillReplica || std::find(pa->replicas.begin(), pa->replicas.end(), h) != pa->replicas.end();
    }
    EXPECT_TRUE(stillReplica) << "round 4: the report never edits the replica set";
    // Never leads, sampled.
    for (const auto until = Clock::now() + 2s; Clock::now() < until;) {
        for (int32_t p = 0; p < 3; ++p) {
            const auto pa = assignmentOn(controller, topicId, p);
            ASSERT_TRUE(pa.has_value());
            EXPECT_NE(pa->leaderNode, h) << "partition " << p;
        }
        std::this_thread::sleep_for(100ms);
    }
    // It serves produce from the others.
    for (int32_t p = 0; p < 3; ++p) {
        const int32_t leader = assignmentOn(controller, topicId, p)->leaderNode;
        const auto ack = cl.produceUntilOk(clientChannelOf(c, leader), kClientStream,
                                           sid[static_cast<std::size_t>(leader)], topicId, p, /*acks=*/1,
                                           oneRecordBatch("served"));
        ASSERT_TRUE(ack.has_value()) << "partition " << p << " serves from broker " << leader;
    }
    c.node(h).holdCatalogHaltReportForTest(false);
}
