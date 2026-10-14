// CatalogGatewayTest.cpp — catalog history Task 8: C1 generalized at the gateway
// (spec §5.4): "A topic serves on a broker only when both: its CreateTopic is
// applied on that broker; its placement is committed."
//
//   placed, not applied here  → NotLeader (retriable: a producer that learned
//                               the topic from another broker; this one applies
//                               it next)                     [review focus #4]
//   applied, not placed       → NotLeader (retriable), hidden from metadata
//   neither                   → UnknownTopic
//   applied + placed          → serves
//   dropped                   → UnknownTopic at once — even while its placement
//                               is still in the routing image — and every live
//                               subscription of it ends with an explicit
//                               UnknownTopic (never a silent stall)
//
// Deterministic: no Aeron, no sleeps — makeForTest + the apply*ForTest seams
// (the TopicServabilityTest rig); "committed placement" = the steady-state
// routing image (committed metadata ready).
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-catalog-gw-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kSelf = 1;

struct GatewayRig {
    TempDir                        dir;
    std::unique_ptr<TopicRegistry> registry;
    ClientGateway                  gw;
    int64_t                        sid  = 0;
    int64_t                        corr = 100;
    std::vector<rune::metadata::RoutingImage::BuildEntry> placements;   // the committed placements
    bool committedImage = true;   // false: the static-balancer image (bootstrap / after the last drop)
    std::vector<pc::ProduceResponse>   produceAnswers;
    std::vector<pc::SubscribeResponse> subscribeAnswers;
    std::vector<pc::ErrorResponse>     errors;
    std::optional<pc::MetadataResponse::Decoded> metadata;

    GatewayRig() : gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
                         onFrame(b, n);
                         return 1;
                     }, 1)) {
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setNodeIdForTest(kSelf);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        publish();
        sid = gw.injectTestSession();
    }

    /// The MSM's committed assignment map as the gateway sees it: the steady-state image.
    void publish() {
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(
            placements, committedImage ? rune::metadata::RoutingImage::Source::Committed
                                       : rune::metadata::RoutingImage::Source::Static));
        gw.setCommittedMetadataReady(true);
    }
    void place(int32_t topicId, int32_t partitions) {
        for (int32_t p = 0; p < partitions; ++p)
            placements.push_back({rune::metadata::gpidOf(topicId, p), kSelf, 1});
        publish();
    }
    int32_t apply(int32_t topicId, int32_t partitions, const std::string& name) {
        rune::node::TopicSpec s;
        s.name = name; s.partitionCount = partitions;
        return registry->createTopicAt(topicId, s);
    }
    void placeWithoutApply(int32_t topicId, int32_t partitions) { place(topicId, partitions); }
    void applyWithoutPlacement(int32_t topicId, int32_t partitions) {
        ASSERT_EQ(apply(topicId, partitions, "t" + std::to_string(topicId)), topicId);
    }

    void onFrame(const uint8_t* b, std::size_t n) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
            case pc::TemplateId::ProduceResponse:
                if (auto r = pc::ProduceResponse::decode(b, n)) produceAnswers.push_back(*r);
                break;
            case pc::TemplateId::SubscribeResponse:
                if (auto r = pc::SubscribeResponse::decode(b, n)) subscribeAnswers.push_back(*r);
                break;
            case pc::TemplateId::ErrorResponse:
                if (auto r = pc::ErrorResponse::decode(b, n)) errors.push_back(*r);
                break;
            case pc::TemplateId::MetadataResponse:
                metadata = pc::MetadataResponse::decode(b, n);
                break;
            case pc::TemplateId::ViewRowResponse:
                if (auto r = pc::ViewRowResponse::decode(b, n)) viewRows.emplace_back(r->correlationId, r->status);
                break;
            default: break;
        }
    }
    std::vector<std::pair<int64_t, uint16_t>> viewRows;   // (correlationId, status)
    /// A GetViewRow's ViewRowResponse status (-1: unanswered).
    int viewRowStatus(int32_t viewTopicId) {
        pc::GetViewRow m;
        m.sessionId = sid; m.correlationId = ++corr; m.viewTopicId = viewTopicId; m.keyU64 = 7;
        std::vector<uint8_t> buf(pc::GetViewRow::ENCODED_SIZE);
        m.encode(buf.data());
        gw.applyGetViewRowForTest(buf.data(), buf.size());
        for (const auto& [c, st] : viewRows) if (c == m.correlationId) return st;
        return -1;
    }
    std::optional<int16_t> codeOf(int64_t c) const {
        for (const auto& r : produceAnswers) if (r.correlationId == c) return r.errorCode;
        for (const auto& r : subscribeAnswers) if (r.correlationId == c) return r.errorCode;
        for (const auto& e : errors) if (e.correlationId == c) return e.code;
        return std::nullopt;
    }

    struct ProduceResult { int16_t errorCode = -1; };
    ProduceResult produce(int32_t topicId, int32_t partition) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"v"}, 0);
        const std::vector<uint8_t> batch = b.build();
        pc::Produce m;
        m.sessionId = sid; m.correlationId = ++corr; m.topicId = topicId; m.partition = partition;
        m.partitionKnown = 1; m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        gw.applyProduceForTest(buf.data(), buf.size());
        const auto c = codeOf(m.correlationId);
        EXPECT_TRUE(c.has_value()) << "every produce is answered, never silent";
        return ProduceResult{c.value_or(-1)};
    }
    int64_t subscribe(int32_t topicId, int32_t partition) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = ++corr; s.topicId = topicId; s.partition = partition;
        s.partitionKnown = 1; s.fromOffset = 0; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
        return s.correlationId;
    }
    pc::ErrorCode subscribeError(int32_t topicId, int32_t partition) {
        const int64_t c = subscribe(topicId, partition);
        const auto code = codeOf(c);
        EXPECT_TRUE(code.has_value()) << "every subscribe is answered, never silent";
        return static_cast<pc::ErrorCode>(code.value_or(-1));
    }
    bool listed(int32_t topicId) {
        pc::MetadataRequest req;
        req.sessionId = sid; req.correlationId = ++corr;
        std::vector<uint8_t> buf(req.encodedSize());
        req.encode(buf.data());
        metadata.reset();
        gw.applyMetadataForTest(buf.data(), buf.size());
        if (!metadata) return false;
        for (const auto& p : metadata->partitions) if (p.topicId == topicId) return true;
        return false;
    }
};

constexpr auto kNotLeader    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr auto kUnknownTopic = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);

} // namespace

// Review focus #4 (spec §5.4): a producer that learned the topic from one broker
// and produces to a partition leader that has not applied it yet gets a
// retriable answer.  RED: UnknownTopic (non-retriable — the producer gives up on
// a topic that exists).
TEST(CatalogGatewayRegression, PlacedButNotYetAppliedTopicAnswersNotLeaderRegression) {
    GatewayRig g;
    g.placeWithoutApply(/*topicId=*/3, /*partitions=*/1);
    EXPECT_EQ(g.produce(3, 0).errorCode, kNotLeader);
    EXPECT_EQ(g.subscribeError(3, 0), pc::ErrorCode::NotLeader);
    EXPECT_EQ(g.gw.subscriptionCount(), 0u) << "nothing registered";
}

TEST(CatalogGatewayRegression, NeverPlacedUnknownTopicAnswersUnknownTopic) {
    GatewayRig g;
    EXPECT_EQ(g.produce(9, 0).errorCode, kUnknownTopic);
    EXPECT_EQ(g.subscribeError(9, 0), pc::ErrorCode::UnknownTopic);
}

// RED: an applied topic serving before its placement committed (an append the
// controller never placed — a second leader could accept the same partition).
TEST(CatalogGatewayRegression, AppliedButNotPlacedAnswersNotLeader) {
    GatewayRig g;
    g.applyWithoutPlacement(2, 1);
    EXPECT_EQ(g.produce(2, 0).errorCode, kNotLeader);
    EXPECT_EQ(g.registry->log(2, 0)->logEndOffset(), 0) << "nothing appended";
    EXPECT_EQ(g.subscribeError(2, 0), pc::ErrorCode::NotLeader);
    EXPECT_FALSE(g.listed(2)) << "an unplaced topic is not in metadata";
}

TEST(CatalogGatewayRegression, AppliedAndPlacedServes) {
    GatewayRig g;
    g.applyWithoutPlacement(2, 1);
    g.place(2, 1);
    EXPECT_EQ(g.produce(2, 0).errorCode, 0);
    EXPECT_EQ(g.registry->log(2, 0)->logEndOffset(), 1);
    EXPECT_TRUE(g.listed(2));
    const int64_t s = g.subscribe(2, 0);
    ASSERT_TRUE(g.codeOf(s).has_value());
    EXPECT_EQ(*g.codeOf(s), 0);
    // The ids below the catalog id are Gaps (not seen here): unknown, fillable.
    EXPECT_EQ(g.registry->state(0), TopicRegistry::TopicState::Unknown);
    EXPECT_EQ(g.registry->state(1), TopicRegistry::TopicState::Unknown);
    EXPECT_EQ(g.produce(1, 0).errorCode, kUnknownTopic) << "never placed: unknown";
}

// RED: a dropped topic still served (its placement is in the image until the
// removal record commits), or its subscriptions left silently pushing nothing.
TEST(CatalogGatewayRegression, DroppedTopicAnswersUnknownTopicAndEndsItsSubscriptionsRegression) {
    GatewayRig g;
    g.applyWithoutPlacement(4, 2);
    g.place(4, 2);
    const int64_t s0 = g.subscribe(4, 0);
    const int64_t s1 = g.subscribe(4, 1);
    ASSERT_EQ(g.codeOf(s0).value_or(-1), 0);
    ASSERT_EQ(g.codeOf(s1).value_or(-1), 0);
    ASSERT_EQ(g.gw.subscriptionCount(), 2u);

    ASSERT_TRUE(g.registry->dropTopic(4));
    EXPECT_EQ(g.gw.endTopicSubscriptions(4), 2u);
    EXPECT_EQ(g.gw.subscriptionCount(), 0u);
    int ended = 0;
    for (const auto& e : g.errors)
        if ((e.correlationId == s0 || e.correlationId == s1) && e.code == kUnknownTopic) ++ended;
    EXPECT_EQ(ended, 2) << "each subscription answered UnknownTopic on its Subscribe correlation id";
    EXPECT_EQ(g.gw.endTopicSubscriptions(4), 0u) << "idempotent";

    EXPECT_EQ(g.produce(4, 0).errorCode, kUnknownTopic) << "still placed in the image, but dropped";
    EXPECT_EQ(g.subscribeError(4, 1), pc::ErrorCode::UnknownTopic);
    EXPECT_FALSE(g.listed(4));
    EXPECT_FALSE(g.registry->dropTopic(4)) << "the same drop again is a no-op";
    EXPECT_EQ(g.registry->createTopicAt(4, rune::node::TopicSpec{"t4", 2}), -1)
        << "a dropped catalog id is never re-created";
}

// The registry half of C1 generalized: createTopicAt is idempotent for the same
// id + name, refuses an id holding another name and a name held by a live id.
TEST(CatalogGatewayRegression, CreateTopicAtIsIdempotentAndRefusesConflicts) {
    GatewayRig g;
    ASSERT_EQ(g.apply(5, 1, "orders"), 5);
    EXPECT_EQ(g.apply(5, 1, "orders"), 5) << "the same effect again";
    EXPECT_EQ(g.registry->topicCount(), 6) << "never a duplicate entry";
    EXPECT_EQ(g.apply(5, 1, "other"), -1) << "an id holding another name";
    EXPECT_EQ(g.apply(6, 1, "orders"), -1) << "a name held by a live id";
    ASSERT_TRUE(g.registry->dropTopic(5));
    EXPECT_EQ(g.apply(6, 1, "orders"), 6) << "a dropped name is free for a NEW id";
    EXPECT_EQ(g.registry->topicIdForName("orders"), 6);
    EXPECT_TRUE(g.registry->releaseDroppedLogs(5));
    EXPECT_FALSE(g.registry->releaseDroppedLogs(5)) << "idempotent";
}

// A dropped topic whose (not yet removed) placement names ANOTHER leader is
// UnknownTopic here at once — never a Redirect to a leader for a topic this
// broker already dropped.  RED: Redirect (the client chases a dropped topic).
TEST(CatalogGatewayRegression, DroppedTopicPlacedElsewhereIsUnknownTopicNotRedirectRegression) {
    GatewayRig g;
    ASSERT_EQ(g.apply(6, 1, "t6"), 6);
    g.placements.push_back({rune::metadata::gpidOf(6, 0), /*leader=*/2, 1});
    g.publish();
    const auto before = g.produce(6, 0).errorCode;
    EXPECT_EQ(before, kNotLeader) << "non-vacuous: a live topic led elsewhere is redirected (no known endpoint "
                                     "in this rig: the retriable NotLeader)";
    ASSERT_TRUE(g.registry->dropTopic(6));
    EXPECT_EQ(g.produce(6, 0).errorCode, kUnknownTopic);
    EXPECT_EQ(g.subscribeError(6, 0), pc::ErrorCode::UnknownTopic);
}

// Fix round 1 (C1): a never-seen id below a created one is a GAP — fillable by
// its own (later-applied) create — never a Dropped tombstone; an id whose create
// waits behind a drop is PENDING and answers the retriable NotLeader.  RED: the
// gap was a Dropped tombstone — createTopicAt(1) refused forever, id 1 answered
// the non-retriable UnknownTopic.
TEST(CatalogGatewayRegression, GapIdIsFillableAndAPendingIdAnswersNotLeaderRegression) {
    GatewayRig g;
    ASSERT_EQ(g.apply(3, 1, "u"), 3);                          // ids 0..2 become Gaps
    EXPECT_EQ(g.registry->state(1), TopicRegistry::TopicState::Unknown);
    ASSERT_TRUE(g.registry->reserveTopicAt(1, "t"));            // applied, behind a drop
    EXPECT_TRUE(g.registry->reserveTopicAt(1, "t")) << "idempotent";
    EXPECT_FALSE(g.registry->reserveTopicAt(1, "x")) << "reserved for another name";
    EXPECT_EQ(g.registry->state(1), TopicRegistry::TopicState::Pending);
    EXPECT_EQ(g.produce(1, 0).errorCode, kNotLeader) << "a queued id is retriable, never UnknownTopic";
    EXPECT_EQ(g.subscribeError(1, 0), pc::ErrorCode::NotLeader);
    EXPECT_EQ(g.viewRowStatus(1), pc::ViewRowResponse::kNotReady);
    EXPECT_EQ(g.apply(1, 1, "x"), -1) << "a Pending slot is filled only by its own create";
    ASSERT_EQ(g.apply(1, 1, "t"), 1) << "the waiting create fills its reserved slot";
    ASSERT_EQ(g.apply(2, 1, "late"), 2) << "a Gap is filled by its create";
    g.place(1, 1);
    g.place(2, 1);
    EXPECT_EQ(g.produce(1, 0).errorCode, 0);
    EXPECT_EQ(g.produce(2, 0).errorCode, 0);
    EXPECT_EQ(g.registry->topicCount(), 4);
    ASSERT_TRUE(g.registry->dropTopic(0)) << "a Gap dropped (seen now)";
    EXPECT_EQ(g.apply(0, 1, "zero"), -1) << "a dropped id is never refilled";
}

// Fix round 1 (I1): only the image built from the COMMITTED assignment map names
// committed placements.  After the last user topic is dropped the runtime
// publishes the STATIC-balancer image (committed metadata stays "ready"): a new
// topic in it must not serve before its placement commits.  RED: it served on
// the static image (produce OK, listed) — a placement nobody committed.
TEST(CatalogGatewayRegression, StaticImageIsNeverACommittedPlacementRegression) {
    GatewayRig g;
    g.applyWithoutPlacement(1, 1);
    g.committedImage = false;   // the static image names a leader for the new topic
    g.place(1, 1);
    EXPECT_EQ(g.produce(1, 0).errorCode, kNotLeader);
    EXPECT_EQ(g.subscribeError(1, 0), pc::ErrorCode::NotLeader);
    EXPECT_FALSE(g.listed(1));
    EXPECT_EQ(g.registry->log(1, 0)->logEndOffset(), 0) << "nothing appended";
    g.committedImage = true;    // the placement committed
    g.publish();
    EXPECT_EQ(g.produce(1, 0).errorCode, 0);
    EXPECT_TRUE(g.listed(1));
}

// Fix round 1 (I5): view reads get the same C1 gate.  RED: the worker's
// permanent UnknownView for a view another broker already serves (placed, not
// applied here), and a dropped view forwarded to the worker.
TEST(CatalogGatewayRegression, ViewReadOfAnUnappliedViewIsNotReadyAndOfADroppedViewUnknownViewRegression) {
    GatewayRig g;
    g.placeWithoutApply(4, 1);
    EXPECT_EQ(g.viewRowStatus(4), pc::ViewRowResponse::kNotReady) << "placed, not applied here: retriable";
    g.applyWithoutPlacement(6, 1);
    EXPECT_EQ(g.viewRowStatus(6), pc::ViewRowResponse::kNotReady) << "applied, not placed: retriable";
    ASSERT_TRUE(g.registry->dropTopic(6));
    EXPECT_EQ(g.viewRowStatus(6), pc::ViewRowResponse::kUnknownView) << "dropped: permanent";
    EXPECT_EQ(g.viewRowStatus(9), pc::ViewRowResponse::kUnknownView) << "never placed, never applied";
}

// Fix round 1 (I3): a dropped topic's GROUP-created subscriptions go with it —
// erased from the member's tracking, never answered on correlation id 0 (a
// request the member never issued); the member learns the drop from its group
// (GroupCoordinationService::onTopicDropped rebalances it away).  RED: an
// ErrorResponse on correlation 0, or the member still tracking the dead subId.
TEST(CatalogGatewayRegression, DroppedTopicsGroupSubscriptionsLeaveTheMemberTrackingRegression) {
    GatewayRig g;
    g.applyWithoutPlacement(4, 1);
    g.place(4, 1);
    ASSERT_GT(g.gw.addGroupSubscription(g.sid, 4, 0, 0, 0), 0);
    ASSERT_EQ(g.gw.groupMemberSubCount(g.sid), 1u);
    const auto errorsBefore = g.errors.size();
    ASSERT_TRUE(g.registry->dropTopic(4));
    EXPECT_EQ(g.gw.endTopicSubscriptions(4), 1u);
    EXPECT_EQ(g.gw.groupMemberSubCount(g.sid), 0u) << "the member no longer tracks the dead subscription";
    EXPECT_EQ(g.gw.subscriptionCount(), 0u);
    for (std::size_t i = errorsBefore; i < g.errors.size(); ++i)
        EXPECT_NE(g.errors[i].correlationId, 0) << "never an answer to a request the member did not issue";
}

// Fix round 1 (M5): the gateway forgets a dropped topic's Logs — a freed Log's
// address can be reused by a new Log, which must never inherit the dropped one's
// worker-owned / dirty membership.  RED: the stale Log* stays in the set.
TEST(CatalogGatewayRegression, DroppedTopicsLogsLeaveTheGatewaysLogSetsRegression) {
    GatewayRig g;
    g.applyWithoutPlacement(4, 2);
    rune::storage::Log* logs[] = {g.registry->log(4, 0), g.registry->log(4, 1)};
    g.gw.addWorkerOwnedLog(logs[0]);
    g.gw.addWorkerOwnedLog(logs[1]);
    ASSERT_EQ(g.gw.workerOwnedLogCountForTest(), 2u);
    ASSERT_TRUE(g.registry->dropTopic(4));
    g.gw.forgetDroppedTopic(4, 2, logs);
    EXPECT_EQ(g.gw.workerOwnedLogCountForTest(), 0u);
}
