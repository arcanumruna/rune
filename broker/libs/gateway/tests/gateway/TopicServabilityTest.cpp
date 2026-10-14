// TopicServabilityTest.cpp — DDL conformance final review C1 at the gateway,
// catalog form (catalog history Task 9 port: the old runtime path's "uncommitted"
// precondition is now "applied but not placed").
//
// A catalog topic applied here (TopicRegistry::createTopicAt) whose placement is
// not committed yet is neither writable nor readable: Produce is answered
// NotLeader (retriable; nothing appended), Subscribe is answered NotLeader
// (nothing registered), Metadata does not list it.  Once placed (the committed
// routing image names it) it serves like any boot topic.  An unknown topic stays
// UnknownTopic.
//
// Deterministic: no Aeron, no sleeps — makeForTest + the apply*ForTest seams.
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
        path = fs::temp_directory_path() / ("rune-servability-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct Rig {
    TempDir                        dir;
    std::unique_ptr<TopicRegistry> registry;
    ClientGateway                  gw;
    int64_t                        sid = 0;
    int64_t                        corr = 100;
    std::vector<pc::ProduceResponse>   produceAnswers;
    std::vector<pc::SubscribeResponse> subscribeAnswers;
    std::vector<pc::ErrorResponse>     errors;
    std::optional<pc::MetadataResponse::Decoded> metadata;
    int32_t runtimeId = -1;

    // Produce answers leave through the makeForTest egress fn; subscribe /
    // metadata answers through onEgressFrame (null-egress test session).
    Rig() : gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
                   onFrame(b, n);
                   return 1;
               }, 1)) {
        rune::node::TopicSpec boot;
        boot.name = "events"; boot.partitionCount = 1; boot.topicId = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{boot}, dir.path.string());
        rune::node::TopicSpec rt;
        rt.name = "runtime"; rt.partitionCount = 1; rt.topicId = 1;
        runtimeId = registry->createTopicAt(1, rt);   // applied at its catalog id, not placed yet
        gw.setTopicRegistry(registry.get());
        gw.setCommittedMetadataReady(true);
        std::vector<rune::metadata::RoutingImage::BuildEntry> img = {{rune::metadata::gpidOf(0, 0), 1, 1}};
        gw.setRoutingImageSnapshot(
            rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        gw.setNodeIdForTest(1);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        sid = gw.injectTestSession();
    }

    /// The placement commits: the committed routing image names the topic's leader.
    void place() {
        std::vector<rune::metadata::RoutingImage::BuildEntry> img = {
            {rune::metadata::gpidOf(0, 0), 1, 1}, {rune::metadata::gpidOf(runtimeId, 0), 1, 1}};
        gw.setRoutingImageSnapshot(
            rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
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
            default: break;
        }
    }

    /// The code answering correlation `c` (ProduceResponse / SubscribeResponse /
    /// ErrorResponse), or nullopt when unanswered.
    std::optional<int16_t> codeOf(int64_t c) const {
        for (const auto& r : produceAnswers) if (r.correlationId == c) return r.errorCode;
        for (const auto& r : subscribeAnswers) if (r.correlationId == c) return r.errorCode;
        for (const auto& e : errors) if (e.correlationId == c) return e.code;
        return std::nullopt;
    }
    int64_t produce(int32_t topicId) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"v"}, 0);
        const std::vector<uint8_t> batch = b.build();
        pc::Produce m;
        m.sessionId = sid; m.correlationId = ++corr; m.topicId = topicId; m.partition = 0;
        m.partitionKnown = 1; m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        gw.applyProduceForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    int64_t subscribe(int32_t topicId) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = ++corr; s.topicId = topicId; s.partition = 0;
        s.partitionKnown = 1; s.fromOffset = 0; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
        return s.correlationId;
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

constexpr int16_t kNotLeader    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kUnknownTopic = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);

} // namespace

TEST(TopicServabilityRegression, AppliedButNotPlacedTopicRefusesProduceRetriablyRegression) {
    Rig r;
    ASSERT_TRUE(r.registry->placementGated(r.runtimeId));
    const int64_t c = r.produce(r.runtimeId);
    ASSERT_TRUE(r.codeOf(c).has_value()) << "answered, never silent";
    EXPECT_EQ(*r.codeOf(c), kNotLeader) << "retriable";
    rune::storage::Log* const log = r.registry->log(r.runtimeId, 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 0) << "nothing appended";
    // Fix round 1 (Minor 4): the refusal is the DATA PLANE's placement gate, not a
    // missing log — the very log the produce would have written exists, and the
    // same produce once placed lands in it (0 -> 1).
    EXPECT_FALSE(r.gw.topicAssignable(r.runtimeId)) << "the gateway's gate: not placed";
    r.place();
    const int64_t ok = r.produce(r.runtimeId);
    ASSERT_TRUE(r.codeOf(ok).has_value());
    EXPECT_EQ(*r.codeOf(ok), 0);
    EXPECT_EQ(r.registry->log(r.runtimeId, 0), log) << "the same log";
    EXPECT_EQ(log->logEndOffset(), 1) << "only the placed produce appended";
}

// Fix round 1 (Minor 1): topicAssignable is the gate group rebalances assign by —
// applied-but-not-placed is not assignable (its partitions would be refused
// NotPlaced); placed and static topics are; an unknown id is not.
TEST(TopicServabilityRegression, OnlyPlacedOrStaticTopicsAreAssignableRegression) {
    Rig r;
    EXPECT_TRUE(r.gw.topicAssignable(0)) << "a static topic";
    EXPECT_FALSE(r.gw.topicAssignable(r.runtimeId)) << "applied, not placed";
    EXPECT_FALSE(r.gw.topicAssignable(99)) << "unknown";
    r.place();
    EXPECT_TRUE(r.gw.topicAssignable(r.runtimeId)) << "placed";
}

TEST(TopicServabilityRegression, AppliedButNotPlacedTopicRefusesSubscribeRetriablyRegression) {
    Rig r;
    const int64_t c = r.subscribe(r.runtimeId);
    ASSERT_TRUE(r.codeOf(c).has_value()) << "answered, never silent";
    EXPECT_EQ(*r.codeOf(c), kNotLeader) << "retriable";
    EXPECT_TRUE(r.subscribeAnswers.empty()) << "no subscription was registered";
}

TEST(TopicServabilityRegression, AppliedButNotPlacedTopicIsHiddenFromMetadataRegression) {
    Rig r;
    EXPECT_TRUE(r.listed(0)) << "a boot topic is listed";
    EXPECT_FALSE(r.listed(r.runtimeId)) << "a topic whose placement is not committed is not";
}

TEST(TopicServabilityRegression, PlacedCatalogTopicServesLikeABootTopicRegression) {
    Rig r;
    r.place();
    EXPECT_EQ(r.registry->createTopicAt(r.runtimeId, *r.registry->topic(r.runtimeId)), r.runtimeId) << "idempotent";
    EXPECT_TRUE(r.listed(r.runtimeId));
    const int64_t p = r.produce(r.runtimeId);
    ASSERT_TRUE(r.codeOf(p).has_value());
    EXPECT_EQ(*r.codeOf(p), 0);
    EXPECT_EQ(r.registry->log(r.runtimeId, 0)->logEndOffset(), 1);
    const int64_t s = r.subscribe(r.runtimeId);
    ASSERT_TRUE(r.codeOf(s).has_value());
    EXPECT_EQ(*r.codeOf(s), 0);
}

TEST(TopicServabilityRegression, BootTopicsServeAndUnknownTopicsStayUnknownRegression) {
    Rig r;
    EXPECT_FALSE(r.registry->placementGated(0)) << "static topics serve from boot";
    EXPECT_EQ(r.registry->state(99), TopicRegistry::TopicState::Unknown);
    const int64_t c = r.produce(99);
    ASSERT_TRUE(r.codeOf(c).has_value());
    EXPECT_EQ(*r.codeOf(c), kUnknownTopic) << "an unknown topic is not retriable";
    const int64_t ok = r.produce(0);
    ASSERT_TRUE(r.codeOf(ok).has_value());
    EXPECT_EQ(*r.codeOf(ok), 0);
}
