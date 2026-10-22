// AuthUnappliedTopicTest.cpp - issue #127: an id this broker has placed (committed routing image) but whose catalog
// event it has not applied yet (not in its ObjectIndex) is a CREATE / GRANT racing the apply.  Every id-addressed
// request answers it the retriable NotLeader, never the terminal NotAuthorized (45); an id neither applied nor placed
// keeps the D22 denial (a hidden and a missing id answer alike).
//
// Deterministic: no Aeron, no sleeps - makeForTest + the apply*ForTest seams.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/security/Permissions.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc  = rune::protocol::client;
namespace sec = rune::security;
namespace fs  = std::filesystem;

namespace {

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-auth-unapplied-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kPlaced  = 7;    // placed here, catalog event not applied here yet
constexpr int32_t kMissing = 99;   // neither
constexpr int16_t kNotLeader     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kNotAuthorized = static_cast<int16_t>(pc::ErrorCode::NotAuthorized);

struct Rig {
    TempDir                        dir;
    std::unique_ptr<TopicRegistry> registry;
    rune::node::WorkerPool         pool;   // GetViewRow is authorized only on a gateway with workers
    ClientGateway                  gw;
    int64_t                        sid = 0;
    int64_t                        corr = 100;
    std::vector<std::pair<int64_t, int16_t>> answers;   // (correlation, code)

    Rig() : pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16)
          , gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t { onFrame(b, n); return 1; }, 1)) {
        rune::node::TopicSpec boot;
        boot.name = "w.boot"; boot.partitionCount = 1; boot.topicId = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{boot}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setWorkerPool(&pool);
        gw.setCommittedMetadataReady(true);
        std::vector<rune::metadata::RoutingImage::BuildEntry> img = {{rune::metadata::gpidOf(0, 0), 1, 1},
                                                                      {rune::metadata::gpidOf(kPlaced, 0), 1, 1}};
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        gw.setNodeIdForTest(1);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.onCatalogObjectApplied(0, "w.boot", false);
        sid = gw.injectTestSession();
        // WRITE / READ on every topic named "w.*" - the topic at kPlaced will be "w.z" once applied here.
        gw.authenticateWithGrantsForTest(sid, 42, {
            sec::GrantSpec{sec::Operation::Read, sec::ResourcePattern{sec::ResourceType::Topic, sec::PatternKind::Prefix, "w."}},
            sec::GrantSpec{sec::Operation::Write, sec::ResourcePattern{sec::ResourceType::Topic, sec::PatternKind::Prefix, "w."}},
            sec::GrantSpec{sec::Operation::Read, sec::ResourcePattern{sec::ResourceType::View, sec::PatternKind::Prefix, "w."}}});
    }

    void onFrame(const uint8_t* b, std::size_t n) {
        const auto h = pc::MessageHeader::decode(b, n);
        if (!h) return;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
            case pc::TemplateId::ProduceResponse:
                if (auto r = pc::ProduceResponse::decode(b, n)) answers.emplace_back(r->correlationId, r->errorCode);
                break;
            case pc::TemplateId::SubscribeResponse:
                if (auto r = pc::SubscribeResponse::decode(b, n)) answers.emplace_back(r->correlationId, r->errorCode);
                break;
            case pc::TemplateId::ViewRowResponse:
                if (auto r = pc::ViewRowResponse::decode(b, n)) answers.emplace_back(r->correlationId, static_cast<int16_t>(r->status));
                break;
            case pc::TemplateId::ErrorResponse:
                if (auto r = pc::ErrorResponse::decode(b, n)) answers.emplace_back(r->correlationId, r->code);
                break;
            default: break;
        }
    }
    std::optional<int16_t> codeOf(int64_t c) const {
        for (const auto& [k, v] : answers) if (k == c) return v;
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
    int64_t getViewRow(int32_t viewId) {
        pc::GetViewRow m;
        m.sessionId = sid; m.correlationId = ++corr; m.viewTopicId = viewId; m.keyU64 = 1;
        std::vector<uint8_t> buf(64);
        buf.resize(m.encode(buf.data()));
        gw.applyGetViewRowForTest(buf.data(), buf.size());
        return m.correlationId;
    }
};

} // namespace

TEST(AuthUnappliedTopicRegression, ATopicPlacedButNotAppliedHereIsRetriableNotUnauthorizedRegression) {
    Rig r;
    ASSERT_EQ(r.gw.objectIndex().at(kPlaced), nullptr) << "precondition: not applied here";
    for (const auto& [what, corr] : {std::pair<const char*, int64_t>{"Produce", r.produce(kPlaced)},
                                     {"Subscribe", r.subscribe(kPlaced)},
                                     {"GetViewRow", r.getViewRow(kPlaced)}}) {
        const auto code = r.codeOf(corr);
        ASSERT_TRUE(code.has_value()) << what << ": answered, never silent";
        EXPECT_EQ(*code, kNotLeader) << what << ": retriable (NotLeader; for GetViewRow its NotReady, also 2) while the catalog apply is in flight, never terminal 45";
    }
}

TEST(AuthUnappliedTopicRegression, AnIdNeitherAppliedNorPlacedKeepsTheD22DenialRegression) {
    Rig r;
    for (const auto& [what, corr] : {std::pair<const char*, int64_t>{"Produce", r.produce(kMissing)},
                                     {"Subscribe", r.subscribe(kMissing)},
                                     {"GetViewRow", r.getViewRow(kMissing)}}) {
        const auto code = r.codeOf(corr);
        ASSERT_TRUE(code.has_value()) << what;
        EXPECT_EQ(*code, kNotAuthorized) << what << ": a missing id answers like a hidden one (D22)";
    }
}

TEST(AuthUnappliedTopicRegression, OnceAppliedAnUngrantedPlacedTopicIsDeniedTerminallyRegression) {
    Rig r;
    r.gw.onCatalogObjectApplied(kPlaced, "secret", false);   // applied here now; the user's "w." grants miss it
    const auto code = r.codeOf(r.produce(kPlaced));
    ASSERT_TRUE(code.has_value());
    EXPECT_EQ(*code, kNotAuthorized) << "only the not-yet-applied window is retriable";
}
