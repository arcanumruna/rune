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
#include <rune/gateway/SecurityView.h>
#include <rune/gateway/SessionAuth.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <map>
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

/// Task 11: an admin sink that only counts what reached it (every DDL Accepted).
struct CountingAdminSink final : IAdminRequestSink {
    std::size_t submits = 0;
    std::size_t gets    = 0;
    AdminSubmit submitDefineTopic(int64_t, int64_t, std::string_view, uint32_t, std::string_view, bool,
                                  const uint8_t*, uint32_t, uint8_t, int16_t, const rune::protocol::TopicConfigView&,
                                  const DdlMetaView&, std::string_view) override { ++submits; return AdminSubmit::Accepted; }
    bool submitGetSchema(int64_t, int64_t, std::string_view, uint64_t) override { ++gets; return true; }
    AdminSubmit submitAlterTopic(int64_t, int64_t, std::string_view, uint16_t, const uint8_t*, uint32_t,
                                 const DdlMetaView&) override { ++submits; return AdminSubmit::Accepted; }
    AdminSubmit submitCreateView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, const uint8_t*,
                                 uint32_t, const DdlMetaView&) override { ++submits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropTopic(int64_t, int64_t, std::string_view, const DdlMetaView&) override {
        ++submits; return AdminSubmit::Accepted;
    }
    AdminSubmit submitAlterTopicConfig(int64_t, int64_t, std::string_view, uint32_t, std::span<const uint8_t>,
                                       const DdlMetaView&) override { ++submits; return AdminSubmit::Accepted; }
    AdminSubmit submitDropView(int64_t, int64_t, std::string_view, const DdlMetaView&) override {
        ++submits; return AdminSubmit::Accepted;
    }
    AdminSubmit submitAlterView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, uint32_t,
                                const uint8_t*, uint32_t, const DdlMetaView&) override {
        ++submits; return AdminSubmit::Accepted;
    }
    // Task 12: catalog reads.  `ringFull` makes every catalog submit refuse (back-pressure).
    struct CatalogSub { int64_t session, corr, from; };
    struct CatalogUnsub { int64_t session, subscriptionId; };
    std::vector<CatalogSub>   catalogSubs;
    std::vector<CatalogUnsub> catalogUnsubs;
    bool ringFull = false;
    bool submitCatalogSubscribe(int64_t s, int64_t c, int64_t from) override {
        if (ringFull) return false;
        catalogSubs.push_back({s, c, from});
        return true;
    }
    bool submitCatalogUnsubscribe(int64_t s, int64_t id) override {
        if (ringFull) return false;
        catalogUnsubs.push_back({s, id});
        return true;
    }
    AdminSubmit submitSecurityDdl(const pc::SecurityDdl&, const DdlMetaView&, std::string_view, bool) override { return AdminSubmit::Accepted; }
    AdminSubmit submitSecurityBootstrap(const pc::SecurityBootstrap&, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    bool submitSecuritySubscribe(int64_t, int64_t, int64_t) override { return true; }
    std::vector<CatalogUnsub> securityUnsubs;
    bool submitSecurityUnsubscribe(int64_t s, int64_t id) override {
        if (ringFull) return false;
        securityUnsubs.push_back({s, id});
        return true;
    }
};

/// One DDL answer as the client sees it: the frame's template, its code and message.
struct DdlAnswer {
    uint16_t      responseTemplate = 0;
    pc::ErrorCode code             = pc::ErrorCode::OK;
    std::string   message;
    bool          answered         = false;
};

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

    CountingAdminSink admin;
    std::map<int64_t, DdlAnswer> ddlAnswers;   // by correlation id
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> logSink;
    std::shared_ptr<spdlog::logger> previousLogger;

    int32_t clientVersion_ = pc::kClientVersion;
    explicit GatewayRig(int32_t clientVersion = pc::kClientVersion)
        : gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t {
              onFrame(b, n);
              return 1;
          }, 1)) {
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setNodeIdForTest(kSelf);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.setAdminSink(&admin);
        publish();
        clientVersion_ = clientVersion;
        sid = gw.injectTestSession(0, clientVersion);
        // Task 11: the gateway's warn lines are captured (F37 names its reason only in the log).
        logSink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
        previousLogger = spdlog::default_logger();
        auto capture = std::make_shared<spdlog::logger>("catalog-gw-capture", logSink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~GatewayRig() { spdlog::set_default_logger(previousLogger); }
    ClientGateway& gateway() noexcept { return gw; }
    [[nodiscard]] std::size_t adminSubmits() const noexcept { return admin.submits; }
    [[nodiscard]] bool warnLogContains(const std::string& text) const {
        for (const std::string& l : logSink->last_formatted())
            if (l.find("warn") != std::string::npos && l.find(text) != std::string::npos) return true;
        return false;
    }

    /// Every DDL template (64, 68, 70, 74, 75, 76, 77).
    static std::vector<pc::TemplateId> allDdlTemplates() {
        return {pc::TemplateId::DefineTopic, pc::TemplateId::AlterTopic, pc::TemplateId::CreateView,
                pc::TemplateId::DropTopic, pc::TemplateId::AlterTopicConfig, pc::TemplateId::DropView,
                pc::TemplateId::AlterView};
    }
    static uint16_t ownResponseTemplate(pc::TemplateId t) {
        switch (t) {
        case pc::TemplateId::DefineTopic: return static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse);
        case pc::TemplateId::AlterTopic:  return static_cast<uint16_t>(pc::TemplateId::AlterTopicResponse);
        case pc::TemplateId::CreateView:  return static_cast<uint16_t>(pc::TemplateId::CreateViewResponse);
        default:                          return static_cast<uint16_t>(pc::TemplateId::DdlResponse);
        }
    }
    /// One well-formed DDL frame of `t` (v8 extension unless the session is pre-v8).
    std::vector<uint8_t> ddlFrame(pc::TemplateId t, int64_t c, bool withExt) {
        static const std::vector<uint8_t> blob{1, 2, 3};
        pc::DdlExtension ext;
        ext.clientName = "gw-test";
        ext.requestId  = {0x7E57, static_cast<uint64_t>(c)};
        std::vector<uint8_t> buf;
        auto enc = [&buf](const auto& m) { buf.resize(m.encodedSize()); buf.resize(m.encode(buf.data())); };
        switch (t) {
        case pc::TemplateId::DefineTopic: {
            pc::DefineTopic m; m.sessionId = sid; m.correlationId = c; m.topicName = "t"; m.partitionCount = 1;
            m.hasExt = withExt; m.ext = ext; enc(m); break;
        }
        case pc::TemplateId::AlterTopic: {
            pc::AlterTopic m; m.sessionId = sid; m.correlationId = c; m.topicName = "t";
            m.opsBlob = blob.data(); m.opsLen = static_cast<uint32_t>(blob.size());
            m.hasExt = withExt; m.ext = ext; enc(m); break;
        }
        case pc::TemplateId::CreateView: {
            pc::CreateView m; m.sessionId = sid; m.correlationId = c; m.viewName = "v"; m.keyField = "id";
            m.planBlob = blob.data(); m.planLen = static_cast<uint32_t>(blob.size());
            m.hasExt = withExt; m.ext = ext; enc(m); break;
        }
        case pc::TemplateId::DropTopic: {
            pc::DropTopic m; m.sessionId = sid; m.correlationId = c; m.topicName = "t"; m.ext = ext; enc(m); break;
        }
        case pc::TemplateId::AlterTopicConfig: {
            static std::vector<uint8_t> changes = [] {
                std::vector<uint8_t> b(pc::configChangesEncodedSize(1));
                const pc::ConfigChangeWire one{1, 60'000};
                pc::putConfigChanges(b.data(), std::span<const pc::ConfigChangeWire>(&one, 1));
                return b;
            }();
            pc::AlterTopicConfig m; m.sessionId = sid; m.correlationId = c; m.topicName = "t";
            m.changes = changes; m.ext = ext; enc(m); break;
        }
        case pc::TemplateId::DropView: {
            pc::DropView m; m.sessionId = sid; m.correlationId = c; m.viewName = "v"; m.ext = ext; enc(m); break;
        }
        case pc::TemplateId::AlterView: {
            pc::AlterView m; m.sessionId = sid; m.correlationId = c; m.viewName = "v"; m.keyField = "id";
            m.planBlob = blob.data(); m.planLen = static_cast<uint32_t>(blob.size()); m.ext = ext; enc(m); break;
        }
        default: break;
        }
        return buf;
    }
    /// A DDL of `t` through the real dispatch; its (one) answer.
    DdlAnswer sendDdl(pc::TemplateId t) {
        const int64_t c = ++corr;
        const bool v8 = pc::clientUnderstandsCatalogDdl(clientVersion_);
        const std::vector<uint8_t> f = ddlFrame(t, c, v8);
        EXPECT_FALSE(f.empty()) << "the test frame encodes";
        gw.applyDdlFrameForTest(f.data(), f.size());
        const auto it = ddlAnswers.find(c);
        return it == ddlAnswers.end() ? DdlAnswer{} : it->second;
    }
    struct SchemaRead { bool forwardedToAdmin = false; bool answered = false; };
    SchemaRead sendGetTopicSchema(const std::string& name) {
        pc::GetTopicSchema m;
        m.sessionId = sid; m.correlationId = ++corr; m.topicName = name;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        const std::size_t before = admin.gets;
        gw.applyGetTopicSchemaForTest(buf.data(), buf.size());
        return SchemaRead{admin.gets == before + 1, ddlAnswers.contains(m.correlationId)};
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
                if (auto r = pc::ErrorResponse::decode(b, n)) {
                    errors.push_back(*r);
                    noteDdl(r->correlationId, h->templateId, static_cast<uint16_t>(r->code), r->message);
                }
                break;
            case pc::TemplateId::DefineTopicResponse:
                if (auto r = pc::DefineTopicResponse::decode(b, n)) noteDdl(r->correlationId, h->templateId, r->errorCode, {});
                break;
            case pc::TemplateId::AlterTopicResponse:
                if (auto r = pc::AlterTopicResponse::decode(b, n)) noteDdl(r->correlationId, h->templateId, r->errorCode, {});
                break;
            case pc::TemplateId::CreateViewResponse:
                if (auto r = pc::CreateViewResponse::decode(b, n)) noteDdl(r->correlationId, h->templateId, r->errorCode, {});
                break;
            case pc::TemplateId::DdlResponse:
                if (auto r = pc::DdlResponse::decode(b, n)) noteDdl(r->correlationId, h->templateId, r->errorCode, r->detail);
                break;
            case pc::TemplateId::TopicSchemaResponse:
                if (auto r = pc::TopicSchemaResponse::decode(b, n)) noteDdl(r->correlationId, h->templateId, r->errorCode, {});
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
    void noteDdl(int64_t c, uint16_t tmpl, uint16_t code, std::string_view message) {
        DdlAnswer& a = ddlAnswers[c];
        EXPECT_FALSE(a.answered) << "corr " << c << " answered twice";
        a = DdlAnswer{tmpl, static_cast<pc::ErrorCode>(code), std::string(message), true};
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
    // Task 16 fix round 1 (R1): the dropped incarnation's dir (marker 5) is retired (renamed away) before the
    // name is re-created — the control thread's drop sequence; a create over it would be refused.
    EXPECT_EQ(g.apply(6, 1, "orders"), -1) << "the older incarnation's dir is still in place";
    std::filesystem::rename(g.dir.path / "topics" / "orders", g.dir.path / "retired-orders");
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

// ===========================================================================
// Catalog history Task 11: the DDL route (spec §5.1 Redirect, §5.5 NotLeader,
// §9 Internal when halted; F37 pre-v8 sessions).  The route is a control-thread
// value; the gateway answers a non-local DDL itself — it never reaches the admin.
// ===========================================================================

// Fix-on-sight (Task 11): a non-leader answered DDL Internal (non-retriable).
// RED: Internal / a submit to the admin ring.
TEST(CatalogGatewayRegression, NonLeaderRedirectsEveryDdlToTheCatalogLeader) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteRemote{"10.0.0.2:40200"});
    for (auto tmpl : g.allDdlTemplates()) {                    // 64, 68, 70, 74, 75, 76, 77
        const auto r = g.sendDdl(tmpl);
        ASSERT_TRUE(r.answered) << static_cast<int>(tmpl);
        EXPECT_EQ(r.responseTemplate, static_cast<uint16_t>(pc::TemplateId::ErrorResponse)) << static_cast<int>(tmpl);
        EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::Redirect) << static_cast<int>(tmpl);
        EXPECT_EQ(r.message, "10.0.0.2:40200");
        EXPECT_EQ(g.adminSubmits(), 0u);
    }
    EXPECT_EQ(g.gateway().ddlRoutedAway(), g.allDdlTemplates().size());
}

TEST(CatalogLeadershipRegression, HaltedBrokerAnswersDdlInternalNotRedirectRegression) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteHalted{41});
    for (auto tmpl : g.allDdlTemplates()) {
        const auto r = g.sendDdl(tmpl);
        ASSERT_TRUE(r.answered);
        EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::Internal) << static_cast<int>(tmpl);
        EXPECT_NE(r.message.find("offset 41"), std::string::npos) << r.message;
    }
    EXPECT_EQ(g.adminSubmits(), 0u) << "a halted broker accepts no catalog change";
}

TEST(CatalogGatewayRegression, UnknownCatalogLeaderIsNotLeader) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteUnknown{});
    for (auto tmpl : g.allDdlTemplates()) {
        const auto r = g.sendDdl(tmpl);
        ASSERT_TRUE(r.answered);
        EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::NotLeader) << static_cast<int>(tmpl);
    }
    EXPECT_EQ(g.adminSubmits(), 0u);
}

// A remote route whose endpoint is unknown (a peer without a client port) is the
// retriable NotLeader — never a Redirect to "".
TEST(CatalogGatewayRegression, RemoteLeaderWithoutAnEndpointIsNotLeader) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteRemote{""});
    EXPECT_EQ(g.sendDdl(pc::TemplateId::DropTopic).code, rune::protocol::client::ErrorCode::NotLeader);
    EXPECT_EQ(g.adminSubmits(), 0u);
}

TEST(CatalogGatewayRegression, LocalLeaderSubmitsEveryDdlToTheAdmin) {
    GatewayRig g;
    g.gateway().setCatalogRoute(rune::gateway::RouteLocalLeader{});
    for (auto tmpl : g.allDdlTemplates()) EXPECT_FALSE(g.sendDdl(tmpl).answered) << "the admin answers later";
    EXPECT_EQ(g.adminSubmits(), g.allDdlTemplates().size());
    EXPECT_EQ(g.gateway().ddlRoutedAway(), 0u);
}

TEST(CatalogGatewayRegression, SchemaReadsAreServedLocallyOnAFollower) {
    GatewayRig g;
    for (const rune::gateway::CatalogRoute& r :
         {rune::gateway::CatalogRoute{rune::gateway::RouteRemote{"x:1"}}, rune::gateway::CatalogRoute{rune::gateway::RouteUnknown{}},
          rune::gateway::CatalogRoute{rune::gateway::RouteHalted{3}}}) {
        g.gateway().setCatalogRoute(r);
        const auto read = g.sendGetTopicSchema("t");
        EXPECT_EQ(read.forwardedToAdmin, true) << "reads are never redirected (" << catalogRouteName(r) << ")";
        EXPECT_FALSE(read.answered) << "the admin answers it from the local projection";
    }
    EXPECT_EQ(g.adminSubmits(), 0u);
}

TEST(CatalogGatewayRegression, PreV8DdlToANonLeaderIsInvalidRequestNotARedirectRegression) {
    // F37: a v7 client cannot follow a DDL Redirect and waits out an ErrorResponse — answer it explicitly.
    GatewayRig g(/*clientVersion=*/7);
    g.gateway().setCatalogRoute(rune::gateway::RouteRemote{"10.0.0.2:40200"});
    for (auto tmpl : {rune::protocol::client::TemplateId::DefineTopic, rune::protocol::client::TemplateId::AlterTopic,
                      rune::protocol::client::TemplateId::CreateView}) {
        const auto r = g.sendDdl(tmpl);
        ASSERT_TRUE(r.answered);
        EXPECT_EQ(r.responseTemplate, g.ownResponseTemplate(tmpl)) << "its own response type, not ErrorResponse";
        EXPECT_EQ(r.code, rune::protocol::client::ErrorCode::InvalidRequest);
        EXPECT_EQ(g.adminSubmits(), 0u);
    }
    EXPECT_TRUE(g.warnLogContains("client too old for cluster DDL routing; upgrade"));
    EXPECT_EQ(g.gateway().preV8DdlRefused(), 3u);
}

// F37: a pre-v8 session on a halted broker gets Internal, with no known leader
// NotLeader — each in its own response type; on the local leader it is served.
TEST(CatalogGatewayRegression, PreV8DdlHaltedIsInternalAndUnknownIsNotLeaderInItsOwnType) {
    GatewayRig g(/*clientVersion=*/7);
    g.gateway().setCatalogRoute(rune::gateway::RouteHalted{9});
    auto r = g.sendDdl(pc::TemplateId::DefineTopic);
    EXPECT_EQ(r.responseTemplate, g.ownResponseTemplate(pc::TemplateId::DefineTopic));
    EXPECT_EQ(r.code, pc::ErrorCode::Internal);
    g.gateway().setCatalogRoute(rune::gateway::RouteUnknown{});
    r = g.sendDdl(pc::TemplateId::AlterTopic);
    EXPECT_EQ(r.responseTemplate, g.ownResponseTemplate(pc::TemplateId::AlterTopic));
    EXPECT_EQ(r.code, pc::ErrorCode::NotLeader);
    EXPECT_EQ(g.adminSubmits(), 0u);
    g.gateway().setCatalogRoute(rune::gateway::RouteLocalLeader{});
    EXPECT_FALSE(g.sendDdl(pc::TemplateId::CreateView).answered);
    EXPECT_EQ(g.adminSubmits(), 1u) << "on the leader a pre-v8 DDL is served as before";
    EXPECT_EQ(g.gateway().preV8DdlRefused(), 0u);
}

// ===========================================================================
// Catalog history Task 12 (spec §7, F19): `__catalog` (topic id −6) is readable
// with Subscribe on ANY broker.  The gateway never redirects / routes it: it is
// forwarded to the admin thread (the catalog replica's owner), which answers.
// ===========================================================================
namespace {
int64_t catalogSubscribe(GatewayRig& g, int32_t partition, uint8_t partitionKnown = 1, int64_t from = -2) {
    pc::Subscribe s;
    s.sessionId = g.sid; s.correlationId = ++g.corr; s.topicId = pc::kCatalogTopicId; s.partition = partition;
    s.partitionKnown = partitionKnown; s.fromOffset = from; s.isolation = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    g.gw.applySubscribeForTest(buf.data(), buf.size());
    return s.correlationId;
}
void unsubscribe(GatewayRig& g, int64_t subscriptionId) {
    pc::Unsubscribe u;
    u.sessionId = g.sid; u.correlationId = ++g.corr; u.subscriptionId = subscriptionId;
    std::vector<uint8_t> buf(pc::Unsubscribe::ENCODED_SIZE);
    u.encode(buf.data());
    g.gw.applyUnsubscribeForTest(buf.data(), buf.size());
}
}   // namespace

// RED (before Task 12): a −6 Subscribe went through the data-plane routing —
// Redirect / NotLeader on a non-leader, UnknownTopic everywhere (no −6 in the
// TopicRegistry) — so no broker served the catalog.
TEST(CatalogGatewayRegression, CatalogSubscribeIsForwardedOnAnyBrokerAndPartitionMustBeZero) {
    GatewayRig g;
    std::size_t forwarded = 0;
    for (const rune::gateway::CatalogRoute& route :
         {rune::gateway::CatalogRoute{rune::gateway::RouteRemote{"10.0.0.2:40200"}},
          rune::gateway::CatalogRoute{rune::gateway::RouteUnknown{}},
          rune::gateway::CatalogRoute{rune::gateway::RouteHalted{5}},
          rune::gateway::CatalogRoute{rune::gateway::RouteLocalLeader{}}}) {
        g.gateway().setCatalogRoute(route);
        const int64_t c = catalogSubscribe(g, /*partition=*/0, 1, /*from=*/7);
        ++forwarded;
        ASSERT_EQ(g.admin.catalogSubs.size(), forwarded) << catalogRouteName(route);
        EXPECT_EQ(g.admin.catalogSubs.back().corr, c);
        EXPECT_EQ(g.admin.catalogSubs.back().from, 7);
        EXPECT_EQ(g.admin.catalogSubs.back().session, g.sid);
        EXPECT_FALSE(g.codeOf(c).has_value()) << "the admin thread answers it (" << catalogRouteName(route) << ")";
    }
    EXPECT_EQ(g.gw.catalogSubscribesForwarded(), forwarded);
    EXPECT_EQ(g.gw.subscriptionCount(), 0u) << "never a gateway (data-plane) subscription";

    const int64_t bad = catalogSubscribe(g, /*partition=*/1);
    EXPECT_EQ(g.codeOf(bad).value_or(-1), static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    const int64_t keyed = catalogSubscribe(g, 0, /*partitionKnown=*/0);
    EXPECT_EQ(g.codeOf(keyed).value_or(-1), static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(g.admin.catalogSubs.size(), forwarded) << "refused ones are not forwarded";
}

TEST(CatalogGatewayRegression, PreV8CatalogSubscribeIsAnsweredUnknownTopic) {
    GatewayRig g(/*clientVersion=*/7);
    const int64_t c = catalogSubscribe(g, 0);
    EXPECT_EQ(g.codeOf(c).value_or(-1), kUnknownTopic) << "answered, as before Task 12 (it cannot parse the marker)";
    EXPECT_TRUE(g.admin.catalogSubs.empty());
}

TEST(CatalogGatewayRegression, CatalogSubscribeOnAFullAdminRingIsTheRetriableNotLeaderRegression) {
    GatewayRig g;
    g.admin.ringFull = true;
    const int64_t c = catalogSubscribe(g, 0);
    EXPECT_EQ(g.codeOf(c).value_or(-1), kNotLeader) << "back-pressure is answered, never a silent stall";
    EXPECT_EQ(g.gw.catalogSubscribesForwarded(), 0u);
}

// A catalog Unsubscribe and a session close reach the admin thread — retried
// while its ring is full, in order, never dropped (a lost close would hold a
// bounded read slot and keep pushing to a dead session).
TEST(CatalogGatewayRegression, CatalogUnsubscribeAndSessionCloseReachTheAdminEvenThroughAFullRingRegression) {
    GatewayRig g;
    const int64_t catalogSubId = rune::gateway::kCatalogSubscriptionIdBase | (int64_t{2} << 40) | 1;
    unsubscribe(g, catalogSubId);
    ASSERT_EQ(g.admin.catalogUnsubs.size(), 1u);
    EXPECT_EQ(g.admin.catalogUnsubs[0].subscriptionId, catalogSubId);
    EXPECT_EQ(g.admin.catalogUnsubs[0].session, g.sid);

    g.admin.ringFull = true;
    unsubscribe(g, catalogSubId + 1);
    g.gw.closeSessionForTest(g.sid);
    EXPECT_EQ(g.gw.pendingCatalogUnsubscribes(), 1u) << "fix round 1 (M3): the close supersedes the single one";
    (void)g.gw.pumpCatalogUnsubscribesForTest();   // the doWork retry step
    EXPECT_EQ(g.gw.pendingCatalogUnsubscribes(), 1u) << "still full: kept";
    g.admin.ringFull = false;
    (void)g.gw.pumpCatalogUnsubscribesForTest();
    EXPECT_EQ(g.gw.pendingCatalogUnsubscribes(), 0u);
    ASSERT_EQ(g.admin.catalogUnsubs.size(), 2u);
    EXPECT_EQ(g.admin.catalogUnsubs[1].subscriptionId, -1) << "the session close: every catalog subscription";
}

// A data-plane Unsubscribe is never sent to the admin thread.
TEST(CatalogGatewayRegression, DataPlaneUnsubscribeStaysInTheGateway) {
    GatewayRig g;
    g.applyWithoutPlacement(2, 1);
    g.place(2, 1);
    const int64_t c = g.subscribe(2, 0);
    ASSERT_EQ(g.codeOf(c).value_or(-1), 0);
    const int64_t subId = g.subscribeAnswers.back().subscriptionId;
    EXPECT_FALSE(rune::gateway::isCatalogSubscriptionId(subId));
    unsubscribe(g, subId);
    EXPECT_TRUE(g.admin.catalogUnsubs.empty());
    EXPECT_EQ(g.gw.subscriptionCount(), 0u);
}

// BINDING (Task 8 review N5): a view sink that is applied and placed here but
// whose plan has not reached every worker yet answers the retriable NotReady —
// never the worker's permanent UnknownView.  RED: the gate passed it (Serving +
// placed) and the worker, with no plan, answered UnknownView.
TEST(CatalogGatewayRegression, AppliedPlacedSinkWithoutItsPlanOnTheWorkersIsNotReadyRegression) {
    GatewayRig g;
    g.applyWithoutPlacement(5, 1);
    g.place(5, 1);
    g.gw.noteViewSinkAwaitingPlan(5);              // kA2cCatalogSinkApplied: the plan is still in flight
    EXPECT_EQ(g.viewRowStatus(5), pc::ViewRowResponse::kNotReady);
    g.gw.noteViewPlanDelivered(5);                 // on every worker's install ring
    EXPECT_EQ(g.viewRowStatus(5), pc::ViewRowResponse::kUnknownView)
        << "the gate lifts (this rig has no workers: their answer)";
    g.gw.noteViewSinkAwaitingPlan(6);
    ASSERT_TRUE(g.gw.viewSinkAwaitingPlan(6));
    ASSERT_EQ(g.apply(6, 1, "s6"), 6);
    ASSERT_TRUE(g.registry->dropTopic(6));
    rune::storage::Log* logs[] = {g.registry->log(6, 0)};
    g.gw.forgetDroppedTopic(6, 1, logs);
    EXPECT_FALSE(g.gw.viewSinkAwaitingPlan(6)) << "a dropped sink waits for nothing";
    EXPECT_EQ(g.viewRowStatus(6), pc::ViewRowResponse::kUnknownView);
}

// Fix round 1 (C2): the resume threshold is reachable by ANY receiver window —
// a fraction of the window actually observed, of the term — never a constant a
// small window can never reach (a paused reader would hang).  RED: 16 KiB for an
// 8 KiB window.
TEST(CatalogGatewayRegression, CatalogResumeThresholdFitsEveryReceiverWindowRegression) {
    using G = rune::gateway::ClientGateway;
    for (int64_t window : {int64_t{1408}, int64_t{4096}, int64_t{8192}, int64_t{12 * 1024}, int64_t{1} << 20}) {
        const int64_t t = G::catalogResumeThreshold(/*term=*/64 * 1024, window);
        EXPECT_LE(t, window) << "window " << window << ": a fully drained window always resumes";
        EXPECT_GE(t, 1);
        EXPECT_LE(t, G::kCatalogResumeWindowBytes);
    }
    EXPECT_LE(G::catalogResumeThreshold(/*term=*/16 * 1024, 0), 8 * 1024) << "bounded by the term too";
}

// Fix round 1 (M3): waiting catalog Unsubscribes stay bounded — a session close
// supersedes the session's waiting single unsubscribes, duplicates coalesce.
TEST(CatalogGatewayRegression, PendingCatalogUnsubscribesCoalesceRegression) {
    GatewayRig g;
    g.admin.ringFull = true;
    const int64_t base = rune::gateway::kCatalogSubscriptionIdBase | (int64_t{2} << 40);
    for (int i = 0; i < 50; ++i) unsubscribe(g, base | (1 + i % 5));   // 5 distinct, repeated
    EXPECT_EQ(g.gw.pendingCatalogUnsubscribes(), 5u) << "duplicates coalesce";
    g.gw.closeSessionForTest(g.sid);
    EXPECT_EQ(g.gw.pendingCatalogUnsubscribes(), 1u) << "the close supersedes them";
    g.admin.ringFull = false;
    (void)g.gw.pumpCatalogUnsubscribesForTest();
    ASSERT_EQ(g.admin.catalogUnsubs.size(), 1u);
    EXPECT_EQ(g.admin.catalogUnsubs[0].subscriptionId, -1);
}

// Fix round 1 (M4): a back-pressured catalog frame is never shown to the capture hook as sent.
TEST(CatalogGatewayRegression, BackPressuredCatalogFrameIsNotCapturedAsSent) {
    GatewayRig g;
    std::size_t captured = 0;
    g.gw.setOnControlFrame([&](int64_t, const uint8_t*, std::size_t) { ++captured; });
    const uint8_t frame[52] = {};
    g.gw.setCatalogBackPressureForTest(g.sid, true);
    EXPECT_EQ(g.gw.tryOfferCatalogFrame(g.sid, frame, sizeof frame), ClientGateway::CatalogOffer::BackPressured);
    EXPECT_EQ(captured, 0u);
    g.gw.setCatalogBackPressureForTest(g.sid, false);
    EXPECT_EQ(g.gw.tryOfferCatalogFrame(g.sid, frame, sizeof frame), ClientGateway::CatalogOffer::Sent);
    EXPECT_EQ(captured, 1u);
}

// Fix round 3 (F3b): more session closes than the gateway's fixed queue holds
// while the admin ring is stalled — the queue never grows; the overflow is
// counted and left to the admin's liveness sweep (which reclaims the slots —
// CatalogReadServerRegression.ClosesBeyondTheGatewayQueueAreReclaimedByTheLivenessSweepRegression).
TEST(CatalogGatewayRegression, SessionClosesBeyondTheQueueAreLeftToTheSweepNeverGrowingRegression) {
    GatewayRig g;
    g.admin.ringFull = true;
    std::vector<int64_t> sids;
    for (int i = 0; i < 1100; ++i) sids.push_back(g.gw.injectTestSession(0, pc::kClientVersion));
    for (int64_t s : sids) g.gw.closeSessionForTest(s);
    EXPECT_EQ(g.gw.pendingCatalogUnsubscribes(), 1024u) << "fixed capacity";
    EXPECT_EQ(g.gw.catalogClosesLeftToTheSweep(), 1100u - 1024u);
    g.admin.ringFull = false;
    (void)g.gw.pumpCatalogUnsubscribesForTest();
    EXPECT_EQ(g.admin.catalogUnsubs.size(), 1024u) << "every queued close is delivered";
}

// Issue #117 (X017 part, M10): a live permission loss's end of a session's `__security` / `__catalog` reads (-3 / -2) is
// never dropped - not even with the admin ring full and the waiting-unsubscribe queue at its cap (it used to be: "it ends
// with the session", so a session that lost ADMIN kept streaming every new verifier until it disconnected).  RED before
// the fix: the -3 was dropped at the cap and never reached the admin.
TEST(AuthLiveChangeRegression, SecurityReadEndNeverDroppedUnderAdminRingPressureRegression) {
    GatewayRig g;
    g.admin.ringFull = true;
    std::vector<int64_t> others;
    for (int i = 0; i < 1100; ++i) others.push_back(g.gw.injectTestSession(0, pc::kClientVersion));
    for (int64_t s : others) g.gw.closeSessionForTest(s);
    ASSERT_EQ(g.gw.pendingCatalogUnsubscribes(), 1024u) << "the waiting queue is at its cap";
    g.gw.forwardCatalogUnsubscribeForTest(g.sid, rune::gateway::kEndSecurityReadsOnly);
    g.gw.forwardCatalogUnsubscribeForTest(g.sid, rune::gateway::kEndCatalogReadsOnly);
    g.gw.forwardCatalogUnsubscribeForTest(g.sid, rune::gateway::kEndSecurityReadsOnly);   // coalesces
    for (int pass = 0; pass < 3; ++pass) (void)g.gw.pumpCatalogUnsubscribesForTest();    // still full: kept
    g.admin.ringFull = false;
    (void)g.gw.pumpCatalogUnsubscribesForTest();                                         // the duty-cycle retries
    std::size_t securityEnds = 0, catalogEnds = 0;
    for (const auto& u : g.admin.securityUnsubs) securityEnds += u.session == g.sid && u.subscriptionId == -1 ? 1 : 0;
    for (const auto& u : g.admin.catalogUnsubs)
        catalogEnds += u.session == g.sid && u.subscriptionId == rune::gateway::kEndCatalogReadsOnly ? 1 : 0;
    EXPECT_EQ(securityEnds, 1u) << "the end of the session's __security reads reached the admin (once: coalesced)";
    EXPECT_EQ(catalogEnds, 1u) << "the end of the session's __catalog reads reached the admin";
}

// Issue #117, the control thread's own gate: an admin-log Records frame reaches the session only while it holds the
// grant NOW (ADMIN ON CLUSTER for `__security`, DESCRIBE ON CLUSTER for `__catalog`), whether or not the admin thread has
// processed the end yet; and a `__security` frame whose event this broker's SecurityView has not applied waits (the
// admin can apply and stream a REVOKE and the CREATE USER after it before this thread applies the REVOKE).  RED before
// the fix: every frame was offered (the admin processing the end was the only stop).
namespace {
std::vector<uint8_t> securityRecordsFrame(int64_t sid, int64_t subId, int64_t eventOffset, bool control = false) {
    rune::protocol::RecordBatchBuilder b(eventOffset, 0);
    b.setControl(control);
    b.append("k", "verifier-bearing event", 0);
    const std::vector<uint8_t> batch = b.build();
    pc::Records rec;
    rec.sessionId = sid; rec.correlationId = 9; rec.subscriptionId = subId;
    rec.topicId = -7; rec.partition = 0; rec.baseOffset = eventOffset;
    rec.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> f(rec.encodedSize());
    f.resize(rec.encode(f.data()));
    return f;
}
rune::gateway::PermissionTable clusterTable(std::initializer_list<rune::security::Operation> ops) {
    std::vector<rune::security::GrantSpec> g;
    for (const auto op : ops)
        g.push_back({op, rune::security::ResourcePattern{rune::security::ResourceType::Cluster,
                                                         rune::security::PatternKind::Exact, ""}});
    std::sort(g.begin(), g.end());
    return rune::gateway::PermissionTable::compile(g, rune::gateway::ObjectIndex{});
}
}  // namespace

TEST(AuthLiveChangeRegression, AdminLogFramesNeedTheReadersGrantAtSendTimeRegression) {
    using A = ClientGateway::AdminLogAdmit;
    GatewayRig g;
    rune::gateway::SecurityView view{rune::security::SecurityState{}};   // applied: nothing yet (-1)
    g.gw.setAuthMode(GatewayAuthMode::Required, &view);
    const int64_t secSub = rune::gateway::kCatalogSubscriptionIdBase | rune::gateway::kSecuritySubscriptionIdBit | 1;
    const int64_t catSub = rune::gateway::kCatalogSubscriptionIdBase | (int64_t{2} << 40) | 1;
    const auto event0 = securityRecordsFrame(g.sid, secSub, 0);
    const auto ctrl0  = securityRecordsFrame(g.sid, secSub, 0, /*control=*/true);

    g.gw.setSessionAuthForTest(g.sid, AuthAuthenticated{1, "ops", clusterTable({rune::security::Operation::Admin}), {}});
    EXPECT_EQ(g.gw.admitAdminLogFrame(g.sid, secSub, event0.data(), event0.size()), A::Hold)
        << "an event this broker has not applied yet waits (it may follow a revoke of this very reader)";
    EXPECT_EQ(g.gw.admitAdminLogFrame(g.sid, secSub, ctrl0.data(), ctrl0.size()), A::Deliver)
        << "a control batch is no event (the applier never mirrors it): never held";
    EXPECT_EQ(g.gw.admitAdminLogFrame(g.sid, catSub, event0.data(), event0.size()), A::Withhold)
        << "ADMIN does not give DESCRIBE ON CLUSTER here (no implied cluster ops in this table)"
           " - unless the table says so";

    g.gw.setSessionAuthForTest(g.sid, AuthAuthenticated{1, "ops", clusterTable({rune::security::Operation::Describe}), {}});
    EXPECT_EQ(g.gw.admitAdminLogFrame(g.sid, secSub, ctrl0.data(), ctrl0.size()), A::Withhold)
        << "lost ADMIN: no __security frame, whatever the admin thread still streams";
    EXPECT_EQ(g.gw.admitAdminLogFrame(g.sid, catSub, event0.data(), event0.size()), A::Deliver);
    EXPECT_GE(g.gw.adminLogFramesWithheld(), 1u);

    g.gw.setAuthMode(GatewayAuthMode::InProcess, nullptr);
    EXPECT_EQ(g.gw.admitAdminLogFrame(g.sid, secSub, event0.data(), event0.size()), A::Deliver) << "dev / in-process";
}
