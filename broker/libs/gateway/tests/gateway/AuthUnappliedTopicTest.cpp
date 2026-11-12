// AuthUnappliedTopicTest.cpp - issue #127: an id this broker has placed (committed routing image) but whose catalog
// event it has not applied yet (not in its ObjectIndex) is a CREATE / GRANT racing the apply.  Every id-addressed
// request answers it the retriable NotLeader, never the terminal NotAuthorized (45); an id neither applied nor placed
// keeps the D22 denial (a hidden and a missing id answer alike).
//
// Deterministic: no Aeron, no sleeps - makeForTest + the apply*ForTest seams.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/IAdminRequestSink.h>
#include <rune/gateway/IGroupRequestHandler.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/gateway/SecurityView.h>
#include <rune/security/Permissions.h>
#include <rune/security/SecurityEvent.h>
#include "support/SecurityEventChain.h"

#include <gtest/gtest.h>

#include <climits>
#include <filesystem>
#include <functional>
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
// neither: below the applied frontier (kPlaced), so decided here - a gap that was never created
constexpr int32_t kMissing = 5;
constexpr int16_t kNotLeader     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kNotAuthorized = static_cast<int16_t>(pc::ErrorCode::NotAuthorized);

// The coordination services only count: every request in these tests is answered (or not) by the gateway's own check.
struct CountingGroup final : IGroupRequestHandler {
    int calls = 0;
    size_t handleJoinGroup(const uint8_t*, size_t, int32_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleSyncGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleHeartbeat(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleLeaveGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleOffsetFetch(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
};
struct CountingTxn final : ITxnRequestHandler {
    int calls = 0;
    size_t handleInitProducerId(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleAddPartitions(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleAddOffsets(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleTxnOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
    size_t handleEndTxn(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { ++calls; return 0; }
};
struct CountingAdminSink final : IAdminRequestSink {
    std::size_t gets = 0;
    uint8_t lastKinds = 0;
    AdminSubmit submitDefineTopic(int64_t, int64_t, std::string_view, uint32_t, std::string_view, bool, const uint8_t*,
                                  uint32_t, uint8_t, int16_t, const rune::protocol::TopicConfigView&, const DdlMetaView&,
                                  std::string_view, const rune::protocol::TopicKeyFeatures&,
                                  const std::optional<rune::protocol::LeaseSpec>&) override { return AdminSubmit::Accepted; }
    bool submitGetSchema(int64_t, int64_t, std::string_view, uint64_t, uint32_t, uint8_t kinds) override {
        ++gets; lastKinds = kinds; return true;
    }
    AdminSubmit submitAlterTopic(int64_t, int64_t, std::string_view, uint16_t, const uint8_t*, uint32_t,
                                 const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitCreateView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, const uint8_t*, uint32_t,
                                 const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitDropTopic(int64_t, int64_t, std::string_view, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitAlterTopicConfig(int64_t, int64_t, std::string_view, uint32_t, std::span<const uint8_t>,
                                       const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitDropView(int64_t, int64_t, std::string_view, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    AdminSubmit submitAlterView(int64_t, int64_t, std::string_view, std::string_view, uint8_t, uint32_t, const uint8_t*,
                                uint32_t, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    bool submitCatalogSubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitCatalogUnsubscribe(int64_t, int64_t) override { return true; }
    AdminSubmit submitSecurityDdl(const pc::SecurityDdl&, const DdlMetaView&, std::string_view, bool) override {
        return AdminSubmit::Accepted;
    }
    AdminSubmit submitNamespaceDdl(const pc::NamespaceDdlRequest&, const DdlMetaView&, std::string_view, uint16_t) override { return AdminSubmit::Accepted; }
    AdminSubmit submitListNamespaces(int64_t, int64_t, uint32_t, uint16_t, uint32_t, bool) override { return AdminSubmit::Accepted; }
    AdminSubmit submitSecurityBootstrap(const pc::SecurityBootstrap&, const DdlMetaView&) override { return AdminSubmit::Accepted; }
    bool submitSecuritySubscribe(int64_t, int64_t, int64_t) override { return true; }
    bool submitSecurityUnsubscribe(int64_t, int64_t) override { return true; }
};

sec::GrantSpec grant(sec::Operation op, sec::ResourceType t, std::string prefix) {
    return sec::GrantSpec{op, sec::ResourcePattern{t, sec::PatternKind::Prefix, std::move(prefix)}};
}
// The default session: WRITE / READ on every topic named "w.*" (the topic at kPlaced will be "w.z" once applied here),
// READ on views "w.*", READ on groups "g", WRITE on transactional ids "t".
std::vector<sec::GrantSpec> defaultGrants() {
    return {grant(sec::Operation::Read, sec::ResourceType::Topic, "w."),
            grant(sec::Operation::Write, sec::ResourceType::Topic, "w."),
            grant(sec::Operation::Read, sec::ResourceType::View, "w."),
            grant(sec::Operation::Read, sec::ResourceType::Group, "g"),
            grant(sec::Operation::Write, sec::ResourceType::TransactionalId, "t")};
}

std::vector<rune::metadata::RoutingImage::BuildEntry> defaultImage() {
    return {{rune::metadata::gpidOf(0, 0), 1, 1}, {rune::metadata::gpidOf(kPlaced, 0), 1, 1}};
}

struct Rig {
    TempDir                        dir;
    std::unique_ptr<TopicRegistry> registry;
    rune::node::WorkerPool         pool;   // GetViewRow is authorized only on a gateway with workers
    CountingGroup                  group;
    CountingTxn                    txn;
    CountingAdminSink              admin;
    ClientGateway                  gw;
    int64_t                        sid = 0;
    int64_t                        corr = 100;
    std::vector<std::pair<int64_t, int16_t>> answers;   // (correlation, code)
    std::string lastDdlDetail;
    bool dispatch = false;   // #603: send through the gateway's whole client-frame dispatch (the request gate included)

    explicit Rig(std::vector<sec::GrantSpec> grants = defaultGrants(),
                 std::vector<rune::metadata::RoutingImage::BuildEntry> img = defaultImage(), bool withPool = true,
                 bool feedFrontier = true)
          : pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16)
          , gw(ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t { onFrame(b, n); return 1; }, 1)) {
        rune::node::TopicSpec boot;
        boot.name = "w.boot"; boot.partitionCount = 1; boot.topicId = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{boot}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        if (withPool) gw.setWorkerPool(&pool);   // #219: false = a gateway without workers
        gw.setCommittedMetadataReady(true);
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        gw.setNodeIdForTest(1);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { onFrame(b, n); });
        gw.setGroupService(&group);
        gw.setTxnService(&txn);
        gw.setAdminSink(&admin);
        gw.onCatalogObjectApplied(0, "w.boot", false);
        // #988: the catalog applied every id below kPlaced; kPlaced is the one in flight
        if (feedFrontier) gw.setAppliedTopicFrontier(kPlaced);
        sid = gw.injectTestSession();
        gw.authenticateWithGrantsForTest(sid, 42, std::move(grants));
    }

    std::vector<std::vector<uint8_t>> frames;   // every frame the gateway emitted, raw (#988: byte comparisons)

    void onFrame(const uint8_t* b, std::size_t n) {
        frames.emplace_back(b, b + n);
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
            case pc::TemplateId::ScanViewRowsResponse:
                if (auto r = pc::ScanViewRowsResponse::decode(b, n)) answers.emplace_back(r->correlationId, static_cast<int16_t>(r->status));
                break;
            case pc::TemplateId::JoinGroupResponse:
                if (auto r = pc::JoinGroupResponse::decode(b, n)) answers.emplace_back(r->correlationId, r->errorCode);
                break;
            case pc::TemplateId::OffsetCommitResponse:
                if (auto r = pc::OffsetCommitResponse::decode(b, n))
                    for (const auto e : r->results) { answers.emplace_back(r->correlationId, e.errorCode); break; }
                break;
            case pc::TemplateId::OffsetFetchResponse:
                if (auto r = pc::OffsetFetchResponse::decode(b, n))
                    for (const auto e : r->results) { answers.emplace_back(r->correlationId, e.errorCode); break; }
                break;
            case pc::TemplateId::AddPartitionsToTxnResponse:
                if (auto r = pc::AddPartitionsToTxnResponse::decode(b, n))
                    for (const auto e : r->results) { answers.emplace_back(r->correlationId, e.errorCode); break; }
                break;
            case pc::TemplateId::TxnOffsetCommitResponse:
                if (auto r = pc::TxnOffsetCommitResponse::decode(b, n))
                    for (const auto e : r->results) { answers.emplace_back(r->correlationId, e.errorCode); break; }
                break;
            case pc::TemplateId::TopicSchemaResponse:
                if (auto r = pc::TopicSchemaResponse::decode(b, n))
                    answers.emplace_back(r->correlationId, static_cast<int16_t>(r->errorCode));
                break;
            case pc::TemplateId::DdlResponse:   // #506: the DDL family carries its code and detail here
                if (auto r = pc::DdlResponse::decode(b, n)) {
                    answers.emplace_back(r->correlationId, static_cast<int16_t>(r->errorCode));
                    lastDdlDetail = std::string(r->detail);
                }
                break;
            case pc::TemplateId::StatsResponse:   // #988: GetStats refusals are status-only StatsResponses
                if (auto r = pc::StatsResponse::decode(b, n))
                    answers.emplace_back(r->correlationId, static_cast<int16_t>(r->status));
                break;
            case pc::TemplateId::Pong:   // #603: a session-level answer (code 0)
                answers.emplace_back(h->correlationId, 0);
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
    int64_t produce(int32_t topicId, int32_t partition = 0) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"v"}, 0);
        const std::vector<uint8_t> batch = b.build();
        pc::Produce m;
        m.sessionId = sid; m.correlationId = ++corr; m.topicId = topicId; m.partition = partition;
        m.partitionKnown = 1; m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyProduceForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    int64_t subscribe(int32_t topicId, int32_t partition = 0) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = ++corr; s.topicId = topicId; s.partition = partition;
        s.partitionKnown = 1; s.fromOffset = 0; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applySubscribeForTest(buf.data(), buf.size());
        return s.correlationId;
    }
    int64_t getViewRow(int32_t viewId) {
        pc::GetViewRow m;
        m.sessionId = sid; m.correlationId = ++corr; m.viewTopicId = viewId; m.keyU64 = 1;
        std::vector<uint8_t> buf(64);
        buf.resize(m.encode(buf.data()));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyGetViewRowForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    int64_t getStats(int32_t topicId, int32_t partition = 0) {
        pc::GetStats m;
        m.sessionId = sid; m.correlationId = ++corr; m.scope = pc::StatsScope::TopicPartition;
        m.objectId = topicId; m.partition = partition;
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        gw.applyGetStatsForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    int64_t getViewRowK(int32_t viewId) {
        pc::GetViewRowK m;
        m.sessionId = sid; m.correlationId = ++corr; m.viewTopicId = viewId; m.partition = -1; m.minOffset = -1;
        const std::vector<uint8_t> key{1, 0, 0, 0, 0, 0, 0, 0};
        m.key = key;
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        gw.applyGetViewRowKForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    int64_t scanViewRows(int32_t viewId) {
        pc::ScanViewRows m;
        m.sessionId = sid; m.correlationId = ++corr; m.viewTopicId = viewId; m.limit = 0;
        const std::vector<uint8_t> prefix{1};   // a prefix of length 0 does not decode
        m.prefix = prefix;
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        gw.applyScanViewRowsForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    static std::span<const uint8_t> bytes(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
    int64_t joinGroup(int32_t topicId) {
        pc::JoinGroup m;
        m.sessionId = sid; m.correlationId = ++corr; m.sessionTimeoutMs = 10'000; m.groupId = bytes("g");
        const int32_t ids[] = {topicId};
        std::vector<uint8_t> buf(pc::JoinGroup::encodedSizeFor(m.groupId, m.memberId, 1, 0));
        buf.resize(m.encode(buf.data(), std::span<const int32_t>(ids)));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyGroupForTest(buf.data(), buf.size(), static_cast<uint16_t>(pc::TemplateId::JoinGroup));
        return m.correlationId;
    }
    int64_t offsetCommit(int32_t topicId, int32_t partition) {
        pc::OffsetCommit m;
        m.sessionId = sid; m.correlationId = ++corr; m.generationId = 1; m.groupId = bytes("g"); m.memberId = bytes("m");
        const pc::OffsetCommitEntry e[] = {{topicId, partition, 5, {}}};
        std::vector<uint8_t> buf(pc::OffsetCommit::encodedSizeFor(m.groupId, m.memberId, e));
        buf.resize(m.encode(buf.data(), e));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyGroupForTest(buf.data(), buf.size(), static_cast<uint16_t>(pc::TemplateId::OffsetCommit));
        return m.correlationId;
    }
    int64_t offsetFetch(int32_t topicId, int32_t partition) {
        pc::OffsetFetch m;
        m.sessionId = sid; m.correlationId = ++corr; m.groupId = bytes("g");
        const pc::OffsetFetchEntry e[] = {{topicId, partition}};
        std::vector<uint8_t> buf(pc::OffsetFetch::encodedSizeFor(m.groupId, 1));
        buf.resize(m.encode(buf.data(), e));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyGroupForTest(buf.data(), buf.size(), static_cast<uint16_t>(pc::TemplateId::OffsetFetch));
        return m.correlationId;
    }
    int64_t addPartitionsToTxn(int32_t topicId, int32_t partition) {
        pc::AddPartitionsToTxn m;
        m.sessionId = sid; m.correlationId = ++corr; m.producerId = 1; m.transactionalId = bytes("t");
        const pc::TxnTopicPartition p[] = {{topicId, partition}};
        std::vector<uint8_t> buf(pc::AddPartitionsToTxn::encodedSizeFor(m.transactionalId, 1));
        buf.resize(m.encode(buf.data(), p));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyTxnForTest(buf.data(), buf.size(), static_cast<uint16_t>(pc::TemplateId::AddPartitionsToTxn));
        return m.correlationId;
    }
    int64_t txnOffsetCommit(int32_t topicId, int32_t partition) {
        pc::TxnOffsetCommit m;
        m.sessionId = sid; m.correlationId = ++corr; m.producerId = 1; m.transactionalId = bytes("t"); m.groupId = bytes("g");
        const pc::TxnOffsetCommitEntry e[] = {{topicId, partition, 5, {}}};
        std::vector<uint8_t> buf(m.encodedSize(e));
        buf.resize(m.encode(buf.data(), e));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyTxnForTest(buf.data(), buf.size(), static_cast<uint16_t>(pc::TemplateId::TxnOffsetCommit));
        return m.correlationId;
    }
    int64_t getTopicSchema(std::string_view name) {
        pc::GetTopicSchema m;
        m.sessionId = sid; m.correlationId = ++corr; m.topicName = name;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        if (dispatch) (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        else gw.applyGetTopicSchemaForTest(buf.data(), buf.size());
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

// #511: a view sink id is placed (its source's placement) but never indexed as a topic, so it looked "placed but not
// applied here" for good - an ungranted Produce / Subscribe got the retriable NotLeader forever and a hidden sink was
// distinguishable from a missing id.  A sink the registry knows answers like a missing id: 45, never 2.
TEST(AuthUnappliedTopicRegression, AnUngrantedViewSinkIdIsNotAuthorizedNeverNotLeaderRegression) {
    Rig r;
    rune::node::TopicSpec sink;
    sink.name = "hidden_sink"; sink.partitionCount = 1; sink.viewSink = true;
    ASSERT_EQ(r.registry->createTopicAt(kPlaced, sink), kPlaced);
    ASSERT_TRUE(r.registry->isViewSink(kPlaced));
    ASSERT_EQ(r.gw.objectIndex().at(kPlaced), nullptr) << "precondition: a sink is never in the object index as a topic";
    for (const auto& [what, corr] : {std::pair<const char*, int64_t>{"Produce", r.produce(kPlaced)},
                                     {"Subscribe", r.subscribe(kPlaced)}}) {
        const auto code = r.codeOf(corr);
        ASSERT_TRUE(code.has_value()) << what << ": answered, never silent";
        EXPECT_EQ(*code, kNotAuthorized) << what << ": a hidden sink answers like a missing id (M14 / D22)";
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

// #988: an id this broker has not applied answers the same whether the catalog placed it (a foreign namespace's new
// topic, committed here before the apply) or never created it.  The session is another namespace's, with no grant.
// Below the frontier the answer is decided here: a foreign applied id and a dropped id are both NotAuthorized, byte
// for byte.  After the apply a foreign session is refused, the owner served.
namespace {
using Frame = std::vector<uint8_t>;
// Send one request kind for `id` with the fixed correlation `corr`; the single frame the gateway answered with.
template <class Send>
Frame ask(Rig& r, Send send, int32_t id, int64_t corr) {
    r.frames.clear();
    r.answers.clear();
    r.corr = corr - 1;
    (void)send(id);
    EXPECT_EQ(r.frames.size(), 1u) << "id " << id << ": exactly one answer";
    return r.frames.empty() ? Frame{} : r.frames.front();
}
} // namespace

TEST(AuthUnappliedTopicRegression, AJustPlacedForeignIdAnswersLikeANeverCreatedIdRegression) {
    Rig r(std::vector<sec::GrantSpec>{});   // a namespace-A session with no grant on anything of namespace B
    const int32_t F = kPlaced;              // namespace B's new topic: placement committed here, not applied here
    const int32_t G = kPlaced + 1;          // never created, not placed
    ASSERT_EQ(r.gw.appliedTopicFrontierForTest(), F);
    ASSERT_EQ(r.gw.objectIndex().at(F), nullptr);

    struct Req { const char* name; std::function<int64_t(int32_t)> send; };
    const Req reqs[] = {
        {"GetStats",  [&](int32_t id) { return r.getStats(id); }},
        {"Subscribe", [&](int32_t id) { return r.subscribe(id); }},
        {"Produce",   [&](int32_t id) { return r.produce(id); }},
    };
    for (const Req& q : reqs) {
        const Frame fa = ask(r, q.send, F, 900);
        const auto codeF = r.codeOf(900);
        const Frame ga = ask(r, q.send, G, 900);
        const auto codeG = r.codeOf(900);
        ASSERT_FALSE(fa.empty()) << q.name;
        EXPECT_EQ(fa, ga) << q.name << ": a just-placed foreign id and a never-created id answer byte for byte alike";
        ASSERT_TRUE(codeF.has_value()) << q.name;
        EXPECT_EQ(*codeF, kNotLeader) << q.name << ": at or above the frontier: retriable for every session";
        EXPECT_EQ(codeF, codeG) << q.name;
    }

    // Below the frontier: a foreign applied id and a dropped id both answer the terminal NotAuthorized, alike.
    constexpr int32_t kForeign = 3, kDropped = 4;
    r.gw.onCatalogObjectApplied(2, kForeign, "b.applied", false);
    rune::node::TopicSpec dropped;
    dropped.name = "b.dropped"; dropped.partitionCount = 1;
    ASSERT_EQ(r.registry->createTopicAt(kDropped, dropped), kDropped);
    r.gw.onCatalogObjectApplied(2, kDropped, "b.dropped", false);
    r.gw.onCatalogObjectDropped(kDropped);
    ASSERT_TRUE(r.registry->dropTopic(kDropped));
    for (const Req& q : reqs) {
        const Frame fa = ask(r, q.send, kForeign, 901);
        const auto codeA = r.codeOf(901);
        const Frame da = ask(r, q.send, kDropped, 901);
        const auto codeD = r.codeOf(901);
        ASSERT_FALSE(fa.empty()) << q.name;
        EXPECT_EQ(fa, da) << q.name << ": a foreign applied id and a dropped id answer alike";
        ASSERT_TRUE(codeA.has_value()) << q.name;
        EXPECT_EQ(*codeA, kNotAuthorized) << q.name;
        EXPECT_EQ(codeA, codeD) << q.name;
    }

    // After the apply (registry + frontier F+1): the foreign session is refused, the owner is served.
    rune::node::TopicSpec applied;
    applied.name = "b.new"; applied.partitionCount = 1;
    ASSERT_EQ(r.registry->createTopicAt(F, applied), F);
    r.gw.onCatalogObjectApplied(2, F, "b.new", false);
    r.gw.setAppliedTopicFrontier(F + 1);
    EXPECT_EQ(r.codeOf(r.produce(F)), kNotAuthorized) << "applied: a foreign session is refused";
    r.answers.clear();
    const int64_t owner = r.gw.injectTestSession();
    r.gw.authenticateWithGrantsForTest(owner, 43, {grant(sec::Operation::Write, sec::ResourceType::Topic, "b.")}, 2);
    r.sid = owner;
    const auto served = r.codeOf(r.produce(F));
    EXPECT_NE(served, std::optional<int16_t>{kNotAuthorized}) << "the owner is not refused";
    EXPECT_NE(served, std::optional<int16_t>{kNotLeader});
}

// #988: the frontier never moves back; a lower value is ignored and counted.
TEST(AuthUnappliedTopicRegression, TheFrontierNeverMovesBackRegression) {
    Rig r;
    r.gw.setAppliedTopicFrontier(20);
    EXPECT_EQ(r.gw.appliedTopicFrontierForTest(), 20);
    r.gw.setAppliedTopicFrontier(12);
    EXPECT_EQ(r.gw.appliedTopicFrontierForTest(), 20) << "a lower value is ignored";
    EXPECT_EQ(r.gw.topicFrontierRegressionsIgnored(), 1u);
    r.gw.setAppliedTopicFrontier(20);
    r.gw.setAppliedTopicFrontier(21);
    EXPECT_EQ(r.gw.appliedTopicFrontierForTest(), 21);
    EXPECT_EQ(r.gw.topicFrontierRegressionsIgnored(), 1u);
    // an id at or above the frontier answers retriable, one below it terminal
    EXPECT_EQ(r.codeOf(r.produce(21)), kNotLeader);
    EXPECT_EQ(r.codeOf(r.produce(kMissing)), kNotAuthorized);
}

// #988: a gateway without a frontier feed keeps the terminal answer for an unknown id (nothing is "above").
TEST(AuthUnappliedTopicRegression, WithoutAFrontierFeedAnUnknownIdKeepsTheTerminalAnswerRegression) {
    Rig r(defaultGrants(), defaultImage(), /*withPool=*/true, /*feedFrontier=*/false);
    ASSERT_EQ(r.gw.appliedTopicFrontierForTest(), INT32_MAX);
    EXPECT_EQ(r.codeOf(r.produce(kPlaced)), kNotAuthorized) << "placed but no feed: nothing is above the frontier";
    EXPECT_EQ(r.codeOf(r.produce(kMissing)), kNotAuthorized);
    r.gw.setAppliedTopicFrontier(kPlaced);
    EXPECT_EQ(r.gw.appliedTopicFrontierForTest(), kPlaced) << "the first feed is accepted whatever the default";
    EXPECT_EQ(r.codeOf(r.produce(kPlaced)), kNotLeader);
}

TEST(AuthUnappliedTopicRegression, OnceAppliedAnUngrantedPlacedTopicIsDeniedTerminallyRegression) {
    Rig r;
    r.gw.onCatalogObjectApplied(kPlaced, "secret", false);   // applied here now; the user's "w." grants miss it
    const auto code = r.codeOf(r.produce(kPlaced));
    ASSERT_TRUE(code.has_value());
    EXPECT_EQ(*code, kNotAuthorized) << "only the not-yet-applied window is retriable";
}

// ── PR #501 adversarial review: the whole #127 / #128 area through the gateway's public request handlers ──────────

// Every id-addressed template that runs the topic check answers the placed-but-unapplied id NotLeader in its OWN
// response type (encodeRejection: a single {-1,-1,2} result entry where the type has no top-level code), and keeps 45
// for a missing id.  None of them is forwarded to its service.
TEST(AuthUnappliedTopicRegression, EveryTopicCheckedGroupAndTxnTemplateAnswersNotLeaderInItsOwnTypeAdversarialRegression) {
    Rig r;
    struct Leg { const char* what; int64_t (Rig::*send)(int32_t, int32_t); };
    const Leg legs[] = {{"OffsetCommit", &Rig::offsetCommit}, {"OffsetFetch", &Rig::offsetFetch},
                        {"AddPartitionsToTxn", &Rig::addPartitionsToTxn}, {"TxnOffsetCommit", &Rig::txnOffsetCommit}};
    for (const Leg& l : legs) {
        const auto placed  = r.codeOf((r.*l.send)(kPlaced, 0));
        const auto missing = r.codeOf((r.*l.send)(kMissing, 0));
        ASSERT_TRUE(placed.has_value()) << l.what << ": answered in its own response type, never silent";
        ASSERT_TRUE(missing.has_value()) << l.what;
        EXPECT_EQ(*placed, kNotLeader) << l.what;
        EXPECT_EQ(*missing, kNotAuthorized) << l.what << ": D22";
    }
    const auto join = r.codeOf(r.joinGroup(kPlaced));
    ASSERT_TRUE(join.has_value());
    EXPECT_EQ(*join, kNotLeader) << "JoinGroup";
    const auto joinMissing = r.codeOf(r.joinGroup(kMissing));
    ASSERT_TRUE(joinMissing.has_value());
    EXPECT_EQ(*joinMissing, kNotAuthorized) << "JoinGroup: D22";
    EXPECT_EQ(r.group.calls, 0) << "no refused request reaches the group service";
    EXPECT_EQ(r.txn.calls, 0) << "no refused request reaches the txn service";
}

// A retriable verdict is not a refusal: the NotAuthorized counter (the audit / rate-limited warn) does not move.
TEST(AuthUnappliedTopicRegression, ARetriableVerdictIsNotCountedAsARefusalAdversarialRegression) {
    Rig r;
    const uint64_t before = r.gw.notAuthorizedAnswers();
    (void)r.produce(kPlaced);
    (void)r.subscribe(kPlaced);
    EXPECT_EQ(r.gw.notAuthorizedAnswers(), before);
    (void)r.produce(kMissing);
    EXPECT_EQ(r.gw.notAuthorizedAnswers(), before + 1) << "a real refusal still counts";
}

// Placement is per partition: a topic whose partition 0 is leaderless (D15) but whose partition 1 is led is placed.
// The request's own partition decides, exactly as the unauthenticated path (unappliedTopicCode_(id, partition)) does.
TEST(AuthUnappliedTopicRegression, APlacedPartitionOtherThanZeroIsRetriableAdversarialRegression) {
    constexpr int32_t kP1 = 8;   // partition 1 led by node 1, partition 0 leaderless
    Rig r(defaultGrants(), {{rune::metadata::gpidOf(0, 0), 1, 1}, {rune::metadata::gpidOf(kP1, 1), 1, 1}});
    for (const auto& [what, corr] : {std::pair<const char*, int64_t>{"Produce", r.produce(kP1, 1)},
                                     {"Subscribe", r.subscribe(kP1, 1)},
                                     {"OffsetCommit", r.offsetCommit(kP1, 1)},
                                     {"OffsetFetch", r.offsetFetch(kP1, 1)},
                                     {"AddPartitionsToTxn", r.addPartitionsToTxn(kP1, 1)},
                                     {"TxnOffsetCommit", r.txnOffsetCommit(kP1, 1)}}) {
        const auto code = r.codeOf(corr);
        ASSERT_TRUE(code.has_value()) << what;
        EXPECT_EQ(*code, kNotLeader) << what << ": partition 1 is placed - retriable, never terminal 45";
    }
}

// Input bounds: gpid = topicId * 1024 + partition, so partition 1024 of id kPlaced-1 is gpid(kPlaced, 0).  A partition
// outside [0, 1024) never borrows another id's placement; negative and INT32_MAX partitions keep the D22 denial too.
TEST(AuthUnappliedTopicRegression, APartitionPastTheGpidRangeNeverAliasesTheNextIdAdversarialRegression) {
    Rig r;
    constexpr int32_t kBelow = kPlaced - 1;   // neither applied nor placed
    for (const int32_t p : {1024, 1025, INT32_MAX, -1, INT32_MIN}) {
        for (const auto& [what, corr] : {std::pair<const char*, int64_t>{"Produce", r.produce(kBelow, p)},
                                         {"Subscribe", r.subscribe(kBelow, p)},
                                         {"OffsetCommit", r.offsetCommit(kBelow, p)},
                                         {"AddPartitionsToTxn", r.addPartitionsToTxn(kBelow, p)}}) {
            const auto code = r.codeOf(corr);
            ASSERT_TRUE(code.has_value()) << what << " p=" << p;
            EXPECT_EQ(*code, kNotAuthorized) << what << " p=" << p << ": a missing id stays 45";
        }
    }
    // A negative partition of the placed id itself is read as "partition unknown" (0), like unappliedTopicCode_.
    const auto neg = r.codeOf(r.produce(kPlaced, -1));
    ASSERT_TRUE(neg.has_value());
    EXPECT_EQ(*neg, kNotLeader);
}

// Dropped here while its placement removal has not committed yet: terminal (45), never a NotLeader the client would
// retry forever on a topic that will never come back (ids are never reused).
TEST(AuthUnappliedTopicRegression, ADroppedIdStillInTheRoutingImageIsTerminalAdversarialRegression) {
    Rig r;
    ASSERT_TRUE(r.registry->dropTopic(kPlaced));
    for (const auto& [what, corr] : {std::pair<const char*, int64_t>{"Produce", r.produce(kPlaced)},
                                     {"Subscribe", r.subscribe(kPlaced)},
                                     {"GetViewRow", r.getViewRow(kPlaced)},
                                     {"JoinGroup", r.joinGroup(kPlaced)},
                                     {"AddPartitionsToTxn", r.addPartitionsToTxn(kPlaced, 0)}}) {
        const auto code = r.codeOf(corr);
        ASSERT_TRUE(code.has_value()) << what;
        EXPECT_EQ(*code, kNotAuthorized) << what;
    }
}

// Applied then dropped here (ObjectIndex cleared too): still terminal.
TEST(AuthUnappliedTopicRegression, AnAppliedThenDroppedIdIsTerminalAdversarialRegression) {
    Rig r;
    r.gw.onCatalogObjectApplied(kPlaced, "w.z", false);
    ASSERT_NE(r.codeOf(r.produce(kPlaced)), kNotAuthorized) << "granted: not refused at the auth check";
    r.answers.clear();
    r.gw.onCatalogObjectDropped(kPlaced);
    ASSERT_TRUE(r.registry->dropTopic(kPlaced));
    const auto code = r.codeOf(r.produce(kPlaced));
    ASSERT_TRUE(code.has_value());
    EXPECT_EQ(*code, kNotAuthorized);
}

// Auth: a session with NO grants.  During the apply window it sees the same retriable answer as everyone (the
// unauthenticated path answers it too: no record data, no name); once applied the hidden id and a missing id answer
// alike again (D22), in every template.
TEST(AuthUnappliedTopicRegression, ASessionWithNoGrantsSeesHiddenAndMissingAlikeOnceAppliedAdversarialRegression) {
    Rig r(std::vector<sec::GrantSpec>{});
    EXPECT_EQ(r.codeOf(r.produce(kPlaced)), kNotLeader) << "the apply window";
    r.gw.onCatalogObjectApplied(kPlaced, "w.z", false);
    for (const int32_t id : {kPlaced, kMissing, 0}) {
        EXPECT_EQ(r.codeOf(r.produce(id)), kNotAuthorized) << "Produce id=" << id;
        EXPECT_EQ(r.codeOf(r.subscribe(id)), kNotAuthorized) << "Subscribe id=" << id;
        EXPECT_EQ(r.codeOf(r.getViewRow(id)), kNotAuthorized) << "GetViewRow id=" << id;
    }
}

// Auth: DESCRIBE ON CLUSTER turns a missing id into the request's own "unknown" answer (D22); a placed-unapplied id
// passes the check and the handler's own unapplied answer (NotLeader) follows - never 45.
TEST(AuthUnappliedTopicRegression, DescribeOnClusterStillAnswersThePlacedIdRetriableAdversarialRegression) {
    auto g = defaultGrants();
    g.push_back(sec::GrantSpec{sec::Operation::Describe, sec::ResourcePattern{sec::ResourceType::Cluster, sec::PatternKind::Exact, ""}});
    Rig r(std::move(g));
    const auto placed = r.codeOf(r.produce(kPlaced));
    ASSERT_TRUE(placed.has_value());
    EXPECT_EQ(*placed, kNotLeader);
    const auto missing = r.codeOf(r.produce(kMissing));
    ASSERT_TRUE(missing.has_value());
    EXPECT_EQ(*missing, static_cast<int16_t>(pc::ErrorCode::UnknownTopic));
}

// #128, #504, #505 through the handler: the gateway no longer consults its (lagging) object index for GetTopicSchema.
// It forwards with only the kind bits the grants allow under the name, and the admin thread serves the object only if
// its real kind is among them (otherwise it answers like a missing name).  A session with no grant on the name under
// either kind gets the terminal 45 here, whether or not the object exists, and the admin thread is never reached.
TEST(AuthUnappliedTopicRegression, GetTopicSchemaChecksOnlyTheLiveObjectsOwnKindAdversarialRegression) {
    Rig viewOnly({grant(sec::Operation::Describe, sec::ResourceType::View, "pay")});
    viewOnly.gw.onCatalogObjectApplied(kPlaced, "payments", false);
    viewOnly.gw.onCatalogObjectApplied(kPlaced + 1, "payboard", true);
    EXPECT_EQ(viewOnly.codeOf(viewOnly.getTopicSchema("payments")), std::nullopt) << "forwarded, only as a view";
    EXPECT_EQ(viewOnly.admin.gets, 1u);
    EXPECT_EQ(viewOnly.admin.lastKinds, rune::gateway::kKindView);
    EXPECT_EQ(viewOnly.codeOf(viewOnly.getTopicSchema("payboard")), std::nullopt);
    EXPECT_EQ(viewOnly.admin.lastKinds, rune::gateway::kKindView);
    EXPECT_EQ(viewOnly.codeOf(viewOnly.getTopicSchema("payroll")), std::nullopt) << "unknown: either kind (D22)";
    EXPECT_EQ(viewOnly.admin.gets, 3u);
    EXPECT_EQ(viewOnly.codeOf(viewOnly.getTopicSchema("orders")), kNotAuthorized) << "no grant on the name, any kind";
    EXPECT_EQ(viewOnly.admin.gets, 3u) << "never reached the admin thread";

    Rig topicOnly({grant(sec::Operation::Describe, sec::ResourceType::Topic, "pay")});
    topicOnly.gw.onCatalogObjectApplied(kPlaced, "payments", false);
    topicOnly.gw.onCatalogObjectApplied(kPlaced + 1, "payboard", true);
    EXPECT_EQ(topicOnly.codeOf(topicOnly.getTopicSchema("payboard")), std::nullopt);
    EXPECT_EQ(topicOnly.admin.lastKinds, rune::gateway::kKindTopic);
    EXPECT_EQ(topicOnly.codeOf(topicOnly.getTopicSchema("payments")), std::nullopt);
    EXPECT_EQ(topicOnly.admin.lastKinds, rune::gateway::kKindTopic);
    // Drop "payments" and re-create the NAME as a view at a new id: the gateway still sends the same kinds; the admin
    // thread decides by the live object.
    topicOnly.gw.onCatalogObjectDropped(kPlaced);
    topicOnly.gw.onCatalogObjectApplied(kPlaced + 2, "payments", true);
    EXPECT_EQ(topicOnly.codeOf(topicOnly.getTopicSchema("payments")), std::nullopt) << "forwarded with kKindTopic";
    EXPECT_EQ(topicOnly.admin.lastKinds, rune::gateway::kKindTopic);
    EXPECT_EQ(topicOnly.admin.gets, 3u);
    EXPECT_EQ(topicOnly.codeOf(topicOnly.getTopicSchema("orders")), kNotAuthorized) << "no grant on the name";
    EXPECT_EQ(topicOnly.admin.gets, 3u);
}

// #504: a just-created topic is not in this gateway's object index yet (onCatalogObjectApplied has not run for it).
// A VIEW-only session's GetTopicSchema must reach the admin thread carrying ONLY the view kind, so the admin thread
// (whose catalog is authoritative; see GetSchemaKindRegression.TheAdminServesOnlyTheKindsTheGatewayAllowedRegression)
// answers it like a missing name instead of serving the topic's schema.  The two tests together close the window.
TEST(AuthUnappliedTopicRegression, AViewGrantNeverDescribesAJustCreatedTopicRegression) {
    Rig r({grant(sec::Operation::Describe, sec::ResourceType::View, "pay")});
    ASSERT_EQ(r.gw.objectIndex().at(kPlaced), nullptr) << "precondition: the topic is not applied here";
    EXPECT_EQ(r.codeOf(r.getTopicSchema("payments")), std::nullopt) << "forwarded";
    EXPECT_EQ(r.admin.gets, 1u);
    EXPECT_EQ(r.admin.lastKinds, rune::gateway::kKindView) << "the admin thread must be told: a view, never a topic";
}

// #128 input bounds: an empty name, a name that is a strict prefix / extension of a live one, and a 64 KiB-1 name are
// judged by the grants alone; the empty name is malformed.
TEST(AuthUnappliedTopicRegression, GetTopicSchemaNameBoundsResolveByExactMatchAdversarialRegression) {
    Rig r({grant(sec::Operation::Describe, sec::ResourceType::View, "pay")});
    r.gw.onCatalogObjectApplied(kPlaced, "payments", false);
    EXPECT_EQ(r.codeOf(r.getTopicSchema("paymentsX")), std::nullopt) << "not the live 'payments': unknown, view grant ok";
    EXPECT_EQ(r.codeOf(r.getTopicSchema("payment")), std::nullopt);
    // Namespaces D7: the empty name is a malformed qualified name, answered InvalidTopicName for everyone (it names
    // nothing, so the answer reveals nothing) before any grant check.
    EXPECT_EQ(r.codeOf(r.getTopicSchema("")), static_cast<int16_t>(pc::ErrorCode::InvalidTopicName))
        << "the empty name is malformed (D7)";
    const std::string huge = "pay" + std::string(65535 - 3, 'x');
    EXPECT_EQ(r.codeOf(r.getTopicSchema(huge)), std::nullopt);
    EXPECT_EQ(r.codeOf(r.getTopicSchema("payments")), std::nullopt) << "forwarded with kKindView; the admin decides";
    EXPECT_EQ(r.admin.lastKinds, rune::gateway::kKindView);
}

// #511 adversarial review (PR #516): every id-addressed template, on a view sink id, answers EXACTLY like a missing id
// (D22 / M14) - Produce, Subscribe, GetViewRow, the group templates (JoinGroup, OffsetCommit, OffsetFetch) and the txn
// templates (AddPartitionsToTxn, TxnOffsetCommit), with a partition known, unknown (-1) and past the gpid range.  A
// sink is never "placed but not applied here": the retriable NotLeader would retry forever and tell a hidden sink
// from a missing id.  The coordination services are never reached.
TEST(AuthUnappliedTopicRegression, AViewSinkIdAnswersLikeAMissingIdOnEveryTemplateAdversarialRegression) {
    Rig r({}, {{rune::metadata::gpidOf(0, 0), 1, 1}, {rune::metadata::gpidOf(kPlaced, 0), 1, 1}});
    rune::node::TopicSpec sink;
    sink.name = "w.sink"; sink.partitionCount = 1; sink.viewSink = true;   // a name a grant prefix could match
    ASSERT_EQ(r.registry->createTopicAt(kPlaced, sink), kPlaced);
    ASSERT_TRUE(r.registry->isViewSink(kPlaced));
    struct Op { const char* what; std::function<int64_t(int32_t)> send; };
    const std::vector<Op> ops = {
        {"Produce",            [&](int32_t id) { return r.produce(id); }},
        {"Subscribe",          [&](int32_t id) { return r.subscribe(id); }},
        {"Subscribe p=-1",     [&](int32_t id) { return r.subscribe(id, -1); }},
        {"GetViewRow",         [&](int32_t id) { return r.getViewRow(id); }},
        {"JoinGroup",          [&](int32_t id) { return r.joinGroup(id); }},
        {"OffsetCommit",       [&](int32_t id) { return r.offsetCommit(id, 0); }},
        {"OffsetCommit p=max", [&](int32_t id) { return r.offsetCommit(id, rune::metadata::kMaxGpidPartitions); }},
        {"OffsetFetch",        [&](int32_t id) { return r.offsetFetch(id, 0); }},
        {"AddPartitionsToTxn", [&](int32_t id) { return r.addPartitionsToTxn(id, 0); }},
        {"TxnOffsetCommit",    [&](int32_t id) { return r.txnOffsetCommit(id, 0); }},
    };
    for (const Op& op : ops) {
        const auto sinkCode = r.codeOf(op.send(kPlaced));
        const auto missingCode = r.codeOf(op.send(kMissing));
        ASSERT_TRUE(sinkCode.has_value()) << op.what << ": answered, never silent";
        ASSERT_TRUE(missingCode.has_value()) << op.what;
        EXPECT_EQ(*sinkCode, *missingCode) << op.what << ": a hidden sink answers like a missing id";
        EXPECT_NE(*sinkCode, kNotLeader) << op.what << ": never the retriable NotLeader for a sink";
    }
    EXPECT_EQ(r.group.calls, 0);
    EXPECT_EQ(r.txn.calls, 0);
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issue #603: while this broker's `__security` view is behind committed events it holds (setSecurityBehind), every
// grant-checked request - through the gateway's whole client-frame dispatch - answers the retriable AuthUnavailable (48)
// in its own response type, reaches no service and appends nothing; session-level frames are never refused.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
constexpr int16_t kAuthUnavailable = static_cast<int16_t>(pc::ErrorCode::AuthUnavailable);
struct BehindRig : Rig {
    SecurityView view{rune::security::SecurityState{}};
    BehindRig() {
        dispatch = true;
        gw.setAuthMode(GatewayAuthMode::Required, &view);
    }
    int64_t ping() {
        pc::Ping p;
        p.sessionId = sid; p.correlationId = ++corr;
        std::vector<uint8_t> buf(64);
        buf.resize(p.encode(buf.data()));
        (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        return p.correlationId;
    }
    std::vector<std::pair<const char*, std::function<int64_t()>>> legs() {
        return {{"Produce", [this] { return produce(0); }},
                {"Subscribe", [this] { return subscribe(0); }},
                {"GetViewRow", [this] { return getViewRow(0); }},
                {"JoinGroup", [this] { return joinGroup(0); }},
                {"OffsetCommit", [this] { return offsetCommit(0, 0); }},
                {"OffsetFetch", [this] { return offsetFetch(0, 0); }},
                {"AddPartitionsToTxn", [this] { return addPartitionsToTxn(0, 0); }},
                {"TxnOffsetCommit", [this] { return txnOffsetCommit(0, 0); }},
                {"GetTopicSchema", [this] { return getTopicSchema("w.boot"); }}};
    }
};
}  // namespace

// RED before #603 (and with either refusal removed): the produce was appended and the other requests reached their
// services on grants the held events may already have revoked.
TEST(SecurityBehindGatewayRegression, WhileBehindEveryGrantCheckedRequestAnswersAuthUnavailableRegression) {
    BehindRig r;
    const int64_t leo = r.registry->log(0, 0)->logEndOffset();
    r.gw.setSecurityBehind(true);
    ASSERT_TRUE(r.gw.securityBehind());
    const auto legs = r.legs();
    for (const auto& [what, send] : legs) {
        const auto code = r.codeOf(send());
        ASSERT_TRUE(code.has_value()) << what << ": answered, never silent";
        EXPECT_EQ(*code, kAuthUnavailable) << what << ": retriable, in its own response type";
    }
    EXPECT_EQ(r.group.calls, 0) << "no group request reached the coordinator";
    EXPECT_EQ(r.txn.calls, 0) << "no transaction request reached the coordinator";
    EXPECT_EQ(r.admin.gets, 0u) << "no schema request reached the admin thread";
    EXPECT_EQ(r.registry->log(0, 0)->logEndOffset(), leo) << "nothing appended";
    EXPECT_EQ(r.gw.securityBehindRefusals(), legs.size());
    const auto pong = r.codeOf(r.ping());
    ASSERT_TRUE(pong.has_value()) << "a session-level frame is never refused";
    EXPECT_EQ(*pong, 0);
    EXPECT_TRUE(r.gw.hasSessionForTest(r.sid)) << "the session stays (only R ends it, SecurityServing)";

    // Applied: served again, judged by the (unchanged) grants.
    r.gw.setSecurityBehind(false);
    for (const auto& [what, send] : legs) {
        const auto code = r.codeOf(send());
        EXPECT_TRUE(!code.has_value() || *code != kAuthUnavailable) << what << " after the view caught up";
    }
    EXPECT_GT(r.group.calls, 0);
    EXPECT_GT(r.txn.calls, 0);
    EXPECT_EQ(r.gw.securityBehindRefusals(), legs.size()) << "nothing refused for it after the clear";
}

// Only an authenticating gateway refuses: an in-process / dev gateway has no `__security` grants to wait for.
TEST(SecurityBehindGatewayRegression, AnInProcessGatewayIgnoresTheFlagRegression) {
    Rig r;
    r.dispatch = true;
    r.gw.setSecurityBehind(true);
    EXPECT_FALSE(r.gw.securityBehind());
    const auto code = r.codeOf(r.produce(0));
    EXPECT_TRUE(!code.has_value() || *code != kAuthUnavailable);
    EXPECT_EQ(r.gw.securityBehindRefusals(), 0u);
}

// #219: on a gateway WITHOUT a worker pool the three view reads answered the C1 gate (or "no view is servable here")
// before the session's READ was checked, so a caller without READ told a hidden view (gate code) from a missing id.
// Authorization comes first on every path; only then the gate.
TEST(GetViewRowAuthRegression, NoPoolPathAuthorizesBeforeTheGateRegression) {
    // The sink id is Dropped (the gate answers UnknownView); a never-defined id has no gate at all.  (A placed-but-not-
    // applied id answers the retriable apply-window verdict on the pooled path too, by design: not tested here.)
    Rig r(std::vector<sec::GrantSpec>{}, defaultImage(), /*withPool=*/false);
    ASSERT_TRUE(r.registry->dropTopic(kPlaced));
    const std::vector<std::pair<const char*, std::function<int64_t(int32_t)>>> reads = {
        {"GetViewRow",   [&](int32_t id) { return r.getViewRow(id); }},
        {"GetViewRowK",  [&](int32_t id) { return r.getViewRowK(id); }},
        {"ScanViewRows", [&](int32_t id) { return r.scanViewRows(id); }}};
    for (const auto& [what, read] : reads) {
        const auto gated = r.codeOf(read(kPlaced));
        const auto missing = r.codeOf(read(kMissing));
        ASSERT_TRUE(gated.has_value()) << what << ": answered, never silent";
        ASSERT_TRUE(missing.has_value()) << what;
        EXPECT_EQ(*gated, kNotAuthorized) << what << ": no READ: the gated view answers NotAuthorized, not its gate code";
        EXPECT_EQ(*gated, *missing) << what << ": a hidden view and a missing id answer alike";
    }
    // Control (non-vacuity): a principal WITH READ on the view gets the gate code, not NotAuthorized.
    Rig granted(defaultGrants(), defaultImage(), /*withPool=*/false);
    granted.gw.onCatalogObjectApplied(kPlaced, "w.z", false);
    ASSERT_TRUE(granted.registry->dropTopic(kPlaced));
    const auto unknownView = static_cast<int16_t>(pc::ViewRowResponse::kUnknownView);
    EXPECT_EQ(granted.codeOf(granted.getViewRow(kPlaced)), unknownView);
    EXPECT_EQ(granted.codeOf(granted.getViewRowK(kPlaced)), unknownView);
    EXPECT_EQ(granted.codeOf(granted.scanViewRows(kPlaced)), unknownView);
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issue #506: a denial is not a verdict while this broker holds `__security` events its view has not applied (one of
// them may be the grant the request relies on): retriable AuthUnavailable, never the terminal NotAuthorized.  Deny path
// only - nothing is ever allowed on unapplied events.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
struct HeldEventsRig : Rig {
    rune::node::test::SecurityEventChain chain;
    SecurityView view;
    static rune::node::test::SecurityEventChain boot() {
        rune::node::test::SecurityEventChain c;
        (void)c.createUser("u0");
        return c;
    }
    HeldEventsRig() : Rig(std::vector<sec::GrantSpec>{}), chain(boot()), view(chain.state()) {
        dispatch = true;
        gw.setAuthMode(GatewayAuthMode::Required, &view);
    }
    int64_t newSession(std::vector<sec::GrantSpec> grants) {
        const int64_t s = gw.injectTestSession();
        gw.authenticateWithGrantsForTest(s, 43, std::move(grants));
        return s;
    }
    void applyNextEvent(const char* user) {
        const auto ev = chain.createUser(user);
        const auto bytes = sec::encode(ev);
        ASSERT_TRUE(bytes.has_value());
        const int64_t off = static_cast<int64_t>(chain.count()) - 1;
        auto applied = view.onApplied(off, *bytes);
        ASSERT_TRUE(applied.has_value()) << applied.error().detail;
        gw.onSecurityApplied(off, *applied);
    }
    int64_t dropTopic(std::string_view name) {
        pc::DropTopic m;
        m.sessionId = sid; m.correlationId = ++corr; m.topicName = name;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        (void)gw.applyClientFrameForTest(buf.data(), buf.size());
        return m.correlationId;
    }
    int64_t leo() const { return registry->log(0, 0)->logEndOffset(); }
};
}  // namespace

// #507: GetViewRow for a view this broker has not applied yet is retriable (NotReady), with auth on or off, whether
// or not the id is placed: it is at or above the applied frontier.  An id decided here keeps the terminal answers.
// Mutation: viewRowGate_ decides by placement again -> the never-placed id above the frontier is not NotReady (red).
TEST(GetViewRowAuthRegression, AViewNotAppliedHereIsRetriableNotTerminalRegression) {
    constexpr int32_t kAbove = 20, kDropped = 3;   // kPlaced (7) is the frontier: placed here, not applied
    const auto notReady    = static_cast<int16_t>(pc::ViewRowResponse::kNotReady);
    const auto unknownView = static_cast<int16_t>(pc::ViewRowResponse::kUnknownView);
    for (const bool authOn : {true, false}) {
        for (const bool pooled : {false, true}) {
            // Every view read path asks the same question: GetViewRow (id 0), GetViewRowK (1), ScanViewRows (2).
            auto answerBy = [&](int kind, int32_t id) -> std::optional<int16_t> {
                Rig r(authOn ? std::vector<sec::GrantSpec>{} : defaultGrants(), defaultImage(), pooled);
                // auth off: an anonymous session of a gateway that authenticates nobody is allow-all
                if (!authOn) r.sid = r.gw.injectTestSession();
                rune::node::TopicSpec dropped;
                dropped.name = "w.dropped"; dropped.partitionCount = 1;
                EXPECT_EQ(r.registry->createTopicAt(kDropped, dropped), kDropped);
                EXPECT_TRUE(r.registry->dropTopic(kDropped));
                return r.codeOf(kind == 0 ? r.getViewRow(id) : kind == 1 ? r.getViewRowK(id) : r.scanViewRows(id));
            };
            auto answer = [&](int32_t id) { return answerBy(0, id); };
            const std::string cell = std::string{authOn ? "auth on" : "auth off"} + (pooled ? ", pooled" : ", no pool");
            EXPECT_EQ(answer(kPlaced), notReady) << cell << ": at the frontier (placed, not applied)";
            EXPECT_EQ(answer(kAbove), notReady) << cell << ": above the frontier (never created)";
            for (int kind = 1; kind <= 2; ++kind) {
                EXPECT_EQ(answerBy(kind, kPlaced), notReady) << cell << ": read kind " << kind << " at the frontier";
                EXPECT_EQ(answerBy(kind, kAbove), notReady) << cell << ": read kind " << kind << " above the frontier";
            }
            const auto below = answer(kMissing), gone = answer(kDropped);
            if (authOn) {
                EXPECT_EQ(below, std::optional<int16_t>{kNotAuthorized}) << cell << ": below the frontier, unknown";
                EXPECT_EQ(gone, std::optional<int16_t>{kNotAuthorized}) << cell << ": below the frontier, dropped";
            } else if (!pooled) {
                EXPECT_EQ(below, std::optional<int16_t>{unknownView}) << cell << ": below the frontier, unknown";
                EXPECT_EQ(gone, std::optional<int16_t>{unknownView}) << cell << ": below the frontier, dropped";
            } else {
                EXPECT_FALSE(below.has_value()) << cell << ": no gate below the frontier: forwarded to the worker";
                EXPECT_EQ(gone, std::optional<int16_t>{unknownView}) << cell << ": dropped";
            }
        }
    }
    // After the apply (the sink is registered, the frontier moves past it) the view is served: not refused, forwarded.
    for (const bool authOn : {true, false}) {
        Rig r(authOn ? defaultGrants() : std::vector<sec::GrantSpec>{}, defaultImage(), /*withPool=*/true);
        if (!authOn) r.sid = r.gw.injectTestSession();
        rune::node::TopicSpec sink;
        sink.name = "w.v"; sink.partitionCount = 1; sink.viewSink = true;
        ASSERT_EQ(r.registry->createTopicAt(kPlaced, sink), kPlaced);
        r.gw.onCatalogObjectApplied(kPlaced, "w.v", true);
        r.gw.setAppliedTopicFrontier(kPlaced + 1);
        EXPECT_FALSE(r.codeOf(r.getViewRow(kPlaced)).has_value())
            << (authOn ? "auth on" : "auth off") << ": applied: no gateway refusal, forwarded to the worker";
    }
}

TEST(AuthUnappliedTopicRegression, ADenialWhileTheViewHoldsUnappliedEventsIsRetriableRegression) {
    HeldEventsRig r;
    const int64_t applied = r.view.appliedOffset();
    ASSERT_EQ(r.gw.objectIndex().at(0) != nullptr, true) << "precondition: the topic is applied here";
    const int64_t leo = r.leo();
    const int64_t noGrant = r.sid;
    r.gw.setSecurityKnownEnd(applied + 2);   // one event held by this broker, not applied: the grant may be in it
    const uint64_t refusedBefore = r.gw.notAuthorizedAnswers();

    // Step 2: the denied produce is retriable, reaches no sink, is not audited as a denial.
    EXPECT_EQ(r.codeOf(r.produce(0)), kAuthUnavailable);
    EXPECT_EQ(r.leo(), leo) << "nothing reaches the produce sink";
    EXPECT_EQ(r.gw.deniesDeferredViewBehind(), 1u);
    EXPECT_EQ(r.gw.notAuthorizedAnswers(), refusedBefore) << "a non-verdict is not audited as a denial";

    // Step 5: a DDL-family template (own response type, no object name in the detail) and a Subscribe.
    const auto drop = r.codeOf(r.dropTopic("w.boot"));
    ASSERT_TRUE(drop.has_value());
    EXPECT_EQ(*drop, kAuthUnavailable);
    EXPECT_EQ(r.lastDdlDetail.find("w.boot"), std::string::npos) << "the detail does not name the object";
    EXPECT_EQ(r.codeOf(r.subscribe(0)), kAuthUnavailable);
    EXPECT_EQ(r.gw.deniesDeferredViewBehind(), 3u);
    EXPECT_EQ(r.gw.notAuthorizedAnswers(), refusedBefore);

    // Step 6: a session WITH the grant is not affected (deny path only): its produce is appended.
    {
        const int64_t granted = r.newSession(defaultGrants());
        r.sid = granted;
        EXPECT_FALSE(r.codeOf(r.produce(0)).has_value()) << "an allowed acks=0 produce is forwarded, never answered";
        r.sid = noGrant;
    }
    EXPECT_EQ(r.gw.deniesDeferredViewBehind(), 3u);

    // Step 3: the event reaches the view (knownEnd stays applied + 2 -> still one held) and the session picks the
    // grant up: forwarded.
    r.applyNextEvent("u1");
    ASSERT_EQ(r.view.appliedOffset(), applied + 1);
    r.gw.authenticateWithGrantsForTest(noGrant, 42, defaultGrants());
    EXPECT_FALSE(r.codeOf(r.produce(0)).has_value()) << "forwarded once the grant is applied";

    // Step 4 (control): fully applied view, a session with no grant -> the terminal 45 as today.
    r.gw.setSecurityKnownEnd(r.view.appliedOffset() + 1);
    const uint64_t deferred = r.gw.deniesDeferredViewBehind();
    const uint64_t refusedControl = r.gw.notAuthorizedAnswers();
    r.sid = r.newSession({});
    EXPECT_EQ(r.codeOf(r.produce(0)), kNotAuthorized);
    EXPECT_EQ(r.gw.deniesDeferredViewBehind(), deferred);
    EXPECT_EQ(r.gw.notAuthorizedAnswers(), refusedControl + 1);
}
