// Catalog history (Task 5): what the client puts on the wire for every DDL op —
// DefineTopic's replicationFactor + topic config, AlterView's expectedVersion,
// AlterTopicConfig's changes, and the DDL extension (statement, clientName,
// 128-bit requestId, replayedFrom) on all seven ops (F14: hi = a random
// client-instance id, lo = a per-instance counter; an explicit id verbatim).
// Non-blocking ops on the single-threaded fake (F7).
#include <filesystem>
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

#include <functional>
#include <map>
#include <set>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace pc = rune::protocol::client;
namespace rc = rune::client;
using rune::protocol::RequestId;

namespace {
int g_ctr = 0;   // test thread only
std::vector<std::string> g_dirs;   // test thread only: every data dir invokerCfg handed out (#577: removed in TearDown)

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {   // as InvokerClientTest.cpp's invokerCfg
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = (std::filesystem::temp_directory_path() /
                 ("catalog_ddl_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++))).string();
    g_dirs.push_back(c.dataDir);
    c.runnerMode = rc::RunnerMode::Invoker;
    return c;
}
struct Rig {   // InvokerClientTest.cpp's Rig: client + fake on the client's embedded driver
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
    /// Captures every frame of `templateId` (bytes kept) and answers it with an
    /// InvalidRequest so the op completes (the content is what is under test).
    std::vector<std::vector<uint8_t>>& capture(uint16_t templateId) {
        auto& v = captured[templateId];
        gw.templateHandlers[templateId] = [this, &v](int ep, const uint8_t* f, std::size_t len) {
            v.emplace_back(f, f + len);
            const auto h = pc::MessageHeader::decode(f, len);
            gw.sendError(ep, static_cast<int16_t>(pc::ErrorCode::InvalidRequest), "captured",
                         h ? h->sessionId : 0, h ? h->correlationId : 0);
        };
        return v;
    }
    /// Issues one op and drives it to its completion.
    void run(const std::function<rc::OpResult(rc::Completions&, rc::Token)>& op) {
        test_support::TestCq cq(client);
        auto f = cq.issue(op);
        ASSERT_TRUE(f.accepted());
        ASSERT_TRUE(drive([&] { return f.ready(); }));
    }
    std::map<uint16_t, std::vector<std::vector<uint8_t>>> captured;
};

/// #577: each test's data dirs are removed when it ends (its Rigs are destroyed by then), pass or fail.
class ClientCatalogDdl : public ::testing::Test {
protected:
    void TearDown() override {
        std::error_code ec;
        for (const std::string& d : g_dirs) std::filesystem::remove_all(d, ec);
        g_dirs.clear();
    }
};

const uint8_t kBlob[] = {9, 8, 7};
const std::span<const uint8_t> blob(kBlob, sizeof kBlob);
const pc::ConfigChangeWire kChanges[] = {{1, 60'000}, {6, -1}};
} // namespace

TEST_F(ClientCatalogDdl, DefineTopicSendsReplicationFactorConfigAndExtension) {
    rc::ClientConfig cfg = invokerCfg(3980, 3981);
    cfg.clientName = "ql-test";
    Rig r(cfg);
    r.connect();
    auto& frames = r.capture(pc::DefineTopic::TEMPLATE_ID);
    rc::TopicOptions topic;
    topic.replicationFactor          = 3;
    topic.config.cleanupPolicy       = 1;
    topic.config.retentionMs         = 1234;
    topic.config.durabilityTier      = 2;
    topic.config.coPartitionGroup    = "grp";
    topic.config.partitionKey        = "player";
    rc::DdlOptions ddl;
    ddl.statement = "CREATE TOPIC t PARTITIONS 4";
    r.run([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("t", 4, "player", {}, rc::Persistence::Transient, topic, ddl, q, t);
    });
    ASSERT_EQ(frames.size(), 1u);
    const auto m = pc::DefineTopic::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->topicName, "t");
    EXPECT_EQ(m->partitionCount, 4u);
    EXPECT_EQ(m->keyField, "player");
    EXPECT_EQ(m->descriptorLen, 0u) << "an empty descriptor = an untyped topic";
    EXPECT_TRUE(m->validate);
    EXPECT_EQ(m->persistence, pc::DefineTopic::kPersistenceTransient);
    ASSERT_TRUE(m->hasExt);
    EXPECT_EQ(m->replicationFactor, 3);
    EXPECT_EQ(m->config, (rune::protocol::convertTopicConfig<pc::TopicConfigWire>(topic.config)));
    EXPECT_EQ(m->ext.statement, "CREATE TOPIC t PARTITIONS 4");
    EXPECT_EQ(m->ext.clientName, "ql-test") << "defaults to ClientConfig::clientName";
    EXPECT_FALSE(m->ext.requestId.none());
    EXPECT_EQ(m->ext.hasReplayedFrom, 0);
}

TEST_F(ClientCatalogDdl, DdlOptionsClientNameOverridesTheConfigAndReplayedFromIsCarried) {
    Rig r(invokerCfg(3982, 3983));
    r.connect();
    auto& frames = r.capture(pc::DropTopic::TEMPLATE_ID);
    rc::DdlOptions ddl;
    ddl.clientName   = "rune-catalog-replay";
    ddl.statement    = "DROP TOPIC t";
    ddl.replayedFrom = rc::DdlOptions::ReplayedFrom{"cluster-a", 77, "alice@10.0.0.1"};
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", ddl, q, t); });
    ASSERT_EQ(frames.size(), 1u);
    const auto m = pc::DropTopic::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->topicName, "t");
    EXPECT_EQ(m->ext.clientName, "rune-catalog-replay");
    EXPECT_EQ(m->ext.statement, "DROP TOPIC t");
    ASSERT_EQ(m->ext.hasReplayedFrom, 1);
    EXPECT_EQ(m->ext.replayedClusterId, "cluster-a");
    EXPECT_EQ(m->ext.replayedOffset, 77);
    EXPECT_EQ(m->ext.replayedPrincipal, "alice@10.0.0.1");
    // Review M4: the generated requestId is stamped on the replayedFrom branch
    // too (the requestId then sits before the replayed fields, not at the tail).
    EXPECT_FALSE(m->ext.requestId.none());
    EXPECT_EQ(m->ext.requestId.hi, r.client.instanceId());
    EXPECT_NE(m->ext.requestId.lo, 0u);
}

TEST_F(ClientCatalogDdl, AlterViewSendsItsExpectedVersionKeyPersistenceAndPlan) {
    Rig r(invokerCfg(3984, 3985));
    r.connect();
    auto& frames = r.capture(pc::AlterView::TEMPLATE_ID);
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.alterView("v", "k", 1, 7, blob, {}, q, t); });
    ASSERT_EQ(frames.size(), 1u);
    const auto m = pc::AlterView::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->viewName, "v");
    EXPECT_EQ(m->keyField, "k");
    EXPECT_EQ(m->persistence, 1);
    EXPECT_EQ(m->expectedVersion, 7u);
    ASSERT_EQ(m->planLen, sizeof kBlob);
    EXPECT_EQ(std::vector<uint8_t>(m->planBlob, m->planBlob + m->planLen), std::vector<uint8_t>(blob.begin(), blob.end()));
    EXPECT_EQ(m->ext.clientName, "rune-client") << "the ClientConfig default";
}

TEST_F(ClientCatalogDdl, AlterTopicConfigSendsEveryChange) {
    Rig r(invokerCfg(3986, 3987));
    r.connect();
    auto& frames = r.capture(pc::AlterTopicConfig::TEMPLATE_ID);
    r.run([&](rc::Completions& q, rc::Token t) {
        return r.client.alterTopicConfig("t", 5, std::span<const pc::ConfigChangeWire>(kChanges), {}, q, t);
    });
    ASSERT_EQ(frames.size(), 1u);
    const auto m = pc::AlterTopicConfig::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->topicName, "t");
    EXPECT_EQ(m->expectedVersion, 5u);
    ASSERT_EQ(pc::configChangeCount(m->changes), 2u);
    EXPECT_EQ(pc::configChangeAt(m->changes, 0), kChanges[0]);
    EXPECT_EQ(pc::configChangeAt(m->changes, 1), kChanges[1]);
}

TEST_F(ClientCatalogDdl, EveryDdlOpCarriesADistinctRequestIdUnderOneInstanceId) {
    Rig r(invokerCfg(3988, 3989));
    r.connect();
    std::vector<RequestId> ids;
    auto& define = r.capture(pc::DefineTopic::TEMPLATE_ID);
    auto& alter  = r.capture(pc::AlterTopic::TEMPLATE_ID);
    auto& create = r.capture(pc::CreateView::TEMPLATE_ID);
    auto& dropT  = r.capture(pc::DropTopic::TEMPLATE_ID);
    auto& dropV  = r.capture(pc::DropView::TEMPLATE_ID);
    auto& alterV = r.capture(pc::AlterView::TEMPLATE_ID);
    auto& alterC = r.capture(pc::AlterTopicConfig::TEMPLATE_ID);
    // The pre-catalog overloads (no DdlOptions) carry the extension too.
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.defineTopic("t", 1, "", {}, q, t); });
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.alterTopic("t", 1, blob, q, t); });
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.createView("v", "k", 0, blob, q, t); });
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.dropView("v", {}, q, t); });
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.alterView("v", "k", 0, 1, blob, {}, q, t); });
    r.run([&](rc::Completions& q, rc::Token t) {
        return r.client.alterTopicConfig("t", 1, std::span<const pc::ConfigChangeWire>(kChanges), {}, q, t);
    });
    ASSERT_EQ(define.size() + alter.size() + create.size() + dropT.size() + dropV.size() + alterV.size() + alterC.size(), 7u);
    { auto m = pc::DefineTopic::decode(define[0].data(), define[0].size()); ASSERT_TRUE(m && m->hasExt); ids.push_back(m->ext.requestId); }
    { auto m = pc::AlterTopic::decode(alter[0].data(), alter[0].size()); ASSERT_TRUE(m && m->hasExt); ids.push_back(m->ext.requestId); }
    { auto m = pc::CreateView::decode(create[0].data(), create[0].size()); ASSERT_TRUE(m && m->hasExt); ids.push_back(m->ext.requestId); }
    { auto m = pc::DropTopic::decode(dropT[0].data(), dropT[0].size()); ASSERT_TRUE(m); ids.push_back(m->ext.requestId); }
    { auto m = pc::DropView::decode(dropV[0].data(), dropV[0].size()); ASSERT_TRUE(m); ids.push_back(m->ext.requestId); }
    { auto m = pc::AlterView::decode(alterV[0].data(), alterV[0].size()); ASSERT_TRUE(m); ids.push_back(m->ext.requestId); }
    { auto m = pc::AlterTopicConfig::decode(alterC[0].data(), alterC[0].size()); ASSERT_TRUE(m); ids.push_back(m->ext.requestId); }
    std::set<RequestId> distinct;
    for (const RequestId& id : ids) {
        EXPECT_FALSE(id.none());
        EXPECT_NE(id.hi, 0u) << "the random client-instance id is never 0";
        EXPECT_EQ(id.hi, ids.front().hi) << "one instance id per Client";
        distinct.insert(id);
    }
    EXPECT_EQ(distinct.size(), ids.size()) << "every DDL gets its own requestId";
}

TEST_F(ClientCatalogDdl, TwoClientsDrawDifferentInstanceIds) {
    Rig a(invokerCfg(3990, 3991));
    Rig b(invokerCfg(3992, 3993));
    a.connect();
    b.connect();
    auto& fa = a.capture(pc::DropTopic::TEMPLATE_ID);
    auto& fb = b.capture(pc::DropTopic::TEMPLATE_ID);
    a.run([&](rc::Completions& q, rc::Token t) { return a.client.dropTopic("t", {}, q, t); });
    b.run([&](rc::Completions& q, rc::Token t) { return b.client.dropTopic("t", {}, q, t); });
    ASSERT_EQ(fa.size(), 1u);
    ASSERT_EQ(fb.size(), 1u);
    const auto ma = pc::DropTopic::decode(fa[0].data(), fa[0].size());
    const auto mb = pc::DropTopic::decode(fb[0].data(), fb[0].size());
    ASSERT_TRUE(ma && mb);
    EXPECT_NE(ma->ext.requestId.hi, mb->ext.requestId.hi) << "F14: a random 64-bit instance id per Client";
}

TEST_F(ClientCatalogDdl, AnExplicitRequestIdIsSentVerbatim) {
    Rig r(invokerCfg(3994, 3995));
    r.connect();
    auto& frames = r.capture(pc::CreateView::TEMPLATE_ID);
    rc::DdlOptions ddl;
    ddl.requestId = RequestId{0xABCDEF0123456789ull, 42};
    r.run([&](rc::Completions& q, rc::Token t) { return r.client.createView("v", "k", 0, blob, ddl, q, t); });
    ASSERT_EQ(frames.size(), 1u);
    const auto m = pc::CreateView::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(m && m->hasExt);
    EXPECT_EQ(m->ext.requestId, ddl.requestId);
}

TEST_F(ClientCatalogDdl, OversizeFieldsAreRejectedSynchronously) {
    Rig r(invokerCfg(3996, 3997));
    test_support::TestCq cq(r.client);
    const std::string huge(70'000, 'x');   // > a u16 varstr
    rc::DdlOptions ddl;
    ddl.clientName = huge;
    EXPECT_EQ(r.client.dropTopic("t", ddl, cq.q(), 1), rc::OpResult::InvalidArgument);
    EXPECT_EQ(r.client.dropView(huge, {}, cq.q(), 2), rc::OpResult::InvalidArgument);
    std::vector<pc::ConfigChangeWire> tooMany(70'000, pc::ConfigChangeWire{1, 0});
    EXPECT_EQ(r.client.alterTopicConfig("t", 0, tooMany, {}, cq.q(), 3), rc::OpResult::InvalidArgument);
}
