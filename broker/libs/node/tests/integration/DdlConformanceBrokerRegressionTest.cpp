// DdlConformanceBrokerRegressionTest.cpp — DDL conformance plan Task 12.
//
// A REAL single-node BrokerNode over REAL Aeron (embedded drivers), driven by the
// wire client ShardTestClient.  One representative slice per timing class:
//   T1  sequential collisions answer the exact codes (incl. the name rule);
//   T2  pipelined same-name DDL from two sessions: exactly one wins (FIFO);
//   T3/T4 a 30-frame interleaved burst: every answer matches the expectation table;
//   T5  a failed control reply (before / after creating) + B5: the broker restarts;
//   T7  catalog + collisions are identical across a restart.
// T6 / 3-node failover: out of scope (NullPartitionTransport catalogs; KL4).
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include "support/DdlHarness.h"          // charBlob / viewPlanBlob / alterOpsFor / the table
#include <rune/node/RuntimeTopicRecovery.h>   // orphanPlaceholderName
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include "support/OwnerThreadTestHelpers.h"   // leoOnOwner
#include "support/ShardTestClient.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

fs::path makeTempDataDir(std::string_view tag) {
    static std::atomic<uint32_t> seq{0};
    return fs::temp_directory_path() / (std::string("rune_ddlbn_") + std::string(tag) +
                                        std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
}

/// The test's data dir, removed on scope exit — also when an ASSERT returns early
/// (declared first in each test, so it outlives every BrokerNode / client in it).
struct TempDataDir {
    fs::path path;
    explicit TempDataDir(std::string_view tag) : path(makeTempDataDir(tag)) {}
    ~TempDataDir() { std::error_code ec; fs::remove_all(path, ec); }
    TempDataDir(const TempDataDir&) = delete;
    TempDataDir& operator=(const TempDataDir&) = delete;
};

NodeConfig makeSingleNodeConfig(const fs::path& dataDir) {
    NodeConfig cfg;
    pinLegacyRuntime(cfg);
    cfg.nodeId = 0;
    cfg.dataDir = dataDir.string();
    cfg.consensusStreamId = 10;
    PeerConfig self;
    self.id = 0;
    self.host = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
    cfg.peers = {self};
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" +
                                 std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;
    cfg.workerCount = workerCountFromEnv();
    return cfg;
}

struct Wire {
    ShardTestClient& c;
    std::string gw;
    int32_t stream;

    int define(int64_t sid, const std::string& n, uint8_t persistence = 0, uint32_t parts = 1) {
        const auto r = c.awaitDefine(c.sendDefineTopic(gw, stream, sid, n, parts, charBlob(), persistence),
                                     Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    std::optional<ShardTestClient::SchemaAnswer> schema(int64_t sid, const std::string& n) {
        return c.awaitSchema(c.sendGetTopicSchema(gw, stream, sid, n), Clock::now() + 10s);
    }
    int view(int64_t sid, const std::string& n, const std::string& src, uint8_t persistence = 0) {
        const auto s = schema(sid, src);
        const int32_t srcId = s && s->errorCode == 0 ? s->topicId : -1;
        const auto r = c.awaitCreateView(
            c.sendCreateView(gw, stream, sid, n, "id", persistence, viewPlanBlob(n, srcId, persistence)),
            Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    int alter(int64_t sid, const std::string& n, uint16_t expectedVersion = 0) {
        const auto r = c.awaitAlter(
            c.sendAlterTopic(gw, stream, sid, n, expectedVersion, alterOpsFor(Op::AlterAdd, static_cast<uint64_t>(Clock::now().time_since_epoch().count()))),
            Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    int renameKey(int64_t sid, const std::string& n) {
        const auto r = c.awaitAlter(c.sendAlterTopic(gw, stream, sid, n, 0, alterOpsFor(Op::AlterRenameKey, 0)),
                                    Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    /// The admin thread answers Internal until it leads __topic_schemas: retry a
    /// probe define (bounded) — the documented settle, not a correctness gate.
    bool awaitDdlReady(int64_t sid) {
        const auto deadline = Clock::now() + 20s;
        while (Clock::now() < deadline) {
            const int code = define(sid, "ddl_ready");
            if (code == kOk || code == kTopicExists) return true;
            if (code != kInternal) return false;
            std::this_thread::sleep_for(20ms);   // backoff while leadership settles
        }
        return false;
    }
};

} // namespace

TEST(DdlConformanceBrokerRegression, T1_SequentialCollisionsAnswerExactCodes) {
    const TempDataDir tmp("t1_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_GT(sid, 0);
        ASSERT_TRUE(w.awaitDdlReady(sid));

        EXPECT_EQ(w.define(sid, "character"), kOk);
        EXPECT_EQ(w.define(sid, "character"), kTopicExists);
        EXPECT_EQ(w.define(sid, "character", 1), kTopicExists);
        EXPECT_EQ(w.view(sid, "char_hp", "character"), kOk);
        EXPECT_EQ(w.view(sid, "char_hp", "character"), kViewExists);
        EXPECT_EQ(w.define(sid, "char_hp"), kTopicExists);
        EXPECT_EQ(w.alter(sid, "char_hp"), kViewNotAlterable);
        ASSERT_TRUE(w.schema(sid, "char_hp").has_value());
        EXPECT_EQ(w.schema(sid, "char_hp")->errorCode, kOk);
        EXPECT_EQ(w.alter(sid, "character"), kOk);
        EXPECT_EQ(w.alter(sid, "character", 1), kSchemaConflict);
        EXPECT_EQ(w.view(sid, "hp_of_hp", "char_hp"), kViewSourceIsView) << "B9: a view over a view";
        EXPECT_EQ(w.renameKey(sid, "character"), kOk) << "B8: RENAME id -> pk";
        EXPECT_EQ(w.define(sid, ""), kInvalidTopicName);
        EXPECT_EQ(w.define(sid, "__views"), kInvalidTopicName);
        EXPECT_EQ(w.define(sid, "a/b"), kInvalidTopicName);
        EXPECT_EQ(w.define(sid, "Character"), kTopicExists);
        EXPECT_EQ(w.define(sid, "zero", 0, 0), kInvalidSchema);
        ASSERT_TRUE(w.schema(sid, "__topic_schemas").has_value());
        EXPECT_EQ(w.schema(sid, "__topic_schemas")->errorCode, kUnknownTopic);
        node.stop();
        EXPECT_EQ(node.schemaRegistryForTest()->byName("Character"), nullptr);
        EXPECT_EQ(node.schemaRegistryForTest()->byName("character")->version, 3u);
        EXPECT_EQ(node.schemaRegistryForTest()->byName("character")->keyField, "pk") << "B8";
        EXPECT_EQ(node.viewRegistryForTest()->byName("hp_of_hp"), nullptr) << "B9: nothing created";
        EXPECT_NE(node.viewRegistryForTest()->byName("char_hp"), nullptr);
    }
    EXPECT_FALSE(fs::exists(dir / "topics" / "a")) << "no directory for a rejected name";
}

TEST(DdlConformanceBrokerRegression, T2_PipelinedSameNameFromTwoSessionsExactlyOneWins) {
    const TempDataDir tmp("t2_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t s1 = client.connect(w.gw, w.stream);
    const int64_t s2 = client.connect(w.gw, w.stream);
    ASSERT_GT(s1, 0); ASSERT_GT(s2, 0);
    ASSERT_TRUE(w.awaitDdlReady(s1));
    ASSERT_EQ(w.define(s1, "character"), kOk);
    const auto src = w.schema(s1, "character");
    ASSERT_TRUE(src.has_value());

    // Sent back to back, no reply awaited in between: one publication => the gateway
    // (and the admin FIFO) sees them in send order.
    const int64_t d1 = client.sendDefineTopic(w.gw, w.stream, s1, "race", 1, charBlob(), 0);
    const int64_t d2 = client.sendDefineTopic(w.gw, w.stream, s2, "race", 1, charBlob(), 1);
    const int64_t v1 = client.sendCreateView(w.gw, w.stream, s1, "rview", "id", 0, viewPlanBlob("rview", src->topicId, 0));
    const int64_t v2 = client.sendCreateView(w.gw, w.stream, s2, "rview", "id", 1, viewPlanBlob("rview", src->topicId, 1));
    const int64_t m1 = client.sendDefineTopic(w.gw, w.stream, s1, "rmix", 1, charBlob(), 0);
    const int64_t m2 = client.sendCreateView(w.gw, w.stream, s2, "rmix", "id", 0, viewPlanBlob("rmix", src->topicId, 0));
    const auto deadline = Clock::now() + 15s;
    const auto rd1 = client.awaitDefine(d1, deadline), rd2 = client.awaitDefine(d2, deadline);
    const auto rv1 = client.awaitCreateView(v1, deadline), rv2 = client.awaitCreateView(v2, deadline);
    const auto rm1 = client.awaitDefine(m1, deadline);
    const auto rm2 = client.awaitCreateView(m2, deadline);
    ASSERT_TRUE(rd1 && rd2 && rv1 && rv2 && rm1 && rm2) << "every pipelined DDL is answered";
    EXPECT_EQ(rd1->errorCode, kOk);          EXPECT_EQ(rd2->errorCode, kTopicExists);
    EXPECT_EQ(rv1->errorCode, kOk);          EXPECT_EQ(rv2->errorCode, kViewExists);
    EXPECT_EQ(rm1->errorCode, kOk);          EXPECT_EQ(rm2->errorCode, kTopicExists);
    node.stop();
    EXPECT_EQ(node.schemaRegistryForTest()->byName("race")->persistence, PersistenceMode::Disk)
        << "the winner's mode, never flipped by the loser";
}

TEST(DdlConformanceBrokerRegression, T3T4_InterleavedBurstMatchesTheTable) {
    const TempDataDir tmp("burst_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t s1 = client.connect(w.gw, w.stream);
    const int64_t s2 = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(s1));
    ASSERT_EQ(w.define(s1, "character"), kOk);
    const int32_t srcId = w.schema(s1, "character")->topicId;

    const Op script[] = {Op::DefineDisk, Op::ViewPersisted, Op::DefineTransient, Op::AlterAdd, Op::ViewTransient,
                         Op::DefineDisk, Op::AlterAdd, Op::ViewPersisted, Op::DefineTransient, Op::AlterAdd};
    struct Sent { Op op; std::string name; int64_t corr; };
    std::vector<Sent> sent;
    for (int i = 0; i < 30; ++i) {
        const Op op = script[i % 10];
        const std::string name = "b" + std::to_string(i % 3);
        const int64_t sid = (i % 2) ? s2 : s1;
        int64_t corr = 0;
        if (isDefine(op)) corr = client.sendDefineTopic(w.gw, w.stream, sid, name, 1, charBlob(), op == Op::DefineTransient ? 1 : 0);
        else if (isView(op)) {
            const uint8_t p = op == Op::ViewTransient ? 1 : 0;
            corr = client.sendCreateView(w.gw, w.stream, sid, name, "id", p, viewPlanBlob(name, srcId, p));
        } else corr = client.sendAlterTopic(w.gw, w.stream, sid, name, 0, alterOpsFor(Op::AlterAdd, static_cast<uint64_t>(i)));
        ASSERT_NE(corr, 0);
        sent.push_back({op, name, corr});
    }
    std::map<std::string, Occupant> model;   // sequential FIFO model over the table
    const auto deadline = Clock::now() + 30s;
    for (std::size_t i = 0; i < sent.size(); ++i) {
        const Sent& s = sent[i];
        const Occupant o = model.count(s.name) ? model[s.name] : Occupant::Free;
        const Cell want = expectedCell(o, s.op);
        int got = -1;
        if (isDefine(s.op)) { if (auto r = client.awaitDefine(s.corr, deadline)) got = r->errorCode; }
        else if (isView(s.op)) { if (auto r = client.awaitCreateView(s.corr, deadline)) got = r->errorCode; }
        else { if (auto r = client.awaitAlter(s.corr, deadline)) got = r->errorCode; }
        EXPECT_EQ(got, want.code) << "frame #" << i << " " << opName(s.op) << " '" << s.name << "'";
        if (got == kOk) model[s.name] = occupantAfterSuccess(o, s.op);
    }
    node.stop();
}

TEST(DdlConformanceBrokerRegression, T5_FailedCreateLeavesAnOrphanAndTheBrokerStillRestarts) {
    const TempDataDir tmp("t5_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t laterId = -1;
    {
        BrokerNode node{cfg};
        node.setAdminCreateFaultForTest("orphan_v", /*afterCreate=*/true);   // T5b: sink created, reply fails
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "character"), kOk);
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kInternal);
        ASSERT_EQ(w.define(sid, "later"), kOk);   // a later recorded id: the orphan is a gap
        laterId = w.schema(sid, "later")->topicId;
        // Task 14: no retry here — a retry would adopt the orphan sink (see
        // T5_RetryAdoptsItsOwnOrphanSinkAndSurvivesRestart); this test keeps the orphan
        // unclaimed across the restart to prove the placeholder + dir deletion.
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();   // B5: must boot
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        const auto later = w.schema(sid, "later");
        ASSERT_TRUE(later.has_value());
        EXPECT_EQ(later->errorCode, kOk);
        EXPECT_EQ(later->topicId, laterId) << "a recorded topic keeps its id across the gap";
        EXPECT_FALSE(fs::exists(dir / "topics" / "orphan_v")) << "B10: the orphan sink's data dir was deleted at boot";

        // D1: the placeholder for the orphan's id is hidden and inert.
        const int32_t orphanId = laterId - 1;   // ids: ddl_ready, character, orphan_v, later
        const std::string ph = orphanPlaceholderName(orphanId);
        ASSERT_TRUE(w.schema(sid, ph).has_value());
        EXPECT_EQ(w.schema(sid, ph)->errorCode, kUnknownTopic) << "hidden from schema gets";
        for (const PartitionInfo& p : client.metadata(w.gw, w.stream, sid))
            EXPECT_NE(p.topicId, orphanId) << "hidden from the metadata listing";
        const int64_t pc = client.nextCorrelationId();
        ASSERT_TRUE(client.produce(w.gw, w.stream, sid, pc, orphanId, 0, /*acks=*/1,
                                   [] { rune::protocol::RecordBatchBuilder b(0, 0);
                                        b.append(std::string_view{}, std::string_view{"x"}, 0);
                                        return b.build(); }()));
        const auto ack = client.awaitProduceResponse(pc);
        ASSERT_TRUE(ack.has_value()) << "a produce to the placeholder is answered";
        EXPECT_NE(ack->errorCode, 0) << "never accepts produce";
        const auto sub = client.awaitSubscribeCode(client.sendSubscribe(w.gw, w.stream, sid, orphanId, 0),
                                                   Clock::now() + 10s);
        ASSERT_TRUE(sub.has_value()) << "a subscribe to the placeholder is answered";
        EXPECT_NE(*sub, 0) << "never accepts subscribe";
        EXPECT_EQ(w.define(sid, ph), kInvalidTopicName) << "never accepts DDL (reserved __ name)";
        EXPECT_EQ(w.alter(sid, ph), kUnknownTopic);
        EXPECT_EQ(w.view(sid, ph, "character"), kInvalidTopicName);

        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kOk) << "the orphan's name is free after restart";
        EXPECT_EQ(w.define(sid, "character"), kTopicExists);
        node.stop();
    }
}

// Task 14 (KL1 follow-up): the same failed-after-create CREATE VIEW, then a retry
// on the real broker — it adopts its own orphan sink (same id), the adopting commit
// promotes the dir marker, and the view survives a restart (never a placeholder).
TEST(DdlConformanceBrokerRegression, T5_RetryAdoptsItsOwnOrphanSinkAndSurvivesRestart) {
    const TempDataDir tmp("t5adopt_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t sinkId = -1;
    {
        BrokerNode node{cfg};
        node.setAdminCreateFaultForTest("orphan_v", /*afterCreate=*/true);   // T5b: sink created, reply fails
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "character"), kOk);
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kInternal);
        EXPECT_TRUE(fs::exists(dir / "topics" / "orphan_v")) << "the failed create left its sink behind";
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kOk) << "the retry adopts its own orphan sink";
        const auto s = w.schema(sid, "orphan_v");
        ASSERT_TRUE(s.has_value());
        ASSERT_EQ(s->errorCode, kOk);
        sinkId = s->topicId;
        ASSERT_EQ(w.define(sid, "later"), kOk);
        EXPECT_EQ(w.schema(sid, "later")->topicId, sinkId + 1) << "the adopted sink kept its id; no new topic";
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kViewExists);
        node.stop();
    }
    {
        std::ifstream in(dir / "topics" / "orphan_v" / rune::gateway::kTopicIdMarkerFile, std::ios::binary);
        const std::string marker((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        EXPECT_EQ(marker, "committed " + std::to_string(sinkId) + "\n") << "the adopting commit promoted the marker";
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        EXPECT_TRUE(fs::exists(dir / "topics" / "orphan_v")) << "an adopted sink's dir is never reclaimed at boot";
        const auto s = w.schema(sid, "orphan_v");
        ASSERT_TRUE(s.has_value());
        EXPECT_EQ(s->errorCode, kOk);
        EXPECT_EQ(s->topicId, sinkId) << "the view keeps its sink id across the restart";
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kViewExists);
        node.stop();
    }
}

TEST(DdlConformanceBrokerRegression, T5a_ReplyFailsBeforeCreatingThenRetrySucceeds) {
    const TempDataDir tmp("t5a_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.setAdminCreateFaultForTest("x", /*afterCreate=*/false);
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t sid = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(sid));
    EXPECT_EQ(w.define(sid, "x"), kInternal);
    EXPECT_EQ(w.define(sid, "x"), kOk) << "nothing was created; the retry creates it";
    node.stop();
}

TEST(DdlConformanceBrokerRegression, T7_RestartPreservesTheCatalogAndTheCollisions) {
    const TempDataDir tmp("t7_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    const std::vector<std::string> names = {"t_disk", "t_tr", "v_p", "v_t"};
    std::map<std::string, ShardTestClient::SchemaAnswer> before;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "t_disk"), kOk);
        ASSERT_EQ(w.define(sid, "t_tr", 1), kOk);
        ASSERT_EQ(w.view(sid, "v_p", "t_disk"), kOk);
        ASSERT_EQ(w.view(sid, "v_t", "t_tr", 1), kOk);
        ASSERT_EQ(w.alter(sid, "t_disk"), kOk);
        ASSERT_EQ(w.renameKey(sid, "t_tr"), kOk) << "B8";
        for (const std::string& n : names) before[n] = *w.schema(sid, n);
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        for (const std::string& n : names) {
            const auto now = w.schema(sid, n);
            ASSERT_TRUE(now.has_value()) << n;
            EXPECT_EQ(*now, before[n]) << n << ": id/version/partitions/descriptor identical after restart";
        }
        EXPECT_EQ(w.define(sid, "t_disk"), kTopicExists);
        EXPECT_EQ(w.define(sid, "t_tr", 1), kTopicExists);
        EXPECT_EQ(w.view(sid, "v_p", "t_disk"), kViewExists);
        EXPECT_EQ(w.alter(sid, "v_p"), kViewNotAlterable);
        EXPECT_EQ(w.view(sid, "v_of_v", "v_p"), kViewSourceIsView) << "B9 after restart";
        node.stop();
        EXPECT_EQ(node.schemaRegistryForTest()->byName("t_tr")->keyField, "pk") << "B8 survives the restart";
    }
}

// D4 (amended): a static topic removed from RUNE_TOPICS is QUARANTINED at boot —
// moved to dataDir/quarantine/<name>-<ts>/ with its data intact and readable — and
// re-adding it starts EMPTY (no automatic restore; the restore is a manual move).
TEST(DdlConformanceBrokerRegression, T8_RemovedStaticTopicIsQuarantinedNotDeleted) {
    const TempDataDir tmp("t8_");
    const fs::path& dir = tmp.path;
    NodeConfig cfg = makeSingleNodeConfig(dir);
    TopicSpec events; events.name = "events"; events.partitionCount = 1; events.topicId = 0;
    cfg.topics = {events};
    {   // boot 1: produce one record to the static topic
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        const int64_t sid = client.connect(cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId);
        ASSERT_GT(sid, 0);
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"kept-record"}, 0);
        ASSERT_TRUE(client.produceUntilOk(cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId, sid,
                                          /*topicId=*/0, /*partition=*/0, /*acks=*/1, b.build()).has_value());
        node.stop();
    }
    fs::path quarantined;
    {   // boot 2: "events" removed from RUNE_TOPICS
        NodeConfig cfg2 = cfg;
        cfg2.topics.clear();
        BrokerNode node{cfg2};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        node.stop();
        EXPECT_FALSE(fs::exists(dir / "topics" / "events")) << "moved out of topics/";
        for (const auto& e : fs::directory_iterator(dir / "quarantine"))
            if (e.path().filename().string().starts_with("events-")) quarantined = e.path();
        ASSERT_FALSE(quarantined.empty()) << "quarantined, never deleted";
        rune::storage::Log moved((quarantined / "0").string());
        moved.recover();
        EXPECT_GE(moved.logEndOffset(), 1) << "data intact";
        const std::vector<uint8_t> raw = moved.read(0, 1 << 20);
        EXPECT_NE(std::string(raw.begin(), raw.end()).find("kept-record"), std::string::npos) << "and readable";
    }
    {   // boot 3: "events" re-added — a fresh, EMPTY topic; the quarantine is untouched
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        EXPECT_EQ(rune::node::test::leoOnOwner(node, 0, 0), 0) << "no automatic restore";
        node.stop();
        EXPECT_TRUE(fs::exists(quarantined / "0")) << "the quarantined copy stays until restored by hand";
    }
}

// ── Final review C1 / I1 / I3 / M7 (final fix wave) ─────────────────────────────
namespace {

constexpr int16_t kNotLeaderCode = 2;   // rune::protocol::client::ErrorCode::NotLeader (retriable)

std::vector<uint8_t> oneRecord(std::string_view value) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, value, 0);
    return b.build();
}

/// One produce, no retry: its ProduceResponse code, or the code of an ErrorResponse
/// on its correlation id (nullopt = unanswered within 10 s).
std::optional<int16_t> produceOnce(ShardTestClient& c, const Wire& w, int64_t sid, int32_t topicId,
                                   std::string_view value) {
    const int64_t corr = c.nextCorrelationId();
    if (!c.produce(w.gw, w.stream, sid, corr, topicId, 0, /*acks=*/1, oneRecord(value))) return std::nullopt;
    const auto deadline = Clock::now() + 10s;
    std::optional<int16_t> code;
    while (!code && Clock::now() < deadline) {
        (void)c.pollUntil([&] {
            if (auto a = c.ackFor(corr)) { code = a->errorCode; return true; }
            for (const auto& e : c.errors()) if (e.correlationId == corr) { code = e.code; return true; }
            return false;
        });
    }
    return code;
}

bool listed(ShardTestClient& c, const Wire& w, int64_t sid, int32_t topicId) {
    for (const PartitionInfo& p : c.metadata(w.gw, w.stream, sid))
        if (p.topicId == topicId) return true;
    return false;
}

std::size_t logBytes(const fs::path& topicDir) {
    std::size_t n = 0;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(topicDir, ec), end; !ec && it != end; it.increment(ec))
        if (it->path().extension() == ".log") n += static_cast<std::size_t>(fs::file_size(it->path(), ec));
    return n;
}

} // namespace

// C1: a runtime topic whose catalog record never committed (a DefineTopic whose
// control reply failed after the create) is neither writable nor readable: produce
// and subscribe are refused NotLeader (retriable, never silent), metadata hides it.
// Nothing can be acked into it, so boot deletes its (empty) dir — and a committed
// topic created later serves at once.
TEST(DdlConformanceBrokerRegression, C1_UncommittedTopicIsNeitherWritableNorReadableRegression) {
    const TempDataDir tmp("c1_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t orphanId = -1, laterId = -1;
    {
        BrokerNode node{cfg};
        node.setAdminCreateFaultForTest("orph", /*afterCreate=*/true);
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        EXPECT_EQ(w.define(sid, "orph"), kInternal);
        ASSERT_EQ(w.define(sid, "later"), kOk);
        laterId = w.schema(sid, "later")->topicId;
        orphanId = laterId - 1;   // ids: ddl_ready, orph, later
        ASSERT_TRUE(fs::exists(dir / "topics" / "orph")) << "the failed create left its topic behind";

        const auto p = produceOnce(client, w, sid, orphanId, "must-not-land");
        ASSERT_TRUE(p.has_value()) << "a produce to an uncommitted topic is answered (never silent)";
        EXPECT_EQ(*p, kNotLeaderCode) << "refused, retriable";
        const auto sub = client.awaitSubscribeCode(client.sendSubscribe(w.gw, w.stream, sid, orphanId, 0),
                                                   Clock::now() + 10s);
        ASSERT_TRUE(sub.has_value()) << "a subscribe to an uncommitted topic is answered";
        EXPECT_EQ(*sub, kNotLeaderCode) << "refused, retriable";
        EXPECT_FALSE(listed(client, w, sid, orphanId)) << "hidden from metadata";
        EXPECT_EQ(leoOrMinusOne(node, orphanId, 0), -1) << "no replica: nothing can be routed to it";

        // M7: the committed topic is listed and serves on the FIRST produce after its OK.
        EXPECT_TRUE(listed(client, w, sid, laterId));
        const auto ok = produceOnce(client, w, sid, laterId, "kept");
        ASSERT_TRUE(ok.has_value());
        EXPECT_EQ(*ok, 0) << "a committed topic serves at once";
        node.stop();
    }
    EXPECT_EQ(logBytes(dir / "topics" / "orph"), 0u) << "nothing was ever written to the orphan";
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        EXPECT_FALSE(fs::exists(dir / "topics" / "orph")) << "the empty orphan dir is deleted at boot";
        EXPECT_FALSE(fs::exists(dir / "quarantine"));
        EXPECT_EQ(leoOnOwner(node, laterId, 0), 1) << "the committed topic's acked record survives";
        node.stop();
    }
}

// C1 + M7: every DefineTopic OK is answered only once the topic serves — the
// first produce after the answer is accepted (no retry).  Disk topics: this
// (legacy-runtime) gateway produce path answers a transient topic UnknownPartition
// on the base code too (TopicRegistry holds no log for it) — not a DDL property.
TEST(DdlConformanceBrokerRegression, C1_DefineOkMeansTheTopicServesAtOnceRegression) {
    const TempDataDir tmp("c1ok_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t sid = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(sid));
    for (int i = 0; i < 12; ++i) {
        const std::string n = "served" + std::to_string(i);
        const auto r = client.awaitDefine(client.sendDefineTopic(w.gw, w.stream, sid, n, 1, charBlob(), 0),
                                          Clock::now() + 10s);
        ASSERT_TRUE(r.has_value());
        ASSERT_EQ(r->errorCode, kOk) << n;
        const auto p = produceOnce(client, w, sid, r->topicId, "first");
        ASSERT_TRUE(p.has_value()) << n;
        EXPECT_EQ(*p, 0) << n << ": the first produce after the OK is accepted";
    }
    node.stop();
}

// C1 / I1 / I5(a): write to an orphan view SINK, then retry the CREATE VIEW, then
// restart.  The write is refused (the sink is unroutable until a record claims it),
// so the retry adopts an EMPTY sink — raw bytes can never become view output — and
// the view survives the restart with an empty sink log.
TEST(DdlConformanceBrokerRegression, C1_WriteToAnOrphanSinkThenRetryAndRestartRegression) {
    const TempDataDir tmp("c1sink_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t sinkId = -1;
    {
        BrokerNode node{cfg};
        node.setAdminCreateFaultForTest("osink", /*afterCreate=*/true);
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "character"), kOk);
        const int32_t charId = w.schema(sid, "character")->topicId;
        EXPECT_EQ(w.view(sid, "osink", "character"), kInternal);
        sinkId = charId + 1;
        for (int i = 0; i < 3; ++i) {
            const auto p = produceOnce(client, w, sid, sinkId, "foreign-bytes");
            ASSERT_TRUE(p.has_value());
            EXPECT_EQ(*p, kNotLeaderCode) << "a raw write to an orphan sink is refused";
        }
        EXPECT_EQ(w.view(sid, "osink", "character"), kOk) << "the retry adopts its own (empty) orphan sink";
        EXPECT_EQ(w.schema(sid, "osink")->topicId, sinkId);
        EXPECT_EQ(leoOnOwner(node, sinkId, 0), 0) << "the adopted sink holds no foreign bytes";
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        EXPECT_EQ(w.view(sid, "osink", "character"), kViewExists);
        EXPECT_EQ(w.schema(sid, "osink")->topicId, sinkId);
        EXPECT_EQ(leoOnOwner(node, sinkId, 0), 0);
        node.stop();
    }
}

// C1 / I5(a): write to an orphan DefineTopic topic, then retry the define: the write
// was refused, the retry adopts the topic, and from then on it serves.
TEST(DdlConformanceBrokerRegression, C1_WriteToAnOrphanTopicThenRetryRegression) {
    const TempDataDir tmp("c1retry_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.setAdminCreateFaultForTest("orph", /*afterCreate=*/true);
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t sid = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(sid));
    EXPECT_EQ(w.define(sid, "orph"), kInternal);
    const int32_t orphanId = 1;   // ids: ddl_ready, orph
    const auto refused = produceOnce(client, w, sid, orphanId, "early");
    ASSERT_TRUE(refused.has_value());
    EXPECT_EQ(*refused, kNotLeaderCode);
    ASSERT_EQ(w.define(sid, "orph"), kOk) << "the retry adopts the orphan";
    EXPECT_EQ(w.schema(sid, "orph")->topicId, orphanId);
    EXPECT_EQ(leoOnOwner(node, orphanId, 0), 0) << "the refused write never landed";
    const auto ok = produceOnce(client, w, sid, orphanId, "after-commit");
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(*ok, 0);
    EXPECT_TRUE(listed(client, w, sid, orphanId));
    node.stop();
}

// I3 / I5(c): a RUNE_TOPICS name equal to (or a case variant of) a recovered runtime
// topic refuses boot, naming both — two specs over one directory would corrupt it
// (fold) or shift every later id (exact).  Nothing on disk is touched; the old
// configuration still boots.
TEST(DdlConformanceBrokerRegression, I3_StaticNameCollidingWithARuntimeTopicRefusesBootRegression) {
    const TempDataDir tmp("i3_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "orders"), kOk);
        node.stop();
    }
    for (const char* staticName : {"orders", "Orders", "ORDERS"}) {
        SCOPED_TRACE(staticName);
        NodeConfig c2 = cfg;
        TopicSpec s; s.name = staticName; s.partitionCount = 1; s.topicId = 0;
        c2.topics = {s};
        BrokerNode node{c2};
        try {
            node.start();
            ADD_FAILURE() << "boot must refuse a static name colliding with runtime topic 'orders'";
            node.stop();
        } catch (const std::exception& ex) {
            const std::string what = ex.what();
            EXPECT_NE(what.find(staticName), std::string::npos) << what;
            EXPECT_NE(what.find("'orders'"), std::string::npos) << what;
        }
        EXPECT_TRUE(fs::exists(dir / "topics" / "orders" / "0")) << "nothing moved or deleted";
        EXPECT_FALSE(fs::exists(dir / "quarantine"));
    }
    {
        BrokerNode node{cfg};
        node.start();   // the unchanged configuration still boots
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));   // the admin thread replayed the catalog
        const auto s = w.schema(sid, "orders");
        ASSERT_TRUE(s.has_value());
        EXPECT_EQ(s->errorCode, kOk) << "the runtime topic is intact";
        node.stop();
    }
}

// I5(c): a NON-colliding RUNE_TOPICS addition while runtime topics exist.  A static
// topic's id is its RUNE_TOPICS position, so adding one moves the static/runtime id
// boundary (firstRuntimeId) — the documented, pre-existing limitation: the runtime
// topic whose id the new entry takes is no longer recovered as a runtime topic.
// This pins the DATA-SAFETY half: the broker boots, every other runtime topic keeps
// its id and its acked records, and the displaced topic's dir is quarantined
// (committed marker), never deleted.
TEST(DdlConformanceBrokerRegression, I5c_AddingAStaticTopicWhileRuntimeTopicsExistNeverDeletesDataRegression) {
    const TempDataDir tmp("i5c_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t ordersId = -1;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));   // "ddl_ready": runtime id 0
        ASSERT_EQ(w.define(sid, "orders"), kOk);
        ordersId = w.schema(sid, "orders")->topicId;
        ASSERT_EQ(ordersId, 1);
        const auto p = produceOnce(client, w, sid, ordersId, "acked-order");
        ASSERT_TRUE(p.has_value());
        ASSERT_EQ(*p, 0);
        node.stop();
    }
    NodeConfig c2 = cfg;
    TopicSpec extra; extra.name = "extra"; extra.partitionCount = 1; extra.topicId = 0;
    c2.topics = {extra};
    {
        BrokerNode node{c2};
        node.start();   // no name collides: boots
        ASSERT_TRUE(node.awaitLeader(30s));
        EXPECT_EQ(leoOnOwner(node, ordersId, 0), 1) << "a runtime topic above the boundary keeps id + data";
        node.stop();
    }
    EXPECT_FALSE(fs::exists(dir / "topics" / "ddl_ready")) << "the displaced runtime topic left topics/";
    bool quarantined = false;
    if (fs::exists(dir / "quarantine"))
        for (const auto& e : fs::directory_iterator(dir / "quarantine"))
            quarantined = quarantined || e.path().filename().string().starts_with("ddl_ready-");
    EXPECT_TRUE(quarantined) << "quarantined, never deleted";
}
