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
    const fs::path dir = makeTempDataDir("t1_");
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
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T2_PipelinedSameNameFromTwoSessionsExactlyOneWins) {
    const fs::path dir = makeTempDataDir("t2_");
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
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T3T4_InterleavedBurstMatchesTheTable) {
    const fs::path dir = makeTempDataDir("burst_");
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
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T5_FailedCreateLeavesAnOrphanAndTheBrokerStillRestarts) {
    const fs::path dir = makeTempDataDir("t5_");
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
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kTopicExists) << "KL1: the orphan sink blocks the name";
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
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T5a_ReplyFailsBeforeCreatingThenRetrySucceeds) {
    const fs::path dir = makeTempDataDir("t5a_");
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
    std::error_code ec; fs::remove_all(dir, ec);
}

TEST(DdlConformanceBrokerRegression, T7_RestartPreservesTheCatalogAndTheCollisions) {
    const fs::path dir = makeTempDataDir("t7_");
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
    std::error_code ec; fs::remove_all(dir, ec);
}

// D4 (amended): a static topic removed from RUNE_TOPICS is QUARANTINED at boot —
// moved to dataDir/quarantine/<name>-<ts>/ with its data intact and readable — and
// re-adding it starts EMPTY (no automatic restore; the restore is a manual move).
TEST(DdlConformanceBrokerRegression, T8_RemovedStaticTopicIsQuarantinedNotDeleted) {
    const fs::path dir = makeTempDataDir("t8_");
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
    std::error_code ec; fs::remove_all(dir, ec);
}
