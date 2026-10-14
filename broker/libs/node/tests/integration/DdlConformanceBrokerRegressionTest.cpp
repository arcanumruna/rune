// DdlConformanceBrokerRegressionTest.cpp — DDL conformance plan Task 12, on the
// __catalog write path since catalog history Task 10.
//
// A REAL single-node BrokerNode over REAL Aeron (embedded drivers), driven by the
// wire client ShardTestClient.  One representative slice per timing class:
//   T1  sequential collisions answer the exact codes (incl. the name rule);
//   T2  pipelined same-name DDL from two sessions: exactly one wins (FIFO);
//   T3/T4 a 30-frame interleaved burst: every answer matches the expectation table;
//   T5  a refused DDL leaves no trace and the broker restarts (was: a failed
//       control reply's orphan + its placeholder — gone with the create round-trip);
//   T7  catalog + collisions are identical across a restart (the boot replay).
// T6 / 3-node failover: the catalog leadership suites (Tasks 11 / 20).
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/catalog/CatalogEvent.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include "support/DdlHarness.h"          // charBlob / viewPlanBlob / alterOpsFor / the table
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
    /// Until the broker is the serving catalog leader a DDL is answered the
    /// retriable NotLeader (Task 9): retry a probe define (bounded) — the
    /// documented settle, not a correctness gate.
    bool awaitDdlReady(int64_t sid) {
        const auto deadline = Clock::now() + 20s;
        while (Clock::now() < deadline) {
            const int code = define(sid, "ddl_ready");
            if (code == kOk || code == kTopicExists) return true;
            if (code != kNotLeader) return false;
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

// Was T5_FailedCreateLeavesAnOrphanAndTheBrokerStillRestarts.  On the catalog path a
// DDL either becomes one applied event or leaves nothing: a refused CREATE VIEW
// uses no id, the broker restarts (B5), the later topic keeps its id, and every
// "__"-name (the former __orphan placeholders' included) is refused DDL.
TEST(DdlConformanceBrokerRegression, T5_RefusedDdlLeavesNoTraceAndTheBrokerStillRestarts) {
    const TempDataDir tmp("t5_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t laterId = -1, charId = -1;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "character"), kOk);
        charId = w.schema(sid, "character")->topicId;
        EXPECT_EQ(w.view(sid, "orphan_v", "nosuch"), kViewSourceNotFound) << "refused: nothing appended";
        ASSERT_EQ(w.define(sid, "later"), kOk);
        laterId = w.schema(sid, "later")->topicId;
        EXPECT_EQ(laterId, charId + 1) << "the refused CREATE VIEW used no topic id";
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
        EXPECT_EQ(later->topicId, laterId) << "a topic keeps its id across the restart";
        EXPECT_FALSE(fs::exists(dir / "topics" / "orphan_v")) << "the refused view never had a directory";
        for (const std::string ph : {std::string("__orphan.") + std::to_string(laterId), std::string("__catalog")}) {
            SCOPED_TRACE(ph);
            ASSERT_TRUE(w.schema(sid, ph).has_value());
            EXPECT_EQ(w.schema(sid, ph)->errorCode, kUnknownTopic);
            EXPECT_EQ(w.define(sid, ph), kInvalidTopicName) << "never accepts DDL (reserved __ name)";
            EXPECT_EQ(w.alter(sid, ph), kUnknownTopic);
            EXPECT_EQ(w.view(sid, ph, "character"), kInvalidTopicName);
        }
        EXPECT_EQ(w.view(sid, "orphan_v", "character"), kOk) << "the refused view's name was never taken";
        EXPECT_EQ(w.define(sid, "character"), kTopicExists);
        node.stop();
    }
}

// Was T5a_ReplyFailsBeforeCreatingThenRetrySucceeds: a DDL refused before its
// event (a zero partition count) creates nothing; the corrected retry creates it.
TEST(DdlConformanceBrokerRegression, T5a_RefusedDefineThenRetrySucceeds) {
    const TempDataDir tmp("t5a_");
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
    EXPECT_EQ(w.define(sid, "x", 0, 0), kInvalidSchema);
    ASSERT_TRUE(w.schema(sid, "x").has_value());
    EXPECT_EQ(w.schema(sid, "x")->errorCode, kUnknownTopic) << "nothing was created";
    EXPECT_EQ(w.define(sid, "x"), kOk) << "the retry creates it";
    node.stop();
}

// Restores the T5_Retry cell over the wire (Task 10 → Task 11 binding: the test
// client can now pin a requestId).  A CREATE VIEW retried with the SAME requestId
// gets its ORIGINAL sink — not ViewExists, never a second sink — before and after
// a restart (the dedupe record is part of the replayed catalog).  RED (dedupe
// keyed on anything the restart loses): ViewExists after the restart.
TEST(DdlConformanceBrokerRegression, T5_RetryWithTheSameRequestIdGetsItsOriginalSinkAcrossARestartRegression) {
    const TempDataDir tmp("t5r_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    const rune::protocol::RequestId viewRid{0x75E7, 1};
    const rune::protocol::RequestId topicRid{0x75E7, 2};
    int32_t viewTopicId = -1, topicId = -1;
    uint64_t outputFp = 0;
    auto createView = [&](ShardTestClient& c, Wire& w, int64_t sid, int32_t srcId) {
        return c.awaitCreateView(c.sendCreateView(w.gw, w.stream, sid, "v", "id", 0, viewPlanBlob("v", srcId, 0),
                                                  viewRid),
                                 Clock::now() + 10s);
    };
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        const auto first = client.awaitDefine(
            client.sendDefineTopic(w.gw, w.stream, sid, "character", 1, charBlob(), 0, topicRid), Clock::now() + 10s);
        ASSERT_TRUE(first.has_value());
        ASSERT_EQ(first->errorCode, kOk);
        topicId = first->topicId;
        const auto again = client.awaitDefine(
            client.sendDefineTopic(w.gw, w.stream, sid, "character", 1, charBlob(), 0, topicRid), Clock::now() + 10s);
        ASSERT_TRUE(again.has_value());
        EXPECT_EQ(again->errorCode, kOk) << "a retried DefineTopic is its original result, not TopicExists";
        EXPECT_EQ(again->topicId, topicId);

        const auto v1 = createView(client, w, sid, topicId);
        ASSERT_TRUE(v1.has_value());
        ASSERT_EQ(v1->errorCode, kOk);
        viewTopicId = v1->viewTopicId;
        outputFp    = v1->outputFingerprint;
        const auto v2 = createView(client, w, sid, topicId);
        ASSERT_TRUE(v2.has_value());
        EXPECT_EQ(v2->errorCode, kOk) << "not ViewExists";
        EXPECT_EQ(v2->viewTopicId, viewTopicId) << "its own sink, never a second one";
        EXPECT_EQ(v2->outputFingerprint, outputFp);
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
        const auto v3 = createView(client, w, sid, topicId);
        ASSERT_TRUE(v3.has_value());
        EXPECT_EQ(v3->errorCode, kOk) << "the dedupe survives the restart";
        EXPECT_EQ(v3->viewTopicId, viewTopicId);
        EXPECT_EQ(v3->outputFingerprint, outputFp);
        // A NEW requestId for the same name is a new DDL: ViewExists.
        const auto fresh = client.awaitCreateView(
            client.sendCreateView(w.gw, w.stream, sid, "v", "id", 0, viewPlanBlob("v", topicId, 0)),
            Clock::now() + 10s);
        ASSERT_TRUE(fresh.has_value());
        EXPECT_EQ(fresh->errorCode, kViewExists);
        node.stop();
    }
    // Exactly one CreateView event and one CreateTopic for the two request ids.
    rune::storage::Log log((dir / "__catalog").string());
    log.recover();
    std::size_t views = 0, topics = 0;
    const std::vector<uint8_t> data = log.read(0, 1 << 22);
    std::size_t pos = 0;
    while (pos < data.size()) {
        rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
        if (!reader.ensureValid()) break;
        reader.forEachRecord([&](std::string_view, std::string_view val, int64_t, int64_t) {
            const auto ev = rune::catalog::decode(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
            ASSERT_TRUE(ev.has_value());
            if (ev->principal.requestId == viewRid) ++views;
            if (ev->principal.requestId == topicRid) ++topics;
        });
        if (reader.totalLength() == 0) break;
        pos += static_cast<std::size_t>(reader.totalLength());
    }
    EXPECT_EQ(views, 1u);
    EXPECT_EQ(topics, 1u);
}

// Binding (Task 10 → 11): every catalog DDL op 74–77 over REAL Aeron, each
// answered exactly once by its DdlResponse (78).  Also pins the fix-on-sight it
// found: an AlterView's fresh sink (same name) was refused while the old sink
// was live — RED: AlterView answered Internal after the 5 s answer-gate hold.
TEST(DdlConformanceBrokerRegression, T9_EveryCatalogDdlOpRoundTripsOverTheWireRegression) {
    const TempDataDir tmp("t9_");
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
    ASSERT_EQ(w.define(sid, "u"), kOk);
    const int32_t uId = w.schema(sid, "u")->topicId;

    auto ddl = [&](int64_t corr) {
        const auto a = client.awaitDdl(corr, Clock::now() + 10s);
        EXPECT_TRUE(a.has_value()) << "corr " << corr << " answered";
        EXPECT_EQ(client.answersFor(corr), 1u) << "corr " << corr << " answered exactly once";
        return a.value_or(ShardTestClient::DdlAnswer{.errorCode = 0xFFFF});
    };
    // 75 AlterTopicConfig: the topic object's version 1 → 2.
    const auto cfgA = ddl(client.sendAlterTopicConfig(w.gw, w.stream, sid, "u", 1, {{1, 60'000}}));
    EXPECT_EQ(cfgA.errorCode, kOk) << cfgA.detail;
    EXPECT_EQ(cfgA.newVersion, 2u);
    const auto stale = ddl(client.sendAlterTopicConfig(w.gw, w.stream, sid, "u", 1, {{1, 70'000}}));
    EXPECT_NE(stale.errorCode, kOk) << "expectedVersion 1 is stale now";
    // 70 CreateView, then 77 AlterView (a fresh sink; the OK is gated on it serving).
    ASSERT_EQ(w.view(sid, "v", "u"), kOk);
    const auto av = ddl(client.sendAlterView(w.gw, w.stream, sid, "v", "id", 0, 0,
                                             viewPlanBlob("v", {uId}, 0, /*extraColumn=*/true)));
    EXPECT_EQ(av.errorCode, kOk) << av.detail;
    EXPECT_GE(av.gateTopicId, 0) << "AlterView names its fresh sink";
    // 74 DropTopic of a topic a view reads: InvalidRequest naming the view.
    const auto busy = ddl(client.sendDropTopic(w.gw, w.stream, sid, "u"));
    EXPECT_EQ(busy.errorCode, 39);
    EXPECT_NE(busy.detail.find("v"), std::string::npos) << busy.detail;
    // 76 DropView, then 74 DropTopic.
    EXPECT_EQ(ddl(client.sendDropView(w.gw, w.stream, sid, "v")).errorCode, kOk);
    EXPECT_EQ(ddl(client.sendDropTopic(w.gw, w.stream, sid, "u")).errorCode, kOk);
    const auto gone = ddl(client.sendDropTopic(w.gw, w.stream, sid, "u"));
    EXPECT_EQ(gone.errorCode, kUnknownTopic);
    EXPECT_FALSE(gone.detail.empty()) << "UnknownTopic carries a detail";
    EXPECT_EQ(w.schema(sid, "u")->errorCode, kUnknownTopic);
    node.stop();
}

namespace {
/// Rewrite dataDir/__catalog so the event creating `name` carries a WRONG
/// fingerprint (a determinism break — the boot replay halts there).  Every other
/// record is copied verbatim; the applied checkpoint is kept.
void tamperCatalogCreateOf(const fs::path& dataDir, const std::string& name) {
    const fs::path cat = dataDir / "__catalog";
    const fs::path tmp = dataDir / "__catalog.rewrite";
    fs::remove_all(tmp);
    fs::create_directories(tmp);
    {
        rune::storage::Log in(cat.string());
        in.recover();
        rune::storage::Log out(tmp.string());
        out.recover();
        int64_t next = 0;
        bool tampered = false;
        while (next < in.logEndOffset()) {
            const std::vector<uint8_t> data = in.read(next, 1 << 20);
            ASSERT_FALSE(data.empty());
            std::size_t pos = 0;
            const int64_t before = next;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                rune::protocol::RecordBatchBuilder b(reader.baseOffset(), 0);
                int64_t last = -1;
                bool changed = false;
                reader.forEachRecord([&](std::string_view key, std::string_view val, int64_t off, int64_t ts) {
                    auto ev = rune::catalog::decode(
                        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                    std::string v(val);
                    if (ev && ev->kind == rune::catalog::EventKind::CreateTopic && ev->objectName == name) {
                        ev->fingerprint ^= 1;
                        auto enc = rune::catalog::encode(*ev);
                        ASSERT_TRUE(enc.has_value());
                        v.assign(reinterpret_cast<const char*>(enc->data()), enc->size());
                        changed = tampered = true;
                    }
                    b.append(key, v, ts);
                    last = off;
                });
                if (changed) {
                    const std::vector<uint8_t> nb = b.build();
                    ASSERT_GE(out.append(nb, last), 0);
                } else {
                    ASSERT_GE(out.append(data.data() + pos, tl, reader.baseOffset() + reader.lastOffsetDelta()), 0);
                }
                next = last + 1;
                if (tl == 0) break;
                pos += tl;
            }
            ASSERT_GT(next, before);
        }
        ASSERT_TRUE(tampered) << "found the create of " << name;
    }
    // Swap the segments in; keep the checkpoint and every non-segment file.
    for (const auto& e : fs::directory_iterator(cat))
        if (e.path().extension() == ".log" || e.path().extension() == ".index" ||
            e.path().extension() == ".timeindex")
            fs::remove(e.path());
    for (const auto& e : fs::directory_iterator(tmp))
        if (e.path().extension() == ".log" || e.path().extension() == ".index" ||
            e.path().extension() == ".timeindex")
            fs::copy_file(e.path(), cat / e.path().filename(), fs::copy_options::overwrite_existing);
    fs::remove_all(tmp);
}
} // namespace

// Task 11 fix round 4 (rule 3, the Part E rule: catalog damage ⇒ no destructive
// cleanup): a boot whose catalog replay HALTS before its checkpoint moves,
// renames, deletes and quarantines NO topic dir — the topics created after the
// halt keep their committed data in place; once the operator repairs the
// catalog, the next boot serves every committed record.  RED (rounds ≤ 3): the
// halted boot quarantined the unclaimed dir and the repaired boot rebuilt the
// topic EMPTY — committed records lost.
TEST(DdlConformanceBrokerRegression, HaltedBootReplayMovesNoTopicDirAndLosesNoCommittedRecordRegression) {
    const TempDataDir tmp("hbr_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t laterId = -1;
    constexpr int kRecords = 5;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_TRUE(w.awaitDdlReady(sid));
        ASSERT_EQ(w.define(sid, "keep"), kOk);
        ASSERT_EQ(w.define(sid, "later"), kOk);
        laterId = w.schema(sid, "later")->topicId;
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"committed"}, 0);
        const std::vector<uint8_t> batch = b.build();
        for (int i = 0; i < kRecords; ++i)
            ASSERT_TRUE(client.produceUntilOk(w.gw, w.stream, sid, laterId, 0, /*acks=*/1, batch).has_value());
        ASSERT_EQ(leoOnOwner(node, laterId, 0), kRecords);
        node.stop();
    }
    const fs::path saved = dir / "__catalog.saved";
    fs::copy(dir / "__catalog", saved, fs::copy_options::recursive);
    tamperCatalogCreateOf(dir, "later");
    {
        BrokerNode node{cfg};
        node.start();   // halted replay: still boots, serves what it applied
        ASSERT_TRUE(node.awaitLeader(30s));
        node.stop();
    }
    EXPECT_TRUE(fs::exists(dir / "topics" / "later" / "0")) << "the halted boot moved / quarantined the dir";
    bool quarantined = false;
    std::error_code qec;
    for (const auto& e : fs::directory_iterator(dir / "quarantine", qec))
        quarantined = quarantined || e.path().filename().string().rfind("later", 0) == 0;
    EXPECT_FALSE(quarantined) << "nothing quarantined while the catalog replay halts";
    // The operator repairs the catalog; the next boot serves every committed record.
    fs::remove_all(dir / "__catalog");
    fs::copy(saved, dir / "__catalog", fs::copy_options::recursive);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        bool serves = false;
        for (const auto deadline = Clock::now() + 15s; Clock::now() < deadline && !serves;) {
            serves = node.catalogTopicServesForTest(laterId);
            if (!serves) std::this_thread::sleep_for(20ms);
        }
        ASSERT_TRUE(serves);
        EXPECT_EQ(leoOnOwner(node, laterId, 0), kRecords) << "no committed record lost";
        node.stop();
    }
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
        // Task 16 (B6): the local catalog is empty here, so the reconcile is deferred until this
        // broker has caught up to the catalog leader (itself) — then it quarantines the dir.
        ASSERT_TRUE(node.awaitBootReconcileSettledForTest(30s));
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

// ── Final review C1 / M7 / concern 4 (final fix wave) ───────────────────────────
namespace {

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

} // namespace

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

// Concern 4 (controller ruling): the replica build of a committed topic FAILS, so
// the topic never serves.  The DDL answer gate must not send the held OK after its
// hold limit (a false OK is a silent fallback): the define is answered Internal (6,
// in doubt).  Later DDL is not wedged behind it.
TEST(DdlConformanceBrokerRegression, C4_HeldOkForATopicThatNeverServesIsAnsweredInternalRegression) {
    const TempDataDir tmp("c4_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    BrokerNode node{cfg};
    node.setCommitReplicaFaultForTest("nobuild");
    node.start();
    ASSERT_TRUE(node.awaitLeader(30s));
    ShardTestClient client(dir / "client-aeron");
    client.keepSessionsAlive(true);
    Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
    const int64_t sid = client.connect(w.gw, w.stream);
    ASSERT_TRUE(w.awaitDdlReady(sid));

    const auto t0 = Clock::now();
    const auto r = client.awaitDefine(client.sendDefineTopic(w.gw, w.stream, sid, "nobuild", 1, charBlob(), 0),
                                      Clock::now() + 20s);
    ASSERT_TRUE(r.has_value()) << "answered, never dropped (no-hang)";
    EXPECT_EQ(static_cast<int>(r->errorCode), kInternal) << "in doubt — never an OK for a topic that does not serve";
    EXPECT_NE(static_cast<int>(r->errorCode), kOk);
    EXPECT_GE(Clock::now() - t0, 4s) << "held for the hold limit first";

    EXPECT_EQ(w.define(sid, "afterwards"), kOk) << "the gate is not wedged";
    node.stop();
}


// ── Task 10 fix round 1 (review I2): RUNE_TOPICS added after catalog topics ──────
// INTERIM until Task 17 removes RUNE_TOPICS.  A static topic whose name (exact or
// an ASCII case variant) or id (its RUNE_TOPICS position) collides with a catalog
// topic would open, or quarantine, the catalog topic's directory: boot REFUSES,
// naming both, before anything on disk is touched — and the old configuration
// still boots with the catalog topic intact.

// I3, the boot direction: a static name equal to (or a case variant of) a catalog topic.
TEST(DdlConformanceBrokerRegression, I3_StaticNameCollidingWithACatalogTopicRefusesBootRegression) {
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
            ADD_FAILURE() << "boot must refuse a static name colliding with catalog topic 'orders'";
            node.stop();
        } catch (const std::exception& ex) {
            const std::string what = ex.what();
            EXPECT_NE(what.find(std::string("'") + staticName + "'"), std::string::npos) << what;
            EXPECT_NE(what.find("'orders'"), std::string::npos) << what;
            EXPECT_NE(what.find("by name"), std::string::npos) << what;
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
        ASSERT_TRUE(w.awaitDdlReady(sid));
        const auto s = w.schema(sid, "orders");
        ASSERT_TRUE(s.has_value());
        EXPECT_EQ(s->errorCode, kOk) << "the catalog topic is intact";
        node.stop();
    }
}

// I5c, the boot direction: a NON-colliding static NAME added while catalog topics
// exist still takes a catalog id (static ids are RUNE_TOPICS positions, catalog ids
// start at 0): boot refuses, and the catalog topic keeps its id and acked records.
TEST(DdlConformanceBrokerRegression, I5c_AddingAStaticTopicWhileCatalogTopicsExistRefusesBootAndKeepsDataRegression) {
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
        ASSERT_TRUE(w.awaitDdlReady(sid));   // "ddl_ready": catalog id 0
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
        try {
            node.start();
            ADD_FAILURE() << "boot must refuse a static topic taking catalog id 0";
            node.stop();
        } catch (const std::exception& ex) {
            const std::string what = ex.what();
            EXPECT_NE(what.find("'extra'"), std::string::npos) << what;
            EXPECT_NE(what.find("'ddl_ready'"), std::string::npos) << what;
            EXPECT_NE(what.find("by topic id"), std::string::npos) << what;
        }
    }
    EXPECT_TRUE(fs::exists(dir / "topics" / "ddl_ready")) << "the catalog topic's dir was not touched";
    EXPECT_FALSE(fs::exists(dir / "quarantine")) << "nothing quarantined";
    EXPECT_FALSE(fs::exists(dir / "topics" / "extra")) << "the static topic never opened a dir";
    {
        BrokerNode node{cfg};
        node.start();   // the unchanged configuration boots with every record
        ASSERT_TRUE(node.awaitLeader(30s));
        EXPECT_EQ(leoOnOwner(node, ordersId, 0), 1) << "the catalog topic keeps its id and acked record";
        node.stop();
    }
}
