// LeaseTopicBrokerTest.cpp — lease topics Task 3b: the broker effects of a lease topic over a REAL single-node
// BrokerNode and real Aeron (embedded drivers), driven by the wire client ShardTestClient (a v18 client; the lease
// define is sent through the client's DefineTopic wire form, the same path a real client takes).
//
//   define a lease topic -> the topic serves; its lease view `<topic>.leases` is a plan-less catalog-only sink
//   (TopicRegistry ViewSink, its name resolves, the gateway never awaits a plan for it); GetTopicSchema carries the
//   lease; restart on the same data dir -> the same holds after boot, nothing halted; drop the topic -> both ids are
//   gone from the ObjectIndex and Dropped in the registry; restart -> still gone.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/RecordBatch.h>

#include "support/DdlHarness.h"
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
namespace rp = rune::protocol;
using Clock  = std::chrono::steady_clock;
using TopicState = rune::gateway::TopicRegistry::TopicState;

namespace {


struct TempDataDir {
    fs::path path;
    explicit TempDataDir(std::string_view tag) {
        static std::atomic<uint32_t> seq{0};
        path = fs::temp_directory_path() / (std::string("rune_leasebn_") + std::string(tag) + std::to_string(::getpid()) +
                                            "_" + std::to_string(seq.fetch_add(1)));
    }
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

rp::LeaseSpec validSpec() {
    rp::LeaseSpec s;
    s.ttlMs = 10'000; s.minTtlMs = 1'000; s.maxTtlMs = 60'000; s.maxKeys = 1; s.expiryBatch = 64;
    s.checkpoint = 0; s.checkpointIntervalMs = 1'000; s.checkpointRate = 1;
    return s;
}

/// The user's columns: just the key `id` string(64); the admin prepends the four system columns.
std::vector<uint8_t> userBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("inst", 1).addFixedStr("id", 64, 1, /*isKey=*/true).build());
}

bool eventually(const std::function<bool()>& cond, std::chrono::milliseconds budget = 10s) {
    const auto deadline = Clock::now() + budget;
    while (Clock::now() < deadline) {
        if (cond()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return cond();
}

struct Wire {
    ShardTestClient& c;
    std::string gw;
    int32_t stream;

    int defineProbe(int64_t sid) {   // NotLeader is retriable until the catalog leader serves
        const auto deadline = Clock::now() + 20s;
        int code = -1;
        while (Clock::now() < deadline) {
            const auto r = c.awaitDefine(c.sendDefineTopic(gw, stream, sid, "probe", 1, charBlob(), 0), Clock::now() + 10s);
            code = r ? static_cast<int>(r->errorCode) : -1;
            if (code == kOk || code == kTopicExists) return kOk;
            if (code != kNotLeader) break;
            std::this_thread::sleep_for(20ms);
        }
        std::cout << "[lease] probe define code " << code << "\n";
        return code;
    }
    int defineLease(int64_t sid, const std::string& name) {
        const auto r = c.awaitDefine(c.sendDefineLeaseTopic(gw, stream, sid, name, 2, userBlob(), validSpec()), Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
    std::optional<ShardTestClient::SchemaAnswer> schema(int64_t sid, const std::string& n) {
        return c.awaitSchema(c.sendGetTopicSchema(gw, stream, sid, n), Clock::now() + 10s);
    }
    int drop(int64_t sid, const std::string& name) {
        const auto r = c.awaitDdl(c.sendDropTopic(gw, stream, sid, name), Clock::now() + 10s);
        return r ? static_cast<int>(r->errorCode) : -1;
    }
};

struct ControlView {
    TopicState topic = TopicState::Unknown, sink = TopicState::Unknown;
    bool       sinkAwaitingPlan = true;
    std::optional<int32_t> sinkByName, topicByName;
};
ControlView controlView(BrokerNode& node, int32_t topicId, const std::string& name) {
    return node.onControlThreadForTest([&]() {
        ControlView v;
        auto* gw = node.gatewayForTest();
        if (gw == nullptr || gw->topicRegistry() == nullptr) return v;
        v.topic = gw->topicRegistry()->state(topicId);
        v.sink  = gw->topicRegistry()->state(topicId + 1);
        v.sinkAwaitingPlan = gw->viewSinkAwaitingPlan(topicId + 1);
        v.sinkByName  = gw->objectIndex().byName(0, name + ".leases");
        v.topicByName = gw->objectIndex().byName(0, name);
        return v;
    });
}
bool applierHalted(BrokerNode& node) {
    return node.onAdminThreadForTest([&]() { return node.adminWorkerAgentForTest()->worker().applier().halted(); });
}

} // namespace

TEST(LeaseTopicBrokerRegression, LeaseViewIsAPlanLessSinkAcrossCreateRestartDropRestart) {
    const TempDataDir tmp("full_");
    const fs::path& dir = tmp.path;
    const NodeConfig cfg = makeSingleNodeConfig(dir);
    int32_t topicId = -1;

    // ── create ───────────────────────────────────────────────────────────────
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron");
        client.setClientVersion(rp::client::kClientVersionLeases);
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_GT(sid, 0);
        ASSERT_EQ(w.defineProbe(sid), kOk);

        ASSERT_EQ(w.defineLease(sid, "inst"), kOk);
        const auto s = w.schema(sid, "inst");
        ASSERT_TRUE(s.has_value());
        ASSERT_EQ(s->errorCode, kOk);
        topicId = s->topicId;
        ASSERT_TRUE(s->lease.has_value()) << "GetTopicSchema carries the lease to a v18 client";
        EXPECT_EQ(*s->lease, validSpec());

        ASSERT_TRUE(eventually([&] { return controlView(node, topicId, "inst").sink == TopicState::ViewSink; }));
        const ControlView v = controlView(node, topicId, "inst");
        EXPECT_EQ(v.sink, TopicState::ViewSink) << "the lease view is a catalog-only sink";
        EXPECT_EQ(v.sinkByName, std::optional<int32_t>(topicId + 1)) << "inst.leases resolves to topic+1";
        EXPECT_EQ(v.topicByName, std::optional<int32_t>(topicId));
        EXPECT_FALSE(v.sinkAwaitingPlan) << "the gateway never awaits a plan for a lease view";
        EXPECT_FALSE(applierHalted(node));

        // The lease view's own name is a view: GetTopicSchema of it answers (it is listed, resolvable), no lease on it.
        const auto sv = w.schema(sid, "inst.leases");
        ASSERT_TRUE(sv.has_value());
        EXPECT_FALSE(sv->lease.has_value());

        // Produce one record with the four system columns zero-filled: recorded, no assertion (the lease hook is Task 5).
        std::string key(64, '\0');
        key.replace(0, 4, "a-01");
        std::string value;
        value.push_back(static_cast<char>(96)); value.push_back(0);   // blockLength = 4 * 8 + 64
        value.push_back(1); value.push_back(0);                       // schema version 1
        value += std::string(32, '\0');                               // lease_version, ttl, holder, remaining
        value += key;                                                 // id
        rp::RecordBatchBuilder b(0, 0);
        b.append(key, value, 0, 0);
        const auto ack = client.produceUntilOk(w.gw, w.stream, sid, topicId, 0, 1, b.build(), /*maxAttempts=*/5);
        std::cout << "[lease] produce of a zero-system-column record: "
                  << (ack ? "accepted" : "not accepted: " + client.lastProduceFailure()) << "\n";
        node.stop();
    }

    // ── restart: the same holds after boot ───────────────────────────────────
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron2");
        client.setClientVersion(rp::client::kClientVersionLeases);
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_GT(sid, 0);
        ASSERT_EQ(w.defineProbe(sid), kOk);
        const auto s = w.schema(sid, "inst");
        ASSERT_TRUE(s.has_value());
        ASSERT_EQ(s->errorCode, kOk);
        EXPECT_EQ(s->topicId, topicId);
        ASSERT_TRUE(s->lease.has_value());
        EXPECT_EQ(*s->lease, validSpec());
        ASSERT_TRUE(eventually([&] { return controlView(node, topicId, "inst").sink == TopicState::ViewSink; }));
        const ControlView v = controlView(node, topicId, "inst");
        EXPECT_EQ(v.sinkByName, std::optional<int32_t>(topicId + 1));
        EXPECT_FALSE(v.sinkAwaitingPlan);
        EXPECT_FALSE(applierHalted(node)) << "a boot replay of a lease topic halts nothing";
        EXPECT_EQ(node.catalogApplierPhaseForTest(), "Live");

        // ── drop: both ids gone ───────────────────────────────────────────────
        ASSERT_EQ(w.drop(sid, "inst"), kOk);
        ASSERT_TRUE(eventually([&] {
            const ControlView d = controlView(node, topicId, "inst");
            return d.topic == TopicState::Dropped && d.sink == TopicState::Dropped;
        }));
        const ControlView d = controlView(node, topicId, "inst");
        EXPECT_EQ(d.sinkByName, std::nullopt);
        EXPECT_EQ(d.topicByName, std::nullopt);
        EXPECT_FALSE(applierHalted(node));
        const auto gone = w.schema(sid, "inst");
        ASSERT_TRUE(gone.has_value());
        EXPECT_EQ(gone->errorCode, kUnknownTopic);
        node.stop();
    }

    // ── restart: still gone ──────────────────────────────────────────────────
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ShardTestClient client(dir / "client-aeron3");
        client.setClientVersion(rp::client::kClientVersionLeases);
        client.keepSessionsAlive(true);
        Wire w{client, cfg.clientEndpoint.channel, cfg.clientEndpoint.streamId};
        const int64_t sid = client.connect(w.gw, w.stream);
        ASSERT_GT(sid, 0);
        ASSERT_EQ(w.defineProbe(sid), kOk);
        const auto gone = w.schema(sid, "inst");
        ASSERT_TRUE(gone.has_value());
        EXPECT_EQ(gone->errorCode, kUnknownTopic);
        const auto goneLeases = w.schema(sid, "inst.leases");
        ASSERT_TRUE(goneLeases.has_value());
        EXPECT_EQ(goneLeases->errorCode, kUnknownTopic);
        const ControlView v = controlView(node, topicId, "inst");
        EXPECT_NE(v.topic, TopicState::ViewSink);
        EXPECT_NE(v.sink, TopicState::ViewSink);
        EXPECT_NE(v.topic, TopicState::Serving);
        EXPECT_EQ(v.sinkByName, std::nullopt);
        EXPECT_EQ(v.topicByName, std::nullopt);
        EXPECT_FALSE(applierHalted(node));
        node.stop();
    }
}
