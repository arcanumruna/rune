// LeaseTopicBrokerTest.cpp — lease topics Task 3b: the broker effects of a lease topic over a REAL single-node
// BrokerNode and real Aeron (embedded drivers), driven by the wire client ShardTestClient (a v18 client; the lease
// define is sent through the client's DefineTopic wire form, the same path a real client takes).
//
//   define a lease topic -> the topic serves; its lease view `<topic>.leases` is a plan-less catalog-only sink
//   (TopicRegistry ViewSink, its name resolves, the gateway never awaits a plan for it); GetTopicSchema carries the
//   lease; restart on the same data dir -> the same holds after boot, nothing halted; drop the topic -> both ids are
//   gone from the ObjectIndex and Dropped in the registry; restart -> still gone.
//
// Task 13a review (H1): the PRODUCTION wiring of the lease view READ over a real authenticating shard-mode broker:
// a granted key is read with a raw GetViewRowK on `<t>.leases`; READ on the source topic is the rule (a VIEW-only grant on
// the lease view is NotAuthorized); the gateway's registry-held lease view -> source mapping is set after create and after
// restart, and gone after a real DropTopic (the read is UnknownView).  The 3-node follower-Redirect leg: see the note at the end.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/PartitionHash.h>
#include <rune/protocol/RecordBatch.h>

#include "support/AuthRig.h"
#include "support/CatalogBrokerCluster.h"
#include "support/DdlHarness.h"
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <set>
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

        // The lease view is a plan-less sink with no schema entry of its own: GetTopicSchema of its name answers
        // UnknownTopic exactly (the topic's name answers; the view's does not), and carries no lease.
        const auto sv = w.schema(sid, "inst.leases");
        ASSERT_TRUE(sv.has_value());
        EXPECT_EQ(sv->errorCode, kUnknownTopic);
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
        // A positive signal that the boot stash's SinkApplied frame (the lease view's) was drained on the control thread:
        // only then is "not awaiting a plan" a statement about the drained frame and not about a frame not yet handled.
        ASSERT_TRUE(eventually([&] { return node.onControlThreadForTest([&] { return node.sinkAppliedDrainedForTest(); }) >= 1; }));
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


// ── Task 13a review (H1): the lease view read through the REAL wiring (create -> grant -> read -> auth -> restart -> drop) ──
namespace {
using SO = rp::SecurityOperation;
using ST = rp::SecurityResourceType;
constexpr int kStatusOk = 0, kStatusUnknownView = 4, kStatusNotAuthorized = 45;

struct LeaseWiring {
    AuthRig&  rig;
    int32_t   topicId = -1;
    int32_t   viewId  = -1;
    std::string key = [] { std::string k(64, '\0'); k.replace(0, 4, "a-01"); return k; }();

    int64_t loginAs2(const std::string& user, const char* pw) { return rig.login(user, pw, rp::client::kClientVersionLeases); }
    int64_t loginAs(const std::string& user) { return rig.login(user, AuthRig::kPw, rp::client::kClientVersionLeases); }

    /// One GetViewRowK as `sid`; the code is the ViewRowResponse status or the ErrorResponse code.  Retries only the transient
    /// answers (NotReady while the view rebuilds / the grant folds, NotLeader while leadership settles); bounded.
    std::pair<int, std::vector<uint8_t>> readK(int64_t sid, bool retryUnknown = false) {
        const auto deadline = Clock::now() + 30s;
        std::pair<int, std::vector<uint8_t>> last{-1, {}};
        while (Clock::now() < deadline) {
            const auto kb = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size());
            const int64_t corr = rig.c().sendGetViewRowK(rig.ch(), AuthRig::stream(), sid, viewId, kb);
            const auto a = rig.c().awaitViewRowOrError(corr, Clock::now() + 10s);
            if (!a) { last = {-1, {}}; continue; }
            last = {a->isError ? int(a->errorCode) : int(a->status), a->row};
            const bool transient = last.first == int(rp::client::ViewRowResponse::kNotReady) ||
                                   (a->isError && last.first == int(rp::client::ErrorCode::NotLeader)) ||
                                   (retryUnknown && !a->isError && last.first == kStatusUnknownView);
            if (!transient) return last;
            std::this_thread::sleep_for(20ms);
        }
        return last;
    }
    struct Map { int32_t lease = -2, viewSource = -2; };
    Map mapping() {
        return rig.broker().node().onControlThreadForTest([&] {
            Map m;
            if (auto* gw = rig.broker().node().gatewayForTest()) {
                m.lease = gw->leaseSourceOfView_(viewId);
                m.viewSource = gw->viewSourceOf_(viewId);
            }
            return m;
        });
    }
    bool grantKey(int64_t writerSid) {
        std::string value;
        value.push_back(static_cast<char>(96)); value.push_back(0);   // blockLength = 4 * 8 + 64
        value.push_back(1); value.push_back(0);                       // schema version 1
        auto put64 = [&](uint64_t v) { for (int i = 0; i < 8; ++i) value.push_back(static_cast<char>(v >> (8 * i))); };
        put64(0);        // lease_version
        put64(5'000);    // ttl_ms
        put64(1);        // holder
        put64(0);        // remaining
        value += key;    // id
        // A lease write is an explicit-key intent: idempotent, acks=All|View (the checked-writes rule list).
        const auto ids = rig.initProducer(writerSid);
        if (ids.code != 0) return false;
        rp::RecordBatchBuilder b(0, 0);
        b.setProducerId(ids.pid); b.setProducerEpoch(ids.epoch); b.setBaseSequence(0);
        b.append(key, value, 0, /*attrs: grant*/ 1);
        const std::vector<uint8_t> batch = b.build();
        int last = -1;
        for (int attempt = 0; attempt < 200; ++attempt) {
            const int64_t corr = rig.c().nextCorrelationId();
            if (!rig.c().produceIdempotent(rig.broker().workerIngressChannel(), AuthRig::stream(), writerSid, corr, topicId, 0,
                                           rp::client::kAcksWireAll | rp::client::kAcksWireView, ids.pid, ids.epoch, 0, batch))
                return false;
            const auto a = rig.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
            last = a ? a->errorCode : -1;
            if (last == 0) return true;
            if (last != int(rp::client::ErrorCode::NotLeader) && last != int(rp::client::ErrorCode::UnknownTopic) && last != -1) break;
            std::this_thread::sleep_for(25ms);
        }
        std::cout << "[lease] grant produce answered " << last << "\n";
        return false;
    }
};
} // namespace

TEST(LeaseTopicBrokerRegression, LeaseViewReadThroughTheRealWiringAndRequiredAuthRegression) {
    AuthBroker::Options o;
    o.shard = true;
    AuthRig rig("leasewire", o);
    rig.broker().createUser("lbuild", AuthRig::kPw, {grants::cluster(SO::Create), grants::cluster(SO::Alter), grants::cluster(SO::Drop),
                                                       grants::cluster(SO::Describe)});
    rig.broker().createUser("lwriter", AuthRig::kPw, {grants::exact(SO::Write, ST::Topic, "inst")});
    rig.broker().createUser("lreader", AuthRig::kPw, {grants::exact(SO::Read, ST::Topic, "inst")});
    rig.broker().createUser("lviewonly", AuthRig::kPw, {grants::exact(SO::Read, ST::View, "inst.leases")});
    LeaseWiring w{rig};

    int64_t builder = w.loginAs("lbuild");
    const auto def = rig.c().awaitDefine(
        rig.c().sendDefineLeaseTopic(rig.ch(), AuthRig::stream(), builder, "inst", 1, userBlob(), validSpec()), AuthRig::dl(30));
    ASSERT_TRUE(def.has_value());
    ASSERT_EQ(def->errorCode, kOk);
    w.topicId = def->topicId;
    w.viewId  = w.topicId + 1;   // the catalog allocates the lease view right after the topic (asserted below against the registry)

    // The registry-held mapping is set after create.
    ASSERT_TRUE(eventually([&] { return w.mapping().lease == w.topicId; })) << "leaseSourceOfView_ after create";
    EXPECT_EQ(w.mapping().viewSource, w.topicId) << "viewSourceOf_ answers the lease source first";

    int64_t writer = w.loginAs("lwriter");
    ASSERT_TRUE(w.grantKey(writer));

    // Topic READ: Ok with a row.  VIEW-only on the lease view: NotAuthorized (the rule is READ on the SOURCE topic).
    int64_t reader = w.loginAs("lreader");
    int64_t viewOnly = w.loginAs("lviewonly");
    {
        const auto r = w.readK(reader);
        EXPECT_EQ(r.first, kStatusOk) << "a topic-READ session reads the lease row";
        EXPECT_FALSE(r.second.empty()) << "the row bytes are carried";
        EXPECT_EQ(w.readK(viewOnly).first, kStatusNotAuthorized) << "VIEW on <t>.leases alone is not the rule";
        EXPECT_EQ(w.readK(writer).first, kStatusNotAuthorized) << "WRITE is not READ";
    }

    // Restart on the same data dir: the boot stash re-registers the mapping; the same reads hold.
    rig.restart();
    builder = w.loginAs("lbuild");
    ASSERT_TRUE(eventually([&] { return w.mapping().lease == w.topicId; }, 30s)) << "leaseSourceOfView_ after restart";
    EXPECT_EQ(w.mapping().viewSource, w.topicId);
    reader = w.loginAs("lreader");
    viewOnly = w.loginAs("lviewonly");
    {
        const auto r = w.readK(reader, /*retryUnknown=*/true);
        EXPECT_EQ(r.first, kStatusOk) << "after restart the lease row is read again";
        EXPECT_FALSE(r.second.empty());
        EXPECT_EQ(w.readK(viewOnly, true).first, kStatusNotAuthorized);
    }

    // A real DropTopic: the registry entry dies with the topic, so both accessors answer -1 and the read is UnknownView.
    ASSERT_EQ(rig.dropTopicCode(builder, "inst"), kOk);
    ASSERT_TRUE(eventually([&] { return w.mapping().lease == -1; })) << "leaseSourceOfView_ after DropTopic";
    EXPECT_EQ(w.mapping().viewSource, -1);
    // A session whose only grant was on the dropped topic fails closed (NotAuthorized: nothing resolves for it any more, the
    // existence of the dropped view is not revealed); a session allowed to read every view (the bootstrap admin) sees the truth.
    EXPECT_EQ(w.readK(reader).first, kStatusNotAuthorized) << "a dropped lease view fails closed for a grant on the dropped topic";
    EXPECT_EQ(w.readK(w.loginAs2("admin", AuthBroker::kAdminPassword)).first, kStatusUnknownView) << "the dropped lease view is unknown";
}

// The 3-node leg of H1: a lease view read at a broker that does not lead the source partition is a Redirect to the source
// leader (the gateway's viewSourceLedElsewhere_ / registry-held mapping on a follower), and the leader serves the row.
TEST(LeaseTopicBrokerRegression, LeaseViewReadAtAFollowerRedirectsToTheSourceLeaderRegression) {
    using namespace rune::node::test::catalog_cluster;
    auto c = ThreeNodes::start("leasef3");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    ShardTestClient cl(c.clientsDir->path / "lease-client");
    cl.setClientVersion(rp::client::kClientVersionLeases);
    cl.keepSessionsAlive(true);
    const auto chOf = [&](int id) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.client[std::size_t(id - 1)]); };
    int64_t sid[4] = {0, 0, 0, 0};
    for (int id = 1; id <= 3; ++id) {
        sid[id] = cl.connect(chOf(id), kClientStream);
        ASSERT_GT(sid[id], 0) << "node " << id;
    }
    // Define through whichever node leads the catalog (a follower answers Redirect, which awaitDefine does not return): try all, bounded.
    int32_t topicId = -1;
    for (const auto deadline = Clock::now() + 60s; topicId < 0 && Clock::now() < deadline;) {
        for (int id = 1; id <= 3 && topicId < 0; ++id) {
            const auto r = cl.awaitDefine(cl.sendDefineLeaseTopic(chOf(id), kClientStream, sid[id], "inst", 1, userBlob(), validSpec()),
                                          Clock::now() + 5s);
            if (r && r->errorCode == kOk) topicId = r->topicId;
        }
    }
    ASSERT_GE(topicId, 0) << "the lease topic is defined";
    const int32_t viewId = topicId + 1;

    // The source partition's leader, as every broker's metadata sees it.
    int leader = -1;
    ASSERT_TRUE(eventually([&] {
        int agreed = -1;
        for (int id = 1; id <= 3; ++id) {
            const int32_t l = c.node(id).onControlThreadForTest([&]() -> int32_t {
                const MetadataService* ms = c.node(id).metadataServiceForTest();
                const auto* pa = ms ? ms->assignment(topicId, 0) : nullptr;
                return pa ? pa->leaderNode : -1;
            });
            if (l < 1) return false;
            if (agreed >= 0 && agreed != l) return false;
            agreed = l;
        }
        leader = agreed;
        return true;
    }, 60s));
    const int follower = leader == 1 ? 2 : 1;

    // Grant a key straight to the leader's worker (idempotent, acks All|View).
    std::string key(64, '\0'); key.replace(0, 4, "a-01");
    std::string value;
    value.push_back(static_cast<char>(96)); value.push_back(0); value.push_back(1); value.push_back(0);
    const auto put64 = [&](uint64_t v) { for (int i = 0; i < 8; ++i) value.push_back(static_cast<char>(v >> (8 * i))); };
    put64(0); put64(5'000); put64(1); put64(0);
    value += key;
    rp::RecordBatchBuilder b(0, 0);
    b.setProducerId(777); b.setProducerEpoch(0); b.setBaseSequence(0);
    b.append(key, value, 0, 1);
    const std::vector<uint8_t> batch = b.build();
    const std::string ingress = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.ib[std::size_t(leader - 1)]);
    int granted = -1;
    for (int attempt = 0; attempt < 200 && granted != 0; ++attempt) {
        const int64_t corr = cl.nextCorrelationId();
        ASSERT_TRUE(cl.produceIdempotent(ingress, kClientStream, sid[leader], corr, topicId, 0,
                                         rp::client::kAcksWireAll | rp::client::kAcksWireView, 777, 0, 0, batch));
        const auto a = cl.awaitProduceResponseUntil(corr, Clock::now() + 10s);
        granted = a ? a->errorCode : -1;
        if (granted != 0) std::this_thread::sleep_for(25ms);
    }
    ASSERT_EQ(granted, 0) << "the grant is acked by the source leader";

    const auto kb = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size());
    // The follower: a Redirect naming the leader's client endpoint (retriable answers while its registry / metadata settle).
    ShardTestClient::ViewRowAnswer fa;
    ASSERT_TRUE(eventually([&] {
        const auto a = cl.awaitViewRowOrError(cl.sendGetViewRowK(chOf(follower), kClientStream, sid[follower], viewId, kb), Clock::now() + 10s);
        if (!a) return false;
        fa = *a;
        return a->isError && a->errorCode == int16_t(rp::client::ErrorCode::Redirect);
    }, 60s)) << "a follower answers Redirect (last: error=" << fa.isError << " code=" << (fa.isError ? fa.errorCode : fa.status) << ")";
    EXPECT_NE(fa.message.find(std::to_string(c.ports.client[std::size_t(leader - 1)])), std::string::npos)
        << "the Redirect names the source leader: " << fa.message;
    // The leader serves the row.
    int code = -1;
    std::size_t rowLen = 0;
    ASSERT_TRUE(eventually([&] {
        const auto a = cl.awaitViewRowOrError(cl.sendGetViewRowK(chOf(leader), kClientStream, sid[leader], viewId, kb), Clock::now() + 10s);
        if (!a) return false;
        code = a->isError ? a->errorCode : a->status;
        rowLen = a->row.size();
        return !a->isError && a->status == 0;
    }, 60s)) << "the leader serves the lease row (last code " << code << ")";
    EXPECT_GT(rowLen, 0u);
}

// ── Checkpoint C: a REAL broker's ScanLeases (1 node, 2 partitions, 300 keys) ─────────────────────────────────────────────────
// Raw ScanLeases frames through ShardTestClient: every partition paged with maxRows 64 to done -> every key exactly once; the node restarts
// and the same set comes back (the index rebuilds from the log); READ on the source topic serves the scan, a VIEW-only grant on `<t>.leases`
// is NotAuthorized (done 1).  The 3-node LeaderChangeMidScan leg is Task 17's.
// Mutation: authorise ScanLeases on the VIEW grant -> the VIEW-only session is served: red.
namespace {
struct ScanWire {
    AuthRig& rig;
    int32_t  viewId = -1;

    struct Page {
        bool        got = false;
        int         code = -1;      // ScanLeasesResponse code, or the ErrorResponse code
        bool        isError = false;
        bool        done = false;
        uint64_t    lastHash = 0;
        std::vector<std::vector<uint8_t>> keys;
    };
    Page page(int64_t sid, int32_t partition, uint64_t resume, bool first, uint16_t maxRows) {
        Page p;
        const int64_t corr = rig.c().sendScanLeases(rig.ch(), AuthRig::stream(), sid, viewId, partition, resume, first, maxRows);
        if (corr == 0) return p;
        const auto raw = rig.c().awaitRaw(corr, Clock::now() + 10s);
        if (!raw) return p;
        p.got = true;
        if (const auto s = rp::client::ScanLeasesResponse::decode(raw->data(), raw->size())) {
            p.code = s->code; p.done = s->done == 1; p.lastHash = s->lastHash;
            std::size_t at = 0;
            for (int i = 0; i < s->rowCount; ++i) {
                const std::size_t kl = s->rows[at];
                p.keys.emplace_back(s->rows.begin() + std::ptrdiff_t(at + 1), s->rows.begin() + std::ptrdiff_t(at + 1 + kl));
                uint32_t rl = 0;
                std::memcpy(&rl, s->rows.data() + at + 1 + kl, 4);
                at += 5 + kl + rl;
            }
        } else if (const auto e = rp::client::ErrorResponse::decode(raw->data(), raw->size())) {
            p.isError = true; p.code = e->code;
        }
        return p;
    }
    /// Every key of every partition, paged to done.  A transient answer (NotLeader / Throttled while leadership or the index settles) is
    /// retried by restarting that partition's scan; bounded.
    std::set<std::vector<uint8_t>> scanAll(int64_t sid, int32_t partitions, std::size_t* total = nullptr) {
        std::set<std::vector<uint8_t>> all;
        std::size_t seen = 0;
        for (int32_t part = 0; part < partitions; ++part) {
            const auto deadline = Clock::now() + 60s;
            bool finished = false;
            while (!finished && Clock::now() < deadline) {
                std::set<std::vector<uint8_t>> mine;
                std::size_t mineSeen = 0;
                uint64_t resume = 0;
                bool first = true, restart = false;
                for (int pg = 0; pg < 64 && !restart; ++pg) {
                    const Page p = page(sid, part, resume, first, 64);
                    if (!p.got || p.code != 0) { restart = true; std::this_thread::sleep_for(25ms); break; }
                    for (const auto& k : p.keys) { mine.insert(k); ++mineSeen; }
                    resume = p.lastHash; first = false;
                    if (p.done) { finished = true; break; }
                }
                if (finished) { all.insert(mine.begin(), mine.end()); seen += mineSeen; }
            }
            EXPECT_TRUE(finished) << "partition " << part << " was paged to done";
        }
        if (total != nullptr) *total = seen;
        return all;
    }
};
} // namespace

TEST(LeaseTopicBrokerRegression, ScanLeasesThroughTheRealBrokerRegression) {
    constexpr int kKeys = 300;
    constexpr int32_t kParts = 2;
    AuthBroker::Options o;
    o.shard = true;
    AuthRig rig("leasescan", o);
    rig.c().captureRawAnswers(true);
    rig.broker().createUser("lbuild", AuthRig::kPw, {grants::cluster(SO::Create), grants::cluster(SO::Alter), grants::cluster(SO::Drop),
                                                       grants::cluster(SO::Describe)});
    rig.broker().createUser("lwriter", AuthRig::kPw, {grants::exact(SO::Write, ST::Topic, "inst")});
    rig.broker().createUser("lreader", AuthRig::kPw, {grants::exact(SO::Read, ST::Topic, "inst")});
    rig.broker().createUser("lviewonly", AuthRig::kPw, {grants::exact(SO::Read, ST::View, "inst.leases")});
    LeaseWiring w{rig};
    ScanWire sw{rig};

    rp::LeaseSpec spec = validSpec();
    spec.maxKeys = 1000;
    const int64_t builder = w.loginAs("lbuild");
    const auto def = rig.c().awaitDefine(
        rig.c().sendDefineLeaseTopic(rig.ch(), AuthRig::stream(), builder, "inst", kParts, userBlob(), spec), AuthRig::dl(30));
    ASSERT_TRUE(def.has_value());
    ASSERT_EQ(def->errorCode, kOk);
    w.topicId = def->topicId;
    sw.viewId = w.viewId = w.topicId + 1;
    ASSERT_TRUE(eventually([&] { return w.mapping().lease == w.topicId; }));

    // 300 grants: keys "k-0000".."k-0299" (the 64-byte id field), split by the partitioner, produced in batches of 50 per partition.
    const auto keyOf = [](int i) { std::string k(64, '\0'); const std::string n = "k-" + std::to_string(10000 + i).substr(1); k.replace(0, n.size(), n); return k; };
    std::vector<std::vector<std::string>> byPart(kParts);
    std::set<std::vector<uint8_t>> expected;
    for (int i = 0; i < kKeys; ++i) {
        const std::string k = keyOf(i);
        const std::span<const uint8_t> kb(reinterpret_cast<const uint8_t*>(k.data()), k.size());
        byPart[std::size_t(rp::partitionForKey(kb, kParts))].push_back(k);
        expected.insert(std::vector<uint8_t>(kb.begin(), kb.end()));
    }
    const int64_t writer = w.loginAs("lwriter");
    const auto ids = rig.initProducer(writer);
    ASSERT_EQ(ids.code, 0);
    for (int32_t part = 0; part < kParts; ++part) {
        int32_t seq = 0;
        for (std::size_t from = 0; from < byPart[std::size_t(part)].size(); from += 50) {
            const std::size_t to = std::min(byPart[std::size_t(part)].size(), from + 50);
            rp::RecordBatchBuilder b(0, 0);
            b.setProducerId(ids.pid); b.setProducerEpoch(ids.epoch); b.setBaseSequence(seq);
            for (std::size_t i = from; i < to; ++i) {
                const std::string& key = byPart[std::size_t(part)][i];
                std::string value;
                value.push_back(static_cast<char>(96)); value.push_back(0);
                value.push_back(1); value.push_back(0);
                const auto put64 = [&](uint64_t v) { for (int j = 0; j < 8; ++j) value.push_back(static_cast<char>(v >> (8 * j))); };
                put64(0); put64(60'000); put64(1); put64(0);
                value += key;
                b.append(key, value, 0, /*attrs: grant*/ 1);
            }
            const std::vector<uint8_t> batch = b.build();
            int last = -1;
            for (int attempt = 0; attempt < 200 && last != 0; ++attempt) {
                const int64_t corr = rig.c().nextCorrelationId();
                ASSERT_TRUE(rig.c().produceIdempotent(rig.broker().workerIngressChannel(), AuthRig::stream(), writer, corr, w.topicId, part,
                                                      rp::client::kAcksWireAll | rp::client::kAcksWireView, ids.pid, ids.epoch, seq, batch));
                const auto a = rig.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
                last = a ? a->errorCode : -1;
                if (last != 0) std::this_thread::sleep_for(25ms);
            }
            ASSERT_EQ(last, 0) << "partition " << part << " grants acked";
            seq += static_cast<int32_t>(to - from);
        }
    }

    int64_t reader = w.loginAs("lreader");
    int64_t viewOnly = w.loginAs("lviewonly");
    {
        std::size_t total = 0;
        const auto got = sw.scanAll(reader, kParts, &total);
        EXPECT_EQ(got, expected) << "every key once, across both partitions";
        EXPECT_EQ(total, std::size_t(kKeys)) << "no key twice";
        const ScanWire::Page denied = sw.page(viewOnly, 0, 0, true, 64);
        ASSERT_TRUE(denied.got);
        EXPECT_EQ(denied.code, kStatusNotAuthorized) << "VIEW on <t>.leases alone is not the rule";
        EXPECT_TRUE(denied.done);
        EXPECT_TRUE(denied.keys.empty());
    }

    rig.restart();
    reader = w.loginAs("lreader");
    viewOnly = w.loginAs("lviewonly");
    ASSERT_TRUE(eventually([&] { return w.mapping().lease == w.topicId; }, 30s));
    {
        std::size_t total = 0;
        const auto got = sw.scanAll(reader, kParts, &total);
        EXPECT_EQ(got, expected) << "after a restart the same keys come back";
        EXPECT_EQ(total, std::size_t(kKeys));
        const ScanWire::Page denied = sw.page(viewOnly, 1, 0, true, 64);
        ASSERT_TRUE(denied.got);
        EXPECT_EQ(denied.code, kStatusNotAuthorized);
    }
}
