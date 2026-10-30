// CatalogBootTest.cpp — catalog history Task 16: the broker BOOTS FROM __catalog.
//
// REAL BrokerNodes over REAL Aeron, driven by the wire client (ShardTestClient): DDL goes
// through the production write path (gateway -> admin ring -> __catalog event -> applier ->
// effects), a restart is a fresh BrokerNode over the same data dir.  What is pinned:
//   F4  the ONE boot replay runs on the startup thread, before any boot builder: the
//       TopicRegistry / replication / placement see every catalog topic at start();
//   B1  a checkpoint ahead of the local log, or a corrupt one, REFUSES to start (naming both
//       offsets); a missing one means "applied nothing yet";
//   B3  dropped topics stay dropped across restarts (a Dropped slot for every id the catalog
//       dropped, including ones this broker never built; names re-creatable under NEW ids;
//       placement removed);
//   B4  every effect kind survives two real restarts with identical digest / ids / states / data;
//   B5  a halted boot replay boots halted and does NOTHING destructive;
//   B6  a broker whose local catalog is empty while its data dir holds topic dirs does not
//       reconcile until its applier caught up to the catalog leader;
//   B7  leftover __topic_schemas / __views dirs are ignored (never read, never touched);
//   B8  boot never deletes a topic dir (it quarantines) and purges only dropped/ entries the
//       catalog marks dropped.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/NamespacePaths.h>
#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/Descriptor.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/BrokerNode.h>
#include <rune/gateway/TopicDirMarker.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/TopicDirReconcile.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/storage/Log.h>
#include "support/CatalogBrokerCluster.h"
#include "support/DdlHarness.h"            // charBlob / viewPlanBlob / alterOpsFor
#include "support/OwnerThreadTestHelpers.h" // leoOnOwner
#include "support/ShardTestClient.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
using rune::node::test::catalog_cluster::Mode;
using rune::node::test::catalog_cluster::ThreeNodes;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using Clock = std::chrono::steady_clock;
using TopicState = rune::gateway::TopicRegistry::TopicState;

namespace {

struct TempDir {
    fs::path path;
    explicit TempDir(const std::string& tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_boot_" + tag + "_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(seq++));
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
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

template <class Pred>
bool awaitTrue(Pred p, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    while (Clock::now() < deadline) {
        if (p()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return p();
}

std::vector<uint8_t> oneRecord(const std::string& value) {   // typed topics validate: a valid record carrying `value` as its tail
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(charKey(), charValue(value), 0);
    return b.build();
}

std::string slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

/// name -> (relative path -> bytes) of every regular file below `root` (a tree digest).
std::map<std::string, std::string> treeOf(const fs::path& root) {
    std::map<std::string, std::string> out;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(root, ec), end; !ec && it != end; it.increment(ec)) {
        const std::string rel = fs::relative(it->path(), root).string();
        out[rel] = fs::is_regular_file(it->symlink_status()) ? slurp(it->path()) : "<dir>";
    }
    return out;
}

bool quarantineHasEntryStartingWith(const fs::path& dataDir, const std::string& prefix) {
    std::error_code ec;
    for (fs::directory_iterator it(rune::node::quarantineRoot(dataDir, 0), ec), end; !ec && it != end; it.increment(ec))
        if (it->path().filename().string().rfind(prefix, 0) == 0) return true;
    return false;
}

/// ONE single-node broker over a data dir that survives restarts, plus a wire session.
class BootRig {
public:
    struct Def { std::string name; uint32_t partitions = 1; bool typed = false; uint8_t persistence = 0; bool validate = false; };
    struct Res { int code = -1; };

    explicit BootRig(const std::string& tag) : dir_(tag), cfg_(makeSingleNodeConfig(dir_.path)) {}
    ~BootRig() { stop(); }

    const fs::path& dataDir() const { return dir_.path; }
    BrokerNode& node() { return *node_; }

    /// Construct + start() the broker (may throw: the refusal tests).
    void startNoAwait() {
        node_ = std::make_unique<BrokerNode>(cfg_);
        if (holdApplyAtBoot_) node_->holdCatalogApplyAtBootForTest(true);
        node_->start();
    }
    /// A started broker serving DDL: the consensus leader, the catalog leader serving, and a wire session.
    bool start() {
        startNoAwait();
        if (!node_->awaitLeader(30s)) return false;
        if (!awaitTrue([&] { return node_->onAdminThreadForTest([&] {
                return node_->adminWorkerAgentForTest()->worker().isLeader() ||
                       node_->adminWorkerAgentForTest()->worker().applier().halted(); }); }, 30s))
            return false;
        client_ = std::make_unique<ShardTestClient>(dir_.path / ("client-aeron" + std::to_string(++clients_)));
        client_->keepSessionsAlive(true);
        sid_ = client_->connect(cfg_.clientEndpoint.channel, cfg_.clientEndpoint.streamId);
        return sid_ > 0;
    }
    void stop() {
        client_.reset();
        if (node_) { node_->stop(); node_.reset(); }
    }
    bool restart() { stop(); return start(); }
    bool isServing() { return node_ && node_->isLeader() && client_ && sid_ > 0; }

    // ── DDL, through the production wire path ──────────────────────────────
    Res define(const Def& d) {
        const auto r = client_->awaitDefine(
            client_->sendDefineTopic(gw(), stream(), sid_, d.name, d.partitions,
                                     d.typed ? charBlob() : std::vector<uint8_t>{}, d.persistence,
                                     rune::protocol::RequestId{}, 0, boundedTestTopicConfig(), d.validate),
            Clock::now() + 10s);
        return {r ? static_cast<int>(r->errorCode) : -1};
    }
    Res alterSchema(const std::string& name, Op op = Op::AlterAdd) {
        const auto r = client_->awaitAlter(
            client_->sendAlterTopic(gw(), stream(), sid_, name, 0, alterOpsFor(op, uniq_++)), Clock::now() + 10s);
        return {r ? static_cast<int>(r->errorCode) : -1};
    }
    Res alterConfig(const std::string& name, int64_t retentionMs) {
        const pc::ConfigChangeWire ch{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), retentionMs};
        return ddl(client_->sendAlterTopicConfig(gw(), stream(), sid_, name, 0, {ch}));
    }
    Res dropTopic(const std::string& name) { return ddl(client_->sendDropTopic(gw(), stream(), sid_, name)); }
    Res dropView(const std::string& name) { return ddl(client_->sendDropView(gw(), stream(), sid_, name)); }
    Res createView(const std::string& name, const std::string& src, uint8_t persistence = 0) {
        const auto srcId = topicIdForName(src);
        if (!srcId) return {-2};
        const auto r = client_->awaitCreateView(
            client_->sendCreateView(gw(), stream(), sid_, name, "id", persistence,
                                    viewPlanBlob(name, *srcId, persistence)),
            Clock::now() + 10s);
        return {r ? static_cast<int>(r->errorCode) : -1};
    }
    Res alterView(const std::string& name, const std::string& src) {
        const auto srcId = topicIdForName(src);
        if (!srcId) return {-2};
        return ddl(client_->sendAlterView(gw(), stream(), sid_, name, "id", 0, 0,
                                          viewPlanBlob(name, std::vector<int32_t>{*srcId}, 0, /*extraColumn=*/true)));
    }
    std::optional<ShardTestClient::SchemaAnswer> schema(const std::string& name) {
        return client_->awaitSchema(client_->sendGetTopicSchema(gw(), stream(), sid_, name), Clock::now() + 10s);
    }
    bool produce(int32_t topicId, int32_t partition, const std::string& value) {
        return client_->produceUntilOk(gw(), stream(), sid_, topicId, partition, /*acks=*/1, oneRecord(value))
            .has_value();
    }
    /// The produce error code of one attempt (0 = OK).
    int produceCode(int32_t topicId, int32_t partition, const std::string& value = "probe") {
        const int64_t corr = ++pcorr_;
        if (!client_->produce(gw(), stream(), sid_, corr, topicId, partition, /*acks=*/1, oneRecord(value)))
            return -1;
        const auto r = client_->awaitProduceResponse(corr);
        return r ? r->errorCode : -1;
    }

    // ── observations (owner threads) ───────────────────────────────────────
    uint64_t catalogDigest() {
        return node_->onAdminThreadForTest([&] { return node_->adminWorkerAgentForTest()->worker().applier().state().digest(); });
    }
    int64_t catalogAppliedOffset() {
        return node_->onAdminThreadForTest([&] { return node_->adminWorkerAgentForTest()->worker().applier().nextOffset(); });
    }
    std::optional<int32_t> topicIdForName(const std::string& name) {
        return node_->onControlThreadForTest(
            [&] { return node_->gatewayForTest()->topicRegistry()->topicIdForName(name); });
    }
    TopicState topicState(int32_t id) {
        return node_->onControlThreadForTest([&] { return node_->gatewayForTest()->topicRegistry()->state(id); });
    }
    int32_t registryTopicCount() {
        return node_->onControlThreadForTest([&] { return node_->gatewayForTest()->topicRegistry()->topicCount(); });
    }
    bool hasPlacement(int32_t topicId, int32_t partition) {
        return node_->onControlThreadForTest(
            [&] { return node_->metadataServiceForTest()->assignment(topicId, partition) != nullptr; });
    }
    std::vector<int32_t> removedPlacementIds() {
        return node_->onControlThreadForTest([&] { return node_->metadataServiceForTest()->removedTopicIdsForTest(); });
    }
    int64_t logEndOffset(int32_t topicId, int32_t partition) { return leoOnOwner(*node_, topicId, partition); }
    bool awaitServes(int32_t topicId, std::chrono::milliseconds timeout = 15s) {
        return awaitTrue([&] { return node_->catalogTopicServesForTest(topicId); }, timeout);
    }
    bool awaitViewReady(const std::string& view, std::chrono::milliseconds timeout) {
        const auto id = topicIdForName(view);
        if (!id) return false;
        return awaitTrue([&] {
            const int64_t corr = client_->sendGetViewRow(gw(), stream(), sid_, *id, 7);
            if (corr == 0) return false;
            const auto st = client_->awaitViewRow(corr, Clock::now() + 5s);
            // kOk / kAbsentKey = the view's plan is installed and its sink serves; kUnknownView = it was never installed.
            return st && (*st == pc::ViewRowResponse::kOk || *st == pc::ViewRowResponse::kAbsentKey);
        }, timeout);
    }
    int catalogHaltedCounter() {   // 1 iff the applier is halted (the metric's value)
        return node_->onAdminThreadForTest([&] { return node_->adminWorkerAgentForTest()->worker().applier().halted() ? 1 : 0; });
    }
    std::string applierPhaseName() { return node_->catalogApplierPhaseForTest(); }
    int bootReplaysForTest() { return node_->bootReplaysForTest(); }
    bool registryHadTopicBeforeReplicationBuild(int32_t id) {
        return node_->registryHadTopicBeforeReplicationBuildForTest(id);
    }

    // ── on-disk manipulation (the broker must be stopped) ──────────────────
    void corruptCheckpointDigest() {
        const std::string cdir = (dir_.path / "__catalog").string();
        auto c = readCatalogCheckpoint(cdir);
        ASSERT_TRUE(c.has_value());
        c->digest ^= 1;
        ASSERT_TRUE(writeCatalogCheckpoint(cdir, *c));
    }
    void holdApplyAtBoot(bool on) { holdApplyAtBoot_ = on; }

private:
    std::string gw() const { return cfg_.clientEndpoint.channel; }
    int32_t stream() const { return cfg_.clientEndpoint.streamId; }
    Res ddl(int64_t corr) {
        const auto a = client_->awaitDdl(corr, Clock::now() + 10s);
        return {a ? static_cast<int>(a->errorCode) : -1};
    }

    TempDir                          dir_;
    NodeConfig                       cfg_;
    std::unique_ptr<BrokerNode>      node_;
    std::unique_ptr<ShardTestClient> client_;
    int64_t                          sid_ = 0;
    int                              clients_ = 0;
    uint64_t                         uniq_ = 1;
    int64_t                          pcorr_ = 5'000'000;
    bool                             holdApplyAtBoot_ = false;
};

} // namespace

// ═════════════════════════════════════════════════════════════════════════════
// The brief's tests
// ═════════════════════════════════════════════════════════════════════════════
TEST(CatalogBootRegression, TopicsSchemasAndViewsComeBackFromTheCatalogAfterRestart) {
    BootRig r("boot_all");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "players", .partitions = 2, .typed = true}).code, kOk);
    ASSERT_EQ(r.define({.name = "events", .partitions = 3}).code, kOk);
    ASSERT_EQ(r.createView("top", "players").code, kOk);
    ASSERT_EQ(r.dropTopic("events").code, kOk);
    ASSERT_TRUE(r.awaitServes(0));
    ASSERT_TRUE(r.produce(0, 0, "v"));
    const uint64_t digest = r.catalogDigest();
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.catalogDigest(), digest);
    EXPECT_EQ(r.topicIdForName("players"), 0);
    EXPECT_FALSE(r.topicIdForName("events").has_value()) << "a dropped topic stays dropped";
    EXPECT_EQ(r.topicState(1), TopicState::Dropped);
    EXPECT_EQ(r.logEndOffset(0, 0), 1) << "data survived";
    EXPECT_TRUE(r.awaitViewReady("top", 10s));
}

TEST(CatalogBootRegression, BootReplaysBeforeAnyBuilderSoTopicsServeImmediately) {
    BootRig r("boot_order");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "t", .partitions = 1}).code, kOk);
    ASSERT_TRUE(r.restart());
    EXPECT_TRUE(r.registryHadTopicBeforeReplicationBuild(0))
        << "startup-phase hook: TopicRegistry built with t before replication was built";
    EXPECT_EQ(r.node().bootTopicsForTest().size(), 1u);
}

TEST(CatalogBootRegression, UnclaimedTopicDirIsQuarantinedNeverDeleted) {
    BootRig r("boot_quar");
    ASSERT_TRUE(r.start());
    r.stop();
    fs::create_directories(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0");
    { std::ofstream(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0" / "00000000000000000000.log") << "x"; }
    ASSERT_TRUE(r.start());   // start() awaits the (single-node) catch-up that runs the deferred reconcile
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(r.dataDir(), 0) / "ghost"));
    EXPECT_TRUE(quarantineHasEntryStartingWith(r.dataDir(), "ghost-"));
}

// The interrupted-purge edge case, B8: only the dropped/ entries the catalog marks dropped go.
TEST(CatalogBootRegression, LeftoverDroppedDirIsPurgedAtBootButOnlyForIdsTheCatalogDropped) {
    BootRig r("boot_purge");
    ASSERT_TRUE(r.start());
    for (const char* n : {"a", "b", "c", "d"}) ASSERT_EQ(r.define({.name = n}).code, kOk);   // ids 0..3
    ASSERT_EQ(r.dropTopic("d").code, kOk);                                                   // id 3 is dropped
    r.stop();
    fs::create_directories(rune::node::droppedRoot(r.dataDir(), 0) / "3.old");
    { std::ofstream(rune::node::droppedRoot(r.dataDir(), 0) / "3.old" / "left.log") << "x"; }
    fs::create_directories(rune::node::droppedRoot(r.dataDir(), 0) / "1.live");   // id 1 is LIVE: not the catalog's to purge
    fs::create_directories(rune::node::droppedRoot(r.dataDir(), 0) / "99.unknown");
    ASSERT_TRUE(r.start());
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "3.old")) << "an interrupted purge is finished at boot";
    EXPECT_TRUE(fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "1.live")) << "a live id's dropped/ entry is never deleted";
    EXPECT_TRUE(fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "99.unknown")) << "an id the catalog does not mark dropped";
}

// B7
TEST(CatalogBootRegression, RetiredCatalogDirsAreIgnoredNotRead) {
    BootRig r("boot_retired");
    // ISR design v3 Step 5: the dir is formatted before it is seeded (the subject is only that retired dirs
    // are never read; seeded state alone would be a partly deleted pre-storage-id dir, R-3).
    rune::node::test::formatFounderForTest(r.dataDir());
    fs::create_directories(r.dataDir() / "__topic_schemas");
    { std::ofstream(r.dataDir() / "__topic_schemas" / "00000000000000000000.log") << "garbage"; }
    fs::create_directories(r.dataDir() / "__views" / "0");
    { std::ofstream(r.dataDir() / "__views" / "00000000000000000000.log") << "more garbage"; }
    { std::ofstream(r.dataDir() / "__views" / "0" / "x.log") << "x"; }
    const auto before = treeOf(r.dataDir());
    ASSERT_TRUE(r.start()) << "no migration: the old catalog is never read";
    EXPECT_TRUE(r.isServing());
    ASSERT_EQ(r.define({.name = "t", .typed = true}).code, kOk) << "the broker serves DDL over a clean __catalog";
    r.stop();
    for (const char* retired : {"__topic_schemas", "__views"}) {
        const auto tree = treeOf(r.dataDir() / retired);
        for (const auto& [rel, bytes] : treeOf(r.dataDir() / retired)) {
            const auto it = before.find(std::string{retired} + "/" + rel);
            ASSERT_NE(it, before.end()) << retired << "/" << rel << " appeared: the broker wrote into a retired dir";
            EXPECT_EQ(it->second, bytes) << retired << "/" << rel << " was modified";
        }
        EXPECT_EQ(tree.size(), [&] { std::size_t n = 0; for (const auto& [k, v] : before) n += k.rfind(retired, 0) == 0 ? 1 : 0;
                                     return n - 1; /* the dir entry itself */ }())
            << "a retired dir lost or gained files";
    }
}

// F4, B2
TEST(CatalogBootRegression, TheCatalogPrefixIsAppliedExactlyOnceAtBootRegression) {
    // Task 9 replayed on the admin thread; Task 16 replays on the startup thread.  Both together would
    // re-apply recorded request ids (Precondition) and halt the broker on every restart.
    BootRig r("boot_once");
    ASSERT_TRUE(r.start());
    EXPECT_EQ(r.bootReplaysForTest(), 1) << "a fresh data dir: still the one (empty) boot replay";
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    ASSERT_EQ(r.define({.name = "b"}).code, kOk);
    EXPECT_EQ(r.bootReplaysForTest(), 1) << "a live DDL replays nothing";
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.catalogHaltedCounter(), 0);
    EXPECT_EQ(r.applierPhaseName(), "Live");
    EXPECT_EQ(r.bootReplaysForTest(), 1) << "one replayLocal that returned >= 0, on the startup thread";
    EXPECT_EQ(r.define({.name = "c"}).code, kOk);
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.bootReplaysForTest(), 1);
    EXPECT_EQ(r.catalogHaltedCounter(), 0);
}

TEST(CatalogBootRegression, HaltedBootReplayStillBootsAndRefusesDdl) {
    BootRig r("boot_halt");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    ASSERT_EQ(r.define({.name = "b"}).code, kOk);
    r.stop();
    r.corruptCheckpointDigest();
    ASSERT_TRUE(r.start());
    EXPECT_TRUE(r.isServing());
    EXPECT_EQ(r.define({.name = "c"}).code, kInternal);
    EXPECT_EQ(r.catalogHaltedCounter(), 1);
}

// ═════════════════════════════════════════════════════════════════════════════
// B5 — a halted boot replay does NOTHING destructive
// ═════════════════════════════════════════════════════════════════════════════
TEST(CatalogBootRegression, HaltedBootMovesRenamesDeletesAndQuarantinesNothingRegression) {
    BootRig r("boot_halt_nd");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);   // id 0
    ASSERT_EQ(r.define({.name = "b"}).code, kOk);   // id 1
    ASSERT_EQ(r.dropTopic("b").code, kOk);          // id 1 dropped
    r.stop();
    // Everything a healthy reconcile would act on:
    fs::create_directories(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0");
    { std::ofstream(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0" / "00000000000000000000.log") << "ghost"; }
    fs::create_directories(rune::node::droppedRoot(r.dataDir(), 0) / "1.b");                    // id 1: the catalog marks it dropped
    { std::ofstream(rune::node::droppedRoot(r.dataDir(), 0) / "1.b" / "left.log") << "left"; }
    r.corruptCheckpointDigest();
    const auto ghostBefore   = treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "ghost");
    const auto droppedBefore = treeOf(rune::node::droppedRoot(r.dataDir(), 0));
    ASSERT_TRUE(r.start());   // halted: still boots
    EXPECT_EQ(r.catalogHaltedCounter(), 1);
    EXPECT_EQ(r.node().bootReconcileStateForTest(), "None") << "a halted boot defers nothing either";
    r.stop();
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0" / "00000000000000000000.log"));
    EXPECT_TRUE(fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "1.b" / "left.log")) << "no purge on a halted boot";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0))) << "no quarantine on a halted boot";
    EXPECT_EQ(treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "ghost"), ghostBefore) << "the unclaimed dir is byte-identical";
    EXPECT_EQ(treeOf(rune::node::droppedRoot(r.dataDir(), 0)), droppedBefore);
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(r.dataDir(), 0) / "a" / "0")) << "the live topic's dir stayed where it was";
}

// ═════════════════════════════════════════════════════════════════════════════
// B1 — refuse a boot whose checkpoint is ahead of the log
// ═════════════════════════════════════════════════════════════════════════════
namespace {
std::string startMessage(BootRig& r) {
    try {
        r.startNoAwait();
    } catch (const std::runtime_error& ex) {
        r.stop();
        return ex.what();
    }
    r.stop();
    return {};
}
} // namespace

TEST(CatalogBootRegression, CheckpointAheadOfTheLogRefusesToStartNamingBothOffsetsRegression) {
    BootRig r("boot_b1_ahead");
    ASSERT_TRUE(r.start());
    for (const char* n : {"a", "b", "c"}) ASSERT_EQ(r.define({.name = n}).code, kOk);   // events 0..2, checkpoint 2
    r.stop();
    // The log lost the applied events (an operator replaced / truncated it): the segment files go, the checkpoint stays.
    for (const auto& e : fs::directory_iterator(r.dataDir() / "__catalog")) {
        const auto ext = e.path().extension();
        if (ext == ".log" || ext == ".index" || ext == ".timeindex") fs::remove(e.path());
    }
    const std::string msg = startMessage(r);
    ASSERT_FALSE(msg.empty()) << "the broker must REFUSE to start, never replay a shorter history";
    EXPECT_NE(msg.find("offset 2"), std::string::npos) << msg;      // the checkpoint's
    EXPECT_NE(msg.find("ends at 0"), std::string::npos) << msg;     // the log's
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0))) << "refused before anything was touched";
}

TEST(CatalogBootRegression, CheckpointOnePastTheLogEndAlsoRefusesToStartRegression) {
    BootRig r("boot_b1_edge");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    ASSERT_EQ(r.define({.name = "b"}).code, kOk);   // log end 2, checkpoint 1
    r.stop();
    const std::string cdir = (r.dataDir() / "__catalog").string();
    auto c = readCatalogCheckpoint(cdir);
    ASSERT_TRUE(c.has_value());
    ASSERT_EQ(c->appliedOffset, 1);
    c->appliedOffset = 2;   // names the offset the log has NOT got (log end = 2)
    ASSERT_TRUE(writeCatalogCheckpoint(cdir, *c));
    const std::string msg = startMessage(r);
    ASSERT_FALSE(msg.empty());
    EXPECT_NE(msg.find("offset 2"), std::string::npos) << msg;
    EXPECT_NE(msg.find("ends at 2"), std::string::npos) << msg;
}

TEST(CatalogBootRegression, CorruptCheckpointRefusesToStartExplicitlyRegression) {
    BootRig r("boot_b1_corrupt");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    r.stop();
    const fs::path ck = r.dataDir() / "__catalog" / kCatalogCheckpointFile;
    std::string bytes = slurp(ck);
    ASSERT_FALSE(bytes.empty());
    bytes[bytes.size() / 2] ^= 0x5A;
    { std::ofstream(ck, std::ios::binary | std::ios::trunc) << bytes; }
    const std::string msg = startMessage(r);
    ASSERT_FALSE(msg.empty()) << "a damaged checkpoint must not read as 'nothing applied'";
    EXPECT_NE(msg.find(kCatalogCheckpointFile), std::string::npos) << msg;
    // An unreadable (short) file refuses too.
    { std::ofstream(ck, std::ios::binary | std::ios::trunc) << "short"; }
    EXPECT_FALSE(startMessage(r).empty());
}

TEST(CatalogBootRegression, MissingCheckpointMeansAppliedNothingYetAndBootsWithEveryTopicRegression) {
    BootRig r("boot_b1_missing");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "players", .partitions = 2, .typed = true}).code, kOk);
    ASSERT_TRUE(r.awaitServes(0));
    ASSERT_TRUE(r.produce(0, 1, "kept"));
    const uint64_t digest = r.catalogDigest();
    r.stop();
    fs::remove(r.dataDir() / "__catalog" / kCatalogCheckpointFile);
    ASSERT_TRUE(r.start()) << "a missing checkpoint is fine: the whole log is applied live";
    EXPECT_EQ(r.catalogDigest(), digest);
    ASSERT_TRUE(r.awaitServes(0)) << "the live apply builds the topic over its surviving dir";
    EXPECT_EQ(r.logEndOffset(0, 1), 1) << "data survived";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0))) << "the deferred reconcile claimed the catalog's topics";
}

// ═════════════════════════════════════════════════════════════════════════════
// B3 — dropped topics stay dropped
// ═════════════════════════════════════════════════════════════════════════════
TEST(CatalogBootRegression, DroppedTopicsStayDroppedIncludingIdsThisBrokerNeverBuiltRegression) {
    BootRig r("boot_b3");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "keep", .typed = true}).code, kOk);   // 0
    ASSERT_EQ(r.define({.name = "b"}).code, kOk);                      // 1 (dropped: a Gap between live ids at boot)
    ASSERT_EQ(r.define({.name = "c"}).code, kOk);                      // 2 (dropped: the TAIL — beyond every live id)
    ASSERT_TRUE(r.awaitServes(1));
    ASSERT_TRUE(r.produce(1, 0, "doomed"));
    ASSERT_EQ(r.dropTopic("b").code, kOk);
    ASSERT_EQ(r.dropTopic("c").code, kOk);
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.node().bootDroppedIdsForTest(), (std::vector<int32_t>{1, 2}));
    EXPECT_EQ(r.removedPlacementIds(), (std::vector<int32_t>{1, 2}))
        << "boot hands every dropped id to the controller's placement removal (removeRuntimeTopic)";
    EXPECT_EQ(r.topicState(1), TopicState::Dropped);
    EXPECT_EQ(r.topicState(2), TopicState::Dropped) << "an id this broker never built still gets its Dropped slot";
    EXPECT_EQ(r.registryTopicCount(), 3);
    EXPECT_FALSE(r.topicIdForName("b").has_value());
    EXPECT_EQ(r.produceCode(1, 0), kUnknownTopic) << "a dropped topic never re-serves";
    EXPECT_EQ(r.produceCode(2, 0), kUnknownTopic);
    // The names are re-creatable ONLY under new ids; the dropped ids are never refilled.
    ASSERT_EQ(r.define({.name = "b"}).code, kOk);
    EXPECT_EQ(r.topicIdForName("b"), 3);
    ASSERT_TRUE(r.awaitServes(3));
    EXPECT_EQ(r.logEndOffset(3, 0), 0) << "a fresh, EMPTY topic: the dropped incarnation's data is not resurrected";
    EXPECT_EQ(r.topicState(1), TopicState::Dropped);
    // The controller unplaces them (placement removal), even across a restart that rebuilt the metadata log's map.
    EXPECT_TRUE(awaitTrue([&] { return !r.hasPlacement(1, 0) && !r.hasPlacement(2, 0); }, 15s))
        << "removeRuntimeTopic for every dropped id";
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.node().bootDroppedIdsForTest(), (std::vector<int32_t>{1, 2}));
    EXPECT_EQ(r.topicIdForName("b"), 3);
    EXPECT_EQ(r.topicState(1), TopicState::Dropped);
}

// Edge: a catalog with only drops.
TEST(CatalogBootRegression, CatalogWithOnlyDropsBootsEveryIdDroppedAndNothingLiveRegression) {
    BootRig r("boot_onlydrops");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    ASSERT_EQ(r.define({.name = "b", .typed = true}).code, kOk);
    ASSERT_EQ(r.dropTopic("a").code, kOk);
    ASSERT_EQ(r.dropTopic("b").code, kOk);
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.node().bootDroppedIdsForTest(), (std::vector<int32_t>{0, 1}));
    EXPECT_TRUE(r.node().bootTopicsForTest().empty());
    EXPECT_EQ(r.topicState(0), TopicState::Dropped);
    EXPECT_EQ(r.topicState(1), TopicState::Dropped);
    ASSERT_EQ(r.define({.name = "c"}).code, kOk);
    EXPECT_EQ(r.topicIdForName("c"), 2) << "ids are never reused";
    EXPECT_EQ(r.bootReplaysForTest(), 1);
}

// Edge: a re-created name after a drop.
TEST(CatalogBootRegression, ReCreatedNameAfterADropResolvesToItsNewIdAfterRestartRegression) {
    BootRig r("boot_recreate");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "players", .typed = true}).code, kOk);   // 0
    ASSERT_TRUE(r.awaitServes(0));
    ASSERT_TRUE(r.produce(0, 0, "old life"));
    ASSERT_EQ(r.dropTopic("players").code, kOk);
    ASSERT_EQ(r.define({.name = "players", .typed = true}).code, kOk);   // 1
    ASSERT_TRUE(r.awaitServes(1));
    ASSERT_TRUE(r.produce(1, 0, "new life"));
    ASSERT_TRUE(r.restart());
    EXPECT_EQ(r.topicIdForName("players"), 1);
    EXPECT_EQ(r.topicState(0), TopicState::Dropped);
    EXPECT_EQ(r.logEndOffset(1, 0), 1) << "only the new incarnation's record";
    EXPECT_EQ(r.produceCode(0, 0), kUnknownTopic);
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0)));
}

// Edge: an empty catalog.
TEST(CatalogBootRegression, EmptyCatalogBootsServesAndRestartsRegression) {
    BootRig r("boot_empty");
    ASSERT_TRUE(r.start());
    EXPECT_TRUE(r.node().bootTopicsForTest().empty());
    EXPECT_TRUE(r.node().bootDroppedIdsForTest().empty());
    EXPECT_EQ(r.applierPhaseName(), "Live");
    EXPECT_EQ(r.bootReplaysForTest(), 1);
    ASSERT_TRUE(r.restart());
    EXPECT_TRUE(r.node().bootTopicsForTest().empty());
    EXPECT_EQ(r.bootReplaysForTest(), 1);
    ASSERT_EQ(r.define({.name = "first"}).code, kOk);
    EXPECT_EQ(r.topicIdForName("first"), 0);
}

// ═════════════════════════════════════════════════════════════════════════════
// B4 — restart idempotence across a REAL restart
// ═════════════════════════════════════════════════════════════════════════════
namespace {
struct Snapshot {
    uint64_t digest = 0;
    int64_t  applied = -1;
    std::vector<int32_t> dropped;
    std::map<std::string, int32_t> ids;                       // live topic name -> id
    std::vector<TopicState> states;                           // ids 0..N-1
    std::map<std::pair<int32_t, int32_t>, int64_t> leo;       // (id, partition) -> log end
    std::map<std::string, std::tuple<int32_t, uint16_t, uint64_t>> schemas;   // name -> (id, version, fingerprint)
    bool operator==(const Snapshot&) const = default;
};

Snapshot snapshotOf(BootRig& r, const std::vector<std::pair<std::string, int>>& liveTopics /* name, partitions */) {
    Snapshot s;
    s.digest  = r.catalogDigest();
    s.applied = r.catalogAppliedOffset();
    s.dropped = r.node().bootDroppedIdsForTest();
    const int32_t n = r.registryTopicCount();
    for (int32_t id = 0; id < n; ++id) s.states.push_back(r.topicState(id));
    for (const auto& [name, parts] : liveTopics) {
        const auto id = r.topicIdForName(name);
        if (!id) continue;
        s.ids[name] = *id;
        for (int p = 0; p < parts; ++p) s.leo[{*id, p}] = r.logEndOffset(*id, p);
        const auto* e = r.node().onAdminThreadForTest([&] { return r.node().schemaRegistryForTest()->byName(name); });
        if (e != nullptr) s.schemas[name] = {e->topicId, e->version, e->fingerprint};
    }
    return s;
}
} // namespace

TEST(CatalogBootRegression, EveryEffectKindSurvivesTwoRealRestartsIdenticallyRegression) {
    BootRig r("boot_b4");
    ASSERT_TRUE(r.start());
    // create typed + untyped
    ASSERT_EQ(r.define({.name = "players", .partitions = 2, .typed = true}).code, kOk);       // 0
    ASSERT_EQ(r.define({.name = "events", .partitions = 3}).code, kOk);                       // 1
    // schema (an untyped topic gains its schema) + alter + set-compatibility
    ASSERT_EQ(r.define({.name = "late", .partitions = 1}).code, kOk);                         // 2
    ASSERT_EQ(r.define({.name = "late", .partitions = 1, .typed = true}).code, kOk);          // CreateSchema
    ASSERT_EQ(r.alterSchema("late", Op::AlterAdd).code, kOk);                                 // AlterSchema
    ASSERT_EQ(r.alterSchema("late", Op::AlterSetCompat).code, kOk);                           // SetCompatibility
    // config
    ASSERT_EQ(r.alterConfig("events", 90'000).code, kOk);
    // transient typed topic
    ASSERT_EQ(r.define({.name = "presence", .typed = true, .persistence = 1}).code, kOk);     // 3
    // views: create, alter (a fresh sink id), drop
    ASSERT_EQ(r.createView("top", "players").code, kOk);                                      // sink 4
    ASSERT_EQ(r.createView("doomed_view", "players").code, kOk);                              // sink 5
    ASSERT_EQ(r.alterView("top", "players").code, kOk);                                       // sink 6, 4 retired
    ASSERT_EQ(r.dropView("doomed_view").code, kOk);                                           // sink 5 retired
    // topic drop
    ASSERT_EQ(r.define({.name = "tmp", .typed = true}).code, kOk);                            // 7
    ASSERT_EQ(r.dropTopic("tmp").code, kOk);
    // data
    ASSERT_TRUE(r.awaitServes(0)); ASSERT_TRUE(r.awaitServes(1));
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(r.produce(0, 0, "p0-" + std::to_string(i)));
    ASSERT_TRUE(r.produce(0, 1, "p1"));
    for (int i = 0; i < 2; ++i) ASSERT_TRUE(r.produce(1, 2, "e2-" + std::to_string(i)));

    const std::vector<std::pair<std::string, int>> live = {{"players", 2}, {"events", 3}, {"late", 1}};   // VLS B.2: the sink "top" owns no log / replica -- not in the data snapshot
    const Snapshot s0 = snapshotOf(r, live);
    ASSERT_EQ(s0.ids.size(), 3u);

    ASSERT_TRUE(r.restart());
    const Snapshot s1 = snapshotOf(r, live);
    EXPECT_EQ(s1.digest, s0.digest) << "the catalog digest";
    EXPECT_EQ(s1.ids, s0.ids) << "topic ids";
    EXPECT_EQ(s1.states, s0.states) << "registry states";
    EXPECT_EQ(s1.leo, s0.leo) << "data";
    EXPECT_EQ(s1.schemas, s0.schemas) << "schema registry (id, version, fingerprint)";
    EXPECT_EQ(s1.applied, s0.applied);
    EXPECT_TRUE(s1.dropped == (std::vector<int32_t>{4, 5, 7}));
    EXPECT_EQ(r.bootReplaysForTest(), 1);
    EXPECT_EQ(r.catalogHaltedCounter(), 0);
    EXPECT_TRUE(r.awaitViewReady("top", 15s));

    ASSERT_TRUE(r.restart());
    const Snapshot s2 = snapshotOf(r, live);
    EXPECT_EQ(s2.digest, s0.digest);
    EXPECT_EQ(s2.ids, s0.ids);
    EXPECT_EQ(s2.states, s0.states);
    EXPECT_EQ(s2.leo, s0.leo);
    EXPECT_EQ(s2.schemas, s0.schemas);
    EXPECT_EQ(s2.dropped, s1.dropped);
    EXPECT_EQ(r.catalogHaltedCounter(), 0);
    EXPECT_TRUE(r.awaitViewReady("top", 15s));
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0))) << "nothing the catalog names was quarantined";
    // and the catalog still takes writes.
    ASSERT_EQ(r.define({.name = "after", .typed = true}).code, kOk);
    EXPECT_EQ(r.topicIdForName("after"), 8);
}

// A validate=true typed topic that evolved to v2: a producer still writing the OLDER registered
// v1 (the typed-transport gate accepts any known version) must be accepted by the workers both
// live and after a restart -- the boot fan-out must install EVERY registered version, not only
// the current one.
namespace {
/// A well-formed `character` v1 record value: [blockLength:u16][version:u16][id:i64][hp:i64].
std::string charRecordV1() {
    std::string v(4 + 16, '\0');
    v[0] = 16; v[2] = 1;
    v[4] = 7;
    return v;
}
} // namespace

TEST(CatalogBootRegression, OlderRegisteredSchemaVersionStillAcceptedByWorkersAfterRestartRegression) {
    BootRig r("boot_oldver");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "chars", .partitions = 1, .typed = true, .validate = true}).code, kOk);   // v1
    ASSERT_EQ(r.alterSchema("chars", Op::AlterAdd).code, kOk);                                             // v2
    const auto id = r.topicIdForName("chars");
    ASSERT_TRUE(id.has_value());
    ASSERT_TRUE(r.awaitServes(*id));
    ASSERT_EQ(r.schema("chars")->version, 2);
    EXPECT_EQ(r.produceCode(*id, 0, charRecordV1()), 0) << "live: the v1 validator was fanned out when v1 registered";
    ASSERT_TRUE(r.restart());
    const auto id2 = r.topicIdForName("chars");
    ASSERT_TRUE(id2.has_value());
    ASSERT_TRUE(r.awaitServes(*id2));
    EXPECT_EQ(r.produceCode(*id2, 0, charRecordV1()), 0)
        << "after restart the workers must still hold the v1 validator (SchemaValidationFailed = only the current version was pushed)";
}

// ═════════════════════════════════════════════════════════════════════════════
// B6 — a wiped local catalog does not reconcile before it caught up (3 brokers)
// ═════════════════════════════════════════════════════════════════════════════
namespace {
constexpr int32_t kClientStream = catalog_cluster::kClientStream;

std::string clientChannelOf(const ThreeNodes& c, int id) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(id - 1)]);
}

/// A typed one-partition topic defined through the catalog leader's wire endpoint, retried while the
/// leadership settles.
bool defineOnLeader(ThreeNodes& c, ShardTestClient& cl, const std::string& name, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        const int32_t leader = c.catalogLeaderId();
        if (leader > 0 && c.running(leader)) {
            const std::string ch = clientChannelOf(c, leader);
            const int64_t sid = cl.connect(ch, kClientStream);
            if (sid > 0) {
                const auto r = cl.awaitDefine(cl.sendDefineTopic(ch, kClientStream, sid, name, 1, charBlob()),
                                              Clock::now() + 10s);
                if (r && (r->errorCode == kOk)) return true;
            }
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

struct ClusterWithData {
    ThreeNodes c;
    int32_t    victim = -1;
    int32_t    topicId = -1;
    int64_t    catalogEnd = 0;
    static constexpr int kRecords = 4;
};

/// A 3-broker (legacy runtime) cluster with topic "keep" holding kRecords acked records on every broker.
/// "" = built; otherwise the step that failed.
std::string buildClusterWithData(ClusterWithData& d, ShardTestClient& cl) {
    if (!d.c.awaitCatalogLeader(30s)) return "no catalog leader";
    if (!defineOnLeader(d.c, cl, "keep", 30s)) return "define on the catalog leader";
    d.topicId = 0;
    for (int id = 1; id <= 3; ++id)
        if (!awaitTrue([&] { return d.c.node(id).catalogTopicServesForTest(d.topicId); }, 20s))
            return "topic does not serve on broker " + std::to_string(id);
    // produce through the partition's leader (legacy runtime: the gateway endpoint takes produces)
    const auto pa = d.c.node(1).onControlThreadForTest([&]() -> std::optional<int32_t> {
        const auto* a = d.c.node(1).metadataServiceForTest()->assignment(d.topicId, 0);
        return a ? std::optional<int32_t>(a->leaderNode) : std::nullopt;
    });
    if (!pa) return "no assignment on broker 1";
    const std::string ch = clientChannelOf(d.c, *pa);
    const int64_t sid = cl.connect(ch, kClientStream);
    if (sid <= 0) return "connect to the partition leader " + std::to_string(*pa);
    for (int i = 0; i < ClusterWithData::kRecords; ++i)
        if (!cl.produceUntilOk(ch, kClientStream, sid, d.topicId, 0, /*acks=All*/1, oneRecord("r" + std::to_string(i))))
            return "produce " + std::to_string(i) + " to leader " + std::to_string(*pa);
    for (int id = 1; id <= 3; ++id)
        if (!awaitTrue([&] { return leoOnOwner(d.c.node(id), d.topicId, 0) == ClusterWithData::kRecords; }, 20s))
            return "records not on broker " + std::to_string(id);
    const int32_t leader = d.c.catalogLeaderId();
    d.victim = leader == 3 ? 2 : 3;
    d.catalogEnd = catalog_cluster::catalogOf(d.c.node(leader)).leo;
    if (d.catalogEnd <= 0 || !d.c.awaitCatalogEndOffset(d.c.node(d.victim), d.catalogEnd, 10s)) return "victim catalog end";
    return {};
}

void plantOrphan(const fs::path& dataDir) {
    fs::create_directories(rune::node::topicsRoot(dataDir, 0) / "orphan" / "0");
    { std::ofstream(rune::node::topicsRoot(dataDir, 0) / "orphan" / "0" / "00000000000000000000.log") << "orphan"; }
}
} // namespace

namespace {
/// `.catalog-catchup` of a data dir.
rune::node::CatchUpSentinel sentinelOf(const fs::path& dataDir) { return rune::node::readCatchUpSentinel(dataDir.string()); }

/// A DDL (drop) on the catalog leader's wire endpoint, retried while the leadership settles.
bool dropOnLeader(ThreeNodes& c, ShardTestClient& cl, const std::string& name, std::chrono::seconds within) {
    const auto deadline = Clock::now() + within;
    while (Clock::now() < deadline) {
        const int32_t leader = c.catalogLeaderId();
        if (leader > 0 && c.running(leader)) {
            const std::string ch = clientChannelOf(c, leader);
            const int64_t sid = cl.connect(ch, kClientStream);
            if (sid > 0) {
                const auto r = cl.awaitDdl(cl.sendDropTopic(ch, kClientStream, sid, name), Clock::now() + 10s);
                if (r && r->errorCode == 0) return true;
            }
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}
} // namespace

// R2 (fix round 1): a wiped broker catches up as a FOLLOWER, writes the durable catch-up sentinel, and only the
// NEXT boot reconciles (on the startup thread).  Nothing is touched before.  The applier is held across the first
// part so the replica holds the whole prefix while NOTHING is applied: not caught up.
TEST(CatalogBootRegression, WipedCatalogBrokerQuarantinesNothingBeforeCatchUpAndTheNextBootReconcilesRegression) {
    ClusterWithData d{ThreeNodes("boot_b6", Mode::Legacy)};
    ShardTestClient cl(d.c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    for (int id = 1; id <= 3; ++id) d.c.startNode(id);
    const std::string built = buildClusterWithData(d, cl);
    ASSERT_TRUE(built.empty()) << built;
    const fs::path vdir = d.c.dirs[static_cast<std::size_t>(d.victim - 1)]->path;
    d.c.stop(d.victim);
    fs::remove_all(vdir / "__catalog");   // an operator deleted the catalog as a "repair" (or a new disk)
    plantOrphan(vdir);
    ASSERT_TRUE(fs::exists(rune::node::topicsRoot(vdir, 0) / "keep" / "0"));
    d.c.beforeStart = [&](int id, BrokerNode& n) { if (id == d.victim) n.holdCatalogApplyAtBootForTest(true); };
    d.c.startNode(d.victim);
    BrokerNode& v = d.c.node(d.victim);
    ASSERT_TRUE(d.c.awaitCatalogEndOffset(v, d.catalogEnd, 30s)) << "the replica refetched the catalog from 0";
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(v.bootReconcileStateForTest(), "AwaitingCatchUp");
    EXPECT_EQ(sentinelOf(vdir).kind, rune::node::CatchUpSentinel::Kind::Pending);
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(vdir, 0) / "orphan" / "0" / "00000000000000000000.log"))
        << "NO topic dir is touched before the applier caught up";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(vdir, 0)));
    v.holdCatalogApplyForTest(false);
    ASSERT_TRUE(v.awaitBootReconcileSettledForTest(30s));
    EXPECT_EQ(v.bootReconcileStateForTest(), "Done");
    const auto s = sentinelOf(vdir);
    EXPECT_EQ(s.kind, rune::node::CatchUpSentinel::Kind::CaughtUp);
    EXPECT_GE(s.offset, d.catalogEnd - 1) << "the sentinel names the offset it applied up to";
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(vdir, 0) / "orphan")) << "the reconcile does NOT run on the admin thread";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(vdir, 0)));
    ASSERT_TRUE(awaitTrue([&] { return v.catalogTopicServesForTest(d.topicId); }, 20s));
    EXPECT_EQ(leoOnOwner(v, d.topicId, 0), ClusterWithData::kRecords) << "the data serves after catch-up";

    d.c.beforeStart = nullptr;
    d.c.stop(d.victim);
    d.c.startNode(d.victim);
    BrokerNode& v2 = d.c.node(d.victim);
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(vdir, 0) / "orphan")) << "the NEXT boot quarantined the orphan";
    EXPECT_TRUE(quarantineHasEntryStartingWith(vdir, "orphan-"));
    EXPECT_FALSE(quarantineHasEntryStartingWith(vdir, "keep-"));
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(vdir, 0) / "keep" / "0"));
    EXPECT_EQ(sentinelOf(vdir).kind, rune::node::CatchUpSentinel::Kind::Absent);
    ASSERT_TRUE(awaitTrue([&] { return v2.catalogTopicServesForTest(d.topicId); }, 20s));
    EXPECT_EQ(leoOnOwner(v2, d.topicId, 0), ClusterWithData::kRecords);
}

TEST(CatalogBootRegression, WipedCatalogBrokerReconcilesOnItsOwnAtTheBootAfterCatchUpWithoutAnyHoldRegression) {
    ClusterWithData d{ThreeNodes("boot_b6b", Mode::Legacy)};
    ShardTestClient cl(d.c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    for (int id = 1; id <= 3; ++id) d.c.startNode(id);
    const std::string built = buildClusterWithData(d, cl);
    ASSERT_TRUE(built.empty()) << built;
    const fs::path vdir = d.c.dirs[static_cast<std::size_t>(d.victim - 1)]->path;
    d.c.stop(d.victim);
    fs::remove_all(vdir / "__catalog");
    plantOrphan(vdir);
    d.c.startNode(d.victim);
    BrokerNode& v = d.c.node(d.victim);
    ASSERT_TRUE(v.awaitBootReconcileSettledForTest(30s)) << "the follower catches up and writes the sentinel by itself";
    EXPECT_EQ(sentinelOf(vdir).kind, rune::node::CatchUpSentinel::Kind::CaughtUp);
    EXPECT_GE(sentinelOf(vdir).offset, d.catalogEnd - 1);
    d.c.stop(d.victim);
    d.c.startNode(d.victim);
    EXPECT_TRUE(quarantineHasEntryStartingWith(vdir, "orphan-"));
    EXPECT_FALSE(quarantineHasEntryStartingWith(vdir, "keep-"));
    BrokerNode& v2 = d.c.node(d.victim);
    ASSERT_TRUE(awaitTrue([&] { return v2.catalogTopicServesForTest(d.topicId); }, 20s));
    EXPECT_EQ(leoOnOwner(v2, d.topicId, 0), ClusterWithData::kRecords);
}

// R1 (fix round 1, C2): a wiped broker applies the HISTORY live; a historic CreateTopic of a name whose dir
// belongs to a NEWER incarnation must not open it (it would be dropped and its data purged by the later events).
TEST(CatalogBootRegression, WipedCatalogCatchUpNeverOpensANewerIncarnationsDirRegression) {
    ClusterWithData d{ThreeNodes("boot_c2", Mode::Legacy)};
    ShardTestClient cl(d.c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    for (int id = 1; id <= 3; ++id) d.c.startNode(id);
    ASSERT_TRUE(d.c.awaitCatalogLeader(30s));
    ASSERT_TRUE(defineOnLeader(d.c, cl, "t", 30s));      // id 0
    ASSERT_TRUE(dropOnLeader(d.c, cl, "t", 30s));
    ASSERT_TRUE(defineOnLeader(d.c, cl, "t", 30s));      // id 1
    d.topicId = 1;
    for (int id = 1; id <= 3; ++id)
        ASSERT_TRUE(awaitTrue([&] { return d.c.node(id).catalogTopicServesForTest(1); }, 20s)) << id;
    const auto pa = d.c.node(1).onControlThreadForTest([&]() -> std::optional<int32_t> {
        const auto* a = d.c.node(1).metadataServiceForTest()->assignment(1, 0);
        return a ? std::optional<int32_t>(a->leaderNode) : std::nullopt;
    });
    ASSERT_TRUE(pa.has_value());
    const std::string ch = clientChannelOf(d.c, *pa);
    const int64_t sid = cl.connect(ch, kClientStream);
    ASSERT_GT(sid, 0);
    for (int i = 0; i < ClusterWithData::kRecords; ++i)
        ASSERT_TRUE(cl.produceUntilOk(ch, kClientStream, sid, 1, 0, /*acks=*/1, oneRecord("r" + std::to_string(i))));
    for (int id = 1; id <= 3; ++id)
        ASSERT_TRUE(awaitTrue([&] { return leoOnOwner(d.c.node(id), 1, 0) == ClusterWithData::kRecords; }, 20s)) << id;
    const int32_t leader = d.c.catalogLeaderId();
    d.victim = leader == 3 ? 2 : 3;
    const fs::path vdir = d.c.dirs[static_cast<std::size_t>(d.victim - 1)]->path;
    d.c.stop(d.victim);
    fs::remove_all(vdir / "__catalog");
    { std::ofstream(rune::node::topicsRoot(vdir, 0) / "t" / "0" / "probe.txt") << "p"; }
    d.c.startNode(d.victim);
    BrokerNode& v = d.c.node(d.victim);
    ASSERT_TRUE(awaitTrue([&] { return v.catalogTopicServesForTest(1); }, 30s));
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(vdir, 0) / "t" / "0" / "probe.txt"))
        << "the dir was never renamed or purged by a historic create of the older incarnation";
    EXPECT_EQ(leoOnOwner(v, 1, 0), ClusterWithData::kRecords);
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(vdir, 0) / "0.t"));
}

// R1 (fix round 1, C1): a dropped incarnation's data is never served under a re-created name — the crash window
// between the drop's checkpoint and the dir rename.
TEST(CatalogBootRegression, DroppedIncarnationDataIsNeverResurrectedByARecreatedNameRegression) {
    BootRig r("boot_c1");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "t", .typed = true}).code, kOk);   // id 0
    ASSERT_TRUE(r.awaitServes(0));
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(r.produce(0, 0, "old-" + std::to_string(i)));
    r.node().holdDroppedDirPurgeForTest(true);
    ASSERT_EQ(r.dropTopic("t").code, kOk);
    ASSERT_TRUE(awaitTrue([&] { return fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "0.t"); }, 15s));
    ASSERT_EQ(r.define({.name = "t", .typed = true}).code, kOk);   // id 1
    ASSERT_TRUE(r.awaitServes(1));
    r.stop();
    // "crash before the rename": the older incarnation's dir is (still) at topics/t
    fs::remove_all(rune::node::topicsRoot(r.dataDir(), 0) / "t");
    fs::rename(rune::node::droppedRoot(r.dataDir(), 0) / "0.t", rune::node::topicsRoot(r.dataDir(), 0) / "t");
    ASSERT_TRUE(r.start());
    EXPECT_EQ(r.topicIdForName("t"), 1);
    EXPECT_EQ(r.logEndOffset(1, 0), 0) << "the dropped incarnation's 3 records are NOT id 1's";
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "0.t")) << "purged";
    EXPECT_FALSE(quarantineHasEntryStartingWith(r.dataDir(), "t-"));
    EXPECT_EQ(slurp(rune::node::topicsRoot(r.dataDir(), 0) / "t" / "topic-id"), "1\n");
    ASSERT_TRUE(r.produce(1, 0, "new"));
    EXPECT_EQ(r.logEndOffset(1, 0), 1);
}

// R2 (fix round 1, I1): a crash in the middle of a catch-up leaves the sentinel pending: the next boot touches
// nothing, however much of the catalog it already applied.
TEST(CatalogBootRegression, CrashMidCatchUpNeverReconcilesAgainstAPartialCatalogRegression) {
    BootRig r("boot_i1");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    r.stop();
    fs::create_directories(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0");
    { std::ofstream(rune::node::topicsRoot(r.dataDir(), 0) / "ghost" / "0" / "00000000000000000000.log") << "ghost"; }
    fs::create_directories(rune::node::topicsRoot(r.dataDir(), 0) / "late" / "0");
    { std::ofstream(rune::node::topicsRoot(r.dataDir(), 0) / "late" / "topic-id") << "7\n"; }
    { std::ofstream(rune::node::topicsRoot(r.dataDir(), 0) / "late" / "0" / "00000000000000000000.log") << "x"; }
    ASSERT_TRUE(rune::node::writeCatchUpSentinel(r.dataDir().string(), -1));
    const auto ghost = treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "ghost");
    const auto late  = treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "late");
    ASSERT_TRUE(r.start());
    EXPECT_EQ(r.node().bootReconcileStateForTest(), "AwaitingCatchUp");
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "ghost"), ghost);
    EXPECT_EQ(treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "late"), late);
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0)));
    EXPECT_EQ(sentinelOf(r.dataDir()).kind, rune::node::CatchUpSentinel::Kind::Pending);
}

// R2 (fix round 1, I3): a wiped broker that LEADS the catalog (single node: always) is never "caught up" on an
// empty history — its topic dirs stay in place.
TEST(CatalogBootRegression, WipedCatalogLeaderNeverReconcilesAgainstAnEmptyCatalogRegression) {
    BootRig r("boot_i3");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);
    ASSERT_TRUE(r.awaitServes(0));
    ASSERT_TRUE(r.produce(0, 0, "r1"));
    ASSERT_TRUE(r.produce(0, 0, "r2"));
    r.stop();
    fs::remove_all(r.dataDir() / "__catalog");
    ASSERT_TRUE(r.start());
    std::this_thread::sleep_for(2s);
    std::error_code ec;
    std::uintmax_t bytes = 0;
    for (const auto& e : fs::recursive_directory_iterator(rune::node::topicsRoot(r.dataDir(), 0) / "a", ec))
        if (e.path().extension() == ".log") bytes += fs::file_size(e.path());
    EXPECT_GT(bytes, 0u) << "topics/a keeps its log bytes";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0)));
    EXPECT_EQ(r.node().bootReconcileStateForTest(), "AwaitingCatchUp");
    EXPECT_EQ(sentinelOf(r.dataDir()).kind, rune::node::CatchUpSentinel::Kind::Pending);
}

// ═════════════════════════════════════════════════════════════════════════════
// Fix round 2
// ═════════════════════════════════════════════════════════════════════════════
// F1: a deferred boot (the sentinel is Pending) whose prefix topic's dir carries another incarnation's marker
// leaves that topic unbuilt and STARTS — it never refuses to start, at this boot or any later one.
TEST(CatalogBootRegression, DeferredBootWithAnotherIncarnationsDirLeavesTheTopicUnbuiltAndStartsRegression) {
    BootRig r("boot_f1");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "t", .typed = true}).code, kOk);   // id 0
    ASSERT_TRUE(r.awaitServes(0));
    ASSERT_TRUE(r.produce(0, 0, "x"));
    r.stop();
    { std::ofstream(rune::node::topicsRoot(r.dataDir(), 0) / "t" / "topic-id", std::ios::trunc) << "5\n"; }   // another incarnation
    ASSERT_TRUE(rune::node::writeCatchUpSentinel(r.dataDir().string(), -1));   // a crash in the middle of catch-up
    const auto before = treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "t");
    ASSERT_TRUE(r.start()) << "a deferred boot never refuses to start over a refused topic";
    EXPECT_EQ(r.node().bootReconcileStateForTest(), "AwaitingCatchUp");
    std::this_thread::sleep_for(1s);
    EXPECT_FALSE(r.node().catalogTopicServesForTest(0)) << "never opened over the wrong dir";
    EXPECT_EQ(treeOf(rune::node::topicsRoot(r.dataDir(), 0) / "t"), before) << "byte-identical";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0)));
}

// F2: a wiped broker that LEADS the catalog accepts no DDL — an explicit Internal — so it never extends an
// untrusted history (the ids would restart at 0 and re-open an older history's dir).
TEST(CatalogBootRegression, WipedCatalogLeaderRefusesDdlExplicitlyAndNeverOpensAnOlderHistorysDirRegression) {
    BootRig r("boot_f2");
    ASSERT_TRUE(r.start());
    ASSERT_EQ(r.define({.name = "a"}).code, kOk);   // id 0
    ASSERT_TRUE(r.awaitServes(0));
    ASSERT_TRUE(r.produce(0, 0, "r1"));
    ASSERT_TRUE(r.produce(0, 0, "r2"));
    r.stop();
    fs::remove_all(r.dataDir() / "__catalog");
    ASSERT_TRUE(r.start());
    EXPECT_EQ(r.define({.name = "a"}).code, kInternal) << "explicit, non-retriable";
    EXPECT_FALSE(r.node().catalogTopicServesForTest(0));
    const uint64_t refused = r.node().onAdminThreadForTest(
        [&] { return r.node().adminWorkerAgentForTest()->worker().ddlRefusedUntrustedLeader(); });
    EXPECT_GE(refused, 1u);
    EXPECT_EQ(slurp(rune::node::topicsRoot(r.dataDir(), 0) / "a" / "topic-id"), "0\n");
    std::uintmax_t bytes = 0;
    for (const auto& e : fs::recursive_directory_iterator(rune::node::topicsRoot(r.dataDir(), 0) / "a"))
        if (e.path().extension() == ".log") bytes += fs::file_size(e.path());
    EXPECT_GT(bytes, 0u) << "the old history's records are untouched";
    r.stop();
    ASSERT_TRUE(r.start()) << "the next boot still starts";
    EXPECT_EQ(r.node().bootReconcileStateForTest(), "AwaitingCatchUp");
}

// F4(b): a drop of a never-built id retires the dir it marked; a dir marked with ANOTHER id is never retired.
TEST(CatalogBootRegression, DropOfANeverBuiltIdRetiresTheDirItMarkedRegression) {
    BootRig r("boot_f4");
    ASSERT_TRUE(r.start());
    fs::create_directories(rune::node::topicsRoot(r.dataDir(), 0) / "u");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(rune::node::topicsRoot(r.dataDir(), 0) / "u", 3));
    fs::create_directories(rune::node::topicsRoot(r.dataDir(), 0) / "v");
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(rune::node::topicsRoot(r.dataDir(), 0) / "v", 4));
    auto postDrop = [&](int32_t id, const std::string& name) {
        std::vector<uint8_t> p(rune::node::catalogTopicDroppedWireLen(name));
        rune::node::writeCatalogTopicDropped(p.data(), id, name);
        r.node().postCatalogEffectForTest(rune::node::kA2cCatalogTopicDropped, std::move(p));
    };
    postDrop(3, "u");
    postDrop(3, "v");   // the control: v is marked 4, not 3
    EXPECT_TRUE(awaitTrue([&] { return !fs::exists(rune::node::topicsRoot(r.dataDir(), 0) / "u"); }, 5s));
    EXPECT_TRUE(awaitTrue([&] { return !fs::exists(rune::node::droppedRoot(r.dataDir(), 0) / "3.u"); }, 5s)) << "purged";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(r.dataDir(), 0)));
    std::this_thread::sleep_for(1s);
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(r.dataDir(), 0) / "v")) << "a marker naming another id is never retired";
}
