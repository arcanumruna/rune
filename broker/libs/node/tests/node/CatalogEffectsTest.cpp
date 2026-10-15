// CatalogEffectsTest.cpp — catalog history Task 8: the control thread executes
// the CatalogApplier's effects on a real single-node BrokerNode.  Each effect is
// posted exactly as the applier posts it — encoded on the admin→control ring FROM
// THE ADMIN THREAD through the production sink path
// (BrokerNode::postCatalogEffectForTest) — and observed on the real data path
// (a wire client over Aeron) or on the owning thread (onControlThreadForTest /
// onOwnerThreadForTest).  Polls are bounded (the deadline is the failure).
//
// Pinned here:
//   - a CreateTopic applied builds the topic at its CATALOG id and places it
//     (spec §5.4: it serves once applied AND placed);
//   - a dropped name reused immediately gets a fresh, EMPTY topic under its NEW
//     id — never the dropped topic's data or directory (review focus #3);
//   - a dropped topic answers UnknownTopic at once and loses its placement
//     (never stranded);
//   - a config change reaches the cleaner, never the replica;
//   - BINDING (Task 7 review): every effect applied TWICE leaves the same final
//     state — never a duplicate, an error or a double placement.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/MetadataService.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using Clock = std::chrono::steady_clock;

namespace {

/// One config key of an AlterTopicConfig (kA2cCatalogTopicConfig).
struct ConfigSet { rune::protocol::ConfigKey key; int64_t value; };

/// The worker / runtime layouts the effects must work under: shard mode (the
/// default — user replicas are WORKER-owned, so a dropped topic's Logs are freed
/// only after the workers acked the ownership change), fused placement, legacy.
enum class Mode { Shard, ShardFused, Legacy };
const char* modeName(Mode m) { return m == Mode::Shard ? "Shard" : m == Mode::ShardFused ? "ShardFused" : "Legacy"; }

NodeConfig makeConfig(const fs::path& dir, Mode mode) {
    NodeConfig cfg;
    cfg.dataDir  = dir.string();
    cfg.aeronDir = (dir / "aeron").string();
    cfg.consensusStreamId = 10;
    cfg.workerCount = workerCountFromEnv();
    PeerConfig self;
    self.host = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" +
                                 std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;
    if (mode == Mode::Legacy) {
        pinLegacyRuntime(cfg);
        cfg.nodeId = self.id = 0;
        cfg.peers = {self};
        return cfg;
    }
    // Shard mode, collision-free per-worker base ports (ctest -j).
    const auto blocks = freeUdpPortBlocks(3, std::max<int32_t>(1, cfg.workerCount));
    if (blocks.size() != 3) throw std::runtime_error("makeConfig: no free UDP port blocks");
    cfg.nodeId = self.id = 1;
    cfg.workerMode       = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = mode == Mode::ShardFused ? NodeConfig::RuntimePlacement::Fused
                                                    : NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort       = blocks[0];
    cfg.workerReplBasePort          = blocks[1];
    cfg.workerEgressControlHost     = "127.0.0.1";
    cfg.workerEgressControlBasePort = blocks[2];
    cfg.workerIngressBaseStreamId   = 111;
    cfg.workerCoreBase = -1;   // never auto-pin busy-spinning workers under ctest -j
    self.replicationPort = rune::aeronutil::testing::allocFreeUdpPort();
    self.clientPort      = static_cast<uint16_t>(std::stoi(cfg.clientEndpoint.channel.substr(
                                cfg.clientEndpoint.channel.rfind(':') + 1)));
    self.replBasePort = blocks[1]; self.ingressBasePort = blocks[0]; self.workerCount = cfg.workerCount;
    self.egressControlBasePort = blocks[2];
    cfg.peers = {self};
    return cfg;
}

/// A single-node broker with the catalog wired, no static topics: every user
/// topic here comes from a catalog effect.
class SingleNode {
public:
    explicit SingleNode(const std::string& tag, Mode mode = Mode::Shard)
        : dir_(fs::temp_directory_path() /
               ("rune_cateff_" + tag + "_" + modeName(mode) + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(seq_++))) {
        fs::create_directories(dir_);
        const NodeConfig cfg = makeConfig(dir_, mode);
        gwChannel_ = cfg.clientEndpoint.channel;
        gwStream_  = cfg.clientEndpoint.streamId;
        node_ = std::make_unique<BrokerNode>(cfg);
        node_->start();
        if (!node_->awaitLeader(30s)) throw std::runtime_error("SingleNode: no leader");
    }
    ~SingleNode() {
        client_.reset();
        if (node_) node_->stop();
        node_.reset();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    BrokerNode* node() { return node_.get(); }
    const fs::path& dataDir() const { return dir_; }

    // ── catalog effects, as the applier's production sink posts them ─────────
    void applyCreateTopic(int32_t topicId, const std::string& name, int32_t partitions) {
        const rune::protocol::TopicConfig c;   // the catalog's defaults (Delete, retention off)
        std::vector<uint8_t> f(catalogTopicAppliedWireLen(name, c));
        writeCatalogTopicApplied(f.data(), topicId, name, partitions, /*rf*/0, /*persistence=*/0, c);
        node_->postCatalogEffectForTest(kA2cCatalogTopicApplied, std::move(f));
    }
    void applyDropTopic(int32_t topicId, const std::string& name) {
        std::vector<uint8_t> f(catalogTopicDroppedWireLen(name));
        writeCatalogTopicDropped(f.data(), topicId, name);
        node_->postCatalogEffectForTest(kA2cCatalogTopicDropped, std::move(f));
    }
    void applyTopicConfig(int32_t topicId, std::vector<ConfigSet> kv) {
        std::vector<uint8_t> f(catalogTopicConfigWireLen(kv.size()));
        writeCatalogTopicConfigHeader(f.data(), topicId, static_cast<uint16_t>(kv.size()));
        for (std::size_t i = 0; i < kv.size(); ++i)
            writeCatalogTopicConfigEntry(f.data(), i, static_cast<uint8_t>(kv[i].key), kv[i].value);
        node_->postCatalogEffectForTest(kA2cCatalogTopicConfig, std::move(f));
    }
    void applyViewUninstall(int32_t viewTopicId) {
        std::vector<uint8_t> f(kViewUninstallWireLen);
        writeViewUninstall(f.data(), viewTopicId);
        node_->postCatalogEffectForTest(kA2cViewUninstall, std::move(f));
    }

    // ── observations (owner threads; bounded polls) ──────────────────────────
    template <class Pred> static bool awaitTrue(Pred p, std::chrono::milliseconds timeout) {
        const auto deadline = Clock::now() + timeout;
        while (Clock::now() < deadline) {
            if (p()) return true;
            std::this_thread::sleep_for(10ms);
        }
        return p();
    }
    bool awaitServes(int32_t topicId, std::chrono::milliseconds timeout) {
        return awaitTrue([&] { return node_->catalogTopicServesForTest(topicId); }, timeout);
    }
    std::optional<int32_t> topicIdForName(const std::string& name) {
        return node_->onControlThreadForTest(
            [&] { return node_->gatewayForTest()->topicRegistry()->topicIdForName(name); });
    }
    rune::gateway::TopicRegistry::TopicState topicState(int32_t topicId) {
        return node_->onControlThreadForTest(
            [&] { return node_->gatewayForTest()->topicRegistry()->state(topicId); });
    }
    bool awaitTopicState(int32_t topicId, rune::gateway::TopicRegistry::TopicState st,
                         std::chrono::milliseconds timeout) {
        return awaitTrue([&] { return topicState(topicId) == st; }, timeout);
    }
    int32_t registryTopicCount() {
        return node_->onControlThreadForTest([&] { return node_->gatewayForTest()->topicRegistry()->topicCount(); });
    }
    bool hasPlacement(int32_t topicId, int32_t partition) {
        return node_->onControlThreadForTest([&] {
            return node_->metadataServiceForTest()->assignment(topicId, partition) != nullptr;
        });
    }
    std::optional<rune::metadata::PartitionAssignment> placement(int32_t topicId, int32_t partition) {
        return node_->onControlThreadForTest([&]() -> std::optional<rune::metadata::PartitionAssignment> {
            const auto* pa = node_->metadataServiceForTest()->assignment(topicId, partition);
            return pa ? std::optional(*pa) : std::nullopt;
        });
    }
    std::size_t placementsOf(int32_t topicId) {
        return node_->onControlThreadForTest([&] {
            std::size_t n = 0;
            node_->metadataServiceForTest()->committedMap().forEach(
                [&](int32_t t, int32_t, const rune::metadata::PartitionAssignment&) { n += t == topicId ? 1 : 0; });
            return n;
        });
    }
    std::size_t removalsPending() {
        return node_->onControlThreadForTest([&] { return node_->metadataServiceForTest()->removalsInFlightForTest(); });
    }
    int64_t logEndOffset(int32_t topicId, int32_t partition) { return leoOnOwner(*node_, topicId, partition); }
    bool awaitPathGone(const fs::path& p, std::chrono::milliseconds timeout) {
        return awaitTrue([&] { return !fs::exists(p); }, timeout);
    }
    /// Fix round 1 (M6): read on the CLEANER thread (owner-thread hook), never cross-thread.
    bool awaitCleanerOverride(int32_t topicId, int64_t retentionMs, std::chrono::milliseconds timeout,
                              uint64_t minUpdates = 1) {
        return awaitTrue([&] {
            return node_->onCleanerThreadForTest([&] {
                const LogCleanerAgent* c = node_->cleanerForTest();
                const auto* ov = c->policyOverrideForTest(topicId);
                return c->policyUpdatesForTest() >= minUpdates && ov != nullptr &&
                       ov->retentionMs == std::optional<int64_t>{retentionMs};
            });
        }, timeout);
    }
    std::size_t heldDroppedLogs(int32_t topicId) {
        return node_->onControlThreadForTest(
            [&] { return node_->gatewayForTest()->topicRegistry()->heldLogCountForTest(topicId); });
    }
    /// A raw admin→control catalog frame (the view install the applier's viewInstalled posts).
    void postRaw(int32_t typeId, std::vector<uint8_t> f) { node_->postCatalogEffectForTest(typeId, std::move(f)); }
    std::optional<uint16_t> viewRowStatus(int32_t viewTopicId, uint64_t key) {
        ShardTestClient& c = client();
        const int64_t corr = c.sendGetViewRow(gwChannel_, gwStream_, sid_, viewTopicId, key);
        if (corr == 0) return std::nullopt;
        return c.awaitViewRow(corr, Clock::now() + 5s);
    }
    bool subscribeFrom0(int32_t topicId, int32_t partition) {
        return client().subscribe(gwChannel_, gwStream_, sid_, topicId, partition, 0);
    }
    bool awaitErrorCode(int16_t code, std::chrono::milliseconds timeout) {
        ShardTestClient& c = client();
        const auto deadline = Clock::now() + timeout;
        return c.pollUntil([&] {
            for (const auto& e : c.errors()) if (e.code == code) return true;
            return Clock::now() >= deadline;
        }) && [&] { for (const auto& e : c.errors()) if (e.code == code) return true; return false; }();
    }

    // ── the real data path: a wire client over Aeron ─────────────────────────
    ShardTestClient& client() {
        if (!client_) {
            client_ = std::make_unique<ShardTestClient>(dir_ / "client-aeron");
            client_->keepSessionsAlive(true);
            sid_ = client_->connect(gwChannel_, gwStream_);
            if (sid_ <= 0) throw std::runtime_error("SingleNode: client connect failed");
        }
        return *client_;
    }
    static std::vector<uint8_t> batchOf(const std::string& value) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, value, 0);
        return b.build();
    }
    bool produceSync(int32_t topicId, int32_t partition, const std::string& value) {
        return client().produceUntilOk(gwChannel_, gwStream_, sid_, topicId, partition, /*acks=*/1,
                                       batchOf(value)).has_value();
    }
    int16_t produceErrorCode(int32_t topicId, int32_t partition) {
        ShardTestClient& c = client();
        const int64_t corr = ++corr_;
        if (!c.produce(gwChannel_, gwStream_, sid_, corr, topicId, partition, /*acks=*/1, batchOf("probe")))
            return -1;
        const auto r = c.awaitProduceResponse(corr);
        return r ? r->errorCode : int16_t{-1};
    }
    /// Subscribe from offset 0 and collect what the broker pushes within `window`.
    std::vector<ShardTestClient::PushedRecords> consumeFromEarliest(int32_t topicId, int32_t partition,
                                                                   std::chrono::milliseconds window) {
        ShardTestClient& c = client();
        const std::size_t before = c.records().size();
        if (!c.subscribe(gwChannel_, gwStream_, sid_, topicId, partition, 0)) return {};
        const auto deadline = Clock::now() + window;
        (void)c.pollUntil([&] { return Clock::now() >= deadline; });
        std::vector<ShardTestClient::PushedRecords> out;
        for (std::size_t i = before; i < c.records().size(); ++i)
            if (!c.records()[i].bytes.empty()) out.push_back(c.records()[i]);
        return out;
    }

private:
    static inline std::atomic<int> seq_{0};
    fs::path                         dir_;
    std::string                      gwChannel_;
    int32_t                          gwStream_ = 0;
    std::unique_ptr<BrokerNode>      node_;
    std::unique_ptr<ShardTestClient> client_;
    int64_t                          sid_  = 0;
    int64_t                          corr_ = 1'000'000;
};

constexpr int16_t kUnknownTopic = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);
constexpr int16_t kNotLeader    = static_cast<int16_t>(pc::ErrorCode::NotLeader);

class CatalogEffectsRegression : public ::testing::TestWithParam<Mode> {};

} // namespace

TEST_P(CatalogEffectsRegression, AppliedTopicIsCreatedAtItsCatalogIdAndPlaced) {
    SingleNode n("eff_create", GetParam());
    n.applyCreateTopic(/*topicId=*/0, "events", /*partitions=*/2);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    EXPECT_EQ(n.topicIdForName("events"), 0);
    EXPECT_TRUE(n.hasPlacement(0, 0) && n.hasPlacement(0, 1));
    EXPECT_TRUE(n.produceSync(0, 1, "hello")) << "the applied + placed topic serves on the data path";

    // A catalog id with a gap below it: the never-seen ids are Dropped slots.
    n.applyCreateTopic(5, "later", 1);
    ASSERT_TRUE(n.awaitServes(5, 10s));
    EXPECT_EQ(n.topicIdForName("later"), 5);
    EXPECT_EQ(n.topicState(3), rune::gateway::TopicRegistry::TopicState::Unknown) << "a never-seen id: a Gap";
    EXPECT_EQ(n.produceErrorCode(3, 0), kUnknownTopic);
}

// Review focus #3 (F23: the assertion checks the NEW object id).  RED: the reused
// name served the dropped topic's data or directory (a create that ran before the
// dir rename reopened topics/t/0 and recovered the old record).
TEST_P(CatalogEffectsRegression, CreateBehindDropWaitsForTheDirRenameAndStartsEmptyRegression) {
    SingleNode n("eff_reuse", GetParam());
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    ASSERT_TRUE(n.produceSync(0, 0, "old-record"));
    n.node()->holdDroppedDirPurgeForTest(true);        // keep the renamed dir observable (F23)
    n.applyDropTopic(0, "t");
    n.applyCreateTopic(1, "t", 1);                     // same name, immediately
    ASSERT_TRUE(n.awaitServes(1, 10s));
    EXPECT_EQ(n.topicIdForName("t"), 1) << "the reused name names the NEW object id";
    EXPECT_EQ(n.topicState(0), rune::gateway::TopicRegistry::TopicState::Dropped);
    EXPECT_TRUE(fs::exists(n.dataDir() / "dropped" / "0.t"))
        << "the old data was renamed away before the reused name was created";
    EXPECT_EQ(n.logEndOffset(1, 0), 0) << "the reused name starts EMPTY";
    EXPECT_TRUE(n.consumeFromEarliest(1, 0, 500ms).empty()) << "the old record is unreadable under the reused name";
    // Non-vacuous: the subscription is live — a new record arrives at offset 0.
    ASSERT_TRUE(n.produceSync(1, 0, "new-record"));
    EXPECT_EQ(n.logEndOffset(1, 0), 1);
    n.node()->holdDroppedDirPurgeForTest(false);
    EXPECT_TRUE(n.awaitPathGone(n.dataDir() / "dropped" / "0.t", 10s)) << "purged by the admin thread";
    EXPECT_TRUE(fs::exists(n.dataDir() / "topics" / "t" / "0")) << "the new topic's dir is untouched";
}

// Fix-on-sight (Task 11, found by the over-the-wire AlterView): an AlterView posts
// its fresh sink (same name, new id) BEFORE the old sink's drop (install before
// uninstall).  RED: createTopicAt refused the name held by the still-live old
// sink — the new sink never served and the gated AlterView OK became Internal.
TEST_P(CatalogEffectsRegression, AlterViewFreshSinkPostedBeforeTheOldSinkDropWaitsBehindItRegression) {
    SingleNode n("eff_altview", GetParam());
    n.applyCreateTopic(0, "v", 1);                     // the view's current sink
    ASSERT_TRUE(n.awaitServes(0, 10s));
    n.applyCreateTopic(1, "v", 1);                     // AlterView: the fresh sink first …
    n.applyDropTopic(0, "v");                          // … then the old sink's drop
    ASSERT_TRUE(n.awaitServes(1, 10s)) << "the fresh sink serves once the old one retired";
    EXPECT_EQ(n.topicIdForName("v"), 1);
    EXPECT_EQ(n.topicState(0), rune::gateway::TopicRegistry::TopicState::Dropped);
    EXPECT_EQ(n.logEndOffset(1, 0), 0) << "the fresh sink starts empty";
}

TEST_P(CatalogEffectsRegression, DroppedTopicAnswersUnknownTopicAtOnce) {
    SingleNode n("eff_drop", GetParam());
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    n.applyDropTopic(0, "t");
    ASSERT_TRUE(n.awaitTopicState(0, rune::gateway::TopicRegistry::TopicState::Dropped, 10s));
    EXPECT_EQ(n.produceErrorCode(0, 0), kUnknownTopic);
    EXPECT_FALSE(n.topicIdForName("t").has_value()) << "the name is free";
}

// Placement add/remove never strands a partition: the controller removes a
// dropped topic's placement (RemoveTopicPlacementRecord) and forgets it once
// committed.  RED: removeRuntimeTopic a no-op — the dropped topic's partitions
// stay assigned in __cluster_metadata forever.
TEST_P(CatalogEffectsRegression, DropRemovesEveryPartitionsPlacementRegression) {
    SingleNode n("eff_unplace", GetParam());
    n.applyCreateTopic(0, "t", 3);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    ASSERT_EQ(n.placementsOf(0), 3u);
    n.applyDropTopic(0, "t");
    EXPECT_TRUE(SingleNode::awaitTrue([&] { return n.placementsOf(0) == 0; }, 10s))
        << "every partition's placement is removed at commit";
    EXPECT_TRUE(SingleNode::awaitTrue([&] { return n.removalsPending() == 0; }, 10s))
        << "the removal committed: nothing left in flight (nothing stranded)";
    EXPECT_TRUE(n.awaitPathGone(n.dataDir() / "topics" / "t", 10s)) << "its data was moved away (dropped/)";
}

TEST_P(CatalogEffectsRegression, ConfigChangeReachesTheCleanerNotTheReplica) {
    SingleNode n("eff_cfg", GetParam());
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    n.applyTopicConfig(0, {{rune::protocol::ConfigKey::RetentionMs, 1}});
    EXPECT_TRUE(n.awaitCleanerOverride(0, /*retentionMs=*/1, 10s));
    const int64_t replicaRetention = n.node()->onOwnerThreadForTest(
        0, 0, [](rune::replication::PartitionReplica& r) { return r.retentionMs(); });
    EXPECT_EQ(replicaRetention, -1) << "the replica's own config is never touched";
}

// BINDING (Task 7 review): a crash between the applier's checkpoint and its
// effects re-runs the same effects — every handler is idempotent.  Each effect
// kind is applied TWICE; the final state equals applying it once: no duplicate
// entry, no error, no double placement, no second rename.
TEST_P(CatalogEffectsRegression, EveryEffectKindAppliedTwiceLeavesTheSameFinalStateRegression) {
    SingleNode n("eff_twice", GetParam());
    // Create ×2.
    n.applyCreateTopic(0, "a", 2);
    n.applyCreateTopic(0, "a", 2);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    EXPECT_EQ(n.registryTopicCount(), 1) << "never a duplicate entry";
    EXPECT_EQ(n.placementsOf(0), 2u) << "never a double placement";
    const auto pa0 = n.placement(0, 0);
    ASSERT_TRUE(pa0.has_value());
    ASSERT_TRUE(n.produceSync(0, 0, "x"));
    n.applyCreateTopic(0, "a", 2);   // once more while serving with data
    ASSERT_TRUE(n.awaitServes(0, 10s));
    EXPECT_EQ(n.logEndOffset(0, 0), 1) << "a repeated create never rebuilds / wipes the partition";
    const auto pa1 = n.placement(0, 0);
    ASSERT_TRUE(pa1.has_value());
    EXPECT_EQ(pa1->leaderEpoch, pa0->leaderEpoch) << "no re-placement";

    // Config ×2.
    n.applyTopicConfig(0, {{rune::protocol::ConfigKey::RetentionMs, 7}});
    n.applyTopicConfig(0, {{rune::protocol::ConfigKey::RetentionMs, 7}});
    EXPECT_TRUE(n.awaitCleanerOverride(0, 7, 10s, /*minUpdates=*/2));

    // View uninstall ×2 (a view the workers never installed): no error, nothing disturbed.
    n.applyViewUninstall(0x40000001);
    n.applyViewUninstall(0x40000001);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    EXPECT_TRUE(n.produceSync(0, 1, "y"));

    // Drop ×2.
    n.node()->holdDroppedDirPurgeForTest(true);
    n.applyDropTopic(0, "a");
    n.applyDropTopic(0, "a");
    ASSERT_TRUE(n.awaitTopicState(0, rune::gateway::TopicRegistry::TopicState::Dropped, 10s));
    EXPECT_TRUE(SingleNode::awaitTrue([&] { return fs::exists(n.dataDir() / "dropped" / "0.a"); }, 10s));
    EXPECT_TRUE(SingleNode::awaitTrue([&] { return n.placementsOf(0) == 0 && n.removalsPending() == 0; }, 10s));
    EXPECT_EQ(n.produceErrorCode(0, 0), kUnknownTopic);
    // A create re-posted after the drop (a stale effect) never resurrects the id.
    n.applyCreateTopic(0, "a", 2);
    n.applyCreateTopic(1, "b", 1);   // a fence: effects are FIFO — once 1 serves, 0's re-create was handled
    ASSERT_TRUE(n.awaitServes(1, 10s));
    EXPECT_EQ(n.topicState(0), rune::gateway::TopicRegistry::TopicState::Dropped);
    EXPECT_EQ(n.placementsOf(0), 0u) << "a dropped id is never placed again";
    n.node()->holdDroppedDirPurgeForTest(false);
    EXPECT_TRUE(n.awaitPathGone(n.dataDir() / "dropped" / "0.a", 10s));
    EXPECT_FALSE(fs::exists(n.dataDir() / "topics" / "a")) << "renamed once, never re-created";
}

// Fix round 1 (C1) + review I2 (the retire gate).  The cleaner HOLDS its view
// ack: the dropped topic's replica is retired but not yet freed, so its Logs stay
// owned, topics/t is not renamed and the create of t behind it waits.  Meanwhile
// a HIGHER id (u) is created and serves — it must not tombstone t's queued id.
// RED (C1): t's id 1 became a Dropped tombstone when u (id 2) was created —
// createTopicAt(1) refused forever, id 1 answered the non-retriable UnknownTopic.
// RED (I2): the gate released at once — the dir renamed / the Logs freed while
// a holder had not acked.
TEST_P(CatalogEffectsRegression, CreateBehindDropSurvivesAHigherIdAndWaitsForTheRetireGateRegression) {
    SingleNode n("eff_gate", GetParam());
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    ASSERT_TRUE(n.produceSync(0, 0, "old-record"));
    n.node()->holdCleanerViewAckForTest(true);
    n.applyDropTopic(0, "t");
    n.applyCreateTopic(1, "t", 1);   // waits behind the drop
    n.applyCreateTopic(2, "u", 1);   // a higher id, created at once
    ASSERT_TRUE(n.awaitServes(2, 10s));
    // The gate holds while the cleaner has not acked the generation dropping t.
    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(20ms);
        ASSERT_TRUE(fs::exists(n.dataDir() / "topics" / "t" / "0")) << "renamed before every holder acked";
        ASSERT_FALSE(fs::exists(n.dataDir() / "dropped" / "0.t"));
        ASSERT_EQ(n.heldDroppedLogs(0), 1u) << "the dropped topic's Log was freed before every holder acked";
        ASSERT_FALSE(n.node()->catalogTopicServesForTest(1)) << "the queued create ran before the rename";
    }
    EXPECT_EQ(n.topicState(1), rune::gateway::TopicRegistry::TopicState::Pending);
    EXPECT_EQ(n.produceErrorCode(1, 0), kNotLeader) << "a queued id answers the retriable NotLeader";
    n.node()->holdCleanerViewAckForTest(false);
    ASSERT_TRUE(n.awaitServes(1, 10s)) << "t serves under its NEW id once the gate opened";
    EXPECT_EQ(n.topicIdForName("t"), 1);
    EXPECT_EQ(n.heldDroppedLogs(0), 0u) << "released after the ack";
    EXPECT_EQ(n.logEndOffset(1, 0), 0) << "the reused name starts EMPTY";
    EXPECT_TRUE(n.awaitPathGone(n.dataDir() / "dropped" / "0.t", 10s));
}

// Fix round 1 (M4): on a case-insensitive filesystem (the macOS dev boxes) a
// case-variant name shares dataDir/topics/<name>/ — its create waits behind the
// drop exactly like the same name.  RED (macOS): CREATE t ran at once and
// RECOVERED topics/T's old record under the new id.
TEST_P(CatalogEffectsRegression, CaseVariantCreateWaitsBehindTheDropRegression) {
    SingleNode n("eff_case", GetParam());
    n.applyCreateTopic(0, "T", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    ASSERT_TRUE(n.produceSync(0, 0, "old-record"));
    n.node()->holdCleanerViewAckForTest(true);
    n.applyDropTopic(0, "T");
    n.applyCreateTopic(1, "t", 1);
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(n.topicState(1), rune::gateway::TopicRegistry::TopicState::Pending) << "waits behind the drop";
    n.node()->holdCleanerViewAckForTest(false);
    ASSERT_TRUE(n.awaitServes(1, 10s));
    EXPECT_EQ(n.logEndOffset(1, 0), 0) << "the case-variant name starts EMPTY";
}

// Review M7: a live subscription of a dropped topic ends with an explicit
// UnknownTopic over the wire — in every worker mode (Shard: the owning worker
// pushes it).  RED: endTopicSubscriptions a no-op — the subscription stays
// silently registered and pushes nothing, forever.
TEST_P(CatalogEffectsRegression, LiveSubscriptionOfADroppedTopicEndsWithUnknownTopicRegression) {
    SingleNode n("eff_subend", GetParam());
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    ASSERT_TRUE(n.produceSync(0, 0, "r"));
    ASSERT_EQ(n.consumeFromEarliest(0, 0, 500ms).size(), 1u) << "non-vacuous: the subscription is live";
    n.applyDropTopic(0, "t");
    EXPECT_TRUE(n.awaitErrorCode(kUnknownTopic, 10s)) << "the live subscription ended explicitly";
}

// Review M7: a view installed through the catalog effect path, then uninstalled
// (DropView's viewUninstalled → the control thread → every worker's install
// ring): reads of it end in UnknownView over the wire.  RED: onViewUninstall_ /
// the install-ring fan-out a no-op — the dropped view keeps answering.
TEST_P(CatalogEffectsRegression, InstalledViewUninstalledThroughTheCatalogAnswersUnknownViewRegression) {
    SingleNode n("eff_view", GetParam());
    const auto src = rune::codec::DescriptorBuilder("character", 1)
                         .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
                         .add("hp", rune::codec::PrimType::Int32)
                         .build();
    n.applyCreateTopic(0, "character", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    rune::view::ViewPlan v;
    v.viewName = "char_hp";
    v.key      = "characterId";
    v.sources  = {{0, "characterId"}};
    v.output   = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                  {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;   // transient: no checkpoint files
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, /*viewTopicId=*/1);
    ASSERT_TRUE(comp.has_value());
    {   // the sink (sinkApplied: a keyed compacted table) and the plan (viewInstalled)
        rune::protocol::TopicConfig c;
        c.cleanupPolicy = 1;
        c.partitionKey  = "characterId";
        std::vector<uint8_t> f(catalogTopicAppliedWireLen("char_hp", c));
        writeCatalogTopicApplied(f.data(), 1, "char_hp", 1, 0, /*persistence=*/1, c);
        n.postRaw(kA2cCatalogSinkApplied, std::move(f));   // Task 12: a sink's own effect type
    }
    ASSERT_TRUE(n.awaitServes(1, 10s));
    const auto plan = rune::view::serialize(comp->plan);
    const auto out  = rune::codec::serialize(comp->outputDescriptor);
    const auto sd   = rune::codec::serialize(src);
    std::vector<uint8_t> f(viewPlanUpdateWireLen(plan, out, sd));
    writeViewPlanUpdate(f.data(), 1, /*persistence=*/1, /*recovery=*/0, plan, out, sd);
    n.postRaw(kA2cViewPlanUpdate, std::move(f));
    std::optional<uint16_t> st;
    ASSERT_TRUE(SingleNode::awaitTrue([&] {
        st = n.viewRowStatus(1, 7);
        return st && *st != pc::ViewRowResponse::kUnknownView;
    }, 10s)) << "non-vacuous: the installed view answers (status " << (st ? int(*st) : -1) << ")";
    n.applyViewUninstall(1);
    EXPECT_TRUE(SingleNode::awaitTrue([&] {
        st = n.viewRowStatus(1, 7);
        return st && *st == pc::ViewRowResponse::kUnknownView;
    }, 10s)) << "the uninstalled view still answers (status " << (st ? int(*st) : -1) << ")";
}

// BINDING (catalog history Task 12 / Task 8 review N5): a view sink APPLIED and
// PLACED here whose plan has not reached the workers yet answers the retriable
// NotReady over the wire — never the permanent UnknownView; once its plan is
// fanned out the view serves.  RED: UnknownView while the sink served (the gate
// passed a Serving + placed sink; the worker had no plan).
TEST_P(CatalogEffectsRegression, AppliedPlacedSinkAwaitingItsPlanAnswersNotReadyNeverUnknownViewRegression) {
    SingleNode n("eff_n5", GetParam());
    const auto src = rune::codec::DescriptorBuilder("character", 1)
                         .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
                         .add("hp", rune::codec::PrimType::Int32)
                         .build();
    n.applyCreateTopic(0, "character", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    rune::protocol::TopicConfig c;
    c.cleanupPolicy = 1;
    c.partitionKey  = "characterId";
    std::vector<uint8_t> sink(catalogTopicAppliedWireLen("char_hp", c));
    writeCatalogTopicApplied(sink.data(), 1, "char_hp", 1, 0, /*persistence=*/1, c);
    n.postRaw(kA2cCatalogSinkApplied, std::move(sink));
    ASSERT_TRUE(n.awaitServes(1, 10s)) << "applied + placed: the sink topic serves";
    for (int i = 0; i < 5; ++i) {   // the plan is still in flight: retriable, every time
        const std::optional<uint16_t> st = n.viewRowStatus(1, 7);
        ASSERT_TRUE(st.has_value());
        EXPECT_EQ(*st, pc::ViewRowResponse::kNotReady) << "read " << i;
        std::this_thread::sleep_for(20ms);
    }
    rune::view::ViewPlan v;
    v.viewName = "char_hp";
    v.key      = "characterId";
    v.sources  = {{0, "characterId"}};
    v.output   = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                  {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, /*viewTopicId=*/1);
    ASSERT_TRUE(comp.has_value());
    const auto plan = rune::view::serialize(comp->plan);
    const auto out  = rune::codec::serialize(comp->outputDescriptor);
    const auto sd   = rune::codec::serialize(src);
    std::vector<uint8_t> f(viewPlanUpdateWireLen(plan, out, sd));
    writeViewPlanUpdate(f.data(), 1, /*persistence=*/1, /*recovery=*/0, plan, out, sd);
    n.postRaw(kA2cViewPlanUpdate, std::move(f));
    std::optional<uint16_t> st;
    EXPECT_TRUE(SingleNode::awaitTrue([&] {
        st = n.viewRowStatus(1, 7);
        return st && *st == pc::ViewRowResponse::kAbsentKey;
    }, 10s)) << "the view serves once its plan is on every worker (status " << (st ? int(*st) : -1) << ")";
}

// Fix round 1 (M2): a dropped topic's dir rename that FAILS is retried (with a
// backoff — never every duty pass) and the data is never deleted in place; once
// the cause is gone the rename completes.  RED: a failed rename given up (the
// dir stays forever) or the data deleted in place.
TEST_P(CatalogEffectsRegression, FailedDirRenameIsRetriedAndNeverDeletesInPlaceRegression) {
    SingleNode n("eff_rename", GetParam());
    n.applyCreateTopic(0, "t", 1);
    ASSERT_TRUE(n.awaitServes(0, 10s));
    ASSERT_TRUE(n.produceSync(0, 0, "r"));
    { std::ofstream block(n.dataDir() / "dropped"); block << "x"; }   // dropped/ is a FILE: the rename fails
    n.applyDropTopic(0, "t");
    ASSERT_TRUE(n.awaitTopicState(0, rune::gateway::TopicRegistry::TopicState::Dropped, 10s));
    std::this_thread::sleep_for(300ms);
    EXPECT_TRUE(fs::exists(n.dataDir() / "topics" / "t" / "0")) << "never deleted in place";
    fs::remove(n.dataDir() / "dropped");
    EXPECT_TRUE(n.awaitPathGone(n.dataDir() / "topics" / "t", 15s)) << "the retried rename completed";
}

INSTANTIATE_TEST_SUITE_P(Modes, CatalogEffectsRegression,
                         ::testing::Values(Mode::Shard, Mode::ShardFused, Mode::Legacy),
                         [](const ::testing::TestParamInfo<Mode>& i) { return std::string(modeName(i.param)); });
