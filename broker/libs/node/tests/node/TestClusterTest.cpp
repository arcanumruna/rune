// TestClusterTest.cpp — catalog history Task 17: TestCluster::defineTopic, the in-process way to create a topic
// now that the static topic list / NodeConfig::topics are gone (topics exist only through DDL).
//
// REAL BrokerNodes over REAL Aeron, DDL over the wire.  Pinned:
//   * dense ids in creation order, and the topic serves on return;
//   * every former static-topic option reaches the broker (cleanup policy, every compaction setting, retention,
//     segment roll, durability tiers, persistence, co-partition group, partition key);
//   * a transient topic;
//   * explicit failures, never a hang: a duplicate name, an rf above the broker count, an rf below minIsr,
//     a dead gateway (the deadline);
//   * a 3-node cluster asked through a FOLLOWER follows the Redirect to the catalog leader;
//   * the FD budget counts the catalog's topics at boot (F6).
#include "support/CatalogBrokerCluster.h"
#include "support/OwnerThreadTestHelpers.h"
#include "support/TestCluster.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/FdBudget.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest-spi.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <sys/resource.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
using rune::node::test::catalog_cluster::Mode;
using rune::node::test::catalog_cluster::Ports;
using rune::node::test::catalog_cluster::ThreeNodes;
using rune::node::test::catalog_cluster::TmpDir;
namespace pc = rune::protocol::client;

namespace {

/// One single-node broker over a data dir that survives restarts.
class SingleNodeForTest {
public:
    explicit SingleNodeForTest(const std::string& tag)
        : dir_(tag), cfg_(rune::node::test::catalog_cluster::makeConfig(1, dir_.path, Ports::discover(1), 1, Mode::Shard)) {
        start();
    }
    ~SingleNodeForTest() { stop(); }
    BrokerNode* node() { return node_.get(); }
    const NodeConfig& config() const { return cfg_; }
    void start() {
        node_ = std::make_unique<BrokerNode>(cfg_);
        node_->start();
        EXPECT_TRUE(node_->awaitLeader(30s));
    }
    void stop() {
        if (node_) { node_->stop(); node_.reset(); }
    }
    void restart() { stop(); start(); }
    /// The topic's spec as the control thread's registry holds it (what the broker built).
    TopicSpec topicSpecOnControl(int32_t id) {
        return node_->onControlThreadForTest([&] {
            const TopicSpec* t = node_->gatewayForTest()->topicRegistry()->topic(id);
            return t != nullptr ? *t : TopicSpec{};
        });
    }

private:
    TmpDir                      dir_;
    NodeConfig                  cfg_;
    std::unique_ptr<BrokerNode> node_;
};

}  // namespace

TEST(TestClusterRegression, DefineTopicReturnsDenseIdsInOrderAndTheTopicServes) {
    SingleNodeForTest n("tc_basic");
    TestCluster c({n.node()});
    EXPECT_EQ(c.defineTopic("a", 1), 0);
    EXPECT_EQ(c.defineTopic("b", 2), 1);
    EXPECT_TRUE(n.node()->catalogTopicServesForTest(0));
    EXPECT_TRUE(n.node()->catalogTopicServesForTest(1));
    EXPECT_EQ(n.topicSpecOnControl(1).partitionCount, 2);
    EXPECT_EQ(n.topicSpecOnControl(1).name, "b");
}

TEST(TestClusterRegression, DefineTopicCarriesEveryFormerStaticTopicOption) {
    SingleNodeForTest n("tc_opts");
    TopicSpec s;
    s.name = "c";
    s.partitionCount = 1;
    s.cleanupPolicy = TopicSpec::CleanupPolicy::Compact;
    s.segmentRollBytes = 1 << 20;
    s.retentionMs = 1000;
    s.durabilityTier = 0;
    s.persistence = PersistenceMode::Transient;
    s.coPartitionGroup = "grp";
    s.partitionKey = "userId";
    s.durability = 1;
    const int32_t id = TestCluster({n.node()}).defineTopic(s);
    ASSERT_EQ(id, 0);
    const auto got = n.topicSpecOnControl(id);
    EXPECT_EQ(got.cleanupPolicy, s.cleanupPolicy);
    EXPECT_EQ(got.segmentRollBytes, s.segmentRollBytes);
    EXPECT_EQ(got.retentionMs, 1000);
    EXPECT_EQ(got.durabilityTier, 0);
    EXPECT_EQ(got.persistence, PersistenceMode::Transient);
    EXPECT_EQ(got.coPartitionGroup, "grp");
    EXPECT_EQ(got.partitionKey, "userId");
    EXPECT_EQ(got.durability, 1);
}

// A compacted topic with EVERY compaction setting (both policies that compact).
class CompactedTopicRegression : public ::testing::TestWithParam<TopicSpec::CleanupPolicy> {};

TEST_P(CompactedTopicRegression, EveryCompactionSettingReachesTheBroker) {
    SingleNodeForTest n(std::string("tc_compact") + std::to_string(static_cast<int>(GetParam())));
    TopicSpec s;
    s.name = "compacted";
    s.partitionCount = 2;
    s.cleanupPolicy = GetParam();
    s.minCleanableDirtyRatio = 0.25;
    s.deleteRetentionMs = 12'345;
    s.minCompactionLagMs = 1'500;
    s.maxCompactionLagMs = 90'000;
    s.retentionMs = 3'600'000;
    s.retentionBytes = 1LL << 30;
    s.retentionCheckMs = 777;
    s.fileDeleteDelayMs = 888;
    s.segmentRollBytes = 4096;
    const int32_t id = TestCluster({n.node()}).defineTopic(s);
    ASSERT_EQ(id, 0);
    const auto got = n.topicSpecOnControl(id);
    EXPECT_EQ(got.cleanupPolicy, s.cleanupPolicy);
    EXPECT_DOUBLE_EQ(got.minCleanableDirtyRatio, 0.25);
    EXPECT_EQ(got.deleteRetentionMs, 12'345);
    EXPECT_EQ(got.minCompactionLagMs, 1'500);
    EXPECT_EQ(got.maxCompactionLagMs, 90'000);
    EXPECT_EQ(got.retentionMs, 3'600'000);
    EXPECT_EQ(got.retentionBytes, 1LL << 30);
    EXPECT_EQ(got.retentionCheckMs, 777);
    EXPECT_EQ(got.fileDeleteDelayMs, 888);
    EXPECT_EQ(got.segmentRollBytes, 4096);
    EXPECT_EQ(got.partitionCount, 2);
}

INSTANTIATE_TEST_SUITE_P(EveryCompactingPolicy, CompactedTopicRegression,
                         ::testing::Values(TopicSpec::CleanupPolicy::Compact, TopicSpec::CleanupPolicy::CompactDelete),
                         [](const ::testing::TestParamInfo<TopicSpec::CleanupPolicy>& i) {
                             return std::string(i.param == TopicSpec::CleanupPolicy::Compact ? "Compact" : "CompactDelete");
                         });

TEST(TestClusterRegression, ATransientTopicIsMemoryBackedAndServes) {
    SingleNodeForTest n("tc_transient");
    TopicSpec s;
    s.name = "presence";
    s.partitionCount = 1;
    s.persistence = PersistenceMode::Transient;
    ASSERT_EQ(TestCluster({n.node()}).defineTopic(s), 0);
    EXPECT_TRUE(n.node()->catalogTopicServesForTest(0));
    const auto [memBacked, diskBacked] = n.node()->onOwnerThreadForTest(0, 0, [](rune::replication::PartitionReplica& r) {
        return std::pair{r.isMemoryBacked(), r.isDiskBacked()};
    });
    EXPECT_TRUE(memBacked);
    EXPECT_FALSE(diskBacked);
}

TEST(TestClusterRegression, ADuplicateNameIsAnExplicitFailureNotAHang) {
    SingleNodeForTest n("tc_dup");
    TestCluster c({n.node()});
    ASSERT_EQ(c.defineTopic("dup", 1), 0);
    const auto t0 = TestCluster::Clock::now();
    TopicSpec s;
    s.name = "dup";
    const auto r = c.tryDefineTopic(s, 20s);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.code, static_cast<int>(pc::ErrorCode::TopicExists)) << r.detail;
    EXPECT_LT(TestCluster::Clock::now() - t0, 15s) << "answered, never waited out";
    // The gtest-facing call turns it into a recorded failure and -1.
    int32_t id = 0;
    EXPECT_NONFATAL_FAILURE(id = c.defineTopic(s, 20s), "TestCluster::defineTopic('dup') failed: code 25");
    EXPECT_EQ(id, -1);
    EXPECT_EQ(c.defineTopic("after_dup", 1), 1) << "a refused define consumes no id";
}

TEST(TestClusterRegression, ADeadGatewayFailsAtTheDeadlineInsteadOfHanging) {
    // A broker that was never started: nothing listens on its client endpoint, so no DDL can be answered.
    TmpDir dir("tc_timeout");
    BrokerNode neverStarted{rune::node::test::catalog_cluster::makeConfig(1, dir.path, Ports::discover(1), 1, Mode::Shard)};
    TestCluster dead({&neverStarted});
    TopicSpec s;
    s.name = "never";
    const auto t0 = TestCluster::Clock::now();
    const auto r = dead.tryDefineTopic(s, 2s);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.code, -1) << "the deadline: " << r.detail;
    EXPECT_NE(r.detail.find("timed out"), std::string::npos) << r.detail;
    EXPECT_LT(TestCluster::Clock::now() - t0, 25s) << "bounded (one connect wait past the deadline at most)";
    int32_t id = 0;
    EXPECT_NONFATAL_FAILURE(id = dead.defineTopic(s, 2s), "(deadline)");
    EXPECT_EQ(id, -1);
}

TEST(TestClusterRegression, ReplicationFactorAboveTheBrokerCountIsAnExplicitError) {
    SingleNodeForTest n("tc_rf_hi");
    TopicSpec s;
    s.name = "toowide";
    s.replicationFactor = 2;   // one broker
    const auto r = TestCluster({n.node()}).tryDefineTopic(s, 20s);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.code, static_cast<int>(pc::ErrorCode::InvalidRequest)) << r.detail;
    EXPECT_EQ(r.detail, "replicationFactor 2 > broker count 1");
}

TEST(TestClusterRegression, ReplicationFactorBelowMinIsrIsAnExplicitError) {
    auto c3 = ThreeNodes::start("tc_rf_lo");
    ASSERT_TRUE(c3.awaitCatalogLeader(30s));
    TopicSpec s;
    s.name = "toonarrow";
    s.replicationFactor = 1;   // minIsr is the 3-node majority, 2
    const auto r = TestCluster(c3.live()).tryDefineTopic(s, 30s);
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.code, static_cast<int>(pc::ErrorCode::InvalidRequest)) << r.detail;
    EXPECT_EQ(r.detail, "replicationFactor 1 < minIsr 2");
}

TEST(TestClusterRegression, ThreeNodeDefineThroughAFollowerFollowsTheRedirect) {
    auto c3 = ThreeNodes::start("tc_redirect");
    ASSERT_TRUE(c3.awaitCatalogLeader(30s));
    const int32_t leader = c3.catalogLeaderId();
    ASSERT_GT(leader, 0);
    // The follower FIRST: the cluster asks it, is redirected to the leader.
    std::vector<BrokerNode*> order;
    for (int id = 1; id <= 3; ++id)
        if (id != leader) order.push_back(&c3.node(id));
    order.push_back(&c3.node(leader));
    TestCluster c(order);
    EXPECT_EQ(c.defineTopic("via_follower", 3), 0);
    EXPECT_GE(c.redirectsFollowed(), 1u) << "the follower must have answered Redirect";
    for (int id = 1; id <= 3; ++id) EXPECT_TRUE(c3.node(id).catalogTopicServesForTest(0)) << "node " << id;
    EXPECT_EQ(c.defineTopic("second", 1), 1);
}

TEST(TestClusterRegression, ThreeNodeTopicWithRfBelowTheClusterWaitsOnlyForItsReplicaHolders) {
    auto c3 = ThreeNodes::start("tc_rf2");
    ASSERT_TRUE(c3.awaitCatalogLeader(30s));
    TopicSpec s;
    s.name = "pair";
    s.partitionCount = 1;
    s.replicationFactor = 2;   // two of three brokers hold it
    TestCluster c(c3.live());
    ASSERT_EQ(c.defineTopic(s), 0);
    // Every broker's committed placement says the same: two replicas.
    for (int id = 1; id <= 3; ++id) {
        BrokerNode& n = c3.node(id);
        const std::size_t replicas = n.onControlThreadForTest([&n] {
            const auto* a = n.metadataServiceForTest()->assignment(0, 0);
            return a == nullptr ? std::size_t{0} : a->replicas.size();
        });
        EXPECT_EQ(replicas, 2u) << "node " << id;
    }
    // R7 (M4): defineTopic already WAITED for the replica holders -- no extra wait here.
    BrokerNode& any = c3.node(1);
    const std::vector<int32_t> holders = any.onControlThreadForTest([&any] {
        const auto* a = any.metadataServiceForTest()->assignment(0, 0);
        return a == nullptr ? std::vector<int32_t>{} : std::vector<int32_t>(a->replicas.begin(), a->replicas.end());
    });
    ASSERT_EQ(holders.size(), 2u);
    for (int32_t h : holders)
        EXPECT_TRUE(c3.node(h).catalogTopicServesForTest(0)) << "holder " << h << " must already serve on return";
}

// R7 (M4): a catalog leader that holds NONE of a topic's partitions (rf 1 on 3 brokers) must still answer the DDL OK
// promptly -- the "held nowhere here" branch of catalogTopicServes_.  RED: replace `heldHere &&` with `true &&` in
// catalogTopicServes_: the define is answered Internal after the 5000 ms hold.
TEST(TestClusterRegression, ALeaderHoldingNoPartitionAnswersOkRegression) {
    // Placement of a one-partition rf-1 topic is the LOWEST live broker (node 1).  Node 1 is made slow to campaign so
    // that another broker leads the catalog and therefore holds none of those topics.
    auto c3 = ThreeNodes::startWith("tc_nohold", [](NodeConfig& cfg) {
        cfg.gatewayMinIsr = 1;
        if (cfg.nodeId == 1) { cfg.electionTimeoutMs = 3000; cfg.startupCanvassTimeoutMs = 3000; }
    });
    ASSERT_TRUE(c3.awaitCatalogLeader(30s));
    const int32_t leader = c3.catalogLeaderId();
    ASSERT_GT(leader, 0);
    TestCluster c(c3.live());
    bool found = false;
    for (int i = 0; i < 6 && !found; ++i) {
        TopicSpec s;
        s.name = "p" + std::to_string(i);
        s.partitionCount = 1;
        s.replicationFactor = 1;
        const auto t0 = TestCluster::Clock::now();
        const auto r = c.tryDefineTopic(s, 30s);
        ASSERT_TRUE(r.ok()) << s.name << ": code " << r.code << ": " << r.detail;
        EXPECT_LT(TestCluster::Clock::now() - t0, 10s) << s.name << " must be answered promptly";
        BrokerNode& ln = c3.node(leader);
        const bool leaderHolds = ln.onControlThreadForTest([&ln, &r] {
            const auto* a = ln.metadataServiceForTest()->assignment(r.topicId, 0);
            if (a == nullptr) return true;
            for (int32_t h : a->replicas)
                if (h == ln.nodeIdForTest()) return true;
            return false;
        });
        found = !leaderHolds;
    }
    ASSERT_TRUE(found) << "no rf-1 topic landed away from the catalog leader in 6 tries";
}

// Task 17 fix-on-sight (the C1 gate): a topic with replicationFactor BELOW the broker count is not held on every
// broker.  The DDL OK used to be held until EVERY partition had a routed role on the answering broker — a
// partition held elsewhere never gets one — so the OK was answered Internal (in doubt) after the 5 s hold limit.
// RED (before the fix): "DDL OK for topic id=0 ... held 5000 ms and the topic still does not serve — answered
// Internal (in doubt)"; defineTopic failed with code 6.
TEST(TestClusterRegression, ReplicationFactorOneMultiPartitionTopicIsAnsweredOkNotInDoubtRegression) {
    auto c3 = ThreeNodes::startWith("tc_rf1", [](NodeConfig& cfg) { cfg.gatewayMinIsr = 1; });
    ASSERT_TRUE(c3.awaitCatalogLeader(30s));
    TopicSpec s;
    s.name = "spread";
    s.partitionCount = 3;
    s.replicationFactor = 1;   // each partition lives on ONE broker; no broker holds all three
    TestCluster c(c3.live());
    const auto t0 = TestCluster::Clock::now();
    const auto r = c.tryDefineTopic(s, 30s);
    ASSERT_TRUE(r.ok()) << "code " << r.code << ": " << r.detail;
    EXPECT_LT(TestCluster::Clock::now() - t0, 20s) << "well inside the DDL hold limit's retry window";
    for (int id = 1; id <= 3; ++id) EXPECT_TRUE(c3.node(id).catalogTopicServesForTest(r.topicId)) << "node " << id;
}

// F6: the FD budget used to come from NodeConfig::topics (deleted); it now comes from the catalog's boot topics.
TEST(FdBudgetRegression, BootBudgetCountsTheCatalogTopicsRegression) {
    SingleNodeForTest n("tc_fd");
    const uint64_t depth = static_cast<uint64_t>(n.config().segmentProvisionDepth);
    EXPECT_EQ(n.node()->fdNeedAtBootForTest(), estimateFdNeed({}, depth)) << "an empty catalog counts no topic";
    TestCluster c({n.node()});
    ASSERT_EQ(c.defineTopic("a", 4), 0);
    ASSERT_EQ(c.defineTopic("b", 2), 1);
    n.restart();
    const std::vector<TopicSpec> specs{n.topicSpecOnControl(0), n.topicSpecOnControl(1)};
    EXPECT_EQ(n.node()->fdNeedAtBootForTest(), estimateFdNeed(specs, depth));
    EXPECT_GT(n.node()->fdNeedAtBootForTest(), estimateFdNeed({}, depth)) << "the catalog's partitions are counted";
}

// R3 (I2): a restart refuses to boot when the CATALOG's topics need more open files than the hard limit allows.
// RED: delete the applyFdLimitAtStartup call in BrokerNode::start() -- the child exits 0.
TEST(FdBudgetRegression, ARestartRefusesWhenTheCatalogTopicsExceedTheHardLimitRegression) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    SingleNodeForTest n("tc_fd_refuse");
    TopicSpec huge;
    huge.name = "huge";
    huge.partitionCount = 1;
    huge.retentionBytes = 1LL << 40;
    huge.segmentRollBytes = 4096;
    ASSERT_EQ(TestCluster({n.node()}).defineTopic(huge), 0);
    const std::vector<TopicSpec> specs{n.topicSpecOnControl(0)};
    ASSERT_GT(estimateFdNeed(specs, static_cast<uint64_t>(n.config().segmentProvisionDepth)), 65536u);
    n.stop();
    const NodeConfig cfg = n.config();
    const auto restartUnderALowHardLimit = [&cfg] {
            rlimit rl{};
            getrlimit(RLIMIT_NOFILE, &rl);
            const rlim_t hard = std::min<rlim_t>(rl.rlim_max, 65536);   // a FINITE hard limit
            rlimit low{std::min(rl.rlim_cur, hard), hard};
            if (setrlimit(RLIMIT_NOFILE, &low) != 0) _exit(4);
            BrokerNode b{cfg};
            try { b.start(); } catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); _exit(3); }
            _exit(0);
        };
    EXPECT_EXIT(restartUnderALowHardLimit(), ::testing::ExitedWithCode(3), "hard open-file limit is below");
}
