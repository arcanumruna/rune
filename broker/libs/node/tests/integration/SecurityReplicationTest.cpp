// SecurityReplicationTest.cpp — authentication Task 9: the `__security` partition (topic -7, partition 0) on every
// broker, replicated for real (acks=all, minIsr = majority) on the admin-thread replication plane it shares with
// `__catalog`, its leader always elected from its ISR by the controller (never unclean), every applied event
// mirrored to each broker's control-thread SecurityView in offset order.  In-process BrokerNodes over REAL Aeron UDP
// (InProcessUnauthenticated - authentication is not the subject here, the log is).
#include "support/SecurityBrokerCluster.h"
#include "support/SecurityEventChain.h"

#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/SecurityPartition.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::security_cluster;
namespace fs = std::filesystem;
namespace rr = rune::replication;
using namespace std::chrono_literals;

TEST(SecurityReplicationRegression, FormationCreatesSecurityOnEveryBrokerWithACleanLeader) {
    ThreeNodes c = ThreeNodes::start("sec_form");
    ASSERT_TRUE(awaitSecurityLeader(c, 30s));
    const int32_t leader = settledSecurityLeader(c);
    ASSERT_GT(leader, 0);
    for (BrokerNode* n : c.live()) {
        const auto pa = securityAssignmentOf(*n);
        ASSERT_TRUE(pa.has_value()) << "node " << n->nodeIdForTest();
        EXPECT_EQ(pa->leaderNode, leader);
        EXPECT_EQ(pa->replicas.size(), 3u) << "replicated to every broker";
        EXPECT_TRUE(contains(pa->isr, leader));
        EXPECT_TRUE(pa->cleanElection) << "never unclean";
        const auto v = securityOf(*n);
        EXPECT_TRUE(v.present) << "every broker builds __security";
        EXPECT_EQ(v.role == rr::Role::Leader, n->nodeIdForTest() == leader)
            << "exactly the committed leader leads (node " << n->nodeIdForTest() << ")";
        EXPECT_TRUE(fs::is_directory(c.dirs[static_cast<std::size_t>(n->nodeIdForTest() - 1)]->path / "__security"));
    }
    // __catalog and __security are separate partitions with their own leadership (each its own assignment).
    EXPECT_TRUE(c.awaitCatalogLeader(30s));
}

TEST(SecurityReplicationRegression, AnAppendedEventReachesEveryBrokersViewInOffsetOrder) {
    ThreeNodes c = ThreeNodes::start("sec_view");
    ASSERT_TRUE(awaitSecurityLeader(c, 30s));
    GoldenAppender g;
    for (int i = 0; i < 6; ++i) ASSERT_GE(g.appendNext(c), 1) << "event " << i;
    ASSERT_TRUE(awaitViewsAt(c, 5, 20s));
    for (BrokerNode* n : c.live()) {
        const auto v = n->securityViewForTest();
        EXPECT_TRUE(v.present);
        EXPECT_EQ(v.appliedOffset, 5) << "node " << n->nodeIdForTest();
        EXPECT_EQ(v.digest, g.digestOfFirst(6)) << "every broker's view agrees with the library replay";
        EXPECT_FALSE(v.halted);
    }
    // The control-thread counters follow: applied offset 5, not halted.
    for (BrokerNode* n : c.live()) {
        const auto m = n->securityMetricsForTest();
        EXPECT_EQ(m.applied, 5);
        EXPECT_EQ(m.halted, 0);
    }
}

TEST(SecurityReplicationRegression, NeverCompactedOrRetentionDeleted) {
    const auto cfg = rune::node::makeSecurityPartitionConfig(rune::node::NodeConfig{}, 3);
    EXPECT_EQ(cfg.topicId, kSecurityTopicId);
    EXPECT_EQ(cfg.cleanupPolicy, rune::replication::CleanupPolicy::Delete);
    EXPECT_EQ(cfg.retentionMs, -1);
    EXPECT_EQ(cfg.retentionBytes, -1);
    EXPECT_EQ(cfg.logStartOffset, 0);
    EXPECT_EQ(cfg.minIsr, 2);
    EXPECT_EQ(rune::node::securityMinIsr(5), 3);
    EXPECT_EQ(cfg.durability, rune::replication::DurabilityMode::Majority);

    ThreeNodes c = ThreeNodes::start("sec_nocompact");
    ASSERT_TRUE(awaitSecurityLeader(c, 30s));
    GoldenAppender g;
    for (int i = 0; i < 4; ++i) ASSERT_GE(g.appendNext(c), 1);
    for (BrokerNode* n : c.live()) {
        EXPECT_TRUE(awaitSecurityEndOffset(*n, 4, 10s));
        EXPECT_EQ(securityOf(*n).logStart, 0) << "the security log always starts at offset 0";
    }
}

// E2 lesson (ISR v3, for -7): a follower's log reset (a wiped data dir, a replaced disk) is handled - the broker
// refetches the whole security log from offset 0 and rejoins the ISR; its view is rebuilt from the replay.
TEST(SecurityReplicationRegression, AWipedFollowerRefetchesSecurityFromZero) {
    ThreeNodes c = ThreeNodes::start("sec_wipe");
    ASSERT_TRUE(awaitSecurityLeader(c, 30s));
    GoldenAppender g;
    for (int i = 0; i < 3; ++i) ASSERT_GE(g.appendNext(c), 1);
    const int32_t leader = settledSecurityLeader(c);
    const int32_t victim = leader == 3 ? 2 : 3;
    ASSERT_TRUE(awaitViewsAt(c, 2, 20s));
    c.stop(victim);
    c.wipe(victim);
    const int64_t hw2 = g.appendNext(c);
    ASSERT_GT(hw2, 3) << "two of three keep committing";
    c.startNode(victim);
    EXPECT_TRUE(awaitSecurityEndOffset(c.node(victim), hw2, 30s)) << "refetched from offset 0";
    EXPECT_TRUE(awaitInSecurityIsr(c, victim, 30s));
    EXPECT_EQ(securityOf(c.node(victim)).logStart, 0);
    ASSERT_TRUE(awaitViewsAt(c, 3, 30s));
    EXPECT_EQ(c.node(victim).securityViewForTest().digest, g.digestOfFirst(4)) << "the wiped broker re-mirrors the whole log";
}

// A single node leads __security itself (no controller path), keeps it across a restart, and boots its control-thread
// view from the local prefix (the startup thread hands the booted state over before any thread runs).
TEST(SecurityReplicationRegression, SingleNodeLeadsSecurityAndBootsItsViewFromTheLocalPrefix) {
    TmpDir dir{"sec_single"};
    const Ports ports = Ports::discover(1);
    GoldenAppender g;
    int64_t endOffset = -1;
    {
        BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
        n.start();
        ASSERT_TRUE(n.awaitLeader(10'000ms));
        const auto events = rune::security::testing::goldenScriptV1();
        for (std::size_t i = 0; i < 5; ++i) {
            auto bytes = rune::security::encode(events[i]);
            int64_t base = -1;
            const auto deadline = std::chrono::steady_clock::now() + 10s;
            while (base < 0 && std::chrono::steady_clock::now() < deadline) {
                base = n.appendSecurityRecordOnLeaderForTest(std::span<const uint8_t>(*bytes));
                if (base < 0) std::this_thread::sleep_for(20ms);
            }
            ASSERT_EQ(base, static_cast<int64_t>(i));
        }
        endOffset = 5;
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (n.securityViewForTest().appliedOffset < 4 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(20ms);
        EXPECT_EQ(n.securityViewForTest().appliedOffset, 4);
        EXPECT_EQ(n.securityViewForTest().digest, g.digestOfFirst(5));
        n.stop();
        ASSERT_NE(n.securityLogForTest(), nullptr);
        EXPECT_GE(n.securityLogForTest()->logEndOffset(), endOffset);
        EXPECT_EQ(n.securityLogForTest()->dir(), (dir.path / "__security").string());
    }
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    // The view was booted from the checkpointed prefix before any thread ran.
    const auto v = n.securityViewForTest();
    EXPECT_EQ(v.appliedOffset, 4);
    EXPECT_EQ(v.digest, g.digestOfFirst(5));
    EXPECT_FALSE(v.halted);
    EXPECT_EQ(securityOf(n).leo, endOffset) << "the security log is disk-backed: recovered on restart";
    n.stop();
}

// D28/D29: a record the rules refuse HALTS the admin applier, and the control thread learns it (the halt counter
// is the offset + 1, the view stops): this broker can no longer enforce revocations and fails closed (Task 14).
TEST(SecurityReplicationRegression, ATamperedRecordHaltsTheApplierAndTheControlViewRegression) {
    TmpDir dir{"sec_halt"};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    auto events = rune::security::testing::goldenScriptV1();
    events[1].fingerprint ^= 1;   // offset 1 does not reproduce its fingerprint
    for (std::size_t i = 0; i < 3; ++i) {
        auto bytes = rune::security::encode(events[i]);
        int64_t base = -1;
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (base < 0 && std::chrono::steady_clock::now() < deadline) {
            base = n.appendSecurityRecordOnLeaderForTest(std::span<const uint8_t>(*bytes));
            if (base < 0) std::this_thread::sleep_for(20ms);
        }
        ASSERT_EQ(base, static_cast<int64_t>(i));
    }
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!n.securityViewForTest().halted && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(20ms);
    const auto v = n.securityViewForTest();
    EXPECT_TRUE(v.halted);
    EXPECT_EQ(v.appliedOffset, 0) << "what was applied stays; nothing after the tampered record";
    const auto m = n.securityMetricsForTest();
    EXPECT_EQ(m.halted, 2) << "the halt counter is (halted offset + 1)";
    n.stop();
}
