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

// ===========================================================================
// Authentication Task 10 (D36 / D24): the security write path through real brokers.
// ===========================================================================
namespace {
namespace pc = rune::protocol::client;

std::vector<uint8_t> createRoleFrame(int64_t sid, int64_t corr, std::string_view role, uint64_t ridLo) {
    pc::SecurityDdl m;
    const std::string statement = "CREATE ROLE " + std::string(role) + ";";   // DdlExtension holds a view: keep it alive
    m.sessionId = sid;
    m.correlationId = corr;
    m.ext.clientName = "sec-wire-test";
    m.ext.statement = statement;
    m.ext.requestId = {0x5EC7, ridLo};
    m.kind = pc::SecurityOpKind::CreateRole;
    m.name = role;
    std::vector<uint8_t> buf(m.encodedSize());
    buf.resize(m.encode(buf.data()));
    return buf;
}
}  // namespace

// A follower redirects a security DDL to the __security leader's client endpoint, exactly like catalog DDL; the leader
// answers OK only once ITS OWN control-thread view applied the event (D24), and every broker converges on it.
TEST(SecurityReplicationRegression, SecurityDdlViaAFollowerIsRedirectedToTheLeader) {
    ThreeNodes c = ThreeNodes::start("sec_ddl_redirect");
    ASSERT_TRUE(awaitSecurityLeader(c, 30s));
    const int32_t leader = settledSecurityLeader(c);
    ASSERT_GT(leader, 0);
    const int32_t follower = leader == 1 ? 2 : 1;
    ShardTestClient cl(c.clientsDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);

    // 1. Via the follower: Redirect (3) naming the leader's client endpoint.
    int64_t fsid = -1;
    for (const auto dl = std::chrono::steady_clock::now() + 30s; fsid < 0 && std::chrono::steady_clock::now() < dl;) {
        fsid = cl.connect(c.clientChannel(follower), kClientStream);
        if (fsid < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(fsid, 0);
    const int64_t corr1 = cl.nextCorrelationId();
    const auto f1 = createRoleFrame(fsid, corr1, "world_server", 1);
    ASSERT_TRUE(cl.send(c.clientChannel(follower), kClientStream, f1.data(), f1.size()));
    const auto a1 = cl.awaitAnyDdlAnswer(corr1, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(a1.has_value()) << "a follower must answer, never hang";
    EXPECT_EQ(a1->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(a1->message, c.clientChannel(leader)) << "the __security leader's client endpoint";

    // 2. Via the leader: DdlResponse OK; the leader's own view already holds the event when the OK arrives.
    int64_t lsid = -1;
    for (const auto dl = std::chrono::steady_clock::now() + 30s; lsid < 0 && std::chrono::steady_clock::now() < dl;) {
        lsid = cl.connect(c.clientChannel(leader), kClientStream);
        if (lsid < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(lsid, 0);
    const int64_t corr2 = cl.nextCorrelationId();
    const auto f2 = createRoleFrame(lsid, corr2, "world_server", 2);
    ASSERT_TRUE(cl.send(c.clientChannel(leader), kClientStream, f2.data(), f2.size()));
    const auto a2 = cl.awaitDdl(corr2, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(a2.has_value());
    EXPECT_EQ(a2->errorCode, 0) << a2->detail;
    EXPECT_NE(a2->objectId, 0u);
    EXPECT_EQ(a2->newVersion, 1u);
    EXPECT_GE(c.node(leader).securityViewForTest().appliedOffset, 0)
        << "the OK left only after THIS broker's view applied the event (D24)";

    // 3. Every broker converges on the same state.
    ASSERT_TRUE(awaitViewsAt(c, 0, 20s));
    const uint64_t digest = c.node(leader).securityViewForTest().digest;
    for (BrokerNode* n : c.live()) EXPECT_EQ(n->securityViewForTest().digest, digest) << "node " << n->nodeIdForTest();

    // 4. The retry of the SAME request (same requestId) gets its original result - applied once.
    const int64_t corr3 = cl.nextCorrelationId();
    const auto f3 = createRoleFrame(lsid, corr3, "world_server", 2);
    ASSERT_TRUE(cl.send(c.clientChannel(leader), kClientStream, f3.data(), f3.size()));
    const auto a3 = cl.awaitDdl(corr3, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(a3.has_value());
    EXPECT_EQ(a3->errorCode, 0);
    EXPECT_EQ(a3->objectId, a2->objectId);
    EXPECT_EQ(c.node(leader).securityViewForTest().appliedOffset, 0) << "no second event";
}
