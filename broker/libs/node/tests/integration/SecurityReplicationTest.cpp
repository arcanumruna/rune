// SecurityReplicationTest.cpp — authentication Task 9: the `__security` partition (topic -7, partition 0) on every
// broker, replicated for real (acks=all, minIsr = majority) on the admin-thread replication plane it shares with
// `__catalog`, its leader always elected from its ISR by the controller (never unclean), every applied event
// mirrored to each broker's control-thread SecurityView in offset order.  In-process BrokerNodes over REAL Aeron UDP
// (InProcessUnauthenticated - authentication is not the subject here, the log is).
#include "support/SecurityBrokerCluster.h"
#include "support/SecurityEventChain.h"

#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/AdminChannels.h>
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

// Review C I5 (focus 4, D24): the real wiring - the egress drain holds the gated OK and pumpSecurityGate_ releases it at
// min(view applied, refreshed).  The control thread's mirror is held; the admin has committed AND applied the event;
// no OK may leave until THIS broker's view applied it.
TEST(SecurityAnswerGateRegression, TheBrokerHoldsTheOkUntilItsOwnViewAppliedTheEventRegression) {
    TmpDir dir{"sec_gate_real"};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    const auto ready = std::chrono::steady_clock::now() + 20s;
    while (securityOf(n).role != rune::replication::Role::Leader && std::chrono::steady_clock::now() < ready)
        std::this_thread::sleep_for(20ms);
    TmpDir cdir{"sec_gate_real_client"};
    ShardTestClient cl(cdir.path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    int64_t sid = -1;
    for (const auto dl = std::chrono::steady_clock::now() + 30s; sid < 0 && std::chrono::steady_clock::now() < dl;) {
        sid = cl.connect("aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports.client[0]), kClientStream);
        if (sid < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(sid, 0);
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports.client[0]);
    n.holdSecurityMirrorForTest(true);
    const int64_t corr = cl.nextCorrelationId();
    const auto f = createRoleFrame(sid, corr, "gated_role", 77);
    ASSERT_TRUE(cl.send(ch, kClientStream, f.data(), f.size()));
    // The admin thread committed and applied the event ...
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    auto adminApplied = [&] {
        return n.onAdminThreadForTest([&n]() { return n.adminWorkerAgentForTest()->worker().securityApplier()->state().appliedOffset(); });
    };
    while (adminApplied() < 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(10ms);
    ASSERT_EQ(adminApplied(), 0);
    // ... but THIS broker's view has not: no OK for a full second.
    EXPECT_FALSE(cl.awaitAnyDdlAnswer(corr, std::chrono::steady_clock::now() + 1s).has_value()) << "the OK left before the view applied";
    EXPECT_EQ(n.securityViewForTest().appliedOffset, -1);
    n.holdSecurityMirrorForTest(false);
    const auto ok = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, 0) << ok->detail;
    EXPECT_EQ(cl.answersFor(corr), 1u) << "exactly once";
    EXPECT_EQ(n.securityViewForTest().appliedOffset, 0);
    // A dedupe retry of the committed request: its original OK is released through the same gate.
    n.holdSecurityMirrorForTest(true);   // nothing new is mirrored, but the retry's gate offset (0) is already covered
    const int64_t corr2 = cl.nextCorrelationId();
    const auto retry = createRoleFrame(sid, corr2, "gated_role", 77);
    ASSERT_TRUE(cl.send(ch, kClientStream, retry.data(), retry.size()));
    const auto again = cl.awaitDdl(corr2, std::chrono::steady_clock::now() + 20s);
    n.holdSecurityMirrorForTest(false);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->errorCode, 0);
    EXPECT_EQ(again->objectId, ok->objectId);
    n.stop();
}

// #203: a kA2cSecurityApplied frame the control thread cannot parse may carry the latest event (a REVOKE, a DISABLE).
// It used to be counted and skipped: the mirror stayed stale (fail-open) and that event's held DDL OK waited forever.
// Now it halts the mirror fail-closed: the held OK is answered Internal at once and the view reports halted.
TEST(SecurityAnswerGateRegression, AMalformedAppliedFrameHaltsTheMirrorAndAnswersTheHeldOkRegression) {
    TmpDir dir{"sec_gate_malformed"};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    const auto ready = std::chrono::steady_clock::now() + 20s;
    while (securityOf(n).role != rune::replication::Role::Leader && std::chrono::steady_clock::now() < ready)
        std::this_thread::sleep_for(20ms);
    TmpDir cdir{"sec_gate_malformed_client"};
    ShardTestClient cl(cdir.path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    int64_t sid = -1;
    for (const auto dl = std::chrono::steady_clock::now() + 30s; sid < 0 && std::chrono::steady_clock::now() < dl;) {
        sid = cl.connect("aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports.client[0]), kClientStream);
        if (sid < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(sid, 0);
    const std::string ch = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(ports.client[0]);
    n.holdSecurityMirrorForTest(true);
    const int64_t corr = cl.nextCorrelationId();
    const auto f = createRoleFrame(sid, corr, "malformed_role", 78);
    ASSERT_TRUE(cl.send(ch, kClientStream, f.data(), f.size()));
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    auto adminApplied = [&] {
        return n.onAdminThreadForTest([&n]() { return n.adminWorkerAgentForTest()->worker().securityApplier()->state().appliedOffset(); });
    };
    while (adminApplied() < 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(10ms);
    ASSERT_EQ(adminApplied(), 0);
    ASSERT_FALSE(n.securityViewForTest().halted);
    // The event's frame stays held; a truncated one (shorter than its offset field) reaches the handler instead.
    n.injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityApplied), std::vector<uint8_t>{1, 2, 3});
    const auto v = n.securityViewForTest();
    EXPECT_TRUE(v.halted) << "an unparseable security frame fails closed, never skipped";
    EXPECT_EQ(v.appliedOffset, -1);
    EXPECT_EQ(n.securityMetricsForTest().halted, 1) << "halted at the next offset the mirror would apply (0) + 1";
    const auto ans = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(ans.has_value()) << "the held OK is answered, never held forever";
    EXPECT_EQ(ans->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal)) << ans->detail;
    n.holdSecurityMirrorForTest(false);   // the held event reaches a halted view: it stays halted
    EXPECT_TRUE(n.securityViewForTest().halted);
    n.stop();
}

// #203, the halt frame itself: a malformed kA2cSecurityHalted still halts the mirror (it was skipped, leaving the view
// serving after the admin applier had already stopped).
TEST(SecurityAnswerGateRegression, AMalformedHaltedFrameStillHaltsTheMirrorRegression) {
    TmpDir dir{"sec_halt_malformed"};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    ASSERT_FALSE(n.securityViewForTest().halted);
    n.injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityHalted), std::vector<uint8_t>{9});
    EXPECT_TRUE(n.securityViewForTest().halted);
    EXPECT_GE(n.securityMetricsForTest().halted, 1);
    n.stop();
}

// #203 adversarial review (PR #516): the third admin-to-control security frame.  kA2cSecurityServing is posted only on
// change, so a skipped one is lost for good - a "not caught up" verdict dropped leaves logins served from a stale view
// (fail-open).  Every malformed shape halts the mirror: the pre-#205 14-byte frame (no knownEnd), one byte too long, an
// out-of-range bool.  A well-formed frame after the halt never un-halts it.
TEST(SecurityAnswerGateRegression, AMalformedServingFrameHaltsTheMirrorAdversarialRegression) {
    for (const auto& [what, frame] : std::vector<std::pair<const char*, std::vector<uint8_t>>>{
             {"old 14-byte layout", std::vector<uint8_t>(14, 0)},
             {"pre-#603 22-byte layout (no behind byte)", std::vector<uint8_t>(22, 0)},
             {"pre-#460 23-byte layout (no held byte)", std::vector<uint8_t>(23, 0)},
             {"held byte 2", [] { std::vector<uint8_t> f(kSecurityServingWireLen, 0); f[23] = 2; return f; }()},
             {"behind byte 2", [] { std::vector<uint8_t> f(kSecurityServingWireLen, 0); f[22] = 2; return f; }()},
             {"one byte too long", std::vector<uint8_t>(kSecurityServingWireLen + 1, 0)},
             {"caughtUp byte 2", [] { std::vector<uint8_t> f(kSecurityServingWireLen, 0); f[0] = 2; return f; }()},
             {"empty", std::vector<uint8_t>{}}}) {
        TmpDir dir{"sec_serving_malformed"};
        const Ports ports = Ports::discover(1);
        BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
        n.start();
        ASSERT_TRUE(n.awaitLeader(10'000ms)) << what;
        ASSERT_FALSE(n.securityViewForTest().halted) << what;
        n.injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityServing), frame);
        EXPECT_TRUE(n.securityViewForTest().halted) << what << ": an unparseable verdict fails closed, never skipped";
        EXPECT_GE(n.securityMetricsForTest().halted, 1) << what;
        std::vector<uint8_t> good(kSecurityServingWireLen);
        writeSecurityServing(good.data(), true, 0, 1, true, 0);
        n.injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityServing), good);
        EXPECT_TRUE(n.securityViewForTest().halted) << what << ": a later good verdict never un-halts the mirror";
        n.stop();
    }
}

// Review C (parked item): a `__security` leader whose APPLIER halted (local damage) reports it in its node-control
// register (kNodeSecurityHalted); the controller steers the (-7, 0) leadership to a healthy broker, so security DDL
// works again without restarting anything.
TEST(SecurityReplicationRegression, AHaltedSecurityLeaderIsSteeredOffRegression) {
    ThreeNodes c = ThreeNodes::start("sec_halt_steer");
    ASSERT_TRUE(awaitSecurityLeader(c, 30s));
    const int32_t halted = settledSecurityLeader(c);
    ASSERT_GT(halted, 0);
    BrokerNode& hn = c.node(halted);
    hn.onAdminThreadForTest([&hn]() {   // damage on the LEADER's applier only (an admin-thread seam)
        hn.adminWorkerAgentForTest()->worker().securityApplierForTest()->tamperNextFingerprintForTest();
        return 0;
    });
    GoldenAppender g;
    ASSERT_GE(g.appendNext(c), 0) << "one valid event, committed by the healthy majority";
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (!hn.securityViewForTest().halted && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(20ms);
    ASSERT_TRUE(hn.securityViewForTest().halted) << "the damaged leader's mirror halted";

    // The (-7, 0) leadership moves to a healthy broker (settled: committed ISR member, admin replica Leader).
    int32_t now = halted;
    while (now == halted && std::chrono::steady_clock::now() < deadline + 30s) {
        now = settledSecurityLeader(c);
        if (now <= 0 || now == halted) { now = halted; std::this_thread::sleep_for(50ms); }
    }
    ASSERT_NE(now, halted) << "the halted leader was never steered off";
    ASSERT_GT(now, 0);

    // CREATE ROLE through the NEW leader commits ...
    ShardTestClient cl(c.clientsDir->path / "wire");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersionAuth);
    int64_t sid = -1;
    for (const auto dl = std::chrono::steady_clock::now() + 30s; sid < 0 && std::chrono::steady_clock::now() < dl;) {
        sid = cl.connect(c.clientChannel(now), kClientStream);
        if (sid < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(sid, 0);
    int64_t corr = cl.nextCorrelationId();
    auto f = createRoleFrame(sid, corr, "after_steer", 9);
    ASSERT_TRUE(cl.send(c.clientChannel(now), kClientStream, f.data(), f.size()));
    auto ok = cl.awaitDdl(corr, std::chrono::steady_clock::now() + 30s);
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->errorCode, 0) << ok->detail;
    // ... and the old leader still answers Internal (its route is Halted), never silently.
    int64_t hsid = -1;
    for (const auto dl = std::chrono::steady_clock::now() + 30s; hsid < 0 && std::chrono::steady_clock::now() < dl;) {
        hsid = cl.connect(c.clientChannel(halted), kClientStream);
        if (hsid < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(hsid, 0);
    corr = cl.nextCorrelationId();
    f = createRoleFrame(hsid, corr, "on_halted", 10);
    ASSERT_TRUE(cl.send(c.clientChannel(halted), kClientStream, f.data(), f.size()));
    const auto internal = cl.awaitAnyDdlAnswer(corr, std::chrono::steady_clock::now() + 20s);
    ASSERT_TRUE(internal.has_value());
    EXPECT_EQ(internal->code, static_cast<int16_t>(pc::ErrorCode::Internal)) << internal->message;
}
