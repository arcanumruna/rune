// CatalogReplicationTest.cpp — catalog history Task 6: the `__catalog`
// partition (topic -6, partition 0) on every broker, replicated for real
// (acks=all, minIsr = majority) on its own admin-thread replication plane, its
// leader always elected from its ISR by the controller (unclean election
// disabled).  In-process BrokerNodes over REAL Aeron UDP (shard mode — the
// default — and legacy workers), plus the admin/control relay units that
// carry its role and ISR.
//
// E2 lessons pinned here: a leader is never unclean; a follower whose log is
// reset (new broker / wiped dir) refetches from offset 0 and rejoins the ISR; a
// controller answer reaches the admin-owned replica with its leaderId (shard
// mode: leaderId stayed -1 for worker-owned replicas, and their acks never
// matched); a relay ring that is full re-delivers, never drops.
#include "support/OwnerThreadTestHelpers.h"
#include "support/ShardTestClient.h"
#include "support/CatalogBrokerCluster.h"

#include <rune/node/AdminIsrChannel.h>
#include <cstdlib>
#include <fstream>
#include <string_view>
#include <rune/node/BrokerNode.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerPool.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/NodeConfig.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
namespace rr = rune::replication;
using namespace std::chrono_literals;

using namespace rune::node::test::catalog_cluster;

namespace {

void expectFormation(ThreeNodes& c) {
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    for (BrokerNode* n : c.live()) {
        const auto pa = catalogAssignmentOf(*n);
        ASSERT_TRUE(pa.has_value()) << "node " << n->nodeIdForTest();
        EXPECT_EQ(pa->leaderNode, leader);
        EXPECT_EQ(pa->replicas.size(), 3u) << "replicated to every broker";
        EXPECT_TRUE(contains(pa->isr, leader));
        EXPECT_TRUE(pa->cleanElection);
        const auto v = catalogOf(*n);
        EXPECT_TRUE(v.present) << "every broker builds the catalog";
        EXPECT_EQ(v.role == rr::Role::Leader, n->nodeIdForTest() == leader)
            << "exactly the committed leader leads (node " << n->nodeIdForTest() << ")";
    }
}

void expectReplication(ThreeNodes& c) {
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendEventOnCatalogLeaderForTest();   // admin-thread task (onAdminThreadForTest)
    ASSERT_GE(hw, 1);
    for (BrokerNode* n : c.live())
        EXPECT_TRUE(c.awaitCatalogEndOffset(*n, hw, 10s)) << "node " << n->nodeIdForTest();
    // Every broker's catalog is a disk log under dataDir/__catalog.
    for (int id = 1; id <= 3; ++id)
        EXPECT_TRUE(fs::is_directory(c.dirs[static_cast<std::size_t>(id - 1)]->path / "__catalog")) << id;
}

} // namespace

// ===========================================================================
// 3 brokers over real Aeron
// ===========================================================================

// Catalog history §3/§5.6: __catalog exists on every broker from cluster formation,
// is replicated for real (acks=all, minIsr = majority), and its leader is always an
// ISR member (unclean election disabled).
TEST(CatalogReplicationRegression, FormationCreatesTheCatalogOnEveryBrokerWithACleanLeader) {
    ThreeNodes c = ThreeNodes::start("cat_form");
    expectFormation(c);
}

TEST(CatalogReplicationRegression, FormationCreatesTheCatalogOnEveryBrokerWithLegacyWorkers) {
    ThreeNodes c = ThreeNodes::start("cat_form_l", {1, 2, 3}, Mode::Legacy);
    expectFormation(c);
}

// The production default: shard workers + the runtime FUSED on worker 0 (the
// node client is worker 0's invoker client; the admin thread polls the catalog
// plane on it).
TEST(CatalogReplicationRegression, FormationAndReplicationInTheFusedDefaultPlacement) {
    ThreeNodes c = ThreeNodes::start("cat_fused", {1, 2, 3}, Mode::ShardFused);
    expectFormation(c);
    expectReplication(c);
}

// (Fix round 1 M7: named for what it shows — a committed append reaches every
// broker's log.  The HW-only-after-a-majority ordering is pinned at replica
// level by CatalogMinIsrRegression below.)
TEST(CatalogReplicationRegression, CommittedAppendsReachEveryBroker) {
    ThreeNodes c = ThreeNodes::start("cat_repl");
    expectReplication(c);
}

TEST(CatalogReplicationRegression, CommittedAppendsReachEveryBrokerWithLegacyWorkers) {
    ThreeNodes c = ThreeNodes::start("cat_repl_l", {1, 2, 3}, Mode::Legacy);
    expectReplication(c);
}

// With both followers unreachable the leader may still take an append into its
// log, but the HW never covers it: the followers stay in its (maximal) ISR and
// hold the HW (fix round 1 I1: renamed for what it shows — the min-ISR gate
// itself is pinned by CatalogMinIsrRegression).
TEST(CatalogReplicationRegression, NoHighWatermarkProgressWhileBothFollowersAreUnreachable) {
    ThreeNodes c = ThreeNodes::start("cat_major");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw0 = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GE(hw0, 1);
    const int32_t leader = c.catalogLeaderId();
    for (int id = 1; id <= 3; ++id) if (id != leader) c.stop(id);
    BrokerNode& l = c.node(leader);
    const int64_t leo = l.onAdminThreadForTest([&l]() -> int64_t {
        rr::PartitionReplica* r = l.catalogReplicaForTest();
        const std::vector<uint8_t> rec{8};
        const rr::Offset base = r->append(std::span<const uint8_t>(rec), /*acksAll=*/true);
        return base < 0 ? base : r->logEndOffset();
    });
    const auto until = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < until) {
        EXPECT_LE(c.catalogHighWatermark(l), hw0) << "the HW moved without a majority (leo=" << leo << ")";
        std::this_thread::sleep_for(100ms);
    }
}

TEST(CatalogReplicationRegression, LeaderKillElectsAnIsrMemberAndKeepsEveryCommittedRecord) {
    ThreeNodes c = ThreeNodes::start("cat_kill");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GE(hw, 1);
    const int32_t old = c.catalogLeaderId();
    c.stop(old);
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(old, 30s));
    auto& nl = c.node(c.catalogLeaderId());
    // Never unclean: the new leader's LOG already holds the committed prefix the
    // moment it leads (it was elected from the ISR) ...
    EXPECT_GE(catalogOf(nl).leo, hw) << "the new leader lacks committed records — an unclean election";
    // ... and its HW covers it once its ISR re-forms.
    ASSERT_TRUE(c.awaitCatalogHighWatermark(nl, hw, 20s));
    EXPECT_GE(c.catalogHighWatermark(nl), hw) << "never unclean: the new leader holds the committed prefix";
    // And it keeps taking catalog writes.
    EXPECT_GT(c.appendEventOnCatalogLeaderForTest(), hw);
}

TEST(CatalogReplicationRegression, CatalogIsNeverCompactedOrRetentionDeleted) {
    const auto cfg = rune::node::makeCatalogPartitionConfig(rune::node::NodeConfig{}, 3);
    EXPECT_EQ(cfg.cleanupPolicy, rune::replication::CleanupPolicy::Delete);
    EXPECT_EQ(cfg.retentionMs, -1);
    EXPECT_EQ(cfg.retentionBytes, -1);
    EXPECT_EQ(cfg.logStartOffset, 0);
    EXPECT_EQ(cfg.minIsr, 2);
    EXPECT_EQ(rune::node::catalogMinIsr(5), 3);
}

TEST(CatalogReplicationRegression, ANewBrokerWithAnEmptyDataDirFetchesTheCatalogFromOffsetZero) {
    ThreeNodes c = ThreeNodes::start("cat_join", /*startOnly=*/{1, 2});
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    // Fix round 1 (M6): the bootstrap assignment seeds the ISR with EVERY
    // configured broker (MetadataService::decideCoordinatorPartition), including
    // broker 3, which never started.  HW = min over the ISR, so the catalog
    // commits nothing until the controller fences it / the leader shrinks it
    // (≈ the fencing timeout, at most replicaLagMaxMs) — a bounded formation
    // stall, never a commit without a majority.
    const auto t0 = std::chrono::steady_clock::now();
    const int64_t hw = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GE(hw, 1);
    const auto pa = catalogAssignmentOf(c.node(1));
    ASSERT_TRUE(pa.has_value());
    EXPECT_FALSE(contains(pa->isr, 3)) << "the first commit can only follow the never-started broker leaving the ISR";
    EXPECT_EQ(pa->replicas.size(), 3u) << "it stays a replica (it fetches from 0 when it starts)";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 20s) << "the formation stall is bounded";
    c.startNode(3);                                       // empty data dir
    EXPECT_TRUE(c.awaitCatalogEndOffset(c.node(3), hw, 20s));
    EXPECT_EQ(catalogOf(c.node(3)).logStart, 0) << "the catalog always starts at offset 0";
    EXPECT_TRUE(c.awaitInCatalogIsr(3, 20s)) << "AlterIsr admits it once caught up";
}

// E2 lesson: a follower's log reset (a wiped data dir, a replaced disk) must be
// handled — the broker refetches the whole catalog from offset 0 and rejoins
// the ISR; it is never counted in sync with a log it no longer has.
TEST(CatalogReplicationRegression, AFollowerWhoseDataDirIsWipedRefetchesFromZeroAndRejoinsTheIsr) {
    ThreeNodes c = ThreeNodes::start("cat_wipe");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw1 = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GE(hw1, 1);
    const int32_t leader = c.catalogLeaderId();
    const int32_t victim = leader == 3 ? 2 : 3;
    ASSERT_TRUE(c.awaitCatalogEndOffset(c.node(victim), hw1, 10s));
    c.stop(victim);
    c.wipe(victim);
    const int64_t hw2 = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GT(hw2, hw1) << "two of three keep committing";
    c.startNode(victim);
    EXPECT_TRUE(c.awaitCatalogEndOffset(c.node(victim), hw2, 20s)) << "refetched from offset 0";
    EXPECT_TRUE(c.awaitInCatalogIsr(victim, 20s));
}

// ISR v3 Step 10 (B-I8): the same disk loss for a node formatted `auto` (a raw wipe, no storage.meta): it decides Joiner
// at runtime, is stripped and confirmed by the controller, refetches the catalog from 0 and rejoins the ISR.
TEST(CatalogReplicationRegression, AnAutoFormattedFollowerWhoseDataDirIsWipedRefetchesFromZeroAndRejoinsTheIsr) {
    ThreeNodes c = ThreeNodes::start("cat_wipe_auto");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw1 = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GE(hw1, 1);
    const int32_t leader = c.catalogLeaderId();
    const int32_t victim = leader == 3 ? 2 : 3;
    ASSERT_TRUE(c.awaitCatalogEndOffset(c.node(victim), hw1, 10s));
    c.stop(victim);
    c.wipeRaw(victim, rune::node::StorageFormat::Auto);
    const int64_t hw2 = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GT(hw2, hw1) << "two of three keep committing";
    c.startNode(victim);
    EXPECT_TRUE(c.awaitCatalogEndOffset(c.node(victim), hw2, 30s)) << "refetched from offset 0";
    EXPECT_TRUE(c.awaitInCatalogIsr(victim, 30s));
}

// A full-cluster restart: every broker recovers its disk catalog, and the
// committed leader (which already led at the committed epoch in its previous
// incarnation — the restart fence) is re-elected only at a NEW epoch through
// the admin→control AlterIsr relay; the committed prefix survives and the
// catalog keeps taking writes.
TEST(CatalogReplicationRegression, FullClusterRestartRecoversTheCatalogAndReElectsAtANewEpoch) {
    ThreeNodes c = ThreeNodes::start("cat_restart");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int64_t hw = c.appendEventOnCatalogLeaderForTest();
    ASSERT_GE(hw, 1);
    for (int id = 1; id <= 3; ++id) ASSERT_TRUE(c.awaitCatalogEndOffset(c.node(id), hw, 10s)) << id;
    const auto before = catalogAssignmentOf(c.node(1));
    ASSERT_TRUE(before.has_value());
    for (int id = 3; id >= 1; --id) c.stop(id);
    for (int id = 1; id <= 3; ++id) c.startNode(id);
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const auto after = catalogAssignmentOf(c.node(1));
    ASSERT_TRUE(after.has_value());
    EXPECT_GT(after->leaderEpoch, before->leaderEpoch) << "a new leadership always gets a new epoch";
    for (int id = 1; id <= 3; ++id) EXPECT_GE(catalogOf(c.node(id)).leo, hw) << "recovered from disk: " << id;
    EXPECT_GT(c.appendEventOnCatalogLeaderForTest(), hw);
}

// ===========================================================================
// Single node: the consensus leader leads the catalog (no controller path).
// ===========================================================================
TEST(CatalogReplicationRegression, SingleNodeLeadsTheCatalogAndKeepsItAcrossARestart) {
    TmpDir dir{"cat_single"};
    const Ports ports = Ports::discover(1);
    int64_t hw = -1;
    {
        BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
        n.start();
        ASSERT_TRUE(n.awaitLeader(10'000ms));
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (hw < 0 && std::chrono::steady_clock::now() < deadline) {
            hw = n.onAdminThreadForTest([&n]() -> int64_t {
                rr::PartitionReplica* r = n.catalogReplicaForTest();
                if (r == nullptr || r->role() != rr::Role::Leader) return -1;
                const std::vector<uint8_t> rec{3, 1, 4};
                if (r->append(std::span<const uint8_t>(rec), /*acksAll=*/true) < 0) return -1;
                return r->highWatermark() >= r->logEndOffset() ? r->highWatermark() : -1;
            });
            if (hw < 0) std::this_thread::sleep_for(20ms);
        }
        ASSERT_GE(hw, 1) << "a single node commits catalog writes (minIsr 1)";
        n.stop();
        // Quiesced: the Log is readable from the test thread now.
        ASSERT_NE(n.catalogLogForTest(), nullptr);
        EXPECT_GE(n.catalogLogForTest()->logEndOffset(), hw);
        EXPECT_EQ(n.catalogLogForTest()->dir(), (dir.path / "__catalog").string());
    }
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    EXPECT_GE(catalogOf(n).leo, hw) << "the catalog is disk-backed: recovered on restart";
    n.stop();
}

// Catalog history Task 7 (review M8, the Task 6 carry-over): the applied checkpoint
// must never let a broker silently replay a SHORTER history than it applied.  A
// checkpoint at or past the recovered __catalog log end refuses to start, with an
// explicit error; one below it starts; a damaged checkpoint refuses too.
TEST(CatalogReplicationRegression, AppliedCheckpointPastTheLocalLogEndRefusesToStartRegression) {
    TmpDir dir{"cat_m8"};
    const Ports ports = Ports::discover(1);
    int64_t leo = -1;
    {
        BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
        n.start();
        ASSERT_TRUE(n.awaitLeader(10'000ms));
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (leo < 2 && std::chrono::steady_clock::now() < deadline) {
            leo = n.onAdminThreadForTest([&n]() -> int64_t {
                rr::PartitionReplica* r = n.catalogReplicaForTest();
                if (r == nullptr || r->role() != rr::Role::Leader) return -1;
                const std::vector<uint8_t> rec{2, 7};
                if (r->append(std::span<const uint8_t>(rec), /*acksAll=*/true) < 0) return -1;
                return r->logEndOffset();
            });
            if (leo < 2) std::this_thread::sleep_for(20ms);
        }
        ASSERT_GE(leo, 2);
        n.stop();
        leo = n.catalogLogForTest()->logEndOffset();
    }
    const std::string catalogDir = (dir.path / "__catalog").string();
    auto expectRefused = [&](const char* why, const char* needle) {
        BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
        try {
            n.start();
            ADD_FAILURE() << why << ": the broker started";
        } catch (const std::runtime_error& e) {
            const std::string what = e.what();
            EXPECT_NE(what.find("__catalog"), std::string::npos) << what;
            EXPECT_NE(what.find(needle), std::string::npos) << what;
        }
        n.stop();
    };
    ASSERT_TRUE(writeCatalogCheckpoint(catalogDir, {leo, 0x1234}));
    expectRefused("checkpoint == LEO (the event at it is missing)", "Refusing to start");
    ASSERT_TRUE(writeCatalogCheckpoint(catalogDir, {leo + 40, 0x1234}));
    expectRefused("checkpoint beyond the LEO", "Refusing to start");
    {   // a damaged checkpoint is not "none" (that would re-apply every effect)
        std::FILE* f = std::fopen((dir.path / "__catalog" / kCatalogCheckpointFile).string().c_str(), "wb");
        ASSERT_NE(f, nullptr);
        std::fputs("RCAT", f);
        std::fclose(f);
    }
    expectRefused("damaged checkpoint", "damaged");
    // A checkpoint below the log end is consistent: the broker starts.
    ASSERT_TRUE(writeCatalogCheckpoint(catalogDir, {leo - 1, 0x1234}));
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    n.start();
    EXPECT_TRUE(n.awaitLeader(10'000ms));
    n.stop();
}

// ===========================================================================
// The admin/control relay units (no cluster).
// ===========================================================================
namespace {
struct CatalogRig {
    TmpDir                          dir{"cat_rig"};
    rune::storage::Log              log{dir.path.string()};
    rr::NullPartitionTransport      transport;
    std::unique_ptr<rr::PartitionReplica> replica;
    SchemaRegistry                  reg;
    std::unique_ptr<AdminWorkerAgent> agent;
    CatalogRig() {
        log.recover();
        NodeConfig nc;
        nc.nodeId = 1;
        replica = std::make_unique<rr::PartitionReplica>(makeCatalogPartitionConfig(nc, 3), rr::Role::Follower, 0,
                                                         std::vector<rr::NodeId>{1, 2, 3},
                                                         std::vector<rr::NodeId>{1, 2, 3}, &log, &transport);
        agent = std::make_unique<AdminWorkerAgent>(reg, 1, AdminDeps{}, nullptr, replica.get(), nullptr);
        replica->setIsrChannel(&agent->catalogIsrChannel());
    }
    std::vector<rr::AlterIsrMsg> drainAlterIsr() {
        std::vector<rr::AlterIsrMsg> out;
        while (agent->failureRing().drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                              aeron::util::index_t off, aeron::util::index_t len) {
            if (tid != kA2cCatalogAlterIsr) return;
            rr::AlterIsrMsg m{};
            if (decodeCatalogAlterIsr(reinterpret_cast<const uint8_t*>(b.buffer() + off),
                                      static_cast<std::size_t>(len), m))
                out.push_back(m);
        }, 64) > 0) {}
        return out;
    }
};
} // namespace

TEST(CatalogAlterIsrRelayRegression, AdminWorkerAppliesTheCatalogRoleOnItsOwnThread) {
    CatalogRig rig;
    AdminRoleMsg lead;
    lead.leader = true; lead.epoch = 3; lead.leaderId = 1; lead.partitionEpoch = 0;
    lead.isrCount = 2; lead.isr[0] = 1; lead.isr[1] = 2;
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), lead, kMsgCatalogRole));
    (void)rig.agent->doWork(1'000'000);
    EXPECT_EQ(rig.replica->role(), rr::Role::Leader);
    EXPECT_EQ(rig.replica->epoch(), 3u);
    EXPECT_EQ(rig.replica->isr().size(), 2u) << "the committed ISR, not the replica set";
    EXPECT_EQ(rig.replica->committedPartitionEpochForTest(), 0);

    AdminRoleMsg follow;
    follow.leader = false; follow.epoch = 4; follow.leaderId = 2;
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), follow, kMsgCatalogRole));
    (void)rig.agent->doWork(2'000'000);
    EXPECT_EQ(rig.replica->role(), rr::Role::Follower);
    EXPECT_EQ(rig.replica->epoch(), 4u);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 0u);

    // A malformed catalog role is refused and reported — never applied.
    const uint8_t junk[3] = {1, 2, 3};
    const auto idx = rig.agent->roleRing().tryClaim(kMsgCatalogRole, sizeof junk);
    ASSERT_GE(idx, 0);
    std::memcpy(rig.agent->roleRing().base() + idx, junk, sizeof junk);
    rig.agent->roleRing().commit(idx);
    (void)rig.agent->doWork(3'000'000);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 1u);
    EXPECT_EQ(rig.replica->role(), rr::Role::Follower);
    EXPECT_EQ(rig.replica->epoch(), 4u);
}

// E2 shard-mode lesson, admin edition: the controller's answer reaches the
// admin-owned replica WITH its leaderId, so the replica matches its own
// leaderless-partition report and stops re-sending it (left at -1 it never
// matched: re-sent forever).
TEST(CatalogAlterIsrRelayRegression, ControllerAnswerReachesTheAdminReplicaAndMatchesItsReportRegression) {
    CatalogRig rig;
    AdminRoleMsg leaderless;
    leaderless.leader = false; leaderless.epoch = 5; leaderless.leaderId = -1;   // no live ISR member
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), leaderless, kMsgCatalogRole));
    (void)rig.agent->doWork(1'000'000);
    ASSERT_TRUE(rig.replica->leaderlessReportPendingForTest());
    const auto sent = rig.drainAlterIsr();
    ASSERT_EQ(sent.size(), 1u) << "the report rides the admin→control ring (kA2cCatalogAlterIsr)";
    EXPECT_EQ(sent[0].topicId, kCatalogTopicId);
    EXPECT_EQ(sent[0].leaderId, 1) << "the reporting replica";
    EXPECT_EQ(sent[0].leaderEpoch, 5);
    EXPECT_EQ(sent[0].isrCount, rr::AlterIsrMsg::kLeaderEpochBump);
    EXPECT_NE(sent[0].isr[1] & rr::AlterIsrMsg::kBumpFollowerReport, 0);

    // An answer that lost its leaderId (the E2 bug) is NOT this replica's.
    rr::AlterIsrResponseMsg lost{};
    lost.topicId = kCatalogTopicId; lost.partition = 0; lost.leaderId = -1; lost.leaderEpoch = 5;
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(rig.agent->roleRing(), lost));
    (void)rig.agent->doWork(2'000'000);
    EXPECT_TRUE(rig.replica->leaderlessReportPendingForTest());

    // The verbatim relay (leaderId = the asker) clears it.
    rr::AlterIsrResponseMsg ack = lost;
    ack.leaderId = 1;
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(rig.agent->roleRing(), ack));
    (void)rig.agent->doWork(3'000'000);
    EXPECT_FALSE(rig.replica->leaderlessReportPendingForTest())
        << "the controller's answer must reach the admin-owned replica and match";
}

// A full relay ring re-delivers (ABORT), never drops; a local answer refused by
// it is reported refused (the outbox retries), not delivered.
TEST(CatalogAlterIsrRelayRegression, ForeignAnswerRingFullIsRedeliveredNotDroppedRegression) {
    TmpDir ad{"cat_drv"};
    EmbeddedDriverConfig dc;
    dc.dir = ad.path.string();
    dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rr::AeronPartitionTransport tr(client, "aeron:ipc", 7311, /*self*/1, 1 << 20);

    struct Sink { int calls = 0; bool accept = false; rr::AlterIsrResponseMsg last{}; } sink;
    tr.setForeignAlterIsrResponseHandler(
        [](void* ctx, const rr::AlterIsrResponseMsg& r) noexcept {
            auto* s = static_cast<Sink*>(ctx);
            ++s->calls;
            s->last = r;
            return s->accept;
        }, &sink);

    rr::AlterIsrResponseMsg r{};
    r.topicId = kCatalogTopicId; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 2; r.partitionEpoch = 1;
    uint8_t wire[rr::AlterIsrResponseMsg::TOTAL_SIZE];
    ASSERT_EQ(r.encode(wire, sizeof wire), sizeof wire);

    EXPECT_FALSE(tr.deliverFrameForTest(wire, sizeof wire, 1)) << "a full relay ring leaves the fragment unconsumed";
    EXPECT_FALSE(tr.sendAlterIsrResponse(1, r)) << "a refused local answer is not reported delivered";
    sink.accept = true;
    EXPECT_TRUE(tr.deliverFrameForTest(wire, sizeof wire, 1));
    EXPECT_TRUE(tr.sendAlterIsrResponse(1, r));
    EXPECT_EQ(sink.calls, 4);
    EXPECT_EQ(sink.last.leaderId, 1) << "verbatim";
}

// ===========================================================================
// Fix-on-sight (hard rule 1), found reading ReplicationMetadataBridge.h for
// this task: the bridge kept std::atomic leadership/epoch MIRRORS for other
// threads to read.  Observers now read the bridge ON its own (control) thread.
// ===========================================================================
template <class B>
concept HasAtomicLeadershipMirror = requires(const B& b) { b.isLeaderAtomic(); } ||
                                    requires(const B& b) { b.lastEpochAtomic(); };
static_assert(!HasAtomicLeadershipMirror<rune::node::ReplicationMetadataBridge>,
              "no cross-thread atomic mirror on the bridge");

TEST(BridgeObserverRegression, LeadershipIsObservedOnTheControlThreadNotThroughAnAtomicMirrorRegression) {
    TmpDir dir{"cat_obs"};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, Mode::Shard)};
    EXPECT_FALSE(n.replicationIsLeaderForTest()) << "not running: nothing observed";
    EXPECT_EQ(n.replicationEpochForTest(), 0);
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!n.replicationIsLeaderForTest() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(n.replicationIsLeaderForTest());
    EXPECT_GE(n.replicationEpochForTest(), 1);
    n.stop();
    EXPECT_FALSE(n.replicationIsLeaderForTest()) << "stopped: the bridge is gone";
}

// ===========================================================================
// Fix round 0 (Task 6 review rulings).
// ===========================================================================

// Ruling on concern 3: a multi-node broker without a replication transport
// REFUSES to start — never a NullPartitionTransport catalog that can never be
// replicated (nor reach its majority minIsr).
TEST(CatalogReplicationRegression, MultiNodeWithoutAReplicationTransportRefusesToStartRegression) {
    TmpDir dir{"cat_norepl"};
    const Ports ports = Ports::discover(3);
    NodeConfig cfg = makeConfig(1, dir.path, ports, 3, Mode::Legacy);
    cfg.perWorkerReplication = false;
    for (auto& p : cfg.peers) p.replicationPort = 0;   // no replication endpoint anywhere
    BrokerNode n{cfg};
    try {
        n.start();
        ADD_FAILURE() << "a multi-node broker without a replication transport started";
    } catch (const std::runtime_error& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("__catalog"), std::string::npos) << what;
        EXPECT_NE(what.find("replication transport"), std::string::npos) << what;
    }
    n.stop();
}

// Ruling on concern 2: the control-thread observers, called ON the control
// thread, fail loudly instead of awaiting themselves forever.
namespace {
void expectObserversRefuseOnTheControlThread(Mode mode, const char* tag) {
    TmpDir dir{tag};
    const Ports ports = Ports::discover(1);
    BrokerNode n{makeConfig(1, dir.path, ports, 1, mode)};
    n.start();
    ASSERT_TRUE(n.awaitLeader(10'000ms));
    const std::thread::id onControl = n.onControlThreadForTest([] { return std::this_thread::get_id(); });
    EXPECT_EQ(n.controlThreadIdForTest(), onControl) << "start() records the real control thread";
    EXPECT_NE(n.controlThreadIdForTest(), std::this_thread::get_id());
    EXPECT_THROW((void)n.onControlThreadForTest([&n] { return n.replicationIsLeaderForTest(); }), std::logic_error);
    EXPECT_THROW((void)n.onControlThreadForTest([&n] { return n.replicationEpochForTest(); }), std::logic_error);
    EXPECT_NO_THROW((void)n.replicationIsLeaderForTest()) << "the test thread still reads it";
    n.stop();
    EXPECT_EQ(n.controlThreadIdForTest(), std::thread::id{});
}
} // namespace

TEST(BridgeObserverRegression, ObserverCalledOnTheControlThreadFailsLoudlyNotDeadlocksRegression) {
    expectObserversRefuseOnTheControlThread(Mode::Shard, "cat_obs_thr");
}

TEST(BridgeObserverRegression, ObserverCalledOnTheFusedControlThreadFailsLoudlyNotDeadlocksRegression) {
    expectObserversRefuseOnTheControlThread(Mode::ShardFused, "cat_obs_fus");
}


// ===========================================================================
// Fix round 1 (I1): the catalog's minIsr = majority + acks=all, fired at
// replica level with THE catalog config (no timing: a controller-managed
// leader, its committed ISR set explicitly).
// ===========================================================================
TEST(CatalogMinIsrRegression, AcksAllIsGatedBelowAMajorityAndCommitsOnceAFollowerAcksRegression) {
    TmpDir dir{"cat_minisr"};
    rune::storage::Log log{dir.path.string()};
    log.recover();
    rr::NullPartitionTransport transport;
    OneToOneByteRing a2c(1 << 16);
    AdminIsrChannel isrChannel(a2c);
    NodeConfig nc;
    nc.nodeId = 1;
    const auto cfg = makeCatalogPartitionConfig(nc, 3);
    EXPECT_EQ(cfg.minIsr, 2);   // EXPECT: the gate checks below must fire on their own
    rr::PartitionReplica r(cfg, rr::Role::Follower, 0, {1, 2, 3}, {1, 2, 3}, &log, &transport);
    r.setIsrChannel(&isrChannel);   // controller-managed, as in production
    const std::vector<uint8_t> rec{1, 2, 3};
    int64_t now = 1'000'000;

    // Committed ISR {1}: below the majority → acks=all is refused.
    r.becomeLeader(3, {1}, /*partitionEpoch=*/0);
    ASSERT_EQ(r.role(), rr::Role::Leader);
    (void)r.doWork(now += 1'000'000);
    EXPECT_EQ(r.append(std::span<const uint8_t>(rec), /*acksAll=*/true), rr::PartitionReplica::kAppendMinIsr)
        << "acks=all must be refused with a committed ISR below minIsr (majority)";
    EXPECT_EQ(r.logEndOffset(), 0) << "nothing entered the log";
    EXPECT_EQ(r.highWatermark(), 0);

    // The controller commits ISR {1, 2} (same epoch, partitionEpoch 1): the gate opens,
    // but the HW stays below the record until follower 2 holds it.
    r.becomeLeader(3, {1, 2}, /*partitionEpoch=*/1);
    const rr::Offset base = r.append(std::span<const uint8_t>(rec), /*acksAll=*/true);
    ASSERT_GE(base, 0) << "a majority ISR admits acks=all";
    const rr::Offset leo = r.logEndOffset();
    ASSERT_GT(leo, base);
    (void)r.doWork(now += 1'000'000);
    EXPECT_LE(r.highWatermark(), base) << "no follower holds it yet: not committed";

    // Follower 2 acks (fetches at the leader's LEO) → committed.
    rr::FetchRequestMsg ack{};
    ack.leaderEpoch = 3; ack.partition = 0; ack.followerId = 2; ack.fetchOffset = leo;
    ack.maxBytes = 1 << 20; ack.topicId = kCatalogTopicId; ack.lastFetchedEpoch = -1;
    r.onFetchRequest(ack, now += 1'000'000);
    (void)r.doWork(now += 1'000'000);
    EXPECT_EQ(r.highWatermark(), leo) << "the HW advances once a majority (leader + 1 follower) holds it";
}

// Fix round 1 (M1): -6's never-unclean wiring — no live ISR member → NO_LEADER
// with the ISR lineage kept (an out-of-sync live replica is never elected).
TEST(CatalogNeverUncleanRegression, NoLiveIsrMemberLeavesTheCatalogLeaderlessWithItsIsrKeptRegression) {
    rune::metadata::PartitionAssignment cur;
    cur.leaderNode = 1; cur.isr = {1}; cur.replicas = {1, 2, 3};
    cur.leaderEpoch = 4; cur.partitionEpoch = 7;
    const auto next = MetadataService::decideCoordinatorPartition(
        kCatalogTopicId, kCatalogPartition, /*self=*/2, &cur, /*eligible=*/{2, 3}, /*positions=*/nullptr,
        /*bootstrapEpoch=*/9, /*replicas=*/{1, 2, 3});
    ASSERT_TRUE(next.has_value()) << "the dead leader must be replaced";
    EXPECT_EQ(next->leaderNode, rune::metadata::NO_LEADER) << "never an unclean (out-of-ISR) leader";
    EXPECT_EQ(next->isr, std::vector<int32_t>{1}) << "the ISR lineage is kept";
    EXPECT_GT(next->leaderEpoch, cur.leaderEpoch);

    // With a live ISR member it is elected — and only it.
    cur.isr = {1, 3};
    const auto clean = MetadataService::decideCoordinatorPartition(
        kCatalogTopicId, kCatalogPartition, 2, &cur, {2, 3}, nullptr, 9, {1, 2, 3});
    ASSERT_TRUE(clean.has_value());
    EXPECT_EQ(clean->leaderNode, 3);
    EXPECT_TRUE(contains(clean->isr, 3));
    EXPECT_FALSE(contains(clean->isr, 2)) << "the out-of-sync controller is not admitted by an election";
}

// Task 11 fix round 1 (review M2): a catalog-halted ISR member is never chosen
// as the __catalog leader while a live, non-halted ISR member exists — and a
// halted current leader is moved off; with no healthy candidate the rules are
// unchanged (a leader beats none).  The halted broker stays in the ISR.  RED: the
// halted broker 1 (lowest id) was elected / kept, so every DDL answered Internal.
TEST(CatalogNeverUncleanRegression, CatalogLeadershipPrefersANonHaltedIsrMemberRegression) {
    rune::metadata::PartitionAssignment cur;
    cur.leaderNode = 9; cur.isr = {1, 3}; cur.replicas = {1, 2, 3};
    cur.leaderEpoch = 4; cur.partitionEpoch = 7;
    const std::vector<int32_t> halted{1};
    // Leader dead; the controller (2) is not in the ISR; ISR = {halted 1, healthy 3}.
    auto next = MetadataService::decideCoordinatorPartition(
        kCatalogTopicId, kCatalogPartition, /*self=*/2, &cur, /*eligible=*/{1, 2, 3}, nullptr, 9, {1, 2, 3}, false,
        &halted);
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->leaderNode, 3) << "the healthy ISR member, never the halted one";
    EXPECT_TRUE(contains(next->isr, 1)) << "the halted broker still replicates: kept in the ISR";
    // A halted CURRENT leader is moved to the healthy ISR member.
    cur.leaderNode = 1;
    next = MetadataService::decideCoordinatorPartition(kCatalogTopicId, kCatalogPartition, 2, &cur, {1, 2, 3},
                                                       nullptr, 9, {1, 2, 3}, false, &halted);
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->leaderNode, 3);
    // No healthy candidate: unchanged rules — the halted leader is kept.
    cur.isr = {1};
    next = MetadataService::decideCoordinatorPartition(kCatalogTopicId, kCatalogPartition, 2, &cur, {1, 2, 3},
                                                       nullptr, 9, {1, 2, 3}, false, &halted);
    EXPECT_FALSE(next.has_value()) << "a halted leader beats no leader";
    // A halted controller in the ISR does not take the leadership from a healthy one.
    cur.leaderNode = 3; cur.isr = {1, 3};
    next = MetadataService::decideCoordinatorPartition(kCatalogTopicId, kCatalogPartition, /*self=*/1, &cur,
                                                       {1, 2, 3}, nullptr, 9, {1, 2, 3}, false, &halted);
    EXPECT_FALSE(next.has_value()) << "the halted controller never prefers itself";
}

// Fix round 1 (M3 + M5): the controller-answer relay decision.
TEST(CatalogAlterIsrRelayRegression, AnswerNeverOvertakesAHeldRoleAndIsDroppedOnceTheAdminThreadIsGoneRegression) {
    OneToOneByteRing ring(1 << 12);
    rr::AlterIsrResponseMsg r{};
    r.topicId = kCatalogTopicId; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 2;
    EXPECT_EQ(relayCatalogAlterIsrResponse(&ring, /*rolePending=*/true, r), CatalogAnswerRelay::Held)
        << "FIFO: a held catalog role goes first";
    EXPECT_EQ(relayCatalogAlterIsrResponse(nullptr, false, r), CatalogAnswerRelay::Dropped)
        << "admin thread gone: dropped (counted), never blocking the peer's image";
    EXPECT_EQ(relayCatalogAlterIsrResponse(nullptr, true, r), CatalogAnswerRelay::Dropped);
    int frames = 0;
    (void)ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) {
        ++frames;
    }, 64);
    EXPECT_EQ(frames, 0) << "neither encoded anything";
    EXPECT_EQ(relayCatalogAlterIsrResponse(&ring, false, r), CatalogAnswerRelay::Delivered);
    OneToOneByteRing full(256);
    while (encodeCatalogAlterIsrResponse(full, r)) {}
    EXPECT_EQ(relayCatalogAlterIsrResponse(&full, false, r), CatalogAnswerRelay::Held) << "ring full: re-delivered";
}

// Fix round 1 (M4, fix-on-sight): a malformed __catalog role frame is
// reported, never applied as a default Follower@0 role.
TEST(CatalogAlterIsrRelayRegression, MalformedCatalogRoleFrameIsReportedNotAppliedRegression) {
    CatalogRig rig;
    const uint8_t junk[3] = {1, 9, 9};
    const auto idx = rig.agent->roleRing().tryClaim(kMsgCatalogRole, sizeof junk);
    ASSERT_GE(idx, 0);
    std::memcpy(rig.agent->roleRing().base() + idx, junk, sizeof junk);
    rig.agent->roleRing().commit(idx);
    (void)rig.agent->doWork(1'000'000);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 1u);
}

// Task 17: the retired __topic_schemas / __views role (kMsgAdminRole) is no longer a message type: a
// WELL-FORMED one is refused and counted like any unknown frame — never applied to the catalog replica.
TEST(CatalogAlterIsrRelayRegression, ARetiredSchemaRoleFrameIsRefusedNotAppliedRegression) {
    CatalogRig rig;
    AdminRoleMsg role;
    role.leader = true;
    role.epoch  = 7;
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), role, kMsgAdminRole));   // the retired type, explicit
    (void)rig.agent->doWork(1'000'000);
    EXPECT_EQ(rig.agent->badRoleFramesForTest(), 1u);
    EXPECT_NE(rig.replica->role(), rr::Role::Leader) << "the retired role must not make the catalog replica lead";
}

// ===========================================================================
// DR-bugs fix round 1, review A-I5: every fixed-port endpoint of the node driver (consensus, replication, client;
// in fused shard mode worker 0's ingress / replication / egress control) is BOUND before the driver's first
// publication auto-binds an ephemeral port.
// ===========================================================================
// Wiring (every platform): the anchors exist after start.  RED: no anchorFixedEndpoints_ call at start (0).
TEST(BindOrderRegression, FixedEndpointsAreAnchoredAtStartRegression) {
    ThreeNodes c = ThreeNodes::start("anchors", {1}, Mode::ShardFused);
    EXPECT_EQ(c.node(1).bindAnchorsForTest(), 6u)
        << "consensus + replication + client + shard ingress + shard replication subscriptions, egress control";
}

// The real proof (Linux gate, run in a container whose netns ephemeral range is NARROW — see
// scripts/run-linux-regression-vm.sh): the node's fixed ports and its publications' ephemeral sockets come from
// the same few dozen ports, so a fixed bind that happens after a publication loses its port most of the time.
// Only node 1 runs (its peers are configured, so it publishes to them; one netns per node in production).
// RED (anchors removed): most of the 20 starts refuse with "Address already in use".
TEST(BindOrderRegression, FixedPortsBindBeforeAnyEphemeralPublicationRegression) {
    const char* gate = std::getenv("RUNE_BIND_ORDER_GATE");
    if (gate == nullptr || std::string_view(gate) != "1")
        GTEST_SKIP() << "Linux gate only (RUNE_BIND_ORDER_GATE=1 inside a narrowed ip_local_port_range)";
#if !defined(__linux__)
    FAIL() << "RUNE_BIND_ORDER_GATE=1 off Linux";
#else
    std::ifstream in("/proc/sys/net/ipv4/ip_local_port_range");
    long lo = 0, hi = 0;
    ASSERT_TRUE(static_cast<bool>(in >> lo >> hi));
    ASSERT_LE(hi - lo + 1, 64) << "the gate needs a narrow range to make the race likely: " << lo << "-" << hi;
    // The test allocator (FreePort.h) never hands out ephemeral ports — so node 1's six fixed ports are placed
    // INSIDE the narrowed range explicitly: [lo, lo + 5], the rest of the range is what its own publications
    // auto-bind from.
    const auto p0 = static_cast<uint16_t>(lo);
    auto tweak = [p0](NodeConfig& cfg) {
        if (cfg.nodeId != 1) return;
        for (auto& peer : cfg.peers) {
            if (peer.id != 1) continue;
            peer.consensusPort = p0; peer.clientPort = static_cast<uint16_t>(p0 + 1);
            peer.replicationPort = static_cast<uint16_t>(p0 + 2); peer.ingressBasePort = static_cast<uint16_t>(p0 + 3);
            peer.replBasePort = static_cast<uint16_t>(p0 + 4); peer.egressControlBasePort = static_cast<uint16_t>(p0 + 5);
        }
        cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p0 + 1);
        cfg.workerIngressBasePort = p0 + 3;
        cfg.workerReplBasePort = p0 + 4;
        cfg.workerEgressControlBasePort = p0 + 5;
    };
    int refused = 0;
    std::string lastWhat;
    for (int i = 0; i < 20; ++i) {
        try {
            ThreeNodes c = ThreeNodes::start("bindorder", {}, Mode::ShardFused);
            c.configTweak = tweak;
            c.startNode(1);
        } catch (const std::exception& e) {
            ++refused;
            lastWhat = e.what();
        }
    }
    EXPECT_EQ(refused, 0) << refused << " of 20 starts refused; last: " << lastWhat;
#endif
}

// DR-bugs fix round 1, review B-I6 (catalog sibling): the controller's answer through the REAL catalog relay
// (encodeCatalogAlterIsrResponse -> role ring -> AdminWorker -> the admin-owned replica) keeps a stale-answered
// proposal pending and still clears its own InvalidRequest at the base.  RED: the relay loses partitionEpoch.
TEST(CatalogAlterIsrRelayRegression, StaleAnswerThroughTheCatalogRelayKeepsTheProposalRegression) {
    CatalogRig rig;
    AdminRoleMsg lead;
    lead.leader = true; lead.epoch = 2; lead.leaderId = 1; lead.partitionEpoch = 4;
    lead.isrCount = 3; lead.isr[0] = 1; lead.isr[1] = 2; lead.isr[2] = 3;
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), lead, kMsgCatalogRole));
    (void)rig.agent->doWork(1'000'000);
    ASSERT_EQ(rig.replica->role(), rr::Role::Leader);
    const int32_t live[] = {1, 2};
    rig.replica->shrinkIsrToLive(std::span<const int32_t>(live, 2), 2'000'000);
    ASSERT_TRUE(rig.replica->isrProposalPendingForTest());
    rr::AlterIsrResponseMsg stale{};
    stale.topicId = kCatalogTopicId; stale.partition = 0; stale.leaderId = 1; stale.leaderEpoch = 2;
    stale.partitionEpoch = 5; stale.errorCode = rr::kAlterIsrInvalidUpdateVersion;
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(rig.agent->roleRing(), stale));
    (void)rig.agent->doWork(3'000'000);
    EXPECT_TRUE(rig.replica->isrProposalPendingForTest()) << "a stale answer through the catalog relay dropped it";
    const auto& maximal = rig.replica->maximalIsrForTest();
    EXPECT_NE(std::find(maximal.begin(), maximal.end(), 3), maximal.end());
    lead.partitionEpoch = 5;   // the committed assignment resolves it
    ASSERT_TRUE(encodeAdminRole(rig.agent->roleRing(), lead, kMsgCatalogRole));
    (void)rig.agent->doWork(4'000'000);
    EXPECT_FALSE(rig.replica->isrProposalPendingForTest());
    rig.replica->shrinkIsrToLive(std::span<const int32_t>(live, 2), 5'000'000);
    ASSERT_TRUE(rig.replica->isrProposalPendingForTest());
    rr::AlterIsrResponseMsg own = stale;
    own.errorCode = rr::kAlterIsrInvalidRequest;   // at its base (5)
    ASSERT_TRUE(encodeCatalogAlterIsrResponse(rig.agent->roleRing(), own));
    (void)rig.agent->doWork(6'000'000);
    EXPECT_FALSE(rig.replica->isrProposalPendingForTest()) << "the relay lost partitionEpoch: its own InvalidRequest "
                                                              "no longer clears the proposal";
}

// Re-check M1 + M2: the anchors in thread placement (node driver 3 subscriptions; shard driver 2 subscriptions + 1
// MDC publication) and legacy per-worker egress (3 + W), every anchor publication at the minimum term.
// RED: delete the attachShardRuntimes_ anchor call (3), the legacy `pubs` loop (3), the term-length suffix.
namespace {
void expectAnchorPubsAtMinimumTerm(BrokerNode& n) {
    int pubs = 0;
    for (const auto& ch : n.bindAnchorChannelsForTest()) {
        if (ch.find("control-mode=dynamic") == std::string::npos) continue;
        ++pubs;
        EXPECT_NE(ch.find("term-length=65536"), std::string::npos) << "an anchor publication at the default term: " << ch;
    }
    EXPECT_GE(pubs, 1);
}
} // namespace
TEST(BindOrderRegression, ThreadPlacementShardDriverIsAnchoredRegression) {
    ThreeNodes c = ThreeNodes::start("anchors-t", {1}, Mode::Shard);
    EXPECT_EQ(c.node(1).bindAnchorsForTest(), 6u) << "node driver 3 + shard driver 2 subscriptions + 1 publication";
    expectAnchorPubsAtMinimumTerm(c.node(1));
}
TEST(BindOrderRegression, LegacyPerWorkerEgressIsAnchoredRegression) {
    ThreeNodes c = ThreeNodes::start("anchors-l", {}, Mode::Legacy);
    c.configTweak = [](NodeConfig& cfg) { cfg.perWorkerEgressEnabled = true; };
    c.startNode(1);
    EXPECT_EQ(c.node(1).bindAnchorsForTest(), 4u) << "consensus + replication + client + one per-worker egress (W=1)";
    expectAnchorPubsAtMinimumTerm(c.node(1));
}

// ===========================================================================
// ISR protocol, Step 3: the transport filters AlterIsr answers before any relay.
// ===========================================================================
namespace {
struct IsrChannelStub final : rr::InMemoryTransport {
    bool sendAlterIsr(const rr::AlterIsrMsg&) override { return true; }
};
rr::PartitionConfig isrFilterCfg() {
    rr::PartitionConfig c;
    c.topicId = 0; c.partitionId = 0; c.selfId = 1;
    c.replicaLagMaxMs = 10'000; c.replicaLagMaxOffset = 10'000; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000; c.isrCheckIntervalNs = 1'000'000; c.maxFetchBytes = 1 << 20;
    return c;
}
std::vector<uint8_t> isrAnswerFrame(int16_t code, int64_t controllerEpoch) {
    rr::AlterIsrResponseMsg r{};
    r.topicId = 0; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 4; r.partitionEpoch = 1;
    r.errorCode = code; r.controllerEpoch = controllerEpoch;
    std::vector<uint8_t> wire(rr::AlterIsrResponseMsg::TOTAL_SIZE);
    EXPECT_EQ(r.encode(wire.data(), wire.size()), wire.size());
    return wire;
}
}  // namespace

TEST(AeronTransportRegression, StaleControllerAnswerIsDroppedBeforeAnyRelayRegression) {
    TmpDir ad{"isr_stale"};
    EmbeddedDriverConfig dc;
    dc.dir = ad.path.string();
    dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rr::AeronPartitionTransport tr(client, "aeron:ipc", 7321, /*self*/1, 1 << 20);
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    IsrChannelStub chan;
    const std::vector<rr::NodeId> all{1, 2, 3};
    rr::PartitionReplica rep(isrFilterCfg(), rr::Role::Follower, 0, all, all, &log, &chan);
    rep.setIsrChannel(&chan);
    rep.becomeFollower(4, rr::NodeId{-1});
    ASSERT_TRUE(rep.leaderlessReportPendingForTest());
    tr.registerReplica(0, 0, &rep);
    tr.setControllerId(1, 7);

    auto stale = isrAnswerFrame(rr::kAlterIsrOk, 6);
    EXPECT_TRUE(tr.deliverFrameForTest(stale.data(), stale.size(), 1));
    EXPECT_TRUE(rep.leaderlessReportPendingForTest()) << "an answer of a deposed controller term cleared the report";
    EXPECT_EQ(tr.staleControllerAnswersForTest(), 1u);

    auto fresh = isrAnswerFrame(rr::kAlterIsrOk, 7);
    EXPECT_TRUE(tr.deliverFrameForTest(fresh.data(), fresh.size(), 1));
    EXPECT_FALSE(rep.leaderlessReportPendingForTest()) << "the current controller's answer must be delivered";
    EXPECT_EQ(tr.staleControllerAnswersForTest(), 1u);
}

TEST(AeronTransportRegression, NodeLocalCodeOnTheWireReachesNoReplicaRegression) {
    TmpDir ad{"isr_local"};
    EmbeddedDriverConfig dc;
    dc.dir = ad.path.string();
    dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rr::AeronPartitionTransport tr(client, "aeron:ipc", 7322, /*self*/1, 1 << 20);
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    IsrChannelStub chan;
    const std::vector<rr::NodeId> all{1, 2, 3};
    rr::PartitionReplica rep(isrFilterCfg(), rr::Role::Follower, 0, all, all, &log, &chan);
    rep.setIsrChannel(&chan);
    rep.becomeFollower(4, rr::NodeId{-1});
    ASSERT_TRUE(rep.leaderlessReportPendingForTest());
    tr.registerReplica(0, 0, &rep);
    tr.setControllerId(1, 7);

    // Code 101 / 100 with the CURRENT term (so the term filter passes them): only the code filter stops them,
    // and its counter proves they were dropped at the transport, before any relay.
    auto c101 = isrAnswerFrame(rr::kAlterIsrLocalHoldLeadership, 7);
    EXPECT_TRUE(tr.deliverFrameForTest(c101.data(), c101.size(), 1));
    EXPECT_TRUE(rep.leaderlessReportPendingForTest());
    EXPECT_EQ(rep.role(), rr::Role::Follower);
    EXPECT_EQ(tr.localCodeOnWireForTest(), 1u);
    auto c100 = isrAnswerFrame(rr::kAlterIsrLocalLineageIdentity, 7);
    EXPECT_TRUE(tr.deliverFrameForTest(c100.data(), c100.size(), 1));
    EXPECT_TRUE(rep.leaderlessReportPendingForTest());
    EXPECT_EQ(tr.localCodeOnWireForTest(), 2u);
}

// ISR protocol CP1 review I1: the PRODUCTION relay of the correlation ids, transport -> WorkerOp -> Worker.  Every user
// partition has a worker pool; dropping `op.roleLeaderId = r.sendSeq;` (transport) or `r.sendSeq = op.roleLeaderId();`
// (Worker) silently turns the A-C1 older-send rule off for all of them while every replica-level test stays green.
// The replica is driven to send #2 (60 ms after #1), then answered Ineligible for send 1 (older: must keep the
// proposal) and for send 2 (the newest: clears), through deliverFrameForTest -> WorkerPool -> Worker::doWork.
// RED (each separately): delete `op.roleLeaderId = r.sendSeq;`; delete `op.nextOffset = r.proposalId;` (id 0 -> the
// legacy rule clears on the seq-1 frame).
TEST(AeronTransportRegression, WorkerRelayCarriesProposalIdAndSendSeqRegression) {
    struct CapturingIsrChannel final : rr::InMemoryTransport {
        std::vector<rr::AlterIsrMsg> sent;
        bool sendAlterIsr(const rr::AlterIsrMsg& m) override { sent.push_back(m); return true; }
    };
    TmpDir ad{"isr_relay"};
    EmbeddedDriverConfig dc;
    dc.dir = ad.path.string();
    dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx;
    ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rr::AeronPartitionTransport tr(client, "aeron:ipc", 7323, /*self*/1, 1 << 20);

    TmpDir fenceDir{"isr_relay_fence"};
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingIsrChannel chan;
    const std::vector<rr::NodeId> all{1, 2, 3};
    rr::PartitionConfig cfg;
    cfg.topicId = 0; cfg.partitionId = 0; cfg.selfId = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000; cfg.minIsr = 1;
    cfg.fetchIntervalNs = 1'000'000; cfg.isrCheckIntervalNs = 1'000'000; cfg.maxFetchBytes = 1 << 20;
    auto rep = std::make_unique<rr::PartitionReplica>(cfg, rr::Role::Follower, 0, all, all, &log, &chan);
    rep->setIsrChannel(&chan);
    rep->setLeaderEpochFenceFile((fenceDir.path / "fence-0-0").string());
    rep->becomeLeader(5, {1, 2}, /*pe=*/7);
    ASSERT_EQ(rep->role(), rr::Role::Leader);
    for (int i = 0; i < 10; ++i) ASSERT_GE(rep->append(std::vector<uint8_t>(16, uint8_t(i))), 0);
    int64_t now = 1'000'000'000;
    rr::FetchRequestMsg f{};
    f.leaderEpoch = 5; f.partition = 0; f.followerId = 3; f.fetchOffset = 10; f.maxBytes = 0;
    f.topicId = 0; f.lastFetchedEpoch = 5; f.incarnation = 1;
    (void)rep->onFetchRequest(f, now);
    auto proposals = [&] { std::size_t n = 0; for (const auto& m : chan.sent) n += (m.isrCount > 0); return n; };
    for (int i = 0; i < 50 && proposals() == 0; ++i) rep->doWork(now += 1'000'000);
    ASSERT_EQ(proposals(), 1u);
    rep->doWork(now += 60'000'000);   // the 50 ms resend: send #2
    ASSERT_EQ(proposals(), 2u);
    ASSERT_EQ(chan.sent.back().sendSeq, 2);
    const int64_t id = rep->pendingProposalIdForTest();
    ASSERT_NE(id, 0);

    rune::node::ReplicaRegistry reg;
    reg.add(0, 0, std::move(rep));
    rune::node::WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 20, 1 << 20, 16, 16};
    pool.assignFromRegistry(reg);
    pool.setRegistry(&reg);
    pool.setRunnersStartedForTest(true);
    tr.setReplicaRegistry(&reg);
    tr.setWorkerPool(&pool);
    tr.setControllerId(1, 7);

    auto answer = [&](int32_t sendSeq) {
        rr::AlterIsrResponseMsg r{};
        r.topicId = 0; r.partition = 0; r.leaderId = 1; r.leaderEpoch = 5; r.partitionEpoch = 7;
        r.errorCode = rr::kAlterIsrIneligibleReplica; r.proposalId = id; r.controllerEpoch = 7; r.sendSeq = sendSeq;
        std::vector<uint8_t> wire(rr::AlterIsrResponseMsg::TOTAL_SIZE);
        ASSERT_EQ(r.encode(wire.data(), wire.size()), wire.size());
        ASSERT_TRUE(tr.deliverFrameForTest(wire.data(), wire.size(), now));
        pool.worker(0).doWork(now += 1'000'000);
    };
    rr::PartitionReplica* owned = reg.lookup(0, 0);
    ASSERT_NE(owned, nullptr);
    answer(1);   // the answer to the OLDER send
    EXPECT_TRUE(owned->isrProposalPendingForTest()) << "an older-send answer cleared the proposal through the relay";
    EXPECT_EQ(owned->olderSendAnswersForTest(), 1u);
    answer(2);   // the newest send's answer
    EXPECT_FALSE(owned->isrProposalPendingForTest());
}

// CP1 review I1 (term wiring): the controller's term must reach every node's transport (setControllerId's 2nd
// argument, NodeRuntimeAgent) and its MetadataService (setControllerEpoch), or the stale-term filter and the term
// stamp are dead in production while the hand-wired unit tests stay green.
// RED (each separately): the 1-argument setControllerId(lid) in NodeRuntimeAgent (known stays 0); delete the
// setControllerEpoch call (stays 0).
TEST(CatalogReplicationRegression, ControllerTermIsWiredIntoEveryTransportAndAnswerRegression) {
    ThreeNodes c = ThreeNodes::start("cat_term");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    // The term is re-sampled from the CURRENT controller on every iteration: a re-election mid-test moves it.
    auto controllerTerm = [&]() -> int64_t {
        for (BrokerNode* n : c.live()) {
            const int64_t t = n->onControlThreadForTest([&]() -> int64_t {
                return n->electionForTest()->leaderMemberId() == n->nodeIdForTest()
                           ? static_cast<int64_t>(n->electionForTest()->leadershipTermId()) : 0;
            });
            if (t > 0) return t;
        }
        return 0;
    };
    auto knownOf = [&](BrokerNode* n) { return n->onControlThreadForTest([&] { return n->replTransportForTest()->knownControllerTermForTest(); }); };
    auto stampOf = [&](BrokerNode* n) { return n->onControlThreadForTest([&] { return n->metadataServiceForTest()->controllerEpochForTest(); }); };
    int64_t T = 0;
    bool all = false;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline && !all) {
        T = controllerTerm();
        all = T > 0;
        for (BrokerNode* n : c.live()) all = all && knownOf(n) == T && stampOf(n) == T;
        if (!all) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(T, 0);
    for (BrokerNode* n : c.live()) {
        EXPECT_EQ(knownOf(n), T) << "node " << n->nodeIdForTest() << ": the transport never learned the controller term";
        EXPECT_EQ(stampOf(n), T) << "node " << n->nodeIdForTest() << ": the metadata service never stamps the controller term";
    }
}

// CP1 review M7: wipe() (a replaced disk, formatted --join) must clear a wipeRaw override, or the node would restart
// under the stale Auto/New format of an earlier wipeRaw.  RED: drop the reset in ThreeNodes::wipe.
TEST(CatalogReplicationRegression, WipeClearsAPriorWipeRawFormatOverrideRegression) {
    ThreeNodes c("wipe_override", Mode::Shard);
    c.wipeRaw(1, rune::node::StorageFormat::Auto);
    ASSERT_TRUE(c.hasStorageFormatOverrideForTest(1));
    c.wipe(1);
    EXPECT_FALSE(c.hasStorageFormatOverrideForTest(1)) << "wipe() kept the earlier wipeRaw format override";
}

// ISR v3 Step 6 (M10): each manifest class is recorded the first time it exists durably - consensus at boot, the
// internal-ISR knowledge after its first persist, the metadata log at its first committed apply.  Reading the file
// from the test thread is safe: nothing is shared in memory.
// RED: drop the knowledge hook call (the manifest never gets `knowledge`).
TEST(StorageIdentityRegression, MarkersSetOnFirstExistenceRegression) {
    ThreeNodes c = ThreeNodes::start("markers");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const auto manifestOf = [&]() {
        std::ifstream in(c.dirs[0]->path / "storage.meta");
        std::string line;
        while (std::getline(in, line)) if (line.rfind("manifest=", 0) == 0) return line;
        return std::string{};
    };
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    std::string m = manifestOf();
    const auto complete = [&] {
        return m.find("consensus") != std::string::npos && m.find("knowledge") != std::string::npos &&
               m.find("metadataLog") != std::string::npos;
    };
    while (!complete() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(50ms);
        m = manifestOf();
    }
    EXPECT_NE(m.find("consensus"), std::string::npos) << m;
    EXPECT_NE(m.find("knowledge"), std::string::npos) << m;
    EXPECT_NE(m.find("metadataLog"), std::string::npos) << m;
}

// ISR v3 Step 9 (R12, v2-review I2): the canvass flags are RECOMPUTED every control cycle - a long-running founder
// advertises history once it has led or stood (term >= 1), not only what it held at boot.
// RED: flags computed once at boot (2).
TEST(ElectionStorageRegression, ALongRunningFounderAdvertisesHistoryRegression) {
    ThreeNodes c = ThreeNodes::start("flags");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    for (BrokerNode* n : c.live()) {
        const int32_t flags = n->onControlThreadForTest([&] { return n->canvassFlagsForTest(); });
        EXPECT_EQ(flags, 3) << "node " << n->nodeIdForTest() << ": valid flags (2) + history (1) once a term >= 1 ran";
    }
}

// v2-review I3: a node whose data dir was wiped RAW (no storage.meta) and restarted with `auto` decides Joiner (it hears
// the established cluster), registers with the DECIDED origin, and the controller commits its storage.
// RED: drop the setStorage call in the `joined` branch (the register keeps origin 4 and is never processed).
TEST(StorageGateWiringRegression, AutoWipedNodeIsConfirmedAsJoinerRegression) {
    ThreeNodes c = ThreeNodes::start("autowipe");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int X = c.catalogLeaderId() == 1 ? 2 : 1;
    c.stop(X);
    c.wipeRaw(X, rune::node::StorageFormat::Auto);
    c.startNode(X);
    const auto metaOf = [&](const char* key) -> std::string {
        std::ifstream in(c.dirs[static_cast<std::size_t>(X - 1)]->path / "storage.meta");
        std::string line;
        const std::string p = std::string(key) + "=";
        while (std::getline(in, line)) if (line.rfind(p, 0) == 0) return line.substr(p.size());
        return {};
    };
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    bool ok = false;
    int64_t committed = 0, id = 0;
    while (!ok && std::chrono::steady_clock::now() < deadline) {
        if (metaOf("origin") == "joiner") {
            id = static_cast<int64_t>(std::stoull(metaOf("storageId"), nullptr, 16));
            const int l = c.catalogLeaderId();
            if (l > 0 && c.running(l)) {
                committed = c.node(l).onControlThreadForTest([&] {
                    return c.node(l).metadataServiceForTest()->msmForTest().nodeStorage(X);
                });
                ok = committed != 0 && committed == id;
            }
        }
        if (!ok) std::this_thread::sleep_for(50ms);
    }
    EXPECT_EQ(metaOf("origin"), "joiner") << "the auto-formatted wiped node did not decide Joiner";
    EXPECT_TRUE(ok) << "the controller never committed the joiner's storage (committed " << committed << ", id " << id << ")";
}
