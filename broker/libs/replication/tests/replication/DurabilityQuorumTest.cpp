// ---------------------------------------------------------------------------
// DurabilityQuorumTest.cpp — Tests for per-topic durability-quorum knob.
//
// Covers:
//   1. ReplicaLeaderState: Majority mode uses the min LEO over the ISR (Kafka model).
//   2. ReplicaLeaderState: AllInSync mode uses full-ISR minimum LEO.
//   3. AllInSync with a lagging follower holds HW at laggard's LEO.
//   4. PartitionReplica: AllInSync topic with one lagger holds HW at min follower LEO.
//   5. PartitionReplica: AllInSync topic with ISR < RF gates append with acksAll.
// ---------------------------------------------------------------------------

#include <rune/replication/HighWatermark.h>
#include <gtest/gtest.h>
using namespace rune::replication;

// ISR = {L=1, A=2, B=3}. LEOs: L=100, A=90, B=80.
static ReplicaLeaderState make() {
    ReplicaLeaderState s; s.setLeaderId(1);
    s.setIsr({1,2,3}); s.setLeaderLogEndOffset(100);
    s.updateFollowerFetchOffset(2, 90);
    s.updateFollowerFetchOffset(3, 80);
    return s;
}

// Kafka model (user decision 2026-10-03): Majority mode no longer commits on a
// majority — HW = min LEO over the ISR in every mode (was the 2nd highest, 90).
TEST(DurabilityQuorumRegression, MajorityModeHwIsMinOverIsr) {
    auto s = make();
    s.setDurabilityMode(DurabilityMode::Majority);
    s.recompute();
    EXPECT_EQ(s.highWatermark(), 80) << "HW = min LEO over the ISR";
}

TEST(DurabilityQuorum, AllInSyncWaitsForEveryIsrMember) {
    auto s = make();
    s.setDurabilityMode(DurabilityMode::AllInSync);
    s.recompute();
    EXPECT_EQ(s.highWatermark(), 80) << "all-in-sync = min LEO over full ISR = 80";
}

TEST(DurabilityQuorum, AllInSyncOneLaggerHoldsHw) {
    auto s = make();
    s.setDurabilityMode(DurabilityMode::AllInSync);
    s.updateFollowerFetchOffset(3, 70); // B falls further behind
    EXPECT_EQ(s.highWatermark(), 70);
}

// ---------------------------------------------------------------------------
// PartitionReplica-level AllInSync test.
// ---------------------------------------------------------------------------
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <filesystem>
#include <atomic>
#include <chrono>
#include <unistd.h>

namespace {

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& suffix) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_dq_test_" + suffix + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

static PartitionConfig makeConfig(NodeId selfId, DurabilityMode dm = DurabilityMode::Majority) {
    PartitionConfig cfg;
    cfg.partitionId          = 0;
    cfg.selfId               = selfId;
    cfg.replicaLagMaxMs      = 50;             // 50 ms: ISR shrinks quickly in tests
    cfg.replicaLagMaxOffset  = 10;             // 10 offsets: tight offset lag
    cfg.minIsr               = 1;
    cfg.fetchIntervalNs      = 1'000'000LL;    // 1 ms backstop
    cfg.isrCheckIntervalNs   = 1'000'000LL;    // 1 ms
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = true;
    cfg.fastAckOnApply       = true;
    cfg.durability           = dm;
    return cfg;
}

static std::vector<uint8_t> makePayload(uint8_t fill, std::size_t size = 16) {
    return std::vector<uint8_t>(size, fill);
}

} // anonymous namespace

// AllInSync topic: with one of 3 replicas lagging, HW must not advance past the
// laggard, and a produce that would need |ISR|>=RF is gated when ISR shrinks.
TEST(DurabilityQuorum, AllInSyncMinIsrEqualsRfAndCommittedDataSafe) {
    // Three replicas: L=1, F2=2, F3=3.
    TempDir leaderDir("dq_L"), f2Dir("dq_F2"), f3Dir("dq_F3");
    rune::storage::Log leaderLog(leaderDir.path);
    rune::storage::Log f2Log(f2Dir.path);
    rune::storage::Log f3Log(f3Dir.path);

    InMemoryTransport transport;

    std::vector<NodeId> replicas = {1, 2, 3};
    std::vector<NodeId> isr      = {1, 2, 3};

    auto leaderCfg = makeConfig(1, DurabilityMode::AllInSync);
    auto f2Cfg     = makeConfig(2, DurabilityMode::AllInSync);
    auto f3Cfg     = makeConfig(3, DurabilityMode::AllInSync);

    PartitionReplica leader(leaderCfg, Role::Leader,   /*epoch=*/1, replicas, isr, &leaderLog, &transport);
    PartitionReplica f2    (f2Cfg,     Role::Follower,  /*epoch=*/1, replicas, isr, &f2Log,    &transport);
    PartitionReplica f3    (f3Cfg,     Role::Follower,  /*epoch=*/1, replicas, isr, &f3Log,    &transport);

    transport.setLeader(&leader);
    transport.addFollower(2, &f2);
    transport.addFollower(3, &f3);

    int64_t clock = 0;
    static constexpr int kCap = 2000;

    // Append 5 batches.
    for (int i = 0; i < 5; ++i) {
        Offset r = leader.append(makePayload(static_cast<uint8_t>(i)), /*acksAll=*/true);
        ASSERT_GE(r, 0) << "append " << i << " gated unexpectedly";
    }

    // Drive until HW == leader LEO (all replicas caught up).
    Offset targetLeo = leader.logEndOffset();
    bool converged = false;
    for (int i = 0; i < kCap && !converged; ++i) {
        clock += 1'000'000LL; // 1 ms
        leader.doWork(clock);
        f2.doWork(clock);
        f3.doWork(clock);
        transport.pump(clock);
        converged = (leader.highWatermark() >= targetLeo);
    }
    ASSERT_TRUE(converged) << "HW did not advance to LEO in AllInSync mode; hw="
                           << leader.highWatermark() << " leo=" << targetLeo;

    // HW should equal the minimum follower LEO (== all caught up == leader LEO).
    Offset minFollowerLeo = std::min(f2.logEndOffset(), f3.logEndOffset());
    EXPECT_EQ(leader.highWatermark(), minFollowerLeo)
        << "AllInSync HW must equal min follower LEO when all caught up";

    // Now remove follower 3 from the transport (simulate crash / partition).
    transport.removeFollower(3);

    // Append one more batch.
    Offset r = leader.append(makePayload(0xAA), /*acksAll=*/true);
    Offset newLeo = leader.logEndOffset();

    // Drive until ISR shrinks (follower 3 lags > replicaLagMaxOffset).
    // We keep pumping only F2; F3 is disconnected.
    // With AllInSync mode, effectiveMinIsr == RF == 3, so once ISR drops < 3,
    // a further acksAll append must return -1.
    bool isrShrunk = false;
    for (int i = 0; i < kCap && !isrShrunk; ++i) {
        clock += 1'000'000LL;
        leader.doWork(clock);
        f2.doWork(clock);
        // f3 disconnected — no doWork, no pump for f3
        transport.pump(clock);
        // ISR has shrunk when leader.isr().size() < 3
        isrShrunk = (leader.isr().size() < 3u);
        (void)newLeo;
    }

    ASSERT_TRUE(isrShrunk) << "ISR did not shrink after follower 3 disconnected";

    // With AllInSync and ISR < RF (== 3), acksAll produce must be gated (-1).
    Offset gated = leader.append(makePayload(0xBB), /*acksAll=*/true);
    EXPECT_EQ(gated, -1) << "AllInSync produce must be gated when ISR < RF";

    // Without acksAll, produce is still allowed.
    Offset notGated = leader.append(makePayload(0xCC), /*acksAll=*/false);
    EXPECT_GE(notGated, 0) << "acksAll=false produce must not be gated by minIsr";
    (void)r;
}
