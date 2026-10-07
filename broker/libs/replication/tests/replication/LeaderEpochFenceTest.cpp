// LeaderEpochFenceTest.cpp — the PartitionReplica leader-epoch restart fence
// (epoch-restart fix + fix round 1, 2026-10-05).  A controller-managed replica
// never leads at an epoch an earlier incarnation of it led at, never leads
// without a DURABLE fence, and asks the controller for a new epoch instead.
#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {

struct Dir {
    fs::path path;
    Dir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               ("rune_fence_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~Dir() { std::error_code ec; fs::remove_all(path, ec); }
};

/// Captures the AlterIsr frames (the leader-epoch bump requests).
struct CapturingTransport final : InMemoryTransport {
    std::vector<AlterIsrMsg> sent;
    bool sendAlterIsr(const AlterIsrMsg& m) override { sent.push_back(m); return true; }
};

PartitionConfig cfg() {
    PartitionConfig c;
    c.topicId = 0; c.partitionId = 0; c.selfId = 1;
    c.replicaLagMaxMs = 10'000; c.replicaLagMaxOffset = 10'000; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000; c.isrCheckIntervalNs = 1'000'000; c.maxFetchBytes = 1 << 20;
    return c;
}

const std::vector<NodeId> kAll{1, 2, 3};

std::unique_ptr<PartitionReplica> memReplica(rune::storage::InMemoryLog& log, CapturingTransport& t) {
    auto r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
    r->setIsrChannel(&t);
    return r;
}

} // namespace

// M4.3: a MEMORY-backed replica (its records do not survive a restart) still
// never re-leads an epoch it led in an earlier incarnation — the fence file
// under the data dir does.
TEST(LeaderEpochFenceRegression, MemoryBackedFenceSurvivesRestartRegression) {
    Dir d;
    const std::string fence = (d.path / "fence-0-0").string();
    {
        rune::storage::InMemoryLog log(1 << 20, 10'000);
        CapturingTransport t;
        auto r = memReplica(log, t);
        r->setLeaderEpochFenceFile(fence);
        r->becomeLeader(5, kAll);
        ASSERT_EQ(r->role(), Role::Leader) << "a fresh replica leads at a new epoch";
    }
    rune::storage::InMemoryLog log(1 << 20, 10'000);   // restarted: empty
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setLeaderEpochFenceFile(fence);
    r->becomeLeader(5, kAll);
    EXPECT_NE(r->role(), Role::Leader) << "re-led epoch 5 after a restart";
    EXPECT_FALSE(r->acceptsProduce());
    r->doWork(1'000'000'000);
    ASSERT_EQ(t.sent.size(), 1u) << "no leader-epoch bump requested";
    EXPECT_EQ(t.sent[0].isrCount, AlterIsrMsg::kLeaderEpochBump);
    EXPECT_EQ(t.sent[0].isr[0], 5) << "the request carries the fence (L2)";
    EXPECT_NE(t.sent[0].isr[1] & AlterIsrMsg::kBumpMemoryBacked, 0) << "memory-backed flag missing";
    r->becomeLeader(6, kAll);
    EXPECT_EQ(r->role(), Role::Leader) << "a new epoch past the fence must be accepted";
}

// M3: no fence file at all (memory log, no data dir): the first leadership of
// the incarnation is refused (earlier epochs unknown) and the next granted
// epoch is accepted.
TEST(LeaderEpochFenceRegression, NoFenceFileRefusesFirstLeadershipRegression) {
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->becomeLeader(3, kAll);
    EXPECT_NE(r->role(), Role::Leader) << "led without any durable fence";
    r->doWork(1'000'000'000);
    ASSERT_EQ(t.sent.size(), 1u);
    r->becomeLeader(4, kAll);
    EXPECT_EQ(r->role(), Role::Leader);
    r->becomeLeader(9, kAll);   // a later election in the same incarnation
    EXPECT_EQ(r->role(), Role::Leader);
    EXPECT_EQ(r->epoch(), 9);
}

// M3 + fix round 2 N1 (KIP-112): the fence cannot be written — the log dir
// goes OFFLINE for the rest of the incarnation (sticky): the leadership is
// refused, the bump request says so (kBumpLogDirFailed), later grants are
// refused WITHOUT retrying the disk, and as a follower of the leaderless
// partition it reports itself once.  A new incarnation with a healthy dir leads.
TEST(LeaderEpochFenceRegression, FenceWriteFailureTakesLogDirOfflineStickyRegression) {
    Dir d;
    const fs::path logDir = d.path / "log";
    fs::create_directories(logDir);
    {
        rune::storage::Log log(logDir.string());
        log.recover();
        CapturingTransport t;
        auto r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
        r->setIsrChannel(&t);
        r->setLeaderEpochFenceFile((d.path / "missing-dir" / "fence").string());
        r->becomeLeader(2, kAll);
        EXPECT_NE(r->role(), Role::Leader) << "led although the fence could not be persisted";
        EXPECT_TRUE(r->logDirOfflineForTest());
        r->doWork(1'000'000'000);
        ASSERT_EQ(t.sent.size(), 1u) << "no new epoch requested after the refusal";
        EXPECT_NE(t.sent[0].isr[1] & AlterIsrMsg::kBumpLogDirFailed, 0) << "log-dir failure not reported";
        r->setLeaderEpochFenceFile((logDir / "leader-epoch-fence").string());   // the disk "heals"
        r->becomeLeader(3, kAll);
        EXPECT_NE(r->role(), Role::Leader) << "an offline log dir was re-tried within the incarnation";
        r->becomeFollower(4, NodeId{-1});   // the controller made it leaderless
        EXPECT_TRUE(r->leaderlessReportPendingForTest());
        r->doWork(2'000'000'000);
        ASSERT_EQ(t.sent.size(), 2u);
        EXPECT_NE(t.sent[1].isr[1] & AlterIsrMsg::kBumpFollowerReport, 0);
        EXPECT_NE(t.sent[1].isr[1] & AlterIsrMsg::kBumpLogDirFailed, 0);
        AlterIsrResponseMsg ack{};
        ack.topicId = 0; ack.partition = 0; ack.leaderId = 1; ack.leaderEpoch = 4;
        r->onAlterIsrResponse(ack);
        EXPECT_FALSE(r->leaderlessReportPendingForTest()) << "the answered report keeps being re-sent";
    }
    rune::storage::Log log(logDir.string());   // new incarnation, healthy dir
    log.recover();
    CapturingTransport t;
    auto r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
    r->setIsrChannel(&t);
    r->becomeLeader(5, kAll);
    EXPECT_EQ(r->role(), Role::Leader);
}

