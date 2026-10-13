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


// D15 review m5: a MEMORY-backed replica's restart fence is written by the
// owning worker's async epoch-state writer (leader-epoch-state in its own
// dir), never by a synchronous fsync on the worker: a new-epoch leadership is
// DEFERRED until the slot carrying the fence completed (same durability-
// before-acting ordering as the disk path), and the fence survives a restart.
// A legacy plain fence file (pre-D15 layout) is migrated into it.
#include <fcntl.h>
TEST(LeaderEpochFenceRegression, MemoryFenceWrittenByTheAsyncEpochStateWriterRegression) {
    Dir d;
    const std::string stateDir = (d.path / "0-0.state").string();
    const std::string legacy = (d.path / "0-0").string();
    {
        std::FILE* fp = std::fopen(legacy.c_str(), "w");
        ASSERT_NE(fp, nullptr);
        std::fputs("4\n", fp);
        std::fclose(fp);
    }
    {
        rune::storage::InMemoryLog log(1 << 20, 10'000);
        CapturingTransport t;
        auto r = memReplica(log, t);
        r->setExternalFollowerFlush(true);   // worker-driven (every user replica)
        r->setMemoryEpochStateDir(stateDir, legacy);
        EXPECT_EQ(r->restartFenceEpochForTest(), 4) << "the legacy fence file was not migrated";
        const auto inlineBefore = r->epochStateInlineWritesForTest();
        r->becomeLeader(5, kAll);
        EXPECT_NE(r->role(), Role::Leader) << "led before its fence was durable";
        EXPECT_TRUE(r->leadershipPendingForTest()) << "the memory fence was not deferred to the async writer";
        EXPECT_EQ(r->epochStateInlineWritesForTest(), inlineBefore) << "synchronous fence write on the worker";
        // The owning worker's io backend.
        std::vector<uint8_t> buf(64 * 1024);
        PartitionReplica::EpochStateWrite w;
        std::size_t need = 0;
        ASSERT_TRUE(r->prepareEpochStateWrite(buf.data(), buf.size(), w, need));
        r->epochStateWriteSubmitted();
        ASSERT_EQ(::pwrite(w.fd, w.data, w.len, w.offset), static_cast<ssize_t>(w.len));
        ASSERT_EQ(::fsync(w.fd), 0);
        (void)r->epochStateWriteCompleted(w.seq, 0);
        EXPECT_EQ(r->role(), Role::Leader) << "the durable fence never completed the leadership";
    }
    EXPECT_FALSE(fs::exists(legacy)) << "the migrated legacy fence file is still trusted next to the state";
    rune::storage::InMemoryLog log(1 << 20, 10'000);   // restarted: empty
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setExternalFollowerFlush(true);
    r->setMemoryEpochStateDir(stateDir, legacy);
    EXPECT_EQ(r->restartFenceEpochForTest(), 5);
    r->becomeLeader(5, kAll);
    EXPECT_NE(r->role(), Role::Leader) << "re-led epoch 5 after a restart";
    EXPECT_TRUE(r->staleLeadershipPendingForTest());
}

// D23 (D21 review L4): a leader-epoch-state file left SPARSE by an earlier
// build (slot 1 a hole) is reserved when the replica enables it (cold,
// construction) — the worker's first in-place write of slot 1 must never
// allocate a block (ENOSPC on a full disk).  64 MiB: sparse on every fs.
#include <rune/storage/LeaderEpochState.h>
TEST(LeaderEpochFenceRegression, SparseEpochStateFileIsReservedOnEnableRegression) {
    using ES = rune::storage::LeaderEpochState;
    Dir d;
    const fs::path logDir = d.path / "log";
    fs::create_directories(logDir);
    const std::size_t stride = std::size_t{32} << 20;
    {   // the pre-D23 layout: size set, slot 0's image only
        const std::vector<std::pair<int32_t, int64_t>> e;   // matches the empty log: no write at enable
        std::vector<uint8_t> img(ES::imageBytes(e.size()));
        const std::size_t len = ES::encode(img.data(), 1, 1, e.begin(), e.end());
        const int fd = ::open(ES::path(logDir.string()).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(2 * stride)), 0);
        ASSERT_EQ(::pwrite(fd, img.data(), len, 0), static_cast<ssize_t>(len));
        ASSERT_EQ(::lseek(fd, static_cast<off_t>(stride), SEEK_HOLE), static_cast<off_t>(stride))
            << "precondition: slot 1 is a hole";
        ::close(fd);
    }
    rune::storage::Log log(logDir.string());
    log.recover();
    CapturingTransport t;
    auto r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
    r->setIsrChannel(&t);
    ASSERT_TRUE(r->epochStateEnabledForTest());
    ASSERT_EQ(r->epochStateInlineWritesForTest(), 0u) << "precondition: enable wrote no slot";
    const int fd = ::open(ES::path(logDir.string()).c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    const off_t hole = ::lseek(fd, static_cast<off_t>(stride), SEEK_HOLE);
    ::close(fd);
    EXPECT_GE(hole, static_cast<off_t>(stride + 4096)) << "slot 1's head is still a hole after enable";
    EXPECT_GE(r->epochStateReservedForTest(), 4096u);
    // The in-place write that follows fits the reserved head (no re-create).
    const uint64_t recreates = r->epochStateRecreatesForTest();
    EXPECT_TRUE(r->writeEpochStateInlineForTest());
    EXPECT_EQ(r->epochStateRecreatesForTest(), recreates);
}

// Lows A/B/C review M2: D23 made the inline epoch-state re-create (tmp write +
// fsync + rename + dir fsync: milliseconds) reachable from the WORKER duty
// pass (prepareEpochStateWrite) whenever an image outgrew the reserved head —
// and at ONCE when reserving failed at enable (reserved = 0: ENOSPC / EIO at
// startup, the very disk-full case D23 L4 targets).  Fix: the worker re-creates
// only when the image outgrows the SLOT (> 4096 entries, the pre-D23
// threshold); past the reserved head it writes in place, async, through the
// io backend.  Hot-path budget: prepareEpochStateWrite on the duty pass stays
// an encode into the caller's buffer (no syscall beyond the lazy open) — well
// under the 100 us pass budget; the ms re-create is off the common path.
TEST(LeaderEpochFenceRegression, FailedReserveAtEnableNeverRecreatesOnTheWorkerPassRegression) {
    Dir d;
    const std::string stateDir = (d.path / "0-0.state").string();
    const std::string legacy = (d.path / "0-0").string();
    {   // first incarnation: creates the state file
        rune::storage::InMemoryLog log(1 << 20, 10'000);
        CapturingTransport t;
        auto r = memReplica(log, t);
        r->setExternalFollowerFlush(true);
        r->setMemoryEpochStateDir(stateDir, legacy);
        ASSERT_TRUE(r->epochStateEnabledForTest());
    }
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setExternalFollowerFlush(true);
    PartitionReplica::failEpochStateReserveForTest(ENOSPC);   // the disk is full at startup
    r->setMemoryEpochStateDir(stateDir, legacy);
    PartitionReplica::failEpochStateReserveForTest(0);
    ASSERT_TRUE(r->epochStateEnabledForTest());
    ASSERT_EQ(r->epochStateReservedForTest(), 0u) << "precondition: the reservation failed";
    r->becomeLeader(7, kAll);
    ASSERT_TRUE(r->leadershipPendingForTest());
    const uint64_t recreates = r->epochStateRecreatesForTest();
    const uint64_t inlineWrites = r->epochStateInlineWritesForTest();
    std::vector<uint8_t> buf(64 * 1024);
    PartitionReplica::EpochStateWrite w;
    std::size_t need = 0;
    // The owning worker's duty pass.
    ASSERT_TRUE(r->prepareEpochStateWrite(buf.data(), buf.size(), w, need))
        << "the worker pass did not get an async in-place write";
    EXPECT_EQ(r->epochStateRecreatesForTest(), recreates) << "the worker pass re-created the file (fsync x2)";
    EXPECT_EQ(r->epochStateInlineWritesForTest(), inlineWrites) << "synchronous epoch-state write on the worker";
    r->epochStateWriteSubmitted();
    ASSERT_EQ(::pwrite(w.fd, w.data, w.len, w.offset), static_cast<ssize_t>(w.len));
    (void)r->epochStateWriteCompleted(w.seq, 0);
    EXPECT_EQ(r->role(), Role::Leader);
}

// Lows A/B/C review M2: a FAILED re-create on the worker pass (the history
// outgrew the slot, the disk is full / the dir gone) left the replica due with
// no backoff: open(O_TRUNC) + pwrite + fsync retried on EVERY pass.  It now
// fails through the async failure path: backoff (retryNotBeforeNs), counted,
// logged, fail-closed for a pending leadership.
TEST(LeaderEpochFenceRegression, FailedWorkerRecreateBacksOffRegression) {
    using ES = rune::storage::LeaderEpochState;
    Dir d;
    const std::string stateDir = (d.path / "0-0.state").string();
    const std::string legacy = (d.path / "0-0").string();
    fs::create_directories(stateDir);
    {   // a valid file with ONE-block slots: ~338 entries before the slot is outgrown
        const std::vector<std::pair<int32_t, int64_t>> e;
        const int fd = ES::create(stateDir, 4096, 1, 0, e.begin(), e.end());
        ASSERT_GE(fd, 0);
        ::close(fd);
    }
    rune::storage::InMemoryLog log(1 << 20, 100'000);
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setExternalFollowerFlush(true);
    r->setMemoryEpochStateDir(stateDir, legacy);
    ASSERT_TRUE(r->epochStateEnabledForTest());
    std::vector<uint8_t> buf(64 * 1024);
    const std::vector<uint8_t> rec(64, 0x5a);
    int64_t now = 1'000'000'000;
    Epoch e = 1;
    bool outgrown = false;
    for (; e < 1000 && !outgrown; ++e) {
        r->doWork(now);
        r->becomeLeader(e, kAll);
        ASSERT_TRUE(r->leadershipPendingForTest()) << "epoch " << e;
        PartitionReplica::EpochStateWrite w;
        std::size_t need = 0;
        if (ES::imageBytes(r->leaderEpochCacheForTest().size() + 1) > 4096) {
            outgrown = true;
            break;
        }
        ASSERT_TRUE(r->prepareEpochStateWrite(buf.data(), buf.size(), w, need)) << "epoch " << e;
        r->epochStateWriteSubmitted();
        ASSERT_EQ(::pwrite(w.fd, w.data, w.len, w.offset), static_cast<ssize_t>(w.len));
        (void)r->epochStateWriteCompleted(w.seq, 0);
        ASSERT_EQ(r->role(), Role::Leader) << "epoch " << e;
        ASSERT_GE(r->append(rec, false), 0);   // the next epoch starts later (a new entry)
    }
    ASSERT_TRUE(outgrown) << "precondition: the history outgrew the slot";
    fs::remove_all(stateDir);   // the re-create fails (the dir is gone)
    const uint64_t recreates = r->epochStateRecreatesForTest();
    PartitionReplica::EpochStateWrite w;
    std::size_t need = 0;
    EXPECT_FALSE(r->prepareEpochStateWrite(buf.data(), buf.size(), w, need));
    EXPECT_EQ(r->epochStateRecreatesForTest(), recreates + 1) << "precondition: the worker pass tried the re-create";
    const uint64_t errors = r->epochStateErrorsForTest();
    // The next passes (same time) must not retry it.
    for (int i = 0; i < 5; ++i) {
        r->doWork(now);
        EXPECT_FALSE(r->prepareEpochStateWrite(buf.data(), buf.size(), w, need));
    }
    EXPECT_EQ(r->epochStateRecreatesForTest(), recreates + 1)
        << "a failed re-create was retried on every pass (no backoff)";
    EXPECT_EQ(r->epochStateErrorsForTest(), errors) << "the failed write was retried within its backoff";
    EXPECT_FALSE(r->leadershipPendingForTest()) << "a failed fence write must fail closed";
    EXPECT_FALSE(r->epochStateDurable()) << "the change was dropped (must stay due)";
    // After the backoff it is retried (still due, never dropped).
    r->doWork(now + 1'000'000'000);
    (void)r->prepareEpochStateWrite(buf.data(), buf.size(), w, need);
    EXPECT_GT(r->epochStateErrorsForTest(), errors) << "never retried after the backoff";
}

// ===========================================================================
// E2 final fix wave (C1 multi-node, USER DECISION option b): a TRANSIENT
// leader reports, for its leadership, the leader epoch its ring's data came
// from — -1 when the ring is unborn (a process start: a full-cluster restart,
// the first leadership); its leader's epoch when it was promoted from a
// follower that fetched from that leader (ISR handover / rolling restart).  The
// report is re-sent until the controller confirms it committed.  A disk
// replica never reports.  RED: no lineage report was ever sent.
// ===========================================================================
namespace {
std::vector<AlterIsrMsg> lineageReports(const CapturingTransport& t) {
    std::vector<AlterIsrMsg> out;
    for (const auto& m : t.sent)
        if (m.isrCount == AlterIsrMsg::kLeaderEpochBump && (m.isr[1] & AlterIsrMsg::kLineageReport) != 0)
            out.push_back(m);
    return out;
}
AlterIsrResponseMsg lineageAck(int16_t code, Epoch epoch) {
    AlterIsrResponseMsg a{};
    a.topicId = 0; a.partition = 0; a.leaderId = 1; a.leaderEpoch = epoch; a.partitionEpoch = 0; a.errorCode = code;
    return a;
}
}  // namespace

TEST(LeaderEpochFenceRegression, UnbornTransientLeaderReportsANewLineageUntilConfirmedRegression) {
    Dir d;
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setLeaderEpochFenceFile((d.path / "fence-0-0").string());
    r->becomeLeader(5, kAll);
    ASSERT_EQ(r->role(), Role::Leader);
    int64_t now = 1'000'000'000;
    r->doWork(now);
    auto reps = lineageReports(t);
    ASSERT_EQ(reps.size(), 1u) << "an unborn transient leader sent no lineage report";
    EXPECT_EQ(reps[0].leaderEpoch, 5);
    EXPECT_EQ(reps[0].leaderId, 1);
    EXPECT_EQ(reps[0].isr[2], -1) << "an unborn ring holds no lineage data";
    r->doWork(now + 1'000'000);
    EXPECT_EQ(lineageReports(t).size(), 1u) << "re-sent faster than the AlterIsr cadence";
    now += PartitionReplica::kAlterIsrResendNsForTest + 1;
    r->doWork(now);
    EXPECT_EQ(lineageReports(t).size(), 2u) << "never re-sent while unconfirmed";
    r->onAlterIsrResponse(lineageAck(kAlterIsrLineageConfirmed, 5));
    EXPECT_FALSE(r->lineageReportPendingForTest());
    now += 10 * PartitionReplica::kAlterIsrResendNsForTest;
    r->doWork(now);
    EXPECT_EQ(lineageReports(t).size(), 2u) << "re-sent after the controller confirmed it";
}

TEST(LeaderEpochFenceRegression, PromotedTransientFollowerReportsItsLeadersEpochRegression) {
    Dir d;
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setLeaderEpochFenceFile((d.path / "fence-0-0").string());
    r->becomeFollower(7, /*leaderId=*/2);
    EXPECT_EQ(r->lineageSrcEpochForTest(), -1) << "nothing fetched yet: unborn";
    FetchResponseMsg resp{};
    resp.leaderEpoch = 7; resp.partition = 0; resp.highWatermark = 0; resp.logStartOffset = 0;
    resp.errorCode = kErrNone; resp.topicId = 0; resp.records = {};
    r->applyReplicated(resp);
    EXPECT_EQ(r->lineageSrcEpochForTest(), 7) << "the follower did not adopt its leader's lineage";
    FetchResponseMsg stale = resp;
    stale.leaderEpoch = 6;
    r->applyReplicated(stale);
    EXPECT_EQ(r->lineageSrcEpochForTest(), 7) << "a deposed leader's frame moved the lineage source";
    r->becomeLeader(8, kAll);   // promoted (ISR handover / rolling restart)
    ASSERT_EQ(r->role(), Role::Leader);
    r->doWork(1'000'000'000);
    const auto reps = lineageReports(t);
    ASSERT_EQ(reps.size(), 1u);
    EXPECT_EQ(reps[0].leaderEpoch, 8);
    EXPECT_EQ(reps[0].isr[2], 7) << "the promoted leader must report the epoch its data came from";
    // A fenced answer (no longer this leadership) stops it too.
    r->onAlterIsrResponse(lineageAck(kAlterIsrFencedLeaderEpoch, 8));
    EXPECT_FALSE(r->lineageReportPendingForTest());
}

TEST(LeaderEpochFenceRegression, DiskReplicaNeverSendsALineageReportRegression) {
    Dir d;
    const fs::path logDir = d.path / "log";
    fs::create_directories(logDir);
    rune::storage::Log log(logDir.string());
    log.recover();
    CapturingTransport t;
    auto r = std::make_unique<PartitionReplica>(cfg(), Role::Follower, 0, kAll, kAll, &log, &t);
    r->setIsrChannel(&t);
    r->setLeaderEpochFenceFile((d.path / "fence").string());
    r->becomeLeader(5, kAll);
    r->doWork(1'000'000'000);
    EXPECT_TRUE(lineageReports(t).empty()) << "a disk partition has no data lineage";
}

// E2 final fix wave, residual round 5 — R2: the process identity ROTATED.  A
// transient LEADER whose lineage was already confirmed re-reports it under the
// new identity, with its own epoch as the data source (same leadership, same
// data: the controller keeps the lineage); a follower only adopts it (its
// next leadership reports under it).  RED (identity fixed at add): no
// re-report — the controller's new identity never matches the record.
TEST(LeaderEpochFenceRegression, RotatedIdentityIsReportedAgainByATransientLeaderRegression) {
    Dir d;
    rune::storage::InMemoryLog log(1 << 20, 10'000);
    CapturingTransport t;
    auto r = memReplica(log, t);
    r->setLineageIdentity(0x0B0B);
    r->setLeaderEpochFenceFile((d.path / "fence-0-0").string());
    r->becomeLeader(5, kAll);
    int64_t now = 1'000'000'000;
    r->doWork(now);
    r->onAlterIsrResponse(lineageAck(kAlterIsrLineageConfirmed, 5));
    ASSERT_FALSE(r->lineageReportPendingForTest());
    const std::size_t before = lineageReports(t).size();

    r->rotateLineageIdentity(0x0C0C);
    EXPECT_EQ(r->lineageIdentityForTest(), 0x0C0C);
    EXPECT_TRUE(r->lineageReportPendingForTest()) << "the confirmed leader did not re-arm its report";
    r->doWork(now += 1'000'000);
    const auto reps = lineageReports(t);
    ASSERT_EQ(reps.size(), before + 1) << "no lineage report under the rotated identity";
    const auto& m = reps.back();
    const int64_t id = static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(m.isr[4])) << 32) |
                                            static_cast<uint32_t>(m.isr[3]));
    EXPECT_EQ(id, 0x0C0C) << "the re-report carried the OLD identity";
    EXPECT_EQ(m.leaderEpoch, 5);
    EXPECT_EQ(m.isr[2], 5) << "the data source is this leadership (the controller keeps the lineage)";
    r->onAlterIsrResponse(lineageAck(kAlterIsrLineageConfirmed, 5));
    EXPECT_FALSE(r->lineageReportPendingForTest());
    r->rotateLineageIdentity(0x0C0C);
    EXPECT_FALSE(r->lineageReportPendingForTest()) << "an unchanged identity re-armed the report";

    // A follower adopts it without reporting.
    rune::storage::InMemoryLog log2(1 << 20, 10'000);
    CapturingTransport t2;
    auto f = memReplica(log2, t2);
    f->becomeFollower(7, 2);
    f->rotateLineageIdentity(0x0D0D);
    EXPECT_EQ(f->lineageIdentityForTest(), 0x0D0D);
    EXPECT_FALSE(f->lineageReportPendingForTest());
}
