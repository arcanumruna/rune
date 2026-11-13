// ---------------------------------------------------------------------------
// SegmentRepairTest.cpp — #880: a damaged SEALED segment is rebuilt from an in-sync peer.
//
// A real Log + PartitionReplica per node over the in-memory transport, an injected clock, bounded driver loops.  Three
// nodes hold byte-identical logs; the test damages one or two copies on disk (as the #839 tests do) and reopens them.
// ---------------------------------------------------------------------------
#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::replication;
namespace fs = std::filesystem;
using rune::protocol::RecordBatchBuilder;
using rune::protocol::RecordBatchReader;

namespace {

constexpr int64_t kRoll = 2048;                  // a few dozen small batches per segment
constexpr int64_t kStep = 100'000'000LL;         // 100 ms of injected clock per driver step

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& tag) {
        static std::atomic<uint64_t> seq{0};
        path = (fs::temp_directory_path() / ("rune_880_" + tag + "_" + std::to_string(::getpid()) + "_" +
                                              std::to_string(seq.fetch_add(1)))).string();
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

PartitionConfig cfgFor(NodeId self, int64_t lagMaxMs = 600'000, int32_t maxFetch = 4096) {
    PartitionConfig c;
    c.partitionId         = 0;
    c.selfId              = self;
    c.replicaLagMaxMs     = lagMaxMs;   // default: no lag eviction (the injected clock runs fast)
    c.replicaLagMaxOffset = 1'000'000;
    c.minIsr              = 1;
    c.fetchIntervalNs     = 1'000'000LL;
    c.isrCheckIntervalNs  = 1'000'000LL;
    c.maxFetchBytes       = maxFetch;
    c.pushOnAppend        = false;
    return c;
}

std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream in(p, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(in)), {});
}

std::string firstSegment(const std::string& dir) {
    std::string seg;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ".log" && (seg.empty() || e.path().string() < seg)) seg = e.path().string();
    return seg;
}

// Damage the 2nd batch of the first segment; returns the offset of that batch (the damage offset).
int64_t damageFirstSegment(const std::string& dir) {
    const std::string seg = firstSegment(dir);
    auto buf = slurp(seg);
    RecordBatchReader first(buf.data(), buf.size());
    const std::size_t second = static_cast<std::size_t>(first.totalLength());
    const int64_t d = RecordBatchReader(buf.data() + second, buf.size() - second).baseOffset();
    const std::size_t at = second + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
    const char c = static_cast<char>(buf[at] ^ 0x5A);
    f.seekp(static_cast<std::streamoff>(at));
    f.write(&c, 1);
    return d;
}

FetchRequestMsg ackOf(NodeId follower, Offset leo) {
    FetchRequestMsg r{};
    r.leaderEpoch = 1;
    r.partition   = 0;
    r.followerId  = follower;
    r.fetchOffset = leo;
    r.maxBytes    = 0;
    return r;
}

// Records every repair message and can drop either direction.
class SpyTransport : public InMemoryTransport {
public:
    std::vector<FetchRequestMsg>  repairReqs;
    std::vector<NodeId>           repairReqTo;
    int  responses = 0;
    std::map<NodeId, int> dataFetchesFrom;     // ... of which asked for records (maxBytes > 0)
    bool dropRequests = false;
    bool dropFetches = false;   // follower fetches never reach the leader: its HW cannot move
    std::set<NodeId> dropFetchesFrom;          // ... only those of these followers
    std::map<NodeId, int> fetchesFrom;         // every fetch frame a follower handed to the transport
    int64_t frameLimit = std::numeric_limits<int64_t>::max();   // maxFetchRecordsBytes()
    std::vector<int16_t> statuses;
    [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override { return frameLimit; }
    bool sendFetchRequest(const FetchRequestMsg& req) override {
        ++fetchesFrom[req.followerId];
        if (req.maxBytes > 0) ++dataFetchesFrom[req.followerId];
        if (dropFetches || dropFetchesFrom.count(req.followerId) != 0) return true;
        return InMemoryTransport::sendFetchRequest(req);
    }
    bool sendRepairRequest(NodeId to, const FetchRequestMsg& req) override {
        repairReqs.push_back(req);
        repairReqTo.push_back(to);
        if (dropRequests) return true;   // "sent", never delivered
        return InMemoryTransport::sendRepairRequest(to, req);
    }
    bool sendRepairResponse(NodeId to, const FetchResponseMsg& resp) override {
        ++responses;
        statuses.push_back(resp.errorCode);
        return InMemoryTransport::sendRepairResponse(to, resp);
    }
};

// Nodes 1 (leader, epoch 1), 2 and 3 (followers) over byte-identical logs of >= 4 segments.
struct Rig {
    TempDir dirs[3] = {TempDir("n1"), TempDir("n2"), TempDir("n3")};
    SpyTransport transport;
    std::unique_ptr<rune::storage::Log> logs[3];
    std::unique_ptr<PartitionReplica> rep[3];
    std::vector<uint8_t> scratch = std::vector<uint8_t>(64 * 1024);
    int64_t clock = 0;
    int64_t lagMaxMs = 600'000;       // the replica lag window of every node
    int32_t maxFetch = 4096;
    int64_t leo = 0;
    int64_t D = -1;                 // the damage offset (2nd batch of segment 0)
    std::vector<int64_t> bases;
    std::vector<uint8_t> intact;    // segment 0 before the damage

    void seed() {
        {
            rune::storage::Log log(dirs[0].path, kRoll);
            log.recover();
            log.setDeferSealSync(false);
            log.setEpochHistoryCap(std::numeric_limits<int64_t>::max());
            const std::vector<NodeId> solo{1};
            PartitionReplica l(cfgFor(1), Role::Leader, 1, solo, solo, &log, &transport);
            const std::vector<uint8_t> small(64, 0x51);
            for (int i = 0; i < 600 && log.segmentBaseOffsets().size() < 4; ++i) ASSERT_GE(l.append(small), 0);
            ASSERT_GE(l.append(small), 0);
            log.sync();
            bases = log.segmentBaseOffsets();
            leo = log.logEndOffset();
        }   // clean shutdown
        ASSERT_GE(bases.size(), 4u);
        for (int n = 1; n < 3; ++n)
            for (const auto& e : fs::directory_iterator(dirs[0].path))
                fs::copy(e.path(), fs::path(dirs[n].path) / e.path().filename(), fs::copy_options::overwrite_existing);
        intact = slurp(firstSegment(dirs[0].path));
    }
    // Open node `n`; leader = node 1.  `nodes` = how many replicas take part (2 or 3).
    void open(int nodes) {
        std::vector<NodeId> replicas{1, 2};
        if (nodes == 3) replicas.push_back(3);
        for (int n = 0; n < nodes; ++n) {
            logs[n] = std::make_unique<rune::storage::Log>(dirs[n].path, kRoll);
            logs[n]->recover();
            logs[n]->setDeferSealSync(false);
            logs[n]->setEpochHistoryCap(std::numeric_limits<int64_t>::max());
            rep[n] = std::make_unique<PartitionReplica>(
                cfgFor(n + 1, lagMaxMs, maxFetch), n == 0 ? Role::Leader : Role::Follower, 1, replicas, replicas,
                logs[n].get(), &transport);
            rep[n]->setReadScratch(scratch.data(), scratch.size());
            if (n > 0) rep[n]->becomeFollower(1, 1);
        }
        transport.setLeader(rep[0].get());
        for (int n = 1; n < nodes; ++n) transport.addFollower(n + 1, rep[n].get());
        nodes_ = nodes;
    }
    // Followers report in and the HW reaches the LEO everywhere.
    void settle() {
        for (int n = 1; n < nodes_; ++n) (void)rep[0]->onFetchRequest(ackOf(n + 1, leo), clock);
        drive(10);
    }
    void drive(int steps) {
        for (int i = 0; i < steps; ++i) {
            clock += kStep;
            for (int n = 0; n < nodes_; ++n) rep[n]->doWork(clock);
            transport.pump(clock);
        }
    }
    // Drive until `pred` or the cap.
    template <class P> bool driveUntil(P pred, int cap = 200) {
        for (int i = 0; i < cap && !pred(); ++i) drive(1);
        return pred();
    }
    int nodes_ = 0;
};

}  // namespace

TEST(SegmentRepairRegression, AFollowerRepairsItsDamagedSegmentFromTheLeaderRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    ASSERT_EQ(r.logs[1]->damagedSegmentCount(), 1u);
    r.settle();
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairsCompleted() == 1; }));
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 0u);
    r.logs[1]->runCleanerFileOps(0);
    EXPECT_EQ(slurp(firstSegment(r.dirs[1].path)), r.intact);
    EXPECT_EQ(r.rep[1]->segmentRepairAttemptsFailed(), 0u);
    EXPECT_EQ(r.rep[1]->segmentRepairNoPeer(), 0u);
    EXPECT_GE(r.rep[0]->repairRequestsServed(), 1u);
    EXPECT_EQ(r.rep[1]->segmentRepairActive(), false);
    // The counter movement is handed to the worker once (it folds it into the node's metrics).
    ASSERT_TRUE(r.rep[1]->repairEventsPending());
    const auto ev = r.rep[1]->takeRepairEvents();
    EXPECT_EQ(ev.completed, 1u);
    EXPECT_EQ(ev.failed, 0u);
    EXPECT_EQ(ev.noPeer, 0u);
    EXPECT_FALSE(r.rep[1]->repairEventsPending());
    EXPECT_EQ(r.rep[1]->takeRepairEvents().completed, 0u);
    // The leader was never damaged: nothing of its own to repair.
    EXPECT_EQ(r.rep[0]->segmentRepairsCompleted(), 0u);
    // The repaired copy reads over the whole range.
    std::vector<uint8_t> buf(1 << 16);
    rune::storage::ReadIntoStatus st = rune::storage::ReadIntoStatus::Corrupt;
    const auto v = r.logs[1]->readInto(r.D, 1 << 16, buf.data(), buf.size(), st);
    EXPECT_EQ(st, rune::storage::ReadIntoStatus::Ok);
    EXPECT_FALSE(v.empty());
}

TEST(SegmentRepairRegression, ALeaderRepairsItsDamagedSegmentFromAnInSyncFollowerRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);
    r.open(2);
    ASSERT_EQ(r.logs[0]->damagedSegmentCount(), 1u);
    r.settle();
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[0]->segmentRepairsCompleted() == 1; }));
    EXPECT_EQ(r.logs[0]->damagedSegmentCount(), 0u);
    r.logs[0]->runCleanerFileOps(0);
    EXPECT_EQ(slurp(firstSegment(r.dirs[0].path)), r.intact);
    EXPECT_EQ(r.rep[0]->role(), Role::Leader) << "no leadership change";
    EXPECT_EQ(r.rep[0]->epoch(), 1);
    EXPECT_EQ(r.logs[0]->logEndOffset(), r.leo);
    EXPECT_EQ(r.transport.repairReqTo.front(), 2);
}

TEST(SegmentRepairRegression, ARefusingFirstPeerIsFollowedByASecondThatServesRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);        // the leader
    damageFirstSegment(r.dirs[1].path);              // node 2 holds it damaged too: it refuses; node 3 serves
    r.open(3);
    r.settle();
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[0]->segmentRepairsCompleted() == 1; }));
    ASSERT_GE(r.transport.repairReqTo.size(), 2u);
    EXPECT_EQ(r.transport.repairReqTo.front(), 2) << "peers are tried in id order";
    EXPECT_NE(std::find(r.transport.repairReqTo.begin(), r.transport.repairReqTo.end(), 3),
              r.transport.repairReqTo.end());
    EXPECT_GE(r.rep[1]->repairRequestsRefused(), 1u);
    EXPECT_EQ(r.logs[0]->damagedSegmentCount(), 0u);
    r.logs[0]->runCleanerFileOps(0);
    EXPECT_EQ(slurp(firstSegment(r.dirs[0].path)), r.intact);
    EXPECT_EQ(r.rep[0]->segmentRepairNoPeer(), 0u);
}

TEST(SegmentRepairRegression, WhenEveryCopyIsDamagedTheSegmentStaysDamagedAndIsCountedRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    const auto damaged = (r.D = damageFirstSegment(r.dirs[0].path), damageFirstSegment(r.dirs[1].path), r.D);
    r.open(2);
    const auto before = slurp(firstSegment(r.dirs[0].path));
    r.settle();
    r.drive(80);   // several rounds
    EXPECT_EQ(r.rep[0]->segmentRepairsCompleted(), 0u);
    EXPECT_EQ(r.logs[0]->damagedSegmentCount(), 1u);
    EXPECT_EQ(r.logs[0]->firstDamageOffset(), std::optional<int64_t>(damaged));
    EXPECT_EQ(r.rep[0]->segmentRepairNoPeer(), 1u) << "counted once per segment, however many rounds";
    EXPECT_EQ(slurp(firstSegment(r.dirs[0].path)), before) << "the damaged file is exactly as #839 leaves it";
    EXPECT_FALSE(fs::exists(rune::storage::Log::repairLogPath(r.dirs[0].path, r.bases[0])))
        << "no side file between rounds";
}

TEST(SegmentRepairRegression, RfOneStaysDamagedExactlyAsBeforeRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);
    // A solo replica: no peer exists.
    r.logs[0] = std::make_unique<rune::storage::Log>(r.dirs[0].path, kRoll);
    r.logs[0]->recover();
    const std::vector<NodeId> solo{1};
    r.rep[0] = std::make_unique<PartitionReplica>(cfgFor(1), Role::Leader, 1, solo, solo, r.logs[0].get(), &r.transport);
    r.nodes_ = 1;
    r.transport.setLeader(r.rep[0].get());
    r.drive(40);
    EXPECT_EQ(r.rep[0]->segmentRepairNoPeer(), 1u);
    EXPECT_EQ(r.logs[0]->damagedSegmentCount(), 1u);
    EXPECT_TRUE(r.transport.repairReqs.empty());
}

TEST(SegmentRepairRegression, ASegmentEndingAboveTheHighWatermarkWaitsUntilItPassesRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);   // the leader: its HW needs the follower's report
    r.open(2);
    r.transport.dropFetches = true;
    ASSERT_EQ(r.rep[0]->highWatermark(), 0);
    r.drive(40);
    EXPECT_TRUE(r.transport.repairReqs.empty()) << "nothing is requested while the segment is above the HW";
    EXPECT_EQ(r.logs[0]->damagedSegmentCount(), 1u);
    EXPECT_FALSE(r.rep[0]->segmentRepairActive());
    // The follower reports: the HW passes the segment's end and the repair starts and completes.
    r.transport.dropFetches = false;
    r.settle();
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[0]->segmentRepairsCompleted() == 1; }));
}

TEST(SegmentRepairRegression, ALeaderRefusesToServeAboveItsOwnHighWatermarkRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.open(2);
    // The leader's HW is still 0 (the follower has not reported): it may not serve any committed range.
    ASSERT_EQ(r.rep[0]->highWatermark(), 0);
    FetchRequestMsg req{};
    req.leaderEpoch = 1; req.partition = 0; req.followerId = 2; req.maxBytes = 4096;
    req.fetchOffset = r.bases[0]; req.lastEpochStartOffset = r.bases[1]; req.incarnation = 7;
    r.rep[0]->onRepairRequest(req);
    r.transport.pump(r.clock);
    ASSERT_EQ(r.transport.statuses.size(), 1u);
    EXPECT_EQ(r.transport.statuses.back(), kRepairRefused);
    // Below the HW the same request is served.
    (void)r.rep[0]->onFetchRequest(ackOf(2, r.leo), r.clock);
    ASSERT_GE(r.rep[0]->highWatermark(), r.bases[1]);
    r.rep[0]->onRepairRequest(req);
    EXPECT_EQ(r.transport.statuses.size(), 2u);
    EXPECT_EQ(r.transport.statuses.back(), kErrNone);
}

TEST(SegmentRepairRegression, AResponseForAStaleAttemptIsIgnoredRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    r.settle();
    r.transport.dropRequests = true;   // hold the first request
    ASSERT_TRUE(r.driveUntil([&] { return !r.transport.repairReqs.empty(); }));
    const FetchRequestMsg first = r.transport.repairReqs.front();
    // An answer that echoes another attempt id, and one for another cursor: both ignored.
    FetchResponseMsg stale{};
    stale.leaderEpoch = 1; stale.partition = 0; stale.topicId = 0;
    stale.errorCode = kRepairDone;
    stale.fromOffset = first.fetchOffset;
    stale.logStartOffset = first.incarnation + 99;
    r.rep[1]->applyRepairResponse(stale);
    stale.logStartOffset = first.incarnation;
    stale.fromOffset = first.fetchOffset + 5;
    r.rep[1]->applyRepairResponse(stale);
    EXPECT_EQ(r.rep[1]->segmentRepairsCompleted(), 0u);
    EXPECT_TRUE(r.rep[1]->segmentRepairActive());
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 1u);
    // The genuine Done for that very attempt commits (nothing was appended: the segment keeps its valid prefix).
    stale.fromOffset = first.fetchOffset;
    r.rep[1]->applyRepairResponse(stale);
    EXPECT_EQ(r.rep[1]->segmentRepairsCompleted(), 1u);
}

TEST(SegmentRepairRegression, ADuplicateOrReorderedAnswerIsIgnoredRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    r.settle();
    // Capture the first answer the leader sends, then deliver it twice more after the repair moved on.
    FetchResponseMsg captured{};
    std::vector<uint8_t> recs;
    bool have = false;
    r.transport.dropRequests = false;
    ASSERT_TRUE(r.driveUntil([&] { return r.transport.responses >= 1; }));
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairsCompleted() == 1; }));
    // Replay an old answer (attempt 1, cursor D): the repair is over, so it is dropped without effect.
    captured.leaderEpoch = 1; captured.partition = 0; captured.errorCode = kErrNone;
    captured.logStartOffset = r.transport.repairReqs.front().incarnation;
    captured.fromOffset = r.D;
    recs = slurp(firstSegment(r.dirs[1].path));
    captured.records = std::span<const uint8_t>(recs.data(), recs.size());
    have = true;
    r.rep[1]->applyRepairResponse(captured);
    r.rep[1]->applyRepairResponse(captured);
    EXPECT_TRUE(have);
    EXPECT_EQ(r.rep[1]->segmentRepairsCompleted(), 1u);
    EXPECT_EQ(r.rep[1]->segmentRepairAttemptsFailed(), 0u);
    EXPECT_FALSE(r.rep[1]->segmentRepairActive());
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 0u);
}

TEST(SegmentRepairRegression, BatchesOutsideTheRangeOrOfTheWrongEpochFailTheAttemptRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    r.settle();
    r.transport.dropRequests = true;
    ASSERT_TRUE(r.driveUntil([&] { return !r.transport.repairReqs.empty(); }));
    const FetchRequestMsg req = r.transport.repairReqs.back();
    auto answer = [&](std::vector<uint8_t>& bytes) {
        FetchResponseMsg m{};
        m.leaderEpoch = 1; m.partition = 0; m.topicId = 0; m.errorCode = kErrNone;
        m.fromOffset = req.fetchOffset; m.logStartOffset = req.incarnation;
        m.records = std::span<const uint8_t>(bytes.data(), bytes.size());
        r.rep[1]->applyRepairResponse(m);
    };
    // The peer's real batches of the range, from the intact copy.
    std::vector<uint8_t> good;
    {
        std::size_t pos = 0;
        bool take = false;
        while (pos < r.intact.size()) {
            RecordBatchReader b(r.intact.data() + pos, r.intact.size() - pos);
            const std::size_t total = static_cast<std::size_t>(b.totalLength());
            if (b.baseOffset() >= r.D) take = true;
            if (take) good.insert(good.end(), r.intact.begin() + static_cast<std::ptrdiff_t>(pos),
                                  r.intact.begin() + static_cast<std::ptrdiff_t>(pos + total));
            pos += total;
        }
    }
    // (a) a batch outside the range: one starting at the segment's end offset.
    {
        RecordBatchBuilder b(r.bases[1], 1000);
        b.append("k", "v", 1000);
        std::vector<uint8_t> outside = b.build();
        answer(outside);
        EXPECT_EQ(r.rep[1]->segmentRepairAttemptsFailed(), 1u);
    }
    // The only peer failed: the segment stays damaged, the side file is gone.
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 1u);
    EXPECT_FALSE(r.logs[1]->segmentRepairBase().has_value());
    // (b) the next round, a batch of the right range but stamped with an epoch the cache does not hold for it.
    r.transport.dropRequests = true;
    const std::size_t seen = r.transport.repairReqs.size();
    ASSERT_TRUE(r.driveUntil([&] { return r.transport.repairReqs.size() > seen; }, 400));
    const FetchRequestMsg req2 = r.transport.repairReqs.back();
    auto wrongEpoch = good;
    RecordBatchReader::stampLeaderEpoch(wrongEpoch.data(), 9);
    FetchResponseMsg m{};
    m.leaderEpoch = 1; m.partition = 0; m.topicId = 0; m.errorCode = kErrNone;
    m.fromOffset = req2.fetchOffset; m.logStartOffset = req2.incarnation;
    m.records = std::span<const uint8_t>(wrongEpoch.data(), wrongEpoch.size());
    r.rep[1]->applyRepairResponse(m);
    EXPECT_EQ(r.rep[1]->segmentRepairAttemptsFailed(), 2u);
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 1u);
    EXPECT_EQ(r.rep[1]->segmentRepairsCompleted(), 0u);
}

TEST(SegmentRepairRegression, ALeaderChangeMidRepairDropsTheSideFileAndStartsOverRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    r.settle();
    r.transport.dropRequests = true;
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairActive(); }));
    ASSERT_TRUE(fs::exists(rune::storage::Log::repairLogPath(r.dirs[1].path, r.bases[0])));
    r.rep[0]->becomeLeader(2, {1, 2});   // a new leader epoch: the attempt began under epoch 1
    r.rep[1]->becomeFollower(2, 1);
    r.drive(2);
    EXPECT_GE(r.rep[1]->segmentRepairsAborted(), 1u);
    // It starts over under the new epoch and, with the leader reachable again, completes.
    r.transport.dropRequests = false;
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairsCompleted() == 1; }, 400));
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 0u);
}

TEST(SegmentRepairRegression, ACleanerSwapOfTheSegmentMidRepairAbortsItRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    r.settle();
    r.transport.dropRequests = true;
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairActive() && !r.transport.repairReqs.empty(); }));
    const std::size_t asked = r.transport.repairReqs.size();
    const int64_t firstAttempt = r.transport.repairReqs.back().incarnation;
    r.logs[1]->reloadSegment(r.bases[0]);   // a new incarnation of the segment (what a cleaner swap installs)
    r.drive(2);
    EXPECT_GE(r.rep[1]->segmentRepairsAborted(), 1u) << "the side file of the old incarnation is dropped";
    EXPECT_GT(r.transport.repairReqs.size(), asked) << "and the repair starts over on the new incarnation";
    EXPECT_NE(r.transport.repairReqs.back().incarnation, firstAttempt);
    EXPECT_EQ(r.rep[1]->segmentRepairsCompleted(), 0u);
    r.transport.dropRequests = false;
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairsCompleted() == 1; }, 400));
    EXPECT_EQ(r.logs[1]->damagedSegmentCount(), 0u);
}

TEST(SegmentRepairRegression, ATruncationBelowTheSegmentEndMidRepairAbortsItRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[1].path);
    r.open(2);
    r.settle();
    r.transport.dropRequests = true;
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->segmentRepairActive() && !r.transport.repairReqs.empty(); }));
    ASSERT_TRUE(r.logs[1]->truncateTo(r.bases[0] + 1));   // below the end of the segment under repair
    r.drive(2);
    EXPECT_GE(r.rep[1]->segmentRepairsAborted(), 1u);
    EXPECT_FALSE(r.logs[1]->segmentRepairBase().has_value());
    EXPECT_FALSE(fs::exists(rune::storage::Log::repairLogPath(r.dirs[1].path, r.bases[0])));
}

// ---- #880 follow-up: a follower wedged on the leader's damaged batch probes again --------------------------------

namespace {
// Node 3 holds the log up to (not including) the damage offset: it is "behind the damage".
void truncateNode3To(Rig& r, int64_t at) {
    rune::storage::Log l(r.dirs[2].path, kRoll);
    l.recover();
    ASSERT_TRUE(l.truncateTo(at));
}
constexpr int64_t kProbeNs = 1'000'000'000;   // PartitionReplica::kCorruptWedgeProbeNs
}  // namespace

TEST(SegmentRepairRegression, AFollowerWedgedOnTheLeadersDamageResumesAfterTheRepairRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);
    ASSERT_NO_FATAL_FAILURE(truncateNode3To(r, r.D));
    r.lagMaxMs = 3000;                             // node 3 is evicted by the lag check, then the HW follows node 2
    r.open(3);
    r.transport.dropFetchesFrom = {2};             // node 2 has not reported: the HW is 0, no repair yet
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[2]->wedgedAtOffset() == r.D; }));
    EXPECT_EQ(r.rep[2]->wedgedRefusals(), 1u);
    const int sentAtWedge = r.transport.fetchesFrom[3];
    // Less than the probe period: C is silent.
    r.drive(5);
    EXPECT_EQ(r.transport.fetchesFrom[3], sentAtWedge) << "a wedged follower sends nothing between probes";
    EXPECT_EQ(r.rep[2]->corruptWedgeProbes(), 0u);
    // The leader repairs from node 2.  (Its repair pass runs once a second, so a probe or two that is still answered
    // "corrupt" may land before it completes: the wedge just stands.)
    r.transport.dropFetchesFrom.clear();
    (void)r.rep[0]->onFetchRequest(ackOf(2, r.leo), r.clock);
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[0]->segmentRepairsCompleted() == 1; }));
    ASSERT_EQ(r.logs[0]->damagedSegmentCount(), 0u);
    EXPECT_EQ(r.rep[2]->wedgedAtOffset(), r.D);
    // From now on the next probe (within one period) is answered with data: exactly one fetch, not a burst.
    const int sentAtRepair = r.transport.dataFetchesFrom[3];
    const uint64_t probesAtRepair = r.rep[2]->corruptWedgeProbes();
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[2]->corruptWedgeProbes() > probesAtRepair; }, 12));
    EXPECT_EQ(r.rep[2]->corruptWedgeProbes(), probesAtRepair + 1);
    EXPECT_EQ(r.transport.dataFetchesFrom[3], sentAtRepair + 1) << "one probe, not a burst";
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[2]->wedgedAtOffset() == -1; }, 5));
    ASSERT_TRUE(r.driveUntil([&] { return r.logs[2]->logEndOffset() == r.leo; }, 100));
    EXPECT_EQ(r.rep[2]->wedgedRefusals(), 1u);
    ASSERT_TRUE(r.driveUntil([&] {
        const auto& isr = r.rep[0]->isr();
        return std::find(isr.begin(), isr.end(), 3) != isr.end();
    }, 100)) << "caught up: it re-enters the ISR";
}

TEST(SegmentRepairRegression, AWedgedFollowerProbesOncePerPeriodAndIsNotIsrLivenessRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);
    damageFirstSegment(r.dirs[1].path);            // no peer can serve the repair
    ASSERT_NO_FATAL_FAILURE(truncateNode3To(r, r.D));
    r.lagMaxMs = 3000;
    r.open(3);
    r.settle();   // (node 3's report is answered "empty ok" after it wedged, too: that must not clear the wedge)
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[2]->wedgedAtOffset() == r.D; }));
    const int sentAtWedge = r.transport.fetchesFrom[3];
    const uint64_t servedAtWedge = r.rep[0]->corruptReadsServed();
    const int64_t t0 = r.clock;
    r.drive(100);   // 10 probe periods of 1 s
    ASSERT_EQ(r.clock - t0, 10 * kProbeNs);
    EXPECT_EQ(r.transport.fetchesFrom[3], sentAtWedge + 10);
    EXPECT_EQ(r.rep[2]->corruptWedgeProbes(), 10u);
    EXPECT_EQ(r.rep[0]->corruptReadsServed(), servedAtWedge + 10) << "every probe was answered, none served";
    EXPECT_EQ(r.rep[2]->wedgedAtOffset(), r.D);
    EXPECT_EQ(r.rep[2]->wedgedRefusals(), 1u) << "the repeated answer neither re-wedges nor counts";
    const auto& isr = r.rep[0]->isr();
    EXPECT_EQ(std::find(isr.begin(), isr.end(), 3), isr.end()) << "never live: evicted by the lag check";
    EXPECT_EQ(r.rep[0]->segmentRepairsCompleted(), 0u);
}

TEST(SegmentRepairRegression, ATooLargeWedgeNeverProbesRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    for (const auto& e : fs::directory_iterator(r.dirs[1].path)) fs::remove_all(e.path());   // node 2 starts empty
    r.maxFetch = 64;                  // smaller than the first batch
    r.transport.frameLimit = 100;     // and the frame cannot hold it either
    r.open(2);
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[1]->wedgedAtOffset() == 0; }));
    EXPECT_EQ(r.rep[0]->recordTooLargeServed() >= 1, true);
    const int sentAtWedge = r.transport.fetchesFrom[2];
    r.drive(50);
    EXPECT_EQ(r.transport.fetchesFrom[2], sentAtWedge) << "an unreplicable batch never becomes servable by waiting";
    EXPECT_EQ(r.rep[1]->corruptWedgeProbes(), 0u);
    EXPECT_EQ(r.rep[1]->wedgedAtOffset(), 0);
}

TEST(SegmentRepairRegression, AWedgeClearsWhenTheLeadersLogNowEndsAtTheWedgedOffsetRegression) {
    Rig r;
    ASSERT_NO_FATAL_FAILURE(r.seed());
    r.D = damageFirstSegment(r.dirs[0].path);
    ASSERT_NO_FATAL_FAILURE(truncateNode3To(r, r.D));
    r.lagMaxMs = 3000;
    r.open(3);
    r.settle();
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[2]->wedgedAtOffset() == r.D; }));
    // The leader's log now ends exactly where the follower's does (the damaged tail was cut away).
    bool cut = false;
    for (int i = 0; i < 20 && !cut; ++i) {   // the cleaner's posted file ops may defer it
        r.logs[0]->runCleanerFileOps(0);
        cut = r.logs[0]->truncateTo(r.D);
    }
    ASSERT_TRUE(cut);
    ASSERT_EQ(r.logs[0]->logEndOffset(), r.D);
    ASSERT_TRUE(r.driveUntil([&] { return r.rep[2]->wedgedAtOffset() == -1; }, 30));
    EXPECT_EQ(r.rep[2]->wedgedRefusals(), 1u);
    EXPECT_GE(r.rep[2]->corruptWedgeProbes(), 1u);
}
