// ---------------------------------------------------------------------------
// LargeBatchServeTest.cpp — F18 regression: a stored batch larger than the
// follower's fetch maxBytes MUST still be replicated (Kafka KIP-74: the first
// whole batch at fetchOffset is always served, later batches stay bounded by
// maxBytes).  Before the fix every serve path (fetch, push, reconcile) trimmed
// the response to whole batches <= maxBytes, so an oversized first batch was
// never sent and the follower stalled at that offset forever.
//
// Deterministic: injected clock, InMemoryTransport, bounded driver loop.
// ---------------------------------------------------------------------------
#include <gtest/gtest.h>
#include <rune/storage/testing/DemandChannelForTest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::replication;

namespace {

constexpr NodeId      kLeaderId   = 1;
constexpr NodeId      kFollowerId = 2;
constexpr int32_t     kMaxBytes   = 4096;          // follower fetch cap
constexpr std::size_t kBigValue   = 16 * 1024;     // one batch ~4x the cap
constexpr std::size_t kSmallValue = 64;
constexpr int         kDriverCap  = 200;

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& tag) {
        static std::atomic<uint64_t> seq{0};
        path = (std::filesystem::temp_directory_path() /
                ("rune_f18_" + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(seq.fetch_add(1)) + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())))
                   .string();
        std::filesystem::create_directories(path);
    }
    ~TempDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

PartitionConfig makeCfg(NodeId self, bool pushOnAppend) {
    PartitionConfig c;
    c.partitionId         = 0;
    c.selfId              = self;
    c.replicaLagMaxMs     = 1000;
    c.replicaLagMaxOffset = 100;
    c.minIsr              = 1;
    c.fetchIntervalNs     = 1'000'000LL;
    c.isrCheckIntervalNs  = 1'000'000LL;
    c.maxFetchBytes       = kMaxBytes;
    c.pushOnAppend        = pushOnAppend;
    return c;
}

// Records every served FetchResponse / ReconcileResponse records payload, then
// delivers exactly like InMemoryTransport.
class CapturingTransport : public InMemoryTransport {
public:
    std::vector<std::vector<uint8_t>> fetchResponses;
    std::vector<std::vector<uint8_t>> reconcileResponses;
    std::vector<int16_t>              fetchErrors;     // errorCode of every response
    // F18: a real transport's per-frame records ceiling.  A response above it is
    // DROPPED (exactly what AeronPartitionTransport does) — never delivered.
    int64_t frameCap        = std::numeric_limits<int64_t>::max();
    int     oversizeDropped = 0;
    int     fetchRequests   = 0;                       // every follower FetchRequest
    int     oleRequests     = 0;                       // every OffsetsForLeaderEpoch probe
    bool sendOffsetsForLeaderEpochRequest(NodeId to, const OffsetsForLeaderEpochReqMsg& req) override {
        ++oleRequests;
        InMemoryTransport::sendOffsetsForLeaderEpochRequest(to, req);
        return true;
    }

    [[nodiscard]] int64_t maxFetchRecordsBytes() const noexcept override { return frameCap; }
    std::vector<int32_t> fetchMaxBytes;                // maxBytes of every follower FetchRequest
    bool sendFetchRequest(const FetchRequestMsg& req) override {
        ++fetchRequests;
        fetchMaxBytes.push_back(req.maxBytes);
        InMemoryTransport::sendFetchRequest(req);
        return true;
    }
    bool sendFetchResponse(NodeId to, const FetchResponseMsg& resp) override {
        if (static_cast<int64_t>(resp.records.size()) > frameCap) { ++oversizeDropped; return false; }
        fetchResponses.emplace_back(resp.records.begin(), resp.records.end());
        fetchErrors.push_back(resp.errorCode);
        InMemoryTransport::sendFetchResponse(to, resp);
        return true;
    }
    bool sendReconcileResponse(NodeId to, const FetchResponseMsg& resp) override {
        reconcileResponses.emplace_back(resp.records.begin(), resp.records.end());
        InMemoryTransport::sendReconcileResponse(to, resp);
        return true;
    }
};

struct BatchSpan { int64_t base; int64_t last; std::size_t bytes; };

// Walk whole batches; ASSERT-style helper returns the batch list.
std::vector<BatchSpan> batchesOf(const std::vector<uint8_t>& b) {
    std::vector<BatchSpan> out;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= b.size()) {
        rune::protocol::RecordBatchReader r(b.data() + pos, b.size() - pos);
        const int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > b.size()) break;
        out.push_back({r.baseOffset(), r.baseOffset() + r.lastOffsetDelta(),
                       static_cast<std::size_t>(total)});
        pos += static_cast<std::size_t>(total);
    }
    EXPECT_EQ(pos, b.size()) << "served bytes must be whole batches only";
    return out;
}

FetchRequestMsg fetchReq(Offset off, int32_t maxBytes) {
    FetchRequestMsg r{};
    r.leaderEpoch = 1;
    r.partition   = 0;
    r.followerId  = kFollowerId;
    r.fetchOffset = off;
    r.maxBytes    = maxBytes;
    return r;
}

class LargeBatchReplicationRegression : public ::testing::Test {
protected:
    TempDir            leaderDir{"leader"}, followerDir{"follower"};
    rune::storage::Log leaderLog{leaderDir.path}, followerLog{followerDir.path};
    CapturingTransport transport;
    std::unique_ptr<PartitionReplica> leader, follower;
    int64_t clock = 0;

    void build(bool pushOnAppend, bool withFollower) {
        const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
        leader = std::make_unique<PartitionReplica>(makeCfg(kLeaderId, pushOnAppend),
                                                    Role::Leader, 1, replicas, replicas,
                                                    &leaderLog, &transport);
        transport.setLeader(leader.get());
        if (withFollower) {
            follower = std::make_unique<PartitionReplica>(makeCfg(kFollowerId, pushOnAppend),
                                                          Role::Follower, 1, replicas, replicas,
                                                          &followerLog, &transport);
            transport.addFollower(kFollowerId, follower.get());
            // KIP-320/595: the follower reports in (a validated pure ack on its
            // empty log) — a leader pushes only to validated followers.
            (void)leader->onFetchRequest(fetchReq(0, 0), 0);
            transport.pump(0);
            transport.fetchResponses.clear();
            transport.fetchErrors.clear();
        }
    }
    // big (offset 0), small (1), small (2)
    void appendBigThenSmalls() {
        const std::vector<uint8_t> big(kBigValue, 0xB1), small(kSmallValue, 0x51);
        ASSERT_EQ(leader->append(big), 0);
        ASSERT_EQ(leader->append(small), 1);
        ASSERT_EQ(leader->append(small), 2);
    }
};

} // namespace

// The leader answers a fetch whose maxBytes is smaller than the first batch with
// EXACTLY that whole batch (never empty, never a torn prefix); later batches stay
// bounded by maxBytes.
TEST_F(LargeBatchReplicationRegression, FetchServesWholeFirstBatchLargerThanMaxBytes) {
    build(/*push*/false, /*follower*/false);
    appendBigThenSmalls();

    ASSERT_EQ(leader->onFetchRequest(fetchReq(0, kMaxBytes), ++clock), kErrNone);
    ASSERT_EQ(transport.fetchResponses.size(), 1u);
    const auto served = batchesOf(transport.fetchResponses.back());
    ASSERT_EQ(served.size(), 1u) << "exactly the oversized first batch, nothing after it";
    EXPECT_EQ(served[0].base, 0);
    EXPECT_EQ(served[0].last, 0);
    EXPECT_GT(served[0].bytes, static_cast<std::size_t>(kMaxBytes));

    // Next fetch continues normally: both small batches fit under maxBytes.
    ASSERT_EQ(leader->onFetchRequest(fetchReq(1, kMaxBytes), ++clock), kErrNone);
    ASSERT_EQ(transport.fetchResponses.size(), 2u);
    const auto next = batchesOf(transport.fetchResponses.back());
    ASSERT_EQ(next.size(), 2u);
    EXPECT_EQ(next[0].base, 1);
    EXPECT_EQ(next[1].base, 2);
}

// Subsequent batches are still bounded: maxBytes = big + one small → big + one small.
TEST_F(LargeBatchReplicationRegression, BatchesAfterTheFirstStayBoundedByMaxBytes) {
    build(false, false);
    appendBigThenSmalls();
    ASSERT_EQ(leader->onFetchRequest(fetchReq(0, kMaxBytes), ++clock), kErrNone);
    const std::size_t bigBytes = batchesOf(transport.fetchResponses.back()).at(0).bytes;
    const std::size_t smallBytes = [&] {
        EXPECT_EQ(leader->onFetchRequest(fetchReq(1, kMaxBytes), ++clock), kErrNone);
        return batchesOf(transport.fetchResponses.back()).at(0).bytes;
    }();

    ASSERT_EQ(leader->onFetchRequest(
                  fetchReq(0, static_cast<int32_t>(bigBytes + smallBytes)), ++clock),
              kErrNone);
    const auto served = batchesOf(transport.fetchResponses.back());
    ASSERT_EQ(served.size(), 2u);
    EXPECT_EQ(served[0].base, 0);
    EXPECT_EQ(served[1].base, 1);
}

// End to end over the fetch loop (push off): the follower applies the oversized
// batch, advances past it, keeps fetching, and the leader HW commits everything.
TEST_F(LargeBatchReplicationRegression, FollowerAppliesOversizedBatchAndAdvancesViaFetch) {
    build(/*push*/false, /*follower*/true);
    appendBigThenSmalls();
    const Offset leo = leader->logEndOffset();
    ASSERT_EQ(leo, 3);
    for (int i = 0; i < kDriverCap &&
                    !(follower->logEndOffset() == leo && leader->highWatermark() == leo); ++i) {
        clock += 1'000'000LL;
        leader->doWork(clock);
        follower->doWork(clock);
        transport.pump(clock);
    }
    EXPECT_EQ(follower->logEndOffset(), leo) << "follower stuck behind the oversized batch";
    EXPECT_EQ(leader->highWatermark(), leo) << "acks=all commit never happened";
}

// Push path: the leader pushes the oversized just-appended batch whole.
TEST_F(LargeBatchReplicationRegression, PushServesWholeOversizedBatch) {
    build(/*push*/true, /*follower*/true);
    const std::vector<uint8_t> big(kBigValue, 0xB2);
    ASSERT_EQ(leader->append(big), 0);
    ASSERT_FALSE(transport.fetchResponses.empty()) << "append pushed nothing";
    const auto served = batchesOf(transport.fetchResponses.back());
    ASSERT_EQ(served.size(), 1u);
    EXPECT_EQ(served[0].base, 0);
    EXPECT_GT(served[0].bytes, static_cast<std::size_t>(kMaxBytes));

    // ... and the follower converges with push on as well.
    for (int i = 0; i < kDriverCap &&
                    !(follower->logEndOffset() == 1 && leader->highWatermark() == 1); ++i) {
        clock += 1'000'000LL;
        leader->doWork(clock);
        follower->doWork(clock);
        transport.pump(clock);
    }
    EXPECT_EQ(follower->logEndOffset(), 1);
    EXPECT_EQ(leader->highWatermark(), 1);
}

// (ReconcileServesWholeOversizedFirstBatch retired with the DR-BUG-6 reverse
//  reconcile: a leader never pulls records it did not hold when elected.)

// ===========================================================================
// F18 fix round 1 (I-1): an UNSERVABLE batch must never stall silently.
// The leader holds a stored batch larger than its own replication frame (e.g.
// accepted under a larger config).  Before: every serve was dropped by the
// transport, the follower re-fetched it forever, each re-fetch refreshed its ISR
// liveness → HW pinned below the batch forever, nothing reported.
// Required: an explicit kErrRecordTooLarge answer (counted), the follower stops
// re-fetching (wedged, counted), the request is NOT ISR liveness → the follower
// leaves the ISR via the existing lag check, HW advances with the remaining ISR,
// acks=all with minIsr=2 then fails loudly.
// ===========================================================================
namespace {
PartitionConfig lagCfg(NodeId self, int minIsr = 2) {
    PartitionConfig c = makeCfg(self, /*push*/true);
    c.replicaLagMaxMs = 20;            // existing ISR lag check (ms of the injected clock)
    c.minIsr          = minIsr;
    return c;
}
} // namespace

// minIsr=2: after the eviction the served HW stays min-ISR-gated (DR-BUG-3) and
// acks=all is refused loudly.  minIsr=1 (below): HW advances with the remaining ISR.
TEST_F(LargeBatchReplicationRegression, LeaderUnservableBatchEvictsFollowerInsteadOfSilentStall) {
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    leader = std::make_unique<PartitionReplica>(lagCfg(kLeaderId), Role::Leader, 1, replicas,
                                                replicas, &leaderLog, &transport);
    follower = std::make_unique<PartitionReplica>(lagCfg(kFollowerId), Role::Follower, 1,
                                                  replicas, replicas, &followerLog, &transport);
    follower->becomeFollower(1, kLeaderId);
    transport.setLeader(leader.get());
    transport.addFollower(kFollowerId, follower.get());
    transport.frameCap = 4096;                         // the leader's replication frame
    const std::vector<uint8_t> big(kBigValue, 0xE1);   // ~16 KiB stored batch
    ASSERT_EQ(leader->append(big, /*acksAll=*/true), 0);

    constexpr int kIters = 200;                        // 200 ms of injected time >> 20 ms lag
    int fetchesAtWedge = -1;
    for (int i = 0; i < kIters; ++i) {
        clock += 1'000'000LL;
        leader->doWork(clock);
        follower->doWork(clock);
        transport.pump(clock);
        if (fetchesAtWedge < 0 && follower->wedgedAtOffset() == 0) fetchesAtWedge = transport.fetchRequests;
    }
    // Leader: explicit, counted answer — never a silent drop.
    EXPECT_GE(leader->recordTooLargeServed(), 1u);
    EXPECT_NE(std::find(transport.fetchErrors.begin(), transport.fetchErrors.end(), kErrRecordTooLarge),
              transport.fetchErrors.end()) << "the follower must be TOLD (kErrRecordTooLarge)";
    // Follower: wedged at the unservable offset, counted, and stops re-fetching.
    EXPECT_EQ(follower->wedgedAtOffset(), 0);
    EXPECT_GE(follower->wedgedRefusals(), 1u);
    ASSERT_GE(fetchesAtWedge, 0) << "the follower never wedged";
    EXPECT_LE(transport.fetchRequests - fetchesAtWedge, 1) << "no infinite silent re-fetch";
    // ISR: the stuck follower leaves (existing lag check), HW advances with the
    // remaining ISR, acks=all (minIsr=2) fails loudly.
    EXPECT_EQ(leader->isr(), std::vector<NodeId>{kLeaderId}) << "the follower must leave the ISR";
    EXPECT_EQ(leader->append(std::vector<uint8_t>(8, 1), /*acksAll=*/true), -1)
        << "acks=all must fail visibly (MinIsr), not hang";
}

TEST_F(LargeBatchReplicationRegression, LeaderUnservableBatchMinIsrOneHwAdvancesWithRemainingIsr) {
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    leader = std::make_unique<PartitionReplica>(lagCfg(kLeaderId, 1), Role::Leader, 1, replicas,
                                                replicas, &leaderLog, &transport);
    follower = std::make_unique<PartitionReplica>(lagCfg(kFollowerId, 1), Role::Follower, 1,
                                                  replicas, replicas, &followerLog, &transport);
    follower->becomeFollower(1, kLeaderId);
    transport.setLeader(leader.get());
    transport.addFollower(kFollowerId, follower.get());
    transport.frameCap = 4096;
    ASSERT_EQ(leader->append(std::vector<uint8_t>(kBigValue, 0xE2), true), 0);
    for (int i = 0; i < 200 && leader->highWatermark() < 1; ++i) {
        clock += 1'000'000LL;
        leader->doWork(clock);
        follower->doWork(clock);
        transport.pump(clock);
    }
    EXPECT_EQ(leader->isr(), std::vector<NodeId>{kLeaderId});
    EXPECT_EQ(leader->highWatermark(), 1) << "HW advances with the remaining ISR (never pinned)";
    EXPECT_EQ(follower->wedgedAtOffset(), 0);
}

// Follower side: its OWN worker ring cannot hold the batch (heterogeneous config).
// The transport reports that as a local kErrRecordTooLarge response (no records);
// the follower must wedge, stop re-fetching, and leave the ISR — same contract.
TEST_F(LargeBatchReplicationRegression, FollowerUnapplicableBatchWedgesAndLeavesIsr) {
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    leader = std::make_unique<PartitionReplica>(lagCfg(kLeaderId), Role::Leader, 1, replicas,
                                                replicas, &leaderLog, &transport);
    follower = std::make_unique<PartitionReplica>(lagCfg(kFollowerId), Role::Follower, 1,
                                                  replicas, replicas, &followerLog, &transport);
    follower->becomeFollower(1, kLeaderId);
    transport.setLeader(leader.get());
    transport.addFollower(kFollowerId, follower.get());

    FetchResponseMsg refused{};
    refused.leaderEpoch = 1;
    refused.partition   = 0;
    refused.errorCode   = kErrRecordTooLarge;
    follower->applyReplicated(refused);
    EXPECT_EQ(follower->wedgedAtOffset(), 0);
    EXPECT_EQ(follower->wedgedRefusals(), 1u);

    const int before = transport.fetchRequests;
    for (int i = 0; i < 200; ++i) {
        clock += 1'000'000LL;
        leader->doWork(clock);
        follower->doWork(clock);
        transport.pump(clock);
    }
    EXPECT_EQ(transport.fetchRequests, before) << "a wedged follower emits no fetch (no liveness)";
    EXPECT_EQ(leader->isr(), std::vector<NodeId>{kLeaderId});

    // A leadership/epoch change clears the wedge (time-independent recovery path).
    follower->becomeFollower(2, kLeaderId);
    EXPECT_EQ(follower->wedgedAtOffset(), -1);
}

// A stale-epoch kErrRecordTooLarge (from a deposed leader) must NOT wedge.
TEST_F(LargeBatchReplicationRegression, StaleEpochRecordTooLargeDoesNotWedge) {
    build(false, true);
    follower->becomeFollower(3, kLeaderId);
    FetchResponseMsg refused{};
    refused.leaderEpoch = 2;
    refused.errorCode   = kErrRecordTooLarge;
    follower->applyReplicated(refused);
    EXPECT_EQ(follower->wedgedAtOffset(), -1);
    EXPECT_EQ(follower->wedgedRefusals(), 0u);
}

// m-3: the push path bounds LATER batches by the transport frame, not 1 MiB
// maxFetchBytes — a multi-batch push above the frame was dropped (and still
// advanced pushedThrough, costing a resend round-trip).
TEST_F(LargeBatchReplicationRegression, PushPrefixBoundedByTransportFrame) {
    build(/*push*/true, /*follower*/true);
    transport.frameCap = 2048;
    // Three ~1 KiB batches appended straight to the log (the gateway/worker
    // append-direct path), then ONE coalesced push in doWork.
    for (int64_t base = 0; base < 3; ++base) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(900, 'z'), 1000);
        const auto bytes = b.build();
        ASSERT_GE(leaderLog.append(bytes, base), 0);
    }
    leader->onLeaderLogAppended();
    transport.fetchResponses.clear();
    transport.oversizeDropped = 0;
    leader->doWork(++clock);
    EXPECT_EQ(transport.oversizeDropped, 0) << "a push prefix above the frame was built and dropped";
    ASSERT_FALSE(transport.fetchResponses.empty()) << "nothing pushed";
    EXPECT_LE(static_cast<int64_t>(transport.fetchResponses.front().size()), transport.frameCap);
}

// F18 fix round 2 (B-2): the unservable-batch check must not add a log lookup.
// A Data serve costs exactly ONE readView (the check and the serve share it),
// and a conflated duplicate fetch reads nothing (as before F18).  Before round 2:
// 2 lookups per Data fetch and 1 per conflated duplicate.
TEST_F(LargeBatchReplicationRegression, FetchCheckCostsNoExtraLogLookup) {
    build(/*push*/false, /*follower*/false);
    appendBigThenSmalls();
    uint64_t before = leaderLog.readViewCallsForTest();
    ASSERT_EQ(leader->onFetchRequest(fetchReq(1, kMaxBytes), ++clock), kErrNone);    // Data
    EXPECT_EQ(leaderLog.readViewCallsForTest() - before, 1u) << "a Data serve: one lookup";
    ASSERT_EQ(transport.fetchResponses.size(), 1u);
    before = leaderLog.readViewCallsForTest();
    ASSERT_EQ(leader->onFetchRequest(fetchReq(1, kMaxBytes), ++clock), kErrNone);    // conflated
    EXPECT_EQ(leaderLog.readViewCallsForTest() - before, 0u) << "a conflated duplicate reads nothing";
    EXPECT_EQ(transport.fetchResponses.size(), 1u) << "the duplicate was conflated";
    // The oversized-first-batch Data serve (KIP-74) also costs exactly one lookup.
    before = leaderLog.readViewCallsForTest();
    ASSERT_EQ(leader->onFetchRequest(fetchReq(0, kMaxBytes), ++clock), kErrNone);
    EXPECT_EQ(leaderLog.readViewCallsForTest() - before, 1u);
}

// F18 fix round 2 (minor 1): OffsetsForLeaderEpoch probes refresh the leader's
// ISR liveness, so a wedged follower must not keep probing (or plain-fetching
// from the probe arm) — else it stays in the ISR while its probe is unanswered.
TEST_F(LargeBatchReplicationRegression, WedgedFollowerSendsNoEpochProbe) {
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    follower = std::make_unique<PartitionReplica>(lagCfg(kFollowerId), Role::Follower, 1,
                                                  replicas, replicas, &followerLog, &transport);
    // An uncommitted tail (LEO 1 > HW 0) + an epoch change ARMS the KIP-101 probe.
    {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", "v", 1000);
        ASSERT_GE(followerLog.append(b.build(), 0), 0);
    }
    follower->becomeFollower(2, kLeaderId);
    follower->onUnapplicableBatch(/*batchBase=*/1, /*leaderEpoch=*/2);   // the batch at LEO
    ASSERT_EQ(follower->wedgedAtOffset(), 1);
    const int ole0 = transport.oleRequests, fetch0 = transport.fetchRequests;
    for (int i = 0; i < 200; ++i) {                    // nobody answers the probe
        clock += 1'000'000LL;
        follower->doWork(clock);
    }
    EXPECT_EQ(transport.oleRequests - ole0, 0) << "a wedged follower must not probe (liveness)";
    EXPECT_EQ(transport.fetchRequests - fetch0, 0) << "nor plain-fetch from the probe arm";
}

TEST_F(LargeBatchReplicationRegression, UnapplicableBatchNotAtLeoDoesNotWedge) {
    build(false, true);
    follower->becomeFollower(1, kLeaderId);
    follower->onUnapplicableBatch(/*batchBase=*/5, /*leaderEpoch=*/1);    // LEO is 0
    EXPECT_EQ(follower->wedgedAtOffset(), -1);
    follower->onUnapplicableBatch(/*batchBase=*/0, /*leaderEpoch=*/0);    // stale epoch
    EXPECT_EQ(follower->wedgedAtOffset(), -1);
    follower->onUnapplicableBatch(/*batchBase=*/0, /*leaderEpoch=*/1);    // at LEO, current epoch
    EXPECT_EQ(follower->wedgedAtOffset(), 0);
}


// hotpath-A review m2: a follower whose (provisioned) log cannot roll right now
// — no ready segment yet — stops asking for DATA (pure-ack keepalives only, so
// it stays in the ISR) instead of applying until the forced-roll cap makes the
// worker create a segment inline.  It resumes once a ready segment exists.
TEST(FollowerRollBackPressureRegression, FollowerStopsFetchingDataWhileRollDeferredRegression) {
    TempDir leaderDir{"bp_leader"}, followerDir{"bp_follower"};
    rune::storage::Log leaderLog{leaderDir.path};
    rune::storage::Log followerLog{followerDir.path, /*rollBytes=*/512};
    followerLog.recover();
    struct Hook { static bool fn(void*, rune::storage::Log*, rune::storage::Log::ProvisionDemand) noexcept { return true; } };
    rune::storage::testing::DemandChannelForTest demandCh1{&Hook::fn, nullptr};
    followerLog.setProvisioning(&demandCh1);
    CapturingTransport transport;
    const std::vector<NodeId> replicas{kLeaderId, kFollowerId};
    auto leader = std::make_unique<PartitionReplica>(makeCfg(kLeaderId, false), Role::Leader, 1, replicas, replicas,
                                                     &leaderLog, &transport);
    transport.setLeader(leader.get());
    auto follower = std::make_unique<PartitionReplica>(makeCfg(kFollowerId, false), Role::Follower, 1, replicas,
                                                       replicas, &followerLog, &transport);
    transport.addFollower(kFollowerId, follower.get());
    const std::vector<uint8_t> small(kSmallValue, 0x5A);
    int64_t clock = 0;
    auto drive = [&](int n) {
        for (int i = 0; i < n; ++i) {
            clock += 1'000'000LL;
            leader->doWork(clock);
            follower->doWork(clock);
            transport.pump(clock);
        }
    };
    // Replicate record by record until the follower's segment is full.
    for (int i = 0; i < 400 && !followerLog.rollDeferred(); ++i) {
        ASSERT_GE(leader->append(small), 0);
        drive(5);
    }
    ASSERT_TRUE(followerLog.rollDeferred()) << "the follower's active segment filled with no ready segment";
    // The leader keeps producing.
    for (int i = 0; i < 32; ++i) ASSERT_GE(leader->append(small), 0);
    drive(5);   // a fetch already in flight may still land
    const Offset stuckLeo = follower->logEndOffset();
    ASSERT_LT(stuckLeo, leader->logEndOffset());
    transport.fetchMaxBytes.clear();
    drive(50);
    ASSERT_FALSE(transport.fetchMaxBytes.empty()) << "keepalives keep flowing (ISR liveness)";
    for (int32_t mb : transport.fetchMaxBytes) EXPECT_EQ(mb, 0) << "a data fetch while the roll is deferred";
    EXPECT_EQ(follower->logEndOffset(), stuckLeo);
    EXPECT_EQ(followerLog.forcedRollsForTest(), 0u);
    // A ready segment arrives: the follower catches up.
    for (int i = 0; i < 40 && follower->logEndOffset() < leader->logEndOffset(); ++i) {
        (void)followerLog.provisionReady(1);
        (void)followerLog.finalizeStagingRenames();
        (void)followerLog.drainStagingRenamesDone();
        drive(10);
    }
    EXPECT_EQ(follower->logEndOffset(), leader->logEndOffset());
}
