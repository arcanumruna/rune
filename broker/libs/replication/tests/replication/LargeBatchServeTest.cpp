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

#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
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

    void sendFetchResponse(NodeId to, const FetchResponseMsg& resp) override {
        fetchResponses.emplace_back(resp.records.begin(), resp.records.end());
        InMemoryTransport::sendFetchResponse(to, resp);
    }
    void sendReconcileResponse(NodeId to, const FetchResponseMsg& resp) override {
        reconcileResponses.emplace_back(resp.records.begin(), resp.records.end());
        InMemoryTransport::sendReconcileResponse(to, resp);
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

// Reconcile / catch-up serve path: an ahead survivor serves the oversized first
// batch whole even when the pull's maxBytes is smaller.
TEST_F(LargeBatchReplicationRegression, ReconcileServesWholeOversizedFirstBatch) {
    build(false, false);
    appendBigThenSmalls();
    leader->onReconcileRequest(fetchReq(0, kMaxBytes));
    ASSERT_EQ(transport.reconcileResponses.size(), 1u);
    const auto served = batchesOf(transport.reconcileResponses.back());
    ASSERT_EQ(served.size(), 1u);
    EXPECT_EQ(served[0].base, 0);
    EXPECT_GT(served[0].bytes, static_cast<std::size_t>(kMaxBytes));

    leader->onReconcileRequest(fetchReq(1, kMaxBytes));
    ASSERT_EQ(transport.reconcileResponses.size(), 2u);
    EXPECT_EQ(batchesOf(transport.reconcileResponses.back()).size(), 2u);
}
