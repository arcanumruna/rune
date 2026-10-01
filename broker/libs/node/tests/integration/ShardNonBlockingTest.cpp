// ShardNonBlockingTest.cpp — spec §3.1: no synchronous disk call on a shard loop;
// background flush tokens can never fire a produce ack (token routing).
//
// Every wait is a condition wait with a hard iteration cap (never a sleep or a
// fixed spin).  The Strict-tier test drives the real shard pass (ShardAgent:
// own Invoker driver + own client conductor + Worker) on the test thread — the
// one and only pumper of that driver.
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/ShardAgent.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
using rune::storage::Log;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int64_t kMs = 1'000'000;
constexpr int kCap = 2'000'000;   // hard cap on any condition-wait loop

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_shnb_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {}
};
std::vector<uint8_t> batchAt(int64_t base, int n = 1) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < n; ++i) b.append("k", "v", 1000 + i);
    return b.build();
}
PartitionConfig cfg(rune::replication::NodeId self, uint8_t tier = 1) {
    PartitionConfig c; c.topicId = 1; c.selfId = self; c.minIsr = 1; c.durabilityTier = tier;
    c.fetchIntervalNs = 1'000 * kMs; c.isrCheckIntervalNs = 1'000 * kMs; c.pushOnAppend = false;
    return c;
}
void applyOneBatch(PartitionReplica& follower, int64_t base) {
    const auto b = batchAt(base);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, 1,
                                             std::span<const uint8_t>(b)};
    follower.applyReplicated(resp);
}
} // namespace

TEST(ShardNonBlockingRegression, FollowerFlushNeverSyncsOnTheWorkerThread) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    w.setShardMode(true);
    applyOneBatch(follower, 0);                            // follower now dirty
    ASSERT_GT(log.logEndOffset(), 0);
    w.doWork(25 * kMs);                                    // past the 20 ms flush interval
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "the follower flush must not block the shard loop";
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "it is submitted to the io backend instead";
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) {
        w.doWork(26 * kMs);
        std::this_thread::yield();
    }
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, LegacyFollowerFlushStillSyncsInline) {
    // Legacy (non-shard) mode is unchanged: the replica fsyncs inline.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    applyOneBatch(follower, 0);
    (void)follower.doWork(25 * kMs);
    EXPECT_EQ(log.syncCallsForTest(), 1u);
    EXPECT_FALSE(follower.takeFollowerFlushRequest()) << "no request is raised in legacy mode";
}

TEST(ShardNonBlockingRegression, OwnedLogSyncIsAsyncAndIdleLogIsNotResubmitted) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setShardMode(true);
    const std::vector<uint8_t> payload(16, 1);
    leader.append(std::span<const uint8_t>(payload));
    const uint64_t syncsAfterAppend = log.syncCallsForTest();   // append() itself is the legacy syncing API
    w.doWork(1 * kMs);                                     // arms the sync interval
    w.doWork(12 * kMs);                                    // > 10 ms: sync due
    EXPECT_EQ(log.syncCallsForTest(), syncsAfterAppend) << "the shard loop never calls Log::sync";
    ASSERT_EQ(w.bgFlushSubmitsForTest(), 1u);
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) { w.doWork(13 * kMs); std::this_thread::yield(); }
    ASSERT_EQ(w.bgFlushInFlightForTest(), 0u);
    const int work = w.doWork(30 * kMs);                   // next interval, no new appends
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "an unchanged log is not resubmitted (no phantom work)";
    EXPECT_EQ(work, 0) << "an idle shard reports no work (backoff can park)";
    EXPECT_EQ(log.syncCallsForTest(), syncsAfterAppend);
}

TEST(ShardNonBlockingRegression, BackgroundFlushCompletionNeverFiresProduceAck) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
    op.replica = &leader; op.logPtr = &log; op.sessionId = 1; op.correlationId = 1;
    op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 2;
    op.recordBatchBytes = batchAt(0);
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    w.doWork(1);                                           // append + one coalesced submit (token T1)
    const auto t1 = w.durablePendingTokenForTest(0);
    ASSERT_NE(t1, kNoToken);
    const auto t2 = w.submitBgFlushForTest(&log);          // a background flush on the SAME log
    ASSERT_NE(t2, kNoToken);
    ASSERT_NE(t1, t2);
    int acks = 0;
    auto drain = [&] { return static_cast<int>(w.drainReturn([&](WorkerReturn) { ++acks; })); };
    // Same token routing a real reap uses; boundary covers T1's entry, so only
    // routing (not the boundary check) can keep it from firing.
    w.deliverCompletionForTest(FlushCompletion{t2, 0, log.flushBoundary()});
    drain();
    EXPECT_EQ(acks, 0) << "a background flush token must never fire a produce ack";
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u) << "the background token is retired by routing";
    w.deliverCompletionForTest(FlushCompletion{t1, 0, log.flushBoundary()});
    drain();
    EXPECT_EQ(acks, 1);
    // The real completions for T1 and T2 arrive later through io_.reap: wait for
    // exactly those two (condition, capped) and prove neither fires anything.
    const uint64_t reapedBefore = w.ioCompletionsReapedForTest();
    for (int i = 0; i < kCap && w.ioCompletionsReapedForTest() < reapedBefore + 2; ++i) {
        w.doWork(2 + i); drain(); std::this_thread::yield();
    }
    ASSERT_EQ(w.ioCompletionsReapedForTest(), reapedBefore + 2);
    drain();
    EXPECT_EQ(acks, 1) << "late real completions for T1/T2 must be ignored";
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, BackgroundFlushRequestedWhileInFlightIsResubmittedOnRetire) {
    // A second request for a (log, kind) already in flight is NOT dropped: the
    // in-flight entry is marked and resubmitted (covering the newer data) when
    // its completion retires it.  No per-pass retry spinning meanwhile.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setShardMode(true);
    const auto t1 = w.submitBgFlushForTest(&log);
    ASSERT_NE(t1, kNoToken);
    EXPECT_EQ(w.submitBgFlushForTest(&log), kNoToken) << "coalesced into the in-flight flush";
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u);
    EXPECT_EQ(w.bgFlushInFlightForTest(), 1u);
    w.deliverCompletionForTest(FlushCompletion{t1, 0, log.flushBoundary()});
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 2u) << "re-requested flush resubmitted on retire";
    EXPECT_EQ(w.bgFlushInFlightForTest(), 1u);
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) { w.doWork(1); std::this_thread::yield(); }
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 2u);
}

TEST(ShardNonBlockingRegression, StrictTierAcksFromCompletionWithoutInlineSync) {
    TmpDir d, ad;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Invoker);
    aeron::Context ctx; ctx.aeronDir(ad.path.string()); ctx.useConductorAgentInvoker(true);
    auto client = aeron::Aeron::connect(ctx);
    WorkerIngress ing(client, "aeron:ipc", 7101, drv.pumpWith(client.get()));
    ASSERT_TRUE(ing.resolved());
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setWorkerIngress(&ing, true);
    w.setShardMode(true);
    rune::consensus::MonotonicNanoClock clk;
    ShardAgent shard(w, drv, *client, clk);                // the ONE pumper of drv
    const int64_t preg = client->addExclusivePublication("aeron:ipc", 7101);
    std::shared_ptr<aeron::ExclusivePublication> pub;
    int64_t now = 1'000;
    for (int i = 0; i < kCap && !(pub && pub->isConnected()); ++i) {
        shard.doWork(now += 1'000);
        if (!pub) pub = client->findExclusivePublication(preg);
    }
    ASSERT_TRUE(pub && pub->isConnected());
    pc::Produce msg; const auto b = batchAt(0);
    msg.sessionId = 1; msg.correlationId = 9; msg.topicId = 1; msg.partition = 0; msg.partitionKnown = 1;
    msg.acks = 1; msg.recordBatch = std::span<const uint8_t>(b);
    std::vector<uint8_t> frame(msg.encodedSize()); msg.encode(frame.data());
    aeron::concurrent::AtomicBuffer ab(frame.data(), static_cast<aeron::util::index_t>(frame.size()));
    ASSERT_GT(pub->offer(ab, 0, static_cast<aeron::util::index_t>(frame.size())), 0);
    int acks = 0; int16_t err = -1;
    uint64_t reapedAtAck = 0;
    for (int i = 0; i < kCap && acks == 0; ++i) {
        shard.doWork(now += 1'000);
        w.drainReturn([&](WorkerReturn r) { ++acks; err = r.errorCode; reapedAtAck = w.ioCompletionsReapedForTest(); });
    }
    ASSERT_EQ(acks, 1);
    EXPECT_EQ(err, 0);
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "Strict tier must not fsync inline on the shard loop";
    EXPECT_GE(reapedAtAck, 1u) << "the Strict ack fires only after its own flush completion was reaped";
    EXPECT_GE(w.ioSubmitCountForTest(), 1u);
    pub.reset();
}
