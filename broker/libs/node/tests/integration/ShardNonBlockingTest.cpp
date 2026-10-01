// ShardNonBlockingTest.cpp — spec §3.1: no synchronous disk call on a shard loop;
// background flush tokens can never fire a produce ack (token routing); fsync
// errors are FAIL-STOP (fsyncgate: sticky per-log failure, pending acks failed,
// runtime signalled to halt).
//
// Every wait is a condition wait with a hard iteration cap (never a sleep or a
// fixed spin).  The Strict-tier Aeron test drives the real shard pass (ShardAgent:
// own Invoker driver + own client conductor + Worker) on the test thread — the
// one and only pumper of that driver.  The other tests feed produce frames
// straight into the worker's ingress handler (handleIngressFrameForTest) and
// control completion delivery (setReapPausedForTest / deliverCompletionForTest)
// so durability ordering is asserted deterministically.
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/ShardAgent.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <memory>
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
constexpr int16_t kStorageError = static_cast<int16_t>(pc::ErrorCode::StorageError);

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
std::vector<uint8_t> batchAt(int64_t base, int n = 1, std::size_t valueBytes = 1) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < n; ++i) b.append("k", v, 1000 + i);
    return b.build();
}
PartitionConfig cfg(rune::replication::NodeId self, uint8_t tier = 1, int32_t topicId = 1, int32_t part = 0) {
    PartitionConfig c; c.topicId = topicId; c.partitionId = part; c.selfId = self; c.minIsr = 1; c.durabilityTier = tier;
    c.fetchIntervalNs = 1'000 * kMs; c.isrCheckIntervalNs = 1'000 * kMs; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
void applyOneBatch(PartitionReplica& follower, int64_t base) {
    const auto b = batchAt(base);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, 1,
                                             std::span<const uint8_t>(b)};
    follower.applyReplicated(resp);
}
// One produce frame (full client wire frame) as WorkerIngress would deliver it.
std::vector<uint8_t> produceFrame(int64_t corr, const std::vector<uint8_t>& batch, uint8_t acks,
                                  int32_t topicId = 1, int32_t part = 0,
                                  int64_t pid = -1, int32_t seq = -1) {
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = topicId; msg.partition = part;
    msg.partitionKnown = 1; msg.acks = acks; msg.producerId = pid; msg.producerEpoch = 0;
    msg.baseSequence = seq; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}
struct Acks {
    int n = 0;
    std::vector<WorkerReturn> rets;
    int drain(Worker& w) {
        return static_cast<int>(w.drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult) { ++n; rets.push_back(r); }
        }));
    }
};
// Shard worker over one leader replica (single-ISR), produce via the ingress handler.
struct StrictShard {
    TmpDir d;
    std::unique_ptr<Log> log;
    NullTransport t;
    std::unique_ptr<PartitionReplica> leader;
    Worker w{0, 1 << 16, 1 << 16, 64, 64};
    explicit StrictShard(int64_t rollBytes = 64LL * 1024 * 1024,
                         std::vector<rune::replication::NodeId> isr = {1}) {
        log = std::make_unique<Log>(d.path.string(), rollBytes);
        log->recover();
        leader = std::make_unique<PartitionReplica>(cfg(1, /*tier=*/2), Role::Leader, 1, isr, isr, log.get(), &t);
        leader->setExternalFollowerFlush(true);
        w.setOwnedDirect(Worker::OwnedSet{leader.get()});
        w.setWorkerIngress(nullptr, true);
        w.setShardMode(true);
    }
    int produce(int64_t corr, const std::vector<uint8_t>& batch, uint8_t acks, int64_t now,
                int64_t pid = -1, int32_t seq = -1) {
        const auto f = produceFrame(corr, batch, acks, 1, 0, pid, seq);
        return w.handleIngressFrameForTest(f.data(), f.size(), now);
    }
};
} // namespace

// ============================================================================
// Never-block: no inline fsync on the shard loop.
// ============================================================================

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
    EXPECT_EQ(w.lastSyncedLeoForTest(&log), log.logEndOffset())
        << "a follower-flush success advances the log's synced LEO (no second fsync)";
    w.doWork(40 * kMs);                                    // owned-log sync interval: LEO unchanged
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "no double fsync of an already-synced follower log";
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
    StrictShard s;
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(16, 1)));   // shard: no inline fsync
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "replica append (txn-marker path) never fsyncs inline in shard mode";
    s.w.doWork(1 * kMs);                                   // arms the sync interval (+ marker-style request)
    for (int i = 0; i < kCap && s.w.bgFlushInFlightForTest() > 0; ++i) { s.w.doWork(2 * kMs); std::this_thread::yield(); }
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(16, 2)));
    const uint64_t before = s.w.bgFlushSubmitsForTest();
    s.w.doWork(12 * kMs);                                  // > 10 ms: sync due
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
    ASSERT_GE(s.w.bgFlushSubmitsForTest(), before);
    for (int i = 0; i < kCap && s.w.bgFlushInFlightForTest() > 0; ++i) { s.w.doWork(13 * kMs); std::this_thread::yield(); }
    ASSERT_EQ(s.w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), s.log->logEndOffset());
    const uint64_t settled = s.w.bgFlushSubmitsForTest();
    const int work = s.w.doWork(30 * kMs);                 // next interval, no new appends
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), settled) << "an unchanged log is not resubmitted (no phantom work)";
    EXPECT_EQ(work, 0) << "an idle shard reports no work (backoff can park)";
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, TxnMarkerAppendIsAsyncInShardMode) {
    StrictShard s;
    const auto marker = batchAt(0);
    WorkerOp op;
    op.type = WorkerOp::Type::AppendControlRecord; op.topicId = 1; op.partition = 0;
    op.replica = s.leader.get(); op.logPtr = s.log.get();
    op.recordBatchBytes = marker;
    ASSERT_TRUE(s.w.tryEnqueueOp(std::move(op)));
    s.w.doWork(1);
    EXPECT_GT(s.log->logEndOffset(), 0) << "the marker was appended";
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "the marker append must not fsync inline on the shard loop";
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 1u) << "its durability is a background io token";
    for (int i = 0; i < kCap && s.w.bgFlushInFlightForTest() > 0; ++i) { s.w.doWork(2); std::this_thread::yield(); }
    EXPECT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), s.log->logEndOffset());
}

// ============================================================================
// Token routing.
// ============================================================================

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
    w.setReapPausedForTest(true);                          // only synthetic deliveries until unpaused
    w.doWork(1);                                           // append + one coalesced submit (token T1)
    const auto t1 = w.durablePendingTokenForTest(0);
    ASSERT_NE(t1, kNoToken);
    const auto t2 = w.submitBgFlushForTest(&log);          // a background flush on the SAME log
    ASSERT_NE(t2, kNoToken);
    ASSERT_NE(t1, t2);
    Acks a;
    w.deliverCompletionForTest(FlushCompletion{t2, 0, log.flushBoundary()});
    a.drain(w);
    EXPECT_EQ(a.n, 0) << "a background flush token must never fire a produce ack";
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u) << "the background token is retired by routing";
    w.deliverCompletionForTest(FlushCompletion{t1, 0, log.flushBoundary()});
    a.drain(w);
    EXPECT_EQ(a.n, 1);
    EXPECT_EQ(w.durableAckTokenForTest(), t1);
    // The real completions for T1 and T2 arrive later through io_.reap: wait for
    // exactly those two (condition, capped) and prove neither fires anything.
    w.setReapPausedForTest(false);
    const uint64_t reapedBefore = w.ioCompletionsReapedForTest();
    for (int i = 0; i < kCap && w.ioCompletionsReapedForTest() < reapedBefore + 2; ++i) {
        w.doWork(2 + i); a.drain(w); std::this_thread::yield();
    }
    ASSERT_EQ(w.ioCompletionsReapedForTest(), reapedBefore + 2);
    a.drain(w);
    EXPECT_EQ(a.n, 1) << "late real completions for T1/T2 must be ignored";
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, BackgroundFlushRequestedWhileInFlightIsResubmittedOnRetire) {
    StrictShard s;
    s.w.setReapPausedForTest(true);
    const auto t1 = s.w.submitBgFlushForTest(s.log.get());
    ASSERT_NE(t1, kNoToken);
    EXPECT_EQ(s.w.submitBgFlushForTest(s.log.get()), kNoToken) << "coalesced into the in-flight flush";
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));   // newer data meanwhile
    EXPECT_EQ(s.w.submitBgFlushForTest(s.log.get()), kNoToken) << "coalesced: marked dirty-again";
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 1u);
    EXPECT_EQ(s.w.bgFlushInFlightForTest(), 1u);
    s.w.deliverCompletionForTest(FlushCompletion{t1, 0, 0});
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 2u) << "re-requested flush (newer data) resubmitted on retire";
    EXPECT_EQ(s.w.bgFlushInFlightForTest(), 1u);
    s.w.setReapPausedForTest(false);
    for (int i = 0; i < kCap && s.w.bgFlushInFlightForTest() > 0; ++i) { s.w.doWork(1); std::this_thread::yield(); }
    EXPECT_EQ(s.w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 2u);
}

TEST(ShardNonBlockingRegression, StrictAckNotWedgedByTokenAliasingRegression) {
    // I2: completions are matched by token ONLY.  A completion whose reported
    // flushBoundary is unrelated to the entry (io_uring reports none; an aliased
    // token→boundary table reported ANOTHER log's boundary) must still fire.
    StrictShard a;
    StrictShard b;
    a.w.setReapPausedForTest(true);
    ASSERT_EQ(a.produce(1, batchAt(0, 5), /*acks=*/0, 1), 1);
    a.w.doWork(1);
    const auto ta = a.w.durablePendingTokenForTest(0);
    ASSERT_NE(ta, kNoToken);
    Acks acks;
    a.w.deliverCompletionForTest(FlushCompletion{ta, 0, /*flushBoundary (aliased / unknown)=*/0});
    a.w.doWork(2);
    acks.drain(a.w);
    ASSERT_EQ(acks.n, 1) << "an ack is never wedged by the completion's reported boundary";
    EXPECT_EQ(acks.rets[0].errorCode, 0);
    EXPECT_EQ(acks.rets[0].lastOffset, 4);
}

// ============================================================================
// Strict tier (shard): ack only after its OWN fsync; acks=all then HW.
// ============================================================================

TEST(ShardNonBlockingRegression, StrictAcksAllWaitsForFsyncThenHw) {
    StrictShard s(64LL * 1024 * 1024, {1, 2});            // follower 2 in ISR: HW needs its fetch
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(7, batchAt(0, 3), /*acks=all*/1, 1), 1);
    s.w.doWork(1);
    const auto tok = s.w.durablePendingTokenForTest(0);
    ASSERT_NE(tok, kNoToken);
    Acks a;
    s.w.doWork(2); a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "no ack before the fsync completion";
    s.w.deliverCompletionForTest(FlushCompletion{tok, 0, 0});
    s.w.doWork(3); a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "fsync done but HW has not passed the batch: still parked (acks=all)";
    rune::replication::FetchRequestMsg fr{1, 0, 2, s.log->logEndOffset(), 1 << 20, 1};
    (void)s.leader->onFetchRequest(fr, 4);                 // follower caught up → HW advances
    for (int i = 0; i < 100 && a.n == 0; ++i) { s.w.doWork(5 + i); a.drain(s.w); }
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, 0);
    EXPECT_EQ(s.w.durableAckTokenForTest(), tok);
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, StrictDuplicateIsParkedOnFsync) {
    StrictShard s;
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0, 2), 0, 1, /*pid=*/7, /*seq=*/0), 1);
    s.w.doWork(1);
    const auto t1 = s.w.durablePendingTokenForTest(0);
    Acks a;
    // Retry of the same idempotent batch BEFORE the original's fsync completed.
    ASSERT_EQ(s.produce(2, batchAt(0, 2), 0, 2, 7, 0), 1);
    s.w.doWork(2); a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "a Strict duplicate must not be acked before the data is durable";
    EXPECT_EQ(s.w.durablePendingCountForTest(), 2u);
    s.w.deliverCompletionForTest(FlushCompletion{t1, 0, 0});
    const auto t2 = s.w.durablePendingTokenForTest(0);
    s.w.deliverCompletionForTest(FlushCompletion{t2, 0, 0});
    s.w.doWork(3); a.drain(s.w);
    ASSERT_EQ(a.n, 2);
    for (const auto& r : a.rets) EXPECT_EQ(r.errorCode, 0);
}

TEST(ShardNonBlockingRegression, StrictParkedAcksReserveReturnSlots) {
    // G-A6: Strict acks parked on their fsync fire into the return ring later —
    // the intake reservation must count them (never poll what cannot be acked).
    StrictShard s;
    s.w.setReapPausedForTest(true);
    for (int i = 0; i < 5; ++i) ASSERT_EQ(s.produce(i, batchAt(0), 0, 1), 1);
    s.w.doWork(1);
    EXPECT_EQ(s.w.durablePendingCountForTest(), 5u);
    EXPECT_EQ(s.w.reservedAckSlotsForTest(), 5) << "parked Strict acks reserve return-ring slots";
}

TEST(ShardNonBlockingRegression, StrictTierAcksFromItsOwnCompletionWithoutInlineSync) {
    TmpDir d, ad;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    leader.setExternalFollowerFlush(true);
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
    const auto frame = produceFrame(9, batchAt(0), /*acks=*/1);
    aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(frame.data()), static_cast<aeron::util::index_t>(frame.size()));
    w.setReapPausedForTest(true);                          // hold completions: the append's token is observable
    ASSERT_GT(pub->offer(ab, 0, static_cast<aeron::util::index_t>(frame.size())), 0);
    Acks a;
    for (int i = 0; i < kCap && w.durablePendingCountForTest() == 0; ++i) { shard.doWork(now += 1'000); a.drain(w); }
    ASSERT_EQ(w.durablePendingCountForTest(), 1u);
    const auto own = w.durablePendingTokenForTest(0);
    ASSERT_NE(own, kNoToken);
    for (int i = 0; i < 1'000; ++i) { shard.doWork(now += 1'000); a.drain(w); }
    EXPECT_EQ(a.n, 0) << "no ack while its fsync completion is held";
    w.setReapPausedForTest(false);
    for (int i = 0; i < kCap && a.n == 0; ++i) { shard.doWork(now += 1'000); a.drain(w); }
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, 0);
    EXPECT_EQ(w.durableAckTokenForTest(), own) << "the ack fired from its OWN token's completion";
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "Strict tier must not fsync inline on the shard loop";
    pub.reset();
}

// ============================================================================
// Roll seal: async in shard mode; the sealed segment's fd outlives its flush.
// ============================================================================

TEST(ShardNonBlockingRegression, RollSealIsAsyncAndGatesStrictAcks) {
    StrictShard s(/*rollBytes=*/512);
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0, 1, 600), 0, 1), 1);  // segment A (>= rollBytes)
    ASSERT_EQ(s.produce(2, batchAt(0, 1, 16), 0, 1), 1);   // rolls: A sealed, appended to B
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "the seal fsync is not inline in shard mode";
    s.w.doWork(1);
    EXPECT_EQ(s.w.sealSubmitsForTest(), 1u) << "the sealed segment is flushed through the io backend";
    ASSERT_EQ(s.w.durablePendingCountForTest(), 2u);
    const auto active = s.w.durablePendingTokenForTest(0);
    Acks a;
    s.w.deliverCompletionForTest(FlushCompletion{active, 0, 0});
    s.w.doWork(2); a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "a Strict ack never fires while the sealed segment it may live in is unflushed";
    s.w.setReapPausedForTest(false);
    for (int i = 0; i < kCap && a.n < 2; ++i) { s.w.doWork(3 + i); a.drain(s.w); std::this_thread::yield(); }
    ASSERT_EQ(a.n, 2);
    for (const auto& r : a.rets) EXPECT_EQ(r.errorCode, 0);
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, SegmentWithInFlightFlushDefersItsClose) {
    // M1: a segment removed (truncate / retention / reload) while a flush token is
    // in flight on its fd keeps the fd open until that completion; no fsync is
    // ever issued on a closed (possibly reused) fd.
    StrictShard s;
    ASSERT_EQ(s.produce(1, batchAt(0, 3), 0, 1), 1);
    s.w.setReapPausedForTest(true);
    s.w.doWork(1);
    const auto tok = s.w.durablePendingTokenForTest(0);
    const int fd = s.log->activeSegmentFd();
    s.log->truncateTo(0);                                   // removes the active segment object
    EXPECT_EQ(s.log->retiredSegmentsForTest(), 1u) << "the pinned segment is retired, not destroyed";
    EXPECT_NE(::fcntl(fd, F_GETFD), -1) << "its fd stays open while the flush is in flight";
    s.w.deliverCompletionForTest(FlushCompletion{tok, 0, 0});
    EXPECT_EQ(s.log->retiredSegmentsForTest(), 0u) << "closed on the completion that unpins it";
}

// ============================================================================
// I3 / M2: per-log flush slots.
// ============================================================================

TEST(ShardNonBlockingRegression, ManyLogsSaturationIsBoundedAndQuiet) {
    // More dirty logs than the io backend's in-flight depth (16): refused submits
    // are NOT retried every pass (no retry table, no warn storm) — the next sync
    // interval re-requests; every log is eventually synced.
    constexpr int kLogs = 300;
    TmpDir d;
    NullTransport t;
    std::vector<std::unique_ptr<Log>> logs;
    std::vector<std::unique_ptr<PartitionReplica>> reps;
    Worker::OwnedSet owned;
    for (int i = 0; i < kLogs; ++i) {
        auto dir = d.path / std::to_string(i);
        fs::create_directories(dir);
        logs.push_back(std::make_unique<Log>(dir.string(), 1 << 20));
        logs.back()->recover();
        reps.push_back(std::make_unique<PartitionReplica>(cfg(1, 1, 1, i), Role::Leader, 1,
            std::vector<rune::replication::NodeId>{1}, std::vector<rune::replication::NodeId>{1},
            logs.back().get(), &t));
        reps.back()->setExternalFollowerFlush(true);
        owned.push_back(reps.back().get());
    }
    Worker w(0, 1 << 16, 1 << 16, /*ioSubmitDepth=*/16, /*ioComplDepth=*/16);
    w.setOwnedDirect(owned);
    w.setShardMode(true);
    for (auto& r : reps) r->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));
    EXPECT_EQ(w.flushSlotCountForTest(), static_cast<std::size_t>(kLogs));
    int64_t now = 1 * kMs;
    w.doWork(now);                                         // replica requests: ≤ 16 accepted, rest deferred
    EXPECT_LE(w.bgFlushInFlightForTest(), 16u);
    const uint64_t deferredAfterFirst = w.bgFlushDeferredForTest();
    for (int i = 0; i < 100; ++i) w.doWork(now);           // same interval: no per-pass retry
    EXPECT_EQ(w.bgFlushDeferredForTest(), deferredAfterFirst) << "no per-pass retry spin";
    int intervals = 0;
    auto allSynced = [&] {
        for (int i = 0; i < kLogs; ++i)
            if (w.lastSyncedLeoForTest(logs[i].get()) != logs[i]->logEndOffset()) return false;
        return true;
    };
    for (int i = 0; i < kCap && !allSynced(); ++i) {
        if (w.bgFlushInFlightForTest() == 0) { now += 11 * kMs; ++intervals; }
        w.doWork(now);
        std::this_thread::yield();
    }
    EXPECT_TRUE(allSynced());
    EXPECT_LE(intervals, kLogs / 16 + 3);
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
    for (auto& l : logs) EXPECT_EQ(l->syncCallsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, OwnershipChangePurgesFlushSlots) {
    StrictShard a;
    TmpDir d2;
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    PartitionReplica other(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &log2, &a.t);
    a.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));
    a.w.setReapPausedForTest(true);
    const auto tok = a.w.submitBgFlushForTest(a.log.get());
    ASSERT_NE(tok, kNoToken);
    WorkerOp op;                                            // runtime ownership change: {other} only
    op.type = WorkerOp::Type::SetOwnership;
    op.ownedSet = {&other};
    ASSERT_TRUE(a.w.tryEnqueueOp(std::move(op)));
    a.w.doWork(1);
    EXPECT_EQ(a.w.flushSlotCountForTest(), 1u);
    EXPECT_EQ(a.w.lastSyncedLeoForTest(a.log.get()), -1) << "no slot holds a log this worker no longer owns";
    a.w.deliverCompletionForTest(FlushCompletion{tok, 0, 0});   // stale-generation completion: ignored safely
    EXPECT_EQ(a.w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(a.w.lastSyncedLeoForTest(&log2), 0);
}

// ============================================================================
// C1: RUNE_IO_BACKEND=portable is the REAL portable backend (Linux included).
// ============================================================================

TEST(ShardNonBlockingRegression, PortableBackendFsyncsAndAcksStrict) {
    ::setenv("RUNE_IO_BACKEND", "portable", 1);
    {
        StrictShard s;
        EXPECT_STREQ(s.w.ioBackendNameForTest(), "portable");
        EXPECT_TRUE(s.w.ioAcceptsSubmissions());
        ASSERT_EQ(s.produce(1, batchAt(0, 2), 0, 1), 1);
        Acks a;
        for (int i = 0; i < kCap && a.n == 0; ++i) { s.w.doWork(1 + i); a.drain(s.w); std::this_thread::yield(); }
        ASSERT_EQ(a.n, 1);
        EXPECT_EQ(a.rets[0].errorCode, 0);
        EXPECT_NE(s.w.durableAckTokenForTest(), kNoToken) << "acked from a real completion";
        s.w.doWork(20 * kMs);
        s.w.doWork(40 * kMs);                               // owned-log sync due
        for (int i = 0; i < kCap && s.w.lastSyncedLeoForTest(s.log.get()) != s.log->logEndOffset(); ++i) {
            s.w.doWork(41 * kMs); std::this_thread::yield();
        }
        EXPECT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), s.log->logEndOffset());
        EXPECT_EQ(s.log->syncCallsForTest(), 0u);
    }
    ::unsetenv("RUNE_IO_BACKEND");
}

TEST(ShardNonBlockingRegression, UnknownIoBackendCannotSubmit) {
    ::setenv("RUNE_IO_BACKEND", "bogus", 1);
    {
        Worker w(0, 1 << 16, 1 << 16, 64, 64);
        EXPECT_FALSE(w.ioAcceptsSubmissions()) << "an unknown backend selection never silently no-ops";
    }
    ::unsetenv("RUNE_IO_BACKEND");
}

// ============================================================================
// C2: fsync errors are FAIL-STOP.
// ============================================================================

TEST(StorageFailStopRegression, FsyncErrorThenSuccessNeverAcksOkRegression) {
    StrictShard s;
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0, 2), 0, 1, /*pid=*/3, /*seq=*/0), 1);
    s.w.doWork(1);
    const auto tok = s.w.durablePendingTokenForTest(0);
    std::vector<WorkerReturn> all;
    auto drainAll = [&] { s.w.drainReturn([&](WorkerReturn r) { all.push_back(r); }); };
    s.w.deliverCompletionForTest(FlushCompletion{tok, EIO, 0});
    s.w.deliverCompletionForTest(FlushCompletion{tok, 0, 0});   // a later "success" for the same data
    s.w.doWork(2);
    drainAll();
    ASSERT_TRUE(s.log->storageFailed()) << "the first fsync error permanently fails the log";
    EXPECT_EQ(s.log->storageErrno(), EIO);
    int produceResults = 0, failures = 0;
    for (const auto& r : all) {
        if (r.type == WorkerReturn::Type::ProduceResult) {
            ++produceResults;
            EXPECT_EQ(r.errorCode, kStorageError) << "the original Strict entry fails — never OK";
        } else if (r.type == WorkerReturn::Type::StorageFailure) {
            ++failures;
            EXPECT_EQ(r.errorCode, EIO);
            EXPECT_EQ(r.topicId, 1);
        }
    }
    EXPECT_EQ(produceResults, 1);
    EXPECT_EQ(failures, 1) << "the runtime is signalled exactly once (halt)";
    all.clear();
    ASSERT_EQ(s.produce(2, batchAt(0, 2), 0, 3, 3, 0), 1);     // idempotent retry of the failed batch
    ASSERT_EQ(s.produce(3, batchAt(0, 1), 0, 3), 1);           // a new batch
    s.w.doWork(3);
    drainAll();
    ASSERT_EQ(all.size(), 2u);
    for (const auto& r : all) {
        EXPECT_EQ(r.type, WorkerReturn::Type::ProduceResult);
        EXPECT_EQ(r.errorCode, kStorageError) << "a failed log refuses every produce, duplicates included";
    }
    EXPECT_EQ(s.w.durablePendingCountForTest(), 0u);
    EXPECT_EQ(s.w.storageFailureSignalsForTest(), 1u);
}

TEST(StorageFailStopRegression, BackgroundFlushErrorFailStopsWithoutRetry) {
    StrictShard s;
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));
    s.w.setReapPausedForTest(true);
    const auto tok = s.w.submitBgFlushForTest(s.log.get());
    s.w.deliverCompletionForTest(FlushCompletion{tok, EIO, 0});
    EXPECT_TRUE(s.log->storageFailed());
    EXPECT_EQ(s.w.bgFlushErrorsForTest(), 1u);
    for (int i = 0; i < 50; ++i) s.w.doWork(30 * kMs + i * 11 * kMs);
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 1u) << "an fsync error is never retried";
    EXPECT_EQ(s.w.storageFailureSignalsForTest(), 1u);
    int failures = 0;
    s.w.drainReturn([&](WorkerReturn r) { failures += r.type == WorkerReturn::Type::StorageFailure; });
    EXPECT_EQ(failures, 1);
    EXPECT_THROW(s.log->appendNoSync(batchAt(1).data(), batchAt(1).size(), 1), std::exception)
        << "the failed log refuses appends at the storage layer too";
}

TEST(StorageFailStopRegression, LegacySyncFailurePropagatesRegression) {
    // Legacy (non-shard): LogSegment::sync checks fsync's result, the Log goes
    // permanently failed, and the owning worker signals the runtime to halt.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    leader.append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));
    EXPECT_EQ(log.syncCallsForTest(), 1u) << "the counter sits on LogSegment::sync (legacy append fsyncs)";
    log.setSyncFaultForTest(EIO);
    EXPECT_THROW(log.sync(), std::system_error);
    EXPECT_TRUE(log.storageFailed());
    log.setSyncFaultForTest(0);
    EXPECT_THROW(log.sync(), std::system_error) << "sticky: a later sync never 'succeeds' over lost pages";
    w.doWork(1);
    int failures = 0;
    w.drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::StorageFailure) { ++failures; EXPECT_EQ(r.errorCode, EIO); }
    });
    EXPECT_EQ(failures, 1) << "legacy mode fail-stops too";
}
