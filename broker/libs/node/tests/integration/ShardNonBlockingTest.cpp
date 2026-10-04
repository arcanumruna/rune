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
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/BrokerNode.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/node/ShardAgent.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/ReplicationFrameLimits.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

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
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
std::vector<uint8_t> batchAt(int64_t base, int n = 1, std::size_t valueBytes = 1,
                             int64_t pid = -1, int32_t seq = -1) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    // An idempotent batch's header carries the frame's identity (the broker
    // rejects a frame/header mismatch — EOS-DUP defense).
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < n; ++i) b.append("k", v, 1000 + i);
    return b.build();
}
// Round 4: production routing (WorkerPool::tryEnqueueOp) stamps the target
// identity from the registry; tests that enqueue straight into a Worker stamp
// it from the (test-owned, live) replica the same way.
WorkerOp stamp(WorkerOp op) {
    if (op.replica) { op.targetReplicaId = op.replica->registryId(); op.incarnation = op.replica->incarnation(); }
    return op;
}
PartitionConfig cfg(rune::replication::NodeId self, uint8_t tier = 1, int32_t topicId = 1, int32_t part = 0) {
    PartitionConfig c; c.topicId = topicId; c.partitionId = part; c.selfId = self; c.minIsr = 1; c.durabilityTier = tier;
    c.fetchIntervalNs = 1'000 * kMs; c.isrCheckIntervalNs = 1'000 * kMs; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
void applyOneBatch(PartitionReplica& follower, int64_t base, std::size_t valueBytes = 0) {
    const auto b = valueBytes ? batchAt(base, 1, valueBytes) : batchAt(base);
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
// Produce answers (F15 S5: every ProduceResult is an answer — the S2
// append-time facts-only return is deleted; one is a failure).
struct Acks {
    int n = 0;
    std::vector<WorkerReturn> rets;
    int drain(Worker& w) {
        return static_cast<int>(w.drainReturn([&](WorkerReturn r) {
            if (r.type != WorkerReturn::Type::ProduceResult) return;
            if (r.deferAck) ADD_FAILURE() << "a facts-only return (deleted by F15 S5)";
            ++n; rets.push_back(r);
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
    // F30 fix round 1: `shard == false` is thread placement (the default
    // WorkerMode) — the same worker-driven, never-inline durability rules hold.
    explicit StrictShard(int64_t rollBytes = 64LL * 1024 * 1024,
                         std::vector<rune::replication::NodeId> isr = {1}, bool shard = true) {
        log = std::make_unique<Log>(d.path.string(), rollBytes);
        log->recover();
        leader = std::make_unique<PartitionReplica>(cfg(1, /*tier=*/2), Role::Leader, 1, isr, isr, log.get(), &t);
        leader->setExternalFollowerFlush(true);
        w.setOwnedDirect(Worker::OwnedSet{leader.get()});
        w.setWorkerIngress(nullptr, true);
        w.setShardMode(shard);
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
    // A replica WITHOUT the external flush (internal partitions, driven on the
    // control thread) is unchanged: it fsyncs inline.  Every worker-driven user
    // replica uses the external flush in every placement (F30).
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

// ============================================================================
// F30: the same never-block rules hold for a NON-shard (thread-placement) worker.
// Before the fix the non-shard loop fsynced inline on the worker thread (follower
// flush every 20 ms + time-gated owned-log sync every 10 ms) and never submitted
// deferred roll seals — stalling every partition the worker owns for ms.
// ============================================================================

TEST(NonShardNonBlockingRegression, FollowerFlushIsSubmittedNotSyncedInlineRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    w.setWorkerIngress(nullptr, true);                     // per-worker ingress (the optimal config)
    applyOneBatch(follower, 0);
    ASSERT_GT(log.logEndOffset(), 0);
    w.doWork(25 * kMs);                                    // past the 20 ms follower flush interval
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "the follower flush must not block a non-shard worker";
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "it is submitted to the io backend";
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) { w.doWork(26 * kMs); std::this_thread::yield(); }
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
    EXPECT_EQ(w.lastSyncedLeoForTest(&log), log.logEndOffset());
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}

TEST(NonShardNonBlockingRegression, OwnedLogTimedSyncIsAsyncRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    leader.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setWorkerIngress(nullptr, true);
    w.doWork(1 * kMs);                                     // arms the owned-log sync interval
    leader.append(std::span<const uint8_t>(std::vector<uint8_t>(16, 1)));
    // F30 fix round 1 (F6): consume the append's flush request so ONLY the
    // timed owned-log sync can submit a flush for this log.
    ASSERT_TRUE(leader.takeFollowerFlushRequest());
    w.doWork(5 * kMs);                                     // < 10 ms: not due yet
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 0u) << "nothing submits before the owned-log sync interval";
    w.doWork(12 * kMs);                                    // > 10 ms: owned-log sync due
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "the timed owned-log sync must not fsync inline on a non-shard worker";
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "the timed owned-log sync submitted exactly one flush";
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) { w.doWork(13 * kMs); std::this_thread::yield(); }
    EXPECT_EQ(w.lastSyncedLeoForTest(&log), log.logEndOffset());
    const uint64_t settled = w.bgFlushSubmitsForTest();
    w.doWork(30 * kMs);                                    // next interval, unchanged log
    EXPECT_EQ(w.bgFlushSubmitsForTest(), settled) << "an unchanged log is not resubmitted";
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}

TEST(NonShardNonBlockingRegression, DeferredRollSealIsSubmittedRegression) {
    TmpDir d;
    Log log(d.path.string(), /*rollBytes=*/512); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);               // defers the roll seal to the worker
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    w.setWorkerIngress(nullptr, true);
    applyOneBatch(follower, 0, 600);                       // fills segment A
    applyOneBatch(follower, 1, 16);                        // rolls: A sealed (deferred)
    ASSERT_GT(log.pendingSealCount(), 0u);
    w.doWork(1 * kMs);
    EXPECT_EQ(w.sealSubmitsForTest(), 1u) << "a non-shard worker must submit the deferred seal (else it is never fsynced)";
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}

// F30 fix round 1 (F4): the DEFAULT configuration (thread placement, per-worker
// ingress OFF).  A follower flush the io backend refuses (in-flight depth
// reached) is consumed and never re-raised by an idle follower — the time-gated
// owned-log sync must run in every configuration and re-request it.
TEST(NonShardNonBlockingRegression, DefaultConfigRefusedFollowerFlushIsEventuallyFlushedRegression) {
    TmpDir d, d2;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    Log other(d2.path.string(), 64LL * 1024 * 1024); other.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    PartitionReplica holder(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &other, &t);
    holder.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, /*ioSubmitDepth=*/1, /*ioComplDepth=*/1);
    w.setOwnedDirect(Worker::OwnedSet{&follower, &holder});   // default config: no per-worker ingress
    w.setReapPausedForTest(true);
    ASSERT_NE(w.submitBgFlushForTest(&other), kNoToken);   // occupies the whole io depth
    applyOneBatch(follower, 0);
    ASSERT_GT(log.logEndOffset(), 0);
    w.doWork(25 * kMs);                                    // the follower flush request is raised + refused
    ASSERT_EQ(w.bgFlushDeferredForTest(), 1u) << "test setup: the follower flush must be refused";
    ASSERT_EQ(w.lastSyncedLeoForTest(&log), 0);
    w.setReapPausedForTest(false);
    int64_t now = 25 * kMs;
    for (int i = 0; i < kCap && w.lastSyncedLeoForTest(&log) != log.logEndOffset(); ++i) {
        w.doWork(now += kMs);                              // no further applies: only time passes
        std::this_thread::yield();
    }
    EXPECT_EQ(w.lastSyncedLeoForTest(&log), log.logEndOffset())
        << "a refused follower flush must be re-requested (never unflushed forever)";
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}

// F30 fix round 1 (F4): an ownership move drops the old owner's coalesced
// "dirty again" re-request; the NEW owner (default config) must flush the
// unflushed data within one owned-log sync interval.
TEST(NonShardNonBlockingRegression, OwnershipMoveUnflushedDataIsFlushedByNewOwnerRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    Worker a(0, 1 << 16, 1 << 16, 64, 64);
    Worker b(1, 1 << 16, 1 << 16, 64, 64);
    a.setOwnedDirect(Worker::OwnedSet{&follower});
    a.setReapPausedForTest(true);                          // A's flush stays in flight
    applyOneBatch(follower, 0);
    a.doWork(25 * kMs);                                    // A submits the follower flush
    ASSERT_EQ(a.bgFlushSubmitsForTest(), 1u);
    applyOneBatch(follower, 1);
    a.doWork(50 * kMs);                                    // newer data: coalesced as "dirty again" on A
    ASSERT_EQ(a.bgFlushSubmitsForTest(), 1u);
    a.setOwnedDirect(Worker::OwnedSet{});                  // ownership move: A purges the slot
    b.setOwnedDirect(Worker::OwnedSet{&follower});
    int64_t now = 50 * kMs;
    for (int i = 0; i < kCap && b.lastSyncedLeoForTest(&log) != log.logEndOffset(); ++i) {
        b.doWork(now += kMs);
        std::this_thread::yield();
    }
    EXPECT_EQ(b.lastSyncedLeoForTest(&log), log.logEndOffset())
        << "the new owner flushes data the old owner never made durable";
    EXPECT_EQ(log.syncCallsForTest(), 0u);
    a.setReapPausedForTest(false);
    for (int i = 0; i < kCap && a.ioCompletionsReapedForTest() < 1; ++i) { a.doWork(now); std::this_thread::yield(); }
}

// F30 fix round 1 (F5): a follower truncation below the synced LEO is
// reconciled in the thread-placement pass — re-appended bytes below the stale
// mark are flushed by the timed sync (else never, once traffic stops).
namespace {
void followerTruncationThenReappendIsReflushed(bool shard) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    w.setWorkerIngress(nullptr, true);                     // timed sync active before F30 too: isolates F5
    w.setShardMode(shard);
    applyOneBatch(follower, 0);
    applyOneBatch(follower, 1);
    w.doWork(25 * kMs);                                    // follower flush submitted
    int64_t now = 25 * kMs;
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) { w.doWork(now += 1000); std::this_thread::yield(); }
    ASSERT_EQ(w.lastSyncedLeoForTest(&log), 2);
    log.truncateTo(0);                                     // follower truncation (divergence / role change)
    const auto b = batchAt(0);
    log.appendNoSync(b.data(), b.size(), 0);               // re-append below the stale mark (LEO 1),
                                                           // BEFORE the worker's next pass sees the cut
    ASSERT_EQ(log.logEndOffset(), 1);
    const uint64_t before = w.bgFlushSubmitsForTest();
    for (int i = 0; i < 64 && w.lastSyncedLeoForTest(&log) != log.logEndOffset(); ++i) {
        now += 11 * kMs;                                   // one owned-log sync interval per step
        w.doWork(now);
        for (int j = 0; j < kCap && w.bgFlushInFlightForTest() > 0; ++j) { w.doWork(now); std::this_thread::yield(); }
    }
    EXPECT_GT(w.bgFlushSubmitsForTest(), before) << "re-appended data below the stale synced mark is flushed";
    EXPECT_EQ(w.lastSyncedLeoForTest(&log), 1);
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}
} // namespace

TEST(NonShardNonBlockingRegression, FollowerTruncationThenReappendIsReflushedRegression) {
    followerTruncationThenReappendIsReflushed(/*shard=*/false);
}
TEST(ShardNonBlockingRegression, FollowerTruncationThenReappendIsReflushedRegression) {
    followerTruncationThenReappendIsReflushed(/*shard=*/true);
}

// F30 fix round 1 (F7): a deferred roll seal the io backend refuses stays queued
// (pinned) and is submitted on a later pass in thread placement — never lost —
// and completes.
TEST(NonShardNonBlockingRegression, RefusedRollSealIsRetriedAndCompletesRegression) {
    TmpDir d, d2;
    Log log(d.path.string(), /*rollBytes=*/512); log.recover();
    Log other(d2.path.string(), 64LL * 1024 * 1024); other.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    PartitionReplica holder(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &other, &t);
    holder.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, /*ioSubmitDepth=*/1, /*ioComplDepth=*/1);
    w.setOwnedDirect(Worker::OwnedSet{&follower, &holder});
    w.setReapPausedForTest(true);
    ASSERT_NE(w.submitBgFlushForTest(&other), kNoToken);   // occupies the whole io depth
    applyOneBatch(follower, 0, 600);                       // fills segment A
    applyOneBatch(follower, 1, 16);                        // rolls: A sealed (deferred)
    ASSERT_EQ(log.pendingSealCount(), 1u);
    w.doWork(1 * kMs);
    EXPECT_EQ(w.sealSubmitsForTest(), 0u) << "refused: io depth reached";
    EXPECT_EQ(log.pendingSealCount(), 1u) << "a refused seal stays queued";
    w.setReapPausedForTest(false);
    for (int i = 0; i < kCap && (w.sealSubmitsForTest() == 0 || w.ioCompletionsReapedForTest() < 2); ++i) {
        w.doWork(1 * kMs + i);
        std::this_thread::yield();
    }
    EXPECT_EQ(w.sealSubmitsForTest(), 1u);
    EXPECT_EQ(log.pendingSealCount(), 0u);
    EXPECT_GE(w.ioCompletionsReapedForTest(), 2u) << "the seal completed";
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
    EXPECT_EQ(log.syncCallsForTest(), 0u);
}

// F30 fix round 1 (F8): in thread placement a txn marker no longer fsyncs
// inline on the worker (as in shard mode) — its durability is an io token.
TEST(NonShardNonBlockingRegression, TxnMarkerAppendIsAsyncInThreadPlacementRegression) {
    StrictShard s(64LL * 1024 * 1024, {1}, /*shard=*/false);
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    WorkerOp op;
    op.type = WorkerOp::Type::AppendControlRecord; op.topicId = 1; op.partition = 0;
    op.replica = s.leader.get(); op.logPtr = s.log.get();
    op.recordBatchBytes = marker;
    ASSERT_TRUE(s.w.tryEnqueueOp(std::move(op)));
    s.w.doWork(1);
    EXPECT_GT(s.log->logEndOffset(), 0) << "the marker was appended";
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "the marker append must not fsync inline on a worker";
    // Its durability is an io token — since the F15 S5 merge (M-6: one fsync in
    // flight per log) the pass's coalesced durable fsync, which also serves the
    // deferred background request (same as the shard-mode twin below).
    EXPECT_EQ(s.w.ioSubmitCountForTest(), 1u) << "its durability is the pass's durable io token";
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 0u) << "a background fsync concurrent with the durable one";
    for (int i = 0; i < kCap && s.w.lastSyncedLeoForTest(s.log.get()) < s.log->logEndOffset(); ++i) {
        s.w.doWork(2); std::this_thread::yield();
    }
    EXPECT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), s.log->logEndOffset());
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, TxnMarkerAppendIsAsyncInShardMode) {
    StrictShard s;
    // A real control batch (F15 N2: appendControlBatch refuses non-markers).
    const auto marker = rune::transaction::ControlMarker::build(7, 0, rune::transaction::ControlType::Commit, 0);
    WorkerOp op;
    op.type = WorkerOp::Type::AppendControlRecord; op.topicId = 1; op.partition = 0;
    op.replica = s.leader.get(); op.logPtr = s.log.get();
    op.recordBatchBytes = marker;
    ASSERT_TRUE(s.w.tryEnqueueOp(std::move(op)));
    s.w.doWork(1);
    EXPECT_GT(s.log->logEndOffset(), 0) << "the marker was appended";
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "the marker append must not fsync inline on the shard loop";
    // Its durability is an io token: the coalesced durable fsync (F15 S5 fix
    // round 2, M-6: one fsync in flight per log — the follower-flush background
    // request is served by it, never concurrent with it).
    // M-8 (re-review 2): exactly ONE fsync — the coalesced durable one; the
    // replica's follower-flush background request (pass step 4b) is deferred
    // behind it and served by it, never submitted concurrently.
    EXPECT_EQ(s.w.ioSubmitCountForTest(), 1u) << "its durability is the pass's durable io token";
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 0u) << "a background fsync concurrent with the durable one";
    for (int i = 0; i < kCap && s.w.lastSyncedLeoForTest(s.log.get()) < s.log->logEndOffset(); ++i) {
        s.w.doWork(2); std::this_thread::yield();
    }
    EXPECT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), s.log->logEndOffset());
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), 0u) << "the deferred background request was served by the durable fsync";
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
}

// ============================================================================
// Token routing.
// ============================================================================

TEST(ShardNonBlockingRegression, BackgroundFlushCompletionNeverFiresProduceAck) {
    // Token routing: a background token's completion is retired by routing and
    // never reaches produce-ack matching — even while a STAMPED durable entry is
    // parked (M-8, re-review 2: on a second log, since one log never has a
    // background and a durable fsync in flight together since F15 S5 M-6).
    TmpDir d, d2;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    auto c2 = cfg(1, /*tier=*/2); c2.partitionId = 1;
    PartitionReplica leader2(c2, Role::Leader, 1, {1}, {1}, &log2, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader, &leader2});
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 1;
    op.replica = &leader2; op.logPtr = &log2; op.sessionId = 1; op.correlationId = 1;
    op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 2;
    op.recordBatchBytes = batchAt(0);
    w.setReapPausedForTest(true);                          // only synthetic deliveries until unpaused
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    w.doWork(1);                                           // append on log2 + its durable submit (T1)
    const auto t1 = w.durablePendingTokenForTest(0);
    ASSERT_NE(t1, kNoToken) << "precondition: a STAMPED durable entry is parked";
    const auto t2 = w.submitBgFlushForTest(&log);          // a background flush on log 1
    ASSERT_NE(t2, kNoToken);
    ASSERT_NE(t1, t2);
    Acks a;
    a.drain(w);
    EXPECT_EQ(a.n, 0) << "a Strict produce's answer waits for its fsync";
    w.deliverCompletionForTest(FlushCompletion{t2, 0, log.flushBoundary()});
    a.drain(w);
    EXPECT_EQ(a.n, 0) << "a background flush token must never fire a produce ack";
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u) << "the background token is retired by routing";
    EXPECT_EQ(w.durablePendingTokenForTest(0), t1) << "the stamped durable entry is untouched";
    w.deliverCompletionForTest(FlushCompletion{t1, 0, log2.flushBoundary()});
    a.drain(w);
    EXPECT_EQ(a.n, 1);
    EXPECT_EQ(w.durableAckTokenForTest(), t1);
    // The real completions for T1 and T2 arrive later through io_.reap: wait for
    // exactly those two (condition, capped) and prove neither fires anything.
    w.setReapPausedForTest(false);
    const uint64_t reapedBefore = w.ioCompletionsReapedForTest();
    for (int i = 0; i < kCap && w.ioCompletionsReapedForTest() < reapedBefore + 2; ++i) {
        w.doWork(3 + i); a.drain(w); std::this_thread::yield();
    }
    ASSERT_EQ(w.ioCompletionsReapedForTest(), reapedBefore + 2);
    a.drain(w);
    EXPECT_EQ(a.n, 1) << "late real completions for T1/T2 must be ignored";
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
}

// F15 S5 fix round 3 (re-review 2 I-3): a Strict waiter deferred behind an
// in-flight background flush must not be deferred AGAIN by a background flush
// re-armed in the meantime (sync-interval tick / follower flush): the durable
// fsync goes next and serves the background request.  Bound: one in-flight
// fsync + its own.
TEST(ShardNonBlockingRegression, StrictWaiterIsNeverStarvedByRearmedBackgroundFlushes) {
    StrictShard s;
    s.w.setReapPausedForTest(true);
    const auto b1 = s.w.submitBgFlushForTest(s.log.get());   // background fsync B1 in flight
    ASSERT_NE(b1, kNoToken);
    ASSERT_EQ(s.produce(1, batchAt(0), 0, 1), 1);           // Strict append while B1 is in flight
    s.w.doWork(1);
    ASSERT_EQ(s.w.durablePendingTokenForTest(0), kNoToken) << "precondition: deferred behind B1";
    // A sync tick lands while B1 is in flight (LEO > bgLeo): background re-armed.
    (void)s.w.submitBgFlushForTest(s.log.get());
    Acks a;
    for (int round = 0; round < 3; ++round) {
        const uint64_t bgBefore = s.w.bgFlushSubmitsForTest();
        const auto inflightBg = s.w.bgFlushInFlightForTest();
        if (inflightBg == 0) break;
        // retire whatever background token is in flight (B1 first)
        s.w.deliverCompletionForTest(FlushCompletion{round == 0 ? b1 : s.w.lastBgTokenForTest(s.log.get()), 0,
                                                     s.log->flushBoundary()});
        s.w.doWork(2 + round);
        a.drain(s.w);
        if (s.w.durablePendingCountForTest() == 0 || s.w.durablePendingTokenForTest(0) != kNoToken) break;
        EXPECT_EQ(s.w.bgFlushSubmitsForTest(), bgBefore)
            << "round " << round << ": a background flush was re-armed ahead of the waiting Strict fsync";
    }
    ASSERT_EQ(s.w.durablePendingCountForTest(), 1u);
    EXPECT_NE(s.w.durablePendingTokenForTest(0), kNoToken)
        << "the Strict fsync was not submitted on the pass after the in-flight background fsync completed";
    EXPECT_EQ(s.w.bgFlushInFlightForTest(), 0u) << "a background fsync is in flight instead of the Strict one";
}

// F15 S5 fix round 3 (re-review 2 N-5): the one-in-flight gate belongs to the
// LOG.  Purged and re-owned while its durable fsync is in flight, the log must
// not get a second, concurrent fsync from the fresh slot.
TEST(ShardNonBlockingRegression, ReownedLogKeepsItsInFlightFsyncGate) {
    StrictShard s;
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0), 0, 1), 1);
    s.w.doWork(1);
    const auto t1 = s.w.durablePendingTokenForTest(0);
    ASSERT_NE(t1, kNoToken) << "precondition: durable fsync T1 in flight";
    s.w.setOwnedDirect(Worker::OwnedSet{});                  // purged (its waiter is failed)
    s.w.setOwnedDirect(Worker::OwnedSet{s.leader.get()});   // re-owned: a fresh slot
    Acks a;
    s.w.doWork(2); a.drain(s.w);
    ASSERT_EQ(s.produce(2, batchAt(0), 0, 3), 1);
    s.w.doWork(3); a.drain(s.w);
    ASSERT_EQ(s.w.durablePendingCountForTest(), 1u);
    EXPECT_EQ(s.w.durablePendingTokenForTest(0), kNoToken)
        << "a second fsync was submitted on the log while T1 (from before the re-own) is in flight";
    s.w.deliverCompletionForTest(FlushCompletion{t1, 0, s.log->flushBoundary()});
    s.w.doWork(4); a.drain(s.w);
    EXPECT_NE(s.w.durablePendingTokenForTest(0), kNoToken) << "T1's completion released the gate";
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

// F30 fix round 1 (F1 + F2): the Strict tier is completion-gated (and gated on
// outstanding roll seals) in EVERY placement — thread placement never fsyncs
// inline on a worker either.  Param: true = shard, false = thread placement.
class StrictPlacementRegression : public ::testing::TestWithParam<bool> {};

TEST_P(StrictPlacementRegression, StrictAcksAllWaitsForFsyncThenHw) {
    StrictShard s(64LL * 1024 * 1024, {1, 2}, GetParam()); // follower 2 in ISR: HW needs its fetch
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
    ASSERT_EQ(s.produce(1, batchAt(0, 2, 1, 7, 0), 0, 1, /*pid=*/7, /*seq=*/0), 1);
    s.w.doWork(1);
    const auto t1 = s.w.durablePendingTokenForTest(0);
    Acks a;
    // Retry of the same idempotent batch BEFORE the original's fsync completed.
    ASSERT_EQ(s.produce(2, batchAt(0, 2, 1, 7, 0), 0, 2, 7, 0), 1);
    s.w.doWork(2); a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "a Strict duplicate must not be acked before the data is durable";
    EXPECT_EQ(s.w.durablePendingCountForTest(), 2u);
    // F15 S5 fix round 2 (M-6): the retry's fsync waits behind T1 (one in
    // flight per log) — it is submitted on the pass after T1 completes.
    s.w.deliverCompletionForTest(FlushCompletion{t1, 0, 0});
    s.w.doWork(3); a.drain(s.w);
    ASSERT_EQ(a.n, 1) << "the original answers on its own fsync";
    const auto t2 = s.w.durablePendingTokenForTest(0);
    ASSERT_NE(t2, kNoToken);
    ASSERT_NE(t2, t1);
    s.w.deliverCompletionForTest(FlushCompletion{t2, 0, 0});
    s.w.doWork(4); a.drain(s.w);
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

TEST_P(StrictPlacementRegression, RollSealIsAsyncAndGatesStrictAcks) {
    StrictShard s(/*rollBytes=*/512, {1}, GetParam());
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0, 1, 600), 0, 1), 1);  // segment A (>= rollBytes)
    ASSERT_EQ(s.produce(2, batchAt(0, 1, 16), 0, 1), 1);   // rolls: A sealed, appended to B
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "the seal fsync is never inline on a worker (any placement)";
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

INSTANTIATE_TEST_SUITE_P(Placement, StrictPlacementRegression, ::testing::Values(true, false),
                         [](const ::testing::TestParamInfo<bool>& i) {
                             return std::string(i.param ? "Shard" : "ThreadPlacement");
                         });

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
    ASSERT_EQ(s.produce(1, batchAt(0, 2, 1, 3, 0), 0, 1, /*pid=*/3, /*seq=*/0), 1);
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
    ASSERT_EQ(s.produce(2, batchAt(0, 2, 1, 3, 0), 0, 3, 3, 0), 1);     // idempotent retry of the failed batch
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

// ============================================================================
// Fix round 2.
// ============================================================================

TEST(StorageFailStopRegression, ErrorCompletionAfterOwnershipMoveHaltsAndNeverAcksOkRegression) {
    // C-1: a Strict entry + an async seal in flight; ownership moves the log
    // away; the seal then completes with an error.  The error is NOT swallowed
    // by the purged slot (halt signal), no OK ack is ever sent, and this worker
    // never writes the (now foreign) Log object.
    StrictShard s(/*rollBytes=*/512);
    TmpDir d2;
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    PartitionReplica other(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &log2, &s.t);
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0, 1, 600), 0, 1), 1);
    ASSERT_EQ(s.produce(2, batchAt(0, 1, 16), 0, 1), 1);   // roll → async seal
    s.w.doWork(1);
    ASSERT_EQ(s.w.sealSubmitsForTest(), 1u);
    ASSERT_EQ(s.w.durablePendingCountForTest(), 2u);
    const auto active = s.w.durablePendingTokenForTest(0);
    // The seal is the token submitted just before the active flush.
    const auto seal = active - 1;
    WorkerOp op;
    op.type = WorkerOp::Type::SetOwnership;
    op.ownedSet = {&other};
    ASSERT_TRUE(s.w.tryEnqueueOp(std::move(op)));
    s.w.doWork(2);
    std::vector<WorkerReturn> all;
    auto drainAll = [&] { s.w.drainReturn([&](WorkerReturn r) { all.push_back(r); }); };
    drainAll();
    s.w.deliverCompletionForTest(FlushCompletion{seal, EIO, 0});
    s.w.deliverCompletionForTest(FlushCompletion{active, 0, 0});
    s.w.doWork(3);
    drainAll();
    int produce = 0, ok = 0, signals = 0;
    for (const auto& r : all) {
        if (r.type == WorkerReturn::Type::ProduceResult) { ++produce; ok += r.errorCode == 0; }
        if (r.type == WorkerReturn::Type::StorageFailure) { ++signals; EXPECT_EQ(r.errorCode, EIO); }
    }
    EXPECT_EQ(produce, 2) << "both entries answered (retriable NotLeader at the move)";
    EXPECT_EQ(ok, 0) << "never an OK ack after a failed seal";
    EXPECT_EQ(signals, 1) << "the error on a purged slot still halts the broker";
    EXPECT_FALSE(s.log->storageFailed()) << "the foreign Log object is never written by this worker";
}

// E1 (DUR-1): periodic fsync is OFF by default (A3) — a dead disk on the LEADER append path must still halt
// the broker through the write error itself: the produce is never acked OK and the worker signals the
// storage failure (the Kafka log-dir-failure halt), with no fsync involved.
TEST(StorageFailStopRegression, LeaderAppendWriteErrorHaltsWithPeriodicFsyncOff) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setWorkerIngress(nullptr, true);
    w.setShardMode(true);
    log.setWriteFaultForTest(EIO);
    const auto f = produceFrame(1, batchAt(0), /*acks=*/1, 1, 0, -1, -1);
    EXPECT_NO_THROW(w.handleIngressFrameForTest(f.data(), f.size(), 1)) << "a write error never escapes the loop";
    EXPECT_NO_THROW(w.doWork(2));
    EXPECT_TRUE(log.storageFailed());
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "detected without any fsync";
    int ok = 0, answered = 0, signals = 0;
    w.drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) { ++answered; ok += r.errorCode == 0; }
        if (r.type == WorkerReturn::Type::StorageFailure) { ++signals; EXPECT_EQ(r.errorCode, EIO); }
    });
    EXPECT_EQ(answered, 1) << "the producer gets an answer (never a hang)";
    EXPECT_EQ(ok, 0) << "never an OK ack for a batch the disk refused";
    EXPECT_EQ(signals, 1) << "the write error reaches the broker halt exactly once";
}

TEST(StorageFailStopRegression, LegacyFollowerFlushFailureHaltsNotTerminates) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    applyOneBatch(follower, 0);
    log.setSyncFaultForTest(EIO);
    EXPECT_NO_THROW(w.doWork(25 * kMs)) << "a failed follower fsync never escapes the loop";
    EXPECT_TRUE(log.storageFailed());
    int signals = 0;
    w.drainReturn([&](WorkerReturn r) { signals += r.type == WorkerReturn::Type::StorageFailure; });
    EXPECT_EQ(signals, 1);
}

namespace {
class NoopConsensus final : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};
} // namespace

TEST(StorageFailStopRegression, InternalPartitionFlushFailureReachesHalt) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    auto rep = std::make_unique<PartitionReplica>(cfg(2, 1, /*internal topic*/-2, 0), Role::Follower, 1,
        std::vector<rune::replication::NodeId>{1, 2}, std::vector<rune::replication::NodeId>{1, 2}, &log, &t);
    rep->becomeFollower(1, 1);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, -2,
                                             std::span<const uint8_t>(batchAt(0))};
    const auto b = batchAt(0);
    resp.records = std::span<const uint8_t>(b);
    rep->applyReplicated(resp);
    ReplicaRegistry reg;
    reg.add(-2, 0, std::move(rep));
    NoopConsensus noop;
    NodeRuntimeAgent rt(noop, nullptr, nullptr, nullptr, &reg, nullptr);
    std::string haltedDir; int haltedErr = 0; int calls = 0;
    rt.setStorageFailureHook([&](const std::string& dir, int err) { haltedDir = dir; haltedErr = err; ++calls; });
    log.setSyncFaultForTest(EIO);
    EXPECT_NO_THROW(rt.doWork(25 * kMs)) << "control-thread flush failure must not terminate";
    EXPECT_NO_THROW(rt.doWork(50 * kMs));
    EXPECT_EQ(calls, 1) << "an internal-partition fsync failure reaches the halt exactly once";
    EXPECT_EQ(haltedErr, EIO);
    EXPECT_EQ(haltedDir, log.dir());
}

TEST(ShardNonBlockingRegression, TruncateDroppingPendingSealFailsGatedEntriesRetriably) {
    StrictShard s(/*rollBytes=*/512);
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0, 1, 600), 0, 1), 1);   // segment A
    s.w.doWork(1);                                            // T1 (fd A)
    const auto t1 = s.w.durablePendingTokenForTest(0);
    ASSERT_EQ(s.produce(2, batchAt(0, 1, 16), 0, 2), 1);    // rolls: A pending seal (unsubmitted)
    ASSERT_EQ(s.log->pendingSealCount(), 1u);
    s.w.deliverCompletionForTest(FlushCompletion{t1, 0, 0}); // P1 now gated on A's seal
    s.log->truncateTo(0);                                     // role change drops the pending seal
    s.w.doWork(3);
    Acks a;
    a.drain(s.w);
    ASSERT_EQ(a.n, 2) << "gated entries never wedge (or hold ack reservations)";
    for (const auto& r : a.rets)
        EXPECT_EQ(r.errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "retriable, never OK";
    EXPECT_EQ(s.w.durablePendingCountForTest(), 0u);
    EXPECT_EQ(s.w.reservedAckSlotsForTest(), 0);
}

TEST(ShardNonBlockingRegression, TruncateClampsSyncedLeo) {
    StrictShard s;
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 2)));
    s.w.doWork(1);
    for (int i = 0; i < kCap && s.w.bgFlushInFlightForTest() > 0; ++i) { s.w.doWork(2); std::this_thread::yield(); }
    ASSERT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), 2);
    const uint64_t roleSyncsBefore = s.log->roleChangeSyncCallsForTest();
    s.log->truncateTo(1);
    EXPECT_GT(s.log->roleChangeSyncCallsForTest(), roleSyncsBefore)
        << "truncate's index msync + dir fsync are counted as accepted role-change sync work";
    EXPECT_EQ(s.log->syncCallsForTest(), 0u);
    s.w.doWork(3);
    EXPECT_EQ(s.w.lastSyncedLeoForTest(s.log.get()), 1) << "clamped to the new LEO";
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 3)));   // LEO back to 2
    const uint64_t before = s.w.bgFlushSubmitsForTest();
    s.w.doWork(4);
    EXPECT_GT(s.w.bgFlushSubmitsForTest(), before) << "re-appended data below the stale mark is flushed";
}

TEST(StorageFailStopRegression, FailedLogIsNeverSubmittedAgain) {
    StrictShard s;
    ASSERT_EQ(s.produce(1, batchAt(0), 0, 1), 1);            // appended, not yet submitted
    s.log->markStorageFailed(EIO);
    s.w.doWork(1);
    EXPECT_EQ(s.w.ioSubmitCountForTest(), 0u) << "no io submission on a failed log";
    EXPECT_EQ(s.w.submitBgFlushForTest(s.log.get()), kNoToken);
    Acks a;
    a.drain(s.w);
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, kStorageError);
}

TEST(StorageFailStopRegression, ThreadPlacementStrictFsyncFailureIsStorageErrorRegression) {
    // F30 fix round 1 (F2): the thread-placement WorkerIngress Strict path no
    // longer fsyncs inline — its OWN io completion carries the fsync error, and
    // the ack fails StorageError (never OK).
    StrictShard s(64LL * 1024 * 1024, {1}, /*shard=*/false);
    s.w.setReapPausedForTest(true);
    ASSERT_EQ(s.produce(1, batchAt(0), 0, 1), 1);
    EXPECT_EQ(s.log->syncCallsForTest(), 0u) << "no inline fsync on a thread-placement worker";
    s.w.doWork(1);
    const auto tok = s.w.durablePendingTokenForTest(0);
    ASSERT_NE(tok, kNoToken);
    s.w.deliverCompletionForTest(FlushCompletion{tok, EIO, 0});
    Acks a;
    a.drain(s.w);
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, kStorageError);
    EXPECT_TRUE(s.log->storageFailed());
}

TEST(StorageFailStopRegression, OwnershipMoveFailsAcksAllWaitersNoCrossThreadReadRegression) {
    // After an ownership move this worker holds no reference to — and makes no
    // reads of — the moved replica or log: acks=all waiters (which read the
    // replica's HW) fail retriably, push subscriptions are parked.
    auto s = std::make_unique<StrictShard>(64LL * 1024 * 1024, std::vector<rune::replication::NodeId>{1, 2});
    TmpDir d2;
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    PartitionReplica other(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &log2, &s->t);
    s->leader->setRegistryHandle(1, 21);   // registered (ReplicaRegistry::add does this)
    other.setRegistryHandle(2, 22);
    s->w.setOwnedDirect(Worker::OwnedSet{s->leader.get()});
    s->w.setReapPausedForTest(true);
    ASSERT_EQ(s->produce(7, batchAt(0, 2), /*acks=all*/1, 1), 1);
    s->w.doWork(1);
    const auto tok = s->w.durablePendingTokenForTest(0);
    s->w.deliverCompletionForTest(FlushCompletion{tok, 0, 0});   // fsynced → parked on HW (follower 2 behind)
    ASSERT_EQ(s->w.reservedAckSlotsForTest(), 1);
    WorkerOp sub;
    sub.type = WorkerOp::Type::Subscribe; sub.topicId = 1; sub.partition = 0;
    sub.replica = s->leader.get(); sub.logPtr = s->log.get(); sub.subscriptionId = 42; sub.sessionId = 1;
    ASSERT_TRUE(s->w.tryEnqueueOp(stamp(std::move(sub))));
    s->w.doWork(2);
    ASSERT_EQ(s->w.subscriptionsForTest().size(), 1u);
    EXPECT_TRUE(s->w.subscriptionsForTest()[0].active);
    WorkerOp op;
    op.type = WorkerOp::Type::SetOwnership;
    op.ownedSet = {&other};
    ASSERT_TRUE(s->w.tryEnqueueOp(std::move(op)));
    s->w.doWork(3);
    Acks a;
    a.drain(s->w);
    ASSERT_EQ(a.n, 1) << "the acks=all waiter is answered at the move";
    EXPECT_EQ(a.rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "retriable, never OK";
    EXPECT_EQ(s->w.reservedAckSlotsForTest(), 0) << "no waiter holds the moved replica";
    EXPECT_FALSE(s->w.subscriptionsForTest()[0].active) << "the push cursor on the moved log is parked";
    // The moved replica + log are now another worker's: destroy them here — this
    // worker must never touch them again (ASan / a crash would catch a read).
    s->leader.reset();
    s->log.reset();
    s->w.deliverCompletionForTest(FlushCompletion{tok + 1, EIO, 0});   // unrelated late error: no crash
    for (int i = 0; i < 50; ++i) s->w.doWork(4 + i * 11 * kMs);
    a.drain(s->w);
    EXPECT_EQ(a.n, 1);
}

TEST(StorageFailStopRegression, RecoveryMsyncFailureMarksLogFailed) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    ASSERT_FALSE(log.storageFailed());
    log.setSyncFaultForTest(EIO);
    EXPECT_NO_THROW(log.recover()) << "a recovery msync failure never escapes as an exception";
    log.setSyncFaultForTest(0);
    EXPECT_TRUE(log.storageFailed());
    EXPECT_EQ(log.storageErrno(), EIO);
}

TEST(StorageFailStopRegression, OpQueuedBeforeOwnershipMoveAnswersNotLeaderRegression) {
    // A produce + a replicated apply routed to this worker under a stale
    // ownership view, processed AFTER a SetOwnership moved the partition away:
    // answered NotLeader / refused, the moved log is never touched.
    TmpDir d, d2;
    auto log = std::make_unique<Log>(d.path.string(), 64LL * 1024 * 1024); log->recover();
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    NullTransport t;
    auto moved = std::make_unique<PartitionReplica>(cfg(1), Role::Leader, 1,
        std::vector<rune::replication::NodeId>{1}, std::vector<rune::replication::NodeId>{1}, log.get(), &t);
    PartitionReplica other(cfg(1, 1, 1, 1), Role::Leader, 1, {1}, {1}, &log2, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    moved->setRegistryHandle(1, 11);       // registered replicas (ReplicaRegistry::add does this)
    other.setRegistryHandle(2, 12);
    w.setOwnedDirect(Worker::OwnedSet{moved.get(), &other});
    ASSERT_TRUE(w.ownsReplicaForTest(moved.get()));
    WorkerOp own;
    own.type = WorkerOp::Type::SetOwnership;
    own.ownedSet = {&other};
    ASSERT_TRUE(w.tryEnqueueOp(std::move(own)));
    WorkerOp prod;
    prod.type = WorkerOp::Type::Produce; prod.topicId = 1; prod.partition = 0;
    prod.replica = moved.get(); prod.logPtr = log.get(); prod.sessionId = 5; prod.correlationId = 77;
    prod.recordCount = 1; prod.durabilityTier = 1; prod.recordBatchBytes = batchAt(0);
    ASSERT_TRUE(w.tryEnqueueOp(stamp(std::move(prod))));
    WorkerOp apply;
    apply.type = WorkerOp::Type::ApplyReplicated; apply.topicId = 1; apply.partition = 0;
    apply.replica = moved.get(); apply.logPtr = log.get();
    const auto b = batchAt(0);
    apply.replicatedPayload.records = b; apply.replicatedPayload.leaderEpoch = 1; apply.replicatedPayload.topicId = 1;
    ASSERT_TRUE(w.tryEnqueueOp(stamp(std::move(apply))));
    const int64_t leoBefore = log->logEndOffset();
    // The moved replica + log now belong to another worker: destroy them so any
    // dereference by this worker would be caught (ASan / crash).
    moved.reset();
    log.reset();
    w.doWork(1);
    Acks a;
    a.drain(w);
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].correlationId, 77);
    EXPECT_EQ(a.rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "retriable on the new owner";
    EXPECT_EQ(w.opsRefusedNotOwnedForTest(), 2u) << "the apply is refused too";
    EXPECT_EQ(leoBefore, 0);
    EXPECT_EQ(log2.logEndOffset(), 0) << "no op is redirected to another log either";
}

// ============================================================================
// Fix round 3 — id + incarnation ownership guard, ordering, F7, I1.
// ============================================================================
namespace {
struct Reg {
    TmpDir d;
    NullTransport t;
    ReplicaRegistry reg;
    std::vector<std::unique_ptr<Log>> logs;
    PartitionReplica* add(int32_t topicId, int32_t part,
                          std::vector<rune::replication::NodeId> replicas = {1}) {
        auto dir = d.path / (std::to_string(topicId) + "_" + std::to_string(part) + "_" + std::to_string(logs.size()));
        fs::create_directories(dir);
        logs.push_back(std::make_unique<Log>(dir.string(), 64LL * 1024 * 1024));
        logs.back()->recover();
        auto r = std::make_unique<PartitionReplica>(cfg(1, 1, topicId, part), Role::Leader, 1,
            replicas, std::vector<rune::replication::NodeId>{1},
            logs.back().get(), &t);
        auto* raw = r.get();
        reg.add(topicId, part, std::move(r));
        return raw;
    }
};
WorkerOp produceOp(PartitionReplica* r, int64_t corr) {
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = r->topicId(); op.partition = static_cast<int32_t>(r->partitionId());
    op.replica = r; op.logPtr = r->log(); op.sessionId = 1; op.correlationId = corr;
    op.recordCount = 1; op.durabilityTier = 1; op.recordBatchBytes = batchAt(0);
    return stamp(std::move(op));
}
} // namespace

TEST(OwnershipGuardRegression, OpForReplicaGainedInSameCycleIsExecutedNotRefused) {
    // I2(a)+(b): an op routed in the same cycle as the registry add reaches the
    // gaining worker AFTER its SetOwnership (inline reconcile in tryEnqueueOp).
    Reg R;
    rune::metadata::ShardRouter router(1);
    WorkerPool pool(1, std::move(router), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);             // startup: empty
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    auto* r = R.add(5, 0);                      // registry add — no explicit reconcile
    ASSERT_TRUE(pool.tryEnqueueOp(5, 0, produceOp(r, 41)));
    pool.worker(0).doWork(1);
    Acks a; a.drain(pool.worker(0));
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, 0) << "executed, not refused";
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 0u);
    EXPECT_EQ(r->log()->logEndOffset(), 1);
}

TEST(OwnershipGuardRegression, UndeliveredSetOwnershipBackPressuresRouting) {
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), /*ingress*/ 1 << 12, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    auto* r = R.add(5, 0);
    // Fill worker 0's ingress ring so the SetOwnership cannot be delivered.
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (pool.worker(0).tryEnqueueOp(filler)) {}
    EXPECT_FALSE(pool.tryEnqueueOp(5, 0, produceOp(r, 1)))
        << "never route an op ahead of an undelivered SetOwnership (back-pressure)";
    pool.worker(0).doWork(1);                   // drains the ring
    {
        Acks a; a.drain(pool.worker(0));
    }
    EXPECT_TRUE(pool.tryEnqueueOp(5, 0, produceOp(r, 2))) << "delivered on retry, then routed";
    Acks a;
    for (int i = 0; i < 16 && a.n == 0; ++i) { pool.worker(0).doWork(2 + i); a.drain(pool.worker(0)); }  // ring wrap: bounded
    ASSERT_EQ(a.n, 1) << "owned=" << pool.worker(0).ownedCount()
                      << " refused=" << pool.worker(0).opsRefusedNotOwnedForTest()
                      << " acks=" << pool.worker(0).ownershipAcksForTest();
    EXPECT_EQ(a.rets[0].errorCode, 0);
}

TEST(OwnershipGuardRegression, PointerReuseIsRefusedByIncarnation) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    rep.setRegistryHandle(3, 100);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    // "destroy + a new replica at the same address": same pointer, same id, NEW incarnation.
    rep.setRegistryHandle(3, 101);
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(&rep, 9)));
    w.doWork(1);
    Acks a; a.drain(w);
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_EQ(log.logEndOffset(), 0) << "a reused address never passes as ownership";
}

TEST(OwnershipGuardRegression, EveryGuardedOpTypeIsRefusedWhenNotOwned) {
    // Each partition op type for an incarnation this worker does NOT own is
    // refused without dereferencing the replica / log it carries (both are
    // destroyed before the worker runs).
    TmpDir d;
    auto log = std::make_unique<Log>(d.path.string(), 64LL * 1024 * 1024); log->recover();
    NullTransport t;
    auto rep = std::make_unique<PartitionReplica>(cfg(1), Role::Leader, 1,
        std::vector<rune::replication::NodeId>{1}, std::vector<rune::replication::NodeId>{1}, log.get(), &t);
    rep->setRegistryHandle(4, 200);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{});
    const WorkerOp::Type types[] = {
        WorkerOp::Type::PushRead, WorkerOp::Type::Subscribe, WorkerOp::Type::FetchRequest,
        WorkerOp::Type::OffsetsForLeaderEpochReq, WorkerOp::Type::ReconcileFetchResp,
        WorkerOp::Type::AppendControlRecord, WorkerOp::Type::ApplyReplicated,
        WorkerOp::Type::BecomeLeader, WorkerOp::Type::BecomeFollower};
    for (auto ty : types) {
        WorkerOp op;
        op.type = ty; op.topicId = 1; op.partition = 0; op.replica = rep.get(); op.logPtr = log.get();
        op.subscriptionId = 77; op.sessionId = 1;
        if (ty == WorkerOp::Type::AppendControlRecord) op.recordBatchBytes = batchAt(0);
        ASSERT_TRUE(w.tryEnqueueOp(stamp(std::move(op))));
    }
    rep.reset();
    log.reset();
    w.doWork(1);
    int pushRead = 0, refusedMarkers = 0, other = 0;
    w.drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::PushReadResult) ++pushRead;
        else if (r.type == WorkerReturn::Type::ControlRecordRefused) {
            ++refusedMarkers;
            EXPECT_EQ(r.errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
            EXPECT_FALSE(r.chunk.empty()) << "the marker travels back for re-routing";
        } else ++other;
    });
    EXPECT_EQ(w.opsRefusedNotOwnedForTest(), 9u);
    EXPECT_EQ(pushRead, 1) << "PushRead answered with an empty result (gateway re-issues)";
    EXPECT_EQ(refusedMarkers, 1) << "AppendControlRecord refusal is never silent";
    EXPECT_EQ(other, 0);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    EXPECT_FALSE(w.subscriptionsForTest()[0].active) << "Subscribe registered parked";
}

TEST(OwnershipGuardRegression, ParkedSubscriptionReactivatesByIncarnation) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    rep.setRegistryHandle(6, 300);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    WorkerOp sub;
    sub.type = WorkerOp::Type::Subscribe; sub.topicId = 1; sub.partition = 0;
    sub.replica = &rep; sub.logPtr = &log; sub.subscriptionId = 5; sub.sessionId = 1;
    ASSERT_TRUE(w.tryEnqueueOp(stamp(sub)));
    WorkerOp away; away.type = WorkerOp::Type::SetOwnership;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(away)));
    w.doWork(1);
    ASSERT_FALSE(w.subscriptionsForTest()[0].active);
    WorkerOp back; back.type = WorkerOp::Type::SetOwnership; back.ownedSet = {&rep};
    ASSERT_TRUE(w.tryEnqueueOp(std::move(back)));
    w.doWork(2);
    EXPECT_TRUE(w.subscriptionsForTest()[0].active) << "same incarnation granted back → re-activated";
    WorkerOp away2; away2.type = WorkerOp::Type::SetOwnership;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(away2)));
    w.doWork(3);
    WorkerOp back2; back2.type = WorkerOp::Type::SetOwnership; back2.ownedSet = {&rep};
    ASSERT_TRUE(w.tryEnqueueOp(std::move(back2)));
    ASSERT_TRUE(w.tryEnqueueOp(stamp(sub)));               // re-subscribe while owned
    w.doWork(4);
    EXPECT_TRUE(w.subscriptionsForTest()[0].active) << "a re-subscribe sets active = true";
}

namespace {
struct CapturingTxn final : rune::gateway::ITxnRequestHandler {
    int refused = 0; int32_t topic = -1, part = -1; int16_t err = 0;
    size_t handleInitProducerId(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddPartitions(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleAddOffsets(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleTxnOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    size_t handleEndTxn(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    void onControlMarkerRefused(int32_t t, int32_t p, int64_t, int16_t e, int32_t) override { ++refused; topic = t; part = p; err = e; }
};
} // namespace

TEST(OwnershipGuardRegression, MarkerRefusedForRemovedReplicaReachesTheCoordinator) {
    // A marker refused by the guard for a replica REMOVED from this node is
    // surfaced to the txn coordinator as NotLeader (never a silent drop).
    TmpDir d;
    auto log = std::make_unique<Log>(d.path.string(), 64LL * 1024 * 1024); log->recover();
    NullTransport t;
    auto rep = std::make_unique<PartitionReplica>(cfg(1), Role::Leader, 1,
        std::vector<rune::replication::NodeId>{1}, std::vector<rune::replication::NodeId>{1}, log.get(), &t);
    rep->setRegistryHandle(8, 400);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{});                 // removed: no longer owned anywhere here
    WorkerOp m;
    m.type = WorkerOp::Type::AppendControlRecord; m.topicId = 1; m.partition = 0;
    m.replica = rep.get(); m.logPtr = log.get(); m.recordBatchBytes = batchAt(0);
    ASSERT_TRUE(w.tryEnqueueOp(stamp(std::move(m))));
    w.doWork(1);
    std::vector<WorkerReturn> rets;
    w.drainReturn([&](WorkerReturn r) { rets.push_back(r); });
    ASSERT_EQ(rets.size(), 1u);
    ASSERT_EQ(rets[0].type, WorkerReturn::Type::ControlRecordRefused);
    auto gw = rune::gateway::ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; });
    CapturingTxn txn;
    gw.setTxnService(&txn);
    gw.handleWorkerReturnForTest(rets[0]);
    EXPECT_EQ(txn.refused, 1) << "the coordinator sees the refusal";
    EXPECT_EQ(txn.topic, 1);
    EXPECT_EQ(txn.err, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_EQ(gw.controlRecordsRefusedForTest(), 1u);
}

TEST(OwnershipGuardRegression, RemovedReplicaIsFreedOnlyAfterEveryWorkerAcked) {
    Reg R;
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    auto* r = R.add(5, 0);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    (void)r;
    R.reg.remove(5, 0);
    pool.reconcile(R.reg);                                 // SetOwnership(gen) to both workers
    EXPECT_EQ(R.reg.collectRetired(pool.minAckedOwnershipGen()), 0u) << "no ack yet: still reachable";
    EXPECT_EQ(R.reg.retiredCountForTest(), 1u);
    for (int i = 0; i < 2; ++i) {
        pool.worker(i).doWork(1);
        pool.worker(i).drainReturn([&](WorkerReturn ret) {
            if (ret.type == WorkerReturn::Type::OwnershipAck)
                pool.onOwnershipAck(ret.partition, static_cast<std::uint64_t>(ret.lastOffset));
        });
        if (i == 0) EXPECT_EQ(R.reg.collectRetired(pool.minAckedOwnershipGen()), 0u) << "one worker still unacked";
    }
    EXPECT_EQ(R.reg.collectRetired(pool.minAckedOwnershipGen()), 1u) << "freed once every worker acked";
}

TEST(OwnershipGuardRegression, RuntimeReplicaMoveIsAHardError) {
    Reg R;
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    auto* r = R.add(5, 0);
    const int32_t owner = pool.ownerWorkerId(5, 0);
    pool.setRunnersStartedForTest(true);
    pool.seedOwnerForTest(r->incarnation(), 1 - owner);   // as if it lived on the other worker
    EXPECT_THROW(pool.reconcile(R.reg), std::logic_error);
    // The control loop turns it into a halt (no exception on the runner thread).
    NoopConsensus noop;
    NodeRuntimeAgent rt(noop, nullptr, nullptr, nullptr, &R.reg, nullptr, nullptr, nullptr, nullptr, &pool);
    std::string what;
    rt.setFatalHook([&](const std::string& w) { what = w; });
    pool.seedOwnerForTest(r->incarnation(), 1 - owner);
    R.add(6, 0);                                           // registry changed → reconcile runs
    EXPECT_NO_THROW(rt.doWork(1));
    EXPECT_NE(what.find("MOVE"), std::string::npos) << what;
}

TEST(StorageFailStopRegression, LateErrorAfterMoveSignalsTheCachedDir) {
    StrictShard s;
    s.leader->setRegistryHandle(9, 500);
    s.w.setOwnedDirect(Worker::OwnedSet{s.leader.get()});
    s.leader->append(std::span<const uint8_t>(std::vector<uint8_t>(8, 1)));
    s.w.setReapPausedForTest(true);
    const auto tok = s.w.submitBgFlushForTest(s.log.get());
    ASSERT_NE(tok, kNoToken);
    const std::string dir = s.log->dir();
    WorkerOp away; away.type = WorkerOp::Type::SetOwnership;
    ASSERT_TRUE(s.w.tryEnqueueOp(std::move(away)));
    s.w.doWork(1);
    s.leader.reset();
    s.log.reset();                                         // the moved log is gone
    s.w.deliverCompletionForTest(FlushCompletion{tok, EIO, 0});
    int signals = 0; std::string got;
    s.w.drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::StorageFailure) { ++signals; got.assign(r.chunk.begin(), r.chunk.end()); }
    });
    EXPECT_EQ(signals, 1);
    EXPECT_EQ(got, dir) << "signalled with the dir cached while owned (no read of the moved Log)";
}

TEST(StorageFailStopRegression, WorkerPoolPathInternalReplicaFailureReachesHalt) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    auto rep = std::make_unique<PartitionReplica>(cfg(2, 1, -2, 0), Role::Follower, 1,
        std::vector<rune::replication::NodeId>{1, 2}, std::vector<rune::replication::NodeId>{1, 2}, &log, &t);
    rep->becomeFollower(1, 1);
    const auto b = batchAt(0);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, -2, std::span<const uint8_t>(b)};
    rep->applyReplicated(resp);
    ReplicaRegistry reg;
    reg.add(-2, 0, std::move(rep));
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(reg);
    NoopConsensus noop;
    NodeRuntimeAgent rt(noop, nullptr, nullptr, nullptr, &reg, nullptr, nullptr, nullptr, nullptr, &pool);
    int calls = 0;
    rt.setStorageFailureHook([&](const std::string&, int) { ++calls; });
    log.setSyncFaultForTest(EIO);
    EXPECT_NO_THROW(rt.doWork(25 * kMs));
    EXPECT_EQ(calls, 1) << "the WorkerPool-path internal replica reaches the halt";
}

TEST(StorageFailStopRegression, SchemaFollowerFlushFailureSignalsTheControlThread) {
    // I1: the admin thread's schema log → admin→control failure ring.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica schema(cfg(2, 1, -4, 0), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    schema.becomeFollower(1, 1);
    const auto b = batchAt(0);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, -4, std::span<const uint8_t>(b)};
    schema.applyReplicated(resp);
    SchemaRegistry sreg;
    AdminWorkerAgent agent(sreg, &schema, 2, {1, 2}, AdminDeps{});
    log.setSyncFaultForTest(EIO);
    EXPECT_NO_THROW(agent.doWork(25 * kMs));
    int msgs = 0; int32_t e = 0; std::string dir;
    agent.failureRing().drain([&](int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                                  aeron::util::index_t len) {
        ++msgs;
        const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
        std::memcpy(&e, p, 4);
        dir.assign(reinterpret_cast<const char*>(p) + 4, static_cast<std::size_t>(len) - 4);
    }, 4);
    EXPECT_EQ(msgs, 1);
    EXPECT_EQ(e, EIO);
    EXPECT_EQ(dir, log.dir());
    agent.doWork(50 * kMs);
    msgs = 0;
    agent.failureRing().drain([&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                  aeron::util::index_t) { ++msgs; }, 4);
    EXPECT_EQ(msgs, 0) << "signalled once";
}

TEST(StorageFailStopRegression, RecoverIndexRenameFailureFailStopsWithoutThrowing) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    const auto idx = d.path / "00000000000000000000.index";
    fs::remove(idx);
    fs::create_directories(idx / "blocker");               // rename(.tmp → .index) now fails
    EXPECT_NO_THROW(log.recover());
    EXPECT_TRUE(log.storageFailed()) << "recover rename failure = fail-stop (halt), not an uncaught throw";
}

// ============================================================================
// Fix round 3 addendum.
// ============================================================================

TEST(OwnershipGuardRegression, BackPressuredRoleOpIsDeliveredOnALaterCycle) {
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), /*ingress*/ 1 << 12, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    auto* r = R.add(5, 0);
    pool.reconcile(R.reg);
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (pool.worker(0).tryEnqueueOp(filler)) {}          // ring full: the role op is back-pressured
    ReplicationMetadataBridge bridge(1, {1, 2}, R.reg);
    bridge.setWorkerPool(&pool);
    bridge.routeBecomeFollowerForTest(r, 2, 2);
    ASSERT_EQ(bridge.pendingRoleOpsForTest(), 1u) << "not dropped: queued for retry";
    for (int i = 0; i < 16 && bridge.pendingRoleOpsForTest() > 0; ++i) {   // later cycles
        pool.worker(0).doWork(1 + i);
        pool.worker(0).drainReturn([](WorkerReturn) {});
        bridge.flushPendingRoleOpsForTest();
    }
    ASSERT_EQ(bridge.pendingRoleOpsForTest(), 0u);
    for (int i = 0; i < 16 && r->role() != Role::Follower; ++i) pool.worker(0).doWork(100 + i);
    EXPECT_EQ(r->role(), Role::Follower) << "the role change was delivered and applied";
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 0u);
}

TEST(OwnershipGuardRegression, PendingRoleOpsKeepPerReplicaOrderAndDropRemovedReplicas) {
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 12, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    auto* r = R.add(5, 0);
    auto* gone = R.add(6, 0);
    pool.reconcile(R.reg);
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (pool.worker(0).tryEnqueueOp(filler)) {}
    ReplicationMetadataBridge bridge(1, {1, 2}, R.reg);
    bridge.setWorkerPool(&pool);
    bridge.routeBecomeFollowerForTest(r, 2, 2);
    bridge.routeBecomeLeaderForTest(r, 3, {1});              // must apply AFTER the follower op
    bridge.routeBecomeFollowerForTest(gone, 2, 2);
    // Round 4 (M2): the Leader op SUPERSEDES the pending Follower op of the same
    // replica (coalesced in place — latest absolute role wins).
    ASSERT_EQ(bridge.pendingRoleOpsForTest(), 2u);
    EXPECT_EQ(bridge.roleOpsCoalescedForTest(), 1u);
    R.reg.remove(6, 0);                                      // removed before delivery: moot
    for (int i = 0; i < 32 && bridge.pendingRoleOpsForTest() > 0; ++i) {
        pool.worker(0).doWork(1 + i);
        pool.worker(0).drainReturn([](WorkerReturn) {});
        bridge.flushPendingRoleOpsForTest();
    }
    ASSERT_EQ(bridge.pendingRoleOpsForTest(), 0u);
    for (int i = 0; i < 16; ++i) pool.worker(0).doWork(100 + i);
    EXPECT_EQ(r->role(), Role::Leader) << "FIFO: follower then leader";
}

TEST(OwnershipGuardRegression, RefusedMarkerWithUnknownTokenForRegisteredPartitionHalts) {
    // Round 4: a refusal the gateway cannot match to a marker it issued, while
    // the partition is still registered here, is an invariant violation (halt).
    // (A refusal of a KNOWN marker is re-routed — see MarkerRefusedWhileRegistered...)
    Reg R;
    R.add(5, 0);
    auto gw = rune::gateway::ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; });
    gw.setReplicaRegistry(&R.reg);
    CapturingTxn txn;
    gw.setTxnService(&txn);
    std::string what;
    gw.setInvariantViolationHandler([&](const std::string& w) { what = w; });
    WorkerReturn ret;                                        // forced: refused while still on the node
    ret.type = WorkerReturn::Type::ControlRecordRefused;
    ret.topicId = 5; ret.partition = 0; ret.correlationId = 999;
    ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
    const int64_t leoBefore = R.reg.lookup(5, 0)->log()->logEndOffset();
    gw.handleWorkerReturnForTest(ret);
    EXPECT_NE(what.find("is registered on this node"), std::string::npos) << what;
    EXPECT_EQ(R.reg.lookup(5, 0)->log()->logEndOffset(), leoBefore) << "nothing is re-appended";
    EXPECT_EQ(txn.refused, 0);
}

TEST(OwnershipGuardRegression, InvariantMapStaysBoundedUnderChurn) {
    Reg R;
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    R.add(1, 0);                                             // two long-lived replicas
    R.add(1, 1);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    auto ackAll = [&] {
        for (int w = 0; w < 2; ++w) {
            pool.worker(w).doWork(1);
            pool.worker(w).drainReturn([&](WorkerReturn ret) {
                if (ret.type == WorkerReturn::Type::OwnershipAck)
                    pool.onOwnershipAck(ret.partition, static_cast<std::uint64_t>(ret.lastOffset));
            });
        }
        R.reg.collectRetired(pool.minAckedOwnershipGen());
    };
    for (int i = 0; i < 200; ++i) {
        R.add(100 + i, 0);
        pool.reconcile(R.reg); ackAll();
        R.reg.remove(100 + i, 0);
        pool.reconcile(R.reg); ackAll();
        pool.reconcile(R.reg); ackAll();
    }
    EXPECT_EQ(pool.ownerMapSizeForTest(), R.reg.replicaCount()) << "bounded by the live replicas";
    EXPECT_EQ(R.reg.retiredCountForTest(), 0u);
}

// ============================================================================
// Fix round 4 — replica lifetime on the CONTROL thread + residual races.
// Every control-thread holder refers to a replica by (registryId,
// incarnation) and resolves it through the registry at use time.
// ============================================================================
namespace {
// Drive every worker once and feed OwnershipAcks back (what the gateway does).
void ackAll(WorkerPool& pool, int64_t now = 1) {
    for (int w = 0; w < pool.workerCount(); ++w) {
        pool.worker(w).doWork(now);
        pool.worker(w).drainReturn([&](WorkerReturn ret) {
            if (ret.type == WorkerReturn::Type::OwnershipAck)
                pool.onOwnershipAck(ret.partition, static_cast<std::uint64_t>(ret.lastOffset));
        });
    }
}
// Run a worker for a fixed, bounded number of passes (enough to drain a full
// ingress ring of filler ops — a no-op op may report zero work, so "idle" is
// not a usable stop condition).  Never sleeps.
void drainIdle(Worker& w) {
    for (int i = 0; i < 4'096; ++i) {
        w.doWork(1 + i);
        w.drainReturn([](WorkerReturn) {});
    }
}
// Partition of `topic` routed to worker `w` (ShardRouter is a pure function).
int32_t partitionOn(const WorkerPool& pool, int32_t topic, int32_t w) {
    for (int32_t p = 0; p < 1024; ++p) if (pool.ownerWorkerId(topic, p) == w) return p;
    return -1;
}
std::vector<uint8_t> encodeFetchReq(int32_t topic, int32_t part, uint16_t tid = rune::replication::FetchRequestMsg::TEMPLATE_ID) {
    std::vector<uint8_t> f(rune::replication::FetchRequestMsg::TOTAL_SIZE);
    rune::replication::FetchRequestMsg::encode(f.data(), 0, 1, part, 2, 0, 4096, topic);
    rune::replication::stampTemplateId(f.data(), 0, tid);
    return f;
}
std::vector<uint8_t> encodeFetchResp(int32_t topic, int32_t part, uint16_t tid = rune::replication::FetchResponseMsg::TEMPLATE_ID) {
    const auto b = batchAt(0);
    std::vector<uint8_t> f(rune::replication::FetchResponseMsg::encodedSize(b.size()));
    rune::replication::FetchResponseMsg::encode(f.data(), 0, 1, part, 0, 0, rune::replication::kErrNone,
                                                std::span<const uint8_t>(b), topic);
    rune::replication::stampTemplateId(f.data(), 0, tid);
    return f;
}
std::vector<uint8_t> encodeOleReq(int32_t topic, int32_t part) {
    std::vector<uint8_t> f(rune::replication::OffsetsForLeaderEpochReqMsg::TOTAL_SIZE);
    rune::replication::OffsetsForLeaderEpochReqMsg::encode(f.data(), 0, 1, part, 2, 1, 0, topic);
    return f;
}
std::vector<uint8_t> encodeOleResp(int32_t topic, int32_t part) {
    std::vector<uint8_t> f(rune::replication::OffsetsForLeaderEpochRespMsg::TOTAL_SIZE);
    rune::replication::OffsetsForLeaderEpochRespMsg::encode(f.data(), 0, 1, part, 1, 0, rune::replication::kErrNone, topic);
    return f;
}
std::vector<std::vector<uint8_t>> everyReplicationFrame(int32_t topic, int32_t part) {
    return {encodeFetchReq(topic, part), encodeFetchResp(topic, part), encodeOleReq(topic, part),
            encodeOleResp(topic, part),
            encodeFetchReq(topic, part, rune::replication::kReconcileFetchReqTemplateId),
            encodeFetchResp(topic, part, rune::replication::kReconcileFetchRespTemplateId)};
}
struct Captured {
    std::vector<int16_t> produceErrors;
    rune::gateway::ClientGateway::TestEgressFn fn() {
        return [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (auto r = pc::ProduceResponse::decode(b, n)) produceErrors.push_back(r->errorCode);
            return 1;
        };
    }
};
} // namespace

TEST(ReplicaLifetimeRegression, WorkerEnqueueNeverReadsThroughTheOpPointer) {
    // Critical 1 (Worker.h tryEnqueueOp): the identity is carried in the op —
    // the control thread never dereferences op.replica.  A garbage pointer
    // (= a freed replica) must not be touched at enqueue NOR by the worker.
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{});
    WorkerOp op;
    op.type = WorkerOp::Type::FetchRequest; op.topicId = 5; op.partition = 0;
    op.replica = reinterpret_cast<PartitionReplica*>(static_cast<std::uintptr_t>(0x10));   // unmapped
    op.targetReplicaId = 0; op.incarnation = ReplicaRegistry::kStaleIncarnation;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op))) << "enqueue reads nothing through the pointer";
    w.doWork(1);
    EXPECT_EQ(w.opsRefusedNotOwnedForTest(), 1u) << "refused by identity, the pointer never dereferenced";
}

TEST(ReplicaLifetimeRegression, LateOpsForAFreedReplicaAreStampedStaleAndRefused) {
    // Critical 1: remove a partition → every worker acks → collectRetired frees
    // it → late ops carrying the FREED pointer (what the transport's old
    // replicas_ map handed out) are routed: WorkerPool stamps them from the
    // registry at routing time (gone → stale, nullptr) and the worker refuses
    // them through each op's normal response path.  Nothing is dereferenced.
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    PartitionReplica* stale = R.add(5, 0);
    pool.reconcile(R.reg);
    ackAll(pool);
    ASSERT_EQ(pool.worker(0).ownedCount(), 1u);
    R.reg.remove(5, 0);
    pool.reconcile(R.reg);
    ackAll(pool, 2);
    ASSERT_EQ(R.reg.collectRetired(pool.minAckedOwnershipGen()), 1u) << "freed: every worker acked";
    const WorkerOp::Type types[] = {
        WorkerOp::Type::FetchRequest, WorkerOp::Type::ApplyReplicated,
        WorkerOp::Type::OffsetsForLeaderEpochReq, WorkerOp::Type::OffsetsForLeaderEpochResp,
        WorkerOp::Type::ReconcileFetchReq, WorkerOp::Type::ReconcileFetchResp,
        WorkerOp::Type::Produce, WorkerOp::Type::PushRead, WorkerOp::Type::AppendControlRecord};
    for (auto ty : types) {
        WorkerOp op;
        op.type = ty; op.topicId = 5; op.partition = 0; op.replica = stale;   // dangling
        op.sessionId = 1; op.correlationId = 77; op.subscriptionId = 3;
        if (ty == WorkerOp::Type::AppendControlRecord || ty == WorkerOp::Type::Produce) op.recordBatchBytes = batchAt(0);
        if (ty == WorkerOp::Type::ApplyReplicated || ty == WorkerOp::Type::ReconcileFetchResp) op.replicatedPayload.records = batchAt(0);
        ASSERT_TRUE(pool.tryEnqueueOp(5, 0, std::move(op)));
    }
    EXPECT_EQ(pool.staleOpsRoutedForTest(), 9u) << "every late op stamped stale at routing time";
    pool.worker(0).doWork(3);
    int produceNotLeader = 0, markerRefused = 0, pushRead = 0;
    pool.worker(0).drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult &&
            r.errorCode == static_cast<int16_t>(pc::ErrorCode::NotLeader)) ++produceNotLeader;
        if (r.type == WorkerReturn::Type::ControlRecordRefused) ++markerRefused;
        if (r.type == WorkerReturn::Type::PushReadResult) ++pushRead;
    });
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 9u);
    EXPECT_EQ(produceNotLeader, 1);
    EXPECT_EQ(markerRefused, 1);
    EXPECT_EQ(pushRead, 1);

    // Re-created at the same (topic, partition) — possibly at the SAME address:
    // a late op carrying the old pointer is routed to the NEW incarnation by the
    // registry (the wire keys by topic/partition), never by the pointer.
    PartitionReplica* fresh = R.add(5, 0);
    WorkerOp late;
    late.type = WorkerOp::Type::FetchRequest; late.topicId = 5; late.partition = 0; late.replica = stale;
    late.fetchRequestPayload.followerId = 2; late.fetchRequestPayload.maxBytes = 4096; late.fetchRequestPayload.topicId = 5;
    ASSERT_TRUE(pool.tryEnqueueOp(5, 0, std::move(late)));
    pool.worker(0).doWork(4);
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 9u) << "routed to the live incarnation (address reused="
                                                              << (fresh == stale) << ")";
}

TEST(ReplicaLifetimeRegression, TransportHoldsNoReplicaPointerAndDropsFramesForAFreedPartition) {
    // Critical 1 (transport): with the registry wired, the transport holds NO
    // replica pointer (registerReplica is ignored) and resolves each frame at
    // use time — a freed partition's late FetchRequest / FetchResponse / OLE /
    // Reconcile frames are dropped without any dereference.
    TmpDir ad;
    EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx; ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rune::replication::AeronPartitionTransport tr(client, "aeron:ipc", 7301, /*self*/1, 1 << 20);
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    tr.setReplicaRegistry(&R.reg);
    tr.setWorkerPool(&pool);
    PartitionReplica* r = R.add(5, 0);
    tr.registerReplica(5, 0, r);
    EXPECT_EQ(tr.heldReplicaPointersForTest(), 0u) << "the registry is the only source";
    pool.reconcile(R.reg);
    ackAll(pool);
    // Live: a FetchRequest frame reaches the owning worker.
    const auto live = encodeFetchReq(5, 0);
    tr.deliverFrameForTest(live.data(), live.size(), 1);
    pool.worker(0).doWork(2);
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 0u);
    R.reg.remove(5, 0);
    pool.reconcile(R.reg);
    ackAll(pool, 3);
    ASSERT_EQ(R.reg.collectRetired(pool.minAckedOwnershipGen()), 1u);
    for (const auto& f : everyReplicationFrame(5, 0)) tr.deliverFrameForTest(f.data(), f.size(), 4);
    pool.worker(0).doWork(5);
    EXPECT_EQ(pool.staleOpsRoutedForTest(), 0u) << "dropped at the transport: nothing routed";
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 0u);
}

TEST(ReplicaLifetimeRegression, FollowerOleResponseAndReconcileRequestRunOnTheOwningWorker) {
    // Pre-existing race fixed on sight: OLE responses and reconcile requests for
    // a USER replica were executed inline on the control thread.  They are now
    // routed to the owning worker (gated by identity like every partition op).
    TmpDir ad;
    EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx; ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rune::replication::AeronPartitionTransport tr(client, "aeron:ipc", 7302, /*self*/1, 1 << 20);
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    tr.setReplicaRegistry(&R.reg);
    tr.setWorkerPool(&pool);
    R.add(5, 0);
    pool.reconcile(R.reg);
    ackAll(pool);
    const uint64_t before = pool.worker(0).followerSideOpsForTest();
    const auto ole = encodeOleResp(5, 0);
    const auto rec = encodeFetchReq(5, 0, rune::replication::kReconcileFetchReqTemplateId);
    tr.deliverFrameForTest(ole.data(), ole.size(), 1);
    tr.deliverFrameForTest(rec.data(), rec.size(), 1);
    pool.worker(0).doWork(2);
    EXPECT_EQ(pool.worker(0).followerSideOpsForTest() - before, 2u) << "both executed by the owning worker";
    EXPECT_EQ(pool.worker(0).opsRefusedNotOwnedForTest(), 0u);
}

TEST(ReplicaLifetimeRegression, RingFullWorkerGetsOneSetOwnershipPerGenerationOthersExactlyOne) {
    // Important 1: one worker's ring is full during a topic create.  The other
    // worker receives exactly ONE SetOwnership for the generation; routed ops
    // never trigger a full re-computation; the full worker gets exactly one
    // once its ring drains.
    Reg R;
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 12, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    const int32_t pA = partitionOn(pool, 7, 0), pB = partitionOn(pool, 7, 1);
    ASSERT_GE(pA, 0); ASSERT_GE(pB, 0);
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (pool.worker(0).tryEnqueueOp(filler)) {}          // worker 0: ring full
    const uint64_t sent0 = pool.setOwnershipSentForTest(0), sent1 = pool.setOwnershipSentForTest(1);
    R.add(7, pA);
    R.add(7, pB);                                            // "topic create"
    pool.reconcile(R.reg);
    const uint64_t computed = pool.assignmentsComputedForTest();
    EXPECT_EQ(pool.setOwnershipSentForTest(1) - sent1, 1u);
    EXPECT_EQ(pool.setOwnershipSentForTest(0) - sent0, 0u) << "ring full: not delivered yet";
    for (int i = 0; i < 100; ++i) {
        EXPECT_FALSE(pool.tryEnqueueOp(7, pA, produceOp(R.reg.lookup(7, pA), i))) << "back-pressured";
        pool.reconcile(R.reg);                               // every control cycle
    }
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(pool.tryEnqueueOp(7, pB, produceOp(R.reg.lookup(7, pB), i)));
    EXPECT_EQ(pool.assignmentsComputedForTest(), computed) << "routed ops never re-compute the owned sets";
    EXPECT_EQ(pool.setOwnershipSentForTest(1) - sent1, 1u) << "the healthy worker got exactly one per generation";
    pool.worker(0).doWork(1);                               // drains the fillers
    pool.reconcile(R.reg);
    EXPECT_EQ(pool.setOwnershipSentForTest(0) - sent0, 1u) << "delivered once, on retry";
    EXPECT_EQ(pool.setOwnershipSentForTest(1) - sent1, 1u) << "no re-send to the healthy worker";
    EXPECT_EQ(pool.assignmentsComputedForTest(), computed);
}

TEST(ReplicaLifetimeRegression, ParkedSubscriptionIsNotReactivatedOntoANewIncarnationAtTheSameAddress) {
    // Important 2: rebuildFlushSlots_ used to overwrite `active` with a pointer
    // compare after rebuildOwnedIndex_ set it by incarnation.  Same address, same
    // id, NEW incarnation = a different replica: the parked subscription stays parked.
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica rep(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    rep.setRegistryHandle(6, 300);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    WorkerOp sub;
    sub.type = WorkerOp::Type::Subscribe; sub.topicId = 1; sub.partition = 0;
    sub.replica = &rep; sub.logPtr = &log; sub.subscriptionId = 5; sub.sessionId = 1;
    ASSERT_TRUE(w.tryEnqueueOp(stamp(sub)));
    WorkerOp away; away.type = WorkerOp::Type::SetOwnership;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(away)));
    w.doWork(1);
    ASSERT_FALSE(w.subscriptionsForTest()[0].active);
    rep.setRegistryHandle(6, 301);                          // "destroyed + re-created at the same address"
    WorkerOp back; back.type = WorkerOp::Type::SetOwnership; back.ownedSet = {&rep};
    ASSERT_TRUE(w.tryEnqueueOp(std::move(back)));
    w.doWork(2);
    EXPECT_FALSE(w.subscriptionsForTest()[0].active) << "a new incarnation never re-activates a parked subscription";
}

TEST(ReplicaLifetimeRegression, PendingAckForARemovedPartitionFailsNotLeaderWithoutDereference) {
    // Important 3: an acks=all entry in flight when its partition is removed and
    // FREED → it fails NotLeader without dereferencing the freed replica.  Also
    // for a re-created partition (new incarnation, same registry id).
    // F15 I-A: the waiter lives on the OWNING worker in every mode (the gateway's
    // pendingAcks_ no longer holds worker-owned acks), so the invariant is pinned
    // where the waiter is: a gateway-routed acks=all produce (executeProduceWire_,
    // deferAck) parked on the worker; the ownership drop answers it.
    for (bool recreate : {false, true}) {
        Reg R;
        auto* rep = R.add(5, 0, {1, 2});                      // replica set {1, 2}
        rep->becomeLeader(2, {1, 2});                         // follower 2 behind → the ack parks on the HW
        // Follower 2 reports in at offset 0 (behind): the HW stays put.
        (void)rep->onFetchRequest(rune::replication::FetchRequestMsg{2, 0, 2, 0, 0, 0}, 1);
        ASSERT_TRUE(rep->acceptsProduce());
        Worker w(0, 1 << 16, 1 << 16, 64, 64);
        w.setOwnedDirect(Worker::OwnedSet{rep});
        WorkerOp op = produceOp(rep, 77);
        op.deferAck = true;                                   // acks=all, steady state
        ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
        w.doWork(1);
        ASSERT_EQ(w.reservedAckSlotsForTest(), 1) << "precondition: parked on the owning worker";
        std::vector<WorkerReturn> rets;
        w.drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult) rets.push_back(std::move(r));
        });
        ASSERT_TRUE(rets.empty()) << "nothing answered before the move (F15 S5: no facts-only return)";
        R.reg.remove(5, 0);
        PartitionReplica* again = recreate ? R.add(5, 0) : nullptr;
        WorkerOp own;
        own.type = WorkerOp::Type::SetOwnership;
        if (again) own.ownedSet = {again};
        ASSERT_TRUE(w.tryEnqueueOp(std::move(own)));
        w.doWork(2);
        ASSERT_EQ(R.reg.collectRetired(R.reg.generation()), 1u) << "freed";
        w.doWork(3);                                          // no read of the freed replica
        w.drainReturn([&](WorkerReturn r) {
            // answers only (skip OwnershipAck)
            if (r.type != WorkerReturn::Type::ProduceResult) return;
            if (r.deferAck) ADD_FAILURE() << "a facts-only return (deleted by F15 S5)";
            rets.push_back(std::move(r));
        });
        EXPECT_EQ(w.reservedAckSlotsForTest(), 0);
        ASSERT_EQ(rets.size(), 1u) << "recreate=" << recreate;
        EXPECT_EQ(rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "recreate=" << recreate;
    }
}

namespace {
// A gateway in per-worker mode over one real user partition (topic 0 / part 0).
struct MarkerRig {
    TmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    NullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    Captured cap;
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(cap.fn());
    PartitionReplica* r = nullptr;
    explicit MarkerRig(int minIsr = 1) {
        auto pc0 = cfg(1, 1, 0, 0);
        pc0.minIsr = minIsr;
        auto rep = std::make_unique<PartitionReplica>(pc0, Role::Leader, 1,
            std::vector<rune::replication::NodeId>{1}, std::vector<rune::replication::NodeId>{1},
            topics.log(0, 0), &t);
        r = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setPerWorkerIngressActive(true);
    }
    void runWorker(int64_t now) {
        pool.worker(0).doWork(now);
        pool.worker(0).drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
    }
};
// A REAL txn marker (control batch) for `pid`.  F15 N2: markers are appended
// as control batches (appendControlBatch), which refuses anything else — the
// old stand-in (a data batch with the pid stamped in) is no longer a marker.
std::vector<uint8_t> txnBatch(int64_t pid) {
    return rune::transaction::ControlMarker::build(pid, 0, rune::transaction::ControlType::Abort, 0);
}
} // namespace

TEST(ReplicaLifetimeRegression, MarkerOffsetIsTheWorkersActualAppendOffset) {
    // P1: produces queued AHEAD of the marker in the worker ring land first; the
    // coordinator's bookkeeping must record the marker where it ACTUALLY landed
    // (3), not log->logEndOffset() at enqueue time (0).
    MarkerRig m;
    // F15 S4: the read_committed state lives on the OWNING worker (Option B) —
    // the txn is opened by a real transactional batch at offset 0 (the first of
    // the three produces queued ahead of the marker), observed on the worker.
    for (int i = 0; i < 3; ++i) {
        WorkerOp p = produceOp(m.r, 100 + i);
        if (i == 0) {
            rune::protocol::RecordBatchBuilder b(0, 1000);
            b.setTransactional(true); b.setProducerId(7); b.setProducerEpoch(0); b.setBaseSequence(0);
            b.append("k", "txn", 1000);
            p.recordBatchBytes = b.build(); p.producerId = 7;   // txn opened at offset 0
        }
        ASSERT_TRUE(m.pool.tryEnqueueOp(0, 0, std::move(p)));
    }
    const auto marker = txnBatch(7);
    const int64_t rc = m.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 7, /*commit=*/false, /*coordinatorEpoch=*/0);
    EXPECT_EQ(rc, rune::gateway::ClientGateway::kControlRecordAsync) << "never a pre-computed offset";
    EXPECT_TRUE(m.pool.worker(0).rcAbortedRangesForTest(0, 0).empty()) << "nothing recorded before the append happened";
    m.runWorker(1);
    const auto ranges = m.pool.worker(0).rcAbortedRangesForTest(0, 0);
    ASSERT_EQ(ranges.size(), 1u);
    EXPECT_EQ(ranges[0].producerId, 7);
    EXPECT_EQ(ranges[0].lastOffset, 3) << "the marker's AUTHORITATIVE offset (3 produces landed first)";
    EXPECT_EQ(m.pool.worker(0).rcOpenTxnsForTest(0, 0), 0u);
    EXPECT_EQ(m.gw.markersInFlightForTest(), 0u);
}

TEST(ReplicaLifetimeRegression, BackPressuredMarkerIsQueuedNotDroppedAndHoldsLaterProduces) {
    MarkerRig m;
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (m.pool.worker(0).tryEnqueueOp(filler)) {}       // ring full
    const auto marker = txnBatch(9);
    EXPECT_EQ(m.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 9, true, /*coordinatorEpoch=*/0),
              rune::gateway::ClientGateway::kControlRecordAsync);
    EXPECT_EQ(m.gw.markersQueuedForTest(), 1u) << "queued, never dropped";
    drainIdle(m.pool.worker(0));                            // ring drains (fillers; bounded)
    // The ring has space again but the marker has not been re-sent yet: a later
    // produce to the SAME partition must not overtake it (retriable NotLeader).
    const int64_t sid = m.gw.injectTestSession();
    {
        pc::Produce msg;
        const auto batch = batchAt(0);
        msg.sessionId = sid; msg.correlationId = 6; msg.topicId = 0; msg.partition = 0; msg.partitionKnown = 1;
        msg.acks = 0; msg.producerId = -1; msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        m.gw.applyProduceForTest(frame.data(), frame.size());
    }
    ASSERT_EQ(m.cap.produceErrors.size(), 1u);
    EXPECT_EQ(m.cap.produceErrors[0], static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "held behind the marker";
    EXPECT_EQ(m.gw.pumpRetryQueuesForTest(), 1);
    EXPECT_EQ(m.gw.markersQueuedForTest(), 0u);
    for (int i = 0; i < 64 && m.gw.markersInFlightForTest() > 0; ++i) m.runWorker(10'000 + i);   // bounded
    EXPECT_EQ(m.gw.markersInFlightForTest(), 0u) << "appended and acknowledged";
    EXPECT_EQ(m.r->log()->logEndOffset(), 1);
}

TEST(ReplicaLifetimeRegression, MarkerRefusedWhileRegisteredIsReRoutedAndLandsOnce) {
    // P1: re-routing is SAFE now (no pre-computed offset).  A refusal of a known
    // marker while the partition is registered re-routes it; it lands exactly once.
    MarkerRig m;
    const auto marker = txnBatch(11);
    ASSERT_EQ(m.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 11, true, /*coordinatorEpoch=*/0),
              rune::gateway::ClientGateway::kControlRecordAsync);
    // Drop the worker's copy and simulate the guard refusing it (token = 1).
    m.pool.worker(0).doWork(1);
    std::vector<WorkerReturn> rets;
    m.pool.worker(0).drainReturn([&](WorkerReturn r) { rets.push_back(r); });
    ASSERT_EQ(rets.size(), 1u);
    ASSERT_EQ(rets[0].type, WorkerReturn::Type::ControlRecordAppended);
    const int64_t leoAfterFirst = m.r->log()->logEndOffset();
    // A forced refusal for a DIFFERENT issued marker (re-issue a second one).
    ASSERT_EQ(m.gw.appendControlRecord(0, 0, std::span<const uint8_t>(marker), 11, true, /*coordinatorEpoch=*/0),
              rune::gateway::ClientGateway::kControlRecordAsync);
    WorkerReturn refused;
    refused.type = WorkerReturn::Type::ControlRecordRefused;
    refused.topicId = 0; refused.partition = 0; refused.correlationId = 2;
    refused.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
    std::string what;
    m.gw.setInvariantViolationHandler([&](const std::string& w) { what = w; });
    m.gw.handleWorkerReturnForTest(rets[0]);              // the first one's real reply
    m.gw.handleWorkerReturnForTest(refused);               // refusal of #2 → re-routed
    EXPECT_TRUE(what.empty()) << what;
    m.runWorker(2);                                         // #2 original + its re-route
    EXPECT_EQ(m.gw.markersInFlightForTest(), 0u);
    EXPECT_GE(m.r->log()->logEndOffset(), leoAfterFirst + 1);
}

TEST(ReplicaLifetimeRegression, RunnersStartedIsNeverWrittenByStopWhileControlRuns) {
    // P2: stopAndJoinRunners() must not write the control-thread flag; the pool
    // is armed before the control thread starts and disarmed after it is joined.
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.armRingOwnership();
    EXPECT_TRUE(pool.ringOwnershipArmedForTest());
    pool.stopAndJoinRunners();                              // (never started) — must not touch the flag
    EXPECT_TRUE(pool.ringOwnershipArmedForTest()) << "stop never writes control-thread state";
    pool.disarmAfterControlJoined();
    EXPECT_FALSE(pool.ringOwnershipArmedForTest());
}

TEST(ReplicaLifetimeRegression, BridgeCatchesAnOwnershipInvariantThrowAndHandsItToTheHaltPath) {
    // M1: the F7 throw (inside WorkerPool's inline reconcile) used to cross the
    // bridge's noexcept chain → std::terminate.  Caught at the bridge boundary.
    Reg R;
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    auto* r = R.add(5, 0);                                  // generation moved, not reconciled yet
    pool.seedOwnerForTest(r->incarnation(), 1 - pool.ownerWorkerId(5, 0));
    ReplicationMetadataBridge bridge(1, {1, 2}, R.reg);
    bridge.setWorkerPool(&pool);
    bridge.routeBecomeFollowerForTest(r, 2, 2);             // would std::terminate before
    std::string what;
    ASSERT_TRUE(bridge.takeFatal(what));
    EXPECT_NE(what.find("MOVE"), std::string::npos) << what;
    EXPECT_FALSE(bridge.takeFatal(what)) << "reported once";
}

TEST(ReplicaLifetimeRegression, RoleOpQueuesArePerWorkerAndCoalesce) {
    // M2: a full ring on worker 0 never holds back role ops for worker 1.
    Reg R;
    WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 12, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    const int32_t pA = partitionOn(pool, 3, 0), pB = partitionOn(pool, 3, 1);
    auto* a = R.add(3, pA);
    auto* b = R.add(3, pB);
    pool.reconcile(R.reg);
    ackAll(pool);
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (pool.worker(0).tryEnqueueOp(filler)) {}
    ReplicationMetadataBridge bridge(1, {1, 2}, R.reg);
    bridge.setWorkerPool(&pool);
    bridge.routeBecomeFollowerForTest(a, 2, 2);             // worker 0: queued
    bridge.routeBecomeLeaderForTest(a, 3, {1});             // supersedes it
    bridge.routeBecomeFollowerForTest(a, 4, 2);             // supersedes again
    EXPECT_EQ(bridge.pendingRoleOpsForWorkerForTest(0), 1u) << "bounded by replicas, not churn";
    EXPECT_EQ(bridge.roleOpsCoalescedForTest(), 2u);
    bridge.routeBecomeFollowerForTest(b, 2, 2);             // worker 1: delivered immediately
    EXPECT_EQ(bridge.pendingRoleOpsForWorkerForTest(1), 0u) << "no head-of-line blocking across workers";
    for (int i = 0; i < 8; ++i) pool.worker(1).doWork(10 + i);
    EXPECT_EQ(b->role(), Role::Follower);
    pool.worker(0).doWork(20);
    bridge.flushPendingRoleOpsForTest();
    for (int i = 0; i < 8; ++i) pool.worker(0).doWork(30 + i);
    EXPECT_EQ(a->role(), Role::Follower) << "the latest (coalesced) role applied";
    EXPECT_EQ(bridge.pendingRoleOpsForTest(), 0u);
}

TEST(ReplicaLifetimeRegression, BackPressuredRemovalFenceIsMootOnceTheReplicaIsDropped) {
    // M2: a back-pressured removal fence is never silently lost — either
    // delivered, or provably moot: the SetOwnership dropping the replica is
    // delivered to the worker, which then never drives it again (and the
    // replica is freed only after that worker acked).  The drop is counted.
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 12, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    R.add(5, 0);
    pool.reconcile(R.reg);
    ackAll(pool);
    ASSERT_EQ(pool.worker(0).ownedCount(), 1u);
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (pool.worker(0).tryEnqueueOp(filler)) {}
    ReplicationMetadataBridge bridge(1, {1, 2}, R.reg);
    bridge.setWorkerPool(&pool);
    bridge.applyDeleteForTest(5, 0);                        // fence (back-pressured) + registry remove
    ASSERT_EQ(bridge.pendingRoleOpsForTest(), 1u) << "the fence waits; it is not dropped";
    pool.worker(0).doWork(1);                               // ring drains
    pool.worker(0).drainReturn([](WorkerReturn) {});
    pool.reconcile(R.reg);                                  // top of the next control cycle
    bridge.flushPendingRoleOpsForTest();
    EXPECT_EQ(bridge.pendingRoleOpsForTest(), 0u);
    EXPECT_EQ(bridge.fencesMootForTest(), 1u) << "moot: the replica is no longer registered";
    ackAll(pool, 2);
    EXPECT_EQ(pool.worker(0).ownedCount(), 0u) << "the worker no longer drives it";
    EXPECT_EQ(R.reg.collectRetired(pool.minAckedOwnershipGen()), 1u) << "freed only after the worker acked";
}

TEST(ReplicaLifetimeRegression, SchemaFailureWithAnOverlongPathNeverThrowsOnTheAdminThread) {
    // M3: failureRing_ max message = 4096/8; a longer log dir made write() throw.
    TmpDir d;
    fs::path deep = d.path / std::string(200, 'a') / std::string(200, 'b') / std::string(200, 'c');
    fs::create_directories(deep);
    Log log(deep.string(), 64LL * 1024 * 1024); log.recover();
    ASSERT_GT(log.dir().size(), 512u);
    NullTransport t;
    PartitionReplica schema(cfg(2, 1, -4, 0), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    schema.becomeFollower(1, 1);
    const auto b = batchAt(0);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, -4, std::span<const uint8_t>(b)};
    schema.applyReplicated(resp);
    SchemaRegistry sreg;
    AdminWorkerAgent agent(sreg, &schema, 2, {1, 2}, AdminDeps{});
    log.setSyncFaultForTest(EIO);
    EXPECT_NO_THROW(agent.doWork(25 * kMs));
    int msgs = 0; std::string dir;
    agent.failureRing().drain([&](int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                                  aeron::util::index_t len) {
        ++msgs;
        dir.assign(reinterpret_cast<const char*>(buf.buffer() + off) + 4, static_cast<std::size_t>(len) - 4);
    }, 4);
    EXPECT_EQ(msgs, 1) << "signalled, never thrown";
    // Integration wave 2: F3 round 6 grew the admin→control ring to carry
    // SchemaUpdate descriptor blobs (max message 128 KiB > PATH_MAX), so a real
    // path now arrives whole; the M3 tail-truncation stays as defence in depth.
    const std::size_t maxDir = static_cast<std::size_t>(agent.failureRing().maxMsgLength()) - 4u;
    if (log.dir().size() > maxDir) {
        EXPECT_EQ(dir.rfind("...", 0), 0u);
        EXPECT_EQ(log.dir().substr(log.dir().size() - (dir.size() - 3)), dir.substr(3)) << "the path's tail is kept";
    } else {
        EXPECT_EQ(dir, log.dir()) << "a path that fits the ring is signalled whole";
    }
}

TEST(ReplicaLifetimeRegression, IncarnationWrapSkipsZeroStaleAndLiveValues) {
    // M4: 32-bit incarnations wrap; 0 (unregistered) / kStaleIncarnation and any
    // value still held by a live replica are skipped.
    Reg R;
    auto* a = R.add(1, 0);
    ASSERT_EQ(a->incarnation(), 1u);
    R.reg.setNextIncarnationForTest(0xFFFFFFFEu);
    auto* b = R.add(1, 1);
    EXPECT_EQ(b->incarnation(), 0xFFFFFFFEu);
    auto* c = R.add(1, 2);
    EXPECT_NE(c->incarnation(), 0u);
    EXPECT_NE(c->incarnation(), ReplicaRegistry::kStaleIncarnation);
    EXPECT_NE(c->incarnation(), a->incarnation()) << "never live twice";
    EXPECT_EQ(c->incarnation(), 2u);
}

TEST(ReplicaLifetimeRegression, BackPressuredProduceIsRetriableNotInternal) {
    // M5: a produce the worker ring cannot take answers the RETRIABLE NotLeader.
    MarkerRig m;
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (m.pool.worker(0).tryEnqueueOp(filler)) {}
    const int64_t sid = m.gw.injectTestSession();
    pc::Produce msg;
    const auto batch = batchAt(0);
    msg.sessionId = sid; msg.correlationId = 5; msg.topicId = 0; msg.partition = 0; msg.partitionKnown = 1;
    msg.acks = 0; msg.producerId = -1; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    m.gw.applyProduceForTest(frame.data(), frame.size());
    ASSERT_EQ(m.cap.produceErrors.size(), 1u);
    EXPECT_EQ(m.cap.produceErrors[0], static_cast<int16_t>(pc::ErrorCode::NotLeader));
}

TEST(ReplicaLifetimeRegression, BackPressuredSubscriptionLifecycleOpsAreQueuedInOrder) {
    // M5: Subscribe / Unsubscribe the ring refused are queued per worker and
    // retried in order — never dropped.
    MarkerRig m;
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (m.pool.worker(0).tryEnqueueOp(filler)) {}
    m.gw.forwardSubscribeForTest(41, /*session*/0, 0, 0, 0, 0, false, false);
    m.gw.forwardUnsubscribeForTest(41, 0, 0);
    EXPECT_EQ(m.gw.pendingSubOpsForTest(), 2u);
    drainIdle(m.pool.worker(0));                            // fillers drain (bounded)
    EXPECT_EQ(m.gw.pumpRetryQueuesForTest(), 2);
    drainIdle(m.pool.worker(0));
    EXPECT_TRUE(m.pool.worker(0).subscriptionsForTest().empty()) << "subscribe then unsubscribe, in order";
    EXPECT_EQ(m.gw.pendingSubOpsForTest(), 0u);
}

TEST(ReplicaLifetimeRegression, CleanerUsesItsOwnRingViewAndGatesTheFree) {
    // Found on sight: the LogCleanerAgent thread iterated the registry vector the
    // control thread mutates, and could hold a replica collectRetired then freed.
    // It now works off a ring-delivered view; a removed replica is freed only
    // after the cleaner acked the generation that dropped it.
    Reg R;
    NoopConsensus noop;
    NodeRuntimeAgent rt(noop, nullptr, nullptr, nullptr, &R.reg, nullptr);
    LogCleanerAgent cleaner(&R.reg);
    cleaner.attachViewChannel(rt.enableCleanerView());
    R.add(5, 0);
    R.add(5, 1);
    rt.doWork(1);                                          // publishes generation 2
    EXPECT_EQ(cleaner.viewSizeForTest(), 0u) << "nothing until the cleaner runs";
    cleaner.doWork(1);
    EXPECT_EQ(cleaner.viewSizeForTest(), 2u);
    EXPECT_EQ(cleaner.viewGenerationForTest(), R.reg.generation());
    R.reg.remove(5, 0);
    rt.doWork(2);                                          // ack of the OLD view only
    EXPECT_EQ(R.reg.retiredCountForTest(), 1u) << "the cleaner may still hold it: not freed";
    cleaner.doWork(2);                                     // swaps to the new view + acks
    EXPECT_EQ(cleaner.viewSizeForTest(), 1u);
    rt.doWork(3);
    EXPECT_EQ(rt.cleanerAckedGenForTest(), R.reg.generation());
    EXPECT_EQ(R.reg.retiredCountForTest(), 0u) << "freed once the cleaner acked";
}

// ============================================================================
// Fix round 5 — a transaction completes only after EVERY marker has landed;
// marker replies are FIFO with produce results; the ISR gate is worker-owned.
// ============================================================================
namespace {
// A coordinator over MarkerRig's gateway (per-worker mode: markers are async).
struct TxnRig {
    MarkerRig m;
    TransactionCoordinationService tcs{nullptr, &m.topics};
    std::vector<std::vector<uint8_t>> controlFrames;     // deferred responses (gateway egress)
    std::string txnId = "tx-1";
    int64_t pid = -1; int16_t epoch = 0; int32_t seq = 0; int64_t corr = 100;
    std::array<uint8_t, 4096> out{};
    TxnRig() {
        tcs.setGateway(&m.gw);
        m.gw.setTxnService(&tcs);
        m.gw.setCommittedMetadataReady(true);
        m.gw.setOnControlFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            controlFrames.emplace_back(b, b + n);
        });
    }
    std::span<const uint8_t> tid() const {
        return {reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size()};
    }
    int16_t initPid() {
        pc::InitProducerId q; q.sessionId = 1; q.correlationId = ++corr; q.transactionTimeoutMs = 60'000;
        q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = tcs.handleInitProducerId(f.data(), f.size(), 1, out.data(), out.size());
        auto r = pc::InitProducerIdResponse::decode(out.data(), n);
        if (r && r->errorCode == 0) { pid = r->producerId; epoch = r->producerEpoch; }
        return r ? r->errorCode : -1;
    }
    int16_t addPartition() {
        pc::AddPartitionsToTxn q; q.sessionId = 1; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.transactionalId = tid();
        const pc::TxnTopicPartition tp{0, 0};
        std::vector<uint8_t> f(pc::AddPartitionsToTxn::encodedSizeFor(tid(), 1));
        q.encode(f.data(), std::span<const pc::TxnTopicPartition>(&tp, 1));
        const auto n = tcs.handleAddPartitions(f.data(), f.size(), 1, out.data(), out.size());
        auto r = pc::AddPartitionsToTxnResponse::decode(out.data(), n);
        return (r && r->results.size() == 1) ? r->results[0].errorCode : int16_t{-1};
    }
    // Returns the EndTxn error code if answered synchronously, or 999 if deferred.
    int16_t endTxn(bool commit) {
        pc::EndTxn q; q.sessionId = 1; q.correlationId = ++corr; q.producerId = pid;
        q.producerEpoch = epoch; q.commit = commit ? 1 : 0; q.transactionalId = tid();
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        const auto n = tcs.handleEndTxn(f.data(), f.size(), 1, out.data(), out.size());
        if (n == 0) return 999;
        auto r = pc::EndTxnResponse::decode(out.data(), n);
        return r ? r->errorCode : -1;
    }
    // A transactional data batch through the REAL gateway produce path.
    void produceTxn(const std::string& value) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq);
        b.append("k", value, 1000);
        const auto batch = b.build();
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = ++corr; msg.topicId = 0; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = 0; msg.producerId = pid; msg.producerEpoch = epoch;
        msg.baseSequence = seq++; msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        m.gw.applyProduceForTest(frame.data(), frame.size());
    }
    int64_t sid = 0;
};
constexpr int16_t kConcurrent = static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions);
} // namespace

TEST(ReplicaLifetimeRegression, MarkerAppendedBeforeTxnCompletesRegression) {
    // The EndTxn response and the producer's next transaction are held until
    // every participant's marker has LANDED (Kafka CONCURRENT_TRANSACTIONS).
    TxnRig t;
    t.sid = t.m.gw.injectTestSession();
    ASSERT_EQ(t.initPid(), 0);
    ASSERT_EQ(t.addPartition(), 0);
    t.produceTxn("a");
    EXPECT_EQ(t.endTxn(/*commit=*/true), 999) << "EndTxn response deferred while the marker is in flight";
    EXPECT_EQ(t.tcs.awaitingMarkerTxnsForTest(), 1u);
    EXPECT_EQ(t.addPartition(), kConcurrent) << "next txn refused (retriable) until the marker lands";
    EXPECT_EQ(t.endTxn(true), kConcurrent);
    EXPECT_EQ(t.initPid(), kConcurrent);
    EXPECT_TRUE(t.controlFrames.empty());
    for (int i = 0; i < 64 && t.tcs.awaitingMarkerTxnsForTest() > 0; ++i) t.m.runWorker(1 + i);   // bounded
    ASSERT_EQ(t.tcs.awaitingMarkerTxnsForTest(), 0u) << "completed when the last marker landed";
    ASSERT_EQ(t.controlFrames.size(), 1u) << "the deferred EndTxn response is sent now";
    auto r = pc::EndTxnResponse::decode(t.controlFrames[0].data(), t.controlFrames[0].size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 0);
    EXPECT_EQ(t.addPartition(), 0) << "the next transaction may start now";
}

TEST(ReplicaLifetimeRegression, FullReturnRingDoesNotReorderMarkerVsProduceResultRegression) {
    // The marker's ControlRecordAppended travels the SAME FIFO as ProduceResults:
    // a later produce's result never overtakes it — not within a pass, and not
    // through a ring slot that frees up while earlier returns sit in overflow.
    Reg R;
    PartitionReplica* r = R.add(5, 0);
    Worker w(0, 1 << 16, /*return ring*/ 1 << 12, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{r});
    w.setWorkerIngress(nullptr, true);
    for (int i = 0; i < 40; ++i) ASSERT_TRUE(w.tryEnqueueOp(produceOp(r, 1 + i)));   // overflows the return ring
    WorkerOp mk;
    mk.type = WorkerOp::Type::AppendControlRecord; mk.topicId = 5; mk.partition = 0;
    mk.replica = r; mk.correlationId = 999; mk.recordBatchBytes = batchAt(0);
    ASSERT_TRUE(w.tryEnqueueOp(stamp(std::move(mk))));
    ASSERT_TRUE(w.tryEnqueueOp(produceOp(r, 1000)));     // the NEXT transaction's batch
    std::vector<std::pair<int, int64_t>> order;          // (type, correlationId)
    for (int i = 0; i < 256 && order.size() < 42; ++i) {
        w.doWork(1 + i);
        if (i % 3 == 2)   // let returns pile up in overflow between drains
            w.drainReturn([&](WorkerReturn x) { order.emplace_back(static_cast<int>(x.type), x.correlationId); });
    }
    for (int i = 0; i < 64 && order.size() < 42; ++i) {
        w.doWork(500 + i);
        w.drainReturn([&](WorkerReturn x) { order.emplace_back(static_cast<int>(x.type), x.correlationId); });
    }
    ASSERT_EQ(order.size(), 42u);
    std::size_t markerAt = order.size(), nextAt = order.size();
    for (std::size_t i = 0; i < order.size(); ++i) {
        if (order[i].first == static_cast<int>(WorkerReturn::Type::ControlRecordAppended)) markerAt = i;
        if (order[i].second == 1000) nextAt = i;
    }
    ASSERT_LT(markerAt, order.size());
    EXPECT_LT(markerAt, nextAt) << "the marker's reply precedes the later produce's result";
    EXPECT_EQ(markerAt, 40u) << "exactly in op order";
}

TEST(ReplicaLifetimeRegression, AbortedTxnRangeNeverCoversNextTxnRegression) {
    // End to end through the gateway: txn1 aborts, the producer then runs txn2
    // and aborts it too.  Each txn's data must be covered by ITS abort range,
    // so a read_committed consumer never sees aborted records — the next txn's
    // batch is never adopted into (or released by) the previous txn's marker.
    TxnRig t;
    t.sid = t.m.gw.injectTestSession();
    ASSERT_EQ(t.initPid(), 0);
    ASSERT_EQ(t.addPartition(), 0);
    t.produceTxn("txn1");
    ASSERT_EQ(t.endTxn(/*commit=*/false), 999);
    // The producer retries its next transaction (ConcurrentTransactions is
    // retriable) — it can only begin once txn1's marker landed.
    int tries = 0;
    for (; tries < 64 && t.addPartition() == kConcurrent; ++tries) t.m.runWorker(1 + tries);
    ASSERT_LT(tries, 64);
    t.produceTxn("txn2");
    for (int i = 0; i < 8; ++i) t.m.runWorker(100 + i);
    ASSERT_EQ(t.endTxn(false), 999);
    for (int i = 0; i < 64 && t.tcs.awaitingMarkerTxnsForTest() > 0; ++i) t.m.runWorker(200 + i);
    ASSERT_EQ(t.tcs.awaitingMarkerTxnsForTest(), 0u);
    // Log: [0]=txn1 data, [1]=ABORT marker, [2]=txn2 data, [3]=ABORT marker.
    // F15 S4: observed on the OWNING worker (Option B).
    const auto ranges = t.m.pool.worker(0).rcAbortedRangesForTest(0, 0);
    ASSERT_EQ(ranges.size(), 2u);
    auto covered = [&](int64_t off) {
        for (const auto& rg : ranges) if (rg.contains(off)) return true;
        return false;
    };
    EXPECT_TRUE(covered(0)) << "txn1 data aborted";
    EXPECT_TRUE(covered(2)) << "txn2 data aborted — a read_committed consumer never sees it";
    EXPECT_EQ(ranges[0].lastOffset, 1);
    EXPECT_EQ(ranges[1].firstOffset, 2);
    EXPECT_EQ(ranges[1].lastOffset, 3);
    EXPECT_EQ(t.m.pool.worker(0).rcOpenTxnsForTest(0, 0), 0u);
}

TEST(ReplicaLifetimeRegression, MinIsrViolationIsReturnedByTheOwningWorker) {
    // The gateway no longer reads replica->isr() on the control thread: an
    // acks=all produce under min-ISR is routed and the OWNING worker answers
    // MinIsrViolation (log untouched).
    MarkerRig m(/*minIsr=*/2);
    m.gw.setCommittedMetadataReady(true);
    m.gw.setMinIsr(2);                     // the admission policy the op carries
    m.gw.syncMinIsrForTest();
    const int64_t sid = m.gw.injectTestSession();
    pc::Produce msg;
    const auto batch = batchAt(0);
    msg.sessionId = sid; msg.correlationId = 7; msg.topicId = 0; msg.partition = 0; msg.partitionKnown = 1;
    msg.acks = 1; msg.producerId = -1; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    m.gw.applyProduceForTest(frame.data(), frame.size());
    EXPECT_TRUE(m.cap.produceErrors.empty()) << "no control-thread ISR read: the op went to the worker";
    for (int i = 0; i < 8 && m.cap.produceErrors.empty(); ++i) m.runWorker(1 + i);
    ASSERT_EQ(m.cap.produceErrors.size(), 1u);
    EXPECT_EQ(m.cap.produceErrors[0], static_cast<int16_t>(pc::ErrorCode::MinIsrViolation));
    EXPECT_EQ(m.r->log()->logEndOffset(), 0) << "nothing appended";
}

TEST(ReplicaLifetimeRegression, PromotionRacingProduceDoesNotCrash) {
    // The crash: the control thread read replica->isr() while becomeLeader (on
    // the worker thread) replaced the ISR manager.  With the gate on the worker,
    // promotions and acks=all produces race freely (worker on its own thread,
    // bounded iterations, no sleeps).
    MarkerRig m(/*minIsr=*/1);
    m.gw.setCommittedMetadataReady(true);
    const int64_t sid = m.gw.injectTestSession();
    rune::consensus::MonotonicNanoClock clk;
    m.pool.startRunners(NodeConfig::WorkerIdleStrategy::BusySpin, clk);
    int responses = 0;
    const auto batch = batchAt(0);
    for (int i = 0; i < 2'000; ++i) {
        WorkerOp role;
        role.type = (i % 2) ? WorkerOp::Type::BecomeLeader : WorkerOp::Type::BecomeFollower;
        role.topicId = 0; role.partition = 0; role.roleEpoch = 1 + i; role.roleLeaderId = 1;
        role.isr = {1};
        (void)m.pool.tryEnqueueOp(0, 0, std::move(role));
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = i; msg.topicId = 0; msg.partition = 0; msg.partitionKnown = 1;
        msg.acks = 1; msg.producerId = -1; msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        m.gw.applyProduceForTest(frame.data(), frame.size());
        m.pool.drainAllReturns([&](WorkerReturn r) { m.gw.handleWorkerReturnForTest(r); });
    }
    m.pool.stopAndJoinRunners();
    m.pool.drainAllReturns([&](WorkerReturn r) { m.gw.handleWorkerReturnForTest(r); });
    responses = static_cast<int>(m.cap.produceErrors.size());
    EXPECT_GT(responses, 0) << "produces were answered while the role flipped (no crash)";
}

TEST(ReplicaLifetimeRegression, IncarnationWrapSkipsValuesTheOwnerMapStillHolds) {
    // Minor: after a wrap, an incarnation still held by WorkerPool's F7 owner
    // map (removed, awaiting acks) is never reissued.
    Reg R;
    WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    R.reg.setIncarnationInUseHook(&WorkerPool::tracksIncarnationThunk, &pool);
    auto* a = R.add(1, 0);                                   // incarnation 1 (stays live)
    auto* b = R.add(1, 1);                                   // incarnation 2
    pool.reconcile(R.reg);
    ackAll(pool);
    ASSERT_EQ(a->incarnation(), 1u);
    ASSERT_EQ(b->incarnation(), 2u);
    R.reg.remove(1, 1);
    pool.reconcile(R.reg);                                   // inc 2: removed, NOT yet acked
    ASSERT_TRUE(pool.tracksIncarnation(2)) << "pending erase in the F7 owner map";
    ASSERT_EQ(R.reg.collectRetired(R.reg.generation()), 1u) << "registry side freed (forced)";
    R.reg.setNextIncarnationForTest(0xFFFFFFFEu);
    R.add(1, 2);                                             // 0xFFFFFFFE (pre-wrap)
    auto* c = R.add(1, 3);                                   // wraps: FFFFFFFF, 0, 1 (live), 2 (held) skipped
    EXPECT_EQ(c->incarnation(), 3u) << "an incarnation still held by the owner map is never reissued";
}

// ============================================================================
// F18: the per-worker ingress path enforces the same validated produce-batch
// limit as the gateway — an over-limit batch is refused LOUDLY (MessageTooLarge)
// and never appended (it could be appended but never replicated in one frame).
// ============================================================================
TEST(ProduceBatchLimitRegression, WorkerIngressRejectsOverLimitBatchMessageTooLarge) {
    StrictShard s;
    const auto b = batchAt(0, 1, 512);
    s.w.setMaxBatchBytes(b.size() - 1);
    ASSERT_EQ(s.produce(1, b, 0, 1), 1);
    Acks a;
    a.drain(s.w);
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(s.log->logEndOffset(), 0) << "an over-limit batch must never be appended";

    s.w.setMaxBatchBytes(b.size());                 // exactly at the limit: accepted
    ASSERT_EQ(s.produce(2, b, 0, 2), 1);
    EXPECT_EQ(s.log->logEndOffset(), 1);
}

// ============================================================================
// F18: the FOLLOWER apply path (FetchResponse / reconcile response → worker op
// ring) must never terminate the process on records larger than one worker
// ingress frame (tryClaim THROWS on an oversized claim), nor livelock the
// subscription by ABORTing (redelivered forever).  It is refused as a counted
// replication error and the fragment is consumed; normal frames still route.
// ============================================================================
namespace {
std::vector<uint8_t> encodeBigFetchResp(int32_t topic, int32_t part, std::size_t valueBytes,
                                        uint16_t tid = rune::replication::FetchResponseMsg::TEMPLATE_ID) {
    const auto b = batchAt(0, 1, valueBytes);
    std::vector<uint8_t> f(rune::replication::FetchResponseMsg::encodedSize(b.size()));
    rune::replication::FetchResponseMsg::encode(f.data(), 0, 1, part, 0, 0, rune::replication::kErrNone,
                                                std::span<const uint8_t>(b), topic);
    rune::replication::stampTemplateId(f.data(), 0, tid);
    return f;
}
} // namespace

TEST(ProduceBatchLimitRegression, FollowerApplyOfOversizedRecordsIsRefusedNotFatal) {
    TmpDir ad;
    EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Threaded);
    aeron::Context ctx; ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rune::replication::AeronPartitionTransport tr(client, "aeron:ipc", 7303, /*self*/1, 1 << 20);
    // A FOLLOWER replica (node 1 follows leader 2) owned by a worker with 8 KiB frames.
    Reg R;
    fs::create_directories(R.d.path / "follower");
    Log flog((R.d.path / "follower").string(), 64LL * 1024 * 1024);
    flog.recover();
    auto rep = std::make_unique<PartitionReplica>(cfg(1, 1, 5, 0), Role::Follower, 1,
        std::vector<rune::replication::NodeId>{1, 2}, std::vector<rune::replication::NodeId>{1, 2},
        &flog, &R.t);
    rep->becomeFollower(1, 2);
    PartitionReplica* const follower = rep.get();
    R.reg.add(5, 0, std::move(rep));
    WorkerPool pool(1, rune::metadata::ShardRouter(1), /*ingress*/ 1 << 16, 1 << 16, 16, 16);   // 8 KiB frames
    pool.assignFromRegistry(R.reg);
    pool.setRegistry(&R.reg);
    pool.setRunnersStartedForTest(true);
    tr.setReplicaRegistry(&R.reg);
    tr.setWorkerPool(&pool);
    pool.reconcile(R.reg);
    ackAll(pool);

    const auto big = encodeBigFetchResp(5, 0, 16 * 1024);
    ASSERT_FALSE(pool.opTailFits(5, 0, big.size()));
    bool consumed = false;
    EXPECT_NO_THROW(consumed = tr.deliverFrameForTest(big.data(), big.size(), 1));
    EXPECT_TRUE(consumed) << "never ABORT (redelivered forever → subscription livelock)";
    EXPECT_EQ(tr.oversizedApplyRefused(), 1u);
    // F18 fix round 1 (I-1): the follower is TOLD (on its owning worker) and wedges
    // at its LEO — it stops fetching and leaves the ISR instead of a silent stall.
    pool.worker(0).doWork(2);
    EXPECT_EQ(follower->wedgedAtOffset(), 0) << "the follower must wedge, not re-fetch forever";
    EXPECT_EQ(follower->wedgedRefusals(), 1u);

    const auto bigRec = encodeBigFetchResp(5, 0, 16 * 1024, rune::replication::kReconcileFetchRespTemplateId);
    EXPECT_NO_THROW(consumed = tr.deliverFrameForTest(bigRec.data(), bigRec.size(), 3));
    EXPECT_TRUE(consumed);
    EXPECT_EQ(tr.oversizedApplyRefused(), 2u);

    // A normal-size response still routes to the worker (no false refusals).
    const auto small = encodeFetchResp(5, 0);
    EXPECT_TRUE(tr.deliverFrameForTest(small.data(), small.size(), 4));
    EXPECT_EQ(tr.oversizedApplyRefused(), 2u);
    pool.worker(0).doWork(5);
    EXPECT_EQ(pool.worker(0).oversizedOpsRefusedForTest(), 0u);
}

TEST(ProduceBatchLimitRegression, WorkerEnqueueOfOversizedOpRefusesInsteadOfThrowing) {
    Worker w(0, /*ingress*/ 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{});
    WorkerOp op;
    op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 5; op.partition = 0;
    op.replicatedPayload.records.assign(16 * 1024, 0xAB);
    ASSERT_FALSE(w.opTailFits(op.replicatedPayload.records.size()));
    EnqueueResult r = EnqueueResult::Enqueued;
    EXPECT_NO_THROW(r = w.enqueueOp(std::move(op)));
    EXPECT_EQ(r, EnqueueResult::TooLarge) << "distinguishable from ring-full (never retried)";
    EXPECT_EQ(w.oversizedOpsRefusedForTest(), 1u);

    // A full ring is reported as Full, not TooLarge.
    WorkerOp filler; filler.type = WorkerOp::Type::Unsubscribe;
    while (w.tryEnqueueOp(filler)) {}
    EXPECT_EQ(w.enqueueOp(filler), EnqueueResult::Full);
}

// F18: the GATEWAY produce intake answers MessageTooLarge for a batch above the
// intake limit (BrokerNode sets it to NodeConfig::produceIntakeLimit(), at most
// one replication frame) — before any append; a batch at the limit is accepted.
TEST(ProduceBatchLimitRegression, GatewayIntakeRejectsBatchAboveLimitBeforeAppend) {
    MarkerRig m;
    const int64_t sid = m.gw.injectTestSession();
    const auto batch = batchAt(0, 1, 2048);
    auto produce = [&](int64_t corr) {
        pc::Produce msg;
        msg.sessionId = sid; msg.correlationId = corr; msg.topicId = 0; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = 0; msg.producerId = -1;
        msg.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> frame(msg.encodedSize());
        msg.encode(frame.data());
        m.gw.applyProduceForTest(frame.data(), frame.size());
    };
    m.gw.setMaxProduceBatchBytes(batch.size() - 1);
    produce(1);
    ASSERT_EQ(m.cap.produceErrors.size(), 1u);
    EXPECT_EQ(m.cap.produceErrors[0], static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    m.runWorker(1);
    EXPECT_EQ(m.r->log()->logEndOffset(), 0) << "refused before append";

    m.gw.setMaxProduceBatchBytes(batch.size());
    produce(2);
    for (int i = 0; i < 64 && m.r->log()->logEndOffset() == 0; ++i) m.runWorker(10 + i);   // bounded
    EXPECT_EQ(m.r->log()->logEndOffset(), 1) << "a batch at the limit is accepted";
}

// F18 fix round 1 (m-1): a produce batch the gateway admits but the worker ring
// can never hold is answered the NON-retriable MessageTooLarge — not the
// retriable NotLeader (which made the client re-send it forever).
TEST(ProduceBatchLimitRegression, GatewayAnswersMessageTooLargeWhenWorkerFrameIsSmaller) {
    MarkerRig m;                                           // worker ingress 64 KiB → 8 KiB frames
    const int64_t sid = m.gw.injectTestSession();
    const auto batch = batchAt(0, 1, 16 * 1024);           // > worker frame
    // Integration wave 2 (F15 S3 ⇄ F18): setWorkerPool narrows the gateway's
    // limit to the workers' (ONE limit), so the batch is refused by the limit
    // guard itself ...
    ASSERT_LT(m.gw.maxProduceBatchBytes(), batch.size());
    // ... and F18's enqueue-time TooLarge answer stays as defence in depth:
    // lift the gateway limit to its 8 MiB ceiling to reach it.
    m.gw.setMaxProduceBatchBytes(8u * 1024u * 1024u);
    ASSERT_GE(m.gw.maxProduceBatchBytes(), batch.size());
    pc::Produce msg;
    msg.sessionId = sid; msg.correlationId = 1; msg.topicId = 0; msg.partition = 0;
    msg.partitionKnown = 1; msg.acks = 0; msg.producerId = -1;
    msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    EXPECT_NO_THROW(m.gw.applyProduceForTest(frame.data(), frame.size()));
    ASSERT_EQ(m.cap.produceErrors.size(), 1u);
    EXPECT_EQ(m.cap.produceErrors[0], static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(m.r->log()->logEndOffset(), 0);
}

// ============================================================================
// F18 fix round 2 (B-1): the follower-side check must look at the BATCH at the
// follower's LEO, not the whole response.  A multi-batch response (fetch
// prefix / push / overlapping duplicate) larger than this node's worker frame,
// where every batch fits on its own, must NOT wedge a healthy follower — it is
// dropped (the follower's capped fetch then gets it one frame at a time).  An
// oversized batch that is NOT at the follower's LEO must not wedge either.
// ============================================================================
namespace {
std::vector<uint8_t> encodeFetchRespOf(int32_t topic, int32_t part, const std::vector<uint8_t>& recs) {
    std::vector<uint8_t> f(rune::replication::FetchResponseMsg::encodedSize(recs.size()));
    rune::replication::FetchResponseMsg::encode(f.data(), 0, 1, part, 0, 0, rune::replication::kErrNone,
                                                std::span<const uint8_t>(recs), topic);
    return f;
}
struct FollowerRig {
    TmpDir ad;
    EmbeddedDriverConfig dc;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::unique_ptr<rune::replication::AeronPartitionTransport> tr;
    Reg R;
    std::unique_ptr<Log> flog;
    PartitionReplica* follower = nullptr;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), /*ingress*/ 1 << 16, 1 << 16, 16, 16};   // 8 KiB frames
    explicit FollowerRig(int32_t stream) {
        dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
        drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Threaded);
        aeron::Context ctx; ctx.aeronDir(ad.path.string());
        client = aeron::Aeron::connect(ctx);
        tr = std::make_unique<rune::replication::AeronPartitionTransport>(client, "aeron:ipc", stream, 1, 1 << 20);
        fs::create_directories(R.d.path / "follower");
        flog = std::make_unique<Log>((R.d.path / "follower").string(), 64LL * 1024 * 1024);
        flog->recover();
        auto rep = std::make_unique<PartitionReplica>(cfg(1, 1, 5, 0), Role::Follower, 1,
            std::vector<rune::replication::NodeId>{1, 2}, std::vector<rune::replication::NodeId>{1, 2},
            flog.get(), &R.t);
        rep->becomeFollower(1, 2);
        follower = rep.get();
        R.reg.add(5, 0, std::move(rep));
        pool.assignFromRegistry(R.reg);
        pool.setRegistry(&R.reg);
        pool.setRunnersStartedForTest(true);
        tr->setReplicaRegistry(&R.reg);
        tr->setWorkerPool(&pool);
        pool.reconcile(R.reg);
        ackAll(pool);
    }
};
} // namespace

TEST(ProduceBatchLimitRegression, MultiBatchResponseAboveWorkerFrameDoesNotWedgeHealthyFollower) {
    FollowerRig rig(7304);
    // Two batches [0] and [1], ~5 KiB each: each fits the 8 KiB frame, the pair does not.
    std::vector<uint8_t> recs = batchAt(0, 1, 5 * 1024);
    const auto b1 = batchAt(1, 1, 5 * 1024);
    recs.insert(recs.end(), b1.begin(), b1.end());
    ASSERT_FALSE(rig.pool.opTailFits(5, 0, recs.size()));
    ASSERT_TRUE(rig.pool.opTailFits(5, 0, b1.size()));
    const auto f = encodeFetchRespOf(5, 0, recs);
    EXPECT_TRUE(rig.tr->deliverFrameForTest(f.data(), f.size(), 1));
    rig.pool.worker(0).doWork(2);
    EXPECT_EQ(rig.follower->wedgedAtOffset(), -1) << "every batch fits: a healthy follower must never wedge";
    EXPECT_EQ(rig.follower->wedgedRefusals(), 0u);
    // ...and a single fitting batch is then applied normally.
    const auto f0 = encodeFetchRespOf(5, 0, batchAt(0, 1, 5 * 1024));
    EXPECT_TRUE(rig.tr->deliverFrameForTest(f0.data(), f0.size(), 3));
    rig.pool.worker(0).doWork(4);
    EXPECT_EQ(rig.follower->logEndOffset(), 1);
    EXPECT_EQ(rig.follower->wedgedAtOffset(), -1);
}

TEST(ProduceBatchLimitRegression, OversizedBatchNotAtFollowerLeoDoesNotWedge) {
    FollowerRig rig(7305);
    // An oversized batch at base 7 (e.g. a push from further ahead) while LEO = 0.
    const auto f = encodeFetchRespOf(5, 0, batchAt(7, 1, 16 * 1024));
    EXPECT_TRUE(rig.tr->deliverFrameForTest(f.data(), f.size(), 1));
    rig.pool.worker(0).doWork(2);
    EXPECT_EQ(rig.follower->wedgedAtOffset(), -1) << "only the batch AT the follower's LEO can wedge it";
}

// (ProduceBatchLimitRegression.ReconcilePullOfSmallBatchesAboveOwnFrameCompletes
//  was retired with the DR-BUG-6 reverse reconcile: a leader never pulls records
//  it did not hold when elected.  The same "multi-batch answer above the
//  receiver's own frame" property on the fetch / push path is pinned by
//  LargeBatchReplicationRegression.MultiBatchPushAboveFollowerFrameNeverWedges*.)

// F15 S5 fix round 3 follow-up (re-review 3 M-9): S5 e69a215 submitted one
// durable fsync PER PASS per log; pipelined Strict produces on one log filled
// the io in-flight depth and the back-pressure branch answered healthy
// produces Internal.  One fsync in flight per log (M-6) can never exhaust it.
TEST(ShardNonBlockingRegression, PipelinedStrictOnOneLogNeverExhaustsIoDepth) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    leader.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, /*ioSubmitDepth=*/2, /*ioComplDepth=*/2);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setWorkerIngress(nullptr, true);
    w.setShardMode(true);
    w.setReapPausedForTest(true);                             // fsyncs complete, but are not reaped yet
    Acks a;
    for (int i = 0; i < 8; ++i) {                             // one Strict produce per pass
        const auto f = produceFrame(i + 1, batchAt(0), 0, 1, 0);
        ASSERT_EQ(w.handleIngressFrameForTest(f.data(), f.size(), i + 1), 1);
        w.doWork(i + 1);
        a.drain(w);
    }
    EXPECT_EQ(a.n, 0) << "a pipelined Strict produce was answered (Internal: io depth exhausted) before any fsync";
    w.setReapPausedForTest(false);
    for (int i = 0; i < kCap && a.n < 8; ++i) { w.doWork(100 + i); a.drain(w); std::this_thread::yield(); }
    ASSERT_EQ(a.n, 8);
    for (const auto& r : a.rets) EXPECT_EQ(r.errorCode, 0) << "corr " << r.correlationId;
}

// N-7 (re-review 3): a log re-owned while its fsync is in flight — that
// fsync's completion unpins its segment (no leaked pin) and re-serves a
// background request made meanwhile.
TEST(ShardNonBlockingRegression, ReownedLogCompletionUnpinsAndReservesBackgroundRequest) {
    StrictShard s;
    s.w.setReapPausedForTest(true);
    const auto b1 = s.w.submitBgFlushForTest(s.log.get());   // background fsync in flight (segment pinned)
    ASSERT_NE(b1, kNoToken);
    rune::storage::LogSegment* seg = s.log->activeSegmentForFlush();
    ASSERT_EQ(seg->flushPins(), 1u);
    s.w.setOwnedDirect(Worker::OwnedSet{});                  // purged ...
    s.w.setOwnedDirect(Worker::OwnedSet{s.leader.get()});   // ... and re-owned: a fresh slot (gate seeded)
    ASSERT_EQ(s.produce(1, batchAt(0), 0, 1), 1);           // newer data ...
    const uint64_t bgBefore = s.w.bgFlushSubmitsForTest();
    (void)s.w.submitBgFlushForTest(s.log.get());            // ... and a background request while B1 is in flight
    ASSERT_EQ(s.w.bgFlushSubmitsForTest(), bgBefore) << "precondition: coalesced behind B1 (bgAgain)";
    s.w.deliverCompletionForTest(FlushCompletion{b1, 0, s.log->flushBoundary()});
    EXPECT_EQ(seg->flushPins(), s.w.bgFlushInFlightForTest()) << "B1's segment pin leaked on the re-owned log";
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), bgBefore + 1) << "the background request made meanwhile was not re-served";
}

// LAT-1 (e)+(k): the 10 ms owned-log sync and the 20 ms follower flush became config, OFF by default in the
// broker (measured: the background fsync halved the median and cost 6x p99).  0 must mean NEVER.
TEST(NonShardNonBlockingRegression, FollowerFlushIntervalZeroNeverFlushesRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionConfig c = cfg(2);
    c.followerFlushIntervalNs = 0;
    PartitionReplica follower(c, Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    w.setWorkerIngress(nullptr, true);
    w.setBackgroundSyncIntervalNs(0);
    applyOneBatch(follower, 0);
    ASSERT_GT(log.logEndOffset(), 0);
    for (int64_t now = 25 * kMs; now < 2000 * kMs; now += 25 * kMs) w.doWork(now);
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 0u) << "follower flush interval 0 must never flush";
    EXPECT_EQ(log.syncCallsForTest(), 0u);

    // Positive control: the same scenario with the 20 ms interval DOES flush (the test is not vacuous).
    TmpDir d2;
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    PartitionReplica on(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log2, &t);
    on.becomeFollower(1, 1);
    on.setExternalFollowerFlush(true);
    Worker w2(0, 1 << 16, 1 << 16, 64, 64);
    w2.setOwnedDirect(Worker::OwnedSet{&on});
    w2.setWorkerIngress(nullptr, true);
    w2.setBackgroundSyncIntervalNs(0);
    applyOneBatch(on, 0);
    for (int64_t now = 25 * kMs; now < 2000 * kMs && w2.bgFlushSubmitsForTest() == 0; now += 25 * kMs) w2.doWork(now);
    EXPECT_GE(w2.bgFlushSubmitsForTest(), 1u) << "positive control: interval 20 ms flushes";
}

TEST(NonShardNonBlockingRegression, OwnedLogSyncIntervalZeroNeverSyncsRegression) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    leader.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setWorkerIngress(nullptr, true);
    w.setBackgroundSyncIntervalNs(0);
    w.doWork(1 * kMs);
    leader.append(std::span<const uint8_t>(std::vector<uint8_t>(16, 1)));
    (void)leader.takeFollowerFlushRequest();
    for (int64_t now = 12 * kMs; now < 2000 * kMs; now += 12 * kMs) w.doWork(now);
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 0u) << "owned-log sync interval 0 must never sync";
    EXPECT_EQ(log.syncCallsForTest(), 0u);

    // Positive control: the same scenario at the 10 ms library default DOES submit a background flush.
    TmpDir d2;
    Log log2(d2.path.string(), 64LL * 1024 * 1024); log2.recover();
    PartitionReplica l2(cfg(1), Role::Leader, 1, {1}, {1}, &log2, &t);
    l2.setExternalFollowerFlush(true);
    Worker w2(0, 1 << 16, 1 << 16, 64, 64);
    w2.setOwnedDirect(Worker::OwnedSet{&l2});
    w2.setWorkerIngress(nullptr, true);
    w2.doWork(1 * kMs);
    l2.append(std::span<const uint8_t>(std::vector<uint8_t>(16, 1)));
    (void)l2.takeFollowerFlushRequest();
    for (int64_t now = 12 * kMs; now < 2000 * kMs && w2.bgFlushSubmitsForTest() == 0; now += 12 * kMs) w2.doWork(now);
    EXPECT_GE(w2.bgFlushSubmitsForTest(), 1u) << "positive control: interval 10 ms syncs";
}
