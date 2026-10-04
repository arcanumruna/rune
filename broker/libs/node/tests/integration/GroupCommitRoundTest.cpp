// GroupCommitRoundTest.cpp — A10: GROUP COMMIT.  One durability round per worker at a time: every log
// with something to make durable (periodic owned-log sync, follower flush request, Strict waiters) is
// submitted in ONE batch, and the round completes as a unit — the next round starts only after every
// member of the previous one completed.  Semantics are unchanged: a round marks each log it covered
// durable up to the LEO sampled at round start; a Strict ack fires only after the round that covers it;
// an fsync error is fail-stop; no disk syscall ever runs on the worker loop.
//
// Every wait is a condition wait with a hard iteration cap (never a sleep).  Completion delivery is
// controlled (setReapPausedForTest + deliverCompletionForTest) where ordering is asserted.
#include <rune/node/Worker.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
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
constexpr int kLogs = 3;

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_gcr_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
std::vector<uint8_t> batchAt(int64_t base) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", "v", 1000);
    return b.build();
}
std::vector<uint8_t> produceFrame(int64_t corr, const std::vector<uint8_t>& batch, int32_t part) {
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = 1; msg.partition = part;
    msg.partitionKnown = 1; msg.acks = 0; msg.producerId = -1; msg.producerEpoch = 0;
    msg.baseSequence = -1; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}
PartitionConfig cfg(uint8_t tier, int32_t part) {
    PartitionConfig c; c.topicId = 1; c.partitionId = part; c.selfId = 1; c.minIsr = 1; c.durabilityTier = tier;
    c.fetchIntervalNs = 1'000 * kMs; c.isrCheckIntervalNs = 1'000 * kMs; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
struct Acks {
    int n = 0;
    std::vector<WorkerReturn> rets;
    void drain(Worker& w) {
        (void)w.drainReturn([&](WorkerReturn r) {
            if (r.type != WorkerReturn::Type::ProduceResult) return;
            ++n; rets.push_back(r);
        });
    }
};
// One worker owning kLogs single-ISR leaders (partition 1 is Strict when `strictP1`), periodic owned-log
// sync every 5 ms.
struct Logs {
    TmpDir d[kLogs];
    std::unique_ptr<Log> log[kLogs];
    NullTransport t;
    std::unique_ptr<PartitionReplica> rep[kLogs];
    Worker w{0, 1 << 16, 1 << 16, 64, 64};
    explicit Logs(bool shard, bool strictP1 = false) {
        Worker::OwnedSet owned;
        for (int i = 0; i < kLogs; ++i) {
            log[i] = std::make_unique<Log>(d[i].path.string(), 64LL * 1024 * 1024);
            log[i]->recover();
            const uint8_t tier = (strictP1 && i == 1) ? 2 : 1;
            rep[i] = std::make_unique<PartitionReplica>(cfg(tier, i), Role::Leader, 1,
                                                        std::vector<rune::replication::NodeId>{1},
                                                        std::vector<rune::replication::NodeId>{1}, log[i].get(), &t);
            rep[i]->setExternalFollowerFlush(true);
            owned.push_back(rep[i].get());
        }
        w.setOwnedDirect(owned);
        w.setWorkerIngress(nullptr, true);
        w.setShardMode(shard);
        w.setBackgroundSyncIntervalNs(5 * kMs);
    }
    int64_t next[kLogs] = {};
    // Page-cache append straight to the log (no flush request raised): only a round makes it durable.
    void dirty(int i, uint8_t) {
        const auto b = batchAt(next[i]);
        log[i]->appendNoSync(b.data(), b.size(), next[i]);
        ++next[i];
    }
    void dirtyAll(uint8_t v) { for (int i = 0; i < kLogs; ++i) dirty(i, v); }
    CompletionToken tok(int i) { return w.lastBgTokenForTest(log[i].get()); }
    int64_t synced(int i) { return w.lastSyncedLeoForTest(log[i].get()); }
    int produce(int64_t corr, int32_t part, int64_t now) {
        const auto f = produceFrame(corr, batchAt(0), part);
        return w.handleIngressFrameForTest(f.data(), f.size(), now);
    }
};

// A round covers EVERY dirty log, marks each durable up to the LEO sampled at round start, and the next
// round waits until the WHOLE previous round completed (one round in flight per worker).
void roundCoversEveryDirtyLog(bool shard) {
    Logs s(shard);
    s.w.setReapPausedForTest(true);
    s.dirtyAll(1);
    s.w.doWork(1 * kMs);                                   // arms the periodic interval
    s.w.doWork(7 * kMs);                                   // > 5 ms: periodic round R1
    CompletionToken r1[kLogs];
    int64_t leoAtStart[kLogs];
    for (int i = 0; i < kLogs; ++i) {
        r1[i] = s.tok(i);
        ASSERT_NE(r1[i], kNoToken) << "log " << i << ": the round must cover every dirty log";
        leoAtStart[i] = s.log[i]->logEndOffset();
    }
    s.dirtyAll(2);                                         // newer data after R1 started
    s.w.deliverCompletionForTest(FlushCompletion{r1[0], 0, 0});
    EXPECT_EQ(s.synced(0), leoAtStart[0]) << "covered up to the LEO sampled at round start, not beyond";
    s.w.doWork(13 * kMs);                                  // interval due again, R1 still has 2 members out
    EXPECT_EQ(s.tok(0), kNoToken) << "no new round while the previous round has members outstanding";
    s.w.deliverCompletionForTest(FlushCompletion{r1[1], 0, 0});
    s.w.deliverCompletionForTest(FlushCompletion{r1[2], 0, 0});
    for (int i = 0; i < kLogs; ++i) EXPECT_EQ(s.synced(i), leoAtStart[i]) << "log " << i;
    s.w.doWork(14 * kMs);                                  // R1 complete: R2 covers the newer data of all
    for (int i = 0; i < kLogs; ++i) {
        EXPECT_NE(s.tok(i), kNoToken) << "log " << i << ": R2 must cover every log dirty since R1";
        EXPECT_NE(s.tok(i), r1[i]);
    }
    EXPECT_EQ(s.w.syncRoundsStartedForTest(), 2u);
    for (int i = 0; i < kLogs; ++i) EXPECT_EQ(s.log[i]->syncCallsForTest(), 0u);
}

} // namespace

TEST(GroupCommitRegression, RoundCoversEveryDirtyLogShardRegression) { roundCoversEveryDirtyLog(true); }
TEST(GroupCommitRegression, RoundCoversEveryDirtyLogThreadPlacementRegression) { roundCoversEveryDirtyLog(false); }

// A Strict ack fires only after the round that COVERS it: a Strict append arriving while a round is in
// flight joins the next round (its fsync is not submitted beside the in-flight round), and its answer
// waits for that next round's completion.
TEST(GroupCommitRegression, StrictAckWaitsForTheCoveringRoundRegression) {
    Logs s(/*shard=*/true, /*strictP1=*/true);
    s.w.setReapPausedForTest(true);
    s.dirty(0, 1);
    s.w.doWork(1 * kMs);
    s.w.doWork(7 * kMs);                                   // periodic round R1 = {log 0}
    const CompletionToken r1 = s.tok(0);
    ASSERT_NE(r1, kNoToken);
    ASSERT_EQ(s.produce(1, /*part=*/1, 8 * kMs), 1);       // Strict append on log 1 while R1 is in flight
    s.w.doWork(8 * kMs);
    Acks a;
    a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "a Strict answer waits for its fsync";
    EXPECT_EQ(s.w.ioSubmitCountForTest(), 0u) << "the Strict fsync joins the NEXT round, not beside R1";
    ASSERT_EQ(s.w.durablePendingCountForTest(), 1u);
    EXPECT_EQ(s.w.durablePendingTokenForTest(0), kNoToken);
    s.w.deliverCompletionForTest(FlushCompletion{r1, 0, 0});
    s.w.doWork(9 * kMs);                                   // R1 complete: R2 = {log 1 (Strict)}
    EXPECT_EQ(s.w.ioSubmitCountForTest(), 1u);
    const CompletionToken r2 = s.w.durablePendingTokenForTest(0);
    ASSERT_NE(r2, kNoToken) << "the Strict waiter is covered by the next round";
    a.drain(s.w);
    EXPECT_EQ(a.n, 0) << "never before the covering round completes";
    s.w.deliverCompletionForTest(FlushCompletion{r2, 0, 0});
    s.w.doWork(10 * kMs);
    a.drain(s.w);
    ASSERT_EQ(a.n, 1);
    EXPECT_EQ(a.rets[0].errorCode, 0);
    EXPECT_EQ(s.w.durableAckTokenForTest(), r2);
}

// A failed round member is fail-stop exactly like a failed single fsync: its log is failed (never
// touched again), the runtime is signalled; the other members complete and the next round goes on
// without the failed log.
TEST(GroupCommitRegression, FailedRoundIsFailStopRegression) {
    Logs s(/*shard=*/true);
    s.w.setReapPausedForTest(true);
    s.dirtyAll(1);
    s.w.doWork(1 * kMs);
    s.w.doWork(7 * kMs);
    CompletionToken r1[kLogs];
    for (int i = 0; i < kLogs; ++i) { r1[i] = s.tok(i); ASSERT_NE(r1[i], kNoToken); }
    s.w.deliverCompletionForTest(FlushCompletion{r1[0], EIO, 0});
    EXPECT_TRUE(s.log[0]->storageFailed()) << "an fsync error is fail-stop (fsyncgate)";
    EXPECT_GE(s.w.storageFailureSignalsForTest(), 1u);
    EXPECT_EQ(s.w.bgFlushErrorsForTest(), 1u);
    s.w.deliverCompletionForTest(FlushCompletion{r1[1], 0, 0});
    s.w.deliverCompletionForTest(FlushCompletion{r1[2], 0, 0});
    s.dirty(1, 2); s.dirty(2, 2);
    s.w.doWork(13 * kMs);
    EXPECT_EQ(s.tok(0), kNoToken) << "a failed log is never fsynced again";
    EXPECT_NE(s.tok(1), kNoToken);
    EXPECT_NE(s.tok(2), kNoToken);
}

// The round goes through the io backend as ONE submission batch (one io_uring_enter for all members)
// and never calls a disk sync on the worker loop; real completions retire it as a unit.
TEST(GroupCommitRegression, RoundIsOneBackendBatchAndNeverSyncsOnTheLoopRegression) {
    Logs s(/*shard=*/true);
    s.dirtyAll(1);
    s.w.doWork(1 * kMs);
    const uint64_t batchesBefore = s.w.ioSubmitBatchesForTest();
    s.w.doWork(7 * kMs);
    EXPECT_EQ(s.w.ioSubmitBatchesForTest(), batchesBefore + 1) << "all members of a round in one submission";
    EXPECT_EQ(s.w.bgFlushSubmitsForTest(), static_cast<uint64_t>(kLogs));
    int64_t now = 7 * kMs;
    for (int i = 0; i < kCap && s.w.syncRoundInFlightForTest(); ++i) { s.w.doWork(now += 1000); std::this_thread::yield(); }
    ASSERT_FALSE(s.w.syncRoundInFlightForTest());
    for (int i = 0; i < kLogs; ++i) {
        EXPECT_EQ(s.synced(i), s.log[i]->logEndOffset());
        EXPECT_EQ(s.log[i]->syncCallsForTest(), 0u) << "never an inline sync on the worker loop";
    }
    EXPECT_EQ(s.w.bgFlushErrorsForTest(), 0u);
}
