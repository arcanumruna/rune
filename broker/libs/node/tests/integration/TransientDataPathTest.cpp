// TransientDataPathTest.cpp — Phase 2b E2: the worker data path of a TRANSIENT
// (InMemoryLog) partition — produce over shard ingress and the op ring, push
// reads served from the ring, read_committed, the full-ring rule, the evicted
// cursor, ownership moves.  Deterministic: one worker driven by hand, no Aeron.
#include "TransientRig.h"

#include <rune/node/RcState.h>
#include <rune/storage/ProducerStateManager.h>

#include <array>
#include <limits>
#include <random>

using namespace transient_rig;

namespace {
constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);   // 42 (Task 1)
} // namespace

TEST(TransientDataPathRegression, ShardIngressProduceAppendsToTheRingRegression) {
    Rig rig;
    const auto b = batchOf(3, 100);
    int64_t base = -1;
    ASSERT_EQ(rig.produce(b, /*acks=*/1, &base), kOk) << "a transient produce over shard ingress was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    ASSERT_EQ(rig.produce(b, 1, &base), kOk);
    EXPECT_EQ(base, 3);
    EXPECT_EQ(rig.mem.logEndOffset(), 6);
    EXPECT_EQ(rig.rep->highWatermark(), 6) << "single node: acks=all commits at once";
}

TEST(TransientDataPathRegression, ProducedBatchCarriesTheLeaderEpochRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(2, 50), /*acks=*/0), kOk);
    const auto v = rig.mem.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 0);
    EXPECT_EQ(r.partitionLeaderEpoch(), 1) << "followers build their epoch cache from the stamped epoch (KIP-320)";
    EXPECT_TRUE(r.ensureValid());
}

TEST(TransientDataPathRegression, GatewayRoutedProduceOpAppendsToTheRingRegression) {
    Rig rig;
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = kTopic; op.partition = 0;
    op.logPtr = nullptr;   // the gateway has no disk Log for a transient partition (legacy mode)
    op.sessionId = 1; op.correlationId = 77;
    op.lastOffsetDelta = 2; op.recordCount = 3; op.durabilityTier = 1;
    op.recordBatchBytes = batchOf(3, 100);
    ASSERT_TRUE(rig.pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == 77) { code = r.errorCode; base = r.baseOffset; }
        });
    }
    ASSERT_EQ(code, kOk) << "a gateway-routed (legacy mode) transient produce was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    EXPECT_EQ(rig.mem.logEndOffset(), 3);
}

TEST(TransientDataPathRegression, FullRingRefusesRetriablyAndNeverEvictsUncommittedRegression) {
    Rig rig(/*ringBytes=*/16 * 1024, /*maxBatches=*/1000, /*withFollower=*/true);   // ISR {1,2}: HW waits for 2
    const auto b = batchOf(1, 1000);
    int ok = 0;
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) {
        code = rig.produce(b, /*acks=*/0);
        if (code == kOk) ++ok;
    }
    ASSERT_EQ(code, kTransientLogFull) << "a full ring must refuse (42, retriable), not evict unreplicated data";
    EXPECT_GT(ok, 0);
    EXPECT_EQ(rig.mem.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(rig.mem.fullRefusals(), 1u);
    EXPECT_GE(rig.worker().transientFullRefusalsForTest(), 1u);
    const int64_t leo = rig.mem.logEndOffset();
    EXPECT_EQ(leo, ok) << "a refused produce appended nothing";

    rig.fetch(leo);   // follower 2 now holds everything: the HW reaches the LEO
    for (int i = 0; i < 8; ++i) rig.pass();
    ASSERT_EQ(rig.rep->highWatermark(), leo);
    EXPECT_EQ(rig.produce(b, 0), kOk) << "committed records make room";
    EXPECT_GT(rig.mem.logStartOffset(), 0);
}

TEST(TransientDataPathRegression, FullRingMarkerIsRefusedRetriablyRegression) {
    // Full by the batch-count cap (8 slots), not by bytes: a byte-full ring can
    // still hold a ~80-byte marker in its slack (the marker then lands and parks
    // on the HW — correct, but not the refusal this test pins).
    Rig rig(/*ringBytes=*/1u << 20, /*maxBatches=*/8, /*withFollower=*/true);
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) code = rig.produce(batchOf(1, 100), 0);
    ASSERT_EQ(code, kTransientLogFull) << "precondition: the ring is full of uncommitted data";
    const int64_t leo = rig.mem.logEndOffset();
    const uint64_t refusedBefore = rig.worker().transientFullRefusalsForTest();
    rig.marker(/*token=*/5, /*pid=*/9, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 5) mcode = r.errorCode;
        });
    }
    EXPECT_EQ(mcode, kTransientLogFull) << "the coordinator must retry a refused marker (was: Internal — dropped)";
    EXPECT_EQ(rig.mem.logEndOffset(), leo) << "a refused marker appended nothing";
    EXPECT_EQ(rig.worker().transientFullRefusalsForTest(), refusedBefore + 1);
}

// C1 at the worker: a transient topic whose catalog record has not committed has
// NO replica on any worker (BrokerNode builds it on kA2cCatalogTopicApplied), so a
// shard-ingress produce for it is "not owned" — retriable NotLeader, nothing
// appended anywhere.  Pins that the transient branch (replica->memoryLog())
// never turns "no replica" into an append.
TEST(TransientDataPathRegression, UnregisteredTransientPartitionIsNotLeaderRegression) {
    Rig rig;
    rig.worker().setOwnedDirect({});   // the partition is not (yet) a replica of this worker
    const auto b = batchOf(1, 50);
    EXPECT_EQ(rig.produce(b, /*acks=*/1), kNotLeader);
    EXPECT_EQ(rig.mem.logEndOffset(), 0) << "an uncommitted / unowned transient partition was appended to";
}

// ---------------------------------------------------------------------------
// Task 5: push reads served from the ring; the evicted cursor (TRANSIENT_EVICTED);
// read_committed; ownership moves.
// ---------------------------------------------------------------------------
TEST(TransientDataPathRegression, PushReadServesTheRingRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(3, 100), 0), kOk);
    ASSERT_EQ(rig.produce(batchOf(3, 100), 0), kOk);
    const auto r = rig.readOnce(/*subId=*/7, /*from=*/0);
    ASSERT_TRUE(r.has_value()) << "no PushReadResult";
    ASSERT_FALSE(r->chunk.empty()) << "a transient PushRead was left unanswered (no disk Log)";
    EXPECT_EQ(r->pushEffectiveBound(), 6);
    EXPECT_EQ(r->newNextOffset, 6);
    std::size_t pos = 0;
    int64_t expect = 0;
    while (pos < r->chunk.size()) {
        rune::protocol::RecordBatchReader b(r->chunk.data() + pos, r->chunk.size() - pos);
        ASSERT_GT(b.totalLength(), 0);
        EXPECT_EQ(b.baseOffset(), expect);
        expect = b.baseOffset() + b.lastOffsetDelta() + 1;
        pos += static_cast<std::size_t>(b.totalLength());
    }
    EXPECT_EQ(expect, 6) << "whole contiguous batches from the cursor";
}

TEST(TransientDataPathRegression, PushReadBelowRingStartResumesAtRingStartRegression) {
    Rig rig(/*ringBytes=*/8 * 1024, /*maxBatches=*/1000);
    for (int i = 0; i < 20; ++i) ASSERT_EQ(rig.produce(batchOf(1, 1000), 0), kOk);
    const int64_t start = rig.mem.logStartOffset();
    ASSERT_GT(start, 0) << "precondition: the ring evicted committed records (retention)";
    const auto r = rig.readOnce(9, /*from=*/0);   // a consumer that fell behind the ring
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->chunk.empty()) << "a cursor below the ring start stalled";
    EXPECT_EQ(rune::protocol::RecordBatchReader(r->chunk.data(), r->chunk.size()).baseOffset(), start);
    EXPECT_EQ(rig.worker().transientCursorSkipsForTest(), 1u);
    EXPECT_EQ(rig.worker().transientRecordsSkippedForTest(), static_cast<uint64_t>(start));
}

// E2 final fix wave (final review M2): the gateway resolves Earliest to 0 for a
// transient partition, so every Earliest / low-offset subscribe on a ring that
// evicted anything used to be counted as `start - 0` evicted-unread records
// and warned as "a reader fell behind its ring" — consumer lag that never
// happened.  A PushRead carrying the gateway's FRESH bind-time cursor (bit 2 of
// the op's flag byte, through the real ring encode/decode) is raised to the
// ring start and counted apart; a non-fresh one (a cursor from a delivery)
// keeps the lag count.  RED (before M2): the fresh read counted `start`
// records skipped.
TEST(TransientDataPathRegression, FreshCursorBelowTheRingStartIsNotCountedAsLagRegression) {
    Rig rig(/*ringBytes=*/8 * 1024, /*maxBatches=*/1000);
    for (int i = 0; i < 20; ++i) ASSERT_EQ(rig.produce(batchOf(1, 1000), 0), kOk);
    const int64_t start = rig.mem.logStartOffset();
    ASSERT_GT(start, 0) << "precondition: the ring evicted committed records (retention)";
    const auto r = rig.readOnce(11, /*from=*/0, /*isolation=*/0, /*fresh=*/true);   // subscribed Earliest
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->chunk.empty()) << "a fresh cursor below the ring start stalled";
    EXPECT_EQ(rune::protocol::RecordBatchReader(r->chunk.data(), r->chunk.size()).baseOffset(), start);
    EXPECT_EQ(rig.worker().transientCursorSkipsForTest(), 0u) << "a subscribe below the ring start counted as lag";
    EXPECT_EQ(rig.worker().transientRecordsSkippedForTest(), 0u);
    EXPECT_EQ(rig.worker().transientFreshCursorsRaisedForTest(), 1u);
    // The same cursor from a delivery (not fresh) is lag: counted as before.
    ASSERT_TRUE(rig.readOnce(12, /*from=*/0).has_value());
    EXPECT_EQ(rig.worker().transientCursorSkipsForTest(), 1u);
    EXPECT_EQ(rig.worker().transientRecordsSkippedForTest(), static_cast<uint64_t>(start));
}

TEST(TransientDataPathRegression, ReadCommittedHoldsTheOpenTransactionUntilItsCommitRegression) {
    Rig rig;
    constexpr int64_t kPid = 7;
    const auto txn = batchOf(1, 50, kPid, /*epoch=*/0, /*seq=*/0, /*txn=*/true);
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code != kOk; ++i)   // NotLeader while the RC state builds (retriable)
        code = rig.produce(txn, /*acks=*/0, &base, kPid, 0, 0);
    ASSERT_EQ(code, kOk);
    ASSERT_EQ(base, 0);

    const auto open = rig.readOnce(21, 0, /*read_committed=*/1);
    ASSERT_TRUE(open.has_value());
    EXPECT_TRUE(open->chunk.empty()) << "an OPEN transaction leaked to read_committed";
    EXPECT_EQ(open->pushEffectiveBound(), 0) << "the LSO pins at the open transaction's first offset";

    rig.marker(/*token=*/3, kPid, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 3) mcode = r.errorCode;
        });
    }
    ASSERT_EQ(mcode, kOk);

    const auto committed = rig.readOnce(22, 0, 1);
    ASSERT_TRUE(committed.has_value());
    ASSERT_FALSE(committed->chunk.empty()) << "the committed transaction was never delivered";
    rune::protocol::RecordBatchReader b(committed->chunk.data(), committed->chunk.size());
    EXPECT_EQ(b.baseOffset(), 0);
    EXPECT_EQ(static_cast<std::size_t>(b.totalLength()), committed->chunk.size()) << "a marker was delivered";
    EXPECT_EQ(committed->newNextOffset, 2) << "the cursor passes the marker";
}

TEST(TransientDataPathRegression, DeferredPushReadAfterOwnershipMovedNeverReadsTheRingRegression) {
    Rig rig;   // 4 MiB ring
    const auto b = batchOf(4, 1000);
    for (int i = 0; i < 300; ++i) ASSERT_EQ(rig.produce(b, 0), kOk);   // ~1.2 MiB: several passes of push budget
    for (int s = 0; s < 4; ++s) rig.pushRead(200 + s, 0);
    rig.pass();
    ASSERT_GT(rig.worker().deferredPushReadsForTest(), 0u) << "precondition: reads deferred";
    int answeredWithData = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::PushReadResult && !r.chunk.empty()) ++answeredWithData;
    });
    rig.worker().setOwnedDirect({});   // the partition moved to another worker
    for (int k = 0; k < 4; ++k) rig.pass();
    int unanswered = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type != WorkerReturn::Type::PushReadResult) return;
        EXPECT_TRUE(r.chunk.empty()) << "read a ring this worker no longer owns (a cross-thread read)";
        EXPECT_EQ(r.pushEffectiveBound(), -1);
        ++unanswered;
    });
    EXPECT_EQ(answeredWithData + unanswered, 4);
    EXPECT_EQ(rig.worker().deferredPushReadsForTest(), 0u);
}

// ---------------------------------------------------------------------------
// E2 Task 11 fix round 1 (matrix cells ReadCommitted_Transient_*_Leader_Committed_
// LeaderMovesMidOp): every ownership / leadership change builds a FRESH RcState.
// Its memory-backed edge went Unbuilt -> Ready rebuilding only the producer
// state — an EMPTY LsoTracker (LSO = HW: an open transaction delivered to
// read_committed) and an EMPTY AbortedTxnIndex (aborted batches delivered).  The
// ring must be scanned for both, exactly like the disk log's build.
// ---------------------------------------------------------------------------
namespace {
int16_t produceTxnOk(Rig& rig, int64_t pid, int32_t seq, int64_t* base) {
    const auto txn = batchOf(1, 50, pid, /*epoch=*/0, seq, /*txn=*/true);
    int16_t code = -1;
    for (int i = 0; i < 64 && code != kOk; ++i)   // NotLeader while the RC state builds (retriable)
        code = rig.produce(txn, /*acks=*/0, base, pid, 0, seq);
    return code;
}
int16_t markerAnswer(Rig& rig, int64_t token, int64_t pid, bool commit) {
    rig.marker(token, pid, commit);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == token) mcode = r.errorCode;
        });
    }
    return mcode;
}
void moveOwnershipAwayAndBack(Rig& rig) {
    rig.worker().setOwnedDirect({});
    for (int i = 0; i < 4; ++i) rig.pass();
    Worker::OwnedSet s;
    s.push_back(rig.rep);
    rig.worker().setOwnedDirect(std::move(s));
    for (int i = 0; i < 4; ++i) rig.pass();
}
} // namespace

TEST(TransientDataPathRegression, ReadCommittedKeepsAnOpenTransactionAcrossAnOwnershipChangeRegression) {
    Rig rig;
    constexpr int64_t kPid = 7;
    int64_t base = -1;
    ASSERT_EQ(produceTxnOk(rig, kPid, 0, &base), kOk);
    ASSERT_EQ(base, 0);
    ASSERT_EQ(rig.produce(batchOf(1, 50), /*acks=*/0, &base), kOk);   // plain at 1
    ASSERT_EQ(base, 1);

    moveOwnershipAwayAndBack(rig);   // a fresh RcState, rebuilt from the ring

    const auto open = rig.readOnce(31, 0, /*read_committed=*/1);
    ASSERT_TRUE(open.has_value());
    EXPECT_TRUE(open->chunk.empty()) << "an OPEN transaction leaked to read_committed after an ownership change";
    EXPECT_EQ(open->pushEffectiveBound(), 0) << "the rebuilt LSO must pin at the open transaction's first offset";

    ASSERT_EQ(markerAnswer(rig, 3, kPid, /*commit=*/true), kOk);
    const auto committed = rig.readOnce(32, 0, 1);
    ASSERT_TRUE(committed.has_value());
    ASSERT_FALSE(committed->chunk.empty()) << "the committed transaction was never delivered";
    EXPECT_EQ(rune::protocol::RecordBatchReader(committed->chunk.data(), committed->chunk.size()).baseOffset(), 0);
    EXPECT_EQ(committed->newNextOffset, 3) << "txn at 0, plain at 1, marker at 2";
}

TEST(TransientDataPathRegression, ReadCommittedFiltersAnAbortedTransactionAcrossAnOwnershipChangeRegression) {
    Rig rig;
    constexpr int64_t kPid = 9;
    int64_t base = -1;
    ASSERT_EQ(produceTxnOk(rig, kPid, 0, &base), kOk);   // txn at 0
    ASSERT_EQ(markerAnswer(rig, 4, kPid, /*commit=*/false), kOk);   // ABORT marker at 1
    ASSERT_EQ(rig.produce(batchOf(1, 50), /*acks=*/0, &base), kOk);   // plain at 2
    ASSERT_EQ(base, 2);

    moveOwnershipAwayAndBack(rig);

    std::vector<int64_t> bases;
    int64_t from = 0;
    for (int i = 0; i < 8 && from < 3; ++i) {
        const auto r = rig.readOnce(40 + i, from, /*read_committed=*/1);
        ASSERT_TRUE(r.has_value());
        std::size_t pos = 0;
        while (pos < r->chunk.size()) {
            rune::protocol::RecordBatchReader b(r->chunk.data() + pos, r->chunk.size() - pos);
            bases.push_back(b.baseOffset());
            pos += static_cast<std::size_t>(b.totalLength());
        }
        if (r->newNextOffset <= from) break;
        from = r->newNextOffset;
    }
    EXPECT_EQ(bases, (std::vector<int64_t>{2})) << "an ABORTED batch was delivered after an ownership change "
                                                   "(the rebuilt state lost the aborted index)";
}

// ---------------------------------------------------------------------------
// E2 Task 12 fix round 1 (finding B, controller ruling "option (c) refined"):
// the RcState rebuilt from the ring after an ownership change saw only the
// in-ring batches of an OPEN transaction whose FIRST batch the ring had
// evicted — it opened it at its first IN-RING batch, so read_committed got the
// committed records between the ring start and that batch although the
// transaction is still open.  The rebuild now pins such a transaction at the
// ring start (its first batch may lie below it); a transaction whose producer
// left a COMMIT/ABORT marker in the ring before its first in-ring batch is
// provably complete in the ring and opens at that batch.  The producer stays
// known (its in-ring sequence) and its marker resolves it as normal.  A
// transaction ALL of whose batches were evicted is unrecoverable (documented:
// UnknownProducerId on its next produce; read_committed follows the evicted
// cursor rule).
// ---------------------------------------------------------------------------

namespace {
int64_t ringPut(rune::storage::InMemoryLog& mem, const std::vector<uint8_t>& b) {
    int64_t base = -1;
    const int32_t delta = rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
    EXPECT_GE(mem.appendNoSyncRestamp(b.data(), b.size(), delta, base), 0);
    return base;
}
std::vector<uint8_t> markerOf(int64_t pid, bool commit) {
    return rune::transaction::ControlMarker::build(
        pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
}
} // namespace

TEST(TransientDataPathRegression, RcRingRebuildPinsAnOpenTxnWhoseFirstBatchWasEvictedAtTheRingStartRegression) {
    rune::storage::InMemoryLog mem(8 * 1024, 1000);   // plain DropOldest: evicts as needed
    constexpr int64_t kA = 9, kB = 11;
    ASSERT_EQ(ringPut(mem, batchOf(1, 50, kA, 0, 0, /*txn=*/true)), 0);   // A's txn opens at 0
    for (int i = 0; i < 4; ++i) ringPut(mem, batchOf(1, 1500));
    const int64_t a1 = ringPut(mem, batchOf(1, 50, kA, 0, 1, true));       // A's 2nd batch
    ringPut(mem, batchOf(1, 50, kB, 0, 0, true));                          // B: a whole txn in the ring
    ringPut(mem, markerOf(kB, true));
    const int64_t b1 = ringPut(mem, batchOf(1, 50, kB, 0, 1, true));       // B's next txn: provably its first
    for (int i = 0; i < 8 && mem.logStartOffset() == 0; ++i) ringPut(mem, batchOf(1, 1500));
    const int64_t ringStart = mem.logStartOffset();
    ASSERT_GT(ringStart, 0) << "precondition: A's first batch was evicted";
    ASSERT_LT(ringStart, a1) << "precondition: A's second batch (and B's batches) are still in the ring";

    rune::node::RcState rc;
    rc.reserve(64);
    rc.reserveRingRebuild();
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;
    (void)rc.advance(f, nullptr, rune::node::RcState::kDefaultScanBytesPerPass, &mem);
    ASSERT_TRUE(rc.ready());
    EXPECT_EQ(rc.tracker().firstOffsetOf(kA), ringStart)
        << "an open txn whose first batch may have been evicted must pin at the ring start (was: its first "
           "in-ring batch " << a1 << ")";
    EXPECT_EQ(rc.tracker().firstOffsetOf(kB), b1) << "a txn after its producer's in-ring marker opens at its batch";
    EXPECT_EQ(rc.lso(INT64_MAX), ringStart);

    rc.onMarker(kA, mem.logEndOffset(), /*commit=*/false);   // A's ABORT resolves it as normal
    EXPECT_EQ(rc.lso(INT64_MAX), b1);
    EXPECT_TRUE(rc.aborted().isAborted(kA, a1)) << "the abort covers A's in-ring batch";
    rc.onMarker(kB, mem.logEndOffset() + 1, true);
    EXPECT_EQ(rc.lso(INT64_MAX), INT64_MAX) << "nothing open";
}

// ---------------------------------------------------------------------------
// E2 final fix wave (final review I3): the memory-backed read_committed (and
// producer-state) rebuild walked the WHOLE ring in one worker pass — up to
// 8 MiB of ~10^5 small batches per partition, for every partition whose
// leadership / ownership changed in that pass: tens to hundreds of ms on the
// worker.  It is now the budgeted RingScanning state: at most ~the scan budget
// (plus one batch) per advance step, many steps, and the result is EXACTLY the
// one-shot result — the same open transactions, aborted ranges, LSO and
// producer state (the PSM checked against ProducerStateManager::rebuildFromLog,
// the old memory path).  RED (one-shot): the first advance parsed the whole
// ring (maxStepBytes ~ 8 MiB) and was Ready at once.
// ---------------------------------------------------------------------------
namespace {
struct RingContent {
    std::vector<int64_t> pids;
};
// A full 8 MiB ring of minimum-size batches with transactions interleaved:
// committed, aborted, idempotent and one still-open transaction per producer
// group; the ring has wrapped (evicted its oldest batches).
RingContent fillFullRing(rune::storage::InMemoryLog& mem) {
    RingContent c;
    const auto plain = batchOf(1, 0);
    for (int64_t p = 0; p < 16; ++p) c.pids.push_back(100 + p);
    std::vector<int32_t> seq(16, 0);
    int64_t i = 0;
    while (mem.logStartOffset() == 0 || i < 200'000) {
        if (i % 997 == 0) {
            const std::size_t k = static_cast<std::size_t>((i / 997) % 16);
            const int64_t pid = c.pids[k];
            ringPut(mem, batchOf(1, 0, pid, 0, seq[k]++, /*txn=*/true));
            if ((i / 997) % 3 != 2) ringPut(mem, markerOf(pid, /*commit=*/((i / 997) % 3) == 0));
        } else if (i % 499 == 0) {
            ringPut(mem, batchOf(1, 0, 500 + (i % 7), 0, static_cast<int32_t>(i / 499)));   // idempotent
        } else {
            ringPut(mem, plain);
        }
        ++i;
        if (i > 2'000'000) break;   // bounded
    }
    return c;
}
}  // namespace

TEST(TransientDataPathRegression, RcRingRebuildIsBudgetedPerStepAndEqualsTheOneShotBuildRegression) {
    rune::storage::InMemoryLog mem(8u << 20, 1'000'000);
    const RingContent content = fillFullRing(mem);
    ASSERT_GT(mem.logStartOffset(), 0) << "precondition: the ring wrapped (full)";
    ASSERT_GT(mem.logEndOffset() - mem.logStartOffset(), 50'000) << "precondition: ~10^5 small batches";
    constexpr int32_t kTopicId = 7, kPart = 0;
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;

    // The budgeted build.
    rune::storage::ProducerStateManager psm;
    rune::node::RcState rc;
    rc.reserve(256);
    rc.reserveRingRebuild();
    rc.bindProducerState(&psm, kTopicId, kPart);
    const std::size_t budget = rune::node::RcState::kDefaultScanBytesPerPass;
    int steps = 0;
    std::size_t firstStep = 0;
    for (; steps < 100'000 && !rc.ready(); ++steps) {
        const std::size_t n = rc.advance(f, nullptr, static_cast<int32_t>(budget), &mem);
        if (steps == 0) firstStep = n;
    }
    ASSERT_TRUE(rc.ready()) << "the budgeted ring build never finished";
    EXPECT_LE(firstStep, budget + 1024) << "one advance step parsed " << firstStep << " B (the whole ring in one pass)";
    EXPECT_LE(rc.maxStepBytes(), budget + 1024) << "a step exceeded the budget by more than one batch";
    EXPECT_GE(steps, static_cast<int>((8u << 20) / budget) - 1) << "the 8 MiB ring was not spread over the passes";

    // The one-shot reference (the pre-I3 behaviour: a budget larger than the ring)
    // and the old producer-state rebuild.
    rune::storage::ProducerStateManager psmRef;
    ASSERT_TRUE(psmRef.rebuildFromLog(kTopicId, mem, mem.logEndOffset(), kPart));
    rune::node::RcState ref;
    ref.reserve(256);
    ref.reserveRingRebuild();
    (void)ref.advance(f, nullptr, INT32_MAX, &mem);
    ASSERT_TRUE(ref.ready());

    EXPECT_EQ(rc.lso(INT64_MAX), ref.lso(INT64_MAX));
    EXPECT_EQ(rc.tracker().openCount(), ref.tracker().openCount());
    for (int64_t pid : content.pids)
        EXPECT_EQ(rc.tracker().firstOffsetOf(pid), ref.tracker().firstOffsetOf(pid)) << "producer " << pid;
    const auto& a = rc.aborted().ranges();
    const auto& b = ref.aborted().ranges();
    ASSERT_EQ(a.size(), b.size());
    for (std::size_t k = 0; k < a.size(); ++k) {
        EXPECT_EQ(a[k].producerId, b[k].producerId);
        EXPECT_EQ(a[k].firstOffset, b[k].firstOffset);
        EXPECT_EQ(a[k].lastOffset, b[k].lastOffset);
    }
    EXPECT_EQ(rc.ringPinnedTxns(), ref.ringPinnedTxns());
    std::vector<int64_t> allPids = content.pids;
    for (int64_t p = 500; p < 507; ++p) allPids.push_back(p);
    for (int64_t pid : allPids) {
        const auto* x = psm.entry(kTopicId, pid, kPart);
        const auto* y = psmRef.entry(kTopicId, pid, kPart);
        ASSERT_EQ(x == nullptr, y == nullptr) << "producer " << pid;
        if (x == nullptr) continue;
        EXPECT_EQ(x->producerEpoch, y->producerEpoch) << "producer " << pid;
        EXPECT_EQ(x->lastSequence, y->lastSequence) << "producer " << pid;
        EXPECT_EQ(x->markerOnly, y->markerOnly) << "producer " << pid;
        EXPECT_EQ(x->coordinatorEpoch, y->coordinatorEpoch) << "producer " << pid;
    }
}

// I3: a budgeted ring build spans passes, and the ring may EVICT batches it has
// not read yet meanwhile (non-idempotent produce keeps appending; eviction is
// below the HW).  A marker among them could close a transaction the build
// already opened — clamping the cursor would leave that transaction open
// forever (LSO pinned).  The build restarts from the new ring start instead.
// RED (mutation: clamp the cursor instead of restarting): producer A stays open
// and the LSO is pinned at its batch.
TEST(TransientDataPathRegression, RcRingBuildRestartsWhenTheRingEvictsPastItsCursorRegression) {
    rune::storage::InMemoryLog mem(64 * 1024, 100'000);   // DropOldest: evicts as needed
    constexpr int64_t kA = 41;
    const auto pad = batchOf(1, 200);
    for (int i = 0; i < 20; ++i) ringPut(mem, pad);
    const int64_t a0 = ringPut(mem, batchOf(1, 50, kA, 0, 0, /*txn=*/true));   // A opens ...
    for (int i = 0; i < 20; ++i) ringPut(mem, pad);
    ringPut(mem, markerOf(kA, /*commit=*/true));                               // ... and commits
    for (int i = 0; i < 100; ++i) ringPut(mem, pad);
    ASSERT_EQ(mem.logStartOffset(), 0) << "precondition: nothing evicted yet";

    rune::node::RcState rc;
    rc.reserve(64);
    rc.reserveRingRebuild();
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;
    // Steps of ~1 KiB: the build reads A's batch (opens A) but not yet its marker.
    while (!rc.ready() && rc.tracker().firstOffsetOf(kA) < 0)
        (void)rc.advance(f, nullptr, /*budget=*/1024, &mem);
    ASSERT_FALSE(rc.ready());
    ASSERT_EQ(rc.tracker().firstOffsetOf(kA), a0) << "precondition: the build opened A";
    // The ring evicts past the cursor — A's batch AND its marker go before the build reads the marker.
    while (mem.logStartOffset() <= a0 + 22) ringPut(mem, pad);
    for (int i = 0; i < 10'000 && !rc.ready(); ++i) (void)rc.advance(f, nullptr, 1024, &mem);
    ASSERT_TRUE(rc.ready());
    EXPECT_GE(rc.ringScanRestarts(), 1u) << "the build did not restart after the ring evicted past its cursor";
    EXPECT_LT(rc.tracker().firstOffsetOf(kA), 0) << "A (committed, all evicted) stayed open";
    EXPECT_EQ(rc.lso(INT64_MAX), INT64_MAX) << "the LSO is pinned by a transaction that committed";
}

// #37: a partition that appends (and so evicts) more bytes per worker pass than the build's budget overtakes every
// restarted build of the same budget: read_committed never became Ready and the restarts never stopped.  Each restart
// of a build doubles its per-step slice, so once the slice covers the (bounded) ring the next step applies the whole
// [cursor, target) in one pass - nothing evicts during it (single writer).
// RED (mutation: no boost): never Ready within the cap.
namespace {
// The ring filled to eviction with 200-B pads; `tail` batches appended last (newest, evicted last).
void fillRingToEviction(rune::storage::InMemoryLog& mem, const std::vector<std::vector<uint8_t>>& tail) {
    const auto pad = batchOf(1, 200);
    while (mem.logStartOffset() == 0) ringPut(mem, pad);
    for (const auto& b : tail) ringPut(mem, b);
}
// Advance the build (budget 512) then append 8 pads (~1.6 KB > the budget) per pass, up to `cap` passes.
int advanceUnderSustainedEviction(rune::node::RcState& rc, rune::storage::InMemoryLog& mem, int cap) {
    const auto pad = batchOf(1, 200);
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;
    int passes = 0;
    for (; passes < cap && !rc.ready(); ++passes) {
        (void)rc.advance(f, nullptr, /*budget=*/512, &mem);
        if (rc.ready()) break;
        for (int i = 0; i < 8; ++i) ringPut(mem, pad);
    }
    return passes;
}
}  // namespace

TEST(TransientDataPathRegression, RcRingBuildConvergesUnderSustainedEvictionFasterThanItsBudgetRegression) {
    rune::storage::InMemoryLog mem(64 * 1024, 100'000);   // DropOldest: evicts as needed
    fillRingToEviction(mem, {});
    ASSERT_GT(mem.logStartOffset(), 0) << "precondition: the ring evicts";
    rune::node::RcState rc;
    rc.reserve(64);
    rc.reserveRingRebuild();
    const int passes = advanceUnderSustainedEviction(rc, mem, /*cap=*/64);
    ASSERT_TRUE(rc.ready()) << "read_committed never became Ready in " << passes << " passes (restarts="
                            << rc.ringScanRestarts() << ")";
    EXPECT_GE(rc.ringScanRestarts(), 1u) << "precondition: the eviction overtook the unboosted build";
    EXPECT_LE(rc.ringScanRestarts(), 10u) << "log2(64K / 512) = 7 restarts at most, plus slack";
}

TEST(TransientDataPathRegression, RcRingBuildConvergedUnderSustainedEvictionIsExactRegression) {
    rune::storage::InMemoryLog mem(64 * 1024, 100'000);
    constexpr int64_t kCommitted = 51, kAborted = 52;
    fillRingToEviction(mem, {batchOf(1, 50, kCommitted, 0, 0, /*txn=*/true), batchOf(1, 50, kAborted, 0, 0, true),
                             markerOf(kCommitted, /*commit=*/true), markerOf(kAborted, /*commit=*/false)});
    rune::node::RcState rc;
    rc.reserve(64);
    rc.reserveRingRebuild();
    (void)advanceUnderSustainedEviction(rc, mem, /*cap=*/64);
    ASSERT_TRUE(rc.ready());
    ASSERT_GE(rc.ringScanRestarts(), 1u) << "precondition: a boosted (restarted) build computed the state";
    EXPECT_TRUE(rc.tracker().empty()) << "a transaction closed by its marker in the ring stayed open";
    EXPECT_LT(rc.tracker().firstOffsetOf(kCommitted), 0);
    EXPECT_LT(rc.tracker().firstOffsetOf(kAborted), 0);
    EXPECT_EQ(rc.lso(INT64_MAX), INT64_MAX) << "the LSO is pinned although every transaction ended";
    bool abortedIndexed = false, committedIndexed = false;
    for (const auto& r : rc.aborted().ranges()) {
        abortedIndexed   |= r.producerId == kAborted;
        committedIndexed |= r.producerId == kCommitted;
    }
    EXPECT_TRUE(abortedIndexed) << "the aborted transaction is missing from the aborted index";
    EXPECT_FALSE(committedIndexed) << "the committed transaction was indexed as aborted";
}

TEST(TransientDataPathRegression, OpenTxnWithItsFirstBatchEvictedSurvivesAnOwnershipChangeRegression) {
    Rig rig(/*ringBytes=*/8 * 1024, /*maxBatches=*/1000);   // single node: HW = LEO, the ring evicts freely
    constexpr int64_t kPid = 7;
    int64_t base = -1;
    ASSERT_EQ(produceTxnOk(rig, kPid, 0, &base), kOk);
    ASSERT_EQ(base, 0);
    for (int i = 0; i < 4; ++i) ASSERT_EQ(rig.produce(batchOf(1, 1500), 0), kOk);
    ASSERT_EQ(produceTxnOk(rig, kPid, 1, &base), kOk);
    const int64_t second = base;
    for (int i = 0; i < 8 && rig.mem.logStartOffset() == 0; ++i) ASSERT_EQ(rig.produce(batchOf(1, 1500), 0), kOk);
    const int64_t ringStart = rig.mem.logStartOffset();
    ASSERT_GT(ringStart, 0);
    ASSERT_LT(ringStart, second);

    moveOwnershipAwayAndBack(rig);   // the RcState is rebuilt from the ring

    const auto open = rig.readOnce(51, 0, /*read_committed=*/1);
    ASSERT_TRUE(open.has_value());
    EXPECT_TRUE(open->chunk.empty()) << "read_committed delivered past an open transaction after the move";
    EXPECT_EQ(open->pushEffectiveBound(), ringStart) << "the LSO pins at the ring start";

    ASSERT_EQ(rig.produce(batchOf(1, 50, kPid, 0, 2, true), 0, &base, kPid, 0, 2), kOk)
        << "the producer must stay known (no UnknownProducerId)";
    ASSERT_EQ(markerAnswer(rig, 8, kPid, /*commit=*/true), kOk);
    const auto committed = rig.readOnce(52, 0, 1);
    ASSERT_TRUE(committed.has_value());
    ASSERT_FALSE(committed->chunk.empty()) << "the COMMIT did not resolve the pinned transaction";
    EXPECT_EQ(rune::protocol::RecordBatchReader(committed->chunk.data(), committed->chunk.size()).baseOffset(),
              rig.mem.logStartOffset()) << "delivery resumes at the (current) ring start";
}

// ---------------------------------------------------------------------------
// PR #555 adversarial review (#37): the boosted (restarted) ring build.
// ---------------------------------------------------------------------------
namespace {
// Advance (budget) then append `perPass` pads per pass until Ready or `cap` passes; returns the passes.
int advanceEvicting(rune::node::RcState& rc, rune::storage::InMemoryLog& mem, const rune::node::RcFacts& f,
                    int32_t budget, int perPass, int cap) {
    const auto pad = batchOf(1, 200);
    int passes = 0;
    for (; passes < cap && !rc.ready(); ++passes) {
        (void)rc.advance(f, nullptr, budget, &mem);
        if (rc.ready()) break;
        for (int i = 0; i < perPass; ++i) ringPut(mem, pad);
    }
    return passes;
}
rune::node::RcFacts leaderFacts(uint64_t leadGen = 0) {
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1; f.leadershipGeneration = leadGen;
    return f;
}
}  // namespace

// Input bound: budget 0 (and 1) - the smallest slice, one batch per step - still converges under eviction, within
// ceil(log2(ring bytes)) + slack restarts, never an endless restart loop.
TEST(TransientDataPathRegression, RcRingBuildWithAZeroOrOneByteBudgetConvergesUnderEvictionAdversarialRegression) {
    for (const int32_t budget : {0, 1}) {
        rune::storage::InMemoryLog mem(64 * 1024, 100'000);
        fillRingToEviction(mem, {});
        rune::node::RcState rc;
        rc.reserve(64);
        rc.reserveRingRebuild();
        const int passes = advanceEvicting(rc, mem, leaderFacts(), budget, /*perPass=*/8, /*cap=*/256);
        ASSERT_TRUE(rc.ready()) << "budget " << budget << ": never Ready in " << passes << " passes (restarts="
                                << rc.ringScanRestarts() << ")";
        EXPECT_GE(rc.ringScanRestarts(), 1u) << "precondition: eviction overtook the unboosted build";
        EXPECT_LE(rc.ringScanRestarts(), 24u) << "budget " << budget << ": log2(64 KiB) = 16 restarts, plus slack";
    }
}

// Input bound: the maximum budget - one step covers the ring, no restart, no overflow of the slice.
TEST(TransientDataPathRegression, RcRingBuildWithTheMaxBudgetIsReadyInOneStepAdversarialRegression) {
    rune::storage::InMemoryLog mem(64 * 1024, 100'000);
    fillRingToEviction(mem, {batchOf(1, 50, 5, 0, 0, /*txn=*/true)});   // pid 5 open at the ring tail
    rune::node::RcState rc;
    rc.reserve(64);
    rc.reserveRingRebuild();
    const std::size_t bytes = rc.advance(leaderFacts(), nullptr, std::numeric_limits<int32_t>::max(), &mem);
    ASSERT_TRUE(rc.ready());
    EXPECT_GT(bytes, 0u);
    EXPECT_EQ(rc.ringScanRestarts(), 0u);
    EXPECT_GE(rc.tracker().firstOffsetOf(5), mem.logStartOffset()) << "the open txn was lost";
}

// A boosted build abandoned on an ownership change: the NEXT build (another leadership generation) starts unboosted
// - its first step parses about one budget, not the boosted slice - and itself converges exactly.
TEST(TransientDataPathRegression, RcRingBuildAfterAnOwnershipChangeStartsUnboostedAdversarialRegression) {
    rune::storage::InMemoryLog mem(256 * 1024, 100'000);
    fillRingToEviction(mem, {});
    rune::node::RcState rc;
    rc.reserve(64);
    rc.reserveRingRebuild();
    const auto pad = batchOf(1, 200);
    const auto f1 = leaderFacts(/*leadGen=*/1);
    std::size_t lastStep = 0;
    for (int i = 0; i < 64 && rc.ringScanRestarts() < 3; ++i) {
        lastStep = rc.advance(f1, nullptr, /*budget=*/512, &mem);
        ASSERT_FALSE(rc.ready()) << "precondition: still building";
        for (int k = 0; k < 32; ++k) ringPut(mem, pad);   // ~8 KiB evicted per pass: needs a boost of 4+
    }
    ASSERT_GE(rc.ringScanRestarts(), 3u) << "precondition: a boosted build";
    ASSERT_GT(lastStep, 512u * 4) << "precondition: the boosted slice (budget << 3)";
    const uint64_t restartsBefore = rc.ringScanRestarts();
    const auto f2 = leaderFacts(/*leadGen=*/2);              // ownership changed: the build is reset
    const std::size_t first = rc.advance(f2, nullptr, /*budget=*/512, &mem);
    EXPECT_EQ(rc.ringScanRestarts(), restartsBefore) << "the reset counted as an eviction restart";
    EXPECT_LE(first, 512u + pad.size()) << "the new build inherited the old build's boost";
    EXPECT_GT(first, 0u);
    const int passes = advanceEvicting(rc, mem, f2, 512, 32, 256);
    EXPECT_TRUE(rc.ready()) << "the new build never converged in " << passes << " passes";
}

// Exact state (model check, seeded): a build boosted by sustained eviction of random transactional traffic (open /
// committed / aborted txns of several producers, markers) equals a one-step rebuild of the same ring from the same
// ring start: same LSO, same open txns, same aborted index.
TEST(TransientDataPathRegression, RcRingBuildBoostedEqualsAOneStepRebuildSeededAdversarialRegression) {
    int compared = 0;
    for (uint32_t seed = 1; seed <= 40; ++seed) {
        std::mt19937 rng(seed);
        rune::storage::InMemoryLog mem(32 * 1024, 100'000);
        std::array<bool, 6> open{};
        auto putRandom = [&] {
            const int64_t pid = 10 + static_cast<int64_t>(rng() % open.size());
            switch (rng() % 4) {
                case 0: ringPut(mem, batchOf(1, 150)); break;
                case 1: ringPut(mem, batchOf(1, 120, pid, 0, 0, /*txn=*/true)); open[pid - 10] = true; break;
                default:
                    if (open[pid - 10]) { ringPut(mem, markerOf(pid, rng() % 2 == 0)); open[pid - 10] = false; }
                    else ringPut(mem, batchOf(1, 150));
            }
        };
        while (mem.logStartOffset() == 0) putRandom();
        rune::node::RcState rc;
        rc.reserve(64);
        rc.reserveRingRebuild();
        const auto f = leaderFacts();
        // Sustained eviction until a step restarts the build a second time (boost >= 2); from that step on nothing
        // is appended, so the boosted build finishes from exactly the ring start the reference starts from.
        bool frozen = false;
        for (int pass = 0; pass < 256 && !rc.ready(); ++pass) {
            const uint64_t before = rc.ringScanRestarts();
            (void)rc.advance(f, nullptr, /*budget=*/256, &mem);
            if (rc.ringScanRestarts() != before && rc.ringScanRestarts() >= 2) frozen = true;
            if (rc.ready() || frozen) continue;
            const int n = 6 + static_cast<int>(rng() % 6);
            for (int i = 0; i < n; ++i) putRandom();
        }
        ASSERT_TRUE(rc.ready()) << "seed " << seed << ": never Ready (restarts=" << rc.ringScanRestarts() << ")";
        if (!frozen) continue;   // converged before a second restart: built from an older ring start
        ++compared;
        rune::node::RcState ref;
        ref.reserve(64);
        ref.reserveRingRebuild();
        (void)ref.advance(f, nullptr, std::numeric_limits<int32_t>::max(), &mem);
        ASSERT_TRUE(ref.ready());
        EXPECT_EQ(rc.lso(INT64_MAX), ref.lso(INT64_MAX)) << "seed " << seed;
        for (int64_t pid = 10; pid < 16; ++pid)
            EXPECT_EQ(rc.tracker().firstOffsetOf(pid), ref.tracker().firstOffsetOf(pid)) << "seed " << seed << " pid " << pid;
        const auto& a = rc.aborted().ranges();
        const auto& b = ref.aborted().ranges();
        ASSERT_EQ(a.size(), b.size()) << "seed " << seed;
        for (std::size_t i = 0; i < a.size(); ++i) {
            EXPECT_EQ(a[i].producerId, b[i].producerId) << "seed " << seed;
            EXPECT_EQ(a[i].firstOffset, b[i].firstOffset) << "seed " << seed;
            EXPECT_EQ(a[i].lastOffset, b[i].lastOffset) << "seed " << seed;
        }
    }
    EXPECT_GT(compared, 10) << "too few seeds converged on a restart step to check exactness";
}
