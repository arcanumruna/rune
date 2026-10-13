// TransientDataPathTest.cpp — Phase 2b E2: the worker data path of a TRANSIENT
// (InMemoryLog) partition — produce over shard ingress and the op ring, push
// reads served from the ring, read_committed, the full-ring rule, the evicted
// cursor, ownership moves.  Deterministic: one worker driven by hand, no Aeron.
#include "TransientRig.h"

#include <rune/node/RcState.h>

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
// NO replica on any worker (BrokerNode builds it on kA2cTopicCommitted), so a
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
