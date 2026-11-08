// Checked writes Part 11 Task 8 (P11-0, P11-9): on a real Worker, a committed transaction becomes visible at its commit marker,
// whole: a read never sees part of it.  An application the pass cap splits (a transaction at its marker, an atomic batch) holds the
// view's reads until it is whole; an open transaction alone never holds them.
#include "CheckedWritesRig.h"

#include <array>

using namespace rune::node;
using namespace rune::node::cw;
using RS = rune::protocol::client::ViewRowResponse;

namespace {
constexpr int32_t kView = 0x40000602;
constexpr uint8_t kLeader = 1;

/// The view-key encoding of a u64 id (big-endian).
std::array<uint8_t, 8> keyOf(uint64_t id) {
    std::array<uint8_t, 8> k{};
    for (int i = 0; i < 8; ++i) k[static_cast<std::size_t>(i)] = static_cast<uint8_t>(id >> (8 * (7 - i)));
    return k;
}
std::vector<Rec> recs(uint64_t from, uint64_t to) {
    std::vector<Rec> out;
    for (uint64_t id = from; id <= to; ++id) out.push_back(Rec{id, static_cast<int64_t>(id), 1});
    return out;
}

/// A projection over a typed source, its instance folding with a pass cap of `cap` records (0 = uncapped).
struct Txv {
    VerdictRig r;
    explicit Txv(int64_t cap) {
        r.typed();
        r.enableAcks();
        r.installProjection(kView);
        EXPECT_EQ(r.produce({{99, 99, 1}}), 0);
        for (int i = 0; i < 200 && !r.hasRow(kView, 99); ++i) r.pass();
        EXPECT_TRUE(r.hasRow(kView, 99));
        r.vs->setMaxRecordsPerPassForTest(kView, 0, cap);
    }
    [[nodiscard]] int visible(uint64_t from, uint64_t to) const {
        int n = 0;
        for (uint64_t id = from; id <= to; ++id) n += r.hasRow(kView, id) ? 1 : 0;
        return n;
    }
    /// Producer `pid`'s transactional batch of `ids`, acked.
    void txn(int64_t pid, std::vector<Rec> ids) { ASSERT_TRUE(r.ack(r.produceFrame(ids, kLeader, pid, 0, /*txn=*/true))); }
    /// A Produce op of `bytes` on the op ring, without a pass.
    void enqueue(std::vector<uint8_t> bytes, int64_t pid = -1) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = VerdictRig::kSrc; op.partition = 0; op.replica = r.replica.get(); op.logPtr = r.log.get();
        op.sessionId = 1; op.correlationId = ++r.corr; op.durabilityTier = 1;
        op.producerId = pid; op.producerEpoch = 0; op.baseSequence = pid >= 0 ? 0 : -1;
        op.recordBatchBytes = std::move(bytes);
        ASSERT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
    }
    [[nodiscard]] bool fenced() const { return r.vs->readFenced(kView, 0); }
    /// The coordinator's marker, enqueued without a pass: the test drives every pass itself.
    void marker(int64_t pid, bool commit) {
        WorkerOp op;
        op.type = WorkerOp::Type::AppendControlRecord; op.topicId = VerdictRig::kSrc; op.partition = 0; op.replica = r.replica.get();
        op.logPtr = r.log.get(); op.correlationId = 9000 + pid; op.producerId = pid;
        op.targetReplicaId = r.replica->registryId(); op.incarnation = r.replica->incarnation();
        op.recordBatchBytes = rune::transaction::ControlMarker::build(
            pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
        ASSERT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
    }
    /// Every pass: an unfenced view shows all of [from, to] or none of it.  Counts the fenced passes.
    void watchWhole(uint64_t from, uint64_t to, int& fencedPasses) {
        r.onEveryPass([this, from, to, &fencedPasses] {
            const int n = visible(from, to);
            if (fenced()) { ++fencedPasses; return; }
            EXPECT_TRUE(n == 0 || n == static_cast<int>(to - from + 1)) << n << " of the records visible, unfenced";
        });
    }
    /// Drives until the view is fenced; then a read of `id` (the split application's last record) is held and answered whole.
    void readHeldWhileFenced(uint64_t id) {
        bool sawFence = false;
        for (int i = 0; i < 50 && !sawFence; ++i) {
            r.pass();
            sawFence = fenced();
        }
        ASSERT_TRUE(sawFence) << "the pass cap never split the application (the vacuity guard)";
        const auto k = keyOf(id);
        const auto got = r.getViewRowK(kView, k);
        EXPECT_EQ(got.code, RS::kOk) << "a read during the application is held, then served whole (never AbsentKey)";
        EXPECT_FALSE(got.row.empty());
    }
};
}  // namespace

// P11-9: six records of one transaction, a pass cap of two: the application at the marker takes three passes, and no read sees
// two or four of them.
TEST(CheckedWritesTxnVisibility, CommittedTxnLargerThanThePassCapIsNeverReadHalfApplied) {
    Txv t(2);
    int fencedPasses = 0;
    t.watchWhole(1, 6, fencedPasses);
    t.txn(31, recs(1, 6));
    EXPECT_EQ(t.visible(1, 6), 0) << "an open transaction is not visible";
    t.marker(31, true);
    t.readHeldWhileFenced(6);
    t.r.settle();
    EXPECT_EQ(t.visible(1, 6), 6);
    EXPECT_EQ(fencedPasses, 2) << "6 records at 2 a pass: two passes end inside the application";
    EXPECT_FALSE(t.fenced());
}

// P11-9: an atomic batch the pass cap cuts is held the same way.
TEST(CheckedWritesTxnVisibility, AtomicBatchSplitByThePassCapIsFenced) {
    Txv t(2);
    int fencedPasses = 0;
    t.watchWhole(1, 6, fencedPasses);
    std::vector<uint8_t> b = VerdictRig::batch(recs(1, 6));
    VerdictRig::setBatchAttribute(b, rune::protocol::batch::kAttrAtomic);
    ASSERT_EQ(t.r.produceBatch(b), 0);
    t.readHeldWhileFenced(6);
    t.r.settle();
    EXPECT_EQ(t.visible(1, 6), 6);
    EXPECT_EQ(fencedPasses, 2) << "6 records at 2 a pass: two passes end inside the batch";
    EXPECT_FALSE(t.fenced());
}

// Hot path: plain batches the cap splits never raise the fence (their records become visible as they fold).
TEST(CheckedWritesTxnVisibility, NoTransactionsNoFence) {
    Txv t(2);
    int partial = 0;
    t.r.onEveryPass([&] {
        EXPECT_FALSE(t.fenced());
        const int n = t.visible(1, 6);
        partial += n > 0 && n < 6 ? 1 : 0;
    });
    ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(1, 6))), 0);
    t.r.settle();
    EXPECT_EQ(t.visible(1, 6), 6);
    EXPECT_GE(partial, 1) << "the cap split the batch (the vacuity guard)";
    EXPECT_EQ(t.r.vs->foldCounters(kView, 0)->txnSpanTableFull, 0u);
}

// REGRESSION (P11-9): A commits behind an open B, so the fold stops at B's first batch (the LSO) with A's span open below it.
// That is not an application in progress: reads are served (A absent) for as long as B stays open, never held to NotReady.
// Once B commits, A and B each appear whole, in one pass.
TEST(CheckedWritesTxnVisibilityRegression, CommittedSpanCrossingTheLsoNeverHoldsReadsRegression) {
    Txv t(0);
    t.txn(41, recs(10, 12));
    t.txn(42, recs(20, 20));
    t.marker(41, true);
    t.r.passes(5);
    ASSERT_TRUE(t.r.markerAnswer(41)) << "A's marker is in the log";
    const auto k10 = keyOf(10);
    for (int s = 0; s < 10; ++s) {   // ten seconds of the injected clock, B open throughout
        t.r.now += 1'000'000'000;
        t.r.pass();
        EXPECT_FALSE(t.fenced()) << "second " << s;
        EXPECT_EQ(t.r.getViewRowK(kView, k10).code, RS::kAbsentKey) << "second " << s << ": served at once, A not yet visible";
        EXPECT_EQ(t.r.worker->heldViewReadsForTest(), 0u);
    }
    t.r.onEveryPass([&] {
        const int a = t.visible(10, 12), b = t.visible(20, 20);
        EXPECT_TRUE(a == 0 || a == 3) << a;
        EXPECT_EQ(a == 3, b == 1) << "A and B appear in the same pass";
    });
    t.marker(42, true);
    t.r.settle();
    EXPECT_EQ(t.visible(10, 12), 3);
    EXPECT_EQ(t.visible(20, 20), 1);
}

// REGRESSION (P11-9): the read-your-writes barrier serves a held read once the fold cursor reaches the read's target, and an
// application stands at its marker, at or above that target.  A read that arrived before the marker (target = the open
// transaction's first offset) would be served after the pass that applied two of the four records; the fence holds it.
// Cap 8: pass 1 folds 8 of Q's 10 records (the read arrives first, held to Q's end); the marker lands in pass 2, whose fold
// walks Q's rest, the span and the marker (6 units) and applies 2 of the 4 (fenced).
TEST(CheckedWritesTxnVisibilityRegression, HeldReadIsNeverServedInsideAnApplicationRegression) {
    Txv t(8);
    bool sent = false;
    int fencedPasses = 0;
    t.r.onEveryPass([&] {
        if (!sent) { sent = true; t.marker(31, true); }
        fencedPasses += t.fenced() ? 1 : 0;
    });
    t.enqueue(VerdictRig::batch(recs(40, 49)));
    t.enqueue(VerdictRig::batch(recs(1, 4), 31, 0, {}, /*txn=*/true), 31);
    const auto got = t.r.getViewRowK(kView, keyOf(4));
    EXPECT_EQ(got.code, RS::kOk) << "served inside the application: 2 of the transaction's 4 records visible";
    EXPECT_GE(fencedPasses, 1) << "the application spanned passes (the vacuity guard)";
    t.r.settle();
    EXPECT_EQ(t.visible(1, 4), 4);
    EXPECT_EQ(t.visible(40, 49), 10);
}
