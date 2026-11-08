// Checked writes Part 11 Task 8 (P11-0, P11-9): on a real Worker, a committed transaction becomes visible at its commit marker,
// whole: a read never sees part of it.  An application the pass cap splits (a transaction at its marker, an atomic batch) holds the
// view's reads until it is whole; an open transaction alone never holds them.
#include "CheckedWritesRig.h"

#include <array>
#include <filesystem>
#include <random>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

using namespace rune::node;
using namespace rune::node::cw;
using RS = rune::protocol::client::ViewRowResponse;

namespace {
constexpr int32_t kView = 0x40000602;

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
        r.setViewPassCap(cap);
        r.installProjection(kView);
        EXPECT_EQ(r.produce({{99, 99, 1}}), 0);
        for (int i = 0; i < 200 && !r.hasRow(kView, 99); ++i) r.pass();
        EXPECT_TRUE(r.hasRow(kView, 99));
    }
    [[nodiscard]] int visible(uint64_t from, uint64_t to) const {
        int n = 0;
        for (uint64_t id = from; id <= to; ++id) n += r.hasRow(kView, id) ? 1 : 0;
        return n;
    }
    /// Producer `pid`'s transactional batch of `ids`, appended in one pass.
    void txn(int64_t pid, const std::vector<Rec>& ids) {
        const int64_t leo = r.leo();
        enqueue(VerdictRig::batch(ids, pid, 0, {}, /*txn=*/true), pid);
        r.pass();
        ASSERT_EQ(r.leo(), leo + static_cast<int64_t>(ids.size()));
    }
    /// A Produce op of `bytes` on the op ring, without a pass.
    void enqueue(std::vector<uint8_t> bytes, int64_t pid = -1, int32_t seq = 0) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = VerdictRig::kSrc; op.partition = 0; op.replica = r.replica.get(); op.logPtr = r.log.get();
        op.sessionId = 1; op.correlationId = ++r.corr; op.durabilityTier = 1;
        op.producerId = pid; op.producerEpoch = 0; op.baseSequence = pid >= 0 ? seq : -1;
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
    const int64_t leo = t.r.leo();
    t.marker(41, true);
    t.r.passes(5);
    ASSERT_EQ(t.r.leo(), leo + 1) << "A's marker is in the log";
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

namespace {
// A persisted projection with pass cap 2 over plain records interleaved with a two-batch transaction of producer 31: the fold
// stands inside the span (first, M] for several passes, with dirty rows of the plain records.
struct Interleaved {
    static constexpr std::array<uint64_t, 13> kIds{1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15};
    Txv t{2};
    int64_t first = -1, marker = -1;
    Interleaved() {
        t.r.dropView(kView);
        t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
        const int64_t base = t.r.leo();
        t.enqueue(VerdictRig::batch(recs(1, 2)));
        t.enqueue(VerdictRig::batch(recs(10, 12), 31, 0, {}, /*txn=*/true), 31, 0);
        t.enqueue(VerdictRig::batch(recs(3, 4)));
        t.enqueue(VerdictRig::batch(recs(13, 15), 31, 3, {}, /*txn=*/true), 31, 3);
        t.enqueue(VerdictRig::batch(recs(5, 5)));
        t.marker(31, true);
        t.enqueue(VerdictRig::batch(recs(6, 7)));
        first = base + 2;
        marker = base + 11;
    }
    [[nodiscard]] const ViewInstance* inst() const { return t.r.vs->instanceForTest(kView + 1, 0); }
    [[nodiscard]] bool folded() const { return inst() != nullptr && inst()->foldCursor >= marker + 3 && inst()->readiness == ViewReadiness::Ready; }
    [[nodiscard]] std::vector<std::optional<std::pair<int64_t, uint16_t>>> image() const {
        std::vector<std::optional<std::pair<int64_t, uint16_t>>> out;
        for (const uint64_t id : kIds) out.push_back(t.r.idRow(kView + 1, id));
        return out;
    }
};
}  // namespace

// REGRESSION (Task 8 review 1, m2): vetoedCheckpointPushes counts pushes refused, not passes that stood in a span with nothing to
// push.  The walk through a span of T's records alone dirties nothing; the fenced application that follows does.
TEST(CheckedWritesTxnVisibilityRegression, VetoCountsOnlyRefusedPushesRegression) {
    Txv t(2);
    t.r.dropView(kView);
    t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
    const int64_t first = t.r.leo();
    t.enqueue(VerdictRig::batch(recs(10, 15), 31, 0, {}, /*txn=*/true), 31, 0);
    t.marker(31, true);
    const auto inst = [&] { return t.r.vs->instanceForTest(kView + 1, 0); };
    for (int i = 0; i < 100 && (inst() == nullptr || inst()->foldCursor < first + 6); ++i) t.r.pass();
    ASSERT_NE(inst(), nullptr);
    ASSERT_EQ(inst()->foldCursor, first + 6) << "at the marker, nothing of T applied";
    EXPECT_EQ(inst()->counters.vetoedCheckpointPushes, 0u) << "the walk through T's own batches dirtied nothing";
    for (int i = 0; i < 100 && inst()->foldCursor <= first + 6; ++i) t.r.pass();
    EXPECT_GT(inst()->counters.vetoedCheckpointPushes, 0u) << "the fenced application had rows to push";
}

// REGRESSION (Task 8 review 1, C1; P11-9, D4): on path A a persisted ordinary view whose span (T's first record to its marker) covers
// more than 2C distinct-key plain records folds them with every checkpoint vetoed; the delta at the marker then outgrows one ring
// message.  It goes out as part frames, so the view keeps folding and checkpointing without a restart.
TEST(CheckedWritesTxnVisibilityRegression, LongSpanKeepsCheckpointingWithoutARestartRegression) {
    constexpr int64_t kCap = 2000;
    Txv t(kCap);
    t.r.dropView(kView);
    t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
    t.txn(31, recs(10, 10));
    for (uint64_t from = 100; from < 100 + 5 * kCap; from += 500) ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(from, from + 499))), 0);
    const int64_t m = t.r.leo();
    t.marker(31, true);
    t.r.settle();
    for (int i = 0; i < 50; ++i) t.r.pass();
    EXPECT_TRUE(t.r.idRow(kView + 1, 10).has_value());
    EXPECT_TRUE(t.r.idRow(kView + 1, 100 + 5 * kCap - 1).has_value());
    EXPECT_GT(t.r.agent->lastDurableOffset(kView + 1, 0), m) << "the delta past one message is durable";
    ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(20000, 20001))), 0);
    const int64_t after = t.r.leo();
    t.r.settle();
    for (int i = 0; i < 50; ++i) t.r.pass();
    EXPECT_TRUE(t.r.idRow(kView + 1, 20001).has_value()) << "the fold goes on past the marker";
    EXPECT_GE(t.r.agent->lastDurableOffset(kView + 1, 0), after - 1) << "and checkpoints go on";
    EXPECT_EQ(t.r.pass(), 0) << "and the worker idles: no held delta retried every pass";
}

// REGRESSION (Task 8 review 2, I-C): the window's dirty growth is view memory while checkpoints are vetoed, and it is given back
// with the push that ends the window, as is the buffer of a delta past one message.  A persisted view with nothing in flight
// carries no charge past its tables, before the span and after it.
TEST(CheckedWritesTxnVisibilityRegression, LongSpanChargeComesBackDownAfterItsPushRegression) {
    constexpr int64_t kCap = 2000;
    Txv t(kCap);
    t.r.dropView(kView);
    t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
    const auto tables = [](const ViewInstance& i) { return i.table.memoryBytes() + (i.owners ? i.owners->memoryBytes() : 0); };
    ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(1, 50))), 0);
    t.r.settle();
    const ViewInstance* inst = t.r.vs->instanceForTest(kView + 1, 0);
    ASSERT_NE(inst, nullptr);
    ASSERT_NE(inst->nsSlot, kNoNsSlot) << "the view is charged to a namespace";
    EXPECT_EQ(inst->memCharged, tables(*inst)) << "a pushed delta leaves no standing charge";
    const int64_t first = t.r.leo();
    t.txn(31, recs(10, 10));
    for (uint64_t from = 100; from < 100 + 5 * kCap; from += 500) ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(from, from + 499))), 0);
    const int64_t m = t.r.leo();
    t.marker(31, true);
    for (int i = 0; i < 50 && inst->foldCursor < first + 9000; ++i) t.r.pass();
    ASSERT_GE(inst->foldCursor, first + 9000);
    ASSERT_LE(inst->foldCursor, m) << "the window is still open";
    EXPECT_GT(inst->memCharged, tables(*inst)) << "the window's dirty growth is charged";
    t.r.settle();
    ASSERT_GT(t.r.agent->lastDurableOffset(kView + 1, 0), m);
    EXPECT_FALSE(inst->pendingValid);
    EXPECT_EQ(inst->pendingDelta.capacity(), 0u) << "the part run's buffer is given back";
    EXPECT_EQ(inst->memCharged, tables(*inst)) << "the charge comes back down with the push";
}

// Task 8 review 2, m-f: a part run the delta ring cannot take whole stops where the ring filled and resumes there on a later pass
// (pendingSent); the agent reassembles one delta, made durable past the marker.
TEST(CheckedWritesTxnVisibilityRegression, PartRunResumesWhereTheRingFilledRegression) {
    constexpr int64_t kCap = 2000;
    Txv t(kCap);
    t.r.dropView(kView);
    t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
    ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(1, 50))), 0);
    t.r.settle();
    const ViewInstance* inst = t.r.vs->instanceForTest(kView + 1, 0);
    ASSERT_NE(inst, nullptr);
    t.txn(31, recs(10, 10));
    for (uint64_t from = 100; from < 100 + 20 * kCap; from += 500) ASSERT_EQ(t.r.produceBatch(VerdictRig::batch(recs(from, from + 499))), 0);
    const int64_t m = t.r.leo();
    t.marker(31, true);
    t.r.pauseAgent = true;   // nothing drains the ring
    for (int i = 0; i < 100 && inst->pendingSent == 0; ++i) t.r.pass();
    ASSERT_GT(inst->pendingSent, 0u) << "a part run stopped on a full ring";
    const std::size_t sent = inst->pendingSent;
    t.r.pass();
    EXPECT_EQ(inst->pendingSent, sent) << "the full ring holds the run where it stands";
    EXPECT_TRUE(inst->pendingValid);
    t.r.pauseAgent = false;
    t.r.settle();
    EXPECT_EQ(inst->pendingSent, 0u);
    EXPECT_FALSE(inst->pendingValid);
    EXPECT_GT(t.r.agent->lastDurableOffset(kView + 1, 0), m) << "the resumed run is one durable delta";
    EXPECT_TRUE(t.r.idRow(kView + 1, 10).has_value());
    EXPECT_TRUE(t.r.idRow(kView + 1, 100 + 20 * kCap - 1).has_value());
}

// REGRESSION (Task 8 review 1, D-A): past 64 spans open at once (67 transactions, then their markers) the excess apply at their own
// offsets; the instance warns once, not per batch.
TEST(CheckedWritesTxnVisibilityRegression, FullSpanTableWarnsOnceRegression) {
    const auto sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
    const auto prev = spdlog::default_logger();
    spdlog::set_default_logger(std::make_shared<spdlog::logger>("capture", sink));
    {
        Txv t(0);
        t.r.dropView(kView);
        t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
        for (int64_t pid = 100; pid < 167; ++pid) t.txn(pid, recs(static_cast<uint64_t>(pid), static_cast<uint64_t>(pid)));
        for (int64_t pid = 100; pid < 167; ++pid) t.marker(pid, true);
        t.r.settle();
        for (uint64_t id = 100; id < 167; ++id) EXPECT_TRUE(t.r.idRow(kView + 1, id).has_value()) << id;
        EXPECT_EQ(t.r.vs->foldCounters(kView + 1, 0)->txnSpanTableFull, 3u);
    }
    spdlog::set_default_logger(prev);
    int warned = 0;
    for (const auto& l : sink->last_formatted()) warned += l.find("transactions open at once") != std::string::npos ? 1 : 0;
    EXPECT_EQ(warned, 1);
}

// REGRESSION (Task 8 review 1, m4): a discard taken while a marker application is in progress clears the fence with the cursor.  A
// kept applyingThrough would resume the refold's application above T's first records and hold its reads from the first pass.
TEST(CheckedWritesTxnVisibilityRegression, DiscardMidApplicationClearsTheFenceRegression) {
    Txv t(2);
    t.r.dropView(kView);
    t.r.installProjection(kView + 1, "pproj", /*persisted=*/true);
    t.enqueue(VerdictRig::batch(recs(10, 15), 31, 0, {}, /*txn=*/true), 31, 0);
    t.marker(31, true);
    const auto inst = [&] { return t.r.vs->instanceForTest(kView + 1, 0); };
    for (int i = 0; i < 100 && (inst() == nullptr || inst()->spanFence.applyingThrough < 0); ++i) t.r.pass();
    ASSERT_TRUE(inst() != nullptr && inst()->spanFence.fenced());
    ASSERT_TRUE(t.r.vs->beginDiscardForTest(kView + 1, 0, ViewDiscardReason::AheadOfLog));
    EXPECT_FALSE(inst()->spanFence.fenced());
    EXPECT_EQ(inst()->spanFence.count, 0);
    for (int i = 0; i < 200 && !(inst()->foldCursor > t.r.leo() - 1 && inst()->readiness == ViewReadiness::Ready); ++i) t.r.pass();
    for (uint64_t id = 10; id <= 15; ++id) EXPECT_TRUE(t.r.idRow(kView + 1, id).has_value()) << id;
}

// REGRESSION (Task 8 review 2, I-B): retention deletes the first batch of an open span while the fold stands above the log start.
// The marker could never be applied (its read-back is gone), so the view is discarded (BelowLogStart) and refolds from the log start,
// instead of waiting at the marker for good.
TEST(CheckedWritesTxnVisibilityRegression, RetentionBelowAnOpenSpanDiscardsAndRefoldsRegression) {
    VerdictRig r(false, /*rollBytes=*/1);   // one batch per segment
    r.typed();
    r.setViewPassCap(2);
    r.installProjection(kView, "pproj", /*persisted=*/true);
    ASSERT_EQ(r.produce({{1, 1, 1}}), 0);
    r.settle(10);
    const auto enqueue = [&r](WorkerOp op) { ASSERT_TRUE(r.worker->tryEnqueueOp(std::move(op))); };
    WorkerOp t;
    t.type = WorkerOp::Type::Produce; t.topicId = VerdictRig::kSrc; t.partition = 0; t.replica = r.replica.get(); t.logPtr = r.log.get();
    t.sessionId = 1; t.correlationId = ++r.corr; t.durabilityTier = 1;
    t.producerId = 31; t.producerEpoch = 0; t.baseSequence = 0;
    t.recordBatchBytes = VerdictRig::batch(recs(10, 12), 31, 0, {}, /*txn=*/true);
    enqueue(std::move(t));
    ASSERT_EQ(r.produceBatch(VerdictRig::batch(recs(20, 20))), 0);
    ASSERT_EQ(r.produceBatch(VerdictRig::batch(recs(21, 21))), 0);
    const int64_t m = r.leo();
    WorkerOp c;
    c.type = WorkerOp::Type::AppendControlRecord; c.topicId = VerdictRig::kSrc; c.partition = 0; c.replica = r.replica.get();
    c.logPtr = r.log.get(); c.correlationId = 9031; c.producerId = 31;
    c.targetReplicaId = r.replica->registryId(); c.incarnation = r.replica->incarnation();
    c.recordBatchBytes = rune::transaction::ControlMarker::build(31, 0, rune::transaction::ControlType::Commit, 0);
    enqueue(std::move(c));
    const ViewInstance* inst = nullptr;
    for (int i = 0; i < 40 && !(inst != nullptr && inst->foldCursor == m && inst->spanFence.count == 1); ++i) {
        r.pass();
        inst = r.vs->instanceForTest(kView, 0);
    }
    ASSERT_TRUE(inst != nullptr && inst->foldCursor == m && inst->spanFence.count == 1) << "the fold stands at T's marker, its span open";
    ASSERT_GT(r.log->deleteOldSegments(0), 0);
    ASSERT_EQ(r.log->logStartOffset(), m) << "retention took T's batches; the fold stands at the log start";
    ASSERT_EQ(r.produceBatch(VerdictRig::batch(recs(30, 30))), 0);
    r.settle(40);
    inst = r.vs->instanceForTest(kView, 0);
    EXPECT_EQ(r.vs->foldCounters(kView, 0)->checkpointDiscards, 1u) << "exactly one BelowLogStart discard";
    EXPECT_TRUE(r.idRow(kView, 30).has_value()) << "the refold goes on past the marker";
    EXPECT_FALSE(r.idRow(kView, 20).has_value()) << "the refold starts at the log start";
    EXPECT_EQ(inst->spanFence.count, 0);
    EXPECT_FALSE(inst->spanFence.fenced());
    EXPECT_EQ(inst->readiness, ViewReadiness::Ready);
}

// REGRESSION (P11-0, contract 13): a checkpoint at O with first < O <= M would hold none of the span's records and a refold from it
// would never see the ones below O.  The push is vetoed while the cursor stands in the span; a crash at every pass boundary then
// recovers the undisturbed tables.
TEST(CheckedWritesTxnVisibilityRegression, CheckpointNeverInsideAnOpenSpanRegression) {
    std::vector<std::optional<std::pair<int64_t, uint16_t>>> want;
    int passes = 0;
    {
        Interleaved w;
        std::vector<int64_t> pushed;
        for (; passes < 100 && !w.folded(); ++passes) {
            w.t.r.pass();
            if (w.inst() != nullptr && (pushed.empty() || pushed.back() != w.inst()->lastEncodedO)) pushed.push_back(w.inst()->lastEncodedO);
        }
        ASSERT_TRUE(w.folded());
        ASSERT_EQ(w.t.r.leo(), w.marker + 3) << "the layout the test assumes";
        for (const int64_t o : pushed) EXPECT_FALSE(w.first < o && o <= w.marker) << "a checkpoint at " << o << " inside (" << w.first << ", " << w.marker << "]";
        EXPECT_GT(w.inst()->counters.vetoedCheckpointPushes, 0u) << "the fold stood inside the span with dirty rows (the vacuity guard)";
        want = w.image();
        for (const auto& row : want) EXPECT_TRUE(row.has_value());
    }
    for (int k = 1; k <= passes; ++k) {   // pass 1 appends the writes (a crash before it loses them, unacked)
        Interleaved c;
        c.t.r.passes(k);
        c.t.r.restart();
        for (int i = 0; i < 200 && !c.folded(); ++i) c.t.r.pass();
        EXPECT_TRUE(c.folded()) << "crash after pass " << k;
        EXPECT_EQ(c.image(), want) << "crash after pass " << k;
    }
}

namespace {
// R11b: a source {id, g, v} under count(*), last(v), lastNonNull(v) by g, once as a verdict member with check (lv >= 0) and once
// as an ordinary view; each again as a copy installed after the writes.
constexpr const char* kR11Topic = "topic r11 { id uint64, g uint64, v int64 } partitions 1 key id;";
constexpr int32_t kChk = 0x40000701, kOrd = 0x40000702, kChkCopy = 0x40000703, kOrdCopy = 0x40000704;

rune::view::CompileResult r11View(int32_t viewTopicId, bool checked) {
    const std::string stmt = std::string("create view r11v key g as select count(*) as n, last(v) as lv, last(v) as lnn from r11 group by g") +
                             (checked ? " constraint nonneg check (lv >= 0);" : ";");
    const rune::ql::TopicDef topic = VerdictRig::parseTopic(kR11Topic);
    auto parsed = rune::ql::parse(stmt);
    EXPECT_TRUE(parsed.has_value());
    auto authored = rune::ql::applyViewPlan(std::get<rune::ql::CreateViewDef>(*parsed), rune::ql::viewSourceOf(topic));
    EXPECT_TRUE(authored.has_value());
    authored->sources[0].topicId = VerdictRig::kSrc;
    for (auto& f : authored->output)
        if (f.name == "lnn") f.rule = rune::view::MergeRule::LastNonNull;   // QL has no lastNonNull: the plan says it
    const rune::codec::Descriptor src = rune::ql::toDescriptor(topic);
    const rune::codec::Descriptor* srcs[] = {&src};
    auto compiled = rune::view::compileView(*authored, srcs, viewTopicId);
    EXPECT_TRUE(compiled.has_value());
    compiled->plan.sizing.chunkBytes = 1u << 16;
    return std::move(*compiled);
}

struct R11Rec { uint64_t id; uint64_t g; std::optional<int64_t> v; };

/// A batch of {id, g, v} records; a null v is a value block that ends before v.
std::vector<uint8_t> r11Batch(const std::vector<R11Rec>& recs, int64_t pid, int32_t seq, bool txn) {
    static const rune::codec::Descriptor d = rune::ql::toDescriptor(VerdictRig::parseTopic(kR11Topic));
    rune::protocol::RecordBatchBuilder b(0, 1000);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
    b.setTransactional(txn);
    const auto put = [&](std::vector<uint8_t>& val, const char* field, uint64_t x) {
        const auto* f = d.find(field);
        for (uint32_t i = 0; i < rune::codec::fieldWidth(*f); ++i) val[4 + f->offset + i] = static_cast<uint8_t>(x >> (8 * i));
    };
    for (const R11Rec& r : recs) {
        std::string key(8, '\0');
        for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<char>(r.id >> (8 * i));
        const uint16_t block = r.v ? d.blockLength : d.find("v")->offset;
        std::vector<uint8_t> val(4 + block, 0);
        val[0] = static_cast<uint8_t>(block); val[1] = static_cast<uint8_t>(block >> 8);
        val[2] = static_cast<uint8_t>(d.version); val[3] = static_cast<uint8_t>(d.version >> 8);
        put(val, "id", r.id);
        put(val, "g", r.g);
        if (r.v) put(val, "v", static_cast<uint64_t>(*r.v));
        b.append(key, {reinterpret_cast<const char*>(val.data()), val.size()}, 1000, 0);
    }
    return b.build();
}

/// What D5 compares: each view's rows by group (logical, no row or group ids) and the source's rejected verdicts.
struct R11Image {
    std::vector<std::vector<uint8_t>> rows;
    std::vector<std::tuple<int64_t, uint16_t, uint32_t>> rejected;
    bool operator==(const R11Image&) const = default;
    friend void PrintTo(const R11Image& im, std::ostream* os) {
        for (const auto& row : im.rows) {
            *os << "\n  row";
            for (const uint8_t b : row) *os << ' ' << int(b);
        }
        for (const auto& [o, code, cid] : im.rejected) *os << "\n  rejected " << o << " code " << code << " constraint " << cid;
    }
};

struct R11 {
    VerdictRig r;
    int64_t writesEnd = 0;
    explicit R11(uint32_t seed) {
        r.createQlTopic(kR11Topic, false);
        r.setViewPassCap(2);
        r.installCompiledView(r11View(kChk, true), 0x5701, /*persisted=*/true);
        r.installCompiledView(r11View(kOrd, false), 0x5702, /*persisted=*/true);
        r.passes(20);
        write(seed);
    }
    /// 3 producers' transactions (1-8 records, commit or abort) interleaved with plain writes on the same 6 keys and 3 groups.
    void write(uint32_t seed) {
        std::mt19937 rng(seed);
        const auto pick = [&](uint32_t n) { return static_cast<uint32_t>(rng() % n); };
        const auto rec = [&] { return R11Rec{1 + pick(6), 1 + pick(3), pick(6) == 0 ? std::nullopt : std::optional<int64_t>(int64_t(pick(8)) - 1)}; };
        std::array<int, 3> open{};   // records written in each producer's open transaction
        std::array<int32_t, 3> seq{};
        for (int step = 0; step < 14; ++step) {
            const uint32_t p = pick(3), what = pick(4);
            const int64_t pid = 41 + p;
            if (what == 0) {
                std::vector<R11Rec> recs{rec()};
                if (pick(2) == 0) recs.push_back(rec());
                // The produce path refuses a value block cut before v: a previous leader wrote it (an older schema version).
                if (std::all_of(recs.begin(), recs.end(), [](const R11Rec& x) { return x.v.has_value(); })) ASSERT_EQ(r.produceBatch(r11Batch(recs, -1, -1, false)), 0);
                else (void)r.appendTxnReplicated(r11Batch(recs, -1, -1, false));
            } else if (what == 3 && open[p] > 0) {
                r.marker(pid, pick(3) != 0);
                open[p] = 0;
            } else if (open[p] < 8) {
                std::vector<R11Rec> recs{rec()};
                if (open[p] < 7 && pick(2) == 0) recs.push_back(rec());
                (void)r.appendTxnReplicated(r11Batch(recs, pid, seq[p], true));
                seq[p] += static_cast<int32_t>(recs.size());
                open[p] += static_cast<int>(recs.size());
            }
        }
        for (uint32_t p = 0; p < 3; ++p)
            if (open[p] > 0) r.marker(41 + p, pick(3) != 0);
        // A judged record last, in its own group: a stretch with nothing judged (an aborted transaction) gets its marker with it.
        ASSERT_EQ(r.produceBatch(r11Batch({{99, 4, 0}}, -1, -1, false)), 0);
        writesEnd = r.leo();
    }
    /// Every record judged (and, live, its markers committed: a later change record is marked only with the next record), and
    /// every instance of `views` folded past the writes' last marker and not fenced.
    void settle(std::initializer_list<int32_t> views, bool live = false) {
        ASSERT_TRUE(live ? r.driveUntilJudged() : r.driveUntilJudgeReaches());
        const auto done = [&] {
            for (const int32_t v : views)
                if (const ViewInstance* inst = r.vs->instanceForTest(v, 0); inst == nullptr || inst->foldCursor < writesEnd || inst->spanFence.fenced()) return false;
            return true;
        };
        for (int i = 0; i < 2000 && !done(); ++i) r.pass();
        ASSERT_TRUE(done());
    }
    [[nodiscard]] R11Image image(int32_t chk, int32_t ord) const {
        R11Image out;
        for (const int32_t v : {chk, ord}) {
            const ViewInstance* inst = r.vs->instanceForTest(v, 0);
            out.rows.push_back({static_cast<uint8_t>(inst->table.size())});
            for (uint64_t g = 1; g <= 3; ++g) {
                const uint8_t* row = inst->table.get(g);
                out.rows.push_back(row == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(row, row + inst->table.rowLen()));
            }
        }
        const auto& rej = r.writeState()->rejected;
        for (int64_t o = rej.nextRejectedAtOrAfter(0); o != INT64_MAX; o = rej.nextRejectedAtOrAfter(o + 1)) {
            const auto* e = rej.find(o);
            out.rejected.emplace_back(o, e->code, e->constraintId);
        }
        return out;
    }
    [[nodiscard]] uint64_t mismatches() const { return r.writeState()->rejected.mismatches(); }
};
}  // namespace

// REGRESSION (R11b, D5): with transactions of 3 producers interleaved with plain writes on the same keys and groups, the live fold, a
// refold from 0 after a restart without checkpoints, copies installed after the writes, and a failover's leadership scan with
// activation replay agree on every table and every rejected verdict.  One seed per instance, 1-100.
class CheckedWritesTxnDeterminismRegression : public ::testing::TestWithParam<uint32_t> {};

TEST_P(CheckedWritesTxnDeterminismRegression, LiveEqualsRefoldEqualsRebuildUnderInterleavedTxnsRegression) {
    R11 w(GetParam());
    w.settle({kChk, kOrd}, /*live=*/true);
    const R11Image live = w.image(kChk, kOrd);
    EXPECT_EQ(w.mismatches(), 0u);

    w.r.installCompiledView(r11View(kChkCopy, true), 0x5703, /*persisted=*/true);
    w.r.installCompiledView(r11View(kOrdCopy, false), 0x5704, /*persisted=*/true);
    w.settle({kChk, kOrd, kChkCopy, kOrdCopy});
    EXPECT_EQ(w.image(kChkCopy, kOrdCopy), live) << "the copies installed after the writes";
    EXPECT_EQ(w.image(kChk, kOrd), live);

    w.r.agent.reset();
    for (const int32_t v : {kChk, kOrd, kChkCopy, kOrdCopy}) std::filesystem::remove(viewCheckpointFilePath(w.r.snapDir(), v, 0));
    w.r.agent = std::make_unique<ViewSnapshotIoAgent>(w.r.channel, w.r.snapDir(), /*fsyncMs=*/0, w.r.backupPolicy);
    w.r.restart();
    w.settle({kChk, kOrd, kChkCopy, kOrdCopy});
    EXPECT_EQ(w.image(kChk, kOrd), live) << "the refold from 0";
    EXPECT_EQ(w.image(kChkCopy, kOrdCopy), live) << "the copies' refold from 0";
    EXPECT_EQ(w.mismatches(), 0u);

    w.r.failover();
    w.settle({kChk, kOrd, kChkCopy, kOrdCopy});
    EXPECT_EQ(w.image(kChk, kOrd), live) << "the failover";
    EXPECT_EQ(w.image(kChkCopy, kOrdCopy), live) << "the copies after the failover";
    EXPECT_EQ(w.mismatches(), 0u);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesTxnDeterminismRegression, ::testing::Range(1u, 101u));
