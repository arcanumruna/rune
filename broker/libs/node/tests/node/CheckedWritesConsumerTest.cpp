// Checked writes Part 04 Task 7: the consumers of a verdict source - the bound at judgedThrough and the cut of rejected records.
#include "CheckedWritesRig.h"

#include <algorithm>

using namespace rune::node;
using namespace rune::node::cw;

namespace cr = rune::storage::control_record;

namespace {
constexpr int32_t kRej = 0x40000601;
constexpr uint64_t kObj = 77;

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }

struct TypedRig {
    VerdictRig r;
    TypedRig() {
        r.typed();
        r.installRejectView(kRej, kObj, tagRule(9));
        r.passes(50);   // the set change is logged first
    }
};

std::vector<int64_t> offsetsOf(const VerdictRig::Inbox& box) {
    std::vector<int64_t> o;
    for (const auto& d : box.records) o.push_back(d.offset);
    return o;
}

class CheckedWritesConsumer : public ::testing::TestWithParam<SubKind> {};
class CheckedWritesConsumerRegression : public ::testing::TestWithParam<SubKind> {};

const char* kindName(SubKind k) { return k == SubKind::Raw ? "Raw" : k == SubKind::Filtered ? "Filtered" : "PushRead"; }
std::string paramName(const ::testing::TestParamInfo<SubKind>& i) { return kindName(i.param); }
}  // namespace

INSTANTIATE_TEST_SUITE_P(Kinds, CheckedWritesConsumer, ::testing::Values(SubKind::Raw, SubKind::Filtered, SubKind::PushRead), paramName);
INSTANTIATE_TEST_SUITE_P(Kinds, CheckedWritesConsumerRegression, ::testing::Values(SubKind::Raw, SubKind::Filtered, SubKind::PushRead), paramName);

// Filtered subscriptions only see tag 1; the other kinds see every accepted record.
TEST_P(CheckedWritesConsumer, RejectedRecordIsNeverDelivered) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}, {2, 20, 9}, {3, 30, 1}}), 0);
    s.r.passes(300);
    const int64_t first = s.r.leo() - 4;   // the batch starts after the change record; its marker follows it
    const auto sub = s.r.subscribe(GetParam());
    const auto& box = s.r.collect(sub);
    std::vector<int64_t> got = offsetsOf(box);
    const std::vector<int64_t> want = {first, first + 2};
    EXPECT_EQ(got, want);
    ASSERT_FALSE(box.frames.empty());
    EXPECT_EQ(box.frames.front().base, first) << "the delivered batch keeps its base offset";
    EXPECT_EQ(box.frames.front().lastOffsetDelta, 2) << "and its last offset delta";
}

TEST_P(CheckedWritesConsumer, NothingBeyondJudgedThroughIsDelivered) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}}), 0);
    s.r.passes(300);
    VerdictSource* judge = s.r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    judge->holdJudgeForTest(true);
    ASSERT_EQ(s.r.produce({{2, 20, 1}, {3, 30, 9}}), 0);
    s.r.passes(300);
    const int64_t held = judge->judgedThrough();
    ASSERT_LT(held, s.r.leo());
    const auto sub = s.r.subscribe(GetParam());
    for (const auto& d : s.r.collect(sub).records) EXPECT_LT(d.offset, held) << "id " << d.id;
    judge->holdJudgeForTest(false);
    s.r.passes(300);
    const auto& box = s.r.collect(sub);
    EXPECT_TRUE(std::any_of(box.records.begin(), box.records.end(), [](const Delivered& d) { return d.id == 2; })) << "released: delivered";
    EXPECT_TRUE(std::none_of(box.records.begin(), box.records.end(), [](const Delivered& d) { return d.id == 3; }));
}

// A PushRead answered at the judge's cursor is not parked on the HW (the HW will not move again): its effective bound is -1.
TEST(CheckedWritesConsumerRegression, PushReadHeldByTheJudgeIsNotParkedOnTheHwRegression) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}}), 0);
    s.r.passes(300);
    VerdictSource* judge = s.r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    judge->holdJudgeForTest(true);
    ASSERT_EQ(s.r.produce({{2, 20, 1}}), 0);
    s.r.passes(100);
    const auto pushBound = [&](int64_t from) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead;
        op.subscriptionId = 1; op.sessionId = 4201; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.nextOffset = from; op.hwBound = s.r.leo();
        op.replica = s.r.replica.get(); op.logPtr = s.r.log.get();
        EXPECT_TRUE(s.r.worker->tryEnqueueOp(std::move(op)));
        int64_t bound = -2;
        s.r.now += 1'000'000;
        s.r.worker->doWork(s.r.now);
        s.r.worker->drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::PushReadResult) bound = r.pushEffectiveBound();
        }, [](const TxnReturn&) {});
        return bound;
    };
    EXPECT_EQ(pushBound(judge->judgedThrough()), -1) << "at the judge's cursor: unanswered, the gateway re-issues";
    judge->holdJudgeForTest(false);
    s.r.passes(300);
    EXPECT_GE(pushBound(0), s.r.replica->highWatermark()) << "the HW holds the bound again: parkable";
}

TEST_P(CheckedWritesConsumer, ReplayFromStartSkipsRejected) {
    TypedRig s;
    int rejected = 0;
    for (uint64_t b = 0; b < 20; ++b) {
        std::vector<Rec> recs = {{b * 10 + 1, 1, 1}, {b * 10 + 2, 2, 1}};
        if (b % 4 == 0) { recs.push_back({b * 10 + 3, 3, 9}); ++rejected; }
        ASSERT_EQ(s.r.produce(recs), 0);
    }
    s.r.passes(600);
    ASSERT_EQ(rejected, 5);
    const auto sub = s.r.subscribe(GetParam());
    const auto& box = s.r.collect(sub);
    EXPECT_EQ(box.records.size(), 40u);
    for (const auto& d : box.records) EXPECT_NE(d.id % 10, 3u) << "rejected record " << d.id;
}

TEST_P(CheckedWritesConsumerRegression, FullyRejectedBatchIsSkippedOnEveryPushPathRegression) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}}), 0);
    ASSERT_EQ(s.r.produce({{2, 20, 9}, {3, 30, 9}, {4, 40, 9}, {5, 50, 9}}), 0);
    ASSERT_EQ(s.r.produce({{6, 60, 1}}), 0);
    s.r.passes(300);
    const auto sub = s.r.subscribe(GetParam());
    const auto& box = s.r.collect(sub);
    std::vector<uint64_t> ids;
    for (const auto& d : box.records) ids.push_back(d.id);
    EXPECT_EQ(ids, (std::vector<uint64_t>{1, 6}));
    for (const auto& f : box.frames) EXPECT_GT(f.count, 0) << "no zero-record frame";
    EXPECT_EQ(box.frames.size(), 2u);
}

TEST_P(CheckedWritesConsumerRegression, FailsClosedBeforeTheVerdictSourceExistsRegression) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}, {2, 20, 9}}), 0);
    s.r.passes(300);
    s.r.restart(/*reinstall=*/false);   // no judge, no install: the set is only known from the log
    const auto sub = s.r.subscribe(GetParam());
    const int64_t w = s.r.worker->writeStateForTest(VerdictRig::kSrc, 0)->verdictKnownThrough();
    std::vector<uint64_t> ids;
    for (const auto& d : s.r.collect(sub).records) {
        EXPECT_LT(d.offset, w);
        ids.push_back(d.id);
    }
    EXPECT_EQ(ids, (std::vector<uint64_t>{1})) << "the accepted record below the known verdicts is delivered, the rejected one is not";
}

// A restarted leader has no timeline until the leadership scan is Ready: nothing is known, so nothing is delivered.
TEST_P(CheckedWritesConsumerRegression, RejectedNeverDeliveredWhileTheLeadershipScanRunsRegression) {
    TypedRig s;
    int accepted = 0;
    for (uint64_t b = 0; b < 40; ++b) {
        std::vector<Rec> recs = {{b * 10 + 1, 1, 1}, {b * 10 + 2, 2, 1}};
        accepted += 2;
        if (b % 4 == 0) recs.push_back({b * 10 + 3, 3, 9});
        ASSERT_EQ(s.r.produce(recs), 0);
    }
    s.r.passes(600);
    s.r.enableEgress();   // connecting takes passes: it must not run under the scan
    s.r.rcScanBytesPerPass = 256;   // the restarted worker scans a few batches per pass
    s.r.restart();
    ASSERT_NE(s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcReady) << "the scan is still running";
    const auto sub = s.r.subscribe(GetParam());
    if (GetParam() == SubKind::PushRead)   // one pass per call: the others drive passes until idle, which finishes the scan
        for (int i = 0; i < 4 && s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0) != Worker::kRcReady; ++i)
            EXPECT_TRUE(s.r.collect(sub).records.empty()) << "nothing is delivered before the scan is Ready";
    while (s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0) != Worker::kRcReady) s.r.pass();
    s.r.passes(300);
    const auto& box = s.r.collect(sub);
    for (const auto& d : box.records) EXPECT_NE(d.id % 10, 3u) << "rejected record " << d.id;
    EXPECT_EQ(static_cast<int>(box.records.size()), accepted) << "the accepted records are delivered once the scan is Ready";
}

// Part 04: dropping the topic frees its write state while the partition stays owned: the scan feed and the judge must go with it.
TEST(CheckedWritesConsumerRegression, DropMidScanClearsTheVerdictHooksRegression) {
    TypedRig s;
    for (uint64_t b = 0; b < 40; ++b) ASSERT_EQ(s.r.produce({{b * 10 + 1, 1, 1}, {b * 10 + 2, 2, 9}}), 0);
    s.r.passes(600);
    ASSERT_NE(s.r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr);
    s.r.worker->setRcScanBytesPerPass(256);
    s.r.demotePromote();
    ASSERT_EQ(s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcScanning) << "the scan is still running";
    ASSERT_NE(s.r.worker->rcVerdictHooksCtxForTest(VerdictRig::kSrc, 0), nullptr);
    ASSERT_NE(s.r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr);
    WorkerOp d;
    d.type = WorkerOp::Type::TopicDropped; d.topicId = VerdictRig::kSrc;
    s.r.send(std::move(d));
    EXPECT_EQ(s.r.worker->rcVerdictHooksCtxForTest(VerdictRig::kSrc, 0), nullptr) << "the scan no longer feeds the freed state";
    EXPECT_EQ(s.r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr) << "the judge no longer points at the freed state";
    for (int i = 0; i < VerdictRig::kMaxPass && s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0) != Worker::kRcReady; ++i) s.r.pass();
    EXPECT_EQ(s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcReady);
    EXPECT_EQ(s.r.worker->rcVerdictHooksCtxForTest(VerdictRig::kSrc, 0), nullptr);
}

TEST(CheckedWritesConsumerRegression, DropWhileUnbuiltClearsTheVerdictHooksRegression) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}, {2, 20, 9}}), 0);
    s.r.passes(300);
    s.r.demote();
    s.r.promoteWithoutPass();
    ASSERT_EQ(s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcUnbuilt);
    ASSERT_NE(s.r.worker->rcVerdictHooksCtxForTest(VerdictRig::kSrc, 0), nullptr);
    WorkerOp d;
    d.type = WorkerOp::Type::TopicDropped; d.topicId = VerdictRig::kSrc;
    ASSERT_TRUE(s.r.worker->tryEnqueueOp(std::move(d)));
    s.r.pass();
    EXPECT_EQ(s.r.worker->rcVerdictHooksCtxForTest(VerdictRig::kSrc, 0), nullptr);
    EXPECT_EQ(s.r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr);
    for (int i = 0; i < VerdictRig::kMaxPass && s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0) != Worker::kRcReady; ++i) s.r.pass();
    EXPECT_EQ(s.r.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcReady);
}

// Lease Task 18 finding: a typed partition (it has a write state) is served by its leader only, at any isolation: a follower
// holds no verdict state, so Worker::verdictBound_ delivers nothing there.  The owning worker publishes that bit
// (PartitionReplica::publishLeaderOnlyReads) as soon as the write state exists - no constraint set needed - and keeps it across a restart.
// RED: publish `sl.write && constraintSets.size() != 0` in Worker::publishVerdictCleanerBounds_.
TEST(CheckedWritesConsumerRegression, TypedPartitionIsLeaderOnlyRegression) {
    VerdictRig r;
    r.typed();
    r.passes(50);
    EXPECT_TRUE(r.replica->gatewayLeaderOnlyReads()) << "a typed partition is leader-only with no constraint set";
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    EXPECT_TRUE(r.replica->gatewayLeaderOnlyReads());
    r.restart(/*reinstall=*/false);
    r.passes(300);
    EXPECT_TRUE(r.replica->gatewayLeaderOnlyReads()) << "still leader-only after a restart";
}

// Checkpoint E ownership minor: a slot made for a replica that already carries a published leader-only bit starts from it, so a stale
// `true` on an untyped partition (an ownership change hands the worker a replica another worker published) is corrected on the first pass.
// RED: leave RcSlot::leaderOnlyReadsPublished at false when the slot is created (Worker::rebuildRcSlots_).
TEST(CheckedWritesConsumerRegression, StaleLeaderOnlyBitIsCorrectedWhenTheSlotIsRebuiltRegression) {
    VerdictRig r;
    ASSERT_EQ(r.produce({{1, 10, 1}}), 0);   // never typed: no write state
    r.passes(20);
    ASSERT_FALSE(r.replica->gatewayLeaderOnlyReads());
    r.replica->publishLeaderOnlyReads(true);   // a stale bit
    r.worker->setOwnedDirect(Worker::OwnedSet{r.replica.get()});   // an ownership apply rebuilds the slots
    r.passes(20);
    EXPECT_FALSE(r.replica->gatewayLeaderOnlyReads());
}

TEST(CheckedWritesConsumer, UntypedTopicPaysOneBranch) {
    VerdictRig r;
    ASSERT_EQ(r.produce({{1, 10, 1}, {2, 20, 9}}), 0);   // never typed: no write state
    r.passes(20);
    const auto sub = r.subscribe(SubKind::Raw);
    EXPECT_EQ(r.collect(sub).records.size(), 2u);
    const auto pr = r.subscribe(SubKind::PushRead);
    EXPECT_EQ(r.collect(pr).records.size(), 2u);
    EXPECT_EQ(r.worker->verdictCutsForTest(), 0u);
}

TEST(CheckedWritesConsumer, OversizedRebuildGrowsOnceAndIsCounted) {
    TypedRig s;
    std::vector<Rec> big;
    // ~600 KiB of records would need padding the fixed descriptor does not have; instead many records in one batch.
    for (uint64_t i = 1; i <= 30000; ++i) big.push_back({i, int64_t(i), i == 5 ? uint8_t(9) : uint8_t(1)});
    ASSERT_EQ(s.r.produce(big), 0);
    s.r.passes(600);
    const auto pr = s.r.subscribe(SubKind::PushRead);
    const auto& box = s.r.collect(pr);
    EXPECT_EQ(box.records.size(), 29999u);
    EXPECT_LE(s.r.worker->verdictScratchGrowthsForTest(), 1u);
    const uint64_t growths = s.r.worker->verdictScratchGrowthsForTest();
    ASSERT_EQ(s.r.produce(big), 0);
    s.r.passes(600);
    (void)s.r.collect(pr);
    EXPECT_EQ(s.r.worker->verdictScratchGrowthsForTest(), growths) << "a second identical batch does not grow again";
}

TEST(CheckedWritesConsumerRegression, CutOfABatchAboveTheSpanReserveGrowsOnceAndIsCountedRegression) {
    TypedRig s;
    std::vector<Rec> big;
    for (uint64_t i = 1; i <= 5000; ++i) big.push_back({i, int64_t(i), i == 5 ? uint8_t(9) : uint8_t(1)});
    ASSERT_EQ(s.r.produce(big), 0);
    s.r.passes(600);
    const auto pr = s.r.subscribe(SubKind::PushRead);
    EXPECT_EQ(s.r.collect(pr).records.size(), 4999u) << "the rejected record is dropped, the rest delivered";
    EXPECT_EQ(s.r.worker->verdictSpanGrowthsForTest(), 1u);
    ASSERT_EQ(s.r.produce(big), 0);
    s.r.passes(600);
    EXPECT_EQ(s.r.collect(pr).records.size(), 9998u);
    EXPECT_EQ(s.r.worker->verdictSpanGrowthsForTest(), 1u) << "a second batch of the same size does not grow again";
}

TEST_P(CheckedWritesConsumer, ControlRecordsSixteenToNineteenAreNeverPushed) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}}), 0);
    for (const int16_t type : {cr::kReservation, cr::kVerdictMarker, cr::kConstraintSetChange, cr::kTxnJudge}) {
        std::vector<uint8_t> b;
        if (type == cr::kVerdictMarker) {
            b.resize(cr::markerBatchBytes(0));
            (void)cr::buildVerdictMarker(b, cr::MarkerHeader{0, 0, 0, 0}, {});
        } else if (type == cr::kConstraintSetChange) {
            b.resize(cr::changeBatchBytes(0));
            (void)cr::buildConstraintSetChange(b, 1, {});
        } else {
            b.resize(cr::detail::batchBytes(6));
            std::size_t p = cr::detail::beginBatch(b.data(), type, 6);
            b[p++] = cr::kPayloadVersion;
            for (int i = 0; i < 5; ++i) b[p++] = 0;
            cr::detail::sealBatch(b.data(), p);
        }
        ASSERT_GE(s.r.replica->appendControlBatch(b, false), 0);
    }
    ASSERT_EQ(s.r.produce({{2, 20, 1}}), 0);
    s.r.passes(300);
    const auto sub = s.r.subscribe(GetParam());
    std::vector<uint64_t> ids;
    for (const auto& d : s.r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(ids, (std::vector<uint64_t>{1, 2}));
}

TEST(CheckedWritesConsumer, FilteredGroupNeverZeroCopiesABatchWithARejectedRecord) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}, {2, 20, 9}, {3, 30, 1}}), 0);
    s.r.passes(300);
    const auto sub = s.r.subscribe(SubKind::Filtered);
    const auto& box = s.r.collect(sub);
    ASSERT_EQ(box.records.size(), 2u);
    const filter::FilterGroup* g = s.r.worker->filterGroupOfSubForTest(sub);
    ASSERT_NE(g, nullptr);
    EXPECT_EQ(g->groupCounters().rebuilt, 1u);
    EXPECT_EQ(g->groupCounters().zeroCopy, 0u);
}

TEST(CheckedWritesConsumer, DeliveryWaitsDuringABuildWritesDoNot) {
    VerdictRig r;
    r.typed();
    r.installProjection(0x40000602);
    std::vector<Rec> first;
    for (uint64_t i = 1; i <= 500; ++i) first.push_back({i, int64_t(i), i == 7 ? uint8_t(9) : uint8_t(1)});
    ASSERT_EQ(r.produce(first), 0);
    r.passes(50);
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(3);   // the change record lands at A
    const int64_t A = r.leo() - 1;
    ASSERT_EQ(r.produce({{501, 1, 1}, {502, 2, 9}, {503, 3, 1}}), 0);
    const auto sub = r.subscribe(SubKind::PushRead);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    for (int i = 0; i < 4000 && judge->judgedThrough() <= A + 1; ++i) {
        r.pass();
        for (const auto& d : r.collect(sub).records) EXPECT_TRUE(d.offset <= A || judge->judgedThrough() > A + 1) << "id " << d.id;
    }
    r.passes(300);
    const auto& box = r.collect(sub);
    EXPECT_TRUE(std::any_of(box.records.begin(), box.records.end(), [](const Delivered& d) { return d.id == 7; })) << "pre-A records are delivered";
    EXPECT_TRUE(std::none_of(box.records.begin(), box.records.end(), [](const Delivered& d) { return d.id == 502; }));
    EXPECT_TRUE(std::any_of(box.records.begin(), box.records.end(), [](const Delivered& d) { return d.id == 503; }));
}

TEST(CheckedWritesConsumerRegression, LeaderWithoutTheInstallDeliversNothingAboveTheJudgeRegression) {
    TypedRig s;
    ASSERT_EQ(s.r.produce({{1, 10, 1}, {2, 20, 9}}), 0);
    s.r.passes(300);
    s.r.restart(/*reinstall=*/false);
    ASSERT_EQ(s.r.produce({{3, 30, 9}, {4, 40, 1}}), 0);
    s.r.passes(300);
    const auto sub = s.r.subscribe(SubKind::PushRead);
    const VerdictSource* judge = s.r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    ASSERT_TRUE(judge->waitingForInstall());
    for (const auto& d : s.r.collect(sub).records) EXPECT_LT(d.offset, judge->judgedThrough());
    s.r.installRejectView(kRej, kObj, tagRule(9));
    s.r.passes(500);
    std::vector<uint64_t> ids;
    for (const auto& d : s.r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(ids, (std::vector<uint64_t>{1, 4}));
}

// Checkpoint E review (adversarial): the gateway's leader-only criterion (PartitionReplica::gatewayLeaderOnlyReads, once = a non-empty
// constraint-set timeline) was narrower than the worker's hold (Worker::verdictBound_: ANY write state + RC not Ready -> -1).
// A follower that never led since boot has an empty timeline (only the leadership scan feeds it), so the gateway accepts a
// read_uncommitted subscription there, and the worker answers it with nothing, forever: a silent stall.
namespace {
int64_t pushBoundOn(VerdictRig& r, int64_t from) {
    WorkerOp op;
    op.type = WorkerOp::Type::PushRead;
    op.subscriptionId = 77; op.sessionId = 4277; op.topicId = VerdictRig::kSrc; op.partition = 0;
    op.nextOffset = from; op.hwBound = r.leo();
    op.replica = r.replica.get(); op.logPtr = r.log.get();
    EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
    int64_t bound = -2;
    r.now += 1'000'000;
    r.worker->doWork(r.now);
    r.worker->drainReturn([&](WorkerReturn w) {
        if (w.type == WorkerReturn::Type::PushReadResult) bound = w.pushEffectiveBound();
    }, [](const TxnReturn&) {});
    return bound;
}
}  // namespace

// A typed topic WITHOUT any constraint set, on a follower: the gateway says "followers serve it" and the worker serves nothing.
TEST(CheckedWritesConsumerAdversarialRegression, TypedFollowerServedByGatewayIsNeverDeliveredAdversarialRegression) {
    VerdictRig r;
    r.typed();
    r.passes(50);
    r.demote();
    r.replicate({VerdictRig::batch({{1, 10, 1}, {2, 20, 1}})});
    r.passes(50);
    ASSERT_GT(r.replica->highWatermark(), 0);
    const bool gatewayRefuses = r.replica->gatewayLeaderOnlyReads() || r.replica->leaseTopic();
    const int64_t bound = pushBoundOn(r, 0);
    EXPECT_TRUE(gatewayRefuses || bound > 0)
        << "SILENT STALL: the gateway accepts a read_uncommitted subscription on this follower (no verdict source published) "
           "but the worker's bound is " << bound;
}

// A verdict source (a replicated constraint-set change) on a follower that never led: the timeline is fed by the leadership
// scan only, so the bit stays false and the gateway accepts the subscription the worker never serves.
TEST(CheckedWritesConsumerAdversarialRegression, FollowerNeverLedVerdictSourceIsNotPublishedAdversarialRegression) {
    VerdictRig r;
    r.typed();
    r.passes(50);
    r.demote();
    r.replicate({VerdictRig::changeBatch(1, {{kObj, static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject), tagRule(9).fingerprint()}}),
                 VerdictRig::batch({{1, 10, 1}, {2, 20, 9}})});
    r.passes(50);
    ASSERT_GT(r.replica->highWatermark(), 0);
    const int64_t bound = pushBoundOn(r, 0);
    EXPECT_TRUE(r.replica->gatewayLeaderOnlyReads() || bound > 0)
        << "SILENT STALL: a follower holding a logged constraint set publishes no verdict source (gateway accepts) and serves bound " << bound;
}
