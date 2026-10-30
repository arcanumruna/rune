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
    for (const auto& d : s.r.collect(sub).records) EXPECT_LT(d.offset, w);
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
