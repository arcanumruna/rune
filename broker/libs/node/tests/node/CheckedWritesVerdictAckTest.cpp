// Checked writes Part 04 Task 9: View acks carry the per-record verdicts of a verdict source.
#include "CheckedWritesRig.h"

#include <rune/view/Verdict.h>

using namespace rune::node;
using namespace rune::node::cw;

namespace {
constexpr int32_t kRej = 0x40000601;
constexpr uint64_t kObj = 7;
constexpr uint8_t kAcksView = rune::protocol::client::kAcksWireView;
constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
constexpr int64_t kMs = 1'000'000;
const uint32_t kCid = rune::view::constraintIdOf(kObj, 1);
constexpr uint16_t kCode = 55;   // ConstraintViolation (RB1)

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }

struct AckRigFixture : ::testing::Test {
    VerdictRig r;
    void SetUp() override {
        r.typed();
        r.installRejectView(kRej, kObj, tagRule(9));
        r.enableAcks();
        r.passes(60);   // the set change is logged before the records it is to judge
    }
};
using V = std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>;
}  // namespace

using CheckedWritesVerdictAck = AckRigFixture;
using CheckedWritesVerdictAckRegression = AckRigFixture;

TEST_F(CheckedWritesVerdictAck, TrailerCarriesExactlyTheRejections) {
    const int64_t c = r.produceFrame({{1, 1, 1}, {2, 2, 9}, {3, 3, 1}, {4, 4, 9}}, kAcksView);
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_TRUE(a->hasVerdicts);
    EXPECT_EQ(a->frameLen, 54u + 2 + 16);
    EXPECT_EQ(a->verdicts, (V{{1, kCode, kCid}, {3, kCode, kCid}}));
}

TEST_F(CheckedWritesVerdictAck, AllAcceptedIsAnEmptyTrailer) {
    const int64_t c = r.produceFrame({{1, 1, 1}, {2, 2, 1}}, kAcksView);
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_TRUE(a->hasVerdicts);
    EXPECT_TRUE(a->verdicts.empty());
    EXPECT_EQ(a->frameLen, 56u);
}

TEST_F(CheckedWritesVerdictAck, PlainAllOnAVerdictSourceHasNoTrailer) {
    const int64_t c = r.produceFrame({{1, 1, 1}, {2, 2, 9}}, rune::protocol::client::kAcksWireAll);
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_FALSE(a->hasVerdicts);
    EXPECT_EQ(a->frameLen, 54u);
}

TEST_F(CheckedWritesVerdictAck, AViewAckWaitsForTheJudge) {
    // The ack is not sent before the judge passed the batch, whatever the HW does.
    const int64_t c = r.produceFrame({{1, 1, 9}}, kAcksView);
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_GT(judge->judgedThrough(), a->baseOffset);
    EXPECT_EQ(a->verdicts, (V{{0, kCode, kCid}}));
}

TEST(CheckedWritesVerdictAckRigRegression, WaitsForTheJudgeNotTheMarker) {
    VerdictRig r(/*follower=*/true);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.enableAcks();
    r.passes(60);
    const int64_t c = r.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView);
    r.settle(60);
    EXPECT_FALSE(r.acked(c)) << "the HW is held: no ack";
    r.followerFetch();
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->verdicts, (V{{1, kCode, kCid}}));
}

TEST(CheckedWritesVerdictAckOld, OldSessionNeverSeesATrailer) {
    VerdictRig o;
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks(/*clientVersion=*/11);
    o.passes(60);
    const int64_t c = o.produceFrame({{1, 1, 1}}, rune::protocol::client::kAcksWireAll);
    const auto a = o.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->frameLen, 46u);
    EXPECT_FALSE(a->hasVerdicts);
}

TEST_F(CheckedWritesVerdictAckRegression, FourThousandRejectionsAckIsOfferedRegression) {
    std::vector<Rec> recs;
    for (uint64_t i = 0; i < 4096; ++i) recs.push_back({i + 1, 1, 9});
    const uint64_t claimsBefore = r.sinkForTest()->claimFramesForTest();
    const int64_t c = r.produceFrame(recs, kAcksView);
    const auto a = r.ack(c, 3000);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->frameLen, 32'824u);
    ASSERT_EQ(a->verdicts.size(), 4096u);
    for (uint16_t i = 0; i < 4096; ++i) EXPECT_EQ(a->verdicts[i], std::make_tuple(i, kCode, kCid));
    EXPECT_EQ(r.sinkForTest()->offerFramesForTest(), 1u);
    EXPECT_EQ(r.sinkForTest()->claimFramesForTest(), claimsBefore) << "the big ack did not go through tryClaim";
}

TEST(CheckedWritesVerdictAckRigRegression, FourThousandRejectionsEncryptedAckIsOfferedRegression) {
    VerdictRig o;
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks(17, 0, /*encrypted=*/true);
    o.passes(60);
    std::vector<Rec> recs;
    for (uint64_t i = 0; i < 4096; ++i) recs.push_back({i + 1, 1, 9});
    const uint64_t claimsBefore = o.sinkForTest()->claimFramesForTest();
    const int64_t c = o.produceFrame(recs, kAcksView);
    const auto a = o.ack(c, 3000);
    ASSERT_TRUE(a) << "the sealed ack is above kMaxEncodedSize: it was refused as fatal";
    EXPECT_EQ(a->frameLen, 32'824u);
    ASSERT_EQ(a->verdicts.size(), 4096u);
    EXPECT_EQ(o.sinkForTest()->offerFramesForTest(), 1u);
    EXPECT_EQ(o.sinkForTest()->claimFramesForTest(), claimsBefore);
}

TEST(CheckedWritesVerdictAckRigRegression, ViewProduceBeforeTheLeadershipScanIsReadyIsNotLeaderRegression) {
    VerdictRig o;
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks();
    o.passes(60);
    for (uint64_t i = 1; i <= 40; ++i) ASSERT_TRUE(o.ack(o.produceFrame({{i, 1, 1}}, kAcksView)));
    o.rcScanBytesPerPass = 256;   // the restarted worker scans a few batches per pass
    o.restart();
    const int64_t c = o.produceFrame({{100, 1, 9}}, kAcksView);
    const auto a = o.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader)) << "the timeline is still being built";
    int64_t retry = 0;
    std::optional<VerdictRig::Ack> done;
    do { retry = o.produceFrame({{100, 1, 9}}, kAcksView); done = o.ack(retry); ASSERT_TRUE(done); } while (done->code != 0);
    EXPECT_EQ(done->verdicts, (V{{0, kCode, kCid}})) << "once Ready the write is judged";
}

TEST_F(CheckedWritesVerdictAckRegression, ReplayOfAnOverlongTrailerIsInternalNotAHaltRegression) {
    std::vector<Rec> recs;
    for (uint64_t i = 0; i < 5000; ++i) recs.push_back({i + 1, 1, 9});
    const auto plain = r.ack(r.produceFrame(recs, rune::protocol::client::kAcksWireAll, /*pid=*/500, /*seq=*/0), 3000);
    ASSERT_TRUE(plain);
    ASSERT_EQ(plain->code, 0);
    const auto replay = r.ack(r.produceFrame(recs, kAcksView, 500, 0), 3000);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->code, static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal));
    EXPECT_FALSE(replay->hasVerdicts);
    const auto next = r.ack(r.produceFrame({{9001, 1, 9}}, kAcksView), 3000);
    ASSERT_TRUE(next) << "the worker keeps answering";
    EXPECT_EQ(next->code, 0);
}

TEST_F(CheckedWritesVerdictAckRegression, OneRecordAckIsClaimedNotOffered) {
    const int64_t c = r.produceFrame({{1, 1, 9}}, kAcksView);
    ASSERT_TRUE(r.ack(c));
    EXPECT_EQ(r.sinkForTest()->offerFramesForTest(), 0u);
    EXPECT_GE(r.sinkForTest()->claimFramesForTest(), 1u);
}

TEST(CheckedWritesVerdictAckRigRegression, ArenaFullDefersInPartitionOrderRegression) {
    VerdictRig o;
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks(17, /*arenaEntries=*/4096);
    o.passes(60);
    std::vector<Rec> big, small{{9001, 1, 9}};
    for (uint64_t i = 0; i < 4096; ++i) big.push_back({i + 1, 1, 9});
    const int64_t a = o.produceFrame(big, kAcksView);
    const int64_t b = o.produceFrame(small, kAcksView);
    ASSERT_TRUE(o.ack(a, 3000));
    ASSERT_TRUE(o.ack(b, 3000));
    ASSERT_EQ(o.ackOrder().size(), 2u);
    EXPECT_EQ(o.ackOrder()[0], a);
    EXPECT_EQ(o.ackOrder()[1], b);
    EXPECT_EQ(o.ack(b)->verdicts.size(), 1u);
}

TEST_F(CheckedWritesVerdictAckRegression, ViewRetryInTheWindowReplaysTheOriginalVerdictsRegression) {
    const int64_t c1 = r.produceFrame({{1, 1, 1}, {2, 2, 9}, {3, 3, 1}, {4, 4, 9}}, kAcksView, /*pid=*/500, /*seq=*/0);
    const auto first = r.ack(c1);
    ASSERT_TRUE(first);
    const int64_t leoBefore = r.leo();
    const int64_t c2 = r.produceFrame({{1, 1, 1}, {2, 2, 9}, {3, 3, 1}, {4, 4, 9}}, kAcksView, 500, 0);
    const auto again = r.ack(c2, 3000);
    ASSERT_TRUE(again);
    EXPECT_EQ(again->code, 0);
    EXPECT_EQ(again->baseOffset, first->baseOffset);
    EXPECT_EQ(again->verdicts, first->verdicts);
    EXPECT_EQ(again->verdicts, (V{{1, kCode, kCid}, {3, kCode, kCid}}));
    EXPECT_EQ(r.leo(), leoBefore) << "nothing was appended again";
    EXPECT_EQ(r.stats()->verdictReplays, 1u);
}

TEST_F(CheckedWritesVerdictAckRegression, ViewRetryOutsideTheWindowIsDuplicateSequenceRegression) {
    const int64_t c1 = r.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView, 500, 0);
    ASSERT_TRUE(r.ack(c1));
    for (int32_t s = 2; s <= 34; ++s) {   // the first batch took sequences 0 and 1
        const auto f = r.ack(r.produceFrame({{100u + uint64_t(s), 1, 1}}, kAcksView, 500, s), 3000);
        ASSERT_TRUE(f);
        ASSERT_EQ(f->code, 0) << "seq " << s;
    }
    r.settle(150);   // a marker may follow; the retry must not append data
    const int64_t leoBefore = r.leo();
    const uint64_t unknownBefore = r.stats()->verdictUnknownDuplicates;
    const int64_t c2 = r.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView, 500, 0);
    const auto a = r.ack(c2, 3000);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, static_cast<int16_t>(rune::protocol::client::ErrorCode::DuplicateSequence));
    EXPECT_EQ(a->frameLen, 54u);
    EXPECT_FALSE(a->hasVerdicts);
    EXPECT_EQ(r.stats()->verdictUnknownDuplicates, unknownBefore + 1);
    EXPECT_EQ(r.leo(), leoBefore);
}

// A replay waits for the marker that commits its verdicts; an ack of the same partition behind it waits too, so the acks of a
// connection leave in the order the batches were written.
TEST(CheckedWritesVerdictAckRigRegression, ReplayNotYetCommittedKeepsTheAcksBehindItInOrderRegression) {
    VerdictRig o(/*follower=*/true);
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks();
    o.passes(60);
    const int64_t first = o.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView, 500, 0);
    ASSERT_TRUE(o.ackFetching(first));
    const int64_t replay = o.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView, 500, 0);
    const int64_t fresh = o.produceFrame({{3, 3, 1}}, kAcksView);
    ASSERT_TRUE(o.ackFetching(replay));
    ASSERT_TRUE(o.ackFetching(fresh));
    const auto& order = o.ackOrder();
    const auto at = [&](int64_t c) { return std::find(order.begin(), order.end(), c) - order.begin(); };
    EXPECT_LT(at(replay), at(fresh));
}

TEST_F(CheckedWritesVerdictAckRegression, LeaderWithoutTheInstallParksTheViewAckRegression) {
    r.restart(/*reinstall=*/false);
    const int64_t c = r.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView);
    r.settle(300);
    EXPECT_FALSE(r.acked(c)) << "no ack while the judge waits for the install";
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_TRUE(judge->waitingForInstall());
    r.reinstallAll();
    const auto a = r.ack(c, 3000);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->verdicts, (V{{1, kCode, kCid}}));
}

TEST_F(CheckedWritesVerdictAck, LeaderLossWhileParkedIsNotLeader) {
    r.vs->verdictSource(VerdictRig::kSrc, 0)->holdJudgeForTest(true);
    const int64_t c = r.produceFrame({{1, 1, 1}}, kAcksView);
    r.settle(200);
    ASSERT_FALSE(r.acked(c));
    r.demote();
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
    EXPECT_FALSE(a->hasVerdicts);
}

TEST_F(CheckedWritesVerdictAck, DeadlineIsInternal) {
    r.worker->setAckTimeoutNs(50 * kMs);
    r.vs->verdictSource(VerdictRig::kSrc, 0)->holdJudgeForTest(true);
    const int64_t c = r.produceFrame({{1, 1, 1}}, kAcksView);
    r.settle(30);
    EXPECT_FALSE(r.acked(c)) << "before the deadline";
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal));
    EXPECT_FALSE(a->hasVerdicts);
}

TEST_F(CheckedWritesVerdictAckRegression, ReplacingAViewWithTheSamePlanKeepsViewAcksRegression) {
    // #786: the new view carries the old one's object and plan, so the logged set does not change and the old member never
    // retires.  The new view must still fold, or no View ack passes it.
    constexpr int32_t kRej2 = 0x40000602;
    ASSERT_TRUE(r.ack(r.produceFrame({{1, 1, 1}}, kAcksView)));
    r.replaceRejectView(kRej, kRej2, kObj, tagRule(9));
    r.passes(60);
    const auto a = r.ack(r.produceFrame({{2, 2, 9}, {3, 3, 1}}, kAcksView));
    ASSERT_TRUE(a) << "the new view never folded";
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->verdicts, (V{{0, kCode, kCid}}));
    r.passes(60);
    EXPECT_FALSE(r.vs->isViewSink(kRej)) << "the old view is released";
}

TEST_F(CheckedWritesVerdictAckRegression, ReinstallingADroppedPlanWhileItRetiresKeepsViewAcksRegression) {
    // #786: the drop is logged and the old member retires; the same plan installed again before the drop commits joins anew.
    constexpr int32_t kRej2 = 0x40000602;
    VerdictRig o(/*follower=*/true);
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks();
    o.passes(60);
    ASSERT_TRUE(o.ackFetching(o.produceFrame({{1, 1, 1}}, kAcksView)));
    o.dropRejectView(kRej);
    o.passes(60);
    o.followerFetch(o.writeState()->constraintSets.lastChangeOffset() + 1);   // the drop commits, the marker after it does not
    o.passes(10);
    ASSERT_TRUE(o.vs->verdictSource(VerdictRig::kSrc, 0)->retiringForTest(kObj));
    o.installRejectView(kRej2, kObj, tagRule(9), "rej2");
    o.passes(60);
    EXPECT_TRUE(o.vs->verdictSource(VerdictRig::kSrc, 0)->hasView(kRej2)) << "joins while the old member still retires";
    const auto a = o.ackFetching(o.produceFrame({{2, 2, 9}, {3, 3, 1}}, kAcksView));
    ASSERT_TRUE(a) << "the judge never resolved the re-added plan";
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->verdicts, (V{{0, kCode, kCid}}));
    for (int i = 0; i < 20; ++i) { o.followerFetch(); o.passes(5); }
    EXPECT_FALSE(o.vs->isViewSink(kRej)) << "the old view is released";
    EXPECT_TRUE(o.vs->isViewSink(kRej2));
}

// #807: the last verdict view is dropped; the judge must be erased (the gate in driveViews_ must still drive a source whose judge
// is alive with an empty latest set), or the verdict bound freezes and View acks time out.
TEST_F(CheckedWritesVerdictAckRegression, ViewAckAndConsumersAfterTheLastVerdictViewIsDroppedRegression) {
    r.worker->setAckTimeoutNs(50 * kMs);
    const int64_t c0 = r.produceFrame({{1, 1, 1}}, kAcksView, 600, 0);
    const auto a0 = r.ack(c0);
    ASSERT_TRUE(a0);
    ASSERT_EQ(a0->code, 0);

    r.dropRejectView(kRej);
    r.driveUntilJudged();
    r.passes(20);
    EXPECT_EQ(r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr) << "the judge outlived its last verdict view";
    ASSERT_NE(r.writeState(), nullptr);
    EXPECT_EQ(r.writeState()->verdicts, nullptr);

    const int64_t c1 = r.produceFrame({{9, 9, 9}}, kAcksView, 601, 0);   // tag 9 is accepted: no view rejects it any more
    const auto a1 = r.ack(c1, 200);
    ASSERT_TRUE(a1);
    EXPECT_EQ(a1->code, 0);
    EXPECT_TRUE(a1->verdicts.empty());

    const int64_t leoBefore = r.leo();
    const int64_t c2 = r.produceFrame({{9, 9, 9}}, kAcksView, 601, 0);   // a dedup replay, not a timeout
    const auto a2 = r.ack(c2, 200);
    ASSERT_TRUE(a2);
    EXPECT_EQ(a2->code, 0);
    EXPECT_EQ(a2->baseOffset, a1->baseOffset);
    EXPECT_EQ(r.leo(), leoBefore);

    const int64_t s = r.subscribe(SubKind::Raw, 0);
    const auto& box = r.collect(s);
    bool saw0 = false, saw1 = false;
    for (const auto& f : box.frames) {
        saw0 = saw0 || (f.base <= a0->baseOffset && a0->baseOffset < f.base + f.count);
        saw1 = saw1 || (f.base <= a1->baseOffset && a1->baseOffset < f.base + f.count);
    }
    EXPECT_TRUE(saw0);
    EXPECT_TRUE(saw1);
}

// Review fix round: a View ack whose partition is NOT a verdict source when it fires (the judge is erased, the logged set is
// empty) still carries the rejections its batch has in the write state's rejected index (as after a restart, which rebuilds the
// index from the markers); firing code 0 with no trailer would report a rejected record as accepted.  The judge and the ack fire
// in the same pass in normal flow, so the state is built by hand: the last verdict view is dropped and the judge erased, a batch is
// produced with the HW held (a follower in the ISR: the ack is owed), the index gets the rejection of the batch's second record,
// then the HW is released.
TEST(CheckedWritesVerdictAckRigRegression, ViewAckAfterTheJudgeIsErasedStillCarriesItsVerdictsRegression) {
    VerdictRig o(/*follower=*/true);
    o.typed();
    o.installRejectView(kRej, kObj, tagRule(9));
    o.enableAcks();
    o.passes(60);
    o.dropRejectView(kRej);
    ASSERT_TRUE(o.driveUntilJudged());
    o.passes(20);
    ASSERT_EQ(o.vs->verdictSource(VerdictRig::kSrc, 0), nullptr) << "the judge is erased";
    auto* ws = const_cast<PartitionWriteState*>(o.writeState());
    ASSERT_NE(ws, nullptr);
    ASSERT_EQ(ws->verdicts, nullptr);
    ASSERT_FALSE(ws->isVerdictSource()) << "no judge and an empty logged set: not a verdict source";

    const int64_t base = o.leo();
    const int64_t c = o.produceFrame({{1, 1, 1}, {2, 2, 9}}, kAcksView);
    o.passes(60);
    ASSERT_FALSE(o.acked(c)) << "the HW is held: the ack is owed";
    ASSERT_TRUE(ws->rejected.add(base + 1, kCode, kCid));

    o.followerFetch();
    const auto a = o.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_TRUE(a->hasVerdicts) << "the rejection must not be dropped from the ack";
    EXPECT_EQ(a->verdicts, (V{{1, kCode, kCid}}));
}

// Batch 1 adversarial pass (judge erasure, fdcc3c26): the public path to an ack whose partition stopped being a verdict source
// with rejections still in its range.  A View batch is produced and judged (record 1 rejected); then the last verdict view is
// dropped and the judge erased; then the SAME batch is retried (an idempotent replay inside the dedupe window).  The replay must
// report the original rejection from the rejected index, never "code 0, no trailer" (a rejected record reported as accepted),
// and must append nothing.  A second partition-mate batch produced after the drop has no rejections and an empty trailer.
TEST_F(CheckedWritesVerdictAckRegression, ReplayAfterTheJudgeIsErasedReportsTheOriginalRejectionAdversarialRegression) {
    const int64_t c1 = r.produceFrame({{1, 1, 1}, {2, 2, 9}, {3, 3, 1}}, kAcksView, /*pid=*/700, /*seq=*/0);
    const auto first = r.ack(c1);
    ASSERT_TRUE(first);
    ASSERT_EQ(first->code, 0);
    ASSERT_EQ(first->verdicts, (V{{1, kCode, kCid}}));
    r.settle(150);   // the marker committing the verdicts is written

    r.dropRejectView(kRej);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    ASSERT_EQ(r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr) << "the judge is erased";
    ASSERT_NE(r.writeState(), nullptr);
    ASSERT_FALSE(r.writeState()->isVerdictSource());

    const int64_t leoBefore = r.leo();
    const int64_t c2 = r.produceFrame({{1, 1, 1}, {2, 2, 9}, {3, 3, 1}}, kAcksView, 700, 0);
    const auto again = r.ack(c2, 3000);
    ASSERT_TRUE(again);
    EXPECT_EQ(again->code, 0);
    EXPECT_EQ(again->baseOffset, first->baseOffset);
    EXPECT_TRUE(again->hasVerdicts) << "the replay of a batch with a rejected record must carry its trailer";
    EXPECT_EQ(again->verdicts, first->verdicts) << "the original rejection, from the rejected index";
    EXPECT_EQ(r.leo(), leoBefore) << "a replay appends nothing";

    const int64_t c3 = r.produceFrame({{4, 4, 9}}, kAcksView, 701, 0);   // tag 9: no view rejects it any more
    const auto fresh = r.ack(c3, 3000);
    ASSERT_TRUE(fresh);
    EXPECT_EQ(fresh->code, 0);
    EXPECT_TRUE(fresh->verdicts.empty());
}

// The same replay across a restart and across a failover (this node's tail above the HW cut): the rejected index is rebuilt from
// the kept marker, the judge is erased after the reboot, and the retried batch still reports its original rejection.
TEST(CheckedWritesVerdictAckRigRegression, ReplayAfterRestartAndJudgeErasureReportsTheOriginalRejectionAdversarialRegression) {
    for (const bool failover : {false, true}) {
        SCOPED_TRACE(failover ? "failover" : "restart");
        VerdictRig o;
        o.typed();
        o.installRejectView(kRej, kObj, tagRule(9));
        o.enableAcks();
        o.passes(60);
        const int64_t c1 = o.produceFrame({{1, 1, 9}, {2, 2, 1}}, kAcksView, /*pid=*/800, /*seq=*/0);
        const auto first = o.ack(c1);
        ASSERT_TRUE(first);
        ASSERT_EQ(first->verdicts, (V{{0, kCode, kCid}}));
        o.settle(150);   // the marker is written and committed

        if (failover) o.failover(); else o.restart();
        o.enableAcks();
        o.passes(60);
        o.dropRejectView(kRej);
        ASSERT_TRUE(o.driveUntilJudged());
        o.passes(20);
        ASSERT_EQ(o.vs->verdictSource(VerdictRig::kSrc, 0), nullptr) << "the judge is erased after the reboot";

        const int64_t leoBefore = o.leo();
        const int64_t c2 = o.produceFrame({{1, 1, 9}, {2, 2, 1}}, kAcksView, 800, 0);
        const auto again = o.ack(c2, 3000);
        ASSERT_TRUE(again);
        EXPECT_EQ(again->code, 0) << "the dedupe state survives the reboot: an in-window replay";
        EXPECT_EQ(again->baseOffset, first->baseOffset);
        EXPECT_TRUE(again->hasVerdicts) << "a replay of a batch with a rejected record must carry its trailer";
        EXPECT_EQ(again->verdicts, first->verdicts);
        EXPECT_EQ(o.leo(), leoBefore) << "a replay appends nothing";
    }
}
