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
