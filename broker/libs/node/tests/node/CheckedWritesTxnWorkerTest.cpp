// Checked writes Part 11 Task 7: the TxnVote op on a real Worker - the vote-time scan, the TxnJudge record J, answers parked until
// the judge judged J, the marker's answer held until its transaction is final, the stats 11-20 and the read_uncommitted bound.
// A transaction with intents is written by replication where the test needs the previous leader's bytes.
#include "CheckedWritesRig.h"

#include <rune/protocol/client/StatsMessages.h>

#include <algorithm>
#include <fstream>
#include <iterator>

using namespace rune::node;
using namespace rune::node::cw;
namespace pc = rune::protocol::client;

namespace {
constexpr uint8_t kLeader = 1;
constexpr uint16_t kKeyExists = 56, kTooLarge = static_cast<uint16_t>(pc::ErrorCode::TxnTooLarge);
constexpr int16_t kNotLeader = static_cast<int16_t>(pc::ErrorCode::NotLeader);

Rec ins(uint64_t id) { return Rec{id, 1, 0, false, 1}; }
Rec ups(uint64_t id) { return Rec{id, 1, 0, false, 0}; }

/// A key-index topic with acks, its change record logged.
struct Ki {
    VerdictRig r;
    explicit Ki(bool follower = false) : r(follower) {
        r.createKeyIndex();
        r.enableAcks();
        r.passes(60);
        if (follower) r.followerFetch();
    }
    /// `recs` as producer `pid`'s open transaction, written by the previous leader.
    int64_t txn(int64_t pid, const std::vector<Rec>& recs) {
        const int64_t base = r.appendTxnReplicated(VerdictRig::batch(recs, pid, 0, {}, /*txn=*/true));
        r.passes(30);
        return base;
    }
    [[nodiscard]] const TxnJudgeStats& stats() const { return r.writeState()->txn.stats; }
    VerdictRig::VoteReply vote(int64_t pid) {
        const int64_t tok = r.sendVote(VerdictRig::kSrc, pid, 0);
        EXPECT_TRUE(r.driveUntilVoted(tok)) << "the vote of " << pid << " was never answered";
        return r.voteReply(tok).value_or(VerdictRig::VoteReply{-1});
    }
};
}  // namespace

// P11-1: the yes is answered with J; the commit marker applies the transaction's intents in one step.
TEST(CheckedWritesTxnWorker, VoteYesThenCommitAppliesInOneStep) {
    Ki k;
    k.txn(31, {ins(1), ins(2)});
    const auto v = k.vote(31);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, 0);
    EXPECT_EQ(k.r.txnJudgeOffsetsInLog(), std::vector<int64_t>{v.judgeOffset}) << "one TxnJudge, the one answered";
    EXPECT_TRUE(k.r.keyIndexKeys().empty()) << "nothing applied before the marker";
    k.r.marker(31, true);
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{1, 2}));
    EXPECT_EQ(k.stats().votesYes, 1u);
}

// P11-1: a transaction inserting a key that exists votes no with 56 and the failing record; its abort leaves the index alone.
TEST(CheckedWritesTxnWorker, InsertOfAnExistingKeyVotesKeyExists) {
    Ki k;
    k.txn(31, {ins(1)});
    ASSERT_EQ(k.vote(31).code, 0);
    k.r.marker(31, true);
    ASSERT_TRUE(k.r.driveUntilJudged());
    const int64_t base = k.txn(32, {ins(7), ins(1)});
    const auto v = k.vote(32);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, kKeyExists);
    EXPECT_EQ(v.failingOffset, base + 1);
    k.r.marker(32, false);
    ASSERT_TRUE(k.r.driveUntilJudgeReaches());
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{1})) << "the aborted transaction's 7 is never applied";
    EXPECT_EQ(k.stats().votesNo, 1u);
    EXPECT_EQ(k.stats().votesNoKeyExists, 1u);
}

// P11-1: a transaction without intents on a key-index-only source does not vote: yes, and no TxnJudge is written.
TEST(CheckedWritesTxnWorker, NonVotingTransactionAnswersYesWithoutWriting) {
    Ki k;
    const auto a = k.r.ack(k.r.produceFrame({ups(4), ups(5)}, kLeader, 33, 0, /*txn=*/true));
    ASSERT_TRUE(a && a->code == 0);
    const auto v = k.vote(33);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, 0);
    EXPECT_EQ(v.judgeOffset, -1);
    EXPECT_TRUE(k.r.txnJudgeOffsetsInLog().empty());
    k.r.marker(33, true);
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{4, 5}));
}

// P11-1: a partition that is not a verdict source answers yes at once and writes nothing.
TEST(CheckedWritesTxnWorker, PartitionWithoutAVerdictSourceAnswersYesAtOnce) {
    VerdictRig r;
    r.typed();
    r.passes(30);
    const int64_t leo = r.leo();
    const int64_t tok = r.sendVote(VerdictRig::kSrc, 40, 0);
    const auto v = r.voteReply(tok);
    ASSERT_TRUE(v) << "answered in the pass that took it";
    EXPECT_EQ(v->errorCode, 0);
    EXPECT_EQ(v->code, 0);
    EXPECT_EQ(r.leo(), leo);
}

// P11-1: no open transaction of the producer here: yes, counted.
TEST(CheckedWritesTxnWorker, VoteForAnUnknownTransactionAnswersYes) {
    Ki k;
    const auto v = k.vote(555);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, 0);
    EXPECT_TRUE(k.r.txnJudgeOffsetsInLog().empty());
    EXPECT_EQ(k.stats().votesYes, 1u);
}

// The knob: a transaction over RUNE_TXN_JUDGED_RECORDS_MAX votes TxnTooLarge, deterministically, without a TxnJudge.
TEST(CheckedWritesTxnWorker, OverTheCapVotesTooLargeWithoutAJudgeRecord) {
    Ki k;
    k.r.worker->setTxnJudgedRecordsMax(2);
    k.txn(34, {ins(1), ins(2), ins(3)});
    const auto v = k.vote(34);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, kTooLarge);
    EXPECT_TRUE(k.r.txnJudgeOffsetsInLog().empty());
    EXPECT_EQ(k.vote(34).code, kTooLarge) << "the same answer again";
    EXPECT_EQ(k.stats().votesNoTooLarge, 2u);
    k.r.marker(34, false);
    ASSERT_TRUE(k.r.driveUntilJudgeReaches());
    EXPECT_TRUE(k.r.keyIndexKeys().empty());
}

// P11-1: votes in flight together and a vote after the answer are all answered from the one J.
TEST(CheckedWritesTxnWorker, RepeatedVoteIsAnsweredFromTheSameJudge) {
    Ki k;
    k.txn(35, {ins(1)});
    const int64_t t1 = k.r.sendVote(VerdictRig::kSrc, 35, 0), t2 = k.r.sendVote(VerdictRig::kSrc, 35, 0);
    ASSERT_TRUE(k.r.driveUntilVoted(t1) && k.r.driveUntilVoted(t2));
    const auto again = k.vote(35);
    EXPECT_EQ(k.r.voteReply(t1)->judgeOffset, again.judgeOffset);
    EXPECT_EQ(k.r.voteReply(t2)->judgeOffset, again.judgeOffset);
    EXPECT_EQ(k.r.voteRepliesOf(t1), 1u);
    EXPECT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 0);
}

// A J above the HW is not judged yet: the vote is answered only once J commits (a failover could lose it).
TEST(CheckedWritesTxnWorker, UncommittedJudgeIsNeverAnsweredRegression) {
    Ki k(/*follower=*/true);
    k.txn(36, {ins(1)});
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 36, 0);
    k.r.passes(60);
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u) << "J appended";
    EXPECT_FALSE(k.r.voteReply(tok)) << "J above the HW";
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 1);
    ASSERT_TRUE(k.r.driveUntilVoted(tok));
    EXPECT_EQ(k.r.voteReply(tok)->code, 0);
}

// A demotion answers every parked vote NotLeader (retriable) and releases its owed slot.
TEST(CheckedWritesTxnWorker, ParkedVoteIsNotLeaderOnDemotion) {
    Ki k(/*follower=*/true);
    k.txn(37, {ins(1)});
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 37, 0);
    k.r.passes(30);
    ASSERT_FALSE(k.r.voteReply(tok));
    k.r.demote();
    k.r.passes(5);
    const auto v = k.r.voteReply(tok);
    ASSERT_TRUE(v);
    EXPECT_EQ(v->errorCode, kNotLeader);
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 0);
}

// P11-12: the marker of a judged transaction is answered only once the transaction is final (its intents applied).
TEST(CheckedWritesTxnWorker, MarkerAnswerWaitsForFinalizeRegression) {
    Ki k;
    k.txn(38, {ins(1)});
    ASSERT_EQ(k.vote(38).code, 0);
    k.r.holdJudge(true);
    k.r.marker(38, true);
    k.r.passes(60);
    EXPECT_FALSE(k.r.markerAnswer(38)) << "the marker is answered before its transaction is final";
    k.r.holdJudge(false);
    for (int i = 0; i < 600 && !k.r.markerAnswer(38); ++i) k.r.pass();
    ASSERT_TRUE(k.r.markerAnswer(38));
    EXPECT_GE(*k.r.markerAnswer(38), 0);
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{1})) << "applied when the marker is answered";
}

// P11-10: on a verdict source read_uncommitted is served as read_committed - an aborted transaction never reaches a consumer.
TEST(CheckedWritesTxnWorker, ReadUncommittedOnAVerdictSourceSkipsAbortedTxns) {
    Ki k;
    ASSERT_TRUE(k.r.ack(k.r.produceFrame({ups(1)}, kLeader)));
    ASSERT_TRUE(k.r.ack(k.r.produceFrame({ups(2), ups(3)}, kLeader, 39, 0, /*txn=*/true)));
    const int64_t sub = k.r.subscribe(SubKind::Raw, 0);   // isolation 0: read_uncommitted
    k.r.passes(30);
    std::vector<uint64_t> ids;
    for (const auto& d : k.r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(ids, std::vector<uint64_t>{1}) << "the open transaction is not served";
    k.r.marker(39, false);
    ASSERT_TRUE(k.r.ack(k.r.produceFrame({ups(4)}, kLeader)));
    ASSERT_TRUE(k.r.driveUntilJudged());
    ids.clear();
    for (const auto& d : k.r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(ids, (std::vector<uint64_t>{1, 4})) << "the aborted one never is";
}

// Stats 11-20 of a verdict source's partition.
TEST(CheckedWritesTxnWorker, StatsElevenToTwentyAreServed) {
    Ki k;
    k.txn(41, {ins(1)});
    ASSERT_EQ(k.vote(41).code, 0);
    k.r.marker(41, true);
    k.r.worker->setTxnJudgedRecordsMax(1);
    k.txn(42, {ins(2), ins(3)});
    ASSERT_EQ(k.vote(42).code, kTooLarge);
    k.r.marker(42, false);
    ASSERT_TRUE(k.r.driveUntilJudgeReaches());
    const auto s = k.r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    std::map<uint16_t, int64_t> by;
    for (const auto& [id, val] : s.stats) by[id] = val;
    for (uint16_t id = 11; id <= 20; ++id) EXPECT_TRUE(by.count(id)) << "stat " << id;
    EXPECT_EQ(by[11], 1);   // votesYes
    EXPECT_EQ(by[12], 1);   // votesNo
    EXPECT_EQ(by[16], 1);   // votesNoTooLarge
    EXPECT_GT(by[18], 0);   // voteLatencyNsTotal
    EXPECT_GE(by[18], by[17]);
    std::vector<uint16_t> order;
    for (const auto& e : s.stats) order.push_back(e.first);
    EXPECT_TRUE(std::is_sorted(order.begin(), order.end())) << "ids ascend";
}

// A source whose only member is its key index (no view subscribes it) reserves the transaction overlays too; it used to return
// before the reserve, so its first judged transaction stalled the judge on "view capacity exhausted".
TEST(CheckedWritesTxnWorker, KeyIndexOnlySourceReservesTheTxnOverlaysRegression) {
    Ki k;
    auto& pool = k.r.vs->txnOverlaysForTest();
    EXPECT_GT(pool.at(0).maxRowLen(), 0u);
    k.txn(43, {ins(1)});
    ASSERT_EQ(k.vote(43).code, 0);
    EXPECT_FALSE(k.r.vs->verdictSource(VerdictRig::kSrc, 0)->stalled());
}

// P11-1 on a constrained source: a transaction whose name another character holds votes no with the unique view's violation.
TEST(CheckedWritesTxnWorker, ConstrainedSourceVotesTheUniqueViolation) {
    VerdictRig r;
    r.createQlTopic(VerdictRig::kCharacterTopic, false);
    r.enableAcks();
    r.passes(20);
    r.createConstrainedView(ConstrainedView::CharacterName);
    r.passes(20);
    const auto write = [&](int64_t pid, std::vector<VerdictRig::Char> chars) {
        std::vector<VerdictRig::SrcRecord> recs;
        for (const auto& c : chars) recs.push_back(VerdictRig::srcOf(c));
        const int64_t base = r.appendTxnReplicated(r.sourceBatch(recs, pid, 0, /*txn=*/true));
        r.passes(30);
        return base;
    };
    const auto vote = [&](int64_t pid) {
        const int64_t tok = r.sendVote(VerdictRig::kSrc, pid, 0);
        EXPECT_TRUE(r.driveUntilVoted(tok));
        return r.voteReply(tok).value_or(VerdictRig::VoteReply{-1});
    };
    write(51, {VerdictRig::Char{.id = 1, .userId = 1, .name = "Foo"}});
    const auto yes = vote(51);
    EXPECT_EQ(yes.errorCode, 0);
    EXPECT_EQ(yes.code, 0);
    r.marker(51, true);
    ASSERT_TRUE(r.driveUntilJudgeReaches());
    const int64_t base = write(52, {VerdictRig::Char{.id = 2, .userId = 2, .name = "foo"}});
    const auto no = vote(52);
    EXPECT_EQ(no.errorCode, 0);
    EXPECT_EQ(no.code, 55);
    EXPECT_EQ(no.constraintId, 7u << 16);
    EXPECT_EQ(no.failingOffset, base);
    r.marker(52, false);
    ASSERT_TRUE(r.driveUntilJudgeReaches());
    EXPECT_EQ(r.constrainedInstance(ConstrainedView::CharacterName)->table.size(), 1u) << "only Foo holds a name";
    EXPECT_EQ(r.writeState()->txn.stats.votesNoConstraint, 1u);
}

// Task 5 carry (no leak): a judge dropped while a voted transaction holds its overlay gives the overlay back, and its parked vote
// is answered NotLeader rather than lost.
TEST(CheckedWritesTxnWorker, DroppingALockedJudgeReleasesItsOverlay) {
    Ki k;
    auto& pool = k.r.vs->txnOverlaysForTest();
    k.txn(44, {ins(1)});
    ASSERT_EQ(k.vote(44).code, 0);
    ASSERT_EQ(k.r.writeState()->txn.find(44)->phase, TxnPhase::Locked);
    EXPECT_EQ(pool.freeCount(), rune::view::kTxnOverlaysPerWorker - 1) << "the voted transaction holds one";
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 44, 0);
    k.r.vs->dropSourceTopic(VerdictRig::kSrc);
    WorkerOp d;
    d.type = WorkerOp::Type::TopicDropped;
    d.topicId = VerdictRig::kSrc;
    k.r.send(std::move(d));
    EXPECT_EQ(pool.freeCount(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 0);
    ASSERT_TRUE(k.r.voteReply(tok));
}

TEST(CheckedWritesTxnWorker, OwnershipLossWhileLockedReleasesItsOverlay) {
    Ki k;
    auto& pool = k.r.vs->txnOverlaysForTest();
    k.txn(45, {ins(1)});
    ASSERT_EQ(k.vote(45).code, 0);
    ASSERT_EQ(pool.freeCount(), rune::view::kTxnOverlaysPerWorker - 1);
    k.r.worker->setOwnedDirect(Worker::OwnedSet{});
    k.r.passes(3);
    EXPECT_EQ(pool.freeCount(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 0);
}

// Task 5 carry (no leak): transactions on a partition that is not a verdict source never take an overlay.
TEST(CheckedWritesTxnWorker, NoVerdictSourceNeverTakesAnOverlay) {
    VerdictRig r;
    r.typed();
    r.enableAcks();
    r.passes(30);
    ASSERT_TRUE(r.ack(r.produceFrame({ups(1), ups(2)}, kLeader, 46, 0, /*txn=*/true)));
    const int64_t tok = r.sendVote(VerdictRig::kSrc, 46, 0);
    ASSERT_TRUE(r.voteReply(tok));
    r.marker(46, true);
    r.passes(30);
    EXPECT_EQ(r.vs->txnOverlaysForTest().freeCount(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_TRUE(r.txnJudgeOffsetsInLog().empty());
}

// P11-1: an update intent makes a key-index-only partition vote; on an absent key it is 57.
TEST(CheckedWritesTxnWorker, ExplicitIntentMakesAKeyIndexOnlyPartitionVote) {
    Ki k;
    const int64_t base = k.txn(47, {ups(3), Rec{9, 1, 0, false, 2}});
    const auto v = k.vote(47);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, 57);
    EXPECT_EQ(v.failingOffset, base + 1);
    EXPECT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
    EXPECT_EQ(k.stats().votesNoKeyNotFound, 1u);
}

// A J lost with the leader's uncommitted tail is never answered; the re-sent vote to the next leader appends a new J and is.
TEST(CheckedWritesTxnWorker, JudgeLostInAFailoverIsJudgedAgainRegression) {
    Ki k(/*follower=*/true);
    k.txn(48, {ins(1)});
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 48, 0);
    k.r.passes(60);
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
    const int64_t lost = k.r.txnJudgeOffsetsInLog()[0];
    ASSERT_FALSE(k.r.voteReply(tok));
    const int64_t hw = k.r.replica->highWatermark();
    k.r.demote();   // another replica takes over: the parked vote is answered NotLeader, not lost
    ASSERT_TRUE(k.r.voteReply(tok));
    EXPECT_EQ(k.r.voteReply(tok)->errorCode, kNotLeader);
    k.r.restart(true, hw);   // the failover: the tail above the HW is gone
    k.r.passes(30);
    EXPECT_TRUE(k.r.txnJudgeOffsetsInLog().empty()) << "J was above the HW";
    const auto v = k.vote(48);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, 0);
    EXPECT_EQ(k.r.txnJudgeOffsetsInLog(), std::vector<int64_t>{v.judgeOffset});
    EXPECT_EQ(v.judgeOffset, lost) << "the new J lands where the lost one was";
}

// The vote's scan appends J only once the transaction's last batch is below the HW: a J above an uncommitted batch could judge
// records a failover drops.
TEST(CheckedWritesTxnWorker, VoteWaitsForTheHw) {
    Ki k(/*follower=*/true);
    k.txn(53, {ins(1)});
    k.r.followerFetch();
    (void)k.r.produceFrame({ups(2)}, kLeader, 53, 1, /*txn=*/true);   // its last batch, above the HW until the follower fetches
    k.r.passes(5);
    const int64_t last = k.r.leo() - 1;
    ASSERT_LE(k.r.replica->highWatermark(), last);
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 53, 0);
    k.r.passes(30);
    EXPECT_TRUE(k.r.txnJudgeOffsetsInLog().empty()) << "no J while HW <= last";
    EXPECT_FALSE(k.r.voteReply(tok));
    ASSERT_TRUE(k.r.driveUntilVoted(tok));
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
    EXPECT_GT(k.r.txnJudgeOffsetsInLog()[0], last);
    EXPECT_EQ(k.r.voteReply(tok)->errorCode, 0);
    EXPECT_EQ(k.r.voteReply(tok)->code, 0);
}

// Task 7 (T7 review 1, F2): the coordinator's abort lands while the vote's scan waits for the HW.  The scan writes no J past the
// marker (it would judge nothing, hold K behind the marker and wedge the producer's next J); the vote is answered, and the
// producer's next transaction votes and applies.  K never steps back.
TEST(CheckedWritesTxnWorker, AbortWhileTheScanWaitsForTheHwWritesNoJudgeRegression) {
    Ki k(/*follower=*/true);
    k.txn(49, {ins(1)});
    k.r.followerFetch();
    (void)k.r.produceFrame({ups(2)}, kLeader, 49, 1, /*txn=*/true);
    k.r.passes(5);
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 49, 0);
    k.r.passes(30);
    ASSERT_FALSE(k.r.voteReply(tok)) << "the scan waits for the HW";
    k.r.marker(49, false);
    int64_t judged = -1;
    bool back = false;
    k.r.onEveryPass([&] {
        if (const VerdictSource* j = k.r.vs->verdictSource(VerdictRig::kSrc, 0)) {
            back = back || j->judgedThrough() < judged;
            judged = std::max(judged, j->judgedThrough());
        }
    });
    k.r.followerFetch();
    ASSERT_TRUE(k.r.driveUntilVoted(tok));
    EXPECT_EQ(k.r.voteReply(tok)->errorCode, 0);
    EXPECT_TRUE(k.r.txnJudgeOffsetsInLog().empty()) << "no J past the abort";
    ASSERT_TRUE(k.r.driveUntilJudgeReaches());
    k.txn(49, {ins(5)});
    const auto v = k.vote(49);
    EXPECT_EQ(v.errorCode, 0);
    EXPECT_EQ(v.code, 0);
    EXPECT_GE(v.judgeOffset, 0);
    k.r.marker(49, true);
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{5}));
    EXPECT_FALSE(back) << "K stepped back";
    k.r.onEveryPass({});
}

// Task 7 (T7 review 1, F6): a vote the full park refuses leaves no scan behind: driven by the other parked votes, it would append a J
// nobody waits for, or sit until the next vote.
TEST(CheckedWritesTxnWorker, RefusedParkLeavesNoScanRegression) {
    Ki k(/*follower=*/true);
    k.txn(54, {ins(1)});
    k.txn(55, {ins(2)});
    std::vector<int64_t> parked;
    for (std::size_t i = 0; i < TxnJudgeState::kMaxParkedVotes; ++i) parked.push_back(k.r.sendVote(VerdictRig::kSrc, 54, 0));
    k.r.passes(5);
    ASSERT_EQ(k.r.worker->parkedVotesForTest(), int64_t(TxnJudgeState::kMaxParkedVotes)) << "J above the HW: every vote waits";
    ASSERT_EQ(k.r.worker->voteScansForTest(), 0u);
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 55, 0);
    ASSERT_TRUE(k.r.voteReply(tok));
    EXPECT_EQ(k.r.voteReply(tok)->errorCode, kNotLeader);
    EXPECT_EQ(k.r.worker->voteScansForTest(), 0u) << "the refused vote's scan was left behind";
    k.r.passes(30);
    EXPECT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u) << "54's J only: none for the refused vote";
    k.r.marker(55, false);   // the judge passes 55's records only once 55 ends, and reaches 54's J
    for (const int64_t t : parked) ASSERT_TRUE(k.r.driveUntilVoted(t));
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 0);
}

// Task 7 (T7 review 1, F4): an atomic batch of txnJudgedRecordsMax + 1 records is refused by produce (rule 10); one that arrives by
// replication is rejected whole (62) instead of stalling the judge, and stays rejected across a failover; the records after it are judged.
TEST(CheckedWritesTxnWorker, AtomicBatchPastTheKnobIsRejectedAcrossAFailoverRegression) {
    Ki k;
    const int32_t knob = k.r.vs->txnJudgedRecordsMax();
    std::vector<Rec> big;
    for (int32_t i = 0; i <= knob; ++i) big.push_back(ups(1000 + uint64_t(i)));
    std::vector<uint8_t> bytes = VerdictRig::batch(big);
    VerdictRig::setBatchAttribute(bytes, rune::protocol::batch::kAttrAtomic);
    const auto refused = k.r.ack(k.r.produceBytes(bytes, kLeader));
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(k.r.worker->hookRefusalsForTest(HookRefusal::AtomicBatchTooLarge), 1u);
    const int64_t base = k.r.appendTxnReplicated(bytes);
    ASSERT_TRUE(k.r.ack(k.r.produceFrame({ups(1)}, kLeader)));
    ASSERT_TRUE(k.r.driveUntilJudged());
    const auto rejectedWhole = [&] {
        for (int64_t o = base; o <= base + knob; ++o) {
            const auto* e = k.r.writeState()->rejected.find(o);
            if (e == nullptr || e->code != kTooLarge) return false;
        }
        return true;
    };
    EXPECT_FALSE(k.r.vs->verdictSource(VerdictRig::kSrc, 0)->stalled());
    EXPECT_TRUE(rejectedWhole());
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{1}));
    EXPECT_EQ(k.stats().atomicTooLarge, 1u);
    k.r.failover();
    ASSERT_TRUE(k.r.ack(k.r.produceFrame({ups(2)}, kLeader)));
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_TRUE(rejectedWhole()) << "the new leader keeps the rejection";
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{1, 2}));
}

namespace {
constexpr int32_t kRej = 0x40000611;
constexpr uint64_t kRejObj = 78;
constexpr int64_t kRaPid = 60;

std::vector<uint8_t> slurp(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

/// A key-index source also judged by a persisted reject view (tag 9 is rejected), with acks: a transaction's inserts vote.
struct Persisted {
    VerdictRig r;
    int64_t cap = 0;
    Persisted() {
        r.createKeyIndex();
        r.enableAcks();
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag = 9;
        r.installRejectView(kRej, kRejObj, rule, "rej", /*persisted=*/true);
        r.passes(50);
        cap = r.vs->maxRecordsPerPassForTest(kRej, 0);
    }
    /// `n` plain records from id `from`, in batches of 500.
    static std::vector<std::vector<uint8_t>> plain(uint64_t from, int64_t n) {
        std::vector<std::vector<uint8_t>> out;
        for (int64_t i = 0; i < n; i += 500) {
            std::vector<Rec> recs;
            for (int64_t k = i; k < std::min(n, i + 500); ++k) recs.push_back(Rec{from + static_cast<uint64_t>(k), 1});
            out.push_back(VerdictRig::batch(recs));
        }
        return out;
    }
    /// The batches as the previous leader wrote them; returns the first one's base offset.
    int64_t replicated(std::vector<std::vector<uint8_t>> batches) {
        const int64_t base = r.leo();
        r.demote();
        r.replicate(std::move(batches));
        r.promote();
        for (int i = 0; i < 100 && r.writeState() == nullptr; ++i) r.pass();
        r.passes(100);   // the leadership scan rebuilds the open transactions
        return base;
    }
    [[nodiscard]] int64_t durable() const { return r.agent->lastDurableOffset(kRej, 0); }
};
}  // namespace

// P11-8 (a): while the judge runs ahead of a voted transaction T to its J, the member pushes its checkpoint at O inside T (the pass
// cap counts from its last push).  A restart from that checkpoint, after T committed, backfills T's records below O from J: the
// replay applies T at its TxnJudge from firstOffset (D2; re-pinned in Task 8, the separate backfill pass is gone).
TEST(CheckedWritesTxnWorker, RunAheadCheckpointIsBackfilledOnReplayRegression) {
    Persisted p;
    ASSERT_GT(p.cap, 0);
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ins(1001)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, 3 * p.cap)) batches.push_back(std::move(b));
    batches.push_back(VerdictRig::batch({ins(1002)}, kRaPid, 1, {}, /*txn=*/true));
    const int64_t first = p.replicated(std::move(batches));
    const int64_t tok = p.r.sendVote(VerdictRig::kSrc, kRaPid, 0);
    std::vector<uint8_t> inside;
    int64_t insideAt = -1;
    for (int i = 0; i < 2000 && !p.r.voteReply(tok); ++i) {
        p.r.pass();
        const auto js = p.r.txnJudgeOffsetsInLog();
        if (insideAt < 0 && js.size() == 1 && p.durable() > first && p.durable() < js[0]) {
            insideAt = p.durable();
            inside = slurp(viewCheckpointFilePath(p.r.snapDir(), kRej, 0));
        }
    }
    ASSERT_TRUE(p.r.voteReply(tok));
    ASSERT_EQ(p.r.voteReply(tok)->code, 0);
    ASSERT_GE(insideAt, 0) << "no checkpoint was pushed while the judge ran ahead of T";
    p.r.marker(kRaPid, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    ASSERT_TRUE(p.r.hasRow(kRej, 1001));
    ASSERT_TRUE(p.r.hasRow(kRej, 1002));
    const auto rows = [&] {
        std::vector<uint64_t> out;
        const ViewInstance* inst = p.r.vs->instanceForTest(kRej, 0);
        for (uint64_t k = 0; k < 10000 + static_cast<uint64_t>(3 * p.cap) + 10; ++k)
            if (inst->table.get(k) != nullptr) out.push_back(k);
        return std::make_pair(out, inst->table.size());
    };
    const auto leader = rows();
    // The crash took every later checkpoint: the file holds the run-ahead one.
    p.r.agent.reset();
    {
        std::ofstream out(viewCheckpointFilePath(p.r.snapDir(), kRej, 0), std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(inside.data()), static_cast<std::streamsize>(inside.size()));
    }
    p.r.agent = std::make_unique<ViewSnapshotIoAgent>(p.r.channel, p.r.snapDir(), /*fsyncMs=*/0, p.r.backupPolicy);
    p.r.restart();
    p.r.passes(300);
    ASSERT_TRUE(p.r.driveUntilJudged());
    ASSERT_NE(p.r.stats(), nullptr);
    EXPECT_EQ(p.r.stats()->verdictCheckpointsDiscarded, 0u) << "the markers cover the run-ahead checkpoint: it is kept";
    EXPECT_EQ(p.r.writeState()->txn.stats.checkpointBackfills, 1u);
    EXPECT_TRUE(p.r.hasRow(kRej, 1001)) << "T's record below the checkpoint is backfilled from J";
    EXPECT_TRUE(p.r.hasRow(kRej, 1002));
    EXPECT_TRUE(p.r.hasRow(kRej, 10000));
    EXPECT_TRUE(p.r.hasRow(kRej, 10000 + static_cast<uint64_t>(3 * p.cap) - 1));
    EXPECT_EQ(rows(), leader) << "the refold equals the leader's table";
}

// REGRESSION (found by R11b; P11-8): the checkpoint cap holds the judge at the markers' coverage plus the cap, and a marker covers
// only a stretch with a judged record.  A stretch past the cap with nothing to judge (an aborted transaction, then its marker) held
// the judge there for good, and every later record went unjudged.  While nothing judged waits for a marker the cap counts from the
// judge's cursor: the members' tables did not change past the coverage.
TEST(CheckedWritesTxnWorkerRegression, UnjudgedStretchPastTheCapNeverHoldsTheJudgeRegression) {
    VerdictRig r;
    r.createKeyIndex();
    r.setViewPassCap(3);   // the change and plan records at 0 and 1 stay within it
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::TagEquals;
    rule.tag = 9;
    r.installRejectView(kRej, kRejObj, rule, "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produceBatch(VerdictRig::batch({ups(1)})), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    (void)r.appendTxnReplicated(VerdictRig::batch({ups(2), ups(3), ups(4), ups(6)}, kRaPid, 0, {}, /*txn=*/true));
    r.marker(kRaPid, false);
    ASSERT_EQ(r.produceBatch(VerdictRig::batch({ups(5)})), 0);
    EXPECT_TRUE(r.driveUntilJudged());
    EXPECT_TRUE(r.hasRow(kRej, 5)) << "the record past the aborted transaction is judged and folded";
    EXPECT_FALSE(r.hasRow(kRej, 3));
}

// REGRESSION (found by R11b; design 3.1): the judge applies an ordinary transaction T at its marker M while a later one B, opened
// inside T's span, still waits for its own: the markers' coverage stops at B's first record, inside (first, M].  A failover then
// restarted the judge there, below M but above every record of T, so T's marker matched nothing and T was lost from the rebuilt
// member.  The coverage never lies inside an applied ordinary span: a hold inside one counts from the span's first record.
TEST(CheckedWritesTxnWorkerRegression, CoverageNeverInsideAnAppliedOrdinaryTxnRegression) {
    VerdictRig r;
    r.createKeyIndex();
    r.setViewPassCap(3);
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::TagEquals;
    rule.tag = 9;
    r.installRejectView(kRej, kRejObj, rule, "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produceBatch(VerdictRig::batch({ups(1)})), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    // Written by a previous leader, so the judge meets them all at once: T [first, M], B opened inside it, then B's marker far past M.
    namespace tx = rune::transaction;
    const int64_t first = r.leo();
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ups(2), ups(3)}, kRaPid, 0, {}, /*txn=*/true),
                                              VerdictRig::batch({ups(4)}, kRaPid + 1, 0, {}, /*txn=*/true),
                                              tx::ControlMarker::build(kRaPid, 0, tx::ControlType::Commit, 0)};
    for (uint64_t k = 10; k < 30; ++k) batches.push_back(VerdictRig::batch({ups(k)}));
    batches.push_back(tx::ControlMarker::build(kRaPid + 1, 0, tx::ControlType::Commit, 0));
    r.demote();
    r.replicate(std::move(batches));
    r.promoteWithoutPass();
    const int64_t marker = first + 3, bMarker = marker + 21;
    const ViewInstance* member = r.vs->instanceForTest(kRej, 0);
    for (int i = 0; i < 200 && member->foldCursor <= marker; ++i) r.pass();
    ASSERT_GT(member->foldCursor, marker) << "T is applied at its marker";
    ASSERT_LE(member->foldCursor, bMarker) << "while B still waits for its own";
    const int64_t covered = r.vs->verdictSource(VerdictRig::kSrc, 0)->judgedThrough();
    EXPECT_TRUE(covered <= first || covered > marker) << "the coverage " << covered << " is not inside T's span";
    r.demotePromote();
    EXPECT_TRUE(r.driveUntilJudged());
    EXPECT_TRUE(r.hasRow(kRej, 2)) << "the member rebuilt after the failover holds T";
    EXPECT_TRUE(r.hasRow(kRej, 3));
    EXPECT_TRUE(r.hasRow(kRej, 4));
    EXPECT_TRUE(r.hasRow(kRej, 29));
}

// REGRESSION (design 3.1): as above with the later transaction D voted: its TxnJudge defers it, so the judge passes D and applies T
// at M while D holds the coverage at its first record, inside T's span.
TEST(CheckedWritesTxnWorkerRegression, CoverageNeverInsideAnAppliedOrdinaryTxnUnderAVotedTxnRegression) {
    Ki k;
    const int64_t first = k.r.leo();
    k.r.demote();
    k.r.replicate({VerdictRig::batch({ups(2), ups(3)}, kRaPid, 0, {}, /*txn=*/true), VerdictRig::batch({ins(4)}, kRaPid + 1, 0, {}, /*txn=*/true)});
    k.r.promote();
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, kRaPid + 1, 0);
    k.r.marker(kRaPid, true);
    ASSERT_TRUE(k.r.driveUntilVoted(tok));
    ASSERT_EQ(k.r.voteReply(tok)->code, 0);
    for (int i = 0; i < 200 && !k.r.keyIndexKeys().contains(2); ++i) k.r.pass();
    ASSERT_TRUE(k.r.keyIndexKeys().contains(2)) << "T is applied at its marker";
    ASSERT_FALSE(k.r.keyIndexKeys().contains(4)) << "while D waits for its own";
    const int64_t covered = k.r.vs->verdictSource(VerdictRig::kSrc, 0)->judgedThrough();
    EXPECT_LE(covered, first) << "the coverage is not inside T's span";
    k.r.demotePromote();
    k.r.marker(kRaPid + 1, true);
    EXPECT_TRUE(k.r.driveUntilJudged());
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<uint64_t>{2, 3, 4})) << "the member rebuilt after the failover holds T";
}

// REGRESSION (Task 8 step 6, design 3.1, the activation invariant): an ordinary transaction (no vote, so no J to backfill from)
// holds every member checkpoint at or below its first record until its marker.  (Its traffic stays within one delta frame; more is
// step 7's multi-frame delta.)  So a recovered K
// never lies in an ordinary span (first, M], and a recovering member starts with an empty fence.
TEST(CheckedWritesTxnWorkerRegression, MemberCheckpointNeverInsideAnOrdinaryTxnRegression) {
    Persisted p;
    ASSERT_GT(p.cap, 0);
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ups(1001)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, p.cap / 4)) batches.push_back(std::move(b));
    batches.push_back(VerdictRig::batch({ups(1002)}, kRaPid, 1, {}, /*txn=*/true));
    const int64_t first = p.replicated(std::move(batches));
    std::vector<int64_t> durable;
    const auto watch = [&](int n) {
        for (int i = 0; i < n; ++i) {
            p.r.pass();
            if (durable.empty() || durable.back() != p.durable()) durable.push_back(p.durable());
        }
    };
    watch(300);
    const int64_t m = p.r.leo();
    p.r.marker(kRaPid, true);
    watch(300);
    ASSERT_TRUE(p.r.driveUntilJudged());
    EXPECT_TRUE(p.r.hasRow(kRej, 1001));
    EXPECT_TRUE(p.r.hasRow(kRej, 1002));
    ASSERT_GT(durable.back(), m) << "checkpoints resume past the marker (the vacuity guard)";
    for (const int64_t k : durable) EXPECT_FALSE(k > first && k <= m) << k << " lies in the ordinary span (" << first << ", " << m << "]";
}

// P11-8 (b): while a voted transaction is Locked with its J below the member's cursor, no checkpoint is pushed (counted), however
// much traffic follows; its commit lets them go again.
TEST(CheckedWritesTxnWorker, NoCheckpointPushWhileLockedRegression) {
    Persisted p;
    const int64_t first = p.replicated({VerdictRig::batch({ins(1001)}, kRaPid, 0, {}, /*txn=*/true)});
    ASSERT_TRUE(p.r.driveUntilVoted(p.r.sendVote(VerdictRig::kSrc, kRaPid, 0)));
    ASSERT_EQ(p.r.writeState()->txn.find(kRaPid)->phase, TxnPhase::Locked);
    const int64_t j = p.r.txnJudgeOffsetsInLog().at(0);
    std::vector<int64_t> corrs;
    for (uint64_t i = 0; i < 20; ++i) corrs.push_back(p.r.produceFrame({Rec{20000 + i, 1}}, kLeader));
    p.r.passes(300);
    EXPECT_GT(p.r.vs->verdictSource(VerdictRig::kSrc, 0)->judgedThrough(), first - 1);
    EXPECT_TRUE(p.r.hasRow(kRej, 20019)) << "the traffic is folded";
    EXPECT_LE(p.durable(), first) << "no checkpoint past T's first record while T is Locked";
    EXPECT_GT(p.r.writeState()->txn.stats.vetoedCheckpointPushes, 0u);
    p.r.marker(kRaPid, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    p.r.passes(50);
    EXPECT_GT(p.durable(), j) << "the commit lets the checkpoints go";
}

// The judge runs to an ordinary transaction's marker in one go, and a member cannot checkpoint inside it (no J to backfill from):
// the delta at the marker outgrows one ring message.  REGRESSION (P11-9, D4; re-pins OversizedDeltaIsHeldNotClaimed, T7 fix 1 and
// fix 2): it goes out as a run of part frames, is acked as one checkpoint without a restart, and a restart recovers from it.
TEST(CheckedWritesTxnWorkerRegression, OversizedDeltaPushesAcrossFramesRegression) {
    Persisted p;
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ups(1001)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, 3 * p.cap)) batches.push_back(std::move(b));
    (void)p.replicated(std::move(batches));
    const int64_t m = p.r.leo();
    p.r.marker(kRaPid, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    p.r.passes(50);
    EXPECT_TRUE(p.r.hasRow(kRej, 1001));
    EXPECT_GT(p.durable(), m) << "the delta past one message is durable without a restart";
    const int64_t durable = p.durable();
    p.r.restart();
    for (int i = 0; i < 20 && (p.r.vs->instanceForTest(kRej, 0) == nullptr || p.r.vs->instanceForTest(kRej, 0)->foldCursor <= 0); ++i) p.r.pass();
    ASSERT_NE(p.r.vs->instanceForTest(kRej, 0), nullptr);
    EXPECT_GE(p.r.vs->instanceForTest(kRej, 0)->foldCursor, durable) << "recovered from the multi-frame checkpoint, not refolded";
    p.r.passes(300);
    ASSERT_TRUE(p.r.driveUntilJudged());
    EXPECT_EQ(p.r.vs->instanceForTest(kRej, 0)->counters.checkpointDiscards, 0u);
    EXPECT_EQ(p.r.agent->tailTrimsForTest(), 0);
    EXPECT_TRUE(p.r.hasRow(kRej, 1001));
    EXPECT_TRUE(p.r.hasRow(kRej, 10000 + static_cast<uint64_t>(3 * p.cap) - 1));
}

// T7 review 2, N2 (re-pinned in Task 8, D4): after a delta past one message, the member keeps pushing, so the run-ahead cap counts
// from its push past the first transaction: a voted transaction whose J lies more than one cap past that is judged, answered and
// applied without a restart.
TEST(CheckedWritesTxnWorker, OversizedDeltaNeverHoldsTheRunAheadRegression) {
    Persisted p;
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ups(1001)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, 3 * p.cap)) batches.push_back(std::move(b));
    (void)p.replicated(std::move(batches));
    const int64_t m = p.r.leo();
    p.r.marker(kRaPid, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    p.r.passes(50);
    batches = {VerdictRig::batch({ins(2001)}, kRaPid + 1, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(30000, 2 * p.cap)) batches.push_back(std::move(b));
    batches.push_back(VerdictRig::batch({ins(2002)}, kRaPid + 1, 1, {}, /*txn=*/true));
    (void)p.replicated(std::move(batches));
    ASSERT_GT(p.durable(), m) << "the member pushed past the first transaction";
    const int64_t tok = p.r.sendVote(VerdictRig::kSrc, kRaPid + 1, 0);
    for (int i = 0; i < 2000 && !p.r.voteReply(tok); ++i) p.r.pass();
    ASSERT_TRUE(p.r.voteReply(tok)) << "the judge never reached T's J";
    EXPECT_EQ(p.r.voteReply(tok)->code, 0);
    p.r.marker(kRaPid + 1, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    EXPECT_TRUE(p.r.hasRow(kRej, 2001));
    EXPECT_TRUE(p.r.hasRow(kRej, 2002));
    EXPECT_TRUE(p.r.hasRow(kRej, 30000 + static_cast<uint64_t>(2 * p.cap) - 1));
}

// P11-8 (c), fix round 1 F5: the cleaner keeps every TxnJudge at or above the lowest checkpoint a persisted member restarts from,
// even once the committed markers have passed the J: a member whose checkpoint lags inside T backfills from that J.
TEST(CheckedWritesTxnWorker, LaggingCheckpointKeepsTheTxnJudgeItNeedsRegression) {
    Persisted p;
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ins(1001)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, 3 * p.cap)) batches.push_back(std::move(b));
    batches.push_back(VerdictRig::batch({ins(1002)}, kRaPid, 1, {}, /*txn=*/true));
    const int64_t first = p.replicated(std::move(batches));
    const int64_t tok = p.r.sendVote(VerdictRig::kSrc, kRaPid, 0);
    for (int i = 0; i < 2000 && !p.r.voteReply(tok) && p.durable() <= first; ++i) p.r.pass();
    const int64_t lagging = p.durable();
    ASSERT_GT(lagging, first) << "a run-ahead checkpoint inside T";
    p.r.agent.reset();   // the IO agent stalls: no later checkpoint becomes durable
    ASSERT_TRUE(p.r.driveUntilVoted(tok));
    const int64_t j = p.r.txnJudgeOffsetsInLog().at(0);
    ASSERT_GT(j, lagging);
    p.r.marker(kRaPid, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    ASSERT_GT(p.r.writeState()->markers.committedThrough(), j) << "the markers alone would let the J go";
    EXPECT_EQ(p.r.replica->cleanerTxnJudgeBound(), lagging) << "the J the lagging checkpoint backfills from is kept";
}

// P11-1 on a constrained aggregate: the yes is answered with J and nothing is applied before the marker; the commit applies the
// transaction's records to account_stats in one step.
TEST(CheckedWritesTxnWorker, AccountStatsVoteYesThenCommitAppliesInOneStep) {
    VerdictRig r;
    r.createQlTopic(VerdictRig::kCharacterTopic, false);
    r.enableAcks();
    r.passes(20);
    r.createConstrainedView(ConstrainedView::AccountStats, 2);
    r.passes(20);
    const auto characters = [&](uint64_t user) -> int64_t {
        const ViewInstance* i = r.constrainedInstance(ConstrainedView::AccountStats);
        const uint8_t* row = i == nullptr ? nullptr : i->table.get(user);
        if (row == nullptr) return -1;
        const auto& live = r.constrained.at(ConstrainedView::AccountStats);
        const auto& fs = live.comp.outputDescriptor.fields;
        for (std::size_t f = 0; f < fs.size(); ++f)
            if (fs[f].name == "characters") return rune::view::readSlot(row, live.comp.rowLayout, static_cast<uint16_t>(f)).i;
        return -2;
    };
    std::vector<VerdictRig::SrcRecord> recs;
    for (uint64_t id : {1, 2}) recs.push_back(VerdictRig::srcOf(VerdictRig::Char{.id = id, .userId = 1, .name = "c" + std::to_string(id)}));
    (void)r.appendTxnReplicated(r.sourceBatch(recs, 31, 0, /*txn=*/true));
    r.passes(30);
    const int64_t tok = r.sendVote(VerdictRig::kSrc, 31, 0);
    ASSERT_TRUE(r.driveUntilVoted(tok));
    const auto v = r.voteReply(tok);
    ASSERT_TRUE(v);
    EXPECT_EQ(v->errorCode, 0);
    EXPECT_EQ(v->code, 0);
    EXPECT_EQ(r.txnJudgeOffsetsInLog(), std::vector<int64_t>{v->judgeOffset});
    EXPECT_EQ(characters(1), -1) << "nothing applied before the marker";
    r.marker(31, true);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(characters(1), 2);
    EXPECT_EQ(r.writeState()->txn.stats.votesYes, 1u);
}

// R04a: a source dropped while a voted transaction is Locked, then a failover: the vote parked on another transaction's J is
// answered NotLeader, and neither the overlay nor a parked vote nor a scan is left behind.
TEST(CheckedWritesTxnWorker, DropWhileLockedThenFailoverLeaksNothing) {
    Ki k;
    auto& pool = k.r.vs->txnOverlaysForTest();
    k.txn(46, {ins(1)});
    ASSERT_EQ(k.vote(46).code, 0);
    ASSERT_EQ(k.r.writeState()->txn.find(46)->phase, TxnPhase::Locked);
    k.r.holdJudge(true);
    k.txn(47, {ins(2)});
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 47, 0);
    k.r.passes(10);
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 2u);
    ASSERT_FALSE(k.r.voteReply(tok)) << "parked: the judge has not judged its J";
    ASSERT_EQ(k.r.worker->parkedVotesForTest(), 1);
    k.r.vs->dropSourceTopic(VerdictRig::kSrc);
    WorkerOp d;
    d.type = WorkerOp::Type::TopicDropped;
    d.topicId = VerdictRig::kSrc;
    k.r.send(std::move(d));
    ASSERT_TRUE(k.r.voteReply(tok));
    EXPECT_EQ(k.r.voteReply(tok)->errorCode, kNotLeader);
    EXPECT_EQ(pool.freeCount(), rune::view::kTxnOverlaysPerWorker);
    k.r.holdJudge(false);
    k.r.failover();
    k.r.passes(30);
    EXPECT_EQ(k.r.worker->parkedVotesForTest(), 0);
    EXPECT_EQ(k.r.worker->voteScansForTest(), 0u);
}

// T7 review 3, N3: an ordinary transaction whose segment waits for a voted one's marker holds the worker's one-step set; a demotion
// gives it back (the worker no longer drives that judge), so the worker's other sources never wait for it.
TEST(CheckedWritesTxnWorker, DemotedJudgeReleasesTheOneStepSetRegression) {
    Ki k;
    auto& pool = k.r.vs->txnOverlaysForTest();
    k.txn(55, {ups(4)});
    k.txn(66, {ins(4)});
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 66, 0);
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
    k.r.marker(55, true);   // above 66's J: the judge stands at 55's first record until then
    ASSERT_TRUE(k.r.driveUntilVoted(tok));
    ASSERT_EQ(k.r.voteReply(tok)->code, 0);
    k.r.passes(30);
    ASSERT_EQ(k.r.writeState()->txn.find(66)->phase, TxnPhase::Locked);
    ASSERT_TRUE(pool.oneStepHeld()) << "55's segment waits for 66's marker";
    k.r.demote();
    EXPECT_FALSE(pool.oneStepHeld());
}

// A voted transaction aborted at its marker: a persisted member restarted from its checkpoint refolds to the leader's rows.
TEST(CheckedWritesTxnWorker, VotedAbortedRefoldsEqual) {
    Persisted p;
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ins(1001), ins(1002)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, 600)) batches.push_back(std::move(b));
    (void)p.replicated(std::move(batches));
    ASSERT_TRUE(p.r.driveUntilVoted(p.r.sendVote(VerdictRig::kSrc, kRaPid, 0)));
    p.r.marker(kRaPid, false);
    for (uint64_t i = 0; i < 5; ++i) (void)p.r.produceFrame({Rec{20000 + i, 1}}, kLeader);
    ASSERT_TRUE(p.r.driveUntilJudged());
    p.r.passes(100);
    const auto rows = [&] {
        std::vector<bool> out;
        for (uint64_t k : {1001, 1002, 10000, 10599, 20000, 20004}) out.push_back(p.r.hasRow(kRej, k));
        return std::make_pair(out, p.r.vs->instanceForTest(kRej, 0)->table.size());
    };
    const auto leader = rows();
    EXPECT_EQ(leader.first, (std::vector<bool>{false, false, true, true, true, true}));
    ASSERT_GT(p.durable(), 0);
    p.r.restart();
    p.r.passes(300);
    ASSERT_TRUE(p.r.driveUntilJudged());
    EXPECT_EQ(rows(), leader);
}

// Contract addition 19: one transaction over two topics.  Both vote yes: the commit applies it on both.  One votes no: it aborts on
// both, and the other applies nothing.
TEST(CheckedWritesTxnWorker, TxnPairCommitsOnBothOrNeither) {
    TxnPair t;
    t.write(70, {ins(1)}, {ins(2)});
    EXPECT_TRUE(t.end(70));
    EXPECT_EQ(t.a.keyIndexKeys(), (std::set<uint64_t>{1}));
    EXPECT_EQ(t.b.keyIndexKeys(), (std::set<uint64_t>{2}));
    t.write(71, {ins(1)}, {ins(3)});   // 1 exists on a: a votes KeyExists
    EXPECT_FALSE(t.end(71));
    EXPECT_EQ(t.a.keyIndexKeys(), (std::set<uint64_t>{1}));
    EXPECT_EQ(t.b.keyIndexKeys(), (std::set<uint64_t>{2})) << "b's yes is aborted with a's no";
}

// Design L6: after the partition's vote, a producer (and epoch) that cached the partition as verified may not append to its
// transaction until the marker lands; the judge's yes or no covers only the rows below its TxnJudge.
namespace {
constexpr int16_t kInvalidTxnState = static_cast<int16_t>(pc::ErrorCode::InvalidTxnState);
constexpr int16_t kZombieCode = static_cast<int16_t>(pc::ErrorCode::InvalidProducerEpoch);

int16_t txnProduce(Ki& k, int64_t pid, uint64_t id, int32_t seq, int16_t epoch = 0) {
    const auto a = k.r.ack(k.r.produceFrame({ups(id)}, kLeader, pid, seq, /*txn=*/true, epoch));
    EXPECT_TRUE(a.has_value());
    return a ? a->code : int16_t{-1};
}
}  // namespace

TEST(CheckedWritesTxnVoteFreezeRegression, AppendAfterYesVoteIsRefusedUntilTheMarker) {
    Ki k;
    k.txn(61, {ins(1)});
    ASSERT_EQ(k.vote(61).errorCode, 0);
    const int64_t leo = k.r.replica->logEndOffset();
    EXPECT_EQ(txnProduce(k, 61, 7, 1), kInvalidTxnState);
    EXPECT_EQ(k.r.replica->logEndOffset(), leo) << "the refused row is not in the log";
    EXPECT_EQ(k.r.worker->txnBatchesRefusedAfterVoteForTest(), 1u);
    k.r.marker(61, true);
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_EQ(txnProduce(k, 61, 8, 1), 0) << "the next transaction appends once the marker landed";
    EXPECT_GT(k.r.replica->logEndOffset(), leo);
    EXPECT_EQ(txnProduce(k, 61, 9, 2), 0) << "the judged transaction's entry does not freeze the next one";
}

TEST(CheckedWritesTxnVoteFreezeRegression, AppendAfterNoVoteIsRefused) {
    Ki k;
    k.txn(62, {ins(1), ins(1)});   // the second insert of the key: KeyExists
    const auto v = k.vote(62);
    ASSERT_EQ(v.errorCode, 0);
    ASSERT_EQ(v.code, kKeyExists);
    const int64_t leo = k.r.replica->logEndOffset();
    EXPECT_EQ(txnProduce(k, 62, 7, 1), kInvalidTxnState);
    EXPECT_EQ(k.r.replica->logEndOffset(), leo);
    k.r.marker(62, false);
    ASSERT_TRUE(k.r.driveUntilJudgeReaches());
    EXPECT_EQ(txnProduce(k, 62, 8, 1), 0);
}

TEST(CheckedWritesTxnVoteFreezeRegression, OtherProducerAppendsDuringAVote) {
    Ki k;
    k.txn(63, {ins(1)});
    ASSERT_EQ(k.vote(63).errorCode, 0);
    EXPECT_EQ(txnProduce(k, 64, 7, 0), 0) << "another producer's transaction";
    const auto plain = k.r.ack(k.r.produceFrame({ups(9)}, kLeader));
    ASSERT_TRUE(plain.has_value());
    EXPECT_EQ(plain->code, 0) << "a non-transactional append";
    EXPECT_EQ(txnProduce(k, 63, 8, 1), kInvalidTxnState);
}

TEST(CheckedWritesTxnVoteFreezeRegression, NewLeaderRefusesAppendAfterVoteInLog) {
    Ki k(/*follower=*/true);
    k.txn(65, {ins(1)});
    ASSERT_EQ(k.vote(65).errorCode, 0);
    k.r.followerFetch();
    k.r.failover();   // another replica takes over with the log up to the HW: J is in it, the marker is not
    k.r.passes(60);
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
    EXPECT_EQ(txnProduce(k, 65, 7, 1), kInvalidTxnState);
}

// A yes that wrote no TxnJudge (nothing to judge) freezes the transaction too: the marker-time judge would otherwise reject rows the
// commit decision never covered.
TEST(CheckedWritesTxnVoteFreezeRegression, AppendAfterAVoteWithoutAJudgeRecordIsRefused) {
    Ki k;
    ASSERT_EQ(txnProduce(k, 66, 4, 0), 0);
    const auto v = k.vote(66);
    ASSERT_EQ(v.errorCode, 0);
    ASSERT_EQ(v.judgeOffset, -1) << "a non-voting yes";
    const int64_t leo = k.r.replica->logEndOffset();
    EXPECT_EQ(txnProduce(k, 66, 5, 1), kInvalidTxnState);
    EXPECT_EQ(k.r.replica->logEndOffset(), leo);
    k.r.marker(66, true);
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_EQ(txnProduce(k, 66, 6, 1), 0);
}

// A new leader freezes from the TxnJudge in the log even while the judge has not taken the record into its table.
TEST(CheckedWritesTxnVoteFreezeRegression, FreezeComesFromTheLogNotFromTheJudgeTable) {
    Ki k(/*follower=*/true);
    k.txn(67, {ins(1)});
    ASSERT_EQ(k.vote(67).errorCode, 0);
    k.r.followerFetch();
    k.r.failover();
    k.r.passes(60);
    ASSERT_EQ(k.r.txnJudgeOffsetsInLog().size(), 1u);
        PartitionWriteState* ws = const_cast<PartitionWriteState*>(k.r.writeState());
    ws->txn.releaseOverlays([](int32_t) {});   // truncateFrom's precondition; the pool slot stays with the finished test
    ws->txn.truncateFrom(0);   // as if the table had refused J
    ASSERT_EQ(k.r.writeState()->txn.find(67), nullptr);
    EXPECT_EQ(txnProduce(k, 67, 7, 1), kInvalidTxnState);
}

// Only the voted epoch is frozen; an older epoch is fenced as before.
TEST(CheckedWritesTxnVoteFreezeRegression, OlderEpochIsFencedNotFrozen) {
    Ki k;
    (void)k.r.appendTxnReplicated(VerdictRig::batch({ins(1)}, 68, 0, {}, /*txn=*/true, /*epoch=*/1));
    k.r.passes(30);
    const int64_t tok = k.r.sendVote(VerdictRig::kSrc, 68, 1);
    ASSERT_TRUE(k.r.driveUntilVoted(tok));
    ASSERT_EQ(k.r.voteReply(tok)->errorCode, 0);
    EXPECT_EQ(txnProduce(k, 68, 7, 1, /*epoch=*/1), kInvalidTxnState) << "the voted epoch";
    const int16_t zombie = txnProduce(k, 68, 8, 1, /*epoch=*/0);
    EXPECT_EQ(zombie, kZombieCode) << "fenced as before";
}

// A vote of an older epoch (a zombie coordinator's resend) answers, but it does not lift the freeze of the newer epoch.
TEST(CheckedWritesTxnVoteFreezeRegression, StaleVoteOfAnOlderEpochKeepsTheFreeze) {
    Ki k;
    (void)k.r.appendTxnReplicated(VerdictRig::batch({ins(1)}, 69, 0, {}, /*txn=*/true, /*epoch=*/1));
    k.r.passes(30);
    const int64_t fresh = k.r.sendVote(VerdictRig::kSrc, 69, 1);
    ASSERT_TRUE(k.r.driveUntilVoted(fresh));
    const int64_t stale = k.r.sendVote(VerdictRig::kSrc, 69, 0);
    ASSERT_TRUE(k.r.driveUntilVoted(stale));
    const int64_t leo = k.r.replica->logEndOffset();
    EXPECT_EQ(txnProduce(k, 69, 7, 1, /*epoch=*/1), kInvalidTxnState);
    EXPECT_EQ(k.r.replica->logEndOffset(), leo);
}

// A vote that wrote no TxnJudge leaves nothing in the log for a new leader to see: the open transaction it inherits is verified
// again by the producer's next batch (answered ConcurrentTransactions while the ask is out) instead of being admitted.
TEST(CheckedWritesTxnVoteFreezeRegression, NewLeaderVerifiesAgainAfterAVoteWithoutAJudgeRecord) {
    Ki k;
    ASSERT_EQ(txnProduce(k, 70, 4, 0), 0);
    ASSERT_EQ(k.vote(70).judgeOffset, -1) << "a non-voting yes";
    k.r.failover();
    k.r.passes(60);
    k.r.worker->setTxnVerification(true);
    const int64_t leo = k.r.replica->logEndOffset();
    EXPECT_EQ(txnProduce(k, 70, 5, 1), static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(k.r.replica->logEndOffset(), leo);
    EXPECT_EQ(k.r.worker->txnVerifyRequestsSentForTest(), 1u);
    k.r.answerVerify(70, 0, 0);
    EXPECT_EQ(txnProduce(k, 70, 5, 1), 0) << "the retry is admitted once the coordinator confirmed";
    EXPECT_GT(k.r.replica->logEndOffset(), leo);
    EXPECT_EQ(txnProduce(k, 70, 6, 2), 0) << "the open transaction is verified for good";
    EXPECT_EQ(k.r.worker->txnVerifyRequestsSentForTest(), 1u);
}

// The hook's facts come from the batch header and the node's knob: an atomic batch past the knob is refused, and the atomic bit of a
// transactional batch is ignored (the transaction is judged at its vote).
TEST(CheckedWritesTxnWorker, AtomicFactsComeFromTheHeaderAndTheKnob) {
    Ki k;
    constexpr int32_t kKnob = 16;
    k.r.worker->setTxnJudgedRecordsMax(kKnob);
    std::vector<Rec> recs;
    for (int32_t i = 0; i <= kKnob; ++i) recs.push_back(ups(2000 + uint64_t(i)));
    std::vector<uint8_t> atomic = VerdictRig::batch(recs);
    VerdictRig::setBatchAttribute(atomic, rune::protocol::batch::kAttrAtomic);
    const auto refused = k.r.ack(k.r.produceBytes(atomic, kLeader));
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_TRUE(k.r.worker->lastWriteFactsForTest().atomicBatch);
    EXPECT_EQ(k.r.worker->lastWriteFactsForTest().txnJudgedRecordsMax, kKnob);
    std::vector<uint8_t> txn = VerdictRig::batch(recs, 31, 0, {}, /*txn=*/true);
    VerdictRig::setBatchAttribute(txn, rune::protocol::batch::kAttrAtomic);
    const auto accepted = k.r.ack(k.r.produceBytes(txn, kLeader, 31, 0));
    ASSERT_TRUE(accepted);
    EXPECT_EQ(accepted->code, 0);
    EXPECT_FALSE(k.r.worker->lastWriteFactsForTest().atomicBatch);
    EXPECT_TRUE(k.r.worker->lastWriteFactsForTest().transactional);
}

// P6-13: a voted-no transaction's key-0 ids are burned: the next create is above them, the key index holds neither, nothing is delivered.
TEST(CheckedWritesTxnWorker, NoVoteBurnsTheTransactionsIds) {
    VerdictRig r;
    r.createCharacterTopic();
    r.enableAcks();
    r.passes(20);
    r.createConstrainedView(ConstrainedView::AccountStats, 12);
    const auto chars = [](int from, int to) {
        std::vector<VerdictRig::SrcRecord> out;
        for (int i = from; i < to; ++i) out.push_back(VerdictRig::srcOf(VerdictRig::Char{.userId = 11, .name = "c" + std::to_string(i), .intent = 1}));
        return out;
    };
    constexpr uint8_t kAllView = pc::kAcksWireView | pc::kAcksWireAll;
    const auto first = r.ack(r.appendRecords(chars(0, 11), kAllView, 40, 0));
    ASSERT_TRUE(first);
    ASSERT_EQ(first->code, 0);
    ASSERT_TRUE(first->verdicts.empty());
    const auto txnRecords = chars(20, 22);
    const auto txn = r.ack(r.appendRecords(txnRecords, kAllView, 77, 0, /*txn=*/true));
    ASSERT_TRUE(txn);
    ASSERT_EQ(txn->code, 0);
    ASSERT_NE(txn->assignedBase, 0u);
    const auto end = r.commitTxn(77);
    EXPECT_FALSE(end.committed);
    ASSERT_EQ(end.votes.size(), 1u);
    EXPECT_EQ(end.votes.front().second.code, 55);
    ASSERT_TRUE(r.driveUntilJudgeReaches());
    const auto next = r.ack(r.appendRecords(chars(30, 31), kAllView, 40, 11));
    ASSERT_TRUE(next);
    ASSERT_EQ(next->code, 0);
    EXPECT_GT(next->assignedBase, txn->assignedBase + 1) << "both of the aborted transaction's ids are burned";
    ASSERT_TRUE(r.driveUntilJudgeReaches());
    const std::set<uint64_t> keys = r.keyIndexKeys();
    EXPECT_EQ(keys.count(txn->assignedBase), 0u);
    EXPECT_EQ(keys.count(txn->assignedBase + 1), 0u);
    EXPECT_EQ(keys.count(next->assignedBase), 1u);
    const int64_t sub = r.subscribe(SubKind::Raw, 0);
    const int64_t txnEnd = txn->baseOffset + static_cast<int64_t>(txnRecords.size());
    bool nextDelivered = false;
    for (const auto& d : r.collect(sub).records) {
        EXPECT_TRUE(d.offset < txn->baseOffset || d.offset >= txnEnd) << "an aborted record was delivered at " << d.offset;
        nextDelivered |= d.offset == next->baseOffset;
    }
    EXPECT_TRUE(nextDelivered) << "delivery passed the aborted transaction";
}
