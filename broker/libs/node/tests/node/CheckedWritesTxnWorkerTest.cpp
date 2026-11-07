// Checked writes Part 11 Task 7: the TxnVote op on a real Worker - the vote-time scan, the TxnJudge record J, answers parked until
// the judge judged J, the marker's answer held until its transaction is final, the stats 11-20 and the read_uncommitted bound.
// A transaction with intents is written by replication (the produce path refuses it until Part 11 Task 12).
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
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"1", "2"}));
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
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"1"})) << "the aborted transaction's 7 is never applied";
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
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"4", "5"}));
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
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"1"})) << "applied when the marker is answered";
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
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"5"}));
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

// Task 7 (T7 review 1, F4): an atomic batch of txnJudgedRecordsMax + 1 records is rejected whole (62) instead of stalling the judge,
// and stays rejected across a failover; the records after it are judged.
TEST(CheckedWritesTxnWorker, AtomicBatchPastTheKnobIsRejectedAcrossAFailoverRegression) {
    Ki k;
    const int32_t knob = k.r.vs->txnJudgedRecordsMax();
    std::vector<Rec> big;
    for (int32_t i = 0; i <= knob; ++i) big.push_back(ups(1000 + uint64_t(i)));
    std::vector<uint8_t> bytes = VerdictRig::batch(big);
    VerdictRig::setBatchAttribute(bytes, rune::protocol::batch::kAttrAtomic);
    const int64_t base = k.r.leo();
    ASSERT_TRUE(k.r.ack(k.r.produceBytes(bytes, kLeader)));
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
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"1"}));
    EXPECT_EQ(k.stats().atomicTooLarge, 1u);
    k.r.failover();
    ASSERT_TRUE(k.r.ack(k.r.produceFrame({ups(2)}, kLeader)));
    ASSERT_TRUE(k.r.driveUntilJudged());
    EXPECT_TRUE(rejectedWhole()) << "the new leader keeps the rejection";
    EXPECT_EQ(k.r.keyIndexKeys(), (std::set<std::string>{"1", "2"}));
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
// cap counts from its last push).  A restart from that checkpoint, after T committed, backfills T's records below O from J.
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
// a delta past one ring message is held, never claimed (a claim that large throws on the worker thread).  Until Task 8 checkpoints
// stop there and a restart refolds from the last one.
TEST(CheckedWritesTxnWorker, OversizedDeltaIsHeldNotClaimedRegression) {
    Persisted p;
    const int64_t before = p.durable();
    std::vector<std::vector<uint8_t>> batches{VerdictRig::batch({ups(1001)}, kRaPid, 0, {}, /*txn=*/true)};
    for (auto& b : Persisted::plain(10000, 3 * p.cap)) batches.push_back(std::move(b));
    (void)p.replicated(std::move(batches));
    p.r.marker(kRaPid, true);
    ASSERT_TRUE(p.r.driveUntilJudged());
    p.r.passes(50);
    EXPECT_TRUE(p.r.hasRow(kRej, 1001));
    EXPECT_TRUE(p.r.hasRow(kRej, 10000 + static_cast<uint64_t>(3 * p.cap) - 1));
    EXPECT_TRUE(p.r.vs->instanceForTest(kRej, 0)->deltaOversized);
    EXPECT_EQ(p.durable(), before) << "the oversized delta never left";
    p.r.restart();
    p.r.passes(300);
    ASSERT_TRUE(p.r.driveUntilJudged());
    EXPECT_TRUE(p.r.hasRow(kRej, 1001));
    EXPECT_TRUE(p.r.hasRow(kRej, 10000 + static_cast<uint64_t>(3 * p.cap) - 1));
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
