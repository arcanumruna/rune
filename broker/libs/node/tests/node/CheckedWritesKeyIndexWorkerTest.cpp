// CheckedWritesKeyIndexWorkerTest.cpp - checked writes Part 05 Task 7: the key index as a verdict source on a real Worker.
// Intents are judged at fold (56 KeyExists / 57 KeyNotFound), the index is rebuilt from the local log at every activation, and
// no intent is applied before the logged constraint set holds it.
#include "CheckedWritesMatrixExpectations.h"
#include "CheckedWritesRig.h"

#include <rune/protocol/NamespaceLimits.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <random>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;

namespace {
constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
constexpr uint16_t kKeyExists = 56, kKeyNotFound = 57;
constexpr int16_t kNotLeader = static_cast<int16_t>(ErrorCode::NotLeader);
constexpr int16_t kInvalidRequest = static_cast<int16_t>(ErrorCode::InvalidRequest);
constexpr int32_t kRej = 0x40000601;
constexpr uint64_t kRejObj = 77;
constexpr int16_t kDuplicateSequence = static_cast<int16_t>(ErrorCode::DuplicateSequence);
using V = std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>;

Rec ins(uint64_t id) { return Rec{id, 1, 0, false, 1}; }
Rec upd(uint64_t id) { return Rec{id, 1, 0, false, 2}; }
Rec ups(uint64_t id) { return Rec{id, 1, 0, false, 0}; }
Rec rm(uint64_t id) { return Rec{id, 0, 0, true, 0}; }
Rec rmEx(uint64_t id) { return Rec{id, 0, 0, true, 2}; }

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }

std::set<std::string> idSet(uint64_t lo, uint64_t hi) {
    std::set<std::string> out;
    for (uint64_t i = lo; i <= hi; ++i) out.insert(std::to_string(i));
    return out;
}

/// An idempotent producer: every frame continues its sequence.
struct Intents {
    VerdictRig& r;
    int64_t     pid;
    int32_t     seq = 0;
    bool        fetching = false;   ///< the rig has a follower: the acks need its fetches

    int64_t send(const std::vector<Rec>& recs, uint8_t acks = kAcksViewAll) {
        const int64_t c = r.produceFrame(recs, acks, pid, seq);
        seq += static_cast<int32_t>(recs.size());
        return c;
    }
    int64_t resend(const std::vector<Rec>& recs, int32_t atSeq, uint8_t acks = kAcksViewAll) { return r.produceFrame(recs, acks, pid, atSeq); }
    std::optional<VerdictRig::Ack> ackOf(int64_t c) { return fetching ? r.ackFetching(c) : r.ack(c); }
    std::optional<VerdictRig::Ack> sendAck(const std::vector<Rec>& recs, uint8_t acks = kAcksViewAll) { return ackOf(send(recs, acks)); }
};

/// A key-index topic with acks, its change record logged (it precedes the records it is to judge) and an idempotent v17 producer.
struct Ready {
    VerdictRig r;
    Intents    p{r, 42};
    explicit Ready(bool follower = false, int64_t rollBytes = 64LL * 1024 * 1024) : r(follower, rollBytes) {
        r.createKeyIndex();
        r.enableAcks();
        r.passes(60);
    }
};

/// The rejected index as (offset relative to `base`, code).
std::vector<std::pair<int64_t, uint16_t>> rejectionsOf(const VerdictRig& r, int64_t base) {
    std::vector<std::pair<int64_t, uint16_t>> out;
    r.writeState()->rejected.forEachIn(0, INT64_MAX, [&](const auto& e) { out.emplace_back(e.offset - base, e.code); });
    return out;
}
}  // namespace

TEST(CheckedWritesKeyIndexWorker, InsertThenInsertIsKeyExistsOnTheWire) {
    Ready g;
    VerdictRig& r = g.r;
    Intents& p = g.p;
    const auto a1 = p.sendAck({ins(5)});
    ASSERT_TRUE(a1);
    EXPECT_EQ(a1->code, 0);
    EXPECT_TRUE(a1->hasVerdicts);
    EXPECT_TRUE(a1->verdicts.empty());
    EXPECT_EQ(a1->frameLen, 56u);
    const auto a2 = p.sendAck({ins(5)});
    ASSERT_TRUE(a2);
    EXPECT_EQ(a2->code, 0);
    EXPECT_EQ(a2->frameLen, 64u);
    EXPECT_EQ(a2->verdicts, (V{{0, kKeyExists, 0}}));
    const int64_t sub = r.subscribe(SubKind::Raw, 0);
    const auto& box = r.collect(sub);
    ASSERT_EQ(box.records.size(), 1u) << "the rejected insert is not delivered";
    EXPECT_EQ(box.records[0].offset, a1->baseOffset);
    EXPECT_EQ(box.records[0].intent, 1) << "op() == Insert";
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->rejectionsKeyExists, 1u);
}

TEST(CheckedWritesKeyIndexWorker, UpdateAndRemoveExistingOnAbsentAreKeyNotFound) {
    Ready g;
    VerdictRig& r = g.r;
    Intents& p = g.p;
    const auto a1 = p.sendAck({upd(9)});
    ASSERT_TRUE(a1);
    EXPECT_EQ(a1->verdicts, (V{{0, kKeyNotFound, 0}}));
    const auto a2 = p.sendAck({rmEx(9)});
    ASSERT_TRUE(a2);
    EXPECT_EQ(a2->verdicts, (V{{0, kKeyNotFound, 0}}));
    const auto a3 = p.sendAck({ups(9)});
    ASSERT_TRUE(a3);
    EXPECT_TRUE(a3->verdicts.empty());
    const auto a4 = p.sendAck({rmEx(9)});
    ASSERT_TRUE(a4);
    EXPECT_EQ(a4->code, 0);
    EXPECT_TRUE(a4->verdicts.empty());
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->rejectionsKeyNotFound, 2u);
    EXPECT_EQ(r.stats()->rejectionsKeyExists, 0u);
}

// P5-12: a plain write is folded into the key index and never rejected by it, whoever sends it.
TEST(CheckedWritesKeyIndexWorker, PlainWritesFromAnyProducerAreFoldedNeverRejected) {
    Ready g;
    VerdictRig& r = g.r;
    Intents& p = g.p;
    const auto a1 = r.ack(r.produceFrame({ups(7)}, /*acks=*/0));   // no producer id, leader ack
    ASSERT_TRUE(a1);
    EXPECT_EQ(a1->code, 0);
    ASSERT_TRUE(r.driveUntilJudged());
    r.enableAcks(16);
    const auto a2 = r.ack(r.produceFrame({rm(8)}, rune::protocol::client::kAcksWireAll));
    ASSERT_TRUE(a2);
    EXPECT_EQ(a2->code, 0);
    ASSERT_TRUE(r.driveUntilJudged());
    r.enableAcks(17);
    const auto a3 = p.sendAck({ins(7)});
    ASSERT_TRUE(a3);
    EXPECT_EQ(a3->verdicts, (V{{0, kKeyExists, 0}})) << "the plain upsert is in the index";
    const auto a4 = p.sendAck({ins(8)});
    ASSERT_TRUE(a4);
    EXPECT_TRUE(a4->verdicts.empty()) << "a remove of an absent key left nothing behind";
    EXPECT_EQ(r.stats()->rejectionsKeyExists, 1u);
}

TEST(CheckedWritesKeyIndexWorker, ChangeRecordPutsTheKeyIndexFirst) {
    Ready g;
    VerdictRig& r = g.r;
    const auto log = r.readLog();
    ASSERT_EQ(log.changes.size(), 1u);
    ASSERT_GE(log.changes[0].members.size(), 1u);
    EXPECT_EQ(log.changes[0].members[0].viewObjectId, 0u);
    EXPECT_EQ(log.changes[0].members[0].kind, static_cast<uint8_t>(rune::view::VerdictViewKind::KeyIndex));
    EXPECT_EQ(log.changes[0].members[0].planFingerprint, kKeyIndexPlanFingerprint);
    ASSERT_NE(r.writeState(), nullptr);
    const ConstraintSet& latest = r.writeState()->constraintSets.latest();
    ASSERT_GE(latest.count, 1);
    EXPECT_EQ(latest.members[0].kind, rune::view::VerdictViewKind::KeyIndex);
}

// The key index is never persisted: the judge's checkpoint hook drops it.
TEST(CheckedWritesKeyIndexWorker, KeyIndexIsNeverCheckpointed) {
    Ready g;
    VerdictRig& r = g.r;
    Intents& p = g.p;
    ASSERT_TRUE(p.sendAck({ins(1), ins(2), ins(3)}));
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(40);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 3u);
    EXPECT_TRUE(r.deltaViewIds.empty()) << "a delta frame left for the key index";
    EXPECT_EQ(r.vs->instanceCount(), 0u) << "it is no view instance";
}

TEST(CheckedWritesKeyIndexWorker, KeyIndexOnlyTopicIsDrivenWithoutViews) {
    VerdictRig r;
    r.createKeyIndex();
    r.enableAcks();
    ASSERT_EQ(r.vs->viewCount(), 0u);
    r.passes(60);
    EXPECT_EQ(r.vs->viewCount(), 0u);
    EXPECT_EQ(r.readLog().changes.size(), 1u) << "the first change record is appended with no view and no earlier set: the key.index flag alone drives the partition";
    Intents p{r, 42};
    const auto a = p.sendAck({ups(1), ups(2)});
    ASSERT_TRUE(a) << "a View ack is released by the judge alone";
    EXPECT_EQ(a->code, 0);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_GT(judge->judgedThrough(), a->baseOffset + 1);
    const int64_t sub = r.subscribe(SubKind::Raw, 0);
    EXPECT_EQ(r.collect(sub).records.size(), 2u);
}

TEST(CheckedWritesKeyIndexWorker, TopicDropReleasesTheKeyIndex) {
    Ready g;
    VerdictRig& r = g.r;
    Intents& p = g.p;
    ASSERT_TRUE(p.sendAck({ins(1), ins(2)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
    ASSERT_FALSE(r.vs->verdictSourceKeysForTest().empty());
    WorkerOp d;
    d.type = WorkerOp::Type::TopicDropped;
    d.topicId = VerdictRig::kSrc;
    r.send(std::move(d));
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u);
    EXPECT_EQ(r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0), -1);
    EXPECT_TRUE(r.vs->verdictSourceKeysForTest().empty());
}

TEST(CheckedWritesKeyIndexWorker, IntentOnATopicWithoutKeyIndexIsDefinite) {
    VerdictRig r;
    r.typed();
    r.enableAcks();
    r.passes(20);
    Intents p{r, 42};
    const auto a = p.sendAck({ins(5)});
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, kInvalidRequest);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::NoKeyIndex), 1u);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::KeyIndexActivating), 0u);
}

// Review focus 3: an intent sent before the logged set holds the key index is retriable and never applied unjudged.
TEST(CheckedWritesKeyIndexRegression, IntentBeforeActivationIsRetriableNeverUnjudgedRegression) {
    VerdictRig r;
    r.enableAcks();
    r.holdJudge(true);
    r.createKeyIndex();
    r.passes(30);
    ASSERT_TRUE(r.writeState()->constraintSets.latest().empty()) << "no change record yet";
    Intents p{r, 42};
    const int64_t leo = r.leo();
    const auto refused = p.sendAck({ins(5)});
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code, kNotLeader);
    EXPECT_EQ(r.leo(), leo);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::KeyIndexActivating), 1u);
    r.holdJudge(false);
    r.passes(40);
    const auto again = r.ack(p.resend({ins(5)}, 0));
    ASSERT_TRUE(again);
    EXPECT_EQ(again->code, 0);
    EXPECT_TRUE(again->verdicts.empty());
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::KeyIndexActivating), 1u);
}

TEST(CheckedWritesKeyIndexRegression, AnAdoptedTopicWaitsForTheReplayThenJudgesAgainstIt) {
    VerdictRig r;
    r.enableAcks();
    r.createKeyIndex({}, /*enabled=*/false);
    r.passes(20);
    for (uint64_t base = 0; base < 300; base += 50) {
        std::vector<Rec> recs;
        for (uint64_t id = base; id < base + 50; ++id) recs.push_back(ups(id));
        ASSERT_EQ(r.produce(recs), 0);
    }
    r.holdJudge(true);
    r.enableKeyIndex();
    r.passes(20);
    Intents p{r, 42};
    const auto refused = p.sendAck({ins(5)});
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code, kNotLeader) << "before the pass that logs the key index";
    r.holdJudge(false);
    r.passes(60);   // the change record is logged and the 300 records are replayed into the index
    const auto a = r.ack(p.resend({ins(5)}, 0));
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->verdicts, (V{{0, kKeyExists, 0}})) << "id 5 was written before the adoption";
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 300u);
}

// Review focus 1 (R04e): a retry of an accepted insert replays OK, never KeyExists against its own record.
TEST(CheckedWritesKeyIndexRegression, InsertRetryAfterALostAckReplaysOkNeverKeyExistsRegression) {
    Ready g;
    VerdictRig& r = g.r;
    Intents& p = g.p;
    const auto lost = p.sendAck({ins(5)});   // the client never sees this ack
    ASSERT_TRUE(lost);
    ASSERT_TRUE(r.driveUntilJudged());
    const int64_t leo = r.leo();
    const auto retry = r.ack(p.resend({ins(5)}, 0));
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->code, 0);
    EXPECT_EQ(retry->baseOffset, lost->baseOffset);
    EXPECT_TRUE(retry->hasVerdicts);
    EXPECT_TRUE(retry->verdicts.empty());
    EXPECT_EQ(retry->frameLen, 56u);
    EXPECT_EQ(r.stats()->verdictReplays, 1u);
    EXPECT_EQ(r.leo(), leo) << "nothing was appended";
    EXPECT_EQ(r.stats()->rejectionsKeyExists, 0u);
    std::vector<int64_t> corrs;
    for (uint64_t i = 0; i < 33; ++i) corrs.push_back(p.send({ups(100 + i)}));
    for (const int64_t c : corrs) ASSERT_TRUE(r.ack(c));
    ASSERT_TRUE(r.driveUntilJudged());
    const auto outside = r.ack(p.resend({ins(5)}, 0));
    ASSERT_TRUE(outside);
    EXPECT_EQ(outside->code, kDuplicateSequence);
    EXPECT_FALSE(outside->hasVerdicts);
}

namespace {
bool hasTombstone(const VerdictRig& r) {
    const auto log = r.readLog();
    return std::any_of(log.data.begin(), log.data.end(), [](const auto& d) { return d.del; });
}
}  // namespace

// Review focus 2: every activation rebuilds the index from the local log; an instance carried over would keep what a compaction removed.
TEST(CheckedWritesKeyIndexRegression, EveryActivationRebuildsFromTheLocalLogRegression) {
    VerdictRig r(/*follower=*/false, /*rollBytes=*/1);
    r.createKeyIndex();
    r.enableAcks();
    r.passes(60);
    Intents p{r, 42};
    std::vector<Rec> inserts, removes;
    for (uint64_t id = 1; id <= 50; ++id) inserts.push_back(ins(id));
    for (uint64_t id = 1; id <= 10; ++id) removes.push_back(rm(id));
    ASSERT_TRUE(p.sendAck(inserts));
    ASSERT_TRUE(p.sendAck(removes));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.keyIndexKeys(), idSet(11, 50));

    const uint64_t rebuilds = r.stats()->keyIndexRebuilds;
    r.holdJudge(true);   // the activation stops before the replay
    r.demotePromote();
    r.passes(30);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 1);
    EXPECT_EQ(r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0), r.log->logStartOffset()) << "the old instance was not reused";
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    EXPECT_EQ(r.keyIndexKeys(), idSet(11, 50));

    for (int i = 0; i < 6 && hasTombstone(r); ++i) (void)r.compact(1'000'000 + i * 5'000);
    ASSERT_FALSE(hasTombstone(r)) << "the tombstones of 1..10 are gone from the log";
    r.demotePromote();
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 2);
    EXPECT_EQ(r.keyIndexKeys(), idSet(11, 50)) << "rebuilt from the compacted log";
    const auto a3 = p.sendAck({ins(3)});
    ASSERT_TRUE(a3);
    EXPECT_TRUE(a3->verdicts.empty());
    const auto a20 = p.sendAck({ins(20)});
    ASSERT_TRUE(a20);
    EXPECT_EQ(a20->verdicts, (V{{0, kKeyExists, 0}}));
}

TEST(CheckedWritesKeyIndexRegression, RestartRebuildsTheSameKeySetRegression) {
    VerdictRig a, b;
    for (VerdictRig* r : {&a, &b}) {
        r->createKeyIndex();
        r->enableAcks();
        r->passes(60);
    }
    Intents pa{a, 42}, pb{b, 42};
    std::mt19937 rng(0x5eed);
    const auto makeFrame = [&] {
        std::vector<Rec> recs;
        for (int i = 0; i < 10; ++i) {
            const uint64_t id = rng() % 64;
            switch (rng() % 5) {
            case 0: recs.push_back(ups(id)); break;
            case 1: recs.push_back(ins(id)); break;
            case 2: recs.push_back(upd(id)); break;
            case 3: recs.push_back(rm(id)); break;
            default: recs.push_back(rmEx(id)); break;
            }
        }
        return recs;
    };
    size_t rejections = 0;
    const auto both = [&](Intents& x, Intents& y, int frames) {
        std::vector<std::pair<int64_t, int64_t>> cs;
        for (int i = 0; i < frames; ++i) {
            const auto recs = makeFrame();
            cs.emplace_back(x.send(recs), y.send(recs));
        }
        for (const auto& [cx, cy] : cs) {
            const auto ax = x.ackOf(cx);
            const auto ay = y.ackOf(cy);
            ASSERT_TRUE(ax);
            ASSERT_TRUE(ay);
            EXPECT_EQ(ax->code, 0);
            EXPECT_EQ(ax->verdicts, ay->verdicts);
            rejections += ax->verdicts.size();
        }
    };
    both(pa, pb, 100);
    ASSERT_GT(rejections, 0u) << "the sequence exercises the rejections";
    ASSERT_TRUE(a.driveUntilJudged());
    ASSERT_TRUE(b.driveUntilJudged());
    const auto before = a.keyIndexKeys();
    ASSERT_EQ(before, b.keyIndexKeys());
    a.restart();
    ASSERT_TRUE(a.driveUntilJudged());
    a.passes(20);
    EXPECT_EQ(a.keyIndexKeys(), before);
    Intents na{a, 43}, nb{b, 43};
    both(na, nb, 20);
    EXPECT_EQ(a.keyIndexKeys(), b.keyIndexKeys());
}

TEST(CheckedWritesKeyIndexRegression, LeaderWithoutTheKeyIndexSpecWaitsRegression) {
    VerdictRig r;
    r.createKeyIndex();
    r.enableAcks();
    r.passes(60);
    const auto log = r.readLog();
    ASSERT_EQ(log.changes.size(), 1u);
    const int64_t change = log.changes[0].offset;
    r.restart(/*reinstall=*/true, /*cutTo=*/-1, /*specDelayPasses=*/100);
    r.passes(30);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_TRUE(judge->waitingForInstall()) << "the logged set names a member this node cannot build yet";
    EXPECT_EQ(judge->judgedThrough(), change + 1);
    Intents p{r, 42};
    const int64_t c = p.send({ups(5)});
    r.settle(40);
    EXPECT_FALSE(r.acked(c)) << "a View ack stays parked";
    EXPECT_EQ(judge->judgedThrough(), change + 1);
    const auto a = r.ack(c);
    ASSERT_TRUE(a) << "released once the spec arrived";
    EXPECT_EQ(a->code, 0);
    EXPECT_TRUE(a->verdicts.empty());
    EXPECT_FALSE(judge->waitingForInstall());
    const auto dup = p.sendAck({ins(5)});
    ASSERT_TRUE(dup);
    EXPECT_EQ(dup->verdicts, (V{{0, kKeyExists, 0}}));
}

TEST(CheckedWritesKeyIndexWorker, FailoverTimingsGiveTheSameVerdicts) {
    const std::vector<std::pair<int64_t, uint16_t>> want{{1, kKeyExists}, {2, kKeyNotFound}};
    const auto setup = [](VerdictRig& r) {
        r.createKeyIndex();
        r.enableAcks();
        r.passes(60);
        r.followerFetch();
        r.passes(10);
        r.followerFetch();
    };
    const std::vector<Rec> batch{ins(5), ins(5), upd(6)};
    {
        VerdictRig r(/*follower=*/true);
        setup(r);
        Intents p{r, 42};
        p.fetching = true;
        const auto a = p.sendAck(batch);
        ASSERT_TRUE(a);
        ASSERT_TRUE(r.driveUntilJudged());
        EXPECT_EQ(rejectionsOf(r, a->baseOffset), want) << "undisturbed";
    }
    {
        VerdictRig r(true);   // FailoverHwFold
        setup(r);
        Intents p{r, 42};
        p.fetching = true;
        const auto a = p.sendAck(batch);
        ASSERT_TRUE(a);
        ASSERT_TRUE(r.driveUntilJudged());
        r.followerFetch();
        r.failover();
        ASSERT_TRUE(r.driveUntilJudged());
        EXPECT_EQ(rejectionsOf(r, a->baseOffset), want);
        EXPECT_EQ(r.writeState()->rejected.mismatches(), 0u);
    }
    {
        VerdictRig r(true);   // FailoverFoldMarker: the marker sits above the HW and is lost with the old leader
        setup(r);
        Intents p{r, 42};
        p.send(batch);
        r.passes(5);
        r.followerFetch();
        for (int i = 0; i < VerdictRig::kMaxPass && r.leo() <= r.replica->highWatermark(); ++i) r.pass();
        ASSERT_GT(r.leo(), r.replica->highWatermark());
        const int64_t base = r.readLog().data.front().offset;
        r.failover();
        ASSERT_TRUE(r.driveUntilJudged());
        EXPECT_EQ(rejectionsOf(r, base), want);
        EXPECT_EQ(r.writeState()->rejected.mismatches(), 0u);
    }
}

TEST(CheckedWritesKeyIndexWorker, ReplayIsBoundedPerPass) {
    VerdictRig r;
    r.createKeyIndex();
    r.passes(60);
    for (uint64_t base = 0; base < 200'000; base += 4096) {
        std::vector<Rec> recs;
        for (uint64_t id = base; id < std::min<uint64_t>(base + 4096, 200'000); ++id) recs.push_back(ups(id));
        ASSERT_EQ(r.produce(recs), 0);
    }
    ASSERT_TRUE(r.driveUntilJudged());
    const int64_t dataEnd = r.dataEnd();
    r.holdJudge(true);
    r.restart();
    const VerdictSource* judge = nullptr;
    for (int i = 0; i < VerdictRig::kMaxPass && (judge = r.vs->verdictSource(VerdictRig::kSrc, 0)) == nullptr; ++i) r.pass();
    ASSERT_NE(judge, nullptr);
    ASSERT_TRUE(judge->activationPending());
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u) << "nothing is replayed while the judge is held";
    r.holdJudge(false);
    r.pass();
    const int64_t k = judge->judgedThrough();
    ASSERT_GE(k, dataEnd);
    int64_t cursor = r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0);
    int progressing = 1;
    EXPECT_LE(cursor, kKeyIndexReplayRecordsPerPass);
    EXPECT_LT(cursor, k) << "200,000 records are not replayed in one pass";
    for (int i = 0; i < 400 && cursor < k; ++i) {
        EXPECT_EQ(judge->judgedThrough(), k) << "the judge keeps K meanwhile";
        EXPECT_EQ(r.writeState()->verdictKnownThrough(), k) << "consumers are released up to K";
        r.pass();
        const int64_t next = r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0);
        EXPECT_LE(next - cursor, kKeyIndexReplayRecordsPerPass);
        if (next > cursor) ++progressing;
        cursor = next;
    }
    ASSERT_GE(cursor, k);
    EXPECT_GE(progressing, 2);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 200'000u);
    const int64_t sub = r.subscribe(SubKind::PushRead, 0);
    const auto& box = r.collect(sub);
    ASSERT_FALSE(box.records.empty());
    EXPECT_LT(box.records.back().offset, k);
}

// Review I1: the key index is the judge's first member, so every co-member reads its writer through the key index's descriptor.
// v1 {id, v, tag}; v2 drops v.  A v1 record must still decode tag at the v1 layout.
TEST(CheckedWritesKeyIndexRegression, OlderWritersDecodeThroughTheEvolvingSchemaRegression) {
    VerdictRig r;
    r.createKeyIndex({}, /*enabled=*/false);
    r.dropValueField();
    r.installRejectView(kRej, kRejObj, tagRule(7));
    r.enableKeyIndex();
    r.enableAcks();
    r.passes(60);
    Intents p{r, 42};
    const auto bad = p.sendAck({Rec{5, 1000, 7}});
    ASSERT_TRUE(bad);
    EXPECT_EQ(bad->code, 0);
    ASSERT_EQ(bad->verdicts.size(), 1u) << "tag 7 of a v1 record is rejected";
    EXPECT_EQ(std::get<1>(bad->verdicts[0]), static_cast<uint16_t>(ErrorCode::ConstraintViolation));
    const auto good = p.sendAck({Rec{6, 1000, 9}});
    ASSERT_TRUE(good);
    EXPECT_TRUE(good->verdicts.empty());
}

// Review M2: a topic that already has a verdict source adopts key.index; its intents are judged, not answered NotLeader for ever.
TEST(CheckedWritesKeyIndexRegression, AdoptionOntoAnExistingVerdictSourceJudgesIntentsRegression) {
    VerdictRig r;
    r.createKeyIndex({}, /*enabled=*/false);
    r.installRejectView(kRej, kRejObj, tagRule(7));
    r.enableAcks();
    r.passes(60);
    ASSERT_NE(r.vs->verdictSource(VerdictRig::kSrc, 0), nullptr);
    ASSERT_EQ(r.writeState()->constraintSets.latest().count, 1) << "only the reject view is in the set";
    r.enableKeyIndex();
    r.passes(60);
    const ConstraintSet& latest = r.writeState()->constraintSets.latest();
    ASSERT_EQ(latest.count, 2);
    EXPECT_EQ(latest.members[0].kind, rune::view::VerdictViewKind::KeyIndex);
    Intents p{r, 42};
    const auto a1 = p.sendAck({ins(5)});
    ASSERT_TRUE(a1);
    EXPECT_EQ(a1->code, 0);
    const auto a2 = p.sendAck({ins(5)});
    ASSERT_TRUE(a2);
    EXPECT_EQ(a2->verdicts, (V{{0, kKeyExists, 0}}));
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::KeyIndexActivating), 0u);
}

// Review M4 (RB5): the key index is charged to its namespace at adoption and released when it is rebuilt, erased or dropped.
TEST(CheckedWritesKeyIndexRegression, KeyIndexMemoryIsChargedAndReleasedWithItsNamespaceRegression) {
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs = 77;
    VerdictRig r;
    r.worker->applyNsSlotAssignDirect(kSlot, kNs, rune::protocol::NamespaceLimits{});
    r.log->setNsTag(kNs, kSlot);
    r.createKeyIndex();
    r.enableAcks();
    r.passes(60);
    Intents p{r, 42};
    ASSERT_TRUE(p.sendAck({ins(1), ins(2), ins(3)}));
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    const uint64_t charged = r.vs->nsViewMemBytes(kSlot);
    EXPECT_GT(charged, 0u) << "charged at adoption";
    EXPECT_EQ(r.vs->nsSlotMismatchesForTest(), 0u);
    r.demotePromote();
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 3u);
    EXPECT_EQ(r.vs->nsViewMemBytes(kSlot), charged) << "the rebuilt instance replaced the old charge, it did not add to it";
    WorkerOp d;
    d.type = WorkerOp::Type::TopicDropped;
    d.topicId = VerdictRig::kSrc;
    r.send(std::move(d));
    EXPECT_EQ(r.vs->nsViewMemBytes(kSlot), 0u) << "net zero after the drop";
}

// Review M8: the first activation uses the instance that was just built; only a later leadership rebuilds.
TEST(CheckedWritesKeyIndexRegression, FirstActivationIsNotARebuildRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, 0u);
    r.demotePromote();
    r.passes(30);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, 1u);
}

// ---- the key-width sweep: the key index judges by the exact bytes of the wire key, at every width the DDL allows (SD8) ----
namespace {
struct WidthCase { const char* name; KeyShape shape; };
const WidthCase kWidthCases[] = {
    {"uint16", {rune::codec::PrimType::UInt16, 0}}, {"int32", {rune::codec::PrimType::Int32, 0}}, {"uint32", {rune::codec::PrimType::UInt32, 0}},
    {"int64", {rune::codec::PrimType::Int64, 0}}, {"uint64", {rune::codec::PrimType::UInt64, 0}},
    {"string1", {rune::codec::PrimType::FixedStr, 1}}, {"string8", {rune::codec::PrimType::FixedStr, 8}},
};

Rec opOf(mx::Op op, uint64_t id) {
    switch (op) {
    case mx::Op::Insert: return ins(id);
    case mx::Op::Update: return upd(id);
    case mx::Op::Upsert: return ups(id);
    case mx::Op::Remove: return rm(id);
    default: return rmEx(id);
    }
}

struct WidthRun {
    VerdictRig r;
    Intents    p{r, 42};
    explicit WidthRun(const KeyShape& s) {
        r.createKeyIndex(s);
        r.enableAcks();
        r.passes(60);
    }
};
}  // namespace

class Shape : public ::testing::TestWithParam<WidthCase> {};

// 5 ops x {KeyNew, KeyExisting}, each on a key of its own: the verdicts are the P5-3 table at every width.
TEST_P(Shape, OpsJudgeByExactKeyBytes) {
    WidthRun g(GetParam().shape);
    uint64_t id = 10;
    for (const mx::Op op : {mx::Op::Insert, mx::Op::Update, mx::Op::Upsert, mx::Op::Remove, mx::Op::RemoveExisting})
        for (const mx::Entity e : {mx::Entity::KeyNew, mx::Entity::KeyExisting}) {
            ++id;
            SCOPED_TRACE(std::string(mx::nameOf(op)) + " x " + mx::nameOf(e));
            if (e == mx::Entity::KeyExisting) {
                const auto seed = g.p.sendAck({ups(id)});
                ASSERT_TRUE(seed);
                ASSERT_EQ(seed->code, 0);
            }
            const auto a = g.p.sendAck({opOf(op, id)});
            ASSERT_TRUE(a) << mx::nameOf(op) << " x " << mx::nameOf(e);
            EXPECT_EQ(a->code, 0);
            const uint16_t code = mx::keyIndexVerdict05(op, e);
            V want;
            if (code != 0) want.emplace_back(0, code, 0);
            EXPECT_EQ(a->verdicts, want) << mx::nameOf(op) << " x " << mx::nameOf(e);
        }
}

// Two keys equal but for the last byte are two keys.
TEST_P(Shape, KeysDifferingInTheLastByteAreDistinct) {
    const KeyShape& shape = GetParam().shape;
    WidthRun g(shape);
    const uint32_t kw = keyWidthOf(shape);
    uint64_t a = 0;   // every byte 1, so a string key has no NUL padding
    for (uint32_t i = 0; i < kw; ++i) a |= uint64_t{1} << (8 * i);
    const uint64_t b = a + (uint64_t{1} << (8 * (kw - 1)));
    const auto first = g.p.sendAck({ins(a)});
    const auto second = g.p.sendAck({ins(b)});
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first->code, 0);
    EXPECT_EQ(second->code, 0);
    EXPECT_TRUE(first->verdicts.empty());
    EXPECT_TRUE(second->verdicts.empty());
    const auto again = g.p.sendAck({ins(b)});
    ASSERT_TRUE(again);
    EXPECT_EQ(again->verdicts, (V{{0, kKeyExists, 0}}));
    ASSERT_TRUE(g.r.driveUntilJudged());
    g.r.passes(40);
    EXPECT_EQ(g.r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
}

// A remove carries the key the way a send does, so it erases exactly what the send put.
TEST_P(Shape, RemoveErasesWhatSendPut) {
    WidthRun g(GetParam().shape);
    ASSERT_TRUE(g.p.sendAck({ups(7)}));
    ASSERT_TRUE(g.r.driveUntilJudged());
    g.r.passes(40);
    ASSERT_EQ(g.r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    ASSERT_TRUE(g.p.sendAck({rm(7)}));
    ASSERT_TRUE(g.r.driveUntilJudged());
    g.r.passes(40);
    EXPECT_EQ(g.r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u);
    const auto reinsert = g.p.sendAck({ins(7)});
    ASSERT_TRUE(reinsert);
    EXPECT_TRUE(reinsert->verdicts.empty());
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesKeyIndexWidths, Shape, ::testing::ValuesIn(kWidthCases),
    [](const ::testing::TestParamInfo<WidthCase>& i) { return std::string(i.param.name); });
