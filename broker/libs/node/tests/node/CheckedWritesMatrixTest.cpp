// Checked writes Part 04 Task 10: the shared interaction matrix (5 ops x 8 entities x 15 timings) on a real Worker, judged against
// the independent reference model, plus the pins of both parts' cell counts.  Part 05 adds 5 intent ops x 6 key states x 21 timings,
// Part 06 6 autoincrement ops x 7 topics x 21 timings.
#include "CheckedWritesMatrixExpectations.h"
#include "CheckedWritesChecks.h"
#include "CheckedWritesModel.h"

#include <rune/codec/UnicodeLower.h>
#include <rune/view/Verdict.h>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewRow.h>

#include <algorithm>
#include <initializer_list>
#include <map>
#include <set>
#include <stdexcept>
#include <string>

using namespace rune::node;
using namespace rune::node::cw;
using namespace rune::node::cw::mx;
using rune::protocol::client::ErrorCode;

namespace cr = rune::storage::control_record;

namespace {

constexpr int32_t kRej = 0x40000701, kRej2 = 0x40000702, kRejNew = 0x40000703, kProj = 0x40000704, kCnt = 0x40000705;
constexpr uint64_t kObj = 7, kObj2 = 8;
constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
constexpr uint8_t kAcksAll = rune::protocol::client::kAcksWireAll;
constexpr int16_t kNotLeader = static_cast<int16_t>(ErrorCode::NotLeader);
constexpr int64_t kPid = 500;

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
TestRejectRule capRule(uint32_t cap) { TestRejectRule r; r.mode = TestRejectRule::Mode::CapLiveKeys; r.cap = cap; return r; }

CheckedWritesModel::Rule modelRule(const TestRejectRule& r) {
    return r.mode == TestRejectRule::Mode::CapLiveKeys ? CheckedWritesModel::capLiveKeys(r.cap) : CheckedWritesModel::tagEquals(r.tag);
}

struct OpPlan { std::vector<Rec> recs; TestRejectRule rule; };

OpPlan planFor(Op op) {
    switch (op) {
    case Op::AcceptedWrite: return {{{1, 10, 1}, {2, 20, 2}, {3, 30, 1}}, tagRule(9)};
    case Op::RejectedWrite: return {{{1, 10, 1}, {2, 20, 9}, {3, 30, 1}}, tagRule(9)};
    case Op::RejectedOverAccepted: return {{{5, 50, 1}, {5, 51, 9}}, tagRule(9)};
    case Op::CapRace: return {{{1, 10, 1}, {2, 20, 1}, {3, 30, 1}, {4, 40, 1}}, capRule(2)};
    case Op::DeleteFreesCap: return {{{1, 10, 1}, {2, 20, 1}, {1, 0, 0, true}, {3, 30, 1}}, capRule(2)};
    default: return {};
    }
}

bool isFaulty(Timing t) {
    return t == Timing::FailoverAppendHw || t == Timing::FailoverHwFold || t == Timing::FailoverFoldMarker || t == Timing::TruncateTail ||
           t == Timing::RestartDuringRebuild;
}
bool isChange(Timing t) { return t == Timing::CreateOnNonEmpty || t == Timing::ReplaceDuringWrites || t == Timing::DropDuringWrites; }
bool isRetry(Timing t) { return t == Timing::RetryBeforeJudge || t == Timing::RetryAfterJudge || t == Timing::RetryAfterWindow; }

struct Sent {
    int64_t corr;
    uint8_t acks;
    int64_t pid;
    int32_t seq;
    std::vector<Rec> recs;
};

class CellRun {
public:
    explicit CellRun(const Cell& c) : c_(c), r_(/*follower=*/true, /*rollBytes=*/1) {}

    void run() {
        const Expected ex = expect(c_);
        plan_ = planFor(c_.op);
        std::vector<Rec> recs = plan_.recs;
        if (c_.entity == Entity::TwoVerdictViews) recs.push_back({9, 90, 8});
        if (c_.timing == Timing::SelfCollidingBatch) recs.push_back({recs.front().id, 77, 2});
        makeBatches_(recs);

        frames_ = c_.entity == Entity::ViewAckWriter || c_.entity == Entity::OldWriters || isRetry(c_.timing);
        r_.typed();
        if (c_.timing != Timing::CreateOnNonEmpty) r_.installRejectView(kRej, kObj, plan_.rule);
        if (c_.entity == Entity::TwoVerdictViews) r_.installRejectView(kRej2, kObj2, tagRule(8), "rej2");
        r_.installProjection(kProj);
        r_.installCountById(kCnt);
        if (frames_) r_.enableAcks(c_.entity == Entity::OldWriters ? 11 : 17);
        r_.passes(50);
        r_.followerFetch();
        r_.passes(10);

        const bool early = !isFaulty(c_.timing) && c_.entity != Entity::ReplayFromStart;
        int64_t sub = 0;
        if (early && (c_.entity == Entity::RawSubscription || c_.entity == Entity::FilteredSubscription))
            sub = r_.subscribe(c_.entity == Entity::RawSubscription ? SubKind::Raw : SubKind::Filtered, 0);

        execute_();
        if (::testing::Test::HasFatalFailure()) return;
        // records after the last member left need no verdicts: the judge stops at the drop
        const auto written = r_.readLog();
        target_ = c_.timing == Timing::DropDuringWrites ? written.changes.back().offset + 1 : r_.dataEnd();
        ASSERT_TRUE(r_.driveUntilJudged(target_));
        r_.passes(60);   // the views fold up to the judged cursor

        if (sub == 0) {
            switch (c_.entity) {
            case Entity::RawSubscription: sub = r_.subscribe(SubKind::Raw, 0); break;
            case Entity::FilteredSubscription: sub = r_.subscribe(SubKind::Filtered, 0); break;
            case Entity::GroupConsumer: case Entity::ReplayFromStart: sub = r_.subscribe(c_.entity == Entity::GroupConsumer ? SubKind::PushRead : SubKind::Raw, 0); break;
            default: break;
            }
        }

        const auto log = r_.readLog();
        CheckedWritesModel model;
        model.declareRule(kObj, plan_.rule.fingerprint(), modelRule(plan_.rule));
        model.declareRule(kObj, tagRule(1).fingerprint(), modelRule(tagRule(1)));
        model.declareRule(kObj2, tagRule(8).fingerprint(), modelRule(tagRule(8)));
        model.reset(log);

        checkIndex(r_, model);
        checkMarkers(model, log, target_);
        checkViews(r_, model, log, kProj, kCnt);
        if (sub != 0) checkDelivered(r_, model, sub, c_.entity == Entity::FilteredSubscription);
        checkAcks_(ex, model);
        if (isRetry(c_.timing)) EXPECT_EQ(log.data.size(), uniqueRecs_) << "a retry is stored once";
        EXPECT_EQ(r_.writeState()->rejected.mismatches(), 0u);
        checkCompaction(r_, model, log);
    }

private:
    void makeBatches_(const std::vector<Rec>& recs) {
        if (c_.timing == Timing::BackToBack) {
            for (const Rec& x : recs) batches_.push_back({x});
        } else if (c_.timing == Timing::Racing) {
            std::vector<Rec> even, odd;
            for (std::size_t i = 0; i < recs.size(); ++i) (i % 2 == 0 ? even : odd).push_back(recs[i]);
            batches_ = {odd, even};
        } else if (isChange(c_.timing)) {
            batches_ = {std::vector<Rec>(recs.begin(), recs.end() - 1), {recs.back()}};
        } else {
            batches_ = {recs};
        }
    }

    // One batch to the broker.  Frames carry a producer id (a View ack writer) or one of the old writers; the rest use the op path.
    void send_(const std::vector<Rec>& recs, std::size_t k) {
        if (!frames_) {
            ASSERT_EQ(r_.produce(recs), 0);
            uniqueRecs_ += recs.size();
            return;
        }
        uint8_t acks = kAcksViewAll;
        int64_t pid = kPid;
        if (c_.entity == Entity::OldWriters) {
            switch ((k + static_cast<std::size_t>(c_.op)) % 3) {
            case 0: acks = 0; pid = kPid + 1; break;   // acks=0 is the leader ack on this wire
            case 1: acks = 0; pid = -1; break;
            default: acks = kAcksAll; pid = -1; break;
            }
        } else if (c_.entity != Entity::ViewAckWriter) {
            acks = 0;
        }
        int32_t seq = -1;
        if (pid >= 0) { seq = seqs_[pid]; seqs_[pid] += static_cast<int32_t>(recs.size()); }
        sent_.push_back({r_.produceFrame(recs, acks, pid, seq), acks, pid, seq, recs});
        uniqueRecs_ += recs.size();
    }
    void resend_(Sent s) {
        sent_.push_back({r_.produceFrame(s.recs, s.acks, s.pid, s.seq), s.acks, s.pid, s.seq, s.recs});
        dups_.push_back({s.corr, sent_.back().corr});
    }

    void change_() {
        r_.passes(5);
        if (c_.timing == Timing::CreateOnNonEmpty) r_.installRejectView(kRej, kObj, plan_.rule);
        else if (c_.timing == Timing::ReplaceDuringWrites) r_.replaceRejectView(kRej, kRejNew, kObj, tagRule(1));
        else r_.dropRejectView(kRej);
        r_.passes(5);
    }

    void execute_() {
        const auto sendAll = [&] { for (std::size_t k = 0; k < batches_.size(); ++k) send_(batches_[k], k); };
        switch (c_.timing) {
        case Timing::RetryBeforeJudge: case Timing::RetryAfterJudge: case Timing::RetryAfterWindow:
            ASSERT_TRUE(executeRetry(r_, c_.timing != Timing::RetryBeforeJudge, c_.timing == Timing::RetryAfterWindow, [&] { send_(batches_[0], 0); }, [&](uint64_t i) { send_({{100 + i, 1, 0}}, 0); }, [&] { resend_(sent_[0]); }));
            return;
        case Timing::CreateOnNonEmpty: case Timing::ReplaceDuringWrites: case Timing::DropDuringWrites:
            send_(batches_[0], 0);
            change_();
            send_(batches_[1], 1);
            return;
        case Timing::FailoverAppendHw: {
            sendAll();
            r_.passes(5);
            const auto lost = sent_;
            if (c_.entity == Entity::ViewAckWriter) {
                r_.demote();
                const auto first = r_.ack(lost.front().corr);
                ASSERT_TRUE(first);
                firstAckCode_ = first->code;
            }
            r_.failover();
            r_.passes(100);   // the partition rebuilds before it takes the writer's re-send
            sent_.clear(); seqs_.clear(); uniqueRecs_ = 0;
            sendAll();
            return;
        }
        case Timing::FailoverHwFold:
            sendAll();
            ASSERT_TRUE(r_.driveUntilJudged());
            r_.followerFetch();
            r_.failover();
            return;
        case Timing::FailoverFoldMarker:
            sendAll();
            r_.passes(5);
            r_.followerFetch();
            while (r_.leo() <= r_.replica->highWatermark()) r_.pass();   // the marker sits above the HW
            ASSERT_GT(r_.leo(), r_.replica->highWatermark());
            r_.failover();
            return;
        case Timing::TruncateTail:
            sendAll();
            ASSERT_TRUE(r_.driveUntilJudged());
            ASSERT_EQ(r_.produce({{200, 1, 0}}), 0);
            r_.passes(150);
            r_.truncateTo(r_.replica->highWatermark());
            r_.demotePromote();
            return;
        case Timing::RestartDuringRebuild:
            sendAll();
            ASSERT_TRUE(r_.driveUntilJudged());
            r_.restart();
            r_.passes(2);
            r_.restart();
            return;
        default:
            sendAll();
            return;
        }
    }

    void checkAcks_(const Expected& ex, const CheckedWritesModel& model) {
        if (c_.entity == Entity::OldWriters) {
            for (const Sent& s : sent_) {
                const auto a = r_.ackFetching(s.corr);
                ASSERT_TRUE(a);
                EXPECT_EQ(a->code, 0);
                EXPECT_EQ(a->frameLen, 46u);
                EXPECT_FALSE(a->hasVerdicts);
            }
            return;
        }
        if (c_.entity != Entity::ViewAckWriter) return;
        if (ex.check == Check::NotLeaderFirst) EXPECT_EQ(firstAckCode_, kNotLeader);
        std::map<int64_t, VerdictRig::Ack> okAcks;
        for (const Sent& s : sent_) {
            const auto a = r_.ackFetching(s.corr);
            ASSERT_TRUE(a) << "corr " << s.corr;
            okAcks[s.corr] = *a;
            if (a->code != 0) {
                EXPECT_EQ(c_.timing, Timing::RetryAfterWindow);
                continue;
            }
            checkModelTrailer(model, *a, s.recs.size(), s.corr);
        }
        for (const auto& [orig, dup] : dups_) checkDupAck(r_, okAcks.at(orig), okAcks.at(dup), ex.check == Check::RetryOutsideWindow);
    }

    Cell c_;
    VerdictRig r_;
    OpPlan plan_;
    std::vector<std::vector<Rec>> batches_;
    bool frames_ = false;
    std::vector<Sent> sent_;
    std::vector<std::pair<int64_t, int64_t>> dups_;
    std::map<int64_t, int32_t> seqs_;
    std::size_t uniqueRecs_ = 0;
    int16_t firstAckCode_ = 0;
    int64_t target_ = 0;
};

}  // namespace

// ---- Part 05: intents against the key index.  Every op is sent by an idempotent v17 producer with All | View. ----
namespace {

constexpr uint64_t kKey = 5;
constexpr int64_t kSetupPid = 41, kOpPid = 500, kOtherPid = 501, kTxnPid = 77;
constexpr int16_t kInternal = static_cast<int16_t>(ErrorCode::Internal);
constexpr int64_t kAckDeadlineNs = 50'000'000;
using Trailer = AckTrailer;

Rec ups(uint64_t id, int64_t v, uint8_t tag = 1) { return Rec{id, v, tag, false, 0}; }

Rec opRec(Op op) {
    switch (op) {
    case Op::Insert: return Rec{kKey, 100, 1, false, 1};
    case Op::Update: return Rec{kKey, 100, 1, false, 2};
    case Op::Upsert: return Rec{kKey, 100, 1, false, 0};
    case Op::Remove: return Rec{kKey, 0, 0, true, 0};
    default: return Rec{kKey, 0, 0, true, 2};   // RemoveExisting
    }
}

RigTuning tuningOf(const Cell& c) {
    RigTuning t;
    if (c.timing == Timing::AckDeadline) { t.minIsr = 2; t.ackTimeoutNs = kAckDeadlineNs; }
    if (c.timing == Timing::StrictTier) t.durabilityTier = 2;
    return t;
}

class KeyIndexCellRun {
public:
    explicit KeyIndexCellRun(const Cell& c) : c_(c), r_(/*follower=*/true, /*rollBytes=*/1, tuningOf(c)) {}

    void run() {
        const Expected ex = expect(c_);
        preIndex_ = c_.entity == Entity::KeyBeforeIndex || c_.timing == Timing::CreateOnNonEmpty;
        r_.createKeyIndex({}, !preIndex_);
        if (c_.entity == Entity::KeyRejectedEarlier) r_.installRejectView(kRej, kObj, tagRule(9));
        r_.installProjection(kProj);
        r_.installCountById(kCnt);
        r_.enableAcks();
        r_.passes(60);
        r_.followerFetch();
        r_.passes(10);
        if (!preIndex_) ASSERT_TRUE(waitActive_());
        setupKey_();
        if (::testing::Test::HasFatalFailure()) return;
        if (c_.timing == Timing::CreateOnNonEmpty) {
            for (uint64_t base = 1000; base < 1500; base += 50) {
                std::vector<Rec> recs;
                for (uint64_t id = base; id < base + 50; ++id) recs.push_back(ups(id, 1));
                ASSERT_EQ(r_.produce(recs), 0);
                stored_ += recs.size();
            }
        }
        if (preIndex_) {
            r_.followerFetch();
            if (c_.timing == Timing::CreateOnNonEmpty) {   // the op is sent in the pass that takes the adoption, with the judge held: before the change record
                r_.holdJudge(true);
                r_.requestKeyIndex();
            } else {
                r_.enableKeyIndex();
                ASSERT_TRUE(waitActive_());
            }
        }
        execute_();
        if (::testing::Test::HasFatalFailure()) return;

        const int64_t target = waitCommitted_();
        ASSERT_TRUE(r_.driveUntilJudged(target, VerdictRig::kMaxPass * strictFactor_()));
        r_.passes(60);   // the views fold up to the judged cursor
        const auto log = r_.readLog();
        CheckedWritesModel model;
        model.declareRule(kObj, tagRule(9).fingerprint(), modelRule(tagRule(9)));
        model.addKeyIndexRule();
        model.reset(withoutAbortedTxn_(log));

        checkIndex(r_, model);                                              // (a)
        checkMarkers(model, log, target);
        std::set<std::string> live;                                         // (b)
        for (const auto& [id, row] : model.projection()) live.insert(std::to_string(id));
        EXPECT_EQ(r_.keyIndexKeys(), live) << "(b) the key index holds the live keys of the accepted records";
        checkDelivered_(model);                                             // (c)
        checkViews(r_, model, log, kProj, kCnt);                            // (d)
        checkAcks_(ex, model, log);
        EXPECT_EQ(r_.writeState()->rejected.mismatches(), 0u);              // (f)
        EXPECT_EQ(r_.stats()->keyIndexSkippedMalformed, 0u);
        checkCounts_(model);                                                // (g)
        checkCompaction(r_, model, log);                                    // (e)

        std::set<int64_t> present;
        for (const auto& d : r_.readLog().data) present.insert(d.offset);
        r_.restart();
        const auto compacted = r_.readLog();   // a change logged again after the compaction has no records to cover
        ASSERT_TRUE(r_.driveUntilJudged(compacted.data.empty() ? 0 : compacted.data.back().offset + 1));
        r_.passes(60);
        EXPECT_EQ(r_.keyIndexKeys(), live) << "(b) the rebuild after a compaction and a restart";
        checkIndex(r_, model, &present);
        ASSERT_NE(r_.stats(), nullptr);
        EXPECT_EQ(r_.stats()->keyIndexSkippedMalformed, 0u);
    }

private:
    struct Sent { int64_t corr; int64_t pid; int32_t seq; std::vector<Rec> recs; };

    // Only a Strict append waits for an fsync, which takes wall-clock time: its caps are 50 times larger.
    int strictFactor_() const { return c_.timing == Timing::StrictTier ? 50 : 1; }
    int ackPasses_() const { return 3000 * strictFactor_(); }

    // The HW reaches the log end; a Strict append reaches it only after its fsync completed, which takes wall-clock time.  Returns what the judge must cover.
    int64_t waitCommitted_() {
        r_.passes(10);   // the frames are taken off the ingress
        for (int i = 0; i < VerdictRig::kMaxPass * strictFactor_() && r_.replica->highWatermark() < r_.leo(); ++i) {
            if (i % 8 == 0) r_.followerFetch();
            r_.pass();
        }
        EXPECT_EQ(r_.replica->highWatermark(), r_.leo());
        return r_.dataEnd();
    }

    bool waitActive_() {
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            const PartitionWriteState* w = r_.writeState();
            if (w != nullptr && w->constraintSets.latest().count > 0 && w->constraintSets.latest().members[0].kind == rune::view::VerdictViewKind::KeyIndex &&
                r_.worker->rcStatusForTest(VerdictRig::kSrc, 0) == Worker::kRcReady) return true;
            if (i % 8 == 0) r_.followerFetch();
            r_.pass();
        }
        ADD_FAILURE() << "the key index was not activated";
        return false;
    }

    Sent send_(int64_t pid, const std::vector<Rec>& recs, uint8_t acks = kAcksViewAll, bool txn = false) {
        const int32_t seq = seqs_[pid];
        seqs_[pid] += static_cast<int32_t>(recs.size());
        stored_ += recs.size();
        const int64_t corr = routed_() ? r_.routedFrame(recs, acks, pid, seq) : r_.produceFrame(recs, acks, pid, seq, txn);
        return {corr, pid, seq, recs};
    }
    bool routed_() const { return c_.timing == Timing::GatewayRouted; }
    void resend_(Sent s) {
        sent_.push_back({r_.produceFrame(s.recs, kAcksViewAll, s.pid, s.seq), s.pid, s.seq, s.recs});
        dups_.emplace_back(s.corr, sent_.back().corr);
    }
    void sendOp_(int64_t pid = kOpPid) { sent_.push_back(send_(pid, opBatch_())); }

    // The hook answers an intent NotLeader (KeyIndexActivating) until the change record naming the key index is logged; upserts and removes are not gated.
    bool gatedByActivation_() const { return c_.op == Op::Insert || c_.op == Op::Update || c_.op == Op::RemoveExisting; }

    std::vector<Rec> opBatch_() const {
        std::vector<Rec> b;
        if (c_.entity == Entity::KeyEarlierInBatch) b.push_back(ups(kKey, 10));
        b.push_back(opRec(c_.op));
        if (c_.timing == Timing::SelfCollidingBatch) b.push_back(opRec(c_.op));
        return b;
    }

    // The state of key k before the op.  Before the key index exists the writes are plain ones on the op path.
    void write_(const std::vector<Rec>& recs) {
        if (preIndex_) {
            ASSERT_EQ(r_.produce(recs), 0);
            stored_ += recs.size();
            r_.followerFetch();
            return;
        }
        const Sent s = send_(kSetupPid, recs);
        const auto a = r_.ackFetching(s.corr, ackPasses_());
        ASSERT_TRUE(a);
        EXPECT_EQ(a->code, 0);
        setupTrailer_ = a->verdicts;
    }
    void setupKey_() {
        switch (c_.entity) {
        case Entity::KeyExisting: case Entity::KeyBeforeIndex: write_({ups(kKey, 10)}); break;
        case Entity::KeyRemovedEarlier: write_({ups(kKey, 10)}); write_({Rec{kKey, 0, 0, true, 0}}); break;
        case Entity::KeyRejectedEarlier:
            write_({Rec{kKey, 10, 9, false, static_cast<uint8_t>(preIndex_ ? 0 : 1)}});
            if (!preIndex_) EXPECT_EQ(setupTrailer_, (Trailer{{0, static_cast<uint16_t>(ErrorCode::ConstraintViolation), CheckedWritesModel::constraintIdOf(kObj)}}));
            break;
        default: break;
        }
    }

    // An op sent while the key index is still built is answered NotLeader and sent again (nothing was appended).
    // A write the hook does not gate is appended before the change record; the held judge is released once it is in the log.
    void sendOpUntilAccepted_() {
        for (int i = 0; i < 100; ++i) {
            const Sent s = send_(kOpPid, opBatch_());
            if (!gatedByActivation_()) {
                r_.passes(5);
                r_.holdJudge(false);
                sent_.push_back(s);
                if (!waitActive_()) return;
                sent_.push_back(send_(kOpPid, {ups(2000, 1)}));   // the op predates the index and is not judged: a record above the change gives the markers something to cover
                return;
            }
            const auto a = r_.ackFetching(s.corr, ackPasses_());
            ASSERT_TRUE(a);
            if (a->code == 0) {
                EXPECT_GE(r_.worker->hookRefusalsForTest(HookRefusal::KeyIndexActivating), 1u) << "the op was sent while the index was built";
                sent_.push_back(s);
                return;
            }
            ASSERT_EQ(a->code, kNotLeader);
            r_.holdJudge(false);
            stored_ -= s.recs.size();
            seqs_[kOpPid] -= static_cast<int32_t>(s.recs.size());
            r_.passes(5);
        }
        ADD_FAILURE() << "the op was never accepted";
    }

    void execute_() {
        switch (c_.timing) {
        case Timing::BackToBack:
            for (const Rec& x : opBatch_()) sent_.push_back(send_(kOpPid, {x}));
            return;
        case Timing::Racing:
            sendOp_(kOpPid);
            sendOp_(kOtherPid);
            return;
        case Timing::RetryBeforeJudge: case Timing::RetryAfterJudge: case Timing::RetryAfterWindow:
            ASSERT_TRUE(executeRetry(r_, c_.timing != Timing::RetryBeforeJudge, c_.timing == Timing::RetryAfterWindow, [&] { sendOp_(); }, [&](uint64_t i) { sent_.push_back(send_(kOpPid, {ups(100 + i, 1)})); }, [&] { resend_(sent_[0]); }));
            return;
        case Timing::CreateOnNonEmpty:
            sendOpUntilAccepted_();
            return;
        case Timing::FailoverAppendHw:
            sendOp_();
            r_.passes(5);
            lostAndResend_([&] { r_.demote(); }, [&] { r_.failover(); });
            return;
        case Timing::TruncateTail:
            sendOp_();
            r_.passes(5);
            lostAndResend_([&] { r_.truncateTo(r_.replica->highWatermark()); r_.demotePromote(); }, [] {});
            return;
        case Timing::FailoverHwFold:
            sendOp_();
            ASSERT_TRUE(r_.driveUntilJudged());
            ASSERT_TRUE(r_.ackFetching(sent_[0].corr, ackPasses_()));   // answered before the node is lost
            r_.followerFetch();
            r_.failover();
            return;
        case Timing::FailoverFoldMarker:
            sendOp_();
            r_.passes(5);
            r_.followerFetch();
            for (int i = 0; i < VerdictRig::kMaxPass && r_.leo() <= r_.replica->highWatermark(); ++i) r_.pass();   // the marker sits above the HW
            ASSERT_GT(r_.leo(), r_.replica->highWatermark());
            r_.failover();
            return;
        case Timing::RestartDuringRebuild:
            sendOp_();
            ASSERT_TRUE(r_.driveUntilJudged());
            ASSERT_TRUE(r_.ackFetching(sent_[0].corr, ackPasses_()));
            r_.restart();
            r_.passes(2);
            r_.restart();
            return;
        case Timing::TxnCommitBehind: case Timing::TxnAbortBehind: {
            const Sent txn = send_(kTxnPid, {ups(kKey, 55)}, kAcksAll, /*txn=*/true);   // a third producer's open transaction on k
            const auto ta = r_.ackFetching(txn.corr, ackPasses_());
            ASSERT_TRUE(ta);
            EXPECT_EQ(ta->code, 0);
            txnOffset_ = ta->baseOffset;
            sendOp_();
            for (int i = 0; i < 300; ++i) {   // the op is below the HW (the follower keeps fetching) while the transaction is open
                if (i % 8 == 0) r_.followerFetch();
                r_.pass();
            }
            ASSERT_FALSE(r_.acked(sent_[0].corr)) << "behind an open transaction the judge is held";
            r_.marker(kTxnPid, c_.timing == Timing::TxnCommitBehind);
            return;
        }
        case Timing::LeaderLossParked:
            sendOp_();
            r_.passes(5);
            ASSERT_FALSE(r_.acked(sent_[0].corr)) << "parked until the HW moves";
            r_.demote();
            ASSERT_TRUE(r_.ack(sent_[0].corr));
            firstAckCode_ = r_.ack(sent_[0].corr)->code;
            r_.promote();
            return;
        case Timing::AckDeadline:   // the follower does not fetch: the parked View ack times out
        {
            const int64_t sentStep = r_.passNo();
            sendOp_();
            const auto a = r_.ack(sent_[0].corr);
            ASSERT_TRUE(a);
            EXPECT_GE(a->step - sentStep, kAckDeadlineNs / 1'000'000) << "never earlier than the deadline (a pass is 1 ms)";
            EXPECT_LE(a->step - sentStep, kAckDeadlineNs / 1'000'000 + 20);
            return;
        }
        default:   // Single, SelfCollidingBatch, StrictTier, GatewayRouted
            sendOp_();
            return;
        }
    }

    // The op is sent, the leader loses it (`lose`), the node is leader again and the op is sent once more from a fresh sequence.
    template <class Lose, class Back>
    void lostAndResend_(Lose lose, Back back) {
        const int64_t first = sent_.front().corr;
        lose();
        const auto a = r_.ack(first);
        ASSERT_TRUE(a);
        firstAckCode_ = a->code;
        back();
        for (const Sent& s : sent_) stored_ -= s.recs.size();   // the cut tail is gone
        sent_.clear();
        seqs_.erase(kOpPid);
        ASSERT_TRUE(waitActive_());
        r_.passes(10);
        sendOp_();
    }

    VerdictRig::RigLog withoutAbortedTxn_(VerdictRig::RigLog log) const {
        if (c_.timing == Timing::TxnAbortBehind)
            std::erase_if(log.data, [&](const VerdictRig::RigLogRec& d) { return d.offset == txnOffset_; });
        return log;
    }

    static uint64_t count_(const CheckedWritesModel& model, uint16_t code) {
        uint64_t n = 0;
        for (const auto& [off, v] : model.verdicts()) n += v.code == code ? 1 : 0;
        return n;
    }
    // (g) A restarted node counts only what it judges itself.  FailoverFoldMarker cuts the op's marker, so it judges the op again; FailoverHwFold
    // and RestartDuringRebuild restart with every marker committed, so they judge nothing.
    void checkCounts_(const CheckedWritesModel& model) {
        const bool judgedNothing = c_.timing == Timing::FailoverHwFold || c_.timing == Timing::RestartDuringRebuild;
        EXPECT_EQ(r_.stats()->rejectionsKeyExists, judgedNothing ? 0u : count_(model, 56));
        EXPECT_EQ(r_.stats()->rejectionsKeyNotFound, judgedNothing ? 0u : count_(model, 57));
    }

    // (c) a raw subscription from 0 receives exactly the accepted records, each with its intent as op(); never the aborted transaction's (P11-10)
    void checkDelivered_(const CheckedWritesModel& model) {
        for (int i = 0; i < VerdictRig::kMaxPass && r_.worker->rcStatusForTest(VerdictRig::kSrc, 0) != Worker::kRcReady; ++i) r_.pass();
        ASSERT_EQ(r_.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcReady);
        const auto& box = r_.collect(r_.subscribe(SubKind::Raw, 0));
        std::vector<std::tuple<int64_t, uint64_t, uint16_t, uint8_t>> got, want;
        for (const auto& d : box.records) got.emplace_back(d.offset, d.id, d.tag, d.intent);
        for (const auto& a : model.acceptedRecords()) want.emplace_back(a.offset, a.id, a.del ? kTombstoneTag : a.tag, a.intent);
        EXPECT_EQ(got, want) << "(c) the delivered records are the accepted ones, with their intents";
    }

    void checkAcks_(const Expected& ex, const CheckedWritesModel& model, const VerdictRig::RigLog& log) {
        if (ex.check == Check::NotLeaderFirst) EXPECT_EQ(firstAckCode_, kNotLeader);
        std::map<int64_t, VerdictRig::Ack> acks;
        for (const Sent& s : sent_) {
            const auto a = r_.ackFetching(s.corr, ackPasses_());
            ASSERT_TRUE(a) << "corr " << s.corr;
            acks[s.corr] = *a;
            if (ex.check == Check::NotLeaderParked) { EXPECT_EQ(a->code, kNotLeader); EXPECT_EQ(firstAckCode_, kNotLeader); continue; }
            if (ex.check == Check::DeadlineInternal) { EXPECT_EQ(a->code, kInternal); continue; }
            if (a->code != 0) {
                EXPECT_EQ(ex.check, Check::RetryOutsideWindow);
                continue;
            }
            checkModelTrailer(model, *a, s.recs.size(), s.corr);
        }
        for (const auto& [orig, dup] : dups_) checkDupAck(r_, acks.at(orig), acks.at(dup), ex.check == Check::RetryOutsideWindow);
        if (ex.check == Check::RetryReplays) EXPECT_EQ(log.data.size(), stored_) << "a retry is stored once";

        if (c_.timing == Timing::CreateOnNonEmpty && !gatedByActivation_()) {   // the op sits below the key index's change record: it met the index while it was built
            const auto change = std::find_if(log.changes.begin(), log.changes.end(), [](const auto& c) {
                return !c.members.empty() && c.members[0].kind == static_cast<uint8_t>(rune::view::VerdictViewKind::KeyIndex);
            });
            ASSERT_TRUE(change != log.changes.end());
            EXPECT_LT(acks.at(sent_[0].corr).baseOffset, change->offset);
        }
        const std::size_t opAt = c_.entity == Entity::KeyEarlierInBatch ? 1 : 0;
        if (ex.check == Check::KeyIndexTable) {
            const uint16_t code = keyIndexVerdict05(c_.op, c_.entity);
            Trailer want;
            if (code != 0) want.emplace_back(static_cast<uint16_t>(opAt), code, 0);
            EXPECT_EQ(acks.at(sent_[0].corr).verdicts, want) << "the P5-3 table";
        } else if (ex.check == Check::SelfColliding) {
            const auto codes = selfCollidingCodes05(c_.op, c_.entity);
            if (codes.empty()) return;
            Trailer want;
            for (std::size_t i = 0; i < 2; ++i)
                if (codes[i] != 0) want.emplace_back(static_cast<uint16_t>(opAt + i), codes[i], 0);
            EXPECT_EQ(acks.at(sent_[0].corr).verdicts, want);
        } else if (ex.check == Check::RacingOneWins) {
            const auto& x = acks.at(sent_[0].corr);
            const auto& y = acks.at(sent_[1].corr);
            const auto& lo = x.baseOffset < y.baseOffset ? x : y;
            const auto& hi = x.baseOffset < y.baseOffset ? y : x;
            EXPECT_EQ(lo.verdicts, Trailer{});
            EXPECT_EQ(hi.verdicts, (Trailer{{0, 56, 0}}));
        }
    }

    Cell c_;
    VerdictRig r_;
    bool preIndex_ = false;
    std::vector<Sent> sent_;
    std::vector<std::pair<int64_t, int64_t>> dups_;
    std::map<int64_t, int32_t> seqs_;
    std::size_t stored_ = 0;
    int16_t firstAckCode_ = 0;
    int64_t txnOffset_ = -1;
    Trailer setupTrailer_;
};

}  // namespace

// ---- Part 06: autoincrement.  Every op is sent by an idempotent producer with All | View, import-granted. ----
namespace {

constexpr int64_t kFillPid = 502;
constexpr int16_t kInvalidRequest = static_cast<int16_t>(ErrorCode::InvalidRequest);
constexpr int16_t kExhausted = static_cast<int16_t>(ErrorCode::AutoIncrementExhausted);
constexpr int16_t kDuplicateSequence = static_cast<int16_t>(ErrorCode::DuplicateSequence);

/// The topic and the rig's partition of an entity, and the creates acked in setup.
struct AutoIncSetup { AutoIncShape shape; int32_t partition = 0; int creates = 3; };

AutoIncSetup setupOf(Entity e) {
    switch (e) {
    case Entity::P4U64: return {AutoIncShape{.partitions = 4}, 2};
    case Entity::P1U32: return {AutoIncShape{.keyType = rune::codec::PrimType::UInt32}, 0};
    case Entity::P2I64: return {AutoIncShape{.partitions = 2, .keyType = rune::codec::PrimType::Int64}, 1};
    case Entity::AtReservationEdge: return {AutoIncShape{.reserve = 8}, 0, 6};
    case Entity::NearMax: return {AutoIncShape{.max = 10}, 0, 7};
    default: return {};
    }
}

Rec createRec(uint8_t tag) { return Rec{0, 100, tag, false, 1}; }

class AutoIncCellRun {
public:
    explicit AutoIncCellRun(const Cell& c) : c_(c), setup_(setupOf(c.entity)), r_(/*follower=*/true, /*rollBytes=*/1, tuningWith_(c, setup_.partition)) {}

    void run() {
        const Expected ex = expect(c_);
        refusal_ = ex.check == Check::AutoExhausted ? kExhausted : ex.check == Check::ImportOutOfRange ? kInvalidRequest : int16_t{0};
        r_.createAutoIncTopic(setup_.shape);
        if (::testing::Test::HasFatalFailure()) return;
        if (constrained_() && c_.timing != Timing::CreateOnNonEmpty) r_.installRejectView(kRej, kObj, tagRule(9));
        r_.enableAcks();
        r_.passes(60);
        r_.followerFetch();
        r_.passes(10);
        setupCreates_();
        if (::testing::Test::HasFatalFailure()) return;
        importAboveId_ = (r_.autoIncState()->reservedThroughN + 100) * partitions_() + static_cast<uint64_t>(setup_.partition);
        execute_();
        if (::testing::Test::HasFatalFailure()) return;

        const auto judgeAll = [&] {
            waitCommitted_();
            const auto appended = withoutAbortedTxn_(r_.readLog());   // an aborted transaction at the tail needs no verdicts
            const int64_t reach = std::max(appended.data.empty() ? 0 : appended.data.back().offset + 1, appended.changes.empty() ? 0 : appended.changes.back().offset + 1);
            EXPECT_TRUE(r_.driveUntilJudged(reach, VerdictRig::kMaxPass * strictFactor_()));
            r_.passes(60);
            return reach;
        };
        int64_t target = judgeAll();
        if (::testing::Test::HasFatalFailure()) return;
        if (ex.check == Check::Burned && !faulty_()) {   // a create after the judge: the burned ids are not given back
            sendOpRecs_(kOpPid, {createRec(1)});
            target = judgeAll();
        }
        const auto log = r_.readLog();
        const auto judged = withoutAbortedTxn_(log);
        CheckedWritesModel model;
        model.declareRule(kObj, tagRule(9).fingerprint(), modelRule(tagRule(9)));
        model.declareRule(kObj, tagRule(1).fingerprint(), modelRule(tagRule(1)));
        model.addAutoIncRule(setup_.shape, setup_.partition);
        model.reset(judged);
        collectAcks_();

        checkIndex(r_, model);                                                           // (a)
        checkMarkers(model, log, target);
        const std::vector<CheckedWritesModel::AutoIncAck> acks = ackList_();
        EXPECT_TRUE(model.ackedIdsMatchTheLog(log, acks)) << "(b) an ack's assignedBase is the first key of its batch, each value carries its key";
        EXPECT_TRUE(model.idsFollowTheStride(log, acks)) << "(b) the keys are base + i * P";
        EXPECT_TRUE(model.noIdTwiceNoReuse(log, acks)) << "(c) no id twice";
        EXPECT_TRUE(model.reservationCoversEveryId(log)) << "(d) every id is under a reservation below it";
        checkAcks_(ex, model, log);
        checkStats_();                                                                   // (g)

        const auto reservedBefore = r_.reservations();
        checkCompaction(r_, model, log);
        const auto reservedAfter = r_.reservations();                                    // (f)
        ASSERT_FALSE(reservedBefore.empty());
        ASSERT_FALSE(reservedAfter.empty()) << "(f) the compaction removed every reservation";
        EXPECT_EQ(reservedAfter.back().second, reservedBefore.back().second) << "(f) the latest reservation is kept";
        (void)r_.compact(2'000'000);   // the first pass stamped the delete horizon of what it found removable; the second drops it
        const auto reservedLater = r_.reservations();
        ASSERT_FALSE(reservedLater.empty()) << "(f) the second compaction removed every reservation";
        EXPECT_EQ(reservedLater.back().second, reservedBefore.back().second) << "(f) the latest reservation is kept by a second pass";

        std::set<int64_t> present;
        for (const auto& d : r_.readLog().data) present.insert(d.offset);
        r_.restart();
        const auto compacted = withoutAbortedTxn_(r_.readLog());
        ASSERT_TRUE(r_.driveUntilJudged(compacted.data.empty() ? 0 : compacted.data.back().offset + 1));
        r_.passes(60);
        checkIndex(r_, model, &present);
        ASSERT_TRUE(waitReady_());
        EXPECT_TRUE(model.counterAboveEveryId(log, *r_.autoIncState())) << "(e) nextN " << r_.autoIncState()->nextN << " reserved " << r_.autoIncState()->reservedThroughN;
        EXPECT_EQ(r_.autoIncState()->nextN, r_.autoIncState()->reservedThroughN + 1) << "(e) a rebuilt counter resumes above the reservation";
        checkStats_();
    }

private:
    struct Sent { int64_t corr; int64_t pid; int32_t seq; std::vector<Rec> recs; bool op = false; };

    static RigTuning tuningWith_(const Cell& c, int32_t partition) {
        RigTuning t = tuningOf(c);
        t.partition = partition;
        return t;
    }
    bool constrained_() const { return c_.entity == Entity::Constrained; }
    uint64_t partitions_() const { return static_cast<uint64_t>(setup_.shape.partitions); }
    bool refused_() const { return refusal_ != 0; }
    bool createOp_() const { return isCreate06(c_.op); }
    int strictFactor_() const { return c_.timing == Timing::StrictTier ? 50 : 1; }
    int ackPasses_() const { return 3000 * strictFactor_(); }

    Sent send_(int64_t pid, const std::vector<Rec>& recs, uint8_t acks = kAcksViewAll, bool txn = false, bool routed = false) {
        const int32_t seq = seqs_[pid];
        seqs_[pid] += static_cast<int32_t>(recs.size());
        const int64_t corr = routed ? r_.routedFrame(recs, acks, pid, seq) : r_.produceFrame(recs, acks, pid, seq, txn);
        return {corr, pid, seq, recs};
    }
    // A refused batch appends nothing: the producer's sequence stays.
    void sendOpRecs_(int64_t pid, const std::vector<Rec>& recs) {
        const int32_t before = seqs_[pid];
        sent_.push_back(send_(pid, recs, kAcksViewAll, false, c_.timing == Timing::GatewayRouted));
        sent_.back().op = true;
        if (refused_()) seqs_[pid] = before;
    }
    void sendOp_(int64_t pid = kOpPid) { sendOpRecs_(pid, opBatch_()); }
    void resend_(Sent s) {
        sent_.push_back({r_.produceFrame(s.recs, kAcksViewAll, s.pid, s.seq), s.pid, s.seq, s.recs, s.op});
        dups_.emplace_back(s.corr, sent_.back().corr);
    }

    void setupCreates_() {
        for (int i = 0; i < setup_.creates; ++i) {
            const Sent s = send_(kSetupPid, {createRec(1)});
            const auto a = r_.ackFetching(s.corr, ackPasses_());
            ASSERT_TRUE(a);
            ASSERT_EQ(a->code, 0);
            EXPECT_TRUE(a->verdicts.empty());
            ids_.push_back(a->assignedBase);
            batches_[a->baseOffset] = 1;
            keyZero_[a->baseOffset] = {a->baseOffset, a->assignedBase, 1};
        }
    }

    std::vector<Rec> opRecs_() const {
        switch (c_.op) {
        case Op::AutoCreate1: return {createRec(1)};
        case Op::AutoCreate8: {
            std::vector<Rec> b;
            for (int i = 0; i < 8; ++i) b.push_back(createRec(i % 2 == 0 ? 1 : 9));
            return b;
        }
        case Op::ImportBelow: return {Rec{ids_.back(), 100, 1, false, 1}};
        case Op::ImportAbove: return {Rec{importAboveId_, 100, 1, false, 1}};
        case Op::UpdateAssigned: return {Rec{ids_.front(), 100, 9, false, 2}};
        default: return {Rec{ids_.front(), 0, 0, true, 2}};   // RemoveExistingAssigned
        }
    }
    std::vector<Rec> opBatch_() const {
        std::vector<Rec> b = opRecs_();
        if (c_.timing == Timing::SelfCollidingBatch) { const auto again = opRecs_(); b.insert(b.end(), again.begin(), again.end()); }
        return b;
    }

    void waitCommitted_() {
        r_.passes(10);
        for (int i = 0; i < VerdictRig::kMaxPass * strictFactor_() && r_.replica->highWatermark() < r_.leo(); ++i) {
            if (i % 8 == 0) r_.followerFetch();
            r_.pass();
        }
        EXPECT_EQ(r_.replica->highWatermark(), r_.leo());
    }
    bool waitReady_() {
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            if (r_.autoIncState() && r_.worker->rcStatusForTest(VerdictRig::kSrc, setup_.partition) == Worker::kRcReady) return true;
            if (i % 8 == 0) r_.followerFetch();
            r_.pass();
        }
        ADD_FAILURE() << "the counter never became ready";
        return false;
    }

    void change_() {
        r_.passes(5);
        if (c_.timing == Timing::CreateOnNonEmpty) r_.installRejectView(kRej, kObj, tagRule(9));
        else if (c_.timing == Timing::ReplaceDuringWrites) r_.replaceRejectView(kRej, kRejNew, kObj, tagRule(1));
        else r_.dropRejectView(kRej);
        r_.passes(5);
    }

    void execute_() {
        switch (c_.timing) {
        case Timing::BackToBack:   // two batches in flight before the first ack
            sendOp_();
            sendOp_();
            return;
        case Timing::Racing:
            sendOp_(kOpPid);
            sendOp_(kOtherPid);
            return;
        case Timing::RetryBeforeJudge: case Timing::RetryAfterJudge: case Timing::RetryAfterWindow: {
            const int64_t fillPid = refused_() ? kFillPid : kOpPid;   // a refused batch is in no window
            const Rec fill{ids_[1], 100, 1, false, 2};
            ASSERT_TRUE(executeRetry(r_, c_.timing != Timing::RetryBeforeJudge, c_.timing == Timing::RetryAfterWindow, [&] { sendOp_(); },
                                     [&](uint64_t) { sent_.push_back(send_(fillPid, {fill})); }, [&] { resend_(sent_[0]); }));
            return;
        }
        case Timing::CreateOnNonEmpty: case Timing::ReplaceDuringWrites: case Timing::DropDuringWrites:
            sendOp_();
            change_();
            sendOp_();
            return;
        case Timing::FailoverAppendHw:
            sendOp_();
            r_.passes(5);
            lostAndResend_([&] { cutTo_ = r_.replica->highWatermark(); r_.demote(); }, [&] { r_.restart(true, cutTo_); });   // a demoted replica forgets its HW: the cut is the leader's
            return;
        case Timing::TruncateTail:
            sendOp_();
            r_.passes(5);
            lostAndResend_([&] { r_.truncateTo(r_.replica->highWatermark()); r_.demotePromote(); }, [] {});
            return;
        case Timing::FailoverHwFold:
            sendOp_();
            ASSERT_TRUE(r_.driveUntilJudged());
            ASSERT_TRUE(r_.ackFetching(sent_[0].corr, ackPasses_()));
            r_.followerFetch();
            r_.failover();
            return;
        case Timing::FailoverFoldMarker:
            sendOp_();
            r_.passes(5);
            r_.followerFetch();
            for (int i = 0; i < VerdictRig::kMaxPass && r_.leo() <= r_.replica->highWatermark(); ++i) r_.pass();   // the marker sits above the HW
            ASSERT_GT(r_.leo(), r_.replica->highWatermark());
            r_.failover();
            return;
        case Timing::RestartDuringRebuild:
            sendOp_();
            ASSERT_TRUE(r_.driveUntilJudged());
            ASSERT_TRUE(r_.ackFetching(sent_[0].corr, ackPasses_()));
            r_.restart();
            r_.passes(2);
            r_.restart();
            return;
        case Timing::TxnCommitBehind: case Timing::TxnAbortBehind: {
            const Sent txn = send_(kTxnPid, {createRec(1), createRec(1)}, kAcksAll, /*txn=*/true);   // a third producer's open transaction
            const auto ta = r_.ackFetching(txn.corr, ackPasses_());
            ASSERT_TRUE(ta);
            EXPECT_EQ(ta->code, 0);
            txnOffset_ = ta->baseOffset;
            batches_[ta->baseOffset] = 2;
            keyZero_[ta->baseOffset] = {ta->baseOffset, ta->assignedBase, 2};
            sendOp_();
            for (int i = 0; i < 300; ++i) {
                if (i % 8 == 0) r_.followerFetch();
                r_.pass();
            }
            if (!refused_()) ASSERT_FALSE(r_.acked(sent_[0].corr)) << "behind an open transaction the judge is held";
            r_.marker(kTxnPid, c_.timing == Timing::TxnCommitBehind);
            return;
        }
        case Timing::LeaderLossParked:
            sendOp_();
            r_.passes(5);
            if (!refused_()) ASSERT_FALSE(r_.acked(sent_[0].corr)) << "parked until the HW moves";
            r_.demote();
            ASSERT_TRUE(r_.ack(sent_[0].corr));
            firstAckCode_ = r_.ack(sent_[0].corr)->code;
            r_.promote();
            return;
        case Timing::AckDeadline: {   // the follower does not fetch: the parked View ack times out
            const int64_t sentStep = r_.passNo();
            sendOp_();
            const auto a = r_.ack(sent_[0].corr);
            ASSERT_TRUE(a);
            if (!refused_()) {
                EXPECT_GE(a->step - sentStep, kAckDeadlineNs / 1'000'000) << "never earlier than the deadline (a pass is 1 ms)";
                EXPECT_LE(a->step - sentStep, kAckDeadlineNs / 1'000'000 + 20);
            }
            return;
        }
        default:   // Single, SelfCollidingBatch, StrictTier, GatewayRouted
            sendOp_();
            return;
        }
    }

    // The op is sent, the leader loses it (`lose`), the node is leader again and the op is sent once more from a fresh sequence.
    template <class Lose, class Back>
    void lostAndResend_(Lose lose, Back back) {
        const int64_t first = sent_.front().corr;
        lose();
        const auto a = r_.ack(first);
        ASSERT_TRUE(a);
        firstAckCode_ = a->code;
        back();
        if (expect(c_).check == Check::NotLeaderThenExhausted) refusal_ = kExhausted;   // the rebuilt counter resumes above the capped reservation
        sent_.clear();
        seqs_.erase(kOpPid);
        ASSERT_TRUE(waitReady_());
        r_.passes(10);
        sendOp_();
    }

    VerdictRig::RigLog withoutAbortedTxn_(VerdictRig::RigLog log) const {
        if (c_.timing == Timing::TxnAbortBehind)
            std::erase_if(log.data, [&](const VerdictRig::RigLogRec& d) { return d.offset == txnOffset_ || d.offset == txnOffset_ + 1; });
        return log;
    }

    // Every OK ack of a batch: the batches stored (a retry's ack names the original's offset) and the key-0 ones among them.
    void collectAcks_() {
        for (const Sent& s : sent_) {
            const auto a = r_.ackFetching(s.corr, ackPasses_());
            ASSERT_TRUE(a) << "corr " << s.corr;
            acksByCorr_[s.corr] = *a;
            if (a->code != 0 || (s.op && refused_())) continue;
            batches_[a->baseOffset] = s.recs.size();
            if (s.recs.front().id == 0) keyZero_[a->baseOffset] = {a->baseOffset, a->assignedBase, s.recs.size()};
        }
    }
    std::vector<CheckedWritesModel::AutoIncAck> ackList_() const {
        std::vector<CheckedWritesModel::AutoIncAck> out;
        for (const auto& [offset, a] : keyZero_) out.push_back(a);
        return out;
    }

    static Trailer want_(std::size_t at, uint16_t code) {
        return code == 0 ? Trailer{} : Trailer{{static_cast<uint16_t>(at), code, code == 55 ? kConstraintId06 : 0u}};
    }

    void checkAcks_(const Expected& ex, const CheckedWritesModel& model, const VerdictRig::RigLog& log) {
        if (ex.check == Check::NotLeaderFirst || ex.check == Check::NotLeaderThenExhausted) EXPECT_EQ(firstAckCode_, kNotLeader);
        for (const Sent& s : sent_) {
            const VerdictRig::Ack& a = acksByCorr_.at(s.corr);
            if (s.op && refused_()) { EXPECT_EQ(a.code, refusal_) << "corr " << s.corr; EXPECT_EQ(a.assignedBase, 0u); continue; }
            if (ex.check == Check::NotLeaderParked) { EXPECT_EQ(a.code, kNotLeader); EXPECT_EQ(firstAckCode_, kNotLeader); continue; }
            if (ex.check == Check::DeadlineInternal) { EXPECT_EQ(a.code, kInternal); continue; }
            if (a.code != 0) {
                EXPECT_EQ(ex.check, Check::RetryOutsideWindow);
                EXPECT_EQ(a.code, kDuplicateSequence);
                EXPECT_EQ(a.assignedBase, 0u) << "an answer outside the window carries no ids";
                continue;
            }
            checkModelTrailer(model, a, s.recs.size(), s.corr);
            EXPECT_EQ(a.assignedBase != 0, s.recs.front().id == 0) << "only a key-0 batch answers its ids";
        }
        for (const auto& [orig, dup] : dups_) {
            if (refused_()) break;   // nothing was stored: the re-send is refused again
            const auto& o = acksByCorr_.at(orig);
            const auto& d = acksByCorr_.at(dup);
            checkDupAck(r_, o, d, ex.check == Check::RetryOutsideWindow);
            if (ex.check == Check::RetryReplays) {
                EXPECT_EQ(d.assignedBase, o.assignedBase) << "the duplicate answers the original's ids";
                EXPECT_EQ(d.frameLen, o.frameLen);
            }
        }
        if (ex.check == Check::RetryReplays) {
            std::size_t stored = 0;
            for (const auto& [offset, n] : batches_) stored += n;
            EXPECT_EQ(log.data.size(), stored) << "a retry is stored once";
        }
        if (refused_()) {
            std::size_t stored = 0;
            for (const auto& [offset, n] : batches_) stored += n;
            EXPECT_EQ(log.data.size(), stored) << "a refused batch stores nothing";
            if (ex.check == Check::AutoExhausted && !faulty_()) {
                uint64_t next = 1;
                for (const auto& [offset, a] : keyZero_) next = std::max(next, (a.assignedBase + (a.count - 1) * partitions_()) / partitions_() + 1);
                EXPECT_EQ(r_.autoIncState()->nextN, next) << "the refusal leaves the counter";
            }
            return;
        }
        if (ex.check == Check::DeadlineInternal && createOp_()) {   // the ids were taken: once the fold resumes the log holds the batch with them
            const std::size_t at = static_cast<std::size_t>(setup_.creates);
            ASSERT_GE(log.data.size(), at + opRecs_().size());
            for (std::size_t i = 0; i < opRecs_().size(); ++i) EXPECT_EQ(log.data[at + i].id, ids_.back() + (i + 1) * partitions_());
        }
        const auto opAck = [&](std::size_t k) -> const VerdictRig::Ack& { return acksByCorr_.at(sent_[k].corr); };
        if (ex.check == Check::AutoVerdict) {
            // the first batch meets the constraint set the Single timing has; CreateOnNonEmpty adds the member after it
            const bool constrained = constrained_() && c_.timing != Timing::CreateOnNonEmpty;
            EXPECT_EQ(opAck(0).verdicts, want_(0, verdict06(c_.op, constrained))) << "the verdict of the op";
        } else if (ex.check == Check::CollidingPair) {
            const auto [first, second] = collidingCodes06(c_.op, c_.entity);
            if (c_.timing == Timing::SelfCollidingBatch) {
                Trailer w = want_(0, first);
                for (const auto& e : want_(1, second)) w.push_back(e);
                EXPECT_EQ(opAck(0).verdicts, w);
            } else {   // Racing: the batch at the lower offset meets the first state
                const bool aFirst = opAck(0).baseOffset < opAck(1).baseOffset;
                EXPECT_EQ(opAck(aFirst ? 0 : 1).verdicts, want_(0, first));
                EXPECT_EQ(opAck(aFirst ? 1 : 0).verdicts, want_(0, second));
            }
        } else if (ex.check == Check::RacingIds) {
            const bool aFirst = opAck(0).baseOffset < opAck(1).baseOffset;
            const auto& lo = opAck(aFirst ? 0 : 1);
            const auto& hi = opAck(aFirst ? 1 : 0);
            EXPECT_EQ(hi.assignedBase, lo.assignedBase + opRecs_().size() * partitions_()) << "the second batch continues the first";
        } else if (ex.check == Check::Burned) {
            std::size_t rejected = 0;
            for (const auto& [offset, a] : keyZero_)
                for (std::size_t i = 0; i < a.count; ++i) rejected += model.verdicts().count(a.baseOffset + static_cast<int64_t>(i));
            EXPECT_GT(rejected, 0u);
            if (!faulty_()) {
                EXPECT_EQ(r_.autoIncStats().idsBurned, rejected) << "every rejected create burned its id";
                const auto& later = acksByCorr_.at(sent_.back().corr);
                uint64_t next = 0;
                for (const auto& [offset, a] : keyZero_)
                    if (offset < later.baseOffset) next = std::max(next, a.assignedBase + a.count * partitions_());
                EXPECT_EQ(later.assignedBase, next) << "a later create continues after the burned ids";
            }
        }
        if (c_.op == Op::ImportAbove && c_.timing == Timing::Single) {
            const auto resv = r_.reservations();
            const auto above = std::find_if(resv.begin(), resv.end(), [&](const auto& x) { return x.second >= importAboveId_ / partitions_(); });
            ASSERT_TRUE(above != resv.end()) << "an import above the reservation reserves through its n";
            EXPECT_EQ(above->first, opAck(0).baseOffset - 1) << "the reservation sits right below the import";
            EXPECT_EQ(r_.autoIncState()->nextN, importAboveId_ / partitions_() + 1);
        }
        if (c_.op == Op::ImportBelow && c_.timing == Timing::Single) EXPECT_EQ(r_.autoIncState()->nextN, ids_.back() / partitions_() + 1) << "the counter is unchanged";
    }
    bool faulty_() const {
        return c_.timing == Timing::FailoverAppendHw || c_.timing == Timing::FailoverHwFold || c_.timing == Timing::FailoverFoldMarker ||
               c_.timing == Timing::TruncateTail || c_.timing == Timing::RestartDuringRebuild || c_.timing == Timing::LeaderLossParked;
    }

    // (g) the side window never missed, no assigned id met a stored key, the rejected index agrees with the log
    void checkStats_() {
        EXPECT_EQ(r_.autoIncStats().windowMisses, 0u);
        EXPECT_EQ(r_.autoIncStats().assignedKeyExists, 0u);
        EXPECT_EQ(r_.writeState()->rejected.mismatches(), 0u);
    }

    Cell c_;
    AutoIncSetup setup_;
    VerdictRig r_;
    int16_t refusal_ = 0;
    uint64_t importAboveId_ = 0;
    std::vector<uint64_t> ids_;
    std::vector<Sent> sent_;
    std::vector<std::pair<int64_t, int64_t>> dups_;
    std::map<int64_t, int32_t> seqs_;
    std::map<int64_t, VerdictRig::Ack> acksByCorr_;
    std::map<int64_t, std::size_t> batches_;
    std::map<int64_t, CheckedWritesModel::AutoIncAck> keyZero_;
    int16_t firstAckCode_ = 0;
    int64_t txnOffset_ = -1, cutTo_ = 0;
};

}  // namespace


// ---- Part 09: view constraints.  A character source (autoincrement, key-indexed) with `character_name` (unique over lower(name), object id 7) and
// `account_stats` (count per user, at most 12, object id 8) as verdict members.  Setup that breaks a constraint is written before the views exist
// (unjudged below their change records).  Every op is sent by idempotent producers with All | View; the model judges the committed log. ----
namespace {

constexpr int64_t kBgPid = 502;

class ConstraintCellRun {
public:
    using CV = ConstrainedView;
    explicit ConstraintCellRun(const Cell& c) : c_(c), sc_(scenario09(c.op, c.entity)), r_(/*follower=*/true, /*rollBytes=*/1) {}

    void run() {
        const Expected ex = expect(c_);
        r_.worker->setVerdictIntervalNs(1'000'000);
        r_.createCharacterTopic(true);
        if (::testing::Test::HasFatalFailure()) return;
        r_.enableAcks();
        r_.passes(60);
        r_.followerFetch();
        r_.passes(10);
        writeSetup_();
        if (::testing::Test::HasFatalFailure()) return;
        createViews_();
        if (::testing::Test::HasFatalFailure()) return;
        execute_();
        if (::testing::Test::HasFatalFailure()) return;

        members_ = c_.timing == Timing::DropDuringWrites ? 2u : 3u;
        ASSERT_TRUE(waitMembers_(members_));
        waitCommitted_();
        const auto written = r_.readLog();
        target_ = written.data.back().offset + 1;
        ASSERT_TRUE(r_.driveUntilJudged(target_));
        r_.passes(60);
        settleViews_();

        const auto log = r_.readLog();
        CheckedWritesModel model = modelOf_(log);
        collectAcks_();

        checkIndex(r_, model);                                              // (a)
        checkMarkers(model, log, target_);                                  // (b)
        checkDelivered_(model);                                             // (c)
        checkAcks_(ex, model, log);                                         // (k)
        checkViews_(model, log);                                            // (h) (i) (j)
        checkStats_();                                                      // (f) (g)
        checkCompaction(r_, model, log);                                    // (e)

        std::set<int64_t> present;
        for (const auto& d : r_.readLog().data) present.insert(d.offset);
        r_.restart();
        const auto compacted = r_.readLog();   // a rebuild from the compacted log gives the same owners and counts
        ASSERT_TRUE(waitMembers_(members_));
        ASSERT_TRUE(r_.driveUntilJudged(compacted.data.empty() ? 0 : compacted.data.back().offset + 1));
        r_.passes(60);
        settleViews_();
        checkIndex(r_, model, &present);
        checkViews_(model, log);
        checkStats_();
    }

private:
    struct Sent { int64_t corr; int64_t pid; int32_t seq; std::vector<Char09> recs; };

    static VerdictRig::Char charOf_(const Char09& c) {
        return VerdictRig::Char{.id = c.id, .userId = c.userId, .name = c.name, .gender = 0, .lastPlayed = c.lastPlayed, .intent = c.intent, .tombstone = c.del};
    }
    static constexpr int ackPasses_() { return 3000; }

    Sent send_(int64_t pid, const std::vector<Char09>& recs) {
        const int32_t seq = seqs_[pid];
        seqs_[pid] += static_cast<int32_t>(recs.size());
        std::vector<VerdictRig::Char> chars;
        for (const Char09& c : recs) chars.push_back(charOf_(c));
        stored_ += recs.size();
        return {r_.appendCharacters(chars, kAcksViewAll, pid, seq), pid, seq, recs};
    }
    void resend_(Sent s) {
        std::vector<VerdictRig::Char> chars;
        for (const Char09& c : s.recs) chars.push_back(charOf_(c));
        sent_.push_back({r_.appendCharacters(chars, kAcksViewAll, s.pid, s.seq), s.pid, s.seq, s.recs});
        dups_.emplace_back(s.corr, sent_.back().corr);
    }

    // The batches of the op: a key-0 create may not share a batch with an explicit key, so a delete and a reuse are two.
    std::vector<std::vector<Char09>> opBatches_() const {
        std::vector<std::vector<Char09>> out;
        if (c_.timing == Timing::BackToBack) {
            for (const Char09& x : sc_.op) out.push_back({x});
            return out;
        }
        if (c_.op == Op::CDeleteThenReuse) out = {{sc_.op[0]}, {sc_.op[1]}};
        else out = {sc_.op};
        if (c_.timing == Timing::SelfCollidingBatch)
            for (auto& b : out) { const std::vector<Char09> again = b; b.insert(b.end(), again.begin(), again.end()); }
        return out;
    }
    void sendOp_(int64_t pid = kOpPid) {
        for (const auto& b : opBatches_()) {
            opSent_.push_back(sent_.size());
            sent_.push_back(send_(pid, b));
        }
    }

    void writeSetup_() {
        for (std::size_t at = 0; at < sc_.setup.size(); at += 16) {
            const std::vector<Char09> chunk(sc_.setup.begin() + static_cast<std::ptrdiff_t>(at), sc_.setup.begin() + static_cast<std::ptrdiff_t>(std::min(at + 16, sc_.setup.size())));
            // an explicit insert: the counter moves above the ids, and a plain upsert is refused on an autoincrement source
            std::vector<Char09> inserts = chunk;
            for (Char09& c : inserts) c.intent = 1;
            const Sent s = send_(kSetupPid, inserts);
            const auto a = r_.ackFetching(s.corr, ackPasses_());
            ASSERT_TRUE(a);
            ASSERT_EQ(a->code, 0);
            ASSERT_TRUE(a->verdicts.empty());
        }
        ASSERT_TRUE(r_.driveUntilJudged());
    }

    void createViews_() {
        if (c_.timing == Timing::CreateOnNonEmpty) r_.holdJudge(true);   // the op is sent while the members build
        r_.createConstrainedView(CV::CharacterName);
        r_.createConstrainedView(CV::AccountStats, 12);
        fingerprint12_ = r_.fingerprintOf(CV::AccountStats, false);
        if (c_.timing == Timing::CreateOnNonEmpty) { r_.passes(30); return; }
        ASSERT_TRUE(waitMembers_(3));
        settleViews_();
    }

    void settleViews_() {
        r_.followerFetch();
        ASSERT_TRUE(r_.driveUntilJudgeReaches());
        for (const auto& [v, l] : r_.constrained) r_.driveUntilFolded(l.topicId, r_.dataEnd());
    }

    // The key index and both views are the logged set's members, and the leadership scan is done.
    bool waitMembers_(uint32_t count) {
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            const PartitionWriteState* w = r_.writeState();
            if (w != nullptr && w->constraintSets.latest().count == count && r_.worker->rcStatusForTest(VerdictRig::kSrc, 0) == Worker::kRcReady) return true;
            if (i % 8 == 0) r_.followerFetch();
            r_.pass();
        }
        ADD_FAILURE() << "the logged set never reached " << count << " members";
        return false;
    }

    void waitCommitted_() {
        r_.passes(10);
        for (int i = 0; i < VerdictRig::kMaxPass && r_.replica->highWatermark() < r_.leo(); ++i) {
            if (i % 8 == 0) r_.followerFetch();
            r_.pass();
        }
        EXPECT_EQ(r_.replica->highWatermark(), r_.leo());
    }

    // A background producer writes creates for W, then the change happens right before the op's last batch.
    void backgroundWrites_() {
        for (uint64_t i = 0; i < 3; ++i) sent_.push_back(send_(kBgPid, {Char09{0, kUserW09, "bg" + std::to_string(i), 0, 1}}));
        r_.passes(3);
    }

    // The op is sent, the leader loses it (`lose`), the node is leader again and the op is sent once more from a fresh sequence.
    template <class Lose, class Back>
    void lostAndResend_(Lose lose, Back back) {
        lose();
        for (const Sent& s : sent_) ASSERT_TRUE(r_.ack(s.corr));
        back();
        stored_ -= opRecords_();
        sent_.clear();
        opSent_.clear();
        seqs_.erase(kOpPid);
        ASSERT_TRUE(waitMembers_(3));
        r_.passes(10);
        sendOp_();
    }
    std::size_t opRecords_() const { std::size_t n = 0; for (const Sent& s : sent_) n += s.recs.size(); return n; }

    void execute_() {
        switch (c_.timing) {
        case Timing::Racing:
            sendOp_(kOpPid);
            sendOp_(kOtherPid);
            return;
        case Timing::RetryBeforeJudge: case Timing::RetryAfterJudge: case Timing::RetryAfterWindow: {
            const Char09& w = sc_.setup[std::find_if(sc_.setup.begin(), sc_.setup.end(), [](const Char09& x) { return x.id == kIdBase09 + 101; }) - sc_.setup.begin()];
            ASSERT_TRUE(executeRetry(r_, c_.timing != Timing::RetryBeforeJudge, c_.timing == Timing::RetryAfterWindow, [&] { sendOp_(); },
                                     [&](uint64_t i) { sent_.push_back(send_(kOpPid, {Char09{w.id, w.userId, w.name, static_cast<int64_t>(1000 + i), 2}})); },   // no state change: it only moves the window
                                     [&] { resend_(sent_[opSent_.back()]); }));
            return;
        }
        case Timing::CreateOnNonEmpty:
            sendOp_();
            r_.passes(10);
            r_.holdJudge(false);
            return;
        case Timing::ReplaceDuringWrites:
            backgroundWrites_();
            r_.replaceConstrainedView(CV::AccountStats, 13);
            fingerprint13_ = r_.fingerprintOf(CV::AccountStats, false);
            r_.passes(5);
            sendOp_();
            return;
        case Timing::DropDuringWrites:
            r_.dropConstrainedView(CV::CharacterName);
            r_.passes(5);
            sendOp_();
            return;
        case Timing::FailoverAppendHw:
            sendOp_();
            r_.passes(5);
            lostAndResend_([&] { r_.demote(); }, [&] { r_.failover(); });
            return;
        case Timing::TruncateTail:
            sendOp_();
            r_.passes(5);
            lostAndResend_([&] { r_.truncateTo(r_.replica->highWatermark()); r_.demotePromote(); }, [] {});
            return;
        case Timing::FailoverHwFold:
            sendOp_();
            ASSERT_TRUE(r_.driveUntilJudged());
            for (const Sent& s : sent_) ASSERT_TRUE(r_.ackFetching(s.corr, ackPasses_()));   // answered before the node is lost
            r_.followerFetch();
            r_.failover();
            return;
        case Timing::FailoverFoldMarker:
            sendOp_();
            r_.passes(5);
            r_.followerFetch();
            for (int i = 0; i < VerdictRig::kMaxPass && r_.leo() <= r_.replica->highWatermark(); ++i) r_.pass();   // the marker sits above the HW
            ASSERT_GT(r_.leo(), r_.replica->highWatermark());
            r_.failover();
            return;
        case Timing::RestartDuringRebuild:
            sendOp_();
            ASSERT_TRUE(r_.driveUntilJudged());
            for (const Sent& s : sent_) ASSERT_TRUE(r_.ackFetching(s.corr, ackPasses_()));
            r_.restart();
            r_.passes(2);
            r_.restart();
            return;
        default:   // Single, BackToBack, SelfCollidingBatch
            sendOp_();
            return;
        }
    }

    CheckedWritesModel modelOf_(const VerdictRig::RigLog& log) const {
        CheckedWritesModel model;
        model.addKeyIndexRule();
        model.addLowerNameUniqueRule(VerdictRig::objectIdOf(CV::CharacterName));
        model.addCountCheckRule(VerdictRig::objectIdOf(CV::AccountStats), 12);
        model.declareCountCap(VerdictRig::objectIdOf(CV::AccountStats), fingerprint12_, 12);
        if (c_.timing == Timing::ReplaceDuringWrites) model.declareCountCap(VerdictRig::objectIdOf(CV::AccountStats), fingerprint13_, 13);
        model.reset(log);
        return model;
    }

    void collectAcks_() {
        for (const Sent& s : sent_) {
            const auto a = r_.ackFetching(s.corr, ackPasses_());
            ASSERT_TRUE(a) << "corr " << s.corr;
            acks_[s.corr] = *a;
        }
    }

    // (k) every rejected completion carries the model's constraint id
    void checkAcks_(const Expected& ex, const CheckedWritesModel& model, const VerdictRig::RigLog& log) {
        for (const Sent& s : sent_) {
            const VerdictRig::Ack& a = acks_.at(s.corr);
            if (a.code != 0) {
                EXPECT_EQ(ex.check, Check::RetryOutsideWindow);
                continue;
            }
            checkModelTrailer(model, a, s.recs.size(), s.corr);
            EXPECT_EQ(a.assignedBase != 0, s.recs.front().id == 0) << "only a key-0 batch answers its ids";
        }
        for (const auto& [orig, dup] : dups_) checkDupAck(r_, acks_.at(orig), acks_.at(dup), ex.check == Check::RetryOutsideWindow);
        if (c_.timing == Timing::RetryBeforeJudge || c_.timing == Timing::RetryAfterJudge) EXPECT_EQ(log.data.size(), stored_) << "a retry is stored once";
        if (ex.check != Check::ConstraintTable) return;

        const std::vector<V09> want = single09(c_.op, c_.entity);
        std::size_t at = 0;
        for (const std::size_t k : opSent_) {
            AckTrailer t;
            for (std::size_t j = 0; j < sent_[k].recs.size(); ++j, ++at)
                if (want[at] != V09::Ok) t.emplace_back(static_cast<uint16_t>(j), 55, want[at] == V09::N ? kNameCid09 : kStatsCid09);
            EXPECT_EQ(acks_.at(sent_[k].corr).verdicts, t) << "the Single table";
        }
        EXPECT_EQ(at, want.size());
        const auto owners = model.uniqueOwners();
        const bool opLeaves = c_.op == Op::CRename || c_.op == Op::CRemove || c_.op == Op::CRemoveExisting || c_.op == Op::CDeleteThenReuse;
        if (c_.entity == Entity::DupOwner && opLeaves) {
            ASSERT_TRUE(owners.count("dup") != 0);
            EXPECT_EQ(owners.at("dup"), kIdBase09 + 9) << "the smallest extra is promoted";
        }
        if (c_.entity == Entity::DupExtra && opLeaves) EXPECT_EQ(model.uniqueExtras(), 0u) << "the extra left the name";
    }

    // (c) a raw subscription from 0 receives exactly the accepted records, with their intents
    void checkDelivered_(const CheckedWritesModel& model) {
        for (int i = 0; i < VerdictRig::kMaxPass && r_.worker->rcStatusForTest(VerdictRig::kSrc, 0) != Worker::kRcReady; ++i) r_.pass();
        ASSERT_EQ(r_.worker->rcStatusForTest(VerdictRig::kSrc, 0), Worker::kRcReady);
        const auto& box = r_.collect(r_.subscribe(SubKind::Raw, 0));
        std::vector<std::tuple<int64_t, uint64_t, uint8_t>> got, want;
        for (const auto& d : box.records) got.emplace_back(d.offset, d.id, d.intent);
        for (const auto& a : model.acceptedRecords()) want.emplace_back(a.offset, a.id, a.intent);
        EXPECT_EQ(got, want) << "(c) the delivered records are the accepted ones, with their intents";
    }

    [[nodiscard]] int64_t column_(CV v, const uint8_t* row, const char* col) const {
        const auto& comp = r_.constrained.at(v).comp;
        for (std::size_t i = 0; i < comp.outputDescriptor.fields.size(); ++i)
            if (comp.outputDescriptor.fields[i].name == col) {
                const auto f = rune::view::readSlot(row, comp.rowLayout, static_cast<uint16_t>(i));
                return f.type == rune::codec::PrimType::Int64 ? f.i : static_cast<int64_t>(f.u);
            }
        throw std::logic_error(std::string("no column ") + col);
    }

    // (h) character_name holds the model's owners and its gauge the extras; (i) account_stats holds the live counts
    void checkViews_(const CheckedWritesModel& model, const VerdictRig::RigLog& log) {
        std::set<std::string> names;
        std::set<uint64_t> users;
        for (const auto& d : log.data) {
            if (d.del) continue;
            names.insert(modelLower(d.name));
            users.insert(static_cast<uint64_t>(d.v));
        }
        if (r_.constrained.count(CV::CharacterName) != 0) {
            const ViewInstance* inst = r_.constrainedInstance(CV::CharacterName);
            ASSERT_NE(inst, nullptr);
            const auto owners = model.uniqueOwners();
            for (const std::string& name : names) {
                std::vector<uint8_t> key(24, 0);
                std::copy(name.begin(), name.end(), key.begin());
                const uint8_t* row = inst->table.get(rune::view::ViewKey::fromBytes(key));
                const auto owner = owners.find(name);
                if (owner == owners.end()) { EXPECT_EQ(row, nullptr) << "(h) '" << name << "' has no live claimant"; continue; }
                ASSERT_NE(row, nullptr) << "(h) '" << name << "'";
                EXPECT_EQ(column_(CV::CharacterName, row, "characterId"), static_cast<int64_t>(owner->second)) << "(h) the owner of '" << name << "'";
            }
            EXPECT_EQ(r_.uniqueConflicts(CV::CharacterName), model.uniqueExtras()) << "(j) the gauge is the extras";
        }
        const ViewInstance* stats = r_.constrainedInstance(CV::AccountStats);
        ASSERT_NE(stats, nullptr);
        const auto counts = model.liveCountByUser();
        for (const uint64_t user : users) {
            const uint8_t* row = stats->table.get(user);
            const auto want = counts.find(user);
            if (want == counts.end()) { EXPECT_EQ(row, nullptr) << "(i) user " << user; continue; }
            ASSERT_NE(row, nullptr) << "(i) user " << user;
            EXPECT_EQ(column_(CV::AccountStats, row, "characters"), want->second) << "(i) user " << user;
        }
    }

    // (f)(g) the side window never missed, no assigned id met a stored key, the rejected index agrees with the log
    void checkStats_() {
        EXPECT_EQ(r_.autoIncStats().windowMisses, 0u);
        EXPECT_EQ(r_.autoIncStats().assignedKeyExists, 0u);
        EXPECT_EQ(r_.writeState()->rejected.mismatches(), 0u);
    }

    Cell c_;
    Scenario09 sc_;
    VerdictRig r_;
    uint32_t members_ = 3;
    int64_t target_ = 0;
    uint64_t fingerprint12_ = 0, fingerprint13_ = 0;
    std::vector<Sent> sent_;
    std::vector<std::size_t> opSent_;   ///< indexes into sent_ of the op's own batches
    std::vector<std::pair<int64_t, int64_t>> dups_;
    std::map<int64_t, int32_t> seqs_;
    std::map<int64_t, VerdictRig::Ack> acks_;
    std::size_t stored_ = 0;
};

}  // namespace

class CheckedWritesMatrix : public ::testing::TestWithParam<Cell> {};

TEST_P(CheckedWritesMatrix, Holds) {
    const Cell c = GetParam();
    const Expected ex = expect(c);
    if (!ex.applicable) GTEST_SKIP() << ex.naReason;
    if (isP09(c)) ConstraintCellRun(c).run();
    else if (isP06(c)) AutoIncCellRun(c).run();
    else if (isP05(c)) KeyIndexCellRun(c).run();
    else CellRun(c).run();
}

INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesMatrix, ::testing::ValuesIn(cellsFor(Part::P04)),
    [](const ::testing::TestParamInfo<Cell>& i) {
        return std::string(nameOf(i.param.op)) + "_" + nameOf(i.param.entity) + "_" + nameOf(i.param.timing);
    });

INSTANTIATE_TEST_SUITE_P(Cell05, CheckedWritesMatrix, ::testing::ValuesIn(cellsFor(Part::P05)),
    [](const ::testing::TestParamInfo<Cell>& i) {
        return std::string(nameOf(i.param.op)) + "_" + nameOf(i.param.entity) + "_" + nameOf(i.param.timing);
    });

INSTANTIATE_TEST_SUITE_P(Cell06, CheckedWritesMatrix, ::testing::ValuesIn(cellsFor(Part::P06)),
    [](const ::testing::TestParamInfo<Cell>& i) {
        return std::string(nameOf(i.param.op)) + "_" + nameOf(i.param.entity) + "_" + nameOf(i.param.timing);
    });

INSTANTIATE_TEST_SUITE_P(Cell09, CheckedWritesMatrix, ::testing::ValuesIn(cellsFor(Part::P09)),
    [](const ::testing::TestParamInfo<Cell>& i) {
        return std::string(nameOf(i.param.op)) + "_" + nameOf(i.param.entity) + "_" + nameOf(i.param.timing);
    });

// ---- Part 07: keyed views.  Every cell drives the real worker over a source of the shape's schema, folds a keyed view of it and compares the
// view table, the owner map, point reads and prefix scans with CheckedWritesModel::refoldKeyedView (a group-by view: refoldKeyedAggregate). ----
namespace {

namespace k07 {

using rune::codec::Descriptor;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::view::MergeRule;
using Cols = std::map<uint16_t, std::string>;

constexpr int32_t kViewA = 0x40000010, kViewB = 0x40000011;
constexpr int64_t kDupPid = 600;
constexpr uint8_t kViewAllAcks = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;

struct Fields { uint64_t id; uint64_t lead; std::string text; uint64_t val; };
struct Rec07 { std::map<std::string, uint64_t> nums; std::map<std::string, std::string> strs; };

/// The value block of `r` under `d` and its wire key (the bytes of the first field).
RawRec encode(const Descriptor& d, const Rec07& r) {
    std::vector<uint8_t> v(4 + d.blockLength, 0);
    v[0] = static_cast<uint8_t>(d.blockLength); v[1] = static_cast<uint8_t>(d.blockLength >> 8);
    v[2] = static_cast<uint8_t>(d.version); v[3] = static_cast<uint8_t>(d.version >> 8);
    for (const auto& f : d.fields) {
        if (f.type == PrimType::FixedStr) {
            if (const auto s = r.strs.find(f.name); s != r.strs.end()) std::copy(s->second.begin(), s->second.end(), v.begin() + 4 + f.offset);
        } else if (const auto n = r.nums.find(f.name); n != r.nums.end()) {
            for (uint32_t i = 0; i < rune::codec::fieldWidth(f); ++i) v[4 + f.offset + i] = static_cast<uint8_t>(n->second >> (8 * i));
        }
    }
    const auto& k = d.fields.front();
    return {std::string(v.begin() + 4 + k.offset, v.begin() + 4 + k.offset + rune::codec::fieldWidth(k)), std::move(v)};
}
RawRec tombstone(const RawRec& of) { return {of.key, {}}; }

struct Scenario {
    Descriptor src;
    KeyShape key;
    rune::view::ViewPlan plan;
    std::vector<RawRec> base, op;
};

std::string repeated(const char* s, int n) { std::string out; for (int i = 0; i < n; ++i) out += s; return out; }

// ---- the projection shapes ----
Descriptor characterSource() {
    return DescriptorBuilder("character", 1).add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).addFixedStr("name", 16)
        .addFixedStr("tag", 80).add("provider", PrimType::UInt16).addFixedStr("subject", 64).add("val", PrimType::UInt64).build();
}

Rec07 recOf(Entity e, const Fields& f) {
    Rec07 r;
    r.nums["val"] = f.val;
    switch (e) {
    case Entity::KeyU16: case Entity::KeyU32: case Entity::KeyU64: r.nums["k"] = f.id; break;
    case Entity::KeyI64: r.nums["k"] = static_cast<uint64_t>(-static_cast<int64_t>(f.id)); break;
    case Entity::KeyStr16: r.strs["k"] = "key-" + std::to_string(f.id); break;
    case Entity::KeyComposite: r.nums["characterId"] = f.id; r.nums["userId"] = f.lead; break;
    case Entity::KeyCompositeWide: r.nums["userId"] = f.id; r.nums["provider"] = f.lead; r.strs["subject"] = f.text; break;
    case Entity::KeyLower: r.nums["characterId"] = f.id; r.strs["name"] = f.text; break;
    default: r.nums["characterId"] = f.id; r.strs["tag"] = f.text; break;   // KeyMaxWidth
    }
    return r;
}

/// The text that fills the key's string column to its width after lower(): a grower where the key lowers.
std::string growerOf(Entity e) {
    return e == Entity::KeyMaxWidth ? repeated("\xC8\xBA", 40) : e == Entity::KeyLower ? repeated("\xC8\xBA", 8) : e == Entity::KeyCompositeWide ? std::string(64, 'x') : "g";
}
std::string renamedOf(Entity e) { return e == Entity::KeyLower ? "Qux" : e == Entity::KeyCompositeWide ? "renamed" : growerOf(e); }

Scenario scenarioOf(const Cell& c) {
    const Entity e = c.entity;
    Scenario s;
    s.plan.viewName = "keyed_07";
    s.plan.persistence = 1;
    const auto output = [&](std::initializer_list<const char*> names, const Descriptor& d) {
        for (const char* n : names) s.plan.output.push_back({n, d.find(n)->type, 0, n, MergeRule::Replace});
    };
    if (e == Entity::KeyU16 || e == Entity::KeyU32 || e == Entity::KeyU64 || e == Entity::KeyI64) {
        const PrimType t = e == Entity::KeyU16 ? PrimType::UInt16 : e == Entity::KeyU32 ? PrimType::UInt32 : e == Entity::KeyU64 ? PrimType::UInt64 : PrimType::Int64;
        s.src = DescriptorBuilder("narrow", 1).add("k", t, 1, true).add("grp", PrimType::UInt64).add("val", PrimType::UInt64).build();
        s.key = KeyShape{t};
        s.plan.key = "k";
        s.plan.sources = {{VerdictRig::kSrc, "k"}};
        output({"k", "val"}, s.src);
    } else if (e == Entity::KeyStr16) {
        s.src = DescriptorBuilder("coded", 1).addFixedStr("k", 16, 1, true).add("grp", PrimType::UInt64).add("val", PrimType::UInt64).build();
        s.key = KeyShape{PrimType::FixedStr, 16};
        s.plan.sources = {{VerdictRig::kSrc, "k"}};
        s.plan.keyComponents = {{"k", 0, "k"}};
        output({"val"}, s.src);
    } else if (e == Entity::KeyCompositeWide) {
        s.src = DescriptorBuilder("account", 1).add("userId", PrimType::UInt64, 1, true).add("provider", PrimType::UInt16).addFixedStr("subject", 64)
                    .add("val", PrimType::UInt64).build();
        s.key = KeyShape{PrimType::UInt64};
        s.plan.sources = {{VerdictRig::kSrc, "userId"}};
        s.plan.keyComponents = {{"provider", 0, "provider"}, {"subject", 0, "subject"}, {"userId", 0, "userId"}};
        output({"val"}, s.src);
    } else {
        s.src = characterSource();
        s.key = KeyShape{PrimType::UInt64};
        s.plan.sources = {{VerdictRig::kSrc, "characterId"}};
        switch (e) {
        case Entity::KeyComposite: s.plan.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}; break;
        case Entity::KeyLower: s.plan.keyComponents = {{"name", rune::codec::kKeyFunctionLower, "nameKey"}, {"characterId", 0, "characterId"}}; break;
        default: s.plan.keyComponents = {{"tag", rune::codec::kKeyFunctionLower, "tagKey"}, {"characterId", 0, "characterId"}}; break;
        }
        if (e == Entity::KeyLower || e == Entity::KeyMaxWidth) s.plan.unicodeVersion = rune::codec::kUnicodeVersion;
        output({"val"}, s.src);
    }
    const auto rec = [&](Fields f) { return encode(s.src, recOf(e, f)); };
    s.base = {rec({1, 10, "Foo", 100}), rec({2, 10, "BAR", 200}), rec({3, 20, growerOf(e), 300})};
    switch (c.op) {
    case Op::ViewUpsertNew: case Op::ViewDuplicateRetry: s.op = {rec({4, 20, "Qux", 400})}; break;
    case Op::ViewUpdateNonKey: s.op = {rec({2, 10, "BAR", 1200})}; break;
    case Op::ViewRename: s.op = {rec({2, 10, renamedOf(e), 200})}; break;
    case Op::ViewMoveGroup: s.op = {rec({2, 30, "BAR", 200})}; break;
    case Op::ViewDelete: s.op = {tombstone(s.base[1])}; break;
    case Op::ViewDeleteThenReuse: s.op = {tombstone(s.base[1]), rec({2, 10, "BAR", 777})}; break;
    default: break;   // reads
    }
    return s;
}

// ---- the group-by shapes: device_history over the device source, one aggregate column ----
struct Ev { uint64_t id, user, dev; std::string fp; uint64_t platform; int64_t lastSeen; uint64_t bytes; };

Descriptor deviceSource() {
    return DescriptorBuilder("device", 1).add("eventId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).add("deviceId", PrimType::UInt64)
        .addFixedStr("deviceFp", 16).add("platform", PrimType::Enum).add("lastSeen", PrimType::Int64).add("bytes", PrimType::UInt64).build();
}

Scenario aggScenarioOf(const Cell& c, AggRule07 rule) {
    Scenario s;
    s.src = deviceSource();
    s.key = KeyShape{PrimType::UInt64};
    s.plan.viewName = "device_history_07";
    s.plan.persistence = 1;
    s.plan.sources = {{VerdictRig::kSrc, "eventId"}};
    s.plan.keyComponents = s.plan.groupBy = c.entity == Entity::KeyComposite
        ? std::vector<rune::view::KeyComponent>{{"userId", 0, "userId"}, {"deviceId", 0, "deviceId"}}
        : std::vector<rune::view::KeyComponent>{{"deviceFp", rune::codec::kKeyFunctionLower, "fpKey"}, {"userId", 0, "userId"}};
    if (c.entity == Entity::KeyLower) s.plan.unicodeVersion = rune::codec::kUnicodeVersion;
    switch (rule) {
    case AggRule07::CountAll: s.plan.output = {{"uses", PrimType::Int64, 0, "", MergeRule::CountAll}}; break;
    case AggRule07::SumBytes: s.plan.output = {{"total", PrimType::UInt64, 0, "bytes", MergeRule::Sum}}; break;
    case AggRule07::MinLastSeen: s.plan.output = {{"firstSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Min}}; break;
    case AggRule07::MaxLastSeen: s.plan.output = {{"lastSeen", PrimType::Int64, 0, "lastSeen", MergeRule::Max}}; break;
    case AggRule07::LastPlatform: s.plan.output = {{"platform", PrimType::Enum, 0, "platform", MergeRule::Last}}; break;
    }
    const auto rec = [&](const Ev& e) {
        Rec07 r;
        r.nums = {{"eventId", e.id}, {"userId", e.user}, {"deviceId", e.dev}, {"platform", e.platform}, {"lastSeen", static_cast<uint64_t>(e.lastSeen)}, {"bytes", e.bytes}};
        r.strs["deviceFp"] = e.fp;
        return encode(s.src, r);
    };
    s.base = {rec({1, 10, 1, "FpA", 1, 50, 5}), rec({2, 10, 1, "fpa", 2, 30, 7}), rec({3, 10, 2, "FpB", 3, 40, 9}), rec({4, 20, 1, "FpA", 1, 10, 11}), rec({5, 20, 1, "FPA", 2, 70, 13})};
    switch (c.op) {
    case Op::ViewUpsertNew: s.op = {rec({6, 10, 1, "FpA", 9, 60, 100})}; break;
    case Op::ViewUpdateNonKey: s.op = {rec({2, 10, 1, "fpa", 5, 99, 70})}; break;
    case Op::ViewMoveGroup: s.op = {rec({2, 20, 1, "FpA", 2, 30, 7})}; break;
    case Op::ViewDelete: s.op = {tombstone(s.base[2]), tombstone(s.base[3])}; break;
    default: break;   // ViewPrefixScan
    }
    return s;
}

std::string sigOf(const Cols& c) {
    std::string out;
    for (const auto& [slot, v] : c) out += std::to_string(slot) + "=" + v + ";";
    return out;
}

class CellRun {
public:
    CellRun(const Cell& c, Scenario s, bool aggregate) : c_(c), s_(std::move(s)), aggregate_(aggregate), r_(/*follower=*/false) {}

    void run() {
        r_.useSource(s_.src, s_.key);
        r_.passes(50);
        const bool lateCreate = c_.timing == Timing::CreateOnNonEmpty;
        if (!lateCreate) install_(kViewA);
        r_.passes(10);

        send_(s_.base, c_.timing == Timing::BackToBack);
        if (c_.timing == Timing::ReplaceDuringWrites) {
            r_.dropView(view_);
            install_(kViewB);
        } else if (c_.timing == Timing::DropDuringWrites) {
            r_.driveUntilFolded(view_, r_.leo());
            r_.dropView(view_);
            EXPECT_NE(r_.getViewRowK(view_, std::vector<uint8_t>(comp_.plan.keyWidth, 0)).code, rune::protocol::client::ViewRowResponse::kOk) << "a dropped view answers nothing";
        }
        if (c_.op == Op::ViewDuplicateRetry) {
            retry_();
            if (::testing::Test::HasFatalFailure()) return;
        } else if (!s_.op.empty()) {
            std::vector<RawRec> op = s_.op;
            if (c_.timing == Timing::SelfCollidingBatch) op.insert(op.end(), s_.op.begin(), s_.op.end());
            send_(op, c_.timing == Timing::BackToBack);
        }
        if (lateCreate) install_(kViewA);
        if (c_.timing == Timing::DropDuringWrites) install_(kViewB);

        r_.driveUntilFolded(view_, r_.leo());
        if (c_.timing == Timing::FailoverHwFold && c_.op != Op::ViewDuplicateRetry) {
            r_.failover();
            r_.driveUntilFolded(view_, r_.leo());
        } else if (c_.timing == Timing::RestartDuringRebuild) {
            r_.restart();
            r_.passes(2);
            r_.restart();
            r_.driveUntilFolded(view_, r_.leo());
        }
        verify_();
        if (c_.timing == Timing::BackToBack && isRead07(c_.op)) verify_();
    }

    std::size_t skippedNoKey = 0;   ///< the records of the log the model found without a key column (P7-16)

private:
    void install_(int32_t topic) {
        comp_ = compile_(topic);
        view_ = topic;
        r_.installKeyedView(comp_);
    }
    rune::view::CompileResult compile_(int32_t topic) const {
        const rune::codec::Descriptor* srcs[] = {&s_.src};
        auto c = rune::view::compileView(s_.plan, srcs, topic);
        if (!c) throw std::runtime_error(c.error().message);
        return std::move(*c);
    }

    void send_(const std::vector<RawRec>& recs, bool oneEach) {
        if (oneEach) {
            for (const RawRec& x : recs) ASSERT_EQ(r_.produceRaw({x}), 0);
        } else if (!recs.empty()) {
            ASSERT_EQ(r_.produceRaw(recs), 0);
        }
    }

    // The same producer batch twice: before the fold, after it, or after a failover.  The log holds it once.
    void retry_() {
        r_.enableAcks();
        const auto bytes = VerdictRig::rawBatch(s_.op, kDupPid, 0);
        const int64_t first = r_.produceBytes(bytes, kViewAllAcks, kDupPid, 0);
        const auto a1 = r_.ackFetching(first);
        ASSERT_TRUE(a1 && a1->code == 0);
        if (c_.timing == Timing::RetryBeforeJudge) {
            const auto a2 = r_.ackFetching(r_.produceBytes(bytes, kViewAllAcks, kDupPid, 0));
            ASSERT_TRUE(a2 && a2->code == 0);
            EXPECT_EQ(a2->baseOffset, a1->baseOffset);
            return;
        }
        r_.driveUntilFolded(view_, r_.leo());
        if (c_.timing == Timing::FailoverHwFold) {
            r_.failover();
            r_.passes(100);
        }
        std::optional<VerdictRig::Ack> a2;
        for (int attempt = 0; attempt < 20 && !(a2 && a2->code == 0); ++attempt) a2 = r_.ackFetching(r_.produceBytes(bytes, kViewAllAcks, kDupPid, 0));
        ASSERT_TRUE(a2 && a2->code == 0);
        EXPECT_EQ(a2->baseOffset, a1->baseOffset);
    }

    Cols colsOf_(const uint8_t* row, const Cols& shape) const {
        Cols out;
        for (const auto& kv : shape) out[kv.first] = modelCanon(rune::view::readSlot(row, comp_.rowLayout, kv.first));
        return out;
    }

    void verify_() {
        const auto log = r_.readRaw();
        const auto& plan = comp_.plan;
        std::map<std::vector<uint8_t>, Cols> want;
        KeyedModel keyed;
        if (aggregate_) {
            want = refoldKeyedAggregate(plan, s_.src, log);
        } else {
            keyed = refoldKeyedView(plan, s_.src, log);
            want = keyed.rows;
            skippedNoKey = keyed.skippedNoKey;
        }
        if (c_.op == Op::ViewDuplicateRetry) EXPECT_EQ(log.size(), s_.base.size() + s_.op.size()) << "a retried batch is stored once";
        const rune::node::ViewInstance* inst = r_.vs->instanceForTest(view_, 0);
        ASSERT_NE(inst, nullptr);
        EXPECT_EQ(inst->table.size(), want.size());   // (a): the same number of rows, and every model row is read back below
        if (!aggregate_) {
            EXPECT_EQ(inst->owners.has_value(), rune::view::ownerMappedOf(plan));   // (b)
            if (inst->owners) {
                EXPECT_EQ(inst->owners->size(), keyed.owners.size());
                for (const auto& [wire, viewKey] : keyed.owners) {
                    const uint8_t* o = inst->owners->get(rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()}));
                    ASSERT_NE(o, nullptr);
                    EXPECT_EQ(std::vector<uint8_t>(o, o + viewKey.size()), viewKey);
                }
            }
        }
        const uint16_t prefix = rune::view::prefixWidthOf(plan);
        std::map<std::vector<uint8_t>, std::vector<std::string>> byPrefix;
        for (const auto& [key, cols] : want) {   // (c): a point read of every model key
            const auto got = r_.getViewRowK(view_, key);
            ASSERT_EQ(got.code, rune::protocol::client::ViewRowResponse::kOk);
            ASSERT_EQ(got.row.size(), comp_.rowLayout.rowLen);
            EXPECT_EQ(colsOf_(got.row.data(), cols), cols);
            if (prefix != 0) byPrefix[std::vector<uint8_t>(key.begin(), key.begin() + prefix)].push_back(sigOf(cols));
            if (rune::view::keyModeOf(plan) == rune::view::ViewKeyMode::Keyed) {
                const uint8_t* row = inst->table.get(rune::view::ViewKey::fromBytes(key));
                ASSERT_NE(row, nullptr);
                const auto kr = inst->table.keyOfRow(row);
                EXPECT_EQ(std::vector<uint8_t>(kr.begin(), kr.end()), key);
            }
        }
        for (auto& [p, sigs] : byPrefix) {   // (c): a full scan of every model prefix, limit 2, as a set
            std::vector<std::string> got;
            for (const auto& row : r_.scanAll(view_, p, 2)) got.push_back(sigOf(colsOf_(row.data(), want.begin()->second)));
            std::sort(got.begin(), got.end());
            std::sort(sigs.begin(), sigs.end());
            EXPECT_EQ(got, sigs);
        }
        if (prefix != 0) EXPECT_TRUE(r_.scanAll(view_, std::vector<uint8_t>(prefix, 0xFF), 2).empty());
        if (c_.op == Op::ViewPointRead) {
            EXPECT_EQ(r_.getViewRowK(view_, std::vector<uint8_t>(plan.keyWidth, 0xFF)).code, rune::protocol::client::ViewRowResponse::kAbsentKey);
            EXPECT_EQ(r_.getViewRowK(view_, std::vector<uint8_t>(plan.keyWidth + 1u, 0x01)).code, rune::protocol::client::ViewRowResponse::kInvalidRequest);
        }
    }

    Cell c_;
    Scenario s_;
    bool aggregate_;
    VerdictRig r_;
    rune::view::CompileResult comp_;
    int32_t view_ = kViewA;
};

}  // namespace k07

}  // namespace

class CheckedWritesMatrix07 : public ::testing::TestWithParam<Cell> {};
class CheckedWritesMatrix07Agg : public ::testing::TestWithParam<AggCell07> {};

TEST_P(CheckedWritesMatrix07, KeyedViewEqualsTheModel) {
    const Cell c = GetParam();
    k07::CellRun(c, k07::scenarioOf(c), /*aggregate=*/false).run();
}

TEST_P(CheckedWritesMatrix07Agg, GroupByViewEqualsTheModel) {
    const AggCell07 c = GetParam();
    k07::CellRun(c.cell, k07::aggScenarioOf(c.cell, c.rule), /*aggregate=*/true).run();
}

namespace {
std::vector<Cell> applicableCells07() {
    std::vector<Cell> out;
    for (const Cell& c : cellsFor(Part::P07)) if (expect(c).applicable) out.push_back(c);
    return out;
}
const char* nameOf(AggRule07 r) {
    switch (r) {
    case AggRule07::CountAll: return "CountAll"; case AggRule07::SumBytes: return "SumBytes"; case AggRule07::MinLastSeen: return "MinLastSeen";
    case AggRule07::MaxLastSeen: return "MaxLastSeen"; case AggRule07::LastPlatform: return "LastPlatform";
    }
    return "?";
}
}  // namespace

INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesMatrix07, ::testing::ValuesIn(applicableCells07()),
    [](const ::testing::TestParamInfo<Cell>& i) {
        return std::string(nameOf(i.param.op)) + "_" + nameOf(i.param.entity) + "_" + nameOf(i.param.timing);
    });

INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesMatrix07Agg, ::testing::ValuesIn(aggCellsFor07()),
    [](const ::testing::TestParamInfo<AggCell07>& i) {
        return std::string(nameOf(i.param.rule)) + "_" + nameOf(i.param.cell.op) + "_" + nameOf(i.param.cell.entity) + "_" + nameOf(i.param.cell.timing);
    });


// A record whose writer version has no key column erases its source key's row and owner (P7-16): the model and the worker agree on it.
TEST(CheckedWritesMatrix07Model, AbsentKeyRecordErasesTheRowAndTheOwner) {
    using namespace k07;
    const Descriptor evolving = DescriptorBuilder("character", 2).add("characterId", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64, 2)
                                    .add("val", PrimType::UInt64).build();
    const Descriptor v1 = rune::codec::materializeAsOf(evolving, 1);
    for (const Timing t : {Timing::Single, Timing::BackToBack}) {
        Scenario s;
        s.src = evolving;
        s.key = KeyShape{PrimType::UInt64};
        s.plan.viewName = "absent_key";
        s.plan.persistence = 1;
        s.plan.sources = {{VerdictRig::kSrc, "characterId"}};
        s.plan.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}};
        s.plan.output = {{"val", PrimType::UInt64, 0, "val", MergeRule::Replace}};
        const auto rec = [](const Descriptor& d, uint64_t id, uint64_t user, uint64_t val) {
            Rec07 r;
            r.nums = {{"characterId", id}, {"userId", user}, {"val", val}};
            return encode(d, r);
        };
        s.base = {rec(evolving, 7, 42, 1), rec(evolving, 8, 42, 2), rec(v1, 7, 0, 5)};
        k07::CellRun run({Op::ViewPointRead, Entity::KeyComposite, t}, std::move(s), /*aggregate=*/false);
        run.run();
        EXPECT_EQ(run.skippedNoKey, 1u);
    }
}

TEST(CheckedWritesMatrixCountRegression, CellCountsArePinned) {
    EXPECT_EQ(applicable(cellsFor(Part::P03)), 78u);
    EXPECT_EQ(skipped(cellsFor(Part::P03)), 2u);
    EXPECT_EQ(applicable(cellsFor(Part::P04)), 585u);
    EXPECT_EQ(skipped(cellsFor(Part::P04)), 15u);
    EXPECT_EQ(applicable(cellsFor(Part::P05)), 560u);
    EXPECT_EQ(skipped(cellsFor(Part::P05)), 70u);
    EXPECT_EQ(applicable(cellsFor(Part::P06)), 798u);
    EXPECT_EQ(skipped(cellsFor(Part::P06)), 84u);
    EXPECT_EQ(applicable(cellsFor(Part::P07)), 420u);
    EXPECT_EQ(skipped(cellsFor(Part::P07)), 129u);
    EXPECT_EQ(aggCellsFor07().size(), 190u);
    EXPECT_EQ(applicable(cellsFor(Part::P07)) + aggCellsFor07().size(), 610u);
    EXPECT_EQ(applicable(cellsFor(Part::P09)), 600u);
    EXPECT_EQ(skipped(cellsFor(Part::P09)), 555u);
}

// The model's key index rule against the Single-timing table, all 30 op x entity pairs, on hand-built logs.
TEST(CheckedWritesModel, KeyIndexRuleMatchesTheTable) {
    using Log = VerdictRig::RigLog;
    using LogRec = VerdictRig::RigLogRec;
    const uint8_t kind = static_cast<uint8_t>(rune::view::VerdictViewKind::KeyIndex);
    for (const Op op : {Op::Insert, Op::Update, Op::Upsert, Op::Remove, Op::RemoveExisting})
        for (const Entity e : {Entity::KeyNew, Entity::KeyExisting, Entity::KeyEarlierInBatch, Entity::KeyRemovedEarlier, Entity::KeyRejectedEarlier, Entity::KeyBeforeIndex}) {
            SCOPED_TRACE(std::string(nameOf(op)) + " x " + nameOf(e));
            Log log;
            int64_t off = 0;
            const auto data = [&](const Rec& x) { log.data.push_back(LogRec{off++, x.id, x.v, x.tag, x.del, x.intent}); };
            std::vector<rune::storage::control_record::ChangeMember> members{{0, kind, kKeyIndexPlanFingerprint}};
            if (e == Entity::KeyRejectedEarlier) members.push_back({kObj, 0, tagRule(9).fingerprint()});
            const auto change = [&] { log.changes.push_back({off++, members}); };
            if (e == Entity::KeyBeforeIndex) { data(ups(kKey, 10)); change(); } else change();
            if (e == Entity::KeyExisting || e == Entity::KeyEarlierInBatch || e == Entity::KeyRemovedEarlier) data(ups(kKey, 10));
            if (e == Entity::KeyRemovedEarlier) data(Rec{kKey, 0, 0, true, 0});
            if (e == Entity::KeyRejectedEarlier) data(Rec{kKey, 10, 9, false, 1});
            const int64_t opOffset = off;
            data(opRec(op));

            CheckedWritesModel m;
            m.declareRule(kObj, tagRule(9).fingerprint(), modelRule(tagRule(9)));
            m.addKeyIndexRule();
            m.reset(log);
            const auto it = m.verdicts().find(opOffset);
            const uint16_t want = keyIndexVerdict05(op, e);
            EXPECT_EQ(it == m.verdicts().end() ? 0 : it->second.code, want) << nameOf(op) << " x " << nameOf(e);
            if (it != m.verdicts().end()) EXPECT_EQ(it->second.constraintId, 0u) << nameOf(op) << " x " << nameOf(e);
        }
}

// The model's unique and count rules against the Single-timing table, all 40 op x entity pairs, on hand-built logs: no broker code runs.
TEST(CheckedWritesModel, ConstraintRulesMatchTheTable) {
    using Log = VerdictRig::RigLog;
    using LogRec = VerdictRig::RigLogRec;
    constexpr uint64_t kName = 7, kStats = 8, kFingerprint = 99;
    std::size_t pairs = 0;
    for (const Cell& cell : cellsFor(Part::P09)) {
        if (cell.timing != Timing::Single || !expect(cell).applicable) continue;
        ++pairs;
        SCOPED_TRACE(std::string(nameOf(cell.op)) + " x " + nameOf(cell.entity));
        const Scenario09 sc = scenario09(cell.op, cell.entity);
        Log log;
        int64_t off = 0;
        uint64_t assigned = kIdBase09 + 5000;
        const auto data = [&](const Char09& c) {
            log.data.push_back(LogRec{off++, c.id == 0 ? ++assigned : c.id, static_cast<int64_t>(c.userId), 0, c.del, c.intent, 0, c.name});
        };
        for (const Char09& c : sc.setup) data(c);
        log.changes.push_back({off++, {{0, static_cast<uint8_t>(rune::view::VerdictViewKind::KeyIndex), kKeyIndexPlanFingerprint}, {kName, 0, kFingerprint}, {kStats, 0, kFingerprint}}});
        const int64_t opAt = off;
        for (const Char09& c : sc.op) data(c);

        CheckedWritesModel m;
        m.addKeyIndexRule();
        m.addLowerNameUniqueRule(kName);
        m.addCountCheckRule(kStats, 12);
        m.reset(log);
        const std::vector<V09> want = single09(cell.op, cell.entity);
        ASSERT_EQ(want.size(), sc.op.size());
        for (std::size_t i = 0; i < want.size(); ++i) {
            const auto it = m.verdicts().find(opAt + static_cast<int64_t>(i));
            if (want[i] == V09::Ok) { EXPECT_TRUE(it == m.verdicts().end()) << "record " << i; continue; }
            ASSERT_TRUE(it != m.verdicts().end()) << "record " << i;
            EXPECT_EQ(it->second, (ModelVerdict{55, want[i] == V09::N ? kNameCid09 : kStatsCid09})) << "record " << i;
        }
        for (const auto& [off2, v] : m.verdicts()) EXPECT_GE(off2, opAt) << "the setup is unjudged";
    }
    EXPECT_EQ(pairs, 40u);
}

// A hand-built log, judged by the model alone: no broker code runs.
TEST(CheckedWritesModel, ReferenceJudgeIsIndependent) {
    using CM = CheckedWritesModel;
    using Log = VerdictRig::RigLog;
    Log log;
    const auto data = [&](int64_t off, uint64_t id, int64_t v, uint8_t tag, bool del = false) { log.data.push_back({off, id, v, tag, del}); };
    const TestRejectRule tag9 = tagRule(9), cap2 = capRule(2);
    // 0: id1 t1 | 1: id2 t9 (no set yet: accepted) | 2: change {7: tag9} | 3: id3 t9 rejected | 4: id1 t2 | 5: id4 t1 | 6: tombstone id1
    // 7: change {7: cap2} | 8: id5 t1 (live 2,4: cap reached, rejected) | 9: id2 t3 | 10: tombstone id4 | 11: id6 t1 (live: 2 -> accepted)
    data(0, 1, 10, 1); data(1, 2, 20, 9);
    log.changes.push_back({2, {{7, 0, tag9.fingerprint()}}});
    data(3, 3, 30, 9); data(4, 1, 11, 2); data(5, 4, 40, 1); data(6, 1, 0, 0, true);
    log.changes.push_back({7, {{7, 0, cap2.fingerprint()}}});
    data(8, 5, 50, 1); data(9, 2, 21, 3); data(10, 4, 0, 0, true); data(11, 6, 60, 1);

    CM m;
    m.declareRule(7, tag9.fingerprint(), CM::tagEquals(9));
    m.declareRule(7, cap2.fingerprint(), CM::capLiveKeys(2));
    m.reset(log);

    const uint32_t cid = rune::view::constraintIdOf(7, 1);
    EXPECT_EQ(cid, CM::constraintIdOf(7));
    EXPECT_EQ(m.verdicts(), (std::map<int64_t, ModelVerdict>{{3, {55, cid}}, {8, {55, cid}}}));
    EXPECT_EQ(m.acceptedRecords().size(), 8u);
    EXPECT_EQ(m.latestAcceptedByKey().at(1).offset, 6);
    EXPECT_TRUE(m.latestAcceptedByKey().at(1).del);
    EXPECT_EQ(m.latestAcceptedByKey().at(2).offset, 9);
    EXPECT_EQ(m.projection(), (std::map<uint64_t, std::pair<int64_t, uint8_t>>{{2, {21, 3}}, {6, {60, 1}}}));
    EXPECT_EQ(m.countByTag(), (std::map<uint8_t, int64_t>{{1, 1}, {3, 1}}));
}

// The id predicates against hand-made logs: each fails on the one violation it names and holds on the clean log.
TEST(CheckedWritesModel, AutoIncPredicatesCatchHandMadeViolations) {
    using Log = VerdictRig::RigLog;
    using LogRec = VerdictRig::RigLogRec;
    using Ack = CheckedWritesModel::AutoIncAck;
    const auto rec = [](int64_t offset, uint64_t id) { return LogRec{offset, id, 1, 1, false, 1, id}; };
    const auto model = [](int32_t partitions, int32_t partition) {
        CheckedWritesModel m;
        m.addAutoIncRule(AutoIncShape{.partitions = partitions}, partition);
        return m;
    };
    Log clean;
    clean.reservations = {{0, 8}};
    clean.data = {rec(1, 1), rec(2, 2), rec(3, 3)};
    const std::vector<Ack> acks{{1, 1, 2}, {3, 3, 1}};
    const CheckedWritesModel one = model(1, 0);
    EXPECT_TRUE(one.noIdTwiceNoReuse(clean, acks));
    EXPECT_TRUE(one.idsFollowTheStride(clean, acks));
    EXPECT_TRUE(one.reservationCoversEveryId(clean));
    EXPECT_TRUE(one.ackedIdsMatchTheLog(clean, acks));
    EXPECT_TRUE(one.counterAboveEveryId(clean, AutoIncReady{4, 8}));

    Log twice = clean;
    twice.data[2] = rec(3, 2);
    EXPECT_FALSE(one.noIdTwiceNoReuse(twice, acks)) << "an id in two key-0 records";
    EXPECT_FALSE(one.noIdTwiceNoReuse(clean, {{1, 1, 2}, {3, 2, 1}})) << "an id in two acks";

    Log above = clean;
    above.data.push_back(rec(4, 9));
    EXPECT_FALSE(one.reservationCoversEveryId(above)) << "an id above the latest reservation";
    Log early = clean;
    early.reservations = {{2, 8}};
    EXPECT_FALSE(one.reservationCoversEveryId(early)) << "the reservation sits above the first id";

    Log skipped = clean;
    skipped.data = {rec(1, 1), rec(2, 3)};
    EXPECT_FALSE(one.idsFollowTheStride(skipped, {{1, 1, 2}})) << "keys 1, 3 at P 1";
    EXPECT_FALSE(model(4, 2).idsFollowTheStride(clean, {{1, 1, 1}})) << "a base on another partition";

    Log valueKey = clean;
    valueKey.data[0].valueId = 7;
    EXPECT_FALSE(one.ackedIdsMatchTheLog(valueKey, acks)) << "a value carrying another key";
    EXPECT_FALSE(one.ackedIdsMatchTheLog(clean, {{1, 2, 1}})) << "assignedBase is not the first key";

    EXPECT_FALSE(one.counterAboveEveryId(clean, AutoIncReady{3, 8})) << "the counter is not above id 3";
    EXPECT_FALSE(one.counterAboveEveryId(clean, AutoIncReady{9, 7})) << "the reservation does not reach nextN - 1";
}
