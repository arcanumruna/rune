// Checked writes Part 04 Task 10: the shared interaction matrix (5 ops x 8 entities x 15 timings) on a real Worker, judged against
// the independent reference model, plus the pins of both parts' cell counts.
#include "CheckedWritesMatrixExpectations.h"
#include "CheckedWritesModel.h"

#include <rune/view/Verdict.h>

#include <algorithm>
#include <map>
#include <set>
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

        checkIndex_(model, log);
        checkMarkers_(model, log);
        checkViews_(model, log);
        if (sub != 0) checkDelivered_(model, sub);
        checkAcks_(ex, model);
        if (isRetry(c_.timing)) EXPECT_EQ(log.data.size(), uniqueRecs_) << "a retry is stored once";
        EXPECT_EQ(r_.writeState()->rejected.mismatches(), 0u);
        checkCompaction_(model, log);
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
        case Timing::RetryBeforeJudge:
            send_(batches_[0], 0);
            resend_(sent_[0]);
            return;
        case Timing::RetryAfterJudge:
            send_(batches_[0], 0);
            ASSERT_TRUE(r_.driveUntilJudged());
            resend_(sent_[0]);
            r_.settle(150);
            return;
        case Timing::RetryAfterWindow: {
            send_(batches_[0], 0);
            ASSERT_TRUE(r_.driveUntilJudged());
            for (uint64_t i = 0; i < 33; ++i) send_({{100 + i, 1, 0}}, 0);
            ASSERT_TRUE(r_.driveUntilJudged());
            r_.settle(150);
            resend_(sent_[0]);
            r_.settle(150);
            return;
        }
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
            for (int i = 0; i < 600 && r_.leo() <= r_.replica->highWatermark(); ++i) r_.pass();
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

    void checkIndex_(const CheckedWritesModel& model, const VerdictRig::RigLog&) {
        std::map<int64_t, ModelVerdict> got;
        r_.writeState()->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { got[e.offset] = {e.code, e.constraintId}; });
        EXPECT_EQ(got, model.verdicts()) << "(a) the rejected offsets";
    }

    void checkMarkers_(const CheckedWritesModel& model, const VerdictRig::RigLog& log) {
        ASSERT_FALSE(log.markers.empty());
        std::map<int64_t, ModelVerdict> union_;
        int64_t prevThrough = -1;
        for (const auto& m : log.markers) {
            if (prevThrough >= 0) EXPECT_EQ(m.header.fromOffset, prevThrough) << "(b) markers tile the log without a gap";
            prevThrough = m.header.finalThrough;
            for (const auto& [o, code, cid] : m.entries) {
                EXPECT_GE(o, m.header.fromOffset);
                EXPECT_LT(o, m.header.finalThrough);
                union_[o] = {code, cid};
            }
        }
        EXPECT_GE(prevThrough, target_);
        EXPECT_EQ(union_, model.verdicts()) << "(b) the markers carry every verdict";
    }

    void checkViews_(const CheckedWritesModel& model, const VerdictRig::RigLog& log) {
        std::set<uint64_t> ids;
        for (const auto& d : log.data) ids.insert(d.id);
        const auto proj = model.projection();
        for (const uint64_t id : ids) {
            const auto row = r_.idRow(kProj, id);
            const auto it = proj.find(id);
            if (it == proj.end()) { EXPECT_FALSE(row) << "(c)(d) key " << id << " is not live"; continue; }
            ASSERT_TRUE(row) << "(d) key " << id;
            EXPECT_EQ(row->first, it->second.first) << "key " << id;
            EXPECT_EQ(row->second, it->second.second) << "key " << id;
        }
        const auto counts = model.countById();
        for (const uint64_t id : ids) {
            const auto it = counts.find(id);
            const int64_t want = it == counts.end() ? 0 : it->second, got = r_.countRow(kCnt, id).value_or(0);
            EXPECT_EQ(got, want) << "count of key " << id;
        }
    }

    void checkDelivered_(const CheckedWritesModel& model, int64_t sub) {
        const auto& box = r_.collect(sub);
        std::vector<std::tuple<int64_t, uint64_t, uint16_t>> got, want;
        for (const auto& d : box.records) got.emplace_back(d.offset, d.id, d.tag);
        const bool filtered = c_.entity == Entity::FilteredSubscription;
        for (const auto& a : model.acceptedRecords()) {
            if (filtered && (a.del || a.tag != 1)) continue;
            want.emplace_back(a.offset, a.id, a.del ? kTombstoneTag : a.tag);
        }
        EXPECT_EQ(got, want) << "(c) the delivered records are the accepted ones";
    }

    void checkAcks_(const Expected& ex, const CheckedWritesModel& model) {
        if (c_.entity == Entity::OldWriters) {
            for (const Sent& s : sent_) {
                if (isFaulty(c_.timing) && c_.timing != Timing::FailoverAppendHw && !r_.acked(s.corr)) continue;   // a fault ate the ack
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
            if (isFaulty(c_.timing) && c_.timing != Timing::FailoverAppendHw && !r_.acked(s.corr)) continue;   // a fault ate the ack: the writer re-sends
            const auto a = r_.ackFetching(s.corr);
            ASSERT_TRUE(a) << "corr " << s.corr;
            okAcks[s.corr] = *a;
            if (a->code != 0) {
                EXPECT_EQ(c_.timing, Timing::RetryAfterWindow);
                continue;
            }
            EXPECT_TRUE(a->hasVerdicts);
            std::vector<std::tuple<uint16_t, uint16_t, uint32_t>> want;
            for (std::size_t i = 0; i < s.recs.size(); ++i)
                if (const auto it = model.verdicts().find(a->baseOffset + static_cast<int64_t>(i)); it != model.verdicts().end())
                    want.emplace_back(static_cast<uint16_t>(i), it->second.code, it->second.constraintId);
            EXPECT_EQ(a->verdicts, want) << "the trailer is the model's rejections, corr " << s.corr;
            EXPECT_EQ(a->frameLen, 54u + 2 + 8 * want.size());
        }
        for (const auto& [orig, dup] : dups_) {
            const auto& o = okAcks.at(orig);
            const auto& d = okAcks.at(dup);
            if (ex.check == Check::RetryOutsideWindow) {
                EXPECT_EQ(d.code, static_cast<int16_t>(ErrorCode::DuplicateSequence));
                EXPECT_EQ(d.frameLen, 54u);
                EXPECT_FALSE(d.hasVerdicts);
                EXPECT_EQ(r_.stats()->verdictUnknownDuplicates, 1u);
            } else {
                EXPECT_EQ(d.code, 0);
                EXPECT_EQ(d.baseOffset, o.baseOffset);
                EXPECT_EQ(d.verdicts, o.verdicts) << "a retry in the window replays the original verdicts";
            }
        }
    }

    void checkCompaction_(const CheckedWritesModel& model, const VerdictRig::RigLog& before) {
        const auto cp = r_.compact();
        const auto after = r_.readLog();
        std::set<int64_t> kept;
        for (const auto& d : after.data) kept.insert(d.offset);
        for (const auto& d : before.data)
            if (d.offset >= cp.bound) EXPECT_TRUE(kept.count(d.offset)) << "(g) offset " << d.offset << " is at or above the verdict bound " << cp.bound;
        for (const auto& [id, latest] : model.latestAcceptedByKey()) {
            if (latest.del) continue;
            EXPECT_TRUE(kept.count(latest.offset)) << "(e) key " << id << " lost its latest accepted record";
        }
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

class CheckedWritesMatrix : public ::testing::TestWithParam<Cell> {};

TEST_P(CheckedWritesMatrix, Holds) {
    const Cell c = GetParam();
    const Expected ex = expect(c);
    if (!ex.applicable) GTEST_SKIP() << ex.naReason;
    CellRun(c).run();
}

INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesMatrix, ::testing::ValuesIn(cellsFor(Part::P04)),
    [](const ::testing::TestParamInfo<Cell>& i) {
        return std::string(nameOf(i.param.op)) + "_" + nameOf(i.param.entity) + "_" + nameOf(i.param.timing);
    });

TEST(CheckedWritesMatrixCountRegression, CellCountsArePinned) {
    EXPECT_EQ(applicable(cellsFor(Part::P03)), 78u);
    EXPECT_EQ(skipped(cellsFor(Part::P03)), 2u);
    EXPECT_EQ(applicable(cellsFor(Part::P04)), 585u);
    EXPECT_EQ(skipped(cellsFor(Part::P04)), 15u);
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
