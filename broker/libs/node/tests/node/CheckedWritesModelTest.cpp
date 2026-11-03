// Checked writes Part 04 Task 11 (rig slice): a seeded random history against an independent model.  Three idempotent producers
// write through the ack path (with retries); the leader fails over, the tail is cut, the cleaner runs, and the constraint set
// gets a member installed, replaced and dropped.  After every step the shared matrix invariants (index, markers, views, delivery)
// hold against the model, built from every committed record the history ever held.  The cluster stress (ThreeNodes) is in
// CheckedWritesStressTest.cpp.  Part 05 adds the same check on a key.index topic (CheckedWritesKeyIndexModelRegression, below).
#include "CheckedWritesChecks.h"
#include "CheckedWritesRig.h"

#include <rune/view/Verdict.h>

#include <algorithm>
#include <functional>
#include <map>
#include <random>
#include <set>
#include <string>

using namespace rune::node;
using namespace rune::node::cw;

namespace {
constexpr int32_t kRejTag = 0x40000601, kProj = 0x40000602, kCnt = 0x40000603, kRejCapBase = 0x40000610;
constexpr uint64_t kObjTag = 7, kObjCap = 8;
constexpr uint8_t kAcksView = rune::protocol::client::kAcksWireView;
constexpr int kSteps = 250;
constexpr uint64_t kKeys = 12;

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
TestRejectRule capRule(uint32_t cap) { TestRejectRule r; r.mode = TestRejectRule::Mode::CapLiveKeys; r.cap = cap; return r; }

struct Producer {
    int64_t pid = 0;
    int32_t seq = 0;
    std::vector<Rec> lastRecs;
    int32_t lastSeq = -1;
};

class Run {
public:
    explicit Run(uint64_t seed) : rng_(seed), rig_(/*follower=*/true, /*rollBytes=*/1) {
        rig_.typed();
        installCap_(3);
        rig_.installRejectView(kRejTag, kObjTag, tagRule(9));
        rig_.installProjection(kProj);
        rig_.installCountById(kCnt);
        rig_.enableAcks();
        rig_.passes(60);
        for (int p = 0; p < 3; ++p) producers_[p].pid = 500 + p;
    }

    std::string trace;

    void step() {
        const unsigned r = static_cast<unsigned>(rng_() % 100);
        if (r < 45) {
            produce_(false);
        } else if (r < 55) {
            produce_(true);
        } else if (r < 63) {
            settle_();
            rig_.failover();
            trace += "F";
        } else if (r < 69) {
            settle_();
            ASSERT_EQ(rig_.produce({{1 + rng_() % kKeys, 0, 1}}), 0);   // an unacked tail the cut removes
            rig_.truncateTo(rig_.replica->highWatermark());
            rig_.demotePromote();
            trace += "T";
        } else if (r < 77) {
            compact_ = true;
            trace += "C";
        } else if (r < 83) {
            settle_();
            if (capTopic_ == 0) installCap_(2 + static_cast<uint32_t>(rng_() % 4)); else produce_(false);
            trace += "I";
        } else if (r < 90) {
            settle_();
            if (capTopic_ != 0) { replaceCap_(2 + (capValue_ - 2 + 1 + static_cast<uint32_t>(rng_() % 3)) % 4); setChanged_ = true; } else produce_(false);
            trace += "R";
        } else if (r < 96) {
            if (capTopic_ != 0) { rig_.dropRejectView(capTopic_); capTopic_ = 0; setChanged_ = true; } else produce_(false);
            trace += "D";
        } else {
            settle_();
            rig_.restart(true);
            trace += "S";
        }
        if (!::testing::Test::HasFatalFailure()) verify_();
    }

private:
    // Until a marker passed a dropped or replaced member's change the judge still holds its plan: a restart would wait for a plan
    // that is no longer installed, and a member of the same object and plan installed again would never join.  A fresh write is
    // acked and marked past the change first.
    void settle_() {
        if (!setChanged_) return;
        lastAcked_ = false;
        for (int i = 0; i < 16 && !lastAcked_ && !::testing::Test::HasFatalFailure(); ++i) produce_(false);
        setChanged_ = false;
        if (!::testing::Test::HasFatalFailure() && judged_()) rig_.passes(4);   // the retired member leaves on the pass after the marker commits
    }
    // The judge and the committed markers cover every data record (a trailing change record needs no marker).
    bool judged_() {
        const auto written = rig_.readLog();
        return rig_.driveUntilJudged(written.data.empty() ? 0 : written.data.back().offset + 1);
    }
    void installCap_(uint32_t cap) {
        capTopic_ = kRejCapBase + (++capSeq_);
        rig_.installRejectView(capTopic_, kObjCap, capRule(cap), "cap");
        caps_.insert(cap);
        capValue_ = cap;
    }
    void replaceCap_(uint32_t cap) {
        const int32_t next = kRejCapBase + (++capSeq_);
        rig_.replaceRejectView(capTopic_, next, kObjCap, capRule(cap));
        capTopic_ = next;
        caps_.insert(cap);
        capValue_ = cap;
    }

    void produce_(bool retry) {
        Producer& p = producers_[rng_() % 3];
        std::vector<Rec> recs;
        int32_t seq;
        if (retry && p.lastSeq >= 0) {
            recs = p.lastRecs;
            seq = p.lastSeq;
            trace += "r";
        } else {
            const int n = 1 + static_cast<int>(rng_() % 5);
            for (int i = 0; i < n; ++i) {
                const uint64_t id = 1 + rng_() % kKeys;
                if (rng_() % 12 == 0) recs.push_back({id, 0, 0, true});
                else recs.push_back({id, static_cast<int64_t>(rng_() % 1000), static_cast<uint8_t>(rng_() % 4 == 0 ? 9 : 1 + rng_() % 2)});
            }
            seq = p.seq;
            p.seq += n;
            p.lastRecs = recs;
            p.lastSeq = seq;
            trace += "p" + std::to_string(n);
        }
        const int64_t c = rig_.produceFrame(recs, kAcksView, p.pid, seq);
        const auto a = rig_.ackFetching(c, 60000);
        ASSERT_TRUE(a) << "the produce was never answered";
        lastAcked_ = a->code == 0;
        trace += lastAcked_ ? "+" : "-";
    }

    void verify_() {
        ASSERT_TRUE(judged_()) << " after " << trace;
        const int64_t hw = rig_.replica->highWatermark();
        const VerdictRig::RigLog log = rig_.readLog(hw);
        for (const auto& d : log.data) data_[d.offset] = d;
        for (const auto& ch : log.changes) changes_[ch.offset] = ch;
        VerdictRig::RigLog all;
        for (const auto& kv : data_) all.data.push_back(kv.second);
        for (const auto& kv : changes_) all.changes.push_back(kv.second);
        CheckedWritesModel model;
        model.declareRule(kObjTag, tagRule(9).fingerprint(), CheckedWritesModel::tagEquals(9));
        for (const uint32_t cap : caps_) model.declareRule(kObjCap, capRule(cap).fingerprint(), CheckedWritesModel::capLiveKeys(cap));
        model.reset(all);
        std::set<int64_t> present;
        for (const auto& d : log.data) present.insert(d.offset);

        const int64_t dataEnd = log.data.empty() ? 0 : log.data.back().offset + 1;
        rig_.driveUntilFolded(kProj, dataEnd);
        rig_.driveUntilFolded(kCnt, dataEnd);
        checkIndex(rig_, model, &present);
        if (!log.data.empty() && !log.markers.empty() && !log.changes.empty())
            checkMarkers(model, log, log.data.back().offset + 1, compacted_);
        checkViews(rig_, model, all, kProj, kCnt);
        checkDelivered(rig_, model, rig_.subscribe(SubKind::PushRead, 0), false, &present);
        if (compact_) {
            compact_ = false;
            compacted_ = true;
            checkCompaction(rig_, model, log);
        }
        trace += "[" + std::to_string(model.acceptedRecords().size()) + "/" + std::to_string(model.verdicts().size()) + "]";
    }

    std::mt19937_64 rng_;
    VerdictRig rig_;
    Producer producers_[3];
    std::map<int64_t, VerdictRig::RigLogRec> data_;
    std::map<int64_t, VerdictRig::RigChange> changes_;
    int32_t capTopic_ = 0, capSeq_ = 0;
    uint32_t capValue_ = 0;
    std::set<uint32_t> caps_;
    bool compact_ = false, compacted_ = false, setChanged_ = false, lastAcked_ = false;
};

std::string runSeed(uint64_t seed) {
    Run run(seed);
    for (int step = 0; step < kSteps && !::testing::Test::HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step) + " trace=" + run.trace);
        run.step();
    }
    return run.trace;
}
}  // namespace

class CheckedWritesModelRegression : public ::testing::TestWithParam<uint64_t> {};

TEST_P(CheckedWritesModelRegression, SeededRigMatchesTheModel) {
    const std::string trace = runSeed(GetParam());
    EXPECT_FALSE(trace.empty());
}

// The history is a function of the seed: two runs of the rig make the same choices and the model judges the same verdicts.
TEST_P(CheckedWritesModelRegression, SameSeedSameTrace) {
    const std::string first = runSeed(GetParam());
    const std::string second = runSeed(GetParam());
    EXPECT_EQ(first, second);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesModelRegression, ::testing::Values(1, 2, 3, 4, 5, 6));


// ---- Checked writes Part 05 Task 9: the same check on a key.index topic --------------------------------------------------------
// Two idempotent All | View producers send the five ops on 8 keys in batches of 1-8 records (so keys repeat within a batch), a
// non-idempotent acks=leader producer sends plain upserts and removes, a TestReject{TagEquals{9}} member is installed, replaced and
// dropped, and the leader fails over, is demoted, restarts, loses its tail, compacts and commits or aborts a transaction.  The
// model is Task 8's: the key index rule answers 56 / 57 from the live keys of the accepted records before the judged one.
namespace {
constexpr uint64_t kKiKeys = 8;
constexpr int kKiSteps = 250;
constexpr int64_t kKiTxnPidBase = 900;
constexpr int32_t kKiRejBase = 0x40000700;
constexpr int16_t kNotLeaderCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
constexpr uint8_t kAcksLeader = 0, kAcksWireAll = rune::protocol::client::kAcksWireAll;
constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;

enum class KiAction : uint8_t { Intents, Plain, RetryBeforeJudge, RetryAfterJudge, RejectMember, Failover, DemotePromote, Restart, TruncateTail, Compact, Transaction };
const char* nameOf(KiAction a) {
    switch (a) {
    case KiAction::Intents: return "Intents";
    case KiAction::Plain: return "Plain";
    case KiAction::RetryBeforeJudge: return "RetryBeforeJudge";
    case KiAction::RetryAfterJudge: return "RetryAfterJudge";
    case KiAction::RejectMember: return "RejectMember";
    case KiAction::Failover: return "Failover";
    case KiAction::DemotePromote: return "DemotePromote";
    case KiAction::Restart: return "Restart";
    case KiAction::TruncateTail: return "TruncateTail";
    case KiAction::Compact: return "Compact";
    case KiAction::Transaction: return "Transaction";
    }
    return "";
}

class KiRun {
public:
    explicit KiRun(uint64_t seed) : rng_(seed), rig_(/*follower=*/true, /*rollBytes=*/1) {
        rig_.createKeyIndex();
        installTag_();
        rig_.installProjection(kProj);
        rig_.installCountById(kCnt);
        rig_.enableAcks();
        rig_.passes(60);
        rig_.followerFetch();
        rig_.passes(10);
        waitActive_();
        for (int p = 0; p < 2; ++p) producers_[p].pid = 500 + p;
    }

    std::string trace;

    KiAction pick() {
        const unsigned r = static_cast<unsigned>(rng_() % 100);
        if (r < 40) return KiAction::Intents;
        if (r < 50) return KiAction::Plain;
        if (r < 55) return KiAction::RetryBeforeJudge;
        if (r < 60) return KiAction::RetryAfterJudge;
        if (r < 66) return KiAction::RejectMember;
        if (r < 71) return KiAction::Failover;
        if (r < 75) return KiAction::DemotePromote;
        if (r < 79) return KiAction::Restart;
        if (r < 83) return KiAction::TruncateTail;
        if (r < 89) return KiAction::Compact;
        return KiAction::Transaction;
    }

    void step(KiAction action) {
        switch (action) {
        case KiAction::Intents: trace += "i"; intents_(); break;
        case KiAction::Plain: trace += "P"; plain_(); break;
        case KiAction::RetryBeforeJudge: trace += "b"; retry_(false); break;
        case KiAction::RetryAfterJudge: trace += "a"; retry_(true); break;
        case KiAction::RejectMember: rejectMember_(); break;
        case KiAction::Failover:
            trace += "F";
            settle_();
            rig_.failover();
            leadershipBarrier_();
            break;
        case KiAction::DemotePromote:
            trace += "D";
            settle_();
            rig_.demotePromote();
            leadershipBarrier_();
            break;
        case KiAction::Restart:
            trace += "S";
            settle_();
            rig_.restart(true);
            leadershipBarrier_();
            break;
        case KiAction::TruncateTail:
            trace += "T";
            settle_();
            ASSERT_EQ(rig_.produce({{1 + rng_() % kKiKeys, 0, 1}}), 0);   // an unacked tail the cut removes
            rig_.truncateTo(rig_.replica->highWatermark());
            rig_.demotePromote();
            leadershipBarrier_();
            break;
        case KiAction::Compact: trace += "C"; compact_ = true; break;
        case KiAction::Transaction: transaction_(); break;
        }

        if (!::testing::Test::HasFatalFailure()) verify_();
    }

private:
    void waitActive_() {
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            const PartitionWriteState* w = rig_.writeState();
            if (w != nullptr && w->constraintSets.latest().count > 0 && w->constraintSets.latest().members[0].kind == rune::view::VerdictViewKind::KeyIndex &&
                rig_.worker->rcStatusForTest(VerdictRig::kSrc, 0) == Worker::kRcReady) return;
            if (i % 8 == 0) rig_.followerFetch();
            rig_.pass();
        }
        FAIL() << "the key index was not activated after " << trace;
    }

    // The key index is rebuilt from the local log by the passes after a leadership (one batch per pass here, a segment per batch), and the
    // judge takes a new write only once it is rebuilt: an intent batch acked is the barrier, so the seeded run covers intents judged after the rebuild.
    void leadershipBarrier_() {
        waitActive_();
        if (::testing::Test::HasFatalFailure()) return;
        ASSERT_TRUE(sendIdem_(producers_[rng_() % 2], intentBatch_(1 + rng_() % 4)));
    }

    void installTag_() {
        rejTopic_ = kKiRejBase + (++rejSeq_);
        rig_.installRejectView(rejTopic_, kObjTag, tagRule(9));
    }
    // A dropped or replaced member's change must be passed by a marker before the next restart, and before a member of the same object
    // is installed again: a fresh write is acked and marked past it.
    void settle_() {
        if (!setChanged_) return;
        bool acked = false;
        for (int i = 0; i < 16 && !acked && !::testing::Test::HasFatalFailure(); ++i) {
            const auto a = sendIdem_(producers_[0], {Rec{1 + rng_() % kKiKeys, ++vSeq_, 1, false, 0}});
            acked = a.has_value();
        }
        ASSERT_TRUE(acked) << "no write was acked after a constraint-set change";
        setChanged_ = false;
        if (judged_()) rig_.passes(4);   // the retired member leaves on the pass after the marker commits
    }
    void rejectMember_() {
        trace += "M";
        const unsigned r = static_cast<unsigned>(rng_() % 3);
        if (rejTopic_ == 0) {
            settle_();
            installTag_();
        } else if (r == 0) {
            rig_.dropRejectView(rejTopic_);
            rejTopic_ = 0;
            setChanged_ = true;
        } else {
            settle_();
            const int32_t next = kKiRejBase + (++rejSeq_);
            rig_.replaceRejectView(rejTopic_, next, kObjTag, tagRule(9));
            rejTopic_ = next;
            setChanged_ = true;
        }
    }

    // One past the last record that gets a verdict: the judge skips an aborted transaction's records and emits no marker for a
    // trailing range of them.
    int64_t judgedEnd_(const VerdictRig::RigLog& log) const {
        int64_t end = 0;
        for (const auto& d : log.data)
            if (aborted_.count(d.offset) == 0) end = d.offset + 1;
        return end;
    }
    bool judged_() { return rig_.driveUntilJudged(judgedEnd_(rig_.readLog())); }

    std::vector<Rec> intentBatch_(uint64_t n) {
        std::vector<Rec> recs;
        for (uint64_t i = 0; i < n; ++i) {
            const uint64_t id = 1 + rng_() % kKiKeys;
            const uint8_t tag = rng_() % 6 == 0 ? 9 : static_cast<uint8_t>(1 + rng_() % 2);
            const int64_t v = ++vSeq_;
            switch (rng_() % 5) {
            case 0: recs.push_back({id, v, tag, false, 1}); break;      // insert
            case 1: recs.push_back({id, v, tag, false, 2}); break;      // update
            case 2: recs.push_back({id, v, tag, false, 0}); break;      // upsert
            case 3: recs.push_back({id, 0, 0, true, 0}); break;         // remove
            default: recs.push_back({id, 0, 0, true, 2}); break;        // removeExisting
            }
        }
        return recs;
    }

    struct Pending { VerdictRig::Ack ack; std::size_t n; int64_t corr; };

    // An intent meets NotLeader until the key index is active; nothing was appended then, so the same sequence is sent again.
    std::optional<VerdictRig::Ack> sendIdem_(Producer& p, const std::vector<Rec>& recs) {
        waitActive_();
        if (::testing::Test::HasFatalFailure()) return std::nullopt;
        for (int tries = 0; tries < 100; ++tries) {
            const int64_t c = rig_.produceFrame(recs, kAcksViewAll, p.pid, p.seq);
            const auto a = rig_.ackFetching(c, 60000);
            if (!a) { ADD_FAILURE() << "the produce was never answered, corr " << c; return std::nullopt; }
            if (a->code == kNotLeaderCode) { rig_.passes(5); continue; }
            if (a->code != 0) { ADD_FAILURE() << "ack code " << a->code << " corr " << c; return std::nullopt; }
            p.seq += static_cast<int32_t>(recs.size());
            pending_.push_back({*a, recs.size(), c});
            return a;
        }
        ADD_FAILURE() << "NotLeader for 100 sends in a row";
        return std::nullopt;
    }

    void intents_() {
        const auto recs = intentBatch_(1 + rng_() % 8);
        trace += std::to_string(recs.size());
        (void)sendIdem_(producers_[rng_() % 2], recs);
    }
    void plain_() {
        std::vector<Rec> recs;
        const uint64_t n = 1 + rng_() % 4;
        for (uint64_t i = 0; i < n; ++i) {
            const uint64_t id = 1 + rng_() % kKiKeys;
            if (rng_() % 3 == 0) recs.push_back({id, 0, 0, true, 0});
            else recs.push_back({id, ++vSeq_, static_cast<uint8_t>(rng_() % 5 == 0 ? 9 : 1 + rng_() % 3), false, 0});
        }
        const int64_t c = rig_.produceFrame(recs, kAcksLeader, -1, -1);
        const auto a = rig_.ackFetching(c, 60000);
        ASSERT_TRUE(a) << "the produce was never answered";
        ASSERT_EQ(a->code, 0);
    }
    // The same batch is sent twice with one pid and sequence: before the judge caught up, or after.  The answer is the original's.
    void retry_(bool judgeFirst) {
        waitActive_();
        if (::testing::Test::HasFatalFailure()) return;
        Producer& p = producers_[rng_() % 2];
        const auto recs = intentBatch_(1 + rng_() % 8);
        const int32_t seq = p.seq;
        int64_t origCorr = 0, dupCorr = 0;
        ASSERT_TRUE(executeRetry(rig_, judgeFirst, false,
                                 [&] { origCorr = rig_.produceFrame(recs, kAcksViewAll, p.pid, seq); },
                                 [](uint64_t) {},
                                 [&] { dupCorr = rig_.produceFrame(recs, kAcksViewAll, p.pid, seq); }));
        const auto orig = rig_.ackFetching(origCorr, 60000);
        const auto dup = rig_.ackFetching(dupCorr, 60000);
        ASSERT_TRUE(orig && dup) << "a send was never answered";
        ASSERT_EQ(orig->code, 0);
        p.seq += static_cast<int32_t>(recs.size());
        checkDupAck(rig_, *orig, *dup, false);
        pending_.push_back({*orig, recs.size(), origCorr});
    }
    // An open transaction of plain upserts, committed or aborted; the judge is held behind it.  A source with a TestReject member refuses
    // transactions (InvalidRequest), so the member is dropped and its change logged first.
    void transaction_() {
        if (rejTopic_ != 0) {
            rig_.dropRejectView(rejTopic_);
            rejTopic_ = 0;
            setChanged_ = true;
        }
        settle_();
        if (::testing::Test::HasFatalFailure()) return;
        const int64_t pid = kKiTxnPidBase + ++txns_;
        std::vector<Rec> recs;
        const uint64_t n = 1 + rng_() % 3;
        for (uint64_t i = 0; i < n; ++i) recs.push_back({1 + rng_() % kKiKeys, ++vSeq_, static_cast<uint8_t>(1 + rng_() % 2), false, 0});
        const int64_t c = rig_.produceFrame(recs, kAcksWireAll, pid, 0, /*txn=*/true);
        const auto a = rig_.ackFetching(c, 60000);
        ASSERT_TRUE(a) << "the transactional produce was never answered";
        ASSERT_EQ(a->code, 0);
        const bool commit = rng_() % 2 == 0;
        trace += commit ? "X" : "Z";
        rig_.marker(pid, commit);
        rig_.followerFetch();   // a failover or a cut must not lose the marker: the transaction would be open again
        rig_.passes(4);
        if (!commit)
            for (uint64_t i = 0; i < n; ++i) aborted_.insert(a->baseOffset + static_cast<int64_t>(i));
    }

    // What a raw and a push-read subscription from 0 deliver: the accepted records with their intents, and the aborted transactions' (they are in the log).
    void checkDelivered_(const CheckedWritesModel& model, const std::set<int64_t>& present) {
        std::vector<VerdictRig::RigLogRec> aborted;
        for (const int64_t off : aborted_)
            if (const auto it = data_.find(off); it != data_.end()) aborted.push_back(it->second);
        checkDelivered(rig_, model, rig_.subscribe(SubKind::Raw, 0), false, &present, &aborted);
        checkDelivered(rig_, model, rig_.subscribe(SubKind::PushRead, 0), false, &present, &aborted);
    }

    // IntentVerdictsExact: every 56 / 57 ever reported (an ack, the index or a marker) is the model's, and a record an ack reported as
    // accepted never gets a verdict.  KeyIndexEqualsLiveKeys: the key index holds the model's live keys.
    void checkIntentVerdicts_(const CheckedWritesModel& model, const VerdictRig::RigLog& log, int64_t hw) {
        const auto note = [&](int64_t off, uint16_t code, uint32_t cid, const char* from) {
            if (code != CheckedWritesModel::kKeyExists && code != CheckedWritesModel::kKeyNotFound) return;
            const auto it = reported_.emplace(off, ModelVerdict{code, cid}).first;
            EXPECT_EQ(it->second, (ModelVerdict{code, cid})) << "IntentVerdictsExact: " << from << " reported another verdict for offset " << off << " than before";
        };
        rig_.writeState()->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { if (e.offset < hw) note(e.offset, e.code, e.constraintId, "the index"); });
        for (const auto& m : log.markers)
            for (const auto& [off, code, cid] : m.entries) note(off, code, cid, "a marker");
        for (const Pending& p : pending_) {
            std::set<std::size_t> rejected;
            for (const auto& [i, code, cid] : p.ack.verdicts) {
                note(p.ack.baseOffset + i, code, cid, "an ack");
                rejected.insert(i);
            }
            for (std::size_t i = 0; i < p.n; ++i)
                if (rejected.count(i) == 0) acceptedByAck_.insert(p.ack.baseOffset + static_cast<int64_t>(i));
        }
        for (const auto& [off, verdict] : reported_) {
            const auto it = model.verdicts().find(off);
            EXPECT_TRUE(it != model.verdicts().end() && it->second == verdict) << "IntentVerdictsExact: the reported " << verdict.code << " at offset " << off << " is not the model's verdict";
        }
        for (const int64_t off : acceptedByAck_)
            EXPECT_EQ(model.verdicts().count(off), 0u) << "IntentVerdictsExact: the record at " << off << " was acked as accepted but the model rejects it";
        std::set<std::string> live;
        for (const auto& [id, row] : model.projection()) live.insert(std::to_string(id));
        EXPECT_EQ(rig_.keyIndexKeys(), live) << "KeyIndexEqualsLiveKeys";
        EXPECT_EQ(rig_.stats()->keyIndexSkippedMalformed, 0u);
    }

    void verify_() {
        ASSERT_TRUE(judged_()) << " after " << trace;
        const int64_t hw = rig_.replica->highWatermark();
        const VerdictRig::RigLog log = rig_.readLog(hw);
        for (const auto& d : log.data) data_[d.offset] = d;
        for (const auto& ch : log.changes) changes_[ch.offset] = ch;
        VerdictRig::RigLog all;
        for (const auto& kv : data_)
            if (aborted_.count(kv.first) == 0) all.data.push_back(kv.second);
        for (const auto& kv : changes_) all.changes.push_back(kv.second);
        CheckedWritesModel model;
        model.declareRule(kObjTag, tagRule(9).fingerprint(), CheckedWritesModel::tagEquals(9));
        model.addKeyIndexRule();
        model.reset(all);
        std::set<int64_t> present;
        for (const auto& d : log.data) present.insert(d.offset);

        const int64_t dataEnd = log.data.empty() ? 0 : log.data.back().offset + 1;
        rig_.driveUntilFolded(kProj, dataEnd);
        rig_.driveUntilFolded(kCnt, dataEnd);
        checkIndex(rig_, model, &present);
        if (!log.data.empty() && !log.markers.empty() && !log.changes.empty()) checkMarkers(model, log, judgedEnd_(log), compacted_);
        for (const Pending& p : pending_) checkModelTrailer(model, p.ack, p.n, p.corr);
        checkIntentVerdicts_(model, log, hw);
        pending_.clear();
        checkViews(rig_, model, all, kProj, kCnt);
        checkDelivered_(model, present);
        if (compact_) {
            compact_ = false;
            compacted_ = true;
            checkCompaction(rig_, model, log);
        }
        trace += "[" + std::to_string(model.acceptedRecords().size()) + "/" + std::to_string(model.verdicts().size()) + "]";
    }

    std::mt19937_64 rng_;
    VerdictRig rig_;
    Producer producers_[2];
    std::vector<Pending> pending_;
    std::map<int64_t, VerdictRig::RigLogRec> data_;
    std::map<int64_t, VerdictRig::RigChange> changes_;
    std::map<int64_t, ModelVerdict> reported_;
    std::set<int64_t> acceptedByAck_, aborted_;
    int32_t rejTopic_ = 0, rejSeq_ = 0;
    int64_t vSeq_ = 0, txns_ = 0;
    bool compact_ = false, compacted_ = false, setChanged_ = false;
};

std::string runKiSeed(uint64_t seed) {
    KiRun run(seed);
    for (int step = 0; step < kKiSteps && !::testing::Test::HasFatalFailure(); ++step) {
        const KiAction action = run.pick();
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step) + " action=" + nameOf(action) + " trace=" + run.trace);
        run.step(action);
    }
    return run.trace;
}
}  // namespace

class CheckedWritesKeyIndexModelRegression : public ::testing::TestWithParam<uint64_t> {};

TEST_P(CheckedWritesKeyIndexModelRegression, SeededRigMatchesTheModel) {
    const std::string trace = runKiSeed(GetParam());
    EXPECT_FALSE(trace.empty());
}

// The history is a function of the seed: seeds 1-8 run twice make the same choices and the model judges the same verdicts.
TEST_P(CheckedWritesKeyIndexModelRegression, SameSeedSameTrace) {
    if (GetParam() > 8) GTEST_SKIP() << "seeds 1-8 only";
    const std::string first = runKiSeed(GetParam());
    const std::string second = runKiSeed(GetParam());
    EXPECT_EQ(first, second);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesKeyIndexModelRegression, ::testing::Range<uint64_t>(1, 101));

// A tombstone is judged by the key index alone: a TestReject rule that would reject any record (a cap of 0 live keys) lets it through,
// and the key index answers 57 for removeExisting of an absent key.
TEST(CheckedWritesModel, TombstonesAreJudgedOnlyByTheKeyIndex) {
    using CM = CheckedWritesModel;
    VerdictRig::RigLog log;
    const TestRejectRule cap0 = capRule(0);
    const uint8_t kind = static_cast<uint8_t>(rune::view::VerdictViewKind::KeyIndex);
    log.changes.push_back({0, {{0, kind, kKeyIndexPlanFingerprint}, {kObjCap, 0, cap0.fingerprint()}}});
    log.data.push_back({1, 5, 0, 0, true, 2});    // removeExisting of an absent key: 57 by the key index
    log.data.push_back({2, 5, 0, 0, true, 0});    // remove of an absent key: accepted
    log.data.push_back({3, 5, 10, 1, false, 0});  // an upsert: the cap rule rejects it
    log.data.push_back({4, 5, 0, 0, true, 2});    // removeExisting of a key whose upsert was rejected: 57 again
    CM m;
    m.declareRule(kObjCap, cap0.fingerprint(), CM::capLiveKeys(0));
    m.addKeyIndexRule();
    m.reset(log);
    EXPECT_EQ(m.verdicts(), (std::map<int64_t, ModelVerdict>{{1, {57, 0}}, {3, {55, CM::constraintIdOf(kObjCap)}}, {4, {57, 0}}}));
    EXPECT_EQ(m.acceptedRecords().size(), 1u);
    EXPECT_EQ(m.acceptedRecords()[0].offset, 2);
}


// One cleaner pass over more segments than the swap ring has slots (256): the rig drains the ring while it queues.
TEST(CheckedWritesRigRegression, CompactQueuesMoreSegmentsThanTheSwapRingHoldsRegression) {
    VerdictRig r(/*follower=*/false, /*rollBytes=*/1);
    r.typed();
    r.installRejectView(kRejTag, kObjTag, tagRule(9));
    r.passes(50);
    for (int64_t v = 1; v <= 300; ++v) ASSERT_EQ(r.produce({{5, v, 0}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_GT(r.compact().groups, 256) << "the pass must queue more groups than the ring has slots";
    (void)r.compact();
    const auto data = r.readLog().data;
    ASSERT_EQ(data.size(), 1u);
    EXPECT_EQ(data[0].v, 300);
}
