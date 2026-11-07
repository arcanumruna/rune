// Checked writes Part 04 Task 11 (rig slice): a seeded random history against an independent model.  Three idempotent producers
// write through the ack path (with retries); the leader fails over, the tail is cut, the cleaner runs, and the constraint set
// gets a member installed, replaced and dropped.  After every step the shared matrix invariants (index, markers, views, delivery)
// hold against the model, built from every committed record the history ever held.  The cluster stress (ThreeNodes) is in
// CheckedWritesStressTest.cpp.  Part 05 adds the same check on a key.index topic (CheckedWritesKeyIndexModelRegression, below).
#include "CheckedWritesChecks.h"
#include "CheckedWritesRig.h"

#include <rune/codec/UnicodeLower.h>
#include <rune/view/Verdict.h>
#include <rune/view/ViewRow.h>

#include <algorithm>
#include <array>
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
// Several batches per segment: the cleaner still has sealed segments of repeated keys to compact, and a restart recovers tens of
// segments, not one per batch (a thousand under a sanitizer took long enough to time the embedded driver's client out, and their
// fds passed the default soft limit of 1024).
constexpr int64_t kModelRollBytes = 4096;

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
    explicit Run(uint64_t seed) : rng_(seed), rig_(/*follower=*/true, kModelRollBytes) {
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
    explicit KiRun(uint64_t seed) : rng_(seed), rig_(/*follower=*/true, kModelRollBytes) {
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

    // What a raw and a push-read subscription from 0 deliver: the accepted records with their intents.  Never an aborted
    // transaction's: read_uncommitted on a verdict source is served as read_committed (P11-10).
    void checkDelivered_(const CheckedWritesModel& model, const std::set<int64_t>& present) {
        checkDelivered(rig_, model, rig_.subscribe(SubKind::Raw, 0), false, &present);
        checkDelivered(rig_, model, rig_.subscribe(SubKind::PushRead, 0), false, &present);
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


// ---- Checked writes Part 06 Task 12: the same check on an autoincrement topic -----------------------------------------------------
// Two idempotent All | View producers send key-0 batches of 1-8 creates and explicit-id batches (imports below and above the counter,
// update / removeExisting / remove of ids the run knows) at P = 1 or 3 on the partition seed % P, a TestReject{TagEquals{9}} member is
// installed, replaced and dropped, a batch is sent again before and after the judge, after a leadership change and past the window,
// and the leader fails over, is demoted, restarts, loses its tail, compacts and commits or aborts a transaction of key-0 creates.  On top of
// 05's checks the ids are held to NoIdTwiceNoReuse, IdsFollowTheStride, ReservationCoversEveryId, AckedIdsMatchTheLog and, after every
// activation, CounterAboveEveryId.
namespace {
using rune::protocol::client::ErrorCode;
constexpr int kAiSteps = 250;
constexpr int64_t kAiTailPid = 700, kAiTxnPidBase = 900;
constexpr int32_t kAiRejBase = 0x40000800;
constexpr int kRetryWindow = 33;   // batches past the producer's window of 32
constexpr int16_t kDuplicateSequenceCode = static_cast<int16_t>(ErrorCode::DuplicateSequence);

enum class AiAction : uint8_t { Creates, Explicit, RetryBeforeJudge, RetryAfterJudge, RetryLast, RetryAfterWindow, RejectMember, Failover, DemotePromote, Restart, TruncateTail, Compact, Transaction };
const char* nameOf(AiAction a) {
    switch (a) {
    case AiAction::Creates: return "Creates";
    case AiAction::Explicit: return "Explicit";
    case AiAction::RetryBeforeJudge: return "RetryBeforeJudge";
    case AiAction::RetryAfterJudge: return "RetryAfterJudge";
    case AiAction::RetryLast: return "RetryLast";
    case AiAction::RetryAfterWindow: return "RetryAfterWindow";
    case AiAction::RejectMember: return "RejectMember";
    case AiAction::Failover: return "Failover";
    case AiAction::DemotePromote: return "DemotePromote";
    case AiAction::Restart: return "Restart";
    case AiAction::TruncateTail: return "TruncateTail";
    case AiAction::Compact: return "Compact";
    case AiAction::Transaction: return "Transaction";
    }
    return "";
}

struct AiProducer {
    int64_t pid = 0;
    int32_t seq = 0;
    std::vector<Rec> lastRecs;
    int32_t lastSeq = -1;
    std::optional<VerdictRig::Ack> lastAck;
};

AutoIncShape aiShapeOf(uint64_t seed) { return AutoIncShape{.partitions = seed % 2 == 0 ? 3 : 1, .reserve = static_cast<uint32_t>(4 + seed % 13)}; }

class AiRun {
public:
    explicit AiRun(uint64_t seed)
        : rng_(seed), shape_(aiShapeOf(seed)), partition_(static_cast<int32_t>(seed % static_cast<uint64_t>(shape_.partitions))),
          rig_(/*follower=*/true, kModelRollBytes, RigTuning{.partition = partition_}) {
        rig_.createAutoIncTopic(shape_);
        installTag_();
        rig_.installProjection(kProj);
        rig_.installCountById(kCnt);
        rig_.enableAcks();
        rig_.passes(60);
        rig_.followerFetch();
        rig_.passes(10);
        waitReady_();
        idModel_.addAutoIncRule(shape_, partition_);
        for (int p = 0; p < 2; ++p) producers_[p].pid = 500 + p;
    }

    std::string trace;
    std::size_t markersRemoved = 0, tombstonesRemoved = 0;   ///< by the compactions: the 8.5 marker drop and the tombstone horizon ran

    AiAction pick() {
        const unsigned r = static_cast<unsigned>(rng_() % 100);
        if (r < 30) return AiAction::Creates;
        if (r < 50) return AiAction::Explicit;
        if (r < 54) return AiAction::RetryBeforeJudge;
        if (r < 58) return AiAction::RetryAfterJudge;
        if (r < 63) return AiAction::RetryLast;
        if (r < 65) return AiAction::RetryAfterWindow;
        if (r < 71) return AiAction::RejectMember;
        if (r < 76) return AiAction::Failover;
        if (r < 80) return AiAction::DemotePromote;
        if (r < 84) return AiAction::Restart;
        if (r < 88) return AiAction::TruncateTail;
        if (r < 94) return AiAction::Compact;
        return AiAction::Transaction;
    }

    void step(AiAction action) {
        switch (action) {
        case AiAction::Creates: trace += "c"; (void)sendIdem_(producers_[rng_() % 2], createBatch_(1 + rng_() % 8)); break;
        case AiAction::Explicit: trace += "e"; explicit_(); break;
        case AiAction::RetryBeforeJudge: trace += "b"; retry_(false); break;
        case AiAction::RetryAfterJudge: trace += "a"; retry_(true); break;
        case AiAction::RetryLast: trace += "l"; retryLast_(); break;
        case AiAction::RetryAfterWindow: trace += "w"; retryAfterWindow_(); break;
        case AiAction::RejectMember: rejectMember_(); break;
        case AiAction::Failover:
            trace += "F";
            settle_();
            rig_.failover();
            activated_(true);
            break;
        case AiAction::DemotePromote:
            trace += "D";
            settle_();
            rig_.demotePromote();
            activated_(false);
            break;
        case AiAction::Restart:
            trace += "S";
            settle_();
            rig_.restart(true);
            activated_(true);
            break;
        case AiAction::TruncateTail:
            trace += "T";
            settle_();
            (void)rig_.produceFrame(createBatch_(1 + rng_() % 3), kAcksViewAll, kAiTailPid, 0);   // an unacked tail the cut removes, with the ids it took
            rig_.passes(5);
            rig_.truncateTo(rig_.replica->highWatermark());
            rig_.demotePromote();
            activated_(false);
            break;
        case AiAction::Compact: trace += "C"; compact_ = true; break;
        case AiAction::Transaction: transaction_(); break;
        }
        if (!::testing::Test::HasFatalFailure()) verify_();
    }

private:
    static constexpr uint8_t kAcksAll = rune::protocol::client::kAcksWireAll;
    static constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
    struct Pending { VerdictRig::Ack ack; std::size_t n; int64_t corr; };

    uint64_t partitions_() const { return static_cast<uint64_t>(shape_.partitions); }
    uint64_t idOf_(uint64_t n) const { return n * partitions_() + static_cast<uint64_t>(partition_); }
    uint64_t nextN_() { return rig_.autoIncState()->nextN; }

    void waitReady_() {
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            if (rig_.autoIncState() && rig_.worker->rcStatusForTest(VerdictRig::kSrc, partition_) == Worker::kRcReady) return;
            if (i % 8 == 0) rig_.followerFetch();
            rig_.pass();
        }
        FAIL() << "the counter was not ready after " << trace;
    }
    // A leadership (a failover, a restart, a demotion): the counter resumes above everything the log holds before the first write.
    void activated_(bool rebuilt) {
        waitReady_();
        if (::testing::Test::HasFatalFailure()) return;
        absorb_(rig_.readLog(rig_.replica->highWatermark()));
        const AutoIncReady counter = *rig_.autoIncState();
        EXPECT_TRUE(idModel_.counterAboveEveryId(history_(true), counter)) << "CounterAboveEveryId: nextN " << counter.nextN << " reserved " << counter.reservedThroughN;
        if (rebuilt) EXPECT_EQ(counter.nextN, counter.reservedThroughN + 1) << "a rebuilt counter resumes above the reservation";
        (void)sendIdem_(producers_[rng_() % 2], createBatch_(1 + rng_() % 4));   // the first batch after an activation takes its ids from the rebuilt counter
    }

    void installTag_() {
        rejTopic_ = kAiRejBase + (++rejSeq_);
        rig_.installRejectView(rejTopic_, kObjTag, tagRule(9));
    }
    // A dropped or replaced member's change must be passed by a marker before the next restart, and before a member of the same object
    // is installed again: a fresh write is acked and marked past it.
    void settle_() {
        if (!setChanged_) return;
        const auto a = sendIdem_(producers_[0], createBatch_(1));
        ASSERT_TRUE(a) << "no write was acked after a constraint-set change";
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
            const int32_t next = kAiRejBase + (++rejSeq_);
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

    uint8_t tag_() { return rng_() % 5 == 0 ? 9 : static_cast<uint8_t>(1 + rng_() % 2); }
    std::vector<Rec> createBatch_(uint64_t n) {
        std::vector<Rec> recs;
        for (uint64_t i = 0; i < n; ++i) recs.push_back({0, ++vSeq_, tag_(), false, 1});
        return recs;
    }
    // Imports below and above the counter, update / removeExisting / remove of ids the run has been given (or, before any, an import).
    std::vector<Rec> explicitBatch_() {
        std::vector<Rec> recs;
        const uint64_t n = 1 + rng_() % 4;
        for (uint64_t i = 0; i < n; ++i) {
            const unsigned k = static_cast<unsigned>(rng_() % 100);
            if (assigned_.empty() || k < 30) {
                const bool below = !assigned_.empty() && rng_() % 2 == 0;
                const uint64_t id = below ? assigned_[rng_() % assigned_.size()] : idOf_(nextN_() + rng_() % 8);
                recs.push_back({id, ++vSeq_, tag_(), false, 1});
                continue;
            }
            const uint64_t id = assigned_[rng_() % assigned_.size()];
            if (k < 60) recs.push_back({id, ++vSeq_, tag_(), false, 2});
            else if (k < 85) recs.push_back({id, 0, 0, true, 2});
            else recs.push_back({id, 0, 0, true, 0});
        }
        return recs;
    }
    void explicit_() { (void)sendIdem_(producers_[rng_() % 2], explicitBatch_()); }

    // What an OK ack of `recs` tells: a key-0 batch answers its ids, the others answer 0.
    void note_(const VerdictRig::Ack& ack, const std::vector<Rec>& recs, int64_t corr) {
        pending_.push_back({ack, recs.size(), corr});
        const bool keyZero = recs.front().id == 0;
        EXPECT_EQ(ack.assignedBase != 0, keyZero) << "only a key-0 batch answers its ids, corr " << corr;
        if (!keyZero) return;
        keyZero_[ack.baseOffset] = {ack.baseOffset, ack.assignedBase, recs.size()};
        for (std::size_t i = 0; i < recs.size(); ++i) assigned_.push_back(ack.assignedBase + i * partitions_());
    }

    // NotLeader until the counter is active; nothing was appended then, so the same sequence is sent again.
    std::optional<VerdictRig::Ack> sendIdem_(AiProducer& p, const std::vector<Rec>& recs) {
        waitReady_();
        if (::testing::Test::HasFatalFailure()) return std::nullopt;
        for (int tries = 0; tries < 100; ++tries) {
            const int64_t c = rig_.produceFrame(recs, kAcksViewAll, p.pid, p.seq);
            const auto a = rig_.ackFetching(c, 60000);
            if (!a) { ADD_FAILURE() << "the produce was never answered, corr " << c; return std::nullopt; }
            if (a->code == static_cast<int16_t>(ErrorCode::NotLeader)) { rig_.passes(5); continue; }
            if (a->code != 0) { ADD_FAILURE() << "ack code " << a->code << " corr " << c; return std::nullopt; }
            p.lastRecs = recs;
            p.lastSeq = p.seq;
            p.lastAck = *a;
            p.seq += static_cast<int32_t>(recs.size());
            note_(*a, recs, c);
            return a;
        }
        ADD_FAILURE() << "NotLeader for 100 sends in a row";
        return std::nullopt;
    }

    // The same batch is sent twice with one pid and sequence: before the judge caught up, or after.  The answer is the original's, ids included.
    void retry_(bool judgeFirst) {
        waitReady_();
        if (::testing::Test::HasFatalFailure()) return;
        AiProducer& p = producers_[rng_() % 2];
        const auto recs = rng_() % 4 == 0 ? explicitBatch_() : createBatch_(1 + rng_() % 8);
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
        p.lastRecs = recs;
        p.lastSeq = seq;
        p.lastAck = *orig;
        p.seq += static_cast<int32_t>(recs.size());
        note_(*orig, recs, origCorr);
        checkSameAck_(*orig, *dup);
    }
    // The producer's last acked batch again, however many faults ago: its ack comes from the rebuilt window.
    void retryLast_() {
        AiProducer& p = producers_[rng_() % 2];
        if (!p.lastAck) return;
        waitReady_();
        if (::testing::Test::HasFatalFailure()) return;
        const auto dup = rig_.ackFetching(rig_.produceFrame(p.lastRecs, kAcksViewAll, p.pid, p.lastSeq), 60000);
        ASSERT_TRUE(dup) << "the send was never answered";
        checkSameAck_(*p.lastAck, *dup);
    }
    void checkSameAck_(const VerdictRig::Ack& orig, const VerdictRig::Ack& dup) {
        EXPECT_EQ(dup.code, 0);
        EXPECT_EQ(dup.baseOffset, orig.baseOffset);
        EXPECT_EQ(dup.assignedBase, orig.assignedBase) << "a retry in the window answers the original's ids";
        EXPECT_EQ(dup.verdicts, orig.verdicts) << "a retry in the window replays the original verdicts";
        EXPECT_EQ(dup.frameLen, orig.frameLen);
    }
    // 33 more batches push the first out of the producer's window: its retry meets DuplicateSequence and carries no ids.
    void retryAfterWindow_() {
        AiProducer& p = producers_[rng_() % 2];
        const auto recs = createBatch_(1 + rng_() % 3);
        const int32_t seq = p.seq;
        if (!sendIdem_(p, recs)) return;
        for (int i = 0; i < kRetryWindow && !::testing::Test::HasFatalFailure(); ++i) (void)sendIdem_(p, createBatch_(1));
        if (::testing::Test::HasFatalFailure()) return;
        ASSERT_TRUE(judged_());
        rig_.settle(150);
        const auto dup = rig_.ackFetching(rig_.produceFrame(recs, kAcksViewAll, p.pid, seq), 60000);
        ASSERT_TRUE(dup) << "the send was never answered";
        EXPECT_EQ(dup->code, kDuplicateSequenceCode);
        EXPECT_EQ(dup->assignedBase, 0u) << "an answer outside the window carries no ids";
        EXPECT_FALSE(dup->hasVerdicts);
    }
    // An open transaction of key-0 creates, committed or aborted; the judge is held behind it.  A source with a TestReject member refuses
    // transactions (InvalidRequest), so the member is dropped and its change logged first.
    void transaction_() {
        if (rejTopic_ != 0) {
            rig_.dropRejectView(rejTopic_);
            rejTopic_ = 0;
            setChanged_ = true;
        }
        settle_();
        if (::testing::Test::HasFatalFailure()) return;
        const int64_t pid = kAiTxnPidBase + ++txns_;
        const auto recs = createBatch_(1 + rng_() % 3);
        const auto a = rig_.ackFetching(rig_.produceFrame(recs, kAcksAll, pid, 0, /*txn=*/true), 60000);
        ASSERT_TRUE(a) << "the transactional produce was never answered";
        ASSERT_EQ(a->code, 0);
        keyZero_[a->baseOffset] = {a->baseOffset, a->assignedBase, recs.size()};
        for (std::size_t i = 0; i < recs.size(); ++i) assigned_.push_back(a->assignedBase + i * partitions_());
        const bool commit = rng_() % 2 == 0;
        trace += commit ? "X" : "Z";
        rig_.marker(pid, commit);
        rig_.followerFetch();   // a failover or a cut must not lose the marker: the transaction would be open again
        rig_.passes(4);
        if (!commit)
            for (std::size_t i = 0; i < recs.size(); ++i) aborted_.insert(a->baseOffset + static_cast<int64_t>(i));
    }

    // Everything committed that was ever read: a compaction may remove records from the log later, the checks still judge them.
    void absorb_(const VerdictRig::RigLog& log) {
        for (const auto& d : log.data) data_[d.offset] = d;
        for (const auto& ch : log.changes) changes_[ch.offset] = ch;
        for (const auto& [off, through] : log.reservations) reservations_[off] = through;
    }
    VerdictRig::RigLog history_(bool withAborted) const {
        VerdictRig::RigLog out;
        for (const auto& kv : data_)
            if (withAborted || aborted_.count(kv.first) == 0) out.data.push_back(kv.second);
        for (const auto& kv : changes_) out.changes.push_back(kv.second);
        for (const auto& kv : reservations_) out.reservations.emplace_back(kv.first, kv.second);
        return out;
    }

    // What a raw and a push-read subscription from 0 deliver: the accepted records with their intents.  Never an aborted
    // transaction's: read_uncommitted on a verdict source is served as read_committed (P11-10).
    void checkDelivered_(const CheckedWritesModel& model, const std::set<int64_t>& present) {
        checkDelivered(rig_, model, rig_.subscribe(SubKind::Raw, 0), false, &present);
        checkDelivered(rig_, model, rig_.subscribe(SubKind::PushRead, 0), false, &present);
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

    // The ids: NoIdTwiceNoReuse, IdsFollowTheStride, ReservationCoversEveryId and AckedIdsMatchTheLog over everything committed, and the side
    // window and the assigned ids of the live worker.
    void checkIds_() {
        const VerdictRig::RigLog full = history_(true);
        std::vector<CheckedWritesModel::AutoIncAck> acks;
        for (const auto& [offset, a] : keyZero_) acks.push_back(a);
        EXPECT_TRUE(idModel_.noIdTwiceNoReuse(full, acks)) << "NoIdTwiceNoReuse";
        EXPECT_TRUE(idModel_.idsFollowTheStride(full, acks)) << "IdsFollowTheStride";
        EXPECT_TRUE(idModel_.reservationCoversEveryId(full)) << "ReservationCoversEveryId";
        EXPECT_TRUE(idModel_.ackedIdsMatchTheLog(full, acks)) << "AckedIdsMatchTheLog";
        EXPECT_EQ(rig_.autoIncStats().windowMisses, 0u) << "AckedIdsMatchTheLog: a retry missed the side window";
        EXPECT_EQ(rig_.autoIncStats().assignedKeyExists, 0u) << "NoIdTwiceNoReuse: an assigned id met a stored key";
    }

    void verify_() {
        ASSERT_TRUE(judged_()) << " after " << trace;
        const int64_t hw = rig_.replica->highWatermark();
        const VerdictRig::RigLog log = rig_.readLog(hw);
        absorb_(log);
        const VerdictRig::RigLog all = history_(false);
        CheckedWritesModel model;
        model.declareRule(kObjTag, tagRule(9).fingerprint(), CheckedWritesModel::tagEquals(9));
        model.addAutoIncRule(shape_, partition_);
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
        checkIds_();
        checkViews(rig_, model, all, kProj, kCnt);
        checkDelivered_(model, present);
        if (compact_) {
            compact_ = false;
            compacted_ = true;
            const auto reservedBefore = rig_.reservations();
            checkCompaction(rig_, model, log, 1'000'000 + 2'000 * ++compactions_);   // past the last pass's horizon (retention 1 s)
            const auto reservedAfter = rig_.reservations();
            const auto after = rig_.readLog();
            std::set<int64_t> kept;
            for (const auto& d : after.data) kept.insert(d.offset);
            for (const auto& m : after.markers) kept.insert(m.offset);
            for (const auto& m : log.markers) markersRemoved += kept.count(m.offset) == 0 ? 1 : 0;
            for (const auto& d : log.data) tombstonesRemoved += d.del && kept.count(d.offset) == 0 ? 1 : 0;
            if (!reservedBefore.empty()) {
                ASSERT_FALSE(reservedAfter.empty()) << "the compaction removed every reservation";
                EXPECT_EQ(reservedAfter.back().second, reservedBefore.back().second) << "the latest reservation is kept";
            }
        }
        trace += "[" + std::to_string(model.acceptedRecords().size()) + "/" + std::to_string(model.verdicts().size()) + "]";
    }

    std::mt19937_64 rng_;
    AutoIncShape shape_;
    int32_t partition_;
    VerdictRig rig_;
    CheckedWritesModel idModel_;
    AiProducer producers_[2];
    std::vector<Pending> pending_;
    std::vector<uint64_t> assigned_;
    std::map<int64_t, CheckedWritesModel::AutoIncAck> keyZero_;
    std::map<int64_t, VerdictRig::RigLogRec> data_;
    std::map<int64_t, VerdictRig::RigChange> changes_;
    std::map<int64_t, uint64_t> reservations_;
    std::map<int64_t, ModelVerdict> reported_;
    std::set<int64_t> acceptedByAck_, aborted_;
    int32_t rejTopic_ = 0, rejSeq_ = 0;
    int64_t vSeq_ = 0, txns_ = 0;
    bool compact_ = false, compacted_ = false, setChanged_ = false;
    int64_t compactions_ = 0;
};

std::string runAiSeed(uint64_t seed, std::size_t* markersRemoved = nullptr, std::size_t* tombstonesRemoved = nullptr) {
    AiRun run(seed);
    for (int step = 0; step < kAiSteps && !::testing::Test::HasFatalFailure(); ++step) {
        const AiAction action = run.pick();
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step) + " action=" + nameOf(action) + " trace=" + run.trace);
        run.step(action);
    }
    if (markersRemoved != nullptr) *markersRemoved = run.markersRemoved;
    if (tombstonesRemoved != nullptr) *tombstonesRemoved = run.tombstonesRemoved;
    return run.trace;
}
}  // namespace

class CheckedWritesAutoIncModelRegression : public ::testing::TestWithParam<uint64_t> {};

TEST_P(CheckedWritesAutoIncModelRegression, SeededRigMatchesTheModel) {
    std::size_t markers = 0, tombstones = 0;
    const std::string trace = runAiSeed(GetParam(), &markers, &tombstones);
    EXPECT_FALSE(trace.empty());
    EXPECT_GT(markers, 0u) << "no compaction removed a marker (8.5)";
    EXPECT_GT(tombstones, 0u) << "no compaction removed a tombstone";
}

// The history is a function of the seed: seeds 1-8 run twice make the same choices, the same ids and the same verdicts.
TEST_P(CheckedWritesAutoIncModelRegression, SameSeedSameTrace) {
    if (GetParam() > 8) GTEST_SKIP() << "seeds 1-8 only";
    const std::string first = runAiSeed(GetParam());
    const std::string second = runAiSeed(GetParam());
    EXPECT_EQ(first, second);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesAutoIncModelRegression, ::testing::Range<uint64_t>(1, 101));


// ---- Checked writes Part 09 Task 9: the same check with view constraints (Interaction matrix C) ------------------------------------
// The rig's character topic (autoincrement on odd seeds, key.index only on even ones) with character_name (unique lower(name), object 7) and account_stats (count(*) group by userId
// check characters <= cap, object 8).  Before the views exist users 1 and 2 hold "Foo" / "foo" and user 3 holds cap + 1 characters.  Two
// idempotent View producers create for users 1-3 with names that collide under lower() (U+023A too), rename, move, remove and removeExisting;
// a v11 session writes plain-acks upserts and tombstones; the leader fails over, is demoted, restarts (twice, the second while the views
// rebuild), loses its tail and compacts; account_stats is replaced (cap <-> cap + 1) and character_name dropped and created again.  After every
// step 04's checks hold against the model, the views equal the model's live set, and AtMostCapLiveCharacters / LowerNameUnique hold over the
// broker's own verdicts.
namespace {
constexpr int kCwSteps = 250;
constexpr int64_t kCwTailPid = 760;
constexpr uint8_t kCwLeader = 1;
constexpr uint64_t kCwObjName = 7, kCwObjStats = 8;
const char* const kCwNames[] = {"Foo", "foo", "FOO", "Bar", "bar", "Baz", "\xC8\xBA" "x", "\xE2\xB1\xA5" "x"};

struct CwParam { uint64_t seed; int64_t cap; };

enum class CwAction : uint8_t { Creates, Rename, Move, Remove, RemoveExisting, Plain, Failover, DemotePromote, Restart, RestartDuringRebuild, TruncateTail, Compact, ReplaceStats, DropCreateName };
const char* nameOf(CwAction a) {
    switch (a) {
    case CwAction::Creates: return "Creates";
    case CwAction::Rename: return "Rename";
    case CwAction::Move: return "Move";
    case CwAction::Remove: return "Remove";
    case CwAction::RemoveExisting: return "RemoveExisting";
    case CwAction::Plain: return "Plain";
    case CwAction::Failover: return "Failover";
    case CwAction::DemotePromote: return "DemotePromote";
    case CwAction::Restart: return "Restart";
    case CwAction::RestartDuringRebuild: return "RestartDuringRebuild";
    case CwAction::TruncateTail: return "TruncateTail";
    case CwAction::Compact: return "Compact";
    case CwAction::ReplaceStats: return "ReplaceStats";
    case CwAction::DropCreateName: return "DropCreateName";
    }
    return "";
}

class CwRun {
public:
    using CV = ConstrainedView;
    using Char = VerdictRig::Char;

    explicit CwRun(CwParam p) : rng_(p.seed), cap_(p.cap), statsCap_(p.cap), autoInc_(p.seed % 2 == 1), rig_(/*follower=*/true, kModelRollBytes) {
        rig_.worker->setVerdictIntervalNs(1'000'000);
        rig_.createCharacterTopic(autoInc_);
        rig_.enableAcks();
        rig_.passes(20);
        rig_.followerFetch();
        waitReady_();
        for (int i = 0; i < 2; ++i) producers_[i].pid = 600 + i;
        std::vector<Char> before{create_(1, "Foo"), create_(2, "foo")};
        for (int64_t i = 0; i <= cap_; ++i) before.push_back(create_(3, "p" + std::to_string(i)));
        (void)sendIdem_(producers_[0], before);
        rig_.createConstrainedView(CV::CharacterName);
        rig_.createConstrainedView(CV::AccountStats, cap_);
        statsCaps_[rig_.fingerprintOf(CV::AccountStats, false)] = cap_;
        rig_.passes(20);
        if (!::testing::Test::HasFatalFailure()) verify_();
    }

    std::string trace;

    CwAction pick() {
        const unsigned r = static_cast<unsigned>(rng_() % 100);
        if (r < 26) return CwAction::Creates;
        if (r < 38) return CwAction::Rename;
        if (r < 46) return CwAction::Move;
        if (r < 51) return CwAction::Remove;
        if (r < 56) return CwAction::RemoveExisting;
        if (r < 62) return CwAction::Plain;
        if (r < 67) return CwAction::Failover;
        if (r < 71) return CwAction::DemotePromote;
        if (r < 75) return CwAction::Restart;
        if (r < 78) return CwAction::RestartDuringRebuild;
        if (r < 82) return CwAction::TruncateTail;
        if (r < 88) return CwAction::Compact;
        if (r < 94) return CwAction::ReplaceStats;
        return CwAction::DropCreateName;
    }

    void step(CwAction action) {
        switch (action) {
        case CwAction::Creates: trace += "c"; write_(creates_(1 + rng_() % 3)); break;
        case CwAction::Rename: trace += "r"; write_(updates_(1 + rng_() % 2, false, 2)); break;
        case CwAction::Move: trace += "m"; write_(updates_(1, true, 2)); break;
        case CwAction::Remove: trace += "x"; write_(tombstones_(1 + rng_() % 2, 0)); break;
        case CwAction::RemoveExisting: trace += "e"; write_(tombstones_(1 + rng_() % 2, 2)); break;
        case CwAction::Plain: trace += "p"; plain_(); break;
        case CwAction::Failover: trace += "F"; settle_(); rig_.failover(); waitReady_(); break;
        case CwAction::DemotePromote: trace += "D"; settle_(); rig_.demotePromote(); waitReady_(); break;
        case CwAction::Restart: trace += "S"; settle_(); rig_.restart(true); waitReady_(); break;
        case CwAction::RestartDuringRebuild:
            trace += "R";
            settle_();
            rig_.restart(true);
            rig_.passes(1 + static_cast<int>(rng_() % 4));   // the views refold the log
            rig_.restart(true);
            waitReady_();
            break;
        case CwAction::TruncateTail: {
            trace += "T";
            settle_();
            const std::vector<Char> tail = creates_(1 + rng_() % 3);
            (void)rig_.appendCharacters(tail, kAllView, kCwTailPid, 0);   // an unacked tail the cut removes
            rig_.passes(5);
            rig_.truncateTo(rig_.replica->highWatermark());
            rig_.demotePromote();
            waitReady_();
            break;
        }
        case CwAction::Compact: trace += "C"; compact_ = true; break;
        case CwAction::ReplaceStats: replaceStats_(); break;
        case CwAction::DropCreateName: dropCreateName_(); break;
        }
        if (!::testing::Test::HasFatalFailure()) verify_();
    }

private:
    static constexpr uint8_t kAllView = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;
    struct CwProducer { int64_t pid = 0; int32_t seq = 0; };
    struct Pending { VerdictRig::Ack ack; std::size_t n; int64_t corr; };
    struct LiveChar { uint64_t id; uint64_t userId; std::string name; };

    // A key-0 create on the autoincrement topic, an insert of a fresh id on the other.
    Char create_(uint64_t user, const std::string& name) { return Char{.id = autoInc_ ? 0 : ++lastId_, .userId = user, .name = name, .intent = 1}; }
    std::string name_() { return kCwNames[rng_() % std::size(kCwNames)]; }
    uint64_t user_() { return 1 + rng_() % 3; }

    std::vector<Char> creates_(uint64_t n) {
        std::vector<Char> out;
        for (uint64_t i = 0; i < n; ++i) out.push_back(create_(user_(), name_()));
        return out;
    }
    // Renames (`move` false) or moves of live characters; with none live, a create.
    std::vector<Char> updates_(uint64_t n, bool move, uint8_t intent) {
        std::vector<Char> out;
        for (uint64_t i = 0; i < n; ++i) {
            if (live_.empty()) { out.push_back(create_(user_(), name_())); continue; }
            const LiveChar& c = live_[rng_() % live_.size()];
            out.push_back(Char{.id = c.id, .userId = move ? user_() : c.userId, .name = move ? c.name : name_(), .intent = intent});
        }
        return out;
    }
    // Tombstones of live characters, or (one in four) of an id that was ever written: it may be gone already.
    std::vector<Char> tombstones_(uint64_t n, uint8_t intent) {
        std::vector<Char> out;
        for (uint64_t i = 0; i < n; ++i) {
            uint64_t id = 0;
            if (!live_.empty() && rng_() % 4 != 0) id = live_[rng_() % live_.size()].id;
            else if (!ids_.empty()) id = *std::next(ids_.begin(), static_cast<std::ptrdiff_t>(rng_() % ids_.size()));
            if (id == 0) return creates_(1);
            out.push_back(Char{.id = id, .intent = intent, .tombstone = true});
        }
        return out;
    }
    void write_(const std::vector<Char>& chars) { (void)sendIdem_(producers_[rng_() % 2], chars); }

    void waitReady_() {
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            if ((!autoInc_ || rig_.autoIncState()) && rig_.worker->rcStatusForTest(VerdictRig::kSrc, rig_.partition()) == Worker::kRcReady) return;
            if (i % 8 == 0) rig_.followerFetch();
            rig_.pass();
        }
        FAIL() << "the partition was not ready after " << trace;
    }
    // NotLeader until the counter is active; nothing was appended then, so the same sequence is sent again.
    std::optional<VerdictRig::Ack> sendIdem_(CwProducer& p, const std::vector<Char>& chars) {
        waitReady_();
        if (::testing::Test::HasFatalFailure()) return std::nullopt;
        for (int tries = 0; tries < 100; ++tries) {
            const int64_t c = rig_.appendCharacters(chars, kAllView, p.pid, p.seq);
            const auto a = rig_.ackFetching(c, 60000);
            if (!a) { ADD_FAILURE() << "the produce was never answered, corr " << c; return std::nullopt; }
            if (a->code == static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader)) { rig_.passes(5); continue; }
            if (a->code != 0) { ADD_FAILURE() << "ack code " << a->code << " corr " << c; return std::nullopt; }
            p.seq += static_cast<int32_t>(chars.size());
            pending_.push_back({*a, chars.size(), c});
            return a;
        }
        ADD_FAILURE() << "NotLeader for 100 sends in a row";
        return std::nullopt;
    }
    // A v11 session, acks 1, no producer id: upserts (renames, moves) and plain tombstones, judged like any write; an autoincrement topic
    // refuses a plain upsert, so there only tombstones.  Its acks carry no trailer.
    void plain_() {
        std::vector<Char> chars = autoInc_ || rng_() % 3 == 0 ? tombstones_(1, 0) : updates_(1 + rng_() % 2, rng_() % 2 == 0, 0);
        if (chars.front().id == 0) return;   // nothing written yet: a key-0 create needs a View ack
        for (Char& c : chars) c.intent = 0;
        waitReady_();
        if (::testing::Test::HasFatalFailure()) return;
        rig_.enableAcks(16);
        for (int tries = 0; tries < 100; ++tries) {
            const auto a = rig_.ackFetching(rig_.appendCharacters(chars, kCwLeader), 60000);
            ASSERT_TRUE(a) << "the plain produce was never answered";
            if (a->code == static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader)) { rig_.passes(5); continue; }
            EXPECT_EQ(a->code, 0);
            EXPECT_FALSE(a->hasVerdicts) << "a v11 session's acks carry no verdicts";
            break;
        }
        rig_.enableAcks(17);
    }

    // account_stats under its other cap; the swap at S writes the change record.  The model learns the new plan's cap by fingerprint.
    void replaceStats_() {
        trace += "A";
        settle_();
        if (::testing::Test::HasFatalFailure()) return;
        statsCap_ = statsCap_ == cap_ ? cap_ + 1 : cap_;
        rig_.replaceConstrainedView(CV::AccountStats, statsCap_);
        swapFp_ = rig_.fingerprintOf(CV::AccountStats, false);
        statsCaps_[swapFp_] = statsCap_;
        setChanged_ = true;
    }
    void dropCreateName_() {
        trace += "N";
        if (rig_.constrained.count(CV::CharacterName) != 0) {
            settle_();
            if (::testing::Test::HasFatalFailure()) return;
            rig_.dropConstrainedView(CV::CharacterName);
            setChanged_ = true;
            return;
        }
        settle_();
        if (::testing::Test::HasFatalFailure()) return;
        rig_.createConstrainedView(CV::CharacterName);
        setChanged_ = true;
    }
    // A constraint-set change must be in the log and passed by a marker before a fault or the next change of the set: the replace's
    // candidate swaps first, then a fresh write is acked and marked past the change.
    void settle_() {
        if (!setChanged_) return;
        if (swapFp_ != 0) {
            bool swapped = false;
            for (int i = 0; i < VerdictRig::kMaxPass && !swapped; ++i) {
                rig_.pass();
                if (i % 8 != 0) continue;
                rig_.followerFetch();
                const auto changes = rig_.readLog().changes;
                for (const auto& m : changes.empty() ? std::vector<rune::storage::control_record::ChangeMember>{} : changes.back().members)
                    swapped = swapped || (m.viewObjectId == kCwObjStats && m.planFingerprint == swapFp_);
            }
            ASSERT_TRUE(swapped) << "the replacement of account_stats never swapped in";
            swapFp_ = 0;
        }
        ASSERT_TRUE(sendIdem_(producers_[0], creates_(1))) << "no write was acked after a constraint-set change";
        setChanged_ = false;
        if (judged_()) rig_.passes(4);   // the retired member leaves on the pass after the marker commits
    }

    // One past the last data record: a trailing change record needs no marker.
    static int64_t dataEnd_(const VerdictRig::RigLog& log) { return log.data.empty() ? 0 : log.data.back().offset + 1; }
    bool judged_() { return rig_.driveUntilJudged(dataEnd_(rig_.readLog())); }

    void absorb_(const VerdictRig::RigLog& log, int64_t hw) {
        for (const auto& d : log.data) { data_[d.offset] = d; ids_.insert(d.id); }
        for (const auto& ch : log.changes) changes_[ch.offset] = ch;
        rig_.writeState()->rejected.forEachIn(0, INT64_MAX, [&](const rune::storage::RejectedOffsetIndex::Entry& e) { if (e.offset < hw) rejected_[e.offset] = {e.code, e.constraintId}; });
    }
    VerdictRig::RigLog history_() const {
        VerdictRig::RigLog out;
        for (const auto& kv : data_) out.data.push_back(kv.second);
        for (const auto& kv : changes_) out.changes.push_back(kv.second);
        return out;
    }

    // AtMostCapLiveCharacters and LowerNameUnique over every committed record with the broker's verdicts (the index, accumulated): under
    // account_stats an accepted write that raises a user's count leaves it at most the cap of the plan that judged it, and a move leaves its
    // old group empty or within that cap; under character_name an accepted live write claims no lowered name another live character holds.
    // So a user over the cap was over before the plan judged it, and every extra of a name was written while no unique member was active.
    void checkConstraintInvariants_(const VerdictRig::RigLog& all) {
        std::map<uint64_t, uint64_t> members;   // object id -> plan fingerprint
        std::map<uint64_t, std::pair<uint64_t, std::string>> live;   // id -> (userId, lowered name)
        std::map<uint64_t, int64_t> count;
        std::size_t next = 0;
        for (const auto& rec : all.data) {
            while (next < all.changes.size() && all.changes[next].offset < rec.offset) {
                members.clear();
                for (const auto& m : all.changes[next].members) members[m.viewObjectId] = m.planFingerprint;
                ++next;
            }
            if (rejected_.count(rec.offset) != 0) continue;
            const auto old = live.find(rec.id);
            if (rec.del) {
                if (old != live.end()) { --count[old->second.first]; live.erase(old); }
                continue;
            }
            const auto user = static_cast<uint64_t>(rec.v);
            std::array<uint8_t, 64> folded{};
            const auto low = rune::codec::lowerUtf8({reinterpret_cast<const uint8_t*>(rec.name.data()), rec.name.size()}, folded);
            const std::string name(reinterpret_cast<const char*>(folded.data()), low.len);
            if (const auto s = members.find(kCwObjStats); s != members.end() && (old == live.end() || old->second.first != user)) {
                const int64_t c = statsCaps_.at(s->second);
                EXPECT_LE(count[user] + 1, c) << "AtMostCapLiveCharacters: the write at " << rec.offset << " took user " << user << " past the cap " << c;
                if (old != live.end()) {
                    const int64_t left = count[old->second.first] - 1;
                    EXPECT_TRUE(left == 0 || left <= c) << "AtMostCapLiveCharacters: the move at " << rec.offset << " left user " << old->second.first << " at " << left;
                }
            }
            if (members.count(kCwObjName) != 0 && (old == live.end() || old->second.second != name))
                for (const auto& [id, held] : live)
                    EXPECT_FALSE(id != rec.id && held.second == name) << "LowerNameUnique: the write at " << rec.offset << " claimed '" << name << "' held by " << id;
            if (old != live.end()) --count[old->second.first];
            ++count[user];
            live[rec.id] = {user, name};
        }
    }

    // ViewEqualsFreshRefold: character_name holds one row per lowered name, owned by its smallest claimant, the gauge counts the others, and
    // account_stats holds every user's live count.
    void checkViews_(const CheckedWritesModel& model) {
        const auto rowColumn = [&](CV v, const uint8_t* row, const char* col) -> int64_t {
            if (row == nullptr) return -1;
            const auto& comp = rig_.constrained.at(v).comp;
            for (std::size_t i = 0; i < comp.outputDescriptor.fields.size(); ++i)
                if (comp.outputDescriptor.fields[i].name == col) {
                    const auto f = rune::view::readSlot(row, comp.rowLayout, static_cast<uint16_t>(i));
                    return f.type == rune::codec::PrimType::Int64 ? f.i : static_cast<int64_t>(f.u);
                }
            ADD_FAILURE() << "no column " << col;
            return -1;
        };
        if (rig_.constrained.count(CV::CharacterName) != 0) {
            const ViewInstance* names = rig_.constrainedInstance(CV::CharacterName);
            ASSERT_NE(names, nullptr);
            const auto owners = model.uniqueOwners();
            EXPECT_EQ(names->table.size(), owners.size()) << "ViewEqualsFreshRefold: character_name rows";
            for (const auto& [lowered, id] : owners) {
                std::vector<uint8_t> key(24, 0);
                std::copy(lowered.begin(), lowered.end(), key.begin());
                EXPECT_EQ(rowColumn(CV::CharacterName, names->table.get(rune::view::ViewKey::fromBytes(key)), "characterId"), static_cast<int64_t>(id))
                    << "ViewEqualsFreshRefold: the owner of '" << lowered << "'";
            }
            EXPECT_EQ(rig_.uniqueConflicts(CV::CharacterName), model.uniqueExtras()) << "ViewEqualsFreshRefold: the uniqueConflicts gauge";
        }
        const ViewInstance* stats = rig_.constrainedInstance(CV::AccountStats);
        ASSERT_NE(stats, nullptr);
        const auto counts = model.liveCountByUser();
        EXPECT_EQ(stats->table.size(), counts.size()) << "ViewEqualsFreshRefold: account_stats rows";
        for (const auto& [user, n] : counts)
            EXPECT_EQ(rowColumn(CV::AccountStats, stats->table.get(user), "characters"), n) << "ViewEqualsFreshRefold: user " << user << "'s characters";
    }

    // What a raw and a push-read subscription from 0 deliver: the accepted records still in the log, with their intents.
    void checkDelivered_(const CheckedWritesModel& model, const std::set<int64_t>& present) {
        std::vector<std::tuple<int64_t, uint64_t, uint8_t>> want;
        for (const auto& a : model.acceptedRecords())
            if (present.count(a.offset) != 0) want.emplace_back(a.offset, a.id, a.intent);
        for (const SubKind kind : {SubKind::Raw, SubKind::PushRead}) {
            for (int i = 0; i < VerdictRig::kMaxPass && rig_.worker->rcStatusForTest(VerdictRig::kSrc, rig_.partition()) != Worker::kRcReady; ++i) rig_.pass();
            std::vector<std::tuple<int64_t, uint64_t, uint8_t>> got;
            for (const auto& d : rig_.collect(rig_.subscribe(kind, 0)).records) got.emplace_back(d.offset, d.id, d.intent);
            EXPECT_EQ(got, want) << "(c) the delivered records are the accepted ones, with their intents";
        }
    }

    // A view created since the last fetch folds nothing until the follower fetched its change record: W stays below it.
    void foldViews_(int64_t target) {
        const auto folded = [&] {
            for (const auto& [v, l] : rig_.constrained) {
                const auto st = rig_.vs->instanceServeState(l.topicId, rig_.partition());
                if (!st.hosted || !st.ready || st.foldCursor < target) return false;
            }
            return true;
        };
        for (int i = 0; i < VerdictRig::kMaxPass; ++i) {
            if (folded()) return;
            if (i % 8 == 0) rig_.followerFetch();
            rig_.pass();
        }
        ADD_FAILURE() << "the constrained views never folded through " << target;
    }

    void verify_() {
        ASSERT_TRUE(judged_()) << " after " << trace;
        const int64_t hw = rig_.replica->highWatermark();
        const VerdictRig::RigLog log = rig_.readLog(hw);
        absorb_(log, hw);
        const VerdictRig::RigLog all = history_();
        for (const auto& ch : all.changes)
            for (const auto& m : ch.members)
                ASSERT_TRUE(m.viewObjectId != kCwObjStats || statsCaps_.count(m.planFingerprint) != 0) << "a change names an account_stats plan the run never authored, at " << ch.offset;
        CheckedWritesModel model;
        model.addAutoIncRule(AutoIncShape{}, 0);
        model.addLowerNameUniqueRule(kCwObjName);
        model.addCountCheckRule(kCwObjStats, cap_);
        for (const auto& [fp, c] : statsCaps_) model.declareCountCap(kCwObjStats, fp, c);
        model.reset(all);
        std::set<int64_t> present;
        for (const auto& d : log.data) present.insert(d.offset);

        foldViews_(dataEnd_(log));
        checkIndex(rig_, model, &present);
        if (!log.data.empty() && !log.markers.empty() && !log.changes.empty()) checkMarkers(model, log, dataEnd_(log), compacted_);
        for (const Pending& p : pending_) checkModelTrailer(model, p.ack, p.n, p.corr);
        pending_.clear();
        checkViews_(model);
        checkConstraintInvariants_(all);
        checkDelivered_(model, present);
        if (compact_) {
            compact_ = false;
            compacted_ = true;
            checkCompaction(rig_, model, log, 1'000'000 + 2'000 * ++compactions_);
        }
        live_.clear();
        for (const auto& [id, r] : model.latestAcceptedByKey())
            if (!r.del) live_.push_back({id, static_cast<uint64_t>(r.v), r.name});
        trace += "[" + std::to_string(model.acceptedRecords().size()) + "/" + std::to_string(model.verdicts().size()) + "]";
    }

    std::mt19937_64 rng_;
    int64_t cap_, statsCap_ = 0;
    bool autoInc_;   ///< odd seeds: the autoincrement topic (key-0 creates); even seeds: key.index only (inserts of fresh ids, plain upserts)
    VerdictRig rig_;
    CwProducer producers_[2];
    std::vector<Pending> pending_;
    std::vector<LiveChar> live_;
    std::set<uint64_t> ids_;
    std::map<uint64_t, int64_t> statsCaps_;   ///< account_stats plan fingerprint -> its cap
    std::map<int64_t, VerdictRig::RigLogRec> data_;
    std::map<int64_t, VerdictRig::RigChange> changes_;
    std::map<int64_t, ModelVerdict> rejected_;   ///< every verdict the broker's index ever held, by offset
    uint64_t swapFp_ = 0, lastId_ = 0;
    bool compact_ = false, compacted_ = false, setChanged_ = false;
    int64_t compactions_ = 0;
};

std::string runCwSeed(CwParam p) {
    CwRun run(p);
    for (int step = 0; step < kCwSteps && !::testing::Test::HasFatalFailure(); ++step) {
        const CwAction action = run.pick();
        SCOPED_TRACE("seed=" + std::to_string(p.seed) + " cap=" + std::to_string(p.cap) + " step=" + std::to_string(step) + " action=" + nameOf(action) + " trace=" + run.trace);
        run.step(action);
    }
    return run.trace;
}
}  // namespace

// A suite of its own: CheckedWritesModelRegression is 04's TEST_P over plain seeds, and a constraint run is a (seed, cap) pair.
class CheckedWritesConstraintModelRegression : public ::testing::TestWithParam<CwParam> {};

TEST_P(CheckedWritesConstraintModelRegression, SeededConstraintsMatchTheModel) {
    EXPECT_FALSE(runCwSeed(GetParam()).empty());
}

// The history is a function of the seed: seeds 1-8 run twice make the same choices and get the same verdicts.
TEST_P(CheckedWritesConstraintModelRegression, SameSeedSameTrace) {
    if (GetParam().seed > 8) GTEST_SKIP() << "seeds 1-8 only";
    const std::string first = runCwSeed(GetParam());
    const std::string second = runCwSeed(GetParam());
    EXPECT_EQ(first, second);
}

namespace {
std::vector<CwParam> cwParams(int64_t cap, uint64_t seeds) {
    std::vector<CwParam> out;
    for (uint64_t s = 1; s <= seeds; ++s) out.push_back({s, cap});
    return out;
}
std::string cwName(const ::testing::TestParamInfo<CwParam>& i) { return "cap" + std::to_string(i.param.cap) + "_seed" + std::to_string(i.param.seed); }
}  // namespace

INSTANTIATE_TEST_SUITE_P(Cap3, CheckedWritesConstraintModelRegression, ::testing::ValuesIn(cwParams(3, 200)), cwName);
INSTANTIATE_TEST_SUITE_P(Cap12, CheckedWritesConstraintModelRegression, ::testing::ValuesIn(cwParams(12, 20)), cwName);

