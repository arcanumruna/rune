// Checked writes Part 04 Task 11 (rig slice): a seeded random history against an independent model.  Three idempotent producers
// write through the ack path (with retries); the leader fails over, the tail is cut, the cleaner runs, and the constraint set
// gets a member installed, replaced and dropped.  After every step the shared matrix invariants (index, markers, views, delivery)
// hold against the model, built from every committed record the history ever held.  The cluster stress (ThreeNodes) is in
// CheckedWritesStressTest.cpp.
#include "CheckedWritesChecks.h"
#include "CheckedWritesRig.h"

#include <algorithm>
#include <map>
#include <random>
#include <set>

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
