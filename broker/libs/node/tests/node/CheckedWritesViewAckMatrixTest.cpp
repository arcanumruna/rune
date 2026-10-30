// CheckedWritesViewAckMatrixTest.cpp - checked writes Part 03 Task 9: the View ack matrix (2 ops x 4 entities x 10 timings)
// and the seeded stress of the waiter state.
#include "support/CheckedWritesAckRig.h"

#include <gtest/gtest.h>

#include <random>
#include <tuple>

using namespace rune::node;
using namespace rune::node::cw;
using rune::protocol::client::ErrorCode;

namespace {

constexpr int16_t kOk = 0;
constexpr int16_t kNotLeader = static_cast<int16_t>(ErrorCode::NotLeader);
constexpr int16_t kInternal = static_cast<int16_t>(ErrorCode::Internal);
constexpr int32_t kView1 = 0x40000001, kView2 = 0x40000002;
constexpr int64_t kTimeoutNs = 50'000'000;
constexpr uint8_t kAll = 1;

enum class MOp : int { LV, AV };
enum class MEntity : int { NoViews, OneView, TwoViewsLate, TransientSource };
enum class MTiming : int { Single, Pipelined8, DupBeforeFold, DupAfterFold, TxnCommit, TxnAbort, LeaderLoss, Deadline, Strict, Routed };
constexpr const char* kOpNames[] = {"LV", "AV"};
constexpr const char* kEntityNames[] = {"NoViews", "OneView", "TwoViewsLate", "TransientSource"};
constexpr const char* kTimingNames[] = {"Single", "Pipelined8", "DupBeforeFold", "DupAfterFold", "TxnCommit", "TxnAbort", "LeaderLoss", "Deadline", "Strict", "Routed"};

bool notApplicable(MEntity e, MTiming t) { return e == MEntity::TransientSource && t == MTiming::Strict; }

AckRig::Write write(int64_t corr, uint8_t acks, int64_t pid = -1, int32_t seq = -1, bool txn = false) {
    AckRig::Write w;
    w.corr = corr; w.acks = acks; w.pid = pid; w.seq = seq; w.txn = txn;
    return w;
}

}  // namespace

class CheckedWritesViewAckMatrix : public ::testing::TestWithParam<std::tuple<int, int, int>> {};

TEST_P(CheckedWritesViewAckMatrix, Cell) {
    const auto op = static_cast<MOp>(std::get<0>(GetParam()));
    const auto entity = static_cast<MEntity>(std::get<1>(GetParam()));
    const auto timing = static_cast<MTiming>(std::get<2>(GetParam()));
    if (notApplicable(entity, timing)) GTEST_SKIP() << "a transient ring has no fd: Strict acks as Majority there, the cell equals Single";

    const uint8_t acks = op == MOp::LV ? 2 : 3;
    AckRig::Options o;
    o.replicas = {1, 2};
    o.minIsr = timing == MTiming::Deadline ? 2 : 1;
    o.transient = entity == MEntity::TransientSource;
    o.durabilityTier = timing == MTiming::Strict ? 2 : 1;
    o.ringBytes = 1 << 18;
    AckRig r(o);
    r.typed(); r.setLevel(5);
    const bool views = entity != MEntity::NoViews;
    if (views) r.installView(kView1);

    const bool routed = timing == MTiming::Routed;
    const auto send = [&](const AckRig::Write& w) { if (routed) r.send_routed(w); else r.produce(w); };

    std::vector<int64_t> ours;
    std::vector<int64_t> viewIds;
    int64_t allCorr = 0;
    switch (timing) {
    case MTiming::Pipelined8:
        for (int64_t c = 1; c <= 8; ++c) { send(write(c, acks)); ours.push_back(c); }
        break;
    case MTiming::DupBeforeFold:
    case MTiming::DupAfterFold:
        send(write(1, acks, 11, 0)); ours.push_back(1);
        break;
    case MTiming::TxnCommit:
    case MTiming::TxnAbort:
        send(write(100, kAll, 77, 0, /*txn=*/true));
        r.steps(50);
        send(write(1, acks)); ours.push_back(1);
        allCorr = 2;
        send(write(allCorr, kAll));
        break;
    default:
        send(write(1, acks)); ours.push_back(1);
        break;
    }
    r.steps(60);
    if (entity == MEntity::TwoViewsLate) r.installView(kView2, "v2");   // after the append, before the HW passes
    if (timing == MTiming::DupBeforeFold) { send(write(2, acks, 11, 0)); ours.push_back(2); r.steps(60); }

    const auto answered = [&](int64_t c) { return r.acks.count(c) != 0; };
    if (timing == MTiming::Deadline) {
        r.steps(10);
        ASSERT_EQ(r.answer(ours[0]), kInternal);
        const int64_t deadline = r.appendedAtNs + kTimeoutNs;
        EXPECT_GE(r.ackNow[ours[0]], deadline) << "never earlier than the deadline";
        EXPECT_LE(r.ackNow[ours[0]], deadline + 30'000);
    } else if (timing == MTiming::LeaderLoss) {
        for (int64_t c : ours) ASSERT_FALSE(answered(c)) << "parked until the HW moves";
        r.demote();
        for (int64_t c : ours) {
            ASSERT_EQ(r.answer(c), kNotLeader);
            EXPECT_LT(r.ackNow[c], r.appendedAtNs + kTimeoutNs) << "before the deadline";
        }
    } else {
        for (int64_t c : ours) ASSERT_FALSE(answered(c)) << "parked until the HW moves";
        if (timing == MTiming::TxnCommit || timing == MTiming::TxnAbort) {
            r.followerFetch();
            r.steps(300);
            if (views) {
                EXPECT_FALSE(answered(1)) << "behind an open transaction the fold is held";
                ASSERT_EQ(r.answer(allCorr), kOk);
                EXPECT_FALSE(answered(1));
            } else {
                ASSERT_EQ(r.answer(allCorr), kOk);
                ASSERT_EQ(r.answer(1), kOk);
                EXPECT_LE(r.ackStep[1], r.ackStep[allCorr] + 1) << "no views: the pass an All ack fires in";
            }
            r.marker(77, timing == MTiming::TxnCommit);
        }
        r.followerFetch();
        for (int64_t c : ours) ASSERT_EQ(r.answer(c), kOk) << "corr " << c;
        if (timing == MTiming::TxnCommit || timing == MTiming::TxnAbort) {
            if (views) EXPECT_LT(r.ackStep[allCorr], r.ackStep[1]) << "an All ack is answered before the View ack behind the transaction (SD16)";
        }
        if (timing == MTiming::DupBeforeFold) EXPECT_EQ(r.acks[2].baseOffset, r.acks[1].baseOffset);
        if (timing == MTiming::DupAfterFold) {
            send(write(2, acks, 11, 0)); ours.push_back(2);
            ASSERT_EQ(r.answer(2), kOk);
            EXPECT_EQ(r.acks[2].baseOffset, r.acks[1].baseOffset);
            EXPECT_EQ(r.leo(), 1);
        }
        if (timing == MTiming::Pipelined8)
            for (int64_t c = 2; c <= 8; ++c) EXPECT_GE(r.ackStep[c], r.ackStep[c - 1]) << "answers leave in append order";

        for (int64_t c : ours) {
            const auto& a = r.acks[c];
            EXPECT_GT(r.replica->highWatermark(), a.baseOffset) << "HW at the answer, corr " << c;
            if (op == MOp::AV) EXPECT_GE(static_cast<int>(r.replica->isr().size()), o.minIsr);
            if (views) {
                EXPECT_GT(r.foldCursor(kView1), a.baseOffset);
                EXPECT_EQ(r.foldLeadership(kView1), r.replica->leadershipGeneration());
            }
            if (entity == MEntity::TwoViewsLate) {
                EXPECT_GT(r.foldCursor(kView2), a.baseOffset);
                EXPECT_EQ(r.foldLeadership(kView2), r.replica->leadershipGeneration());
            }
        }
    }
    r.steps(300);
    for (int64_t c : ours) {
        EXPECT_EQ(r.ackCount[c], 1) << "one answer per correlation id, corr " << c;
        EXPECT_EQ(r.ackLen[c], 54u) << "the 54-byte frame, no trailer, corr " << c;
        EXPECT_EQ(r.acks[c].assignedBase, 0u);
        EXPECT_FALSE(r.acks[c].hasVerdicts);
    }
}

INSTANTIATE_TEST_SUITE_P(Cells, CheckedWritesViewAckMatrix,
    ::testing::Combine(::testing::Range(0, 2), ::testing::Range(0, 4), ::testing::Range(0, 10)),
    [](const ::testing::TestParamInfo<std::tuple<int, int, int>>& i) {
        return std::string(kOpNames[std::get<0>(i.param)]) + "_" + kEntityNames[std::get<1>(i.param)] + "_" + kTimingNames[std::get<2>(i.param)];
    });

TEST(CheckedWritesViewAckMatrixCountRegression, CellCountsArePinned) {
    int applicable = 0, skipped = 0;
    for (int op = 0; op < 2; ++op)
        for (int e = 0; e < 4; ++e)
            for (int t = 0; t < 10; ++t) (notApplicable(static_cast<MEntity>(e), static_cast<MTiming>(t)) ? skipped : applicable)++;
    EXPECT_EQ(applicable, 78);
    EXPECT_EQ(skipped, 2);
}

// ---------------------------------------------------------------------------------------------------------------
// Seeded stress: one action per step; every answer is checked against what the model allows at that step.
// ---------------------------------------------------------------------------------------------------------------
namespace {

struct Tracked {
    int64_t  corr;
    uint8_t  acks;
    int64_t  parkStep;
    int64_t  parkNow;
    bool     demotedSince = false;
    bool     done = false;
};

}  // namespace

class CheckedWritesViewAckStressRegression : public ::testing::TestWithParam<int> {};

TEST_P(CheckedWritesViewAckStressRegression, SeededWaitersMatchTheModel) {
    const int seed = GetParam();
    std::mt19937 rng(static_cast<uint32_t>(seed));
    AckRig::Options o;
    o.replicas = {1, 2};
    o.minIsr = 1;
    o.ringBytes = 1 << 20;
    AckRig r(o);
    r.typed(); r.setLevel(5); r.installView(kView1);
    bool secondView = false;

    std::vector<Tracked> all;
    std::vector<int64_t> openTxns;
    int64_t corr = 0, seq = 0, nextTxnPid = 100;
    int64_t demotedAtStep = -1;
    const auto fail = [&](int step) { return ::testing::Message() << "seed=" << seed << " step=" << step; };

    const auto produce = [&](uint8_t acks, bool idem, bool txn, int64_t pid = -1) {
        AckRig::Write w;
        w.corr = ++corr; w.acks = acks; w.txn = txn;
        if (txn) { w.pid = pid; w.seq = 0; } else if (idem) { w.pid = 11; w.seq = static_cast<int32_t>(seq++); }
        r.produce(w);
        all.push_back({w.corr, acks, r.stepNo, r.now});
    };
    const auto checkAnswers = [&](int step) {
        for (auto& t : all) {
            if (t.done || r.acks.count(t.corr) == 0) continue;
            t.done = true;
            ASSERT_EQ(r.ackCount[t.corr], 1) << fail(step);
            ASSERT_GE(r.ackStep[t.corr], t.parkStep) << fail(step);
            const int16_t code = r.acks[t.corr].errorCode;
            if (code == kOk) {
                if (t.acks == 0) continue;
                const int64_t base = r.acks[t.corr].baseOffset;
                ASSERT_GT(r.replica->highWatermark(), base) << "OK before the HW " << fail(step);
                if (rune::protocol::client::acksView(t.acks)) {
                    ASSERT_GT(r.foldCursor(kView1), base) << "OK before the fold " << fail(step);
                    ASSERT_EQ(r.foldLeadership(kView1), r.replica->leadershipGeneration()) << "OK on a stale fold " << fail(step);
                    if (secondView && r.vs->instanceServeState(kView2, 0).hosted) {
                        ASSERT_GT(r.foldCursor(kView2), base) << fail(step);
                        ASSERT_EQ(r.foldLeadership(kView2), r.replica->leadershipGeneration()) << fail(step);
                    }
                }
            } else if (code == kInternal) {
                ASSERT_GE(r.ackNow[t.corr], t.parkNow + kTimeoutNs) << "Internal before the deadline " << fail(step);
            } else {
                ASSERT_EQ(code, kNotLeader) << fail(step);
                ASSERT_TRUE(t.demotedSince) << "NotLeader without a leadership change " << fail(step);
            }
        }
    };

    constexpr int kSteps = 400;
    for (int step = 0; step < kSteps; ++step) {
        switch (rng() % 14) {
        case 0: case 1: case 2: case 3: produce(static_cast<uint8_t>(rng() % 4), (rng() & 1) != 0, false); break;
        case 4: case 5: case 6: r.followerFetch(); break;
        case 7:
            if (openTxns.size() < 2) { openTxns.push_back(nextTxnPid); produce(kAll, true, true, nextTxnPid++); }
            break;
        case 8:
            if (!openTxns.empty()) { r.marker(openTxns.front(), (rng() & 1) != 0); openTxns.erase(openTxns.begin()); }
            break;
        case 9:
            if (!secondView) { r.installView(kView2, "v2"); secondView = true; }
            break;
        case 10:
            r.demote();
            r.promote();
            for (auto& t : all) if (!t.done) t.demotedSince = true;
            demotedAtStep = step;
            break;
        case 11: r.now += kTimeoutNs / 4; break;
        default: break;
        }
        r.step();
        checkAnswers(step);
        if (::testing::Test::HasFatalFailure()) return;
    }
    (void)demotedAtStep;

    // Quiesce: every transaction ends, then the HW and the folds catch up.
    for (int64_t pid : openTxns) r.marker(pid, true);
    for (int i = 0; i < 6; ++i) { r.followerFetch(); r.steps(300); }
    checkAnswers(kSteps);
    for (const auto& t : all) {
        if (!t.done) {
            EXPECT_TRUE(t.demotedSince || r.now >= t.parkNow + kTimeoutNs) << "corr " << t.corr << " never answered: " << fail(kSteps);
            continue;
        }
        const int16_t code = r.acks[t.corr].errorCode;
        if (!t.demotedSince && r.now < t.parkNow + kTimeoutNs) EXPECT_EQ(code, kOk) << "corr " << t.corr << " acks " << int(t.acks) << " " << fail(kSteps);
    }
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesViewAckStressRegression, ::testing::Range(1, 21));
