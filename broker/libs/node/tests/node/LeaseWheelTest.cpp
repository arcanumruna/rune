// LeaseWheelTest.cpp - lease topics Task 9: the lease wheels, the entry store and the four-state machine (spec 9.1, 9.5, 9.6, 10).
// Standard library + gtest only: the headers under test include nothing else.
#include <rune/node/lease/LeaseEntryStore.h>
#include <rune/node/lease/LeaseWheel.h>

#include "AllocWindow.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <random>
#include <vector>

namespace {

using namespace rune::node::lease;
using S = LeaseEntryState;
using E = LeaseEntryEvent;
using A = LeaseAction;
using WK = LeaseWheelKind;

constexpr int64_t kMs = 1'000'000;

// The reference tick of a deadline, written independently of the wheel: ceil(ns / 1 ms), negative = 0.
int64_t refTickSafe(int64_t ns) { return ns <= 0 ? 0 : ns / kMs + (ns % kMs != 0); }

// A wheel with its own store (the wheel-level tests; LeaseTimers hides its wheels).
template <WK K>
struct WheelRig {
    LeaseEntryStore store;
    LeaseWheel<K> w;
    WheelRig(uint32_t rows, int64_t nowNs) : store(rows), w(store, nowNs) { (void)store.reserveFor(rows - 1); }
};

template <class W>
std::vector<uint32_t> pollAll(W& w, int64_t now, uint32_t max = 200000) {
    std::vector<uint32_t> out;
    w.poll(now, [&](uint32_t rid) { out.push_back(rid); return true; }, max);
    return out;
}

std::vector<uint32_t> dAll(LeaseTimers& t, int64_t now, uint32_t max = 200000) {
    std::vector<uint32_t> out;
    t.pollDeadline(now, [&](uint32_t rid) { out.push_back(rid); return true; }, max);
    return out;
}
std::vector<uint32_t> cAll(LeaseTimers& t, int64_t now, uint32_t max = 200000) {
    std::vector<uint32_t> out;
    t.pollCheckpoint(now, [&](uint32_t rid) { out.push_back(rid); return true; }, max);
    return out;
}

}  // namespace

TEST(LeaseWheelRegression, EntryLayoutRegression) {
    EXPECT_EQ(sizeof(LeaseEntry), 48u);
    EXPECT_EQ(satAdd(INT64_MAX - 5, 5), INT64_MAX);
    EXPECT_EQ(satAdd(INT64_MAX - 5, 6), INT64_MAX);
    EXPECT_EQ(satAdd(1, 2), 3);
}

TEST(LeaseWheelRegression, BoundariesRegression) {
    for (const int64_t startMs : {int64_t{0}, int64_t{777}}) {
        const int64_t nowNs = startMs * kMs;
        for (int l = 0; l <= 4; ++l) {
            for (const int64_t delta : {int64_t{0}, int64_t{1}, int64_t{1023}, int64_t{1024}, int64_t{1025}}) {
                const int64_t ticks = delta << (10 * l);
                if (ticks > (INT64_MAX - nowNs) / kMs) continue;   // the deadline would overflow
                WheelRig<WK::Deadline> r(4096, nowNs);
                const int64_t deadline = nowNs + ticks * kMs;
                const int64_t dTick    = refTickSafe(deadline);
                // the lowest level whose 1024-block distance from cur (= the start tick) is < 1024
                int expect = 4;
                for (int k = 0; k <= 4; ++k) {
                    if (((dTick >> (10 * k)) - (startMs >> (10 * k))) < 1024) { expect = k; break; }
                }
                r.w.arm(7, deadline);
                ASSERT_EQ(r.w.levelOf(7), expect) << "start " << startMs << " level " << l << " delta " << delta;
                if (deadline - 1 >= 0) {
                    EXPECT_TRUE(pollAll(r.w, deadline - 1).empty()) << "early: start " << startMs << " l " << l << " d " << delta;
                    EXPECT_TRUE(r.w.linked(7));
                }
                const auto got = pollAll(r.w, deadline);
                ASSERT_EQ(got.size(), 1u) << "start " << startMs << " level " << l << " delta " << delta;
                EXPECT_EQ(got[0], 7u);
                EXPECT_FALSE(r.w.linked(7));
                EXPECT_EQ(r.w.invariantRepairsForTest(), 0u);
            }
        }
    }

    // F9: a wheel created at a large clock value starts its cursor there, it does not walk up from tick 0.
    const int64_t threeDays = int64_t{3} * 86'400 * 1'000'000'000;
    WheelRig<WK::Deadline> big(4096, threeDays);
    big.w.arm(1, threeDays + 5 * kMs);
    EXPECT_EQ(big.w.levelOf(1), 0);
    EXPECT_EQ(pollAll(big.w, threeDays + 5 * kMs), std::vector<uint32_t>{1});
    EXPECT_LE(big.w.slotsVisitedForTest(), 2u);
    EXPECT_EQ(big.w.invariantRepairsForTest(), 0u);
}

TEST(LeaseWheelRegression, BoundariesRearmCancelAndMaxTtlRegression) {
    const int64_t far = int64_t{1'500'000'000};   // ticks: level 3 from tick 0
    WheelRig<WK::Deadline> r(4096, 0);
    auto& w = r.w;
    // earlier re-arm of a level-3 row
    w.arm(1, far * kMs);
    ASSERT_EQ(w.levelOf(1), 3);
    w.arm(1, 5 * kMs);
    EXPECT_EQ(pollAll(w, 10 * kMs), std::vector<uint32_t>{1});
    EXPECT_TRUE(pollAll(w, far * kMs).empty());
    // later re-arm
    w.arm(2, far * kMs);
    w.arm(2, 2 * far * kMs);
    EXPECT_TRUE(pollAll(w, far * kMs).empty());
    EXPECT_EQ(pollAll(w, 2 * far * kMs), std::vector<uint32_t>{2});
    // cancel before the cascade
    const int64_t base = 2 * far * kMs;
    w.arm(3, satAdd(base, far * kMs));
    w.cancel(3);
    EXPECT_FALSE(w.linked(3));
    EXPECT_TRUE(pollAll(w, satAdd(base, far * kMs + 1000 * kMs)).empty());
    EXPECT_EQ(w.invariantRepairsForTest(), 0u);

    // MaxLeaseTTL and +1 ms
    const int64_t now = 777 * kMs;
    WheelRig<WK::Deadline> m(4096, now);
    const int64_t ttl = int64_t{9'000'000'000'000} * kMs;   // MaxLeaseTTL: 9e12 ms as ns
    m.w.arm(1, satAdd(now, ttl));
    m.w.arm(2, satAdd(now, satAdd(ttl, kMs)));
    EXPECT_EQ(m.w.levelOf(1), 4);
    EXPECT_EQ(m.w.levelOf(2), 4);
    EXPECT_TRUE(pollAll(m.w, now + int64_t{400} * 86'400 * 1'000'000'000).empty());
    EXPECT_TRUE(m.w.linked(1));
    EXPECT_TRUE(m.w.linked(2));
    EXPECT_EQ(m.w.invariantRepairsForTest(), 0u);
}

TEST(LeaseWheelRegression, ReferenceModelRegression) {
    std::mt19937_64 rng(0x1EA5E);
    constexpr uint32_t kRows = 5000;
    int64_t now = 1'000'000'000;
    WheelRig<WK::Deadline> r(kRows, now);
    auto& w = r.w;
    std::map<uint32_t, int64_t> model;   // rid -> dTick
    auto pick = [&](uint64_t n) { return rng() % n; };
    int maxLevelSeen = 0;
    for (int op = 0; op < 100'000; ++op) {
        const uint64_t k0 = pick(100);
        const uint32_t rid = static_cast<uint32_t>(pick(kRows));
        if (k0 < 40) {   // arm or re-arm
            int64_t span;
            const uint64_t k = pick(100);
            if (k < 5) span = static_cast<int64_t>(pick(int64_t{1} << 52));         // up to ~52 days: reaches level 3
            else if (k < 10) span = static_cast<int64_t>(pick(2'000'000'000));      // dense due
            else if (k < 20) span = static_cast<int64_t>(pick(int64_t{1} << 46));   // up to ~19 h: reaches level 2
            else span = static_cast<int64_t>(pick(int64_t{1} << 34));
            const int64_t d = satAdd(now, span);
            w.arm(rid, d);
            const int lv = w.levelOf(rid);
            if (lv <= 4) maxLevelSeen = std::max(maxLevelSeen, lv);
            model[rid] = refTickSafe(d);
        } else if (k0 < 55) {   // cancel
            w.cancel(rid);
            model.erase(rid);
        } else {   // advance and poll
            now += static_cast<int64_t>(pick(3'000'000'000));
            std::vector<uint32_t> got = pollAll(w, now);
            std::sort(got.begin(), got.end());
            std::vector<uint32_t> want;
            for (const auto& [id, tick] : model) if (tick <= now / kMs) want.push_back(id);
            ASSERT_EQ(got, want) << "op " << op << " now " << now;
            for (const uint32_t id : got) {
                ASSERT_LE(r.store.at(id).deadlineNs, now) << "handed early, rid " << id;
                model.erase(id);
            }
        }
    }
    EXPECT_GE(maxLevelSeen, 3);   // level 3 was reached
    // catch up to `now` (arms after the last poll may be due), then drain: everything left comes out exactly at its tick
    {
        std::vector<uint32_t> got = pollAll(w, now);
        std::sort(got.begin(), got.end());
        std::vector<uint32_t> want;
        for (const auto& [id, tick] : model) if (tick <= now / kMs) want.push_back(id);
        ASSERT_EQ(got, want);
        for (const uint32_t id : got) model.erase(id);
    }
    std::map<int64_t, std::vector<uint32_t>> byTick;
    for (const auto& [id, tick] : model) {
        EXPECT_TRUE(w.linked(id));
        byTick[tick].push_back(id);
    }
    for (auto& [tick, ids] : byTick) {
        std::sort(ids.begin(), ids.end());
        ASSERT_GE(tick * kMs - 1, now);
        EXPECT_TRUE(pollAll(w, tick * kMs - 1).empty()) << "early at tick " << tick;
        EXPECT_EQ(pollAll(w, tick * kMs), ids) << "tick " << tick;
        now = tick * kMs;
    }
    EXPECT_EQ(w.invariantRepairsForTest(), 0u);
}

TEST(LeaseWheelRegression, MaxAndDeferRegression) {
    WheelRig<WK::Deadline> r(4096, 0);
    auto& w = r.w;
    for (uint32_t i = 0; i < 10; ++i) w.arm(i, 5 * kMs);
    std::vector<uint32_t> first;
    EXPECT_EQ(w.poll(10 * kMs, [&](uint32_t id) { first.push_back(id); return true; }, 3), 3u);
    ASSERT_EQ(first.size(), 3u);
    std::vector<uint32_t> rest;
    for (uint32_t i = 0; i < 10; ++i) {
        if (std::find(first.begin(), first.end(), i) != first.end()) continue;
        rest.push_back(i);
        EXPECT_TRUE(w.linked(i));
        EXPECT_EQ(w.levelOf(i), 5);
    }
    ASSERT_EQ(rest.size(), 7u);
    const uint32_t deferred = rest[0];
    std::vector<uint32_t> second;
    EXPECT_EQ(w.poll(11 * kMs, [&](uint32_t id) { if (id == deferred) return false; second.push_back(id); return true; },
                     UINT32_MAX), 6u);
    EXPECT_TRUE(w.linked(deferred));
    EXPECT_EQ(w.levelOf(deferred), 5);
    EXPECT_LE(r.store.at(deferred).deadlineNs, 11 * kMs);
    EXPECT_EQ(pollAll(w, 12 * kMs), std::vector<uint32_t>{deferred});
    EXPECT_FALSE(w.linked(deferred));
}

template <WK K>
void sinkRearmCase() {
    {   // A: the sink re-arms the row it was given into the future and returns true: the re-arm survives the poll
        WheelRig<K> r(4096, 0);
        r.w.arm(1, 5 * kMs);
        EXPECT_EQ(r.w.poll(10 * kMs, [&](uint32_t id) { r.w.arm(id, 100 * kMs); return true; }, 10), 1u);
        EXPECT_TRUE(r.w.linked(1));
        EXPECT_TRUE(pollAll(r.w, 99 * kMs).empty());
        EXPECT_EQ(pollAll(r.w, 100 * kMs), std::vector<uint32_t>{1});
        EXPECT_FALSE(r.w.linked(1));
        EXPECT_TRUE(pollAll(r.w, 200 * kMs).empty());
    }
    {   // B: the sink re-arms into the past (the ready list) and returns true: still linked, handed once by the next poll
        WheelRig<K> r(4096, 0);
        r.w.arm(1, 5 * kMs);
        EXPECT_EQ(r.w.poll(10 * kMs, [&](uint32_t id) { r.w.arm(id, 1 * kMs); return true; }, 10), 1u);
        ASSERT_TRUE(r.w.linked(1));
        EXPECT_EQ(r.w.levelOf(1), kLeaseReadyLevel);
        EXPECT_EQ(pollAll(r.w, 11 * kMs), std::vector<uint32_t>{1});
        EXPECT_FALSE(r.w.linked(1));
        EXPECT_TRUE(pollAll(r.w, 12 * kMs).empty());
    }
    {   // C: the sink cancels the row it was given
        WheelRig<K> r(4096, 0);
        r.w.arm(1, 5 * kMs);
        r.w.arm(2, 5 * kMs);
        EXPECT_EQ(r.w.poll(10 * kMs, [&](uint32_t id) { r.w.cancel(id); return true; }, 10), 2u);
        EXPECT_FALSE(r.w.linked(1));
        EXPECT_FALSE(r.w.linked(2));
    }
}

TEST(LeaseWheelRegression, SinkRearmSurvivesPollRegression) {
    sinkRearmCase<WK::Deadline>();
    sinkRearmCase<WK::Checkpoint>();
}

TEST(LeaseWheelRegression, SinkChangesStateThroughTimersRegression) {
    LeaseTimers t(4096, 0);
    t.arm(1, 5 * kMs, 6 * kMs);
    EXPECT_EQ(t.pollDeadline(10 * kMs, [&](uint32_t id) { t.setState(id, S::Emitting, 5); return true; }, 10), 1u);
    EXPECT_EQ(t.entry(1).state, S::Emitting);
    EXPECT_EQ(t.entry(1).inflightOffset, 5);
    EXPECT_FALSE(t.deadline().linked(1));
    EXPECT_FALSE(t.checkpoint().linked(1));   // setState(Emitting) cancelled the checkpoint row too
    EXPECT_TRUE(cAll(t, 20 * kMs).empty());
}

TEST(LeaseWheelRegression, DeferredRowsBoundedScanRegression) {
    constexpr uint32_t kRows = 100'000;
    WheelRig<WK::Deadline> r(kRows, 10 * kMs);
    auto& w = r.w;
    for (uint32_t i = 0; i < kRows; ++i) w.arm(i, 5 * kMs);   // already due: straight into the ready list, in arming order
    // a sink that defers everything: one poll with max 256 examines <= 512 rows
    const uint64_t before = w.touchedForTest();
    EXPECT_EQ(w.poll(10 * kMs, [](uint32_t) { return false; }, 256), 0u);
    EXPECT_LE(w.touchedForTest() - before, 512u);
    const uint64_t before2 = w.touchedForTest();
    EXPECT_EQ(w.poll(11 * kMs, [](uint32_t) { return false; }, 256), 0u);
    EXPECT_LE(w.touchedForTest() - before2, 512u);
    for (uint32_t i = 0; i < kRows; ++i) ASSERT_EQ(w.levelOf(i), kLeaseReadyLevel) << i;

    // a sink that takes rows >= 50 000 and defers the rest: every accepted row is handed exactly once, in a bounded number of polls
    std::vector<uint8_t> seen(kRows, 0);
    uint32_t handed = 0;
    const uint32_t bound = (kRows + 511) / 512 + 50'000 / 256 + 2;
    uint32_t polls = 0;
    int64_t now = 11 * kMs;
    while (handed < 50'000 && polls <= bound) {
        ++polls;
        handed += w.poll(now, [&](uint32_t id) {
            if (id < 50'000) return false;
            EXPECT_EQ(seen[id], 0) << "handed twice: " << id;
            seen[id] = 1;
            return true;
        }, 256);
    }
    EXPECT_EQ(handed, 50'000u);
    EXPECT_LE(polls, bound);
    for (uint32_t i = 0; i < kRows; ++i) {
        if (i >= 50'000) EXPECT_FALSE(w.linked(i)) << i;
        else EXPECT_EQ(w.levelOf(i), kLeaseReadyLevel) << i;
    }
    EXPECT_EQ(w.invariantRepairsForTest(), 0u);
}

TEST(LeaseWheelRegression, DeferredRowsRotateRegression) {
    WheelRig<WK::Deadline> r(4096, 10 * kMs);
    auto& w = r.w;
    w.arm(1, 5 * kMs);   // A, B, C: due, in the ready list in arming order
    w.arm(2, 5 * kMs);
    w.arm(3, 5 * kMs);
    bool deferredA = false;
    auto sink = [&](uint32_t id) {
        if (id == 1 && !deferredA) { deferredA = true; return false; }
        return true;
    };
    std::vector<uint32_t> order;
    for (int i = 0; i < 3; ++i) {
        const uint32_t n = w.poll(10 * kMs, [&](uint32_t id) { const bool ok = sink(id); if (ok) order.push_back(id); return ok; }, 1);
        EXPECT_EQ(n, 1u);
    }
    EXPECT_EQ(order, (std::vector<uint32_t>{2, 3, 1}));
    EXPECT_FALSE(w.linked(1));
    EXPECT_FALSE(w.linked(2));
    EXPECT_FALSE(w.linked(3));
}

TEST(LeaseWheelRegression, StateMachineRegression) {
    struct Cell { S s; E e; S next; A action; };
    // The independent cells (outcome does not depend on the three bools).
    const Cell fixed[] = {
        {S::Free, E::Arm, S::Armed, A::Link},
        {S::Free, E::FallDue, S::Free, A::None},
        {S::Free, E::CheckpointDue, S::Free, A::None},
        {S::Free, E::Keepalive, S::Free, A::None},
        {S::Free, E::OwnRecordFolds, S::Free, A::None},
        {S::Free, E::OtherUpdateFolds, S::Free, A::None},
        {S::Free, E::EndFolds, S::Free, A::None},
        {S::Free, E::Demotion, S::Free, A::None},
        {S::Free, E::OwnRecordRejected, S::Free, A::None},
        {S::Armed, E::Arm, S::Armed, A::None},
        {S::Armed, E::OwnRecordFolds, S::Armed, A::None},
        {S::Armed, E::OtherUpdateFolds, S::Armed, A::ReplayParked},
        {S::Armed, E::EndFolds, S::Free, A::FreeAnswerParkedNotFound},
        {S::Armed, E::Demotion, S::Free, A::DiscardAnswerParkedNotLeader},
        {S::Armed, E::OwnRecordRejected, S::Armed, A::None},
    };
    const S inflight[] = {S::Checkpointing, S::Clearing, S::Emitting};
    int cells = 0;
    for (int mask = 0; mask < 8; ++mask) {
        const bool gate = mask & 1, due = mask & 2, rem = mask & 4;
        auto check = [&](S s, E e, S next, A action) {
            const LeaseStepOut out = leaseStep({s, e, gate, due, rem});
            EXPECT_EQ(out.next, next) << int(s) << "/" << int(e) << " mask " << mask;
            EXPECT_EQ(out.action, action) << int(s) << "/" << int(e) << " mask " << mask;
            ++cells;
        };
        for (const Cell& c : fixed) check(c.s, c.e, c.next, c.action);
        // Armed, the three bool-dependent cells
        check(S::Armed, E::FallDue, gate ? S::Emitting : S::Armed, gate ? A::AppendExpire : A::None);
        // a due key never checkpoints, whatever the gate says
        // (checkpointWanted defaults true here; the full CheckpointDue input space is CheckpointDueTableRegression)
        check(S::Armed, E::CheckpointDue, (gate && !due) ? S::Checkpointing : S::Armed,
              due ? A::Unschedule : (gate ? A::AppendCheckpoint : A::Defer));
        if (due) check(S::Armed, E::Keepalive, S::Armed, A::Park);
        else if (!rem) check(S::Armed, E::Keepalive, S::Armed, A::RearmAnswerOk);
        else if (gate) check(S::Armed, E::Keepalive, S::Clearing, A::AppendClear);
        else check(S::Armed, E::Keepalive, S::Armed, A::Park);
        // the three in-flight states
        for (const S s : inflight) {
            check(s, E::Arm, s, A::None);
            check(s, E::FallDue, s, A::None);
            check(s, E::CheckpointDue, s, A::None);
            check(s, E::Keepalive, s, A::Park);
            check(s, E::OtherUpdateFolds, s, A::None);
            check(s, E::EndFolds, S::Free, A::FreeAnswerParkedNotFound);
            check(s, E::Demotion, S::Free, A::DiscardAnswerParkedNotLeader);
            check(s, E::OwnRecordRejected, S::Armed, A::ReturnArmedReplayParked);
        }
        check(S::Checkpointing, E::OwnRecordFolds, S::Armed, A::ReturnArmedReplayParked);
        check(S::Clearing, E::OwnRecordFolds, S::Armed, A::ReturnArmedAnswerParkedOk);
        check(S::Emitting, E::OwnRecordFolds, S::Free, A::FreeAnswerParkedNotFound);
    }
    EXPECT_EQ(cells, 8 * (15 + 3 + 3 * 8 + 3));   // all 45 table cells (5 states x 9 events), every bool combination (was 40 x 8)
    EXPECT_EQ(cells, 360);

    // R4: AppendExpire / AppendCheckpoint only from Armed.
    for (int s = 0; s <= 4; ++s) {
        for (int e = 0; e <= 8; ++e) {
            for (int mask = 0; mask < 8; ++mask) {
                const LeaseStepOut out = leaseStep({static_cast<S>(s), static_cast<E>(e), bool(mask & 1), bool(mask & 2), bool(mask & 4)});
                if (out.action == A::AppendExpire || out.action == A::AppendCheckpoint) EXPECT_EQ(static_cast<S>(s), S::Armed);
            }
        }
    }
}

// The checkpoint sink decides through leaseStep (rule 3: one code path).  Every input combination of the CheckpointDue event, every state.
TEST(LeaseWheelRegression, CheckpointDueTableRegression) {
    int cells = 0;
    for (int st = 0; st <= 4; ++st) {
        for (int mask = 0; mask < 32; ++mask) {
            const bool gate = mask & 1, due = mask & 2, rem = mask & 4, wanted = mask & 8;
            const LeaseStepOut out = leaseStep({static_cast<S>(st), E::CheckpointDue, gate, due, rem, wanted});
            ++cells;
            if (static_cast<S>(st) != S::Armed) {   // nothing is appended, unscheduled or deferred from any other state
                EXPECT_EQ(out.next, static_cast<S>(st)) << st << " mask " << mask;
                EXPECT_EQ(out.action, A::None) << st << " mask " << mask;
                continue;
            }
            if (due || !wanted) {   // expires first / checkpointing off or about to end: unschedule, whatever the gate says
                EXPECT_EQ(out.next, S::Armed) << mask;
                EXPECT_EQ(out.action, A::Unschedule) << mask;
            } else if (gate) {
                EXPECT_EQ(out.next, S::Checkpointing) << mask;
                EXPECT_EQ(out.action, A::AppendCheckpoint) << mask;
            } else {
                EXPECT_EQ(out.next, S::Armed) << mask;
                EXPECT_EQ(out.action, A::Defer) << mask;
            }
        }
    }
    EXPECT_EQ(cells, 5 * 32);
    // `remainingPositive` never matters to CheckpointDue.
    for (int mask = 0; mask < 16; ++mask) {
        const LeaseStepOut a = leaseStep({S::Armed, E::CheckpointDue, bool(mask & 1), bool(mask & 2), false, bool(mask & 8)});
        const LeaseStepOut b = leaseStep({S::Armed, E::CheckpointDue, bool(mask & 1), bool(mask & 2), true, bool(mask & 8)});
        EXPECT_EQ(a.action, b.action);
    }
    // promote() arms through Free x Arm: Link, to Armed, whatever the other inputs.
    for (int mask = 0; mask < 16; ++mask) {
        const LeaseStepOut out = leaseStep({S::Free, E::Arm, bool(mask & 1), bool(mask & 2), bool(mask & 4), bool(mask & 8)});
        EXPECT_EQ(out.next, S::Armed);
        EXPECT_EQ(out.action, A::Link);
    }
}

TEST(LeaseWheelRegression, PromotionKeepaliveSequenceRegression) {
    // A present row whose entry is still Free (not yet linked by the promotion pass): the caller steps Arm first, then Keepalive.
    EXPECT_EQ(leaseStep({S::Free, E::Keepalive, true, false, false}).action, A::None);
    for (int mask = 0; mask < 2; ++mask) {
        const LeaseStepOut armed = leaseStep({S::Free, E::Arm, bool(mask & 1), false, false});
        EXPECT_EQ(armed.next, S::Armed);
        EXPECT_EQ(armed.action, A::Link);
        const LeaseStepOut ka = leaseStep({armed.next, E::Keepalive, bool(mask & 1), /*due*/ false, /*remainingPositive*/ false});
        EXPECT_EQ(ka.next, S::Armed);
        EXPECT_EQ(ka.action, A::RearmAnswerOk);
    }
}

TEST(LeaseWheelRegression, ArmedOnlyLinkedRegression) {
    const S states[] = {S::Emitting, S::Checkpointing, S::Clearing};
    LeaseTimers t(4096, 0);
    const int64_t d = 20 * kMs, c = 10 * kMs;
    for (uint32_t i = 0; i < 3; ++i) {
        t.rearm(i, d);
        t.scheduleCheckpoint(i, c);
        EXPECT_FALSE(t.deadline().linked(i));   // Free: stored, not linked
        t.setState(i, S::Armed);
        ASSERT_TRUE(t.deadline().linked(i));
        ASSERT_TRUE(t.checkpoint().linked(i));
        t.setState(i, states[i], 42);
        EXPECT_FALSE(t.deadline().linked(i));
        EXPECT_FALSE(t.checkpoint().linked(i));
        EXPECT_EQ(t.entry(i).inflightOffset, 42);
        EXPECT_EQ(t.entry(i).state, states[i]);
    }
    EXPECT_TRUE(dAll(t, 50 * kMs).empty());
    EXPECT_TRUE(cAll(t, 50 * kMs).empty());
    for (uint32_t i = 0; i < 3; ++i) {
        t.setState(i, S::Armed);
        EXPECT_EQ(t.entry(i).inflightOffset, -1);
        EXPECT_TRUE(t.deadline().linked(i));
        EXPECT_TRUE(t.checkpoint().linked(i));
        EXPECT_EQ(t.entry(i).deadlineNs, d);
        EXPECT_EQ(t.entry(i).checkpointNs, c);
    }
    auto dg = dAll(t, 60 * kMs);
    auto cg = cAll(t, 60 * kMs);
    std::sort(dg.begin(), dg.end());
    std::sort(cg.begin(), cg.end());
    EXPECT_EQ(dg, (std::vector<uint32_t>{0, 1, 2}));
    EXPECT_EQ(cg, (std::vector<uint32_t>{0, 1, 2}));

    // rearm while Clearing stores but does not link
    LeaseTimers u(4096, 50 * kMs);
    u.rearm(3, 100 * kMs);
    u.scheduleCheckpoint(3, 90 * kMs);
    u.setState(3, S::Armed);
    u.setState(3, S::Clearing, 9);
    u.rearm(3, 200 * kMs);
    u.scheduleCheckpoint(3, 150 * kMs);
    EXPECT_EQ(u.entry(3).deadlineNs, 200 * kMs);
    EXPECT_EQ(u.entry(3).checkpointNs, 150 * kMs);
    EXPECT_FALSE(u.deadline().linked(3));
    EXPECT_FALSE(u.checkpoint().linked(3));
    EXPECT_TRUE(dAll(u, 150 * kMs).empty());
    u.setState(3, S::Armed);
    EXPECT_TRUE(u.deadline().linked(3));
    EXPECT_TRUE(dAll(u, 199 * kMs).empty());
    EXPECT_EQ(dAll(u, 200 * kMs), std::vector<uint32_t>{3});
}

TEST(LeaseWheelRegression, SetStateFreeFromEveryStateRegression) {
    for (const S from : {S::Armed, S::Checkpointing, S::Clearing, S::Emitting}) {
        LeaseTimers t(4096, 0);
        t.arm(5, 20 * kMs, 10 * kMs);   // Armed with a checkpoint
        ASSERT_TRUE(t.deadline().linked(5));
        ASSERT_TRUE(t.checkpoint().linked(5));
        if (from != S::Armed) t.setState(5, from, 7);
        t.setState(5, S::Free);
        EXPECT_FALSE(t.deadline().linked(5)) << int(from);
        EXPECT_FALSE(t.checkpoint().linked(5)) << int(from);
        const LeaseEntry fresh{};
        EXPECT_EQ(std::memcmp(&t.entry(5), &fresh, sizeof(LeaseEntry)), 0) << int(from);
        EXPECT_TRUE(dAll(t, 1000 * kMs).empty()) << int(from);
        EXPECT_TRUE(cAll(t, 1000 * kMs).empty()) << int(from);
        // the slot of the freed row is not corrupted: a neighbour armed after it still comes out
        t.arm(6, 1100 * kMs, kLeaseNoCheckpoint);
        EXPECT_EQ(dAll(t, 1200 * kMs), std::vector<uint32_t>{6});
    }
}

TEST(LeaseWheelRegression, ArmApiRegression) {
    LeaseTimers t(4096, 100 * kMs);
    // arm with no checkpoint: the checkpoint wheel stays unlinked
    t.arm(1, 500 * kMs, kLeaseNoCheckpoint);
    EXPECT_TRUE(t.deadline().linked(1));
    EXPECT_FALSE(t.checkpoint().linked(1));
    EXPECT_EQ(t.entry(1).state, S::Armed);
    // arm with a checkpoint, then cancel the schedule while Armed
    t.arm(2, 500 * kMs, 300 * kMs);
    EXPECT_TRUE(t.checkpoint().linked(2));
    t.scheduleCheckpoint(2, kLeaseNoCheckpoint);
    EXPECT_FALSE(t.checkpoint().linked(2));
    EXPECT_EQ(t.entry(2).checkpointNs, kLeaseNoCheckpoint);
    EXPECT_TRUE(cAll(t, 400 * kMs).empty());
    // arm with a past deadline: straight into the ready list
    t.arm(3, 50 * kMs, kLeaseNoCheckpoint);
    EXPECT_EQ(t.deadline().levelOf(3), kLeaseReadyLevel);
    // max == 0 hands nothing and leaves the due rows due
    EXPECT_EQ(t.pollDeadline(120 * kMs, [](uint32_t) { return true; }, 0), 0u);
    EXPECT_EQ(t.deadline().levelOf(3), kLeaseReadyLevel);
    EXPECT_EQ(dAll(t, 120 * kMs), std::vector<uint32_t>{3});
    // relink: a poll handed row 1, the caller could not use it
    auto both = dAll(t, 500 * kMs);
    std::sort(both.begin(), both.end());
    EXPECT_EQ(both, (std::vector<uint32_t>{1, 2}));
    EXPECT_FALSE(t.deadline().linked(1));
    t.relink(1);
    EXPECT_EQ(t.deadline().levelOf(1), kLeaseReadyLevel);   // its stored deadline is already due
    EXPECT_EQ(dAll(t, 501 * kMs), std::vector<uint32_t>{1});
}

TEST(LeaseWheelRegression, DestroyWithChunksAndRowsInEveryStateRegression) {
    // Demotion destroys the runtime (no clear pass): destroying a timers object with 3 chunks and rows in every state is clean.
    LeaseTimers t(3 * kLeaseEntryChunk, 0);
    ASSERT_TRUE(t.reserveFor(3 * kLeaseEntryChunk - 1));
    EXPECT_EQ(t.capacity(), 3 * kLeaseEntryChunk);
    const S states[] = {S::Armed, S::Checkpointing, S::Clearing, S::Emitting};
    for (uint32_t i = 0; i < 3 * kLeaseEntryChunk; i += 97) {
        t.arm(i, (1 + i % 5000) * kMs, i % 2 ? 7 * kMs : kLeaseNoCheckpoint);
        if (const S s = states[i % 4]; s != S::Armed) t.setState(i, s, i);
    }
    EXPECT_GT(t.bytes(), 3 * kLeaseEntryChunk * sizeof(LeaseEntry));
}

TEST(LeaseWheelRegression, SaturationRegression) {
    LeaseTimers t(4096, 777 * kMs);
    const int64_t now      = 777 * kMs;
    const int64_t ttl      = 1000;
    const int64_t deadline = satAdd(INT64_MAX - ttl, ttl + 5);
    ASSERT_EQ(deadline, INT64_MAX);
    t.arm(1, deadline, kLeaseNoCheckpoint);
    EXPECT_EQ(t.deadline().levelOf(1), 4);
    EXPECT_TRUE(dAll(t, now + int64_t{365} * 86'400 * 1'000'000'000).empty());
    EXPECT_TRUE(dAll(t, INT64_MAX - 1).empty());
    EXPECT_TRUE(t.deadline().linked(1));

    // msToNsSat
    EXPECT_EQ(msToNsSat(0), 0);
    EXPECT_EQ(msToNsSat(1), 1'000'000);
    const uint64_t lim = static_cast<uint64_t>(INT64_MAX) / 1'000'000;
    EXPECT_EQ(msToNsSat(lim), static_cast<int64_t>(lim) * 1'000'000);
    EXPECT_EQ(msToNsSat(lim + 1), INT64_MAX);
    EXPECT_EQ(msToNsSat(UINT64_MAX), INT64_MAX);
}

TEST(LeaseWheelRegression, StoreGrowthRegression) {
    LeaseEntryStore s(10'000);
    EXPECT_EQ(s.cap(), 12'288u);
    EXPECT_EQ(s.capacity(), 4096u);
    s.noteUse(3070);
    EXPECT_FALSE(s.growOwed());
    s.noteUse(3071);
    EXPECT_TRUE(s.growOwed());
    s.growIfOwed();
    EXPECT_EQ(s.capacity(), 8192u);
    EXPECT_FALSE(s.growOwed());
    EXPECT_TRUE(s.reserveFor(12'287));
    EXPECT_EQ(s.capacity(), 12'288u);
    EXPECT_FALSE(s.reserveFor(12'288));
    EXPECT_EQ(s.capacity(), 12'288u);
    s.noteUse(12'287);
    EXPECT_FALSE(s.growOwed());   // at the cap: never owed
    s.growIfOwed();
    EXPECT_EQ(s.capacity(), 12'288u);
    EXPECT_TRUE(s.covers(12'287));
    EXPECT_FALSE(s.covers(12'288));
}

TEST(LeaseWheelRegression, StoreBoundsRegression) {
    constexpr uint32_t kTop = ((kLeaseMaxKeys + kLeaseEntryChunk - 1) / kLeaseEntryChunk) * kLeaseEntryChunk;
    static_assert(kTop > 0 && kTop < UINT32_MAX - kLeaseEntryChunk, "the rounded cap never wraps and keeps kLeaseNil out of range");
    {
        LeaseEntryStore s(kLeaseMaxKeys);
        EXPECT_EQ(s.cap(), kTop);
        EXPECT_GT(s.cap(), 0u);
        EXPECT_TRUE(s.reserveFor(5));
        EXPECT_EQ(s.capacity(), kLeaseEntryChunk);
        EXPECT_FALSE(s.reserveFor(kTop));
    }
    // above the limit: an assertion in a debug build; clamped to the same cap in a release build
    EXPECT_DEBUG_DEATH({
        LeaseEntryStore s(UINT32_MAX);
        EXPECT_EQ(s.cap(), kTop);
        EXPECT_TRUE(s.reserveFor(5));
    }, "");
    {
        LeaseEntryStore s(0);
        EXPECT_EQ(s.cap(), kLeaseEntryChunk);   // today's roundUp rule: at least one chunk
    }
}

TEST(LeaseWheelRegression, ChunkGrowthNoAllocRegression) {
    constexpr uint32_t kRows = 65'536;
    int64_t now = 1'000'000'000;
    LeaseTimers t(kRows, now);
    ASSERT_TRUE(t.reserveFor(kRows - 1));
    std::mt19937_64 rng(0xA110C);
    for (uint32_t i = 0; i < kRows; ++i) t.arm(i, satAdd(now, static_cast<int64_t>(rng() % (uint64_t{1} << 34))), kLeaseNoCheckpoint);
    uint64_t handed = 0;
    auto sink = [&](uint32_t) { ++handed; return true; };
    rune::testing::AllocWindow w;
    for (int op = 0; op < 1'000'000; ++op) {
        const uint64_t r = rng() % 16;
        const uint32_t rid = static_cast<uint32_t>(rng() % kRows);
        if (r < 7) t.rearm(rid, satAdd(now, static_cast<int64_t>(rng() % (uint64_t{1} << 34))));
        else if (r < 9) t.setState(rid, S::Emitting, 1);     // unlinks both wheels
        else if (r < 11) t.setState(rid, S::Armed);          // links again (a no-op if already Armed)
        else if (r < 14) t.scheduleCheckpoint(rid, satAdd(now, static_cast<int64_t>(rng() % (uint64_t{1} << 30))));
        else {
            now += static_cast<int64_t>(rng() % 50'000'000);
            t.pollDeadline(now, sink, UINT32_MAX);
            t.pollCheckpoint(now, sink, UINT32_MAX);
        }
        t.noteUse(static_cast<uint32_t>(rng() % (kRows / 4 * 3 - 2)));   // just below 75 % of the capacity
        if (t.growOwed()) ADD_FAILURE() << "growth owed below 75 %";
    }
    EXPECT_EQ(w.count(), 0u);
    EXPECT_GT(handed, 0u);
    t.growIfOwed();   // cold, outside the measured window (nothing owed here)
    EXPECT_EQ(t.capacity(), kRows);
}

TEST(LeaseWheelRegression, IdlePollOneCompareRegression) {
    // 1 048 576 rows; if this takes > 5 s in a debug build, use 262 144.
    constexpr uint32_t kRows = 1u << 20;
    const int64_t now = 1'000'000'000;
    LeaseTimers t(kRows, now);
    ASSERT_TRUE(t.reserveFor(kRows - 1));
    std::mt19937_64 rng(0x1D1E);
    for (uint32_t i = 0; i < kRows; ++i) {
        const int64_t span = 10'000'000'000 + static_cast<int64_t>(rng() % 3'590'000'000'001ull);   // U[10 s, 1 h]
        t.arm(i, now + span, kLeaseNoCheckpoint);
    }
    // the earliest slot any of these rows can be in starts at >= 10.24 s (level 1 block start of tick 11 000): far past 5 s
    EXPECT_GT(t.deadline().nextCheckNs(), now + 5000 * kMs);
    const uint64_t touched0 = t.deadline().touchedForTest();
    const uint64_t slots0   = t.deadline().slotsVisitedForTest();
    EXPECT_EQ(touched0, 0u);
    EXPECT_EQ(slots0, 0u);
    for (int k = 1; k <= 1000; ++k) {
        ASSERT_EQ(t.pollDeadline(now + int64_t{k} * 5'000'000, [](uint32_t) { return true; }, UINT32_MAX), 0u);
        ASSERT_GT(t.deadline().nextCheckNs(), now + 5000 * kMs);
    }
    EXPECT_EQ(t.deadline().touchedForTest(), 0u);
    EXPECT_EQ(t.deadline().slotsVisitedForTest(), 0u);
}

// The reviewer's wide model (probe2): cursor starts across every level boundary, advances of 0 / 1 ns / a day / 2^52 / 2^58 ns,
// deferral, random max, deadlines negative / 0 / INT64_MAX.  The wheel is checked after every poll: nothing early, nothing lost,
// due rows exactly the ready list, nextCheckNs never later than the earliest linked row.
TEST(LeaseWheelRegression, WideModelRegression) {
    const auto t0 = std::chrono::steady_clock::now();
    const int64_t starts[] = {0, 1023 * kMs, 1024 * kMs, ((int64_t{1} << 20) - 1) * kMs, (int64_t{1} << 20) * kMs,
                              ((int64_t{1} << 30) - 1) * kMs, (int64_t{1} << 30) * kMs + 1, ((int64_t{1} << 40) - 1) * kMs,
                              ((int64_t{1} << 40) + 777) * kMs, 9'000'000'000'000'000'000LL};
    constexpr uint64_t kSeeds = 20;
    constexpr int kOps = 2000;
    constexpr uint32_t kRows = 2000;
    uint64_t polls = 0, handedTot = 0, repairs = 0;
    bool failed = false;
    for (uint64_t seed = 1; seed <= kSeeds && !failed; ++seed) {
        for (const int64_t start : starts) {
            std::mt19937_64 rng(seed * 7919 + static_cast<uint64_t>(start));
            WheelRig<WK::Deadline> r(kRows, start);
            auto& w = r.w;
            int64_t now = start;
            std::map<uint32_t, int64_t> model;   // linked rid -> deadline
            auto span = [&]() -> int64_t {
                switch (rng() % 12) {
                    case 0: return -static_cast<int64_t>(rng() % (5 * kMs));
                    case 1: return 0;
                    case 2: return static_cast<int64_t>(rng() % 3) - 1 + (int64_t{1023} + static_cast<int64_t>(rng() % 3)) * kMs;
                    case 3: return ((int64_t{1} << 20) + static_cast<int64_t>(rng() % 3) - 1) * kMs + static_cast<int64_t>(rng() % 3) - 1;
                    case 4: return ((int64_t{1} << 30) + static_cast<int64_t>(rng() % 3) - 1) * kMs;
                    case 5: return static_cast<int64_t>(rng() % (int64_t{1} << 55));
                    case 6: return INT64_MAX;
                    case 7: return static_cast<int64_t>(rng() % (int64_t{1} << 40));
                    default: return static_cast<int64_t>(rng() % (3 * 1000 * kMs));
                }
            };
            auto adv = [&]() -> int64_t {
                switch (rng() % 10) {
                    case 0: return 0;
                    case 1: return 1;
                    case 2: return static_cast<int64_t>(rng() % (86'400LL * 1000 * kMs));   // up to a day
                    case 3: return static_cast<int64_t>(rng() % (int64_t{1} << 52));
                    case 4: return static_cast<int64_t>(rng() % (int64_t{1} << 58));
                    default: return static_cast<int64_t>(rng() % (2000 * kMs));
                }
            };
            for (int op = 0; op < kOps && !failed; ++op) {
                const uint32_t rid = static_cast<uint32_t>(rng() % kRows);
                const uint64_t k0 = rng() % 100;
                if (k0 < 45) {
                    const int64_t s  = span();
                    const int64_t dl = s < 0 ? now + s : satAdd(now, s);
                    w.arm(rid, dl);
                    model[rid] = dl;
                } else if (k0 < 60) {
                    w.cancel(rid);
                    model.erase(rid);
                } else {
                    now = satAdd(now, adv());
                    const uint32_t mx = rng() % 4 == 0 ? static_cast<uint32_t>(rng() % 5) : UINT32_MAX;
                    std::vector<uint32_t> got;
                    const int deferPct = rng() % 3 == 0 ? 30 : 0;
                    w.poll(now, [&](uint32_t id) {
                        const int64_t dl = r.store.at(id).deadlineNs;
                        if (!model.count(id)) { ADD_FAILURE() << "handed unknown rid " << id; failed = true; }
                        if (dl > now) { ADD_FAILURE() << "EARLY rid " << id << " dl " << dl << " now " << now; failed = true; }
                        if (static_cast<int>(rng() % 100) < deferPct) return false;
                        got.push_back(id);
                        return true;
                    }, mx);
                    ++polls;
                    handedTot += got.size();
                    for (uint32_t id : got) model.erase(id);
                    if (mx != UINT32_MAX) EXPECT_LE(got.size(), mx);
                    // lossless: every model row with tick <= nowTick is in the ready list, every other row is not, and
                    // nextCheckNs <= its tick in ns
                    for (const auto& [id, dl] : model) {
                        const bool ready = w.levelOf(id) == kLeaseReadyLevel;
                        const int64_t tk  = refTickSafe(dl);
                        const int64_t ntk = now <= 0 ? 0 : now / kMs;
                        if (!w.linked(id)) { ADD_FAILURE() << "LOST (unlinked) rid " << id; failed = true; break; }
                        if (tk <= ntk && !ready) {
                            ADD_FAILURE() << "LOST due-not-ready rid " << id << " dl " << dl << " now " << now << " start " << start << " seed " << seed;
                            failed = true;
                            break;
                        }
                        if (tk > ntk && ready) { ADD_FAILURE() << "ready-but-not-due rid " << id; failed = true; break; }
                        if (!ready) {
                            const int64_t tns = tk > INT64_MAX / kMs ? INT64_MAX : tk * kMs;
                            if (w.nextCheckNs() > tns) {
                                ADD_FAILURE() << "STALE-LATE nextCheck " << w.nextCheckNs() << " > " << tns << " rid " << id;
                                failed = true;
                                break;
                            }
                        } else if (w.nextCheckNs() != INT64_MIN) {
                            ADD_FAILURE() << "ready but nextCheck " << w.nextCheckNs();
                            failed = true;
                            break;
                        }
                    }
                }
            }
            repairs += w.invariantRepairsForTest();
            if (failed) { ADD_FAILURE() << "seed " << seed << " start " << start; break; }
        }
    }
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    std::printf("WideModel: %lld ms, polls=%llu handed=%llu\n", static_cast<long long>(ms), static_cast<unsigned long long>(polls),
                static_cast<unsigned long long>(handedTot));
    EXPECT_EQ(repairs, 0u);
    EXPECT_GT(handedTot, 0u);
    EXPECT_LT(ms, 5000);
}
