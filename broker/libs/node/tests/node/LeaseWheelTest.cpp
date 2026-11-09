// LeaseWheelTest.cpp - lease topics Task 9: the lease wheels, the entry store and the four-state machine (spec 9.1, 9.5, 9.6, 10).
// Standard library + gtest only: the headers under test include nothing else.
#include <rune/node/lease/LeaseEntryStore.h>
#include <rune/node/lease/LeaseWheel.h>

#include "AllocWindow.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <random>
#include <vector>

namespace {

using namespace rune::node::lease;
using S = LeaseEntryState;
using E = LeaseEntryEvent;
using A = LeaseAction;

constexpr int64_t kMs = 1'000'000;

// The reference tick of a deadline, written independently of the wheel: ceil(ns / 1 ms), negative = 0.
int64_t refTickSafe(int64_t ns) { return ns <= 0 ? 0 : ns / kMs + (ns % kMs != 0); }

template <class W>
std::vector<uint32_t> pollAll(W& w, int64_t now, uint32_t max = 200000) {
    std::vector<uint32_t> out;
    w.poll(now, [&](uint32_t rid) { out.push_back(rid); return true; }, max);
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
                LeaseTimers t(4096);
                t.deadline.reset(nowNs);
                const int64_t deadline = nowNs + ticks * kMs;
                const int64_t dTick    = refTickSafe(deadline);
                // the lowest level whose 1024-block distance from cur (= the start tick) is < 1024
                int expect = 4;
                for (int k = 0; k <= 4; ++k) {
                    if (((dTick >> (10 * k)) - (startMs >> (10 * k))) < 1024) { expect = k; break; }
                }
                t.deadline.arm(7, deadline);
                ASSERT_EQ(t.deadline.levelOf(7), expect) << "start " << startMs << " level " << l << " delta " << delta;
                if (deadline - 1 >= 0) {
                    EXPECT_TRUE(pollAll(t.deadline, deadline - 1).empty()) << "early: start " << startMs << " l " << l << " d " << delta;
                    EXPECT_TRUE(t.deadline.linked(7));
                }
                const auto got = pollAll(t.deadline, deadline);
                ASSERT_EQ(got.size(), 1u) << "start " << startMs << " level " << l << " delta " << delta;
                EXPECT_EQ(got[0], 7u);
                EXPECT_FALSE(t.deadline.linked(7));
            }
        }
    }
}

TEST(LeaseWheelRegression, BoundariesRearmCancelAndMaxTtlRegression) {
    const int64_t far = int64_t{1'500'000'000};   // ticks: level 3 from tick 0
    LeaseTimers t(4096);
    t.deadline.reset(0);
    // earlier re-arm of a level-3 row
    t.deadline.arm(1, far * kMs);
    ASSERT_EQ(t.deadline.levelOf(1), 3);
    t.deadline.arm(1, 5 * kMs);
    EXPECT_EQ(pollAll(t.deadline, 10 * kMs), std::vector<uint32_t>{1});
    EXPECT_TRUE(pollAll(t.deadline, far * kMs).empty());
    // later re-arm
    t.deadline.arm(2, far * kMs);
    t.deadline.arm(2, 2 * far * kMs);
    EXPECT_TRUE(pollAll(t.deadline, far * kMs).empty());
    EXPECT_EQ(pollAll(t.deadline, 2 * far * kMs), std::vector<uint32_t>{2});
    // cancel before the cascade
    const int64_t base = 2 * far * kMs;
    t.deadline.arm(3, satAdd(base, far * kMs));
    t.deadline.cancel(3);
    EXPECT_FALSE(t.deadline.linked(3));
    EXPECT_TRUE(pollAll(t.deadline, satAdd(base, far * kMs + 1000 * kMs)).empty());

    // MaxLeaseTTL and +1 ms
    LeaseTimers m(4096);
    const int64_t now = 777 * kMs;
    m.deadline.reset(now);
    const int64_t ttl = int64_t{9'000'000'000'000} * kMs;   // MaxLeaseTTL: 9e12 ms as ns
    m.deadline.arm(1, satAdd(now, ttl));
    m.deadline.arm(2, satAdd(now, satAdd(ttl, kMs)));
    EXPECT_EQ(m.deadline.levelOf(1), 4);
    EXPECT_EQ(m.deadline.levelOf(2), 4);
    EXPECT_TRUE(pollAll(m.deadline, now + int64_t{400} * 86'400 * 1'000'000'000).empty());
    EXPECT_TRUE(m.deadline.linked(1));
    EXPECT_TRUE(m.deadline.linked(2));
}

TEST(LeaseWheelRegression, ReferenceModelRegression) {
    std::mt19937_64 rng(0x1EA5E);
    constexpr uint32_t kRows = 5000;
    LeaseTimers t(kRows);
    ASSERT_TRUE(t.store.reserveFor(kRows - 1));
    int64_t now = 1'000'000'000;
    t.deadline.reset(now);
    std::map<uint32_t, int64_t> model;   // rid -> dTick
    auto pick = [&](uint64_t n) { return rng() % n; };
    for (int op = 0; op < 100'000; ++op) {
        const uint64_t r = pick(100);
        const uint32_t rid = static_cast<uint32_t>(pick(kRows));
        if (r < 40) {   // arm or re-arm
            int64_t span;
            const uint64_t k = pick(100);
            if (k < 10) span = static_cast<int64_t>(pick(2'000'000'000));       // dense due
            else if (k < 20) span = static_cast<int64_t>(pick(int64_t{1} << 46));   // reaches levels 2 and 3 (up to ~19 h)
            else span = static_cast<int64_t>(pick(int64_t{1} << 34));
            const int64_t d = satAdd(now, span);
            t.deadline.arm(rid, d);
            model[rid] = refTickSafe(d);
        } else if (r < 55) {   // cancel
            t.deadline.cancel(rid);
            model.erase(rid);
        } else {   // advance and poll
            now += static_cast<int64_t>(pick(3'000'000'000));
            std::vector<uint32_t> got = pollAll(t.deadline, now);
            std::sort(got.begin(), got.end());
            std::vector<uint32_t> want;
            for (const auto& [id, tick] : model) if (tick <= now / kMs) want.push_back(id);
            ASSERT_EQ(got, want) << "op " << op << " now " << now;
            for (const uint32_t id : got) {
                ASSERT_LE(t.store.at(id).deadlineNs, now) << "handed early, rid " << id;
                model.erase(id);
            }
        }
    }
    // drain: everything left comes out exactly at its tick
    for (const auto& [id, tick] : model) EXPECT_TRUE(t.deadline.linked(id));
}

TEST(LeaseWheelRegression, MaxAndDeferRegression) {
    LeaseTimers t(4096);
    t.deadline.reset(0);
    for (uint32_t i = 0; i < 10; ++i) t.deadline.arm(i, 5 * kMs);
    std::vector<uint32_t> first;
    EXPECT_EQ(t.deadline.poll(10 * kMs, [&](uint32_t r) { first.push_back(r); return true; }, 3), 3u);
    ASSERT_EQ(first.size(), 3u);
    std::vector<uint32_t> rest;
    for (uint32_t i = 0; i < 10; ++i) {
        if (std::find(first.begin(), first.end(), i) != first.end()) continue;
        rest.push_back(i);
        EXPECT_TRUE(t.deadline.linked(i));
        EXPECT_EQ(t.deadline.levelOf(i), 5);
    }
    ASSERT_EQ(rest.size(), 7u);
    const uint32_t deferred = rest[0];
    std::vector<uint32_t> second;
    EXPECT_EQ(t.deadline.poll(11 * kMs, [&](uint32_t r) { if (r == deferred) return false; second.push_back(r); return true; },
                              UINT32_MAX), 6u);
    EXPECT_TRUE(t.deadline.linked(deferred));
    EXPECT_EQ(t.deadline.levelOf(deferred), 5);
    EXPECT_LE(t.store.at(deferred).deadlineNs, 11 * kMs);
    EXPECT_EQ(pollAll(t.deadline, 12 * kMs), std::vector<uint32_t>{deferred});
    EXPECT_FALSE(t.deadline.linked(deferred));
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
        {S::Armed, E::Arm, S::Armed, A::None},
        {S::Armed, E::OwnRecordFolds, S::Armed, A::None},
        {S::Armed, E::OtherUpdateFolds, S::Armed, A::None},
        {S::Armed, E::EndFolds, S::Free, A::FreeAnswerParkedNotFound},
        {S::Armed, E::Demotion, S::Free, A::DiscardAnswerParkedNotLeader},
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
        check(S::Armed, E::CheckpointDue, gate ? S::Checkpointing : S::Armed, gate ? A::AppendCheckpoint : A::None);
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
        }
        check(S::Checkpointing, E::OwnRecordFolds, S::Armed, A::ReturnArmedReplayParked);
        check(S::Clearing, E::OwnRecordFolds, S::Armed, A::ReturnArmedAnswerParkedOk);
        check(S::Emitting, E::OwnRecordFolds, S::Free, A::FreeAnswerParkedNotFound);
    }
    EXPECT_EQ(cells, 8 * (13 + 3 + 3 * 7 + 3));   // all 40 table cells, every bool combination

    // R4: AppendExpire / AppendCheckpoint only from Armed.
    for (int s = 0; s <= 4; ++s) {
        for (int e = 0; e <= 7; ++e) {
            for (int mask = 0; mask < 8; ++mask) {
                const LeaseStepOut out = leaseStep({static_cast<S>(s), static_cast<E>(e), bool(mask & 1), bool(mask & 2), bool(mask & 4)});
                if (out.action == A::AppendExpire || out.action == A::AppendCheckpoint) EXPECT_EQ(static_cast<S>(s), S::Armed);
            }
        }
    }
}

TEST(LeaseWheelRegression, ArmedOnlyLinkedRegression) {
    const S states[] = {S::Emitting, S::Checkpointing, S::Clearing};
    LeaseTimers t(4096);
    t.deadline.reset(0);
    t.checkpoint.reset(0);
    const int64_t d = 20 * kMs, c = 10 * kMs;
    for (uint32_t i = 0; i < 3; ++i) {
        t.rearm(i, d);
        t.scheduleCheckpoint(i, c);
        EXPECT_FALSE(t.deadline.linked(i));   // Free: stored, not linked
        t.setState(i, S::Armed);
        ASSERT_TRUE(t.deadline.linked(i));
        ASSERT_TRUE(t.checkpoint.linked(i));
        t.setState(i, states[i], 42);
        EXPECT_FALSE(t.deadline.linked(i));
        EXPECT_FALSE(t.checkpoint.linked(i));
        EXPECT_EQ(t.store.at(i).inflightOffset, 42);
        EXPECT_EQ(t.store.at(i).state, states[i]);
    }
    EXPECT_TRUE(pollAll(t.deadline, 50 * kMs).empty());
    EXPECT_TRUE(pollAll(t.checkpoint, 50 * kMs).empty());
    for (uint32_t i = 0; i < 3; ++i) {
        t.setState(i, S::Armed);
        EXPECT_EQ(t.store.at(i).inflightOffset, -1);
        EXPECT_TRUE(t.deadline.linked(i));
        EXPECT_TRUE(t.checkpoint.linked(i));
        EXPECT_EQ(t.store.at(i).deadlineNs, d);
        EXPECT_EQ(t.store.at(i).checkpointNs, c);
    }
    auto dg = pollAll(t.deadline, 50 * kMs);
    auto cg = pollAll(t.checkpoint, 50 * kMs);
    std::sort(dg.begin(), dg.end());
    std::sort(cg.begin(), cg.end());
    EXPECT_EQ(dg, (std::vector<uint32_t>{0, 1, 2}));
    EXPECT_EQ(cg, (std::vector<uint32_t>{0, 1, 2}));

    // rearm while Clearing stores but does not link
    t.deadline.reset(50 * kMs);
    t.checkpoint.reset(50 * kMs);
    t.rearm(3, 100 * kMs);
    t.scheduleCheckpoint(3, 90 * kMs);
    t.setState(3, S::Armed);
    t.setState(3, S::Clearing, 9);
    t.rearm(3, 200 * kMs);
    t.scheduleCheckpoint(3, 150 * kMs);
    EXPECT_EQ(t.store.at(3).deadlineNs, 200 * kMs);
    EXPECT_EQ(t.store.at(3).checkpointNs, 150 * kMs);
    EXPECT_FALSE(t.deadline.linked(3));
    EXPECT_FALSE(t.checkpoint.linked(3));
    EXPECT_TRUE(pollAll(t.deadline, 150 * kMs).empty());
    t.setState(3, S::Armed);
    EXPECT_TRUE(t.deadline.linked(3));
    EXPECT_TRUE(pollAll(t.deadline, 199 * kMs).empty());
    EXPECT_EQ(pollAll(t.deadline, 200 * kMs), std::vector<uint32_t>{3});

    // demotion: every entry Free, nothing linked
    t.rearm(4, 400 * kMs);
    t.setState(4, S::Armed);
    ASSERT_TRUE(t.deadline.linked(4));
    t.demote();
    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(t.store.at(i).state, S::Free);
        EXPECT_FALSE(t.deadline.linked(i));
        EXPECT_FALSE(t.checkpoint.linked(i));
    }
    EXPECT_TRUE(pollAll(t.deadline, 1000 * kMs).empty());
    EXPECT_TRUE(pollAll(t.checkpoint, 1000 * kMs).empty());
}

TEST(LeaseWheelRegression, SaturationRegression) {
    LeaseTimers t(4096);
    const int64_t now = 777 * kMs;
    t.deadline.reset(now);
    const int64_t ttl      = 1000;
    const int64_t deadline = satAdd(INT64_MAX - ttl, ttl + 5);
    ASSERT_EQ(deadline, INT64_MAX);
    t.deadline.arm(1, deadline);
    EXPECT_EQ(t.deadline.levelOf(1), 4);
    EXPECT_TRUE(pollAll(t.deadline, now + int64_t{365} * 86'400 * 1'000'000'000).empty());
    EXPECT_TRUE(pollAll(t.deadline, INT64_MAX - 1).empty());
    EXPECT_TRUE(t.deadline.linked(1));
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

TEST(LeaseWheelRegression, ChunkGrowthNoAllocRegression) {
    constexpr uint32_t kRows = 65'536;
    LeaseTimers t(kRows);
    ASSERT_TRUE(t.store.reserveFor(kRows - 1));
    std::mt19937_64 rng(0xA110C);
    int64_t now = 1'000'000'000;
    t.deadline.reset(now);
    t.checkpoint.reset(now);
    for (uint32_t i = 0; i < kRows; ++i) t.deadline.arm(i, satAdd(now, static_cast<int64_t>(rng() % (uint64_t{1} << 34))));
    uint64_t handed = 0;
    auto sink = [&](uint32_t) { ++handed; return true; };
    rune::testing::AllocWindow w;
    for (int op = 0; op < 1'000'000; ++op) {
        const uint64_t r = rng() % 16;
        const uint32_t rid = static_cast<uint32_t>(rng() % kRows);
        if (r < 8) t.deadline.arm(rid, satAdd(now, static_cast<int64_t>(rng() % (uint64_t{1} << 34))));
        else if (r < 11) t.deadline.cancel(rid);
        else if (r < 14) t.checkpoint.arm(rid, satAdd(now, static_cast<int64_t>(rng() % (uint64_t{1} << 30))));
        else {
            now += static_cast<int64_t>(rng() % 50'000'000);
            t.deadline.poll(now, sink, UINT32_MAX);
            t.checkpoint.poll(now, sink, UINT32_MAX);
        }
    }
    EXPECT_EQ(w.count(), 0u);
    EXPECT_GT(handed, 0u);
}

TEST(LeaseWheelRegression, IdlePollOneCompareRegression) {
    // 1 048 576 rows; if this takes > 5 s in a debug build, use 262 144.
    constexpr uint32_t kRows = 1u << 20;
    LeaseTimers t(kRows);
    ASSERT_TRUE(t.store.reserveFor(kRows - 1));
    std::mt19937_64 rng(0x1D1E);
    const int64_t now = 1'000'000'000;
    t.deadline.reset(now);
    for (uint32_t i = 0; i < kRows; ++i) {
        const int64_t span = 10'000'000'000 + static_cast<int64_t>(rng() % 3'590'000'000'001ull);   // U[10 s, 1 h]
        t.deadline.arm(i, now + span);
    }
    const uint64_t touched0 = t.deadline.touchedForTest();
    const uint64_t slots0   = t.deadline.slotsVisitedForTest();
    EXPECT_EQ(touched0, 0u);
    EXPECT_EQ(slots0, 0u);
    for (int k = 1; k <= 1000; ++k) {
        ASSERT_EQ(t.deadline.poll(now + int64_t{k} * 5'000'000, [](uint32_t) { return true; }, UINT32_MAX), 0u);
    }
    EXPECT_EQ(t.deadline.touchedForTest(), 0u);
    EXPECT_EQ(t.deadline.slotsVisitedForTest(), 0u);
}
