// NsLimitAggregatorRegressionTest.cpp - namespaces Phase 2 Task 14 (D25, D26, D28): the control thread's limit aggregator,
// driven deterministically (injected clock, one doWork per tick, no sleeps, hard iteration caps).
//
// Interaction matrix (limit kind x state x timing):
//   | limit                | usage                          | timing                         | expected                                   | test
//   |----------------------|--------------------------------|--------------------------------|--------------------------------------------|-----
//   | produce-bytes/s      | below / at / above limit x dt  | one tick                       | no / no / Throttle{until>now}              | LimitEnforcementTableRegression.RatesRegression
//   | every rate kind      | a burst over the limit         | many ticks, no new traffic     | ONE Throttle, then ONE clear               | NsLimitAggregatorRegression.OverRateEmitsThrottleOnceRegression
//   | produce-bytes/s      | the throttle expired           | the debt decays to 0           | Throttle{0} once                           | NsLimitAggregatorRegression.ClearsWhenUnderRegression
//   | produce-bytes/s      | steady, under the limit        | 200 ticks                      | nothing is broadcast                       | NsLimitAggregatorRegression.NoBroadcastWhenUnchangedRegression
//   | produce-bytes/s      | excess == one tick's allowance | one tick                       | untilNs == now + one tick                  | NsLimitAggregatorRegression.RetryAfterIsNextTickRegression
//   | produce-bytes/s      | throttled                      | limit raised / removed mid-tick | the next tick clears (or shortens) it      | NsLimitAggregatorRegression.LimitRaisedMidTickRegression
//   | max-disk-bytes       | node total of two workers      | below / at / above, then drop  | Throttle{Disk, INT64_MAX} then {Disk, 0}   | NsLimitAggregatorRegression.DiskIsTheNodeTotalRegression
//   | max-view-memory      | node total of two workers      | below / above / below          | ViewMemOver on, then off                   | NsLimitAggregatorRegression.ViewMemIsTheNodeTotalRegression
//   | any                  | a report for a stale nsId      | slot recreated                 | dropped and counted, never applied         | NsLimitAggregatorRegression.StaleNsIdReportDroppedRegression
//   | any                  | slot released and reassigned   | old debt                       | the new owner starts clear                 | NsLimitAggregatorRegression.RecreatedSlotStartsClearRegression
#include <gtest/gtest.h>

#include <rune/node/NsLimitAggregator.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstdint>
#include <limits>
#include <vector>

#include "support/LimitEnforcementTable.h"

using namespace rune::node;
using rune::protocol::NamespaceLimits;

namespace {

constexpr int64_t kTick = 10'000'000;   // 10 ms
constexpr uint16_t kSlot = 1;
constexpr uint32_t kNs = 7;

struct Rig {
    NsLimitAggregator agg;
    int64_t now = 1'000'000'000;
    std::vector<Throttle> throttles;
    std::vector<ViewMemOver> viewMem;
    uint64_t cum[kNsMetrics] = {};

    explicit Rig(const NamespaceLimits& l, uint32_t sources = 1) {
        agg.configure(8, sources, kTick);
        (void)agg.doWork(now);   // baseline
        agg.onSlotAssign(kSlot, kNs, l);
    }
    /// Advance one tick and collect what was broadcast.
    int tick() {
        now += kTick;
        const int n = agg.doWork(now);
        agg.drain([&](const Throttle& t) { throttles.push_back(t); }, [&](const ViewMemOver& v) { viewMem.push_back(v); });
        return n;
    }
    void add(NsMetric m, uint64_t delta, uint32_t source = 0) {
        cum[static_cast<std::size_t>(m)] += delta;
        EXPECT_TRUE(agg.report(source, kSlot, kNs, m, cum[static_cast<std::size_t>(m)]));
    }
    [[nodiscard]] std::size_t countOf(ThrottleKind k) const {
        std::size_t n = 0;
        for (const Throttle& t : throttles) n += t.kind == k ? 1 : 0;
        return n;
    }
};

NamespaceLimits produceBytes(uint64_t perSec) { NamespaceLimits l; l.produceBytesPerSec = perSec; return l; }

struct RateCase { const char* field; ThrottleKind kind; NsMetric metric; };
constexpr RateCase kCases[] = {
    {"produce-bytes-per-sec", ThrottleKind::ProduceBytes, NsMetric::ProduceBytes},
    {"produce-records-per-sec", ThrottleKind::ProduceRecords, NsMetric::ProduceRecords},
    {"fetch-bytes-per-sec", ThrottleKind::FetchBytes, NsMetric::FetchBytes},
    {"request-ops-per-sec", ThrottleKind::RequestOps, NsMetric::RequestOps},
    {"query-cycles-per-sec", ThrottleKind::QueryCycles, NsMetric::QueryCycles},
};

void setRate(NamespaceLimits& l, std::size_t index, uint64_t v) {
    switch (index) {
        case 13: l.produceBytesPerSec = v; break;
        case 14: l.produceRecordsPerSec = v; break;
        case 15: l.fetchBytesPerSec = v; break;
        case 16: l.requestOpsPerSec = v; break;
        case 17: l.queryCyclesPerSec = v; break;
        default: FAIL() << "not a rate index " << index;
    }
}

}  // namespace

// Below / At / Above for every rate row of the table: usage in one tick of u*limit*dt with u = 1/2, 1, 1 + 1/limit-unit.
TEST(LimitEnforcementTableRegression, RatesRegression) {
    using namespace rune::node::test;
    std::size_t rows = 0;
    for (const LimitRow& row : kLimitRows) {
        if (row.area != LimitArea::Rate) continue;
        ++rows;
        EXPECT_EQ(row.codeAbove, 53);
        const RateCase* rc = nullptr;
        for (const RateCase& c : kCases) if (c.field == row.field) rc = &c;
        ASSERT_NE(rc, nullptr) << row.field;
        const uint64_t allowedPerTick = row.limit * kTick / 1'000'000'000ull;   // limit x elapsed
        ASSERT_GE(allowedPerTick, 2u) << row.field << ": the row's limit must allow >= 2 units per tick";
        for (const LimitCell cell : {LimitCell::Below, LimitCell::At, LimitCell::Above}) {
            NamespaceLimits l;
            setRate(l, row.index, row.limit);
            Rig rig(l);
            const uint64_t usage = cell == LimitCell::Below ? allowedPerTick / 2 : cell == LimitCell::At ? allowedPerTick : allowedPerTick + 1;
            rig.add(rc->metric, usage);
            rig.tick();
            if (cell == LimitCell::Above) {
                ASSERT_EQ(rig.throttles.size(), 1u) << row.field << " Above";
                EXPECT_EQ(rig.throttles[0].kind, rc->kind);
                EXPECT_EQ(rig.throttles[0].slot, kSlot);
                EXPECT_EQ(rig.throttles[0].nsId, kNs);
                EXPECT_GT(rig.throttles[0].untilNs, rig.now) << "the retry-after is in the future";
            } else {
                EXPECT_TRUE(rig.throttles.empty()) << row.field << (cell == LimitCell::Below ? " Below" : " At");
            }
        }
    }
    EXPECT_EQ(rows, 5u) << "one row per rate limit";
}

TEST(NsLimitAggregatorRegression, OverRateEmitsThrottleOnceRegression) {
    for (const RateCase& c : kCases) {
        NamespaceLimits l;
        for (const auto& row : rune::node::test::kLimitRows) if (row.field == c.field) setRate(l, row.index, 1000);
        Rig rig(l);
        rig.add(c.metric, 100);   // limit x tick = 10: debt 90 -> 90 ms
        rig.tick();
        ASSERT_EQ(rig.throttles.size(), 1u) << c.field;
        EXPECT_EQ(rig.throttles[0].untilNs, rig.now + 90'000'000) << c.field;
        for (int i = 0; i < 7; ++i) rig.tick();   // no new traffic: the deadline only decays with time
        EXPECT_EQ(rig.throttles.size(), 1u) << c.field << ": not rebroadcast while it merely decays";
    }
}

TEST(NsLimitAggregatorRegression, ClearsWhenUnderRegression) {
    Rig rig(produceBytes(1000));
    rig.add(NsMetric::ProduceBytes, 100);
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 1u);
    int guard = 0;
    while (rig.throttles.size() < 2 && ++guard < 100) rig.tick();
    ASSERT_LT(guard, 100) << "the throttle never cleared";
    EXPECT_EQ(rig.throttles[1].kind, ThrottleKind::ProduceBytes);
    EXPECT_EQ(rig.throttles[1].untilNs, 0);
    EXPECT_EQ(rig.agg.debtOf(kSlot, ThrottleKind::ProduceBytes), 0u);
    for (int i = 0; i < 20; ++i) rig.tick();
    EXPECT_EQ(rig.throttles.size(), 2u) << "exactly one throttle and one clear";
}

TEST(NsLimitAggregatorRegression, NoBroadcastWhenUnchangedRegression) {
    Rig rig(produceBytes(1000));
    for (int i = 0; i < 200; ++i) { rig.add(NsMetric::ProduceBytes, 9); rig.tick(); }   // 9 < 10 allowed per tick
    EXPECT_TRUE(rig.throttles.empty());
    EXPECT_TRUE(rig.viewMem.empty());
    EXPECT_EQ(rig.agg.pending(), 0u);
}

TEST(NsLimitAggregatorRegression, RetryAfterIsNextTickRegression) {
    Rig rig(produceBytes(1000));
    rig.add(NsMetric::ProduceBytes, 20);   // 2 ticks' allowance: the excess is exactly one tick's worth
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 1u);
    EXPECT_EQ(rig.throttles[0].untilNs - rig.now, kTick) << "retry-after == the excess / limit == one tick";
}

TEST(NsLimitAggregatorRegression, LimitRaisedMidTickRegression) {
    Rig rig(produceBytes(1000));
    rig.add(NsMetric::ProduceBytes, 100);
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 1u);
    // SetLimits lands between the tick and the next one: raised x1000.
    rig.agg.onSlotAssign(kSlot, kNs, produceBytes(1'000'000));
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 2u);
    EXPECT_EQ(rig.throttles[1].untilNs, 0) << "the raised limit clears the throttle at the next tick";
    // Removed (0 = unlimited) while throttled.
    Rig r2(produceBytes(1000));
    r2.add(NsMetric::ProduceBytes, 100);
    r2.tick();
    r2.agg.onSlotAssign(kSlot, kNs, NamespaceLimits{});
    r2.tick();
    ASSERT_EQ(r2.throttles.size(), 2u);
    EXPECT_EQ(r2.throttles[1].untilNs, 0);
    // Lowered: a slower rate prolongs the deadline and is rebroadcast.
    Rig r3(produceBytes(1000));
    r3.add(NsMetric::ProduceBytes, 100);
    r3.tick();
    const int64_t first = r3.throttles[0].untilNs;
    r3.agg.onSlotAssign(kSlot, kNs, produceBytes(100));
    r3.tick();
    ASSERT_EQ(r3.throttles.size(), 2u);
    EXPECT_GT(r3.throttles[1].untilNs, first);
}

TEST(NsLimitAggregatorRegression, DiskIsTheNodeTotalRegression) {
    NamespaceLimits l;
    l.maxDiskBytes = 1000;
    Rig rig(l, /*sources*/ 2);
    EXPECT_TRUE(rig.agg.report(0, kSlot, kNs, NsMetric::DiskBytes, 400));
    EXPECT_TRUE(rig.agg.report(1, kSlot, kNs, NsMetric::DiskBytes, 500));   // 900 < 1000: neither worker alone is the total
    rig.tick();
    EXPECT_TRUE(rig.throttles.empty());
    EXPECT_TRUE(rig.agg.report(1, kSlot, kNs, NsMetric::DiskBytes, 600));   // 1000 == limit: at the limit is over (>=)
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 1u);
    EXPECT_EQ(rig.throttles[0].kind, ThrottleKind::Disk);
    EXPECT_EQ(rig.throttles[0].untilNs, std::numeric_limits<int64_t>::max());
    rig.tick();
    EXPECT_EQ(rig.throttles.size(), 1u) << "broadcast on change only";
    // Retention frees bytes on worker 1: the node total falls under the limit.
    EXPECT_TRUE(rig.agg.report(1, kSlot, kNs, NsMetric::DiskBytes, 200));   // total 600
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 2u);
    EXPECT_EQ(rig.throttles[1].untilNs, 0);
    EXPECT_EQ(rig.agg.totalOf(kSlot, NsMetric::DiskBytes), 600u);
}

TEST(NsLimitAggregatorRegression, ViewMemIsTheNodeTotalRegression) {
    NamespaceLimits l;
    l.maxViewMemoryBytes = 1000;
    Rig rig(l, 2);
    EXPECT_TRUE(rig.agg.report(0, kSlot, kNs, NsMetric::ViewMemBytes, 600));
    EXPECT_TRUE(rig.agg.report(1, kSlot, kNs, NsMetric::ViewMemBytes, 400));
    rig.tick();
    EXPECT_TRUE(rig.viewMem.empty()) << "== the limit is not over";
    EXPECT_TRUE(rig.agg.report(1, kSlot, kNs, NsMetric::ViewMemBytes, 401));
    rig.tick();
    ASSERT_EQ(rig.viewMem.size(), 1u);
    EXPECT_TRUE(rig.viewMem[0].on);
    EXPECT_EQ(rig.viewMem[0].nsId, kNs);
    rig.tick();
    EXPECT_EQ(rig.viewMem.size(), 1u);
    EXPECT_TRUE(rig.agg.report(1, kSlot, kNs, NsMetric::ViewMemBytes, 0));
    rig.tick();
    ASSERT_EQ(rig.viewMem.size(), 2u);
    EXPECT_FALSE(rig.viewMem[1].on);
}

TEST(NsLimitAggregatorRegression, StaleNsIdReportDroppedRegression) {
    Rig rig(produceBytes(1000));
    const uint64_t before = rig.agg.staleReports();
    EXPECT_FALSE(rig.agg.report(0, kSlot, kNs + 1, NsMetric::ProduceBytes, 1'000'000));   // another namespace's tag
    EXPECT_FALSE(rig.agg.report(0, 3, 9, NsMetric::ProduceBytes, 5));                      // a free slot
    EXPECT_FALSE(rig.agg.report(5, kSlot, kNs, NsMetric::ProduceBytes, 5));                // no such source
    EXPECT_EQ(rig.agg.staleReports(), before + 3);
    rig.tick();
    EXPECT_TRUE(rig.throttles.empty()) << "a stale report moved nothing";
    EXPECT_EQ(rig.agg.totalOf(kSlot, NsMetric::ProduceBytes), 0u);
}

TEST(NsLimitAggregatorRegression, RecreatedSlotStartsClearRegression) {
    Rig rig(produceBytes(1000));
    rig.add(NsMetric::ProduceBytes, 100);
    rig.tick();
    ASSERT_EQ(rig.throttles.size(), 1u);
    rig.agg.onSlotFreed(kSlot, kNs);
    rig.agg.onSlotAssign(kSlot, kNs + 1, produceBytes(1000));
    EXPECT_EQ(rig.agg.debtOf(kSlot, ThrottleKind::ProduceBytes), 0u);
    EXPECT_EQ(rig.agg.sentUntil(kSlot, ThrottleKind::ProduceBytes), 0);
    // The old owner's in-flight report is refused; the new owner's first report counts from zero.
    EXPECT_FALSE(rig.agg.report(0, kSlot, kNs, NsMetric::ProduceBytes, 500));
    EXPECT_TRUE(rig.agg.report(0, kSlot, kNs + 1, NsMetric::ProduceBytes, 5));
    rig.tick();
    EXPECT_EQ(rig.throttles.size(), 1u) << "no throttle for the new owner";
    // A release with the wrong nsId frees nothing.
    const uint64_t stale = rig.agg.staleReports();
    rig.agg.onSlotFreed(kSlot, kNs);
    EXPECT_EQ(rig.agg.staleReports(), stale + 1);
}

TEST(NsLimitAggregatorRegression, ChurnEmitsBoundedEventsRegression) {
    NamespaceLimits l = produceBytes(1000);
    l.maxDiskBytes = 10;
    l.maxViewMemoryBytes = 10;
    Rig rig(l);
    // Worst case: every kind of the slot changes at once, repeatedly; the event buffer is reserved in configure() for
    // slots x kinds, so the broadcast count per tick is bounded and a drained buffer is empty.
    for (int i = 0; i < 50; ++i) {
        rig.add(NsMetric::ProduceBytes, (i % 2) ? 1'000'000 : 0);
        EXPECT_TRUE(rig.agg.report(0, kSlot, kNs, NsMetric::DiskBytes, (i % 2) ? 100 : 0));
        EXPECT_TRUE(rig.agg.report(0, kSlot, kNs, NsMetric::ViewMemBytes, (i % 2) ? 100 : 0));
        rig.tick();
        EXPECT_EQ(rig.agg.pending(), 0u) << "drained";
    }
    EXPECT_GT(rig.throttles.size(), 3u);
}
