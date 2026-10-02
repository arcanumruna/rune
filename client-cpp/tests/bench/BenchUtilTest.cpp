// tests/bench/BenchUtilTest.cpp — benchmark building blocks (pure, no Aeron).
#include "LogLinearHistogram.h"
#include "OpenLoopSchedule.h"
#include "ProcStat.h"
#include "SeqTimestampRing.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <type_traits>
#include <vector>

using namespace rune::bench;

TEST(LogLinearHistogram, EmptyReportsZero) {
    LogLinearHistogram h;
    EXPECT_EQ(h.count(), 0u);
    EXPECT_EQ(h.valueAtQuantile(0.99), 0u);
    EXPECT_EQ(h.max(), 0u);
}

TEST(LogLinearHistogram, ExactBelow256) {
    LogLinearHistogram h;
    for (int64_t v = 0; v < 256; ++v) h.record(v);
    EXPECT_EQ(h.count(), 256u);
    EXPECT_EQ(h.valueAtQuantile(0.5), 127u);
    EXPECT_EQ(h.valueAtQuantile(1.0), 255u);
    EXPECT_EQ(h.min(), 0u);
}

TEST(LogLinearHistogram, RelativeErrorBelowOnePercent) {
    for (uint64_t v : {256ULL, 1000ULL, 12'345ULL, 1'000'000ULL, 987'654'321ULL, 3'700'000'000'000ULL}) {
        const int i = LogLinearHistogram::index(v);
        const uint64_t ub = LogLinearHistogram::upperBound(i);
        EXPECT_GE(ub, v);
        EXPECT_LE(ub - v, v / 128) << v;
        if (i > 0) EXPECT_LT(LogLinearHistogram::upperBound(i - 1), v) << v;
    }
}

TEST(LogLinearHistogram, QuantilesOfAUniformRun) {
    LogLinearHistogram h;
    for (int64_t v = 1; v <= 100'000; ++v) h.record(v);
    auto near = [](uint64_t got, double want) { return std::abs(static_cast<double>(got) - want) <= want / 128 + 1; };
    EXPECT_TRUE(near(h.valueAtQuantile(0.50), 50'000)) << h.valueAtQuantile(0.50);
    EXPECT_TRUE(near(h.valueAtQuantile(0.99), 99'000)) << h.valueAtQuantile(0.99);
    EXPECT_TRUE(near(h.valueAtQuantile(0.999), 99'900)) << h.valueAtQuantile(0.999);
    EXPECT_EQ(h.valueAtQuantile(1.0), 100'000u);
    EXPECT_EQ(h.max(), 100'000u);
}

TEST(LogLinearHistogram, HugeValuesClampButMaxStaysExact) {
    LogLinearHistogram h;
    h.record(int64_t{1} << 50);
    h.record(-5);  // negative clamps to 0
    EXPECT_EQ(h.count(), 2u);
    EXPECT_EQ(h.max(), uint64_t{1} << 50);
    EXPECT_EQ(h.valueAtQuantile(1.0), uint64_t{1} << 50);
    EXPECT_EQ(h.valueAtQuantile(0.5), 0u);
}

TEST(LogLinearHistogram, MergeAddsAndRecordNeverAllocates) {
    static_assert(std::is_trivially_copyable_v<LogLinearHistogram>,
                  "fixed array storage: record() cannot allocate");
    LogLinearHistogram a, b;
    for (int i = 0; i < 10; ++i) a.record(1000);
    for (int i = 0; i < 30; ++i) b.record(2'000'000);
    a.merge(b);
    EXPECT_EQ(a.count(), 40u);
    EXPECT_EQ(a.max(), 2'000'000u);
    EXPECT_EQ(a.min(), 1000u);
}

TEST(LogLinearHistogram, ResetForgetsEverything) {
    LogLinearHistogram h;
    h.record(5'000);
    h.reset();
    EXPECT_EQ(h.count(), 0u);
    EXPECT_EQ(h.max(), 0u);
    EXPECT_EQ(h.min(), 0u);
    h.record(7);
    EXPECT_EQ(h.min(), 7u);
    EXPECT_EQ(h.valueAtQuantile(0.5), 7u);
}

TEST(OpenLoopSchedule, DueCountMatchesScheduleExactly) {
    for (uint64_t rate : {1ULL, 3ULL, 100'000ULL, 1'000'000ULL, 7'777'777ULL}) {
        const OpenLoopSchedule s(5'000, rate);
        EXPECT_EQ(s.dueCount(4'999), 0u);
        EXPECT_EQ(s.dueCount(5'000), 1u) << "record 0 is due at startNs";
        for (int64_t e = 0; e <= 20'000; e += 7) {
            uint64_t brute = 0;
            while (s.scheduledNs(brute) <= 5'000 + e) ++brute;
            ASSERT_EQ(s.dueCount(5'000 + e), brute) << "rate=" << rate << " e=" << e;
        }
    }
}

TEST(OpenLoopSchedule, NoDriftOverAMinuteAtOneHundredThousandPerSecond) {
    const OpenLoopSchedule s(0, 100'000);
    EXPECT_EQ(s.scheduledNs(6'000'000), 60'000'000'000);
    EXPECT_EQ(s.dueCount(60'000'000'000 - 1), 6'000'000u);
}

TEST(SeqTimestampRing, OutOfOrderTakesKeepTheWindowUntilTheOldestIsTaken) {
    SeqTimestampRing r(4);
    for (int64_t t = 10; t < 14; ++t) r.push(t);
    EXPECT_TRUE(r.full());
    EXPECT_EQ(r.take(2), 12);
    EXPECT_TRUE(r.full()) << "seq 0 still outstanding";
    EXPECT_EQ(r.take(0), 10);
    EXPECT_EQ(r.take(1), 11);
    EXPECT_FALSE(r.full());
    EXPECT_EQ(r.outstanding(), 1u);
    r.push(14);
    EXPECT_EQ(r.nextSeq(), 5u);
    EXPECT_EQ(r.take(4), 14);
}

TEST(SeqTimestampRing, ManyLapsKeepSequenceToTimestampMapping) {
    SeqTimestampRing r(3);  // rounds up to 4
    uint64_t taken = 0;
    for (int64_t t = 0; t < 1000; ++t) {
        if (r.full()) {
            ASSERT_EQ(r.take(taken), static_cast<int64_t>(taken) * 7);
            ++taken;
        }
        r.push(t * 7);
    }
    while (taken < r.nextSeq()) {
        ASSERT_EQ(r.take(taken), static_cast<int64_t>(taken) * 7);
        ++taken;
    }
    EXPECT_EQ(r.outstanding(), 0u);
}

TEST(ProcStat, ParsesCpuLines) {
    const auto agg = parseProcStatCpuLine("cpu  100 5 50 1000 20 1 2 3 0 0");
    ASSERT_TRUE(agg.has_value());
    EXPECT_EQ(agg->first, -1);
    EXPECT_EQ(agg->second.busy, 100u + 5 + 50 + 1 + 2 + 3);
    EXPECT_EQ(agg->second.total, agg->second.busy + 1000 + 20);
    const auto c3 = parseProcStatCpuLine("cpu3 10 0 10 80 0 0 0 0 0 0");
    ASSERT_TRUE(c3.has_value());
    EXPECT_EQ(c3->first, 3);
    EXPECT_FALSE(parseProcStatCpuLine("intr 12345 0 0").has_value());
}

TEST(ProcStat, ParsesTaskStatWithSpacesInComm) {
    const auto t = parseTaskStat(
        "4242 (rune shard 0) R 1 2 3 4 5 6 7 8 9 10 250 50 0 0 20 0 1 0 100 0 0");
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->first, "rune shard 0");
    EXPECT_EQ(t->second, 300u);  // utime 250 + stime 50
}

TEST(ProcStat, RejectsMalformedTaskStat) {
    EXPECT_FALSE(parseTaskStat("4242 rune R 1 2").has_value());
    EXPECT_FALSE(parseTaskStat("4242 (rune) R 1 2 3").has_value()) << "too few fields";
    EXPECT_FALSE(parseTaskStat("4242 (rune) R 1 2 3 4 5 6 7 8 9 10 x 50 0").has_value()) << "non-numeric utime";
}

TEST(ProcStat, BusyFractionsBetweenSnapshots) {
    const std::vector<CpuTimes> a{{100, 1000}, {0, 1000}};
    const std::vector<CpuTimes> b{{600, 2000}, {10, 2000}};
    const auto f = busyFractions(a, b);
    ASSERT_EQ(f.size(), 2u);
    EXPECT_DOUBLE_EQ(f[0], 0.5);
    EXPECT_DOUBLE_EQ(f[1], 0.01);
}
