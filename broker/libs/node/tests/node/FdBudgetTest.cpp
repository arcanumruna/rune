// FD-1 (E1d fix round 3): the startup open-file budget — pure limit
// computation and the refuse / warn decision (no real rlimit change here).
#include <gtest/gtest.h>
#include <rune/node/FdBudget.h>

using namespace rune::node;

namespace {
TopicSpec topic(int32_t partitions, int64_t retentionBytes = -1, int64_t roll = 64LL * 1024 * 1024) {
    TopicSpec t;
    t.name = "t";
    t.partitionCount = partitions;
    t.retentionBytes = retentionBytes;
    t.segmentRollBytes = roll;
    return t;
}
constexpr uint64_t kInternal = kInternalPartitionsReserve * (kDefaultOpenSegments * kFdsPerSegment + kE1dFdsPerLog);
} // namespace

TEST(FdBudgetRegression, EstimateCountsSegmentsAndE1dFdsPerPartition) {
    // Unbounded retention: the default open-segment floor.
    EXPECT_EQ(estimateFdNeed({topic(300)}),
              kBaseFds + kInternal + 300 * (kDefaultOpenSegments * kFdsPerSegment + kE1dFdsPerLog));
    // Bounded retention: ceil(retention / roll) retained + the active segment.
    EXPECT_EQ(estimateFdNeed({topic(10, 10 * 64LL * 1024 * 1024 + 1)}),
              kBaseFds + kInternal + 10 * ((11 + 1) * kFdsPerSegment + kE1dFdsPerLog));
    EXPECT_EQ(estimateFdNeed({topic(2), topic(3)}),
              kBaseFds + kInternal + 5 * (kDefaultOpenSegments * kFdsPerSegment + kE1dFdsPerLog));
    EXPECT_EQ(estimateFdNeed({}), kBaseFds + kInternal);
}

TEST(FdBudgetRegression, SoftIsRaisedToHard) {
    const auto d = decideFdLimit(/*soft=*/1024, /*hard=*/524288, false, /*need=*/10'000);
    EXPECT_EQ(d.verdict, FdLimitVerdict::Ok);
    EXPECT_EQ(d.targetSoft, 524288u);
    EXPECT_NE(d.message.find("1024"), std::string::npos) << "log both values: " << d.message;
    EXPECT_NE(d.message.find("524288"), std::string::npos) << d.message;
}

TEST(FdBudgetRegression, RefusesWhenHardLimitBelowNeed) {
    // systemd's old default hard limit (4096) cannot hold ~300 partitions.
    const uint64_t need = estimateFdNeed({topic(300)});
    const auto d = decideFdLimit(1024, 4096, false, need);
    EXPECT_EQ(d.verdict, FdLimitVerdict::Refuse);
    EXPECT_NE(d.message.find("LimitNOFILE"), std::string::npos) << "the error must say how to fix it: " << d.message;
    EXPECT_NE(d.message.find(std::to_string(need)), std::string::npos) << d.message;
}

TEST(FdBudgetRegression, WarnsWhenHardLimitIsClose) {
    EXPECT_EQ(decideFdLimit(1024, 11'000, false, 10'000).verdict, FdLimitVerdict::Warn);   // < need * 1.25
    EXPECT_EQ(decideFdLimit(1024, 12'500, false, 10'000).verdict, FdLimitVerdict::Ok);
    EXPECT_EQ(decideFdLimit(1024, 10'000, false, 10'000).verdict, FdLimitVerdict::Warn);   // == need: still starts
    EXPECT_EQ(decideFdLimit(1024, 9'999, false, 10'000).verdict, FdLimitVerdict::Refuse);
}

TEST(FdBudgetRegression, UnlimitedHardNeverRefuses) {
    const auto d = decideFdLimit(256, 0, /*unlimited=*/true, 50'000);
    EXPECT_EQ(d.verdict, FdLimitVerdict::Ok);
    EXPECT_GE(d.targetSoft, 50'000u) << "an unlimited hard limit still raises soft to cover the need";
}
