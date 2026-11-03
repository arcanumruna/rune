// Checked writes Part 04 Task 11: the seeded stress of the verdict pipeline on three real brokers.
#include "support/CheckedWritesStressDriver.h"

#include <gtest/gtest.h>

#include <charconv>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <set>
#include <string>
#include <thread>

using namespace rune::node::cw;
using rune::node::test::catalog_cluster::ThreeNodes;

namespace {
int stepsPerSeed() {
    const char* s = std::getenv("RUNE_CW_STRESS_STEPS");
    if (s == nullptr) return 40;
    int steps = 0;
    const auto [end, ec] = std::from_chars(s, s + std::strlen(s), steps);
    if (ec != std::errc{} || *end != '\0' || steps <= 0) throw std::invalid_argument(std::string("RUNE_CW_STRESS_STEPS is not a positive integer: ") + s);
    return steps;
}
}  // namespace

class CheckedWritesStressRegression : public ::testing::TestWithParam<int> {};

TEST_P(CheckedWritesStressRegression, SeededClusterHoldsTheInvariants) {
    const int seed = GetParam();
    ThreeNodes cluster = ThreeNodes::startWith("cw04_" + std::to_string(seed), [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, static_cast<uint64_t>(seed));
    d.setUp();
    ASSERT_FALSE(HasFatalFailure()) << "seed " << seed;
    for (int step = 0; step < stepsPerSeed() && !HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
        d.step();
    }
    ASSERT_FALSE(HasFatalFailure()) << "seed " << seed;
    d.quiesce();
    ASSERT_FALSE(HasFatalFailure()) << "seed " << seed;
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    EXPECT_GT(d.compactedAway(), 0u) << "seed " << seed << ": the cleaner never removed a committed record";
    RecordProperty("compactedAway", static_cast<int>(d.compactedAway()));
    const auto f = d.faults();
    std::printf("seed %d: %zu sends, %d leader crashes, %d restarts (%d cut short), %zu committed records the most compacted log lost\n", seed, d.sent().size(), f[0], f[1], f[2], d.compactedAway());
}

TEST_P(CheckedWritesStressRegression, KeyIndexSeededClusterHoldsTheInvariants) {
    const int seed = GetParam();
    if (seed > 8) GTEST_SKIP() << "the key-index run covers seeds 1-8";
    ThreeNodes cluster = ThreeNodes::startWith("cw05_" + std::to_string(seed), [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, static_cast<uint64_t>(seed), true);
    d.setUp();
    ASSERT_FALSE(HasFatalFailure()) << "seed " << seed;
    for (int step = 0; step < 300 && !HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
        d.step();
    }
    ASSERT_FALSE(HasFatalFailure()) << "seed " << seed;
    d.quiesce();
    ASSERT_FALSE(HasFatalFailure()) << "seed " << seed;
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    std::printf("seed %d: %s\n", seed, d.keyIndexTally().c_str());
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesStressRegression, ::testing::Range(1, 13));

// Only a leader compacts, and it may drop a committed rejected record before the history reads its log: the record is still on the
// followers, and the model must judge it (the stress failed with "index entry N (57,0) is not rejected by the model").
TEST(CheckedWritesStressHistory, RejectionTheLeaderCompactedBeforeAnyHistoryReadIsStillJudged) {
    using Op = CheckedWritesStressDriver::Op;
    using Result = CheckedWritesStressDriver::Sent::Result;
    ThreeNodes cluster = ThreeNodes::startWith("cw05_compacted_reject", [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, 1, true);
    d.setUp();
    ASSERT_FALSE(HasFatalFailure());
    const uint64_t a = 1;
    const int part = static_cast<int>(rune::client::Key(a).partition(CheckedWritesStressDriver::kKiParts));
    uint64_t b = a + 1;
    while (static_cast<int>(rune::client::Key(b).partition(CheckedWritesStressDriver::kKiParts)) != part) ++b;
    ASSERT_EQ(d.kiWrite(Op::Insert, a).result, Result::Ok);
    const CheckedWritesStressDriver::Sent miss = d.kiWrite(Op::Update, b);
    ASSERT_EQ(miss.result, Result::Violation);
    ASSERT_EQ(miss.code, 57);
    // Rejected inserts of `a` roll the segment that holds the 57's record; the leader's cleaner then drops it.
    const auto gone = [&] {
        const std::set<int64_t> offs = d.leaderOffsets(part);
        const auto it = offs.lower_bound(miss.hwBefore);
        return it == offs.end() || *it >= miss.hwAfter;
    };
    for (int i = 0; i < 4000 && !gone(); i += 50)
        for (int j = 0; j < 50; ++j) ASSERT_EQ(d.kiWrite(Op::Insert, a).result, Result::Violation);
    for (int poll = 0; poll < 100 && !gone(); ++poll) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(gone()) << "the leader never compacted the rejected update (" << miss.hwBefore << ", " << miss.hwAfter << ")";
    d.quiesce();
    ASSERT_FALSE(HasFatalFailure());
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    std::printf("%s\n", d.keyIndexTally().c_str());
}
