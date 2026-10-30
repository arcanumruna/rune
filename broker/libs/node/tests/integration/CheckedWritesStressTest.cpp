// Checked writes Part 04 Task 11: the seeded stress of the verdict pipeline on three real brokers.
#include "support/CheckedWritesStressDriver.h"

#include <gtest/gtest.h>

#include <charconv>
#include <cstring>
#include <stdexcept>
#include <string>

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

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesStressRegression, ::testing::Range(1, 13));
