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

// quiesce() came to rest while a follower led; the node it had just restarted then took its partition back, and checkAll judged that
// node's index before its leadership scan had rebuilt it (seed 7: "node 1: rejected offset 1185 is in the log but not in the index").
TEST(CheckedWritesStressHistory, CheckAllWaitsForTheNewLeadersScanRegression) {
    using Op = CheckedWritesStressDriver::Op;
    using Result = CheckedWritesStressDriver::Sent::Result;
    namespace tcc = rune::node::test::catalog_cluster;
    ThreeNodes cluster = ThreeNodes::startWith("cw05_unscanned_leader", [](rune::node::NodeConfig& cfg) {
        cfg.verdictIntervalMs = 20;
        cfg.testHoldAllControllerDecisionsMs = 60'000;
    });
    CheckedWritesStressDriver d(cluster, 1, true);
    d.setUp();
    ASSERT_FALSE(HasFatalFailure());
    const uint64_t a = 1;
    const int part = static_cast<int>(rune::client::Key(a).partition(CheckedWritesStressDriver::kKiParts));
    uint64_t b = a + 1;
    while (static_cast<int>(rune::client::Key(b).partition(CheckedWritesStressDriver::kKiParts)) != part) ++b;
    ASSERT_EQ(d.kiWrite(Op::Insert, a).result, Result::Ok);
    ASSERT_EQ(d.kiWrite(Op::Update, b).code, 57);
    const CheckedWritesStressDriver::Sent dup = d.kiWrite(Op::Insert, a);
    ASSERT_EQ(dup.result, Result::Violation);
    // More rejected inserts of `a` roll segments until the leader's cleaner drops the first one: checkAll waits for a compaction.
    const auto compacted = [&] {
        const std::set<int64_t> offs = d.leaderOffsets(part);
        const auto it = offs.lower_bound(dup.hwBefore);
        return !offs.empty() && (it == offs.end() || *it >= dup.hwAfter);
    };
    for (int i = 0; i < 4000 && !compacted(); i += 50)
        for (int j = 0; j < 50; ++j) ASSERT_EQ(d.kiWrite(Op::Insert, a).result, Result::Violation);
    for (int poll = 0; poll < 100 && !compacted(); ++poll) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(compacted()) << "the leader never compacted the rejected insert (" << dup.hwBefore << ", " << dup.hwAfter << ")";
    d.quiesce();
    ASSERT_FALSE(HasFatalFailure());

    const std::size_t at = static_cast<std::size_t>(part);
    const auto leads = [&](auto pred) {
        std::vector<int32_t> leaders;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (int poll = 0; poll < 100 && std::chrono::steady_clock::now() < deadline; ++poll, std::this_thread::sleep_for(std::chrono::milliseconds(100)))
            if (tcc::awaitLeaders(cluster, d.topicId(), CheckedWritesStressDriver::kKiParts, leaders, std::chrono::milliseconds(100)) && pred(leaders[at])) return leaders[at];
        return -1;
    };
    const int lead = leads([](int32_t) { return true; });
    ASSERT_GT(lead, 0);
    cluster.stop(lead);
    ASSERT_GT(leads([&](int32_t l) { return l != lead; }), 0) << "no follower took partition " << part;
    // The controller decides nothing until the restarted node's scans are held, so it takes its partition back only then.
    for (const int id : cluster.runningNodes()) cluster.node(id).setControllerDecisionHoldForTest(true);
    cluster.startNode(lead);
    cluster.node(lead).setControllerDecisionHoldForTest(true);
    d.holdLeadershipScan(lead, true);
    for (const int id : cluster.runningNodes()) cluster.node(id).setControllerDecisionHoldForTest(false);
    ASSERT_EQ(leads([&](int32_t l) { return l == lead; }), lead) << "node " << lead << " never took partition " << part << " back";
    ASSERT_NE(d.rcStatus(lead, part), rune::node::Worker::kRcReady) << "the leadership scan finished before it was held";
    std::jthread release([&] {
        std::this_thread::sleep_for(std::chrono::seconds(3));
        d.holdLeadershipScan(lead, false);
    });
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    release.join();
}

// A send that threw while the killer thread was crashing the leader destroyed that thread joinable: std::terminate (the key-index
// stress's EXC_CRASH reports, from kiBurst_).  The exception reaches the caller, and the crash has finished.
TEST(CheckedWritesStressHistory, SendThrowingDuringALeaderCrashReachesTheCallerRegression) {
    ThreeNodes cluster = ThreeNodes::startWith("cw05_send_throws", [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, 1, true);
    d.setUp();
    ASSERT_FALSE(HasFatalFailure());
    d.setKiSendHookForTest([](const CheckedWritesStressDriver::Sent& s) {
        if (!s.windowKnown) throw std::runtime_error("send refused while the leader goes down");   // only a crash burst's sends from the killer on
    });
    EXPECT_THROW(d.crashLeaderDuringSends(), std::runtime_error);
    EXPECT_EQ(cluster.runningNodes().size(), 2u);
}

// Final review M5: a view row lookup that never answers is reported as a failed lookup, not as a row that should not be there.
TEST(CheckedWritesStressHistory, ViewLookupThatNeverAnswersSaysSoRegression) {
    using Op = CheckedWritesStressDriver::Op;
    using Result = CheckedWritesStressDriver::Sent::Result;
    ThreeNodes cluster = ThreeNodes::startWith("cw05_lookup_fails", [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, 1, true);
    d.setUp();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(d.kiWrite(Op::Insert, 1).result, Result::Ok);
    d.quiesce();
    ASSERT_FALSE(HasFatalFailure());
    d.setViewLookupHookForTest([] { throw std::runtime_error("no answer"); });
    std::string why;
    EXPECT_FALSE(d.checkInvariantForTest("ViewEqualsFreshRefold", &why));
    EXPECT_NE(why.find("key 1: the lookup failed after "), std::string::npos) << why;
}
