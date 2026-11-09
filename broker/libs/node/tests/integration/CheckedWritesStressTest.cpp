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

// The Stress cases run the whole step count on 64 KiB segments (4 KiB for autoincrement); the fast defaults run one seed of each set for
// a few steps on small segments, so the cleaner has closed segments to compact all the same.  Every assertion stays.
namespace {
constexpr int64_t kFastSegmentBytes = 2 * 1024;

void runSeededCluster(int seed, int steps, int64_t segmentBytes) {
    ThreeNodes cluster = ThreeNodes::startWith("cw04_" + std::to_string(seed), [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, static_cast<uint64_t>(seed));
    d.setSegmentRollBytes(segmentBytes);
    d.setUp();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    for (int step = 0; step < steps && !::testing::Test::HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
        d.step();
    }
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    d.quiesce();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    EXPECT_GT(d.compactedAway(), 0u) << "seed " << seed << ": the cleaner never removed a committed record";
    ::testing::Test::RecordProperty("compactedAway", static_cast<int>(d.compactedAway()));
    const auto f = d.faults();
    std::printf("seed %d: %zu sends, %d leader crashes, %d restarts (%d cut short), %zu committed records the most compacted log lost\n", seed, d.sent().size(), f[0], f[1], f[2], d.compactedAway());
}

void runKeyIndexSeededCluster(int seed, int steps, int64_t segmentBytes) {
    ThreeNodes cluster = ThreeNodes::startWith("cw05_" + std::to_string(seed), [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, static_cast<uint64_t>(seed), true);
    d.setSegmentRollBytes(segmentBytes);
    d.setUp();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    for (int step = 0; step < steps && !::testing::Test::HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
        d.step();
    }
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    d.quiesce();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    std::printf("seed %d: %s\n", seed, d.keyIndexTally().c_str());
}

void runAutoIncSeededCluster(int seed, int steps, int64_t segmentBytes, int64_t producerExpiryMs) {
    // The cleaner keeps a live producer's last 32 batches whole (spec 6.6), and no writer here reaches 32: only expired producers leave
    // anything to compact.  Writers are rebuilt after every fault, so old pids expire and are compacted mid-run, the last ones at rest.
    // Only the cleaner reads this expiry; the producer state still dedupes those pids.
    ThreeNodes cluster = ThreeNodes::startWith("cw06_" + std::to_string(seed), [producerExpiryMs](rune::node::NodeConfig& cfg) {
        cfg.verdictIntervalMs = 20;
        cfg.producerIdExpirationMs = producerExpiryMs;
    });
    CheckedWritesStressDriver d(cluster, static_cast<uint64_t>(seed), false, true);
    d.setSegmentRollBytes(segmentBytes);
    d.setUp();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    for (int step = 0; step < steps && !::testing::Test::HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
        d.step();
    }
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    d.quiesce();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    EXPECT_EQ(d.autoIncWorkShortfall(), "") << "seed " << seed;
    std::printf("seed %d: %s\n", seed, d.autoIncTally().c_str());
}

void runSeededRegressorsPath(int seed, int steps, int64_t segmentBytes, int64_t producerExpiryMs, int64_t cap) {
    ThreeNodes cluster = ThreeNodes::startWith("cw09_" + std::to_string(seed), [producerExpiryMs](rune::node::NodeConfig& cfg) {
        cfg.verdictIntervalMs = 20;
        cfg.producerIdExpirationMs = producerExpiryMs;   // as the autoincrement run: expired producers leave the cleaner something to compact
    });
    CheckedWritesStressDriver d(cluster, static_cast<uint64_t>(seed), false, false, true);
    d.setSegmentRollBytes(segmentBytes);
    d.setRegressorsPathCap(cap);
    d.setUpRegressorsPath();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    for (int step = 0; step < steps && !::testing::Test::HasFatalFailure(); ++step) {
        SCOPED_TRACE("seed=" + std::to_string(seed) + " step=" + std::to_string(step));
        d.step();
    }
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    d.quiesce();
    ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "seed " << seed;
    std::string failed;
    EXPECT_TRUE(d.checkAll(&failed)) << failed;
    EXPECT_GT(d.compactedAway(), 0u) << "seed " << seed << ": the cleaner never removed a committed record";
    EXPECT_EQ(d.regressorsPathShortfall(), "") << "seed " << seed;
    const auto f = d.faults();
    std::printf("seed %d: %s, %d leader crashes, %d restarts (%d cut short), %zu committed records the most compacted log lost\n", seed, d.regressorsPathTally().c_str(), f[0], f[1], f[2], d.compactedAway());
}
}  // namespace

TEST_P(CheckedWritesStressRegression, SeededClusterHoldsTheInvariants) {
    runSeededCluster(GetParam(), stepsPerSeed(), 0);
}

TEST_P(CheckedWritesStressRegression, KeyIndexSeededClusterHoldsTheInvariants) {
    if (GetParam() > 8) GTEST_SKIP() << "the key-index run covers seeds 1-8";
    runKeyIndexSeededCluster(GetParam(), 300, 0);
}

TEST_P(CheckedWritesStressRegression, AutoIncSeededClusterHoldsTheInvariants) {
    if (GetParam() > 8) GTEST_SKIP() << "the autoincrement run covers seeds 1-8";
    runAutoIncSeededCluster(GetParam(), stepsPerSeed(), 0, 3'000);
}

// Plan 09 (matrix D): the RegressorsPath DDL from the golden script, logins, characters for four users near the cap, renames to
// colliding names, moves and removes through crashes, restarts, cleaner passes and replaces of account_stats.  300 steps unless
// RUNE_CW_STRESS_STEPS says otherwise.
TEST_P(CheckedWritesStressRegression, SeededRegressorsPathHoldsTheInvariants) {
    runSeededRegressorsPath(GetParam(), std::getenv("RUNE_CW_STRESS_STEPS") != nullptr ? stepsPerSeed() : 300, 0, 3'000, 12);
}

// Seeds and step counts are the smallest that still make every assertion of the family (compaction, a pinned 56 and 57, creates on both
// sides of a node stop, a refused cap and name) inside the cluster budget.  The autoincrement and regressors-path runs expire producers
// after 500 ms, not 3 s, so the cleaner has them to compact at rest; the regressors-path run caps an account at 2 characters, which a
// short run reaches (the cap of 12 needs 13 creates for one of four users).
TEST(CheckedWritesStressFast, SeededClusterHoldsTheInvariants) {
    runSeededCluster(2, 6, kFastSegmentBytes);
}

TEST(CheckedWritesStressFast, KeyIndexSeededClusterHoldsTheInvariants) {
    runKeyIndexSeededCluster(8, 7, kFastSegmentBytes);
}

TEST(CheckedWritesStressFast, SeededRegressorsPathHoldsTheInvariants) {
    runSeededRegressorsPath(6, 5, kFastSegmentBytes, 500, 2);
}

TEST(CheckedWritesStressFast, AutoIncSeededClusterHoldsTheInvariants) {
    runAutoIncSeededCluster(3, 6, kFastSegmentBytes, 500);
}

INSTANTIATE_TEST_SUITE_P(Seeds, CheckedWritesStressRegression, ::testing::Range(1, 13));

// Only a leader compacts, and it may drop a committed rejected record before the history reads its log: the record is still on the
// followers, and the model must judge it (the stress failed with "index entry N (57,0) is not rejected by the model").
TEST(CheckedWritesStressHistory, RejectionTheLeaderCompactedBeforeAnyHistoryReadIsStillJudged) {
    using Op = CheckedWritesStressDriver::Op;
    using Result = CheckedWritesStressDriver::Sent::Result;
    ThreeNodes cluster = ThreeNodes::startWith("cw05_compacted_reject", [](rune::node::NodeConfig& cfg) { cfg.verdictIntervalMs = 20; });
    CheckedWritesStressDriver d(cluster, 1, true);
    d.setSegmentRollBytes(kFastSegmentBytes);
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
    d.setSegmentRollBytes(kFastSegmentBytes);
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
        for (; std::chrono::steady_clock::now() < deadline; std::this_thread::sleep_for(20ms))
            if (tcc::awaitLeaders(cluster, d.topicId(), CheckedWritesStressDriver::kKiParts, leaders, 20ms) && pred(leaders[at])) return leaders[at];
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
        std::this_thread::sleep_for(300ms);
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
    d.setViewLookupWaitForTest(500ms);
    std::string why;
    EXPECT_FALSE(d.checkInvariantForTest("ViewEqualsFreshRefold", &why));
    EXPECT_NE(why.find("key 1: the lookup failed after "), std::string::npos) << why;
}
