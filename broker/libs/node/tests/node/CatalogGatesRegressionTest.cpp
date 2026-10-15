// CatalogGatesRegressionTest.cpp — catalog history Task 23 (spec §10 gate): a 10 000-event
// __catalog boots in under 1 s (Release; Debug has a looser bound — the Release gate is
// BM_CatalogBootReplay10k), the timed replay verifies every event, and the script has its
// documented shape.  The log is written through production code (support/CatalogBootLog.h).
#include <gtest/gtest.h>

#include "support/CatalogBootLog.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/CatalogCheckpoint.h>

#include <cstdio>
#include <filesystem>
#include <string>

#include <unistd.h>

namespace {

struct GateDir {
    std::filesystem::path path;
    explicit GateDir(const std::string& tag) {
        static int seq = 0;
        path = std::filesystem::temp_directory_path() /
               ("rune_gate_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        std::filesystem::remove_all(path);
    }
    ~GateDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

}  // namespace

using rune::node::BrokerNode;
using rune::node::test::writeCatalogBootLog;

TEST(CatalogGatesRegression, BootReplayOfTenThousandEventsIsUnderOneSecondRegression) {
    GateDir d("boot");
    const auto shape = writeCatalogBootLog(d.path);
    ASSERT_EQ(shape.events, 10'000);
    ASSERT_EQ(shape.bootTopics, 2000u);
    ASSERT_EQ(shape.droppedIds, 2000u);

    const auto r = BrokerNode::bootFromCatalogForBench(d.path.string());
    ASSERT_EQ(r.appliedEvents, 10'000);
    ASSERT_FALSE(r.halted);
    EXPECT_EQ(r.bootTopics, shape.bootTopics);
    EXPECT_EQ(r.droppedIds, shape.droppedIds);
    const int64_t ms = r.bootNs / 1'000'000;
#ifdef NDEBUG
    EXPECT_LT(ms, 1000) << "spec §10 gate";
#else
    EXPECT_LT(ms, 8000) << "Debug build: the Release gate is BM_CatalogBootReplay10k";
#endif
    std::printf("[catalog gate] boot replay of 10000 events: %lld ms\n", (long long)ms);
}

TEST(CatalogGatesRegression, TamperedCheckpointDigestHaltsTheTimedBootReplayRegression) {
    GateDir d("tamper");
    const auto shape = writeCatalogBootLog(d.path, /*unit=*/10);
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(
        (d.path / "__catalog").string(), rune::node::CatalogCheckpoint{shape.events - 1, shape.digest ^ 1}));
    const auto r = BrokerNode::bootFromCatalogForBench(d.path.string());
    EXPECT_TRUE(r.halted) << "the replay must verify the digest, not trust the checkpoint";
}

TEST(CatalogGatesRegression, SmallBootScriptHasTheDocumentedShapeRegression) {
    GateDir d("shape");
    const auto shape = writeCatalogBootLog(d.path, /*unit=*/10);
    ASSERT_EQ(shape.events, 100);
    const auto r = BrokerNode::bootFromCatalogForBench(d.path.string());
    EXPECT_EQ(r.appliedEvents, 100);
    EXPECT_FALSE(r.halted);
    EXPECT_EQ(r.bootTopics, 20u);
    EXPECT_EQ(r.droppedIds, 20u);
}
