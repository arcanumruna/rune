// arcanum-metrics plan Task 7 (D60): the broker consumes arcanum-metrics like the flight
// recorder (ARCANUM_METRICS_DIR or FetchContent at the pinned commit).  This test pins the
// layout version Rune is built against: a library bump that changes the file layout must
// fail here at compile time, and a created file must read back that version at run time.
#include <metrics/MetricsFile.h>
#include <metrics/MetricsLayout.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>

static_assert(metrics::kLayoutVersion == 2, "Rune is built against arcanum-metrics layout version 2");

TEST(BrokerMetricsRegression, LibraryVersionRegression) {
    // Per-binary TMPDIR (cmake/RuneTestTmpRoot.cmake) keeps this off other test binaries' files.
    const std::string path =
        (std::filesystem::temp_directory_path() / ("rune-broker-metrics-" + std::to_string(::getpid()) + ".bin"))
            .string();
    std::filesystem::remove(path);

    metrics::MetricsFile writer;
    const metrics::CreateParams params{path.c_str(), 16, 16, static_cast<int64_t>(::getpid()), 1, {}};
    ASSERT_EQ(metrics::MetricsFile::create(params, writer), metrics::FileStatus::Ok);

    int32_t written = 0;
    std::memcpy(&written, static_cast<const std::byte*>(writer.base()) + metrics::kHdrLayoutVersionOffset,
                sizeof(written));
    EXPECT_EQ(written, 2);

    // A reader (as a separate process would) accepts the file only at the expected version.
    metrics::MetricsFile reader;
    ASSERT_EQ(metrics::MetricsFile::open(path.c_str(), 4, reader), metrics::FileStatus::Ok);
    int32_t readBack = 0;
    std::memcpy(&readBack, static_cast<const std::byte*>(reader.base()) + metrics::kHdrLayoutVersionOffset,
                sizeof(readBack));
    EXPECT_EQ(readBack, metrics::kLayoutVersion);

    std::filesystem::remove(path);
}

// ===========================================================================
// arcanum-metrics plan Task 8 (spec §5.3; D54, D55, D59, D61): every Rune metric lives in the node's metrics file,
// allocated at start before any agent thread; a file or allocation failure refuses the start.
// ===========================================================================
#if defined(RUNE_BROKER_METRICS_NODE_TESTS)

#include "MetricsTestReader.h"
#include "support/TestNodeConfig.h"
#include "support/TestWorkerCount.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/catalog/CatalogState.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/RuneMetrics.h>

#include <metrics/MetricsReader.h>

#include <CncFileReader.h>
#include <concurrent/CountersReader.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <thread>

namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using rune::node::BrokerNode;
using rune::node::kRuneMetrics;
using rune::node::MetricOwner;
using rune::node::NodeConfig;
using rune::node::PeerConfig;
using rune::node::RuneMetric;

struct MetricsTempDir {
    fs::path path;
    explicit MetricsTempDir(std::string_view tag) {
        static std::atomic<uint32_t> seq{0};
        path = fs::temp_directory_path() / ("rune_brmetrics_" + std::string(tag) + std::to_string(::getpid()) + "_" +
                                            std::to_string(seq.fetch_add(1, std::memory_order_relaxed)));
    }
    ~MetricsTempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

NodeConfig singleNode(const fs::path& dataDir) {
    NodeConfig cfg;
    rune::node::test::pinLegacyRuntime(cfg);
    cfg.nodeId            = 0;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;
    PeerConfig self;
    self.id            = 0;
    self.host          = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
    cfg.peers          = {self};
    cfg.clientEndpoint.channel =
        "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;
    cfg.workerCount             = rune::node::test::workerCountFromEnv();
    return cfg;
}

// Polls `id` until it reads `want` (the control thread publishes on its next admin pass); bounded.
bool awaitMetric(const BrokerNode& node, RuneMetric id, std::int64_t want, std::chrono::milliseconds within = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto v = rune::node::test::readRuneMetric(node, id);
        if (v && *v == want) return true;
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

std::int64_t headerStartEpochNs(const metrics::MetricsFile& f) {
    std::int64_t v = 0;
    std::memcpy(&v, static_cast<const std::byte*>(f.base()) + metrics::kHdrStartEpochNsOffset, sizeof(v));
    return v;
}

std::string startError(NodeConfig cfg) {
    BrokerNode node{cfg};
    try {
        node.start();
    } catch (const std::exception& e) {
        return e.what();
    }
    node.stop();
    return {};
}

}  // namespace

// Every kRuneMetrics row is Live in the file with its exact label, key (= node id) and kind; the control-owned
// metrics carry their writers' values.  RED: skip one row in createMetrics_'s allocation loop -> that row is absent.
TEST(BrokerMetricsRegression, EveryMetricMovedRegression) {
    MetricsTempDir tmp{"every_"};
    NodeConfig cfg = singleNode(tmp.path);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    ASSERT_EQ(node.metricsFilePath(), (tmp.path / "metrics" / "rune-metrics.dat").string()) << "the default path";

    for (const auto& def : kRuneMetrics) {
        const auto live = rune::node::test::readRuneMetricLiveAt(node.metricsFilePath(),
                                                                 static_cast<std::int32_t>(def.id), cfg.nodeId);
        ASSERT_TRUE(live.has_value()) << "metric " << static_cast<std::int32_t>(def.id) << " (" << def.labelPrefix
                                      << ") is not in the metrics file";
        EXPECT_EQ(live->label(), std::string(def.labelPrefix) + " node=" + std::to_string(cfg.nodeId));
        EXPECT_EQ(live->kind, def.kind) << def.labelPrefix;
        std::int32_t keyNode = -1;
        ASSERT_TRUE(rune::node::decodeRoleKey(reinterpret_cast<const std::uint8_t*>(live->key().data()),
                                              live->key().size(), keyNode));
        EXPECT_EQ(keyNode, cfg.nodeId) << def.labelPrefix;
    }

    // The control thread's writers (values after start, then one deterministic action).
    EXPECT_TRUE(awaitMetric(node, RuneMetric::AuthMode, 0)) << "auth enabled-or-in-process -> 0";
    EXPECT_TRUE(awaitMetric(node, RuneMetric::StorageGateState, 0)) << "a single node is CONFIRMED";
    EXPECT_TRUE(awaitMetric(node, RuneMetric::CatalogApplyHalted, 0));
    EXPECT_TRUE(awaitMetric(node, RuneMetric::SecurityApplyHalted, 0));
    EXPECT_TRUE(awaitMetric(node, RuneMetric::SecurityAppliedOffset, -1)) << "nothing applied";
    EXPECT_TRUE(awaitMetric(node, RuneMetric::CatalogUnplaceableTopics, 0));
    EXPECT_TRUE(awaitMetric(node, RuneMetric::StuckMarkers, 0));
    EXPECT_TRUE(awaitMetric(node, RuneMetric::PeerLinksEstablished, 0)) << "no peers";

    rune::node::AdminWorkerAgent* agent = node.adminWorkerAgentForTest();
    ASSERT_NE(agent, nullptr);
    node.onAdminThreadForTest([agent] {
        const auto live = agent->catalogLiveEffects();
        live.sink.applied(41, 0xABCD);
        return 0;
    });
    EXPECT_TRUE(awaitMetric(node, RuneMetric::CatalogAppliedOffset, 41)) << "the applier's report reaches the file";
    node.onAdminThreadForTest([agent] {
        rune::catalog::ApplyError err;
        err.offset = 42;
        err.code   = rune::catalog::ApplyError::Code::FingerprintMismatch;
        err.detail = "test";
        const auto live = agent->catalogLiveEffects();
        live.sink.halted(err);
        return 0;
    });
    EXPECT_TRUE(awaitMetric(node, RuneMetric::CatalogApplyHalted, 43)) << "halted offset 42 -> 43";
    node.stop();
}

// cnc.dat holds no control-owned Rune counter any more.  The observer (role / term / out-of-sync) and provisioner
// rows keep their Aeron counters until plan Task 9 moves their writers; Task 9 tightens this to the whole range.
TEST(BrokerMetricsRegression, CncHoldsOnlyDriverCountersRegression) {
    MetricsTempDir tmp{"cnc_"};
    NodeConfig cfg = singleNode(tmp.path);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    node.publishRole();

    aeron::CncFileReader reader = aeron::CncFileReader::mapExisting(cfg.effectiveAeronDir().c_str());
    aeron::concurrent::CountersReader counters = reader.countersReader();
    int driverCounters = 0;
    std::vector<std::int32_t> controlInCnc;
    counters.forEach([&](std::int32_t, std::int32_t typeId, const aeron::concurrent::AtomicBuffer&,
                         const std::string&) {
        if (typeId < 1'000'000) ++driverCounters;
        const std::int32_t row = rune::node::runeMetricRow(typeId);
        if (typeId >= 1'000'201 && typeId <= 1'000'241 &&
            (row < 0 || kRuneMetrics[static_cast<std::size_t>(row)].owner == MetricOwner::Control))
            controlInCnc.push_back(typeId);
    });
    EXPECT_GT(driverCounters, 0) << "the scan saw the driver's own counters (not a vacuous pass)";
    EXPECT_TRUE(controlInCnc.empty()) << "first control-owned type id still in cnc.dat: "
                                      << (controlInCnc.empty() ? 0 : controlInCnc.front());
    node.stop();
}

// A metrics file one slot too small refuses the start with the logged message, before any thread or the driver.
TEST(BrokerMetricsRegression, StartRefusedWhenFileTooSmallRegression) {
    MetricsTempDir tmp{"small_"};
    NodeConfig cfg = singleNode(tmp.path);
    const std::int32_t need = metrics::requiredSlots({rune::node::kNodeMetricCount, 0, 0, 0, 0, 16});
    cfg.metricsMaxSlotsOverrideForTest = need - 1;
    const std::string err = startError(cfg);
    EXPECT_NE(err.find("metrics file " + cfg.effectiveMetricsFile() + ": CapacityTooSmall"), std::string::npos) << err;
    EXPECT_NE(err.find("need " + std::to_string(need) + " slots, have " + std::to_string(need - 1)), std::string::npos)
        << err;
    EXPECT_FALSE(fs::exists(fs::path{cfg.effectiveAeronDir()} / "cnc.dat")) << "the driver never started";
    EXPECT_FALSE(fs::exists(cfg.effectiveMetricsFile())) << "no file is left behind";
}

// The 10th allocation (row 9, rune-catalog-apply-halted) returning Full refuses the start and names that metric.
TEST(BrokerMetricsRegression, StartRefusedOnAllocFailureRegression) {
    MetricsTempDir tmp{"alloc_"};
    NodeConfig cfg = singleNode(tmp.path);
    cfg.metricsFailAllocAtForTest = 9;
    const std::string err = startError(cfg);
    EXPECT_NE(err.find("metrics allocation failed for rune-catalog-apply-halted (type 1000217): Full"),
              std::string::npos)
        << err;
    EXPECT_FALSE(fs::exists(fs::path{cfg.effectiveAeronDir()} / "cnc.dat")) << "the driver never started";
}

// stop + start recreates the file: a new startEpochNs, a reader on the old mapping sees sameInstanceAs() == false,
// and the values restart from the node's recovered state.
TEST(BrokerMetricsRegression, RestartRecreatesFileRegression) {
    MetricsTempDir tmp{"restart_"};
    NodeConfig cfg = singleNode(tmp.path);
    metrics::MetricsFile oldMapping;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(5s));
        ASSERT_TRUE(awaitMetric(node, RuneMetric::StorageGateState, 0));
        ASSERT_EQ(metrics::MetricsFile::open(node.metricsFilePath().c_str(), 64, oldMapping), metrics::FileStatus::Ok);
        ASSERT_TRUE(oldMapping.sameInstanceAs(node.metricsFilePath().c_str()));
        node.stop();
    }
    const std::int64_t oldEpoch = headerStartEpochNs(oldMapping);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    EXPECT_FALSE(oldMapping.sameInstanceAs(node.metricsFilePath().c_str())) << "the old mapping detects the new file";
    metrics::MetricsFile fresh;
    ASSERT_EQ(metrics::MetricsFile::open(node.metricsFilePath().c_str(), 64, fresh), metrics::FileStatus::Ok);
    EXPECT_GT(headerStartEpochNs(fresh), oldEpoch);
    EXPECT_TRUE(awaitMetric(node, RuneMetric::StorageGateState, 0)) << "recovered: CONFIRMED again";
    EXPECT_TRUE(awaitMetric(node, RuneMetric::CatalogApplyHalted, 0));
    EXPECT_TRUE(awaitMetric(node, RuneMetric::AuthMode, 0));
    node.stop();
}

#endif  // RUNE_BROKER_METRICS_NODE_TESTS
