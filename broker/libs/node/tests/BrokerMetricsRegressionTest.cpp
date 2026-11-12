// arcanum-metrics plan Task 7 (D60): the broker consumes arcanum-metrics like the flight
// recorder (ARCANUM_METRICS_DIR or FetchContent at the pinned commit).  This test pins the
// layout version Rune is built against: a library bump that changes the file layout must
// fail here at compile time, and a created file must read back that version at run time.
#include <limits>
#include <metrics/MetricsFile.h>
#include <metrics/MetricsLayout.h>
#include <rune/node/RuneMetrics.h>
#include <rune/node/ViewSnapshotChannel.h>

#include <gtest/gtest.h>

#include <cctype>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

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
// arcanum-metrics plan Task 13 (spec §5.3): the operator's metric table in docs/operations.md is kRuneMetrics.
// The "### Node metrics" table must list every row, in order, with the same type id, label prefix, kind and owner,
// so a renamed, added or removed metric fails here until the doc follows.
// ===========================================================================
namespace {

struct DocMetricRow {
    std::string id, label, kind, owner;
};

std::string trimCell(std::string s) {
    const auto b = s.find_first_not_of(" \t");
    const auto e = s.find_last_not_of(" \t");
    s = b == std::string::npos ? std::string{} : s.substr(b, e - b + 1);
    if (s.size() >= 2 && s.front() == '`' && s.back() == '`') s = s.substr(1, s.size() - 2);
    return s;
}

// The data rows of the first table under "### Node metrics" (header and separator rows skipped).
std::vector<DocMetricRow> parseNodeMetricTable(std::istream& in) {
    std::vector<DocMetricRow> rows;
    std::string line;
    bool inSection = false;
    bool inTable = false;
    while (std::getline(in, line)) {
        if (line.rfind("### Node metrics", 0) == 0) { inSection = true; continue; }
        if (!inSection) continue;
        if (line.rfind('|', 0) != 0) {
            if (inTable) break;  // the table ended
            if (line.rfind("#", 0) == 0) break;  // the next heading before any table
            continue;
        }
        inTable = true;
        std::vector<std::string> cells;
        std::string cell;
        std::istringstream ls(line.substr(1));
        while (std::getline(ls, cell, '|')) cells.push_back(trimCell(cell));
        if (cells.size() < 4 || cells[0].empty() || !std::isdigit(static_cast<unsigned char>(cells[0][0]))) continue;
        rows.push_back({cells[0], cells[1], cells[2], cells[3]});
    }
    return rows;
}

const char* kindName(metrics::MetricKind k) { return k == metrics::MetricKind::Counter ? "counter" : "gauge"; }
const char* ownerName(rune::node::MetricOwner o) {
    switch (o) {
        case rune::node::MetricOwner::Control: return "control";
        case rune::node::MetricOwner::Observer: return "observer";
        case rune::node::MetricOwner::Provisioner: return "provisioner";
    }
    return "?";
}

}  // namespace

TEST(BrokerMetricsRegression, OperationsDocTableMatchesRegression) {
    const std::string docPath = std::string(RUNE_REPO_ROOT) + "/docs/operations.md";
    std::ifstream in(docPath);
    ASSERT_TRUE(in.good()) << "cannot open " << docPath;
    const std::vector<DocMetricRow> rows = parseNodeMetricTable(in);
    ASSERT_EQ(rows.size(), rune::node::kRuneMetrics.size())
        << "docs/operations.md '### Node metrics' must list every kRuneMetrics row";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const auto& def = rune::node::kRuneMetrics[i];
        SCOPED_TRACE("row " + std::to_string(i) + " (" + def.labelPrefix + ")");
        EXPECT_EQ(rows[i].id, std::to_string(rune::node::typeIdOf(def.id)));
        EXPECT_EQ(rows[i].label, def.labelPrefix);
        EXPECT_EQ(rows[i].kind, kindName(def.kind));
        EXPECT_EQ(rows[i].owner, ownerName(def.owner));
    }
}

// #819: docs/operations.md "Restoring a backup" names every discard reason and forbids restoring the unsafe ones as
// committed.delta.  A new ViewDiscardReason value forces a new row here.
static_assert(static_cast<int>(rune::node::ViewDiscardReason::BelowLogStart) == 6,
              "a new ViewDiscardReason needs a row in the docs/operations.md restore table and in this test");

TEST(ViewBackupRestoreDocRegression, RestoreTableNamesEveryReasonAndForbidsLegacyRegression) {
    const std::string docPath = std::string(RUNE_REPO_ROOT) + "/docs/operations.md";
    std::ifstream in(docPath);
    ASSERT_TRUE(in.good()) << "cannot open " << docPath;
    std::vector<std::string> section;
    std::string line;
    bool inSection = false;
    while (std::getline(in, line)) {
        if (!inSection) {
            if (line.rfind("**Restoring a backup**", 0) == 0) inSection = true;
            else continue;
        } else if (line.rfind("## ", 0) == 0) {
            break;
        }
        section.push_back(line);
    }
    ASSERT_FALSE(section.empty()) << "docs/operations.md has no '**Restoring a backup**' section";
    for (int v = 1; v <= 6; ++v) {
        const auto reason = static_cast<rune::node::ViewDiscardReason>(v);
        const std::string name = rune::node::viewDiscardReasonName(reason);
        SCOPED_TRACE(name);
        const std::string prefix = "| `" + name + "` |";
        int rows = 0;
        std::string row;
        for (const std::string& l : section)
            if (l.rfind(prefix, 0) == 0) { ++rows; row = l; }
        ASSERT_EQ(rows, 1) << "exactly one restore-table row per reason";
        const bool never = row.find("**Never**") != std::string::npos;
        if (reason == rune::node::ViewDiscardReason::Unreadable)
            EXPECT_FALSE(never) << row;
        else
            EXPECT_TRUE(never) << row;
    }
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
#include <rune/node/RuneMetricSlot.h>
#include <rune/node/NamespacePaths.h>
#include <rune/node/RuneMetrics.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/storage/testing/DamageSegmentForTest.h>

#include "support/TestCluster.h"

#include <metrics/MetricsReader.h>

#include <CncFileReader.h>
#include <concurrent/CountersReader.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
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

// #839: the halt handler of an owner (the test): records the exit code the node halted with.
struct HaltCapture {
    std::mutex m;
    std::condition_variable cv;
    int code = 0;
    static void handler(void* ctx, int exitCode) noexcept {
        auto* self = static_cast<HaltCapture*>(ctx);
        { std::lock_guard<std::mutex> g(self->m); self->code = exitCode; }
        self->cv.notify_all();
    }
    int await(std::chrono::milliseconds cap) {
        std::unique_lock<std::mutex> l(m);
        cv.wait_for(l, cap, [this] { return code != 0; });
        return code;
    }
};

// #839: writes >= 3 segments (cleanly closed: the recovery point reaches the log end, so the sealed ones recover KEEPING
// damage) into `logDir` with a standalone Log, then flips a byte inside the 2nd batch of segment 0.  The log's content is
// not a valid system-log format - the refusal must fire before any loader reads it.
void writeDamagedSealedLog(const fs::path& logDir, int64_t roll) {
    fs::create_directories(logDir);
    {
        rune::storage::Log raw(logDir.string(), roll);
        raw.recover();
        raw.setEpochHistoryCap(INT64_MAX);
        raw.setDeferSealSync(false);
        for (int64_t v = 0; raw.segmentBaseOffsets().size() < 3 && v < 10'000; ++v) {
            rune::protocol::RecordBatchBuilder b(raw.logEndOffset(), 1000);
            b.append(std::string_view("k"), std::string_view("value-" + std::to_string(v)), 1000);
            const auto batch = b.build();
            raw.append(batch, raw.logEndOffset());
        }
        raw.sync();
    }
    ASSERT_GE(rune::storage::testing::damageSecondBatchOfSegment(logDir.string(), 0), 0);
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
    EXPECT_TRUE(awaitMetric(node, RuneMetric::AuthMode, 2)) << "a default NodeConfig is in-process unauthenticated -> 2";
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
        live.sink.applied(41, 0xABCD, std::numeric_limits<int32_t>::max());
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

    // The observer's rows (Task 9): one publishRole after the election moves role and term off their -1 seeds; the
    // out-of-sync row publishes the observer's own reading (-1 = "not in a controlled shutdown" on a single node).
    node.publishRole();
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::Role),
              static_cast<std::int64_t>(rune::node::RoleCode::Leader));
    const auto term = rune::node::test::readRuneMetric(node, RuneMetric::Term);
    ASSERT_TRUE(term.has_value());
    EXPECT_GE(*term, 0) << "an elected term replaced the -1 seed";
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::OutOfSyncPartitions), node.outOfSyncPartitions());
#ifndef NDEBUG
    for (RuneMetric m : {RuneMetric::Role, RuneMetric::Term, RuneMetric::OutOfSyncPartitions}) {
        EXPECT_EQ(node.metricWrittenByForTest(m), rune::node::metricOwnerBit(rune::node::MetricOwner::Observer))
            << static_cast<std::int32_t>(m);
    }
#endif
    // The provisioner's rows: Live at 0 (no user topic is provisioned here; the write path is
    // SegmentProvisionerRegression.MetricsArePublishedThroughItsOwnSlotsRegression and the wire rig's
    // BrokerMetricsRegression.OwnerTableRegression).
    for (RuneMetric m : {RuneMetric::ProvisionBuildFailures, RuneMetric::ProvisionRollHolds,
                         RuneMetric::ProvisionUnpreallocBuilds, RuneMetric::ProvisionParkedDropped,
                         RuneMetric::ProvisionReadMapFailures}) {
        EXPECT_EQ(rune::node::test::readRuneMetric(node, m), 0) << static_cast<std::int32_t>(m);
    }
    node.stop();
}

// Task 9 (fix on sight: the lazy findCounter resolution): every row is allocated before the first control cycle, so
// the catalog and security rows hold their values straight after start, and one catalog report lands with that
// single store - no lookup that can miss on an early cycle and leave the value pending for a later retry.
TEST(BrokerMetricsRegression, PublishedFromFirstCycleRegression) {
    MetricsTempDir tmp{"first_"};
    NodeConfig cfg = singleNode(tmp.path);
    BrokerNode node{cfg};
    node.start();
    // One read each, no polling: the seeds are in the file before start() returns.
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::CatalogApplyHalted), 0);
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::CatalogAppliedOffset), -1);
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::SecurityApplyHalted), 0);
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::SecurityAppliedOffset), -1);
    ASSERT_TRUE(node.awaitLeader(5s));

    rune::node::AdminWorkerAgent* agent = node.adminWorkerAgentForTest();
    ASSERT_NE(agent, nullptr);
    node.onAdminThreadForTest([agent] {
        agent->catalogLiveEffects().sink.applied(7, 0x77, std::numeric_limits<int32_t>::max());
        return 0;
    });
    ASSERT_TRUE(awaitMetric(node, RuneMetric::CatalogAppliedOffset, 7)) << "the one report reached the file";
    // ...and stays: later control cycles with no new report neither retry nor rewrite it.
    for (int i = 0; i < 20; ++i) node.onAdminThreadForTest([] { return 0; });
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::CatalogAppliedOffset), 7);
    const auto sec = node.securityMetricsForTest();
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::SecurityAppliedOffset), sec.applied);
    EXPECT_EQ(rune::node::test::readRuneMetric(node, RuneMetric::SecurityApplyHalted), sec.halted);
    node.stop();
}

// cnc.dat holds no Rune counter any more (Task 9 moved the observer and provisioner rows too): no type id in Rune's
// 1'000'201-1'000'241 range is in the driver's counters file.
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
    std::vector<std::int32_t> runeInCnc;
    counters.forEach([&](std::int32_t, std::int32_t typeId, const aeron::concurrent::AtomicBuffer&,
                         const std::string&) {
        if (typeId < 1'000'000) ++driverCounters;
        if (typeId >= 1'000'201 && typeId <= 1'000'241) runeInCnc.push_back(typeId);
    });
    EXPECT_GT(driverCounters, 0) << "the scan saw the driver's own counters (not a vacuous pass)";
    EXPECT_TRUE(runeInCnc.empty()) << "first Rune type id still in cnc.dat: "
                                   << (runeInCnc.empty() ? 0 : runeInCnc.front());
    node.stop();
}

// A metrics file one slot too small refuses the start with the logged message, before any thread or the driver.
TEST(BrokerMetricsRegression, StartRefusedWhenFileTooSmallRegression) {
    MetricsTempDir tmp{"small_"};
    NodeConfig cfg = singleNode(tmp.path);
    const std::int32_t need = metrics::requiredSlots(rune::node::runeCapacityPlan(cfg.maxNamespaces));
    cfg.metricsMaxSlotsOverrideForTest = need - 1;
    const std::string err = startError(cfg);
    EXPECT_NE(err.find("metrics file " + cfg.effectiveMetricsFile() + ": CapacityTooSmall"), std::string::npos) << err;
    EXPECT_NE(err.find("need " + std::to_string(need) + " slots, have " + std::to_string(need - 1)), std::string::npos)
        << err;
    EXPECT_FALSE(fs::exists(fs::path{cfg.effectiveAeronDir()} / "cnc.dat")) << "the driver never started";
    EXPECT_FALSE(fs::exists(cfg.effectiveMetricsFile())) << "no file is left behind";
}

// A refused start inside createMetrics_ leaves NO file behind, wherever it stops: the first row, the 10th
// (rune-catalog-apply-halted) and the last.  A half-allocated file with a Role row would make rune_role print
// ROLE ... role=NONE with exit 0 for a process that never started (#631).
// RED (the unlink dropped from discardMetricsFile_): the file exists.
TEST(BrokerMetricsRegression, StartRefusedOnAllocFailureRegression) {
    using rune::node::kRuneMetrics;
    for (const std::int32_t row : {0, 9, static_cast<std::int32_t>(kRuneMetrics.size()) - 1}) {
        SCOPED_TRACE("failAllocAt=" + std::to_string(row));
        MetricsTempDir tmp{"alloc_"};
        NodeConfig cfg = singleNode(tmp.path);
        cfg.metricsFailAllocAtForTest = row;
        const auto& def = kRuneMetrics[static_cast<std::size_t>(row)];
        const std::string err = startError(cfg);
        EXPECT_NE(err.find(std::string{"metrics allocation failed for "} + std::string{def.labelPrefix} + " (type " +
                           std::to_string(static_cast<std::int32_t>(def.id)) + "): Full"),
                  std::string::npos)
            << err;
        EXPECT_FALSE(fs::exists(fs::path{cfg.effectiveAeronDir()} / "cnc.dat")) << "the driver never started";
        EXPECT_FALSE(fs::exists(cfg.effectiveMetricsFile())) << "no half-allocated file is left behind";
    }
}

// A previous run's file at the same path is replaced by create() and then discarded with the refused start: a stale
// role file of a dead process is not left behind.
TEST(BrokerMetricsRegression, StartRefusedOnAllocFailureRemovesAPreviousRunsFileRegression) {
    MetricsTempDir tmp{"alloc_prev_"};
    NodeConfig cfg = singleNode(tmp.path);
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(5s));
        node.stop();
    }
    ASSERT_TRUE(fs::exists(cfg.effectiveMetricsFile())) << "the good run left its file";
    cfg.metricsFailAllocAtForTest = 9;
    const std::string err = startError(cfg);
    EXPECT_NE(err.find("metrics allocation failed for"), std::string::npos) << err;
    EXPECT_FALSE(fs::exists(cfg.effectiveMetricsFile())) << "the refused start leaves no file, not even the old one";
}

// #631 review: a refusal inside createMetrics_ detaches the writer and drops its free stack along with the unmapped
// file, so a later allocate / write can never touch the unmapped mapping.
// RED (the writer reset and the free-stack clear dropped from discardMetricsFile_): the writer still holds its rows.
TEST(BrokerMetricsRegression, DiscardDetachesTheMetricsWriterRegression) {
    MetricsTempDir tmp{"detach_"};
    NodeConfig cfg = singleNode(tmp.path);
    cfg.metricsFailAllocAtForTest = 9;
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_EQ(node.metricsWriterAllocatedForTest(), 0) << "the writer still counts rows of the unmapped file";
    EXPECT_EQ(node.metricsFreeStackSizeForTest(), 0u) << "the free stack still names slots of the unmapped file";
}

// L10 (#631): a start that fails AFTER createMetrics_ returned leaves the complete, valid file (every row allocated,
// the observer's NONE / -1 values): only a refusal inside createMetrics_ removes it.  The later failure here is a
// per-worker replication peer with a different worker count.
TEST(BrokerMetricsRegression, LaterStartFailureLeavesACompleteFileRegression) {
    MetricsTempDir tmp{"later_"};
    NodeConfig cfg = singleNode(tmp.path);
    cfg.perWorkerReplication = true;   // an asymmetric peer worker count refuses the start, after createMetrics_
    PeerConfig other;
    other.id            = 1;
    other.host          = "127.0.0.1";
    other.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
    other.workerCount   = cfg.workerCount + 1;
    cfg.peers.push_back(other);
    const std::string err = startError(cfg);
    ASSERT_FALSE(err.empty()) << "the start must fail after the metrics file was created";
    ASSERT_TRUE(fs::exists(cfg.effectiveMetricsFile())) << err;
    for (const auto& def : kRuneMetrics) {
        const auto live = rune::node::test::readRuneMetricLiveAt(cfg.effectiveMetricsFile(),
                                                                 static_cast<std::int32_t>(def.id), cfg.nodeId);
        EXPECT_TRUE(live.has_value()) << "row " << def.labelPrefix << " missing from the kept file: " << err;
    }
}

#if defined(__linux__)
// #632: the process metrics (fd walk of /proc/self/fd, linear in open fds) are sampled by the ADMIN agent, never the
// control thread.  The row moves with the process's fds AND the admin agent counts its samples.
// RED (the sample moved back into the control loop): the row still moves, but the admin agent took 0 samples.
TEST(BrokerMetricsRegression, SystemMetricsSampleOnTheAdminAgentRegression) {
    MetricsTempDir tmp{"sysadmin_"};
    NodeConfig cfg = singleNode(tmp.path);
    cfg.metricsSystemIntervalMs = 1;
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    constexpr std::int32_t kOpenFds = 1000301;   // process_open_fds
    const auto readFds = [&]() -> std::int64_t {
        const auto v = rune::node::test::readRuneMetricAt(node.metricsFilePath(), kOpenFds, cfg.nodeId);
        return v.value_or(-1);
    };
    const auto await = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) return true;
            std::this_thread::sleep_for(1ms);
        }
        return pred();
    };
    ASSERT_TRUE(await([&] { return readFds() > 0; })) << "the first sample lands";
    const std::int64_t baseline = readFds();
    std::vector<int> fds;
    for (int i = 0; i < 64; ++i) fds.push_back(::dup(0));
    // The baseline sample may have counted readFds()'s own transient fd (one open of the metrics file per read).
    std::int64_t last = -1;
    const bool moved = await([&] { return (last = readFds()) >= baseline + 64 - 1; });
    for (const int fd : fds) if (fd >= 0) ::close(fd);
    EXPECT_TRUE(moved) << "process_open_fds follows 64 new fds (baseline " << baseline << ", last " << last << ")";
    EXPECT_TRUE(await([&] { return node.adminSystemMetricsSamplesForTest() >= 2; }))
        << "the admin agent took the samples: " << node.adminSystemMetricsSamplesForTest();
    node.stop();
}
#endif  // __linux__

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
    EXPECT_TRUE(awaitMetric(node, RuneMetric::AuthMode, 2));
    node.stop();
}

// #839: a sealed segment damaged on disk is counted by the boot gauge (summed over every log the node serves), and the
// node still starts.  RED (the sum skipped): the gauge reads 0.
TEST(BrokerMetricsRegression, DamagedSegmentsGaugeCountsBootDamageRegression) {
    MetricsTempDir tmp{"damaged_"};
    NodeConfig cfg = singleNode(tmp.path);
    rune::node::TopicSpec ts;
    ts.name              = "t";
    ts.partitionCount    = 1;
    ts.topicId           = 0;
    ts.segmentRollBytes  = 2048;
    ASSERT_NO_FATAL_FAILURE(rune::node::test::defineTopicsThenStop(cfg, {ts}));

    const fs::path partDir = rune::node::topicsRoot(tmp.path, 0) / ts.name / "0";
    {   // node stopped: a standalone Log writes the partition (single writer)
        rune::storage::Log raw(partDir.string(), ts.segmentRollBytes);
        raw.recover();
        raw.setEpochHistoryCap(INT64_MAX);   // the node's epoch-history guard holds R until its owner lifts it
        raw.setDeferSealSync(false);   // inline seals: the recovery point is persisted per roll
        for (int64_t v = 0; raw.segmentBaseOffsets().size() < 3 && v < 10'000; ++v) {
            rune::protocol::RecordBatchBuilder b(raw.logEndOffset(), 1000);
            b.append(std::string_view("k"), std::string_view("value-" + std::to_string(v)), 1000);
            const auto batch = b.build();
            raw.append(batch, raw.logEndOffset());
        }
        raw.append(rune::protocol::RecordBatchBuilder(raw.logEndOffset(), 1000).build(), raw.logEndOffset());
        raw.sync();
        ASSERT_GE(raw.segmentBaseOffsets().size(), 3u);
    }
    // Damage the first record byte of the 2nd batch of segment 0.
    fs::path seg0;
    for (const auto& e : fs::directory_iterator(partDir))
        if (e.path().extension() == ".log" && (seg0.empty() || e.path() < seg0)) seg0 = e.path();
    ASSERT_FALSE(seg0.empty());
    {
        std::ifstream in(seg0, std::ios::binary);
        std::vector<uint8_t> buf((std::istreambuf_iterator<char>(in)), {});
        in.close();
        rune::protocol::RecordBatchReader first(buf.data(), buf.size());
        const std::size_t second = static_cast<std::size_t>(first.totalLength());
        ASSERT_LT(second + rune::protocol::batch::kRecordBatchOverhead, buf.size());
        std::fstream f(seg0, std::ios::in | std::ios::out | std::ios::binary);
        const std::size_t at = second + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
        char c = static_cast<char>(buf[at] ^ 0x5A);
        f.seekp(static_cast<std::streamoff>(at));
        f.write(&c, 1);
    }
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10s));
    EXPECT_EQ(node.bootDamagedSegmentsForTest(), 1);
    EXPECT_TRUE(awaitMetric(node, RuneMetric::DamagedSegments, 1));
    node.stop();
}


// #839 (F2): a damaged sealed segment in a SYSTEM log (their loaders replay state from the log and must never load around a
// hole) refuses the start: critical line, halt path (exit 74), nothing served.  One leg per system log.  The logs are
// written by a standalone Log before the first boot; the refusal fires right after each log is recovered, before its first
// reader, so the content need not be a valid system-log format.
TEST(BrokerMetricsRegression, DamagedSystemLogRefusesStartRegression) {
    for (const char* leg : {"__consumer_offsets", "__transaction_state", "__cluster_metadata", "__catalog", "__security"}) {
        SCOPED_TRACE(leg);
        MetricsTempDir tmp{std::string("sysdmg_")};
        NodeConfig cfg = singleNode(tmp.path);
        cfg.offsetLogSegmentRollBytes = 2048;
        {   // a first boot writes node-state.dat / term.log (a data dir without them is refused as partly deleted)
            BrokerNode first{cfg};
            first.start();
            ASSERT_TRUE(first.awaitLeader(10s));
            first.stop();
        }
        fs::remove_all(tmp.path / leg);   // replaced by a standalone Log's damaged multi-segment log
        ASSERT_NO_FATAL_FAILURE(writeDamagedSealedLog(tmp.path / leg, 2048));
        BrokerNode node{cfg};
        HaltCapture halt;
        node.setHaltHandler(&HaltCapture::handler, &halt);
        try {
            node.start();
            ADD_FAILURE() << "a damaged system log never serves";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("damaged segment"), std::string::npos) << e.what();
        }
        EXPECT_EQ(halt.await(1000ms), BrokerNode::kExitStorageFailure) << "through the halt path (exit 74)";
        EXPECT_EQ(node.bootDamagedSegmentsForTest(), 1) << "the boot gauge counts the damage before the refusal";
        node.stop();
    }
}

#endif  // RUNE_BROKER_METRICS_NODE_TESTS
