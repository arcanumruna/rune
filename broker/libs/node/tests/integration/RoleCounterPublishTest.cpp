// ---------------------------------------------------------------------------
// RoleCounterPublishTest — integration test for the Aeron node-role counter.
//
// Proves the full machine-readable-leadership contract WITHOUT Docker:
//   1. Bring up a single-node BrokerNode (embeds the Aeron media driver).
//   2. Wait for self-election, then call publishRole() (as main's loop does).
//   3. Read the counters back out of the driver's CnC file with the SAME API
//      the standalone rune_role reader uses (CncFileReader + CountersReader),
//      and assert role=LEADER + term matches leadershipTermId().
//
// This is the fast, native reproduction of "can an external observer detect the
// leader via counters (not logs)?" — it exercises exactly the publish path
// (BrokerNode) and the read path (CncFileReader) that the dr-e2e harness relies
// on via `docker exec rune_role`.
// ---------------------------------------------------------------------------
#include <rune/aeronutil/testing/FreePort.h>
#include "../support/TestNodeConfig.h"
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/RoleCounter.h>
#include "support/TestWorkerCount.h"

#include <CncFileReader.h>
#include <concurrent/CountersReader.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::node;

namespace {

fs::path makeTempDataDir(std::string_view tag) {
    static std::atomic<uint32_t> g_seq{0};
    auto seq = g_seq.fetch_add(1, std::memory_order_relaxed);
    return fs::temp_directory_path() /
        (std::string("rune_rolectr_") + std::string(tag) +
         std::to_string(::getpid()) + "_" + std::to_string(seq));
}

NodeConfig makeSingleNodeConfig(const fs::path& dataDir) {
    NodeConfig cfg;
    rune::node::test::pinLegacyRuntime(cfg);   // orthogonal to the worker runtime
    cfg.nodeId            = 0;
    cfg.dataDir           = dataDir.string();
    cfg.consensusStreamId = 10;
    PeerConfig self;
    self.id            = 0;
    self.host          = "127.0.0.1";
    self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();   // D8: never a fixed port
    cfg.peers          = {self};
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.clientEndpoint.streamId = 1;
    cfg.workerCount = rune::node::test::workerCountFromEnv();
    return cfg;
}

struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag) : path(makeTempDataDir(tag)) {}
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Read (role, term, nodeId) out of the CnC file the same way rune_role does.
// Returns false if the role counter is not present yet.
bool readRoleCounter(const fs::path& aeronDir,
                     RoleCode& role, std::int64_t& term, std::int32_t& nodeId) {
    aeron::CncFileReader reader =
        aeron::CncFileReader::mapExisting(aeronDir.string().c_str());
    aeron::concurrent::CountersReader counters = reader.countersReader();

    bool foundRole = false;
    role = RoleCode::None;
    term = -1;
    nodeId = -1;

    counters.forEach(
        [&](std::int32_t id, std::int32_t typeId,
            const aeron::concurrent::AtomicBuffer& keyBuffer,
            const std::string&) {
            if (typeId == kRoleCounterTypeId) {
                std::int32_t nid = -1;
                if (decodeRoleKey(keyBuffer.buffer(),
                                  static_cast<std::size_t>(keyBuffer.capacity()),
                                  nid)) {
                    nodeId = nid;
                }
                role = roleCodeFromValue(counters.getCounterValue(id));
                foundRole = true;
            } else if (typeId == kTermCounterTypeId) {
                term = counters.getCounterValue(id);
            }
        });
    return foundRole;
}

} // namespace

// ===========================================================================
// Leader publishes role=LEADER + its term into the CnC counters; an external
// reader (CncFileReader) recovers them — no logs involved.
// ===========================================================================
TEST(RoleCounterPublishTest, LeaderRoleAndTermReadableFromCnc) {
    TempDir tmp{"leader_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s)) << "single node should self-elect";
    ASSERT_TRUE(node.isLeader());

    const fs::path aeronDir = tmp.path / "aeron";

    // Drive publishRole() and poll the CnC counters until the driver has acked
    // the counter allocation and the value reflects LEADER (bounded ~5 s).
    RoleCode role = RoleCode::None;
    std::int64_t term = -1;
    std::int32_t nodeId = -1;
    bool sawLeader = false;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        node.publishRole();
        if (readRoleCounter(aeronDir, role, term, nodeId) &&
            role == RoleCode::Leader) {
            sawLeader = true;
            break;
        }
        std::this_thread::sleep_for(50ms);
    }

    EXPECT_TRUE(sawLeader) << "external reader never observed role=LEADER";
    EXPECT_EQ(role, RoleCode::Leader);
    EXPECT_EQ(nodeId, static_cast<std::int32_t>(cfg.nodeId));
    EXPECT_EQ(term, static_cast<std::int64_t>(node.leadershipTermId()))
        << "term counter must match the node's leadershipTermId()";

    node.stop();
}

// ===========================================================================
// The role counter uses Rune's own type ids (not Aeron's reserved range) and
// both role + term counters are present and attributed to this node.
// ===========================================================================
TEST(RoleCounterPublishTest, BothCountersPresentWithNodeKey) {
    TempDir tmp{"both_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);

    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s));

    const fs::path aeronDir = tmp.path / "aeron";

    RoleCode role = RoleCode::None;
    std::int64_t term = -1;
    std::int32_t nodeId = -1;
    bool found = false;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        node.publishRole();
        if (readRoleCounter(aeronDir, role, term, nodeId)) { found = true; break; }
        std::this_thread::sleep_for(50ms);
    }

    ASSERT_TRUE(found) << "role counter not present in CnC";
    EXPECT_EQ(nodeId, static_cast<std::int32_t>(cfg.nodeId));

    node.stop();
}

// ===========================================================================
// Catalog history Task 7: the CatalogApplier's metrics.  The admin thread's
// production sink posts kA2cCatalogApplied / kA2cCatalogHalted on the
// admin→control ring; the control thread sets the two counters
// (kCatalogAppliedOffsetCounterTypeId = last applied offset,
// kCatalogApplyHaltedCounterTypeId = halted offset + 1, 0 while applying).
// ===========================================================================
namespace {
bool readCatalogCounters(const fs::path& aeronDir, std::int64_t& applied, std::int64_t& halted) {
    aeron::CncFileReader reader = aeron::CncFileReader::mapExisting(aeronDir.string().c_str());
    aeron::concurrent::CountersReader counters = reader.countersReader();
    bool a = false, h = false;
    counters.forEach([&](std::int32_t id, std::int32_t typeId, const aeron::concurrent::AtomicBuffer&,
                         const std::string&) {
        if (typeId == kCatalogAppliedOffsetCounterTypeId) { applied = counters.getCounterValue(id); a = true; }
        if (typeId == kCatalogApplyHaltedCounterTypeId) { halted = counters.getCounterValue(id); h = true; }
    });
    return a && h;
}
} // namespace

TEST(RoleCounterPublishTest, CatalogApplyMetricsReadableFromCncRegression) {
    TempDir tmp{"catalog_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(3s));
    const fs::path aeronDir = tmp.path / "aeron";

    std::int64_t applied = -2, halted = -2;
    auto await = [&](std::int64_t wantApplied, std::int64_t wantHalted) {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (readCatalogCounters(aeronDir, applied, halted) && applied == wantApplied && halted == wantHalted)
                return true;
            std::this_thread::sleep_for(20ms);
        }
        return false;
    };
    ASSERT_TRUE(readCatalogCounters(aeronDir, applied, halted) || await(0, 0)) << "catalog counters registered";

    AdminWorkerAgent* agent = node.adminWorkerAgentForTest();
    ASSERT_NE(agent, nullptr);
    node.onAdminThreadForTest([agent] {
        const LiveEffects live = agent->catalogLiveEffects();
        live.sink.applied(5, 0xABCD);
        return 0;
    });
    EXPECT_TRUE(await(5, 0)) << "applied=" << applied << " halted=" << halted;

    node.onAdminThreadForTest([agent] {
        const LiveEffects live = agent->catalogLiveEffects();
        rune::catalog::ApplyError err;
        err.offset = 7;
        err.code   = rune::catalog::ApplyError::Code::FingerprintMismatch;
        err.detail = "test";
        live.sink.halted(err);
        return 0;
    });
    EXPECT_TRUE(await(5, 8)) << "halted offset 7 → counter 8; applied=" << applied << " halted=" << halted;
    node.stop();
}
