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
#include "support/CatalogBrokerCluster.h"

#include <CncFileReader.h>
#include <concurrent/CountersReader.h>

#include <rune/node/AuthDevMode.h>
#include <rune/node/SecurityPartition.h>
#include <rune/security/SecurityEvent.h>
#include <rune/security/testing/SecurityGoldenScript.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <sys/stat.h>
#include <gtest/gtest.h>

#include <atomic>
#include <fstream>
#include <span>
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

// ===========================================================================
// ISR v3 Step 9 (section 3.10): the storage gate counter is published to the CnC file and an external reader sees it
// move: a wiped (Joiner) node reads UNCONFIRMED (1) while its storage is unconfirmed, then CONFIRMED (0).
// RED: no publish (the counter never leaves its initial value).
// ===========================================================================
namespace {
// The first counter of `typeId` in the CnC file at `aeronDir`; false when absent.
bool readCounterByType(const fs::path& aeronDir, std::int32_t typeId, std::int64_t& value) {
    try {
        aeron::CncFileReader reader = aeron::CncFileReader::mapExisting(aeronDir.string().c_str());
        aeron::concurrent::CountersReader counters = reader.countersReader();
        bool found = false;
        counters.forEach([&](std::int32_t id, std::int32_t t, const aeron::concurrent::AtomicBuffer&, const std::string&) {
            if (t == typeId && !found) { value = counters.getCounterValue(id); found = true; }
        });
        return found;
    } catch (...) {
        return false;
    }
}
}  // namespace

TEST(RoleCounterRegression, StorageGateCountersArePublishedRegression) {
    using namespace rune::node::test::catalog_cluster;
    ThreeNodes c = ThreeNodes::start("rolectr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int X = c.catalogLeaderId() == 1 ? 2 : 1;
    c.stop(X);
    c.wipe(X);
    // The joiner emits no follower fetch for a while, so its UNCONFIRMED state lasts long enough to be observed.
    c.configTweak = [&](NodeConfig& k) { if (k.nodeId == X) k.testHoldFollowerFetchesMs = 2000; };
    c.startNode(X);
    const fs::path aeronDir = c.dirs[static_cast<std::size_t>(X - 1)]->path / "aeron";
    bool sawOne = false, sawZeroAfter = false;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (!sawZeroAfter && std::chrono::steady_clock::now() < deadline) {
        std::int64_t v = -1;
        if (readCounterByType(aeronDir, kStorageGateStateCounterTypeId, v)) {
            if (v == 1) sawOne = true;
            if (v == 0 && sawOne) sawZeroAfter = true;
        }
        if (!sawZeroAfter) std::this_thread::sleep_for(20ms);
    }
    EXPECT_TRUE(sawOne) << "the storage gate counter never read UNCONFIRMED (1) on the wiped node";
    EXPECT_TRUE(sawZeroAfter) << "the storage gate counter never returned to CONFIRMED (0)";
}

// ===========================================================================
// Authentication Task 9: the auth-mode counter (1'000'228), the dev-mode warning, the cluster key check at start, and
// the `__security` metrics (1'000'226 halted offset + 1 / 1'000'227 applied offset), all read from the CnC file.
// ===========================================================================
namespace {

struct WarnCapture {   // RAII: never leaks the capture logger into later tests
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(1024);
    std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    WarnCapture() {
        auto capture = std::make_shared<spdlog::logger>("auth-capture", sink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~WarnCapture() { spdlog::set_default_logger(previous); }
    int count(const std::string& text) const {
        int n = 0;
        for (const std::string& l : sink->last_formatted()) n += l.find(text) != std::string::npos ? 1 : 0;
        return n;
    }
    WarnCapture(const WarnCapture&) = delete;
    WarnCapture& operator=(const WarnCapture&) = delete;
};

bool awaitCounter(const fs::path& aeronDir, std::int32_t typeId, std::int64_t want, std::chrono::milliseconds within) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        std::int64_t v = -999;
        if (readCounterByType(aeronDir, typeId, v) && v == want) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

}  // namespace

// Spec §9: RUNE_AUTH=disabled on one node starts, warns (once at start), and publishes counter 1'000'228 = 1.
TEST(NodeConfigRegression, DevModeSingleNodeStartsAndWarns) {
    TempDir tmp{"devmode_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.authMode = AuthMode::DevDisabled;
    WarnCapture log;   // installed BEFORE any broker thread exists, and outliving the node
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    const fs::path aeronDir = tmp.path / "aeron";
    EXPECT_TRUE(awaitCounter(aeronDir, kAuthModeCounterTypeId, 1, 10s)) << "counter 1'000'228 = 1 in dev mode";
    EXPECT_GE(log.count("AUTHENTICATION DISABLED (RUNE_AUTH=disabled)"), 1) << "the dev-mode warning at start";
    EXPECT_GE(log.count("every request runs as 'anonymous' with full access"), 1);
    node.stop();
}

// Combined fix program M13 (X102): the `__security` log holds every user's StoredKey / ServerKey.  Its directory is 0700
// and its segment, index and checkpoint files 0600 (the default-umask 0644 they used to get).  RED: pass the default mode
// to the security Log (or drop the permissions() call) - the files / directory are group- and world-readable again.
TEST(SecurityAtRestRegression, SecurityLogFilesAreOwnerOnlyRegression) {
    TempDir tmp{"atrest_"};
    const fs::path key = tmp.path.string() + "_atrest.key";
    { std::ofstream(key) << std::string(40, 'k') << "\n"; }
    fs::permissions(key, fs::perms::owner_read | fs::perms::owner_write);
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.authMode = AuthMode::Enabled;
    cfg.clusterKeyFile = key.string();
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    const fs::path dir = tmp.path / "__security";
    ASSERT_TRUE(fs::is_directory(dir));
    auto mode = [](const fs::path& p) { struct stat st {}; return ::stat(p.c_str(), &st) == 0 ? (st.st_mode & 0777) : mode_t{0xFFFF}; };
    EXPECT_EQ(mode(dir), mode_t{0700}) << "directory";
    int logs = 0, indexes = 0;
    for (const auto& e : fs::directory_iterator(dir)) {
        if (!e.is_regular_file()) continue;
        const std::string ext = e.path().extension().string();
        if (ext == ".log") ++logs;
        if (ext == ".index") ++indexes;
        if (ext == ".log" || ext == ".index" || e.path().filename() == "applied")
            EXPECT_EQ(mode(e.path()), mode_t{0600}) << e.path();
    }
    EXPECT_GE(logs, 1);
    EXPECT_GE(indexes, 1);
    node.stop();
}

// M13 (X102): a cluster key file readable by group or others refuses to start (it was only warned about); 0400 starts.
// RED: restore the warn-only branch in NodeConfig::validateAuth - the 0644 file starts.
TEST(SecurityAtRestRegression, AGroupOrWorldReadableClusterKeyRefusesToStartRegression) {
    TempDir tmp{"keymode_"};
    const fs::path key = tmp.path.string() + "_mode.key";
    { std::ofstream(key) << std::string(40, 'k') << "\n"; }
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    cfg.authMode = AuthMode::Enabled;
    cfg.clusterKeyFile = key.string();
    for (const auto bad : {fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read,
                           fs::perms::owner_read | fs::perms::owner_write | fs::perms::others_read,
                           fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::others_read}) {
        fs::permissions(key, bad);
        const auto err = cfg.validateAuth();
        ASSERT_TRUE(err.has_value()) << std::oct << static_cast<int>(bad);
        EXPECT_NE(err->find("RUNE_CLUSTER_KEY_FILE"), std::string::npos) << *err;
        EXPECT_NE(err->find("refusing to start"), std::string::npos) << *err;
    }
    fs::permissions(key, fs::perms::owner_read);
    EXPECT_FALSE(cfg.validateAuth().has_value()) << "0400 is what the launchers mount";
    fs::permissions(key, fs::perms::owner_read | fs::perms::owner_write);
    EXPECT_FALSE(cfg.validateAuth().has_value()) << "0600";
}

// An enabled broker with a good cluster key file starts and publishes 0; one with a 31-byte file refuses to start.
TEST(NodeConfigRegression, EnabledModeNeedsAUsableClusterKeyFileAtStartRegression) {
    TempDir tmp{"enabled_"};
    const fs::path good = tmp.path.string() + "_good.key";
    const fs::path bad  = tmp.path.string() + "_bad.key";
    { std::ofstream(good) << std::string(40, 'k') << "\n"; }
    { std::ofstream(bad) << std::string(31, 'k'); }
    fs::permissions(good, fs::perms::owner_read | fs::perms::owner_write);
    fs::permissions(bad, fs::perms::owner_read | fs::perms::owner_write);
    {
        NodeConfig cfg = makeSingleNodeConfig(tmp.path);
        cfg.authMode = AuthMode::Enabled;
        cfg.clusterKeyFile = bad.string();
        BrokerNode node{cfg};
        try {
            node.start();
            ADD_FAILURE() << "a short cluster key file must refuse to start";
            node.stop();
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("RUNE_CLUSTER_KEY_FILE"), std::string::npos) << e.what();
            EXPECT_NE(std::string(e.what()).find(bad.string()), std::string::npos) << e.what();
        }
    }
    TempDir tmp2{"enabled2_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp2.path);
    cfg.authMode = AuthMode::Enabled;
    cfg.clusterKeyFile = good.string();
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    EXPECT_TRUE(awaitCounter(tmp2.path / "aeron", kAuthModeCounterTypeId, 0, 10s)) << "counter 1'000'228 = 0 when enabled";
    node.stop();
    std::error_code ec; fs::remove(good, ec); fs::remove(bad, ec);
}

// Authentication Task 15: counters 1'000'233 (peers with an established link) and 1'000'234 (peer-link refusals
// logged by this broker, both directions) read from each node's CnC file.  Node 3 holds another cluster key: nodes 1
// and 2 link with each other only and each logs one refusal (they refused node 3's key); node 3 has no link and
// logs the refusals it received.
TEST(RoleCounterRegression, PeerLinkCountersArePublishedRegression) {
    using namespace rune::node::test::catalog_cluster;
    const fs::path keyDir = fs::temp_directory_path() / ("plctr_" + std::to_string(::getpid()));
    fs::create_directories(keyDir);
    { std::ofstream(keyDir / "k.key") << std::string(48, 'k') << "\n"; }
    { std::ofstream(keyDir / "x.key") << std::string(48, 'x') << "\n"; }
    for (const char* n : {"k.key", "x.key"})   // M13: a group/world-readable key refuses to start
        fs::permissions(keyDir / n, fs::perms::owner_read | fs::perms::owner_write);
    ThreeNodes c = ThreeNodes::startWith("plctr", [&](NodeConfig& k) {
        k.authMode = AuthMode::Enabled;
        k.clusterKeyFile = ((k.nodeId == 3) ? keyDir / "x.key" : keyDir / "k.key").string();
    });
    auto aeronDir = [&](int id) { return c.dirs[static_cast<std::size_t>(id - 1)]->path / "aeron"; };
    EXPECT_TRUE(awaitCounter(aeronDir(1), kPeerLinksEstablishedCounterTypeId, 1, 20s)) << "node 1 links with node 2 only";
    EXPECT_TRUE(awaitCounter(aeronDir(2), kPeerLinksEstablishedCounterTypeId, 1, 20s));
    EXPECT_TRUE(awaitCounter(aeronDir(3), kPeerLinksEstablishedCounterTypeId, 0, 20s));
    // 1'000'235: node 3 has no link at all - every frame it tried to send (consensus, the controller / replication
    // transport, its workers' planes, the admin plane) was dropped and counted; the control thread folds the workers'
    // and the admin plane's paced reports into the one counter.
    {
        const auto deadline = std::chrono::steady_clock::now() + 30s;
        bool workers = false, admin = false;
        while (std::chrono::steady_clock::now() < deadline && !(workers && admin)) {
            const auto snap = c.node(3).peerLinksForTest();
            workers = snap.workerDropsReported > 0;
            admin   = snap.adminDropsReported > 0;
            if (!(workers && admin)) std::this_thread::sleep_for(50ms);
        }
        EXPECT_TRUE(workers) << "node 3's worker plane reported its dropped peer frames to the control thread";
        EXPECT_TRUE(admin)   << "node 3's admin plane reported its dropped peer frames to the control thread";
        std::int64_t drops = -1;
        const auto until = std::chrono::steady_clock::now() + 20s;
        while (std::chrono::steady_clock::now() < until) {
            if (readCounterByType(aeronDir(3), kPeerFrameDropsCounterTypeId, drops) && drops >= 3) break;
            std::this_thread::sleep_for(50ms);
        }
        EXPECT_GE(drops, 3) << "counter 1'000'235 sums every owner's drops";
        const auto snap = c.node(3).peerLinksForTest();
        EXPECT_GE(static_cast<uint64_t>(drops), snap.workerDropsReported + snap.adminDropsReported);
    }
    for (int id : {1, 2, 3}) {
        std::int64_t refusals = -1;
        const auto deadline = std::chrono::steady_clock::now() + 20s;
        while (std::chrono::steady_clock::now() < deadline) {
            if (readCounterByType(aeronDir(id), kPeerLinkRefusalsCounterTypeId, refusals) && refusals >= 1) break;
            std::this_thread::sleep_for(20ms);
        }
        EXPECT_GE(refusals, 1) << "node " << id << " logged (and counted) a refusal";
    }
    std::error_code ec; fs::remove_all(keyDir, ec);
}

// The `__security` counters: applied offset follows the mirrored events; a record the rules refuse raises the halt
// counter to (offset + 1) - the operator alert of D28.
TEST(RoleCounterTest, SecurityApplyMetricsReadableFromCncRegression) {
    TempDir tmp{"secmetrics_"};
    NodeConfig cfg = makeSingleNodeConfig(tmp.path);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(5s));
    const fs::path aeronDir = tmp.path / "aeron";
    ASSERT_TRUE(awaitCounter(aeronDir, kSecurityAppliedOffsetCounterTypeId, -1, 10s)) << "registered, nothing applied";
    ASSERT_TRUE(awaitCounter(aeronDir, kSecurityApplyHaltedCounterTypeId, 0, 10s));

    auto events = rune::security::testing::goldenScriptV1();
    events[2].fingerprint ^= 1;
    auto append = [&](std::size_t i) {
        auto bytes = rune::security::encode(events[i]);
        int64_t base = -1;
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (base < 0 && std::chrono::steady_clock::now() < deadline) {
            base = node.appendSecurityRecordOnLeaderForTest(std::span<const uint8_t>(*bytes));
            if (base < 0) std::this_thread::sleep_for(20ms);
        }
        return base;
    };
    ASSERT_EQ(append(0), 0);
    ASSERT_EQ(append(1), 1);
    EXPECT_TRUE(awaitCounter(aeronDir, kSecurityAppliedOffsetCounterTypeId, 1, 10s));
    EXPECT_TRUE(awaitCounter(aeronDir, kSecurityApplyHaltedCounterTypeId, 0, 1s));
    ASSERT_EQ(append(2), 2);   // tampered
    EXPECT_TRUE(awaitCounter(aeronDir, kSecurityApplyHaltedCounterTypeId, 3, 10s)) << "halted offset 2 -> counter 3";
    EXPECT_TRUE(awaitCounter(aeronDir, kSecurityAppliedOffsetCounterTypeId, 1, 1s)) << "applied stays at the last good event";
    node.stop();
}
