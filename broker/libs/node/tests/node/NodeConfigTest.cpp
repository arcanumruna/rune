#include <rune/node/NodeConfig.h>
#include <rune/catalog/TopicName.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/HostDefaults.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>   // Worker::maxAcceptableBatchBytes (THE batch limit)

#include <filesystem>

#include <gtest/gtest.h>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build a getenv_fn that looks up keys in a fixed map.
/// Never touches the real process environment.
static std::function<const char*(const char*)>
fakeEnv(std::map<std::string, std::string> m)
{
    // Capture by value so the lambda owns the map.
    return [m = std::move(m)](const char* key) -> const char* {
        auto it = m.find(key);
        return it != m.end() ? it->second.c_str() : nullptr;
    };
}

/// Minimal valid 3-node environment.
static std::map<std::string, std::string> base3node() {
    return {
        {"RUNE_NODE_ID",           "1"},
        {"RUNE_PEERS",             "0@rune-1:40111,1@rune-2:40111,2@rune-3:40111"},
        {"RUNE_CLIENT_ENDPOINT",   "rune-2:50001"},
        {"RUNE_DATA_DIR",          "/data/rune"},
    };
}

/// Minimal valid single-node environment.
static std::map<std::string, std::string> base1node() {
    return {
        {"RUNE_NODE_ID",           "0"},
        {"RUNE_PEERS",             "0@rune-1:40111"},
        {"RUNE_CLIENT_ENDPOINT",   "rune-1:50001"},
        {"RUNE_DATA_DIR",          "/data/rune"},
    };
}

/// Legacy runtime pinned: the flag-parsing tests below are about LEGACY-mode
/// flags (shard mode — the default since 2026-10-02 — forces them on).
static std::map<std::string, std::string> legacyEnv(std::map<std::string, std::string> m) {
    m["RUNE_WORKER_MODE"] = "legacy";
    m["RUNE_RUNTIME_PLACEMENT"] = "thread";
    return m;
}

// ---------------------------------------------------------------------------
// Happy-path tests
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, ValidThreeNodeConfig) {
    auto r = parseNodeConfig(fakeEnv(base3node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_TRUE(r.config.has_value());

    const NodeConfig& cfg = *r.config;
    EXPECT_EQ(cfg.nodeId, 1);
    EXPECT_EQ(cfg.peers.size(), 3u);
    EXPECT_FALSE(cfg.isSingleNode());

    // Sorted by id
    EXPECT_EQ(cfg.peers[0].id, 0);
    EXPECT_EQ(cfg.peers[1].id, 1);
    EXPECT_EQ(cfg.peers[2].id, 2);

    // self() returns the peer matching nodeId
    EXPECT_EQ(cfg.self().id,   1);
    EXPECT_EQ(cfg.self().host, "rune-2");
    EXPECT_EQ(cfg.self().consensusPort, 40111u);

    // clientEndpoint
    EXPECT_EQ(cfg.clientEndpoint.channel,  "aeron:udp?endpoint=rune-2:50001");
    EXPECT_EQ(cfg.clientEndpoint.streamId, 1); // default

    // consensusStreamId default
    EXPECT_EQ(cfg.consensusStreamId, 10);

    // dataDir
    EXPECT_EQ(cfg.dataDir, "/data/rune");

    // consensusChannel per peer
    EXPECT_EQ(cfg.peers[0].consensusChannel(), "aeron:udp?endpoint=rune-1:40111");
    EXPECT_EQ(cfg.peers[1].consensusChannel(), "aeron:udp?endpoint=rune-2:40111");
    EXPECT_EQ(cfg.peers[2].consensusChannel(), "aeron:udp?endpoint=rune-3:40111");
}

TEST(NodeConfigTest, ValidSingleNodeConfig) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_TRUE(r.config.has_value());

    const NodeConfig& cfg = *r.config;
    EXPECT_EQ(cfg.nodeId, 0);
    EXPECT_EQ(cfg.peers.size(), 1u);
    EXPECT_TRUE(cfg.isSingleNode());
    EXPECT_EQ(cfg.self().id, 0);
}

TEST(NodeConfigTest, PeersSortedByIdRegardlessOfInputOrder) {
    auto env = base3node();
    env["RUNE_PEERS"] = "2@rune-3:40111,0@rune-1:40111,1@rune-2:40111";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    const NodeConfig& cfg = *r.config;
    EXPECT_EQ(cfg.peers[0].id, 0);
    EXPECT_EQ(cfg.peers[1].id, 1);
    EXPECT_EQ(cfg.peers[2].id, 2);
}

// ---------------------------------------------------------------------------
// Stream-id defaults and overrides
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, StreamIdDefaultsWhenUnset) {
    auto r = parseNodeConfig(fakeEnv(base3node()));
    ASSERT_TRUE(r.error.empty());
    EXPECT_EQ(r.config->clientEndpoint.streamId, 1);
    EXPECT_EQ(r.config->consensusStreamId,        10);
}

TEST(NodeConfigTest, StreamIdOverriddenWhenSet) {
    auto env = base3node();
    env["RUNE_CLIENT_STREAM_ID"]    = "7";
    env["RUNE_CONSENSUS_STREAM_ID"] = "42";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->clientEndpoint.streamId, 7);
    EXPECT_EQ(r.config->consensusStreamId,        42);
}

TEST(NodeConfigTest, NonIntClientStreamIdIsError) {
    auto env = base3node();
    env["RUNE_CLIENT_STREAM_ID"] = "abc";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, NonIntConsensusStreamIdIsError) {
    auto env = base3node();
    env["RUNE_CONSENSUS_STREAM_ID"] = "xyz";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

// ---------------------------------------------------------------------------
// read-rework T3/T4: per-worker dynamic-MDC egress config (I1b)
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, WorkerEgressDefaultsWhenUnset) {
    auto r = parseNodeConfig(fakeEnv(base3node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->workerEgressControlHost,     "") << "derived (review B-I3)";
    EXPECT_EQ(r.config->effectiveWorkerEgressControlHost(), "0.0.0.0") << "named peers: the wildcard";
    EXPECT_FALSE(r.config->workerEgressControlBasePort.has_value());
    EXPECT_EQ(r.config->effectiveWorkerEgressControlBasePort(), 40300);
    EXPECT_EQ(r.config->workerEgressBaseStreamId,     0);  // 0 = resolved in buildWorkerPool
}

TEST(NodeConfigTest, WorkerEgressOverriddenWhenSet) {
    auto env = base3node();
    env["RUNE_WORKER_EGRESS_CONTROL_HOST"]      = "10.0.0.5";
    env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = "45000";
    env["RUNE_WORKER_EGRESS_BASE_STREAM"]       = "99";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->workerEgressControlHost,     "10.0.0.5");
    EXPECT_EQ(r.config->workerEgressControlBasePort, std::optional<int32_t>{45000});
    EXPECT_EQ(r.config->effectiveWorkerEgressControlBasePort(), 45000);
    EXPECT_EQ(r.config->workerEgressBaseStreamId,     99);
}

TEST(NodeConfigTest, WorkerEgressEmptyHostFallsBackToDefault) {
    auto env = base3node();
    env["RUNE_WORKER_EGRESS_CONTROL_HOST"] = "";  // empty → keep default (derived)
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->effectiveWorkerEgressControlHost(), "0.0.0.0");
}

// Review B-I3: the MDC control host defaults to the address clients reach this node at (they send to
// <advertised RUNE_PEERS host>:<port>) — the numeric advertised host exactly, a name -> the wildcard; an all-loopback
// cluster keeps 127.0.0.1; an explicit value wins.  RED: the old "127.0.0.1" default ((a) and (b) fail).
TEST(NodeConfigRegression, EgressControlHostDefaultsToTheAdvertisedHostRegression) {
    auto peersEnv = [](const std::string& peers, const char* host) {
        auto env = base3node();
        env["RUNE_PEERS"] = peers;
        if (host) env["RUNE_WORKER_EGRESS_CONTROL_HOST"] = host;
        auto r = parseNodeConfig(fakeEnv(env));
        EXPECT_TRUE(r.error.empty()) << r.error;
        return r.config ? r.config->effectiveWorkerEgressControlHost() : std::string("<none>");
    };
    EXPECT_EQ(peersEnv("0@10.0.0.5:40111,1@10.0.0.6:40111,2@10.0.0.7:40111", nullptr), "10.0.0.6");      // (a) self = node 1
    EXPECT_EQ(peersEnv("0@rune-1:40111,1@rune-2:40111,2@rune-3:40111", nullptr), "0.0.0.0");             // (b)
    EXPECT_EQ(peersEnv("0@127.0.0.1:40111,1@127.0.0.1:40112,2@127.0.0.1:40113", nullptr), "127.0.0.1");  // (c)
    EXPECT_EQ(peersEnv("0@10.0.0.5:40111,1@10.0.0.6:40111,2@10.0.0.7:40111", "10.0.0.9"), "10.0.0.9");    // (d)
    // Re-check I2: RUNE_PEER_BIND_HOST (NAT / port-mapped: the advertised address is on no local interface).
    auto natEnv = [](const char* host) {
        auto env = base3node();
        env["RUNE_PEERS"] = "0@203.0.113.5:40111,1@203.0.113.6:40111,2@203.0.113.7:40111";
        env["RUNE_PEER_BIND_HOST"] = "0.0.0.0";
        if (host) env["RUNE_WORKER_EGRESS_CONTROL_HOST"] = host;
        auto r = parseNodeConfig(fakeEnv(env));
        EXPECT_TRUE(r.error.empty()) << r.error;
        return r.config ? r.config->effectiveWorkerEgressControlHost() : std::string("<none>");
    };
    EXPECT_EQ(natEnv(nullptr), "0.0.0.0") << "(e) the peer bind override, never the unbindable advertised address";
    EXPECT_EQ(natEnv("10.0.0.9"), "10.0.0.9") << "(f) an explicit control host still wins";
}

// Re-check I2: a LOOPBACK RUNE_PEER_BIND_HOST supplying the control host of a node advertised at a non-loopback
// host refuses, as the explicit host does.  RED: the refusal checks only an explicit control host.
TEST(NodeConfigRegression, LoopbackPeerBindOverrideOnANonLoopbackNodeRefusesStartRegression) {
    auto env = base3node();
    env["RUNE_PEER_BIND_HOST"] = "127.0.0.1";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    const auto err = r.config->validateShardConfig();
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("silently fall back"), std::string::npos) << *err;
    EXPECT_NE(err->find("RUNE_PEER_BIND_HOST"), std::string::npos) << *err;
}

// Review B-I3: an explicit LOOPBACK control host on a node advertised at a non-loopback host refuses to start
// (clients elsewhere could never reach per-worker egress; it would silently fall back).  RED: no check.
TEST(NodeConfigRegression, LoopbackEgressControlHostOnANonLoopbackNodeRefusesStartRegression) {
    auto env = base3node();
    env["RUNE_WORKER_EGRESS_CONTROL_HOST"] = "127.0.0.1";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    const auto err = r.config->validateShardConfig();
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("silently fall back"), std::string::npos) << *err;
    // An all-loopback cluster with an explicit loopback host is fine.
    auto lo = base3node();
    lo["RUNE_PEERS"] = "0@127.0.0.1:40111,1@127.0.0.1:40112,2@127.0.0.1:40113";
    lo["RUNE_WORKER_EGRESS_CONTROL_HOST"] = "127.0.0.1";
    auto rl = parseNodeConfig(fakeEnv(lo));
    ASSERT_TRUE(rl.config.has_value()) << rl.error;
    const auto el = rl.config->validateShardConfig();
    EXPECT_TRUE(!el.has_value() || el->find("silently fall back") == std::string::npos) << *el;
}

TEST(NodeConfigTest, NonIntWorkerEgressControlBasePortIsError) {
    auto env = base3node();
    env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = "not-a-port";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, NonIntWorkerEgressBaseStreamIsError) {
    auto env = base3node();
    env["RUNE_WORKER_EGRESS_BASE_STREAM"] = "xyz";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, PerWorkerEgressFlagDefaultsFalseAndParses) {
    // Default (unset) → false.
    {
        auto r = parseNodeConfig(fakeEnv(legacyEnv(base3node())));
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_FALSE(r.config->perWorkerEgressEnabled);
    }
    // "1"/"true"/"TRUE" → true.
    for (const char* v : {"1", "true", "TRUE"}) {
        auto env = legacyEnv(base3node());
        env["RUNE_PER_WORKER_EGRESS"] = v;
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_TRUE(r.config->perWorkerEgressEnabled) << "value=" << v;
    }
    // Other non-empty → false.
    {
        auto env = legacyEnv(base3node());
        env["RUNE_PER_WORKER_EGRESS"] = "no";
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_FALSE(r.config->perWorkerEgressEnabled);
    }
}

// ---------------------------------------------------------------------------
// Missing required variables (5 cases)
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, MissingNodeIdIsError) {
    auto env = base3node();
    env.erase("RUNE_NODE_ID");
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_NODE_ID"), std::string::npos);
}

TEST(NodeConfigTest, MissingPeersIsError) {
    auto env = base3node();
    env.erase("RUNE_PEERS");
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_PEERS"), std::string::npos);
}

TEST(NodeConfigTest, MissingClientEndpointIsError) {
    auto env = base3node();
    env.erase("RUNE_CLIENT_ENDPOINT");
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_CLIENT_ENDPOINT"), std::string::npos);
}

TEST(NodeConfigTest, MissingDataDirIsError) {
    auto env = base3node();
    env.erase("RUNE_DATA_DIR");
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_DATA_DIR"), std::string::npos);
}

TEST(NodeConfigTest, EmptyNodeIdIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

// ---------------------------------------------------------------------------
// RUNE_NODE_ID parse errors
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, NodeIdNotIntIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "notanint";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, NodeIdNotInPeersIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "5";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("5"), std::string::npos);
    EXPECT_NE(r.error.find("RUNE_PEERS"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Malformed peer entries
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, PeersMissingAtSignIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "bad";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersEmptyIdBeforeAtIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "@host:1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersEmptyHostIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "1@:1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersEmptyPortIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "1@host:";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersNonIntIdIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "x@host:1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersNonIntPortIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "1";
    env["RUNE_PEERS"]   = "1@host:notaport";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersPortOutOfRangeIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "1";
    env["RUNE_PEERS"]   = "1@host:70000";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersPortZeroIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "1";
    env["RUNE_PEERS"]   = "1@host:0";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

TEST(NodeConfigTest, PeersEmptyStringIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_PEERS"), std::string::npos);
}

TEST(NodeConfigTest, PeersTrailingCommaIsError) {
    auto env = base3node();
    // Trailing comma produces an empty entry which must be rejected
    env["RUNE_PEERS"] = "0@rune-1:40111,1@rune-2:40111,";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
}

// ---------------------------------------------------------------------------
// Duplicate peer ids
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, DuplicatePeerIdsIsError) {
    auto env = base3node();
    env["RUNE_NODE_ID"] = "0";
    env["RUNE_PEERS"]   = "0@a:1000,0@b:2000";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("duplicate"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Whitespace tolerance
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, WhitespaceTrimmedAroundEntries) {
    // Spaces around commas and entries must be trimmed correctly.
    auto env = base3node();
    env["RUNE_NODE_ID"] = " 1 ";
    env["RUNE_PEERS"]   = " 0@rune-1:40111 , 1@rune-2:40111 , 2@rune-3:40111 ";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->nodeId, 1);
    EXPECT_EQ(r.config->peers.size(), 3u);
    EXPECT_EQ(r.config->peers[0].host, "rune-1");
    EXPECT_EQ(r.config->peers[1].host, "rune-2");
}

// ---------------------------------------------------------------------------
// Aeron channel format
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, ConsensusChannelFormat) {
    PeerConfig p;
    p.host          = "rune-2";
    p.consensusPort = 40111;
    EXPECT_EQ(p.consensusChannel(), "aeron:udp?endpoint=rune-2:40111");
}

TEST(NodeConfigTest, ClientEndpointChannelFormat) {
    auto env = base3node();
    env["RUNE_CLIENT_ENDPOINT"] = "rune-2:50001";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty());
    EXPECT_EQ(r.config->clientEndpoint.channel, "aeron:udp?endpoint=rune-2:50001");
}

// ---------------------------------------------------------------------------
// Election timing env vars (BR-3b)
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, ElectionTimingDefaultsWhenUnset) {
    auto r = parseNodeConfig(fakeEnv(base3node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    // Defaults: 150 ms / 20 ms / 100 ms
    EXPECT_EQ(r.config->electionTimeoutMs,        150LL);
    EXPECT_EQ(r.config->electionStatusIntervalMs,  20LL);
    EXPECT_EQ(r.config->startupCanvassTimeoutMs,  100LL);
}

TEST(NodeConfigTest, ElectionTimingOverriddenWhenSet) {
    auto env = base3node();
    env["RUNE_ELECTION_TIMEOUT_MS"]         = "1000";
    env["RUNE_ELECTION_STATUS_INTERVAL_MS"] = "200";
    env["RUNE_STARTUP_CANVASS_TIMEOUT_MS"]  = "2000";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->electionTimeoutMs,        1000LL);
    EXPECT_EQ(r.config->electionStatusIntervalMs,  200LL);
    EXPECT_EQ(r.config->startupCanvassTimeoutMs,  2000LL);
}

TEST(NodeConfigTest, ElectionTimeoutZeroIsError) {
    auto env = base3node();
    env["RUNE_ELECTION_TIMEOUT_MS"] = "0";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_ELECTION_TIMEOUT_MS"), std::string::npos);
}

TEST(NodeConfigTest, ElectionStatusIntervalNonIntIsError) {
    auto env = base3node();
    env["RUNE_ELECTION_STATUS_INTERVAL_MS"] = "abc";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_ELECTION_STATUS_INTERVAL_MS"), std::string::npos);
}

TEST(NodeConfigTest, StartupCanvassTimeoutNegativeIsError) {
    auto env = base3node();
    env["RUNE_STARTUP_CANVASS_TIMEOUT_MS"] = "-1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("RUNE_STARTUP_CANVASS_TIMEOUT_MS"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Per-peer client ports (GB-4)
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, PeerWithClientPort_ParsesCorrectly) {
    // Three-segment format: id@host:consensusPort:clientPort
    auto env = base3node();
    env["RUNE_PEERS"] = "0@rune-1:40111:40200,1@rune-2:40111:40200,2@rune-3:40111:40200";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->peers.size(), 3u);

    EXPECT_EQ(r.config->peers[0].clientPort, 40200u);
    EXPECT_EQ(r.config->peers[1].clientPort, 40200u);
    EXPECT_EQ(r.config->peers[2].clientPort, 40200u);
}

TEST(NodeConfigTest, PeerWithClientPort_ClientChannelFormat) {
    PeerConfig p;
    p.host          = "rune-2";
    p.consensusPort = 40111;
    p.clientPort    = 40200;
    EXPECT_EQ(p.clientChannel(), "aeron:udp?endpoint=rune-2:40200");
}

TEST(NodeConfigTest, PeerWithoutClientPort_ClientChannelEmpty) {
    // Backward-compatible: omitting :clientPort → clientPort=0 → clientChannel()=""
    PeerConfig p;
    p.host          = "rune-2";
    p.consensusPort = 40111;
    p.clientPort    = 0;
    EXPECT_TRUE(p.clientChannel().empty())
        << "clientChannel() must be empty when clientPort == 0";
}

TEST(NodeConfigTest, PeerBackwardCompat_NoClientPort_ParsesOk) {
    // Existing "id@host:port" format (no clientPort) must still parse correctly.
    auto env = base3node();
    // base3node already uses two-segment format — verify clientPort=0.
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    for (const auto& peer : r.config->peers) {
        EXPECT_EQ(peer.clientPort, 0u)
            << "Peer without clientPort must have clientPort=0";
        EXPECT_TRUE(peer.clientChannel().empty())
            << "Peer without clientPort must have empty clientChannel()";
    }
}

TEST(NodeConfigTest, PeerMixedClientPorts_ParsesOk) {
    // Mixed: some peers with clientPort, some without.
    auto env = base3node();
    env["RUNE_PEERS"] = "0@rune-1:40111:40200,1@rune-2:40111,2@rune-3:40111:40201";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->peers.size(), 3u);

    // Sorted by id: 0, 1, 2
    EXPECT_EQ(r.config->peers[0].clientPort, 40200u);
    EXPECT_EQ(r.config->peers[1].clientPort, 0u);
    EXPECT_EQ(r.config->peers[2].clientPort, 40201u);

    EXPECT_EQ(r.config->peers[0].clientChannel(), "aeron:udp?endpoint=rune-1:40200");
    EXPECT_TRUE(r.config->peers[1].clientChannel().empty());
    EXPECT_EQ(r.config->peers[2].clientChannel(), "aeron:udp?endpoint=rune-3:40201");
}

TEST(NodeConfigTest, PeerClientPortZeroIsError) {
    // Explicit :0 clientPort must be rejected (invalid port).
    auto env = base3node();
    env["RUNE_PEERS"] = "0@rune-1:40111:0,1@rune-2:40111,2@rune-3:40111";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty())
        << "clientPort=0 must be rejected (invalid port)";
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, PeerClientPortOutOfRangeIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "0@rune-1:40111:70000,1@rune-2:40111,2@rune-3:40111";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty())
        << "clientPort > 65535 must be rejected";
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, PeerClientPortNonIntIsError) {
    auto env = base3node();
    env["RUNE_PEERS"] = "0@rune-1:40111:notaport,1@rune-2:40111,2@rune-3:40111";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty())
        << "Non-integer clientPort must be rejected";
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, ConsensusChannelUnaffectedByClientPort) {
    // Adding clientPort must not change the consensusChannel output.
    auto env = base3node();
    env["RUNE_PEERS"] = "0@rune-1:40111:40200,1@rune-2:40111:40200,2@rune-3:40111:40200";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;

    EXPECT_EQ(r.config->peers[0].consensusChannel(), "aeron:udp?endpoint=rune-1:40111");
    EXPECT_EQ(r.config->peers[1].consensusChannel(), "aeron:udp?endpoint=rune-2:40111");
    EXPECT_EQ(r.config->peers[2].consensusChannel(), "aeron:udp?endpoint=rune-3:40111");
}

// CP3 I4: RUNE_BROKER_LIVENESS_TIMEOUT_MS — default 0 (the agent uses 3 x the leader-heartbeat timeout); a positive value
// is carried; garbage is refused.  RED: ignore the variable.
TEST(NodeConfigRegression, BrokerLivenessTimeoutEnvIsParsedRegression) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->brokerLivenessTimeoutMs, 0);
    auto env = base1node();
    env["RUNE_BROKER_LIVENESS_TIMEOUT_MS"] = "60000";
    auto r2 = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r2.error.empty()) << r2.error;
    EXPECT_EQ(r2.config->brokerLivenessTimeoutMs, 60000);
    env["RUNE_BROKER_LIVENESS_TIMEOUT_MS"] = "soon";
    EXPECT_FALSE(parseNodeConfig(fakeEnv(env)).error.empty());
}

// ---------------------------------------------------------------------------
// M1: gatewayMinIsr — majority default + RUNE_MIN_ISR env override
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, GatewayMinIsr_DefaultMajority_SingleNode) {
    // Single-node cluster: majority = 1/2+1 = 1.
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->gatewayMinIsr, 1)
        << "single-node cluster must have gatewayMinIsr=1";
}

TEST(NodeConfigTest, GatewayMinIsr_DefaultMajority_ThreeNodes) {
    // 3-node cluster: majority = 3/2+1 = 2.
    auto r = parseNodeConfig(fakeEnv(base3node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->gatewayMinIsr, 2)
        << "3-node cluster must default gatewayMinIsr to majority=2";
}

TEST(NodeConfigTest, GatewayMinIsr_EnvOverride) {
    // RUNE_MIN_ISR=3 overrides the 3-node majority default.
    auto env = base3node();
    env["RUNE_MIN_ISR"] = "3";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->gatewayMinIsr, 3)
        << "RUNE_MIN_ISR env var must override default majority";
}

TEST(NodeConfigTest, GatewayMinIsr_EnvZeroRejected) {
    auto env = base3node();
    env["RUNE_MIN_ISR"] = "0";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty()) << "RUNE_MIN_ISR=0 must be rejected (must be >= 1)";
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, GatewayMinIsr_EnvNonIntegerRejected) {
    auto env = base3node();
    env["RUNE_MIN_ISR"] = "bad";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty()) << "RUNE_MIN_ISR=bad must be rejected";
    EXPECT_FALSE(r.config.has_value());
}

// ---------------------------------------------------------------------------
// CP-1: TopicSpec cleanup policy + retention fields
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, CP1_TopicSpecDefaultsToDeletePolicy) {
    // A TopicSpec constructed with just name+partitionCount must default to
    // CleanupPolicy::Delete and unbounded retention.
    TopicSpec ts;
    ts.name           = "test";
    ts.partitionCount = 1;
    ts.topicId        = 0;

    EXPECT_EQ(ts.cleanupPolicy,          TopicSpec::CleanupPolicy::Delete);
    EXPECT_EQ(ts.retentionMs,            -1LL);
    EXPECT_EQ(ts.retentionBytes,         -1LL);
    EXPECT_EQ(ts.deleteRetentionMs,      86'400'000LL);
    EXPECT_DOUBLE_EQ(ts.minCleanableDirtyRatio, 0.5);
}

TEST(NodeConfigTest, CP1_TopicSpecFieldsHoldValues) {
    // TopicSpec with Compact policy must preserve all field values when set
    // programmatically — this test verifies the struct holds the values it is given.
    TopicSpec ts;
    ts.name                   = "events";
    ts.partitionCount         = 4;
    ts.topicId                = 0;
    ts.cleanupPolicy          = TopicSpec::CleanupPolicy::Compact;
    ts.retentionMs            = 3'600'000LL;   // 1 hour
    ts.retentionBytes         = 1'073'741'824LL; // 1 GiB
    ts.deleteRetentionMs      = 43'200'000LL;  // 12 hours
    ts.minCleanableDirtyRatio = 0.3;

    EXPECT_EQ(ts.cleanupPolicy,              TopicSpec::CleanupPolicy::Compact);
    EXPECT_EQ(ts.retentionMs,                3'600'000LL);
    EXPECT_EQ(ts.retentionBytes,             1'073'741'824LL);
    EXPECT_EQ(ts.deleteRetentionMs,          43'200'000LL);
    EXPECT_DOUBLE_EQ(ts.minCleanableDirtyRatio, 0.3);
}

TEST(NodeConfigTest, CP1_CleanupPolicyEnumValues) {
    // Enum values must be stable (used in serialization).
    EXPECT_EQ(static_cast<uint8_t>(TopicSpec::CleanupPolicy::Delete),        0u);
    EXPECT_EQ(static_cast<uint8_t>(TopicSpec::CleanupPolicy::Compact),       1u);
    EXPECT_EQ(static_cast<uint8_t>(TopicSpec::CleanupPolicy::CompactDelete), 2u);
}

// ===========================================================================
// CP-10 Fix 2: RUNE_ENABLE_COALESCING parsing
//
// NodeConfig.h documents RUNE_ENABLE_COALESCING as an env override but the
// original NodeConfig.cpp never parsed it.  These tests confirm the fix:
//   - Absent/empty → default true.
//   - "0" → false.
//   - "false" → false (case-insensitive).
//   - "FALSE" → false.
//   - "1"  → true.
//   - "true" → true.
// ===========================================================================

TEST(NodeConfigTest, CP10_EnableCoalescing_DefaultTrue) {
    // Absent env var → default enableCompactionCoalescing = true.
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_TRUE(r.config->enableCompactionCoalescing)
        << "Default must be true (Kafka parity)";
}

TEST(NodeConfigTest, CP10_EnableCoalescing_Zero_DisablesIt) {
    auto env = base1node();
    env["RUNE_ENABLE_COALESCING"] = "0";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_FALSE(r.config->enableCompactionCoalescing)
        << "RUNE_ENABLE_COALESCING=0 must disable coalescing";
}

TEST(NodeConfigTest, CP10_EnableCoalescing_FalseLower_DisablesIt) {
    auto env = base1node();
    env["RUNE_ENABLE_COALESCING"] = "false";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_FALSE(r.config->enableCompactionCoalescing)
        << "RUNE_ENABLE_COALESCING=false must disable coalescing";
}

TEST(NodeConfigTest, CP10_EnableCoalescing_FalseUpper_DisablesIt) {
    auto env = base1node();
    env["RUNE_ENABLE_COALESCING"] = "FALSE";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_FALSE(r.config->enableCompactionCoalescing)
        << "RUNE_ENABLE_COALESCING=FALSE must disable coalescing (case-insensitive)";
}

TEST(NodeConfigTest, CP10_EnableCoalescing_One_KeepsTrue) {
    auto env = base1node();
    env["RUNE_ENABLE_COALESCING"] = "1";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_TRUE(r.config->enableCompactionCoalescing)
        << "RUNE_ENABLE_COALESCING=1 must keep coalescing enabled";
}

TEST(NodeConfigTest, CP10_EnableCoalescing_True_KeepsTrue) {
    auto env = base1node();
    env["RUNE_ENABLE_COALESCING"] = "true";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_TRUE(r.config->enableCompactionCoalescing)
        << "RUNE_ENABLE_COALESCING=true must keep coalescing enabled";
}

// ===========================================================================
// E1 Task 4: NodeConfig.workerCount + RUNE_WORKERS parsing
// ===========================================================================

TEST(NodeConfig, ParsesWorkerCountDefaultsToOne) {
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));  // RUNE_WORKERS unset
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCount, 1);  // default
}

TEST(NodeConfig, ParsesRuneWorkersEnv) {
    auto base = base1node();
    base["RUNE_WORKERS"] = "4";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCount, 4);
}

TEST(NodeConfig, RuneWorkersFloorsToOne) {
    auto base = base1node();
    base["RUNE_WORKERS"] = "0";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCount, 1) << "0/negative/garbage floors to 1";
}

// E1 Task 5: RUNE_WORKER_IDLE parsing + WorkerIdleStrategy field
TEST(NodeConfig, RuneWorkerIdleDefault) {
    // RUNE_WORKER_IDLE absent → default Backoff (must NOT be busy-spin).
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIdleStrategy,
              rune::node::NodeConfig::WorkerIdleStrategy::Backoff)
        << "default must be Backoff (must not peg a core)";
}

TEST(NodeConfig, RuneWorkerIdleBackoff) {
    auto base = base1node();
    base["RUNE_WORKER_IDLE"] = "backoff";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIdleStrategy,
              rune::node::NodeConfig::WorkerIdleStrategy::Backoff);
}

TEST(NodeConfig, RuneWorkerIdleBusySpin) {
    auto base = base1node();
    base["RUNE_WORKER_IDLE"] = "busyspin";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIdleStrategy,
              rune::node::NodeConfig::WorkerIdleStrategy::BusySpin);
}

TEST(NodeConfig, RuneWorkerIdleYielding) {
    auto base = base1node();
    base["RUNE_WORKER_IDLE"] = "YIELDING";  // upper-case — must be case-insensitive
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIdleStrategy,
              rune::node::NodeConfig::WorkerIdleStrategy::Yielding);
}

TEST(NodeConfig, RuneWorkerIdleSleeping) {
    auto base = base1node();
    base["RUNE_WORKER_IDLE"] = "sleeping";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIdleStrategy,
              rune::node::NodeConfig::WorkerIdleStrategy::Sleeping);
}

TEST(NodeConfig, RuneWorkerIdleGarbageFloorsToBackoff) {
    auto base = base1node();
    base["RUNE_WORKER_IDLE"] = "turbo_ultra_fast_999";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIdleStrategy,
              rune::node::NodeConfig::WorkerIdleStrategy::Backoff)
        << "unrecognised value must floor to Backoff";
}

// ---------------------------------------------------------------------------
// A7: shard-worker idle shape — RUNE_IDLE_WAKE / RUNE_IDLE_SPINS / RUNE_IDLE_YIELDS
// (park-until-work; findings §4.12).  Auto shape = the Aeron 10 spins / 20 yields before the first park,
// with or without wake fds (review M1).  Strict parsing (review D3): garbage refuses startup.
// ---------------------------------------------------------------------------
TEST(NodeConfig, IdleWakeDefaultsOnWithAutoShape) {
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_TRUE(res.config->idleWake) << "park-until-work is the default";
    EXPECT_EQ(res.config->idleSpins, rune::node::NodeConfig::kIdleAuto);
    EXPECT_EQ(res.config->idleYields, rune::node::NodeConfig::kIdleAuto);
}

TEST(NodeConfig, IdleWakeAcceptsZeroOneFalseTrue) {
    for (const auto& [v, want] : {std::pair{"0", false}, std::pair{"false", false},
                                  std::pair{"1", true}, std::pair{"true", true}}) {
        auto base = base1node();
        base["RUNE_IDLE_WAKE"] = v;
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_EQ(res.config->idleWake, want) << v;
    }
}

// Review D3: an unrecognised value used to keep wake ON silently (a typo measured the wrong A/B arm).
TEST(NodeConfigRegression, IdleWakeGarbageRefusesStartupRegression) {
    for (const char* v : {"off", "no", "OFF", "garbage", "2"}) {
        auto base = base1node();
        base["RUNE_IDLE_WAKE"] = v;
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        EXPECT_FALSE(res.config.has_value()) << v;
        EXPECT_NE(res.error.find("RUNE_IDLE_WAKE must be 0/1/false/true"), std::string::npos) << res.error;
    }
}

// Catalog history Task 17 fix round 1 (R2/I1): the cleaner's I/O throttle is a broker setting.
TEST(NodeConfigRegression, CleanerIoMaxBytesPerSecondParsesAndRefusesGarbageRegression) {
    {
        auto r = rune::node::parseNodeConfig(fakeEnv(base1node()));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_EQ(r.config->cleanerIoMaxBytesPerSecond, -1) << "absent = unlimited";
    }
    {
        auto base = base1node();
        base["RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND"] = "";
        auto r = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_EQ(r.config->cleanerIoMaxBytesPerSecond, -1) << "empty = unlimited";
    }
    {
        auto base = base1node();
        base["RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND"] = "1048576";
        auto r = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_EQ(r.config->cleanerIoMaxBytesPerSecond, 1048576);
    }
    for (const char* v : {"0", "-5", "12x"}) {
        auto base = base1node();
        base["RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND"] = v;
        auto r = rune::node::parseNodeConfig(fakeEnv(base));
        EXPECT_FALSE(r.config.has_value()) << v;
        EXPECT_NE(r.error.find("RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND"), std::string::npos) << r.error;
    }
}

TEST(NodeConfig, IdleSpinsYieldsParse) {
    auto base = base1node();
    base["RUNE_IDLE_SPINS"]  = "3";
    base["RUNE_IDLE_YIELDS"] = "0";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->idleSpins, 3);
    EXPECT_EQ(res.config->idleYields, 0) << "0 is explicit, not auto";
}

// Review D3: garbage / negative spins or yields used to fall back to auto silently.
TEST(NodeConfigRegression, IdleSpinsYieldsGarbageRefusesStartupRegression) {
    for (const char* name : {"RUNE_IDLE_SPINS", "RUNE_IDLE_YIELDS"}) {
        for (const char* v : {"-5", "abc", "2x"}) {
            auto base = base1node();
            base[name] = v;
            auto res = rune::node::parseNodeConfig(fakeEnv(base));
            EXPECT_FALSE(res.config.has_value()) << name << "=" << v;
            EXPECT_NE(res.error.find(std::string(name) + " must be a non-negative integer"), std::string::npos)
                << res.error;
        }
    }
    auto base = base1node();
    base["RUNE_IDLE_SPINS"] = "";   // empty = unset (auto), like every other knob
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->idleSpins, rune::node::NodeConfig::kIdleAuto);
}

// A9: RUNE_IDLE_DOORBELL + the doorbell park knobs; garbage refuses startup (review D3 rule).
TEST(NodeConfig, IdleDoorbellKnobsParse) {
    auto base = base1node();
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_TRUE(res.config->idleDoorbell) << "default on";
    EXPECT_EQ(res.config->idleDoorbellMinParkUs, 1000);
    EXPECT_EQ(res.config->idleDoorbellParkUs, 1000);
    base["RUNE_IDLE_DOORBELL"] = "false";
    base["RUNE_IDLE_DOORBELL_MIN_PARK_US"] = "50";
    base["RUNE_IDLE_DOORBELL_PARK_US"] = "2000";
    res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_FALSE(res.config->idleDoorbell);
    EXPECT_EQ(res.config->idleDoorbellMinParkUs, 50);
    EXPECT_EQ(res.config->idleDoorbellParkUs, 2000);
}

TEST(NodeConfigRegression, IdleDoorbellGarbageRefusesStartupRegression) {
    for (const char* v : {"off", "yes", "2"}) {
        auto base = base1node();
        base["RUNE_IDLE_DOORBELL"] = v;
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        EXPECT_FALSE(res.config.has_value()) << v;
        EXPECT_NE(res.error.find("RUNE_IDLE_DOORBELL must be 0/1/false/true"), std::string::npos) << res.error;
    }
    for (const char* name : {"RUNE_IDLE_DOORBELL_MIN_PARK_US", "RUNE_IDLE_DOORBELL_PARK_US"}) {
        for (const char* v : {"0", "-1", "abc", "2000000"}) {
            auto base = base1node();
            base[name] = v;
            auto res = rune::node::parseNodeConfig(fakeEnv(base));
            EXPECT_FALSE(res.config.has_value()) << name << "=" << v;
            EXPECT_NE(res.error.find(std::string(name) + " must be an integer in 1..1000000"), std::string::npos)
                << res.error;
        }
    }
    auto base = base1node();
    base["RUNE_IDLE_DOORBELL_MIN_PARK_US"] = "500";
    base["RUNE_IDLE_DOORBELL_PARK_US"] = "100";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    EXPECT_FALSE(res.config.has_value());
    EXPECT_NE(res.error.find("must not exceed RUNE_IDLE_DOORBELL_PARK_US"), std::string::npos) << res.error;
}

// A9: the doorbell shape — the Aeron spin / yield phase (measured: dropping it costs 20k p50), then the
// doorbell park; explicit spins / yields still win.
TEST(NodeConfig, DoorbellShardIdleShape) {
    rune::node::NodeConfig c;
    EXPECT_EQ(c.shardIdleShape(/*doorbell=*/true), (rune::node::IdleShape{10, 20, 1'000'000, 1'000'000}));
    c.idleSpins = 2; c.idleYields = 3; c.idleDoorbellMinParkUs = 10; c.idleDoorbellParkUs = 500;
    EXPECT_EQ(c.shardIdleShape(true), (rune::node::IdleShape{2, 3, 10'000, 500'000}));
    EXPECT_EQ(c.shardIdleShape(false).maxParkNs, c.idleMaxParkUs * 1'000) << "no doorbell: A7's shape";
}

TEST(NodeConfig, ShardIdleShapeDerivation) {
    rune::node::NodeConfig c;
    c.idleMaxParkUs = 200;
    // Auto: the Aeron spin / yield phase before the first park (review M1), park 1 µs doubling to maxPark.
    const auto w = c.shardIdleShape();
    EXPECT_EQ(w.maxSpins, 10);
    EXPECT_EQ(w.maxYields, 20);
    EXPECT_EQ(w.minParkNs, 1'000);
    EXPECT_EQ(w.maxParkNs, 200'000) << "maxPark = RUNE_IDLE_MAX_PARK_US";
    // Explicit values win.
    c.idleSpins = 2; c.idleYields = 4; c.idleMaxParkUs = 50;
    const auto e = c.shardIdleShape();
    EXPECT_EQ(e.maxSpins, 2);
    EXPECT_EQ(e.maxYields, 4);
    EXPECT_EQ(e.maxParkNs, 50'000);
}

TEST(NodeConfig, ShardIdleUsesWakeOnlyWhenEnabled) {
    rune::node::NodeConfig c;
    EXPECT_TRUE(c.shardIdleWantsWake());
    c.idleWake = false;
    EXPECT_FALSE(c.shardIdleWantsWake()) << "RUNE_IDLE_WAKE=0 -> legacy backoff (the A/B off arm)";
}

// ---------------------------------------------------------------------------
// E3 Task 7: io-ring depths + memory-log ring bytes (RUNE_IO_SUBMIT_RING_DEPTH,
// RUNE_IO_COMPLETION_RING_DEPTH, RUNE_TRANSIENT_LOG_RING_BYTES)
// ---------------------------------------------------------------------------

TEST(NodeConfig, IoRingDepthsParseFromEnvPow2) {
    // Override two of the three fields; confirm the third stays at its default.
    auto base = base1node();
    base["RUNE_IO_SUBMIT_RING_DEPTH"]  = "2048";
    base["RUNE_TRANSIENT_LOG_RING_BYTES"] = "16777216"; // 16 MiB = 2^24
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->ioSubmitRingDepth,    2048);
    EXPECT_EQ(res.config->memoryLogRingBytes,   16 * 1024 * 1024);
    // ioCompletionRingDepth was NOT overridden — must stay at default (1024).
    EXPECT_EQ(res.config->ioCompletionRingDepth, 1024);
}

TEST(NodeConfig, IoRingDepthsDefaultValues) {
    // No overrides — all three fields must carry their documented defaults.
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->ioSubmitRingDepth,     1024);
    EXPECT_EQ(res.config->ioCompletionRingDepth, 1024);
    EXPECT_EQ(res.config->memoryLogRingBytes,    8 * 1024 * 1024);
}

TEST(NodeConfig, IoRingDepthsNonPow2KeepsDefault) {
    // Non-power-of-two values must be silently ignored (default kept).
    auto base = base1node();
    base["RUNE_IO_SUBMIT_RING_DEPTH"]     = "1000";  // not a power of two
    base["RUNE_IO_COMPLETION_RING_DEPTH"] = "3000";  // not a power of two
    base["RUNE_TRANSIENT_LOG_RING_BYTES"]    = "7";     // not a power of two
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->ioSubmitRingDepth,     1024) << "non-pow2 must keep default";
    EXPECT_EQ(res.config->ioCompletionRingDepth, 1024) << "non-pow2 must keep default";
    EXPECT_EQ(res.config->memoryLogRingBytes,    8 * 1024 * 1024) << "non-pow2 must keep default";
}

TEST(NodeConfig, IoRingDepthsNegativeKeepsDefault) {
    // Negative values are not valid powers of two and must keep the default.
    auto base = base1node();
    base["RUNE_IO_SUBMIT_RING_DEPTH"] = "-1024";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->ioSubmitRingDepth, 1024) << "negative value must keep default";
}

TEST(NodeConfig, IoRingDepthsAllThreeOverridden) {
    // All three fields can be overridden simultaneously.
    auto base = base1node();
    base["RUNE_IO_SUBMIT_RING_DEPTH"]     = "512";
    base["RUNE_IO_COMPLETION_RING_DEPTH"] = "256";
    base["RUNE_TRANSIENT_LOG_RING_BYTES"]    = "4194304"; // 4 MiB = 2^22
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->ioSubmitRingDepth,     512);
    EXPECT_EQ(res.config->ioCompletionRingDepth, 256);
    EXPECT_EQ(res.config->memoryLogRingBytes,    4 * 1024 * 1024);
}

// F15 S3 fix round 1 (owed-cap ruling): RUNE_WORKER_MAX_OWED_RETURNS.
TEST(ReturnOverflowRegression, MaxOwedReturnsParsedFromEnv) {
    auto res0 = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res0.config.has_value()) << res0.error;
    EXPECT_EQ(res0.config->workerMaxOwedReturns, 4096);
    auto base = base1node();
    base["RUNE_WORKER_MAX_OWED_RETURNS"] = "8192";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerMaxOwedReturns, 8192);
    base["RUNE_WORKER_MAX_OWED_RETURNS"] = "20000";   // parsed; the range is enforced at start()
    auto res2 = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res2.config.has_value()) << res2.error;
    EXPECT_EQ(res2.config->workerMaxOwedReturns, 20000);
    // Re-review 1 m-a: 0 / negative are kept (start() refuses them loudly),
    // non-numeric is a configuration error — never silently the default.
    base["RUNE_WORKER_MAX_OWED_RETURNS"] = "0";
    auto res3 = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res3.config.has_value()) << res3.error;
    EXPECT_EQ(res3.config->workerMaxOwedReturns, 0);
    base["RUNE_WORKER_MAX_OWED_RETURNS"] = "-5";
    auto res4 = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res4.config.has_value()) << res4.error;
    EXPECT_EQ(res4.config->workerMaxOwedReturns, -5);
    base["RUNE_WORKER_MAX_OWED_RETURNS"] = "lots";
    auto res5 = rune::node::parseNodeConfig(fakeEnv(base));
    EXPECT_FALSE(res5.config.has_value()) << "a non-numeric value must be a configuration error";
}

TEST(NodeConfig, WorkerRingBytesDefaultValues) {
    // No overrides — both per-worker ring byte capacities carry the 32 MiB default.
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIngressRingBytes, 32 * 1024 * 1024);
    EXPECT_EQ(res.config->workerReturnRingBytes,  32 * 1024 * 1024);
}

TEST(NodeConfig, WorkerRingBytesParseFromEnvPow2) {
    // Override ingress only; return must stay at its default.
    auto base = base1node();
    base["RUNE_WORKER_INGRESS_RING_BYTES"] = "16777216"; // 16 MiB = 2^24
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIngressRingBytes, 16 * 1024 * 1024);
    EXPECT_EQ(res.config->workerReturnRingBytes,  32 * 1024 * 1024)
        << "un-overridden return ring must keep default";
}

TEST(NodeConfig, WorkerRingBytesBothOverridden) {
    auto base = base1node();
    base["RUNE_WORKER_INGRESS_RING_BYTES"] = "4194304";  // 4 MiB = 2^22
    base["RUNE_WORKER_RETURN_RING_BYTES"]  = "8388608";  // 8 MiB = 2^23
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIngressRingBytes, 4 * 1024 * 1024);
    EXPECT_EQ(res.config->workerReturnRingBytes,  8 * 1024 * 1024);
}

TEST(NodeConfig, WorkerRingBytesNonPow2KeepsDefault) {
    // Non-power-of-two values must be silently ignored (32 MiB default kept).
    auto base = base1node();
    base["RUNE_WORKER_INGRESS_RING_BYTES"] = "3000000"; // not a power of two
    base["RUNE_WORKER_RETURN_RING_BYTES"]  = "5";       // not a power of two
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIngressRingBytes, 32 * 1024 * 1024) << "non-pow2 must keep default";
    EXPECT_EQ(res.config->workerReturnRingBytes,  32 * 1024 * 1024) << "non-pow2 must keep default";
}

TEST(NodeConfig, WorkerRingBytesNegativeKeepsDefault) {
    auto base = base1node();
    base["RUNE_WORKER_INGRESS_RING_BYTES"] = "-33554432";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerIngressRingBytes, 32 * 1024 * 1024) << "negative must keep default";
}

TEST(NodeConfig, TopicSpecDurabilityTierDefaultIsMajority) {
    // durabilityTier defaults to 1 (Majority) so existing tests are parity-unchanged.
    TopicSpec ts;
    EXPECT_EQ(ts.durabilityTier, 1u) << "default durabilityTier must be 1 (Majority)";
}

// ---------------------------------------------------------------------------
// E4: CPU affinity — RUNE_WORKER_CORE_BASE parse tests.
// ---------------------------------------------------------------------------

TEST(NodeConfig, WorkerCoreBaseDefaultIsAuto) {
    // Absent env var → kWorkerCoreBaseAuto (derived at start: HostDefaults.h).
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, NodeConfig::kWorkerCoreBaseAuto)
        << "default workerCoreBase is derived from the host";
}

TEST(NodeConfig, WorkerCoreBaseZero) {
    // RUNE_WORKER_CORE_BASE=0 → worker 0 pins to core 0.
    auto base = base1node();
    base["RUNE_WORKER_CORE_BASE"] = "0";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, 0);
}

TEST(NodeConfig, WorkerCoreBasePositive) {
    // RUNE_WORKER_CORE_BASE=2 → worker i pins to core 2+i.
    auto base = base1node();
    base["RUNE_WORKER_CORE_BASE"] = "2";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, 2);
}

TEST(NodeConfig, WorkerCoreBaseNegativeOneDisables) {
    // RUNE_WORKER_CORE_BASE=-1 → explicitly disabled.
    auto base = base1node();
    base["RUNE_WORKER_CORE_BASE"] = "-1";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, -1);
}

TEST(NodeConfig, WorkerCoreBaseLessThanNegativeOneClampedToDisabled) {
    // Values < -1 are clamped to -1 (disabled) — safe degradation.
    auto base = base1node();
    base["RUNE_WORKER_CORE_BASE"] = "-5";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, -1)
        << "values < -1 must be clamped to -1 (disabled)";
}

TEST(NodeConfig, WorkerCoreBaseUnparsableDisables) {
    // Unparseable value → -1 DISABLED (no error); never the auto layout, which
    // would turn on a busy-spin from garbage input.
    auto base = base1node();
    base["RUNE_WORKER_CORE_BASE"] = "notanumber";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, -1)
        << "unparseable RUNE_WORKER_CORE_BASE must disable pinning";
}

// ---------------------------------------------------------------------------
// G0.2: Aeron MTU + rcv-window config knobs (RUNE_AERON_MTU / RUNE_AERON_RCV_WINDOW)
// ---------------------------------------------------------------------------

TEST(NodeConfig, ParsesAeronMtuAndRcvWindow) {
    auto env = fakeEnv({{"RUNE_NODE_ID",         "0"},
                        {"RUNE_PEERS",            "0@h:1"},
                        {"RUNE_CLIENT_ENDPOINT",  "h:2"},
                        {"RUNE_DATA_DIR",         "/tmp/x"},
                        {"RUNE_AERON_MTU",        "8192"},
                        {"RUNE_AERON_RCV_WINDOW", "262144"}});
    auto res = rune::node::parseNodeConfig(env);
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronMtuBytes,      8192);
    EXPECT_EQ(res.config->aeronRcvWindowBytes, 262144);
}

TEST(NodeConfig, ParsesAeronSendBatching) {
    std::map<std::string, std::string> base{{"RUNE_NODE_ID","0"},{"RUNE_PEERS","0@h:1"},
        {"RUNE_CLIENT_ENDPOINT","h:2"},{"RUNE_DATA_DIR","/tmp/x"}};
    {
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_EQ(res.config->aeronMaxMessagesPerSend, 0) << "default = Aeron default";
    }
    {
        auto kv = base; kv["RUNE_AERON_MAX_MSGS_PER_SEND"] = "16"; kv["RUNE_AERON_SENDER_IOV"] = "16"; kv["RUNE_AERON_RECEIVER_IOV"] = "8";
        auto res = rune::node::parseNodeConfig(fakeEnv(kv));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_EQ(res.config->aeronMaxMessagesPerSend, 16);
        EXPECT_EQ(res.config->aeronSenderIoVectorCapacity, 16);
        EXPECT_EQ(res.config->aeronReceiverIoVectorCapacity, 8);
    }
    for (const char* bad : {"0", "17", "x"}) {
        auto kv = base; kv["RUNE_AERON_MAX_MSGS_PER_SEND"] = bad;
        EXPECT_FALSE(rune::node::parseNodeConfig(fakeEnv(kv)).config.has_value()) << bad;
    }
}

TEST(NodeConfig, AeronUdpGsoDefaultsOnAndEnvTurnsItOff) {
    std::map<std::string, std::string> base{{"RUNE_NODE_ID","0"},{"RUNE_PEERS","0@h:1"},
        {"RUNE_CLIENT_ENDPOINT","h:2"},{"RUNE_DATA_DIR","/tmp/x"}};
    EXPECT_TRUE(rune::node::NodeConfig{}.aeronUdpGso);
    {
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_TRUE(res.config->aeronUdpGso) << "default on (auto-off where the kernel lacks it)";
    }
    for (const char* off : {"0", "false"}) {
        auto kv = base; kv["RUNE_AERON_UDP_GSO"] = off;
        auto res = rune::node::parseNodeConfig(fakeEnv(kv));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_FALSE(res.config->aeronUdpGso) << off;
    }
    for (const char* on : {"1", "true"}) {
        auto kv = base; kv["RUNE_AERON_UDP_GSO"] = on;
        auto res = rune::node::parseNodeConfig(fakeEnv(kv));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_TRUE(res.config->aeronUdpGso) << on;
    }
    auto kv = base; kv["RUNE_AERON_UDP_GSO"] = "maybe";
    auto bad = rune::node::parseNodeConfig(fakeEnv(kv));
    EXPECT_FALSE(bad.config.has_value());
    EXPECT_NE(bad.error.find("RUNE_AERON_UDP_GSO"), std::string::npos) << bad.error;
}

// A7: epoll-first transport polling (rune_driver_wake.h rune_driver_epoll_poll_install) — default on, A/B-able.
TEST(NodeConfig, AeronEpollPollDefaultsOnAndEnvTurnsItOff) {
    std::map<std::string, std::string> base{{"RUNE_NODE_ID","0"},{"RUNE_PEERS","0@h:1"},
        {"RUNE_CLIENT_ENDPOINT","h:2"},{"RUNE_DATA_DIR","/tmp/x"}};
    EXPECT_TRUE(rune::node::NodeConfig{}.aeronEpollPoll);
    {
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_TRUE(res.config->aeronEpollPoll);
        EXPECT_TRUE(rune::node::EmbeddedDriverConfig::fromNode(*res.config, "/tmp/d").epollPoll);
    }
    for (const char* off : {"0", "false"}) {
        auto kv = base; kv["RUNE_AERON_EPOLL_POLL"] = off;
        auto res = rune::node::parseNodeConfig(fakeEnv(kv));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_FALSE(res.config->aeronEpollPoll) << off;
        EXPECT_FALSE(rune::node::EmbeddedDriverConfig::fromNode(*res.config, "/tmp/d").epollPoll) << off;
    }
    auto kv = base; kv["RUNE_AERON_EPOLL_POLL"] = "maybe";
    auto bad = rune::node::parseNodeConfig(fakeEnv(kv));
    EXPECT_FALSE(bad.config.has_value());
    EXPECT_NE(bad.error.find("RUNE_AERON_EPOLL_POLL"), std::string::npos) << bad.error;
}

TEST(NodeConfig, ParsesAeronTermSparse) {
    std::map<std::string, std::string> base{{"RUNE_NODE_ID","0"},{"RUNE_PEERS","0@h:1"},
        {"RUNE_CLIENT_ENDPOINT","h:2"},{"RUNE_DATA_DIR","/tmp/x"}};
    {
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_FALSE(res.config->aeronTermBufferSparse.has_value()) << "default: derived (non-sparse on tmpfs)";
    }
    {
        auto kv = base; kv["RUNE_AERON_TERM_SPARSE"] = "false";
        auto res = rune::node::parseNodeConfig(fakeEnv(kv));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_EQ(res.config->aeronTermBufferSparse, false);
    }
    {
        auto kv = base; kv["RUNE_AERON_TERM_SPARSE"] = "maybe";
        auto res = rune::node::parseNodeConfig(fakeEnv(kv));
        EXPECT_FALSE(res.config.has_value()) << "an invalid value refuses to start";
    }
}

// ---------------------------------------------------------------------------
// G0.3: Configurable gateway fragment poll limit (RUNE_GATEWAY_FRAGMENT_LIMIT)
// ---------------------------------------------------------------------------

TEST(NodeConfig, FragmentLimitDefaultAndOverride) {
    // Default: 256 (no env var set).
    {
        auto env = fakeEnv({{"RUNE_NODE_ID",        "0"},
                            {"RUNE_PEERS",           "0@h:1"},
                            {"RUNE_CLIENT_ENDPOINT", "h:2"},
                            {"RUNE_DATA_DIR",        "/tmp/x"}});
        auto res = rune::node::parseNodeConfig(env);
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_EQ(res.config->gatewayFragmentLimit, 256);
    }
    // Override via RUNE_GATEWAY_FRAGMENT_LIMIT=1000.
    {
        auto env = fakeEnv({{"RUNE_NODE_ID",                 "0"},
                            {"RUNE_PEERS",                   "0@h:1"},
                            {"RUNE_CLIENT_ENDPOINT",         "h:2"},
                            {"RUNE_DATA_DIR",                "/tmp/x"},
                            {"RUNE_GATEWAY_FRAGMENT_LIMIT",  "1000"}});
        auto res = rune::node::parseNodeConfig(env);
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_EQ(res.config->gatewayFragmentLimit, 1000);
    }
}

// ---------------------------------------------------------------------------
// E5.2: RUNE_AERON_THREADING_MODE + RUNE_AERON_DRIVER_IDLE parsing
// ---------------------------------------------------------------------------

TEST(NodeConfig, ParsesAeronThreadingMode_DefaultIsShared) {
    // No RUNE_AERON_THREADING_MODE set → must default to Shared.
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronThreadingMode,
              rune::node::NodeConfig::AeronThreadingMode::Shared)
        << "default aeronThreadingMode must be Shared";
}

TEST(NodeConfig, ParsesAeronThreadingMode_DedicatedLowercase) {
    auto base = base1node();
    base["RUNE_AERON_THREADING_MODE"] = "dedicated";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronThreadingMode,
              rune::node::NodeConfig::AeronThreadingMode::Dedicated);
}

TEST(NodeConfig, ParsesAeronThreadingMode_DedicatedUppercase) {
    auto base = base1node();
    base["RUNE_AERON_THREADING_MODE"] = "DEDICATED";  // upper-case — must be case-insensitive
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronThreadingMode,
              rune::node::NodeConfig::AeronThreadingMode::Dedicated);
}

TEST(NodeConfig, ParsesAeronThreadingMode_SharedExplicit) {
    auto base = base1node();
    base["RUNE_AERON_THREADING_MODE"] = "shared";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronThreadingMode,
              rune::node::NodeConfig::AeronThreadingMode::Shared);
}

TEST(NodeConfig, ParsesAeronThreadingMode_GarbageFloorsToShared) {
    auto base = base1node();
    base["RUNE_AERON_THREADING_MODE"] = "invoker_turbo_9000";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronThreadingMode,
              rune::node::NodeConfig::AeronThreadingMode::Shared)
        << "unrecognised RUNE_AERON_THREADING_MODE must floor to Shared";
}

TEST(NodeConfig, ParsesAeronDriverIdleDefault) {
    // No RUNE_AERON_DRIVER_IDLE set → empty (= derived: backoff in shard mode, sleep-ns in legacy;
    // D2, NodeDriverIdleDefaultsToBackoffInShardModeRegression).
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_TRUE(res.config->driverIdleStrategy.empty()) << "unset = derived";
}

TEST(NodeConfig, ParsesAeronDriverIdleOverride) {
    auto base = base1node();
    base["RUNE_AERON_DRIVER_IDLE"] = "noop";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->driverIdleStrategy, "noop");
}

// ===========================================================================
// Task 1 (per-worker replication transport): perWorkerReplication flag,
// replBaseStreamId, PeerConfig::workerCount, validateStreamRanges()
// ===========================================================================

// Step 1 — TDD: write the test first, confirm it fails, then implement.

TEST(NodeConfig, ParsesPerWorkerReplicationFlagDefaultFalse) {
    // RUNE_PER_WORKER_REPLICATION absent → default false.
    auto res = rune::node::parseNodeConfig(fakeEnv(legacyEnv(base1node())));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_FALSE(res.config->perWorkerReplication)
        << "default perWorkerReplication must be false";
}

TEST(NodeConfig, ParsesPerWorkerReplicationFlagTrue) {
    // "1", "true", "TRUE" → true.
    for (const char* v : {"1", "true", "TRUE"}) {
        auto base = base1node();
        base["RUNE_PER_WORKER_REPLICATION"] = v;
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        ASSERT_TRUE(res.config.has_value()) << res.error;
        EXPECT_TRUE(res.config->perWorkerReplication)
            << "RUNE_PER_WORKER_REPLICATION=" << v << " must set flag true";
    }
}

TEST(NodeConfig, ParsesPerWorkerReplicationFlagFalseForOtherValues) {
    // Non-truthy value → false.
    auto base = legacyEnv(base1node());
    base["RUNE_PER_WORKER_REPLICATION"] = "no";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_FALSE(res.config->perWorkerReplication);
}

TEST(NodeConfig, ParsesReplBaseStreamIdDefault) {
    // RUNE_REPL_BASE_STREAM_ID absent → default 2000.
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->replBaseStreamId, 2000)
        << "default replBaseStreamId must be 2000";
}

TEST(NodeConfig, ParsesReplBaseStreamIdOverride) {
    auto base = base1node();
    base["RUNE_REPL_BASE_STREAM_ID"] = "3000";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->replBaseStreamId, 3000);
}

TEST(NodeConfig, ParsesReplBaseStreamIdInvalidIsError) {
    auto base = base1node();
    base["RUNE_REPL_BASE_STREAM_ID"] = "notanumber";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    EXPECT_FALSE(res.config.has_value())
        << "non-integer RUNE_REPL_BASE_STREAM_ID must be rejected";
    EXPECT_NE(res.error.find("RUNE_REPL_BASE_STREAM_ID"), std::string::npos);
}

// Regression: the broker ignored RUNE_AERON_DIR and always put the Aeron dir
// (CnC + every mmapped term buffer) under dataDir/aeron — on a disk-backed data
// dir that puts Aeron's log buffers on ext4 (page-cache writeback + fault-on-
// sparse-file for every message; ~2x throughput loss measured on the 4-core VM).
TEST(NodeConfigRegression, AeronDirDefaultsUnderDataDir) {
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_TRUE(res.config->aeronDir.empty());
    EXPECT_EQ(res.config->effectiveAeronDir(),
              (std::filesystem::path{res.config->dataDir} / "aeron").string());
}

TEST(NodeConfigRegression, AeronDirHonorsRuneAeronDirEnv) {
    auto base = base1node();
    base["RUNE_AERON_DIR"] = "/dev/shm/rune-node-0-aeron";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->aeronDir, "/dev/shm/rune-node-0-aeron");
    EXPECT_EQ(res.config->effectiveAeronDir(), "/dev/shm/rune-node-0-aeron");
}

// The embedded Aeron driver runs with dir_delete_on_start + delete-on-shutdown and
// recursively deletes its directory. An Aeron dir equal to (or an ancestor of) the
// data dir would wipe every log segment, so it must be refused at parse time.
TEST(NodeConfigRegression, AeronDirEqualToDataDirRefused) {
    auto base = base1node();
    base["RUNE_AERON_DIR"] = "/data/rune";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    EXPECT_FALSE(res.config.has_value());
    EXPECT_NE(res.error.find("RUNE_AERON_DIR"), std::string::npos) << res.error;
}

TEST(NodeConfigRegression, AeronDirAncestorOfDataDirRefused) {
    for (const char* bad : {"/data", "/data/", "/data/rune/..", "/data/./rune/", "/"}) {
        auto base = base1node();
        base["RUNE_AERON_DIR"] = bad;
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        EXPECT_FALSE(res.config.has_value()) << "accepted dangerous RUNE_AERON_DIR=" << bad;
    }
}

TEST(NodeConfigRegression, AeronDirSiblingOrChildOfDataDirAccepted) {
    for (const char* ok : {"/data/rune/aeron", "/data/rune-aeron", "/dev/shm/rune-0"}) {
        auto base = base1node();
        base["RUNE_AERON_DIR"] = ok;
        auto res = rune::node::parseNodeConfig(fakeEnv(base));
        EXPECT_TRUE(res.config.has_value()) << ok << ": " << res.error;
    }
}

TEST(NodeConfigRegression, ValidateAeronDirCatchesProgrammaticConfig) {
    rune::node::NodeConfig cfg;
    cfg.dataDir  = "/data/rune";
    cfg.aeronDir = "/data";
    EXPECT_TRUE(cfg.validateAeronDir().has_value());
    cfg.aeronDir = "";  // default → dataDir/aeron, always safe
    EXPECT_FALSE(cfg.validateAeronDir().has_value());
}

TEST(NodeConfig, PeerWorkerCountDefaultsToZero) {
    // PeerConfig::workerCount defaults to 0 (unknown) for peers parsed from RUNE_PEERS.
    auto res = rune::node::parseNodeConfig(fakeEnv(legacyEnv(base3node())));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    for (const auto& p : res.config->peers) {
        EXPECT_EQ(p.workerCount, 0)
            << "PeerConfig::workerCount must default to 0 for peer id=" << p.id;
    }
}

TEST(NodeConfig, SelfPeerWorkerCountReflectsWorkerCount) {
    // The peer entry for self.id must have workerCount == NodeConfig::workerCount.
    auto base = base3node();
    base["RUNE_WORKERS"] = "4";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->self().workerCount, 4)
        << "self peer workerCount must match NodeConfig::workerCount";
    // Other peers must still be 0.
    for (const auto& p : res.config->peers) {
        if (p.id != res.config->nodeId)
            EXPECT_EQ(p.workerCount, 0)
                << "non-self peer workerCount must be 0, peer id=" << p.id;
    }
}

// Regression test: validateStreamRanges returns std::nullopt for valid default config.
TEST(NodeConfig, ValidateStreamRanges_CleanDefaultConfig) {
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    auto err = res.config->validateStreamRanges();
    EXPECT_FALSE(err.has_value())
        << "default config must pass validateStreamRanges(), got: "
        << (err ? *err : "");
}

// validateStreamRanges detects overlap of replBase range with clientStreamId.
TEST(NodeConfig, ValidateStreamRanges_OverlapWithClientStreamId_ReturnsError) {
    // clientStreamId defaults to 1; replBaseStreamId=1 with workerCount=4
    // means [1,4] overlaps stream 1.
    auto base = base1node();
    base["RUNE_REPL_BASE_STREAM_ID"] = "1";  // collides with clientStreamId=1
    base["RUNE_WORKERS"] = "4";
    base["RUNE_PER_WORKER_REPLICATION"] = "true";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;  // parse still succeeds
    auto err = res.config->validateStreamRanges();
    EXPECT_TRUE(err.has_value())
        << "replBase overlapping clientStreamId must fail validateStreamRanges()";
}

// validateStreamRanges detects overlap of replBase range with consensusStreamId.
TEST(NodeConfig, ValidateStreamRanges_OverlapWithConsensusStreamId_ReturnsError) {
    // consensusStreamId defaults to 10; replBaseStreamId=9 with workerCount=4
    // means range [9,12] contains stream 10.
    auto base = base1node();
    base["RUNE_REPL_BASE_STREAM_ID"] = "9";
    base["RUNE_WORKERS"] = "4";
    base["RUNE_PER_WORKER_REPLICATION"] = "true";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;  // parse still succeeds
    auto err = res.config->validateStreamRanges();
    ASSERT_TRUE(err.has_value())
        << "replBase range overlapping consensusStreamId must fail validateStreamRanges()";
    EXPECT_NE(err->find("consensusStreamId"), std::string::npos)
        << "error message must name 'consensusStreamId', got: " << *err;
}

// validateStreamRanges detects overlap of replBase range with replicationStreamId.
TEST(NodeConfig, ValidateStreamRanges_OverlapWithReplicationStreamId_ReturnsError) {
    // replicationStreamId defaults to 20; replBaseStreamId=20 with workerCount=4
    // means range [20,23] contains stream 20.
    auto base = base1node();
    base["RUNE_REPL_BASE_STREAM_ID"] = "20";
    base["RUNE_WORKERS"] = "4";
    base["RUNE_PER_WORKER_REPLICATION"] = "true";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;  // parse still succeeds
    auto err = res.config->validateStreamRanges();
    ASSERT_TRUE(err.has_value())
        << "replBase range overlapping replicationStreamId must fail validateStreamRanges()";
    EXPECT_NE(err->find("replicationStreamId"), std::string::npos)
        << "error message must name 'replicationStreamId', got: " << *err;
}

// validateStreamRanges detects overlap of replBase range with workerIngress base range.
TEST(NodeConfig, ValidateStreamRanges_OverlapWithWorkerIngressRange_ReturnsError) {
    // workerIngressBaseStreamId resolves to clientStreamId+1 = 2 (default).
    // replBaseStreamId=2 with workerCount=4 → range [2,5] overlaps [2,5].
    // clientStreamId=1 is not in [2,5] so clientStreamId check does not fire first.
    auto base = base1node();
    base["RUNE_REPL_BASE_STREAM_ID"] = "2";
    base["RUNE_WORKERS"] = "4";
    base["RUNE_PER_WORKER_REPLICATION"] = "true";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;  // parse still succeeds
    auto err = res.config->validateStreamRanges();
    ASSERT_TRUE(err.has_value())
        << "replBase range overlapping workerIngress range must fail validateStreamRanges()";
    EXPECT_NE(err->find("workerIngress"), std::string::npos)
        << "error message must name 'workerIngress', got: " << *err;
}

// validateStreamRanges detects overlap of replBase range with workerEgress base range.
TEST(NodeConfig, ValidateStreamRanges_OverlapWithWorkerEgressRange_ReturnsError) {
    // Set workerEgressBaseStreamId=500 explicitly; replBaseStreamId=500
    // with workerCount=4 → range [500,503] overlaps [500,503].
    auto base = base1node();
    base["RUNE_WORKER_EGRESS_BASE_STREAM"] = "500";
    base["RUNE_REPL_BASE_STREAM_ID"] = "500";
    base["RUNE_WORKERS"] = "4";
    base["RUNE_PER_WORKER_REPLICATION"] = "true";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;  // parse still succeeds
    auto err = res.config->validateStreamRanges();
    ASSERT_TRUE(err.has_value())
        << "replBase range overlapping workerEgress range must fail validateStreamRanges()";
    EXPECT_NE(err->find("workerEgress"), std::string::npos)
        << "error message must name 'workerEgress', got: " << *err;
}

// ---------------------------------------------------------------------------
// Self-contained worker shards (spec §3.3): config + validation + refusal.
// ---------------------------------------------------------------------------
static std::map<std::string, std::string> shard3node(int self = 0) {
    return {
        {"RUNE_NODE_ID",          std::to_string(self)},
        {"RUNE_PEERS",            "0@127.0.0.1:40110:40200:40220:40600:40500,"
                                  "1@127.0.0.1:40111:40201:40221:40620:40520,"
                                  "2@127.0.0.1:40112:40202:40222:40640:40540"},
        {"RUNE_CLIENT_ENDPOINT",  "0.0.0.0:" + std::to_string(40200 + self)},
        {"RUNE_DATA_DIR",         "/data/rune"},
        {"RUNE_WORKERS",          "1"},
        {"RUNE_WORKER_MODE",      "shard"},
        {"RUNE_WORKER_INGRESS_BASE_PORT", std::to_string(40500 + 20 * self)},
        {"RUNE_WORKER_REPL_BASE_PORT",    std::to_string(40600 + 20 * self)},
        {"RUNE_WORKER_EGRESS_CONTROL_BASE_PORT", std::to_string(40400 + 20 * self)},
    };
}

TEST(NodeConfig, DefaultsAreShardFusedAndValidOnceDerived) {
    // 2026-10-02: shard + fused is the default; unset per-worker ports are
    // derived at BrokerNode::start (applyHostDerivedDefaults), after which the
    // config validates.
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->workerMode, NodeConfig::WorkerMode::Shard);
    EXPECT_EQ(r.config->runtimePlacement, NodeConfig::RuntimePlacement::Fused);
    EXPECT_FALSE(r.config->workerIngressBasePort.has_value()) << "not set by the env → derived later";
    NodeConfig c = *r.config;
    HostNetFacts f; f.platform = HostPlatform::Linux; f.rmemMax = f.wmemMax = 16 << 20;
    applyHostDerivedDefaults(c, f);
    EXPECT_TRUE(c.workerIngressBasePort.has_value());
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
    // Legacy stays selectable and valid.
    auto l = parseNodeConfig(fakeEnv(legacyEnv(base1node())));
    ASSERT_TRUE(l.config.has_value()) << l.error;
    EXPECT_FALSE(l.config->shardMode());
    EXPECT_FALSE(l.config->validateShardConfig().has_value());
}

TEST(NodeConfig, ParsesShardModePlacementPortsAndPeerBases) {
    auto env = shard3node(1);
    env["RUNE_RUNTIME_PLACEMENT"] = "fused";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    const auto& c = *r.config;
    EXPECT_TRUE(c.shardMode());
    EXPECT_TRUE(c.fused());
    EXPECT_EQ(c.workerIngressBasePort, std::optional<int32_t>{40520});
    EXPECT_EQ(c.workerReplBasePort, std::optional<int32_t>{40620});
    EXPECT_TRUE(c.perWorkerIngressEnabled);
    EXPECT_TRUE(c.perWorkerReplication);
    EXPECT_TRUE(c.perWorkerEgressEnabled);
    EXPECT_EQ(c.peers[2].replBasePort, 40640);
    EXPECT_EQ(c.peers[2].ingressBasePort, 40540);
    EXPECT_EQ(c.shardIngressBindChannel(0), "aeron:udp?endpoint=0.0.0.0:40520");
    EXPECT_EQ(c.shardReplBindChannel(0), "aeron:udp?endpoint=127.0.0.1:40620");
    EXPECT_EQ(NodeConfig::shardPeerReplChannel(c.peers[2], 0), "aeron:udp?endpoint=127.0.0.1:40640");
    EXPECT_EQ(NodeConfig::shardIngressEndpoint(c.peers[0], 0), "127.0.0.1:40500");
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
}

// Task 9 carry (T6 minor): the shard channel helpers are only valid after
// validateShardConfig(); on an unvalidated config they refuse loudly instead of
// dereferencing an unset optional or building a port-0 / out-of-range channel.
TEST(ShardConfigRegression, ChannelHelpersRefuseUnvalidatedConfig) {
    auto r = parseNodeConfig(fakeEnv(base1node()));     // legacy: no shard base ports
    ASSERT_TRUE(r.config.has_value()) << r.error;
    const auto& legacy = *r.config;
    EXPECT_THROW((void)legacy.shardIngressBindChannel(0), std::logic_error);
    EXPECT_THROW((void)legacy.shardReplBindChannel(0), std::logic_error);
    PeerConfig noBase; noBase.id = 7; noBase.host = "127.0.0.1";
    EXPECT_THROW((void)NodeConfig::shardPeerReplChannel(noBase, 0), std::logic_error);
    EXPECT_THROW((void)NodeConfig::shardIngressEndpoint(noBase, 0), std::logic_error);

    auto s = parseNodeConfig(fakeEnv(shard3node(1)));
    ASSERT_TRUE(s.config.has_value()) << s.error;
    const int32_t W = s.config->workerCount;
    EXPECT_THROW((void)s.config->shardIngressBindChannel(W), std::logic_error);
    EXPECT_THROW((void)s.config->shardReplBindChannel(-1), std::logic_error);
    EXPECT_NO_THROW((void)s.config->shardReplBindChannel(W - 1));
}

TEST(NodeConfigRegression, UnknownWorkerModeIsParseError) {
    auto env = shard3node(); env["RUNE_WORKER_MODE"] = "shards";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("RUNE_WORKER_MODE"), std::string::npos);
}

TEST(NodeConfigRegression, UnknownRuntimePlacementIsParseError) {
    auto env = shard3node(); env["RUNE_RUNTIME_PLACEMENT"] = "fuse";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("RUNE_RUNTIME_PLACEMENT"), std::string::npos);
}

TEST(NodeConfigRegression, PeerEntryWithSevenPortsRejected) {
    // 6 segments are valid since 2026-10-02 (6th = egressControlBasePort).
    auto env = base3node();
    env["RUNE_PEERS"] = "0@h:1:2:3:4:5:6:7,1@h:11,2@h:12";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
}

static std::optional<std::string> shardErr(std::map<std::string, std::string> env) {
    auto r = parseNodeConfig(fakeEnv(std::move(env)));
    if (!r.config) return r.error;
    return r.config->validateShardConfig();
}

TEST(ShardConfigRegression, MissingIngressBasePortRefused) {
    auto env = shard3node(); env.erase("RUNE_WORKER_INGRESS_BASE_PORT");
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_WORKER_INGRESS_BASE_PORT"), std::string::npos);
}

TEST(ShardConfigRegression, MissingPeerReplBasePortRefused) {
    auto env = shard3node();
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:40600:40500,"
                        "1@127.0.0.1:40111:40201:40221:40620:40520,"
                        "2@127.0.0.1:40112:40202:40222";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("peer 2"), std::string::npos);
    EXPECT_NE(e->find("replBasePort"), std::string::npos);
}

TEST(ShardConfigRegression, SelfPeerEntryDisagreeingWithEnvRefused) {
    auto env = shard3node(); env["RUNE_WORKER_REPL_BASE_PORT"] = "40700";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_WORKER_REPL_BASE_PORT"), std::string::npos);
}

TEST(ShardConfigRegression, OwnPortCollisionRefusedNamingBothPorts) {
    auto env = shard3node(); env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = "40500";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("40500"), std::string::npos);
    EXPECT_NE(e->find("workerIngress"), std::string::npos);
    EXPECT_NE(e->find("workerEgressControl"), std::string::npos);
}

TEST(ShardConfigRegression, CoLocatedPeerRangeOverlapRefused) {
    auto env = shard3node(0);
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:40600:40500,"
                        "1@127.0.0.1:40111:40201:40221:40620:40500,"   // same ingress base as node 0
                        "2@127.0.0.1:40112:40202:40222:40640:40540";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("co-located peer 1"), std::string::npos);
    EXPECT_NE(e->find("40500"), std::string::npos);
}

TEST(ShardConfigRegression, AsymmetricWorkerCountRefused) {
    auto r = parseNodeConfig(fakeEnv(shard3node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    auto c = *r.config;
    c.peers[1].workerCount = 2;               // peer advertises 2, self runs 1
    auto e = c.validateShardConfig();
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("worker count"), std::string::npos);
}

TEST(ShardConfigRegression, PortRangeOverflowRefused) {
    auto env = shard3node(); env["RUNE_WORKERS"] = "2";
    env["RUNE_WORKER_INGRESS_BASE_PORT"] = "65535";
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:40600:65535,"
                        "1@127.0.0.1:40111:40201:40221:40620:40520,"
                        "2@127.0.0.1:40112:40202:40222:40640:40540";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_WORKER_INGRESS_BASE_PORT"), std::string::npos);
}

TEST(ShardConfigRegression, FusedRequiresShard) {
    auto env = legacyEnv(base1node()); env["RUNE_RUNTIME_PLACEMENT"] = "fused";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("fused requires"), std::string::npos);
}

TEST(ShardConfigRegression, MoreThanSixtyFourShardWorkersRefused) {
    auto env = shard3node(); env["RUNE_WORKERS"] = "65";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("64"), std::string::npos);
}


// --- Task 6 fix round 1: egress-control base port explicit + range + collisions ---
TEST(ShardConfigRegression, MissingEgressControlBasePortRefused) {
    auto env = shard3node(); env.erase("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT");
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value()) << "shard mode must not fall back to the 40300 default";
    EXPECT_NE(e->find("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"), std::string::npos) << *e;
}

TEST(NodeConfigRegression, LegacyModeKeepsDefaultEgressControlBasePort) {
    auto r = parseNodeConfig(fakeEnv(legacyEnv(base1node())));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->effectiveWorkerEgressControlBasePort(), 40300);
    EXPECT_FALSE(r.config->validateShardConfig().has_value());
}

TEST(ShardConfigRegression, EgressControlBasePortOutOfRangeRefused) {
    for (const char* bad : {"0", "-5", "65535"}) {   // 65535 + (W=2) - 1 overflows
        auto env = shard3node(); env["RUNE_WORKERS"] = "2";
        env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = bad;
        auto e = shardErr(env);
        ASSERT_TRUE(e.has_value()) << "egress base " << bad << " must be refused";
        EXPECT_NE(e->find("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"), std::string::npos) << *e;
    }
}

TEST(ShardConfigRegression, EgressRangeCollidingWithIngressRangeRefused) {
    auto env = shard3node(); env["RUNE_WORKERS"] = "4";
    env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = "40497";   // [40497,40500] hits ingress w0=40500
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("40500"), std::string::npos) << *e;
    EXPECT_NE(e->find("workerIngress"), std::string::npos) << *e;
    EXPECT_NE(e->find("workerEgressControl"), std::string::npos) << *e;
}

TEST(ShardConfigRegression, EgressRangeCollidingWithReplRangeRefused) {
    auto env = shard3node(); env["RUNE_WORKERS"] = "4";
    env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = "40602";   // [40602,40605] hits repl w2/w3
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("workerRepl"), std::string::npos) << *e;
    EXPECT_NE(e->find("workerEgressControl"), std::string::npos) << *e;
}

TEST(NodeConfigRegression, IngressAndReplBasePortsOutOfRangeRejectedAtParseEvenInLegacy) {
    for (const char* key : {"RUNE_WORKER_INGRESS_BASE_PORT", "RUNE_WORKER_REPL_BASE_PORT"}) {
        for (const char* bad : {"-1", "0", "65536", "100000"}) {
            auto env = base1node(); env[key] = bad;         // legacy mode
            auto r = parseNodeConfig(fakeEnv(env));
            EXPECT_FALSE(r.config.has_value()) << key << "=" << bad << " must be a parse error";
            EXPECT_NE(r.error.find(key), std::string::npos) << r.error;
        }
        auto env = base1node(); env[key] = "65535";
        EXPECT_TRUE(parseNodeConfig(fakeEnv(env)).config.has_value()) << key << "=65535 is a valid port";
    }
}

TEST(ShardConfigRegression, SixtyFourShardWorkersAcceptedSixtyFiveRefused) {
    // Spread the three per-worker ranges 100 apart so W=64 cannot self-collide.
    auto env = shard3node(0);
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:41000:41100,"
                        "1@10.0.0.2:40111:40201:40221:41000:41100,"
                        "2@10.0.0.3:40112:40202:40222:41000:41100";
    env["RUNE_WORKER_REPL_BASE_PORT"]            = "41000";
    env["RUNE_WORKER_INGRESS_BASE_PORT"]         = "41100";
    env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"]  = "41200";
    env["RUNE_WORKERS"] = "64";
    auto ok = shardErr(env);
    EXPECT_FALSE(ok.has_value()) << "W=64 must be accepted: " << *ok;
    env["RUNE_WORKERS"] = "65";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("64"), std::string::npos) << *e;
}


// --- Task 6 fix round 2: egress-control base port is optional<int32_t> (no side flag) ---
TEST(ShardConfigRegression, ProgrammaticShardConfigWithEgressPortValidatesWithoutSideFlag) {
    // A shard config built in code: setting the port alone must be enough.
    auto env = shard3node(); env.erase("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT");
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    auto c = *r.config;
    ASSERT_TRUE(c.validateShardConfig().has_value()) << "unset must be refused";
    c.workerEgressControlBasePort = 40400;
    auto e = c.validateShardConfig();
    EXPECT_FALSE(e.has_value()) << *e;
    EXPECT_EQ(c.effectiveWorkerEgressControlBasePort(), 40400);
}

TEST(NodeConfigRegression, EgressControlBasePortUnsetResolvesLegacyDefault) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_FALSE(r.config->workerEgressControlBasePort.has_value());
    EXPECT_EQ(r.config->effectiveWorkerEgressControlBasePort(), 40300);
}

// ---------------------------------------------------------------------------
// F18: an accepted produce batch must ALWAYS be replicable in one frame.  The
// intake limit is DERIVED from every hop a batch crosses (worker-ingress op ring;
// replication outbound ring + Aeron term/8 when replicated) and enforced at intake
// (MessageTooLarge) — never a startup refusal: every config starts.
// ---------------------------------------------------------------------------
namespace {
NodeConfig parsedOrDie(std::map<std::string, std::string> env) {
    auto r = parseNodeConfig(fakeEnv(std::move(env)));
    EXPECT_TRUE(r.error.empty()) << r.error;
    return *r.config;
}
} // namespace

TEST(ProduceBatchLimitRegression, IntakeLimitNeverExceedsOneFrameOnAnyHop) {
    for (auto env : {base3node(), base1node()}) {
        const NodeConfig c = parsedOrDie(env);
        EXPECT_GT(c.produceIntakeLimit(), 0);
        // Integration wave 2: a thin wrapper over THE one formula.
        EXPECT_EQ(static_cast<std::size_t>(c.produceIntakeLimit()),
                  Worker::maxAcceptableBatchBytes(
                      static_cast<std::size_t>(c.workerIngressRingBytes),
                      static_cast<std::size_t>(c.workerReturnRingBytes),
                      static_cast<std::size_t>(c.replicationOutboundRingBytes),
                      static_cast<std::size_t>(c.aeronTermBufferBytes),
                      static_cast<std::size_t>(c.aeronTermBufferBytes),
                      static_cast<std::size_t>(c.aeronTermBufferBytes)));
        EXPECT_LE(c.produceIntakeLimit(), NodeConfig::kDefaultMaxProduceBatchBytes);
    }
}

TEST(ProduceBatchLimitRegression, DefaultThreeNodeLimitIsTheTermFrame) {
    // Defaults (2026-10-03): 32 MiB outbound ring (4 MiB frames) and a 1 MiB term
    // (128 KiB max message) → the term frame binds at ~128 KiB: a larger batch must
    // NOT be accepted (appended but never replicated / never pushed to consumers) —
    // and the default config still starts (no refusal; the limit simply shrinks).
    const NodeConfig c = parsedOrDie(base3node());
    EXPECT_LT(c.produceIntakeLimit(), 128 * 1024);
    EXPECT_GT(c.produceIntakeLimit(), 128 * 1024 - 1024);
}

TEST(ProduceBatchLimitRegression, OutboundRingFrameBoundsTheLimit) {
    auto env = base3node();
    env["RUNE_AERON_TERM_BUFFER_BYTES"]   = "33554432";   // term/8 = 4 MiB
    env["RUNE_WORKER_INGRESS_RING_BYTES"] = "33554432";   // /8 = 4 MiB
    env["RUNE_REPL_OUTBOUND_RING_BYTES"]  = "16777216";   // /8 = 2 MiB - framing
    const NodeConfig c = parsedOrDie(env);
    EXPECT_LT(c.produceIntakeLimit(), 2 * 1024 * 1024) << "a 2 MiB batch cannot cross a 2 MiB ring frame";
    EXPECT_GT(c.produceIntakeLimit(), 2 * 1024 * 1024 - 1024);
}

TEST(ProduceBatchLimitRegression, AeronTermFrameBoundsTheLimit) {
    auto env = base3node();
    env["RUNE_REPL_OUTBOUND_RING_BYTES"]  = "33554432";
    env["RUNE_WORKER_INGRESS_RING_BYTES"] = "33554432";
    env["RUNE_AERON_TERM_BUFFER_BYTES"]   = "16777216";   // term/8 = 2 MiB - framing
    const NodeConfig c = parsedOrDie(env);
    EXPECT_LT(c.produceIntakeLimit(), 2 * 1024 * 1024);
    EXPECT_GT(c.produceIntakeLimit(), 2 * 1024 * 1024 - 1024);
}

TEST(ProduceBatchLimitRegression, WorkerIngressFrameBoundsTheLimitEvenSingleNode) {
    auto env = base1node();
    env["RUNE_WORKER_INGRESS_RING_BYTES"] = "16777216";   // /8 = 2 MiB - op framing
    // Integration wave 2: replication terms apply single-node too — give them
    // room so the op ring is the binding hop.
    env["RUNE_REPL_OUTBOUND_RING_BYTES"]  = "33554432";
    env["RUNE_AERON_TERM_BUFFER_BYTES"]   = "33554432";
    const NodeConfig c = parsedOrDie(env);
    EXPECT_LT(c.produceIntakeLimit(), 2 * 1024 * 1024) << "an op that cannot be claimed would throw";
    EXPECT_GT(c.produceIntakeLimit(), 2 * 1024 * 1024 - 1024);
}

// Integration wave 2 (F18 ⇄ F15 S3 merge plan): single-node ALWAYS applies the
// replication terms — the limit must not change when a node later joins a
// cluster (F18's original single-node exemption is overruled).
TEST(ProduceBatchLimitRegression, SingleNodeAppliesReplicationHops) {
    auto env1 = base1node();
    env1["RUNE_REPL_OUTBOUND_RING_BYTES"] = "1048576";     // /8 = 128 KiB frame
    env1["RUNE_AERON_TERM_BUFFER_BYTES"]  = "1048576";
    const NodeConfig c1 = parsedOrDie(env1);
    EXPECT_LT(c1.produceIntakeLimit(), 128 * 1024);
    auto env3 = base3node();
    env3["RUNE_REPL_OUTBOUND_RING_BYTES"] = "1048576";
    env3["RUNE_AERON_TERM_BUFFER_BYTES"]  = "1048576";
    const NodeConfig c3 = parsedOrDie(env3);
    EXPECT_EQ(c1.produceIntakeLimit(), c3.produceIntakeLimit())
        << "the limit must not change when a node joins a cluster";
}

TEST(ProduceBatchLimitRegression, BenchProfileAccepts2MiBBatches) {
    // The documented bench profile (CLAUDE.md / bench/kafka-compare/cluster.sh).
    auto env = base3node();
    env["RUNE_REPL_OUTBOUND_RING_BYTES"]  = "33554432";
    env["RUNE_AERON_TERM_BUFFER_BYTES"]   = "33554432";
    env["RUNE_WORKER_INGRESS_RING_BYTES"] = "33554432";
    const NodeConfig c = parsedOrDie(env);
    EXPECT_GE(c.produceIntakeLimit(), 2 * 1024 * 1024 + 64 * 1024);
}

// PSM fix round 1 (I1): RUNE_PRODUCER_ID_EXPIRATION_MS (Kafka
// producer.id.expiration.ms for compaction's producer-tail retention).
TEST(NodeConfigTest, PsmLogConsistencyRegression_ProducerIdExpirationDefaultAndOverride) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->producerIdExpirationMs, 7LL * 24 * 3600 * 1000) << "default 7 days";
    auto env = base1node();
    env["RUNE_PRODUCER_ID_EXPIRATION_MS"] = "3600000";
    auto r2 = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r2.error.empty()) << r2.error;
    EXPECT_EQ(r2.config->producerIdExpirationMs, 3'600'000);
    env["RUNE_PRODUCER_ID_EXPIRATION_MS"] = "-5";
    auto r3 = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r3.error.empty()) << r3.error;
    EXPECT_EQ(r3.config->producerIdExpirationMs, 7LL * 24 * 3600 * 1000) << "invalid -> default";
}

} // namespace rune::node

// REGRESSION (integ/perf-defaults, shard default): RUNE_CLIENT_ENDPOINT given as
// a full Aeron channel (docker/README.md form) was wrapped again →
// "aeron:udp?endpoint=aeron:udp?endpoint=host:port".  Legacy mode limped along;
// shard mode built its worker ingress from that host and refused to start
// ("unknown host").  Both forms must yield the same channel and bind host.
TEST(NodeConfigRegression, ClientEndpointAcceptsHostPortAndFullChannelRegression) {
    for (const char* raw : {"127.0.0.1:50001", "aeron:udp?endpoint=127.0.0.1:50001"}) {
        auto env = rune::node::base1node();
        env["RUNE_CLIENT_ENDPOINT"] = raw;
        auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_EQ(r.config->clientEndpoint.channel, "aeron:udp?endpoint=127.0.0.1:50001") << raw;
        EXPECT_EQ(r.config->clientBindHost(), "127.0.0.1") << raw;
    }
}

TEST(NodeConfig, ControlTermBytesEnvIsAPowerOfTwoAtLeast64KiB) {
    auto env = rune::node::base1node();
    EXPECT_EQ(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config->aeronControlTermBufferBytes, 1 << 20);
    env["RUNE_AERON_CONTROL_TERM_BYTES"] = "262144";
    EXPECT_EQ(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config->aeronControlTermBufferBytes, 262144);
    for (const char* bad : {"300000", "32768", "abc"}) {   // not pow2 / below Aeron's 64 KiB minimum / garbage
        env["RUNE_AERON_CONTROL_TERM_BYTES"] = bad;
        EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value()) << "refused: " << bad;
    }
}

TEST(NodeConfig, FsyncIntervalsDefaultOffAndParseRegression) {
    auto env = rune::node::base1node();
    auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->workerSyncIntervalMs, 0) << "no periodic leader fsync by default (Kafka model)";
    EXPECT_EQ(r.config->followerFlushIntervalMs, 0) << "no periodic follower fsync by default";
    env["RUNE_WORKER_SYNC_INTERVAL_MS"] = "10";
    env["RUNE_FOLLOWER_FLUSH_INTERVAL_MS"] = "20";
    r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->workerSyncIntervalMs, 10);
    EXPECT_EQ(r.config->followerFlushIntervalMs, 20);
    for (const char* bad : {"-1", "2592000001", "10ms"}) {   // negative / > 30 days (ms->ns overflow guard) / garbage
        env["RUNE_WORKER_SYNC_INTERVAL_MS"] = bad;
        EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value()) << "refused: " << bad;
    }
}

TEST(NodeConfig, IoWorkerCoresEnvParsesACpuList) {
    EXPECT_EQ(rune::node::parseCpuList("0"), (std::vector<int32_t>{0}));
    EXPECT_EQ(rune::node::parseCpuList("0,4-5"), (std::vector<int32_t>{0, 4, 5}));
    EXPECT_EQ(rune::node::parseCpuList("3,1"), (std::vector<int32_t>{1, 3})) << "sorted, deduplicated";
    for (const char* bad : {"", "a", "5-3", "-1", "1,", "1-", "1,,2", "4096"})
        EXPECT_FALSE(rune::node::parseCpuList(bad).has_value()) << "refused: '" << bad << "'";
    auto env = rune::node::base1node();
    env["RUNE_IOWQ_CORES"] = "0,2";
    auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->ioWorkerCpus, (std::vector<int32_t>{0, 2}));
    env["RUNE_IOWQ_CORES"] = "zero";
    EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value());
}

// D2: in shard mode the node driver carries only control-plane traffic (consensus, gateway/metadata,
// coordinators); "sleep-ns" with Aeron's tiny default sleep effectively polls (~45% CPU per node on the
// 4-core box).  Default = Aeron's backoff (spin, yield, park up to 1 ms) — never busy-spin.  Legacy mode
// (data path on that driver) keeps sleep-ns; RUNE_AERON_DRIVER_IDLE still overrides both.
TEST(NodeConfig, NodeDriverIdleDefaultsToBackoffInShardModeRegression) {
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(rune::node::base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    ASSERT_TRUE(res.config->shardMode());
    EXPECT_EQ(res.config->effectiveDriverIdleStrategy(), "backoff");
    EXPECT_EQ(rune::node::EmbeddedDriverConfig::fromNode(*res.config, "/tmp/x").idleStrategy, "backoff");
    auto legacy = rune::node::base1node();
    legacy["RUNE_WORKER_MODE"] = "legacy";
    auto rl = rune::node::parseNodeConfig(rune::node::fakeEnv(legacy));
    ASSERT_TRUE(rl.config.has_value()) << rl.error;
    ASSERT_FALSE(rl.config->shardMode());
    EXPECT_EQ(rl.config->effectiveDriverIdleStrategy(), "sleep-ns");
    auto ov = rune::node::base1node();
    ov["RUNE_AERON_DRIVER_IDLE"] = "sleep-ns";
    auto ro = rune::node::parseNodeConfig(rune::node::fakeEnv(ov));
    ASSERT_TRUE(ro.config.has_value()) << ro.error;
    EXPECT_EQ(ro.config->effectiveDriverIdleStrategy(), "sleep-ns") << "the override wins in shard mode";
}

TEST(NodeConfig, NonWorkerCoresEnvParsesACpuListRegression) {
    auto env = rune::node::base1node();
    env["RUNE_NONWORKER_CORES"] = "0,4-5";
    auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->nonWorkerCpus, (std::vector<int32_t>{0, 4, 5}));
    env["RUNE_NONWORKER_CORES"] = "core0";
    EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value());
}

// D2 round 2 (thread placement, 4-core box): the three nodes' control planes share core 0 and burned ~1 core
// IDLE — node driver ~14% each (Aeron backoff: every heartbeat / status message re-arms 10 spin + 20 yield duty
// cycles, each a recvmmsg per socket: ~17k empty recvmmsg/s), runtime ~10-14% each (10 spins, 20 yields, 1 us
// doubling park: ~3k yields + 3.4k parks/s).  Shard mode: control-plane threads park without yielding, from
// kControlIdleMinParkUs doubling to RUNE_CONTROL_IDLE_MAX_PARK_US; the node driver spins once (one spin = one full
// duty cycle).  Legacy mode (data path on the runtime / node driver) is unchanged.  Never busy-spin.
TEST(NodeConfig, ControlPlaneIdleParksWithoutYieldsInShardModeRegression) {
    using Shape = rune::node::NodeConfig::BackoffShape;
    using NC    = rune::node::NodeConfig;
    const auto parse = [](const std::map<std::string, std::string>& env) {
        auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
        EXPECT_TRUE(r.config.has_value()) << r.error;
        return r.config.value_or(NC{});
    };
    const int64_t minNs = NC::kControlIdleMinParkUs * 1000, maxNs = NC::kControlIdleMaxParkUsDefault * 1000;
    const std::string args = "1-0-" + std::to_string(minNs) + "-" + std::to_string(maxNs);
    const NC s = parse(rune::node::base1node());
    ASSERT_TRUE(s.shardMode());
    EXPECT_EQ(s.runtimeIdleShape(), (Shape{10, 0, minNs, maxNs}));
    EXPECT_EQ(s.adminIdleShape(), (Shape{10, 0, minNs, maxNs}));
    EXPECT_EQ(s.effectiveDriverIdleArgs(), args);
    EXPECT_EQ(rune::node::EmbeddedDriverConfig::fromNode(s, "/tmp/x").idleStrategyArgs, args);

    auto cap = rune::node::base1node();
    cap["RUNE_CONTROL_IDLE_MAX_PARK_US"] = "4000";
    EXPECT_EQ(parse(cap).runtimeIdleShape(), (Shape{10, 0, minNs, 4'000'000}));
    EXPECT_EQ(parse(cap).effectiveDriverIdleArgs(), "1-0-" + std::to_string(minNs) + "-4000000");
    cap["RUNE_CONTROL_IDLE_MAX_PARK_US"] = "20";   // below the min park: min = max
    EXPECT_EQ(parse(cap).runtimeIdleShape(), (Shape{10, 0, 20'000, 20'000}));
    for (const char* bad : {"0", "-5", "fast", "10ms"}) {
        cap["RUNE_CONTROL_IDLE_MAX_PARK_US"] = bad;
        EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(cap)).config.has_value()) << bad;
    }

    // D2 round-2 review R2-Md-1: a park cap near the election cadence would delay an idle leader's heartbeats
    // into endless leadership churn — refuse anything above a quarter of RUNE_ELECTION_STATUS_INTERVAL_MS.
    auto churn = rune::node::base1node();                       // default status interval 20 ms -> cap <= 5000 us
    churn["RUNE_CONTROL_IDLE_MAX_PARK_US"] = "5000";
    EXPECT_TRUE(rune::node::parseNodeConfig(rune::node::fakeEnv(churn)).config.has_value());
    churn["RUNE_CONTROL_IDLE_MAX_PARK_US"] = "100000";
    auto rc = rune::node::parseNodeConfig(rune::node::fakeEnv(churn));
    EXPECT_FALSE(rc.config.has_value()) << "100 ms park vs 20 ms election status interval must be refused";
    EXPECT_NE(rc.error.find("RUNE_ELECTION_STATUS_INTERVAL_MS"), std::string::npos) << rc.error;
    churn["RUNE_ELECTION_STATUS_INTERVAL_MS"] = "500";           // a slower cadence admits the larger cap
    EXPECT_TRUE(rune::node::parseNodeConfig(rune::node::fakeEnv(churn)).config.has_value());

    auto strat = rune::node::base1node();
    strat["RUNE_AERON_DRIVER_IDLE"] = "sleep-ns";
    EXPECT_EQ(parse(strat).effectiveDriverIdleArgs(), "") << "an explicit strategy keeps Aeron's own args";
    strat["RUNE_AERON_DRIVER_IDLE_ARGS"] = "2-1-10us-2ms";
    EXPECT_EQ(parse(strat).effectiveDriverIdleArgs(), "2-1-10us-2ms") << "explicit args win";

    auto legacy = rune::node::base1node();
    legacy["RUNE_WORKER_MODE"] = "legacy";
    const NC l = parse(legacy);
    ASSERT_FALSE(l.shardMode());
    EXPECT_EQ(l.runtimeIdleShape(), (Shape{10, 20, 1'000, l.idleMaxParkUs * 1000})) << "legacy: unchanged";
    EXPECT_EQ(l.adminIdleShape(), (Shape{10, 20, 1'000, 1'000'000})) << "legacy: Agrona defaults, unchanged";
    EXPECT_EQ(l.effectiveDriverIdleArgs(), "");
}

// D8c: RUNE_GATEWAY_PRODUCE_CREDITS is bounded 1 .. 4096 (a small share of a worker's ack queue).
TEST(NodeConfig, GatewayProduceCreditsIsBoundedRegression) {
    auto env = rune::node::base1node();
    auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->gatewayProduceCredits, 0u) << "absent -> the gateway default (1024)";
    for (const char* ok : {"1", "1024", "4096"}) {
        env["RUNE_GATEWAY_PRODUCE_CREDITS"] = ok;
        r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
        ASSERT_TRUE(r.config.has_value()) << ok << ": " << r.error;
        EXPECT_EQ(r.config->gatewayProduceCredits, static_cast<uint32_t>(std::stoul(ok)));
    }
    for (const char* bad : {"0", "-1", "4097", "99999999999999999999", "1k", " "}) {
        env["RUNE_GATEWAY_PRODUCE_CREDITS"] = bad;
        EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value()) << "refused: " << bad;
    }
}

// D8b re-review LOW-B: RUNE_MAX_HELD_IDLE_MS is bounded 1 ms .. 24 h (no ms->ns overflow); anything else is refused.
TEST(NodeConfig, MaxHeldIdleMsIsBoundedRegression) {
    auto env = rune::node::base1node();
    auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->maxHeldIdleMs, 0) << "absent -> the gateway default (10 min)";
    for (const char* ok : {"1", "600000", "86400000"}) {
        env["RUNE_MAX_HELD_IDLE_MS"] = ok;
        r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
        ASSERT_TRUE(r.config.has_value()) << ok << ": " << r.error;
        EXPECT_EQ(r.config->maxHeldIdleMs, std::stoll(ok));
    }
    for (const char* bad : {"0", "-1", "86400001", "9223372036854775807", "99999999999999999999", "10ms", " "}) {
        env["RUNE_MAX_HELD_IDLE_MS"] = bad;
        EXPECT_FALSE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value()) << "refused: " << bad;
    }
}
namespace rune::node {
// Re-review 2026-10-04 MEDIUM-B: the operator override for the consensus
// candidacy (metadata-ISR) gate is default OFF and parsed from
// RUNE_UNCLEAN_CONSENSUS_ELECTION.
TEST(NodeConfigRegression, UncleanConsensusElection_DefaultOffEnvOptIn) {
    auto r = parseNodeConfig(fakeEnv(base3node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_FALSE(r.config->uncleanConsensusElection) << "the unclean consensus election must be opt-in";
    auto env = base3node();
    env["RUNE_UNCLEAN_CONSENSUS_ELECTION"] = "1";
    auto on = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(on.error.empty()) << on.error;
    EXPECT_TRUE(on.config->uncleanConsensusElection);
    env["RUNE_UNCLEAN_CONSENSUS_ELECTION"] = "0";
    auto off = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(off.error.empty()) << off.error;
    EXPECT_FALSE(off.config->uncleanConsensusElection);
}
} // namespace rune::node

namespace rune::node {
// D19 review L5: the provision depth is clamped to [1, 8] (same as the agent).
TEST(NodeConfigTest, SegmentProvisionDepthClampedToOneEight) {
    for (const auto& [raw, want] : std::vector<std::pair<const char*, int32_t>>{
             {"0", 1}, {"1", 1}, {"3", 3}, {"8", 8}, {"64", 8}, {"-5", 1}}) {
        auto env = base3node();
        env["RUNE_SEGMENT_PROVISION_DEPTH"] = raw;
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_EQ(r.config->segmentProvisionDepth, want) << raw;
    }
}
}  // namespace rune::node

// A7 review L7: RUNE_IDLE_MAX_PARK_US bounds every fd-less wake on a parked worker (cross-thread ring hops,
// Aeron NAK / SM / heartbeat timers, a held-back intake).  It was unbounded and garbage was silently ignored
// (a typo measured the default).  Now: an integer in [1, 1000000] us, anything else refuses startup.
TEST(NodeConfigRegression, IdleMaxParkUsIsBoundedAndStrictRegression) {
    auto env = rune::node::base1node();
    env["RUNE_IDLE_MAX_PARK_US"] = "500";
    auto ok = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(ok.config.has_value()) << ok.error;
    EXPECT_EQ(ok.config->idleMaxParkUs, 500);
    env["RUNE_IDLE_MAX_PARK_US"] = "1000000";
    EXPECT_TRUE(rune::node::parseNodeConfig(rune::node::fakeEnv(env)).config.has_value());
    for (const char* bad : {"0", "-5", "fast", "10ms", "1000001", "5000000000"}) {
        env["RUNE_IDLE_MAX_PARK_US"] = bad;
        const auto r = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
        EXPECT_FALSE(r.config.has_value()) << "RUNE_IDLE_MAX_PARK_US=" << bad << " was accepted";
        if (!r.config.has_value()) EXPECT_NE(r.error.find("RUNE_IDLE_MAX_PARK_US"), std::string::npos) << r.error;
    }
}

TEST(NodeConfig, PersistenceModeWireMapping) {
    using rune::node::PersistenceMode;
    EXPECT_EQ(rune::node::persistenceToWire(PersistenceMode::Disk), 0u);
    EXPECT_EQ(rune::node::persistenceToWire(PersistenceMode::Transient), 1u);
    EXPECT_EQ(rune::node::persistenceFromWire(0), PersistenceMode::Disk);
    EXPECT_EQ(rune::node::persistenceFromWire(1), PersistenceMode::Transient);
}

TEST(NodeConfigRegression, PersistenceFromWireRejectsUnknownByte) {
    using rune::node::PersistenceMode;
    EXPECT_FALSE(rune::node::persistenceFromWire(2).has_value());
    EXPECT_FALSE(rune::node::persistenceFromWire(0x7f).has_value());
    EXPECT_FALSE(rune::node::persistenceFromWire(0xff).has_value());
    EXPECT_TRUE(rune::node::persistenceFromWire(0).has_value());
    EXPECT_TRUE(rune::node::persistenceFromWire(1).has_value());
}

// Controller ruling SQ5: RUNE_TRANSIENT_LOG_RING_BYTES is canonical;
// RUNE_MEMORY_LOG_RING_BYTES is a deprecated alias (one warning).
TEST(NodeConfigRegression, TransientLogRingBytesCanonical) {
    auto base = rune::node::base1node();
    base["RUNE_TRANSIENT_LOG_RING_BYTES"] = "16777216";
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->memoryLogRingBytes, 16 * 1024 * 1024);
    EXPECT_TRUE(res.warnings.empty());
}

TEST(NodeConfigRegression, MemoryLogRingBytesIsDeprecatedAlias) {
    auto base = rune::node::base1node();
    base["RUNE_MEMORY_LOG_RING_BYTES"] = "4194304";
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->memoryLogRingBytes, 4 * 1024 * 1024);
    ASSERT_EQ(res.warnings.size(), 1u);
    EXPECT_NE(res.warnings[0].find("RUNE_MEMORY_LOG_RING_BYTES"), std::string::npos) << res.warnings[0];
    EXPECT_NE(res.warnings[0].find("RUNE_TRANSIENT_LOG_RING_BYTES"), std::string::npos) << res.warnings[0];
}

TEST(NodeConfigRegression, TransientLogRingBytesWinsOverAlias) {
    auto base = rune::node::base1node();
    base["RUNE_TRANSIENT_LOG_RING_BYTES"] = "16777216";
    base["RUNE_MEMORY_LOG_RING_BYTES"]    = "4194304";
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->memoryLogRingBytes, 16 * 1024 * 1024);
    EXPECT_EQ(res.warnings.size(), 1u) << "the alias still warns";
}

// ===========================================================================
// Catalog history Task 6: RUNE_CATALOG_REPL_STREAM_ID (the __catalog plane's
// stream on the replication channel) — default 30; distinct from the
// replication stream and outside the per-worker replication range.
// ===========================================================================
TEST(NodeConfigCatalogStream, DefaultsToThirty) {
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(rune::node::base3node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->catalogReplStreamId, 30);
    EXPECT_FALSE(res.config->validateCatalogStream().has_value());
}

TEST(NodeConfigCatalogStream, EnvOverride) {
    auto env = rune::node::base3node();
    env["RUNE_CATALOG_REPL_STREAM_ID"] = "4242";
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->catalogReplStreamId, 4242);
}

TEST(NodeConfigCatalogStream, NonIntegerIsAnError) {
    auto env = rune::node::base3node();
    env["RUNE_CATALOG_REPL_STREAM_ID"] = "thirty";
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    EXPECT_FALSE(res.config.has_value());
    EXPECT_NE(res.error.find("RUNE_CATALOG_REPL_STREAM_ID"), std::string::npos) << res.error;
}

TEST(NodeConfigCatalogStreamRegression, EqualToTheReplicationStreamIsRefused) {
    auto env = rune::node::base3node();
    env["RUNE_REPLICATION_STREAM_ID"] = "30";
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    EXPECT_FALSE(res.config.has_value()) << "the catalog plane would share the replication stream";
    EXPECT_NE(res.error.find("RUNE_CATALOG_REPL_STREAM_ID"), std::string::npos) << res.error;
}

TEST(NodeConfigCatalogStreamRegression, InsideThePerWorkerReplicationRangeIsRefused) {
    auto env = rune::node::base3node();
    env["RUNE_REPL_BASE_STREAM_ID"] = "28";
    env["RUNE_WORKERS"] = "4";   // [28, 31] contains 30
    auto res = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    EXPECT_FALSE(res.config.has_value()) << "the catalog plane would share worker 2's stream";
    EXPECT_NE(res.error.find("per-worker replication range"), std::string::npos) << res.error;
    // Just outside the range: accepted.
    env["RUNE_REPL_BASE_STREAM_ID"] = "31";
    auto ok = rune::node::parseNodeConfig(rune::node::fakeEnv(env));
    ASSERT_TRUE(ok.config.has_value()) << ok.error;
    // Programmatic configs: the same rule (BrokerNode re-checks at start).
    rune::node::NodeConfig nc = *ok.config;
    nc.catalogReplStreamId = 32;
    EXPECT_TRUE(nc.validateCatalogStream().has_value());
}

// ===========================================================================
// Catalog history Task 17 (spec §8, F5): topics exist only through DDL.  The retired topics environment variable is an
// explicit refusal that points at the DDL docs — never silently ignored (a broker that "starts" with no topics
// and a stale compose file is the failure this pins).
// ===========================================================================
namespace rune::node {
TEST(NodeConfigRegression, RuneTopicsIsRefusedWithAPointerToDdl) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "events:1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("topics are created by DDL"), std::string::npos) << r.error;
    EXPECT_NE(r.error.find("RUNE_TOPICS"), std::string::npos) << "the error names the variable: " << r.error;
    EXPECT_NE(r.error.find("docs/SELF-HOSTING.md#topics-are-created-by-ddl"), std::string::npos) << r.error;
}

TEST(NodeConfigRegression, RuneTopicsSetToTheEmptyStringIsStillRefusedRegression) {
    // The old parser treated an empty value as "no topics"; presence alone is the refusal now.
    auto env = base1node();
    env["RUNE_TOPICS"] = "";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("topics are created by DDL"), std::string::npos) << r.error;
}

TEST(NodeConfigRegression, UnsetRuneTopicsBootsWithNoTopicsFieldRegression) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_TRUE(r.warnings.empty());
}
}  // namespace rune::node

// ---------------------------------------------------------------------------
// ISR design v3 §3.2 (Step 5): RUNE_STORAGE_FORMAT.  The env default is Required (an empty data dir refuses
// to start); new / join / auto map; anything else — TestFounder included — is the R-6 configuration error.
// The in-process struct default is TestFounder (rigs only).  RED: env default Auto.
// ---------------------------------------------------------------------------
namespace rune::node {

TEST(NodeConfigRegression, StorageFormatEnvDefaultsToRequiredRegression) {
    {
        auto r = parseNodeConfig(fakeEnv(base3node()));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_EQ(r.config->storageFormat, StorageFormat::Required) << "unset must never format";
        EXPECT_FALSE(r.config->storageRecoveryVote);
    }
    {
        auto env = base3node();
        env["RUNE_STORAGE_FORMAT"] = "";
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_EQ(r.config->storageFormat, StorageFormat::Required);
    }
    const std::pair<const char*, StorageFormat> ok[] = {
        {"new", StorageFormat::New}, {"join", StorageFormat::Join}, {"auto", StorageFormat::Auto}};
    for (const auto& [v, want] : ok) {
        auto env = base1node();
        env["RUNE_STORAGE_FORMAT"] = v;
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.config.has_value()) << v << ": " << r.error;
        EXPECT_EQ(r.config->storageFormat, want) << v;
    }
    for (const char* bad : {"xyz", "testfounder", "TestFounder", "NEW"}) {
        auto env = base3node();
        env["RUNE_STORAGE_FORMAT"] = bad;
        auto r = parseNodeConfig(fakeEnv(env));
        EXPECT_FALSE(r.config.has_value()) << bad << " must be refused";
        EXPECT_EQ(r.error, std::string("RUNE_STORAGE_FORMAT='") + bad +
                               "' is not one of new, join, auto\n(unset = never format an empty data dir)");
    }
    for (const char* on : {"1", "true", "TRUE"}) {
        auto env = base3node();
        env["RUNE_STORAGE_RECOVERY_VOTE"] = on;
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.config.has_value()) << r.error;
        EXPECT_TRUE(r.config->storageRecoveryVote) << on;
    }
    EXPECT_FALSE(parseStorageFormat("testfounder").has_value());
    EXPECT_EQ(NodeConfig{}.storageFormat, StorageFormat::TestFounder) << "the in-process (rig) default";
}

} // namespace rune::node
