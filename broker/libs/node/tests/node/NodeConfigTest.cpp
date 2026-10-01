#include <rune/node/NodeConfig.h>
#include <rune/node/Worker.h>   // Worker::maxAcceptableBatchBytes (THE batch limit)

#include <filesystem>

#include <gtest/gtest.h>
#include <map>
#include <optional>
#include <string>

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
    EXPECT_EQ(r.config->workerEgressControlHost,     "127.0.0.1");
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
    env["RUNE_WORKER_EGRESS_CONTROL_HOST"] = "";  // empty → keep default
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_EQ(r.config->workerEgressControlHost, "127.0.0.1");
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
        auto r = parseNodeConfig(fakeEnv(base3node()));
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_FALSE(r.config->perWorkerEgressEnabled);
    }
    // "1"/"true"/"TRUE" → true.
    for (const char* v : {"1", "true", "TRUE"}) {
        auto env = base3node();
        env["RUNE_PER_WORKER_EGRESS"] = v;
        auto r = parseNodeConfig(fakeEnv(env));
        ASSERT_TRUE(r.error.empty()) << r.error;
        EXPECT_TRUE(r.config->perWorkerEgressEnabled) << "value=" << v;
    }
    // Other non-empty → false.
    {
        auto env = base3node();
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
// RUNE_TOPICS parse/validate tests (GB-1)
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, TopicsAbsentYieldsEmpty) {
    // No RUNE_TOPICS → cfg.topics is empty (valid — no topics configured).
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_TRUE(r.config->topics.empty());
}

TEST(NodeConfigTest, TopicsEmptyStringYieldsEmpty) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_TRUE(r.config->topics.empty());
}

TEST(NodeConfigTest, TopicsSingleTopic) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat:1";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->topics.size(), 1u);
    EXPECT_EQ(r.config->topics[0].name,           "chat");
    EXPECT_EQ(r.config->topics[0].partitionCount, 1);
    EXPECT_EQ(r.config->topics[0].topicId,        0);
}

TEST(NodeConfigTest, TopicsMultiTopic) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "inventory:8,entities:4,chat:1";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->topics.size(), 3u);

    EXPECT_EQ(r.config->topics[0].name,           "inventory");
    EXPECT_EQ(r.config->topics[0].partitionCount, 8);
    EXPECT_EQ(r.config->topics[0].topicId,        0);

    EXPECT_EQ(r.config->topics[1].name,           "entities");
    EXPECT_EQ(r.config->topics[1].partitionCount, 4);
    EXPECT_EQ(r.config->topics[1].topicId,        1);

    EXPECT_EQ(r.config->topics[2].name,           "chat");
    EXPECT_EQ(r.config->topics[2].partitionCount, 1);
    EXPECT_EQ(r.config->topics[2].topicId,        2);
}

TEST(NodeConfigTest, TopicsTopicIdIsDeclarationIndex) {
    // topicId must be 0-based declaration index regardless of name ordering.
    auto env = base1node();
    env["RUNE_TOPICS"] = "z:2,a:4,m:1";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->topics.size(), 3u);
    EXPECT_EQ(r.config->topics[0].topicId, 0);
    EXPECT_EQ(r.config->topics[1].topicId, 1);
    EXPECT_EQ(r.config->topics[2].topicId, 2);
}

TEST(NodeConfigTest, TopicsZeroPartitionCountIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat:0";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsNegativePartitionCountIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat:-1";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsNonIntPartitionCountIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat:abc";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsDuplicateNameIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat:1,entities:4,chat:2";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_NE(r.error.find("chat"), std::string::npos);
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsMissingColonIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsEmptyNameIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = ":4";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsWhitespaceTrimmed) {
    // Whitespace around commas and entries is trimmed.
    auto env = base1node();
    env["RUNE_TOPICS"] = " inventory : 8 , chat : 1 ";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->topics.size(), 2u);
    EXPECT_EQ(r.config->topics[0].name,           "inventory");
    EXPECT_EQ(r.config->topics[0].partitionCount, 8);
    EXPECT_EQ(r.config->topics[1].name,           "chat");
    EXPECT_EQ(r.config->topics[1].partitionCount, 1);
}

TEST(NodeConfigTest, TopicsDoubleCommaIsError) {
    auto env = base1node();
    env["RUNE_TOPICS"] = "chat:1,,entities:4";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty());
    EXPECT_FALSE(r.config.has_value());
}

TEST(NodeConfigTest, TopicsLargePartitionCount) {
    // Large partition count (e.g. 64) is valid.
    auto env = base1node();
    env["RUNE_TOPICS"] = "big:64";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->topics.size(), 1u);
    EXPECT_EQ(r.config->topics[0].partitionCount, 64);
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
    // programmatically (RUNE_TOPICS only carries name:partitionCount; cleanup
    // policy is set directly on the struct — this test verifies the struct holds
    // the values it is given, not that parseNodeConfig threads them through).
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

// ---------------------------------------------------------------------------
// CP-5.2: Extended RUNE_TOPICS format tests
// (name:partitions[:policy[:segmentRollBytes[:deleteRetentionMs[:retentionMs[:retentionBytes[:retentionCheckMs[:fileDeleteDelayMs]]]]]]]])
// ---------------------------------------------------------------------------

TEST(NodeConfigTest, CP52_ExtendedTopics_BackwardCompatible_TwoFields) {
    // "name:N" (2-field) still works exactly as before: Delete policy, defaults.
    auto env = base3node();
    env["RUNE_TOPICS"] = "events:1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << "unexpected error: " << err;
    ASSERT_EQ(cfg->topics.size(), 1u);
    EXPECT_EQ(cfg->topics[0].name,          "events");
    EXPECT_EQ(cfg->topics[0].partitionCount, 1);
    EXPECT_EQ(cfg->topics[0].cleanupPolicy,  TopicSpec::CleanupPolicy::Delete);
    // Default retentionMs and retentionBytes are -1 (unbounded).
    EXPECT_EQ(cfg->topics[0].retentionMs,   -1LL);
    EXPECT_EQ(cfg->topics[0].retentionBytes,-1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_CompactPolicy) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:2:compact";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    ASSERT_EQ(cfg->topics.size(), 1u);
    EXPECT_EQ(cfg->topics[0].name,          "compact_log");
    EXPECT_EQ(cfg->topics[0].partitionCount, 2);
    EXPECT_EQ(cfg->topics[0].cleanupPolicy,  TopicSpec::CleanupPolicy::Compact);
    // Other fields default.
    EXPECT_EQ(cfg->topics[0].segmentRollBytes, 64LL * 1024 * 1024);
    EXPECT_EQ(cfg->topics[0].retentionMs,   -1LL);
    EXPECT_EQ(cfg->topics[0].retentionBytes,-1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_DeletePolicy_Explicit) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "log:1:delete";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    ASSERT_EQ(cfg->topics.size(), 1u);
    EXPECT_EQ(cfg->topics[0].cleanupPolicy, TopicSpec::CleanupPolicy::Delete);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_CompactDeletePolicy) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "mixed:3:compact_delete";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    ASSERT_EQ(cfg->topics.size(), 1u);
    EXPECT_EQ(cfg->topics[0].cleanupPolicy, TopicSpec::CleanupPolicy::CompactDelete);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_PolicyCaseInsensitive) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "a:1:Compact";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].cleanupPolicy, TopicSpec::CleanupPolicy::Compact);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_WithSegmentRollBytes) {
    auto env = base3node();
    // Set 4 KiB segment roll to force quick sealing in tests.
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    ASSERT_EQ(cfg->topics.size(), 1u);
    EXPECT_EQ(cfg->topics[0].cleanupPolicy,    TopicSpec::CleanupPolicy::Compact);
    EXPECT_EQ(cfg->topics[0].segmentRollBytes, 4096LL);
    EXPECT_EQ(cfg->topics[0].deleteRetentionMs, 86'400'000LL);  // default
}

TEST(NodeConfigTest, CP52_ExtendedTopics_WithDeleteRetentionMs) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].segmentRollBytes,   4096LL);
    EXPECT_EQ(cfg->topics[0].deleteRetentionMs,  100LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_WithRetentionMs) {
    auto env = base3node();
    // retentionMs = -1 means unbounded.
    env["RUNE_TOPICS"] = "delete_log:1:delete:4096:1:-1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].cleanupPolicy,    TopicSpec::CleanupPolicy::Delete);
    EXPECT_EQ(cfg->topics[0].segmentRollBytes, 4096LL);
    EXPECT_EQ(cfg->topics[0].retentionMs,      -1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_WithRetentionBytes) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "delete_log:1:delete:4096:1:-1:32768";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].retentionBytes,  32768LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_WithRetentionCheckMs) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "delete_log:1:delete:4096:1:-1:32768:1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].retentionCheckMs, 1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_WithFileDeleteDelayMs) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "delete_log:1:delete:4096:1:-1:32768:1:1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].fileDeleteDelayMs, 1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_FullSpec_Compact) {
    // Full extended spec as used by compaction e2e tests.
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100:-1:-1:1:1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    const auto& ts = cfg->topics[0];
    EXPECT_EQ(ts.name,             "compact_log");
    EXPECT_EQ(ts.partitionCount,   1);
    EXPECT_EQ(ts.cleanupPolicy,    TopicSpec::CleanupPolicy::Compact);
    EXPECT_EQ(ts.segmentRollBytes, 4096LL);
    EXPECT_EQ(ts.deleteRetentionMs, 100LL);
    EXPECT_EQ(ts.retentionMs,      -1LL);
    EXPECT_EQ(ts.retentionBytes,   -1LL);
    EXPECT_EQ(ts.retentionCheckMs, 1LL);
    EXPECT_EQ(ts.fileDeleteDelayMs, 1LL);
}

TEST(NodeConfigTest, CCC_ExtendedTopics_WithMinCleanableDirtyRatio) {
    // Field 9: minCleanableDirtyRatio (double).  0.0 = compact every pass.
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100:-1:-1:1:1:0.0";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_DOUBLE_EQ(r.config->topics[0].minCleanableDirtyRatio, 0.0);
}

TEST(NodeConfigTest, CCC_ExtendedTopics_MinCleanableDirtyRatio_Midpoint) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100:-1:-1:1:1:0.75";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_DOUBLE_EQ(r.config->topics[0].minCleanableDirtyRatio, 0.75);
}

TEST(NodeConfigTest, CCC_ExtendedTopics_MinCleanableDirtyRatio_OutOfRange_Rejected) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100:-1:-1:1:1:1.5";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty()) << "should reject minCleanableDirtyRatio > 1.0";
}

TEST(NodeConfigTest, CCC_ExtendedTopics_MinCleanableDirtyRatio_NonNumber_Rejected) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100:-1:-1:1:1:abc";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.error.empty()) << "should reject non-numeric minCleanableDirtyRatio";
}

TEST(NodeConfigTest, CCC_ExtendedTopics_WithMaxCompactionLagMs) {
    // Field 10: maxCompactionLagMs (-1 = disabled, or positive ms).
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100:-1:-1:1:1:0.0:5000";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_DOUBLE_EQ(r.config->topics[0].minCleanableDirtyRatio, 0.0);
    EXPECT_EQ(r.config->topics[0].maxCompactionLagMs, 5000LL);
}

TEST(NodeConfigTest, CCC_ExtendedTopics_NewFieldsDefaultWhenAbsent) {
    // Omitting fields 9/10 keeps the Kafka defaults (0.5 / -1).
    auto env = base3node();
    env["RUNE_TOPICS"] = "compact_log:1:compact:4096:100";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    EXPECT_DOUBLE_EQ(r.config->topics[0].minCleanableDirtyRatio, 0.5);
    EXPECT_EQ(r.config->topics[0].maxCompactionLagMs, -1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_FullSpec_Delete) {
    // Full extended spec as used by retention e2e tests.
    auto env = base3node();
    env["RUNE_TOPICS"] = "delete_log:1:delete:4096:1:-1:32768:1:1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    const auto& ts = cfg->topics[0];
    EXPECT_EQ(ts.cleanupPolicy,    TopicSpec::CleanupPolicy::Delete);
    EXPECT_EQ(ts.segmentRollBytes, 4096LL);
    EXPECT_EQ(ts.deleteRetentionMs, 1LL);
    EXPECT_EQ(ts.retentionMs,      -1LL);
    EXPECT_EQ(ts.retentionBytes,   32768LL);
    EXPECT_EQ(ts.retentionCheckMs, 1LL);
    EXPECT_EQ(ts.fileDeleteDelayMs, 1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_MultipleTopics_MixedPolicies) {
    // Two topics: one compact, one delete — as used by compaction e2e overlay.
    auto env = base3node();
    env["RUNE_TOPICS"] =
        "compact_log:1:compact:4096:100:-1:-1:1:1,"
        "delete_log:1:delete:4096:1:-1:32768:1:1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    ASSERT_EQ(cfg->topics.size(), 2u);
    EXPECT_EQ(cfg->topics[0].name,          "compact_log");
    EXPECT_EQ(cfg->topics[0].cleanupPolicy, TopicSpec::CleanupPolicy::Compact);
    EXPECT_EQ(cfg->topics[1].name,          "delete_log");
    EXPECT_EQ(cfg->topics[1].cleanupPolicy, TopicSpec::CleanupPolicy::Delete);
    // TopicId is 0-based declaration order.
    EXPECT_EQ(cfg->topics[0].topicId, 0);
    EXPECT_EQ(cfg->topics[1].topicId, 1);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_InvalidPolicy) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "log:1:bad_policy";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    EXPECT_FALSE(err.empty()) << "should reject unknown policy 'bad_policy'";
    EXPECT_NE(err.find("bad_policy"), std::string::npos);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_InvalidSegmentRollBytes_Zero) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "log:1:compact:0";   // 0 is not a positive integer
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    EXPECT_FALSE(err.empty()) << "should reject segmentRollBytes=0";
}

TEST(NodeConfigTest, CP52_ExtendedTopics_InvalidSegmentRollBytes_Negative) {
    auto env = base3node();
    env["RUNE_TOPICS"] = "log:1:compact:-1";  // negative is invalid for roll bytes
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    EXPECT_FALSE(err.empty()) << "should reject negative segmentRollBytes";
}

TEST(NodeConfigTest, CP52_ExtendedTopics_RetentionBytes_MinusOneAllowed) {
    // retentionBytes = -1 means unbounded — this is a valid configuration.
    auto env = base3node();
    env["RUNE_TOPICS"] = "log:1:delete:4096:1:-1:-1";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(cfg->topics[0].retentionBytes, -1LL);
}

TEST(NodeConfigTest, CP52_ExtendedTopics_RetentionBytes_BelowMinusOne_Rejected) {
    // retentionBytes < -1 is invalid.
    auto env = base3node();
    env["RUNE_TOPICS"] = "log:1:delete:4096:1:-1:-2";
    auto r = parseNodeConfig(fakeEnv(env));
    auto& err = r.error;
    auto& cfg = r.config;
    EXPECT_FALSE(err.empty()) << "should reject retentionBytes=-2";
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
// CP-10 Fix 1: ioMaxBytesPerSecond — parsed from TopicSpec, threaded to cleaner
//
// The parsing of TopicSpec::ioMaxBytesPerSecond is outside NodeConfig (it lives
// in TopicSpec directly from RUNE_TOPICS fields).  These tests verify that:
//   - TopicSpec::ioMaxBytesPerSecond retains its default of -1 when not set in
//     RUNE_TOPICS (no extra field).
//   - The BrokerNode wiring picks it up (integration: tested via BrokerNodeTest).
// ===========================================================================

TEST(NodeConfigTest, CP10_IoMaxBytesPerSecond_DefaultMinusOne) {
    // With the standard "name:N" topic format the field stays at -1.
    auto env = base1node();
    env["RUNE_TOPICS"] = "events:2";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.error.empty()) << r.error;
    ASSERT_EQ(r.config->topics.size(), 1u);
    EXPECT_EQ(r.config->topics[0].ioMaxBytesPerSecond, -1LL)
        << "ioMaxBytesPerSecond must default to -1 (unlimited)";
}

// ===========================================================================
// Task M1: co-partition group, partition key, persistence, durability
// ===========================================================================

TEST(NodeConfig, ParsesCoPartitionGroupKeyAndPersistence) {
    auto env = [](const char* k)->const char*{
        std::string s=k;
        if (s=="RUNE_NODE_ID") return "0";
        if (s=="RUNE_PEERS") return "0@h:1";
        if (s=="RUNE_CLIENT_ENDPOINT") return "h:2";
        if (s=="RUNE_DATA_DIR") return "/tmp/x";
        if (s=="RUNE_TOPICS") return "wallet:4:group=acct:key=userId:durability=all-in-sync,presence:2:persistence=memory";
        return nullptr;
    };
    auto res = rune::node::parseNodeConfig(env);
    ASSERT_TRUE(res.config.has_value()) << res.error;
    const auto& ts = res.config->topics;
    ASSERT_EQ(ts.size(), 2u);
    EXPECT_EQ(ts[0].coPartitionGroup, "acct");
    EXPECT_EQ(ts[0].partitionKey, "userId");
    EXPECT_EQ(ts[0].durability, 1);
    EXPECT_EQ(ts[1].persistence, rune::node::PersistenceMode::Memory);
}

TEST(NodeConfig, LegacyTopicGrammarStillParses) {
    auto env=[](const char* k)->const char*{ std::string s=k;
        if (s=="RUNE_NODE_ID") return "0"; if (s=="RUNE_PEERS") return "0@h:1";
        if (s=="RUNE_CLIENT_ENDPOINT") return "h:2"; if (s=="RUNE_DATA_DIR") return "/tmp/x";
        if (s=="RUNE_TOPICS") return "inventory:8,chat:1"; return nullptr; };
    auto res=rune::node::parseNodeConfig(env);
    ASSERT_TRUE(res.config.has_value());
    EXPECT_EQ(res.config->topics[0].coPartitionGroup, "");
    EXPECT_EQ(res.config->topics[0].persistence, rune::node::PersistenceMode::Disk);
}

TEST(NodeConfig, PersistenceMemoryDurabilityAllInSync) {
    // Memory topic + AllInSync durability is a valid configuration.
    // It means: every write must be acked by ALL replicas (but stored in RAM).
    auto env=[](const char* k)->const char*{ std::string s=k;
        if (s=="RUNE_NODE_ID") return "0"; if (s=="RUNE_PEERS") return "0@h:1";
        if (s=="RUNE_CLIENT_ENDPOINT") return "h:2"; if (s=="RUNE_DATA_DIR") return "/tmp/x";
        if (s=="RUNE_TOPICS") return "ephemeral:2:persistence=memory:durability=all-in-sync"; return nullptr; };
    auto res=rune::node::parseNodeConfig(env);
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->topics[0].persistence, rune::node::PersistenceMode::Memory);
    EXPECT_EQ(res.config->topics[0].durability, 1u);
}

TEST(NodeConfig, UnknownKeyValueOptionRejected) {
    auto env=[](const char* k)->const char*{ std::string s=k;
        if (s=="RUNE_NODE_ID") return "0"; if (s=="RUNE_PEERS") return "0@h:1";
        if (s=="RUNE_CLIENT_ENDPOINT") return "h:2"; if (s=="RUNE_DATA_DIR") return "/tmp/x";
        if (s=="RUNE_TOPICS") return "t:2:badkey=badval"; return nullptr; };
    auto res=rune::node::parseNodeConfig(env);
    EXPECT_FALSE(res.config.has_value()) << "unknown key=value option must be rejected";
    EXPECT_NE(res.error.find("badkey"), std::string::npos);
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
// E3 Task 7: io-ring depths + memory-log ring bytes (RUNE_IO_SUBMIT_RING_DEPTH,
// RUNE_IO_COMPLETION_RING_DEPTH, RUNE_MEMORY_LOG_RING_BYTES)
// ---------------------------------------------------------------------------

TEST(NodeConfig, IoRingDepthsParseFromEnvPow2) {
    // Override two of the three fields; confirm the third stays at its default.
    auto base = base1node();
    base["RUNE_IO_SUBMIT_RING_DEPTH"]  = "2048";
    base["RUNE_MEMORY_LOG_RING_BYTES"] = "16777216"; // 16 MiB = 2^24
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
    base["RUNE_MEMORY_LOG_RING_BYTES"]    = "7";     // not a power of two
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
    base["RUNE_MEMORY_LOG_RING_BYTES"]    = "4194304"; // 4 MiB = 2^22
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
    auto base = base1node();
    base["RUNE_TOPICS"] = "events:2";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    ASSERT_EQ(res.config->topics.size(), 1u);
    EXPECT_EQ(res.config->topics[0].durabilityTier, 1u)
        << "default durabilityTier must be 1 (Majority)";
}

// ---------------------------------------------------------------------------
// E4: CPU affinity — RUNE_WORKER_CORE_BASE parse tests.
// ---------------------------------------------------------------------------

TEST(NodeConfig, WorkerCoreBaseDefaultIsDisabled) {
    // Absent env var → workerCoreBase defaults to -1 (affinity disabled).
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, -1)
        << "default workerCoreBase must be -1 (affinity disabled)";
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

TEST(NodeConfig, WorkerCoreBaseUnparsableKeepsDefault) {
    // Unparseable value → default -1 (safe degradation, no error).
    auto base = base1node();
    base["RUNE_WORKER_CORE_BASE"] = "notanumber";
    auto res = rune::node::parseNodeConfig(fakeEnv(base));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->workerCoreBase, -1)
        << "unparseable RUNE_WORKER_CORE_BASE must keep default -1";
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
    // No RUNE_AERON_DRIVER_IDLE set → must default to "sleep-ns".
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->driverIdleStrategy, "sleep-ns")
        << "default driverIdleStrategy must be 'sleep-ns' (byte-identical to prior hard-code)";
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
    auto res = rune::node::parseNodeConfig(fakeEnv(base1node()));
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
    auto base = base1node();
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

TEST(NodeConfig, PeerWorkerCountDefaultsToZero) {
    // PeerConfig::workerCount defaults to 0 (unknown) for peers parsed from RUNE_PEERS.
    auto res = rune::node::parseNodeConfig(fakeEnv(base3node()));
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

TEST(NodeConfig, DefaultsAreLegacyThreadAndValid) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->workerMode, NodeConfig::WorkerMode::Legacy);
    EXPECT_EQ(r.config->runtimePlacement, NodeConfig::RuntimePlacement::Thread);
    EXPECT_FALSE(r.config->workerIngressBasePort.has_value());
    EXPECT_FALSE(r.config->workerReplBasePort.has_value());
    EXPECT_FALSE(r.config->validateShardConfig().has_value());
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

TEST(NodeConfigRegression, PeerEntryWithSixPortsRejected) {
    auto env = base3node();
    env["RUNE_PEERS"] = "0@h:1:2:3:4:5:6,1@h:11,2@h:12";
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
    auto env = base1node(); env["RUNE_RUNTIME_PLACEMENT"] = "fused";
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
    auto r = parseNodeConfig(fakeEnv(base1node()));
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

TEST(ProduceBatchLimitRegression, DefaultThreeNodeLimitIsTheOutboundRingFrame) {
    // Default outbound ring 4 MiB → 512 KiB frames: an 8 MiB batch must NOT be
    // accepted (it would be appended but never replicated) — and the default
    // config still starts (no refusal; the limit simply shrinks).
    const NodeConfig c = parsedOrDie(base3node());
    EXPECT_LT(c.produceIntakeLimit(), 512 * 1024);
    EXPECT_GT(c.produceIntakeLimit(), 511 * 1024);
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

} // namespace rune::node
