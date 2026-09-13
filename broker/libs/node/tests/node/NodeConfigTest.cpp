#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>
#include <map>
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

} // namespace rune::node
