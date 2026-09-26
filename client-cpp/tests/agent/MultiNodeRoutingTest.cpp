// tests/agent/MultiNodeRoutingTest.cpp
//
// Task 1.3 — Multi-node partition routing unit tests.
//
// Validates the agent-owned partition routing table + endpoint-keyed multi-session
// registry that make PRODUCE route to per-partition leaders across nodes:
//
//   MN1: RouteTableMapsPartitionsToLeaderEndpoints
//        Seed a MetadataCache with 8 partitions whose leaders are spread across 3
//        distinct endpoints, mirror the Client::onMetadataResponse route build, and
//        assert each partition's per-worker ingressStreamId resolves (via the agent
//        routing table) to its OWN leader endpoint — i.e. each partition's produce
//        would be sent on its leader session's stream.
//
//   MN2: MultiSessionOpenPerEndpoint
//        sessionForEndpoint() opens exactly one NEW session per distinct endpoint
//        over the shared transport (the seed + one per additional leader) and
//        returns the SAME session on repeat calls for an endpoint.
//
//   MN3: SingleNodeEmptyRouteTableOneSession
//        When every partition's leader is the seed endpoint, the route table is
//        EMPTY (byte-identical hot path) and only the seed session exists.
//
// No live Aeron driver: the ClientAgent is constructed but NOT start()ed.  The
// routing table (setRoute/routeEndpointFor), the session registry
// (sessionForEndpoint/sessionCount), and MetadataCache are all agent-thread plain
// data — exercised directly without any transport I/O.  sessionForEndpoint()'s lazy
// connect() flows into a noexcept transport offer that safely returns false when the
// transport is not started (no send occurs, no throw), which is exactly the parked
// state the routing table models.

#include <rune/client/agent/ClientAgent.h>
#include <rune/client/metadata/MetadataCache.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace rca = rune::client::agent;
namespace mc  = rune::client::metadata;

namespace {

// Build an AgentConfig whose seed endpoint is a distinct, non-leader gateway
// channel (so the seed is never confused with any partition leader endpoint).
rca::AgentConfig makeAgentConfig(const std::string& seedChannel) {
    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = seedChannel;
    cfg.transport.ingressStreamId = 1;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 2;
    cfg.transport.dataDir         = "/tmp/rune-mn-routing-unused";  // never start()ed
    return cfg;
}

// Build the leader channel string exactly as Client::onMetadataResponse does.
std::string leaderChannelFor(const mc::MetadataCache& cache, int32_t topicId,
                             int32_t partition) {
    const int32_t node = cache.leaderFor(topicId, partition);
    const std::string& ep = cache.endpointFor(node);
    if (ep.empty()) return {};
    return (ep.rfind("aeron:", 0) == 0) ? ep : ("aeron:udp?endpoint=" + ep);
}

} // namespace

// ===========================================================================
// MN1: routing table maps each partition to its leader endpoint
// ===========================================================================

TEST(MultiNodeRouting, RouteTableMapsPartitionsToLeaderEndpoints) {
    constexpr int32_t kTopic = 7;

    // 3 broker nodes at distinct endpoints.
    std::vector<mc::BrokerEntry> brokers = {
        {0, 9000, "hostA"},
        {1, 9001, "hostB"},
        {2, 9002, "hostC"},
    };

    // 8 partitions; leaders spread across the 3 nodes; each partition has a
    // distinct per-worker ingressStreamId (broker-globally-unique).
    // partition -> (leaderNode, ingressStreamId)
    struct P { int32_t part; int32_t leader; int32_t stream; };
    std::vector<P> layout = {
        {0, 0, 500}, {1, 1, 501}, {2, 2, 502}, {3, 0, 503},
        {4, 1, 504}, {5, 2, 505}, {6, 0, 506}, {7, 1, 507},
    };

    std::vector<mc::PartitionEntry> parts;
    for (const auto& p : layout) {
        mc::PartitionEntry e;
        e.topicId = kTopic; e.partition = p.part;
        e.leaderId = p.leader; e.leaderEpoch = 1;
        e.ingressStreamId = p.stream;
        parts.push_back(e);
    }

    mc::MetadataCache cache;
    cache.updateFromResponse(brokers, parts);

    // Seed endpoint deliberately NOT equal to any leader endpoint.
    rca::ClientAgent agent(makeAgentConfig("aeron:udp?endpoint=seed:1"));

    // Mirror Client::onMetadataResponse's route build.
    for (const auto& p : layout) {
        const std::string chan = leaderChannelFor(cache, kTopic, p.part);
        ASSERT_FALSE(chan.empty()) << "partition " << p.part << " has no leader endpoint";
        agent.setRoute(p.stream, chan);
    }

    // Assert each partition's stream routes to ITS leader endpoint, and that the
    // three leader groups map to three DISTINCT endpoints.
    const std::string epA = "aeron:udp?endpoint=hostA:9000";
    const std::string epB = "aeron:udp?endpoint=hostB:9001";
    const std::string epC = "aeron:udp?endpoint=hostC:9002";

    EXPECT_EQ(agent.routeEndpointFor(500), epA);
    EXPECT_EQ(agent.routeEndpointFor(503), epA);
    EXPECT_EQ(agent.routeEndpointFor(506), epA);
    EXPECT_EQ(agent.routeEndpointFor(501), epB);
    EXPECT_EQ(agent.routeEndpointFor(504), epB);
    EXPECT_EQ(agent.routeEndpointFor(507), epB);
    EXPECT_EQ(agent.routeEndpointFor(502), epC);
    EXPECT_EQ(agent.routeEndpointFor(505), epC);

    // Three distinct leader endpoints in play.
    EXPECT_NE(epA, epB);
    EXPECT_NE(epB, epC);
    EXPECT_NE(epA, epC);
}

// ===========================================================================
// MN2: multi-session open — one session per distinct endpoint
// ===========================================================================

TEST(MultiNodeRouting, MultiSessionOpenPerEndpoint) {
    rca::ClientAgent agent(makeAgentConfig("aeron:udp?endpoint=seed:1"));

    // Only the seed session exists initially.
    EXPECT_EQ(agent.sessionCount(), 1u);

    // Seed endpoint resolves to the seed session (no new session).
    rune::client::agent::Session& seed =
        agent.sessionForEndpoint("aeron:udp?endpoint=seed:1");
    EXPECT_EQ(agent.sessionCount(), 1u);
    EXPECT_EQ(&seed, &agent.sessionForEndpoint(""));  // empty == seed

    // Two distinct leader endpoints → two NEW sessions.
    auto& sA = agent.sessionForEndpoint("aeron:udp?endpoint=hostA:9000");
    EXPECT_EQ(agent.sessionCount(), 2u);
    auto& sB = agent.sessionForEndpoint("aeron:udp?endpoint=hostB:9001");
    EXPECT_EQ(agent.sessionCount(), 3u);

    // Distinct session objects, each bound to its own endpoint.
    EXPECT_NE(&sA, &sB);
    EXPECT_NE(&sA, &seed);
    EXPECT_EQ(sA.endpoint, "aeron:udp?endpoint=hostA:9000");
    EXPECT_EQ(sB.endpoint, "aeron:udp?endpoint=hostB:9001");

    // Repeat call for an existing endpoint returns the SAME session (idempotent).
    auto& sA2 = agent.sessionForEndpoint("aeron:udp?endpoint=hostA:9000");
    EXPECT_EQ(&sA, &sA2);
    EXPECT_EQ(agent.sessionCount(), 3u);
}

// ===========================================================================
// MN3: single-node → empty route table + one session (byte-identical hot path)
// ===========================================================================

TEST(MultiNodeRouting, SingleNodeEmptyRouteTableOneSession) {
    constexpr int32_t kTopic = 3;
    const std::string kSeed = "aeron:udp?endpoint=solo:1";

    // One broker node whose endpoint IS the seed endpoint.
    std::vector<mc::BrokerEntry> brokers = { {0, 1, "solo"} };
    std::vector<mc::PartitionEntry> parts;
    for (int32_t p = 0; p < 8; ++p) {
        mc::PartitionEntry e;
        e.topicId = kTopic; e.partition = p;
        e.leaderId = 0; e.leaderEpoch = 1;
        e.ingressStreamId = 600 + p;
        parts.push_back(e);
    }
    mc::MetadataCache cache;
    cache.updateFromResponse(brokers, parts);

    rca::ClientAgent agent(makeAgentConfig(kSeed));

    // Build routes; every leader endpoint == the seed → setRoute drops them all.
    for (int32_t p = 0; p < 8; ++p) {
        const std::string chan = leaderChannelFor(cache, kTopic, p);
        agent.setRoute(600 + p, chan);
    }

    // Empty route table → the produce send lambda takes the default offer path.
    for (int32_t p = 0; p < 8; ++p) {
        EXPECT_TRUE(agent.routeEndpointFor(600 + p).empty())
            << "single-node partition " << p << " must have no per-node route";
    }
    // Only the seed session exists.
    EXPECT_EQ(agent.sessionCount(), 1u);
}
