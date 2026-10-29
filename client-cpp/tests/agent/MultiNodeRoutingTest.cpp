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
//        assert each (topicId, partition) resolves (via the agent routing table) to
//        its OWN leader endpoint — i.e. each partition's produce would be sent to its
//        leader session.  Task 1.4: per-worker ingressStreamId REPEATS across nodes
//        (each node reuses the same small stream ids), so the route table is keyed by
//        (topicId, partition) — NOT ingressStreamId — to avoid cross-node collision.
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

#include <filesystem>
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
    cfg.transport.dataDir         = std::filesystem::temp_directory_path().string() + "/rune-mn-routing-unused";  // never start()ed
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

    // 8 partitions; leaders spread across the 3 nodes.  Task 1.4: the per-worker
    // ingressStreamId REPEATS across nodes — every node reuses the SAME small stream
    // ids (here streams 2 and 3).  This is the exact collision that broke the old
    // stream-keyed routing (last-write-wins mapped stream2/3 to one node each), so we
    // model it here to prove (topicId, partition) keying resolves every partition
    // correctly despite the shared stream ids.
    // partition -> (leaderNode, ingressStreamId)
    struct P { int32_t part; int32_t leader; int32_t stream; };
    std::vector<P> layout = {
        {0, 0, 2}, {1, 1, 2}, {2, 2, 2}, {3, 0, 3},
        {4, 1, 3}, {5, 2, 3}, {6, 0, 2}, {7, 1, 2},
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

    // Mirror Client::onMetadataResponse's route build — keyed by (topicId, partition).
    for (const auto& p : layout) {
        const std::string chan = leaderChannelFor(cache, kTopic, p.part);
        ASSERT_FALSE(chan.empty()) << "partition " << p.part << " has no leader endpoint";
        agent.setRoute(kTopic, p.part, chan);
    }

    // Assert each partition routes to ITS leader endpoint, and that the three leader
    // groups map to three DISTINCT endpoints.  Crucially: partitions that SHARE a
    // stream id (e.g. part0 & part1 both use stream 2) still resolve to DIFFERENT
    // leaders — proving no cross-node stream-id collision.
    const std::string epA = "aeron:udp?endpoint=hostA:9000";
    const std::string epB = "aeron:udp?endpoint=hostB:9001";
    const std::string epC = "aeron:udp?endpoint=hostC:9002";

    EXPECT_EQ(agent.routeEndpointFor(kTopic, 0), epA);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 3), epA);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 6), epA);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 1), epB);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 4), epB);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 7), epB);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 2), epC);
    EXPECT_EQ(agent.routeEndpointFor(kTopic, 5), epC);

    // No cross-node collision: part0 (stream 2 → nodeA) and part1 (stream 2 → nodeB)
    // share a stream id but resolve to different leaders.
    EXPECT_NE(agent.routeEndpointFor(kTopic, 0), agent.routeEndpointFor(kTopic, 1));
    EXPECT_NE(agent.routeEndpointFor(kTopic, 3), agent.routeEndpointFor(kTopic, 4));

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
        agent.setRoute(kTopic, p, chan);
    }

    // Empty route table → the produce send lambda takes the default offer path.
    for (int32_t p = 0; p < 8; ++p) {
        EXPECT_TRUE(agent.routeEndpointFor(kTopic, p).empty())
            << "single-node partition " << p << " must have no per-node route";
    }
    // Only the seed session exists.
    EXPECT_EQ(agent.sessionCount(), 1u);
}

// A worker data channel distinct from the session (node) endpoint keeps the route
// even when the session endpoint IS the seed (single-node shard broker).
TEST(MultiNodeRouting, SeparateDataChannelKeepsSessionOnSeed) {
    const std::string seed = "aeron:udp?endpoint=seed:1";
    rca::ClientAgent agent(makeAgentConfig(seed));
    EXPECT_TRUE(agent.setRoute(5, 0, seed, "aeron:udp?endpoint=seed:40500"));
    EXPECT_EQ(agent.routeEndpointFor(5, 0), seed);
    EXPECT_EQ(agent.dataChannelFor(5, 0), "aeron:udp?endpoint=seed:40500");
    EXPECT_TRUE(agent.multiNodeRouting());
    EXPECT_EQ(agent.sessionCount(), 1u);
}

TEST(MultiNodeRouting, SetRouteReportsChanges) {
    const std::string seed = "aeron:udp?endpoint=seed:1";
    rca::ClientAgent agent(makeAgentConfig(seed));
    EXPECT_TRUE(agent.setRoute(5, 1, "aeron:udp?endpoint=hostA:9000", "aeron:udp?endpoint=hostA:40500"));
    EXPECT_FALSE(agent.setRoute(5, 1, "aeron:udp?endpoint=hostA:9000", "aeron:udp?endpoint=hostA:40500"));
    EXPECT_TRUE(agent.setRoute(5, 1, "aeron:udp?endpoint=hostA:9000", "aeron:udp?endpoint=hostA:40501"));
    EXPECT_TRUE(agent.setRoute(5, 1, seed, seed));   // back to the default path: route removed
    EXPECT_TRUE(agent.routeEndpointFor(5, 1).empty());
    EXPECT_FALSE(agent.setRoute(5, 1, seed, seed));
    EXPECT_FALSE(agent.multiNodeRouting());
}

// Regression: getenv("RUNE_MN_DEBUG") ran on every routed offer / session open.
// It is read once, at construction.
TEST(MultiNodeRouting, MnDebugIsReadOnceAtConstructionRegression) {
    ::unsetenv("RUNE_MN_DEBUG");
    rca::ClientAgent agent(makeAgentConfig("aeron:udp?endpoint=seed:1"));
    ::setenv("RUNE_MN_DEBUG", "1", 1);
    testing::internal::CaptureStderr();
    agent.sessionForEndpoint("aeron:udp?endpoint=hostZ:9");
    const std::string err = testing::internal::GetCapturedStderr();
    ::unsetenv("RUNE_MN_DEBUG");
    EXPECT_EQ(err.find("MNDBG"), std::string::npos);
}

// ===========================================================================
// C1 (fix round 1a): the session a produce rides and the session its WorkerBind rides are ONE resolution.
// ===========================================================================

TEST(ClientAgentRegression, SeedFailedOverOntoNodeWithLeaderSessionBindsTheProducingSessionRegression) {
    const std::string kSeed = "aeron:udp?endpoint=seed:1";
    const std::string kB    = "aeron:udp?endpoint=hostB:9001";
    rca::ClientAgent agent(makeAgentConfig(kSeed));

    // A leader session for B (index 1), then the seed fails over ONTO B (its physical ingress is now B).
    const uint32_t bIdx = agent.sessionIndexForTest(kB);
    ASSERT_EQ(bIdx, 1u);
    agent.setSeedPhysicalIngressForTest(kB);

    // The produce path (sessionIndexFor_) and the bind path (findSessionIndex_) must agree: B's own session.
    EXPECT_EQ(agent.sessionIndexForTest(kB), bIdx);
    EXPECT_EQ(agent.findSessionIndexForTest(kB), agent.sessionIndexForTest(kB));
    EXPECT_EQ(agent.sessionCount(), 2u);   // nothing new opened

    // The bind entry for B's worker resolves to that same session.
    agent.addWorkerBindEntryForTest(kB);
    EXPECT_EQ(agent.workerBindSessionIndexForTest(0), bIdx);

    // The seed's old bootstrap node (no session of its own, seed not home): both paths say "none yet / open".
    EXPECT_EQ(agent.findSessionIndexForTest(kSeed), 0xFFFFFFFFu);
    // A node nobody has a session for, and the empty endpoint.
    EXPECT_EQ(agent.findSessionIndexForTest("aeron:udp?endpoint=hostZ:9"), 0xFFFFFFFFu);
    EXPECT_EQ(agent.findSessionIndexForTest(""), 0u);
}

// ===========================================================================
// I6 (fix round 1a): a session opened PLAIN (port unknown) is upgraded when the port is learnt later.
// ===========================================================================

TEST(ClientAgentRegression, LearntPortUpgradesAPlainNodeSessionRegression) {
    const std::string kB = "aeron:udp?endpoint=hostB:9001";
    rca::ClientAgent agent(makeAgentConfig("aeron:udp?endpoint=seed:1"));

    const uint32_t bIdx = agent.sessionIndexForTest(kB);   // opened before any port is known: plain
    ASSERT_EQ(bIdx, 1u);
    EXPECT_EQ(agent.sessionObjForTest(bIdx).responsePort, 0);
    EXPECT_EQ(agent.sessionObjForTest(bIdx).ctlPub, rune::client::transport::AeronTransport::kNoPub);
    // The session's first connect offer opened its PLAIN publication slot: it exists and is not response-paired.
    const auto plain = agent.testTransport().findPublicationForTest(kB, 1);
    ASSERT_NE(plain, rune::client::transport::AeronTransport::kNoPub) << "no plain publication slot yet: the test would be vacuous";
    ASSERT_FALSE(agent.publicationResponsePairedForTest(plain));
    const int64_t corrBefore = agent.sessionMachineForTest(bIdx).correlationId();

    agent.setSessionResponsePort(kB, 41001);
    // Applied on the next duty pass (never re-entrantly), exactly once.
    EXPECT_EQ(agent.serviceResponseUpgradesForTest(1'000'000), 1);
    EXPECT_EQ(agent.serviceResponseUpgradesForTest(2'000'000), 0);

    EXPECT_EQ(agent.sessionObjForTest(bIdx).responsePort, 41001);
    EXPECT_NE(agent.sessionObjForTest(bIdx).ctlPub, rune::client::transport::AeronTransport::kNoPub)
        << "the control publication is now response-correlated";
    EXPECT_TRUE(agent.publicationResponsePairedForTest(agent.sessionObjForTest(bIdx).ctlPub))
        << "the already-open PLAIN publication is converted, not kept";
    EXPECT_EQ(agent.sessionObjForTest(bIdx).ctlPub, plain) << "the same slot was converted in place";
    EXPECT_NE(agent.sessionMachineForTest(bIdx).correlationId(), corrBefore) << "a fresh connect episode";
    EXPECT_EQ(agent.sessionMachineForTest(bIdx).state(), rune::client::session::SessionState::Connecting);

    // A session that already has a port, the seed and unknown endpoints are untouched.
    agent.setSessionResponsePort(kB, 41001);
    EXPECT_EQ(agent.serviceResponseUpgradesForTest(3'000'000), 0);
    agent.setSessionResponsePort("aeron:udp?endpoint=seed:1", 41002);
    EXPECT_EQ(agent.serviceResponseUpgradesForTest(4'000'000), 0);
}
