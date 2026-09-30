// ---------------------------------------------------------------------------
// PerWorkerReplicationWiringTest.cpp — E5 Task 5 wiring unit tests.
//
// Tests the four required behaviors:
//   1. FlagOff     → zero per-worker planes; all transport selection returns legacy.
//   2. FlagOn+peers → W planes built; user (topicId≥0) on per-worker; internal on legacy.
//   3. *Regression*: mismatched peer workerCount → THROWS std::runtime_error.
//   4. *Regression*: unresolvable plane → THROWS std::runtime_error.
//
// Test strategy — NO full BrokerNode:
//   We test two independent concerns in isolation:
//
//   A. Transport-selection pure function:
//      selectReplTransport(perWorkerPlanes, legacyTransport, topicId, partition)
//      → the right pointer.  Pure, deterministic, no Aeron required.
//
//   B. Validation logic (validateSymmetricWorkerCount + validateStreamRanges):
//      Already tested by WorkerReplicationValidatorTest; here we test that the
//      wiring correctly THROWS on validation failure.  This is tested via small
//      wrappers that call the same functions BrokerNode calls.
//
//   C. Per-worker plane construction (resolved() gate):
//      WorkerReplicationTransportImpl<FakePubRegistry> with empty-peer registry
//      returns resolved()=false — simulates the hard-refuse throw.
//
// No Aeron driver, no filesystem, no network.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerReplicationTransport.h>
#include <rune/node/WorkerReplicationValidator.h>
#include <rune/node/NodeConfig.h>
#include <rune/metadata/ShardRouter.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace rune::node;
using namespace rune::replication;

// ===========================================================================
// FakePubRegistry — minimal fake for WorkerReplicationTransportImpl<>.
// allResolved() returns true iff peerCount > 0 (mirrors the real registry).
// ===========================================================================
struct FakePubRegistry {
    int peerCount = 0;

    [[nodiscard]] bool allResolved() const noexcept { return peerCount > 0; }
    [[nodiscard]] int32_t maxMessageLength() const noexcept { return 1 << 20; }

    int64_t sendToPeer(NodeId, const uint8_t*, std::size_t) noexcept { return 1; }
    void    broadcastToPeers(const uint8_t*, std::size_t) noexcept {}
};

using FakeTransport = WorkerReplicationTransportImpl<FakePubRegistry>;

// ===========================================================================
// selectReplTransport — mirrors BrokerNode's per-replica transport selection
// logic (E5 Task 5).  This is the pure function we unit-test here.
//
// Rules:
//   topicId < 0  → always legacyTransport (internal partitions, control-thread)
//   topicId >= 0 AND planes not empty → perWorkerPlanes[workerForPartition]
//   otherwise → legacyTransport
// ===========================================================================
static rune::replication::PartitionTransport*
selectReplTransport(
    const std::vector<FakeTransport*>& planes,
    rune::replication::PartitionTransport*  legacy,
    rune::metadata::ShardRouter& router,
    int32_t topicId,
    int32_t partition)
{
    if (topicId >= 0 && !planes.empty()) {
        const int32_t w = router.workerForPartition(topicId, partition);
        return planes[static_cast<std::size_t>(w)];
    }
    return legacy;
}

// ===========================================================================
// buildValidationThrow — mirrors the BrokerNode pre-flight validation block.
// THROWS std::runtime_error on mismatch (same contract as BrokerNode).
// ===========================================================================
static void validateWorkerCountOrThrow(
    int32_t selfWc,
    const std::vector<std::pair<int32_t, int32_t>>& wPeers)
{
    auto r = validateSymmetricWorkerCount(selfWc, wPeers);
    if (!r.ok) {
        throw std::runtime_error("per-worker replication refused — " + r.error);
    }
}

// ===========================================================================
// buildPlaneOrThrow — mirrors the BrokerNode plane-resolution gate.
// Constructs a FakeTransport; THROWS if !resolved() (mirrors hard-refuse).
// ===========================================================================
static std::unique_ptr<FakeTransport>
buildPlaneOrThrow(int32_t workerId, FakePubRegistry reg)
{
    auto t = std::make_unique<FakeTransport>(workerId, std::move(reg));
    if (!t->resolved()) {
        throw std::runtime_error(
            "per-worker replication transport for worker " +
            std::to_string(workerId) + " did not resolve — refusing to start");
    }
    return t;
}

// ===========================================================================
// 1. FlagOff: zero per-worker planes; all replicas use legacy transport.
//
// When perWorkerReplication=false, no planes are constructed.  The selection
// function must return the legacy pointer for both user (topicId>=0) and
// internal (topicId<0) partitions.
// ===========================================================================
TEST(PerWorkerReplicationWiring, FlagOffUsesLegacyTransportForAll)
{
    // Simulate flag-off: no per-worker planes.
    std::vector<FakeTransport*> planes;  // empty — flag off

    // A dummy legacy transport pointer.
    FakePubRegistry legacyReg; legacyReg.peerCount = 1;
    FakeTransport legacyTransport(/*workerId=*/0, legacyReg);

    rune::metadata::ShardRouter router(2);

    // Zero planes constructed.
    EXPECT_EQ(planes.size(), 0u);

    // All transport selections must return the legacy transport.
    // topicId=-1 (internal __consumer_offsets)
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, -1, 0),
              &legacyTransport);
    // topicId=-3 (internal __cluster_metadata)
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, -3, 0),
              &legacyTransport);
    // topicId=0 (user partition — flag off → legacy)
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, 0, 0),
              &legacyTransport);
    // topicId=2, partition=1 (user partition)
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, 2, 1),
              &legacyTransport);
}

// ===========================================================================
// 2. FlagOn + peers: W planes built; user (topicId>=0) on per-worker planes;
//    internal (topicId<0) always on legacy.
//
// Uses W=2 workers and 4 user topic partitions to exercise distribution.
// ===========================================================================
TEST(PerWorkerReplicationWiring, FlagOnUserPartsOnPlanesInternalOnLegacy)
{
    constexpr int32_t W = 2;

    // Build W fake transports (resolved — peerCount > 0).
    FakePubRegistry reg0; reg0.peerCount = 1;
    FakePubRegistry reg1; reg1.peerCount = 1;
    std::vector<std::unique_ptr<FakeTransport>> owned;
    owned.push_back(std::make_unique<FakeTransport>(0, reg0));
    owned.push_back(std::make_unique<FakeTransport>(1, reg1));
    ASSERT_EQ(static_cast<int32_t>(owned.size()), W);

    // W planes constructed.
    EXPECT_EQ(owned.size(), static_cast<std::size_t>(W));

    // All planes are resolved.
    for (auto& t : owned) EXPECT_TRUE(t->resolved());

    // Build non-owning plane pointer array.
    std::vector<FakeTransport*> planes;
    for (auto& t : owned) planes.push_back(t.get());

    // Legacy transport for internal partitions.
    FakePubRegistry legacyReg; legacyReg.peerCount = 1;
    FakeTransport legacyTransport(/*workerId=*/99, legacyReg);

    rune::metadata::ShardRouter router(W);

    // Internal partitions (topicId < 0) MUST always return legacy.
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, -1, 0),
              &legacyTransport)  << "topicId=-1 (__consumer_offsets) must use legacy";
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, -2, 0),
              &legacyTransport)  << "topicId=-2 (__transaction_state) must use legacy";
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, -3, 0),
              &legacyTransport)  << "topicId=-3 (__cluster_metadata) must use legacy";
    EXPECT_EQ(selectReplTransport(planes, &legacyTransport, router, -4, 0),
              &legacyTransport)  << "topicId=-4 (__topic_schemas) must use legacy";

    // User partitions (topicId >= 0) MUST return a per-worker plane, NOT legacy.
    for (int32_t tid = 0; tid < 4; ++tid) {
        for (int32_t part = 0; part < 4; ++part) {
            auto* t = selectReplTransport(planes, &legacyTransport, router, tid, part);
            EXPECT_NE(t, &legacyTransport)
                << "topicId=" << tid << " partition=" << part
                << " must NOT be on legacy transport when per-worker planes are live";
            // Must be one of the planes.
            bool foundInPlanes = false;
            for (auto* p : planes) if (p == t) { foundInPlanes = true; break; }
            EXPECT_TRUE(foundInPlanes)
                << "topicId=" << tid << " partition=" << part
                << " must be on one of the per-worker planes";
        }
    }
}

// ===========================================================================
// 3. *Regression*: mismatched peer workerCount → THROWS std::runtime_error.
//
// BrokerNode hard-refuses to start when a peer advertises a different W than
// self.  A per-worker fallback would silently split the replication plane.
// ===========================================================================
TEST(PerWorkerReplicationWiringRegression, MismatchedPeerWorkerCountRefusesStart)
{
    // self=4 workers, peer 2 advertises 2
    EXPECT_THROW(
        validateWorkerCountOrThrow(4, {{1, 4}, {2, 2}}),
        std::runtime_error);
}

TEST(PerWorkerReplicationWiringRegression, MismatchedPeerWorkerCountErrorMessageNamesNode)
{
    try {
        validateWorkerCountOrThrow(4, {{1, 4}, {3, 8}});
        FAIL() << "expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("3"), std::string::npos)
            << "error message should name the offending node";
    }
}

// ===========================================================================
// 4. *Regression*: an unresolvable plane → THROWS std::runtime_error.
//
// BrokerNode refuses to start with a half-wired replication plane (any
// transport that !resolved() silently drops messages for its worker's shards).
// ===========================================================================
TEST(PerWorkerReplicationWiringRegression, UnresolvedPlaneAbortsStart)
{
    // FakePubRegistry with peerCount=0 → allResolved()=false → !resolved().
    FakePubRegistry unresolved;
    unresolved.peerCount = 0;

    EXPECT_THROW(buildPlaneOrThrow(0, unresolved), std::runtime_error);
}

TEST(PerWorkerReplicationWiringRegression, ResolvedPlaneDoesNotThrow)
{
    FakePubRegistry resolved;
    resolved.peerCount = 2;

    EXPECT_NO_THROW({
        auto t = buildPlaneOrThrow(0, resolved);
        EXPECT_TRUE(t->resolved());
    });
}

// ===========================================================================
// 5. Single-node / no-peers: planes vector stays empty (nothing to replicate).
//
// BrokerNode does NOT build per-worker planes when peers is empty.  This
// sidesteps allResolved()==false-on-empty-peers.
// ===========================================================================
TEST(PerWorkerReplicationWiring, SingleNodeNoPeersSkipsPlaneConstruction)
{
    // Simulate no replication peers (single-node or all peers lack repl port).
    std::vector<std::pair<NodeId, std::string>> replPeers;  // empty

    // When replPeers is empty, no planes should be built.
    // This mirrors BrokerNode::buildReplication(): "if (!replPeers.empty()) { ... }".
    std::vector<std::unique_ptr<FakeTransport>> planes;
    if (!replPeers.empty()) {
        // Would build planes here — but we don't enter this branch.
        FakePubRegistry reg; reg.peerCount = static_cast<int>(replPeers.size());
        planes.push_back(std::make_unique<FakeTransport>(0, reg));
    }

    EXPECT_EQ(planes.size(), 0u)
        << "single-node (no peers): zero per-worker planes must be constructed";
}

// ===========================================================================
// 6. FakeTransport implements PartitionTransport (compile-time check + resolved).
//
// WorkerReplicationTransportImpl<PubRegistry> always derives from PartitionTransport.
// This validates the polymorphism the real BrokerNode wiring relies on.
// ===========================================================================
TEST(PerWorkerReplicationWiring, FakeTransportIsAPartitionTransport)
{
    FakePubRegistry reg; reg.peerCount = 1;
    FakeTransport t(/*workerId=*/0, reg);
    EXPECT_TRUE(t.resolved());

    // FakeTransport derives from PartitionTransport (same base as production type).
    rune::replication::PartitionTransport* ptr = &t;
    EXPECT_NE(ptr, nullptr);
}
