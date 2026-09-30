// ReplicaTransportKindTest.cpp — the replica classifies its transport ONCE at
// construction; the production per-worker transport takes the concrete
// (de-virtualised) arm, everything else the generic arm (spec §3.5a.1).
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>
#include <span>
#include <type_traits>
#include <vector>

using namespace rune::replication;

static_assert(std::is_final_v<rune::node::WorkerReplicationTransport>,
              "the per-worker transport must be final so calls through it are direct");

namespace {
// Minimal PubRegistry satisfying the concept (Aeron-free).
class MinimalFakeRegistry {
public:
    [[nodiscard]] bool    allResolved() const noexcept { return true; }
    [[nodiscard]] int32_t maxMessageLength() const noexcept { return 1 << 20; }
    [[nodiscard]] bool    hasPeer(NodeId) const noexcept { return true; }
    int64_t sendToPeer(NodeId, const uint8_t*, std::size_t) noexcept { ++sends; return 1; }
    void    broadcastToPeers(const uint8_t*, std::size_t) noexcept { ++sends; }
    int sends = 0;
};

PartitionConfig cfg1() { PartitionConfig c; c.selfId = 1; return c; }
} // namespace

TEST(ReplicaTransportKind, ProductionPerWorkerTransportSelectsConcreteArm) {
    rune::node::WorkerReplicationTransport t(nullptr, 0, "", 0, {}, 0);   // null aeron: inert
    EXPECT_EQ(t.kind(), TransportKind::PerWorker);
    rune::storage::InMemoryLog log(1 << 20, 1024, rune::storage::FullPolicy::DropOldest);
    PartitionReplica r(cfg1(), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    EXPECT_EQ(r.transportKindForTest(), TransportKind::PerWorker);
}

TEST(ReplicaTransportKind, FakeRegistryTransportStaysGeneric) {
    rune::node::WorkerReplicationTransportImpl<MinimalFakeRegistry> t(0, MinimalFakeRegistry{});
    EXPECT_EQ(t.kind(), TransportKind::Generic);
    rune::storage::InMemoryLog log(1 << 20, 1024, rune::storage::FullPolicy::DropOldest);
    PartitionReplica r(cfg1(), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    EXPECT_EQ(r.transportKindForTest(), TransportKind::Generic);
}

TEST(ReplicaTransportKind, InMemoryTransportIsGeneric) {
    InMemoryTransport t;
    rune::storage::InMemoryLog log(1 << 20, 1024, rune::storage::FullPolicy::DropOldest);
    PartitionReplica r(cfg1(), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    EXPECT_EQ(r.transportKindForTest(), TransportKind::Generic);
}
