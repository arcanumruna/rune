#pragma once
// GatewayReplicaView.h — F15 §3.1 (I1): what the CONTROL thread may know about a
// partition replica that a worker owns.
//
// A worker-owned PartitionReplica's live state (role_, epoch_, ISR, HW, log) is
// written by the owning worker only (shared-nothing).  The gateway runs on the
// control thread, so it must never read that state: a role() read there is a
// data race AND a check-then-act race (the role can change between the check and
// the worker's append).  The owning worker is the role authority (role gate in
// executeProduceWire_ / handleWorkerIngressFrame_ / appendControlBatch); the
// gateway only consults the control-owned intended-role HINT the bridge records
// when it routes a role op (ReplicaRegistry::Handle::intendedRole).
//
// This view is the gateway's handle on a replica: the registry identity, the
// routing pointer (stamped into WorkerOps, never dereferenced for live state on
// control), immutable config, the published (release/acquire) snapshots, and the
// hint.  It deliberately has NO role() — pinned by a static_assert below and in
// TxnRoleGateRegression — so a future gateway change cannot reintroduce the race.

#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>

#include <cstdint>

namespace rune::gateway {

class GatewayReplicaView {
public:
    using IntendedRole = rune::node::ReplicaRegistry::IntendedRole;

    GatewayReplicaView() noexcept = default;
    explicit GatewayReplicaView(const rune::node::ReplicaRegistry::Handle& h) noexcept
        : replica_(h.replica), id_(h.id), incarnation_(h.incarnation), intended_(h.intendedRole) {}

    explicit operator bool() const noexcept { return replica_ != nullptr; }

    /// Stamp the routing pointer into a WorkerOp (`op.replica`).  The worker
    /// re-resolves it from its own ownership table; the control thread never
    /// reads through it — so it is written, never returned (review m4: a getter
    /// would let `view.routingPtr()->role()` bypass the no-role() guard).
    template <typename Op>
    void stampRouting(Op& op) const noexcept { op.replica = replica_; }
    [[nodiscard]] uint32_t registryId()  const noexcept { return id_; }
    [[nodiscard]] uint32_t incarnation() const noexcept { return incarnation_; }
    /// The control plane's last ROUTED role for this incarnation (a hint).
    [[nodiscard]] IntendedRole intendedRole() const noexcept { return intended_; }

    /// Immutable partition config (set at construction, never written after).
    [[nodiscard]] uint8_t durabilityTier() const noexcept {
        return replica_ ? replica_->durabilityTier() : uint8_t{1};
    }
    /// Published snapshots (acquire loads of the owner's release stores).
    [[nodiscard]] rune::replication::Offset gatewayHighWatermark() const noexcept {
        return replica_->gatewayHighWatermark();
    }
    [[nodiscard]] int gatewayIsrSize() const noexcept { return replica_->gatewayIsrSize(); }

    /// ONLY for the inline append path, where the control thread IS the owner
    /// (no worker pool, or a gateway-owned internal partition in shard mode):
    /// the full replica, live state included.  Never call it for a partition a
    /// worker owns (routesToWorker_).  The one remaining raw accessor: its sole
    /// caller is the inline branch of ClientGateway::handleProduce (grep-pinned
    /// by review; a type-level restriction would need a split view type).
    [[nodiscard]] rune::replication::PartitionReplica* controlOwned() const noexcept { return replica_; }

private:
    rune::replication::PartitionReplica* replica_ = nullptr;
    uint32_t     id_          = 0;
    uint32_t     incarnation_ = 0;
    IntendedRole intended_    = IntendedRole::Unknown;
};

// The structural guard (F15 §8 S2): the gateway's replica handle cannot read the
// worker-owned role.  (`requires (const GatewayReplicaView& v) { v.role(); }`,
// spelled through a concept: a non-dependent requires-expression with an
// invalid requirement is a hard error rather than `false`.)
template <typename V>
concept ExposesRole = requires(const V& v) { v.role(); };
static_assert(!ExposesRole<GatewayReplicaView>,
              "GatewayReplicaView must not expose role(): the owning worker is the role authority");
static_assert(ExposesRole<rune::replication::PartitionReplica>, "the concept detects role()");

} // namespace rune::gateway
