// ReplicationMetadataBridge.cpp — non-inline implementation for routing methods.
//
// routeBecomeLeader_ / routeBecomeFollower_ live here (not in the .h) to avoid
// pulling WorkerPool.h (and transitively NodeConfig.h / rune::node::TopicSpec)
// into the bridge header, which would cause ambiguity with rune::metadata::TopicSpec
// in test files that use both namespaces.

#include <rune/node/ReplicationMetadataBridge.h>

#include <algorithm>
#include <cassert>
#include <rune/node/WorkerPool.h>
#include <rune/node/StorageGate.h>

#include <spdlog/spdlog.h>

namespace rune::node {

// ---------------------------------------------------------------------------
// routeBecomeLeader_
//
// For USER partitions (topicId >= 0) when workerPool_ != nullptr:
//   Enqueue a BecomeLeader op to the owning worker's SPSC ingress so the
//   role change executes on the worker thread (shared-nothing E2 fix).
//
// For INTERNAL partitions (topicId < 0) OR when workerPool_ == nullptr:
//   Call becomeLeader() directly on the calling (control) thread.
//
//   Internal partitions stay on the control thread because MetadataService,
//   GroupCoordinationService, and TransactionCoordinationService all run on
//   the control thread and read the internal replica's role() / drive its
//   doWork — routing their role changes to a worker would produce a second
//   control-thread vs worker-thread data race (TSan-confirmed).
// ---------------------------------------------------------------------------
void ReplicationMetadataBridge::routeBecomeLeader_(
    rune::replication::PartitionReplica*    replica,
    rune::replication::Epoch                epoch,
    std::vector<rune::replication::NodeId>  isr,
    int32_t                                 partitionEpoch) noexcept
{
    recordIntendedRole_(replica, ReplicaRegistry::IntendedRole::Leader);
    if (workerPool_ && replica->topicId() >= 0) {
        WorkerOp op;
        op.type      = WorkerOp::Type::BecomeLeader;
        op.topicId   = replica->topicId();
        op.partition = static_cast<int32_t>(replica->partitionId());
        op.replica   = replica;
        op.roleEpoch = epoch;
        op.isr       = std::move(isr);
        op.rolePartitionEpoch = partitionEpoch;   // MEDIUM-1: AlterIsr CAS version
        // Never dropped: retried every cycle until the owning ring accepts it.
        enqueueRoleOp_(std::move(op), /*fence=*/false);
    } else {
        // Internal partition (topicId < 0) or no worker pool: direct call.
        // Both the internal replica's doWork AND MetadataService/GCS/TCS run
        // on the control thread, so this is single-threaded — no race.
        replica->becomeLeader(epoch, std::move(isr), partitionEpoch);
    }
}

// ---------------------------------------------------------------------------
// routeBecomeFollower_
//
// Same topology as routeBecomeLeader_: USER partitions through the worker,
// INTERNAL partitions direct (control thread).
// ---------------------------------------------------------------------------
void ReplicationMetadataBridge::routeBecomeFollower_(
    rune::replication::PartitionReplica*  replica,
    rune::replication::Epoch              epoch,
    rune::replication::NodeId             leaderId,
    bool                                  fence) noexcept
{
    recordIntendedRole_(replica, ReplicaRegistry::IntendedRole::Follower);
    if (workerPool_ && replica->topicId() >= 0) {
        WorkerOp op;
        op.type         = WorkerOp::Type::BecomeFollower;
        op.topicId      = replica->topicId();
        op.partition    = static_cast<int32_t>(replica->partitionId());
        op.replica      = replica;
        op.roleEpoch    = epoch;
        op.roleLeaderId = leaderId;
        enqueueRoleOp_(std::move(op), fence);
    } else {
        // Internal partition (topicId < 0) or no worker pool: direct call.
        replica->becomeFollower(epoch, leaderId);
    }
}

// F15 §3.1: the control-owned intended-role HINT the gateway consults for a fast
// NotLeader (the owning worker stays the authority).  Keyed by the registry
// entry (per incarnation); identity is taken from the registry, never read
// through the replica pointer.
void ReplicationMetadataBridge::recordIntendedRole_(rune::replication::PartitionReplica* replica,
                                                    ReplicaRegistry::IntendedRole role) noexcept
{
    const int32_t t = replica->topicId();
    const int32_t p = static_cast<int32_t>(replica->partitionId());
    const auto h = registry_.lookupHandle(t, p);
    if (h && h.replica == replica) registry_.setIntendedRole(t, p, h.incarnation, role);
}

// M1: the bridge chain is noexcept.  WorkerPool::tryEnqueueOp can throw
// std::logic_error (the F7 ownership invariant, from its inline reconcile).
// Catch it HERE and hand it to NodeRuntimeAgent (takeFatal) → halt (exit 70).
bool ReplicationMetadataBridge::tryRoute_(WorkerOp op) noexcept
{
    try {
        const int32_t t = op.topicId, p = op.partition;
        return workerPool_->tryEnqueueOp(t, p, std::move(op));
    } catch (const std::exception& e) {
        if (!fatalPending_) { fatalPending_ = true; fatalWhat_ = e.what(); }
    } catch (...) {
        if (!fatalPending_) { fatalPending_ = true; fatalWhat_ = "unknown exception routing a role op"; }
    }
    return false;   // keep it pending: the broker is halting
}

void ReplicationMetadataBridge::enqueueRoleOp_(WorkerOp op, bool fence) noexcept
{
    // Identity from the registry entry (never read through the pointer).
    const auto h = registry_.lookupHandle(op.topicId, op.partition);
    if (!h) return;   // not registered: nothing to drive (callers resolve from the registry)
    const auto w = static_cast<std::size_t>(workerPool_->ownerWorkerId(op.topicId, op.partition));
    if (pendingRoleOps_.size() <= w) pendingRoleOps_.resize(static_cast<std::size_t>(workerPool_->workerCount()));
    auto& q = pendingRoleOps_[w];
    // Coalesce: a pending op for the same replica incarnation is superseded in
    // place (latest absolute role wins; its queue position keeps per-replica
    // order relative to nothing else that matters — other replicas are
    // independent).
    for (auto& p : q) {
        if (p.op.topicId == op.topicId && p.op.partition == op.partition && p.incarnation == h.incarnation) {
            // Round 5: only a role op at the same or a newer epoch may supersede
            // (role changes are epoch-monotonic per partition).
            assert(op.roleEpoch >= p.op.roleEpoch && "role-op coalescing must never go back in epoch");
            p.op    = std::move(op);
            p.fence = fence;
            ++roleOpsCoalesced_;
            return;
        }
    }
    if (q.empty() && !leaderHeld_(op)) {
        WorkerOp attempt = op;   // copy: tryEnqueueOp consumes it
        if (tryRoute_(std::move(attempt))) return;
    }
    q.push_back(PendingRoleOp{std::move(op), h.incarnation, fence});
    if (++pendingRoleOpsTotal_ >= pendingWarnAt_) {
        spdlog::warn("ReplicationMetadataBridge: {} role op(s) waiting for worker ring space "
                     "(worker {} queue={}) — back-pressured, retried every cycle",
                     pendingRoleOpsTotal_, w, q.size());
        pendingWarnAt_ *= 2;   // rate limit: once per doubling
    }
}

// ---------------------------------------------------------------------------
// flushPendingRoleOps_ — deliver each worker's queue in order until its ring
// refuses (time-independent: one attempt per cycle, no timers).
//
// Stale entries (replica removed or re-registered since the op was queued)
// are dropped WITHOUT dereferencing anything.  For a REMOVAL FENCE this is
// provably moot, not a loss:
//   (1) the fence can only still be pending once registry_.remove() ran —
//       applyDeleteForKey routes it and removes in the same call;
//   (2) after removal any routed op for that (topic, partition) is stamped
//       stale by WorkerPool::tryEnqueueOp and refused by the worker — so a
//       late delivery could never execute anyway;
//   (3) the fence's job — the worker stops acting for the replica — is done by
//       the SetOwnership(gen) that drops it, which WorkerPool delivers to that
//       worker unconditionally (retried every cycle until its ring accepts):
//       from then on the worker never drives, reads or acks for it, and the
//       replica is freed only after that worker acks;
//   (4) everything the worker executes for the replica before that SetOwnership
//       was routed while it was registered — exactly the ops a delivered fence
//       would have been FIFO-ordered behind.
// So the observable outcome equals "fence delivered", and the drop is counted.
// ---------------------------------------------------------------------------
int ReplicationMetadataBridge::flushPendingRoleOps_() noexcept
{
    if (pendingRoleOpsTotal_ == 0 || !workerPool_) return 0;
    int delivered = 0;
    for (auto& q : pendingRoleOps_) {
        for (auto it = q.begin(); it != q.end();) {
            PendingRoleOp& p = *it;
            if (registry_.resolve(p.op.topicId, p.op.partition, p.incarnation) == nullptr) {
                if (p.fence) ++fencesMoot_; else ++staleRoleOpsDropped_;
                it = q.erase(it);
                --pendingRoleOpsTotal_;
                continue;
            }
            // Checked writes: a leader role waits for its typed topic's spec.  One entry per replica (coalesced), so the
            // entries behind it - other replicas - keep their own order.
            if (leaderHeld_(p.op)) { ++it; continue; }
            WorkerOp attempt = p.op;
            if (!tryRoute_(std::move(attempt))) break;   // this worker's ring is full; others proceed
            it = q.erase(it);
            --pendingRoleOpsTotal_;
            ++delivered;
        }
    }
    if (pendingRoleOpsTotal_ == 0) pendingWarnAt_ = 64;
    return delivered;
}

// ---------------------------------------------------------------------------
// Catalog history Task 6: the __catalog role relay (control thread → admin).
// ---------------------------------------------------------------------------
bool ReplicationMetadataBridge::gateClosed_() const noexcept
{
    return gate_ != nullptr && !gate_->open();
}

void ReplicationMetadataBridge::routeHoldLeadership_(rune::replication::PartitionReplica* replica,
                                                     int32_t leaderEpoch, int32_t partitionEpoch) noexcept
{
    recordIntendedRole_(replica, ReplicaRegistry::IntendedRole::Follower);   // it leads nothing
    if (workerPool_ && replica->topicId() >= 0) {
        WorkerOp op;
        op.type               = WorkerOp::Type::AlterIsrResponse;
        op.topicId            = replica->topicId();
        op.partition          = static_cast<int32_t>(replica->partitionId());
        op.replica            = replica;
        op.replicatedPayload.errorCode = rune::replication::kAlterIsrLocalHoldLeadership;
        op.roleEpoch          = leaderEpoch;
        op.rolePartitionEpoch = partitionEpoch;
        enqueueRoleOp_(std::move(op), /*fence=*/false);   // latest-wins per replica; retried while the ring is full
    } else {
        replica->holdLeadershipForStorage(leaderEpoch, partitionEpoch);
    }
}

void ReplicationMetadataBridge::postAdminLogRole_(int32_t topicId, bool leader,
                                                  const rune::metadata::PartitionAssignment* pa,
                                                  bool hold) noexcept
{
    const std::size_t ix = adminLogIndex_(topicId);
    const bool security = ix == 1;
    const char* log = security ? "__security" : "__catalog";
    const int32_t roleType = security ? kMsgSecurityRole : kMsgCatalogRole;
    const int32_t msgType = hold ? (security ? kMsgSecurityHoldLeadership : kMsgCatalogHoldLeadership) : roleType;
    AdminRoleMsg m;
    m.leader         = leader;
    m.epoch          = pa ? pa->leaderEpoch : 0;
    m.leaderId       = (pa && !hold) ? pa->leaderNode : rune::metadata::NO_LEADER;
    m.partitionEpoch = pa ? pa->partitionEpoch : -1;
    if (leader && pa) {
        // Bounded by construction: replicas = every broker <= kMaxReplicas (16).
        const std::size_t n = std::min<std::size_t>(pa->isr.size(), AdminRoleMsg::kMaxIsr);
        if (n < pa->isr.size()) [[unlikely]]
            spdlog::error("ReplicationMetadataBridge: {} ISR of {} members exceeds {} — "
                          "truncated (configuration beyond the replica bound)",
                          log, pa->isr.size(), AdminRoleMsg::kMaxIsr);
        m.isrCount = static_cast<int32_t>(n);
        for (std::size_t i = 0; i < n; ++i) m.isr[i] = pa->isr[i];
    }
    auto& pending = adminRolePending_[ix];
    if (pending) {          // keep order: the newer absolute role supersedes the held one (of the SAME log)
        pending = PendingAdminRole{m, msgType};
        return;
    }
    if (catalogRoleRing_ != nullptr && encodeAdminRole(*catalogRoleRing_, m, msgType)) {
        ++adminRolesPosted_[ix];
        return;
    }
    pending = PendingAdminRole{m, msgType};
    if (adminRolesHeld_[ix]++ % 1024 == 0)
        spdlog::warn("ReplicationMetadataBridge: {} role (leader={} epoch={} leaderId={}) held — "
                     "admin role ring {}; retried every cycle (held so far: {})",
                     log, m.leader, m.epoch, m.leaderId,
                     catalogRoleRing_ ? "full" : "not wired yet", adminRolesHeld_[ix]);
}

int ReplicationMetadataBridge::flushAdminLogRoles_() noexcept
{
    int flushed = 0;
    for (std::size_t ix = 0; ix < adminRolePending_.size(); ++ix) {
        auto& pending = adminRolePending_[ix];
        if (!pending || catalogRoleRing_ == nullptr) continue;
        if (!encodeAdminRole(*catalogRoleRing_, pending->m, pending->msgType)) continue;
        pending.reset();
        ++adminRolesPosted_[ix];
        ++flushed;
    }
    return flushed;
}

} // namespace rune::node
