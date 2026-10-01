// ReplicationMetadataBridge.cpp — non-inline implementation for routing methods.
//
// routeBecomeLeader_ / routeBecomeFollower_ live here (not in the .h) to avoid
// pulling WorkerPool.h (and transitively NodeConfig.h / rune::node::TopicSpec)
// into the bridge header, which would cause ambiguity with rune::metadata::TopicSpec
// in test files that use both namespaces.

#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/WorkerPool.h>

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
    std::vector<rune::replication::NodeId>  isr) noexcept
{
    if (workerPool_ && replica->topicId() >= 0) {
        WorkerOp op;
        op.type      = WorkerOp::Type::BecomeLeader;
        op.topicId   = replica->topicId();
        op.partition = static_cast<int32_t>(replica->partitionId());
        op.replica   = replica;
        op.roleEpoch = epoch;
        op.isr       = std::move(isr);
        // Never dropped: retried every cycle until the owning ring accepts it.
        enqueueRoleOp_(std::move(op), /*fence=*/false);
    } else {
        // Internal partition (topicId < 0) or no worker pool: direct call.
        // Both the internal replica's doWork AND MetadataService/GCS/TCS run
        // on the control thread, so this is single-threaded — no race.
        replica->becomeLeader(epoch, std::move(isr));
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
            p.op    = std::move(op);
            p.fence = fence;
            ++roleOpsCoalesced_;
            return;
        }
    }
    if (q.empty()) {
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
        while (!q.empty()) {
            PendingRoleOp& p = q.front();
            if (registry_.resolve(p.op.topicId, p.op.partition, p.incarnation) == nullptr) {
                if (p.fence) ++fencesMoot_; else ++staleRoleOpsDropped_;
                q.pop_front();
                --pendingRoleOpsTotal_;
                continue;
            }
            WorkerOp attempt = p.op;
            if (!tryRoute_(std::move(attempt))) break;   // this worker's ring is full; others proceed
            q.pop_front();
            --pendingRoleOpsTotal_;
            ++delivered;
        }
    }
    if (pendingRoleOpsTotal_ == 0) pendingWarnAt_ = 64;
    return delivered;
}

} // namespace rune::node
