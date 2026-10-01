// ReplicationMetadataBridge.cpp — non-inline implementation for routing methods.
//
// routeBecomeLeader_ / routeBecomeFollower_ live here (not in the .h) to avoid
// pulling WorkerPool.h (and transitively NodeConfig.h / rune::node::TopicSpec)
// into the bridge header, which would cause ambiguity with rune::metadata::TopicSpec
// in test files that use both namespaces.

#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/WorkerPool.h>

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
        enqueueRoleOp_(std::move(op));
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
    rune::replication::NodeId             leaderId) noexcept
{
    if (workerPool_ && replica->topicId() >= 0) {
        WorkerOp op;
        op.type         = WorkerOp::Type::BecomeFollower;
        op.topicId      = replica->topicId();
        op.partition    = static_cast<int32_t>(replica->partitionId());
        op.replica      = replica;
        op.roleEpoch    = epoch;
        op.roleLeaderId = leaderId;
        enqueueRoleOp_(std::move(op));
    } else {
        // Internal partition (topicId < 0) or no worker pool: direct call.
        replica->becomeFollower(epoch, leaderId);
    }
}

void ReplicationMetadataBridge::enqueueRoleOp_(WorkerOp op) noexcept
{
    const uint32_t inc = op.replica ? op.replica->incarnation() : 0;
    if (pendingRoleOps_.empty()) {
        WorkerOp attempt = op;   // copy: tryEnqueueOp consumes it
        if (workerPool_->tryEnqueueOp(attempt.topicId, attempt.partition, std::move(attempt))) return;
    }
    pendingRoleOps_.push_back(PendingRoleOp{std::move(op), inc});
}

int ReplicationMetadataBridge::flushPendingRoleOps_() noexcept
{
    if (pendingRoleOps_.empty() || !workerPool_) return 0;
    int delivered = 0;
    while (!pendingRoleOps_.empty()) {
        PendingRoleOp& p = pendingRoleOps_.front();
        // Still the same registered replica?  (lookup returns only live ones;
        // the stale pointer itself is never dereferenced.)
        auto* live = registry_.lookup(p.op.topicId, p.op.partition);
        if (live != p.op.replica || live->incarnation() != p.incarnation) {
            pendingRoleOps_.pop_front();   // replica removed / replaced: the op is moot
            continue;
        }
        WorkerOp attempt = p.op;
        if (!workerPool_->tryEnqueueOp(attempt.topicId, attempt.partition, std::move(attempt))) break;
        pendingRoleOps_.pop_front();
        ++delivered;
    }
    return delivered;
}

} // namespace rune::node
