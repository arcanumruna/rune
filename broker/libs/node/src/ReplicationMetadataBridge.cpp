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
        // tryEnqueueOp is lock-free SPSC; back-pressure is acceptable on
        // role-change (rare event, retried next cycle if ring is full).
        (void)workerPool_->tryEnqueueOp(op.topicId, op.partition, std::move(op));
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
        (void)workerPool_->tryEnqueueOp(op.topicId, op.partition, std::move(op));
    } else {
        // Internal partition (topicId < 0) or no worker pool: direct call.
        replica->becomeFollower(epoch, leaderId);
    }
}

} // namespace rune::node
