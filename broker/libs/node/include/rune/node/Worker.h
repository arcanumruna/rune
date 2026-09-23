#pragma once
// Worker.h — E1: a Worker owns a disjoint set of shards' PartitionReplicas and
// runs, for ITS owned replicas only, the exact per-replica duty steps the single
// NodeRuntimeAgent runs today (replica->doWork + disk-log cleaner/retention drains).
//
// E1 non-goals: no thread of its own, no ingress, no io_uring. It is driven
// cooperatively by the single AgentRunner via WorkerPool::doWork.
//
// Shared-nothing: a Worker touches ONLY the PartitionReplicas it owns. The
// ReplicaRegistry still OWNS the PartitionReplica objects; Worker holds
// non-owning pointers. Single-writer: doWork runs on the duty-cycle thread only.

#include <rune/replication/PartitionReplica.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace rune::node {

class Worker {
public:
    explicit Worker(int32_t workerId) noexcept : workerId_(workerId) {}

    Worker(const Worker&)            = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&)                 = default;
    Worker& operator=(Worker&&)      = default;

    void ownReplica(rune::replication::PartitionReplica* r) noexcept {
        if (r) owned_.push_back(r);
    }

    void clearOwned() noexcept { owned_.clear(); }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }
    [[nodiscard]] std::size_t ownedCount() const noexcept { return owned_.size(); }

    [[nodiscard]] bool owns(const rune::replication::PartitionReplica* r) const noexcept {
        return std::find(owned_.begin(), owned_.end(), r) != owned_.end();
    }

    // Run the per-replica duty steps for owned replicas only. Mirrors
    // NodeRuntimeAgent::doWork steps 5 + 6.5 exactly:
    //   Step 5:   replica->doWork(nowNs) for every owned replica.
    //   Step 6.5: drainCleanerSwaps() + drainRetentionDeletes() +
    //             drainStagingRenamesDone() for disk-backed replicas only.
    //             Memory-backed replicas must be skipped — replica->log()
    //             asserts isDiskBacked(), matching the NodeRuntimeAgent guard.
    // NON-virtual: called via Worker& (concrete direct call, no vtable).
    int doWork(int64_t nowNs) {
        int work = 0;

        // Step 5: per-replica ISR checks / follower fetch emission.
        for (auto* replica : owned_) {
            if (!replica) continue;
            work += replica->doWork(nowNs);
        }

        // Step 6.5: drain cleaner/retention/staging queues (disk-backed only).
        for (auto* replica : owned_) {
            if (replica && replica->isDiskBacked()) {
                work += replica->log()->drainCleanerSwaps();
                work += replica->log()->drainRetentionDeletes();
                work += replica->log()->drainStagingRenamesDone();
            }
        }

        return work;
    }

private:
    int32_t workerId_;
    std::vector<rune::replication::PartitionReplica*> owned_;  // non-owning
};

} // namespace rune::node
