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
//
// E1 rework: Worker now implements rune::consensus::Agent so it can be driven
// by an AgentRunner (with a configurable IdleStrategy) in E2. The Agent
// virtual dispatch occurs ONCE PER DUTY CYCLE (the Aeron model); per-record
// paths inside doWork remain concrete — no additional virtuals introduced.
//
// E2 prep: ownership is a lock-free published snapshot. The control thread
// publishes a new immutable OwnedSet via publishOwned() (release), and doWork
// reads a single acquire snapshot. Worker is non-movable (holds a shared_ptr
// that is accessed via atomic free functions).
//
// Note: The C++20 std::atomic<std::shared_ptr<T>> partial specialization
// (P0718R2) is not yet available on Apple libc++ as shipped with Xcode 17.
// We use std::atomic_load_explicit / std::atomic_store_explicit on a plain
// std::shared_ptr member instead — these provide identical acquire/release
// semantics and are the predecessor API that P0718R2 replaced.

#include <rune/consensus/ConsensusAgent.h>
#include <rune/replication/PartitionReplica.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rune::node {

class Worker : public rune::consensus::Agent {
public:
    // Non-owning pointer set published as an immutable snapshot.
    using OwnedSet = std::vector<rune::replication::PartitionReplica*>;

    explicit Worker(int32_t workerId) noexcept : workerId_(workerId) {}

    // Non-copyable and non-movable (std::atomic<shared_ptr> is not movable).
    Worker(const Worker&)            = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&)                 = delete;
    Worker& operator=(Worker&&)      = delete;

    // Publish a new immutable ownership snapshot (release). The control thread
    // calls this; doWork reads it with acquire. An empty set (not null) means
    // no replicas — doWork returns 0.
    void publishOwned(std::shared_ptr<const OwnedSet> set) noexcept {
        std::atomic_store_explicit(&owned_, std::move(set), std::memory_order_release);
    }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }

    [[nodiscard]] std::size_t ownedCount() const noexcept {
        auto snap = std::atomic_load_explicit(&owned_, std::memory_order_acquire);
        return snap ? snap->size() : 0;
    }

    [[nodiscard]] bool owns(const rune::replication::PartitionReplica* r) const noexcept {
        auto snap = std::atomic_load_explicit(&owned_, std::memory_order_acquire);
        return snap && std::find(snap->begin(), snap->end(), r) != snap->end();
    }

    // -----------------------------------------------------------------------
    // rune::consensus::Agent interface
    // -----------------------------------------------------------------------

    // Run the per-replica duty steps for owned replicas only. Mirrors
    // NodeRuntimeAgent::doWork steps 5 + 6.5 exactly:
    //   Step 5:   replica->doWork(nowNs) for every owned replica.
    //   Step 6.5: drainCleanerSwaps() + drainRetentionDeletes() +
    //             drainStagingRenamesDone() for disk-backed replicas only.
    //             Memory-backed replicas must be skipped — replica->log()
    //             asserts isDiskBacked(), matching the NodeRuntimeAgent guard.
    // Virtual dispatch occurs once per duty cycle (AgentRunner call site);
    // the per-record paths inside remain fully concrete.
    // Ownership snapshot is loaded ONCE (acquire) at the top of the call.
    int doWork(int64_t nowNs) override {
        auto snap = std::atomic_load_explicit(&owned_, std::memory_order_acquire);
        if (!snap) return 0;

        int work = 0;

        // Step 5: per-replica ISR checks / follower fetch emission.
        for (auto* replica : *snap) {
            if (!replica) continue;
            work += replica->doWork(nowNs);
        }

        // Step 6.5: drain cleaner/retention/staging queues (disk-backed only).
        for (auto* replica : *snap) {
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
    // Lock-free ownership snapshot accessed via atomic free functions.
    // Control thread publishes (release); doWork reads (acquire).
    // Default-constructs to null (treated as empty ownership by doWork).
    // Worker is non-movable because moving a shared_ptr accessed via
    // atomic free functions without synchronisation is not safe.
    std::shared_ptr<const OwnedSet> owned_;
};

} // namespace rune::node
