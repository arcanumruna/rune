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
// E2 inc2b: genuinely lock-free ownership read.
//
// owned_ is a raw std::atomic<const OwnedSet*> (acquire/release).
// Lock-free guarantee: std::atomic<pointer> is lock-free on all supported
// platforms (x86_64, arm64).  The static_assert in Worker() verifies this at
// compile time.
//
// Lifetime / reclamation (single-writer control thread publishes; single reader
// per worker on its own AgentRunner thread):
//   WorkerPool retains a K=4 deep shared_ptr retain ring per worker.  On every
//   publishOwned() call the new shared_ptr is pushed into the ring (evicting the
//   oldest).  The raw pointer is stored with release ONLY AFTER the shared_ptr is
//   retained.  Ownership publishes happen at registry-generation boundaries
//   (seconds apart); the worker performs thousands of doWork iterations between
//   publishes, so any evicted set is long unreferenced before it is freed.
//
// E2 inc2a: Per-worker ingress + return SPSC rings.
//
//   ingress_: producer = control thread (gateway + transport); consumer = doWork.
//   return_:  producer = doWork; consumer = gateway (drains in gateway::doWork).
//
//   doWork order:
//     1. Drain ingress_ (execute ops, enqueue returns).
//     2. Run per-replica duty steps (Step 5 + 6.5 from E1).
//
//   The rings are LOCK-FREE SPSC (SpscRing<WorkerOp> / SpscRing<WorkerReturn>).
//   No mutex, no condvar. Back-pressure on tryEnqueue: the caller must retry
//   on the next duty cycle (never drop, never overwrite).
//
//   Buffer lifetime: Aeron fragment buffers (ApplyReplicated records span) are
//   copied into a worker-owned vector at enqueue time — correctness first.

#include <rune/consensus/ConsensusAgent.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/node/SpscRing.h>
#include <rune/node/WorkerOp.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

namespace rune::node {

class Worker : public rune::consensus::Agent {
public:
    // Non-owning pointer set published as an immutable snapshot.
    using OwnedSet = std::vector<rune::replication::PartitionReplica*>;

    // SPSC ring capacity — power of two.
    // 256 ops per ring: sufficient for bursts under single-thread cooperative.
    // When inc2b adds per-worker threads, this may be tuned up.
    static constexpr std::size_t kRingCapacity = 256;

    // ringCapacity: per-worker ingress/return SPSC ring slots — NodeConfig
    // configurable (workerRingCapacity). Default kRingCapacity for tests/back-compat.
    explicit Worker(int32_t workerId, std::size_t ringCapacity = kRingCapacity) noexcept
        : workerId_(workerId)
        , ingress_(ringCapacity)
        , ret_(ringCapacity)
    {
        // Verify the raw-pointer atomic is lock-free on this platform.
        // std::atomic<pointer> is lock-free on x86_64 and arm64 (the only
        // platforms we target).  If this fires, the ownership-read is not
        // genuinely lock-free and the design assumption must be revisited.
        static_assert(std::atomic<const OwnedSet*>::is_always_lock_free,
                      "std::atomic<const OwnedSet*> must be lock-free");
    }

    // Non-copyable and non-movable (std::atomic<OwnedSet*> + SpscRing are not
    // movable).
    Worker(const Worker&)            = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&)                 = delete;
    Worker& operator=(Worker&&)      = delete;

    // Publish a new immutable ownership snapshot (release) — raw pointer form.
    //
    // Called from WorkerPool::publishWithRetain() on the control thread.
    // WorkerPool retains the shared_ptr lifetime in its per-worker retain ring
    // BEFORE calling this, so the raw pointer remains valid for as long as
    // any worker might still be reading the previous value.
    //
    // An empty set (not null) means no replicas — doWork returns 0.
    void publishOwned(const OwnedSet* rawPtr) noexcept {
        owned_.store(rawPtr, std::memory_order_release);
    }

    // Convenience overload for tests / standalone use: retains the shared_ptr
    // in a local K=4 ring (same reclamation logic as WorkerPool's retain ring)
    // and then stores the raw pointer with release.
    //
    // Use this when driving Worker directly without a WorkerPool (e.g. unit tests).
    // Production code goes through WorkerPool::publishWithRetain which uses the
    // pool-level retain ring instead.
    void publishOwned(std::shared_ptr<const OwnedSet> set) noexcept {
        const OwnedSet* rawPtr = set.get();
        // Push into local retain ring (evicts oldest — safe, same reasoning as pool ring).
        localRetain_[localRetainHead_ % kLocalRetainDepth] = std::move(set);
        ++localRetainHead_;
        owned_.store(rawPtr, std::memory_order_release);
    }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }

    [[nodiscard]] std::size_t ownedCount() const noexcept {
        const OwnedSet* snap = owned_.load(std::memory_order_acquire);
        return snap ? snap->size() : 0UZ;
    }

    [[nodiscard]] bool owns(const rune::replication::PartitionReplica* r) const noexcept {
        const OwnedSet* snap = owned_.load(std::memory_order_acquire);
        return snap && std::find(snap->begin(), snap->end(), r) != snap->end();
    }

    // -----------------------------------------------------------------------
    // E2 inc2a: ingress ring accessor (producer = control thread).
    //
    // The gateway and transport call tryEnqueueOp() to route log work to this
    // worker.  The ring capacity is kRingCapacity; tryEnqueueOp returns false
    // when full (back-pressure — caller retries next cycle).
    // -----------------------------------------------------------------------
    [[nodiscard]] bool tryEnqueueOp(WorkerOp op) {
        return ingress_.tryEnqueue(std::move(op));
    }

    // -----------------------------------------------------------------------
    // E2 inc2a: return ring accessor (consumer = gateway, on control thread).
    //
    // The gateway calls drainReturn() to collect results from the previous cycle.
    // -----------------------------------------------------------------------
    template <typename Fn>
    std::size_t drainReturn(Fn&& fn) {
        return ret_.drain(std::forward<Fn>(fn));
    }

    // -----------------------------------------------------------------------
    // rune::consensus::Agent interface
    // -----------------------------------------------------------------------

    // Run the per-replica duty steps for owned replicas only. Mirrors
    // NodeRuntimeAgent::doWork steps 5 + 6.5 exactly:
    //   Step 0 (E2 inc2a): drain ingress ring — execute log ops, enqueue returns.
    //   Step 5:   replica->doWork(nowNs) for every owned replica.
    //   Step 6.5: drainCleanerSwaps() + drainRetentionDeletes() +
    //             drainStagingRenamesDone() for disk-backed replicas only.
    //             Memory-backed replicas must be skipped — replica->log()
    //             asserts isDiskBacked(), matching the NodeRuntimeAgent guard.
    // Virtual dispatch occurs once per duty cycle (AgentRunner call site);
    // the per-record paths inside remain fully concrete.
    // Ownership snapshot is loaded ONCE (acquire) at the top of the call.
    int doWork(int64_t nowNs) override {
        // Acquire the ownership snapshot once.  Raw pointer — genuinely lock-free
        // (no internal lock pool unlike the old atomic<shared_ptr> free functions).
        // Lifetime is guaranteed by WorkerPool's per-worker K-deep retain ring.
        const OwnedSet* snap = owned_.load(std::memory_order_acquire);

        int work = 0;

        // Step 0: drain ingress ring (E2 inc2a).
        work += static_cast<int>(drainIngress_());

        if (!snap) return work;

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
    // -----------------------------------------------------------------------
    // drainIngress_ — execute all queued ops and enqueue results to ret_.
    //
    // Called from doWork (the consumer side of ingress_).
    // Single-writer invariant: doWork runs on the duty-cycle thread only in
    // E2 inc2a (cooperative); in E2 inc2b it runs on the worker's own thread.
    // -----------------------------------------------------------------------
    std::size_t drainIngress_() {
        return ingress_.drain([this](WorkerOp&& op) {
            executeOp_(std::move(op));
        });
    }

    void executeOp_(WorkerOp op) {
        switch (op.type) {
        case WorkerOp::Type::Produce:
            executeProduce_(std::move(op));
            break;
        case WorkerOp::Type::PushRead:
            executePushRead_(std::move(op));
            break;
        case WorkerOp::Type::ApplyReplicated:
            executeApplyReplicated_(std::move(op));
            break;
        case WorkerOp::Type::BecomeLeader:
            executeBecomeLeader_(std::move(op));
            break;
        case WorkerOp::Type::BecomeFollower:
            executeBecomeFollower_(std::move(op));
            break;
        }
    }

    // -------------------------------------------------------------------
    // executeProduce_ — append the batch to the owning Log and update the
    // replica's tracked LEO/HW via onLeaderLogAppended().
    //
    // The gateway has already:
    //   1. Validated + restamped the batch with a PLACEHOLDER base offset.
    //      The actual base offset must be re-stamped here after calling
    //      log->logEndOffset() so the stamped offset is authoritative.
    //   2. Validated the CRC (CRC region is AFTER kBaseOffsetOffset, so
    //      re-stamping base does NOT invalidate the CRC).
    //
    // Returns via WorkerReturn::ProduceResult on the return ring.
    // -------------------------------------------------------------------
    void executeProduce_(WorkerOp op) {
        WorkerReturn ret;
        ret.type          = WorkerReturn::Type::ProduceResult;
        ret.sessionId     = op.sessionId;
        ret.correlationId = op.correlationId;
        ret.partition     = op.partition;
        ret.deferAck      = false;
        ret.replica        = op.replica;
        ret.producerId     = op.producerId;
        ret.producerEpoch  = op.producerEpoch;
        ret.baseSequence   = op.baseSequence;
        ret.recordCount    = op.recordCount;
        ret.topicId        = op.topicId;
        ret.batchAttrs     = op.batchAttrs;
        ret.deferAck       = op.deferAck;
        ret.enqueuedAtNs   = op.enqueuedAtNs;

        // Look up the Log for this (topicId, partition).
        // The Log pointer is stored in the replica (disk-backed) or obtained from
        // topicRegistry_. We get it from the op via the replica.
        //
        // Invariant: gateway only enqueues Produce ops for valid (topicId, partition)
        // pairs where a Log exists. We double-check here for safety.
        rune::storage::Log* log = getLog_(op);
        if (!log) {
            spdlog::error("Worker {}: executeProduce_ — no Log for topicId={} partition={}; "
                          "sending Internal error",
                          workerId_, op.topicId, op.partition);
            ret.errorCode = static_cast<int16_t>(6); // ErrorCode::Internal placeholder
            ret_.tryEnqueue(std::move(ret));
            return;
        }

        // Assign the actual base offset (authoritative — worker is the single writer).
        const int64_t base = log->logEndOffset();
        ret.baseOffset = base;
        ret.lastOffset = base + static_cast<int64_t>(op.lastOffsetDelta);

        // Re-stamp the base offset in the batch bytes (LE64 at kBaseOffsetOffset=0).
        // The CRC region starts at kAttributesOffset=21, AFTER the base-offset field,
        // so re-stamping is CRC-safe.
        if (op.recordBatchBytes.size() >= 8u) {
            uint64_t u = static_cast<uint64_t>(base);
            for (int i = 0; i < 8; ++i) {
                op.recordBatchBytes[static_cast<std::size_t>(i)] =
                    static_cast<uint8_t>(u >> (i * 8));
            }
        }

        // Append to the log (no-sync: page-cache write, group-commit later).
        const std::size_t batchLen = op.recordBatchBytes.size();
        try {
            log->appendNoSync(op.recordBatchBytes.data(), batchLen, ret.lastOffset);
        } catch (const std::exception& ex) {
            spdlog::error("Worker {}: appendNoSync threw: {}", workerId_, ex.what());
            ret.errorCode = static_cast<int16_t>(6); // Internal
            ret_.tryEnqueue(std::move(ret));
            return;
        } catch (...) {
            spdlog::error("Worker {}: appendNoSync threw unknown exception", workerId_);
            ret.errorCode = static_cast<int16_t>(6); // Internal
            ret_.tryEnqueue(std::move(ret));
            return;
        }

        // GR-4 Mode A seam: reconcile the replica's tracked LEO/HW.
        if (op.replica) {
            op.replica->onLeaderLogAppended();
        }

        // Success: no errorCode (0 = OK).
        ret.errorCode = 0;
        ret_.tryEnqueue(std::move(ret));
    }

    // -------------------------------------------------------------------
    // executePushRead_ — read a chunk from the log for a push subscription.
    //
    // Returns WorkerReturn::PushReadResult carrying the chunk + context.
    // The gateway iterates the chunk, encodes Records frames, and sends them.
    // -------------------------------------------------------------------
    void executePushRead_(WorkerOp op) {
        WorkerReturn ret;
        ret.type                 = WorkerReturn::Type::PushReadResult;
        ret.subscriptionId       = op.subscriptionId;
        ret.sessionId            = op.sessionId;  // gateway needs this to find the session
        ret.readTopicId          = op.topicId;
        ret.readPartition        = op.partition;
        ret.newNextOffset        = op.nextOffset; // will be advanced by gateway on success
        ret.hwBound              = op.hwBound;
        ret.isolation            = op.isolation;
        ret.isGroup              = op.isGroup;
        ret.groupCarrierReceived = op.groupCarrierReceived;

        rune::storage::Log* log = getLog_(op);
        if (!log) {
            // No log: return empty chunk (gateway will skip this subscription).
            ret_.tryEnqueue(std::move(ret));
            return;
        }

        // Only read if there are records available (within the HW-bound).
        if (op.nextOffset < op.hwBound) {
            try {
                // kMaxChunkBytes constant mirrored here; must stay in sync with
                // ClientGateway.  Gateway sets hwBound before enqueuing.
                static constexpr int32_t kMaxChunkBytes =
                    static_cast<int32_t>(512 * 1024); // 512 KiB
                ret.chunk = log->read(op.nextOffset, kMaxChunkBytes);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: log->read threw for topicId={} partition={}: {}",
                             workerId_, op.topicId, op.partition, ex.what());
                // Return empty chunk — gateway will skip + retry next cycle.
            } catch (...) {
                spdlog::warn("Worker {}: log->read threw unknown exception", workerId_);
            }
        }

        ret_.tryEnqueue(std::move(ret));
    }

    // -------------------------------------------------------------------
    // executeApplyReplicated_ — apply a replicated FetchResponse to the
    // owning follower replica.  Fire-and-forget (no WorkerReturn needed).
    // -------------------------------------------------------------------
    void executeApplyReplicated_(WorkerOp op) {
        if (!op.replica) {
            spdlog::warn("Worker {}: executeApplyReplicated_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId, op.partition);
            return;
        }

        // Reconstruct a FetchResponseMsg whose records span points into the
        // copied buffer stored in op.replicatedPayload.records.
        const auto& rp = op.replicatedPayload;
        rune::replication::FetchResponseMsg msg;
        msg.leaderEpoch   = rp.leaderEpoch;
        msg.partition     = rp.partition;
        msg.highWatermark = rp.highWatermark;
        msg.logStartOffset = rp.logStartOffset;
        msg.errorCode     = rp.errorCode;
        msg.topicId       = rp.topicId;
        // records is a span into the owned vector in op.replicatedPayload.
        msg.records       = std::span<const uint8_t>(rp.records.data(), rp.records.size());

        try {
            op.replica->applyReplicated(msg);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: applyReplicated threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: applyReplicated threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // executeBecomeLeader_ — apply a replica role-change (Leader) on the
    // worker thread.  Fire-and-forget (no WorkerReturn needed).
    //
    // Enqueued by ReplicationMetadataBridge when workerPool_ != nullptr
    // (E2 shared-nothing fix: bridge runs on control thread, must not mutate
    // a replica whose doWork() runs on this worker thread).
    // -------------------------------------------------------------------
    void executeBecomeLeader_(WorkerOp op) {
        if (!op.replica) {
            spdlog::warn("Worker {}: executeBecomeLeader_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId, op.partition);
            return;
        }
        try {
            op.replica->becomeLeader(op.roleEpoch, std::move(op.isr));
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeLeader threw for topicId={} partition={}: {}",
                         workerId_, op.topicId, op.partition, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: becomeLeader threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // executeBecomeFollower_ — apply a replica role-change (Follower) on
    // the worker thread.  Fire-and-forget (no WorkerReturn needed).
    //
    // Enqueued by ReplicationMetadataBridge when workerPool_ != nullptr.
    // -------------------------------------------------------------------
    void executeBecomeFollower_(WorkerOp op) {
        if (!op.replica) {
            spdlog::warn("Worker {}: executeBecomeFollower_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId, op.partition);
            return;
        }
        try {
            op.replica->becomeFollower(op.roleEpoch, op.roleLeaderId);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeFollower threw for topicId={} partition={}: {}",
                         workerId_, op.topicId, op.partition, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: becomeFollower threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // getLog_ — resolve the Log* for the op's (topicId, partition).
    //
    // Preferentially uses the replica's log() accessor for disk-backed
    // replicas.  Falls back to topicRegistry_ for memory-backed or if
    // replica is null.
    //
    // topicRegistry_ is set via setTopicRegistry() (below) when the
    // WorkerPool is wired up by BrokerNode.
    // -------------------------------------------------------------------
    rune::storage::Log* getLog_(const WorkerOp& op) const noexcept {
        // Prefer replica->log() for disk-backed (avoids separate registry lookup).
        if (op.replica && op.replica->isDiskBacked()) {
            return op.replica->log();
        }
        // Fall back to direct Log* pointer stored in op (set by gateway at
        // enqueue time to avoid a registry lookup in doWork).
        if (op.logPtr) {
            return op.logPtr;
        }
        return nullptr;
    }

    int32_t workerId_;

    // E2 inc2a: per-worker SPSC rings.
    SpscRing<WorkerOp>     ingress_;  // control thread → worker
    SpscRing<WorkerReturn> ret_;      // worker → control thread

    // Genuinely lock-free ownership read (E2 inc2b).
    //
    // Raw pointer: std::atomic<const OwnedSet*> is lock-free on all supported
    // platforms (verified by static_assert in the constructor above).
    //
    // Lifetime / reclamation:
    //   WorkerPool owns the OwnedSet shared_ptrs.  Before storing a new raw
    //   pointer here (via publishOwned), WorkerPool retains the shared_ptr in
    //   a per-worker K=4 deep ring.  The oldest entry is evicted only when the
    //   ring is full; at that point the evicted set has been unreferenced for at
    //   least K−1 publish cycles (which happen only on registry-generation
    //   changes, seconds apart), so no worker is still reading it.
    //
    // Single-writer control thread publishes; single reader worker thread reads.
    //
    // When Worker is used standalone (unit tests), the local K=4 retain ring
    // below provides the same reclamation guarantee without a WorkerPool.
    // In production, WorkerPool uses its own pool-level retain rings; the local
    // ring here is populated only by the shared_ptr overload of publishOwned().
    std::atomic<const OwnedSet*> owned_{nullptr};

    // Local retain ring for standalone / unit-test use (publishOwned(shared_ptr)).
    static constexpr std::size_t kLocalRetainDepth = 4;
    std::array<std::shared_ptr<const OwnedSet>, kLocalRetainDepth> localRetain_{};
    std::size_t                                                     localRetainHead_ = 0;
};

} // namespace rune::node
