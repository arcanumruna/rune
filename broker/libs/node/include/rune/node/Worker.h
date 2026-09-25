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
// Shared-nothing ownership ([[rune-shared-nothing-law]]):
//
// owned_ is a PLAIN std::vector member touched ONLY by the worker thread — no
// atomic, no shared state, no cross-thread read. Ownership changes are carried
// to the worker as messages, never as a shared write:
//   - STARTUP (before per-worker AgentRunners start, single-threaded): the
//     WorkerPool sets owned_ directly via setOwnedDirect().
//   - RUNTIME (workers live on their own threads): the control thread enqueues a
//     SetOwnership WorkerOp to this worker's ingress ring; the worker applies it
//     in executeFromWire_ on ITS OWN thread. No lock, no atomic, no cross-thread race
//     by construction.
//
// E2 inc2a: Per-worker ingress + return rings.
//
//   ingress_: producer = control thread (gateway + transport); consumer = doWork.
//             R2: OneToOneByteRing carrying flat [WorkerOpWire][tail] messages.
//             Encode (tryEnqueueOp) → scratch → write (1 copy into ring).
//             Decode (drainIngress_) → zero-copy view into the ring frame.
//
//   ret_:     R3: OneToOneByteRing carrying flat [WorkerReturnWire][tail] messages.
//             producer = doWork (worker thread); consumer = gateway (drains in
//             gateway::doWork on the control thread).
//             Encode (worker): encodeWorkerReturn → returnScratch_ → ret_.write().
//             Decode (gateway): drainReturn() decodes each frame into a WorkerReturn
//             (scalars from WorkerReturnWire + chunk copied from tail) and calls fn.
//             Back-pressure: ret_.write() returns false when full → the encode+write
//             is skipped (the return is dropped for that cycle).
//             OLD BEHAVIOUR (SpscRing): ret_.tryEnqueue() was [[nodiscard]] but its
//             return value was NEVER checked at any call site — i.e. full = silent drop.
//             NEW BEHAVIOUR (R3): write() returns false on full → same silent-drop
//             semantics. A TODO/warn is left to make this observable.
//             NOTE: this is safe for best-effort acks (acks=Leader/None), but could
//             in theory delay a deferred-ack (acks=All) by one drain cycle.
//             TODO(R5): audit full-ret_ back-pressure; add retry logic if needed.
//
//   doWork order:
//     1. Drain ingress_ (execute ops, enqueue returns).
//     2. Run per-replica duty steps (Step 5 + 6.5 from E1).
//
//   Back-pressure on tryEnqueueOp: the caller must retry on the next duty cycle
//   (never drop, never overwrite).
//
// Ring sizing:
//   kIngressByteCapacity  = 8 MiB payload bytes (power-of-two).
//   kReturnByteCapacity   = 8 MiB payload bytes (power-of-two).
//   maxMsgLength() = capacity / 8 = 1 MiB — exceeds any practical chunk (512 KiB).
//   // TODO(R4): thread NodeConfig.*ByteCapacity to the ctor for runtime
//   //            configurability.

#include <rune/consensus/ConsensusAgent.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerWire.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerEgress.h>
#include <rune/node/IoBackend.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/ProducePath.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>  // E5.1.3: pc::ErrorCode enum (replaces magic literals)

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

// E4: CPU affinity helper — self-pin the calling thread to a specific core.
// Linux-only implementation; macOS is a deliberate no-op (mac build stays GREEN).
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace rune::node {

class Worker : public rune::consensus::Agent {
public:
    // Non-owning pointer set published as an immutable snapshot.
    using OwnedSet = std::vector<rune::replication::PartitionReplica*>;

    // Default ingress ring byte capacity (power-of-two payload bytes).
    // Used when the ctor is called without an explicit ingressRingBytes;
    // NodeConfig.workerIngressRingBytes (RUNE_WORKER_INGRESS_RING_BYTES) overrides it.
    // maxMsgLength() = capacity / 8 = 4 MiB — ample for any produce batch.
    static constexpr int kIngressByteCapacity = 32 * 1024 * 1024; // 32 MiB

    // Default return ring byte capacity (power-of-two payload bytes).
    // Used when the ctor is called without an explicit returnRingBytes;
    // NodeConfig.workerReturnRingBytes (RUNE_WORKER_RETURN_RING_BYTES) overrides it.
    // maxMsgLength() = capacity / 8 = 4 MiB — exceeds the 512 KiB max PushRead chunk.
    static constexpr int kReturnByteCapacity = 32 * 1024 * 1024; // 32 MiB

    // Default IoBackend ring depths (power-of-two).  NodeConfig wiring (Task 7)
    // will pass these explicitly; for now the constructor defaults are sensible.
    static constexpr std::size_t kIoSubmitDepth     = 1024;
    static constexpr std::size_t kIoCompletionDepth = 1024;

    // Pre-reserve capacity for the durablePending_ vector (no alloc on hot path).
    static constexpr std::size_t kDurablePendingReserve = 1024;

    // Pre-reserve capacity for the per-cycle dirty-log set (dedup, small).
    static constexpr std::size_t kDirtySetReserve = 64;

    // Pre-reserve capacity for the acks=all pending-ack list (worker ingress path).
    static constexpr std::size_t kPendingAcksAllReserve = 256;

    // Pre-reserve capacity for the overflow ack queue (G-A6: acks that could not
    // be written to ret_ in the same cycle — flushed before next ingress poll).
    static constexpr std::size_t kRetOverflowReserve = 64;

    // Default acks=all commit-wait timeout: 30 s. Accounts for the extra fetch-
    // handling latency (fetch waits a worker cycle, E5.1.2c Note B) so the
    // timeout is not too tight.  Configurable via setAckTimeoutNs.
    static constexpr int64_t kDefaultAckTimeoutNs = 30'000'000'000LL; // 30 s

    // ctor: ingressRingBytes / returnRingBytes are the per-worker SPSC ring
    // byte capacities (power of two). They default to the k*ByteCapacity
    // constants; BrokerNode threads NodeConfig.workerIngress/ReturnRingBytes
    // (RUNE_WORKER_INGRESS_RING_BYTES / RUNE_WORKER_RETURN_RING_BYTES) here.
    explicit Worker(int32_t workerId,
                    std::size_t ingressRingBytes = kIngressByteCapacity,
                    std::size_t returnRingBytes  = kReturnByteCapacity,
                    std::size_t ioSubmitDepth    = kIoSubmitDepth,
                    std::size_t ioComplDepth     = kIoCompletionDepth) noexcept
        : workerId_(workerId)
        , ingress_(static_cast<int>(ingressRingBytes))
        , ret_(static_cast<int>(returnRingBytes))
        , io_(ioSubmitDepth, ioComplDepth)
    {
        durablePending_.reserve(kDurablePendingReserve);
        dirtyThisCycle_.reserve(kDirtySetReserve);
        pendingAcksAll_.reserve(kPendingAcksAllReserve);
        retOverflow_.reserve(kRetOverflowReserve);
        subs_.reserve(kSubsReserve);
        egressScratch_.resize(kEgressScratchSize);
        threadName_ = "rune-wrk-" + std::to_string(workerId_);
    }

    // Thread name for `ps -M`/`top -H`/profilers: "rune-wrk-<id>" (see
    // AgentRunner::setCurrentThreadName). Cached so name() can hand back a
    // stable c_str().
    const char* name() const noexcept override { return threadName_.c_str(); }

    // Non-copyable and non-movable (two OneToOneByteRing members are not movable).
    Worker(const Worker&)            = delete;
    Worker& operator=(const Worker&) = delete;
    Worker(Worker&&)                 = delete;
    Worker& operator=(Worker&&)      = delete;

    // Set the owned replica set DIRECTLY. Shared-nothing ([[rune-shared-nothing-law]]):
    // owned_ is a plain member touched ONLY by the worker thread. This direct
    // setter is legal ONLY when no worker thread is running yet — i.e. at STARTUP
    // (WorkerPool::assignFromRegistry before startRunners) or in single-threaded
    // unit tests. At RUNTIME, ownership changes arrive as a SetOwnership ring op
    // (executeSetOwnership_ from the wire) applied on the worker's own thread —
    // never a cross-thread write to owned_.
    void setOwnedDirect(OwnedSet set) noexcept {
        owned_ = std::move(set);
    }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }

    // -----------------------------------------------------------------------
    // E5.1.2d: per-worker ingress produce path.
    //
    // setWorkerIngress — wire up a WorkerIngress and (optionally) activate the
    // per-worker produce path.  Must be called BEFORE the worker's AgentRunner
    // starts (startup / single-threaded).
    //
    // perWorkerIngressActive_ defaults FALSE so the existing gateway fanout
    // remains the live path and nothing double-processes.  E5.1.3 will construct
    // per-worker subscriptions in BrokerNode, wire them here, set active=true,
    // and retire the gateway fanout — all together.
    // -----------------------------------------------------------------------
    void setWorkerIngress(WorkerIngress* ingress, bool active = false) noexcept {
        workerIngress_          = ingress;
        perWorkerIngressActive_ = active;
    }

    // -----------------------------------------------------------------------
    // read-rework T3/T4: wire this worker's OWN dynamic-MDC egress publication.
    // The worker thread becomes the sole writer of `egress` (WorkerEgress::send).
    // Non-owning: BrokerNode owns the WorkerEgress lifetime.  Must be called
    // BEFORE the worker's AgentRunner starts (startup / single-threaded).
    // -----------------------------------------------------------------------
    void setWorkerEgress(WorkerEgress* egress) noexcept { workerEgress_ = egress; }
    [[nodiscard]] WorkerEgress* workerEgress() const noexcept { return workerEgress_; }

    // -----------------------------------------------------------------------
    // read-rework T3/T4: worker-local subscription cursor (shared-nothing).
    //
    // The gateway routes Subscribe/Unsubscribe ops to the owning worker via the
    // ingress ring; the worker owns the per-subscription push cursor (nextOffset)
    // and the metadata needed to read + encode + push (I3).  This table is touched
    // ONLY by the worker thread — no locks, no atomics, no cross-thread mutation.
    // -----------------------------------------------------------------------
    struct WorkerSubscription {
        int64_t subscriptionId       = 0;
        int64_t sessionId            = 0;
        int32_t topicId              = 0;
        int32_t partition            = 0;
        int64_t nextOffset           = 0;   ///< push cursor (advanced by the worker)
        rune::storage::Log* logPtr   = nullptr;
        rune::replication::PartitionReplica* replica = nullptr;
        uint8_t isolation            = 0;
        bool    isGroup              = false;
        bool    groupCarrierReceived = false;
    };

    /// Test-only: the worker-local subscription table (worker-thread state).
    [[nodiscard]] const std::vector<WorkerSubscription>& subscriptionsForTest() const noexcept {
        return subs_;
    }

    // Set acks=all commit-wait timeout.  Default kDefaultAckTimeoutNs (30 s).
    void setAckTimeoutNs(int64_t timeoutNs) noexcept { ackTimeoutNs_ = timeoutNs; }

    // -----------------------------------------------------------------------
    // E5.1.3: setDesiredFragmentLimit — configure the reservation-before-poll
    // fragment limit K used in pollWorkerIngress_.
    //
    // K = min(desiredFragmentLimit_, freeMaxMsgSlots − retOverflow_.size())
    //
    // Source: NodeConfig::gatewayFragmentLimit (RUNE_GATEWAY_FRAGMENT_LIMIT, default 256).
    // Must be called before doWork() on the worker's thread (before runners start).
    // -----------------------------------------------------------------------
    void setDesiredFragmentLimit(int32_t limit) noexcept {
        desiredFragmentLimit_ = (limit >= 1) ? limit : 256;
    }

    // Expose psm_ for tests (worker-thread only; call only from single-threaded tests).
    [[nodiscard]] rune::storage::ProducerStateManager& psmForTest() noexcept { return psm_; }

    // E4: assign the core this worker's AgentRunner thread should be pinned to.
    //
    // Shared-nothing ([[rune-shared-nothing-law]]): setCoreId is called BEFORE the
    // AgentRunner starts (startup, single-threaded, WorkerPool::startRunners), so
    // there is no cross-thread write to coreId_ at runtime.  After the runner
    // starts, coreId_ is read-only (the self-pin fires once on first doWork).
    //
    // -1 (default) = no pinning.  Any value >= 0 causes a one-time self-pin on
    // the FIRST doWork call (running on the worker's AgentRunner thread — exactly
    // the thread we want pinned).
    void setCoreId(int c) noexcept { coreId_ = c; }

    // owns()/ownedCount() read the plain owned_ — worker-thread only (call from
    // the worker thread, or before runners start / from single-threaded tests).
    [[nodiscard]] std::size_t ownedCount() const noexcept {
        return owned_.size();
    }

    [[nodiscard]] bool owns(const rune::replication::PartitionReplica* r) const noexcept {
        return std::find(owned_.begin(), owned_.end(), r) != owned_.end();
    }

    // -----------------------------------------------------------------------
    // E2 inc2a: ingress ring accessor (producer = control thread).
    //
    // R2: encodes the WorkerOp into ingressScratch_ (flat [WorkerOpWire][tail])
    //     then writes it to the OneToOneByteRing.
    //
    // Signature UNCHANGED — all producer call sites (gateway, transport, bridge,
    // WorkerPool) pass a WorkerOp and observe the same false-on-full contract.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool tryEnqueueOp(WorkerOp op) {
        // Zero-copy: reserve the exact ring slot, then encode the op DIRECTLY into
        // it (no scratch buffer, no scratch→ring copy).  tryClaim() returns < 0 when
        // the ring is full → explicit back-pressure; the caller must hold and retry.
        const std::size_t n = encodedWorkerOpSize(op);
        const aeron::util::index_t idx =
            ingress_.tryClaim(workerOpMsgTypeId(op), n);
        if (idx < 0) return false;  // ring full — back-pressure
        encodeWorkerOp(op, ingress_.base() + idx, n);
        ingress_.commit(idx);
        return true;
    }

    // -----------------------------------------------------------------------
    // R3: return ring drain (consumer = gateway, on control thread).
    //
    // ret_ is now a OneToOneByteRing carrying flat [WorkerReturnWire][tail]
    // messages.  This template drains the ring, decodes each frame into a
    // WorkerReturn struct (scalars from WorkerReturnWire; chunk copied from the
    // tail view), and calls fn(WorkerReturn) — same callback signature as before.
    //
    // The tail view (PushReadResult chunk) is fully consumed (copied into
    // WorkerReturn.chunk) before fn returns.  No view escapes the callback.
    //
    // fn must be callable as fn(rune::node::WorkerReturn).
    // -----------------------------------------------------------------------
    template <typename Fn>
    std::size_t drainReturn(Fn&& fn) {
        int drained = ret_.drain(
            [&fn](int32_t /*typeId*/,
                  aeron::concurrent::AtomicBuffer& buf,
                  aeron::util::index_t off,
                  aeron::util::index_t /*len*/)
            {
                const uint8_t* frame =
                    reinterpret_cast<const uint8_t*>(buf.buffer()) + off;
                const WorkerReturnWire* hdr = decodeWorkerReturnHeader(frame);

                // Reconstruct WorkerReturn from the wire header.
                WorkerReturn ret;
                ret.type                 = static_cast<WorkerReturn::Type>(hdr->type);
                ret.sessionId            = hdr->sessionId;
                ret.correlationId        = hdr->correlationId;
                ret.partition            = hdr->partition;
                ret.baseOffset           = hdr->baseOffset;
                ret.lastOffset           = hdr->lastOffset;
                ret.errorCode            = hdr->errorCode;
                ret.deferAck             = hdr->deferAck != 0;
                ret.replica              = reinterpret_cast<
                    rune::replication::PartitionReplica*>(hdr->replica);
                ret.producerId           = hdr->producerId;
                ret.producerEpoch        = hdr->producerEpoch;
                ret.baseSequence         = hdr->baseSequence;
                ret.recordCount          = hdr->recordCount;
                ret.topicId              = hdr->topicId;
                ret.batchAttrs           = hdr->batchAttrs;
                ret.enqueuedAtNs         = hdr->enqueuedAtNs;
                ret.subscriptionId       = hdr->subscriptionId;
                ret.readTopicId          = hdr->readTopicId;
                ret.readPartition        = hdr->readPartition;
                ret.newNextOffset        = hdr->newNextOffset;
                ret.hwBound              = hdr->hwBound;
                ret.isolation            = hdr->isolation;
                ret.isGroup              = hdr->isGroup != 0;
                ret.groupCarrierReceived = hdr->groupCarrierReceived != 0;

                // Tail: PushReadResult chunk — copy from ring view (1 copy).
                // The view is valid only for the duration of this callback; we
                // copy into ret.chunk so fn can access it safely.
                const uint8_t* tail = workerReturnTail(frame);
                if (tail && hdr->tailLen > 0) {
                    ret.chunk.assign(tail, tail + hdr->tailLen);
                }

                fn(std::move(ret));
            });
        return static_cast<std::size_t>(drained < 0 ? 0 : drained);
    }

    // -----------------------------------------------------------------------
    // E3 Task 5: test accessor — cumulative count of successful io_.submit calls
    // in submitCoalescedFlushes_().  Used by the GroupCommit test to assert
    // that 8 produces to one partition produce exactly 1 submit per cycle.
    // Worker-thread only; call only from single-threaded tests.
    // -----------------------------------------------------------------------
    [[nodiscard]] uint64_t ioSubmitCountForTest() const noexcept {
        return io_submitCount_;
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
    //
    // E5.1.2d: G-A6 backpressure loop order (when perWorkerIngressActive_):
    //   1. Reap async-durability completions.
    //   2. Flush owed acks FIRST (acks=all pending whose replica HW >= lastOffset).
    //   3. Poll WorkerIngress ONLY IF ret_ has space AND no owed acks.
    //      This ensures Aeron term buffer fills → client offer() back-pressures.
    //      NEVER drop an ack.
    int doWork(int64_t nowNs) override {
        int work = 0;

        // E4: self-pin this thread to the assigned core on the FIRST doWork call.
        // Runs on the AgentRunner thread (the exact thread to pin).  One-shot:
        // pinned_ prevents repeat attempts.  Shared-nothing: coreId_ and pinned_
        // are worker-owned; setCoreId() was called before the runner started.
        if (coreId_ >= 0 && !pinned_) {
            pinned_ = true; // set before the call so a spurious failure doesn't retry
            if (pinCurrentThreadToCore_(coreId_)) {
                spdlog::info("Worker {}: pinned AgentRunner thread to core {}",
                             workerId_, coreId_);
            } else {
                spdlog::warn("Worker {}: failed to pin AgentRunner thread to core {} "
                             "(Linux: check RUNE_WORKER_CORE_BASE; macOS: no-op)",
                             workerId_, coreId_);
            }
        }

        // Step E3: reap async-durability completions from the IoBackend FIRST so
        // that completions from the previous cycle fire before the next produce
        // cycle's ops are processed.  Per-worker, no shared state.
        work += reapDurableCompletions_();

        // E5.1.2d: G-A6 backpressure path (only active when perWorkerIngressActive_).
        //
        // 2a. Flush overflow acks FIRST — acks that could not be written to ret_
        //     in a prior cycle.  Stops as soon as ret_ is full again.
        //     While retOverflow_ is non-empty, no new ingress polling occurs.
        // 2b. Flush owed acks=all — checks each pending ack's replica HW;
        //     fires the ack when HW >= lastOffset, or a timeout error if overdue.
        //     While pendingAcksAll_ is non-empty, no new ingress polling occurs.
        // 3.  Poll WorkerIngress ONLY IF ret_ has space AND retOverflow_ is empty
        //     AND pendingAcksAll_ is empty.
        //     Skipping the poll when any ack backlog exists ensures Aeron's term
        //     buffer fills → client offer() back-pressures.  No ack is ever
        //     dropped (G-A6 structural enforcement).
        if (perWorkerIngressActive_) {
            work += flushRetOverflow_();
            work += flushOwedAcksAll_(nowNs);
            if (ret_.hasSpaceForMaxMsg() && retOverflow_.empty() && pendingAcksAll_.empty()) {
                work += pollWorkerIngress_(nowNs);
            }
        }

        // Step 0: drain ingress ring (E2 inc2a / R2). This may include SetOwnership
        // ops that mutate owned_ (on THIS worker thread — single-owner, no lock).
        // Strict-tier produces record their log in dirtyThisCycle_ (token=0).
        work += static_cast<int>(drainIngress_());

        // Step E3 coalesce: after all this-cycle produces are appended, submit
        // ONE fsync per dirty log and stamp the completion token onto every pending
        // entry for that log.  submitCoalescedFlushes_ clears dirtyThisCycle_.
        work += submitCoalescedFlushes_();

        // Shared-nothing: owned_ is a plain member touched only by this thread.
        // Step 5: per-replica ISR checks / follower fetch emission.
        for (auto* replica : owned_) {
            if (!replica) continue;
            work += replica->doWork(nowNs);
        }

        // read-rework T3/T4 (I3): in-worker push — read owned partitions and push
        // Records directly on this worker's egress.  Runs after replica doWork so
        // the HW bound is fresh.  No-op unless subs_ is populated (Subscribe ops)
        // AND a WorkerEgress is wired (pre-I3b: gateway still owns push → inert).
        work += pushOwnedSubscriptions_(nowNs);

        // Step 6.5: drain cleaner/retention/staging queues (disk-backed only).
        for (auto* replica : owned_) {
            if (replica && replica->isDiskBacked()) {
                work += replica->log()->drainCleanerSwaps();
                work += replica->log()->drainRetentionDeletes();
                work += replica->log()->drainStagingRenamesDone();
            }
        }

        // E5.1.3 (d) R7 fix: time-based Log::sync on owned user-partition logs.
        //
        // When per-worker ingress is active, the gateway's flushDirtyLogs() skips
        // worker-owned logs (R7 gate).  Instead, the WORKER calls log->sync() here
        // on ITS OWN thread — the same thread that appends — eliminating the
        // append-vs-sync data race (R7 closed).
        //
        // Applies only when perWorkerIngressActive_ is true (default: false).
        // Internal partitions (topicId < 0) are not in owned_ so they keep the
        // gateway sync path unaffected.
        if (perWorkerIngressActive_) {
            work += syncOwnedLogs_(nowNs);
        }

        return work;
    }

private:
    // -----------------------------------------------------------------------
    // enqueueReturn_ — R3: encode a WorkerReturn into returnScratch_ and
    //                  write it to the ret_ OneToOneByteRing.
    //
    // G-A6 (WorkerIngress path): if ret_.write() fails (ring full), the ack is
    // pushed to retOverflow_ — a per-worker "ack overflow queue".  The overflow
    // queue is flushed at the TOP of the perWorkerIngressActive_ block on the
    // NEXT doWork cycle (before any new ingress polling).  This ensures:
    //   (a) No ack is ever silently dropped.
    //   (b) The worker holds back (does not poll more frames) when ret_ is full
    //       or an overflow ack is pending — G-A6 structural enforcement.
    //
    // Non-WorkerIngress path (existing gateway ingress ring): silent drop is
    // preserved (same as original R3 behaviour — TODO(R5)).
    //
    // Worker-thread only (the producer side of ret_).
    // -----------------------------------------------------------------------
    void enqueueReturn_(const WorkerReturn& ret) {
        // Zero-copy: reserve the exact ring slot then encode the return DIRECTLY into
        // it (no returnScratch_ copy).  tryClaim() < 0 = explicit back-pressure.
        const std::size_t n = encodedWorkerReturnSize(ret);
        aeron::util::index_t idx;
        try {
            idx = ret_.tryClaim(workerReturnMsgTypeId(ret), n);
        } catch (const std::exception& ex) {
            spdlog::error("Worker {}: enqueueReturn_ tryClaim failed: {}", workerId_, ex.what());
            return;
        }
        if (idx < 0) {
            if (perWorkerIngressActive_) {
                // G-A6: push to overflow queue instead of dropping.
                // flushRetOverflow_ will retry on the next doWork cycle.
                retOverflow_.push_back(ret);
            } else {
                // Non-WorkerIngress path: preserve existing silent-drop behaviour.
                // TODO(R5): add retry or back-pressure propagation for this path too.
                spdlog::warn("Worker {}: ret_ ring full — WorkerReturn type={} dropped",
                             workerId_, static_cast<int>(ret.type));
            }
            return;
        }
        encodeWorkerReturn(ret, ret_.base() + idx, n);
        ret_.commit(idx);
    }

    // -----------------------------------------------------------------------
    // submitCoalescedFlushes_ — group-commit: submit ONE fsync per dirty log.
    //
    // Called from doWork AFTER drainIngress_ (so all this-cycle appends are
    // recorded) and BEFORE the per-replica doWork loop.
    //
    // For each log in dirtyThisCycle_:
    //   - Call io_.submit(log->activeSegmentFd(), log->flushBoundary()).
    //   - If submit returns kNoToken (ring full — back-pressure): for every
    //     durablePending_ entry with token==0 && logPtr==log, immediately
    //     return Internal (6) and erase the entry.  Never drop; never false-OK.
    //   - If submit succeeds: stamp the returned token onto every pending entry
    //     with token==0 && logPtr==log (the completion covers all of them).
    //
    // dirtyThisCycle_ is cleared at the end.  Returns work units done.
    // -----------------------------------------------------------------------
    int submitCoalescedFlushes_() {
        if (dirtyThisCycle_.empty()) return 0;

        int work = 0;
        for (rune::storage::Log* log : dirtyThisCycle_) {
            CompletionToken t = io_.submit(log->activeSegmentFd(), log->flushBoundary());
            if (t == kNoToken) {
                // Submit ring full — back-pressure: ack all pending entries for
                // this log with Internal error immediately.  Never drop; never
                // silently hold forever.
                spdlog::warn("Worker {}: submitCoalescedFlushes_ ring full for log@{}; "
                             "back-pressure: acking pending entries Internal",
                             workerId_, static_cast<const void*>(log));
                for (std::size_t i = 0; i < durablePending_.size(); ) {
                    auto& entry = durablePending_[i];
                    if (entry.token == kNoToken && entry.logPtr == log) {
                        entry.ret.errorCode = static_cast<int16_t>(6); // Internal
                        enqueueReturn_(entry.ret);
                        // O(1) erase by swap-with-back.
                        durablePending_[i] = std::move(durablePending_.back());
                        durablePending_.pop_back();
                        ++work;
                        // Do NOT advance i — the swapped entry must be checked.
                    } else {
                        ++i;
                    }
                }
            } else {
                // Successful submit: stamp the token onto every pending entry
                // for this log (all appends this cycle share one fsync token).
                for (auto& entry : durablePending_) {
                    if (entry.token == kNoToken && entry.logPtr == log) {
                        entry.token = t;
                    }
                }
                ++io_submitCount_;
                ++work;
            }
        }
        dirtyThisCycle_.clear();
        return work;
    }

    // -----------------------------------------------------------------------
    // reapDurableCompletions_ — drain IoBackend completions and fire the
    // deferred acks for completed Strict-tier produces.
    //
    // Called at the TOP of doWork so completions from the prior cycle fire
    // before the current cycle's produce ops are executed.
    //
    // Shared-nothing: io_, durablePending_, and ret_ are all owned by this
    // worker thread.  No locks, no atomics outside the SPSC ring internals.
    // -----------------------------------------------------------------------
    int reapDurableCompletions_() {
        if (durablePending_.empty()) {
            // Fast path: nothing pending — still drain completions to keep the
            // completion ring from filling (the flusher may emit completions
            // even for submits that were not strict-tier, though in practice
            // this doesn't happen; the drain is cheap either way).
            return io_.reap([](const FlushCompletion&) {});
        }

        int fired = 0;
        io_.reap([&](const FlushCompletion& c) {
            // Group-commit semantics: a completion with token T and flushBoundary B
            // covers EVERY pending entry with the same token whose lastOffset <= B.
            // Entries with token==0 (not yet submitted) are never matched here.
            for (std::size_t i = 0; i < durablePending_.size(); ) {
                auto& entry = durablePending_[i];
                if (entry.token != 0 &&
                    entry.token == c.token &&
                    entry.lastOffset <= c.flushBoundary) {
                    if (c.errorCode != 0) {
                        // fsync failure: ack with a non-OK error code — never ack OK.
                        spdlog::error("Worker {}: IoBackend fsync completion error={} "
                                      "for token={} lastOffset={}; acking Internal error",
                                      workerId_, c.errorCode, c.token, entry.lastOffset);
                        entry.ret.errorCode = static_cast<int16_t>(6); // Internal
                        // No replica unhealthy-flag exists on PartitionReplica yet
                        // (noted for Task 9 wiring); log the error here so it is
                        // observable without silently swallowing the failure.
                    } else {
                        entry.ret.errorCode = 0; // OK
                    }
                    enqueueReturn_(entry.ret);
                    // Erase by swap-with-back + pop (O(1), order irrelevant for acks).
                    durablePending_[i] = std::move(durablePending_.back());
                    durablePending_.pop_back();
                    ++fired;
                    // Do NOT advance i — the swapped entry needs to be checked.
                } else {
                    ++i;
                }
            }
        });
        return fired;
    }

    // -----------------------------------------------------------------------
    // drainIngress_ — decode and execute all queued ops from the agrona ring.
    //
    // R2: ingress_ is now a OneToOneByteRing. Each ring frame is a flat
    //     [WorkerOpWire][optional tail] message. The handler receives a zero-copy
    //     view into the ring; the view is valid ONLY for the duration of the
    //     callback — every op is fully consumed before the callback returns.
    //
    // Called from doWork (the consumer side of ingress_).
    // Single-writer invariant: doWork runs on the worker's own thread.
    // -----------------------------------------------------------------------
    std::size_t drainIngress_() {
        int drained = ingress_.drain(
            [this](int32_t /*typeId*/,
                   aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off,
                   aeron::util::index_t len)
            {
                // Zero-copy view: frame pointer is valid only inside this callback.
                // Fully consume the message (executeFromWire_) before returning.
                (void)len;
                const uint8_t* frame =
                    reinterpret_cast<const uint8_t*>(buf.buffer()) + off;
                executeFromWire_(frame);
            });
        return static_cast<std::size_t>(drained < 0 ? 0 : drained);
    }

    // -----------------------------------------------------------------------
    // executeFromWire_ — dispatch on WorkerOpWire::type and call the per-type
    //                    handler.  The frame pointer is a zero-copy view into the
    //                    agrona ring — valid only inside drainIngress_ callback.
    // -----------------------------------------------------------------------
    void executeFromWire_(const uint8_t* frame) {
        const WorkerOpWire* hdr = decodeWorkerOpHeader(frame);
        switch (static_cast<WorkerOp::Type>(hdr->type)) {
        case WorkerOp::Type::Produce:
            executeProduceWire_(frame, hdr);
            break;
        case WorkerOp::Type::PushRead:
            executePushReadWire_(frame, hdr);
            break;
        case WorkerOp::Type::ApplyReplicated:
            executeApplyReplicatedWire_(frame, hdr);
            break;
        case WorkerOp::Type::BecomeLeader:
            executeBecomeLeaderWire_(frame, hdr);
            break;
        case WorkerOp::Type::BecomeFollower:
            executeBecomeFollowerWire_(frame, hdr);
            break;
        case WorkerOp::Type::SetOwnership:
            executeSetOwnershipWire_(frame, hdr);
            break;
        // E5.1.2c: leader-side replication state handlers
        case WorkerOp::Type::FetchRequest:
            executeFetchRequestWire_(frame, hdr);
            break;
        case WorkerOp::Type::OffsetsForLeaderEpochReq:
            executeOffsetsForLeaderEpochReqWire_(frame, hdr);
            break;
        case WorkerOp::Type::ReconcileFetchResp:
            executeReconcileFetchRespWire_(frame, hdr);
            break;
        // Task #13: AppendControlRecord — worker appends txn control marker
        case WorkerOp::Type::AppendControlRecord:
            executeAppendControlRecordWire_(frame, hdr);
            break;
        // read-rework T3/T4: subscription lifecycle (worker owns the push cursor)
        case WorkerOp::Type::Subscribe:
            executeSubscribeWire_(hdr);
            break;
        case WorkerOp::Type::Unsubscribe:
            executeUnsubscribeWire_(hdr);
            break;
        }
    }

    // -------------------------------------------------------------------
    // read-rework T3/T4: Subscribe — register or update a worker-local
    // subscription cursor from the wire scalars.  Fire-and-forget (no return).
    // Idempotent: a repeated Subscribe for the same subscriptionId UPDATES the
    // existing entry (e.g. cursor rewind / carrier arrival) rather than
    // duplicating it.  Worker-thread-only mutation of subs_.
    // -------------------------------------------------------------------
    void executeSubscribeWire_(const WorkerOpWire* hdr) {
        for (auto& s : subs_) {
            if (s.subscriptionId == hdr->subscriptionId) {
                s.sessionId            = hdr->sessionId;
                s.topicId              = hdr->topicId;
                s.partition            = hdr->partition;
                s.nextOffset           = hdr->nextOffset;
                s.logPtr               = reinterpret_cast<rune::storage::Log*>(hdr->logPtr);
                s.replica              = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
                s.isolation            = hdr->isolation;
                s.isGroup              = hdr->isGroup != 0;
                s.groupCarrierReceived = hdr->groupCarrierReceived != 0;
                return;
            }
        }
        WorkerSubscription s;
        s.subscriptionId       = hdr->subscriptionId;
        s.sessionId            = hdr->sessionId;
        s.topicId              = hdr->topicId;
        s.partition            = hdr->partition;
        s.nextOffset           = hdr->nextOffset;
        s.logPtr               = reinterpret_cast<rune::storage::Log*>(hdr->logPtr);
        s.replica              = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        s.isolation            = hdr->isolation;
        s.isGroup              = hdr->isGroup != 0;
        s.groupCarrierReceived = hdr->groupCarrierReceived != 0;
        subs_.push_back(s);
    }

    // -------------------------------------------------------------------
    // read-rework T3/T4: Unsubscribe — remove a worker-local subscription by id
    // (swap-and-pop; order does not matter).  No-op if unknown.  Worker-thread-only.
    // -------------------------------------------------------------------
    void executeUnsubscribeWire_(const WorkerOpWire* hdr) {
        for (std::size_t i = 0; i < subs_.size(); ++i) {
            if (subs_[i].subscriptionId == hdr->subscriptionId) {
                subs_[i] = subs_.back();
                subs_.pop_back();
                return;
            }
        }
    }

    // -------------------------------------------------------------------
    // read-rework T3/T4 (I3): in-worker push loop.  For each active owned
    // subscription, zero-copy read whole batches from the cursor (Log::readView),
    // encode each as a Records frame into egressScratch_, and push it directly on
    // THIS worker's dynamic-MDC egress publication (workerEgress_).  Advance the
    // cursor only on a successful send (back-pressure preserves at-least/exactly-
    // once, same invariant as the gateway push path).
    //
    // Shared-nothing: reads only worker-owned state (subs_, the owned Log/replica,
    // workerEgress_).  Zero heap alloc on the hot path (readView span + preallocated
    // egressScratch_).  No locks/atomics.
    //
    // Bound: read_uncommitted → HW (replica->highWatermark(), or LEO passthrough).
    // read_committed (LSO/aborted-txn) is NOT yet applied here — that txn state is
    // gateway-owned; this path is HW-bounded (read_uncommitted) until it migrates.
    // Control batches are skipped (never delivered to clients).
    //
    // Inert until Subscribe ops populate subs_ AND a WorkerEgress is wired, so it
    // is a no-op in the pre-I3b configuration (gateway still owns push).
    // -------------------------------------------------------------------
    int pushOwnedSubscriptions_(int64_t /*nowNs*/) {
        using namespace rune::protocol::batch;
        namespace pc = rune::protocol::client;
        if (!workerEgress_ || subs_.empty()) return 0;

        int work = 0;
        for (auto& s : subs_) {
            if (!s.logPtr) continue;

            // HW bound (read_uncommitted).  Worker owns the replica → live HW.
            const int64_t bound = s.replica ? s.replica->highWatermark()
                                            : s.logPtr->logEndOffset();
            if (s.nextOffset >= bound) continue;

            rune::storage::LogReadView view;
            try {
                view = s.logPtr->readView(s.nextOffset, kPushMaxChunkBytes);
            } catch (...) {
                continue;  // transient read error; retry next cycle
            }
            if (view.empty()) continue;

            std::size_t pos = 0;
            int pushed = 0;
            bool backPressured = false;
            while (pos < view.len && pushed < kPushBatchesPerSubPerCycle && !backPressured) {
                constexpr std::size_t kMinHdr =
                    static_cast<std::size_t>(kRecordCountOffset) + 4u;
                if (pos + kMinHdr > view.len) break;  // partial header (should not happen: whole batches)
                const uint8_t* bp = view.data + pos;

                int64_t baseOffset = 0;
                for (int i = 0; i < 8; ++i)
                    baseOffset |= (static_cast<int64_t>(bp[kBaseOffsetOffset + i]) << (i * 8));
                if (baseOffset >= bound) break;  // reached the HW bound

                uint32_t lengthField = 0;
                for (int i = 0; i < 4; ++i)
                    lengthField |= (static_cast<uint32_t>(bp[kLengthOffset + i]) << (i * 8));
                const std::size_t batchSize = 12u + static_cast<std::size_t>(lengthField);
                if (pos + batchSize > view.len) break;  // incomplete batch at end

                int32_t recordCount = 0;
                for (int i = 0; i < 4; ++i)
                    recordCount |= (static_cast<int32_t>(bp[kRecordCountOffset + i]) << (i * 8));

                uint16_t attrs = static_cast<uint16_t>(
                    static_cast<uint16_t>(bp[kAttributesOffset]) |
                    (static_cast<uint16_t>(bp[kAttributesOffset + 1]) << 8));
                constexpr uint16_t kControlBit = 0x20u;

                if (recordCount < 1) {  // corrupt/empty — skip, make progress
                    pos += batchSize;
                    s.nextOffset = baseOffset + 1;
                    continue;
                }
                if (attrs & kControlBit) {  // control marker — never delivered
                    pos += batchSize;
                    s.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                    continue;
                }

                pc::Records rec;
                rec.sessionId      = s.sessionId;
                rec.correlationId  = 0;
                rec.subscriptionId = s.subscriptionId;
                rec.topicId        = s.topicId;
                rec.partition      = s.partition;
                rec.baseOffset     = baseOffset;
                rec.recordBatch    = std::span<const uint8_t>(bp, batchSize);

                const std::size_t needed =
                    pc::MessageHeader::ENCODED_SIZE + pc::Records::BODY_FIXED + 4u + batchSize;
                if (needed > egressScratch_.size()) {  // unencodable — skip forward
                    pos += batchSize;
                    s.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                    continue;
                }

                const std::size_t encoded = rec.encode(egressScratch_.data());
                const int64_t r = workerEgress_->send(egressScratch_.data(), encoded);
                if (r > 0) {
                    s.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                    pos += batchSize;
                    ++pushed;
                    ++work;
                } else {
                    backPressured = true;  // retry from same cursor next cycle
                }
            }
        }
        return work;
    }

    // -------------------------------------------------------------------
    // executeProduceWire_ — append the batch to the owning Log and update
    // the replica's tracked LEO/HW via onLeaderLogAppended().
    //
    // The tail view IS the record-batch bytes in the ring. We re-stamp the
    // base offset in-place (CRC region is after kBaseOffsetOffset=0, so no
    // CRC recompute) then call log->appendNoSync(tail, tailLen, lastOffset)
    // — zero-copy READ from the ring into the log.
    //
    // CRITICAL: the tail pointer is a zero-copy view valid only for the
    // duration of this callback — it is fully consumed here (appendNoSync
    // copies the bytes into the log's owned buffer).
    // -------------------------------------------------------------------
    void executeProduceWire_(const uint8_t* frame, const WorkerOpWire* hdr) {
        WorkerReturn ret;
        ret.type          = WorkerReturn::Type::ProduceResult;
        ret.sessionId     = hdr->sessionId;
        ret.correlationId = hdr->correlationId;
        ret.partition     = hdr->partition;
        ret.deferAck      = hdr->deferAck != 0;
        ret.replica       = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        ret.producerId    = hdr->producerId;
        ret.producerEpoch = hdr->producerEpoch;
        ret.baseSequence  = hdr->baseSequence;
        ret.recordCount   = hdr->recordCount;
        ret.topicId       = hdr->topicId;
        ret.batchAttrs    = hdr->batchAttrs;
        ret.enqueuedAtNs  = hdr->enqueuedAtNs;

        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        auto* log     = getLogFromWire_(hdr);

        if (!log) {
            spdlog::error("Worker {}: executeProduceWire_ — no Log for topicId={} partition={}; "
                          "sending Internal error",
                          workerId_, hdr->topicId, hdr->partition);
            ret.errorCode = static_cast<int16_t>(6); // ErrorCode::Internal
            enqueueReturn_(ret);
            return;
        }

        // Assign the actual base offset (authoritative — worker is the single writer).
        const int64_t base = log->logEndOffset();
        ret.baseOffset = base;
        ret.lastOffset = base + static_cast<int64_t>(hdr->lastOffsetDelta);

        // Re-stamp the base offset in the tail view (LE64 at kBaseOffsetOffset=0).
        // The tail is a mutable view in the ring (AtomicBuffer); the ring frame
        // is not read by the producer again after write() returned — safe to
        // mutate in-place here.
        // CRC region starts at kAttributesOffset=21, AFTER the base-offset field,
        // so re-stamping does NOT invalidate the CRC.
        uint8_t* tail = const_cast<uint8_t*>(workerOpTail(frame));
        const uint32_t tailLen = hdr->tailLen;
        if (tail && tailLen >= 8u) {
            const uint64_t u = static_cast<uint64_t>(base);
            for (int i = 0; i < 8; ++i) {
                tail[static_cast<std::size_t>(i)] =
                    static_cast<uint8_t>(u >> (i * 8));
            }
        }

        // Zero-copy append: read directly from the ring frame into the log.
        // appendNoSync copies the bytes into the log's owned segment buffer.
        try {
            const int64_t appended =
                log->appendNoSync(tail, static_cast<std::size_t>(tailLen), ret.lastOffset);
            // R5 finding F6 (correctness): a negative return means the append was
            // REFUSED (e.g. InMemoryLog in BackPressure mode with a full ring).
            // We MUST NOT ack OK for a record that was never appended — that is a
            // phantom-durability false-OK.  Return a non-OK error so the client
            // retries ([[rune-shared-nothing-law]]: never false-OK a commit-critical
            // produce under back-pressure).
            if (appended < 0) {
                spdlog::warn("Worker {}: appendNoSync refused (back-pressure) for "
                             "topicId={} partition={}; returning Internal",
                             workerId_, hdr->topicId, hdr->partition);
                ret.errorCode = static_cast<int16_t>(6); // Internal
                enqueueReturn_(ret);
                return;
            }
        } catch (const std::exception& ex) {
            spdlog::error("Worker {}: appendNoSync threw: {}", workerId_, ex.what());
            ret.errorCode = static_cast<int16_t>(6); // Internal
            enqueueReturn_(ret);
            return;
        } catch (...) {
            spdlog::error("Worker {}: appendNoSync threw unknown exception", workerId_);
            ret.errorCode = static_cast<int16_t>(6); // Internal
            enqueueReturn_(ret);
            return;
        }

        // GR-4 Mode A seam: reconcile the replica's tracked LEO/HW.
        if (replica) {
            replica->onLeaderLogAppended();
        }

        // E3 Task 5: Strict durability tier — defer ack until fsync completion is reaped.
        // Only applicable to disk-backed logs (memory logs have no fd to fsync).
        //
        // Group-commit coalescing: we do NOT submit the fsync here.  Instead we
        // push a DurablePending entry with token=0 (un-flushed-yet) and record the
        // log in dirtyThisCycle_ (dedup — only if not already present this cycle).
        // After ALL this-cycle produces are appended (drainIngress_ returns),
        // submitCoalescedFlushes_() submits ONE io_.submit per dirty log and stamps
        // the resulting token onto every pending entry for that log.
        if (hdr->durabilityTier == 2 && log->isDiskBacked()) {
            ret.errorCode = 0; // tentatively OK; updated in submitCoalescedFlushes_ or on completion
            // token=0 means "not yet submitted" — reapDurableCompletions_ will never fire this.
            durablePending_.push_back({kNoToken, ret.lastOffset, log, std::move(ret)});

            // Record this log as dirty for this cycle (dedup).
            bool found = false;
            for (auto* dl : dirtyThisCycle_) {
                if (dl == log) { found = true; break; }
            }
            if (!found) dirtyThisCycle_.push_back(log);
            return;
        }

        // Default (tier 0 / tier 1) or memory-backed: enqueue immediately.
        ret.errorCode = 0;
        enqueueReturn_(ret);
    }

    // -------------------------------------------------------------------
    // executePushReadWire_ — read a chunk from the log for a push subscription.
    //
    // Returns WorkerReturn::PushReadResult carrying the chunk + context.
    // The gateway iterates the chunk, encodes Records frames, and sends them.
    // No tail for PushRead ops.
    // -------------------------------------------------------------------
    void executePushReadWire_(const uint8_t* /*frame*/, const WorkerOpWire* hdr) {
        WorkerReturn ret;
        ret.type                 = WorkerReturn::Type::PushReadResult;
        ret.subscriptionId       = hdr->subscriptionId;
        ret.sessionId            = hdr->sessionId; // gateway needs this to find the session
        ret.readTopicId          = hdr->topicId;
        ret.readPartition        = hdr->partition;
        ret.newNextOffset        = hdr->nextOffset; // will be advanced by gateway on success
        ret.hwBound              = hdr->hwBound;
        ret.isolation            = hdr->isolation;
        ret.isGroup              = hdr->isGroup != 0;
        ret.groupCarrierReceived = hdr->groupCarrierReceived != 0;

        rune::storage::Log* log = getLogFromWire_(hdr);
        if (!log) {
            // No log: return empty chunk (gateway will skip this subscription).
            enqueueReturn_(ret);
            return;
        }

        // Only read if there are records available (within the HW-bound).
        if (hdr->nextOffset < hdr->hwBound) {
            try {
                // kMaxChunkBytes constant mirrored here; must stay in sync with
                // ClientGateway.  Gateway sets hwBound before enqueuing.
                static constexpr int32_t kMaxChunkBytes =
                    static_cast<int32_t>(512 * 1024); // 512 KiB
                ret.chunk = log->read(hdr->nextOffset, kMaxChunkBytes);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: log->read threw for topicId={} partition={}: {}",
                             workerId_, hdr->topicId, hdr->partition, ex.what());
                // Return empty chunk — gateway will skip + retry next cycle.
            } catch (...) {
                spdlog::warn("Worker {}: log->read threw unknown exception", workerId_);
            }
        }

        enqueueReturn_(ret);
    }

    // -------------------------------------------------------------------
    // executeApplyReplicatedWire_ — apply a replicated FetchResponse to the
    // owning follower replica.  Fire-and-forget (no WorkerReturn needed).
    //
    // The tail view IS the records bytes. We build a FetchResponseMsg whose
    // records span points into the ring view, then call applyReplicated within
    // this callback — applyReplicated copies into the follower log before we return.
    // -------------------------------------------------------------------
    void executeApplyReplicatedWire_(const uint8_t* frame, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeApplyReplicatedWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        const uint8_t* tail    = workerOpTail(frame);
        const uint32_t tailLen = hdr->tailLen;

        rune::replication::FetchResponseMsg msg;
        msg.leaderEpoch    = hdr->replLeaderEpoch;
        msg.partition      = hdr->replPartition;
        msg.highWatermark  = hdr->replHighWatermark;
        msg.logStartOffset = hdr->replLogStartOffset;
        msg.errorCode      = hdr->replErrorCode;
        msg.topicId        = hdr->replTopicId;
        // Zero-copy span: records view into the ring frame.
        // Valid only inside this callback — applyReplicated copies before we return.
        msg.records = std::span<const uint8_t>(
            tail ? tail : static_cast<const uint8_t*>(nullptr),
            static_cast<std::size_t>(tailLen));

        try {
            replica->applyReplicated(msg);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: applyReplicated threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: applyReplicated threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // executeBecomeLeaderWire_ — apply a replica role-change (Leader) on the
    // worker thread.  Fire-and-forget (no WorkerReturn needed).
    //
    // The tail is the ISR list as NodeId[] (int32_t[], native LE).
    // BecomeLeader is a rare path — alloc for the vector is acceptable.
    // -------------------------------------------------------------------
    void executeBecomeLeaderWire_(const uint8_t* frame, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeBecomeLeaderWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        // Rebuild ISR vector from the tail (rare path, alloc OK).
        const uint8_t* tail    = workerOpTail(frame);
        const uint32_t tailLen = hdr->tailLen;
        std::vector<rune::replication::NodeId> isr;
        if (tail && tailLen > 0) {
            const std::size_t count = static_cast<std::size_t>(tailLen) /
                                       sizeof(rune::replication::NodeId);
            isr.resize(count);
            std::memcpy(isr.data(), tail, count * sizeof(rune::replication::NodeId));
        }

        const rune::replication::Epoch epoch =
            static_cast<rune::replication::Epoch>(hdr->roleEpoch);

        try {
            replica->becomeLeader(epoch, std::move(isr));
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeLeader threw for topicId={} partition={}: {}",
                         workerId_, hdr->topicId, hdr->partition, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: becomeLeader threw unknown exception", workerId_);
        }

        // E5.1.5: PSM reconstruction on ownership GAIN (worker thread).
        //
        // When this worker becomes leader of a partition, its ProducerStateManager
        // must reflect the last sequence per producer so idempotent dedup holds
        // across a leadership move.  Without this, a retried produce spanning the
        // move could be re-appended (false Accept) — acknowledged data at a
        // different (new) offset, i.e. data duplication.
        //
        // Reuse the same logic as ClientGateway::rebuildProducerStateFromLogs
        // (ClientGateway.cpp:474) adapted for the worker-owned PSM:
        //   - Scan only the committed log tail (HW-bound, NOT LEO) so we never
        //     seed from uncommitted batches that may be truncated by KIP-101
        //     divergence resolution (same TX-2b correctness argument).
        //   - Fallback to LEO when HW is not yet known (replica not yet
        //     initialized, or passthrough/single-node mode).
        //   - rebuildFromLog is idempotent for this (topicId, partition) pair —
        //     it clears and re-seeds only this partition's entries, leaving other
        //     partitions' state intact.
        //
        // Shared-nothing: runs on the WORKER thread (this function is called from
        // executeBecomeLeaderWire_ which is called from drainIngress_, which runs
        // on the worker's duty-cycle thread).  No cross-thread access to psm_.
        //
        // Cold path: alloc OK (rebuildFromLog is a log scan, not on the hot path).
        if (perWorkerIngressActive_ && replica->isDiskBacked()) {
            rune::storage::Log* log = replica->log();
            if (log) {
                // TX-2b: use committed HW, not LEO, as the scan upper bound.
                // highWatermark() is safe to call here on the worker thread — the
                // worker owns all HW writes (E5.1.2c: FetchRequest/ApplyReplicated
                // are on the worker thread; becomeLeader itself sets the initial HW).
                int64_t hw = replica->highWatermark();
                if (hw <= 0) {
                    // HW not yet established (very first becomeLeader): fall back
                    // to LEO.  A new leader with no replication history has no
                    // uncommitted tail, so LEO == HW is safe.
                    hw = log->logEndOffset();
                }
                try {
                    psm_.rebuildFromLog(hdr->topicId, *log, hw, hdr->partition);
                    spdlog::info("Worker {}: PSM rebuilt for topicId={} partition={} "
                                 "hw={} entries={}",
                                 workerId_, hdr->topicId, hdr->partition, hw,
                                 psm_.size());
                } catch (const std::exception& ex) {
                    spdlog::error("Worker {}: PSM rebuild threw for topicId={} "
                                  "partition={}: {}; idempotency window may be stale",
                                  workerId_, hdr->topicId, hdr->partition, ex.what());
                } catch (...) {
                    spdlog::error("Worker {}: PSM rebuild threw unknown exception for "
                                  "topicId={} partition={}",
                                  workerId_, hdr->topicId, hdr->partition);
                }
            }
        }
    }

    // -------------------------------------------------------------------
    // executeBecomeFollowerWire_ — apply a replica role-change (Follower) on
    // the worker thread.  Fire-and-forget (no WorkerReturn needed).
    //
    // Scalars only (no tail for BecomeFollower).
    // -------------------------------------------------------------------
    void executeBecomeFollowerWire_(const uint8_t* /*frame*/, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeBecomeFollowerWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }
        const rune::replication::Epoch  epoch    =
            static_cast<rune::replication::Epoch>(hdr->roleEpoch);
        const rune::replication::NodeId leaderId =
            static_cast<rune::replication::NodeId>(hdr->roleLeaderId);
        try {
            replica->becomeFollower(epoch, leaderId);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeFollower threw for topicId={} partition={}: {}",
                         workerId_, hdr->topicId, hdr->partition, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: becomeFollower threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // executeSetOwnershipWire_ — replace this worker's owned_ set.
    //
    // The tail is an array of uintptr_t (one per PartitionReplica*).
    // Shared-nothing ([[rune-shared-nothing-law]]): applied on THIS worker
    // thread — no cross-thread write to owned_.
    // -------------------------------------------------------------------
    void executeSetOwnershipWire_(const uint8_t* frame, const WorkerOpWire* hdr) {
        const uint8_t* tail    = workerOpTail(frame);
        const uint32_t tailLen = hdr->tailLen;

        owned_.clear();
        if (tail && tailLen > 0) {
            const std::size_t count = static_cast<std::size_t>(tailLen) / sizeof(uintptr_t);
            owned_.reserve(count);
            for (std::size_t i = 0; i < count; ++i) {
                uintptr_t pval = 0;
                std::memcpy(&pval, tail + i * sizeof(uintptr_t), sizeof(uintptr_t));
                owned_.push_back(reinterpret_cast<rune::replication::PartitionReplica*>(pval));
            }
        }
    }

    // -------------------------------------------------------------------
    // E5.1.2c: executeFetchRequestWire_ — leader handles a follower FetchRequest
    // on the worker thread so leaderState_/isrManager_ is single-owner.
    // The FetchResponse is emitted via replica->onFetchRequest which calls
    // transport_->sendFetchResponse → enqueueOutbound (MPSC outbound ring).
    // Fire-and-forget: no WorkerReturn needed (FetchResponse goes outbound).
    // -------------------------------------------------------------------
    void executeFetchRequestWire_(const uint8_t* /*frame*/, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeFetchRequestWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        rune::replication::FetchRequestMsg req;
        req.leaderEpoch = static_cast<rune::replication::Epoch>(hdr->frLeaderEpoch);
        req.partition   = static_cast<rune::replication::PartitionId>(hdr->partition);
        req.followerId  = static_cast<rune::replication::NodeId>(hdr->frFollowerId);
        req.fetchOffset = static_cast<rune::replication::Offset>(hdr->frFetchOffset);
        req.maxBytes    = hdr->frMaxBytes;
        req.topicId     = hdr->topicId;

        try {
            replica->onFetchRequest(req, hdr->frNowNs);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: onFetchRequest threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: onFetchRequest threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // E5.1.2c: executeOffsetsForLeaderEpochReqWire_ — leader handles KIP-101
    // epoch probe on the worker thread (touches leaderState_/isrManager_).
    // Fire-and-forget: the response is emitted via transport_->sendOffsetsForLeaderEpochResponse.
    // -------------------------------------------------------------------
    void executeOffsetsForLeaderEpochReqWire_(const uint8_t* /*frame*/, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeOffsetsForLeaderEpochReqWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        rune::replication::OffsetsForLeaderEpochReqMsg req;
        req.currentEpoch   = static_cast<rune::replication::Epoch>(hdr->oleCurrentEpoch);
        req.partition      = static_cast<rune::replication::PartitionId>(hdr->partition);
        req.followerId     = static_cast<rune::replication::NodeId>(hdr->oleFollowerId);
        req.requestedEpoch = static_cast<rune::replication::Epoch>(hdr->oleRequestedEpoch);
        req.followerLeo    = static_cast<rune::replication::Offset>(hdr->oleFollowerLeo);
        req.topicId        = hdr->topicId;

        try {
            replica->onOffsetsForLeaderEpochRequest(req);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: onOffsetsForLeaderEpochRequest threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: onOffsetsForLeaderEpochRequest threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // E5.1.2c: executeReconcileFetchRespWire_ — deficient leader applies
    // reconcile records pulled from an ahead survivor, on the worker thread.
    // Reuses replicatedPayload scalars + records tail (same layout as ApplyReplicated).
    // Fire-and-forget: no WorkerReturn needed.
    // -------------------------------------------------------------------
    void executeReconcileFetchRespWire_(const uint8_t* frame, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeReconcileFetchRespWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        const uint8_t* tail    = workerOpTail(frame);
        const uint32_t tailLen = hdr->tailLen;

        rune::replication::FetchResponseMsg msg;
        msg.leaderEpoch    = hdr->replLeaderEpoch;
        msg.partition      = hdr->replPartition;
        msg.highWatermark  = hdr->replHighWatermark;
        msg.logStartOffset = hdr->replLogStartOffset;
        msg.errorCode      = hdr->replErrorCode;
        msg.topicId        = hdr->replTopicId;
        msg.records = std::span<const uint8_t>(
            tail ? tail : static_cast<const uint8_t*>(nullptr),
            static_cast<std::size_t>(tailLen));

        try {
            replica->applyReconcileResponse(msg);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: applyReconcileResponse threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: applyReconcileResponse threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // Task #13: executeAppendControlRecordWire_ — append a txn COMMIT/ABORT
    // control marker to the worker-owned user partition log.
    //
    // Called when the gateway routes an appendControlRecord op here because
    // per-worker ingress is active (the worker is the sole appender for this
    // partition — no race).  The gateway pre-computed the base offset from
    // log->logEndOffset() before enqueueing and stored the pre-stamped marker
    // bytes in the tail (same layout as Produce).  Here we just call
    // replica->append(marker, acksAll=true) on the worker thread; append()
    // reads logEndOffset() again internally — fine since the worker is the sole
    // appender and no other thread touched the log in between.
    //
    // Fire-and-forget: no WorkerReturn needed (the gateway already has the
    // pre-computed base offset and calls notifyControlMarker synchronously).
    // -------------------------------------------------------------------
    void executeAppendControlRecordWire_(const uint8_t* frame, const WorkerOpWire* hdr) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        if (!replica) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ — null replica for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        const uint8_t* tail    = workerOpTail(frame);
        const uint32_t tailLen = hdr->tailLen;

        if (!tail || tailLen == 0) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ — empty marker tail for "
                         "topicId={} partition={}", workerId_, hdr->topicId, hdr->partition);
            return;
        }

        const std::span<const uint8_t> markerSpan(tail, static_cast<std::size_t>(tailLen));

        try {
            const rune::replication::Offset base = replica->append(markerSpan, /*acksAll=*/true);
            if (base < 0) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — replica->append returned {} "
                             "(minIsr gate?) topicId={} partition={}",
                             workerId_, base, hdr->topicId, hdr->partition);
            }
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ threw unknown", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // getLogFromWire_ — resolve the Log* for the op's (topicId, partition)
    //                   from the WorkerOpWire scalars.
    //
    // Mirrors the old getLog_(const WorkerOp&) but reads from the wire header.
    // -------------------------------------------------------------------
    rune::storage::Log* getLogFromWire_(const WorkerOpWire* hdr) const noexcept {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(hdr->replica);
        // Prefer replica->log() for disk-backed (avoids separate registry lookup).
        if (replica && replica->isDiskBacked()) {
            return replica->log();
        }
        // Fall back to direct Log* pointer stored in wire header (set by gateway
        // at enqueue time to avoid a registry lookup in doWork).
        if (hdr->logPtr != 0) {
            return reinterpret_cast<rune::storage::Log*>(hdr->logPtr);
        }
        return nullptr;
    }

    int32_t workerId_;
    std::string threadName_; // "rune-wrk-<id>" — stable storage for name()

    // R2: ingress ring — OneToOneByteRing (SPSC, flat byte messages).
    // Producer (control thread): tryEnqueueOp tryClaim()s + encodes the WorkerOp
    //   DIRECTLY into the claimed slot (zero-copy) + commit()s.
    // Consumer (worker thread):  drainIngress_ drains → zero-copy view → executeFromWire_.
    OneToOneByteRing ingress_;

    // R3: return ring — OneToOneByteRing (SPSC, flat byte messages).
    // Producer (worker thread): enqueueReturn_ tryClaim()s + encodes the WorkerReturn
    //   DIRECTLY into the claimed slot (zero-copy) + commit()s.
    // Consumer (control thread): drainReturn() drains → decode view → fn(WorkerReturn).
    OneToOneByteRing ret_;

    // Owned replica set — SHARED-NOTHING ([[rune-shared-nothing-law]]): a PLAIN
    // member touched ONLY by the worker thread. No atomic, no cross-thread read.
    // Written via setOwnedDirect() at startup (before runners start, single-
    // threaded) and via SetOwnership wire ops at runtime (applied in
    // executeSetOwnershipWire_ on this worker's own thread). The control thread
    // NEVER touches it.
    OwnedSet owned_{};

    // E3: per-worker async I/O backend.
    // Owned exclusively by this worker thread — submit()/reap() are called ONLY
    // from executeProduce_() and reapDurableCompletions_() which both run on
    // the worker thread.  No shared state ([[rune-shared-nothing-law]]).
    IoBackend io_;

    // E3: deferred Strict-tier produce acks waiting for fsync completion.
    //
    // token: 0 (kNoToken) means "appended but not yet submitted to IoBackend" —
    //   submitCoalescedFlushes_() assigns the real token after drainIngress_.
    //   reapDurableCompletions_() skips token==0 entries (not yet flushed).
    // logPtr: the disk Log this entry was appended to (used by
    //   submitCoalescedFlushes_ to match the dirty-log set and stamp tokens).
    // lastOffset: the log offset after this append.  A group-commit completion
    //   with flushBoundary B fires every pending entry with lastOffset <= B.
    //
    // Worker-thread only — plain vector, no atomics needed.
    struct DurablePending {
        CompletionToken         token      = 0;      ///< 0 = not yet submitted
        int64_t                 lastOffset = 0;
        rune::storage::Log*     logPtr     = nullptr; ///< which log (for coalescing)
        WorkerReturn            ret;
    };
    std::vector<DurablePending> durablePending_;

    // E3 Task 5: per-cycle set of logs that have had at least one Strict-tier
    // append this cycle.  submitCoalescedFlushes_ drains this and submits one
    // io_.submit per log.  Worker-thread only — plain vector, reserved in ctor.
    std::vector<rune::storage::Log*> dirtyThisCycle_;

    // E3 Task 5: cumulative count of successful io_.submit calls in
    // submitCoalescedFlushes_.  Used by tests (ioSubmitCountForTest()).
    uint64_t io_submitCount_ = 0;

    // E4: CPU affinity — core to pin this worker's AgentRunner thread to.
    // -1 = disabled (default, no pinning).  Set via setCoreId() at startup,
    // before the AgentRunner starts.  Read-only at runtime (single writer).
    int  coreId_ = -1;

    // E4: true after the one-shot self-pin attempt fires in doWork.
    // Prevents repeated pin attempts on every doWork call.
    bool pinned_ = false;

    // E4: pin the calling thread to the given core.
    // Linux: uses pthread_setaffinity_np.  macOS: deliberate no-op (returns false).
    // Called ONCE per worker, from doWork on the AgentRunner thread.
    static bool pinCurrentThreadToCore_(int core) noexcept {
#if defined(__linux__)
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(core, &set);
        return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
        (void)core;
        return false; // no-op on macOS and other non-Linux platforms
#endif
    }

    // -----------------------------------------------------------------------
    // E5.1.2d: per-worker produce path (WorkerIngress → handleProduceBatch → ack).
    // -----------------------------------------------------------------------

    // Per-worker ProducerStateManager — idempotency for this worker's owned partitions.
    // Worker-thread only (single-writer per (log, PSM) pair — shared-nothing).
    //
    // TODO(E5.1.5): At migration time, reconstruct PSM from the log tail via
    //   reconstructPsmFromLog(partition) seam (left as an explicit hook here).
    //   For now, a fresh PSM at construction is safe (loses idempotency window
    //   across restarts, but that is acceptable for the E5.1.2d gate).
    rune::storage::ProducerStateManager psm_{};

    // E5.1.2d: gate — true when the worker should poll workerIngress_ for produce.
    // Default FALSE so the existing gateway fanout remains the live path and
    // nothing double-processes.  E5.1.3 flips this ON and retires the fanout.
    bool perWorkerIngressActive_ = false;

    // Non-owning pointer to this worker's Aeron ingress subscription.
    // Null until setWorkerIngress() is called.  Worker-thread read only (doWork).
    WorkerIngress* workerIngress_ = nullptr;

    // read-rework T3/T4: non-owning pointer to this worker's OWN dynamic-MDC egress
    // publication.  Null until setWorkerEgress() is called.  The worker thread is
    // the SOLE writer (shared-nothing); the control thread never touches it.
    WorkerEgress* workerEgress_ = nullptr;

    // read-rework T3/T4: worker-local subscription cursors.  Worker-thread-only;
    // Subscribe/Unsubscribe ring ops mutate it, the I3 push loop iterates it.
    // Reserved at construction so the common case never reallocates on the (rare,
    // control-plane) Subscribe path.
    static constexpr std::size_t kSubsReserve = 4096;
    std::vector<WorkerSubscription> subs_;

    // read-rework T3/T4 (I3): per-worker encode scratch for the in-worker push
    // loop.  Holds one encoded Records frame (header + one whole record batch).
    // Reused every push; NO per-batch heap allocation on the read hot path.
    // Sized like the gateway's push scratch (68 KiB: 50-byte header + up to a
    // 65535-byte u16-capped batch, rounded up).
    static constexpr std::size_t kEgressScratchSize = 68 * 1024;
    // Max whole batches pushed per subscription per doWork cycle (fairness).
    static constexpr int         kPushBatchesPerSubPerCycle = 1024;
    // Max bytes fetched per subscription per cycle via readView (matches gateway).
    static constexpr int32_t     kPushMaxChunkBytes = static_cast<int32_t>(512 * 1024);
    std::vector<uint8_t> egressScratch_;

    // E5.1.2d: acks=all commit-wait pending list.
    //
    // When a produce with acks=all arrives via WorkerIngress, we append it
    // immediately (like acks=leader) but do NOT ack yet.  Instead we push a
    // PendingAcksAll entry here with the last offset and the correlation info.
    // Each cycle in flushOwedAcksAll_() we check each entry against
    // replica->highWatermark() (safe: worker owns all HW writes).  When HW
    // passes lastOffset the ack fires; if the timeout elapses first we ack with
    // a timeout error (never drop — G-A6 no-silent-failure).
    //
    // Worker-thread only — plain vector, reserved in ctor.
    struct PendingAcksAll {
        rune::replication::PartitionReplica* replica    = nullptr;
        int64_t                              lastOffset = 0;   ///< fire when replica HW > this
        int64_t                              deadlineNs = 0;   ///< monotonic ns deadline
        WorkerReturn                         ret;               ///< ack payload when ready
    };
    std::vector<PendingAcksAll> pendingAcksAll_;

    // E5.1.2d: G-A6 overflow ack queue.
    //
    // When ret_.write() fails inside enqueueReturn_ (ring full) on the
    // WorkerIngress path, the ack is pushed here instead of being dropped.
    // flushRetOverflow_() retries these acks at the top of the next doWork
    // cycle.  The G-A6 gate (retOverflow_.empty()) prevents new ingress
    // polling while overflow acks are pending.
    //
    // Worker-thread only — plain vector, reserved in ctor.
    std::vector<WorkerReturn> retOverflow_;

    // E5.1.2d: acks=all commit-wait timeout (ns).
    int64_t ackTimeoutNs_ = kDefaultAckTimeoutNs;

    // E5.1.3 (d) R7 fix: last time syncOwnedLogs_ was run (ns). 0 = never.
    // Worker-thread only — no atomic needed.
    int64_t lastSyncNs_ = 0;

    // E5.1.3: desired fragment poll limit for reservation-before-poll.
    // K = min(desiredFragmentLimit_, freeMaxMsgSlots − retOverflow_.size()).
    // Source: NodeConfig::gatewayFragmentLimit (RUNE_GATEWAY_FRAGMENT_LIMIT, default 256).
    // Set via setDesiredFragmentLimit() before the runner starts.
    //
    // retOverflow_ hard cap: retOverflow_ is structurally bounded at K entries
    // per poll call since K ack slots were reserved before the poll.  When
    // retOverflow_ is non-empty, the G-A6 gate in doWork suppresses new polling
    // entirely (retOverflow_.empty() is required) — so the overflow vector never
    // accumulates more than one poll-batch (K entries) of acks.
    int32_t desiredFragmentLimit_ = 256;

    // -----------------------------------------------------------------------
    // syncOwnedLogs_ — time-gated Log::sync for worker-owned user-partition logs.
    //
    // E5.1.3 (d) R7 fix: called from doWork when perWorkerIngressActive_ is true.
    // Mirrors the gateway's flushDirtyLogs() group-commit interval (10 ms).
    //
    // For each owned disk-backed replica (user partition, topicId >= 0):
    //   - Call log->sync() to flush page-cache writes to disk.
    //   - If sync throws, log the error (same as gateway: don't crash, just retry).
    //
    // Worker-thread only.  Returns number of sync calls that completed.
    // -----------------------------------------------------------------------

    // Group-commit sync interval (mirrors gateway's kGroupCommitIntervalNs = 10 ms).
    static constexpr int64_t kWorkerSyncIntervalNs = 10'000'000LL; // 10 ms

    int syncOwnedLogs_(int64_t nowNs) {
        // Time-gate: only flush when interval has elapsed.
        if (lastSyncNs_ == 0) {
            lastSyncNs_ = nowNs;
            return 0;
        }
        if (nowNs - lastSyncNs_ < kWorkerSyncIntervalNs) return 0;
        lastSyncNs_ = nowNs;

        int synced = 0;
        for (auto* replica : owned_) {
            if (!replica || !replica->isDiskBacked()) continue;
            rune::storage::Log* log = replica->log();
            if (!log) continue;
            try {
                log->sync();
                ++synced;
            } catch (const std::exception& ex) {
                spdlog::error("Worker {}: syncOwnedLogs_ log->sync() threw for "
                              "topicId={} partition={}: {}; log may be dirty",
                              workerId_, replica->topicId(), replica->partitionId(), ex.what());
            } catch (...) {
                spdlog::error("Worker {}: syncOwnedLogs_ log->sync() threw unknown exception "
                              "for topicId={} partition={}",
                              workerId_, replica->topicId(), replica->partitionId());
            }
        }
        return synced;
    }

    // -----------------------------------------------------------------------
    // flushRetOverflow_ — retry acks that could not be written to ret_ in a
    //                     prior cycle (G-A6: no silent drop on WorkerIngress path).
    //
    // Iterates retOverflow_; writes each ack to ret_ using enqueueReturn_.
    // Stops as soon as a write fails (ring still full) so as not to spin.
    // Entries that succeed are removed from the front of the vector.
    //
    // Worker-thread only.  Returns number of acks flushed.
    // -----------------------------------------------------------------------
    int flushRetOverflow_() {
        if (retOverflow_.empty()) return 0;

        int flushed = 0;
        // Iterate front-to-back; stop on first failure (ring still full).
        // We temporarily disable perWorkerIngressActive_ guard in enqueueReturn_
        // to avoid re-queuing into retOverflow_ during this flush pass.
        // We do this by calling ret_.write() directly here.
        while (!retOverflow_.empty()) {
            const WorkerReturn& ret = retOverflow_.front();
            const std::size_t n = encodedWorkerReturnSize(ret);
            aeron::util::index_t idx;
            try {
                idx = ret_.tryClaim(workerReturnMsgTypeId(ret), n);
            } catch (...) {
                // Encode/size error: discard this entry and continue.
                retOverflow_.erase(retOverflow_.begin());
                continue;
            }
            if (idx < 0) {
                break; // ring still full — stop; retry next cycle
            }
            encodeWorkerReturn(ret, ret_.base() + idx, n);  // zero-copy in place
            ret_.commit(idx);
            retOverflow_.erase(retOverflow_.begin());
            ++flushed;
        }
        return flushed;
    }

    // -----------------------------------------------------------------------
    // flushOwedAcksAll_ — fire acks=all acks whose replica HW has passed
    //                     the required offset, or whose timeout has elapsed.
    //
    // Shared-nothing: reads replica->highWatermark() on THIS worker thread.
    // Safe because the worker owns ALL HW writes after E5.1.2c.
    //
    // Called at the TOP of the perWorkerIngressActive_ block (before polling
    // ingress) to ensure acks are flushed before new produce is accepted.
    // Returns the number of acks fired.
    // -----------------------------------------------------------------------
    int flushOwedAcksAll_(int64_t nowNs) {
        if (pendingAcksAll_.empty()) return 0;

        int fired = 0;
        for (std::size_t i = 0; i < pendingAcksAll_.size(); ) {
            auto& entry = pendingAcksAll_[i];

            // HW-passed: the replica has committed past our last offset.
            const bool hwPassed = entry.replica &&
                (entry.replica->highWatermark() > entry.lastOffset);
            // Timeout: this ack has been waiting too long.
            const bool timedOut = (nowNs >= entry.deadlineNs);

            if (hwPassed || timedOut) {
                if (timedOut && !hwPassed) {
                    spdlog::warn("Worker {}: acks=all timeout for partition={} "
                                 "lastOffset={}; acking Internal error",
                                 workerId_, entry.ret.partition, entry.lastOffset);
                    // E5.1.3 review fix #1: use Internal (6) for acks=all timeout.
                    // The protocol has no RequestTimedOut code; ErrorCode 7 is
                    // UnknownTopic — sending 7 would cause the client to misinterpret
                    // a timeout as a missing topic, a client-facing correctness bug.
                    // Internal (6) is the correct "unexpected server-side failure" code.
                    entry.ret.errorCode =
                        static_cast<int16_t>(
                            rune::protocol::client::ErrorCode::Internal);
                } else {
                    entry.ret.errorCode = 0; // OK
                }
                enqueueReturn_(entry.ret);
                // O(1) erase by swap-with-back.
                pendingAcksAll_[i] = std::move(pendingAcksAll_.back());
                pendingAcksAll_.pop_back();
                ++fired;
                // Do NOT advance i — the swapped entry needs checking.
            } else {
                ++i;
            }
        }
        return fired;
    }

    // -----------------------------------------------------------------------
    // getLogAndReplicaForPartition_ — look up the Log* and PartitionReplica*
    //                                  for (topicId, partition) from owned_.
    //
    // Worker-thread only. O(N) over owned_ — acceptable for the produce path
    // (produce frame rate >> per-batch O(N) owned-set scan, and N is small).
    // Returns {nullptr, nullptr} if the partition is not owned by this worker.
    // -----------------------------------------------------------------------
    std::pair<rune::storage::Log*, rune::replication::PartitionReplica*>
    getLogAndReplicaForPartition_(int32_t topicId, int32_t partition) const noexcept {
        for (auto* replica : owned_) {
            if (!replica) continue;
            if (replica->topicId() == topicId &&
                static_cast<int32_t>(replica->partitionId()) == partition) {
                // Only disk-backed replicas expose a rune::storage::Log*.
                // Memory-backed (InMemoryLog) replicas are NOT supported on the
                // WorkerIngress produce path — the gateway fanout handles them
                // via the existing WorkerOp ring (which carries a logPtr).
                // Return nullptr for memory-backed so the caller sends an
                // Internal error rather than crash.
                rune::storage::Log* log =
                    replica->isDiskBacked() ? replica->log() : nullptr;
                return {log, replica};
            }
        }
        return {nullptr, nullptr};
    }

    // -----------------------------------------------------------------------
    // pollWorkerIngress_ — poll the worker's Aeron subscription for up to K
    //                       produce frames per call, run handleProduceBatch,
    //                       enqueue ack for each.
    //
    // E5.1.3: Reservation-before-poll (G-A6 + throughput).
    //
    // K = min(desiredFragmentLimit_, freeSlots − retOverflow_.size())
    //   where freeSlots = ret_.freeMaxMsgSlots().
    //
    // This preserves G-A6 (never poll more frames than there are available
    // return slots) while allowing K > 1 per doWork cycle — removing the
    // fragmentLimit=1 throughput cap from E5.1.2d.
    //
    // retOverflow_ cap: retOverflow_.size() is subtracted from the free-slot
    // count before computing K.  retOverflow_ is structurally bounded because
    // a non-empty overflow suppresses new ingress polling entirely (the G-A6
    // gate in doWork_ requires retOverflow_.empty() to proceed to this function).
    // When this function is called, retOverflow_.empty() is guaranteed by the
    // caller, so the subtraction is always zero at this call site — it is
    // included for defensive correctness if the call-site invariant ever changes.
    //
    // Never drops an ack: errors return error acks on ret_; acks=all go to
    // pendingAcksAll_ for deferred fire.
    // -----------------------------------------------------------------------
    int pollWorkerIngress_(int64_t nowNs) {
        if (!workerIngress_ || !workerIngress_->resolved()) return 0;

        // Reservation-before-poll (E5.1.3 deliverable b):
        //   K = min(desiredFragmentLimit_, freeSlots − retOverflow_.size())
        // Guard G-A6: never poll more frames than we can ack without dropping.
        const int64_t freeSlots = ret_.freeMaxMsgSlots();
        const int64_t overflow  = static_cast<int64_t>(retOverflow_.size());
        const int64_t available = (freeSlots > overflow) ? (freeSlots - overflow) : 0;
        const int32_t K = static_cast<int32_t>(
            std::min(static_cast<int64_t>(desiredFragmentLimit_), available));
        if (K <= 0) return 0; // no free slots — skip poll (G-A6 preserved)

        int polled = 0;
        workerIngress_->poll(
            K,   // E5.1.3: poll up to K frames (reservation guarantees K ack slots)
            [this, nowNs, &polled]
            (const uint8_t* data, std::size_t len, aeron::Header& /*hdr*/) {
                polled += handleWorkerIngressFrame_(data, len, nowNs);
            });
        return polled;
    }

    // -----------------------------------------------------------------------
    // handleWorkerIngressFrame_ — decode one Produce frame from WorkerIngress,
    //                              run handleProduceBatch, enqueue the ack.
    //
    // Frame format: full rune::protocol::client::Produce wire frame.
    // The data pointer is a zero-copy view into Aeron's term buffer; it is
    // valid only for the duration of this call.  handleProduceBatch receives a
    // mutable copy of the batch bytes (restamped in place).
    //
    // E5.1.3 review fixes applied here:
    //   #2: All error-code literals replaced with pc::ErrorCode enum values.
    //   #3: acksAll derivation: acks != 0 (broader than gateway's acks == 1, but
    //       safer — catches both acks=Leader (1) and any future all-in-sync flag).
    //       Gateway uses acks == 1; we use acks != 0 here for defensive correctness:
    //       the Produce wire field carries 0=NoAck, 1=Leader, -1=All; treating any
    //       non-zero value as "acks requested" prevents silent false-durability.
    // -----------------------------------------------------------------------
    int handleWorkerIngressFrame_(const uint8_t* data, std::size_t len,
                                  int64_t nowNs) {
        namespace pc = rune::protocol::client;

        // Decode the produce frame.
        auto msgOpt = pc::Produce::decode(data, len);
        if (!msgOpt) {
            spdlog::warn("Worker {}: WorkerIngress: failed to decode Produce frame "
                         "len={}; dropping", workerId_, len);
            return 0;
        }
        const pc::Produce& msg = *msgOpt;

        // Look up the owned Log + replica for this partition.
        auto [log, replica] = getLogAndReplicaForPartition_(msg.topicId, msg.partition);

        // Build a WorkerReturn shell for this ack.
        WorkerReturn ret;
        ret.type          = WorkerReturn::Type::ProduceResult;
        ret.sessionId     = msg.sessionId;
        ret.correlationId = msg.correlationId;
        ret.partition     = msg.partition;
        ret.topicId       = msg.topicId;
        ret.producerId    = msg.producerId;
        ret.producerEpoch = msg.producerEpoch;
        ret.baseSequence  = msg.baseSequence;
        ret.replica       = replica;

        if (!log) {
            // E5.1.5: distinguish "partition not owned by this worker" (retriable
            // NotLeader) from "owned but memory-backed log unsupported" (Internal).
            //
            // When getLogAndReplicaForPartition_ returns {nullptr, nullptr} the
            // partition is NOT in owned_ — this worker is no longer the leader.
            // The client must refresh metadata and re-route to the new owner.
            // Return NotLeader (retriable), NOT Internal, so the client can retry.
            //
            // When it returns {nullptr, replica} (replica != nullptr), the partition
            // IS owned but its log is memory-backed — the WorkerIngress path
            // requires a disk-backed log (InMemoryLog is handled by the gateway
            // fanout via the ingress_ ring).  Return Internal (non-retriable bug).
            //
            // Shared-nothing: no cross-worker forwarding per [[rune-shared-nothing-law]].
            if (!replica) {
                // Not owned: retriable NotLeader so client re-routes.
                spdlog::debug("Worker {}: WorkerIngress: partition not owned (topicId={} "
                              "partition={}); returning NotLeader (retriable)",
                              workerId_, msg.topicId, msg.partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            } else {
                // Owned but memory-backed: Internal.
                spdlog::warn("Worker {}: WorkerIngress: owned but memory-backed log for "
                             "topicId={} partition={}; Internal error ack",
                             workerId_, msg.topicId, msg.partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
            }
            enqueueReturn_(ret);
            return 1;
        }

        // Copy the batch bytes so handleProduceBatch can restamp in place.
        // The zero-copy Aeron view is only valid during this callback;
        // we need a mutable local copy for the in-place restamp.
        const std::span<const uint8_t> rbSpan = msg.recordBatch;
        if (rbSpan.empty()) {
            spdlog::warn("Worker {}: WorkerIngress: empty recordBatch topicId={} "
                         "partition={}; Internal error ack",
                         workerId_, msg.topicId, msg.partition);
            // E5.1.3 review fix #2: use enum, not magic literal.
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
            enqueueReturn_(ret);
            return 1;
        }

        // Local mutable copy (one allocation per batch on this path — acceptable
        // for the WorkerIngress path; the hot-path avoids this via the existing
        // ingress_ ring which does a single copy into the ring).
        std::vector<uint8_t> batchBuf(rbSpan.begin(), rbSpan.end());

        // E5.1.3 review fix #3: acksAll = (acks != 0).
        // The Produce wire field carries 0=NoAck, 1=Leader, -1 (0xFF)=All.
        // Using acks != 0 is safer than acks == 1 (the gateway's rule) because
        // it covers any non-zero acks field including potential future extension
        // values, preventing silent false-durability on unknown acks values.
        // The gateway uses acks == 1 for its inline path; both rules are equivalent
        // for the current two-value (0 / 1) protocol but we document the difference.
        const bool acksAll = (msg.acks != 0);

        // ISR size for the min-ISR gate.
        const int isrSize = replica ? static_cast<int>(replica->isr().size()) : 0;
        const int minIsr  = replica ? replica->minIsr() : 1;

        // Durability tier — for the WorkerIngress path we use Majority (tier=1)
        // so the worker's IoBackend group-commit handles fsync.
        constexpr uint8_t kDurabilityTier = 1;

        // Call the shared produce helper.
        rune::storage::ProduceOutcome outcome = rune::storage::handleProduceBatch(
            batchBuf.data(), batchBuf.size(),
            msg.topicId, msg.partition,
            msg.producerId, msg.producerEpoch, msg.baseSequence,
            *log, psm_,
            acksAll, isrSize, minIsr, kDurabilityTier);

        ret.baseOffset  = outcome.baseOffset;
        ret.lastOffset  = outcome.lastOffset;
        ret.recordCount = outcome.recordCount;
        ret.batchAttrs  = outcome.batchAttrs;

        if (!outcome.isOK()) {
            ret.errorCode = static_cast<int16_t>(
                static_cast<int>(outcome.error));
            enqueueReturn_(ret);
            return 1;
        }

        // GR-4 Mode A: advance the replica's LEO/HW after successful append.
        if (replica) {
            replica->onLeaderLogAppended();
        }

        if (!acksAll || outcome.isDuplicate) {
            // acks=leader / duplicate: ack immediately.
            ret.errorCode = 0;
            enqueueReturn_(ret);
            return 1;
        }

        // acks=all (non-duplicate): register a pending ack; fire when HW passes.
        // Timeout: nowNs + ackTimeoutNs_ (generous — accounts for fetch-handling
        // latency added by E5.1.2c, where fetch waits a worker cycle).
        PendingAcksAll pending;
        pending.replica    = replica;
        pending.lastOffset = outcome.lastOffset;
        pending.deadlineNs = nowNs + ackTimeoutNs_;
        pending.ret        = std::move(ret);
        pendingAcksAll_.push_back(std::move(pending));
        return 1;
    }
};

} // namespace rune::node
