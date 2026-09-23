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
#include <rune/node/IoBackend.h>
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

    // R2: ingress ring byte capacity (power-of-two payload bytes).
    // maxMsgLength() = capacity / 8 = 1 MiB — sufficient for any produce batch.
    // TODO(R4): thread NodeConfig.workerIngressByteCapacity here.
    static constexpr int kIngressByteCapacity = 8 * 1024 * 1024; // 8 MiB

    // R3: return ring byte capacity (power-of-two payload bytes).
    // maxMsgLength() = capacity / 8 = 1 MiB — exceeds the 512 KiB max PushRead chunk.
    // TODO(R4): thread NodeConfig.workerReturnByteCapacity here.
    static constexpr int kReturnByteCapacity = 8 * 1024 * 1024; // 8 MiB

    // Legacy alias — callers that pass Worker::kRingCapacity to the ctor still
    // compile; the value is now ignored (both rings use their *ByteCapacity constants).
    static constexpr std::size_t kRetRingCapacity = 256;  // kept for back-compat
    static constexpr std::size_t kRingCapacity    = kRetRingCapacity;

    // Default IoBackend ring depths (power-of-two).  NodeConfig wiring (Task 7)
    // will pass these explicitly; for now the constructor defaults are sensible.
    static constexpr std::size_t kIoSubmitDepth     = 1024;
    static constexpr std::size_t kIoCompletionDepth = 1024;

    // Pre-reserve capacity for the durablePending_ vector (no alloc on hot path).
    static constexpr std::size_t kDurablePendingReserve = 1024;

    // Pre-reserve capacity for the per-cycle dirty-log set (dedup, small).
    static constexpr std::size_t kDirtySetReserve = 64;

    // ctor: ringCapacity is kept for source-compat — it was the SpscRing slot
    // count; now it is IGNORED (ingress uses kIngressByteCapacity; ret_ uses
    // kReturnByteCapacity). Callers that pass Worker::kRingCapacity still compile.
    explicit Worker(int32_t workerId,
                    std::size_t ringCapacity    = kRingCapacity,  // ignored (back-compat)
                    std::size_t ioSubmitDepth   = kIoSubmitDepth,
                    std::size_t ioComplDepth    = kIoCompletionDepth) noexcept
        : workerId_(workerId)
        , ingress_(kIngressByteCapacity)
        , ingressScratch_(sizeof(WorkerOpWire) +
                          static_cast<std::size_t>(ingress_.maxMsgLength()))
        , ret_(kReturnByteCapacity)
        , returnScratch_(sizeof(WorkerReturnWire) +
                         static_cast<std::size_t>(ret_.maxMsgLength()))
        , io_(ioSubmitDepth, ioComplDepth)
    {
        (void)ringCapacity; // kept for source-compat; no longer used
        durablePending_.reserve(kDurablePendingReserve);
        dirtyThisCycle_.reserve(kDirtySetReserve);
    }

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
        // Encode into the per-worker scratch (no heap allocation).
        // ingressScratch_ is sized to sizeof(WorkerOpWire) + maxMsgLength(),
        // which comfortably exceeds the largest produce batch (maxMsgLength = 1 MiB).
        const std::size_t n = encodeWorkerOp(op, ingressScratch_.data(),
                                             ingressScratch_.size());
        // Write into the agrona ring (1 copy: scratch → ring).
        // Returns false when the ring is full → back-pressure; caller must retry.
        return ingress_.write(ingressScratch_.data(), n, workerOpMsgTypeId(op));
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
    // -----------------------------------------------------------------------
    // enqueueReturn_ — R3: encode a WorkerReturn into returnScratch_ and
    //                  write it to the ret_ OneToOneByteRing.
    //
    // OLD BEHAVIOUR (SpscRing): ret_.tryEnqueue() was [[nodiscard]] but its
    // return value was NEVER checked at any call site — i.e. a full ring caused
    // a silent drop of the return message.
    // NEW BEHAVIOUR (R3): ret_.write() returns false when full → same silent-
    // drop.  A warn is emitted to make this observable.
    // TODO(R5): audit full-ret_ back-pressure; consider retry/hold-and-retry.
    //
    // Worker-thread only (the producer side of ret_).
    // -----------------------------------------------------------------------
    void enqueueReturn_(const WorkerReturn& ret) {
        std::size_t n = 0;
        try {
            n = encodeWorkerReturn(ret, returnScratch_.data(), returnScratch_.size());
        } catch (const std::exception& ex) {
            spdlog::error("Worker {}: enqueueReturn_ encode failed: {}", workerId_, ex.what());
            return;
        }
        if (!ret_.write(returnScratch_.data(), n, workerReturnMsgTypeId(ret))) {
            // TODO(R5): add retry or back-pressure propagation.
            spdlog::warn("Worker {}: ret_ ring full — WorkerReturn type={} dropped",
                         workerId_, static_cast<int>(ret.type));
        }
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
        }
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

    // R2: ingress ring — agrona OneToOneByteRing (SPSC, flat byte messages).
    // Producer (control thread): tryEnqueueOp encodes into ingressScratch_ then write().
    // Consumer (worker thread):  drainIngress_ drains → zero-copy view → executeFromWire_.
    OneToOneByteRing ingress_;

    // Per-worker encode scratch for ingress (producer-side, control-thread-owned).
    // Sized to sizeof(WorkerOpWire) + ingress_.maxMsgLength() at construction.
    // No alloc on the hot path — encodeWorkerOp writes into this buffer.
    std::vector<uint8_t> ingressScratch_;

    // R3: return ring — agrona OneToOneByteRing (SPSC, flat byte messages).
    // Producer (worker thread): encodeWorkerReturn into returnScratch_ then ret_.write().
    // Consumer (control thread): drainReturn() drains → decode view → fn(WorkerReturn).
    OneToOneByteRing ret_;

    // Per-worker encode scratch for return (producer-side, worker-thread-owned).
    // Sized to sizeof(WorkerReturnWire) + ret_.maxMsgLength() at construction.
    // No alloc on the hot path — encodeWorkerReturn writes into this buffer.
    std::vector<uint8_t> returnScratch_;

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
};

} // namespace rune::node
