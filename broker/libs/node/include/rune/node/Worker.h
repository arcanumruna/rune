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
#include <rune_fr/FlightRecorder.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/node/WorkerOp.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/WorkerOp.h>
#include <rune_protocol_internal_sbe/WorkerReturn.h>
#include <rune_protocol_internal_sbe/OpType.h>
#include <rune_protocol_internal_sbe/ReturnType.h>
#include <rune_protocol_internal_sbe/TailKind.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerEgress.h>
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/node/IoBackend.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/ProducePath.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>  // E5.1.3: pc::ErrorCode enum (replaces magic literals)
#include <rune/codec/Validator.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>

// E4: CPU affinity helper — self-pin the calling thread to a specific core.
// Linux-only implementation; macOS is a deliberate no-op (mac build stays GREEN).
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace rune::node {

// read-rework SBE-B: the worker ring codec now uses the SBE-generated flyweights.
namespace sbe = rune::protocol::internal::sbe;

// F3 round 6, item 5: tryEnqueueOp outcome.  Full = back-pressure (retry next
// cycle); TooLarge = the op can NEVER fit this worker's ingress ring (the
// caller must not retry it — drop/refuse with an error naming the size).
enum class EnqueueStatus : uint8_t { Ok, Full, TooLarge };
struct [[nodiscard]] EnqueueResult {
    EnqueueStatus status = EnqueueStatus::Ok;
    constexpr explicit operator bool() const noexcept { return status == EnqueueStatus::Ok; }
    [[nodiscard]] constexpr bool full() const noexcept     { return status == EnqueueStatus::Full; }
    [[nodiscard]] constexpr bool tooLarge() const noexcept { return status == EnqueueStatus::TooLarge; }
};

class Worker final : public rune::consensus::Agent {
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

    // Task 6: per-worker replication transport poll fragment limit.
    // Matches desiredFragmentLimit_ (256) so both ingress paths use a consistent
    // per-cycle budget.  No back-pressure mechanics needed here (fire-and-forget
    // replication paths: FetchRequest, FetchResponse, OLE, reconcile).
    static constexpr int kReplFetchLimit = 256;

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
        initRoutes_(ioSubmitDepth > ioComplDepth ? ioSubmitDepth : ioComplDepth);
        dirtyThisCycle_.reserve(kDirtySetReserve);
        pendingAcksAll_.reserve(kPendingAcksAllReserve);
        retOverflow_.reserve(kRetOverflowReserve);
        subs_.reserve(kSubsReserve);
        validators_.reserve(kValidatorsReserve);
        egressScratch_.resize(kEgressScratchSize);
        threadName_ = "rune-wrk-" + std::to_string(workerId_);
        maxProduceBatchBytes_ = maxOpTailBytes();   // F3 r5 C2: sane default; BrokerNode installs
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
    void setOwnedDirect(OwnedSet set) {
        owned_ = std::move(set);
        rebuildFlushSlots_();
    }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }

    // Self-contained worker shard (spec §3.1).  Set before the runner starts.
    void setShardMode(bool on) noexcept { shardMode_ = on; }
    [[nodiscard]] bool shardMode() const noexcept { return shardMode_; }

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
    // E5 Task 5 / Task 6: wire this worker's per-worker replication transport.
    //
    // Non-owning: BrokerNode owns the WorkerReplicationTransport lifetime.
    // Must be called BEFORE the worker's AgentRunner starts.
    //
    // Task 6: stores a (ctx, poll-trampoline) pair so doWork can call poll()
    // without std::function and without virtual dispatch on the hot path.
    // The trampoline is a plain function pointer; ctx is the typed transport
    // pointer erased to void*.  This mirrors WorkerReplicationTransportImpl's
    // own trampoline_ / ctx_ idiom and WorkerIngress::poll exactly.
    //
    // Production path: takes WorkerReplicationTransport* (AeronPubRegistry alias).
    // Test-seam: setReplicationTransportForTest<T> accepts any Impl<T> without
    // requiring AeronPubRegistry — used by unit tests that inject a
    // WorkerReplicationTransportImpl<SharedFakePubRegistry>.  Zero overhead:
    // trampoline is a non-capturing lambda → plain function pointer.
    // -----------------------------------------------------------------------
    void setReplicationTransport(WorkerReplicationTransport* transport) noexcept {
        setReplicationTransportImpl_(transport);
        workerReplTransport_ = transport;
    }

    // Test-seam overload: accepts any WorkerReplicationTransportImpl<T>.
    // Sets the trampoline but leaves the typed workerReplTransport_ null so
    // the replicationTransport() accessor remains production-only.
    template <typename T>
    void setReplicationTransportForTest(WorkerReplicationTransportImpl<T>* transport) noexcept {
        setReplicationTransportImpl_(transport);
    }

    [[nodiscard]] WorkerReplicationTransport* replicationTransport() const noexcept {
        return workerReplTransport_;
    }

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
        /// False while the partition is not owned by this worker (ownership
        /// moved): parked, no reads of the moved replica / log.
        bool    active               = true;
        uint32_t registryId          = 0;   ///< target replica identity (Task 10 fix 3)
        uint32_t incarnation         = 0;
        // Hybrid egress: per-consumer UNICAST egress publication for this
        // subscription's consumer session.  Created + lifetime-owned by the
        // gateway; written ONLY here on the worker thread (single-writer,
        // shared-nothing).  nullptr → fall back to the shared MDC pub
        // (workerEgress_) for broadcast/replication.
        aeron::ExclusivePublication* egressPub = nullptr;
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
    // Per-tick drain batch limits (NodeConfig knobs — see NodeConfig.h).  Call
    // before the worker's AgentRunner starts (startup, single-threaded).
    void setIngressDrainLimit(int32_t n) noexcept { ingressDrainLimit_ = (n >= 1) ? n : 1024; }
    void setReturnDrainLimit(int32_t n)  noexcept { returnDrainLimit_  = (n >= 1) ? n : 1024; }

    // Configure the per-produce-ack return-ring reservation slot size (bytes).  The
    // reservation-before-poll uses this as the expected ProduceResult size to compute
    // how many produce frames it can safely poll; the OLD code reserved 2×maxMsgLength
    // per ack (an 8 MB ret_ ring → only ~4 slots → K≤4 → throttled produce intake).
    // Must be >= the max WorkerReturn ProduceResult size (~160 B framed).
    // Source: NodeConfig::workerAckSlotBytes (RUNE_WORKER_ACK_SLOT_BYTES, default 256).
    void setAckSlotBytes(int32_t n) noexcept {
        ackSlotBytes_ = (n >= 64) ? n : 64;
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
    // -----------------------------------------------------------------------
    // F3 round 5, item I1 (single source of truth): the largest variable tail
    // (record batch / replicated records / ...) a WorkerOp can carry through
    // this worker's ingress O2O ring.  agrona checkMsgLength() THROWS for a
    // message above maxMsgLength (= capacity/8), and the framed op is
    //   SBE header (8) + WorkerOp sbeBlockLength() + tail length prefix (4) + tail
    // (generated-codec constants).  maxOpTailBytesFor() computes it from the
    // configured ring size (BrokerNode's derivation); maxOpTailBytes() from the
    // live ring; tryEnqueueOp() enforces it before tryClaim().
    // -----------------------------------------------------------------------
    static constexpr std::size_t kWorkerOpFrameOverhead =
        sbe::MessageHeader::encodedLength() + sbe::WorkerOp::sbeBlockLength() +
        sbe::WorkerOp::tailHeaderLength();

    [[nodiscard]] static constexpr std::size_t maxOpTailBytesFor(std::size_t ingressRingBytes) noexcept {
        const std::size_t maxMsg = ingressRingBytes / 8;   // O2ORingBuffer: capacity / 8
        return maxMsg > kWorkerOpFrameOverhead ? maxMsg - kWorkerOpFrameOverhead : 0;
    }
    [[nodiscard]] std::size_t maxOpTailBytes() const noexcept {
        const std::size_t maxMsg = static_cast<std::size_t>(ingress_.maxMsgLength());
        return maxMsg > kWorkerOpFrameOverhead ? maxMsg - kWorkerOpFrameOverhead : 0;
    }
    /// True when a tail of tailLen bytes fits this worker's ingress ring (one compare).
    [[nodiscard]] bool fitsIngressTail(std::size_t tailLen) const noexcept {
        return tailLen <= maxOpTailBytes();
    }

    // -----------------------------------------------------------------------
    // F3 round 5, item C2: the EFFECTIVE max produce-batch size this worker
    // accepts on its per-worker ingress path (WorkerIngress frames, including
    // idempotent/transactional produce).  Installed by BrokerNode at build time
    // (cold, before the runner starts) with effectiveMaxProduceBatchBytes() —
    // the same value the gateway enforces.  Defaults to this worker's own ring
    // tail max (a sane bound for unit tests that never install one).
    // -----------------------------------------------------------------------
    void setMaxProduceBatchBytes(std::size_t bytes) noexcept { maxProduceBatchBytes_ = bytes; }
    [[nodiscard]] std::size_t maxProduceBatchBytes() const noexcept { return maxProduceBatchBytes_; }
    [[nodiscard]] uint64_t oversizeProduceRejectsForTest() const noexcept { return oversizeProduceRejects_; }
    [[nodiscard]] uint64_t oversizeProduceRejectLogsForTest() const noexcept { return oversizeProduceRejectLogs_; }

    /// Test seam: run one per-worker ingress Produce frame through the real
    /// handler (no Aeron subscription needed).
    int handleWorkerIngressFrameForTest(const uint8_t* data, std::size_t len, int64_t nowNs) {
        return handleWorkerIngressFrame_(data, len, nowNs);
    }

    EnqueueResult tryEnqueueOp(WorkerOp op) {
        // SBE wrap-in-place: reserve the exact ring slot (computeLength — no
        // hand-rolled size), then encode the WorkerOp DIRECTLY into the claimed
        // buffer via the SBE flyweight (no intermediate struct, no memcpy).
        // tryClaim() < 0 → ring full → explicit back-pressure (caller holds+retries).

        // Determine the variable tail (src, len, kind) from the op type — the ONE
        // place tail selection lives (mirrors the former encodeWorkerOp switch).
        const uint8_t* tailSrc  = nullptr;
        std::size_t    tailLen  = 0;
        sbe::TailKind::Value tailKind = sbe::TailKind::Value::NONE;
        switch (op.type) {
        case WorkerOp::Type::Produce:
        case WorkerOp::Type::AppendControlRecord:
            tailKind = sbe::TailKind::Value::RECORD_BATCH;
            // Zero-copy: prefer the read-only view (aliases the Aeron fragment,
            // valid for this synchronous call) so putTail copies the batch ONCE
            // into the ring with no intermediate heap vector.  Fall back to the
            // owning recordBatchBytes vector when no view was supplied.
            if (!op.recordBatchView.empty()) {
                tailSrc = op.recordBatchView.data();
                tailLen = op.recordBatchView.size();
            } else {
                tailSrc = op.recordBatchBytes.data();
                tailLen = op.recordBatchBytes.size();
            }
            break;
        case WorkerOp::Type::ApplyReplicated:
        case WorkerOp::Type::ReconcileFetchResp:
            tailKind = sbe::TailKind::Value::REPLICATED_RECORDS;
            tailSrc  = op.replicatedPayload.records.data();
            tailLen  = op.replicatedPayload.records.size();
            break;
        case WorkerOp::Type::BecomeLeader:
            tailKind = sbe::TailKind::Value::ISR_LIST;
            tailSrc  = reinterpret_cast<const uint8_t*>(op.isr.data());
            tailLen  = op.isr.size() * sizeof(rune::replication::NodeId);
            break;
        case WorkerOp::Type::SetOwnership:
            tailKind = sbe::TailKind::Value::OWNED_SET;
            tailSrc  = reinterpret_cast<const uint8_t*>(op.ownedSet.data());
            tailLen  = op.ownedSet.size() * sizeof(uintptr_t);
            break;
        case WorkerOp::Type::SchemaUpdate:
            // Task 7: descriptor blob carried as the variable tail.
            // topicId is already encoded in the scalar field.
            tailKind = sbe::TailKind::Value::DESCRIPTOR_BLOB;
            tailSrc  = op.schemaUpdateBlob.data();
            tailLen  = op.schemaUpdateBlob.size();
            break;
        default:
            break;  // NONE tail
        }

        // F3 round 5, item I1: an oversize tail must NEVER reach tryClaim()
        // (agrona checkMsgLength throws).  Refuse it here — callers with a
        // variable tail pre-check with fitsIngressTail() (gateway via the
        // effective max, transport for replicated records), so this is a
        // backstop; rate-limited (producer-thread-owned counter).
        if (tailLen > maxOpTailBytes()) [[unlikely]] {
            if (oversizeOpRejects_++ % 1024 == 0) {
                spdlog::error("Worker {}: tryEnqueueOp refused op type={} tail={} B > ring max "
                              "tail {} B (rejects={})", workerId_,
                              static_cast<int>(op.type), tailLen, maxOpTailBytes(),
                              oversizeOpRejects_);
            }
            return {EnqueueStatus::TooLarge};
        }

        // Total frame = SBE MessageHeader (8) + block + varData.  computeLength()
        // covers block+varData only, so add the header explicitly.
        const std::size_t n =
            sbe::MessageHeader::encodedLength() + sbe::WorkerOp::computeLength(tailLen);
        const aeron::util::index_t idx =
            ingress_.tryClaim(sbe::WorkerOp::sbeTemplateId(), n);
        if (idx < 0) return {EnqueueStatus::Full};  // ring full — back-pressure

        sbe::WorkerOp enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(ingress_.base() + idx), 0, n);
        enc.opType(static_cast<sbe::OpType::Value>(static_cast<std::uint8_t>(op.type)))
           .topicId(op.topicId)
           .partition(op.partition)
           .replica(reinterpret_cast<std::uint64_t>(op.replica))
           .logPtr(reinterpret_cast<std::uint64_t>(op.logPtr))
           .sessionId(op.sessionId)
           .correlationId(op.correlationId)
           .acks(op.acks)
           .producerId(op.producerId)
           .producerEpoch(op.producerEpoch)
           .baseSequence(op.baseSequence)
           .lastOffsetDelta(op.lastOffsetDelta)
           .recordCount(op.recordCount)
           .batchAttrs(op.batchAttrs)
           .deferAck(op.deferAck ? 1 : 0)
           .durabilityTier(op.durabilityTier)
           .enqueuedAtNs(op.enqueuedAtNs)
           .subscriptionId(op.subscriptionId)
           .nextOffset(op.nextOffset)
           .hwBound(op.hwBound)
           .isolation(op.isolation)
           .isGroup(op.isGroup ? 1 : 0)
           .groupCarrierReceived(op.groupCarrierReceived ? 1 : 0)
           .roleEpoch(static_cast<std::int64_t>(op.roleEpoch))
           .roleLeaderId(static_cast<std::int32_t>(op.roleLeaderId))
           .replLeaderEpoch(op.replicatedPayload.leaderEpoch)
           .replPartition(op.replicatedPayload.partition)
           .replHighWatermark(op.replicatedPayload.highWatermark)
           .replLogStartOffset(op.replicatedPayload.logStartOffset)
           .replErrorCode(op.replicatedPayload.errorCode)
           .replTopicId(op.replicatedPayload.topicId)
           .frLeaderEpoch(static_cast<std::int64_t>(op.fetchRequestPayload.leaderEpoch))
           .frFollowerId(static_cast<std::int32_t>(op.fetchRequestPayload.followerId))
           .frFetchOffset(static_cast<std::int64_t>(op.fetchRequestPayload.fetchOffset))
           .frMaxBytes(op.fetchRequestPayload.maxBytes)
           .frNowNs(op.fetchRequestPayload.nowNs)
           .oleCurrentEpoch(static_cast<std::int64_t>(op.offsEpochReqPayload.currentEpoch))
           .oleFollowerId(static_cast<std::int32_t>(op.offsEpochReqPayload.followerId))
           .oleRequestedEpoch(static_cast<std::int64_t>(op.offsEpochReqPayload.requestedEpoch))
           .oleFollowerLeo(static_cast<std::int64_t>(op.offsEpochReqPayload.followerLeo))
           .egressPub(reinterpret_cast<std::uint64_t>(op.egressPub))
           .targetReplicaId(op.replica ? op.replica->registryId() : 0u)
           .incarnation(op.replica ? op.replica->incarnation() : 0u)
           .ownershipGen(op.ownershipGen)
           .tailKind(tailKind);
        enc.putTail(reinterpret_cast<const char*>(tailSrc),
                    static_cast<std::uint32_t>(tailLen));
        ingress_.commit(idx);
        return {EnqueueStatus::Ok};
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
                char* frame = reinterpret_cast<char*>(buf.buffer()) + off;

                // SBE zero-copy decode: wrap the ring frame, read fields in place.
                sbe::MessageHeader mh;
                mh.wrap(frame, 0, 0, static_cast<std::uint64_t>(buf.capacity()) - off);
                sbe::WorkerReturn dec;
                dec.wrapForDecode(frame, sbe::MessageHeader::encodedLength(),
                                  mh.blockLength(), mh.version(),
                                  static_cast<std::uint64_t>(buf.capacity()) - off);

                // Reconstruct the WorkerReturn struct for the consumer callback.
                WorkerReturn ret;
                ret.type                 = static_cast<WorkerReturn::Type>(
                                               static_cast<std::uint8_t>(dec.returnType()));
                ret.sessionId            = dec.sessionId();
                ret.correlationId        = dec.correlationId();
                ret.partition            = dec.partition();
                ret.baseOffset           = dec.baseOffset();
                ret.lastOffset           = dec.lastOffset();
                ret.errorCode            = dec.errorCode();
                ret.deferAck             = dec.deferAck() != 0;
                ret.replica              = reinterpret_cast<
                    rune::replication::PartitionReplica*>(dec.replica());
                ret.producerId           = dec.producerId();
                ret.producerEpoch        = dec.producerEpoch();
                ret.baseSequence         = dec.baseSequence();
                ret.recordCount          = dec.recordCount();
                ret.topicId              = dec.topicId();
                ret.batchAttrs           = dec.batchAttrs();
                ret.enqueuedAtNs         = dec.enqueuedAtNs();
                ret.subscriptionId       = dec.subscriptionId();
                ret.readTopicId          = dec.readTopicId();
                ret.readPartition        = dec.readPartition();
                ret.newNextOffset        = dec.newNextOffset();
                ret.hwBound              = dec.hwBound();
                ret.isolation            = dec.isolation();
                ret.isGroup              = dec.isGroup() != 0;
                ret.groupCarrierReceived = dec.groupCarrierReceived() != 0;

                // Tail: PushReadResult chunk — copy from the ring view (1 copy) so
                // fn can access it safely after the callback returns.
                const std::uint32_t chunkLen = dec.chunkLength();
                if (chunkLen > 0) {
                    const uint8_t* c = reinterpret_cast<const uint8_t*>(dec.chunk());
                    ret.chunk.assign(c, c + chunkLen);
                }

                fn(std::move(ret));
            },
            returnDrainLimit_);   // configurable (RUNE_WORKER_RETURN_DRAIN_LIMIT); was 16
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

    // Task 10 (spec §3.1 never-block / fail-stop) test hooks — owner thread or
    // single-threaded tests only.
    [[nodiscard]] std::size_t bgFlushInFlightForTest() const noexcept { return bgInFlight_; }
    [[nodiscard]] uint64_t    bgFlushSubmitsForTest() const noexcept { return bgFlushSubmits_; }
    [[nodiscard]] uint64_t    bgFlushErrorsForTest() const noexcept { return bgFlushErrors_; }
    [[nodiscard]] uint64_t    bgFlushDeferredForTest() const noexcept { return bgFlushDeferred_; }
    [[nodiscard]] uint64_t    sealSubmitsForTest() const noexcept { return sealSubmits_; }
    [[nodiscard]] uint64_t    storageFailureSignalsForTest() const noexcept { return storageFailureSignals_; }
    [[nodiscard]] std::size_t flushSlotCountForTest() const noexcept { return slots_.size(); }
    /// Completions drained from io_ by reapDurableCompletions_ (any token).
    [[nodiscard]] uint64_t    ioCompletionsReapedForTest() const noexcept { return ioCompletionsReaped_; }
    /// Token whose completion fired the most recent durable (Strict) ack.
    [[nodiscard]] CompletionToken durableAckTokenForTest() const noexcept { return durableAckToken_; }
    [[nodiscard]] std::size_t durablePendingCountForTest() const noexcept { return durablePending_.size(); }
    [[nodiscard]] int64_t     reservedAckSlotsForTest() const noexcept { return reservedAckSlots_(); }
    /// -1 when this worker has no flush slot for `log`.
    [[nodiscard]] int64_t lastSyncedLeoForTest(const rune::storage::Log* log) noexcept {
        const LogFlushSlot* slot = slotOf_(log);
        return slot ? slot->lastSyncedLeo : -1;
    }
    /// Hold io completions in the backend (no reap) so tests deliver them.
    void setReapPausedForTest(bool on) noexcept { reapPausedForTest_ = on; }
    /// Every reaped completion is rewritten to carry `err` (0 = off).
    void forceFlushErrorForTest(int err) noexcept { forcedFlushErrorForTest_ = err; }
    /// Requests a background flush of an OWNED log; returns its token, or
    /// kNoToken when coalesced into one already in flight / deferred.
    CompletionToken submitBgFlushForTest(rune::storage::Log* log) {
        LogFlushSlot* slot = slotOf_(log);
        if (!slot) return kNoToken;
        return requestBgFlush_(*slot) ? slot->bgToken : kNoToken;
    }
    [[nodiscard]] CompletionToken durablePendingTokenForTest(std::size_t i) const {
        return durablePending_.at(i).token;
    }
    /// Routes a completion exactly as a real reap does (token routing first).
    void deliverCompletionForTest(const FlushCompletion& c) { (void)onFlushCompletion_(c); }
    /// Feeds one client Produce frame to the worker-ingress handler.
    int handleIngressFrameForTest(const uint8_t* data, std::size_t len, int64_t nowNs) {
        return handleWorkerIngressFrame_(data, len, nowNs);
    }
    [[nodiscard]] const char* ioBackendNameForTest() const noexcept { return io_.name(); }
    [[nodiscard]] uint64_t opsRefusedNotOwnedForTest() const noexcept { return opsRefusedNotOwned_; }
    [[nodiscard]] uint64_t ownershipAcksForTest() const noexcept { return ownershipAcks_; }
    /// Bench / test: the per-op ownership check.
    [[nodiscard]] bool ownsReplicaForTest(const rune::replication::PartitionReplica* r) const noexcept {
        return ownsIncarnation_(r->registryId(), r->incarnation());
    }
    /// Bench: the per-op guard exactly as the dispatch runs it.
    [[nodiscard]] bool ownsIncarnationForTest(uint32_t id, uint32_t inc) const noexcept {
        return ownsIncarnation_(id, inc);
    }
    /// False when RUNE_IO_BACKEND selects a backend that cannot submit — shard
    /// mode refuses to start on such a worker (BrokerNode).
    [[nodiscard]] bool ioAcceptsSubmissions() const noexcept { return io_.acceptsSubmissions(); }

    // Task 7: test accessor — true if a Validator is installed for the given topic.
    // Worker-thread only; call only from single-threaded tests.
    [[nodiscard]] bool hasValidatorForTest(int32_t topicId) const noexcept {
        return validators_.count(topicId) > 0;
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
        // Per-tick span: inter-tick gap = scheduler handoff/park; duration = tick cost.
        RUNE_FR_SPAN_RUNTIME(rune_fr::Site::WorkerDoWork, 0);
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

        if (shardMode_) return doWorkShard_(nowNs);

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
            // Pipelined acks=all: poll new produce as long as the return ring has
            // space for the acks about to be polled PLUS all already-parked acks
            // (pendingAcksAll_ fire later, into ret_).  Previously the gate required
            // pendingAcksAll_.empty(), which SERIALIZED acks=all on the ~172ms
            // replication round-trip (no new intake while any ack was in flight →
            // throughput = batch / round-trip).  Now pendingAcksAll_ is bounded by
            // ret_ capacity (reservation below subtracts its size) instead of 1.
            if (retOverflow_.empty() &&
                ret_.freeSlotsForMsgSize(ackSlotBytes_) >
                    static_cast<int64_t>(pendingAcksAll_.size())) {
                work += pollWorkerIngress_(nowNs);
            }
        }

        // Step 0: drain ingress ring (E2 inc2a / R2). This may include SetOwnership
        // ops that mutate owned_ (on THIS worker thread — single-owner, no lock).
        // Strict-tier produces record their log in dirtyThisCycle_ (token=0).
        {
            RUNE_FR_SPAN_RUNTIME(rune_fr::Site::WorkerDrainIngress, 0);
            work += static_cast<int>(drainIngress_());
        }

        // Step E3 coalesce: after all this-cycle produces are appended, submit
        // ONE fsync per dirty log and stamp the completion token onto every pending
        // entry for that log.  submitCoalescedFlushes_ clears dirtyThisCycle_.
        work += submitCoalescedFlushes_();

        // Shared-nothing: owned_ is a plain member touched only by this thread.
        // Step 5: per-replica ISR checks / follower fetch emission.
        {
            RUNE_FR_SPAN_RUNTIME(rune_fr::Site::WorkerReplicaDoWorkStage, 0);
            for (auto* replica : owned_) {
                if (!replica) continue;
                work += replica->doWork(nowNs);
            }
        }

        // Task 6: per-worker replication transport poll — inline dispatch.
        //
        // After driving owned replicas, drain inbound replication frames from the
        // per-worker Aeron subscription and apply each frame INLINE to the owning
        // replica on THIS worker thread.  Zero-copy: the frame pointer is valid
        // only inside the callback; the replica copies into its log before the
        // callback returns (same contract as executeApplyReplicatedWire_ ~1443-1471).
        //
        // replTransportCtx_ / replTransportPollFn_ is a (void* ctx, fn*) trampoline
        // set by setReplicationTransportImpl_<T>: the function pointer casts ctx back
        // to the concrete WorkerReplicationTransportImpl<T> and calls its templated
        // poll() with the dispatch lambda.  No std::function, no virtual dispatch —
        // identical idiom to WorkerReplicationTransportImpl's own ctx_/trampoline_.
        //
        // Shared-nothing: replTransportCtx_ is null when the transport is not wired
        // → the poll is never called (flag-off path unchanged).  The dispatch lambda
        // passed to poll() is a non-capturing function pointer (zero overhead).
        if (replTransportCtx_) {
            RUNE_FR_SPAN_RUNTIME(rune_fr::Site::WorkerReplPollDrive, 0);
            work += replTransportPollFn_(replTransportCtx_, kReplFetchLimit,
                                        this, nowNs);
        }

        // read-rework T3/T4 (I3): in-worker push — read owned partitions and push
        // Records directly on this worker's egress.  Runs after replica doWork so
        // the HW bound is fresh.  No-op unless subs_ is populated (Subscribe ops)
        // AND a WorkerEgress is wired (pre-I3b: gateway still owns push → inert).
        {
            RUNE_FR_SPAN_RUNTIME(rune_fr::Site::WorkerEgressPushStage, 0);
            work += pushOwnedSubscriptions_(nowNs);
        }

        // Step 6.5: drain cleaner/retention/staging queues (disk-backed only).
        for (auto* replica : owned_) {
            if (replica && replica->isDiskBacked()) {
                work += replica->log()->drainCleanerSwaps();
                work += replica->log()->drainRetentionDeletes();
                work += replica->log()->drainStagingRenamesDone();
                // fsyncgate: any inline sync failure on this log → fail-stop.
                checkStorageFailure_(replica->log());
            }
        }
        work += flushPendingSignals_();

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
    // Shard pass order (spec §3.1 steps 3-6).  The ShardAgent already ran the
    // driver (1) and client conductor (2).  Never blocks; never waits on another
    // component (a full return ring is a non-blocking intake skip, spec §3.6).
    int doWorkShard_(int64_t nowNs) {
        int work = 0;
        // 2b. runtime→worker ring first: ownership / role changes (and, Task 13,
        //     session announcements) must be applied before the intake they govern.
        work += static_cast<int>(drainIngress_());
        // 3. client ingress → append (G-A6 gate unchanged).
        work += flushRetOverflow_();
        // Owed acks (acks=all parked on HW, Strict parked on its fsync) all fire
        // into ret_ later: reserve for every one of them (G-A6, never drop).
        if (retOverflow_.empty() &&
            ret_.freeSlotsForMsgSize(ackSlotBytes_) > reservedAckSlots_()) {
            work += pollWorkerIngress_(nowNs);
        }
        work += submitCoalescedFlushes_();
        // 4. replication on this worker's plane: apply inbound (inline, fast-acks),
        //    then push / ISR / HW per owned replica.
        if (replTransportCtx_) work += replTransportPollFn_(replTransportCtx_, kReplFetchLimit, this, nowNs);
        for (auto* replica : owned_) {
            if (replica) work += replica->doWork(nowNs);
        }
        // 4b. follower flushes raised by the replicas (shard replicas run with
        //     setExternalFollowerFlush) → io backend, never an inline fsync.
        for (auto* replica : owned_) {
            if (replica && replica->takeFollowerFlushRequest() && replica->isDiskBacked()) {
                if (LogFlushSlot* slot = slotOf_(replica->log())) work += requestBgFlush_(*slot) ? 1 : 0;
            }
        }
        // 5. fire due produce acks.
        work += flushOwedAcksAll_(nowNs);
        // 6. reap durability completions.
        work += reapDurableCompletions_();
        // Cold tail: consumer egress push, cleaner drains, time-gated log sync.
        work += pushOwnedSubscriptions_(nowNs);
        for (auto& slot : slots_) {
            work += slot.log->drainCleanerSwaps();
            work += slot.log->drainRetentionDeletes();
            work += slot.log->drainStagingRenamesDone();
            if (slot.log->pendingSealCount() > 0) work += submitPendingSeals_(slot);   // async roll seals
            reconcileTruncation_(slot);
            checkStorageFailure_(slot.log);
        }
        work += syncOwnedLogsAsync_(nowNs);   // io backend, never Log::sync (spec §3.1)
        work += flushPendingSignals_();
        return work;
    }

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
    // SBE wrap-in-place encode of a WorkerReturn into the claimed ring slot at
    // ret_.base()+idx (no intermediate struct/memcpy).  Shared by enqueueReturn_
    // and flushRetOverflow_.
    void encodeReturnSbe_(const WorkerReturn& ret, aeron::util::index_t idx, std::size_t n) {
        sbe::WorkerReturn enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(ret_.base() + idx), 0, n);
        enc.returnType(static_cast<sbe::ReturnType::Value>(static_cast<std::uint8_t>(ret.type)))
           .sessionId(ret.sessionId)
           .correlationId(ret.correlationId)
           .partition(ret.partition)
           .baseOffset(ret.baseOffset)
           .lastOffset(ret.lastOffset)
           .errorCode(ret.errorCode)
           .deferAck(ret.deferAck ? 1 : 0)
           .replica(reinterpret_cast<std::uint64_t>(ret.replica))
           .producerId(ret.producerId)
           .producerEpoch(ret.producerEpoch)
           .baseSequence(ret.baseSequence)
           .recordCount(ret.recordCount)
           .topicId(ret.topicId)
           .batchAttrs(ret.batchAttrs)
           .enqueuedAtNs(ret.enqueuedAtNs)
           .subscriptionId(ret.subscriptionId)
           .readTopicId(ret.readTopicId)
           .readPartition(ret.readPartition)
           .newNextOffset(ret.newNextOffset)
           .hwBound(ret.hwBound)
           .isolation(ret.isolation)
           .isGroup(ret.isGroup ? 1 : 0)
           .groupCarrierReceived(ret.groupCarrierReceived ? 1 : 0);
        enc.putChunk(reinterpret_cast<const char*>(ret.chunk.data()),
                     static_cast<std::uint32_t>(ret.chunk.size()));
    }

    void enqueueReturn_(const WorkerReturn& ret) {
        // SBE wrap-in-place: reserve the exact ring slot (computeLength) then encode
        // the return DIRECTLY into it.  tryClaim() < 0 = explicit back-pressure.
        const std::size_t n = sbe::MessageHeader::encodedLength() +
                              sbe::WorkerReturn::computeLength(ret.chunk.size());
        aeron::util::index_t idx;
        try {
            idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n);
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
        encodeReturnSbe_(ret, idx, n);
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
            // Async roll seals first: a Strict entry may live in a segment sealed
            // this pass, and its ack waits for that seal (onFlushCompletion_).
            LogFlushSlot* slot = slotOf_(log);
            if (log->storageFailed()) [[unlikely]] {
                // fsyncgate: never touch a failed log's disk again; its pending
                // entries fail (StorageError), never OK.
                failOwnPending_(log, static_cast<int16_t>(rune::protocol::client::ErrorCode::StorageError), false);
                ++work;
                continue;
            }
            if (slot) work += submitPendingSeals_(*slot);
            CompletionToken t = submitActive_(log, RouteKind::Durable, slot ? slot->epoch : 0,
                                              /*pin=*/slot != nullptr);
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
    //
    // Task 10 token routing: every completion is offered to the background-flush
    // table FIRST (retireBgFlush_); a background token is retired there and can
    // never reach the produce-ack matching below.  Completions are always drained
    // (also when durablePending_ is empty) so background tokens are retired.
    int reapDurableCompletions_() {
        if (reapPausedForTest_) [[unlikely]] return 0;
        int fired = 0;
        io_.reap([&](const FlushCompletion& c) {
            ++ioCompletionsReaped_;
            if (forcedFlushErrorForTest_ != 0) [[unlikely]] {
                FlushCompletion forced = c;
                forced.errorCode = forcedFlushErrorForTest_;
                fired += onFlushCompletion_(forced);
            } else {
                fired += onFlushCompletion_(c);
            }
        });
        return fired;
    }

    // One completion.  Token routing first: a Background / Seal token is retired
    // here and never reaches produce-ack matching.  Durable entries match by
    // TOKEN ONLY (I2): an entry is stamped with T after its append and the
    // fdatasync covers every write issued before the submit — the completion's
    // flushBoundary is informational and never gates an ack.
    int onFlushCompletion_(const FlushCompletion& c) {
        TokenRoute r;
        const bool routed = routeTake_(c.token, r);
        LogFlushSlot* slot = routed ? liveSlot_(r) : nullptr;
        // M1: release the segment pin taken at submit (only while this worker
        // still owns the log — a purged slot's log may belong to another worker).
        if (routed && r.seg && slot) r.log->unpinSegmentFlush(r.seg);

        if (routed && r.kind == RouteKind::Background && slot && slot->bgToken == c.token) {
            slot->bgToken = kNoToken;
            if (bgInFlight_) --bgInFlight_;
        }
        if (routed && r.kind == RouteKind::Seal && slot && slot->sealsInFlight > 0) --slot->sealsInFlight;

        // C-1: the error FIRST — before any purged-slot early return.  An fsync
        // error is fail-stop whoever owns the log now (never retried).
        if (c.errorCode != 0) {
            if (routed && r.kind == RouteKind::Background) ++bgFlushErrors_;
            const rune::storage::Log* failed = routed ? r.log : nullptr;
            for (const auto& e : durablePending_)
                if (!failed && e.token == c.token) failed = e.logPtr;
            if (failed) {
                if (LogFlushSlot* owner = slotOf_(failed)) failLog_(owner->log, c.errorCode);
                else failForeignLog_(failed, c.errorCode);
            }
            return 1;
        }

        if (routed && r.kind != RouteKind::Durable) {
            if (!slot) return 1;                               // purged (M2): success ignored safely
            if (r.kind == RouteKind::Background) {
                if (slot->bgLeo > slot->lastSyncedLeo) slot->lastSyncedLeo = slot->bgLeo;
                if (slot->bgAgain) { slot->bgAgain = false; (void)requestBgFlush_(*slot); }
            } else {
                fireSealGated_(*slot);
            }
            return 1;
        }

        // Durable token (or an unrouted one: synthetic / already-retired).
        int fired = 0;
        for (std::size_t i = 0; i < durablePending_.size(); ) {
            auto& entry = durablePending_[i];
            if (entry.token != kNoToken && entry.token == c.token && !entry.awaitSeal) {
                if (sealsOutstanding_(entry.logPtr)) {
                    entry.awaitSeal = true;                     // durable in its active segment;
                    ++i;                                        // fires once the seals complete
                    continue;
                }
                fireDurable_(entry);
                durablePending_[i] = std::move(durablePending_.back());
                durablePending_.pop_back();
                ++fired;
            } else {
                ++i;
            }
        }
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
        // Drain a LARGE batch per tick: the routed replication ops (fetch/apply —
        // the HW-advancing fast-acks) must not queue behind a small per-tick cap.
        // (Default was 16 → under the acks=majority fast-ack storm the ingress ring
        // backed up multiple ticks; see control→worker handoff p99.)
        int drained = ingress_.drain(
            [this](int32_t /*typeId*/,
                   aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off,
                   aeron::util::index_t len)
            {
                // Zero-copy view: frame pointer is valid only inside this callback.
                // Fully consume the message (executeFromWire_) before returning.
                const uint8_t* frame =
                    reinterpret_cast<const uint8_t*>(buf.buffer()) + off;
                executeFromWire_(frame, static_cast<std::size_t>(len));
            },
            ingressDrainLimit_);   // configurable (RUNE_WORKER_INGRESS_DRAIN_LIMIT); was the 16 default
        return static_cast<std::size_t>(drained < 0 ? 0 : drained);
    }

    // -----------------------------------------------------------------------
    // executeFromWire_ — dispatch on WorkerOpWire::type and call the per-type
    //                    handler.  The frame pointer is a zero-copy view into the
    //                    agrona ring — valid only inside drainIngress_ callback.
    // -----------------------------------------------------------------------
    void executeFromWire_(const uint8_t* frame, std::size_t len) {
        // SBE zero-copy decode: wrap the ring frame; the flyweight reads fields in
        // place.  Dispatch on the opType field (the ingress ring carries a single
        // SBE message type, WorkerOp — templateId is constant).
        char* cbuf = reinterpret_cast<char*>(const_cast<uint8_t*>(frame));
        sbe::MessageHeader mh;
        mh.wrap(cbuf, 0, 0, len);
        sbe::WorkerOp op;
        op.wrapForDecode(cbuf, sbe::MessageHeader::encodedLength(),
                         mh.blockLength(), mh.version(), len);
        const auto type = static_cast<WorkerOp::Type>(static_cast<std::uint8_t>(op.opType()));
        // Ownership guard (Task 10 fix 3): every partition op — role ops
        // included — carries the target replica's (registry id, incarnation).
        // It runs only if THIS worker currently owns that exact incarnation: one
        // indexed load + compare + branch (ownedInc_ is rebuilt cold, on this
        // thread, when SetOwnership is applied).  A stale op (ownership moved,
        // replica removed, or a new replica at a reused address) is refused
        // without dereferencing anything it carries.  Unregistered replicas
        // (incarnation 0: tests / passthrough) and ownership-neutral ops
        // (SetOwnership, Unsubscribe, SchemaUpdate, TestTask) are not gated.
        if (const uint32_t inc = op.incarnation(); inc != 0 && gatedByOwnership_(type)) {
            const uint32_t id = op.targetReplicaId();
            if (id >= ownedInc_.size() || ownedInc_[id] != inc) [[unlikely]] {
                refuseNotOwned_(op, type);
                return;
            }
        }
        switch (type) {
        case WorkerOp::Type::Produce:
            executeProduceWire_(op);
            break;
        case WorkerOp::Type::PushRead:
            executePushReadWire_(op);
            break;
        case WorkerOp::Type::ApplyReplicated:
            // Worker-clock: dequeued a routed apply op.  corr=runSeq pairs 1:1 (FIFO)
            // with the control thread's ReplFragRecv → EXACT control→worker handoff.
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplWorkerOpRun, workerRunSeq_++);
            executeApplyReplicatedWire_(op);
            break;
        case WorkerOp::Type::BecomeLeader:
            executeBecomeLeaderWire_(op);
            break;
        case WorkerOp::Type::BecomeFollower:
            executeBecomeFollowerWire_(op);
            break;
        case WorkerOp::Type::SetOwnership:
            executeSetOwnershipWire_(op);
            break;
        // E5.1.2c: leader-side replication state handlers
        case WorkerOp::Type::FetchRequest:
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplWorkerOpRun, workerRunSeq_++);
            executeFetchRequestWire_(op);
            break;
        case WorkerOp::Type::OffsetsForLeaderEpochReq:
            executeOffsetsForLeaderEpochReqWire_(op);
            break;
        case WorkerOp::Type::ReconcileFetchResp:
            executeReconcileFetchRespWire_(op);
            break;
        // Task #13: AppendControlRecord — worker appends txn control marker
        case WorkerOp::Type::AppendControlRecord:
            executeAppendControlRecordWire_(op);
            break;
        // read-rework T3/T4: subscription lifecycle (worker owns the push cursor)
        case WorkerOp::Type::Subscribe:
            executeSubscribeWire_(op);
            break;
        case WorkerOp::Type::Unsubscribe:
            executeUnsubscribeWire_(op);
            break;
        // Task 7: admin → worker schema validator push (shared-nothing).
        case WorkerOp::Type::SchemaUpdate:
            executeSchemaUpdateWire_(op);
            break;
        // F1: TEST-ONLY owner-thread hook — cold branch, never sent by production.
        case WorkerOp::Type::TestTask:
            executeTestTaskWire_(op);
            break;
        }
    }

    // -------------------------------------------------------------------
    // F1: executeTestTaskWire_ — run a test-owned OwnerThreadTestTask on THIS
    // (owning) worker thread against the replica the control thread resolved.
    // The opaque task ref rides in the egressPub slot (see WorkerOp::TestTask).
    // Production code only moves the pointer; all completion synchronisation is
    // inside the test-owned task object.  Out-of-line + cold so the hot dispatch
    // switch is unaffected.
    // -------------------------------------------------------------------
    [[gnu::cold]] [[gnu::noinline]] void executeTestTaskWire_(sbe::WorkerOp& op) noexcept {
        runOwnerThreadTestTask(
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(op.egressPub())),
            reinterpret_cast<rune::replication::PartitionReplica*>(op.replica()));
    }

    // -------------------------------------------------------------------
    // read-rework T3/T4: Subscribe — register or update a worker-local
    // subscription cursor from the wire scalars.  Fire-and-forget (no return).
    // Idempotent: a repeated Subscribe for the same subscriptionId UPDATES the
    // existing entry (e.g. cursor rewind / carrier arrival) rather than
    // duplicating it.  Worker-thread-only mutation of subs_.
    // -------------------------------------------------------------------
    void executeSubscribeWire_(sbe::WorkerOp& op) {
        const int64_t subId = op.subscriptionId();
        for (auto& s : subs_) {
            if (s.subscriptionId == subId) {
                s.sessionId            = op.sessionId();
                s.topicId              = op.topicId();
                s.partition            = op.partition();
                s.nextOffset           = op.nextOffset();
                s.logPtr               = reinterpret_cast<rune::storage::Log*>(op.logPtr());
                s.replica              = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
                s.isolation            = op.isolation();
                s.isGroup              = op.isGroup() != 0;
                s.groupCarrierReceived = op.groupCarrierReceived() != 0;
                s.egressPub            = reinterpret_cast<aeron::ExclusivePublication*>(op.egressPub());
                s.registryId           = op.targetReplicaId();
                s.incarnation          = op.incarnation();
                s.active               = true;   // re-subscribe (passed the guard) re-activates
                return;
            }
        }
        WorkerSubscription s;
        s.subscriptionId       = subId;
        s.sessionId            = op.sessionId();
        s.topicId              = op.topicId();
        s.partition            = op.partition();
        s.nextOffset           = op.nextOffset();
        s.logPtr               = reinterpret_cast<rune::storage::Log*>(op.logPtr());
        s.replica              = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        s.isolation            = op.isolation();
        s.isGroup              = op.isGroup() != 0;
        s.groupCarrierReceived = op.groupCarrierReceived() != 0;
        s.egressPub            = reinterpret_cast<aeron::ExclusivePublication*>(op.egressPub());
        s.registryId           = op.targetReplicaId();
        s.incarnation          = op.incarnation();
        subs_.push_back(s);
    }

    // -------------------------------------------------------------------
    // read-rework T3/T4: Unsubscribe — remove a worker-local subscription by id
    // (swap-and-pop; order does not matter).  No-op if unknown.  Worker-thread-only.
    // -------------------------------------------------------------------
    void executeUnsubscribeWire_(sbe::WorkerOp& op) {
        const int64_t subId = op.subscriptionId();
        for (std::size_t i = 0; i < subs_.size(); ++i) {
            if (subs_[i].subscriptionId == subId) {
                subs_[i] = subs_.back();
                subs_.pop_back();
                return;
            }
        }
    }

    // -------------------------------------------------------------------
    // Task 7: executeSchemaUpdateWire_ — install a Validator for a topic on
    // this worker's own thread (shared-nothing).
    //
    // The descriptor blob arrives in the ring tail (TailKind = DESCRIPTOR_BLOB).
    // We deserialize it here (off-hot-path, admin/control-plane) and store the
    // resulting Validator in validators_ keyed by topicId.  The hot path
    // (executeProduceWire_) does a single-branch lookup; if no entry exists
    // (validation off / not yet pushed) it falls through with zero overhead.
    //
    // Shared-nothing: validators_ is a plain member touched ONLY by this worker
    // thread — no lock, no atomic, no cross-thread mutation.  The blob is copied
    // from the ring tail into the validator at construction; the ring frame is
    // released immediately after this call returns.
    //
    // Off-hot-path allocation: the Validator constructor and the map insert/assign
    // allocate on the WORKER thread (not in the produce path) — HARD RULE 4 is met.
    // -------------------------------------------------------------------
    void executeSchemaUpdateWire_(sbe::WorkerOp& op) {
        const int32_t topicId  = op.topicId();
        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());

        if (!tail || tailLen == 0) {
            spdlog::warn("Worker {}: executeSchemaUpdateWire_ — empty blob for topicId={}; "
                         "validator not installed", workerId_, topicId);
            return;
        }

        try {
            rune::codec::Descriptor desc =
                rune::codec::deserialize(std::span<const uint8_t>(tail, tailLen));
            validators_.insert_or_assign(topicId, rune::codec::Validator(desc));
            spdlog::debug("Worker {}: SchemaUpdate installed validator for topicId={}",
                          workerId_, topicId);
        } catch (const std::exception& ex) {
            spdlog::error("Worker {}: executeSchemaUpdateWire_ — deserialize failed "
                          "for topicId={}: {}", workerId_, topicId, ex.what());
        } catch (...) {
            spdlog::error("Worker {}: executeSchemaUpdateWire_ — unknown exception "
                          "for topicId={}", workerId_, topicId);
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
    // Send one encoded egress frame to a raw ExclusivePublication (unicast per
    // consumer OR the shared MDC pub).  Zero-copy: tryClaim for <= maxPayload,
    // offer() (auto-fragment) otherwise — same policy as WorkerEgress::send.
    // Returns the Aeron result (>0 accepted; <=0 back-pressure/not-connected).
    static int64_t sendEncodedToPub_(aeron::ExclusivePublication* pub,
                                     const uint8_t* data, std::size_t len) {
        if (!pub) [[unlikely]] return -1;
        auto* mutableBuf = const_cast<uint8_t*>(data);
        aeron::concurrent::AtomicBuffer buf(mutableBuf, static_cast<aeron::util::index_t>(len));
        if (len <= static_cast<std::size_t>(pub->maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            const int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(claim.offset(), buf, 0,
                                        static_cast<aeron::util::index_t>(len));
                claim.commit();
            }
            return pos;
        }
        return pub->offer(buf, 0, static_cast<aeron::util::index_t>(len));
    }

    int pushOwnedSubscriptions_(int64_t /*nowNs*/) {
        using namespace rune::protocol::batch;
        namespace pc = rune::protocol::client;
        if (subs_.empty()) return 0;

        int work = 0;
        for (auto& s : subs_) {
            if (!s.logPtr || !s.active) continue;   // parked: partition moved away

            // Hybrid egress: prefer this consumer's UNICAST pub (independent
            // per-consumer offsets); fall back to the shared MDC pub (broadcast).
            aeron::ExclusivePublication* egressPub = s.egressPub;
            if (!egressPub && workerEgress_) egressPub = workerEgress_->pub();
            if (!egressPub) continue;  // nowhere to send yet

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
                const int64_t r = sendEncodedToPub_(egressPub, egressScratch_.data(), encoded);
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
    void executeProduceWire_(sbe::WorkerOp& op) {
        RUNE_FR_SPAN_PRODUCE(rune_fr::Site::WorkerAppend,
                             static_cast<uint32_t>(op.correlationId()));
        const int32_t topicId   = op.topicId();
        const int32_t partition = op.partition();
        WorkerReturn ret;
        ret.type          = WorkerReturn::Type::ProduceResult;
        ret.sessionId     = op.sessionId();
        ret.correlationId = op.correlationId();
        ret.partition     = partition;
        ret.deferAck      = op.deferAck() != 0;
        ret.replica       = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        ret.producerId    = op.producerId();
        ret.producerEpoch = op.producerEpoch();
        ret.baseSequence  = op.baseSequence();
        ret.recordCount   = op.recordCount();
        ret.topicId       = topicId;
        ret.batchAttrs    = op.batchAttrs();
        ret.enqueuedAtNs  = op.enqueuedAtNs();

        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        auto* log     = getLogFromWire_(op);

        if (!log) {
            spdlog::error("Worker {}: executeProduceWire_ — no Log for topicId={} partition={}; "
                          "sending Internal error",
                          workerId_, topicId, partition);
            ret.errorCode = static_cast<int16_t>(6); // ErrorCode::Internal
            enqueueReturn_(ret);
            return;
        }

        // fsyncgate fail-stop: a failed log refuses every produce.
        if (log->storageFailed()) [[unlikely]] {
            ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::StorageError);
            enqueueReturn_(ret);
            return;
        }

        // Zero-copy restamp append: the base offset is assigned from the log's end
        // offset and patched into the batch header via pwritev (LE64 at bytes
        // [0..7], before the CRC region) — the ring frame is NOT mutated, so
        // op.tail() stays a read-only view (no const_cast).  The worker is the
        // single writer, so the assigned base is authoritative.
        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());

        // ── Task 7: per-worker schema validation hook ──────────────────────
        // Immediately before append: if a local Validator is installed for this
        // topic (installed off-hot-path via SchemaUpdate ring op), validate each
        // record's value in the batch.
        //
        // HARD RULE 4 (zero hot-path alloc): validators_ was built by
        // executeSchemaUpdateWire_ off-hot-path.  The validate() call operates
        // on a zero-copy span into the ring tail (no heap).  No per-record
        // allocation; only preallocated state inside the Validator is accessed.
        // HARD RULE 5 (no virtual calls): Validator::validate is a concrete call.
        // HARD RULE 1 (shared-nothing): validators_ is ONLY touched by this worker
        // thread — never shared, never locked.
        //
        // I1 fast-out (ultra-low-latency default path, HARD RULE #4): when no
        // validator has ever been installed (validators_ empty — the common case
        // for topics without a schema), skip the hash lookup entirely.  This
        // reduces the default produce path to one integer load + one
        // well-predicted branch; the hash+bucket+pointer-chase is paid only when
        // at least one schema has been pushed to this worker.
        if (!validators_.empty()) [[unlikely]] {
            auto it = validators_.find(topicId);
            if (it != validators_.end()) [[unlikely]] {
                const rune::codec::Validator& v = it->second;
                bool validationFailed = false;
                if (tailLen >= static_cast<uint32_t>(rune::protocol::batch::kRecordBatchOverhead)) {
                    rune::protocol::RecordBatchReader rdr(tail, static_cast<std::size_t>(tailLen));
                    // M2 (HARD RULE #4 — zero hot-path alloc): the capturing lambda
                    // passed to forEach must fit in std::function's small-object
                    // buffer.  It captures: &v (pointer, 8 B), &validationFailed
                    // (pointer, 8 B) — 16 bytes total, well within the typical 24–32 B
                    // SOB limit.  If future changes add captures, re-verify this; a
                    // larger capture list WILL cause a per-produce heap allocation.
                    // static_assert cannot gate std::function SOB at compile time
                    // (it's implementation-defined), so this comment is the pin.
                    rdr.forEach([&](std::string_view /*key*/,
                                    std::string_view val,
                                    int64_t /*offset*/,
                                    int64_t /*ts*/) {
                        if (validationFailed) return;
                        const rune::codec::ValResult res = v.validate(
                            std::span<const uint8_t>(
                                reinterpret_cast<const uint8_t*>(val.data()),
                                val.size()));
                        if (!res.ok) {
                            validationFailed = true;
                        }
                    });
                } else {
                    // Batch too short to parse header — reject as malformed.
                    validationFailed = true;
                }
                if (validationFailed) {
                    spdlog::debug("Worker {}: schema validation FAILED for "
                                  "topicId={} partition={}; rejecting produce",
                                  workerId_, topicId, partition);
                    ret.errorCode = static_cast<int16_t>(
                        rune::protocol::client::ErrorCode::SchemaValidationFailed);
                    enqueueReturn_(ret);
                    return;
                }
            }
        }
        // ── end Task 7 validation hook ─────────────────────────────────────

        int64_t base = 0;
        try {
            const int64_t appended =
                log->appendNoSyncRestamp(tail, static_cast<std::size_t>(tailLen),
                                         op.lastOffsetDelta(), base);
            // R5 finding F6 (correctness): a negative return means the append was
            // REFUSED (e.g. InMemoryLog in BackPressure mode with a full ring).
            // We MUST NOT ack OK for a record that was never appended — that is a
            // phantom-durability false-OK.  Return a non-OK error so the client
            // retries ([[rune-shared-nothing-law]]: never false-OK a commit-critical
            // produce under back-pressure).
            if (appended < 0) {
                spdlog::warn("Worker {}: appendNoSync refused (back-pressure) for "
                             "topicId={} partition={}; returning Internal",
                             workerId_, topicId, partition);
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

        // Base offset assigned by appendNoSyncRestamp (authoritative).
        ret.baseOffset = base;
        ret.lastOffset = base + static_cast<int64_t>(op.lastOffsetDelta());

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
        if (op.durabilityTier() == 2 && log->isDiskBacked()) {
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
    void executePushReadWire_(sbe::WorkerOp& op) {
        RUNE_FR_SPAN_READ(rune_fr::Site::ReadEncodePush,
                          static_cast<uint32_t>(op.sessionId()));
        const int32_t topicId    = op.topicId();
        const int32_t partition  = op.partition();
        const int64_t nextOffset = op.nextOffset();
        const int64_t hwBound    = op.hwBound();
        WorkerReturn ret;
        ret.type                 = WorkerReturn::Type::PushReadResult;
        ret.subscriptionId       = op.subscriptionId();
        ret.sessionId            = op.sessionId(); // gateway needs this to find the session
        ret.readTopicId          = topicId;
        ret.readPartition        = partition;
        ret.newNextOffset        = nextOffset; // will be advanced by gateway on success
        ret.hwBound              = hwBound;
        ret.isolation            = op.isolation();
        ret.isGroup              = op.isGroup() != 0;
        ret.groupCarrierReceived = op.groupCarrierReceived() != 0;

        rune::storage::Log* log = getLogFromWire_(op);
        if (!log) {
            // No log: return empty chunk (gateway will skip this subscription).
            enqueueReturn_(ret);
            return;
        }

        // Only read if there are records available (within the HW-bound).
        if (nextOffset < hwBound) {
            try {
                // kMaxChunkBytes constant mirrored here; must stay in sync with
                // ClientGateway.  Gateway sets hwBound before enqueuing.
                static constexpr int32_t kMaxChunkBytes =
                    static_cast<int32_t>(512 * 1024); // 512 KiB
                ret.chunk = log->read(nextOffset, kMaxChunkBytes);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: log->read threw for topicId={} partition={}: {}",
                             workerId_, topicId, partition, ex.what());
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
    void executeApplyReplicatedWire_(sbe::WorkerOp& op) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeApplyReplicatedWire_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId(), op.partition());
            return;
        }

        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());

        rune::replication::FetchResponseMsg msg;
        msg.leaderEpoch    = op.replLeaderEpoch();
        msg.partition      = op.replPartition();
        msg.highWatermark  = op.replHighWatermark();
        msg.logStartOffset = op.replLogStartOffset();
        msg.errorCode      = op.replErrorCode();
        msg.topicId        = op.replTopicId();
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
    void executeBecomeLeaderWire_(sbe::WorkerOp& op) {
        const int32_t topicId   = op.topicId();
        const int32_t partition = op.partition();
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeBecomeLeaderWire_ — null replica for "
                         "topicId={} partition={}", workerId_, topicId, partition);
            return;
        }

        // Rebuild ISR vector from the tail (rare path, alloc OK).
        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());
        std::vector<rune::replication::NodeId> isr;
        if (tail && tailLen > 0) {
            const std::size_t count = static_cast<std::size_t>(tailLen) /
                                       sizeof(rune::replication::NodeId);
            isr.resize(count);
            std::memcpy(isr.data(), tail, count * sizeof(rune::replication::NodeId));
        }

        const rune::replication::Epoch epoch =
            static_cast<rune::replication::Epoch>(op.roleEpoch());

        try {
            replica->becomeLeader(epoch, std::move(isr));
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeLeader threw for topicId={} partition={}: {}",
                         workerId_, topicId, partition, ex.what());
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
                    psm_.rebuildFromLog(topicId, *log, hw, partition);
                    spdlog::info("Worker {}: PSM rebuilt for topicId={} partition={} "
                                 "hw={} entries={}",
                                 workerId_, topicId, partition, hw,
                                 psm_.size());
                } catch (const std::exception& ex) {
                    spdlog::error("Worker {}: PSM rebuild threw for topicId={} "
                                  "partition={}: {}; idempotency window may be stale",
                                  workerId_, topicId, partition, ex.what());
                } catch (...) {
                    spdlog::error("Worker {}: PSM rebuild threw unknown exception for "
                                  "topicId={} partition={}",
                                  workerId_, topicId, partition);
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
    void executeBecomeFollowerWire_(sbe::WorkerOp& op) {
        const int32_t topicId   = op.topicId();
        const int32_t partition = op.partition();
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeBecomeFollowerWire_ — null replica for "
                         "topicId={} partition={}", workerId_, topicId, partition);
            return;
        }
        const rune::replication::Epoch  epoch    =
            static_cast<rune::replication::Epoch>(op.roleEpoch());
        const rune::replication::NodeId leaderId =
            static_cast<rune::replication::NodeId>(op.roleLeaderId());
        try {
            replica->becomeFollower(epoch, leaderId);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeFollower threw for topicId={} partition={}: {}",
                         workerId_, topicId, partition, ex.what());
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
    void executeSetOwnershipWire_(sbe::WorkerOp& op) {
        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());

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
        rebuildFlushSlots_();   // cold: carry still-owned slots, purge the rest (M2)
        // Task 10 fix 3: ack the registry generation this SetOwnership reflects.
        // The runtime frees a removed replica only once EVERY worker acked a
        // generation >= its removal (no worker can still hold / reach it).
        WorkerReturn ack;
        ack.type       = WorkerReturn::Type::OwnershipAck;
        ack.partition  = workerId_;
        ack.lastOffset = static_cast<int64_t>(op.ownershipGen());
        enqueueSignal_(ack);
        ++ownershipAcks_;
    }

    // -------------------------------------------------------------------
    // E5.1.2c: executeFetchRequestWire_ — leader handles a follower FetchRequest
    // on the worker thread so leaderState_/isrManager_ is single-owner.
    // The FetchResponse is emitted via replica->onFetchRequest which calls
    // transport_->sendFetchResponse → enqueueOutbound (MPSC outbound ring).
    // Fire-and-forget: no WorkerReturn needed (FetchResponse goes outbound).
    // -------------------------------------------------------------------
    void executeFetchRequestWire_(sbe::WorkerOp& op) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeFetchRequestWire_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId(), op.partition());
            return;
        }

        rune::replication::FetchRequestMsg req;
        req.leaderEpoch = static_cast<rune::replication::Epoch>(op.frLeaderEpoch());
        req.partition   = static_cast<rune::replication::PartitionId>(op.partition());
        req.followerId  = static_cast<rune::replication::NodeId>(op.frFollowerId());
        req.fetchOffset = static_cast<rune::replication::Offset>(op.frFetchOffset());
        req.maxBytes    = op.frMaxBytes();
        req.topicId     = op.topicId();

        try {
            replica->onFetchRequest(req, op.frNowNs());
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
    void executeOffsetsForLeaderEpochReqWire_(sbe::WorkerOp& op) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeOffsetsForLeaderEpochReqWire_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId(), op.partition());
            return;
        }

        rune::replication::OffsetsForLeaderEpochReqMsg req;
        req.currentEpoch   = static_cast<rune::replication::Epoch>(op.oleCurrentEpoch());
        req.partition      = static_cast<rune::replication::PartitionId>(op.partition());
        req.followerId     = static_cast<rune::replication::NodeId>(op.oleFollowerId());
        req.requestedEpoch = static_cast<rune::replication::Epoch>(op.oleRequestedEpoch());
        req.followerLeo    = static_cast<rune::replication::Offset>(op.oleFollowerLeo());
        req.topicId        = op.topicId();

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
    void executeReconcileFetchRespWire_(sbe::WorkerOp& op) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeReconcileFetchRespWire_ — null replica for "
                         "topicId={} partition={}", workerId_, op.topicId(), op.partition());
            return;
        }

        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());

        rune::replication::FetchResponseMsg msg;
        msg.leaderEpoch    = op.replLeaderEpoch();
        msg.partition      = op.replPartition();
        msg.highWatermark  = op.replHighWatermark();
        msg.logStartOffset = op.replLogStartOffset();
        msg.errorCode      = op.replErrorCode();
        msg.topicId        = op.replTopicId();
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
    void executeAppendControlRecordWire_(sbe::WorkerOp& op) {
        const int32_t topicId   = op.topicId();
        const int32_t partition = op.partition();
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ — null replica for "
                         "topicId={} partition={}", workerId_, topicId, partition);
            return;
        }

        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());

        if (!tail || tailLen == 0) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ — empty marker tail for "
                         "topicId={} partition={}", workerId_, topicId, partition);
            return;
        }

        const std::span<const uint8_t> markerSpan(tail, static_cast<std::size_t>(tailLen));

        try {
            const rune::replication::Offset base = replica->append(markerSpan, /*acksAll=*/true);
            if (base < 0) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — replica->append returned {} "
                             "(minIsr gate?) topicId={} partition={}",
                             workerId_, base, topicId, partition);
            }
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ threw unknown", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // setReplicationTransportImpl_ — template helper that wires the (ctx,
    //   trampoline) pair from a concrete WorkerReplicationTransportImpl<T>.
    //
    // The trampoline is a non-capturing lambda (→ plain function pointer) that:
    //   1. Casts ctx back to the concrete type.
    //   2. Calls poll(limit, dispatch) where dispatch is also a non-capturing
    //      lambda (→ function pointer) forwarding to pollReplTransportDispatch_.
    //
    // No std::function, no heap allocation, no virtual dispatch.
    // Mirrors WorkerReplicationTransportImpl's own ctx_/trampoline_ pattern.
    // -------------------------------------------------------------------
    template <typename T>
    void setReplicationTransportImpl_(WorkerReplicationTransportImpl<T>* transport) noexcept
    {
        if (!transport) {
            replTransportCtx_    = nullptr;
            replTransportPollFn_ = nullptr;
            return;
        }
        replTransportCtx_ = static_cast<void*>(transport);
        // The trampoline is a non-capturing lambda → implicitly converts to
        // a plain function pointer (ReplPollFn).  Safe because it is stored as
        // a function pointer and never invoked after the transport is destroyed
        // (BrokerNode owns the lifetime and tears down workers first).
        replTransportPollFn_ = [](void* ctx, int limit, Worker* self, int64_t nowNs) -> int
        {
            auto* t = static_cast<WorkerReplicationTransportImpl<T>*>(ctx);
            // Shard plane handshake (spec §3.3): one predicted branch unless a
            // peer is unverified / refused (then a hello on the retry cadence).
            const int hs = t->driveHandshake(nowNs);
            return hs + t->poll(
                limit,
                // Inner dispatch: also non-capturing → function pointer.
                // GCC/Clang: a non-capturing lambda with a non-capturing
                // closure may capture (Worker*, int64_t) via a Dispatch
                // parameter (templated, inlined) without being a std::function.
                // Here we use a struct-based callable to make the intent
                // explicit and guarantee zero heap allocation.
                [self, nowNs]
                (int32_t topicId, int32_t partition,
                 uint16_t templateId,
                 const uint8_t* frame, std::size_t len)
                {
                    self->pollReplTransportDispatch_(topicId, partition, templateId,
                                                    frame, len, nowNs);
                });
        };
    }

    // -------------------------------------------------------------------
    // findOwnedReplica_ — scan owned_ for a replica matching (topicId, partition).
    //
    // Returns the first match or nullptr if not found.  O(n) over owned_ —
    // acceptable: owned_ has O(partitions-per-worker) entries (typically small,
    // ≤ tens); a hot-path map is overkill and would add per-tick lookup cost.
    //
    // Worker-thread only (owned_ is single-writer on the worker thread).
    // -------------------------------------------------------------------
    rune::replication::PartitionReplica*
    findOwnedReplica_(int32_t topicId, int32_t partition) const noexcept
    {
        for (auto* r : owned_) {
            if (r && r->topicId() == topicId &&
                static_cast<int32_t>(r->partitionId()) == partition) {
                return r;
            }
        }
        return nullptr;
    }

    // -------------------------------------------------------------------
    // pollReplTransportDispatch_ — inline dispatch of one replication frame
    //   received from the per-worker WorkerReplicationTransport::poll callback.
    //
    // Routes by templateId to the owned replica's appropriate handler,
    // INLINE (no ring hop).  The frame pointer is a zero-copy view valid ONLY
    // inside this call; the replica copies data into its log before returning.
    //
    // Mirrors the decode+call logic of the execute*Wire_ ring handlers
    // (executeFetchRequestWire_ ~1664, executeApplyReplicatedWire_ ~1479,
    // executeOffsetsForLeaderEpochReqWire_ ~1698, executeReconcileFetchRespWire_
    // ~1729) but decodes from the raw SBE frame rather than the WorkerOp ring
    // flyweight.  A small comment cross-references each parallel executeXxxWire_.
    //
    // Worker-thread only.
    // -------------------------------------------------------------------
    void pollReplTransportDispatch_(int32_t topicId, int32_t partition,
                                    uint16_t templateId,
                                    const uint8_t* frame, std::size_t len,
                                    int64_t nowNs) noexcept
    {
        namespace rr = rune::replication;

        auto* replica = findOwnedReplica_(topicId, partition);
        if (!replica) {
            // Not owned by this worker — drop silently (shared-nothing: only
            // the owning worker handles its replicas).
            spdlog::trace("Worker {}: pollReplTransportDispatch_ — no owned replica for "
                          "topicId={} partition={} templateId={}",
                          workerId_, topicId, partition, templateId);
            return;
        }

        // Mirrors the executeXxxWire_ ring handlers (listed above):
        // decode the fixed or variable frame and call the replica method inline.
        switch (templateId) {

        // TemplateId 1: FetchRequest — leader handles follower fetch.
        // Mirrors executeFetchRequestWire_ (~1664).
        case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchRequest): {
            const rr::FetchRequestMsg req = rr::FetchRequestMsg::decode(frame, 0);
            try {
                replica->onFetchRequest(req, nowNs);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: pollReplTransport FetchRequest onFetchRequest threw: {}",
                             workerId_, ex.what());
            } catch (...) {
                spdlog::warn("Worker {}: pollReplTransport FetchRequest onFetchRequest threw unknown",
                             workerId_);
            }
            break;
        }

        // TemplateId 2: FetchResponse — follower applies received records.
        // Mirrors executeApplyReplicatedWire_ (~1479).
        // CRITICAL: zero-copy — msg.records is a span VIEW into `frame` (valid
        // only during this call); applyReplicated copies into the follower log
        // before returning.  No records.assign / no intermediate vector.
        case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchResponse): {
            const rr::FetchResponseMsg msg =
                rr::FetchResponseMsg::decode(frame, 0, len);
            try {
                replica->applyReplicated(msg);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: pollReplTransport FetchResponse applyReplicated threw: {}",
                             workerId_, ex.what());
            } catch (...) {
                spdlog::warn("Worker {}: pollReplTransport FetchResponse applyReplicated threw unknown",
                             workerId_);
            }
            break;
        }

        // TemplateId 3: OffsetsForLeaderEpochRequest — leader handles KIP-101
        // epoch probe.  Mirrors executeOffsetsForLeaderEpochReqWire_ (~1698).
        case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochReq): {
            const rr::OffsetsForLeaderEpochReqMsg req =
                rr::OffsetsForLeaderEpochReqMsg::decode(frame, 0);
            try {
                replica->onOffsetsForLeaderEpochRequest(req);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: pollReplTransport OleReq onOffsetsForLeaderEpochRequest threw: {}",
                             workerId_, ex.what());
            } catch (...) {
                spdlog::warn("Worker {}: pollReplTransport OleReq onOffsetsForLeaderEpochRequest threw unknown",
                             workerId_);
            }
            break;
        }

        // TemplateId 4: OffsetsForLeaderEpochResponse — follower applies KIP-101
        // epoch probe answer.  Mirrors the OLE-response apply path the ring uses
        // (applyOffsetsForLeaderEpochResponse, called by the control-thread
        // AeronPartitionTransport path that routes via WorkerOp into executeXxxWire_).
        // No dedicated executeXxxWire_ in the current ring path (OLE-resp arrives
        // at the follower via AeronPartitionTransport → callback → applyOle directly
        // on the control thread).  Here we apply inline on the WORKER thread for
        // the per-worker transport path.
        case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochResp): {
            const rr::OffsetsForLeaderEpochRespMsg resp =
                rr::OffsetsForLeaderEpochRespMsg::decode(frame, 0);
            try {
                replica->applyOffsetsForLeaderEpochResponse(resp);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: pollReplTransport OleResp applyOffsetsForLeaderEpochResponse threw: {}",
                             workerId_, ex.what());
            } catch (...) {
                spdlog::warn("Worker {}: pollReplTransport OleResp applyOffsetsForLeaderEpochResponse threw unknown",
                             workerId_);
            }
            break;
        }

        // TemplateId 5: ReconcileFetchRequest — ahead survivor handles a
        // deficient-leader reverse pull request.  Mirrors the reconcile-request
        // handler path (onReconcileRequest).  The frame reuses FetchRequest layout.
        case rr::kReconcileFetchReqTemplateId: {
            const rr::FetchRequestMsg req = rr::FetchRequestMsg::decode(frame, 0);
            try {
                replica->onReconcileRequest(req);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: pollReplTransport ReconcileFetchReq onReconcileRequest threw: {}",
                             workerId_, ex.what());
            } catch (...) {
                spdlog::warn("Worker {}: pollReplTransport ReconcileFetchReq onReconcileRequest threw unknown",
                             workerId_);
            }
            break;
        }

        // TemplateId 6: ReconcileFetchResponse — deficient leader applies records
        // pulled from an ahead survivor.  Mirrors executeReconcileFetchRespWire_
        // (~1729).  Frame reuses FetchResponse layout (same codec, different
        // templateId → routes to applyReconcileResponse instead of applyReplicated).
        // Zero-copy: msg.records is a span VIEW into `frame` (see templateId 2).
        case rr::kReconcileFetchRespTemplateId: {
            const rr::FetchResponseMsg msg =
                rr::FetchResponseMsg::decode(frame, 0, len);
            try {
                replica->applyReconcileResponse(msg);
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: pollReplTransport ReconcileFetchResp applyReconcileResponse threw: {}",
                             workerId_, ex.what());
            } catch (...) {
                spdlog::warn("Worker {}: pollReplTransport ReconcileFetchResp applyReconcileResponse threw unknown",
                             workerId_);
            }
            break;
        }

        default:
            // Unknown templateId — dispatchFrame_ in WorkerReplicationTransport
            // already logs and drops unknowns before reaching here; this is a
            // defence-in-depth guard only.
            spdlog::trace("Worker {}: pollReplTransportDispatch_ — unhandled templateId={} "
                          "topicId={} partition={}",
                          workerId_, templateId, topicId, partition);
            break;
        }
    }

    // -------------------------------------------------------------------
    // getLogFromWire_ — resolve the Log* for the op's (topicId, partition)
    //                   from the SBE WorkerOp flyweight.
    // -------------------------------------------------------------------
    rune::storage::Log* getLogFromWire_(sbe::WorkerOp& op) const noexcept {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        // Prefer replica->log() for disk-backed (avoids separate registry lookup).
        if (replica && replica->isDiskBacked()) {
            return replica->log();
        }
        // Fall back to direct Log* pointer carried in the op (set by gateway at
        // enqueue time to avoid a registry lookup in doWork).
        const std::uint64_t lp = op.logPtr();
        if (lp != 0) {
            return reinterpret_cast<rune::storage::Log*>(lp);
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
        /// Shard Strict acks=all (Task 10): after the fsync completes the ack
        /// moves to pendingAcksAll_ (HW gate) instead of firing.  null = fire.
        rune::replication::PartitionReplica* thenAwaitHw = nullptr;
        int64_t                 deadlineNs = 0;       ///< acks=all deadline (thenAwaitHw only)
        /// Its token completed, but an async roll seal on the same log was still
        /// outstanding: fires when the seals complete (fireSealGated_).
        bool                    awaitSeal  = false;
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
    bool shardMode_ = false;   // spec §3.1: set before the runner starts, then read-only

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

    // E5 Task 5: non-owning typed pointer to the production replication transport
    // (AeronPubRegistry alias).  Null until setReplicationTransport() is called.
    // BrokerNode owns the WorkerReplicationTransport lifetime.
    WorkerReplicationTransport* workerReplTransport_ = nullptr;

    // Task 6: type-erased (ctx, trampoline) pair for calling
    // WorkerReplicationTransportImpl<T>::poll() from doWork() without std::function
    // and without virtual dispatch.  Set by setReplicationTransportImpl_<T>.
    //
    // Trampoline signature:
    //   int(*)(void* ctx, int limit, Worker* self, int64_t nowNs)
    //
    // The trampoline casts ctx back to WorkerReplicationTransportImpl<T>* and
    // calls poll(limit, dispatch) where dispatch is a non-capturing lambda
    // (plain function pointer) that forwards to self->pollReplTransportDispatch_.
    // This is the same ctx_/trampoline_ idiom WorkerReplicationTransportImpl uses
    // internally and matches WorkerIngress::poll exactly.
    //
    // replTransportCtx_ == nullptr when no transport is wired (flag-off path).
    using ReplPollFn = int(*)(void* ctx, int limit, Worker* self, int64_t nowNs);
    void*       replTransportCtx_    = nullptr;
    ReplPollFn  replTransportPollFn_ = nullptr;

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

    // Per-log LEO at its last successful sync lives in LogFlushSlot::lastSyncedLeo
    // (Task 10 fix 1 / I3: fixed per-owned-log slots, no hot-path map insert).

    // Task 7: per-worker topic → Validator map.
    //
    // Populated via SchemaUpdate ring ops (executeSchemaUpdateWire_), which
    // arrive from the admin thread as ring messages and are applied on THIS
    // worker's own thread (shared-nothing — no lock, no atomic).
    //
    // Looked up in executeProduceWire_ BEFORE appendNoSyncRestamp.  The lookup
    // is a single unordered_map::find (O(1) expected) with a single conditional
    // branch when empty (most topics have no schema validator installed).
    //
    // HARD RULE 1 (shared-nothing): validators_ is ONLY ever touched by the
    // worker thread — the admin thread copies the blob into the ring, the worker
    // builds the Validator from the ring view on ITS OWN thread.
    // HARD RULE 4 (zero hot-path alloc): the map is built off the hot path
    // (in executeSchemaUpdateWire_).  The hot path only does a map::find + span
    // validate (no allocation).
    //
    // Reserved capacity is intentionally small (16): most deployments have
    // O(10s) of topics per worker; the map is rarely rebuilt.
    // TODO(T9): benchmark flat vs unordered_map for typical topic counts.
    static constexpr std::size_t kValidatorsReserve = 16;
    std::unordered_map<int32_t, rune::codec::Validator> validators_;

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

    // Expected ProduceResult return size (bytes) for the return-ring reservation.
    // Configurable (RUNE_WORKER_ACK_SLOT_BYTES); see setAckSlotBytes.  256 comfortably
    // covers the ~160 B framed ProduceResult.
    int32_t ackSlotBytes_ = 256;
    // F3 round 5: effective max produce batch (C2, worker-thread read) and the
    // oversize-op refusal counter (I1, producer/control-thread-owned — only
    // tryEnqueueOp touches it; single producer per SPSC ring).
    std::size_t maxProduceBatchBytes_ = 0;
    uint64_t    oversizeOpRejects_    = 0;
    // F3 round 6, item 2: worker-thread-owned reject counters (per-worker ingress).
    uint64_t    oversizeProduceRejects_    = 0;
    uint64_t    oversizeProduceRejectLogs_ = 0;
    // Per-tick drain batch limits (NodeConfig; raised from the OneToOneByteRing 16
    // default so routed replication ops / acks don't queue behind a tiny cap).
    int32_t ingressDrainLimit_ = 1024;  // RUNE_WORKER_INGRESS_DRAIN_LIMIT
    int32_t returnDrainLimit_  = 1024;  // RUNE_WORKER_RETURN_DRAIN_LIMIT
    // FR trace: monotonic count of routed replication ops (fetch/apply) dequeued
    // from the ingress ring — pairs 1:1 (FIFO) with the control thread's
    // ReplFragRecv seq to measure the exact control→worker handoff latency.
    uint32_t workerRunSeq_ = 0;

    // -----------------------------------------------------------------------
    // Task 10 (spec §3.1 never-block) — per-owned-log flush slots + token routing.
    //
    // ONE slot per owned disk log (fixed set, rebuilt cold on an ownership
    // change; a worker-local Log*→slot open-addressing map gives O(1) lookup):
    //   * the background flush (replica requests: follower flush, txn marker,
    //     reconcile; and the 10 ms owned-log sync) — at most ONE in flight per
    //     log, plus a "dirty again" bit when newer data arrives meanwhile;
    //   * lastSyncedLeo — advanced by ANY successful background flush, so a
    //     follower flush also satisfies the time-based sync (no double fsync);
    //   * sealsInFlight — async roll seals (Log::setDeferSealSync); Strict acks
    //     on the log wait until every seal has completed;
    //   * failureReported — fail-stop signalled once.
    // Every io_ token is ROUTED (open-addressing table sized ≥ 2× the io depth,
    // so it never fills): Durable (Strict / produce acks), Background or Seal,
    // with the segment it pinned (M1: the fd outlives the flush).  A Background
    // or Seal token is retired by routing and can never reach produce-ack
    // matching.  A refused background submit is NOT retried per pass: the next
    // owned-log sync interval re-requests it (LEO > lastSyncedLeo).
    // fsync errors are FAIL-STOP (fsyncgate): never retried.
    // Only fdatasync of the .log fd: Log::recover rebuilds the .index from the
    // .log, so the .index msync is not a durability point.
    // Worker-thread only (shared-nothing).
    // -----------------------------------------------------------------------
    struct LogFlushSlot {
        rune::storage::Log*                  log     = nullptr;
        rune::replication::PartitionReplica* replica = nullptr;
        uint32_t        epoch           = 0;          ///< identity across rebuilds (M2)
        CompletionToken bgToken         = kNoToken;   ///< background flush in flight
        int64_t         bgLeo           = 0;          ///< LEO covered by bgToken
        bool            bgAgain         = false;      ///< newer data requested meanwhile
        int64_t         lastSyncedLeo   = 0;
        uint32_t        sealsInFlight   = 0;
        bool            failureReported = false;
        uint64_t        sealDropsSeen   = 0;          ///< Log::droppedPendingSeals() last seen
        std::string     dir;                          ///< cached at slot creation (C-1 signal)
    };
    /// Logs purged by an ownership move with tokens possibly still in flight:
    /// their cached dir, so a late error can be signalled without reading the Log.
    std::vector<std::pair<const rune::storage::Log*, std::string>> purgedDirs_;
    std::vector<LogFlushSlot> slots_;
    uint32_t                  slotEpochSeq_ = 0;
    // Log* → slots_ index (open addressing, capacity pow2 ≥ 2×slots).
    std::vector<std::pair<rune::storage::Log*, uint32_t>> slotIndex_;
    std::size_t               slotIndexMask_ = 0;

    enum class RouteKind : uint8_t { Durable, Background, Seal };
    struct TokenRoute {
        CompletionToken            token = kNoToken;   ///< kNoToken = empty bucket
        rune::storage::Log*        log   = nullptr;
        rune::storage::LogSegment* seg   = nullptr;     ///< pinned segment (unpin on completion)
        uint32_t                   epoch = 0;
        RouteKind                  kind  = RouteKind::Durable;
    };
    std::vector<TokenRoute> routes_;
    std::size_t             routeMask_ = 0;

    std::size_t bgInFlight_     = 0;
    uint64_t    bgFlushSubmits_ = 0;
    uint64_t    bgFlushErrors_  = 0;
    uint64_t    bgFlushDeferred_ = 0;
    uint64_t    sealSubmits_    = 0;
    uint64_t    ioCompletionsReaped_ = 0;
    uint64_t    storageFailureSignals_ = 0;
    CompletionToken durableAckToken_ = kNoToken;     ///< token whose completion fired the last durable ack
    bool        reapPausedForTest_ = false;
    int         forcedFlushErrorForTest_ = 0;
    std::vector<WorkerReturn> pendingSignals_;       ///< StorageFailure signals awaiting ring space (cold)
    std::vector<const rune::storage::Log*> foreignFailed_;   ///< C-1: signalled failures on un-owned logs (cold)

    static std::size_t pow2AtLeast_(std::size_t n) noexcept {
        std::size_t p = 1;
        while (p < n) p <<= 1;
        return p;
    }
    static std::size_t ptrHash_(const void* p) noexcept {
        return static_cast<std::size_t>((reinterpret_cast<uintptr_t>(p) >> 4) * 0x9E3779B97F4A7C15ull);
    }
    // ---- Ownership table: ownedInc_[registryId] = the incarnation this worker
    // owns (0 = not owned).  Dense, indexed by the registry id carried in the op.
    std::vector<uint32_t> ownedInc_;
    uint64_t    opsRefusedNotOwned_ = 0;
    uint64_t    ownershipAcks_      = 0;
    static constexpr bool gatedByOwnership_(WorkerOp::Type t) noexcept {
        switch (t) {
        case WorkerOp::Type::Produce:
        case WorkerOp::Type::PushRead:
        case WorkerOp::Type::ApplyReplicated:
        case WorkerOp::Type::BecomeLeader:
        case WorkerOp::Type::BecomeFollower:
        case WorkerOp::Type::FetchRequest:
        case WorkerOp::Type::OffsetsForLeaderEpochReq:
        case WorkerOp::Type::ReconcileFetchResp:
        case WorkerOp::Type::AppendControlRecord:
        case WorkerOp::Type::Subscribe:
            return true;
        default:
            return false;
        }
    }
    [[nodiscard]] bool ownsIncarnation_(uint32_t id, uint32_t inc) const noexcept {
        return inc != 0 && id < ownedInc_.size() && ownedInc_[id] == inc;
    }
    void rebuildOwnedIndex_() {
        uint32_t maxId = 0;
        for (const auto* r : owned_) if (r && r->registryId() > maxId) maxId = r->registryId();
        ownedInc_.assign(static_cast<std::size_t>(maxId) + 1, 0u);
        for (const auto* r : owned_) if (r && r->incarnation() != 0) ownedInc_[r->registryId()] = r->incarnation();
        // Parked / active push subscriptions follow ownership by id + incarnation.
        for (auto& sub : subs_) {
            if (sub.incarnation != 0) sub.active = ownsIncarnation_(sub.registryId, sub.incarnation);
            else if (sub.replica) {
                bool owned = false;
                for (const auto* r : owned_) owned = owned || r == sub.replica;
                sub.active = owned;
            }
        }
    }
    // A partition op for a replica this worker does not own: answer through the
    // op's normal response path (retriable), never touching the replica / log.
    [[gnu::cold]] [[gnu::noinline]] void refuseNotOwned_(sbe::WorkerOp& op, WorkerOp::Type type) {
        ++opsRefusedNotOwned_;
        namespace pc = rune::protocol::client;
        switch (type) {
        case WorkerOp::Type::Produce: {
            WorkerReturn ret;
            ret.type          = WorkerReturn::Type::ProduceResult;
            ret.sessionId     = op.sessionId();
            ret.correlationId = op.correlationId();
            ret.partition     = op.partition();
            ret.topicId       = op.topicId();
            ret.deferAck      = op.deferAck() != 0;
            ret.producerId    = op.producerId();
            ret.producerEpoch = op.producerEpoch();
            ret.baseSequence  = op.baseSequence();
            ret.recordCount   = op.recordCount();
            ret.enqueuedAtNs  = op.enqueuedAtNs();
            ret.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return;
        }
        case WorkerOp::Type::PushRead: {
            // Empty result: the gateway clears its in-flight marker and re-issues
            // the read to the (new) owner on its next push cycle.
            WorkerReturn ret;
            ret.type           = WorkerReturn::Type::PushReadResult;
            ret.sessionId      = op.sessionId();
            ret.subscriptionId = op.subscriptionId();
            ret.readTopicId    = op.topicId();
            ret.readPartition  = op.partition();
            enqueueReturn_(ret);
            return;
        }
        case WorkerOp::Type::Subscribe:
            // Registered PARKED (no reads) — activated (by id + incarnation) if
            // that exact replica incarnation is later granted to this worker.
            executeSubscribeWire_(op);
            for (auto& sub : subs_) if (sub.subscriptionId == op.subscriptionId()) sub.active = false;
            return;
        case WorkerOp::Type::AppendControlRecord: {
            // Never silently lost: the marker goes back to the runtime, which
            // re-routes it with the current routing or surfaces NotLeader to the
            // transaction coordinator.
            WorkerReturn ret;
            ret.type      = WorkerReturn::Type::ControlRecordRefused;
            ret.topicId   = op.topicId();
            ret.partition = op.partition();
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            const uint32_t n = op.tailLength();
            const auto* t = reinterpret_cast<const uint8_t*>(op.tail());
            if (t && n) ret.chunk.assign(t, t + n);
            enqueueSignal_(ret);
            return;
        }
        default:
            // Apply / fetch / epoch / reconcile / role / marker ops carry no
            // response: the replication protocol re-drives them against the new
            // owner (follower re-fetch, leader re-push, bridge role re-derive).
            if (opsRefusedNotOwned_ == 1 || (opsRefusedNotOwned_ & 1023u) == 0)
                spdlog::warn("Worker {}: refused op type={} for un-owned topic={} partition={} "
                             "(stale ownership routing; total={})", workerId_,
                             static_cast<int>(type), op.topicId(), op.partition(), opsRefusedNotOwned_);
            return;
        }
    }

    LogFlushSlot* slotOf_(const rune::storage::Log* log) noexcept {
        if (slotIndex_.empty() || log == nullptr) return nullptr;
        for (std::size_t i = ptrHash_(log) & slotIndexMask_;; i = (i + 1) & slotIndexMask_) {
            if (slotIndex_[i].first == log) return &slots_[slotIndex_[i].second];
            if (slotIndex_[i].first == nullptr) return nullptr;
        }
    }
    // Cold: called on every ownership change (setOwnedDirect / SetOwnership op).
    // Slots for logs still owned are carried over (in-flight state intact);
    // slots for logs no longer owned are purged — their in-flight tokens are
    // then ignored on completion (epoch / identity mismatch) (M2).
    void rebuildFlushSlots_() {
        rebuildOwnedIndex_();
        std::vector<LogFlushSlot> next;
        next.reserve(owned_.size());
        for (auto* replica : owned_) {
            if (!replica || !replica->isDiskBacked()) continue;
            rune::storage::Log* log = replica->log();
            if (!log) continue;
            bool dup = false;
            for (const auto& n : next) dup = dup || n.log == log;
            if (dup) continue;
            if (LogFlushSlot* old = slotOf_(log)) {
                LogFlushSlot carried = *old;
                carried.replica = replica;
                next.push_back(carried);
            } else {
                LogFlushSlot fresh;
                fresh.log = log; fresh.replica = replica; fresh.epoch = ++slotEpochSeq_;
                fresh.dir = log->dir();   // cached while owned: never read the Log after a move
                next.push_back(fresh);
            }
        }
        // Ownership move (shared-nothing): after this, the worker holds no
        // reference to — and makes no reads of — a moved replica or log.
        std::vector<const rune::replication::PartitionReplica*> ownedNow(owned_.begin(), owned_.end());
        std::sort(ownedNow.begin(), ownedNow.end());
        auto isOwned = [&](const rune::replication::PartitionReplica* r) {
            return r != nullptr && std::binary_search(ownedNow.begin(), ownedNow.end(), r);
        };
        // acks=all waiters on a moved replica would read its HW cross-thread:
        // fail them RETRIABLY (NotLeader) — the client retries on the new owner.
        for (std::size_t i = 0; i < pendingAcksAll_.size(); ) {
            if (!isOwned(pendingAcksAll_[i].replica)) {
                pendingAcksAll_[i].ret.errorCode =
                    static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
                enqueueReturn_(pendingAcksAll_[i].ret);
                pendingAcksAll_[i] = std::move(pendingAcksAll_.back());
                pendingAcksAll_.pop_back();
            } else {
                ++i;
            }
        }
        // Push subscriptions on a moved partition are parked (cursor kept, no
        // reads); re-activated if the partition comes back to this worker.
        for (auto& sub : subs_) {
            if (sub.replica) sub.active = isOwned(sub.replica);
        }
        // C-1 / M2: logs this worker no longer owns.  Their still-pending durable
        // entries would lose their seal gating (the slot goes) — fail them
        // RETRIABLY now (NotLeader: the client retries on the new owner), and
        // never submit for them again this pass.
        for (const auto& old : slots_) {
            bool kept = false;
            for (const auto& n : next) kept = kept || n.log == old.log;
            if (kept) continue;
            failOwnPending_(old.log, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), false);
            // Tokens still in flight for it resolve through purgedDirs_ (its dir
            // was cached while owned) — the moved Log itself is never read again.
            purgedDirs_.emplace_back(old.log, old.dir);
            dirtyThisCycle_.erase(std::remove(dirtyThisCycle_.begin(), dirtyThisCycle_.end(), old.log),
                                  dirtyThisCycle_.end());
        }
        slots_ = std::move(next);
        // Prune purged entries that are owned again or have no token in flight.
        for (std::size_t i = 0; i < purgedDirs_.size(); ) {
            bool live = false;
            for (const auto& n : slots_) live = live || n.log == purgedDirs_[i].first;
            bool inFlight = false;
            if (!live)
                for (const auto& r : routes_) inFlight = inFlight || (r.token != kNoToken && r.log == purgedDirs_[i].first);
            if (live || !inFlight) {
                purgedDirs_[i] = std::move(purgedDirs_.back());
                purgedDirs_.pop_back();
            } else {
                ++i;
            }
        }
        const std::size_t cap = pow2AtLeast_(2 * slots_.size() + 2);
        slotIndex_.assign(cap, {nullptr, 0u});
        slotIndexMask_ = cap - 1;
        bgInFlight_ = 0;
        for (uint32_t s = 0; s < slots_.size(); ++s) {
            std::size_t i = ptrHash_(slots_[s].log) & slotIndexMask_;
            while (slotIndex_[i].first != nullptr) i = (i + 1) & slotIndexMask_;
            slotIndex_[i] = {slots_[s].log, s};
            if (slots_[s].bgToken != kNoToken) ++bgInFlight_;
        }
    }

    void initRoutes_(std::size_t ioDepth) {
        const std::size_t cap = 2 * pow2AtLeast_(ioDepth < 2 ? 2 : ioDepth);
        routes_.assign(cap, TokenRoute{});
        routeMask_ = cap - 1;
    }
    bool routeInsert_(const TokenRoute& r) noexcept {
        for (std::size_t n = 0, i = static_cast<std::size_t>(r.token) & routeMask_; n <= routeMask_;
             ++n, i = (i + 1) & routeMask_) {
            if (routes_[i].token == kNoToken) { routes_[i] = r; return true; }
        }
        return false;   // impossible: in-flight tokens ≤ io depth ≤ capacity / 2
    }
    // Find + remove (backward-shift deletion keeps probe chains intact).
    bool routeTake_(CompletionToken t, TokenRoute& out) noexcept {
        if (t == kNoToken) return false;
        std::size_t i = static_cast<std::size_t>(t) & routeMask_;
        for (std::size_t n = 0;; ++n, i = (i + 1) & routeMask_) {
            if (n > routeMask_ || routes_[i].token == kNoToken) return false;
            if (routes_[i].token == t) break;
        }
        out = routes_[i];
        std::size_t hole = i;
        for (std::size_t j = (i + 1) & routeMask_; routes_[j].token != kNoToken; j = (j + 1) & routeMask_) {
            const std::size_t home = static_cast<std::size_t>(routes_[j].token) & routeMask_;
            // Move j into the hole iff its home is not in the cyclic range (hole, j].
            const bool inRange = (hole <= j) ? (home > hole && home <= j) : (home > hole || home <= j);
            if (!inRange) { routes_[hole] = routes_[j]; hole = j; }
        }
        routes_[hole] = TokenRoute{};
        return true;
    }
    // A route is still meaningful only while this worker owns the same slot.
    LogFlushSlot* liveSlot_(const TokenRoute& r) noexcept {
        LogFlushSlot* slot = slotOf_(r.log);
        return (slot && slot->epoch == r.epoch) ? slot : nullptr;
    }

    // Pin the active segment (only for a log this worker owns a slot for — the
    // pin is released on the completion through that slot) and submit its fd.
    // kNoToken on back-pressure.
    CompletionToken submitActive_(rune::storage::Log* log, RouteKind kind, uint32_t epoch,
                                  bool pin = true) noexcept {
        if (log->storageFailed()) [[unlikely]] return kNoToken;   // never touch a failed log's disk again
        rune::storage::LogSegment* seg = log->activeSegmentForFlush();
        if (pin) seg->pinFlush();
        const CompletionToken t = io_.submit(seg->logFd(), log->flushBoundary());
        if (t == kNoToken) { if (pin) log->unpinSegmentFlush(seg); return kNoToken; }
        if (!pin) seg = nullptr;
        if (!routeInsert_(TokenRoute{t, log, seg, epoch, kind})) {
            spdlog::critical("Worker {}: io token route table full (token={})", workerId_, t);
        }
        return t;
    }

    // Async roll seals queued by the log (shard mode).  A refused submit leaves
    // the seal queued (still pinned) for the next pass.
    int submitPendingSeals_(LogFlushSlot& slot) noexcept {
        int n = 0;
        rune::storage::Log* log = slot.log;
        while (log->pendingSealCount() > 0 && !log->storageFailed()) {
            rune::storage::LogSegment* seg = log->frontPendingSeal();
            const CompletionToken t = io_.submit(seg->logFd(), 0);
            if (t == kNoToken) break;
            log->popPendingSeal();   // the seal's pin now belongs to the route
            if (!routeInsert_(TokenRoute{t, log, seg, slot.epoch, RouteKind::Seal})) {
                spdlog::critical("Worker {}: io token route table full (token={})", workerId_, t);
            }
            ++slot.sealsInFlight;
            ++sealSubmits_;
            ++n;
        }
        return n;
    }

    // Request a background flush of `slot`'s log.  Returns true iff submitted now.
    bool requestBgFlush_(LogFlushSlot& slot) noexcept {
        rune::storage::Log* log = slot.log;
        if (log->storageFailed()) return false;                   // fail-stop: never again
        if (slot.bgToken != kNoToken) {                           // coalesce
            if (log->logEndOffset() > slot.bgLeo) slot.bgAgain = true;
            return false;
        }
        const CompletionToken t = submitActive_(log, RouteKind::Background, slot.epoch);
        if (t == kNoToken) {
            // Back-pressure (io in-flight depth): no retry table, no per-pass
            // spin — the next owned-log sync interval re-requests it.
            ++bgFlushDeferred_;
            if (bgFlushDeferred_ == 1 || (bgFlushDeferred_ & 1023u) == 0)
                spdlog::warn("Worker {}: background flush deferred (io back-pressure, total={})",
                             workerId_, bgFlushDeferred_);
            return false;
        }
        slot.bgToken = t;
        slot.bgLeo   = log->logEndOffset();
        slot.bgAgain = false;
        ++bgInFlight_;
        ++bgFlushSubmits_;
        return true;
    }

    // Shard-mode replacement for syncOwnedLogs_: same 10 ms gate and the same
    // "unchanged log = no work" rule, but each due log is an io_ submission.
    int syncOwnedLogsAsync_(int64_t nowNs) {
        if (lastSyncNs_ == 0) { lastSyncNs_ = nowNs; return 0; }
        if (nowNs - lastSyncNs_ < kWorkerSyncIntervalNs) return 0;
        lastSyncNs_ = nowNs;
        int n = 0;
        for (auto& slot : slots_) {
            if (slot.log->storageFailed()) continue;
            if (slot.log->logEndOffset() <= slot.lastSyncedLeo) continue;   // unchanged: no phantom work
            if (requestBgFlush_(slot)) ++n;
        }
        return n;
    }

    bool sealsOutstanding_(rune::storage::Log* log) noexcept {
        LogFlushSlot* slot = slotOf_(log);
        if (!slot) return false;
        return slot->sealsInFlight > 0 || log->pendingSealCount() > 0;
    }
    void fireDurable_(DurablePending& entry) {
        durableAckToken_ = entry.token;
        if (entry.thenAwaitHw) {
            // Shard Strict acks=all: durable locally — now the HW gate.  (M4: for
            // a single-ISR Strict log, HW / consumer visibility can precede the
            // fsync in shard mode — Kafka semantics; the PRODUCER ack never does.)
            entry.ret.errorCode = 0;
            PendingAcksAll pending;
            pending.replica    = entry.thenAwaitHw;
            pending.lastOffset = entry.lastOffset;
            pending.deadlineNs = entry.deadlineNs;
            pending.ret        = std::move(entry.ret);
            pendingAcksAll_.push_back(std::move(pending));
        } else {
            entry.ret.errorCode = 0; // OK
            enqueueReturn_(entry.ret);
        }
    }
    void fireSealGated_(LogFlushSlot& slot) {
        if (sealsOutstanding_(slot.log)) return;
        for (std::size_t i = 0; i < durablePending_.size(); ) {
            auto& entry = durablePending_[i];
            if (entry.awaitSeal && entry.logPtr == slot.log) {
                fireDurable_(entry);
                durablePending_[i] = std::move(durablePending_.back());
                durablePending_.pop_back();
            } else {
                ++i;
            }
        }
    }

    // ---- fsyncgate fail-stop -------------------------------------------------
    void enqueueSignal_(const WorkerReturn& r) {
        const std::size_t n = sbe::MessageHeader::encodedLength() +
                              sbe::WorkerReturn::computeLength(r.chunk.size());
        aeron::util::index_t idx = -1;
        try { idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n); } catch (...) { idx = -1; }
        if (idx < 0) { pendingSignals_.push_back(r); return; }   // retried next pass, never dropped
        encodeReturnSbe_(r, idx, n);
        ret_.commit(idx);
    }
    int flushPendingSignals_() {
        if (pendingSignals_.empty()) return 0;
        std::vector<WorkerReturn> again;
        again.swap(pendingSignals_);
        for (const auto& r : again) enqueueSignal_(r);
        return static_cast<int>(again.size() - pendingSignals_.size());
    }
    // The FIRST fsync error on a log: the log is permanently failed, every
    // durable/acks=all ack pending on it fails with StorageError (never OK),
    // and the runtime is signalled once to halt the broker.
    // Fail THIS worker's pending acks on `log` with `code` (never touches the Log).
    void failOwnPending_(const rune::storage::Log* log, int16_t code, bool includeAcksAll) {
        for (std::size_t i = 0; i < durablePending_.size(); ) {
            if (durablePending_[i].logPtr == log) {
                durablePending_[i].ret.errorCode = code;
                enqueueReturn_(durablePending_[i].ret);
                durablePending_[i] = std::move(durablePending_.back());
                durablePending_.pop_back();
            } else {
                ++i;
            }
        }
        if (!includeAcksAll) return;
        for (std::size_t i = 0; i < pendingAcksAll_.size(); ) {
            auto* r = pendingAcksAll_[i].replica;
            if (r && r->isDiskBacked() && r->log() == log) {   // log() is immutable: safe to read
                pendingAcksAll_[i].ret.errorCode = code;
                enqueueReturn_(pendingAcksAll_[i].ret);
                pendingAcksAll_[i] = std::move(pendingAcksAll_.back());
                pendingAcksAll_.pop_back();
            } else {
                ++i;
            }
        }
    }
    void signalStorageFailure_(const std::string& dir, int err, int32_t topicId, int32_t partition) {
        spdlog::critical("Worker {}: fsync FAILED on {} (errno={}) — log permanently failed; "
                         "signalling the runtime to halt the broker", workerId_, dir, err);
        WorkerReturn sig;
        sig.type      = WorkerReturn::Type::StorageFailure;
        sig.errorCode = static_cast<int16_t>(err);
        sig.topicId   = topicId;
        sig.partition = partition;
        sig.chunk.assign(dir.begin(), dir.end());
        ++storageFailureSignals_;
        enqueueSignal_(sig);
    }
    // The FIRST fsync error on a log THIS worker owns: the log is permanently
    // failed, every durable/acks=all ack pending on it fails with StorageError
    // (never OK), and the runtime is signalled once to halt the broker.
    void failLog_(rune::storage::Log* log, int err) {
        if (!log) return;
        log->markStorageFailed(err);
        failOwnPending_(log, static_cast<int16_t>(rune::protocol::client::ErrorCode::StorageError), true);
        LogFlushSlot* slot = slotOf_(log);
        if (slot && slot->failureReported) return;
        if (slot) slot->failureReported = true;
        signalStorageFailure_(log->dir(), log->storageErrno(),
                              slot && slot->replica ? slot->replica->topicId() : -1,
                              slot && slot->replica ? static_cast<int32_t>(slot->replica->partitionId()) : -1);
    }
    // C-1: an fsync error on a log this worker NO LONGER owns (its slot was
    // purged by an ownership move while the token was in flight).  The kernel
    // error state was consumed by THIS fsync, so the new owner's next fsync can
    // succeed over lost pages — the broker must halt.  Never write the other
    // worker's Log object: fail only this worker's own pending entries, signal.
    void failForeignLog_(const rune::storage::Log* log, int err) {
        failOwnPending_(log, static_cast<int16_t>(rune::protocol::client::ErrorCode::StorageError), false);
        for (const auto* f : foreignFailed_) if (f == log) return;
        foreignFailed_.push_back(log);   // cold
        std::string dir = "<moved log>";
        for (const auto& pd : purgedDirs_) if (pd.first == log) dir = pd.second;
        signalStorageFailure_(dir, err, -1, -1);
    }
    // After a truncate (role change): clamp the slot's LEO bookkeeping, and if
    // the truncate dropped an UNSUBMITTED pending seal, this worker's durable
    // entries on the log can no longer prove durability (their seal is gone) —
    // fail them RETRIABLY instead of wedging (and holding ack reservations).
    void reconcileTruncation_(LogFlushSlot& slot) {
        const int64_t leo = slot.log->logEndOffset();
        if (slot.lastSyncedLeo > leo) slot.lastSyncedLeo = leo;
        if (slot.bgLeo > leo) slot.bgLeo = leo;
        const uint64_t drops = slot.log->droppedPendingSeals();
        if (drops != slot.sealDropsSeen) [[unlikely]] {
            slot.sealDropsSeen = drops;
            failOwnPending_(slot.log, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), false);
        }
    }
    // Any inline sync failure (legacy Log::sync / durable append / seal) marks
    // the log failed in storage; this cold check turns it into the fail-stop.
    void checkStorageFailure_(rune::storage::Log* log) {
        if (!log->storageFailed()) [[likely]] return;
        LogFlushSlot* slot = slotOf_(log);
        if (slot && slot->failureReported) return;
        failLog_(log, log->storageErrno());
    }

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
        for (auto& slot : slots_) {
            rune::storage::Log* log = slot.log;
            if (log->storageFailed()) continue;
            // Skip logs with no new appends since their last sync — syncing an
            // unchanged log is a no-op fdatasync that must NOT count as work (else
            // the backoff idle never parks on an otherwise-idle worker).
            const int64_t leo = log->logEndOffset();
            if (leo <= slot.lastSyncedLeo) continue;
            try {
                log->sync();
                slot.lastSyncedLeo = leo;
                ++synced;
            } catch (const std::exception& ex) {
                // fsyncgate: the log is now permanently failed (LogSegment::sync);
                // fail-stop it (pending acks failed, runtime signalled to halt).
                spdlog::error("Worker {}: syncOwnedLogs_ log->sync() failed for {}: {}",
                              workerId_, log->dir(), ex.what());
                checkStorageFailure_(log);
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
            const std::size_t n = sbe::MessageHeader::encodedLength() +
                                  sbe::WorkerReturn::computeLength(ret.chunk.size());
            aeron::util::index_t idx;
            try {
                idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n);
            } catch (...) {
                // Encode/size error: discard this entry and continue.
                retOverflow_.erase(retOverflow_.begin());
                continue;
            }
            if (idx < 0) {
                break; // ring still full — stop; retry next cycle
            }
            encodeReturnSbe_(ret, idx, n);  // SBE wrap-in-place
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
                // Keyed by (partition<<24)|offset — unique across partitions;
                // correlates across nodes with follower apply/fetch + onFetchRequest.
                RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplPendingAckFire,
                    (static_cast<uint32_t>(entry.ret.partition) << 24) |
                        (static_cast<uint32_t>(entry.lastOffset) & 0xFFFFFFu));
                RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplAckSendOff,
                    (static_cast<uint32_t>(entry.ret.partition) << 24) |
                        (static_cast<uint32_t>(entry.lastOffset) & 0xFFFFFFu));
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
    // Acks already owed that will fire into ret_ later: acks=all parked on HW and,
    // in shard mode, Strict acks parked on their fsync (G-A6 reservation).
    [[nodiscard]] int64_t reservedAckSlots_() const noexcept {
        return static_cast<int64_t>(pendingAcksAll_.size()) +
               (shardMode_ ? static_cast<int64_t>(durablePending_.size()) : 0);
    }

    int pollWorkerIngress_(int64_t nowNs) {
        if (!workerIngress_ || !workerIngress_->resolved()) return 0;

        // Reservation-before-poll (E5.1.3 deliverable b):
        //   K = min(desiredFragmentLimit_, freeSlots − retOverflow_.size())
        // Guard G-A6: never poll more frames than we can ack without dropping.
        // Reserve return-ring slots for BOTH the acks about to be polled AND the
        // acks already parked in pendingAcksAll_ (they fire into ret_ later).  This
        // bounds pendingAcksAll_ by ret_ capacity while allowing acks=all pipelining.
        const int64_t freeSlots = ret_.freeSlotsForMsgSize(ackSlotBytes_);
        const int64_t reserved  = static_cast<int64_t>(retOverflow_.size()) + reservedAckSlots_();
        const int64_t available = (freeSlots > reserved) ? (freeSlots - reserved) : 0;
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
        RUNE_FR_SPAN_PRODUCE(rune_fr::Site::WorkerIngressRecv, 0);
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

        // Zero-copy: handleProduceBatch now takes a READ-ONLY view and the log
        // restamps the base offset in place via pwritev — no mutable local copy.
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

        // F3 round 5, item C2: the per-worker ingress path must enforce the
        // SAME effective max the gateway does — a UDP client's own term (not
        // the broker's) governs the image frame size, so nothing upstream
        // bounds it.  One compare; covers idempotent/transactional produce too
        // (same frame, same handler).  Rejected loudly, never appended.
        if (rbSpan.size() > maxProduceBatchBytes_) [[unlikely]] {
            // F3 round 6, item 2: rate-limited (first, then every 1024th) —
            // worker-thread-owned counter, same convention as oversizeOpRejects_.
            if (oversizeProduceRejects_++ % 1024 == 0) {
                ++oversizeProduceRejectLogs_;
                spdlog::warn("Worker {}: WorkerIngress: Produce batch too large ({} > {}) "
                             "topicId={} partition={}; rejected (MessageTooLarge, rejects={})",
                             workerId_, rbSpan.size(), maxProduceBatchBytes_,
                             msg.topicId, msg.partition, oversizeProduceRejects_);
            }
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MessageTooLarge);
            enqueueReturn_(ret);
            return 1;
        }

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

        // Durability tier — honor the topic/replica's configured tier so a
        // tier=strict (D2) topic actually acks after fdatasync (handleProduceBatch
        // does an inline synchronous log->sync() for tier==2).  Defaults to Majority
        // (1) when no replica is wired, preserving prior behavior.  (Previously this
        // was hardcoded to 1, so RUNE_TOPICS tier=strict was silently ignored on the
        // per-worker ingress path.)
        const uint8_t durabilityTier = replica ? replica->durabilityTier() : 1;
        // Task 10 (spec §3.1): shard mode never fsyncs inline on the loop.  Strict
        // appends page-cache only and acks from its own io_ completion (then the
        // HW gate for acks=all).
        const bool asyncStrict = shardMode_ && durabilityTier == 2;

        // Call the shared produce helper.
        rune::storage::ProduceOutcome outcome = rune::storage::handleProduceBatch(
            rbSpan.data(), rbSpan.size(),
            msg.topicId, msg.partition,
            msg.producerId, msg.producerEpoch, msg.baseSequence,
            *log, psm_,
            acksAll, isrSize, minIsr, asyncStrict ? uint8_t{1} : durabilityTier);

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

        // Waterfall: leader appended [baseOffset..lastOffset].  Keyed by lastOffset.
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplLeaderAppend,
            (static_cast<uint32_t>(msg.partition) << 24) |
                (static_cast<uint32_t>(outcome.lastOffset) & 0xFFFFFFu));
        // GR-4 Mode A: advance the replica's LEO/HW after successful append.
        if (replica) {
            replica->onLeaderLogAppended();
        }

        if (asyncStrict) {
            // Park on the coalesced fsync (submitCoalescedFlushes_ stamps the token
            // this pass).  A duplicate retry is parked too: its original may still
            // be awaiting its own fsync, and a Strict ack is never sent before the
            // data it acknowledges is durable.
            const int64_t lastOff = outcome.isDuplicate
                ? outcome.baseOffset + static_cast<int64_t>(outcome.recordCount) - 1
                : outcome.lastOffset;
            ret.errorCode = 0;
            DurablePending dp;
            dp.token       = kNoToken;
            dp.lastOffset  = lastOff;
            dp.logPtr      = log;
            dp.ret         = std::move(ret);
            dp.thenAwaitHw = acksAll ? replica : nullptr;
            dp.deadlineNs  = nowNs + ackTimeoutNs_;
            durablePending_.push_back(std::move(dp));
            if (std::find(dirtyThisCycle_.begin(), dirtyThisCycle_.end(), log) == dirtyThisCycle_.end())
                dirtyThisCycle_.push_back(log);
            return 1;
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
        // Keyed by (partition<<24)|offset (see ReplPendingAckFire).
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplPendingAckEnqueue,
            (static_cast<uint32_t>(ret.partition) << 24) |
                (static_cast<uint32_t>(outcome.lastOffset) & 0xFFFFFFu));
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
