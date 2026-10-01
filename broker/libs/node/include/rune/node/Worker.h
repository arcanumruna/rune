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
//             Back-pressure (F15 S3, design note §6): ONE local return FIFO
//             (retFifo_) in front of ret_ in EVERY mode — a refused claim
//             queues the return, the FIFO flushes front-to-back each pass, and
//             nothing is ever dropped.  A claim / encode exception or a full
//             FIFO is an invariant-violation halt (exit 70).  ret_ carries two
//             SBE templates (WorkerReturn id 2, TxnReturn id 6); drainReturn
//             dispatches on the template id.
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
#include <rune/node/RcState.h>
#include <rune/node/ReturnFifo.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerReturnCodec.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <rune_protocol_internal_sbe/WorkerOp.h>
#include <rune_protocol_internal_sbe/WorkerReturn.h>
#include <rune_protocol_internal_sbe/OutboundFrame.h>
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
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>  // E5.1.3: pc::ErrorCode enum (replaces magic literals)
#include <rune/codec/Validator.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
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


    // Pre-reserve capacity for the per-cycle dirty-log set (dedup, small).
    static constexpr std::size_t kDirtySetReserve = 64;

    // -----------------------------------------------------------------------
    // F15 S3 (design note §6, m5 + I8): ONE return FIFO in every mode, bounded.
    //
    // Returns go to ret_ only while the local FIFO (retFifo_) is empty, else to
    // its back; nothing is ever dropped.
    //
    // Bound (review S3 m1).  Let F = |FIFO| and O = owed returns (parked
    // acks=all + durablePending_ + owed Strict facts; S6 adds commit waiters /
    // parked markers — every owed source MUST be counted in reservedAckSlots_()).
    //   * O <= maxOwedReturns_ always: the cap is checked BEFORE the append on
    //     the gateway path (ackReservationAvailable_) and bounds the shard poll
    //     budget (ingressPollBudget_, K <= cap - O, one owed per frame).
    //   * Every owed→return conversion (HW fire, timeout, §3.2 fail, fsync
    //     completion / failure, ownership purge) moves one unit from O to F:
    //     F + O is unchanged.
    //   * Outside the owed set, one op / one polled frame emits at most
    //     kMaxReturnsPerOp (R = 1) return now.
    //   * F + O grows only through: an op drain with F == 0 (at most the drain
    //     limit, <= kMaxIngressDrainLimit, ops); an op drain with 0 < F <
    //     kOverflowHighWater (at most kOverflowHighWater - F ops); nothing when
    //     F >= kOverflowHighWater; a poll only when F == 0 (K <= cap - O).
    //   So F <= max(F + O) <= max(kOverflowHighWater, kMaxIngressDrainLimit) * R
    //   + kMaxOwedReturnsCeiling (the poll term kMaxFragmentLimit is kept in
    //   kRetFifoBound for margin).  The capacity is static_assert-ed against it
    //   for the CEILING of the configurable cap; exceeding it, a tryClaim
    //   exception or an encode error is an invariant-violation halt.
    // -----------------------------------------------------------------------
    /// Invariant-violation halt code (== BrokerNode::kExitInvariantViolation).
    static constexpr int kExitInvariantViolation = 70;
    /// Op-ring drain pauses while the return FIFO holds at least this many.
    static constexpr std::size_t kOverflowHighWater = 1024;
    /// A high-water pause EPISODE (logged on entry / exit) ends only at or
    /// below this (hysteresis, re-review 2 B-2); the drain itself still resumes
    /// below kOverflowHighWater.
    static constexpr std::size_t kOverflowLowWater = kOverflowHighWater / 2;
    /// Default static cap on owed returns (G-A6 reservation): parked acks=all +
    /// durablePending_ + owed Strict facts.  Configurable per worker
    /// (RUNE_WORKER_MAX_OWED_RETURNS → setMaxOwedReturns) within
    /// [kMinOwedReturns, kMaxOwedReturnsCeiling].  Both vectors are reserved to
    /// the configured cap, so parking never allocates.  4096 waiters/worker
    /// binds only when append→HW exceeds ~41 ms at 100k batches/s per worker.
    static constexpr std::size_t kMaxOwedReturns        = 4096;
    static constexpr std::size_t kMinOwedReturns        = 256;
    static constexpr std::size_t kMaxOwedReturnsCeiling = 8192;
    /// Clamps for the configurable per-pass limits (NodeConfig knobs).
    static constexpr int32_t     kMaxIngressDrainLimit = 4096;
    static constexpr int32_t     kMaxFragmentLimit     = 4096;
    /// Most returns one op / one polled frame emits beyond the owed set.
    static constexpr std::size_t kMaxReturnsPerOp = 1;
    static constexpr std::size_t kRetFifoBound =
        (kOverflowHighWater > static_cast<std::size_t>(kMaxIngressDrainLimit)
             ? kOverflowHighWater : static_cast<std::size_t>(kMaxIngressDrainLimit)) * kMaxReturnsPerOp +
        static_cast<std::size_t>(kMaxFragmentLimit) * kMaxReturnsPerOp + kMaxOwedReturnsCeiling;
    static constexpr std::size_t kRetFifoCapacity   = 16384;
    static_assert(kRetFifoCapacity >= kRetFifoBound,
                  "F15 §6: the return FIFO must hold its static bound at the owed-cap ceiling");
    static_assert((kRetFifoCapacity & (kRetFifoCapacity - 1)) == 0, "power of two (O(1) index mask)");
    static_assert(kMinOwedReturns <= kMaxOwedReturns && kMaxOwedReturns <= kMaxOwedReturnsCeiling);
    // Review S3 I-1: makes "ret ring >= ingress ring" exact for Legacy — a
    // batch that fits an op frame always fits a return frame.
    static_assert(sbe::WorkerReturn::sbeBlockLength() + sbe::WorkerReturn::chunkHeaderLength() <=
                  sbe::WorkerOp::sbeBlockLength() + sbe::WorkerOp::tailHeaderLength(),
                  "a WorkerReturn's framing must not exceed a WorkerOp's");

    /// Largest chunk one PushRead reads (ClientGateway::kMaxChunkBytes must
    /// equal it — static_assert in ClientGateway.cpp).
    static constexpr int32_t     kMaxPushReadChunkBytes = 512 * 1024;
    /// Largest client produce batch any intake path stores (the gateway's
    /// MessageTooLarge guard and the shard-ingress guard; static_assert-ed
    /// equal to ClientGateway::kMaxProduceBatchBytes).
    static constexpr std::size_t kMaxProduceBatchBytes  = 8u * 1024u * 1024u;

    /// Fixed framing of one WorkerReturn on ret_ (header + block + chunk length).
    [[nodiscard]] static constexpr std::size_t returnFixedBytes() noexcept {
        return static_cast<std::size_t>(sbe::MessageHeader::encodedLength()) +
               sbe::WorkerReturn::sbeBlockLength() +
               static_cast<std::size_t>(sbe::WorkerReturn::chunkHeaderLength());
    }
    /// Fixed framing of one WorkerOp on ingress_ (header + block + tail length).
    [[nodiscard]] static constexpr std::size_t opFixedBytes() noexcept {
        return static_cast<std::size_t>(sbe::MessageHeader::encodedLength()) +
               sbe::WorkerOp::sbeBlockLength() +
               static_cast<std::size_t>(sbe::WorkerOp::tailHeaderLength());
    }
    /// Framed bytes of a PushReadResult carrying a full chunk.
    [[nodiscard]] static constexpr std::size_t maxPushReadResultBytes() noexcept {
        return returnFixedBytes() + static_cast<std::size_t>(kMaxPushReadChunkBytes);
    }
    /// F15 §6 startup check (part): the return ring's max message (capacity / 8)
    /// holds one full PushReadResult chunk plus its header.
    [[nodiscard]] static constexpr bool returnRingCoversMaxPushChunk(std::size_t returnRingBytes) noexcept {
        return returnRingBytes / 8 >= maxPushReadResultBytes();
    }
    [[nodiscard]] bool returnRingCoversMaxPushChunk() const {
        return static_cast<std::size_t>(ret_.maxMsgLength()) >= maxPushReadResultBytes();
    }
    /// Fixed framing of one replicated FetchResponse on the wire (header + block
    /// + records length) and of its OutboundFrame envelope on the control
    /// transport's outbound ring.
    [[nodiscard]] static std::size_t replicationFrameFixedBytes() {
        return rune::replication::FetchResponseMsg::encodedSize(0);
    }
    [[nodiscard]] static constexpr std::size_t outboundEnvelopeFixedBytes() noexcept {
        return static_cast<std::size_t>(sbe::MessageHeader::encodedLength()) +
               sbe::OutboundFrame::sbeBlockLength() +
               static_cast<std::size_t>(sbe::OutboundFrame::payloadHeaderLength());
    }
    /// Re-review 2 B-1 / m-g: THE client-visible batch limit — the largest
    /// record batch any intake path accepts (the gateway's MessageTooLarge
    /// guard and the shard-ingress guard both use exactly this value, set by
    /// BrokerNode), and therefore the largest batch the broker can store:
    ///   min(8 MiB ceiling,
    ///       worker op ring max message − op framing      (gateway → worker op),
    ///       worker return ring max message − return framing  (one PushReadResult),
    ///       replication outbound ring max message − envelope − FetchResponse framing,
    ///       replication Aeron max message (min(16 MiB, term / 8)) − FetchResponse framing).
    /// F18 (large-batch replication) adopts this function as is.
    [[nodiscard]] static std::size_t maxAcceptableBatchBytes(std::size_t ingressRingBytes,
                                                             std::size_t returnRingBytes,
                                                             std::size_t replOutboundRingBytes,
                                                             std::size_t aeronTermBufferBytes) {
        auto fit = [](std::size_t maxMsg, std::size_t fixed) { return maxMsg > fixed ? maxMsg - fixed : 0; };
        const std::size_t repl = replicationFrameFixedBytes();
        const std::size_t aeronMax = std::min<std::size_t>(16u * 1024u * 1024u, aeronTermBufferBytes / 8);
        return std::min({kMaxProduceBatchBytes,
                         fit(ingressRingBytes / 8, opFixedBytes()),
                         fit(returnRingBytes / 8, returnFixedBytes()),
                         fit(replOutboundRingBytes / 8, outboundEnvelopeFixedBytes() + repl),
                         fit(aeronMax, repl)});
    }
    /// The part of the limit this worker's own rings impose (op + return ring,
    /// under the 8 MiB ceiling) — the default and the upper bound for
    /// setMaxBatchBytes.
    [[nodiscard]] std::size_t ringBatchFit_() const {
        const std::size_t opMax  = static_cast<std::size_t>(ingress_.maxMsgLength());
        const std::size_t retMax = static_cast<std::size_t>(ret_.maxMsgLength());
        return std::min({kMaxProduceBatchBytes,
                         opMax > opFixedBytes() ? opMax - opFixedBytes() : 0,
                         retMax > returnFixedBytes() ? retMax - returnFixedBytes() : 0});
    }
    /// Startup (before the runner starts): this worker's intake limit, normally
    /// maxAcceptableBatchBytes(config).  Refused loudly if 0 or above what its
    /// own rings can carry.
    void setMaxBatchBytes(std::size_t n) {
        if (n == 0 || n > ringBatchFit_())
            throw std::invalid_argument("Worker::setMaxBatchBytes: " + std::to_string(n) +
                                        " outside (0, " + std::to_string(ringBatchFit_()) + "]");
        maxIntakeBatchBytes_ = n;
    }
    /// F15 §6 + review S3 I-1 / re-review 1 I-3: the return ring's max message
    /// must hold max(one full PushReadResult chunk, the largest batch the Legacy
    /// path can store) plus the return framing; ret ring >= ingress ring (exact
    /// for Legacy since WorkerReturn's framing <= WorkerOp's); the owed-return
    /// cap within [kMinOwedReturns, kMaxOwedReturnsCeiling].  nullopt = OK;
    /// else the refusal reason.  BrokerNode::start() calls it before any side
    /// effect.  (A batch ALREADY on disk that exceeds the ring — stored under an
    /// earlier, bigger config — is answered MessageTooLarge on PushRead, never a
    /// halt: re-review 1 I-4.)
    [[nodiscard]] static std::optional<std::string> validateRingConfig(
        std::size_t returnRingBytes, std::size_t ingressRingBytes, int64_t maxOwedReturns) {
        // Every intake path refuses a batch above maxAcceptableBatchBytes (which
        // is <= the return-ring fit), so a stored batch always fits one
        // PushReadResult; what remains is one full 512 KiB PushRead chunk.
        const std::size_t retMax = returnRingBytes / 8;
        const std::size_t need   = maxPushReadResultBytes();
        if (retMax < need)
            return "RUNE_WORKER_RETURN_RING_BYTES=" + std::to_string(returnRingBytes) +
                   " is too small: its max message (capacity / 8 = " + std::to_string(retMax) +
                   " B) must hold one " + std::to_string(need) + " B PushReadResult (512 KiB chunk + header)";
        if (returnRingBytes < ingressRingBytes)
            return "RUNE_WORKER_RETURN_RING_BYTES=" + std::to_string(returnRingBytes) +
                   " must be >= RUNE_WORKER_INGRESS_RING_BYTES=" + std::to_string(ingressRingBytes);
        if (!owedCapInRange(maxOwedReturns))
            return "RUNE_WORKER_MAX_OWED_RETURNS=" + std::to_string(maxOwedReturns) + " must be in [" +
                   std::to_string(kMinOwedReturns) + ", " + std::to_string(kMaxOwedReturnsCeiling) + "]";
        return std::nullopt;
    }
    [[nodiscard]] static constexpr bool owedCapInRange(int64_t n) noexcept {
        return n >= static_cast<int64_t>(kMinOwedReturns) && n <= static_cast<int64_t>(kMaxOwedReturnsCeiling);
    }
    /// Configure this worker's owed-return cap (startup, before the runner
    /// starts).  Out of [kMinOwedReturns, kMaxOwedReturnsCeiling] (the FIFO
    /// capacity is static_assert-ed at the ceiling) is refused loudly
    /// (std::invalid_argument), never clamped (re-review 1 m-c).  Both waiter
    /// vectors are reserved to it so parking never allocates.
    void setMaxOwedReturns(std::size_t n) {
        if (!owedCapInRange(static_cast<int64_t>(n)))
            throw std::invalid_argument("Worker::setMaxOwedReturns: " + std::to_string(n) + " outside [" +
                                        std::to_string(kMinOwedReturns) + ", " +
                                        std::to_string(kMaxOwedReturnsCeiling) + "]");
        maxOwedReturns_ = n;
        pendingAcksAll_.reserve(maxOwedReturns_);
        durablePending_.reserve(maxOwedReturns_);
    }
    [[nodiscard]] std::size_t maxOwedReturns() const noexcept { return maxOwedReturns_; }
    /// Produces refused (gateway path) / shard polls skipped because the
    /// owed-return cap — not the ring — was the limiter.
    [[nodiscard]] uint64_t owedCapThrottlesForTest() const noexcept { return owedCapThrottles_; }
    // F15 S3 fix round 2 — RED stub / test seam.
    [[nodiscard]] uint64_t pushReadOversizeForTest() const noexcept { return pushReadOversize_; }
    [[nodiscard]] uint64_t retFifoPauseEpisodesForTest() const noexcept { return retFifoPauseEpisodes_; }
    [[nodiscard]] uint64_t owedCapEpisodesForTest() const noexcept { return owedCapEpisodes_; }
    // F15 S3 fix round 3 accessors.
    [[nodiscard]] std::size_t maxIntakeBatchBytes() const noexcept { return maxIntakeBatchBytes_; }
    [[nodiscard]] bool retFifoPausedForTest() const noexcept { return retFifoPaused_; }
    [[nodiscard]] bool owedCapThrottlingForTest() const noexcept { return owedCapThrottling_; }
    /// Test seam: emit an arbitrary return through the one return path.
    void enqueueReturnForTest(const WorkerReturn& r) { enqueueReturn_(r); }

    // -----------------------------------------------------------------------
    // F15 S4 (design note §2.1-§2.4, Option B): worker-owned read_committed
    // state (RcState per owned user partition).  Test seams (worker thread or
    // single-threaded tests only).
    // -----------------------------------------------------------------------
    /// Review I-2: read_committed PushReads held while their partition's RC
    /// state builds (each owes one return; beyond this → notReady fallback).
    static constexpr std::size_t kMaxHeldPushReads = 1024;
    /// Review m1: async RC read-ahead hints in flight per worker (the rest are
    /// skipped this pass), so hints never crowd fsyncs out of the io cap.
    static constexpr std::size_t kMaxRcHintsInFlight = 8;
    [[nodiscard]] std::size_t heldPushReadsForTest() const noexcept { return heldPushReads_.size(); }
    static constexpr int kRcNone     = -1;   ///< no RC state for the partition
    static constexpr int kRcUnbuilt  = 0;
    static constexpr int kRcScanning = 1;
    static constexpr int kRcReady    = 2;
    [[nodiscard]] std::size_t rcOpenTxnsForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->tracker().openCount() : 0;
    }
    [[nodiscard]] std::vector<rune::transaction::AbortedTxnRange> rcAbortedRangesForTest(int32_t topicId,
                                                                                         int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->aborted().ranges() : std::vector<rune::transaction::AbortedTxnRange>{};
    }
    [[nodiscard]] int rcStatusForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->statusCode() : kRcNone;
    }
    [[nodiscard]] uint64_t rcScanPassesForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->scanSteps() : 0;
    }
    [[nodiscard]] std::size_t rcMaxPassBytesForTest() const {
        std::size_t m = 0;
        for (const auto& sl : rcSlots_) m = std::max(m, sl.rc->maxStepBytes());
        return m;
    }
    [[nodiscard]] std::size_t rcTrackerCapacityForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->tracker().capacity() : 0;
    }
    [[nodiscard]] uint64_t rcTrackerGrowthsForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->tracker().growths() : 0;
    }
    /// Per-pass RC build budget (bytes per advance step).  Startup / tests only
    /// (before the runner starts).  Default RcState::kDefaultScanBytesPerPass.
    void setRcScanBytesPerPass(int32_t n) noexcept {
        rcScanBytesPerPass_ = n >= 1 ? n : RcState::kDefaultScanBytesPerPass;
    }
    /// Per-partition open-txn capacity reserved at ownership apply (rcOpenTxnCapacity).
    void setRcOpenTxnCapacity(std::size_t n) noexcept { rcOpenTxnCapacity_ = n >= 1 ? n : 1; }
    /// Bytes the RC build has read from the log (all steps).
    [[nodiscard]] uint64_t rcScannedBytesForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->scannedBytes() : 0;
    }
    /// Test seam: the residency probe every RC build consults (I-3).
    void setRcResidencyProbeForTest(RcResidencyProbe fn, void* ctx) {
        rcProbeFn_ = fn; rcProbeCtx_ = ctx;
        for (auto& sl : rcSlots_) sl.rc->setResidencyProbe(fn, ctx);
    }
    /// Test seam (RR1-1 / RR1-4): the read-ahead issuer every RC build uses.
    void setRcReadAheadForTest(RcReadAhead fn, void* ctx) {
        rcReadAheadOverride_ = fn; rcReadAheadOverrideCtx_ = ctx;
        for (auto& sl : rcSlots_) installReadAhead_(sl);
    }
    [[nodiscard]] uint64_t rcHintsSyncForTest() const noexcept { return rcHintsSync_; }
    [[nodiscard]] uint64_t rcHintsSkippedForTest() const noexcept { return rcHintsSkipped_; }
    [[nodiscard]] uint64_t rcHintErrorsForTest() const noexcept { return rcHintErrors_; }
    /// Fix round 3 test seams.  Fake async hints: the issuer behaves as on
    /// io_uring (token routed as RouteKind::Hint, in flight until its
    /// completion is delivered with deliverCompletionForTest).
    void setRcFakeAsyncHintsForTest(bool on) noexcept { rcFakeAsyncHints_ = on; }
    [[nodiscard]] CompletionToken lastRcHintTokenForTest() const noexcept { return lastRcHintToken_; }
    /// The synchronous hint (default RcState::syncWillNeed).
    using RcSyncHint = void (*)(void* ctx, const RcReadAheadReq& req);
    void setRcSyncHintForTest(RcSyncHint fn, void* ctx) noexcept { rcSyncHintFn_ = fn; rcSyncHintCtx_ = ctx; }
    [[nodiscard]] std::size_t rcHintsInFlightForTest() const noexcept { return rcHintsInFlight_; }
    [[nodiscard]] bool ioAsyncHintsForTest() const noexcept { return io_.asyncHints(); }
    [[nodiscard]] bool rcAsyncHintsDisabledForTest() const noexcept { return rcAsyncHintsDisabled_; }
    /// Test seam: an async read-ahead hint of (topicId, partition) completed.
    void completeRcHintForTest(int32_t topicId, int32_t partition) {
        if (RcState* rc = const_cast<RcState*>(rcFor_(topicId, partition))) rc->onHintCompleted();
    }
    [[nodiscard]] uint64_t rcHintsSubmittedForTest() const noexcept { return rcHintsSubmitted_; }
    /// PushRead ops executed by this worker (both answers and refusals).
    [[nodiscard]] uint64_t pushReadsForTest() const noexcept { return pushReads_; }

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
        durablePending_.reserve(kMaxOwedReturns);   // F15 S3: capped → never grows
        initRoutes_(ioSubmitDepth > ioComplDepth ? ioSubmitDepth : ioComplDepth);
        dirtyThisCycle_.reserve(kDirtySetReserve);
        pendingAcksAll_.reserve(kMaxOwedReturns);   // F15 S3: capped → never grows
        subs_.reserve(kSubsReserve);
        validators_.reserve(kValidatorsReserve);
        egressScratch_.resize(kEgressScratchSize);
        maxIntakeBatchBytes_ = ringBatchFit_();   // BrokerNode narrows it: setMaxBatchBytes
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
        /// F15 S4: the partition's RC state (this worker's), re-resolved on every
        /// ownership apply and Subscribe — null when not owned / internal.
        RcState* rc = nullptr;
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
    // K = ingressPollBudget_() (ring reservation AND the static owed cap)
    //
    // Source: NodeConfig::gatewayFragmentLimit (RUNE_GATEWAY_FRAGMENT_LIMIT, default 256).
    // Must be called before doWork() on the worker's thread (before runners start).
    // -----------------------------------------------------------------------
    void setDesiredFragmentLimit(int32_t limit) noexcept {
        desiredFragmentLimit_ = (limit >= 1) ? std::min(limit, kMaxFragmentLimit) : 256;   // F15 §6 bound
    }
    // Per-tick drain batch limits (NodeConfig knobs — see NodeConfig.h).  Call
    // before the worker's AgentRunner starts (startup, single-threaded).
    void setIngressDrainLimit(int32_t n) noexcept {
        ingressDrainLimit_ = (n >= 1) ? std::min(n, kMaxIngressDrainLimit) : 1024;   // F15 §6 bound
    }
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
    [[nodiscard]] bool tryEnqueueOp(WorkerOp op) {
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

        // Total frame = SBE MessageHeader (8) + block + varData.  computeLength()
        // covers block+varData only, so add the header explicitly.
        const std::size_t n =
            sbe::MessageHeader::encodedLength() + sbe::WorkerOp::computeLength(tailLen);
        const aeron::util::index_t idx =
            ingress_.tryClaim(sbe::WorkerOp::sbeTemplateId(), n);
        if (idx < 0) return false;  // ring full — back-pressure

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
           // Task 10 round 4: identity comes from the op (stamped from a registry
           // lookup at routing time) — never read through op.replica here.
           .targetReplicaId(op.targetReplicaId)
           .incarnation(op.incarnation)
           .minIsr(op.minIsr)
           .ownershipGen(op.ownershipGen)
           .tailKind(tailKind);
        enc.putTail(reinterpret_cast<const char*>(tailSrc),
                    static_cast<std::uint32_t>(tailLen));
        ingress_.commit(idx);
        return true;
    }

    // -----------------------------------------------------------------------
    // R3: return ring drain (consumer = gateway, on control thread).
    //
    // ret_ carries two SBE templates, discriminated by the template id in each
    // frame's message header (F15 §7, review m7):
    //   * WorkerReturn (id 2): decoded into a WorkerReturn struct (scalars in
    //     place; the PushReadResult chunk copied from the tail view) → fn.
    //   * TxnReturn (id 6): decoded (fixed block) → txnFn.
    // Frames stay in emission order across both templates (§1.1 L2).  The tail
    // view is fully consumed before the callback returns; no view escapes it.
    //
    // fn must be callable as fn(rune::node::WorkerReturn); txnFn as
    // txnFn(const rune::node::TxnReturn&).  The one-callback overload is for
    // consumers that never receive a TxnReturn: one arriving there — or any
    // unknown template id — is an invariant violation (std::logic_error; on the
    // runtime thread NodeRuntimeAgent turns it into the exit-70 halt), never a
    // silent skip.
    // -----------------------------------------------------------------------
    template <typename Fn>
    std::size_t drainReturn(Fn&& fn) {
        return drainReturn(std::forward<Fn>(fn), [](const TxnReturn&) {
            throw std::logic_error("Worker::drainReturn: a TxnReturn reached a consumer without a TxnReturn handler");
        });
    }

    template <typename Fn, typename TxnFn>
    std::size_t drainReturn(Fn&& fn, TxnFn&& txnFn) {
        int drained = ret_.drain(
            [&fn, &txnFn](int32_t /*typeId*/,
                          aeron::concurrent::AtomicBuffer& buf,
                          aeron::util::index_t off,
                          aeron::util::index_t len)
            {
                char* frame = reinterpret_cast<char*>(buf.buffer()) + off;

                // SBE zero-copy decode: wrap the ring frame, read fields in place.
                sbe::MessageHeader mh;
                mh.wrap(frame, 0, 0, static_cast<std::uint64_t>(buf.capacity()) - off);
                const auto templateId = mh.templateId();
                if (templateId == kTxnReturnTemplateId) [[unlikely]] {
                    TxnReturn txn;
                    if (!decodeTxnReturn(frame, static_cast<std::size_t>(len), txn)) [[unlikely]]
                        throw std::logic_error("Worker::drainReturn: malformed TxnReturn frame");
                    txnFn(txn);
                    return;
                }
                if (templateId != sbe::WorkerReturn::sbeTemplateId()) [[unlikely]]
                    throw std::logic_error("Worker::drainReturn: unknown return template id " +
                                           std::to_string(templateId));
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
                ret.targetReplicaId      = dec.targetReplicaId();
                ret.incarnation          = dec.incarnation();

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
    // F15 S3: the return FIFO (§6) — size, op-drain high-water pauses
    // (retFifoHighWaterPauses, watched by the DR matrix), TxnReturn emission
    // (S6 emits them; tests drive it directly) and the ingress poll budget.
    [[nodiscard]] std::size_t retFifoSizeForTest() const noexcept { return retFifo_.size(); }
    [[nodiscard]] uint64_t    retFifoHighWaterPausesForTest() const noexcept { return retFifoHighWaterPauses_; }
    void enqueueTxnReturnForTest(const TxnReturn& r) { enqueueTxnReturn_(r); }
    [[nodiscard]] int64_t ingressPollBudgetForTest() const noexcept { return ingressPollBudget_(); }
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
    [[nodiscard]] uint64_t followerSideOpsForTest() const noexcept { return followerSideOps_; }
    /// Bench / test: the per-op ownership check.
    [[nodiscard]] bool ownsReplicaForTest(const rune::replication::PartitionReplica* r) const noexcept {
        return ownsIncarnation_(r->registryId(), r->incarnation());
    }
    /// Bench: the per-op guard exactly as the dispatch runs it.
    [[nodiscard]] bool ownsIncarnationForTest(uint32_t id, uint32_t inc) const noexcept {
        return ownsIncarnation_(id, inc);
    }
    /// Bench (BM_WorkerRoleGate): the F15 produce role gate exactly as both
    /// produce paths run it.
    [[nodiscard]] static bool produceRoleGatePassesForTest(const rune::replication::PartitionReplica* r) noexcept {
        return produceRoleGatePasses_(r);
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

        passNowNs_ = nowNs;   // this pass's clock (acks=all deadlines on the op path)
        if (shardMode_) return doWorkShard_(nowNs);

        // Step E3: reap async-durability completions from the IoBackend FIRST so
        // that completions from the previous cycle fire before the next produce
        // cycle's ops are processed.  Per-worker, no shared state.
        work += reapDurableCompletions_();

        // E5.1.2d: G-A6 backpressure path (only active when perWorkerIngressActive_).
        //
        // 2a. Flush the return FIFO FIRST (F15 §6, every mode) — returns that
        //     could not be written to ret_ in a prior cycle, in emission order.
        //     Stops as soon as ret_ is full again.  While the FIFO is non-empty,
        //     no new ingress polling occurs and the op drain is capped / paused
        //     at kOverflowHighWater (drainIngress_).
        // 2b. Flush owed acks=all — checks each pending ack's replica HW;
        //     fires the ack when HW >= lastOffset, or a timeout error if overdue.
        // 3.  Poll WorkerIngress ONLY IF the FIFO is empty and the reservation
        //     (ring room + static owed cap) has room.  Skipping the poll lets
        //     Aeron's term buffer fill → client offer() back-pressures.  No
        //     return is ever dropped (G-A6 structural enforcement).
        work += flushRetFifo_();
        // F15 S2 (I-A, addendum R-4a): acks=all waiters are parked on THIS worker
        // in EVERY mode (gateway-routed produces included), so the per-pass fire
        // + §3.2 re-validation runs in every mode — not only with per-worker
        // ingress.
        work += flushOwedAcksAll_(nowNs);
        checkOwedCapEpisodeEnd_();
        if (perWorkerIngressActive_) {
            // Pipelined acks=all: poll new produce as long as the return ring has
            // space for the acks about to be polled PLUS all already-owed acks
            // (they fire later, into ret_).  Previously the gate required
            // pendingAcksAll_.empty(), which SERIALIZED acks=all on the ~172ms
            // replication round-trip (no new intake while any ack was in flight →
            // throughput = batch / round-trip).  Now the owed acks are bounded by
            // ret_ capacity (reservation below subtracts them) instead of 1.
            if (retFifo_.empty() &&
                ret_.freeSlotsForMsgSize(ackSlotBytes_) > reservedAckSlots_()) {
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
            work += rcAdvanceAll_();   // F15 §2.3: per-pass RC build driver
            work += serveHeldPushReads_();
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
        work += flushRetFifo_();
        // Owed acks (acks=all parked on HW, Strict parked on its fsync) all fire
        // into ret_ later: reserve for every one of them (G-A6, never drop).
        if (retFifo_.empty() &&
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
        work += rcAdvanceAll_();   // F15 §2.3: per-pass RC build driver
        work += serveHeldPushReads_();
        // 4b. follower flushes raised by the replicas (shard replicas run with
        //     setExternalFollowerFlush) → io backend, never an inline fsync.
        for (auto* replica : owned_) {
            if (replica && replica->takeFollowerFlushRequest() && replica->isDiskBacked()) {
                if (LogFlushSlot* slot = slotOf_(replica->log())) work += requestBgFlush_(*slot) ? 1 : 0;
            }
        }
        // 5. fire due produce acks.
        work += flushOwedAcksAll_(nowNs);
        checkOwedCapEpisodeEnd_();
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
    // F15 S3 (§6, m5 + I8; §1.1 L2): ONE return path in every mode.
    //
    // enqueueReturn_ / enqueueTxnReturn_: if the local FIFO (retFifo_) is
    // non-empty, append to its back (a later return never overtakes an earlier
    // one through a ring slot that freed up in between); else tryClaim a ret_
    // slot and encode in place (SBE wrap-in-place, no intermediate copy); on a
    // refusal (ring full), append to the FIFO.  flushRetFifo_ drains it
    // front-to-back each pass and stops at the first refusal.
    //
    // Never a drop, in any mode (the Legacy-mode silent drop is gone).  A
    // tryClaim exception (a return the ring can never hold), an encode error,
    // or a full FIFO (beyond its static bound) is an invariant-violation halt
    // (exit 70) — never a drop, never a retry loop.
    //
    // Bypass (L2): enqueueSignal_ for StorageFailure / OwnershipAck only.
    // Worker-thread only (the producer side of ret_).
    // -----------------------------------------------------------------------
    void enqueueReturn_(const WorkerReturn& ret) {
        if (retFifo_.empty() && tryWriteReturn_(ret)) [[likely]] return;
        if (!retFifo_.push(ret)) [[unlikely]] haltRetFifoFull_();
    }
    void enqueueReturn_(WorkerReturn&& ret) {
        if (retFifo_.empty() && tryWriteReturn_(ret)) [[likely]] return;
        if (!retFifo_.push(std::move(ret))) [[unlikely]] haltRetFifoFull_();
    }
    void enqueueTxnReturn_(const TxnReturn& r) {
        if (retFifo_.empty() && tryWriteTxnReturn_(r)) return;
        if (!retFifo_.push(r)) [[unlikely]] haltRetFifoFull_();
    }

    // One claim + in-place encode + commit of a WorkerReturn.  False = ring
    // full (back-pressure); any exception halts.
    [[nodiscard]] bool tryWriteReturn_(const WorkerReturn& ret) {
        aeron::util::index_t idx = -1;
        std::size_t n = 0;
        try {
            n   = workerReturnEncodedLength(ret);
            idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n);
        } catch (const std::exception& ex) {
            haltInvariant_("return-ring tryClaim threw", ex.what(), ret.chunk.size());
        } catch (...) {
            haltInvariant_("return-ring tryClaim threw", "non-std exception", ret.chunk.size());
        }
        if (idx < 0) return false;
        try {
            encodeWorkerReturnSbe(reinterpret_cast<char*>(ret_.base() + idx), n, ret);
        } catch (const std::exception& ex) {
            ret_.abort(idx);
            haltInvariant_("WorkerReturn encode threw", ex.what(), ret.chunk.size());
        } catch (...) {
            ret_.abort(idx);
            haltInvariant_("WorkerReturn encode threw", "non-std exception", ret.chunk.size());
        }
        ret_.commit(idx);
        return true;
    }
    [[nodiscard]] bool tryWriteTxnReturn_(const TxnReturn& r) {
        constexpr std::size_t n = txnReturnEncodedLength();
        aeron::util::index_t idx = -1;
        try {
            idx = ret_.tryClaim(kTxnReturnTemplateId, n);
        } catch (const std::exception& ex) {
            haltInvariant_("return-ring tryClaim threw (TxnReturn)", ex.what(), 0);
        } catch (...) {
            haltInvariant_("return-ring tryClaim threw (TxnReturn)", "non-std exception", 0);
        }
        if (idx < 0) return false;
        if (encodeTxnReturn(reinterpret_cast<char*>(ret_.base() + idx), n, r) != n) [[unlikely]] {
            ret_.abort(idx);
            haltInvariant_("TxnReturn encode failed", "short buffer", 0);
        }
        ret_.commit(idx);
        return true;
    }

    // Invariant-violation halt from the worker thread (exit 70, the same code
    // as BrokerNode::kExitInvariantViolation).  Fail-stop: the worker cannot
    // deliver a return it owes, and dropping it would silently lose an answer
    // or a transactional fact.  Cold, never returns.
    // Review S3 m8: this deliberately does NOT go through BrokerNode's
    // HaltHandler — that handler is a runtime-thread hook that requests a
    // graceful stop and RETURNS, and this worker cannot continue the call it
    // is in (the owed return has nowhere to go).  Only the default spdlog
    // logger is flushed before _Exit.
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]]
    void haltInvariant_(const char* what, const char* detail, std::size_t chunkBytes) const noexcept {
        spdlog::critical("Worker {}: INVARIANT VIOLATION — {} ({}; chunk={} B, ret_ maxMsgLength={}, "
                         "return FIFO {}/{}) — halting (exit {})",
                         workerId_, what, detail, chunkBytes, ret_.maxMsgLength(), retFifo_.size(),
                         retFifo_.capacity(), kExitInvariantViolation);
        spdlog::default_logger()->flush();
        std::_Exit(kExitInvariantViolation);
    }
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]] void haltRetFifoFull_() const noexcept {
        haltInvariant_("return FIFO full (beyond its static bound)", "F15 §6 bound exceeded", 0);
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
                        eraseDurable_(i);   // O(1) swap-with-back
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
        // F15 S4 fix round 2 (RR1-4): an RC read-ahead hint — never a flush:
        // success or error, it only re-arms its partition's next hint.
        if (routed && r.kind == RouteKind::Hint) [[unlikely]] {
            onRcHintCompleted_(r.log, c.errorCode);
            return 1;
        }
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
                eraseDurable_(i);
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
        //
        // F15 S3 (§6), every mode: with a non-empty return FIFO, drain at most
        // kOverflowHighWater − |FIFO| ops (each emits at most one return), and
        // none once |FIFO| >= kOverflowHighWater — the rest waits in the op ring
        // (control-side producers already queue on a full op ring).  Transient:
        // the control thread drains ret_ every cycle and never waits on an op
        // ring, so the FIFO always flushes (no deadlock).
        int limit = ingressDrainLimit_;
        // Re-review 2 B-2: hysteresis — the episode ends only at low water.
        if (retFifoPaused_ && retFifo_.size() <= kOverflowLowWater) [[unlikely]] leaveRetFifoPause_();
        if (const std::size_t backlog = retFifo_.size(); backlog != 0) [[unlikely]] {
            if (backlog >= kOverflowHighWater) {
                ++retFifoHighWaterPauses_;
                if (!retFifoPaused_) [[unlikely]] enterRetFifoPause_(backlog);
                return 0;
            }
            limit = std::min(limit, static_cast<int>(kOverflowHighWater - backlog));
        }
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
            limit);   // configurable (RUNE_WORKER_INGRESS_DRAIN_LIMIT, ≤ kMaxIngressDrainLimit); was the 16 default
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
            // Task 10 round 4: execute against THIS worker's own pointer for the
            // owned incarnation — the pointer the op carried is never trusted.
            op.replica(reinterpret_cast<std::uint64_t>(ownedPtr_[id]));
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
        // Task 10 round 4: follower/peer-side handlers (were inline on the
        // control thread — a cross-thread race on a worker-owned replica).
        case WorkerOp::Type::OffsetsForLeaderEpochResp:
            executeOffsetsForLeaderEpochRespWire_(op);
            break;
        case WorkerOp::Type::ReconcileFetchReq:
            executeReconcileFetchReqWire_(op);
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
                s.rc                   = rcOf_(subReplica_(s));
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
        s.rc                   = rcOf_(subReplica_(s));
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
    // read_committed → the LSO of this worker's own RC state (F15 §2.4, K1);
    // aborted transactional batches are skipped.  Control batches are skipped
    // (never delivered to clients).
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

            // Bound: the live HW (the worker owns the replica); read_committed
            // (F15 §2.4, K1): the LSO of THIS worker's RC state — read on the
            // thread that appends, so it can never lag a txn's first batch (K2).
            // Not Ready (§2.3): skip this subscription this pass.
            auto* rep = subReplica_(s);
            if (s.incarnation != 0 && !rep) continue;   // not owned (defensive: active implies owned)
            int64_t bound = rep ? rep->highWatermark()
                                : s.logPtr->logEndOffset();
            const RcState* rc = nullptr;
            if (s.isolation == kIsolationReadCommitted && s.topicId >= 0) [[unlikely]] {
                if (rep == nullptr) continue;   // review m5: no replica → no RC state → fail closed
                rc = rcServe_(rep, s.rc);
                if (rc == nullptr) continue;
                bound = rc->lso(bound);
            }
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
                int32_t lastDelta = 0;
                for (int i = 0; i < 4; ++i)
                    lastDelta |= (static_cast<int32_t>(bp[kLastOffsetDeltaOffset + i]) << (i * 8));
                if (baseOffset + lastDelta >= bound) break;  // reached the HW / LSO bound

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
                // F15 §2.4: read_committed skips aborted transactional batches
                // (only TRANSACTIONAL batches pay the lookup).  R-2: a group sub
                // whose isolation carrier has not arrived stops AT it instead.
                if (rc != nullptr && (attrs & kTxnAttrBit) != 0) [[unlikely]] {
                    int64_t pid = 0;
                    for (int i = 0; i < 8; ++i)
                        pid |= (static_cast<int64_t>(bp[kProducerIdOffset + i]) << (i * 8));
                    if (rc->isAborted(pid, baseOffset)) {
                        if (s.isGroup && !s.groupCarrierReceived) break;
                        pos += batchSize;
                        s.nextOffset = baseOffset + static_cast<int64_t>(lastDelta) + 1;
                        continue;
                    }
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
        // F15 S2 fix round 2 (B1): a worker result is an ANSWER (deferAck false)
        // unless it is the append-time facts-only copy emitted below — never
        // inherit op.deferAck (an error result marked deferAck would be taken
        // for facts by the gateway: no answer, mirrors poisoned).
        ret.deferAck      = false;
        ret.replica       = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        ret.targetReplicaId = op.targetReplicaId();
        ret.incarnation     = op.incarnation();
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

        // F15 §3.1 (I1): the OWNING worker is the role authority.  A produce
        // routed while a BecomeFollower (or a self-demotion) was ahead of it is
        // refused retriably — the follower log is never touched.
        if (!produceRoleGatePasses_(replica)) [[unlikely]] {
            ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
            ret.deferAck  = false;
            enqueueReturn_(ret);
            return;
        }

        // F15 §2.3 produce gate: an idempotent / transactional batch is taken
        // only while this partition's RC (and producer) state is Ready — one
        // inline build step first; else NotLeader (retriable, log untouched).
        // pid -1 batches skip it (the same branch as the PSM check).
        RcState* rc = nullptr;
        if (op.producerId() >= 0 && replica != nullptr && topicId >= 0) [[unlikely]] {
            rc = rcGate_(replica);
            if (rc == nullptr) {
                ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
                ret.deferAck  = false;
                enqueueReturn_(ret);
                return;
            }
        }

        // Task 10 round 5: the min-ISR admission gate for acks=all lives HERE, on
        // the owning thread (the replica's ISR is worker-owned state).  The
        // gateway used to read replica->isr() on the control thread — a data race
        // that could segfault mid-promotion.  deferAck == acks=all on a
        // replicated partition in steady state (exactly the old gateway gate).
        if (op.deferAck() != 0 && replica &&
            static_cast<int>(replica->isr().size()) <
                (op.minIsr() > 0 ? op.minIsr() : replica->minIsr())) [[unlikely]] {
            ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation);
            ret.deferAck  = false;
            enqueueReturn_(ret);   // log untouched — no append, no LEO advance
            return;
        }

        // F15 S2 (I-A, addendum R-4c): the acks=all waiter will be parked on THIS
        // worker and fire into ret_ later — reserve its return slot BEFORE the
        // append (the G-A6 reservation, now covering gateway-routed produces;
        // the gateway's old pendingAcks_ cap is gone).  No slot → retriable
        // NotLeader with the log untouched (a retry is always safe).
        const bool acksAll = op.deferAck() != 0 && replica != nullptr;
        // A produce whose answer is PARKED (acks=all on the HW, Strict on its
        // fsync) also emits an append-time facts-only return (see below): it
        // needs TWO return slots — the facts now, the final answer later.
        const bool strict = op.durabilityTier() == 2 && log->isDiskBacked();
        const bool parks  = acksAll || strict;
        if (parks && !ackReservationAvailable_(/*returnsNow=*/1)) [[unlikely]] {
            ++acksAllRefusedNoReservation_;
            if (!owedCapAllows_(1)) noteOwedCapThrottle_("gateway-routed produce");
            if (acksAllRefusedNoReservation_ == 1 || (acksAllRefusedNoReservation_ & 1023u) == 0)
                spdlog::warn("Worker {}: parked (acks=all / Strict) produce refused NotLeader — return-ring reservation "
                             "exhausted (owed={}, total refused={})", workerId_, reservedAckSlots_(),
                             acksAllRefusedNoReservation_);
            // NotLeader: the only retriable "nothing was appended, retry"
            // code the client protocol has without a protocol change (review
            // m11: MinIsrViolation would misreport the ISR; a dedicated
            // back-pressure code is a protocol change).
            ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
            ret.deferAck  = false;
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

        // F15 §2.1 (C1): the RC update is in the SAME invocation as the append —
        // independent of the ack, the ProduceResult and the session.
        if (rc != nullptr && (txnAttrs_(tail, tailLen) & kTxnAttrBit) != 0) [[unlikely]]
            rc->onTxnBatch(op.producerId(), base);

        // F15 I-A: this worker answers the produce itself (immediately, after
        // its fsync, or when the HW passes it) — the final result is final, the
        // gateway never parks it.
        ret.deferAck = false;
        if (acksAll && !strict) [[unlikely]] {
            // TEMPORARY (F15 S2 fix round 1, review C1; DELETED by S4 — LSO on
            // W — and S5 — PSM on W): until then the gateway still mirrors the
            // idempotent PSM, the LsoTracker and the dirty-log set for
            // gateway-received produces.  They must learn the append NOW, in the
            // same invocation as the append and before any ack decision — not
            // with the final answer, which comes a replication RTT later or as
            // an error that carries no facts (pre-S2 semantics for acks=all).
            // A Strict produce's facts wait for its fsync (fireDurable_, ruling
            // of re-review 1): a duplicate is never answered OK for data that
            // is not durable.
            emitFactsOnly_(ret);   // slot reserved above (ackReservationAvailable_(1))
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
        if (strict) {
            ret.errorCode = 0; // tentatively OK; updated in submitCoalescedFlushes_ or on completion
            // token=0 means "not yet submitted" — reapDurableCompletions_ will never fire this.
            // F15 R-4b: a Strict acks=all ack waits for its fsync, THEN the HW
            // (thenAwaitHw) — in every mode.
            DurablePending dp;
            dp.token       = kNoToken;
            dp.lastOffset  = ret.lastOffset;
            dp.logPtr      = log;
            dp.ret         = std::move(ret);
            dp.thenAwaitHw = acksAll ? replica : nullptr;
            dp.deadlineNs  = passNowNs_ + ackTimeoutNs_;
            dp.replica     = replica;
            dp.logGen      = replica ? replica->logGeneration() : 0;
            dp.minIsr      = acksAll ? (op.minIsr() > 0 ? op.minIsr() : replica->minIsr()) : 1;
            dp.factsOwed   = true;   // TEMPORARY (S5): facts emitted at fsync completion
            ++strictFactsOwed_;      // reserved above (ackReservationAvailable_(1))
            durablePending_.push_back(std::move(dp));

            // Record this log as dirty for this cycle (dedup).
            bool found = false;
            for (auto* dl : dirtyThisCycle_) {
                if (dl == log) { found = true; break; }
            }
            if (!found) dirtyThisCycle_.push_back(log);
            return;
        }

        if (acksAll) {
            // F15 I-A: park on THIS worker (was: the gateway's pendingAcks_ on
            // the published HW snapshot).  Fires in flushOwedAcksAll_ when the
            // HW passes it with the ISR >= minIsr (DR-BUG-3), re-validated every
            // pass against role / ownership / log generation (§3.2).
            PendingAcksAll pending;
            pending.replica    = replica;
            pending.lastOffset = ret.lastOffset;
            pending.deadlineNs = passNowNs_ + ackTimeoutNs_;
            pending.logGen     = replica->logGeneration();
            pending.minIsr     = op.minIsr() > 0 ? op.minIsr() : replica->minIsr();
            pending.ret        = std::move(ret);
            pendingAcksAll_.push_back(std::move(pending));
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
    // F15 S4 fix round 1 (I-2): the scalars of one PushRead — from the op, or
    // from a read HELD while its partition's RC state builds.
    struct PushReadArgs {
        int64_t subscriptionId = 0;
        int64_t sessionId      = 0;
        int32_t topicId        = 0;
        int32_t partition      = 0;
        int64_t nextOffset     = 0;
        int64_t hwBound        = 0;
        uint8_t isolation      = 0;
        bool    isGroup        = false;
        bool    carrier        = true;
        uint32_t targetReplicaId = 0;
        uint32_t incarnation     = 0;
        rune::replication::PartitionReplica* replica = nullptr;
        rune::storage::Log*                  logPtr  = nullptr;
    };
    void executePushReadWire_(sbe::WorkerOp& op) {
        RUNE_FR_SPAN_READ(rune_fr::Site::ReadEncodePush,
                          static_cast<uint32_t>(op.sessionId()));
        ++pushReads_;
        PushReadArgs a;
        a.subscriptionId  = op.subscriptionId();
        a.sessionId       = op.sessionId();
        a.topicId         = op.topicId();
        a.partition       = op.partition();
        a.nextOffset      = op.nextOffset();
        a.hwBound         = op.hwBound();
        a.isolation       = op.isolation();
        a.isGroup         = op.isGroup() != 0;
        a.carrier         = op.groupCarrierReceived() != 0;
        a.targetReplicaId = op.targetReplicaId();
        a.incarnation     = op.incarnation();
        a.replica         = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        a.logPtr          = getLogFromWire_(op);
        servePushRead_(a, /*mayHold=*/true);
    }

    void servePushRead_(const PushReadArgs& a, bool mayHold) {
        const int32_t topicId    = a.topicId;
        const int32_t partition  = a.partition;
        const int64_t nextOffset = a.nextOffset;
        const int64_t hwBound    = a.hwBound;
        WorkerReturn ret;
        ret.type                 = WorkerReturn::Type::PushReadResult;
        ret.subscriptionId       = a.subscriptionId;
        ret.sessionId            = a.sessionId; // gateway needs this to find the session
        ret.readTopicId          = topicId;
        ret.readPartition        = partition;
        ret.newNextOffset        = nextOffset; // will be advanced by gateway on success
        ret.hwBound              = hwBound;
        ret.isolation            = a.isolation;
        ret.isGroup              = a.isGroup;
        ret.groupCarrierReceived = a.carrier;
        ret.setPushEffectiveBound(-1);   // N5: unanswered unless a path below answers explicitly

        rune::storage::Log* log = a.logPtr;
        if (!log) {
            // No log: an UNANSWERED read (review I-1) — the gateway re-issues it
            // and never parks the subscription on it.
            ret.setPushEffectiveBound(-1);
            enqueueReturn_(ret);
            return;
        }

        // F15 §2.4 (I-B): the read_committed decision for the GATEWAY push path
        // is made HERE, on the owning worker, from its own RC state and live HW
        // (L5).  The gateway's HW snapshot (op.hwBound) only ever loosens the
        // request; the worker tightens the answer:
        //   read_uncommitted: bound = min(op.hwBound, live HW)
        //   read_committed:   bound = min(op.hwBound, LSO(live HW)).  While the
        //                     RC state is not Ready (§2.3) the read is HELD here
        //                     and answered once it is (review I-2, addendum R-3
        //                     "or RC readiness": one outstanding read per
        //                     subscription, no gateway re-issue per cycle).
        //                     Hold capacity exhausted → an unanswered notReady
        //                     result (the gateway backs off in cycles).
        // Internal partitions (topicId < 0) carry no transactional batches (§1):
        // HW-bounded only.  A replication-less partition has no RC state: its
        // read_committed fails CLOSED (review m5) — nothing, answered at the cursor.
        auto* replica = a.replica;
        int64_t bound = hwBound;
        const RcState* rc = nullptr;
        if (replica != nullptr) {
            const int64_t liveHw = replica->highWatermark();
            bound = std::min(bound, liveHw);
            if (a.isolation == kIsolationReadCommitted && topicId >= 0) {
                rc = rcServe_(replica, rcOf_(replica));
                if (rc == nullptr) [[unlikely]] {
                    // RR1-2: hold only while the partition is Leader (building);
                    // a follower / demoted partition answers at once (notReady).
                    if (mayHold && replica->role() == rune::replication::Role::Leader &&
                        rcOf_(replica) != nullptr && heldPushReads_.size() < kMaxHeldPushReads &&
                        owedCapAllows_(0)) {
                        heldPushReads_.push_back(a);   // reserved: never allocates
                        return;
                    }
                    ret.setPushRcNotReady(true);
                    ret.setPushEffectiveBound(-1);
                    enqueueReturn_(std::move(ret));
                    return;
                }
                bound = std::min(bound, rc->lso(liveHw));
            }
        } else if (a.isolation == kIsolationReadCommitted && topicId >= 0) [[unlikely]] {
            ret.setPushEffectiveBound(nextOffset);
            enqueueReturn_(std::move(ret));
            return;
        }
        ret.setPushEffectiveBound(bound);

        // Only read if there are records available (within the bound).
        if (nextOffset < bound) {
            try {
                // kMaxPushReadChunkBytes is mirrored by ClientGateway.  Gateway
                // sets hwBound before enqueuing.
                ret.chunk = log->read(nextOffset, kMaxPushReadChunkBytes);
                // F15 S3: Log::read can return MORE than the budget (its window
                // grows to reach a batch larger than it).  A return the ring can
                // never hold would halt (§6) — cut the chunk at whole batches so
                // it fits; only a single batch larger than the ring's max message
                // is left to the claim's invariant halt.
                if (ret.chunk.size() > pushReadChunkFit_()) [[unlikely]] {
                    trimToWholeBatches_(ret.chunk, pushReadChunkFit_());
                    // Re-review 1 I-4: a SINGLE stored batch larger than the
                    // ring's max message (stored under an earlier, bigger
                    // config, or received from a peer and then led) cannot be
                    // returned.  Answer this read non-retriably — never the
                    // claim halt, which would crash-loop on every restart.
                    if (ret.chunk.size() > pushReadChunkFit_()) {
                        std::vector<uint8_t>().swap(ret.chunk);
                        ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::MessageTooLarge);
                        notePushReadOversize_(topicId, partition, nextOffset);
                    }
                }
            } catch (const std::exception& ex) {
                spdlog::warn("Worker {}: log->read threw for topicId={} partition={}: {}",
                             workerId_, topicId, partition, ex.what());
                // Return empty chunk — gateway will skip + retry next cycle.
            } catch (...) {
                spdlog::warn("Worker {}: log->read threw unknown exception", workerId_);
            }
            if (ret.errorCode == 0 && !ret.chunk.empty())
                clampPushChunk_(ret, bound, rc, a.isGroup && !a.carrier);
        }

        enqueueReturn_(std::move(ret));   // chunk moved (never copied) into the FIFO if queued
    }

    // F15 §2.4 (I-B) + addendum R-2: clamp a PushRead chunk to `bound` (batches
    // with lastOffset < bound only), strip control batches and — read_committed
    // (rc != null) — aborted transactional batches, compacting in place (no
    // allocation; one attribute check per batch in the walk the read needs
    // anyway).  newNextOffset = past the last batch EXAMINED (copied or
    // stripped), so the gateway never re-reads a stripped batch (R-1).
    // preCarrier (TX-6b, R-2): a group sub whose isolation carrier has not
    // arrived is CUT at its first aborted batch instead — the batch stays
    // ahead of the cursor until the true isolation is known — and the cut is
    // echoed as the effective bound (the gateway parks there until the carrier).
    static void clampPushChunk_(WorkerReturn& ret, int64_t bound, const RcState* rc, bool preCarrier) {
        using rune::protocol::RecordBatchReader;
        namespace b = rune::protocol::batch;
        std::vector<uint8_t>& c = ret.chunk;
        const std::size_t n = c.size();
        std::size_t rd = 0, wr = 0;
        int64_t next = ret.newNextOffset;
        while (rd + static_cast<std::size_t>(b::kRecordBatchOverhead) <= n) {
            RecordBatchReader r(c.data() + rd, n - rd);
            const int32_t total = r.totalLength();
            if (total <= 0 || rd + static_cast<std::size_t>(total) > n) break;
            const int64_t base = r.baseOffset();
            const int64_t last = base + r.lastOffsetDelta();
            if (last >= bound) break;                                  // the HW / LSO bound
            const auto attrs = static_cast<uint16_t>(r.attributes());
            if ((attrs & static_cast<uint16_t>(b::kAttrControl)) != 0) {   // markers: never delivered
                rd += static_cast<std::size_t>(total);
                next = last + 1;
                continue;
            }
            if (rc != nullptr && (attrs & kTxnAttrBit) != 0 && rc->isAborted(r.producerId(), base)) [[unlikely]] {
                if (preCarrier) {                                      // R-2: cut, never strip
                    ret.setPushEffectiveBound(base);
                    break;
                }
                rd += static_cast<std::size_t>(total);
                next = last + 1;
                continue;
            }
            if (wr != rd) std::memmove(c.data() + wr, c.data() + rd, static_cast<std::size_t>(total));
            wr += static_cast<std::size_t>(total);
            rd += static_cast<std::size_t>(total);
            next = last + 1;
        }
        if (wr == 0) std::vector<uint8_t>().swap(c);   // empty: release the read buffer
        else         c.resize(wr);                       // shrink only: no reallocation
        ret.newNextOffset = next;
    }

    [[gnu::cold]] void notePushReadOversize_(int32_t topicId, int32_t partition, int64_t offset) noexcept {
        ++pushReadOversize_;
        if (pushReadOversize_ == 1 || (pushReadOversize_ & 1023u) == 0)
            spdlog::error("Worker {}: topicId={} partition={} offset={}: a stored batch is larger than the "
                          "return ring's max message ({} B) — PushRead answered MessageTooLarge (total {}); "
                          "raise RUNE_WORKER_RETURN_RING_BYTES", workerId_, topicId, partition, offset,
                          ret_.maxMsgLength(), pushReadOversize_);
    }
    // Largest PushReadResult chunk the return ring's max message can carry.
    [[nodiscard]] std::size_t pushReadChunkFit_() const {
        const std::size_t maxMsg = static_cast<std::size_t>(ret_.maxMsgLength());
        const std::size_t fixed  = static_cast<std::size_t>(sbe::MessageHeader::encodedLength()) +
                                   sbe::WorkerReturn::sbeBlockLength() +
                                   static_cast<std::size_t>(sbe::WorkerReturn::chunkHeaderLength());
        return maxMsg > fixed ? maxMsg - fixed : 0;
    }
    // Cut `chunk` to its longest prefix of whole record batches within `fit`
    // bytes (cold).  A first batch alone larger than `fit` is left untouched.
    [[gnu::cold]] static void trimToWholeBatches_(std::vector<uint8_t>& chunk, std::size_t fit) {
        std::size_t pos = 0;
        while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= chunk.size()) {
            rune::protocol::RecordBatchReader r(chunk.data() + pos, chunk.size() - pos);
            const int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > fit) break;
            pos += static_cast<std::size_t>(total);
        }
        if (pos != 0) chunk.resize(pos);
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
    // Task 10 round 4: executeOffsetsForLeaderEpochRespWire_ — the follower
    // applies a KIP-101 epoch-boundary answer (may truncate its log) on the
    // OWNING worker thread.  Scalars: leaderEpoch→oleCurrentEpoch,
    // responseEpoch→oleRequestedEpoch, endOffset→oleFollowerLeo,
    // errorCode→replErrorCode.  Fire-and-forget.
    // -------------------------------------------------------------------
    void executeOffsetsForLeaderEpochRespWire_(sbe::WorkerOp& op) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) return;
        ++followerSideOps_;
        rune::replication::OffsetsForLeaderEpochRespMsg resp;
        resp.leaderEpoch   = static_cast<rune::replication::Epoch>(op.oleCurrentEpoch());
        resp.partition     = static_cast<rune::replication::PartitionId>(op.partition());
        resp.responseEpoch = static_cast<rune::replication::Epoch>(op.oleRequestedEpoch());
        resp.endOffset     = static_cast<rune::replication::Offset>(op.oleFollowerLeo());
        resp.errorCode     = op.replErrorCode();
        resp.topicId       = op.topicId();
        try {
            replica->applyOffsetsForLeaderEpochResponse(resp);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: applyOffsetsForLeaderEpochResponse threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: applyOffsetsForLeaderEpochResponse threw unknown exception", workerId_);
        }
    }

    // -------------------------------------------------------------------
    // Task 10 round 4: executeReconcileFetchReqWire_ — a survivor serves a
    // deficient leader's reconcile pull on the OWNING worker thread (reads the
    // owned log).  Reuses the fr* scalars.  Fire-and-forget.
    // -------------------------------------------------------------------
    void executeReconcileFetchReqWire_(sbe::WorkerOp& op) {
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        if (!replica) return;
        ++followerSideOps_;
        rune::replication::FetchRequestMsg req;
        req.leaderEpoch = static_cast<rune::replication::Epoch>(op.frLeaderEpoch());
        req.partition   = static_cast<rune::replication::PartitionId>(op.partition());
        req.followerId  = static_cast<rune::replication::NodeId>(op.frFollowerId());
        req.fetchOffset = static_cast<rune::replication::Offset>(op.frFetchOffset());
        req.maxBytes    = op.frMaxBytes();
        req.topicId     = op.topicId();
        try {
            replica->onReconcileRequest(req);
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: onReconcileRequest threw: {}", workerId_, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: onReconcileRequest threw unknown exception", workerId_);
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
    // control marker to the worker-owned user partition log (the worker is the
    // sole appender: no race).
    //
    // Task 10 round 4 (P1): the marker's offset is AUTHORITATIVE from this
    // append — the runtime never pre-computes it (produces queued ahead of the
    // marker would land first and shift it).  The result is returned as
    // ControlRecordAppended{correlationId = the runtime's marker token,
    // baseOffset = where it landed}, or errorCode != 0 when the append failed;
    // the runtime then feeds the LSO / aborted-txn bookkeeping.  Never silent.
    // -------------------------------------------------------------------
    void executeAppendControlRecordWire_(sbe::WorkerOp& op) {
        namespace pc = rune::protocol::client;
        const int32_t topicId   = op.topicId();
        const int32_t partition = op.partition();
        WorkerReturn ret;
        ret.type            = WorkerReturn::Type::ControlRecordAppended;
        ret.correlationId   = op.correlationId();
        ret.topicId         = topicId;
        ret.partition       = partition;
        ret.producerId      = op.producerId();
        ret.targetReplicaId = op.targetReplicaId();
        ret.incarnation     = op.incarnation();
        ret.baseOffset      = -1;
        auto* replica = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        const uint32_t tailLen = op.tailLength();
        const uint8_t* tail    = reinterpret_cast<const uint8_t*>(op.tail());
        if (!replica || !tail || tailLen == 0) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ — {} for topicId={} partition={}",
                         workerId_, replica ? "empty marker tail" : "null replica", topicId, partition);
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
            enqueueReturn_(ret);
            return;
        }
        const std::span<const uint8_t> markerSpan(tail, static_cast<std::size_t>(tailLen));
        using RepT = rune::replication::PartitionReplica;
        try {
            // F15 N2: the marker lands AS a control batch (restamped in place),
            // with the replica's own role gate (NotLeader, retriable) and min-ISR gate.
            const rune::replication::Offset base = replica->appendControlBatch(markerSpan, /*acksAll=*/true);
            if (base >= 0) {
                ret.baseOffset = base;
                // F15 §2.1 step 2: the RC update in the same invocation as the
                // marker append (Ready: applied; Scanning: tail list).
                if (RcState* rc = rcOf_(replica)) {
                    rune::transaction::ControlMarkerPayload mp;
                    if (rune::transaction::ControlMarker::extract(tail, 0, mp, tailLen))
                        rc->onMarker(mp.producerId, base, mp.controlType == rune::transaction::ControlType::Commit);
                }
                // N2 (minor): keep append()'s flush request on the worker — a
                // Strict-tier marker joins this pass's coalesced durable flush
                // (submitCoalescedFlushes_), exactly like a Strict produce.
                if (replica->durabilityTier() == 2 && replica->isDiskBacked()) {
                    rune::storage::Log* log = replica->log();
                    bool found = false;
                    for (auto* dl : dirtyThisCycle_) {
                        if (dl == log) { found = true; break; }
                    }
                    if (!found) dirtyThisCycle_.push_back(log);
                }
            } else if (base == RepT::kAppendNotLeader) {
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            } else if (base == RepT::kAppendMinIsr) {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — min-ISR gate refused the marker "
                             "topicId={} partition={}", workerId_, topicId, partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MinIsrViolation);
            } else {
                spdlog::warn("Worker {}: executeAppendControlRecordWire_ — appendControlBatch returned {} "
                             "topicId={} partition={}", workerId_, base, topicId, partition);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
            }
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ threw: {}", workerId_, ex.what());
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
        } catch (...) {
            spdlog::warn("Worker {}: executeAppendControlRecordWire_ threw unknown", workerId_);
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
        }
        enqueueReturn_(ret);   // same FIFO as ProduceResults, never dropped
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
        /// F15 §3.2: the replica it parked on (null = replication-less) and its
        /// log generation as last validated — re-checked before it may fire OK.
        rune::replication::PartitionReplica* replica = nullptr;
        uint64_t                logGen     = 0;
        int32_t                 minIsr     = 1;       ///< DR-BUG-3 (thenAwaitHw only)
        /// TEMPORARY (F15 S2 fix round 2; deleted by S5): a gateway-routed
        /// Strict produce owes the gateway its facts-only return, emitted at
        /// fsync completion (fireDurable_) — counted in strictFactsOwed_.
        bool                    factsOwed  = false;
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
        /// F15 §3.2: the replica's log generation as last validated for this
        /// waiter (min-cut rule, waiterReplica_).
        uint64_t                             logGen     = 0;
        /// DR-BUG-3 (moved from the gateway's drain with I-A): fire OK only while
        /// |ISR| >= this admission minIsr.
        int32_t                              minIsr     = 1;
        WorkerReturn                         ret;               ///< ack payload when ready
    };
    std::vector<PendingAcksAll> pendingAcksAll_;

    // F15 S3 (§6): the ONE local return FIFO in front of ret_, every mode.
    // Pre-allocated fixed-capacity ring of tagged entries (WorkerReturn |
    // TxnReturn), O(1) push / pop-front.  While it is non-empty: new ingress
    // polling is gated (G-A6) and the op drain pauses at kOverflowHighWater.
    // Worker-thread only.
    ReturnFifo<kRetFifoCapacity> retFifo_;
    // Passes whose op drain was paused at the high-water mark (DR matrix
    // watches it for ISR-lag interaction in Legacy mode).
    uint64_t retFifoHighWaterPauses_ = 0;
    // F15 S3 fix round 1: this worker's owed-return cap (setMaxOwedReturns)
    // and the count of intake throttled by it (cap, not ring, was the limit).
    std::size_t maxOwedReturns_   = kMaxOwedReturns;
    uint64_t    owedCapThrottles_ = 0;       // throttled intake events (refusals + skipped polls)
    uint64_t    owedCapEpisodes_  = 0;       // throttle episodes (edge-triggered log)
    bool        owedCapThrottling_ = false;
    uint64_t    owedCapThrottlesAtEntry_ = 0;
    int64_t     owedCapStartNs_ = 0;
    uint64_t    retFifoPauseEpisodes_ = 0;   // high-water pause episodes (edge-triggered log)
    uint64_t    retFifoPausePassesAtEntry_ = 0;
    int64_t     retFifoPauseStartNs_ = 0;
    bool        retFifoPaused_ = false;
    // Re-review 1 I-4: PushReads answered MessageTooLarge because a stored
    // batch is larger than this worker's return ring's max message.
    uint64_t    pushReadOversize_ = 0;
    uint64_t    pushReads_ = 0;            // F15 S4: PushRead ops executed (R-1 / R-3 tests)
    // Largest client batch the shard intake stores: min(the gateway's max
    // produce batch, one PushReadResult chunk of this worker's ret_).  Set in
    // the ctor (ret_ is fixed).
    std::size_t maxIntakeBatchBytes_ = 0;

    // E5.1.2d: acks=all commit-wait timeout (ns).
    int64_t ackTimeoutNs_ = kDefaultAckTimeoutNs;
    // F15 S2: this pass's clock (set at the top of doWork) — the deadline base
    // for acks=all parked from an op (the op path has no clock of its own).
    int64_t passNowNs_ = 0;
    // F15 S2 (R-4c): gateway-routed acks=all refused for lack of a reservation.
    uint64_t acksAllRefusedNoReservation_ = 0;
    // TEMPORARY (F15 S2 fix round 2; deleted by S5): durablePending_ entries
    // whose facts-only return is still owed (DurablePending::factsOwed).
    int64_t strictFactsOwed_ = 0;

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

    // E5.1.3: desired fragment poll limit for reservation-before-poll (see
    // ingressPollBudget_).  Source: NodeConfig::gatewayFragmentLimit
    // (RUNE_GATEWAY_FRAGMENT_LIMIT, default 256), clamped to kMaxFragmentLimit
    // (F15 §6 bound).  Polling runs only while the return FIFO is empty (G-A6).
    int32_t desiredFragmentLimit_ = 256;

    // Expected ProduceResult return size (bytes) for the return-ring reservation.
    // Configurable (RUNE_WORKER_ACK_SLOT_BYTES); see setAckSlotBytes.  256 comfortably
    // covers the ~160 B framed ProduceResult.
    int32_t ackSlotBytes_ = 256;
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

    enum class RouteKind : uint8_t { Durable, Background, Seal, Hint };
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
    std::vector<rune::replication::PartitionReplica*> ownedPtr_;   // ownedPtr_[id] valid iff ownedInc_[id] != 0
    uint64_t    opsRefusedNotOwned_ = 0;
    uint64_t    ownershipAcks_      = 0;
    uint64_t    followerSideOps_    = 0;   // OLE responses + reconcile requests executed here
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
        case WorkerOp::Type::OffsetsForLeaderEpochResp:
        case WorkerOp::Type::ReconcileFetchReq:
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
    // -----------------------------------------------------------------------
    // F15 S4 (design note §2.1-§2.4): worker-owned read_committed state.
    //
    // One RcState per owned USER partition (topicId >= 0; internal partitions
    // never carry transactional batches, §1), created COLD when ownership is
    // applied and pre-reserved there (addendum m5: no allocation on the
    // produce path).  A still-owned replica keeps its state across ownership
    // changes (same pointer + incarnation).  rcById_ gives O(1) lookup for
    // registered replicas (dense by registry id, like ownedPtr_); unregistered
    // ones (incarnation 0: unit rigs / passthrough) are found by a scan of the
    // small slot vector.  Worker thread only.
    // -----------------------------------------------------------------------
    // F15 S4 fix round 2 (RR1-4): the per-partition context of the default
    // read-ahead issuer (stable address: owned by the slot through a pointer).
    struct RcHintCtx {
        Worker*             w   = nullptr;
        rune::storage::Log* log = nullptr;
    };
    struct RcSlot {
        rune::replication::PartitionReplica* replica = nullptr;
        uint32_t                             inc     = 0;
        std::unique_ptr<RcState>             rc;
        std::unique_ptr<RcHintCtx>           hint;
    };
    // The default read-ahead issuer: an async IORING_OP_FADVISE(WILLNEED) on the
    // segment fd where the worker's io backend is io_uring AND the kernel
    // supports the opcode (probed at backend start, RR2-1) AND async hints
    // have not been latched off; token routed as RouteKind::Hint; <= 1 in
    // flight per partition (RcState) and <= kMaxRcHintsInFlight per worker
    // (m1: fsyncs keep their headroom in the shared io cap); SQ / cap full →
    // skipped this pass.  Otherwise a synchronous MADV_WILLNEED (counted).
    // m8: a hint does not pin its segment (fsyncs do): io-wq resolves the fd
    // when it runs, so a hint on a closed-and-reused fd advises an unrelated
    // file — benign for WILLNEED (a wasted read-ahead, never a correctness
    // effect).
    static bool rcIssueHint_(void* c, const RcReadAheadReq& req) {
        auto* h = static_cast<RcHintCtx*>(c);
        Worker& w = *h->w;
        if ((w.io_.asyncHints() || w.rcFakeAsyncHints_) && !w.rcAsyncHintsDisabled_ && req.fd >= 0 &&
            h->log != nullptr) [[likely]] {
            if (w.rcHintsInFlight_ >= kMaxRcHintsInFlight) { ++w.rcHintsSkipped_; return false; }   // m1
            const auto len = static_cast<uint32_t>(std::min<std::size_t>(req.len, UINT32_MAX));
            const CompletionToken t = w.rcFakeAsyncHints_ ? ++w.rcFakeHintSeq_
                                                          : w.io_.submitWillNeed(req.fd, req.fileOff, len);
            if (t == kNoToken) { ++w.rcHintsSkipped_; return false; }
            if (!w.routeInsert_(TokenRoute{t, h->log, nullptr, 0, RouteKind::Hint}))
                spdlog::critical("Worker {}: io token route table full (hint token={})", w.workerId_, t);
            ++w.rcHintsSubmitted_;
            ++w.rcHintsInFlight_;
            w.lastRcHintToken_ = t;
            return true;
        }
        ++w.rcHintsSync_;
        if (w.rcSyncHintFn_ != nullptr) w.rcSyncHintFn_(w.rcSyncHintCtx_, req);
        else                            RcState::syncWillNeed(req);
        return false;
    }
    void installReadAhead_(RcSlot& sl) {
        if (rcReadAheadOverride_ != nullptr) { sl.rc->setReadAhead(rcReadAheadOverride_, rcReadAheadOverrideCtx_); return; }
        if (!sl.hint) sl.hint = std::make_unique<RcHintCtx>();
        sl.hint->w   = this;
        sl.hint->log = sl.replica->isDiskBacked() ? sl.replica->log() : nullptr;
        sl.rc->setReadAhead(&Worker::rcIssueHint_, sl.hint.get());
    }
    // A hint's CQE: clear its partition's in-flight flag (by log identity; a
    // purged partition is ignored).  Errors are counted, never fatal.
    // m3: matching by Log* means a CQE from a previous incarnation's hint can
    // clear a new incarnation's flag (a transient second hint in flight) —
    // harmless and bounded by the per-worker cap.
    // RR2-1: an "unsupported" completion (EINVAL / EOPNOTSUPP), or
    // kMaxConsecutiveHintErrors errors in a row, latches this worker onto the
    // synchronous hint — a hint that fails forever must never stall a build.
    static constexpr uint32_t kMaxConsecutiveHintErrors = 4;
    void onRcHintCompleted_(const rune::storage::Log* log, int err) noexcept {
        if (rcHintsInFlight_ > 0) --rcHintsInFlight_;
        if (err != 0) {
            ++rcHintErrors_;
            ++rcConsecutiveHintErrors_;
            if (!rcAsyncHintsDisabled_ &&
                (err == EINVAL || err == EOPNOTSUPP || rcConsecutiveHintErrors_ >= kMaxConsecutiveHintErrors)) {
                rcAsyncHintsDisabled_ = true;
                spdlog::warn("Worker {}: async read-ahead hints failing (errno {}, {} in a row) — using "
                             "synchronous hints from now on", workerId_, err, rcConsecutiveHintErrors_);
            }
        } else {
            rcConsecutiveHintErrors_ = 0;
        }
        for (auto& sl : rcSlots_)
            if (sl.hint && sl.hint->log == log) { sl.rc->onHintCompleted(); return; }
    }
    std::vector<RcSlot>   rcSlots_;
    // Review I-2: read_committed PushReads held while their partition's RC
    // state builds — answered on Ready (or unanswered on ownership loss).
    // Each owes one return: counted in reservedAckSlots_(), capped, reserved.
    std::vector<PushReadArgs> heldPushReads_ = [] { std::vector<PushReadArgs> v; v.reserve(kMaxHeldPushReads); return v; }();
    // Per pass, after the RC driver: answer the held reads whose partition is
    // Ready; unanswered (re-issue) when this worker no longer owns it.
    int serveHeldPushReads_() {
        if (heldPushReads_.empty()) [[likely]] return 0;
        int n = 0;
        for (std::size_t i = 0; i < heldPushReads_.size(); ) {
            PushReadArgs& h = heldPushReads_[i];
            rune::replication::PartitionReplica* r = nullptr;
            if (h.incarnation != 0) {
                if (ownsIncarnation_(h.targetReplicaId, h.incarnation)) r = ownedPtr_[h.targetReplicaId];
            } else {
                for (const auto& sl : rcSlots_) if (sl.replica == h.replica) { r = sl.replica; break; }
            }
            RcState* rc = r != nullptr ? rcOf_(r) : nullptr;
            const bool owned = rc != nullptr;
            // Still building AND still Leader: keep holding.  Not Leader (RR1-2):
            // answered now (notReady) — never held for a follower period.
            if (owned && !rc->revalidate(rcFacts_(r)) && r->role() == rune::replication::Role::Leader) { ++i; continue; }
            PushReadArgs a = h;
            heldPushReads_[i] = heldPushReads_.back();
            heldPushReads_.pop_back();
            if (owned) {
                a.replica = r;
                if (r->isDiskBacked()) a.logPtr = r->log();
                servePushRead_(a, /*mayHold=*/false);
            } else {
                a.logPtr = nullptr;   // never read a log this worker no longer owns
                servePushRead_(a, false);   // → unanswered: the gateway re-issues to the new owner
            }
            ++n;
        }
        return n;
    }
    std::vector<RcState*> rcById_;
    int32_t     rcScanBytesPerPass_ = RcState::kDefaultScanBytesPerPass;
    std::size_t rcOpenTxnCapacity_  = rune::transaction::LsoTracker::kDefaultOpenTxnCapacity;
    uint64_t         rcHintsSubmitted_ = 0;   // async IORING_OP_FADVISE hints submitted
    uint64_t         rcHintsSync_      = 0;   // synchronous fallback hints (no io_uring)
    uint64_t         rcHintsSkipped_   = 0;   // SQ / in-flight cap full: skipped this pass
    uint64_t         rcHintErrors_     = 0;   // hint completions with an error (counted, never fatal)
    bool             rcFakeAsyncHints_ = false;
    CompletionToken  lastRcHintToken_  = kNoToken;
    CompletionToken  rcFakeHintSeq_    = CompletionToken{1} << 40;
    RcSyncHint       rcSyncHintFn_     = nullptr;
    void*            rcSyncHintCtx_    = nullptr;
    std::size_t      rcHintsInFlight_  = 0;   // async hints submitted, CQE not reaped yet
    bool             rcAsyncHintsDisabled_ = false;   // RR2-1 latch
    uint32_t         rcConsecutiveHintErrors_ = 0;
    RcReadAhead      rcReadAheadOverride_    = nullptr;
    void*            rcReadAheadOverrideCtx_ = nullptr;
    RcResidencyProbe rcProbeFn_  = nullptr;
    void*            rcProbeCtx_ = nullptr;
    static constexpr uint16_t kTxnAttrBit = static_cast<uint16_t>(rune::protocol::batch::kAttrTransactional);
    static constexpr uint8_t  kIsolationReadCommitted = 1;

    [[nodiscard]] RcState* rcOf_(const rune::replication::PartitionReplica* r) const noexcept {
        if (r == nullptr) return nullptr;
        if (r->incarnation() != 0) {
            const uint32_t id = r->registryId();
            return (id < rcById_.size() && ownsIncarnation_(id, r->incarnation())) ? rcById_[id] : nullptr;
        }
        for (const auto& sl : rcSlots_) if (sl.replica == r) return sl.rc.get();
        return nullptr;
    }
    [[nodiscard]] const RcState* rcFor_(int32_t topicId, int32_t partition) const noexcept {
        return rcOf_(findOwnedReplica_(topicId, partition));
    }
    // Cold (ownership apply): carry still-owned states, create + reserve new
    // ones, drop the rest; re-resolve every subscription's cached pointer.
    void rebuildRcSlots_() {
        std::vector<RcSlot> next;
        next.reserve(owned_.size());
        for (auto* r : owned_) {
            if (r == nullptr || r->topicId() < 0) continue;
            bool dup = false;
            for (const auto& n : next) dup = dup || n.replica == r;
            if (dup) continue;
            RcSlot slot;
            slot.replica = r;
            slot.inc     = r->incarnation();
            for (auto& old : rcSlots_) {
                if (old.replica == r && old.inc == slot.inc && old.rc) {
                    slot.rc = std::move(old.rc);
                    slot.hint = std::move(old.hint);
                    break;
                }
            }
            if (!slot.rc) {
                slot.rc = std::make_unique<RcState>();
                slot.rc->reserve(rcOpenTxnCapacity_);
                slot.rc->setResidencyProbe(rcProbeFn_, rcProbeCtx_);
            }
            installReadAhead_(slot);
            next.push_back(std::move(slot));
        }
        rcSlots_ = std::move(next);
        rcById_.assign(ownedInc_.size(), nullptr);
        for (auto& sl : rcSlots_)
            if (sl.inc != 0 && sl.replica->registryId() < rcById_.size()) rcById_[sl.replica->registryId()] = sl.rc.get();
        for (auto& sub : subs_) sub.rc = rcOf_(subReplica_(sub));
    }
    [[nodiscard]] static RcFacts rcFacts_(const rune::replication::PartitionReplica* r) noexcept {
        RcFacts f;
        f.leader               = r->role() == rune::replication::Role::Leader;
        f.leaderComplete       = r->isLeaderComplete();
        f.logGeneration        = r->logGeneration();
        f.leadershipGeneration = r->leadershipGeneration();
        f.epoch                = static_cast<int32_t>(r->epoch());
        return f;
    }
    // One §2.3 driver step for one partition.
    std::size_t rcAdvance_(rune::replication::PartitionReplica* r, RcState& rc) {
        return rc.advance(rcFacts_(r), r->isDiskBacked() ? r->log() : nullptr, rcScanBytesPerPass_);
    }
    // §2.4: the RC state to serve read_committed from — only if Ready AND still
    // valid for the replica's live role / generations (a demotion can land
    // between the per-pass driver and this read).  nullptr → serve nothing.
    [[nodiscard]] const RcState* rcServe_(const rune::replication::PartitionReplica* r, RcState* rc) noexcept {
        if (rc == nullptr || !rc->revalidate(rcFacts_(r))) return nullptr;
        return rc;
    }
    // Per pass, every owned user partition (state-driven: no role-op hooks).
    int rcAdvanceAll_() {
        int work = 0;
        for (auto& sl : rcSlots_) work += rcAdvance_(sl.replica, *sl.rc) > 0 ? 1 : 0;
        return work;
    }
    // §2.3 produce gate: the RC state for an idempotent / transactional batch,
    // after one inline build step if it is not Ready; nullptr → refuse NotLeader.
    [[nodiscard]] RcState* rcGate_(rune::replication::PartitionReplica* r) {
        RcState* rc = rcOf_(r);
        if (rc == nullptr) return nullptr;
        if (!rc->revalidate(rcFacts_(r))) (void)rcAdvance_(r, *rc);
        return rc->ready() ? rc : nullptr;
    }
    [[nodiscard]] static uint16_t txnAttrs_(const uint8_t* batch, uint32_t len) noexcept {
        if (batch == nullptr || len < static_cast<uint32_t>(rune::protocol::batch::kRecordBatchOverhead)) return 0;
        return static_cast<uint16_t>(rune::protocol::RecordBatchReader(batch, len).attributes());
    }

    // F15 §3.1 (I1): the OWNING worker is the role authority for a produce — one
    // load + compare on the hot replica line (BM_WorkerRoleGate).  A null replica
    // is a replication-less partition (passthrough): nothing to gate.
    [[nodiscard]] [[gnu::always_inline]] static bool
    produceRoleGatePasses_(const rune::replication::PartitionReplica* r) noexcept {
        return r == nullptr || r->role() == rune::replication::Role::Leader;
    }
    void rebuildOwnedIndex_() {
        uint32_t maxId = 0;
        for (const auto* r : owned_) if (r && r->registryId() > maxId) maxId = r->registryId();
        ownedInc_.assign(static_cast<std::size_t>(maxId) + 1, 0u);
        ownedPtr_.assign(static_cast<std::size_t>(maxId) + 1, nullptr);
        for (auto* r : owned_) {
            if (r && r->incarnation() != 0) {
                ownedInc_[r->registryId()] = r->incarnation();
                ownedPtr_[r->registryId()] = r;
            }
        }
        // Parked / active push subscriptions follow ownership by id + incarnation
        // ONLY (Task 10 round 4, Important 2): never by pointer — a new replica at
        // a reused address is a different incarnation and stays parked.
        // Unregistered subscriptions (incarnation 0: unit tests / passthrough)
        // are not ownership-gated, exactly like their ops.
        for (auto& sub : subs_) {
            if (sub.incarnation != 0) sub.active = ownsIncarnation_(sub.registryId, sub.incarnation);
        }
    }
    /// The replica a registered subscription reads: this worker's own pointer for
    /// the owned incarnation (never the pointer carried by the Subscribe op).
    [[nodiscard]] rune::replication::PartitionReplica* subReplica_(const WorkerSubscription& s) const noexcept {
        if (s.incarnation == 0) return s.replica;
        return ownsIncarnation_(s.registryId, s.incarnation) ? ownedPtr_[s.registryId] : nullptr;
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
            ret.deferAck      = false;   // an answer, never facts-only (F15 S2 fix round 2, B1)
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
            ++pushReads_;
            // Empty result: the gateway clears its in-flight marker and re-issues
            // the read to the (new) owner on its next push cycle.
            WorkerReturn ret;
            ret.type           = WorkerReturn::Type::PushReadResult;
            ret.sessionId      = op.sessionId();
            ret.subscriptionId = op.subscriptionId();
            ret.readTopicId    = op.topicId();
            ret.readPartition  = op.partition();
            ret.setPushEffectiveBound(-1);   // review I-1: UNANSWERED — never parks the sub
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
            ret.correlationId = op.correlationId();   // the runtime's marker token
            ret.producerId    = op.producerId();
            ret.topicId   = op.topicId();
            ret.partition = op.partition();
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            const uint32_t n = op.tailLength();
            const auto* t = reinterpret_cast<const uint8_t*>(op.tail());
            if (t && n) ret.chunk.assign(t, t + n);
            enqueueReturn_(ret);
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
        rebuildRcSlots_();
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
        auto stillOwned = [&](const PendingAcksAll& p) {
            return p.ret.incarnation != 0 ? ownsIncarnation_(p.ret.targetReplicaId, p.ret.incarnation)
                                          : isOwned(p.replica);
        };
        for (std::size_t i = 0; i < pendingAcksAll_.size(); ) {
            if (!stillOwned(pendingAcksAll_[i])) {
                pendingAcksAll_[i].ret.errorCode =
                    static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
                enqueueReturn_(pendingAcksAll_[i].ret);
                pendingAcksAll_[i] = std::move(pendingAcksAll_.back());
                pendingAcksAll_.pop_back();
            } else {
                ++i;
            }
        }
        // Push subscriptions: parked / re-activated by id + incarnation in
        // rebuildOwnedIndex_ above — never overwritten by a pointer compare here.
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
        // TEMPORARY (F15 S2 fix round 2, Strict ruling; deleted by S5): the
        // batch is now durable — hand the gateway mirrors its facts (before the
        // §3.2 validation: the batch was appended and is durable either way;
        // for Strict-then-HW, before the HW wait).
        if (entry.factsOwed) {
            entry.factsOwed = false;
            --strictFactsOwed_;
            emitFactsOnly_(entry.ret);
        }
        // F15 §3.2: a durable ack may fire OK only while this worker still owns
        // the replica, it is still Leader and the acked content is unchanged —
        // a Strict ack whose fsync completes after a demotion fails NotLeader.
        if (entry.replica || entry.ret.incarnation != 0) {
            if (waiterReplica_(entry.replica, entry.ret.targetReplicaId, entry.ret.incarnation,
                               entry.lastOffset, entry.logGen) == nullptr) [[unlikely]] {
                entry.ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
                enqueueReturn_(entry.ret);
                return;
            }
        }
        if (entry.thenAwaitHw) {
            // Shard Strict acks=all: durable locally — now the HW gate.  (M4: for
            // a single-ISR Strict log, HW / consumer visibility can precede the
            // fsync in shard mode — Kafka semantics; the PRODUCER ack never does.)
            entry.ret.errorCode = 0;
            PendingAcksAll pending;
            pending.replica    = entry.thenAwaitHw;
            pending.lastOffset = entry.lastOffset;
            pending.deadlineNs = entry.deadlineNs;
            pending.logGen     = entry.logGen;
            pending.minIsr     = entry.minIsr;
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
                eraseDurable_(i);
            } else {
                ++i;
            }
        }
    }

    // ---- fsyncgate fail-stop -------------------------------------------------
    // L2 bypass (StorageFailure / OwnershipAck): straight to ret_, else to
    // pendingSignals_ (retried at the end of the pass) — never dropped; a claim
    // or encode exception halts like every other return (F15 §6).
    void enqueueSignal_(const WorkerReturn& r) {
        if (!tryWriteReturn_(r)) pendingSignals_.push_back(r);   // retried next pass, never dropped
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
                durablePending_[i].ret.errorCode = code;   // an error carries no facts (deferAck false)
                enqueueReturn_(durablePending_[i].ret);
                eraseDurable_(i);
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
    // flushRetFifo_ — F15 §6: flush the return FIFO into ret_ front-to-back,
    // stopping at the first refusal (ring still full; retried next pass).  O(1)
    // per entry (fixed ring, pop-front by index).  A claim / encode exception
    // halts inside tryWrite*_ — never an erase-and-continue drop.
    //
    // Worker-thread only.  Returns the number of returns flushed.
    // -----------------------------------------------------------------------
    int flushRetFifo_() {
        int flushed = 0;
        while (!retFifo_.empty()) {
            ReturnFifoEntry& e = retFifo_.front();
            const bool ok = e.tag == ReturnFifoEntry::Tag::Txn ? tryWriteTxnReturn_(e.txn)
                                                               : tryWriteReturn_(e.worker);
            if (!ok) break;   // ring still full — stop; retry next cycle
            retFifo_.popFront();
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

            // F15 §3.2: re-validate first — ownership, role (BecomeFollower OR a
            // self-demotion in onFetchRequest, no hook needed), log generation.
            auto* rep = waiterReplica_(entry.replica, entry.ret.targetReplicaId, entry.ret.incarnation,
                                       entry.lastOffset, entry.logGen);
            int16_t code = 0;
            if (rep == nullptr) [[unlikely]] {
                code = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
            } else if (rep->highWatermark() > entry.lastOffset &&
                       static_cast<int>(rep->isr().size()) >= entry.minIsr) {
                // HW-passed AND the ISR still >= minIsr at FIRE time (DR-BUG-3:
                // closes the admission→fire shrink-to-minority window; a stranded
                // minority leader never acks OK).
                code = 0;
            } else if (nowNs >= entry.deadlineNs) {
                spdlog::warn("Worker {}: acks=all timeout for partition={} "
                             "lastOffset={}; acking Internal error",
                             workerId_, entry.ret.partition, entry.lastOffset);
                // E5.1.3 review fix #1: use Internal (6) for acks=all timeout.
                // The protocol has no RequestTimedOut code; ErrorCode 7 is
                // UnknownTopic — sending 7 would cause the client to misinterpret
                // a timeout as a missing topic, a client-facing correctness bug.
                // Internal (6) is the correct "unexpected server-side failure" code.
                code = static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
            } else {
                ++i;
                continue;
            }
            entry.ret.errorCode = code;
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
        }
        return fired;
    }

    // -----------------------------------------------------------------------
    // waiterReplica_ — F15 §3.2: may a parked waiter (acks=all on the HW, a
    // Strict ack on its fsync) still fire OK?  Evaluated on THIS worker every
    // pass, state-driven (no role-change hooks):
    //   * ownership: the exact incarnation it parked on is still owned here
    //     (registered replicas; unregistered ones are pointer-tracked by
    //     rebuildFlushSlots_);
    //   * role: still Leader (BecomeFollower op OR self-demotion in
    //     onFetchRequest) — an epoch bump while still Leader keeps waiting;
    //   * content: the log generation is unchanged since the waiter last looked,
    //     or changed by EXACTLY one cut strictly above its offset (then the
    //     content at/below the offset is untouched and the waiter re-bases).  Two
    //     or more changes between passes fail it: only the last cut is recorded,
    //     so the min-cut since park is unknown (addendum: never fire OK over data
    //     an earlier cut replaced).
    // Returns the replica to evaluate the fire condition on, or nullptr → fail
    // the waiter NotLeader (retriable).  Two loads + compares in steady state.
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::replication::PartitionReplica*
    waiterReplica_(rune::replication::PartitionReplica* r, uint32_t id, uint32_t inc,
                   int64_t offset, uint64_t& logGen) const noexcept {
        if (inc != 0) {
            if (!ownsIncarnation_(id, inc)) [[unlikely]] return nullptr;
            r = ownedPtr_[id];
        }
        if (r == nullptr || r->role() != rune::replication::Role::Leader) [[unlikely]] return nullptr;
        const uint64_t g = r->logGeneration();
        if (g != logGen) [[unlikely]] {
            if (g != logGen + 1 || offset >= r->lastTruncationOffset()) return nullptr;
            logGen = g;
        }
        return r;
    }

    // F15 S2 (R-4c): one more owed ack (fires into ret_ later) still fits the
    // G-A6 return-ring reservation — the same rule the shard-ingress poll uses.
    // TEMPORARY (F15 S2 fix rounds 1-2; deleted by S4/S5): emit the facts-only
    // copy of a successful append (deferAck == true, errorCode 0) and strip the
    // facts from `ret` (producerId -1, no attrs) so the final answer never
    // re-applies them.  The caller has reserved the slot.
    void emitFactsOnly_(WorkerReturn& ret) {
        WorkerReturn facts = ret;
        facts.deferAck  = true;
        facts.errorCode = 0;
        enqueueReturn_(facts);
        ret.producerId = -1;
        ret.batchAttrs = 0;
    }
    // O(1) swap-with-back erase of durablePending_[i], keeping strictFactsOwed_
    // exact (an entry dropped with facts still owed releases its reservation).
    void eraseDurable_(std::size_t i) noexcept {
        if (durablePending_[i].factsOwed) --strictFactsOwed_;
        durablePending_[i] = std::move(durablePending_.back());
        durablePending_.pop_back();
    }
    // `returnsNow` = extra returns the caller owes on top of the answer (the
    // facts-only return: now for acks=all, at fsync completion for Strict).
    [[nodiscard]] bool ackReservationAvailable_(int64_t returnsNow = 0) const noexcept {
        return ret_.freeSlotsForMsgSize(ackSlotBytes_) >
               reservedAckSlots_() + static_cast<int64_t>(retFifo_.size()) + returnsNow &&
               owedCapAllows_(returnsNow);
    }
    // F15 S3 (addendum S3): the STATIC owed-return cap — the answer parked now
    // (+1) plus any further owed return (returnsNow: Strict owes its facts at
    // fsync) must stay within maxOwedReturns_, so pendingAcksAll_ /
    // durablePending_ never outgrow their reservation and the §6 FIFO bound
    // holds.  (For non-Strict acks=all the facts are emitted now, not owed, so
    // this is one conservative.)
    [[nodiscard]] bool owedCapAllows_(int64_t returnsNow) const noexcept {
        return reservedAckSlots_() + 1 + returnsNow <= static_cast<int64_t>(maxOwedReturns_);
    }
    // Cold: count + rate-limited log of an owed-cap throttle (the bench / DR
    // matrix watches it: the cap, not the ring, was the limiter).
    // Re-review 1 m-b: logged once on ENTERING and once on LEAVING an episode
    // (never per pass / per event), with the episode count and the events in it.
    [[gnu::cold]] void noteOwedCapThrottle_(const char* where) noexcept {
        ++owedCapThrottles_;
        if (owedCapThrottling_) return;
        owedCapThrottling_ = true;
        ++owedCapEpisodes_;
        owedCapThrottlesAtEntry_ = owedCapThrottles_ - 1;
        owedCapStartNs_ = passNowNs_;
        spdlog::warn("Worker {}: owed-return cap {} reached ({}) — intake throttled, episode {} "
                     "(RUNE_WORKER_MAX_OWED_RETURNS)", workerId_, maxOwedReturns_, where, owedCapEpisodes_);
    }
    // Re-review 2 B-2: hysteresis — the episode ends only once owed <= cap / 2
    // (checked each pass), so owed oscillating at the cap is one episode.
    void checkOwedCapEpisodeEnd_() noexcept {
        if (owedCapThrottling_ && reservedAckSlots_() <= static_cast<int64_t>(maxOwedReturns_ / 2)) [[unlikely]] {
            owedCapThrottling_ = false;
            spdlog::info("Worker {}: owed-return cap episode {} ended — {} throttled intake events over {} ns "
                         "(total {})", workerId_, owedCapEpisodes_, owedCapThrottles_ - owedCapThrottlesAtEntry_,
                         passNowNs_ - owedCapStartNs_, owedCapThrottles_);
        }
    }
    [[gnu::cold]] void enterRetFifoPause_(std::size_t backlog) noexcept {
        retFifoPaused_ = true;
        ++retFifoPauseEpisodes_;
        retFifoPausePassesAtEntry_ = retFifoHighWaterPauses_ - 1;
        retFifoPauseStartNs_ = passNowNs_;
        spdlog::warn("Worker {}: return FIFO at high water ({} >= {}) — op drain paused, episode {}",
                     workerId_, backlog, kOverflowHighWater, retFifoPauseEpisodes_);
    }
    [[gnu::cold]] void leaveRetFifoPause_() noexcept {
        retFifoPaused_ = false;
        spdlog::info("Worker {}: return FIFO pause episode {} ended — {} paused passes over {} ns "
                     "(total retFifoHighWaterPauses={})", workerId_, retFifoPauseEpisodes_,
                     retFifoHighWaterPauses_ - retFifoPausePassesAtEntry_, passNowNs_ - retFifoPauseStartNs_,
                     retFifoHighWaterPauses_);
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
    // K = ingressPollBudget_(): the ring reservation (free slots minus the FIFO
    // and every owed return) AND the static owed-return cap (F15 S3) — each
    // polled frame emits at most one return now or parks at most one owed one.
    //
    // This preserves G-A6 (never poll more frames than there are available
    // return slots) while allowing K > 1 per doWork cycle — removing the
    // fragmentLimit=1 throughput cap from E5.1.2d.  The caller polls only while
    // the return FIFO is empty (G-A6 gate in doWork).
    //
    // Never drops an ack: errors return error acks on ret_; acks=all go to
    // pendingAcksAll_ for deferred fire.
    // -----------------------------------------------------------------------
    // Acks already owed that will fire into ret_ later: acks=all parked on HW and
    // Strict acks parked on their fsync (G-A6 reservation).  F15 S2: every mode —
    // gateway-routed acks=all / Strict produces park on this worker too.
    [[nodiscard]] int64_t reservedAckSlots_() const noexcept {
        return static_cast<int64_t>(pendingAcksAll_.size()) +
               static_cast<int64_t>(durablePending_.size()) +
               static_cast<int64_t>(heldPushReads_.size()) +   // F15 S4 I-2: held PushReads owe a return
               strictFactsOwed_;   // TEMPORARY (S5): owed Strict facts-only returns
    }

    // Reservation-before-poll (E5.1.3 deliverable b), G-A6: never poll more
    // frames than can be answered without dropping.  Reserve return-ring slots
    // for BOTH the returns about to be polled AND every owed return (they fire
    // into ret_ later), and — F15 S3 — keep the owed set within the STATIC cap
    // kMaxOwedReturns (each polled frame parks at most one owed return), so
    // pendingAcksAll_ / durablePending_ never outgrow their reservation and the
    // §6 FIFO bound holds.
    [[nodiscard]] int64_t ingressPollBudget_() const noexcept {
        const int64_t owed      = reservedAckSlots_();
        const int64_t freeSlots = ret_.freeSlotsForMsgSize(ackSlotBytes_);
        const int64_t reserved  = static_cast<int64_t>(retFifo_.size()) + owed;
        const int64_t ringRoom  = (freeSlots > reserved) ? (freeSlots - reserved) : 0;
        const int64_t capRoom   = static_cast<int64_t>(maxOwedReturns_) > owed
                                      ? static_cast<int64_t>(maxOwedReturns_) - owed : 0;
        return std::min({static_cast<int64_t>(desiredFragmentLimit_), ringRoom, capRoom});
    }

    int pollWorkerIngress_(int64_t nowNs) {
        if (!workerIngress_ || !workerIngress_->resolved()) return 0;

        const int32_t K = static_cast<int32_t>(ingressPollBudget_());
        if (K <= 0) [[unlikely]] {   // no room — skip poll (G-A6 preserved)
            if (reservedAckSlots_() >= static_cast<int64_t>(maxOwedReturns_)) noteOwedCapThrottle_("shard poll");
            return 0;
        }

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
        if (replica) { ret.targetReplicaId = replica->registryId(); ret.incarnation = replica->incarnation(); }

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

        // F15 §3.1 (I1): the owning worker is the role authority — a follower
        // (BecomeFollower applied, or self-demoted) never appends a client batch.
        if (!produceRoleGatePasses_(replica)) [[unlikely]] {
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return 1;
        }
        // F15 §2.3 produce gate (see executeProduceWire_).
        RcState* rc = nullptr;
        if (msg.producerId >= 0 && replica != nullptr && msg.topicId >= 0) [[unlikely]] {
            rc = rcGate_(replica);
            if (rc == nullptr) {
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                enqueueReturn_(ret);
                return 1;
            }
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
        // F15 S3 (review I-1, re-review 1 I-3): MessageTooLarge (non-retriable,
        // never appended) above min(the gateway's max batch, what THIS
        // worker's return ring can carry in one PushReadResult) — so the Aeron
        // term size never raises the largest storable batch past the ring.
        if (rbSpan.size() > maxIntakeBatchBytes_) [[unlikely]] {
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

        // F15 §2.1 (C1): RC update in the same invocation as the append; a
        // duplicate retry was registered with its original.
        if (rc != nullptr && !outcome.isDuplicate && (outcome.batchAttrs & kTxnAttrBit) != 0) [[unlikely]]
            rc->onTxnBatch(msg.producerId, outcome.baseOffset);

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
            dp.replica     = replica;
            dp.logGen      = replica->logGeneration();
            dp.minIsr      = minIsr;
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
        pending.logGen     = replica->logGeneration();
        pending.minIsr     = minIsr;
        pending.ret        = std::move(ret);
        pendingAcksAll_.push_back(std::move(pending));
        return 1;
    }
};

} // namespace rune::node
