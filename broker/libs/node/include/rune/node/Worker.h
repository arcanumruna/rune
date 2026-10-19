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

#include <rune/storage/ZeroedBytes.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/replication/PartitionReplica.h>
#include <rune_fr/FlightRecorder.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/detail/RingDoorbell.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/node/OrderedWaitQueue.h>
#include <rune/node/RcState.h>
#include <rune/node/ReturnFifo.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/node/SessionAckSink.h>
#include <limits>
#include <rune/node/WorkerOp.h>
#include <rune/node/detail/NoTypeErasure.h>
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
#include <rune/node/RecordsPush.h>
#include <rune/node/SessionAnnounceKeyTail.h>
#include <bit>
#include <rune/node/SessionCryptoTable.h>
#include <rune/node/WorkerWriteGrants.h>
#include <rune/node/RecordsOpen.h>
#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/replication/ReplicationFrameLimits.h>   // F18 frame limits (maxAcceptableBatchBytes)
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/node/IoBackend.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/ProducePath.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/ControlMessages.h>
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
#include <unordered_set>
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

// (F3 round 6 item 5's {Ok, Full, TooLarge} outcome is F18's EnqueueResult in
//  WorkerOp.h — one enqueue outcome type; integration wave 2.)

class Worker final : public rune::consensus::Agent {
public:
    // Non-owning pointer set published as an immutable snapshot.
    using OwnedSet = std::vector<rune::replication::PartitionReplica*>;

    // Default ingress ring byte capacity (power-of-two payload bytes).
    // Used when the ctor is called without an explicit ingressRingBytes;
    // NodeConfig.workerIngressRingBytes (RUNE_WORKER_INGRESS_RING_BYTES) overrides it.
    // maxMsgLength() = capacity / 8 = 4 MiB — ample for any produce batch.
    static constexpr int kIngressByteCapacity = 32 * 1024 * 1024; // 32 MiB
    /// Task 13 fix round 1 (C1a): the session-lifecycle ring (announce ≤ ~0.6 KiB
    /// per op; a full ring defers the runtime's announce / close to its next
    /// cycle — never dropped).
    static constexpr int kSessionRingBytes = 64 * 1024;

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
    // acks=all + durablePending_ + held PushReads; S6 adds commit waiters /
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
    /// durablePending_ + held PushReads.  Configurable per worker
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
    // D8c fix round 1 (MAJOR-1): the session ack queue (SessionAckSink::
    // kAckCapacity) is split in two STATIC shares.
    //   kAckReserve       — what this worker's own intake may need beyond the
    //                       gateway-routed share: owed returns at their ceiling
    //                       + one full op drain + one full direct-ingress poll
    //                       (each op / frame adds at most one to A + O).
    //   kRoutedAckBudget  — the most GATEWAY-routed produces the runtime may
    //                       have outstanding to this worker (counted from
    //                       forward until the ack is CLAIMED: op ring + owed +
    //                       queued) — ClientGateway's per-worker cap.
    // So stalled gateway sessions, however many, can never take the reserve:
    // once the worker has applied every SessionClose the runtime sent (a close
    // frees the session's cap share at once, its queued acks drop at this
    // worker's next flush — re-review LOW-R1), A + O <= kRoutedAckBudget +
    // (owed <= kMaxOwedReturnsCeiling) + held direct acks, i.e. ackRoom_() >=
    // one drain + one poll while no DIRECT client holds acks.  In the window
    // before that, ackRoom_ (the one physical throttle) still bounds every
    // drain — at worst one op-drain pass finds no room.
    // D8d: DIRECT (worker-ingress) produces get their own budget INSIDE the
    // reserve (kDirectAckBudget, below): counted from admission (owed or queued)
    // until the ack leaves the sink, so direct + held PushReads + one op drain
    // fit the reserve whatever direct clients do — see kDirectAckBudget.
    static constexpr std::size_t kAckReserve =
        kMaxOwedReturnsCeiling + static_cast<std::size_t>(kMaxIngressDrainLimit) +
        static_cast<std::size_t>(kMaxFragmentLimit);
    static constexpr std::size_t kRoutedAckBudget = SessionAckSink::kAckCapacity - kAckReserve;
    static_assert(SessionAckSink::kAckCapacity > kAckReserve && kRoutedAckBudget >= kAckReserve,
                  "D8c: the routed share and the worker's own reserve each get half of the ack queue (or more)");
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
    /// Fixed framing of a key-less client Produce frame: header + fixed body +
    /// key u16 prefix + batch u32 prefix (pc::Produce::encodedSize() − batch).
    static constexpr std::size_t kProduceFrameFixedBytes =
        static_cast<std::size_t>(rune::protocol::client::MessageHeader::ENCODED_SIZE) +
        rune::protocol::client::Produce::BODY_FIXED + 2u + 4u;

    /// THE client-visible batch limit (F15 S3 re-review 2 B-1 / m-g, unified
    /// with F18, F19 and F3 in integration wave 2 — the ONE formula; every other
    /// limit — NodeConfig::produceIntakeLimit(), the gateway's MessageTooLarge
    /// guard, every worker's ingress guard, the PlaneHello advertisement — is
    /// this value, optionally LOWERED by an explicit RUNE_MAX_PRODUCE_BATCH_BYTES):
    /// the largest record batch any intake path accepts, and therefore the
    /// largest batch the broker can ingest, store, replicate and push:
    ///   min(8 MiB ceiling,
    ///       worker op ring max message − op framing         (gateway → worker op),
    ///       worker return ring max message − return framing  (one PushReadResult),
    ///       replication outbound ring frame − envelope − FetchResponse framing,
    ///       replication Aeron max message (min(16 MiB, term / 8)) − FetchResponse framing,
    ///       client egress Aeron max message − Records framing             (F19),
    ///       client ingress Aeron max message − Produce framing            (F3)).
    /// The replication terms apply ALWAYS, single-node too: the limit must not
    /// change when a node later joins a cluster.  The replication terms are
    /// F18's ReplicationFrameLimits.h (one source of truth with the transport's
    /// own fetch ceilings); the egress term is F19's Records frame (a batch at
    /// the limit must be deliverable to a push consumer, never error-parked);
    /// the ingress term is F3's Produce frame on the broker's term (on UDP the
    /// CLIENT's term governs the image — the explicit intake guards bound it).
    [[nodiscard]] static std::size_t maxAcceptableBatchBytes(std::size_t ingressRingBytes,
                                                             std::size_t returnRingBytes,
                                                             std::size_t replOutboundRingBytes,
                                                             std::size_t replTermBufferBytes,
                                                             std::size_t egressTermBufferBytes,
                                                             std::size_t clientIngressTermBufferBytes) {
        namespace rr = rune::replication;
        auto fit  = [](std::size_t maxMsg, std::size_t fixed) { return maxMsg > fixed ? maxMsg - fixed : 0; };
        auto nn   = [](int64_t v) { return v > 0 ? static_cast<std::size_t>(v) : std::size_t{0}; };
        auto aeronMax = [&](std::size_t term) {
            return nn(rr::aeronMaxMessageLength(static_cast<int64_t>(term)));
        };
        return std::min({kMaxProduceBatchBytes,
                         fit(ingressRingBytes / 8, opFixedBytes()),
                         fit(returnRingBytes / 8, returnFixedBytes()),
                         nn(rr::maxFetchRecordsViaOutboundRing(static_cast<int64_t>(replOutboundRingBytes))),
                         nn(rr::maxFetchRecordsViaPublication(static_cast<int64_t>(replTermBufferBytes))),
                         fit(aeronMax(egressTermBufferBytes), kRecordsFixedBytes),
                         fit(aeronMax(clientIngressTermBufferBytes), kProduceFrameFixedBytes)});
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
    /// D8d (D8c review LOW-R2): the most DIRECT (worker-ingress) produces this
    /// worker keeps outstanding — admitted (SessionAckSink::admitDirect, in the
    /// controlled poll) until the ack LEAVES the sink (claimed / dropped): owed
    /// (parked acks=all / Strict) and queued alike.  The own-intake reserve minus
    ///   * kMaxHeldPushReads — held read_committed PushReads each owe a return
    ///     (counted in reservedAckSlots_, so they shrink ackRoom_); bounded by
    ///     that static cap (beyond it: notReady at once) and the owed cap,
    ///     whatever any client does — a fixed term, not a client-driven one;
    ///   * one full op drain (kMaxIngressDrainLimit) — the room role / ownership
    ///     / replication / announce ops need to keep draining (gateway-routed
    ///     produces are inside kRoutedAckBudget).
    /// So A + O <= kRoutedAckBudget + kDirectAckBudget + kMaxHeldPushReads, i.e.
    /// ackRoom_() >= one full op drain, however many direct sessions stall
    /// (after every SessionClose the runtime sent is applied — LOW-R1 window).
    static constexpr std::size_t kDirectAckBudget =
        kAckReserve - kMaxHeldPushReads - static_cast<std::size_t>(kMaxIngressDrainLimit);
    static_assert(kRoutedAckBudget + kDirectAckBudget + kMaxHeldPushReads +
                      static_cast<std::size_t>(kMaxIngressDrainLimit) <= SessionAckSink::kAckCapacity,
                  "D8d: routed + direct shares + held PushReads must leave one full op drain of ack room");
    static_assert(kDirectAckBudget >= 4 * SessionAckSink::kDirectLightInFlight &&
                      kDirectAckBudget >= SessionAckSink::kDefaultDirectSessionBudget,
                  "D8d: the direct share holds a full session budget and a congested band of light sessions");
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
    /// Test seam (D3b r2): the read_committed SERVE check alone (rcServe_), as a
    /// push/fetch read runs it — true when the RC state is served.
    bool rcServeForTest(int32_t topicId, int32_t partition) {
        auto* r = findOwnedReplica_(topicId, partition);
        return r != nullptr && rcServe_(r, rcOf_(r)) != nullptr;
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
    /// E2 residual N5: passes whose rcAdvanceAll_ walked the slots for ring
    /// (memory-backed) rebuild work — stays 0 on a worker with no memory-backed partition.
    [[nodiscard]] uint64_t ringWalksForTest() const noexcept { return ringWalks_; }
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
    /// Fix round 4 (RR3-1): set the RR2-1 latch (synchronous hints from now on),
    /// as a failing async hint would — the post-latch path on any backend.
    void setRcAsyncHintsDisabledForTest(bool on) noexcept { rcAsyncHintsDisabled_ = on; }
    /// Async hints submitted for (topicId, partition) by the default issuer.
    [[nodiscard]] uint64_t rcHintsSubmittedForTest(int32_t topicId, int32_t partition) const {
        const auto* r = findOwnedReplica_(topicId, partition);
        for (const auto& sl : rcSlots_) if (sl.replica == r && sl.hint) return sl.hint->submitted;
        return 0;
    }
    /// RcState::readAheadRequests() of (topicId, partition): hints actually issued.
    [[nodiscard]] uint64_t rcReadAheadRequestsForTest(int32_t topicId, int32_t partition) const {
        const RcState* rc = rcFor_(topicId, partition);
        return rc ? rc->readAheadRequests() : 0;
    }
    /// Test seam: an async read-ahead hint of (topicId, partition) completed.
    void completeRcHintForTest(int32_t topicId, int32_t partition) {
        if (RcState* rc = const_cast<RcState*>(rcFor_(topicId, partition))) rc->onHintCompleted();
    }
    [[nodiscard]] uint64_t rcHintsSubmittedForTest() const noexcept { return rcHintsSubmitted_; }
    /// PushRead ops executed by this worker (both answers and refusals).
    [[nodiscard]] uint64_t pushReadsForTest() const noexcept { return pushReads_; }
    [[nodiscard]] uint64_t getViewRowsForTest() const noexcept { return getViewRows_; }

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
        , doorbell_()
        , ingress_(static_cast<int>(ingressRingBytes))
        , sessionRing_(kSessionRingBytes)
        , ret_(static_cast<int>(returnRingBytes))
        , io_(ioSubmitDepth, ioComplDepth)
    {
        durablePending_.reserve(kMaxOwedReturns);   // F15 S3: capped → never grows
        initRoutes_(ioSubmitDepth > ioComplDepth ? ioSubmitDepth : ioComplDepth);
        dirtyThisCycle_.reserve(kDirtySetReserve);
        pendingAcksAll_.reserve(kMaxOwedReturns);   // F15 S3: capped → never grows
        subs_.reserve(kSubsReserve);
        validators_.reserve(kValidatorsReserve);
        validatedTopics_.reserve(kValidatorsReserve);
        egressScratch_.resize(kEgressScratchSize);
        maxIntakeBatchBytes_ = ringBatchFit_();   // BrokerNode narrows it: setMaxBatchBytes
        threadName_ = "rune-wrk-" + std::to_string(workerId_);
    }

    /// A9 (findings §4.15): wire this worker's doorbell — ONLY when its runner will park on it (review M-1:
    /// otherwise no ring pays the doorbell's fences and nothing is written to an fd nobody polls).  The
    /// cross-thread rings it consumes ring it while its runner parks (the op ring gated off while the worker
    /// itself holds it back); the consumer of ret_ rings it when it frees space this worker waits for
    /// (`retSpaceWait` false when ret_'s consumer is this same thread: fused worker 0, review L-1).
    /// Startup only: before the runner and every producer thread start, and before the first io submission.
    void enableDoorbell(bool retSpaceWait) noexcept {
        if (doorbellEnabled_ || !doorbell_.valid()) return;
        (void)ingress_.attachConsumerDoorbell(doorbell_, &ingressWatchable_);
        (void)sessionRing_.attachConsumerDoorbell(doorbell_);
        if (retSpaceWait) ret_.attachProducerDoorbell(doorbell_);
        io_.attachCompletionDoorbell(doorbell_);   // portable backend: the flusher's completion ring
        doorbellEnabled_ = true;
    }
    [[nodiscard]] bool doorbellEnabled() const noexcept { return doorbellEnabled_; }

    // Thread name for `ps -M`/`top -H`/profilers: "rune-wrk-<id>" (see
    // AgentRunner::setCurrentThreadName). Cached so name() can hand back a
    // stable c_str().
    const char* name() const noexcept override { return threadName_.c_str(); }

    // D23: in-flight epoch-state fsync dups are closed after io_ is gone
    // (teardownSyncFds_), never leaked and never closed under the fsync.
    ~Worker() override {
        for (const TokenRoute& r : routes_)
            if (r.token != kNoToken && r.syncFd >= 0) teardownSyncFds_.fds.push_back(r.syncFd);
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
    void setOwnedDirect(OwnedSet set) {
        owned_ = std::move(set);
        rebuildFlushSlots_();
    }

    [[nodiscard]] int workerId() const noexcept { return workerId_; }

    // Task 15: attach THIS worker's view-fold set (its own per-worker
    // ViewSnapshotChannel is owned by the set).  Wired at STARTUP by
    // BrokerNode (Task 19), before the runner starts — single-threaded, like
    // setOwnedDirect().  nullptr (the default) => the view-fold drive in
    // doWork is skipped entirely; the duty cycle is bit-for-bit unchanged.
    void setViewSet(WorkerViewSet* vs) noexcept { viewSet_ = vs; }
    [[nodiscard]] WorkerViewSet* viewSet() const noexcept { return viewSet_; }

    // Task 16: this worker's view restart-readiness, for the GetViewRow serve
    // gate (Task 20) and the node election-eligibility gate (BrokerNode, Task
    // 19).  INERT (true) when no view set is attached — exactly like the T15
    // nullptr guard — so the no-views case never gates serving/elections closed.
    // True iff every (view, partition) this worker hosts-for-recovery is Ready;
    // reaches true purely from local checkpoint + log replay (no cluster input).
    [[nodiscard]] bool viewsReady() const noexcept {
        return viewSet_ == nullptr || viewSet_->allViewsReady();
    }

    // Self-contained worker shard (spec §3.1).  Set before the runner starts.
    void setShardMode(bool on) noexcept { shardMode_ = on; }
    [[nodiscard]] bool shardMode() const noexcept { return shardMode_; }

    // -----------------------------------------------------------------------
    // Task 13 (spec §3.4): worker-side produce acks.  In shard mode the worker
    // acks clients on its OWN exclusive publications (SessionAckSink), created
    // on its own invoker client from the runtime's SessionAnnounce ops; every
    // ProduceResult ANSWER goes there instead of ret_, and a post-ack facts-only
    // notification (PSM-mirror facts + session liveness) goes to ret_.
    // setShardClient: before the runner starts.  releaseShardClient: after the
    // runner joined and BEFORE the client is closed (the publications release
    // through the client's conductor).  The sink is heap-allocated once (cold):
    // ~0.9 MiB of fixed tables must not live on a thread stack.
    // -----------------------------------------------------------------------
    void setShardClient(aeron::Aeron* client, int32_t controlTermBytes = 0) {
        ackSink_ = client ? std::make_unique<SessionAckSink>(AeronSessionPubPolicy{client, controlTermBytes}) : nullptr;
        if (ackSink_) ackSink_->setDirectBudgets(directSessionAckBudget_, static_cast<uint32_t>(kDirectAckBudget));
    }
    /// D8d: per-session direct (worker-ingress) outstanding-ack budget (cold;
    /// clamped to [1, kDirectAckBudget]).  Default SessionAckSink::
    /// kDefaultDirectSessionBudget.
    void setDirectSessionAckBudget(uint32_t n) noexcept {
        directSessionAckBudget_ = n;
        if (ackSink_) ackSink_->setDirectBudgets(n, static_cast<uint32_t>(kDirectAckBudget));
    }
    /// D8d test seam: a smaller worker direct cap (after setShardClient).
    void setDirectAckBudgetsForTest(uint32_t perSession, uint32_t workerCap) noexcept {
        directSessionAckBudget_ = perSession;
        if (ackSink_) ackSink_->setDirectBudgets(perSession, workerCap);
    }
    [[nodiscard]] std::size_t directAcksInFlightForTest() const noexcept { return ackSink_ ? ackSink_->directInFlight() : 0; }
    [[nodiscard]] uint32_t directAcksInFlightOfForTest(int64_t sid) const noexcept {
        return ackSink_ ? ackSink_->directInFlightOf(sid) : 0;
    }
    [[nodiscard]] uint64_t directExpiriesForTest() const noexcept { return ackSink_ ? ackSink_->directExpiries() : 0; }
    [[nodiscard]] uint64_t directUnderflowsForTest() const noexcept { return ackSink_ ? ackSink_->directUnderflowsForTest() : 0; }
    [[nodiscard]] uint64_t directDroppedClosedForTest() const noexcept { return ackSink_ ? ackSink_->directDroppedClosed() : 0; }
    [[nodiscard]] uint64_t directExpiryCancelsForTest() const noexcept { return ackSink_ ? ackSink_->directExpiryCancelsForTest() : 0; }
    void releaseShardClient() noexcept { ackSink_.reset(); }
    [[nodiscard]] bool workerSideAcks() const noexcept { return ackSink_ != nullptr; }
    [[nodiscard]] uint64_t sessionAnnouncesSeenForTest() const noexcept { return sessionAnnouncesSeen_; }
    [[nodiscard]] int32_t  lastAnnouncedClientVersionForTest() const noexcept { return lastAnnouncedClientVersion_; }
    [[nodiscard]] uint64_t sessionClosesSeenForTest() const noexcept { return sessionClosesSeen_; }
    /// Task 15 test probe (read on THIS worker's thread, or after it joined): the link epoch this worker holds for
    /// `peer` (0 = none), and how many PeerLinkKeys ops it applied.
    [[nodiscard]] uint32_t peerLinkEpochForTest(int32_t peer) const noexcept {
        return workerReplTransport_ != nullptr ? workerReplTransport_->peerLinkEpoch(peer) : 0u;
    }
    [[nodiscard]] uint64_t peerFrameDropsForTest() const noexcept {
        return workerReplTransport_ != nullptr ? workerReplTransport_->peerFrameDrops() : 0u;
    }
    [[nodiscard]] uint64_t peerLinkKeysSeenForTest() const noexcept { return peerLinkKeysSeen_; }
    /// Acks dropped by the sink (closed / failed / stalled sessions).
    [[nodiscard]] uint64_t sessionAcksDroppedForTest() const noexcept { return ackSink_ ? ackSink_->droppedForTest() : 0; }
    [[nodiscard]] std::size_t sessionAcksPendingForTest() const noexcept { return ackSink_ ? ackSink_->pending() : 0; }
    /// Fix round 1: frames ABORTed because their session held acks (G-A6 per-
    /// client pause) / op drains skipped because the ack queue had no room.
    [[nodiscard]] uint64_t intakePausedFramesForTest() const noexcept { return intakePausedFrames_; }
    /// hotpath-A: frames held (ABORT) because their partition's roll was deferred.
    [[nodiscard]] uint64_t intakeRollHoldsForTest() const noexcept { return intakeRollHolds_; }
    /// Test-only: per-session crypto entries (encrypted sessions announced here).
    /// Test-only: probe-chain length of the last session-crypto lookup (the slot-indexed regression).
    [[nodiscard]] std::size_t sessionCryptoLastProbesForTest() const noexcept { return sessionCrypto_.lastProbesForTest(); }
    /// Test-only: look up one session's crypto entry exactly as the produce path does.
    [[nodiscard]] bool sessionCryptoLookupForTest(int64_t sid) noexcept { return sessionCrypto_.find(sid) != nullptr; }
    [[nodiscard]] std::size_t sessionCryptoCountForTest() const noexcept { return sessionCrypto_.size(); }
    [[nodiscard]] uint64_t workerNotAuthorizedForTest() const noexcept { return workerNotAuthorized_; }
    [[nodiscard]] uint64_t sessionPermissionChunksSeenForTest() const noexcept { return sessionPermissionChunksSeen_; }
    [[nodiscard]] uint64_t intakeGrantHoldsForTest() const noexcept { return intakeGrantHolds_; }
    [[nodiscard]] const SessionAckSink* ackSinkForTest() const noexcept { return ackSink_.get(); }
    [[nodiscard]] std::size_t sessionCryptoEntriesForTest() const noexcept { return sessionCrypto_.size(); }
    [[nodiscard]] uint64_t ackRoomOpPausesForTest() const noexcept { return ackRoomOpPauses_; }
    /// Fix round 2 (finding 9): session signals (Failed / Held / Released) the
    /// return ring could not take at once — deferred to a later pass, never lost.
    [[nodiscard]] uint64_t sessionSignalsDeferredForTest() const noexcept { return sessionSignalsDeferred_; }
    /// Fix round 3: ack-publication registrations still being released.
    [[nodiscard]] std::size_t sessionRegistrationsDrainingForTest() const noexcept {
        return ackSink_ ? ackSink_->drainingRegistrations() : 0;
    }
    /// Test seam: write one return straight to ret_ (false = ring full).
    bool injectReturnForTest(const WorkerReturn& r) { return tryWriteReturn_(r); }

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
    // Production path: takes WorkerReplicationTransport* (AeronPubRegistry
    // alias); doWork polls it by a DIRECT call (pollReplTransport_, Task 11 fix 3).
    // Test seam: setReplicationTransportForTest<T> accepts any Impl<T> (e.g. a
    // SharedFakePubRegistry) through a (ctx, fn*) trampoline — tests only.
    // -----------------------------------------------------------------------
    void setReplicationTransport(WorkerReplicationTransport* transport) noexcept {
        // Task 11 fix 3 (m-a): production is polled through the TYPED pointer — a
        // direct call every pass.  The (ctx, fn*) trampoline is the test seam only.
        replTransportCtx_    = nullptr;
        replTransportPollFn_ = nullptr;
        workerReplTransport_ = transport;
    }

    /// Test-only: true when the replication poll goes through the indirect
    /// (function-pointer) test seam instead of the direct production call.
    [[nodiscard]] bool replicationPollIsIndirectForTest() const noexcept { return replTransportPollFn_ != nullptr; }

    // Test-seam overload: accepts any WorkerReplicationTransportImpl<T> (e.g. a
    // fake pub registry).  Sets the indirect trampoline (TEST ONLY) and leaves the
    // typed workerReplTransport_ null so replicationTransport() stays production-only.
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
        int64_t subscribeCorrId      = 0;   ///< the client's Subscribe correlationId (stamped on Records)
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
        /// F19 push state.  Live: pushing.  ErrorOwed: the next batch's Records
        /// frame exceeds the egress maxMessageLength — the cursor is parked AT
        /// it and one ErrorResponse{MessageTooLarge, correlationId 0, message
        /// naming the subscription} is owed (retried each pass until sent).
        /// Failed: the error was sent; parked until Unsubscribe (no reads, no
        /// spin).  A same-id re-Subscribe keeps the state (one error per sub).
        enum class PushState : uint8_t { Live, ErrorOwed, Failed };
        PushState pushState = PushState::Live;
        /// ErrorOwed/Failed: the undeliverable Records frame size (named in the error).
        std::size_t failedFrameLen = 0;
        /// F15 S4: the partition's RC state (this worker's), re-resolved on every
        /// ownership apply and Subscribe — null when not owned / internal.
        RcState* rc = nullptr;
        /// E2 final fix wave (M2): the cursor is still the bind-time resolved
        /// start (no push pass has served it yet) — a transient ring start above
        /// it is not a reader that fell behind (see skipEvictedCursor_).
        bool cursorFresh = true;
    };

    // -----------------------------------------------------------------------
    // Task 19 (per-worker egress crypto addendum, 2026-10-08): this worker's
    // OWN Records-egress crypto state for one encrypted session (Rule 3 — a
    // dedicated state holding exactly what sealing ITS OWN Records pushes for
    // that session needs). Built ONCE, off the hot path, when a SessionAnnounce
    // for the session arrives (executeSessionAnnounceWire_); reused by every
    // subscription of that session on this worker — `sendSeq` is a single
    // per-(worker,session) counter, NEVER per-subscription (two subscriptions
    // of the same session sealing under independently-incrementing sendSeqs
    // would reuse nonces under the same key — exactly the bug class this
    // feature exists to prevent).
    //
    // An entry exists here IFF the session is encrypted (sessionEncrypted was
    // true on its SessionAnnounce) — a plaintext session never gets one, so
    // pushOwnedSubscriptions_'s lookup is a no-op (empty vector) for an
    // all-plaintext deployment: zero impact on plaintext sessions.
    //
    // `send` may be nullopt even though an entry exists: this worker's
    // specific key failed to derive (SessionAnnouncer::announce's fail-closed
    // path) or hasn't arrived yet — sendRecordsFrame's RecordsSealCtx
    // contract (a present ctx with a null `send`) then fails closed
    // (sealRecordsFrame returns 0, nothing is ever transmitted), never a
    // plaintext fallback.
    // Task 22 (shard worker decrypts produce / per-worker ingress,
    // 2026-10-08): `recv`/`recvSeq`/`recvWindow` are this worker's OWN c2b
    // ingress open context + anti-replay window for the session — built
    // alongside `send` above (same struct, same install site:
    // executeSessionAnnounceWire_), but a SEPARATE cipher/counter/window:
    // ingress (client->worker) and egress (worker->client) are independent
    // AEAD directions under independent keys, and this worker's own c2b
    // window is independent of the gateway's (and every other worker's) —
    // see RecordsOpen.h's doc comment for why that is safe (the client's
    // single monotonic c2b sendSeq is never split between receivers; each
    // receiver just sees its own subset with forward gaps, never a nonce
    // hazard). `recv` may be nullopt even though an entry exists (no key
    // yet, or the gateway never shipped one) — RecordsOpenCtx's own
    // null-`recv` contract then fails closed (openProduceFrame returns 0,
    // the frame is dropped), never a plaintext fallback.
    struct WorkerSessionCrypto {
        int64_t                                  sessionId = 0;
        std::optional<rune::crypto::AeadCipher>  send;      ///< this worker's own per-sender b2c cipher (egress)
        uint64_t                                 sendSeq   = 0;   ///< this worker's OWN counter (independent of the gateway's / other workers')
        uint8_t                                  cipher    = 0;   ///< rune::crypto::Cipher value (shared by both directions)
        std::optional<rune::crypto::AeadCipher>  recv;      ///< this session's c2b ingress cipher (shared across every worker; one sender)
        uint64_t                                 recvSeq    = 0;  ///< this worker's OWN replay-window high-water seq (ingress)
        uint64_t                                 recvWindow = 0;  ///< this worker's OWN replay-window bitmap (ingress)
    };

    /// Test-only: the worker-local subscription table (worker-thread state).
    [[nodiscard]] const std::vector<WorkerSubscription>& subscriptionsForTest() const noexcept {
        return subs_;
    }
    /// Test-only: release ops (record-egress retire-on-ack) executed.
    [[nodiscard]] uint64_t egressReleasesForTest() const noexcept { return egressReleases_; }

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
    /// F18: can an op carrying a `tailLen`-byte variable tail (record batch /
    /// replicated records) be claimed in ONE ingress-ring frame?  Callers that
    /// cannot back off by retrying (the follower apply path) check this first.
    [[nodiscard]] bool opTailFits(std::size_t tailLen) const {
        return workerOpFrameBytes(tailLen) <= static_cast<std::size_t>(ingress_.maxMsgLength());
    }
    [[nodiscard]] uint64_t oversizedOpsRefusedForTest() const noexcept { return oversizedOpsRefused_; }
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
    // F3 round 5, item I1: the largest variable tail (record batch / replicated
    // records / ...) a WorkerOp can carry through this worker's ingress O2O ring
    // (agrona maxMsgLength = capacity/8, minus the op framing — S3's
    // opFixedBytes(), the same framing workerOpFrameBytes() and opTailFits()
    // use).  maxOpTailBytesFor() from the configured ring size, maxOpTailBytes()
    // from the live ring.
    // -----------------------------------------------------------------------
    [[nodiscard]] static constexpr std::size_t maxOpTailBytesFor(std::size_t ingressRingBytes) noexcept {
        const std::size_t maxMsg = ingressRingBytes / 8;   // O2ORingBuffer: capacity / 8
        return maxMsg > opFixedBytes() ? maxMsg - opFixedBytes() : 0;
    }
    [[nodiscard]] std::size_t maxOpTailBytes() const noexcept {
        const std::size_t maxMsg = static_cast<std::size_t>(ingress_.maxMsgLength());
        return maxMsg > opFixedBytes() ? maxMsg - opFixedBytes() : 0;
    }
    /// F3 round 6 item 2: oversize-produce rejects on the worker ingress path
    /// (rate-limited log: first, then every 1024th).
    [[nodiscard]] uint64_t oversizeProduceRejectsForTest() const noexcept { return oversizeProduceRejects_; }
    /// EOS-DUP defense: produces rejected because the frame's producer identity
    /// disagreed with the RecordBatch header.
    [[nodiscard]] uint64_t identityMismatchRejects() const noexcept { return identityMismatchRejects_; }
    [[nodiscard]] uint64_t oversizeProduceRejectLogsForTest() const noexcept { return oversizeProduceRejectLogs_; }
    /// No-hang wave: undecodable worker-ingress Produce frames answered InvalidRequest (worker thread).
    [[nodiscard]] uint64_t undecodableProduceAnswers() const noexcept { return undecodableProduceAnswers_; }
    [[nodiscard]] uint64_t undecodableProduceAnswersForTest() const noexcept { return undecodableProduceAnswers_; }

    // No-hang fix round 4 (item 6): this worker's no-hang counters, logged by
    // the worker ITSELF (its own thread — shared-nothing: no other thread ever
    // reads them while it runs) every kNoHangStatsLogNs when any changed.
    // The change detector compares the counters one by one (no signature).
    struct NoHangStats {
        uint64_t undecodableProduceAnswers   = 0;   // worker-ingress Produce answered InvalidRequest
        uint64_t acksAllRefusedNoReservation = 0;   // gateway-routed acks=all refused (no reservation)
        uint64_t sessionSignalsDeferred      = 0;   // session signals a full ring deferred
        bool operator==(const NoHangStats&) const = default;
    };
    [[nodiscard]] NoHangStats noHangStats() const noexcept {
        return NoHangStats{undecodableProduceAnswers_, acksAllRefusedNoReservation_, sessionSignalsDeferred_};
    }
    static constexpr int64_t kNoHangStatsLogNs = 10'000'000'000LL;   // 10 s
    /// Logs the line when a counter changed since the last one; returns whether it logged.
    bool logNoHangStatsIfChanged_(int64_t nowNs) {
        nextNoHangStatsLogNs_ = nowNs + kNoHangStatsLogNs;
        const NoHangStats nh = noHangStats();
        if (nh == lastNoHangStats_) return false;
        lastNoHangStats_ = nh;
        spdlog::info("Worker {}: no-hang stats — worker-ingress Produce answered InvalidRequest={} "
                     "acks=all refused (no reservation)={} session signals deferred={}", workerId_,
                     nh.undecodableProduceAnswers, nh.acksAllRefusedNoReservation, nh.sessionSignalsDeferred);
        return true;
    }
    bool logNoHangStatsForTest(int64_t nowNs) { return logNoHangStatsIfChanged_(nowNs); }
    [[nodiscard]] int64_t nextNoHangStatsLogNsForTest() const noexcept { return nextNoHangStatsLogNs_; }
    /// Test seam: one duty pass's periodic check exactly as doWork runs it.
    bool maybeLogNoHangStatsForTest(int64_t nowNs) {
        return nowNs >= nextNoHangStatsLogNs_ && logNoHangStatsIfChanged_(nowNs);
    }

    /// Task 13 fix round 1 (C1a): ops that ride the session-lifecycle ring.
    [[nodiscard]] static constexpr bool isSessionLifecycleOp(WorkerOp::Type t) noexcept {
        return t == WorkerOp::Type::SessionAnnounce || t == WorkerOp::Type::SessionClose ||
               t == WorkerOp::Type::SessionPermissions ||
               t == WorkerOp::Type::PeerLinkKeys;   // Task 15: cold control-plane, ordered with session lifecycle
    }

    [[nodiscard]] bool tryEnqueueOp(WorkerOp op) {
        return enqueueOp(std::move(op)) == EnqueueResult::Enqueued;
    }

    /// F18 (m-1): tryEnqueueOp with the failure kind exposed — Full (ring
    /// back-pressure: retry) vs TooLarge (can never fit: never retry).
    [[nodiscard]] EnqueueResult enqueueOp(WorkerOp op) {
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
        case WorkerOp::Type::SessionAnnounce:
            tailKind = sbe::TailKind::Value::EGRESS_CHANNEL;
            if (op.sessionEncrypted) [[unlikely]] {
                // Task 19: cold, once per (session, worker) — stamp the
                // [cipher|prefix|key] crypto header ahead of the channel
                // bytes into this worker's own reused scratch (zero heap
                // allocation). PLAINTEXT SessionAnnounce (the common case,
                // op.sessionEncrypted == false by default) takes the
                // untouched branch below — byte-identical to pre-Task-19.
                const std::size_t n = encodeSessionAnnounceCryptoTail(
                    sessionAnnounceTailScratch_.data(), sessionAnnounceTailScratch_.size(),
                    op.senderCipher, op.senderKeyPrefix, op.senderKey,
                    op.c2bKeyPrefix, op.c2bKey,
                    op.egressChannel.data(), op.egressChannel.size());
                if (n == 0) [[unlikely]] {
                    // Unreachable by construction (the egress channel is
                    // already bounded to SessionAckSink::kMaxChannelBytes at
                    // Connect time) — halt rather than enqueue a truncated
                    // crypto tail a worker could misparse.
                    haltInvariant_("SessionAnnounce crypto tail too large for scratch",
                                    "Task 19: egress channel exceeded kMaxChannelBytes", 0);
                }
                tailSrc = sessionAnnounceTailScratch_.data();
                tailLen = n;
            } else {
                // Task 13: the client session's egress channel (view, copied once).
                tailSrc = reinterpret_cast<const uint8_t*>(op.egressChannel.data());
                tailLen = op.egressChannel.size();
            }
            break;
        case WorkerOp::Type::SessionPermissions:
            // Authentication Task 13: the chunk's words, little-endian u64 (a view valid for this call).
            static_assert(std::endian::native == std::endian::little, "permission chunks are shipped as raw LE words");
            tailKind = sbe::TailKind::Value::PERMISSION_BITS;
            tailSrc  = reinterpret_cast<const uint8_t*>(op.permWords);
            tailLen  = op.permWordCount * sizeof(uint64_t);
            break;
        case WorkerOp::Type::PeerLinkKeys:
            tailKind = sbe::TailKind::Value::PEER_KEYS;     // Task 15 (D33): [sendMaster][recvMaster]
            tailSrc  = op.peerKeys.data();
            tailLen  = op.peerKeys.size();
            break;
        default:
            break;  // NONE tail
        }

        // Task 13 fix round 1 (C1a): session lifecycle ops ride their own ring.
        OneToOneByteRing& ring = isSessionLifecycleOp(op.type) ? sessionRing_ : ingress_;

        // Total frame = SBE MessageHeader (8) + block + varData.  computeLength()
        // covers block+varData only, so add the header explicitly.
        const std::size_t n = workerOpFrameBytes(tailLen);
        // F18: an op larger than one ring frame would make tryClaim THROW (agrona
        // checkMsgLength) and terminate the control thread.  Unreachable when
        // intake is bounded (produceIntakeLimit) and the follower apply path
        // pre-checks opTailFits(); refuse loudly instead of crashing.
        // (F3 round 5 item I1 — the same refusal — is folded in: one check,
        //  rate-limited log: first, then every 1024th.)
        if (n > static_cast<std::size_t>(ring.maxMsgLength())) [[unlikely]] {
            if (oversizedOpsRefused_++ % 1024 == 0)
                spdlog::error("Worker {}: op type={} tail {} B exceeds the ingress ring frame "
                              "({} B); refused (TooLarge, not retriable) (refused={})", workerId_,
                              static_cast<int>(op.type), tailLen, ring.maxMsgLength(),
                              oversizedOpsRefused_);
            return EnqueueResult::TooLarge;
        }
        const aeron::util::index_t idx =
            ring.tryClaim(sbe::WorkerOp::sbeTemplateId(), n);
        if (idx < 0) return EnqueueResult::Full;  // ring full — back-pressure

        // Task 19: SessionAnnounce reuses the `isolation` wire byte for
        // sessionEncrypted and the `durabilityTier` wire byte for
        // senderKeyValid (see WorkerOp::sessionEncrypted's doc comment) —
        // every OTHER op type keeps its own field's value unchanged. A
        // plaintext SessionAnnounce (op.sessionEncrypted/senderKeyValid both
        // default false) writes 0/0 here, exactly what op.isolation/
        // op.durabilityTier already default to — byte-identical to every
        // pre-Task-19 SessionAnnounce.
        //
        // Task 22: SessionAnnounce additionally reuses the `isGroup` wire
        // byte for c2bKeyValid (isGroup is otherwise Subscribe-only — unused
        // for SessionAnnounce, same "reuse what's already on the wire"
        // discipline). Every OTHER op type keeps op.isGroup unchanged.
        const std::uint8_t wireIsolation = (op.type == WorkerOp::Type::SessionAnnounce)
            ? (op.sessionEncrypted ? 1u : 0u) : op.isolation;
        const std::uint8_t wireDurabilityTier = (op.type == WorkerOp::Type::SessionAnnounce)
            ? (op.senderKeyValid ? 1u : 0u) : op.durabilityTier;
        const bool wireIsGroup = (op.type == WorkerOp::Type::SessionAnnounce)
            ? op.c2bKeyValid : op.isGroup;

        sbe::WorkerOp enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(ring.base() + idx), 0, n);
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
           .deferAck(static_cast<std::uint8_t>((op.deferAck ? kOpFlagDeferAck : 0u) |
                                               (op.crcValidated ? kOpFlagCrcValidated : 0u) |
                                               (op.freshCursor ? kOpFlagFreshCursor : 0u) |
                                               (op.type == WorkerOp::Type::SessionAnnounce && op.grantAll
                                                    ? kOpFlagGrantAll : 0u)))
           .durabilityTier(wireDurabilityTier)
           .enqueuedAtNs(op.enqueuedAtNs)
           .subscriptionId(op.subscriptionId)
           .nextOffset(op.nextOffset)
           .hwBound(op.hwBound)
           .isolation(wireIsolation)
           .isGroup(wireIsGroup ? 1 : 0)
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
           .frLastFetchedEpoch(op.fetchRequestPayload.lastFetchedEpoch)
           .frIncarnation(op.fetchRequestPayload.incarnation)
           .rolePartitionEpoch(op.rolePartitionEpoch)
           .oleCurrentEpoch(static_cast<std::int64_t>(op.offsEpochReqPayload.currentEpoch))
           .oleFollowerId(static_cast<std::int32_t>(op.offsEpochReqPayload.followerId))
           .oleRequestedEpoch(static_cast<std::int64_t>(op.offsEpochReqPayload.requestedEpoch))
           .oleFollowerLeo(static_cast<std::int64_t>(op.offsEpochReqPayload.followerLeo))
           .egressPub(reinterpret_cast<std::uint64_t>(op.egressPub))
           .egressStreamId(op.egressStreamId)
           // Task 10 round 4: identity comes from the op (stamped from a registry
           // lookup at routing time) — never read through op.replica here.
           .targetReplicaId(op.targetReplicaId)
           .incarnation(op.incarnation)
           .minIsr(op.minIsr)
           .clientVersion(op.clientVersion)   // D6: SessionAnnounce; Task 12: SchemaUpdate's version
           // F27 R1: a FetchRequest op carries the follower's connection
           // generation in ownershipGen (SetOwnership is the field's only other user).
           .ownershipGen(op.type == WorkerOp::Type::FetchRequest
                             ? static_cast<std::uint64_t>(op.fetchRequestPayload.connGen)
                             : op.ownershipGen)
           .tailKind(tailKind);
        enc.putTail(reinterpret_cast<const char*>(tailSrc),
                    static_cast<std::uint32_t>(tailLen));
        ring.commit(idx);
        return EnqueueResult::Enqueued;
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
        return drainReturnImpl_<false>(std::forward<Fn>(fn), std::forward<TxnFn>(txnFn));
    }
    /// F5: the zero-copy drain — a PushReadResult's chunk is handed to `fn` as
    /// WorkerReturn::chunkView (a view of the ring slot, valid ONLY inside the
    /// callback; `chunk` stays empty).  For consumers that finish with the
    /// bytes before returning (the gateway encodes Records from them) — no
    /// per-return heap allocation, no copy.
    template <typename Fn>
    std::size_t drainReturnView(Fn&& fn) {
        return drainReturnImpl_<true>(std::forward<Fn>(fn), [](const TxnReturn&) {
            throw std::logic_error("Worker::drainReturn: a TxnReturn reached a consumer without a TxnReturn handler");
        });
    }

private:
    template <bool kView, typename Fn, typename TxnFn>
    std::size_t drainReturnImpl_(Fn&& fn, TxnFn&& txnFn) {
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

                // Tail: the chunk.  Copy mode: copied from the ring view (1 copy)
                // so fn can keep it after the callback returns.  View mode (F5):
                // handed over as a view of the slot (valid inside fn only).
                const std::uint32_t chunkLen = dec.chunkLength();
                if (chunkLen > 0) {
                    const uint8_t* c = reinterpret_cast<const uint8_t*>(dec.chunk());
                    if constexpr (kView) ret.chunkView = std::span<const uint8_t>(c, chunkLen);
                    else                 ret.chunk.assign(c, c + chunkLen);
                }

                fn(std::move(ret));
            },
            returnDrainLimit_);   // configurable (RUNE_WORKER_RETURN_DRAIN_LIMIT); was 16
        return static_cast<std::size_t>(drained < 0 ? 0 : drained);
    }

public:


    // -----------------------------------------------------------------------
    // E3 Task 5: test accessor — cumulative count of successful io_.submit calls
    // in submitCoalescedFlushes_().  Used by the GroupCommit test to assert
    // that 8 produces to one partition produce exactly 1 submit per cycle.
    // Worker-thread only; call only from single-threaded tests.
    // -----------------------------------------------------------------------
    /// A7 review M1: the io backend's completion fd (io_uring ring fd; -1 for portable) — the shard runner
    /// parks on it so an fsync completion ends the park.  Valid for the worker's lifetime.
    [[nodiscard]] int ioCompletionFd() const noexcept { return io_.completionFd(); }

    /// A9: the doorbell of the cross-thread rings this worker's thread consumes (its op + session rings, the
    /// portable backend's completions; in fused placement the runtime's rings attach to worker 0's too).
    /// Attach only at startup, before the producer threads run.  Valid for the worker's lifetime.
    [[nodiscard]] detail::RingDoorbell& doorbell() noexcept { return doorbell_; }
    /// A9: the worker's rings, for attaching the fused runtime's doorbell at startup (fused placement): ret_
    /// (runtime = consumer), the op + session rings (runtime = producer: space wait, review M-2).
    [[nodiscard]] OneToOneByteRing& returnRingForDoorbell() noexcept { return ret_; }
    [[nodiscard]] OneToOneByteRing& opRingForDoorbell() noexcept { return ingress_; }
    [[nodiscard]] OneToOneByteRing& sessionRingForDoorbell() noexcept { return sessionRing_; }
    /// A9 (tests): ret_ / op ring state.
    [[nodiscard]] bool retConsumerDoorbellForTest() const noexcept { return ret_.hasConsumerDoorbell(); }
    [[nodiscard]] bool opRingConsumerDoorbellForTest() const noexcept { return ingress_.hasConsumerDoorbell(); }
    [[nodiscard]] bool retSpaceWaitArmedForTest() const noexcept { return ret_.spaceWaitArmedForTest(); }
    [[nodiscard]] bool opRingProducerDoorbellForTest() const noexcept { return ingress_.hasProducerDoorbell(); }
    [[nodiscard]] bool sessionRingProducerDoorbellForTest() const noexcept { return sessionRing_.hasProducerDoorbell(); }
    [[nodiscard]] bool retProducerDoorbellForTest() const noexcept { return ret_.hasProducerDoorbell(); }
    /// A9 (tests): the op ring's doorbell watch is on (false while the worker holds its op ring back).
    [[nodiscard]] bool ingressWatchableForTest() const noexcept { return ingressWatchable_; }

    [[nodiscard]] uint64_t ioSubmitCountForTest() const noexcept {
        return io_submitCount_;
    }

    // Task 10 (spec §3.1 never-block / fail-stop) test hooks — owner thread or
    // single-threaded tests only.
    [[nodiscard]] std::size_t bgFlushInFlightForTest() const noexcept { return bgInFlight_; }
    [[nodiscard]] uint64_t    bgFlushSubmitsForTest() const noexcept { return bgFlushSubmits_; }
    /// The background token in flight on `log` (kNoToken if none).
    [[nodiscard]] CompletionToken lastBgTokenForTest(const rune::storage::Log* log) noexcept {
        LogFlushSlot* slot = slotOf_(const_cast<rune::storage::Log*>(log));
        return slot ? slot->bgToken : kNoToken;
    }
    [[nodiscard]] uint64_t    bgFlushErrorsForTest() const noexcept { return bgFlushErrors_; }
    [[nodiscard]] uint64_t    bgFlushDeferredForTest() const noexcept { return bgFlushDeferred_; }
    [[nodiscard]] uint64_t    sealSubmitsForTest() const noexcept { return sealSubmits_; }
    /// E1d: recovery-point checkpoint fdatasyncs (tmp file + directory) submitted to the io backend.
    [[nodiscard]] uint64_t    recoveryPointSubmitsForTest() const noexcept { return rpSubmits_; }
    [[nodiscard]] uint64_t    dirSyncSubmitsForTest() const noexcept { return dirSyncSubmits_; }
    [[nodiscard]] uint64_t    epochStateSubmitsForTest() const noexcept { return epochStateSubmits_; }
    /// The io token of the last leader-epoch-state write submitted (kNoToken = none).
    [[nodiscard]] CompletionToken lastEpochStateTokenForTest() const noexcept { return lastEpochStateToken_; }
    /// The fd the last leader-epoch-state fsync was submitted on.
    [[nodiscard]] int lastEpochStateSyncFdForTest() const noexcept { return lastEpochStateSyncFd_; }
    [[nodiscard]] uint64_t    epochStateNotRoutedForTest() const noexcept { return epochStateNotRouted_; }
    /// D23: epoch-state fsync dups currently open (dup'd, not yet closed).
    [[nodiscard]] int         epochStateSyncFdsOpenForTest() const noexcept { return epochStateSyncFdsOpen_; }
    [[nodiscard]] uint64_t    bgFlushSubmitsCountForTest() const noexcept { return bgFlushSubmits_; }
    /// Review Md3 seam: the next `n` epoch-state route inserts fail.
    void forceRouteFailForTest(int n) noexcept { forceRouteFailForTest_ = n; }
    /// The Strict-ack seal gate for `log` (true = an older seal is still in
    /// flight or pending: no Strict ack may fire).  Test-only.
    [[nodiscard]] bool sealsOutstandingForTest(rune::storage::Log* log) noexcept { return sealsOutstanding_(log); }
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
    /// A recovery-point checkpoint is queued (prepared at the next pass's background-disk step).
    [[nodiscard]] bool recoveryPointQueuedForTest() const noexcept { return !rpQueue_.empty(); }
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
    /// Boot (before the runners start, like setOwnedDirect): the topic ids the catalog dropped.
    void setDroppedTopicsDirect(const std::vector<int32_t>& ids) { for (const int32_t id : ids) markTopicDropped_(id); }
    [[nodiscard]] bool topicDroppedForTest(int32_t topicId) const noexcept { return topicDropped_(topicId); }
    /// Test seams (R-I1): the O(1) index vs the linear oracle.
    [[nodiscard]] rune::replication::PartitionReplica* findOwnedReplicaForTest(int32_t t, int32_t p) const noexcept {
        return findOwnedReplica_(t, p);
    }
    [[nodiscard]] rune::replication::PartitionReplica* findOwnedReplicaLinearForTest(int32_t t, int32_t p) const noexcept {
        return findOwnedReplicaLinearForTest_(t, p);
    }
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
    /// Startup-only: the auto-selected io_uring fell back to portable ("" = no).
    [[nodiscard]] const std::string& ioFallbackReason() const noexcept { return io_.fallbackReason(); }

    // Task 7 / Task 12: test accessor — true if a Validator is installed for
    // the given topic.  version < 0 (default) = "any version installed for
    // this topic" (the original Task 7 semantics); version >= 0 = exactly
    // that (topicId, version) pair.  Worker-thread only; single-threaded tests.
    [[nodiscard]] bool hasValidatorForTest(int32_t topicId, int32_t version = -1) const noexcept {
        if (version < 0) return validatedTopics_.count(topicId) > 0;
        return validators_.count(packValidatorKey_(topicId, static_cast<uint16_t>(version))) > 0;
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
        // E2 final fix wave (residual): ONE read_committed ring-rebuild budget
        // per pass, shared by every memory-backed partition rebuilding (the
        // produce gate's inline step and the per-pass driver alike).
        ringBudgetLeft_ = static_cast<std::size_t>(rcScanBytesPerPass_);

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
        // LAT-1 (f): io-wq is per TASK — bind THIS thread's io-wq (EBUSY = at the in-flight cap,
        // EAGAIN/EINTR = transient submit failure: retry next pass).
        if (ioWorkerCpusResult_ < 0 && !ioWorkerCpus_.empty()) [[unlikely]] {
            const int r = io_.setIoWorkerAffinity(ioWorkerCpus_);
            const bool retry = r == EBUSY || r == EAGAIN || r == EINTR;
            if (!retry) ioWorkerCpusResult_ = r;
            if (r != 0 && !retry && io_.kind() == IoBackendKind::Uring)
                spdlog::warn("Worker {}: IORING_REGISTER_IOWQ_AFF refused (errno={}) — io-wq threads keep the "
                             "worker's affinity", workerId_, ioWorkerCpusResult_);
        }

        passNowNs_ = nowNs;   // this pass's clock (acks=all deadlines on the op path)
        // No-hang fix round 4 (item 6): this worker's counters, logged from its
        // own thread on change — one compare per pass.
        if (nowNs >= nextNoHangStatsLogNs_) [[unlikely]] (void)logNoHangStatsIfChanged_(nowNs);
        // Task 13 fix round 1 (C1a): session lifecycle first, in full, every
        // pass and every mode — never behind data ops or any backlog.
        work += drainSessionOps_();
        work += beginPushPass_();   // F5: push budget + last pass's deferred reads first
        // Re-check I6: one INFO line the first pass a consumer is connected to THIS
        // worker's MDC egress (every mode); latched, so one branch per pass after it.
        if (workerEgress_ && !workerEgress_->connectedLogged()) [[unlikely]]
            workerEgress_->noteConnected(workerId_);
        if (shardMode_) return work + doWorkShard_(nowNs);

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
                const ReplReadScope rs(*replica, replScratch_);
                work += replica->doWork(nowNs);
            }
            work += rcAdvanceAll_();   // F15 §2.3: per-pass RC build driver
            if (viewSet_ != nullptr) [[unlikely]] {
                work += viewSet_->drainInstalls();   // Task 19: install runtime-created view plans (own thread)
                work += driveViews_(nowNs);          // Task 15: view folds, right after RC advance
            }
            if (!passthroughPsm_.empty()) [[unlikely]] work += advancePassthroughPsm_();
            work += serveHeldPushReads_();
        }
        // F30: never block a worker on disk in ANY mode (spec §3.1; replication
        // optimization #5 "fsync off the append thread") — the SAME background
        // disk-work step as the shard pass (truncation reconcile, roll seals,
        // follower flush requests → io backend).
        work += submitBackgroundDiskWork_();

        // Task 6: per-worker replication transport poll — inline dispatch.
        //
        // After driving owned replicas, drain inbound replication frames from the
        // per-worker Aeron subscription and apply each frame INLINE to the owning
        // replica on THIS worker thread.  Zero-copy: the frame pointer is valid
        // only inside the callback; the replica copies into its log before the
        // callback returns (same contract as executeApplyReplicatedWire_ ~1443-1471).
        //
        // Production: a DIRECT call on the typed transport (Task 11 fix 3, m-a).
        // Test seam only: the (ctx, fn*) trampoline for fake pub registries.
        // Neither wired → no poll (flag-off path unchanged).
        if (workerReplTransport_) {
            RUNE_FR_SPAN_RUNTIME(rune_fr::Site::WorkerReplPollDrive, 0);
            work += pollReplTransport_(*workerReplTransport_, nowNs);
        } else if (replTransportCtx_) [[unlikely]] {
            work += replTransportPollFn_(replTransportCtx_, kReplFetchLimit, this, nowNs);
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
                if (!cleanerSwapsDeferred_(replica)) work += replica->drainCleanerSwapsOnOwner();   // D3b
                work += replica->drainRetentionDeletesOnOwner();
                work += replica->log()->drainStagingRenamesDone();
                // fsyncgate: any inline sync failure on this log → fail-stop.
                checkStorageFailure_(replica->log());
            }
        }
        work += flushPendingSignals_();

        // E5.1.3 (d) R7 fix + F30: time-gated owned-log sync, on THIS worker's
        // io backend (never Log::sync on the worker, never from the gateway: the
        // gateway skips every worker-owned user log — R7).  Runs in EVERY
        // configuration while its interval is > 0 (F30 fix round 1, F4): it is what
        // re-requests a flush the io backend refused and flushes a log adopted by an
        // ownership move (fresh slot, lastSyncedLeo 0) — no per-pass retry table.
        // Interval 0 (the broker default, Kafka model): no periodic fsync at all, so a
        // deferred background flush is simply not retried — durability comes from
        // replication; Strict acks use their own coalesced fsync (not this path).
        // F15 S5 (R7, addendum): the gateway no longer learns worker appends (no
        // facts / mirror), so it can never mark a worker-owned log dirty — the
        // owning worker group-commits its own logs, here.
        // Internal partitions (topicId < 0) are not in owned_.
        work += syncOwnedLogsAsync_(nowNs);

        return work;
    }

private:
    // Shard pass order (spec §3.1 steps 3-6).  The ShardAgent already ran the
    // driver (1) and client conductor (2).  Never blocks; never waits on another
    // component (a full return ring is a non-blocking intake skip, spec §3.6).
    int doWorkShard_(int64_t nowNs) {
        int work = 0;
        // 2a. (doWork) session lifecycle ring drained in full.
        // 2b. runtime→worker ring: ownership / role changes must be applied
        //     before the intake they govern.
        work += static_cast<int>(drainIngress_());
        // 3. client ingress → append.  Never skipped for pending ops (fix round
        //    1, I1): a frame whose session announcement is not applied yet has
        //    its ack HELD by the sink until the lifecycle ring is consumed past
        //    the position sampled after the frame was seen (no gate, no time).
        //    G-A6: per-session pause inside the poll (controlled ABORT).
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
        if (workerReplTransport_) work += pollReplTransport_(*workerReplTransport_, nowNs);   // direct
        else if (replTransportCtx_) [[unlikely]]
            work += replTransportPollFn_(replTransportCtx_, kReplFetchLimit, this, nowNs);   // test seam
        for (auto* replica : owned_) {
            if (!replica) continue;
            const ReplReadScope rs(*replica, replScratch_);
            work += replica->doWork(nowNs);
        }
        work += rcAdvanceAll_();   // F15 §2.3: per-pass RC build driver
        if (viewSet_ != nullptr) [[unlikely]] {
            // Task 19: drain runtime-created/recovery view-plan installs off the
            // control→worker ring FIRST (same as the classic doWork_ pass) — a
            // shard worker that skipped this folded views it had never installed,
            // so every CreateView stayed UnknownView on the live path.
            work += viewSet_->drainInstalls();
            work += driveViews_(nowNs);          // Task 15: view folds, right after RC advance
        }
        if (!passthroughPsm_.empty()) [[unlikely]] work += advancePassthroughPsm_();
        work += serveHeldPushReads_();
        // 4b. background disk work → io backend, never an inline fsync
        //     (truncation reconcile, roll seals, follower flush requests).
        work += submitBackgroundDiskWork_();
        // 5. fire due produce acks.
        work += flushOwedAcksAll_(nowNs);
        checkOwedCapEpisodeEnd_();
        // 6. reap durability completions.
        work += reapDurableCompletions_();
        // Cold tail: consumer egress push, cleaner drains, time-gated log sync.
        work += pushOwnedSubscriptions_(nowNs);
        for (auto& slot : slots_) {
            if (!cleanerSwapsDeferred_(slot.replica)) work += slot.replica->drainCleanerSwapsOnOwner();   // D3b
            work += slot.replica->drainRetentionDeletesOnOwner();
            work += slot.log->drainStagingRenamesDone();
            checkStorageFailure_(slot.log);
        }
        work += syncOwnedLogsAsync_(nowNs);   // io backend, never Log::sync (spec §3.1)
        work += flushPendingSignals_();
        // Task 13 (spec §3.4): claim EVERY ack due this pass — appends, HW fires,
        // fsync completions, failures — on its session's own publication, last,
        // so the next driver duty cycle ships each session's acks together.
        // The lifecycle ring is sampled now, after every frame of this pass was
        // observed (consumer first: consumer <= producer) — I1 hold rule.
        if (ackSink_) {
            const int64_t consumed = sessionRing_.consumerPosition();
            // Task 23 (★★ shared sendSeq — CRITICAL nonce safety): the sink
            // owns no crypto table of its own (shared-nothing); this lookup
            // hands it THIS worker's per-session WorkerSessionCrypto entry —
            // the EXACT SAME entry (and therefore the exact same `sendSeq`
            // counter and `send` cipher) pushOwnedSubscriptions_ uses to seal
            // Records for this session. Acks and Records therefore draw from
            // ONE monotonic seq under ONE key — never a separate ack counter
            // (that would reuse nonces, catastrophic for AES-GCM). nullptr
            // (no entry: plaintext session) takes the exact pre-Task-23
            // in-place-encode claim, unchanged.
            work += ackSink_->flush(sessionRing_.producerPosition(), consumed,
                [this](int64_t sid) -> const RecordsSealCtx* {
                    WorkerSessionCrypto* cr = findSessionCrypto_(sid);
                    if (!cr) return nullptr;
                    ackSealCtx_.send     = cr->send.has_value() ? &*cr->send : nullptr;
                    ackSealCtx_.sendSeq  = &cr->sendSeq;
                    ackSealCtx_.cipher   = cr->cipher;
                    ackSealCtx_.senderId = static_cast<uint8_t>(workerId_ + 1);
                    return &ackSealCtx_;
                });
            work += reportSessionSignals_();
        }
        if (work == 0) work += prepareIdle_();   // A9: only a pass that may park pays for it
        return work;
    }

    // -----------------------------------------------------------------------
    // A9 (findings §4.15): before the runner may park — the op ring's doorbell watch follows the op-drain
    // gates, and a worker held back by ret_ (a queued return, or too few free slots to poll intake) arms the
    // ring's space wait so the runtime's next drain rings this worker's doorbell.  Dekker: re-check after
    // arming; space that appeared meanwhile = run again now (returns 1).  Worker thread only.
    // -----------------------------------------------------------------------
    int prepareIdle_() {
        if (!doorbellEnabled_) [[unlikely]] return 0;   // review M-1: no doorbell, no wait to arm
        ingressWatchable_ = retFifo_.size() < kOverflowHighWater && (!ackSink_ || ackRoom_() > 0);
        auto retBlocked = [this] {
            return !retFifo_.empty() || ret_.freeSlotsForMsgSize(ackSlotBytes_) <= reservedAckSlots_();
        };
        if (!retBlocked()) [[likely]] {
            if (spaceWaitArmed_) { ret_.disarmSpaceWait(); spaceWaitArmed_ = false; }
            return 0;
        }
        ret_.armSpaceWait();
        spaceWaitArmed_ = true;
        // The re-check: queued returns re-try their claims (a claim reads the runtime's head after the fence);
        // the intake gate re-reads the ring's fill.
        const int flushed = flushRetFifo_();
        if (flushed == 0 && retBlocked()) return 0;   // park: the runtime's next drain rings the doorbell
        ret_.disarmSpaceWait();
        spaceWaitArmed_ = false;
        return flushed > 0 ? flushed : 1;             // space appeared between the pass and the arm: run again
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
    // Bypass (L2): enqueueSignal_ for StorageFailure / OwnershipAck / EgressReleased only.
    // Worker-thread only (the producer side of ret_).
    // -----------------------------------------------------------------------
    void enqueueReturn_(const WorkerReturn& ret) {
        if (ackSink_ && ret.type == WorkerReturn::Type::ProduceResult && !ret.deferAck) {
            emitProduceAck_(ret);   // Task 13: worker-side ack + post-ack notification
            return;
        }
        enqueueToRuntime_(ret);
    }
    void enqueueReturn_(WorkerReturn&& ret) {
        if (ackSink_ && ret.type == WorkerReturn::Type::ProduceResult && !ret.deferAck) {
            emitProduceAck_(ret);
            return;
        }
        if (retFifo_.empty() && tryWriteReturn_(ret)) [[likely]] return;
        if (!retFifo_.push(std::move(ret))) [[unlikely]] haltRetFifoFull_();
    }
    void enqueueToRuntime_(const WorkerReturn& ret) {
        if (retFifo_.empty() && tryWriteReturn_(ret)) [[likely]] return;
        if (!retFifo_.push(ret)) [[unlikely]] haltRetFifoFull_();
    }

    // -----------------------------------------------------------------------
    // Task 13 (spec §3.4): every ProduceResult ANSWER, in shard mode with
    // worker-side acks (ackSink_ engaged), takes this path — whatever produced
    // it (append, HW fire, §3.2 fail, fsync completion / failure, ownership
    // purge, refusal).  1. the ack is queued on the session's own publication,
    // claimed in this pass's flush (coalesced per session, before the next
    // driver duty cycle);  2. with it (m1: queued at ack ENQUEUE, i.e. before
    // the ack is claimed — the runtime may see it first; it carries liveness
    // only, never an answer), a fire-and-forget notification goes to the
    // runtime on the one return FIFO (never dropped, never delaying an ack): a
    // ProduceResult with deferAck == true that carries the session's liveness
    // (shard-mode produces no longer refresh it on the runtime) and NO facts —
    // F15 S5: the worker psm_ is the sole producer-state authority, the
    // runtime holds no mirror to feed (producerId -1, no attrs, errorCode 0).
    // WorkerReturn PRODUCE_NOTIFY stays retired (F15 §7).  Reservation: the notification takes exactly the ret_ slot the
    // relayed answer took before (kMaxReturnsPerOp unchanged); the ack queue is
    // bounded by the same static bound (static_assert below) and gated by the
    // poll budget + op-drain high water.  A full queue is an invariant halt.
    // -----------------------------------------------------------------------
    void emitProduceAck_(const WorkerReturn& ret) {
        const bool ok = ret.errorCode == 0;
        AckEntry a{ret.sessionId, ret.correlationId, ok ? ret.baseOffset : 0, ret.partition, ret.errorCode,
                   static_cast<uint8_t>(ret.gatewayCredit ? 1 : 0), ret.directAck};
        // D6: an UnknownProducerId result carries the log start in baseOffset
        // (the carrier does not grow); the sink gates it per session version.
        if (ret.errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownProducerId))
            a.logStartOffset = ret.baseOffset;
        if (!ackSink_->enqueue(a)) [[unlikely]]
            haltInvariant_("session ack queue full (beyond its static bound)", "Task 13 G-A6 bound exceeded", 0);
        WorkerReturn n = ret;
        n.deferAck   = true;          // a liveness notification — never an answer
        n.errorCode  = 0;             // deferAck ⇒ errorCode 0 (the runtime's invariant)
        n.producerId = -1;            // F15 S5: no producer-state facts (no runtime mirror)
        n.batchAttrs = 0;
        enqueueToRuntime_(n);
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
            // M-6: one fsync in flight per log.  While one is, the new entries
            // stay un-stamped (kNoToken) and the log stays dirty: the next pass
            // after the in-flight fsync completes submits ONE fsync covering
            // all of them (state-driven retry, no timer).
            //
            // Strict latency bound (fix round 3, I-3): a deferred Strict waiter
            // marks the log strictWaiting, which blocks every NEW background
            // fsync on it (requestBgFlush_), so the next fsync on the log is
            // its durable one.  A Strict ack therefore waits for at most ONE
            // in-flight fsync (durable or background) plus its own — never a
            // chain of re-armed background flushes.
            if (slot && (slot->durableToken != kNoToken || slot->bgToken != kNoToken)) {
                ++durableSubmitsDeferred_;
                slot->strictWaiting = true;
                stillDirty_.push_back(log);
                continue;
            }
            CompletionToken t = submitActive_(log, RouteKind::Durable, slot ? slot->epoch : 0,
                                              /*pin=*/slot != nullptr);
            if (t == kNoToken) {
                // Submit ring full — back-pressure: ack all pending entries for
                // this log with Internal error immediately.  Never drop; never
                // silently hold forever.
                spdlog::warn("Worker {}: submitCoalescedFlushes_ ring full for log@{}; "
                             "back-pressure: acking pending entries Internal",
                             workerId_, static_cast<const void*>(log));
                if (slot) slot->strictWaiting = false;   // its waiters are answered below
                work += static_cast<int>(durablePending_.sweep([&](DurablePending& entry) {
                    if (entry.token != kNoToken || entry.logPtr != log) return false;
                    entry.ret.errorCode = static_cast<int16_t>(6); // Internal
                    enqueueReturn_(entry.ret);
                    return true;
                }));
            } else {
                // Successful submit: stamp the token onto every pending entry
                // for this log (all appends this cycle share one fsync token).
                durablePending_.forEach([&](DurablePending& entry) {
                    if (entry.token == kNoToken && entry.logPtr == log) entry.token = t;
                });
                if (slot) { slot->durableToken = t; slot->durableLeo = log->logEndOffset(); slot->strictWaiting = false; }
                ++io_submitCount_;
                ++work;
            }
        }
        // M-6: logs whose fsync is deferred behind an in-flight one stay dirty.
        dirtyThisCycle_.swap(stillDirty_);
        stillDirty_.clear();
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
        // M-6 / N-5 / N-7: the log's CURRENT slot whatever its epoch — non-null
        // means this worker owns the log (again), so touching it is safe.
        LogFlushSlot* gate = routed ? slotOf_(r.log) : nullptr;
        // M1: release the segment pin taken at submit (only while this worker
        // owns the log — a purged log may belong to another worker).  N-7: keyed
        // on the current slot, so a log re-owned while this fsync was in flight
        // still unpins (no leaked pin / never-closed retired fd).
        // E1d: a successful fdatasync makes its segment durable — up to the LEO
        // at submit for an active flush, the whole segment for a seal — and
        // advances the log's recovery point in order.  BEFORE the unpin (which
        // may close a retired segment).
        if (routed && r.seg && gate && c.errorCode == 0) {
            bool due = false;
            if (r.kind == RouteKind::Seal) due = r.log->noteFlushCompleted(r.seg, INT64_MAX);
            else if (r.kind == RouteKind::Durable || r.kind == RouteKind::Background)
                due = r.log->noteFlushCompleted(r.seg, r.aux);
            if (due) queueRecoveryCheckpoint_(*gate);   // F8: event-driven
        }
        if (routed && r.seg && gate) r.log->unpinSegmentFlush(r.seg);

        // M-6 / N-5: the one-in-flight gate belongs to the LOG — it is cleared on
        // the log's CURRENT slot whatever its epoch (a log purged and re-owned
        // while this fsync was in flight seeded its fresh slot from the route).
        if (routed && r.kind == RouteKind::Background && gate && gate->bgToken == c.token) {
            gate->bgToken = kNoToken;
            if (bgInFlight_) --bgInFlight_;
        }
        // LOW-B (fix round 3): through the log's CURRENT slot whatever its
        // epoch — a log dropped and re-owned on this worker seeded its fresh
        // slot's count from the routed seals (rebuildFlushSlots_), so the
        // Strict-ack seal gate stays closed until each of them completed.
        if (routed && r.kind == RouteKind::Seal && gate && gate->sealsInFlight > 0) --gate->sealsInFlight;
        // M-6: the log's one durable fsync is no longer in flight.  A success
        // syncs everything below its LEO (it also serves a background request
        // deferred behind it); data appended since is re-requested.
        if (routed && r.kind == RouteKind::Durable && gate && gate->durableToken == c.token) {
            gate->durableToken = kNoToken;
            if (c.errorCode == 0) {
                if (gate->durableLeo > gate->lastSyncedLeo) gate->lastSyncedLeo = gate->durableLeo;
                // I-3: re-request background only if no Strict waiter is queued
                // (requestBgFlush_ keeps bgAgain set while strictWaiting).
                if (gate->bgAgain) {
                    gate->bgAgain = false;
                    if (gate->log->logEndOffset() > gate->lastSyncedLeo) (void)requestBgFlush_(*gate);
                }
            }
        }

        // E1d MEDIUM-2: a batched directory fsync completed (only while owned;
        // unowned: the next owner re-issues it — resetDirectorySyncForNewOwner).
        if (routed && r.kind == RouteKind::DirSync) {
            if (gate && r.log->directorySyncCompleted(static_cast<uint64_t>(r.aux), c.errorCode))
                queueRecoveryCheckpoint_(*gate);
            if (c.errorCode == 0) return 1;
        }
        // E1d: a recovery-point checkpoint stage completed (only while owned —
        // a purged log is never touched; an error falls through to fail-stop).
        if (routed && r.kind == RouteKind::RecoveryPoint) {
            // Unowned (gate == null): never touch the Log; its next owner
            // replays the stage (F7: rebuildFlushSlots_ ->
            // resetRecoveryCheckpointForNewOwner).
            if (gate && r.log->recoveryCheckpointCompleted(static_cast<uint64_t>(r.aux), c.errorCode))
                queueRecoveryCheckpoint_(*gate);
            if (c.errorCode == 0) return 1;
        }
        // Hot-path B F7: a leader-epoch-state write completed (only while owned;
        // unowned: the next owner rewrites it — resetEpochStateForNewOwner).  An
        // error is the replica's to handle (a pending leadership is refused and
        // the log dir goes offline — the synchronous fence's semantics), never
        // the log's fail-stop.
        if (routed && r.kind == RouteKind::EpochState) {
            if (r.syncFd >= 0) { ::close(r.syncFd); --epochStateSyncFdsOpen_; }
            if (gate && sameReplica_(r, gate->replica))
                (void)gate->replica->epochStateWriteCompleted(static_cast<uint64_t>(r.aux), c.errorCode);
            return 1;
        }
        // D15 m5: a memory replica's — only while still owned here (else its
        // next owner rewrites it: rebuildFlushSlots_) and still the SAME
        // replica (an owned replica at a reused address is another one).
        if (routed && r.kind == RouteKind::MemEpochState) {
            if (r.syncFd >= 0) { ::close(r.syncFd); --epochStateSyncFdsOpen_; }
            if (std::find(owned_.begin(), owned_.end(), r.replica) != owned_.end() && sameReplica_(r, r.replica))
                (void)r.replica->epochStateWriteCompleted(static_cast<uint64_t>(r.aux), c.errorCode);
            return 1;
        }

        // C-1: the error FIRST — before any purged-slot early return.  An fsync
        // error is fail-stop whoever owns the log now (never retried).
        if (c.errorCode != 0) {
            if (routed && r.kind == RouteKind::Background) ++bgFlushErrors_;
            const rune::storage::Log* failed = routed ? r.log : nullptr;
            durablePending_.forEach([&](const DurablePending& e) {
                if (!failed && e.token == c.token) failed = e.logPtr;
            });
            if (failed) {
                if (LogFlushSlot* owner = slotOf_(failed)) failLog_(owner->log, c.errorCode);
                else failForeignLog_(failed, c.errorCode);
            }
            return 1;
        }

        if (routed && r.kind != RouteKind::Durable) {
            if (r.kind == RouteKind::Background) {
                // N-7: the current slot (a re-owned log re-serves its bgAgain).
                if (!gate) return 1;                           // purged (M2): success ignored safely
                if (gate == slot && gate->bgLeo > gate->lastSyncedLeo) gate->lastSyncedLeo = gate->bgLeo;
                if (gate->bgAgain) { gate->bgAgain = false; (void)requestBgFlush_(*gate); }
            } else if (!slot) {
                return 1;                                      // purged (M2): success ignored safely
            } else {
                fireSealGated_(*slot);
            }
            return 1;
        }

        // Durable token (or an unrouted one: synthetic / already-retired).
        // F15 S5 fix round 2 (M-6 supersedes the round-1 M-1 cover rule): one
        // durable fsync in flight per log, so a log's tokens complete in submit
        // order and its Strict answers leave in park order by construction;
        // the order-preserving sweep keeps them in that order.
        return static_cast<int>(durablePending_.sweep([&](DurablePending& entry) {
            if (entry.token == kNoToken || entry.token != c.token || entry.awaitSeal) return false;
            if (sealsOutstanding_(entry.logPtr)) {
                entry.awaitSeal = true;                         // durable in its active segment;
                return false;                                   // fires once the seals complete
            }
            fireDurable_(entry);
            return true;
        }));
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
    // -----------------------------------------------------------------------
    // Task 13 fix round 1 (C1a): drain the session-lifecycle ring IN FULL (two
    // reads: an Agrona read stops at the buffer's wrap point).  Unconditional —
    // lifecycle ops emit no returns, so no backlog may gate them.
    // -----------------------------------------------------------------------
    int drainSessionOps_() {
        auto exec = [this](int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                           aeron::util::index_t len) {
            executeFromWire_(reinterpret_cast<const uint8_t*>(buf.buffer()) + off, static_cast<std::size_t>(len));
        };
        int n = sessionRing_.drain(exec, std::numeric_limits<int>::max());
        if (n > 0) n += sessionRing_.drain(exec, std::numeric_limits<int>::max());
        return n;
    }

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // Task 13 session signals (WorkerReturn::SessionSignal), fix rounds 1-2:
    //   Failed   (C2) — the session's publication can never deliver: the
    //            runtime closes the session everywhere and tells the client;
    //   Held / Released (finding 1) — G-A6 on the gateway-routed path: the
    //            runtime pauses that session's gateway produce frames while any
    //            worker holds its acks (controlled ABORT on its image);
    //   Lost / Found / Progress (D8b round 2) — its ack publication reports
    //            NOT_CONNECTED / is connected again / a held session's acks are
    //            being claimed: the runtime's lease decides (reapExpired); the
    //            sink never drops acks for them.
    // Written straight to ret_ only when nothing is queued ahead (never through
    // the FIFO: its static bound is unchanged); otherwise the fact stays
    // recorded in the sink's per-session state and is retried next pass
    // (counted: sessionSignalsDeferred_).  Never dropped, never a timer.
    // -----------------------------------------------------------------------
    bool trySessionSignal_(int64_t sid, SessionSignalKind kind, int64_t value = 0) {
        if (retFifo_.empty()) {
            WorkerReturn r;
            r.type       = WorkerReturn::Type::SessionSignal;
            r.sessionId  = sid;
            r.partition  = workerId_;
            r.lastOffset = static_cast<int64_t>(kind);
            r.baseOffset = value;                 // D8c Credit: the count
            if (tryWriteReturn_(r)) return true;
        }
        ++sessionSignalsDeferred_;
        return false;
    }
    int reportSessionSignals_() {
        int n = ackSink_->reportFailures([this](int64_t sid) { return trySessionSignal_(sid, SessionSignalKind::Failed); });
        n += ackSink_->reportSessionEdges([this](int64_t sid, SessionSignalKind k) { return trySessionSignal_(sid, k); });
        // D8c: claimed gateway-routed acks return their produce credits.
        n += ackSink_->reportCredits([this](int64_t sid, uint32_t c) {
            return trySessionSignal_(sid, SessionSignalKind::Credit, static_cast<int64_t>(c));
        });
        // D8d: the direct share is full — the held session with the largest share expires.
        n += ackSink_->reportDirectExpiry([this](int64_t sid, uint32_t share) {
            if (!trySessionSignal_(sid, SessionSignalKind::Expire, static_cast<int64_t>(share))) return false;
            spdlog::warn("Worker {}: direct (worker-ingress) ack share full ({} / {}); expiring held session {} "
                         "(largest share, {} acks) — the client reconnects (expiries={})", workerId_,
                         ackSink_->directInFlight(), ackSink_->directCap(), sid, share, ackSink_->directExpiries());
            return true;
        });
        return n;
    }

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
        // Task 13 fix round 1 (C1b): acks held for slow sessions are NOT a
        // drain gate (they drain at the CLIENT's pace — gating ops on them let
        // one slow client stall role / ownership changes).  Ops answer into the
        // session ack queue too (one each, at most): throttle on its free ROOM
        // only — ops pause only while the queue is genuinely full.
        if (ackSink_) {
            const int64_t room = ackRoom_();
            if (room <= 0) [[unlikely]] { ++ackRoomOpPauses_; return 0; }
            if (room < limit) limit = static_cast<int>(room);
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
        // Task 20: point read of a materialized view row (view-table owner).
        case WorkerOp::Type::GetViewRow:
            executeGetViewRowWire_(op);
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
        // MEDIUM-2: the controller's AlterIsr answer (cold).
        case WorkerOp::Type::AlterIsrResponse:
            if (auto* rep = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
                rep != nullptr && op.replErrorCode() == rune::replication::kAlterIsrLocalLineageIdentity) {
                rep->rotateLineageIdentity(op.nextOffset());   // R2: node-local identity rotation
            } else if (auto* hold = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
                       hold != nullptr && op.replErrorCode() == rune::replication::kAlterIsrLocalHoldLeadership) {
                // ISR v3 Step 10 (R14): a held leader role (storage unconfirmed): node-local, never on the wire.
                hold->holdLeadershipForStorage(op.roleEpoch(), op.rolePartitionEpoch());
            } else if (rep != nullptr) {
                rune::replication::AlterIsrResponseMsg r{};
                r.topicId        = op.topicId();
                r.partition      = op.partition();
                // Round 6 (fix-on-sight): the controller answers the node that
                // asked — this one.  The op carries no leaderId; left at -1 the
                // replica's lineage / leaderless-report acks never matched
                // (reports re-sent forever, a rotation re-sent an unborn report).
                r.leaderId       = rep->selfId();
                r.leaderEpoch    = static_cast<rune::replication::Epoch>(op.roleEpoch());
                r.partitionEpoch = op.rolePartitionEpoch();
                r.errorCode      = op.replErrorCode();
                r.proposalId     = op.nextOffset();      // ISR protocol: correlation ids (R14)
                r.sendSeq        = op.roleLeaderId();
                rep->onAlterIsrResponse(r);
            }
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
        // Task 13 (spec §3.4): session lifecycle from the runtime (cold).
        case WorkerOp::Type::SessionAnnounce:
            executeSessionAnnounceWire_(op);
            break;
        case WorkerOp::Type::TopicDropped:
            markTopicDropped_(op.topicId());
            break;
        case WorkerOp::Type::PeerLinkKeys:
            executePeerLinkKeysWire_(op);
            break;
        case WorkerOp::Type::SessionClose:
            ++sessionClosesSeen_;
            if (ackSink_) ackSink_->close(op.sessionId());
            eraseSessionCrypto_(op.sessionId());   // Task 19: no-op if the session was never encrypted
            break;
        case WorkerOp::Type::SessionPermissions:
            executeSessionPermissionsWire_(op);
            break;
        }
    }

    // -------------------------------------------------------------------
    // Authentication Task 15 (D33): the keys of an established inter-broker link, from the control thread over the
    // lifecycle ring.  This worker's OWN store (shared-nothing); Task 16 hands them to its replication plane's
    // PeerFrameCrypto.  Cold: once per link establishment.
    // -------------------------------------------------------------------
    [[gnu::cold]] void executePeerLinkKeysWire_(sbe::WorkerOp& op) {
        if (op.tailLength() != WorkerOp::kPeerKeysBytes) [[unlikely]] {
            spdlog::error("Worker {}: PeerLinkKeys tail is {} B (expected {}) - refused", workerId_, op.tailLength(),
                          WorkerOp::kPeerKeysBytes);
            return;
        }
        rune::peerlink::LinkKeys k;
        k.epoch = static_cast<uint32_t>(op.partition());
        const int32_t peer = op.topicId();
        const char* tail = op.tail();   // SBE var data: one call (it advances the decode position)
        std::memcpy(k.sendMaster.data(), tail, 32);
        std::memcpy(k.recvMaster.data(), tail + 32, 32);
        ++peerLinkKeysSeen_;
        // This worker's own replication plane (a transport built PeerSealed); none (legacy gateway-routed
        // replication) or PeerPlain: nothing to install.
        if (workerReplTransport_ != nullptr && workerReplTransport_->peerSealed() &&
            !workerReplTransport_->installPeerLink(peer, k)) [[unlikely]]
            spdlog::error("Worker {}: the link keys of node {} (epoch {}) could not be installed - frames to/from it "
                          "are dropped", workerId_, peer, k.epoch);
    }

    // -------------------------------------------------------------------
    // Task 13: SessionAnnounce — remember the session's reply destination; the
    // sink adds this worker's own exclusive publication on its next flush.
    // Workers without worker-side acks (legacy, unit rigs) only count it.
    // -------------------------------------------------------------------
    // Authentication Task 13 (D30): one chunk of a session's WRITE bitmap.  Staged in this worker's one
    // PermissionStager and swapped into the session's slot on the last chunk (a partial table is never visible).
    [[gnu::cold]] void executeSessionPermissionsWire_(sbe::WorkerOp& op) {
        ++sessionPermissionChunksSeen_;
        if (!ackSink_) return;   // a worker without worker-side acks keeps no session table (and enforces nothing)
        const auto first = static_cast<uint64_t>(op.nextOffset());
        const auto total = static_cast<uint64_t>(op.hwBound());
        const std::uint32_t tailLen = op.tailLength();   // SBE var data: the length FIRST, then tail() advances past it
        const auto* tail = reinterpret_cast<const uint8_t*>(op.tail());
        const PermissionChunkResult r = permStager_.apply(
            *ackSink_, op.sessionId(), first, total, op.isolation(), static_cast<uint64_t>(op.correlationId()), tail, tailLen);
        if (r == PermissionChunkResult::Dropped) [[unlikely]]
            spdlog::debug("Worker {}: permission chunk for session {} dropped (closed session or out-of-order chunk)",
                          workerId_, op.sessionId());
    }

    [[gnu::cold]] void executeSessionAnnounceWire_(sbe::WorkerOp& op) {
        ++sessionAnnouncesSeen_;
        const int64_t sid       = op.sessionId();
        const int32_t stream    = op.egressStreamId();
        const int32_t version   = op.clientVersion();   // D6: gates UnknownProducerId acks
        lastAnnouncedClientVersion_ = version;
        const std::uint32_t len = op.tailLength();
        const char* p           = op.tail();

        // Task 19: see WorkerOp::sessionEncrypted's doc comment for the wire
        // reuse (isolation/durabilityTier bytes + the tail's crypto prefix).
        const bool sessionEncrypted = op.isolation() != 0;
        std::string_view channel(p, len);
        if (sessionEncrypted) [[unlikely]] {
            const bool senderKeyValid = op.durabilityTier() != 0;
            // Task 22: c2bKeyValid rides the `isGroup` wire byte for
            // SessionAnnounce (see enqueueOp's wireIsGroup comment).
            const bool c2bKeyValid    = op.isGroup() != 0;
            const auto decoded = rune::node::decodeSessionAnnounceCryptoTail(p, len);
            WorkerSessionCrypto* cr = sessionCrypto_.findOrInsert(sid);
            if (cr == nullptr) [[unlikely]]   // unreachable: the runtime caps sessions below the table size
                haltInvariant_("session crypto table full", "authentication Task 13: runtime session cap violated", 0);
            cr->sessionId  = sid;
            cr->sendSeq    = 0;    // fresh counter for THIS session on THIS worker (egress)
            cr->send.reset();      // fail closed by default: no cipher until proven valid below
            cr->recvSeq    = 0;    // fresh replay window for THIS session on THIS worker (ingress)
            cr->recvWindow = 0;
            cr->recv.reset();      // fail closed by default: no cipher until proven valid below
            if (!decoded) [[unlikely]] {
                // Fail closed: a truncated/garbage tail must never be treated
                // as "this session is fine, push plaintext" — the entry stays
                // present (sessionEncrypted known) with no cipher installed,
                // so the push path parks/drops (RecordsSealCtx's own
                // null-`send` contract) rather than leaking.
                //
                // Review fix (Minor, robustness): `channel` was initialized
                // above to the RAW tail (p, len) — which here still includes
                // the would-be 37-byte crypto header, since decoding it
                // failed. Reset it to empty rather than handing
                // ackSink_->announce() that garbage as if it were a channel
                // string. Unreachable by construction (the gateway never
                // sends a truncated tail), but a malformed tail must fail
                // clean, not mis-announce.
                channel = std::string_view{};
                spdlog::critical("Worker {}: session {} SessionAnnounce crypto tail too short "
                                  "({} B) — Records egress for this session will be parked "
                                  "(fail-closed)", workerId_, sid, len);
            } else {
                channel = decoded->channel;
                cr->cipher = decoded->cipher;
                if (senderKeyValid) {
                    // This worker's OWN per-sender key + its OWN sendSeq
                    // (independent of the gateway's and every other
                    // worker's — see RecordsSealCtx::senderId's doc comment).
                    rune::crypto::AeadKey key{};
                    std::memcpy(key.bytes, decoded->b2cKey, sizeof(key.bytes));
                    cr->send.emplace(static_cast<rune::crypto::Cipher>(decoded->cipher),
                                      key, decoded->b2cPrefix);
                }
                // else: the gateway's own deriveSenderKey() failed for this
                // worker (SessionAnnouncer's fail-closed path) — leave
                // cr->send empty; the push path parks, never plaintext.

                // Task 22: the session's c2b ingress key — the SAME bytes on
                // every worker (no per-worker derivation; see
                // SessionAnnounceKeyTail.h's doc comment). `c2bKeyValid`
                // mirrors `senderKeyValid`'s fail-closed contract.
                if (c2bKeyValid) {
                    rune::crypto::AeadKey ingressKey{};
                    std::memcpy(ingressKey.bytes, decoded->c2bKey, sizeof(ingressKey.bytes));
                    cr->recv.emplace(static_cast<rune::crypto::Cipher>(decoded->cipher),
                                      ingressKey, decoded->c2bPrefix);
                }
                // else: the first (parked) wave, or the gateway never shipped
                // a valid c2b key — leave cr->recv empty; the produce-ingress
                // decrypt path parks/drops, never plaintext.
            }
        } else {
            eraseSessionCrypto_(sid);   // re-announced as plaintext (should not happen, but never stale)
        }

        if (!ackSink_) return;
        // Fix round 1 (I2): unreachable by construction — the runtime counts
        // every session a worker table can hold (accepted + announced-pending +
        // closes not yet queued everywhere) against kMaxSessions − 1, and
        // refuses longer channels at Connect.  A refusal would leave an
        // accepted session that is never acked: halt, never a silent drop.
        if (!ackSink_->announce(sid, channel, stream, version, (op.deferAck() & kOpFlagGrantAll) != 0)) [[unlikely]] {
            spdlog::critical("Worker {}: session {} announcement refused (sessions {}, channel {} B, max {})",
                             workerId_, sid, ackSink_->sessionCount(), channel.size(), SessionAckSink::kMaxChannelBytes);
            haltInvariant_("session announcement refused", "Task 13 I2: runtime session cap violated", 0);
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
                s.subscribeCorrId      = op.correlationId();
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
                s.cursorFresh          = true;   // M2: a requested cursor again (rewind / re-seed)
                return;
            }
        }
        WorkerSubscription s;
        s.subscriptionId       = subId;
        s.sessionId            = op.sessionId();
        s.subscribeCorrId      = op.correlationId();
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
        if (op.egressPub() != 0) [[unlikely]] { executeEgressReleaseWire_(op); return; }
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
    // 2026-10-05 record-egress lifetime (retire-on-ack): an Unsubscribe op
    // carrying egressPub != 0 is the runtime's RELEASE of that record-egress
    // publication (correlationId = its retire token).  It rides the same FIFO
    // op ring as every Subscribe that handed the pointer, so it runs after
    // them: drop every subscription writing through it (the pointer is only
    // compared, never dereferenced), then acknowledge with SessionSignal
    // EgressReleased (never dropped: enqueueSignal_) — only then does the
    // runtime destroy the publication.  Cold (session-close rate).
    // -------------------------------------------------------------------
    [[gnu::cold]] [[gnu::noinline]] void executeEgressReleaseWire_(sbe::WorkerOp& op) {
        const auto pub = static_cast<std::uintptr_t>(op.egressPub());
        for (std::size_t i = 0; i < subs_.size(); ) {
            if (reinterpret_cast<std::uintptr_t>(subs_[i].egressPub) == pub) {
                subs_[i] = subs_.back();
                subs_.pop_back();
            } else {
                ++i;
            }
        }
        WorkerReturn ack;
        ack.type       = WorkerReturn::Type::SessionSignal;
        ack.sessionId  = op.sessionId();
        ack.partition  = workerId_;
        ack.lastOffset = static_cast<int64_t>(SessionSignalKind::EgressReleased);
        ack.baseOffset = op.correlationId();   // the retire token
        enqueueSignal_(ack);
        ++egressReleases_;
    }

    // -------------------------------------------------------------------
    // Task 7 / Task 12: executeSchemaUpdateWire_ — install a per-(topic,version)
    // Validator on this worker's own thread (shared-nothing).
    //
    // The descriptor blob arrives in the ring tail (TailKind = DESCRIPTOR_BLOB)
    // as the MATERIALIZED-as-of-version descriptor (the fan-out source sends
    // RegistryEntry.descriptor, already materializeAsOf(evolving, version) —
    // never the raw evolving blob).  The version itself rides the wire's
    // clientVersion field (reused by SchemaUpdate; see WorkerOp.h).  We
    // deserialize it here (off-hot-path, admin/control-plane) and store the
    // resulting Validator in validators_ keyed by (topicId, version).  The hot
    // path (schemaRejects_) does a single-branch lookup; if the topic has no
    // validated version installed at all it falls through with zero overhead.
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
        const int32_t  topicId  = op.topicId();
        const uint16_t version  = static_cast<uint16_t>(op.clientVersion());
        const uint32_t tailLen  = op.tailLength();
        const uint8_t* tail     = reinterpret_cast<const uint8_t*>(op.tail());

        if (!tail || tailLen == 0) {
            spdlog::warn("Worker {}: executeSchemaUpdateWire_ — empty blob for topicId={} "
                         "version={}; validator not installed", workerId_, topicId, version);
            return;
        }

        try {
            rune::codec::Descriptor desc =
                rune::codec::deserialize(std::span<const uint8_t>(tail, tailLen));
            validators_.insert_or_assign(packValidatorKey_(topicId, version),
                                         rune::codec::Validator(desc));
            validatedTopics_.insert(topicId);
            spdlog::debug("Worker {}: SchemaUpdate installed validator for topicId={} version={}",
                          workerId_, topicId, version);
        } catch (const std::exception& ex) {
            spdlog::error("Worker {}: executeSchemaUpdateWire_ — deserialize failed "
                          "for topicId={} version={}: {}", workerId_, topicId, version, ex.what());
        } catch (...) {
            spdlog::error("Worker {}: executeSchemaUpdateWire_ — unknown exception "
                          "for topicId={} version={}", workerId_, topicId, version);
        }
    }

    // -------------------------------------------------------------------
    // read-rework T3/T4 (I3): in-worker push loop.  For each active owned
    // subscription, read whole batches from the cursor (#65: a pread into the
    // worker's push scratch, readNoFault_ — never the segment read mmap),
    // encode each as a Records frame into egressScratch_, and push it directly on
    // THIS worker's dynamic-MDC egress publication (workerEgress_).  Advance the
    // cursor only on a successful send (back-pressure preserves at-least/exactly-
    // once, same invariant as the gateway push path).
    //
    // Shared-nothing: reads only worker-owned state (subs_, the owned Log/replica,
    // workerEgress_).  Zero heap alloc on the hot path (preallocated push scratch +
    // egressScratch_; one pread per subscription read).  No locks/atomics.
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
            int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos == aeron::ADMIN_ACTION) [[unlikely]]   // A9: term rotated — retry now (Aeron contract)
                pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(claim.offset(), buf, 0,
                                        static_cast<aeron::util::index_t>(len));
                claim.commit();
            }
            return pos;
        }
        const int64_t r = pub->offer(buf, 0, static_cast<aeron::util::index_t>(len));
        return r == aeron::ADMIN_ACTION ? pub->offer(buf, 0, static_cast<aeron::util::index_t>(len)) : r;
    }

    // F19: send the owed ErrorResponse{MessageTooLarge} for a subscription
    // parked at an undeliverable batch (cursor == that batch's offset).
    // ErrorOwed → Failed on success; stays ErrorOwed (retried next pass, same
    // as back-pressured Records) otherwise.  Returns 1 if sent.  Cold path.
    // correlationId 0 + a message naming subscription/topic/partition/offset
    // (encodePushTooLargeError — the F15 S3 m-f convention).
    //
    // Task 23 (mandatory egress audit — 3rd worker->client path found): this
    // frame goes straight to the client on `pub` (the same per-consumer /
    // shared-MDC egress Records uses), via sendEncodedToPub_ — NOT through
    // sendTo/sendRecordsFrame, so it was NEVER sealed even after Task 19
    // sealed Records. Mirrors the gateway's own analogous path exactly:
    // ClientGateway::trySendOwedPushError_ already seals (its sendTo() is
    // the single chokepoint every gateway->client send funnels through —
    // see sendTo's doc comment), but the worker has no such chokepoint, so
    // it must seal here explicitly. `seal` non-null ⇒ encrypted session:
    // seal BEFORE sending (same ordering as sendRecordsFrame/the ack path —
    // never claim/offer on an unsealed buffer), using the SAME per-session
    // cr->send/sendSeq/senderId `pushOwnedSubscriptions_` built for Records
    // (one shared monotonic seq across every worker->client frame type under
    // this session's one b2c key). Fail-closed: seal==0 (no key yet / AEAD
    // error) ⇒ return 0 — s.pushState stays ErrorOwed, retried next pass,
    // exactly like back-pressure; NEVER a plaintext send on an encrypted
    // session.
    int sendPushTooLarge_(WorkerSubscription& s, aeron::ExclusivePublication* pub,
                          const RecordsSealCtx* seal = nullptr) {
        static_assert(kPushTooLargeErrorMaxBytes <= kEgressScratchSize);
        const std::size_t n = encodePushTooLargeError(
            egressScratch_.data(), s.sessionId, s.subscriptionId, s.topicId, s.partition,
            s.nextOffset, s.failedFrameLen, pub->maxMessageLength());
        if (seal) [[unlikely]] {
            const std::size_t sn = sealRecordsFrame(*seal, egressScratch_.data(), n,
                                                    pushTooLargeSealScratch_.data(), pushTooLargeSealScratch_.size());
            if (sn == 0) [[unlikely]] return 0;   // fail-closed — never transmit, retried next pass
            if (sendEncodedToPub_(pub, pushTooLargeSealScratch_.data(), sn) <= 0) return 0;
            s.pushState = WorkerSubscription::PushState::Failed;
            return 1;
        }
        if (sendEncodedToPub_(pub, egressScratch_.data(), n) <= 0) return 0;
        s.pushState = WorkerSubscription::PushState::Failed;
        return 1;
    }

    int pushOwnedSubscriptions_(int64_t /*nowNs*/) {
        using namespace rune::protocol::batch;
        namespace pc = rune::protocol::client;
        if (subs_.empty()) return 0;

        int work = 0;
        // F5: bounded per pass (kPushPassBudgetBytes, shared with PushRead
        // answers) and fair — the walk starts at the subscription the budget
        // ran out on last pass.
        const std::size_t nSubs = subs_.size();
        if (pushRotor_ >= nSubs) pushRotor_ = 0;
        for (std::size_t k = 0; k < nSubs; ++k) {
            const std::size_t si = (pushRotor_ + k) % nSubs;
            auto& s = subs_[si];
            if (pushBudgetLeft_ == 0) { pushRotor_ = si; break; }
            if (!s.active) continue;   // parked: partition moved away (E2: the source is resolved below)

            // Hybrid egress: prefer this consumer's UNICAST pub (independent
            // per-consumer offsets); fall back to the shared MDC pub (broadcast).
            aeron::ExclusivePublication* egressPub = s.egressPub;
            if (!egressPub && workerEgress_) egressPub = workerEgress_->pub();
            if (!egressPub) continue;  // nowhere to send yet

            // Task 19 (per-worker egress crypto addendum): looked up ONCE per
            // subscription-drain call (not per batch) — `cr` is shared across
            // every batch pushed for `s` in this pass, so they all seal under
            // the SAME AeadCipher + the SAME monotonic sendSeq (never a
            // per-batch-reset counter: that would reuse nonces under this
            // worker's key). nullptr == plaintext session (the common case;
            // sessionCrypto_ is empty for an all-plaintext deployment) — takes
            // the exact pre-Task-19 zero-copy path below, untouched.
            // Task 23 audit fix: moved ABOVE the pushState/ErrorOwed check
            // (was below it) so sendPushTooLarge_'s retry call site (just
            // below) ALSO gets `sealPtr` — this frame is worker->client
            // egress too (the audit's 3rd path; see sendPushTooLarge_'s own
            // comment) and MUST seal under the exact same cr/sendSeq as
            // Records for this session.
            WorkerSessionCrypto* cr = findSessionCrypto_(s.sessionId);
            RecordsSealCtx seal;
            const RecordsSealCtx* sealPtr = nullptr;
            if (cr) [[unlikely]] {
                // Fail closed (RecordsSealCtx/sealRecordsFrame's own
                // contract): cr->send may be nullopt (this worker's key
                // failed to derive, or hasn't arrived yet) — seal.send stays
                // null, sealRecordsFrame returns 0, sendRecordsFrame/
                // sendPushTooLarge_ never transmit. NEVER a plaintext
                // fallback once `cr` exists.
                seal.send     = cr->send.has_value() ? &*cr->send : nullptr;
                seal.sendSeq  = &cr->sendSeq;
                seal.cipher   = cr->cipher;
                seal.senderId = static_cast<uint8_t>(workerId_ + 1);   // unique per worker; gateway == 0
                sealPtr = &seal;
            }

            if (s.pushState != WorkerSubscription::PushState::Live) [[unlikely]] {
                if (s.pushState == WorkerSubscription::PushState::ErrorOwed)
                    work += sendPushTooLarge_(s, egressPub, sealPtr);
                continue;   // parked AT the undeliverable batch
            }

            // Bound: the live HW (the worker owns the replica); read_committed
            // (F15 §2.4, K1): the LSO of THIS worker's RC state — read on the
            // thread that appends, so it can never lag a txn's first batch (K2).
            // Not Ready (§2.3): skip this subscription this pass.
            auto* rep = subReplica_(s);
            if (s.incarnation != 0 && !rep) continue;   // not owned (defensive: active implies owned)
            // Phase 2b E2: a TRANSIENT partition's source is the owned replica's
            // ring (the Subscribe op carried no disk Log); no source → nothing to push.
            rune::storage::InMemoryLog* const mem =
                (s.logPtr == nullptr && rep != nullptr) ? rep->memoryLog() : nullptr;
            if (s.logPtr == nullptr && mem == nullptr) continue;
            if (mem != nullptr) {   // TRANSIENT_EVICTED
                s.nextOffset  = skipEvictedCursor_(*mem, s.nextOffset, s.cursorFresh);
                s.cursorFresh = false;   // from now on the cursor is this push's own: a later skip is lag
            } else if (s.cursorFresh) [[unlikely]] {   // T2: the disk log start resolves here
                s.nextOffset  = raiseFreshDiskCursor_(*s.logPtr, s.nextOffset, true);
                s.cursorFresh = false;
            }
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

            const bool freshPass = pushBudgetLeft_ == kPushPassBudgetBytes;
            // #65 (review MAJOR-1): a pread into the worker's scratch — never the
            // segment read mmap — stopping near the bound (D26).  noexcept (F3):
            // a failed read is an empty view (retry next pass; an I/O error is
            // counted + logged, never retried through the mapping).
            const std::size_t readCap = std::min<std::size_t>(kPushMaxChunkBytes, pushBudgetLeft_);
            const rune::storage::LogReadView view =
                s.logPtr != nullptr ? readNoFault_(s.logPtr, s.nextOffset, readCap, bound)
                                    : mem->readView(s.nextOffset, static_cast<int32_t>(readCap));   // E2: zero-copy ring view
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
                // F5: never past this pass's budget, except the one batch a pass
                // that pushed nothing yet must take (progress).
                if (batchSize > pushBudgetLeft_ && !(freshPass && pushed == 0)) {
                    pushRotor_ = si;
                    pushBudgetLeft_ = 0;
                    break;
                }

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
                rec.correlationId  = s.subscribeCorrId;  // the client's Subscribe: exact instance identity
                rec.subscriptionId = s.subscriptionId;
                rec.topicId        = s.topicId;
                rec.partition      = s.partition;
                rec.baseOffset     = baseOffset;
                rec.recordBatch    = std::span<const uint8_t>(bp, batchSize);

                // F19: any frame up to the egress maxMessageLength is delivered
                // (tryClaim fast path / vectored [fixed | batch] offer — zero
                // copy of the batch).  Above it: fail this subscription
                // explicitly and park AT the batch — never skip forward.
                // Task 19: `sealPtr` is null for a plaintext session (the
                // exact pre-Task-19 call, unchanged); non-null seals via this
                // worker's own per-sender key/sendSeq (or fails closed —
                // Retry, never a plaintext frame — if the key never arrived).
                const RecordsSendResult sent =
                    sendRecordsFrame(*egressPub, rec, egressScratch_.data(), egressScratch_.size(),
                                      sealPtr, sealPtr ? &recordsSealScratch_ : nullptr);
                if (sent.kind == RecordsSendKind::TooLarge) [[unlikely]] {
                    spdlog::error("Worker {}: push sub={} (topicId={} partition={}) failed at "
                                  "offset={}: Records frame {} B exceeds the egress "
                                  "maxMessageLength {} B — MessageTooLarge, subscription parked "
                                  "(cursor not advanced)",
                                  workerId_, s.subscriptionId, s.topicId, s.partition, baseOffset,
                                  rec.encodedSize(), egressPub->maxMessageLength());
                    s.pushState      = WorkerSubscription::PushState::ErrorOwed;
                    s.failedFrameLen = rec.encodedSize();
                    work += sendPushTooLarge_(s, egressPub, sealPtr);
                    break;
                }
                const int64_t r = sent.position;
                if (r > 0) {
                    s.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                    pos += batchSize;
                    ++pushed;
                    ++work;
                    pushBudgetLeft_ -= std::min(batchSize, pushBudgetLeft_);
                } else {
                    backPressured = true;  // retry from same cursor next cycle
                }
            }
        }
        return work;
    }

    // -------------------------------------------------------------------
    // Task 7 / Task 12 per-worker schema validation (shared by both produce
    // paths).
    // HARD RULE 4 (zero hot-path alloc): validators_/validatedTopics_ were
    // built off-hot-path by executeSchemaUpdateWire_; validate() runs on a
    // zero-copy span; the per-record version-keyed lookup is a single
    // unordered_map::find on a precomputed uint64_t key — no allocation.
    // I1 fast-out: no validator ever installed for this topic → one load +
    // one predicted branch (validatedTopics_ empty/miss), same cost as before.
    // HARD RULE 5: concrete calls; HARD RULE 1: validators_ is worker-owned.
    // True = reject the batch (SchemaValidationFailed, never appended).
    //
    // Task 12: each record carries its OWN writer version (wire bytes [2..3]
    // of the record value, per rune::codec's [blockLength:u16][version:u16]
    // record header).  The validator is selected by the EXACT (topicId,
    // recVersion) pair; an installed-but-different-version validator is NEVER
    // substituted — an unknown version is a hard reject ("unknown schema
    // version"), not a fall-through to a wrong-version validator.
    // -------------------------------------------------------------------
    [[nodiscard]] bool schemaRejects_(int32_t topicId, const uint8_t* tail, std::size_t tailLen) const {
        if (validatedTopics_.empty()) [[likely]] return false;
        if (validatedTopics_.find(topicId) == validatedTopics_.end()) return false;
        if (tailLen < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
            return true;   // batch too short to parse header — malformed
        bool validationFailed = false;
        rune::protocol::RecordBatchReader rdr(tail, tailLen);
        // Task 11 fix 3 (B4, HARD RULE #4): templated record visitor — inlined,
        // no std::function per record.  The NoTypeErasure capture makes the
        // visitor move-only, so a regression to the type-erased forEach() fails
        // to COMPILE.
        rdr.forEachRecord([&, guard = detail::NoTypeErasure{}](std::string_view /*key*/,
                                                              std::string_view val,
                                                              int64_t /*offset*/,
                                                              int64_t /*ts*/) {
            if (validationFailed) return;
            // Record header: [blockLength:u16 LE][version:u16 LE][fixed block][var data].
            if (val.size() < 4) { validationFailed = true; return; }   // too short to carry a version
            const auto* vb = reinterpret_cast<const uint8_t*>(val.data());
            const uint16_t recVersion = static_cast<uint16_t>(vb[2]) |
                                       (static_cast<uint16_t>(vb[3]) << 8);
            auto it = validators_.find(packValidatorKey_(topicId, recVersion));
            if (it == validators_.end()) {
                validationFailed = true;   // unknown schema version — never guess
                return;
            }
            const rune::codec::ValResult res = it->second.validate(
                std::span<const uint8_t>(vb, val.size()));
            if (!res.ok) validationFailed = true;
        });
        if (validationFailed)
            spdlog::debug("Worker {}: schema validation FAILED for topicId={}; rejecting produce",
                          workerId_, topicId);
        return validationFailed;
    }

    // -------------------------------------------------------------------
    // executeProduceWire_ — a gateway-routed produce (WorkerOp::Produce):
    // produceCore_ on the op's zero-copy tail view (valid only for the
    // duration of this callback — the log restamps + copies it at append).
    // -------------------------------------------------------------------
    void executeProduceWire_(sbe::WorkerOp& op) {
        RUNE_FR_SPAN_PRODUCE(rune_fr::Site::WorkerAppend,
                             static_cast<uint32_t>(op.correlationId()));
        // F15 S5 (§2.5, I6) + Task 13 (spec §3.4): a gateway-received produce
        // joins THE produce path of this worker in EVERY mode (Legacy default
        // config, per-worker ingress, shard — both placements): the worker
        // psm_ validates it (duplicate → the original offsets, fenced epoch →
        // 17) in program order with the append and with every marker's fence.
        // The gateway holds no producer state for a worker-owned partition.
        ProduceFields f;
        f.sessionId      = op.sessionId();
        f.correlationId  = op.correlationId();
        f.producerId     = op.producerId();
        f.topicId        = op.topicId();
        f.partition      = op.partition();
        f.baseSequence   = op.baseSequence();
        f.minIsr         = op.minIsr();
        f.producerEpoch  = op.producerEpoch();
        auto* rep        = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        f.acksAll        = (op.deferAck() & kOpFlagDeferAck) != 0 && rep != nullptr;
        f.crcValidated   = (op.deferAck() & kOpFlagCrcValidated) != 0;
        f.durabilityTier = op.durabilityTier();
        f.enqueuedAtNs   = op.enqueuedAtNs();
        f.fromOp         = true;
        const std::uint32_t tl = op.tailLength();
        const char* tp = op.tail();
        (void)produceCore_(f, getLogFromWire_(op), rep,
                           std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(tp), tl),
                           passNowNs_);
    }

    // -------------------------------------------------------------------
    // -------------------------------------------------------------------
    // executeGetViewRowWire_ — Task 20: a point read of a materialized view
    // row, served on the worker that OWNS the view table (shared-nothing: a
    // pure read of this worker's own WorkerViewSet; no lock, no atomic).
    //
    // The op was routed to this worker for (viewTopicId, partition); the gateway
    // reused existing scalar wire fields (topicId = viewTopicId, nextOffset =
    // keyU64, hwBound = requestedFingerprint — see WorkerOp.h).  The probe is
    // zero-alloc: WorkerViewSet::getRow is an index probe, and on a hit we copy
    // exactly layout.rowLen bytes into the return's chunk.
    //
    // Status mapping (ViewRowResponse wire codes):
    //   UnknownView=4   — no WorkerViewSet, or no view plan installed for this id
    //   SchemaMismatch=3 — a non-zero requestedFingerprint != the view's output fp
    //   NotReady=2      — the (view, partition) instance is still rebuilding
    //                     (Task 16 serve-guard): a RETRIABLE not-ready, never a
    //                     half-built row (Review-focus #5)
    //   AbsentKey=1     — the view is Ready but the key is not present (typed
    //                     absent, never a zero row)
    //   OK=0            — the row bytes (layout.rowLen) copied into the chunk
    // -------------------------------------------------------------------
    void executeGetViewRowWire_(sbe::WorkerOp& op) {
        ++getViewRows_;
        const int32_t  viewTopicId = op.topicId();
        const int32_t  partition   = op.partition();
        const uint64_t keyU64      = static_cast<uint64_t>(op.nextOffset());
        const uint64_t requestedFp = static_cast<uint64_t>(op.hwBound());

        WorkerReturn ret;
        ret.type          = WorkerReturn::Type::ViewRowResult;
        ret.sessionId     = op.sessionId();
        ret.correlationId = op.correlationId();
        ret.readTopicId   = viewTopicId;   // echoed for gateway correlation
        ret.readPartition = partition;
        ret.recordCount   = 0;             // rowLen (informational)

        using RS = rune::protocol::client::ViewRowResponse;

        // 1. No view set on this worker, or no view installed for this id.
        uint64_t outputFp = 0;
        // Catalog history Task 12 (Task 8 review N5): the control thread posts a
        // view's plan to this worker's install ring BEFORE the gateway forwards
        // any read of it (the gateway gates a sink whose plan is not on every
        // install ring yet), but this pass drains ops before installs — drain
        // them once before answering the permanent UnknownView.  Error path only.
        if (viewSet_ != nullptr && !viewSet_->viewOutputFingerprint(viewTopicId, outputFp)) [[unlikely]]
            (void)viewSet_->drainInstalls();
        if (viewSet_ == nullptr || !viewSet_->viewOutputFingerprint(viewTopicId, outputFp)) {
            ret.errorCode = static_cast<int16_t>(RS::kUnknownView);
            enqueueReturn_(std::move(ret));
            return;
        }

        // 2. Schema drift: a pinned (non-zero) fingerprint that no longer matches
        //    the view's current output schema → SchemaMismatch (caught up front).
        if (requestedFp != 0 && requestedFp != outputFp) {
            ret.errorCode = static_cast<int16_t>(RS::kSchemaMismatch);
            enqueueReturn_(std::move(ret));
            return;
        }

        // 2b. C1 (Task 22 fix round 1): a view row is served only by the LEADER of
        //     its source partition, and only once the table is caught up under
        //     THIS leadership.  An ex-leader's frozen table, a follower (no table)
        //     or a broker that does not hold the partition answer the internal
        //     not-leader sentinel (the gateway redirects); a new leader that has
        //     not folded its committed tail yet answers NotReady — never a stale
        //     row or a wrong AbsentKey.  A linear scan of the worker-owned vector.
        {
            const int32_t src = viewSet_->viewSourceTopic(viewTopicId);
            const rune::replication::PartitionReplica* srcRep = findOwnedReplica_(src, partition);
            if (srcRep == nullptr || srcRep->role() != rune::replication::Role::Leader) {
                ret.errorCode = kViewRowNotLeaderInternal;
                enqueueReturn_(std::move(ret));
                return;
            }
            const auto gen = viewSet_->instanceLeadership(viewTopicId, partition);
            if (!gen || *gen != srcRep->leadershipGeneration()) {   // not folded under THIS leadership yet
                ret.errorCode = static_cast<int16_t>(RS::kNotReady);
                enqueueReturn_(std::move(ret));
                return;
            }
        }

        // 3. Probe the view table.  getRow applies the Task 16 serve-guard
        //    (NotReady while rebuilding) and distinguishes a genuinely absent key.
        const rune::view::RowLayout*  layout = nullptr;
        WorkerViewSet::RowStatus      rs     = WorkerViewSet::RowStatus::NotFound;
        const uint8_t* row = viewSet_->getRow(viewTopicId, partition, keyU64, layout, &rs);
        if (row != nullptr && layout != nullptr) {
            ret.errorCode   = static_cast<int16_t>(RS::kOk);
            ret.recordCount = static_cast<int32_t>(layout->rowLen);
            enqueueViewRowResult_(ret, row, layout->rowLen);   // zero-copy gather (hot path)
            return;
        } else if (rs == WorkerViewSet::RowStatus::NotReady) {
            ret.errorCode = static_cast<int16_t>(RS::kNotReady);
        } else {
            ret.errorCode = static_cast<int16_t>(RS::kAbsentKey);
        }
        enqueueReturn_(std::move(ret));   // no chunk: a fixed-size answer
    }

    // Zero-alloc OK answer for GetViewRow (Review-focus: no per-query heap alloc
    // on the read hot path).  The view row lives in STABLE worker-owned table
    // memory (only this thread folds it, and no fold runs between here and the
    // commit below), so — exactly as PushRead's readPushChunk_ does — we GATHER
    // the row bytes straight into the claimed return-ring slot (the single copy
    // between the table and the consumer; no intermediate std::vector).  The
    // back-pressure fallback (a return already queued, or the ring momentarily
    // full) copies into the owned chunk and queues it — never dropped, never
    // reordered; the common case never allocates.
    void enqueueViewRowResult_(WorkerReturn& ret, const uint8_t* row, uint32_t rowLen) {
        if (retFifo_.empty()) [[likely]] {
            const std::size_t n = workerReturnEncodedLength(static_cast<std::size_t>(rowLen));
            aeron::util::index_t idx = -1;
            try {
                idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n);
            } catch (const std::exception& ex) {
                haltInvariant_("return-ring tryClaim threw (ViewRowResult)", ex.what(), rowLen);
            } catch (...) {
                haltInvariant_("return-ring tryClaim threw (ViewRowResult)", "non-std exception", rowLen);
            }
            if (idx >= 0) {
                const ChunkRun run{row, rowLen};
                encodeWorkerReturnGather(reinterpret_cast<char*>(ret_.base() + idx), n, ret,
                                         &run, 1, rowLen);
                ret_.commit(idx);
                return;
            }
        }
        ret.chunk.assign(row, row + rowLen);   // back-pressure fallback (rare): copy + queue
        enqueueReturn_(std::move(ret));
    }

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
        bool     freshCursor     = false;   ///< E2 M2: the gateway's bind-time cursor (kOpFlagFreshCursor)
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
        a.freshCursor     = (op.deferAck() & kOpFlagFreshCursor) != 0;   // the wire flag byte (M2)
        a.targetReplicaId = op.targetReplicaId();
        a.incarnation     = op.incarnation();
        a.replica         = reinterpret_cast<rune::replication::PartitionReplica*>(op.replica());
        a.logPtr          = getLogFromWire_(op);
        servePushRead_(a, /*mayHold=*/true);
    }

    void servePushRead_(const PushReadArgs& in, bool mayHold) {
        PushReadArgs a = in;   // Phase 2b E2: the cursor may move up (TRANSIENT_EVICTED) — a stack copy
        rune::storage::Log* log = a.logPtr;
        // Phase 2b E2: a TRANSIENT partition has no disk Log — its source is the
        // owning replica's InMemoryLog.  a.replica is THIS worker's replica here:
        // PushRead is ownership-gated, and a deferred / held read whose partition
        // moved away is re-served with replica == nullptr (beginPushPass_,
        // serveHeldPushReads_) — never a ring this worker does not own.
        rune::storage::InMemoryLog* const mem =
            (log == nullptr && a.replica != nullptr) ? a.replica->memoryLog() : nullptr;
        if (mem != nullptr) a.nextOffset = skipEvictedCursor_(*mem, a.nextOffset, a.freshCursor);
        else if (log != nullptr) a.nextOffset = raiseFreshDiskCursor_(*log, a.nextOffset, a.freshCursor);   // T2
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

        if (!log && !mem) {
            // No source: an UNANSWERED read (review I-1) — the gateway re-issues it
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
                        owedCapAllows_()) {
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
        // F5 (hot-path B) + #65: zero allocation — the chunk is pread into the
        // worker's push scratch (readNoFault_: one pread, no page fault), then
        // gathered into the claimed return-ring slot (two copies in all),
        // clamped on the fly (bound, markers, aborted batches), within this
        // pass's push byte budget.
        if (nextOffset < bound) {
            switch (readPushChunk_(a, ret, log, mem, bound, rc)) {
            case PushChunk::Written:
                return;                         // answered in place (ret_ slot)
            case PushChunk::Deferred:
                if (deferPushRead_(a)) return;  // re-served next pass (owes one return)
                // No deferral room: UNANSWERED (review I-1) — the gateway re-issues
                // on its next push cycle; nothing parks.
                ret.newNextOffset = nextOffset;
                ret.setPushEffectiveBound(-1);
                break;
            case PushChunk::TooLarge:
                // Re-review 1 I-4: a SINGLE stored batch larger than the ring's
                // max message (stored under an earlier, bigger config, or
                // received from a peer and then led) cannot be returned.
                // Answered non-retriably — never the claim halt.
                ret.newNextOffset = nextOffset;
                ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::MessageTooLarge);
                notePushReadOversize_(topicId, partition, nextOffset);
                break;
            case PushChunk::Empty:
                break;                          // nothing deliverable (cursor may have advanced)
            }
        }

        enqueueReturn_(std::move(ret));   // no chunk: a fixed-size answer
    }

    // F5: one PushRead's chunk.  Walks the whole batches of one read (#65: a
    // pread into the push scratch — readNoFault_) once, applying the F15 §2.4 /
    // R-2 clamp (bound, control markers never delivered, read_committed aborted
    // batches stripped / cut pre-carrier) and collects the kept bytes as runs;
    // then claims ONE ret_ slot and gathers the runs into it.  No allocation;
    // one pread syscall (rarely two: a first batch the window cut) and two
    // copies (kernel -> scratch, scratch -> ring).
    //   Written  — answered in ret_ (budget charged);
    //   Empty    — no deliverable batch; ret.newNextOffset is past what was
    //              examined (answer it — R-1);
    //   Deferred — over this pass's push budget, or ret_ cannot take it in
    //              order now (FIFO non-empty / ring full): serve it next pass
    //              (the read is idempotent — nothing consumed);
    //   TooLarge — the first deliverable batch alone exceeds the ring's max message.
    enum class PushChunk : uint8_t { Written, Empty, Deferred, TooLarge };
    PushChunk readPushChunk_(const PushReadArgs& a, WorkerReturn& ret, rune::storage::Log* log,
                             rune::storage::InMemoryLog* mem, int64_t bound, const RcState* rc) {
        using rune::protocol::RecordBatchReader;
        namespace b = rune::protocol::batch;
        if (pushBudgetLeft_ == 0) return PushChunk::Deferred;
        const bool freshPass = pushBudgetLeft_ == kPushPassBudgetBytes;
        const std::size_t fit = pushReadChunkFit_();
        const std::size_t cap = std::min({static_cast<std::size_t>(kMaxPushReadChunkBytes), fit, pushBudgetLeft_});
        // #65: a pread into the worker's scratch (readNoFault_) — never the
        // segment read mmap — stopping near the bound (D26).  noexcept (F3): a
        // failed read is an empty view — answered empty, the gateway retries
        // next cycle.
        // Phase 2b E2: a TRANSIENT partition reads a zero-copy view of its ring
        // (same thread, no syscall); the bound clamp below applies unchanged.
        const rune::storage::LogReadView v =
            log != nullptr ? readNoFault_(log, a.nextOffset, cap, bound)
                           : mem->readView(a.nextOffset, static_cast<int32_t>(cap));
        if (v.empty()) return PushChunk::Empty;
        const bool preCarrier = a.isGroup && !a.carrier;
        std::size_t rd = 0, nRuns = 0, total = 0;
        int64_t next = ret.newNextOffset;
        int64_t cutBound = -1;
        while (rd + static_cast<std::size_t>(b::kRecordBatchOverhead) <= v.len) {
            RecordBatchReader r(v.data + rd, v.len - rd);
            const int32_t t = r.totalLength();
            if (t <= 0 || rd + static_cast<std::size_t>(t) > v.len) break;
            const auto sz = static_cast<std::size_t>(t);
            const int64_t base = r.baseOffset();
            const int64_t last = base + r.lastOffsetDelta();
            if (last >= bound) break;                                      // the HW / LSO bound
            const auto attrs = static_cast<uint16_t>(r.attributes());
            if ((attrs & static_cast<uint16_t>(b::kAttrControl)) != 0) {   // markers: never delivered
                rd += sz;
                next = last + 1;
                continue;
            }
            if (rc != nullptr && (attrs & kTxnAttrBit) != 0 && rc->isAborted(r.producerId(), base)) [[unlikely]] {
                if (preCarrier) { cutBound = base; break; }                // R-2: cut, never strip
                rd += sz;
                next = last + 1;
                continue;
            }
            if (sz > fit) [[unlikely]] {
                if (total == 0) return PushChunk::TooLarge;                // only ever the view's first batch
                break;
            }
            if (nRuns != 0 && pushRuns_[nRuns - 1].data + pushRuns_[nRuns - 1].len == v.data + rd) {
                pushRuns_[nRuns - 1].len += static_cast<uint32_t>(sz);    // contiguous: extend the run
            } else {
                if (nRuns == kMaxPushRuns) break;                          // cut here (the rest next read)
                pushRuns_[nRuns++] = ChunkRun{v.data + rd, static_cast<uint32_t>(sz)};
            }
            total += sz;
            rd += sz;
            next = last + 1;
        }
        // A pass that already pushed never takes a batch past its budget (the
        // view's first batch is always included — the read's progress rule).
        if (total > pushBudgetLeft_ && !freshPass) return PushChunk::Deferred;
        if (total == 0) {
            if (cutBound >= 0) ret.setPushEffectiveBound(cutBound);
            ret.newNextOffset = next;
            return PushChunk::Empty;
        }
        if (!retFifo_.empty()) return PushChunk::Deferred;                 // never overtake a queued return
        const std::size_t n = workerReturnEncodedLength(total);
        aeron::util::index_t idx = -1;
        try {
            idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n);
        } catch (const std::exception& ex) {
            haltInvariant_("return-ring tryClaim threw", ex.what(), total);
        } catch (...) {
            haltInvariant_("return-ring tryClaim threw", "non-std exception", total);
        }
        if (idx < 0) return PushChunk::Deferred;                           // ring full: next pass
        if (cutBound >= 0) ret.setPushEffectiveBound(cutBound);
        ret.newNextOffset = next;
        encodeWorkerReturnGather(reinterpret_cast<char*>(ret_.base() + idx), n, ret, pushRuns_.data(), nRuns,
                                 static_cast<uint32_t>(total));
        ret_.commit(idx);
        pushBudgetLeft_ -= std::min(total, pushBudgetLeft_);
        ++pushReadChunks_;
        pushReadBytes_ += total;
        return PushChunk::Written;
    }

    // F5: park a PushRead over this pass's budget (or behind a full ring) for
    // the next pass — FIFO (fair: deferred reads are served first next pass).
    // Shares kMaxHeldPushReads with the RC holds (each owes one return:
    // reservedAckSlots_).  False = no room (the caller answers unanswered).
    // Only replica-backed reads defer (ownership is re-validated on serve).
    bool deferPushRead_(const PushReadArgs& a) noexcept {
        if (a.replica == nullptr || heldPushReads_.size() + deferredPushCount_ >= kMaxHeldPushReads ||
            !owedCapAllows_())
            return false;
        deferredPushReads_[(deferredPushHead_ + deferredPushCount_) % kMaxHeldPushReads] = a;
        ++deferredPushCount_;
        ++pushReadsDeferred_;
        return true;
    }

    // F5: start of every pass — refill the push byte budget, then serve the
    // reads deferred by earlier passes (oldest first) before any new op.
    int beginPushPass_() {
        pushBudgetLeft_ = kPushPassBudgetBytes;
        if (deferredPushCount_ == 0 || !retFifo_.empty()) [[likely]] return 0;
        int n = 0;
        for (std::size_t k = deferredPushCount_; k > 0 && pushBudgetLeft_ > 0 && retFifo_.empty(); --k) {
            PushReadArgs a = deferredPushReads_[deferredPushHead_];
            deferredPushHead_ = (deferredPushHead_ + 1) % kMaxHeldPushReads;
            --deferredPushCount_;
            // Re-validate ownership (a SetOwnership / role op may have run since).
            rune::replication::PartitionReplica* r = nullptr;
            if (a.incarnation != 0) {
                if (ownsIncarnation_(a.targetReplicaId, a.incarnation)) r = ownedPtr_[a.targetReplicaId];
            } else {
                for (auto* o : owned_) if (o == a.replica) { r = o; break; }
            }
            if (r != nullptr) {
                a.replica = r;
                a.logPtr  = r->isDiskBacked() ? r->log() : nullptr;
            } else {
                // Never read a log — or (E2) a transient ring, found through the
                // replica — this worker no longer owns → unanswered.
                a.logPtr  = nullptr;
                a.replica = nullptr;
            }
            servePushRead_(a, /*mayHold=*/true);
            ++n;
        }
        return n;
    }

    // Phase 2b E2 — TRANSIENT_EVICTED: the ring evicted committed records this
    // reader had not read yet (the in-memory retention).  The cursor resumes AT
    // the ring start — counted, logged first + every power of two — never a
    // stall on an offset the ring no longer holds.  Disk logs never take this.
    // E2 final fix wave (M2): a FRESH cursor (the subscription's bind-time
    // resolved start — Earliest is 0 on a transient partition — never moved by
    // a read) below the ring start is a subscribe below the retention, not a
    // reader that fell behind: it resumes at the ring start too, but is counted
    // apart (transientFreshCursorsRaised_) and never warned as consumer lag.
    [[nodiscard]] int64_t skipEvictedCursor_(const rune::storage::InMemoryLog& mem, int64_t cursor,
                                             bool fresh) noexcept {
        const int64_t start = mem.logStartOffset();
        if (cursor >= start) [[likely]] return cursor;
        if (fresh) {
            ++transientFreshCursorsRaised_;
            return start;
        }
        ++transientCursorSkips_;
        transientRecordsSkipped_ += static_cast<uint64_t>(start - cursor);
        if ((transientCursorSkips_ & (transientCursorSkips_ - 1)) == 0)
            spdlog::warn("Worker {}: a reader of a transient partition fell behind its ring: cursor {} < ring "
                         "start {} — resumed at {} (skips={}, records skipped={})", workerId_, cursor, start,
                         start, transientCursorSkips_, transientRecordsSkipped_);
        return start;
    }

    // Sanitizer triage T2 (2026-10-14): the gateway no longer reads a worker-owned
    // disk Log's start (a cross-thread walk of its segment map).  A FRESH cursor —
    // the subscription's bind-time start: Earliest (0) or an explicit offset into a
    // retention-deleted region (CP-5.2) — is raised to the log start HERE, on the
    // owning thread, before the first read (Log::read below the first segment is
    // empty forever).  A non-fresh cursor is the push's own and is left as is.
    [[nodiscard]] static int64_t raiseFreshDiskCursor_(const rune::storage::Log& log, int64_t cursor,
                                                       bool fresh) noexcept {
        if (!fresh) return cursor;
        const int64_t start = log.logStartOffset();
        return cursor < start ? start : cursor;
    }

    // #65: the worker's push reads.  First-touch page faults on a segment read
    // MMAP take the process mmap_lock for read — queued behind the provisioner /
    // cleaner threads' mmap/munmap holding it for write: ~1 ms off-CPU worker
    // passes (box perf, briefs/2026-10-07-reads-retention-bisect.md).  So: a
    // pread into the preallocated scratch (no allocation, no fault), sized to
    // the request and stopping near `bound` (the HW / LSO: D26).  Only a first
    // batch larger than the whole scratch falls back to the mapped view —
    // counted (pushReadMapFallbacks_), and only after its bytes read cleanly.
    // An I/O error is answered empty, counted and logged — never retried
    // through the mapping (SIGBUS on the same bad page; D26) — and it fails the
    // log (sticky, review M1): checkStorageFailure_ routes it to the
    // log-dir-failure path this pass; the log is never pread again.
    // The view is valid until the next push read on this thread.
    rune::storage::LogReadView readNoFault_(rune::storage::Log* log, int64_t offset, std::size_t maxBytes,
                                            int64_t bound) noexcept {
        rune::storage::ReadIntoStatus st;
        int err = 0;
        const rune::storage::LogReadView v = log->readInto(offset, static_cast<int32_t>(maxBytes), pushScratch_.data(),
                                                           pushScratch_.size(), st, bound, &err);
        if (st != rune::storage::ReadIntoStatus::FirstTooLarge) [[likely]] {
            if (st == rune::storage::ReadIntoStatus::IoError) [[unlikely]] notePushReadError_(log, offset, err);
            return v;
        }
        ++pushReadMapFallbacks_;
        return log->readView(offset, static_cast<int32_t>(maxBytes));
    }
    [[gnu::cold]] void notePushReadError_(const rune::storage::Log* log, int64_t offset, int err) noexcept {
        ++pushReadErrors_;
        if ((pushReadErrors_ & (pushReadErrors_ - 1)) == 0)   // rate-limited: powers of two
            spdlog::error("Worker {}: push read of {} at offset {} failed (errno={}, errors={}) — answered empty, "
                          "the log is FAILED (never read again; log-dir-failure path)", workerId_, log->dir(), offset, err,
                          pushReadErrors_);
    }

    // Phase 2b E2: a TRANSIENT ring refused an append (full of unreplicated
    // records, or a batch larger than the ring).  Counted; logged first + every 1024th.
    [[gnu::cold]] void noteTransientFull_(int32_t topicId, int32_t partition,
                                          rune::protocol::client::ErrorCode ec) noexcept {
        ++transientFullRefusals_;
        if (transientFullRefusals_ == 1 || (transientFullRefusals_ & 1023u) == 0)
            spdlog::warn("Worker {}: transient topicId={} partition={}: ring refused an append ({}) — {} "
                         "(total={}); raise RUNE_TRANSIENT_LOG_RING_BYTES or let the followers catch up",
                         workerId_, topicId, partition, static_cast<int>(ec),
                         ec == rune::protocol::client::ErrorCode::MessageTooLarge
                             ? "batch larger than the ring" : "ring full of unreplicated records",
                         transientFullRefusals_);
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

        // F18 (B-1): the transport could not hand us the batch at op.nextOffset()
        // (too large for this ring on its own) — the replica decides whether that
        // is the batch at its LEO (wedge) or not (ignore).
        if (op.replErrorCode() == rune::replication::kErrRecordTooLarge) [[unlikely]] {
            replica->onUnapplicableBatch(op.nextOffset(), op.replLeaderEpoch());
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
            replica->becomeLeader(epoch, std::move(isr), op.rolePartitionEpoch());
        } catch (const std::exception& ex) {
            spdlog::warn("Worker {}: becomeLeader threw for topicId={} partition={}: {}",
                         workerId_, topicId, partition, ex.what());
        } catch (...) {
            spdlog::warn("Worker {}: becomeLeader threw unknown exception", workerId_);
        }

        // F15 S5 (§2.5, I3): no synchronous producer-state rebuild here.  The
        // worker psm_ for this partition is rebuilt by the incremental RC build
        // (RcState: bounded per pass, starts at leadership, scans the
        // whole log and applies control batches as epoch raises), in EVERY
        // mode; idempotent / transactional produces are refused NotLeader
        // (retriable) until it is Ready.
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
        req.lastFetchedEpoch = op.frLastFetchedEpoch();
        req.incarnation      = op.frIncarnation();
        req.storageId        = op.hwBound();                // ISR protocol (R14)
        req.lastEpochStartOffset = op.nextOffset();         // ISR protocol (R14)

        const ReplReadScope rs(*replica, replScratch_);
        try {
            replica->onFetchRequest(req, op.frNowNs(),
                                    static_cast<uint32_t>(op.ownershipGen()));   // F27 R1
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
        // F15 S5 (§2.5, C1+): fence the partition producer epoch on RECEIPT —
        // before the role gate, a MinIsr refusal or the append (raise only, no
        // sequence).  By L4 a gateway-routed produce enqueued before this op was
        // validated first and lands before the marker (covered by its range);
        // one enqueued after it — or a shard-ingress frame polled after it — is
        // validated against the raised epoch: a zombie gets 17.  The control
        // batch itself is the durable form (the PSM rebuild applies it).
        rune::transaction::ControlMarkerPayload mp;
        const bool mpOk = rune::transaction::ControlMarker::extract(tail, 0, mp, tailLen);
        static_assert(rune::transaction::kMarkerCoordinatorFencedError ==
                      static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
        // D9 review MAJOR-2 (Kafka TRANSACTION_COORDINATOR_FENCED): a marker from
        // a deposed coordinator — lower coordinatorEpoch than the last marker
        // applied for this producer here — would close the producer's NEXT
        // transaction.  Refused, nothing appended, no state touched.
        if (mpOk && psm_.markerCoordinatorFenced(topicId, mp.producerId, partition, mp.coordinatorEpoch)) {
            spdlog::warn("Worker {}: control marker topicId={} partition={} producerId={} from coordinator epoch {} "
                         "FENCED (a newer coordinator's marker was applied) — refused", workerId_, topicId,
                         partition, mp.producerId, mp.coordinatorEpoch);
            ret.errorCode = rune::transaction::kMarkerCoordinatorFencedError;
            enqueueReturn_(ret);
            return;
        }
        // D9 re-review MEDIUM-3: a remote-origin marker routed under an OLDER
        // leader epoch of this partition (a copy held across leadership changes)
        // is refused NotLeader — retriable: the coordinator re-routes.  roleEpoch
        // carries target+1 (0 = not checked: local markers).
        if (op.roleEpoch() > 0 && op.roleEpoch() - 1 < static_cast<int64_t>(replica->epoch())) [[unlikely]] {
            spdlog::info("Worker {}: control marker topicId={} partition={} routed under leader epoch {} < {} — "
                         "refused NotLeader", workerId_, topicId, partition, op.roleEpoch() - 1, replica->epoch());
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return;
        }
        // D9 review MAJOR-3: the answer parks on the HW (below) — reserve its
        // owed return slot BEFORE the append (G-A6, as a parked produce); no slot
        // → retriable NotLeader, log untouched.
        if (!ackReservationAvailable_()) [[unlikely]] {
            if (!owedCapAllows_()) noteOwedCapThrottle_("control marker");
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return;
        }
        if (mpOk) psm_.fenceEpoch(topicId, mp.producerId, partition,
                                  rune::protocol::RecordBatchReader(tail, tailLen).producerEpoch());
        try {
            // F15 N2: the marker lands AS a control batch (restamped in place),
            // with the replica's own role gate (NotLeader, retriable) and min-ISR gate.
            const ReplReadScope rs(*replica, replScratch_);   // D26: its push reads into the scratch
            const rune::replication::Offset base = replica->appendControlBatch(markerSpan, /*acksAll=*/true);
            if (base >= 0) {
                ret.baseOffset = base;
                if (mpOk) psm_.fenceEpoch(topicId, mp.producerId, partition,   // D9 MAJOR-2: the fence (raise-only)
                                          rune::protocol::RecordBatchReader(tail, tailLen).producerEpoch(),
                                          /*fromRebuild=*/false, mp.coordinatorEpoch);
                // F15 §2.1 step 2: the RC update in the same invocation as the
                // marker append (Ready: applied; Scanning: tail list).
                if (RcState* rc = rcOf_(replica)) {
                    if (mpOk)
                        rc->onMarker(mp.producerId, base, mp.controlType == rune::transaction::ControlType::Commit);
                    publishCleanerLso_(replica, *rc);   // D3b: a resolved txn lifts the cleaner's cap
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
            } else if (base == RepT::kAppendBackPressure) {
                // Phase 2b E2: a TRANSIENT ring full of unreplicated records —
                // retriable: the coordinator re-sends the marker (never dropped;
                // TransactionCoordinationService::onControlMarkerRefused retries
                // every code but Unknown{Topic,Partition} and the fence).
                noteTransientFull_(topicId, partition, pc::ErrorCode::TransientLogFull);
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);
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
        if (ret.errorCode == 0 && ret.baseOffset >= 0 &&
            !(replica->highWatermark() > ret.baseOffset &&
              static_cast<int>(replica->isr().size()) >= replica->minIsr())) {   // not committed yet: park
            // D9 review MAJOR-3 (Kafka WriteTxnMarkers, acks=-1): the marker
            // counts as landed only once the partition HW passes it with the
            // ISR >= minIsr — the acks=all waiter (re-validated every pass:
            // ownership, role, log generation → NotLeader, retried by the
            // coordinator).  No deadline: completion is decided by the HW or by
            // a role change, never by a timer.
            PendingAcksAll pending;
            pending.replica    = replica;
            pending.lastOffset = ret.baseOffset;
            pending.deadlineNs = std::numeric_limits<int64_t>::max();
            pending.logGen     = replica->logGeneration();
            pending.minIsr     = replica->minIsr();
            pending.ret        = std::move(ret);
            pendingAcksAll_.push_back(std::move(pending));
            return;
        }
        enqueueReturn_(ret);   // same FIFO as ProduceResults, never dropped
    }

    // -------------------------------------------------------------------
    // pollReplTransport_ — one replication-plane poll on a concrete transport:
    // handshake drive + templated poll with an inlined dispatch.  Production
    // calls it directly on the typed pointer; the test seam via the trampoline.
    // -------------------------------------------------------------------
    template <typename T>
    void reportPeerFrameDrops_(WorkerReplicationTransportImpl<T>& t, int64_t nowNs) {
        if (!t.peerSealed() || nowNs < nextPeerDropReportNs_) [[likely]] return;
        nextPeerDropReportNs_ = nowNs + 1'000'000'000LL;
        const uint64_t drops = t.peerFrameDrops();
        if (drops == reportedPeerDrops_) return;
        if (trySessionSignal_(0, SessionSignalKind::PeerFrameDrops, static_cast<int64_t>(drops))) reportedPeerDrops_ = drops;
    }

    template <typename T>
    int pollReplTransport_(WorkerReplicationTransportImpl<T>& t, int64_t nowNs) {
        // Shard plane handshake (spec §3.3): one predicted branch unless a peer
        // is unverified / refused (then a hello on the retry cadence).
        reportPeerFrameDrops_(t, nowNs);   // Task 16: paced (<= 1 per second), only when the count changed
        const int hs = t.driveHandshake(nowNs);
        // DR-15 (2026-10-05): a peer whose publication did not resolve at
        // startup (hostname unresolvable while it restarts) is re-added here on
        // a pass-count backoff — one predicted branch once every peer resolved.
        const int rs = t.drivePeerResolve();
        return hs + rs + t.poll(kReplFetchLimit,
                           [this, nowNs](int32_t topicId, int32_t partition, uint16_t templateId,
                                         const uint8_t* frame, std::size_t len) {
                               pollReplTransportDispatch_(topicId, partition, templateId, frame, len, nowNs);
                           });
    }

    // -------------------------------------------------------------------
    // setReplicationTransportImpl_ — TEST SEAM ONLY: wires the (ctx, fn*)
    // trampoline for a WorkerReplicationTransportImpl<T> with a fake registry.
    // -------------------------------------------------------------------
    template <typename T>
    void setReplicationTransportImpl_(WorkerReplicationTransportImpl<T>* transport) noexcept
    {
        if (!transport) {
            replTransportCtx_    = nullptr;
            replTransportPollFn_ = nullptr;
            return;
        }
        replTransportCtx_    = static_cast<void*>(transport);
        replTransportPollFn_ = [](void* ctx, int /*limit*/, Worker* self, int64_t nowNs) -> int {
            return self->pollReplTransport_(*static_cast<WorkerReplicationTransportImpl<T>*>(ctx), nowNs);
        };
    }

    // -------------------------------------------------------------------
    // findOwnedReplica_ — the owned replica for (topicId, partition), or nullptr.
    //
    // O(1): an open-addressing index (tpIndex_) rebuilt COLD on every ownership
    // change (setOwnedDirect / SetOwnership op -> rebuildFlushSlots_ ->
    // rebuildOwnedIndex_).  Maps the key to the FIRST non-null replica of owned_
    // with that key (the linear-scan semantics).  Role is NOT in the index: callers
    // read role()/acceptsProduce() live after the lookup.  Zero allocation.
    //
    // Worker-thread only (owned_ is single-writer on the worker thread).
    // -------------------------------------------------------------------
    rune::replication::PartitionReplica*
    findOwnedReplica_(int32_t topicId, int32_t partition) const noexcept
    {
        if (tpIndex_.empty()) return nullptr;
        const uint64_t k = tpKey_(topicId, partition);
        if (k == kTpEmpty) return nullptr;
        for (uint64_t i = tpHash_(k) & tpMask_;; i = (i + 1) & tpMask_) {
            const TpSlot& sl = tpIndex_[i];
            if (sl.key == k) return sl.r;
            if (sl.key == kTpEmpty) return nullptr;
        }
    }
    /// Test oracle: the old linear scan (first non-null match in owned_).
    rune::replication::PartitionReplica*
    findOwnedReplicaLinearForTest_(int32_t topicId, int32_t partition) const noexcept
    {
        for (auto* r : owned_)
            if (r && r->topicId() == topicId && static_cast<int32_t>(r->partitionId()) == partition) return r;
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
            const rr::FetchRequestMsg req = rr::FetchRequestMsg::decode(frame, 0, len);
            const ReplReadScope rs(*replica, replScratch_);
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
            const rr::FetchRequestMsg req = rr::FetchRequestMsg::decode(frame, 0, len);
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

    // A9: this worker thread's ring doorbell (declared before the rings that point at it).
    detail::RingDoorbell doorbell_;
    // A9: the op ring's doorbell watch — false while drainIngress_ holds the op ring back (return FIFO at high
    // water, no session-ack room): those gates wake the worker through their own sources (ret_ space wait,
    // session-publication status messages), and a watch on a ring the worker will not drain would keep its
    // runner from ever parking.  Worker-thread state (read by its own idle).
    bool ingressWatchable_ = true;
    bool spaceWaitArmed_   = false;   // A9: ret_ space wait armed by prepareIdle_ (worker thread)
    bool doorbellEnabled_  = false;   // A9: enableDoorbell() ran (startup; then read-only)

    // R2: ingress ring — OneToOneByteRing (SPSC, flat byte messages).
    // Producer (control thread): tryEnqueueOp tryClaim()s + encodes the WorkerOp
    //   DIRECTLY into the claimed slot (zero-copy) + commit()s.
    // Consumer (worker thread):  drainIngress_ drains → zero-copy view → executeFromWire_.
    OneToOneByteRing ingress_;

    // Task 13 fix round 1 (C1a): session-lifecycle ring — SessionAnnounce /
    // SessionClose only (control rate; they emit no returns).  Drained IN FULL
    // at the top of every pass, so a session op never waits behind data ops, an
    // op-drain pause or an ack backlog (lease reap / Disconnect always land).
    // Same SPSC producer (the runtime thread) and SBE WorkerOp frames as ingress_.
    OneToOneByteRing sessionRing_;

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

    // Task 15: this worker's view-fold set (its ViewSnapshotChannel + installed
    // views).  Owned/wired by BrokerNode (Task 19); a non-owning pointer here —
    // nullptr means no views on this worker (the common case) and the view
    // drive in doWork is skipped.  Shared-nothing: touched only by this thread.
    WorkerViewSet* viewSet_ = nullptr;

    // D23 (D21 review L1b): the dup fds of epoch-state fsyncs still in flight
    // when the worker is destroyed.  Declared BEFORE io_, so it is destroyed
    // AFTER it: the fds are closed only once the backend is gone (portable:
    // flusher joined; io_uring: ring closed — the kernel holds its own file
    // reference), never under an in-flight fsync.  Filled by ~Worker (cold).
    struct DeferredFdClose {
        std::vector<int> fds;
        ~DeferredFdClose() { for (const int fd : fds) ::close(fd); }
    };
    DeferredFdClose teardownSyncFds_;

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
    };
    // F15 S5 fix round 1 (I-1): park-ordered, O(1) front fire (OrderedWaitQueue.h).
    OrderedWaitQueue<DurablePending> durablePending_;

    // E3 Task 5: per-cycle set of logs that have had at least one Strict-tier
    // append this cycle.  submitCoalescedFlushes_ drains this and submits one
    // io_.submit per log.  Worker-thread only — plain vector, reserved in ctor.
    std::vector<rune::storage::Log*> dirtyThisCycle_;
    // F15 S5 fix round 2 (M-6): dirty logs whose fsync waits behind an in-flight one.
    std::vector<rune::storage::Log*> stillDirty_ = [] { std::vector<rune::storage::Log*> v; v.reserve(64); return v; }();
    uint64_t durableSubmitsDeferred_ = 0;

    // E3 Task 5: cumulative count of successful io_.submit calls in
    // submitCoalescedFlushes_.  Used by tests (ioSubmitCountForTest()).
    uint64_t io_submitCount_ = 0;

    // E4: CPU affinity — core to pin this worker's AgentRunner thread to.
    // -1 = disabled (default, no pinning).  Set via setCoreId() at startup,
    // before the AgentRunner starts.  Read-only at runtime (single writer).
    int  coreId_ = -1;
    bool shardMode_ = false;   // spec §3.1: set before the runner starts, then read-only
    // Task 13 (spec §3.4): worker-side acks — engaged iff shard mode with this
    // worker's own client (setShardClient).  Worker-thread only once running.
    std::unique_ptr<SessionAckSink> ackSink_;
    // Fix round 1 (C1b/c) — the session ack queue bound.  A = queued acks, O =
    // owed returns (reservedAckSlots_, <= kMaxOwedReturnsCeiling).  Every intake
    // step is limited by ackRoom_() = kAckCapacity − A − O (op drain:
    // drainIngress_; client poll: ingressPollBudget_), and each op / frame adds
    // at most one to A + O (an answer now, or one owed); owed → answer
    // conversions (HW fire, §3.2 fail, fsync, purge) keep A + O; a flush only
    // lowers A.  So A + O <= kAckCapacity holds for ANY client behaviour —
    // enqueue never fails (a failure is an invariant halt).  Liveness: with no
    // held acks a pass still admits a full drain AND a full poll at the owed
    // ceiling (assert below).  Per session (G-A6) — the safety invariant above
    // holds regardless; this is the FAIRNESS argument: a client whose acks are
    // held is paused on BOTH intake paths — its direct frames at the next frame
    // (controlled ABORT here), its gateway-routed frames once the runtime has
    // the Held signal (fix round 2: controlled ABORT on its gateway image) or,
    // D8c, once it has forwarded the session's produce credit budget of
    // routed produces whose acks this worker has not CLAIMED (credits return
    // via SessionSignal Credit from reportCredits).  Its share is therefore
    // what it sent before each pause took effect: one pass's direct frames,
    // its owed acks, and at most its credit budget of routed produces (op
    // ring + owed + queued) — independent of burst size / MTU / socket
    // buffers.  The room throttle (ackRoom_) only backs that up.
    static_assert(SessionAckSink::kAckCapacity >=
                      kMaxOwedReturnsCeiling + static_cast<std::size_t>(kMaxIngressDrainLimit) +
                          static_cast<std::size_t>(kMaxFragmentLimit),
                  "Task 13 G-A6: an idle ack queue must admit a full op drain and poll at the owed ceiling");
    uint64_t sessionAnnouncesSeen_ = 0;
    int32_t  lastAnnouncedClientVersion_ = -1;   ///< D6 test probe (the last SessionAnnounce's)
    uint64_t sessionClosesSeen_    = 0;
    uint64_t peerLinkKeysSeen_     = 0;
    int64_t  nextPeerDropReportNs_ = 0;   // Task 16: paced report of the plane's dropped peer frames
    uint64_t reportedPeerDrops_    = 0;
    uint64_t intakePausedFrames_   = 0;   // fix round 1: per-session G-A6 pause (ABORTs); D8d: + direct budget
    uint64_t intakeRollHolds_      = 0;   // hotpath-A: frames held because their partition's roll is deferred
    uint32_t directSessionAckBudget_ = SessionAckSink::kDefaultDirectSessionBudget;   // D8d
    uint64_t ackRoomOpPauses_      = 0;   // fix round 1: op drains skipped, ack queue full
    uint64_t sessionSignalsDeferred_ = 0; // fix round 2 (finding 9)

    // E4: true after the one-shot self-pin attempt fires in doWork.
    // Prevents repeated pin attempts on every doWork call.
    bool pinned_ = false;
    std::vector<int32_t> ioWorkerCpus_;   ///< LAT-1 (f), applied once on the worker thread
    int ioWorkerCpusResult_ = -1;

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
    // It answers a sequence only while PROVEN consistent with the partition's
    // log (the contract: OOS = "NOT appended"; a stored batch is answered OK /
    // duplicate, never appended again):
    //   * a replicated user partition — rebuilt from logStart to LEO by the RC
    //     build (RcState; a memory-backed one from its InMemoryLog) on every
    //     leadership / log generation, idempotent batches refused NotLeader
    //     until Ready (rcGate_);
    //   * a replica-less (passthrough) partition — rebuilt once per log on its
    //     first idempotent batch (passthroughPsm_: nothing else writes it);
    //   * an internal partition (topicId < 0) never carries a producer id —
    //     such a batch is refused (Internal), never validated.
    // (The former "a fresh PSM at construction is safe" note was wrong: an
    // empty state answers OOS for a stored batch with seq > 0 and accepts a
    // stored seq 0 again.)
    rune::storage::ProducerStateManager psm_{};
    // Replica-less (passthrough) partitions: the proof state of psm_ for each,
    // keyed on the stable (topicId, partition) identity + the log's content
    // generation (M4 — no Log* key).  Same rules as the gateway's inline state
    // (fix round 1): a budgeted rebuild (I3: kPassthroughPsmStepBytes per step,
    // one per idempotent batch and one per pass; NotLeader while it runs), a
    // failed one LATCHED (I2: Internal without a rescan until the log changes
    // or the doubling backoff ends).
    struct PassthroughUnproven {};
    struct PassthroughScanning {
        uint64_t logGen = 0;
        rune::storage::Log* log = nullptr;   ///< the log being scanned (driven per pass)
        rune::storage::ProducerStateManager::Rebuild cursor;
        int64_t backoffNs = 0;
    };
    struct PassthroughProven { uint64_t logGen = 0; };
    struct PassthroughFailed { uint64_t logGen = 0; int64_t retryAtNs = 0; int64_t backoffNs = 0; };
    struct PassthroughPsm {
        int32_t topicId = 0;
        int32_t partition = 0;
        std::variant<PassthroughUnproven, PassthroughScanning, PassthroughProven, PassthroughFailed> st;
    };
    std::vector<PassthroughPsm> passthroughPsm_;
    uint64_t passthroughPsmRebuilds_ = 0;
    static constexpr std::size_t kPassthroughPsmStepBytes = 1u << 20;
    static constexpr int64_t     kPassthroughRetryMinNs   = 100'000'000;
    static constexpr int64_t     kPassthroughRetryMaxNs   = 30'000'000'000;
    enum class PsmGate : uint8_t { Proven, NotLeader, Unprovable };
    // Cold the first time per partition; a short linear scan afterwards
    // (replica-less partitions exist only in rigs / passthrough deployments).
    [[nodiscard]] PsmGate provePassthroughPsm_(int32_t topicId, int32_t partition, rune::storage::Log& log) {
        PassthroughPsm* e = nullptr;
        for (auto& p : passthroughPsm_)
            if (p.topicId == topicId && p.partition == partition) { e = &p; break; }
        if (e == nullptr) {
            passthroughPsm_.push_back(PassthroughPsm{topicId, partition, PassthroughUnproven{}});
            e = &passthroughPsm_.back();
        }
        const uint64_t gen = log.contentGeneration();
        if (const auto* p = std::get_if<PassthroughProven>(&e->st); p && p->logGen == gen) [[likely]]
            return PsmGate::Proven;
        int64_t backoff = kPassthroughRetryMinNs;
        if (const auto* f = std::get_if<PassthroughFailed>(&e->st)) {
            if (f->logGen == gen && passNowNs_ < f->retryAtNs) return PsmGate::Unprovable;
            if (f->logGen == gen) backoff = std::min(f->backoffNs * 2, kPassthroughRetryMaxNs);
        }
        auto* sc = std::get_if<PassthroughScanning>(&e->st);
        if (sc == nullptr || sc->logGen != gen || sc->log != &log) {
            PassthroughScanning fresh;
            fresh.logGen = gen; fresh.log = &log; fresh.backoffNs = backoff;
            psm_.beginRebuild(fresh.cursor, topicId, log, log.logEndOffset(), partition);
            ++passthroughPsmRebuilds_;
            e->st = fresh;
            sc = std::get_if<PassthroughScanning>(&e->st);
        }
        using RS = rune::storage::ProducerStateManager::Rebuild::Status;
        const RS r = psm_.rebuildStep(sc->cursor, log, kPassthroughPsmStepBytes);
        if (r == RS::InProgress) return PsmGate::NotLeader;
        if (r == RS::Done) { e->st = PassthroughProven{gen}; return PsmGate::Proven; }
        const int64_t b = sc->backoffNs;
        psm_.clearPartition(topicId, partition);
        e->st = PassthroughFailed{gen, passNowNs_ + b, b};
        spdlog::error("Worker {}: producer-state rebuild of topicId={} partition={} stopped short of the log end "
                      "— idempotent produces answered Internal; retried on a log change or in {} ms",
                      workerId_, topicId, partition, b / 1'000'000);
        return PsmGate::Unprovable;
    }
    // One budgeted step per pass for every passthrough build in progress.
    int advancePassthroughPsm_() {
        int work = 0;
        for (auto& p : passthroughPsm_) {
            if (auto* sc = std::get_if<PassthroughScanning>(&p.st)) {
                (void)provePassthroughPsm_(p.topicId, p.partition, *sc->log);
                ++work;
            }
        }
        return work;
    }
public:
    [[nodiscard]] uint64_t passthroughPsmRebuildsForTest() const noexcept { return passthroughPsmRebuilds_; }
private:

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

    // TEST SEAM ONLY (Task 11 fix 3): (ctx, fn*) trampoline for a
    // WorkerReplicationTransportImpl<T> with a fake registry.  Production polls
    // workerReplTransport_ directly.  Null when the seam is not used.
    using ReplPollFn = int(*)(void* ctx, int limit, Worker* self, int64_t nowNs);
    void*       replTransportCtx_    = nullptr;
    ReplPollFn  replTransportPollFn_ = nullptr;

    // read-rework T3/T4: worker-local subscription cursors.  Worker-thread-only;
    // Subscribe/Unsubscribe ring ops mutate it, the I3 push loop iterates it.
    // Reserved at construction so the common case never reallocates on the (rare,
    // control-plane) Subscribe path.
    static constexpr std::size_t kSubsReserve = 4096;
    std::vector<WorkerSubscription> subs_;
    uint64_t egressReleases_ = 0;   ///< release ops executed (record-egress retire-on-ack)

    // Authentication Task 13: this worker's one chunked-permission assembler (shared-nothing; worker thread only).
    PermissionStager permStager_;
    uint64_t         sessionPermissionChunksSeen_ = 0;
    uint64_t         workerNotAuthorized_ = 0;   ///< shard produces refused NotAuthorized (counted)
    uint64_t         intakeGrantHolds_    = 0;   ///< denied frames held one pass for a lifecycle op in flight

    // Task 19: this worker's per-session Records-egress crypto state — see
    // WorkerSessionCrypto's doc comment. Entries exist ONLY for encrypted
    // sessions (never for plaintext ones): empty for an all-plaintext
    // deployment. Linear scan (bounded by this worker's own concurrently
    // encrypted sessions, not the whole shard) — same style already used for
    // subs_; a hash-by-sessionId is a known follow-up if that count ever
    // gets large enough to matter.
    // Authentication Task 13 (fix-on-sight): slot-indexed (open addressing by session id), no longer a linear scan
    // over every encrypted session on every produce frame.
    SessionCryptoTable<WorkerSessionCrypto> sessionCrypto_;
    [[nodiscard]] WorkerSessionCrypto* findSessionCrypto_(int64_t sid) noexcept { return sessionCrypto_.find(sid); }
    void eraseSessionCrypto_(int64_t sid) noexcept { sessionCrypto_.erase(sid); }
    /// Test-only: whether this worker holds (any) crypto state for a session.
    [[nodiscard]] bool sessionCryptoInstalledForTest(int64_t sid) const noexcept {
        const WorkerSessionCrypto* e = sessionCrypto_.find(sid);
        return e != nullptr && e->send.has_value();
    }

    // Task 19: this worker's OWN RecordsSealScratch for its Records-egress
    // seal path (shared-nothing: never shared across threads — mirrors
    // ClientGateway::recordsSealScratch_). Grow-once, reused every push.
    RecordsSealScratch recordsSealScratch_;

    // Task 23: scratch RecordsSealCtx handed to SessionAckSink::flush's
    // crypto-lookup callback (built fresh per lookup call, consumed
    // synchronously inside that same flush() call — never held across
    // passes). Deliberately a SEPARATE instance from pushOwnedSubscriptions_'s
    // local `seal` (Records): the two run at different points of one doWork()
    // pass on the same thread, never concurrently, but keeping them distinct
    // avoids any accidental aliasing between the Records egress call site and
    // the ack-flush call site. Both still point at the SAME WorkerSessionCrypto
    // entry's `send`/`sendSeq`/`cipher` for a given session — the shared
    // counter lives in sessionCrypto_, not here.
    RecordsSealCtx ackSealCtx_;

    // Task 22 (shard worker decrypts produce / per-worker ingress): this
    // worker's OWN scratch for openProduceFrame()'s decrypted output
    // (RecordsOpen.h). Grow-once (capacity persists; resize() only ever
    // grows), reused every encrypted produce frame — never a per-frame heap
    // allocation on the hot path. The decrypted frame is always
    // kCryptoOverhead (28) bytes SHORTER than the wire frame it came from,
    // so sizing it to the wire frame's own length is always enough.
    std::vector<uint8_t> workerIngressOpenScratch_;

    // Task 19: cold scratch for assembling an encrypted SessionAnnounce's
    // combined [cipher|prefix|key|channel] tail (see SessionAnnounceKeyTail.h).
    // Sized for the fixed crypto header + the largest egress channel the
    // runtime will ever announce (SessionAckSink::kMaxChannelBytes). A
    // worker-owned member (not a per-call stack buffer) so enqueueOp's hot
    // Produce/PushRead/etc. callers never pay for it on their stack frame.
    std::array<uint8_t, kSessionAnnounceCryptoHeaderBytes + SessionAckSink::kMaxChannelBytes>
        sessionAnnounceTailScratch_{};

    // read-rework T3/T4 (I3): per-worker encode scratch for the in-worker push
    // loop.  Holds one encoded single-fragment Records frame (tryClaim fast
    // path), or just the 52-byte fixed part of a larger frame whose batch bytes
    // go out zero-copy in a vectored offer (F19: the scratch size no longer
    // bounds the deliverable batch size — the egress maxMessageLength does).
    // Reused every push; NO per-batch heap allocation on the read hot path.
    static constexpr std::size_t kEgressScratchSize = 68 * 1024;
    // Max whole batches pushed per subscription per doWork cycle (fairness).
    static constexpr int         kPushBatchesPerSubPerCycle = 1024;
    // Max bytes read per subscription per cycle (matches gateway).
    static constexpr int32_t     kPushMaxChunkBytes = static_cast<int32_t>(512 * 1024);
    std::vector<uint8_t> egressScratch_;
    // Task 23 (egress audit, 3rd path): sendPushTooLarge_'s ENCRYPTED-session
    // seal output. kPushTooLargeErrorMaxBytes is a compile-time bound (the
    // ErrorResponse message is a fixed-format snprintf, never user data), so
    // — like the ack path's scratch — a fixed array is strictly simpler than
    // a grow-once vector and still zero-allocation (Rule 4).
    std::array<uint8_t, kPushTooLargeErrorMaxBytes + rune::protocol::client::kCryptoOverhead>
        pushTooLargeSealScratch_{};
public:
    /// F5 (hot-path B): the consumer-push byte budget of ONE worker pass, shared
    /// by gateway PushRead answers and in-worker egress pushes: every pushed
    /// byte is one copy (mmap → return ring / Aeron term), so a pass copies at
    /// most this much (plus, in a pass that pushed nothing yet, the one batch
    /// larger than it).  ~10 µs of memcpy at page-cache bandwidth; the rest
    /// waits for the next pass (served first — fair).
    static constexpr std::size_t kPushPassBudgetBytes = 128 * 1024;
    /// Most contiguous runs one gathered PushRead chunk holds (a run ends at a
    /// stripped marker / aborted batch); the read is cut there beyond it.
    static constexpr std::size_t kMaxPushRuns = 64;
    [[nodiscard]] uint64_t pushReadChunksForTest() const noexcept { return pushReadChunks_; }
    [[nodiscard]] uint64_t pushReadBytesForTest() const noexcept { return pushReadBytes_; }
    [[nodiscard]] uint64_t pushReadsDeferredForTest() const noexcept { return pushReadsDeferred_; }
    /// #65: push reads that fell back to the segment read mmap (first batch > scratch).
    [[nodiscard]] uint64_t pushReadMapFallbacksForTest() const noexcept { return pushReadMapFallbacks_; }
    [[nodiscard]] uint64_t pushReadErrorsForTest() const noexcept { return pushReadErrors_; }
    [[nodiscard]] std::size_t deferredPushReadsForTest() const noexcept { return deferredPushCount_; }
    /// Phase 2b E2 (TRANSIENT partitions): produces/markers the ring refused;
    /// reads whose cursor was below the ring start, and the records they skipped.
    [[nodiscard]] uint64_t transientFullRefusalsForTest() const noexcept { return transientFullRefusals_; }
    [[nodiscard]] uint64_t transientCursorSkipsForTest() const noexcept { return transientCursorSkips_; }
    [[nodiscard]] uint64_t transientRecordsSkippedForTest() const noexcept { return transientRecordsSkipped_; }
    /// M2: fresh cursors (a subscribe below the ring start) raised to it — not lag.
    [[nodiscard]] uint64_t transientFreshCursorsRaisedForTest() const noexcept { return transientFreshCursorsRaised_; }
private:
    std::size_t                           pushBudgetLeft_ = kPushPassBudgetBytes;
    std::array<ChunkRun, kMaxPushRuns>    pushRuns_{};
    /// #65: push-read pread target (allocated once, at construction): one
    /// pass's push budget + the sparse-index skip slack + room for a first
    /// batch past the budget (a fresh pass always takes one).  Larger first
    /// batches fall back to the mapped view (pushReadMapFallbacks_).
    rune::storage::ZeroedBytes          pushScratch_{2 * kPushPassBudgetBytes + 64 * 1024};   // ZeroedBytes: memset, not a per-byte vector fill (Debug-build cost)
    uint64_t transientFullRefusals_   = 0;   ///< E2: ring refused a produce / marker (full or too large)
    uint64_t transientCursorSkips_    = 0;   ///< E2: reads whose cursor was below the ring start
    uint64_t transientRecordsSkipped_ = 0;   ///< E2: records those reads skipped (evicted unread)
    uint64_t transientFreshCursorsRaised_ = 0;   ///< E2 M2: bind-time cursors below the ring start (not lag)
    uint64_t                              pushReadMapFallbacks_ = 0;
    uint64_t                              pushReadErrors_ = 0;   ///< D26: push-read pread failures (answered empty)
    /// D26: replication read scratch — a fetch serve / owed re-serve / push of
    /// an owned replica preads into it (PartitionReplica::setReadScratch), never
    /// faulting through the segment read map.  maxFetchBytes (1 MiB) + the
    /// sparse-index skip slack; larger reads fall back to the mapped view.
    rune::storage::ZeroedBytes          replScratch_{(1u << 20) + 64 * 1024};
    /// Sets the replica's read scratch for one call from this thread, cleared
    /// after: a replica handed to another owner never keeps this worker's buffer.
    struct ReplReadScope {
        rune::replication::PartitionReplica& r;
        ReplReadScope(rune::replication::PartitionReplica& rep, rune::storage::ZeroedBytes& buf) noexcept : r(rep) {
            r.setReadScratch(buf.data(), buf.size());
        }
        ~ReplReadScope() { r.setReadScratch(nullptr, 0); }
        ReplReadScope(const ReplReadScope&) = delete;
        ReplReadScope& operator=(const ReplReadScope&) = delete;
    };
    std::size_t                           pushRotor_ = 0;   ///< in-worker push: first sub served next pass
    uint64_t                              pushReadChunks_ = 0, pushReadBytes_ = 0, pushReadsDeferred_ = 0;

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
    // F15 S5 fix round 1 (I-1): park-ordered, O(1) front fire (OrderedWaitQueue.h).
    OrderedWaitQueue<PendingAcksAll> pendingAcksAll_;

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
    uint64_t    getViewRows_ = 0;          // Task 20: GetViewRow ops executed
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

    // E5.1.3 (d) R7 fix: last time syncOwnedLogsAsync_ was run (ns). 0 = never.
    // Worker-thread only — no atomic needed.
    int64_t lastSyncNs_ = 0;

    // Per-log LEO at its last successful sync lives in LogFlushSlot::lastSyncedLeo
    // (Task 10 fix 1 / I3: fixed per-owned-log slots, no hot-path map insert).

    // Task 7 / Task 12: per-worker (topic, version) → Validator map.
    //
    // Populated via SchemaUpdate ring ops (executeSchemaUpdateWire_), which
    // arrive from the admin thread as ring messages and are applied on THIS
    // worker's own thread (shared-nothing — no lock, no atomic).
    //
    // Looked up in schemaRejects_ BEFORE appendNoSyncRestamp, once per record
    // (keyed by that record's OWN writer version — Task 12).  The lookup is a
    // single unordered_map::find (O(1) expected) on a packed uint64_t key (no
    // allocation, no pair hashing) with a single conditional branch when no
    // version of the topic is validated (most topics have no schema validator
    // installed at all — see validatedTopics_).
    //
    // HARD RULE 1 (shared-nothing): validators_ is ONLY ever touched by the
    // worker thread — the admin thread copies the blob into the ring, the worker
    // builds the Validator from the ring view on ITS OWN thread.
    // HARD RULE 4 (zero hot-path alloc): the map is built off the hot path
    // (in executeSchemaUpdateWire_).  The hot path only does a map::find + span
    // validate (no allocation).
    //
    // Reserved capacity is intentionally small (16): most deployments have
    // O(10s) of (topic, version) pairs per worker; the map is rarely rebuilt.
    // TODO(T9): benchmark flat vs unordered_map for typical topic counts.
    static constexpr std::size_t kValidatorsReserve = 16;
    [[nodiscard]] static constexpr uint64_t packValidatorKey_(int32_t topicId, uint16_t version) noexcept {
        return (static_cast<uint64_t>(static_cast<uint32_t>(topicId)) << 16) |
               static_cast<uint64_t>(version);
    }
    std::unordered_map<uint64_t, rune::codec::Validator> validators_;
    // Task 12: fast "does this topic have ANY validated version installed"
    // pre-check — independent of which exact version — so a topic that never
    // had validate=true (no entries in validators_ for any version) is never
    // mistaken for one whose exact record version is simply unknown (which
    // IS a hard reject; see schemaRejects_).  Grows only (versions are never
    // un-validated), so a topicId inserted once stays a cheap O(1) hit.
    std::unordered_set<int32_t> validatedTopics_;

    // E5.1.3: desired fragment poll limit for reservation-before-poll (see
    // ingressPollBudget_).  Source: NodeConfig::gatewayFragmentLimit
    // (RUNE_GATEWAY_FRAGMENT_LIMIT, default 256), clamped to kMaxFragmentLimit
    // (F15 §6 bound).  Polling runs only while the return FIFO is empty (G-A6).
    int32_t desiredFragmentLimit_ = 256;

    // Expected ProduceResult return size (bytes) for the return-ring reservation.
    // Configurable (RUNE_WORKER_ACK_SLOT_BYTES); see setAckSlotBytes.  256 comfortably
    // covers the ~160 B framed ProduceResult.
    int32_t ackSlotBytes_ = 256;
    // F3 round 6, item 2: worker-thread-owned reject counters (per-worker ingress).
    uint64_t    oversizeProduceRejects_    = 0;
    uint64_t    identityMismatchRejects_   = 0;   // frame identity != batch header (EOS-DUP defense)
    uint64_t    oversizeProduceRejectLogs_ = 0;
    // Per-tick drain batch limits (NodeConfig; raised from the OneToOneByteRing 16
    // default so routed replication ops / acks don't queue behind a tiny cap).
    int32_t ingressDrainLimit_ = 1024;  // RUNE_WORKER_INGRESS_DRAIN_LIMIT
    uint64_t    oversizedOpsRefused_  = 0;                    // F18: ops > one ring frame
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
    // owned-log sync interval (every configuration, F30) re-requests it
    // (LEO > lastSyncedLeo); a truncation clamps lastSyncedLeo / bgLeo to the
    // log's truncation low-water (reconcileTruncation_).
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
        /// F15 S5 fix round 2 (M-6): the ONE durable fsync in flight on this
        /// log's active segment (kNoToken = none).  At most one fsync — durable
        /// or background — is in flight per log, so a writeback error (errseq:
        /// reported once per open file) can never be consumed by one fsync while
        /// a concurrent one on the same file succeeds and acks OK.
        CompletionToken durableToken    = kNoToken;
        int64_t         durableLeo      = 0;          ///< LEO covered by durableToken
        /// F15 S5 fix round 3 (I-3): Strict waiters on this log are deferred
        /// behind the in-flight fsync.  While set, NO background fsync may be
        /// submitted on the log — the next fsync is the durable one, and it
        /// serves any background request (bgAgain) too.
        bool            strictWaiting   = false;
        int64_t         bgLeo           = 0;          ///< LEO covered by bgToken
        bool            bgAgain         = false;      ///< newer data requested meanwhile
        int64_t         lastSyncedLeo   = 0;
        uint32_t        sealsInFlight   = 0;
        bool            failureReported = false;
        uint64_t        sealDropsSeen   = 0;          ///< Log::droppedPendingSeals() last seen
        bool            rpQueued        = false;      ///< E1d: in rpQueue_ (F8)
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

    enum class RouteKind : uint8_t { Durable, Background, Seal, Hint, RecoveryPoint, DirSync, EpochState,
                                     MemEpochState };   ///< D15 m5: a MEMORY replica's epoch-state write (no Log)
    struct TokenRoute {
        CompletionToken            token = kNoToken;   ///< kNoToken = empty bucket
        rune::storage::Log*        log   = nullptr;
        rune::storage::LogSegment* seg   = nullptr;     ///< pinned segment (unpin on completion)
        uint32_t                   epoch = 0;
        RouteKind                  kind  = RouteKind::Durable;
        /// Durable / Background: the LEO at submit (the flush covers every
        /// offset below it in `seg`); RecoveryPoint: the checkpoint sequence;
        /// EpochState (hot-path B F7): the leader-epoch-state write sequence.
        int64_t                    aux   = 0;
        /// EpochState / MemEpochState: the replica (identity only — touched on
        /// completion only while still owned here) and its registry
        /// incarnation: a replica deleted and replaced within one fsync (same
        /// Log, or the same address) never passes as the owner of the old
        /// one's completion (coordinator must-fix: durability-before-acting).
        rune::replication::PartitionReplica* replica = nullptr;
        uint32_t                             incarnation = 0;
        /// EpochState / MemEpochState: the dup'd fd the fsync runs on — closed
        /// by this worker when the completion arrives (whatever its outcome),
        /// so the replica may close its own fd at any time: an fd number is
        /// never closed (and reused) under an in-flight fsync.
        int                                  syncFd = -1;
    };
    /// The replica a routed epoch-state completion belongs to, if it is still
    /// that replica (same object AND incarnation) — else nullptr (dropped).
    [[nodiscard]] static bool sameReplica_(const TokenRoute& r,
                                           const rune::replication::PartitionReplica* live) noexcept {
        return live != nullptr && live == r.replica && live->incarnation() == r.incarnation;
    }
    std::vector<TokenRoute> routes_;
    std::size_t             routeMask_ = 0;

    std::size_t bgInFlight_     = 0;
    uint64_t    bgFlushSubmits_ = 0;
    uint64_t    bgFlushErrors_  = 0;
    uint64_t    bgFlushDeferred_ = 0;
    uint64_t    sealSubmits_    = 0;
    uint64_t    dirSyncSubmits_ = 0;   ///< E1d MEDIUM-2: batched directory fsyncs submitted
    uint64_t    rpSubmits_      = 0;   ///< E1d recovery-point checkpoint write+fdatasyncs submitted
    uint64_t    epochStateSubmits_ = 0;   ///< hot-path B F7: leader-epoch-state syncs submitted
    CompletionToken lastEpochStateToken_ = kNoToken;   ///< tests (lastEpochStateTokenForTest)
    int             lastEpochStateSyncFd_ = -1;        ///< tests (lastEpochStateSyncFdForTest)
    uint64_t    epochStateNotRouted_ = 0; ///< review Md3
    int         epochStateSyncFdsOpen_ = 0;   ///< D23: open epoch-state fsync dups (tests)
    int         forceRouteFailForTest_ = 0;
    std::vector<uint8_t> esScratch_ = std::vector<uint8_t>(4096);   ///< review Md4: one image buffer per worker
    std::vector<rune::storage::Log*> rpQueue_;   ///< E1d F8: logs whose checkpoint is due
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
    // ---- Dropped topic ids (R-M1): worker-thread only.  One bit per topic id (ids are never
    // reused); grows only inside the cold TopicDropped op / the boot setter.
    std::vector<uint64_t> droppedTopicBits_;
    void markTopicDropped_(int32_t topicId) {
        if (topicId < 0) return;
        const std::size_t w = static_cast<std::size_t>(topicId) >> 6;
        if (w >= droppedTopicBits_.size()) droppedTopicBits_.resize(w + 1, 0);
        droppedTopicBits_[w] |= (1ULL << (static_cast<unsigned>(topicId) & 63u));
    }
    [[nodiscard]] bool topicDropped_(int32_t topicId) const noexcept {
        if (topicId < 0) return false;
        const std::size_t w = static_cast<std::size_t>(topicId) >> 6;
        return w < droppedTopicBits_.size() && (droppedTopicBits_[w] >> (static_cast<unsigned>(topicId) & 63u) & 1ULL) != 0;
    }
    std::vector<rune::replication::PartitionReplica*> ownedPtr_;   // ownedPtr_[id] valid iff ownedInc_[id] != 0
    // ---- (topicId, partition) -> owned replica index (R-I1): worker-thread only,
    // rebuilt cold with ownedInc_/ownedPtr_; open addressing, linear probe, power-of-two.
    struct TpSlot { uint64_t key; rune::replication::PartitionReplica* r; };
    static constexpr uint64_t kTpEmpty = ~0ULL;   // (topic -1, partition -1): never a real key
    std::vector<TpSlot> tpIndex_;
    uint64_t            tpMask_ = 0;
    [[nodiscard]] static uint64_t tpKey_(int32_t t, int32_t p) noexcept {
        return (uint64_t(uint32_t(t)) << 32) | uint32_t(p);
    }
    [[nodiscard]] static uint64_t tpHash_(uint64_t k) noexcept {   // splitmix64 finaliser
        k ^= k >> 30; k *= 0xbf58476d1ce4e5b9ULL; k ^= k >> 27; k *= 0x94d049bb133111ebULL; return k ^ (k >> 31);
    }
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
        case WorkerOp::Type::AlterIsrResponse:
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
        Worker*             w         = nullptr;
        rune::storage::Log* log       = nullptr;
        uint64_t            submitted = 0;   ///< async hints submitted for this partition
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
    // skipped this pass (RcHint::Skipped: not counted as issued, no re-request
    // backoff consumed — r3-2).  Otherwise a synchronous MADV_WILLNEED (counted).
    // m8: a hint does not pin its segment (fsyncs do): io-wq resolves the fd
    // when it runs, so a hint on a closed-and-reused fd advises an unrelated
    // file — benign for WILLNEED (a wasted read-ahead, never a correctness
    // effect).
    static RcHint rcIssueHint_(void* c, const RcReadAheadReq& req) {
        auto* h = static_cast<RcHintCtx*>(c);
        Worker& w = *h->w;
        if ((w.io_.asyncHints() || w.rcFakeAsyncHints_) && !w.rcAsyncHintsDisabled_ && req.fd >= 0 &&
            h->log != nullptr) [[likely]] {
            if (w.rcHintsInFlight_ >= kMaxRcHintsInFlight) { ++w.rcHintsSkipped_; return RcHint::Skipped; }   // m1
            const auto len = static_cast<uint32_t>(std::min<std::size_t>(req.len, UINT32_MAX));
            const CompletionToken t = w.rcFakeAsyncHints_ ? ++w.rcFakeHintSeq_
                                                          : w.io_.submitWillNeed(req.fd, req.fileOff, len);
            if (t == kNoToken) { ++w.rcHintsSkipped_; return RcHint::Skipped; }
            if (!w.routeInsert_(TokenRoute{t, h->log, nullptr, 0, RouteKind::Hint}))
                spdlog::critical("Worker {}: io token route table full (hint token={})", w.workerId_, t);
            ++w.rcHintsSubmitted_;
            ++w.rcHintsInFlight_;
            ++h->submitted;
            w.lastRcHintToken_ = t;
            return RcHint::InFlight;
        }
        ++w.rcHintsSync_;
        if (w.rcSyncHintFn_ != nullptr) w.rcSyncHintFn_(w.rcSyncHintCtx_, req);
        else                            RcState::syncWillNeed(req);
        return RcHint::Done;
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
    std::size_t           rcWalkStart_ = 0;   // r3-1: rcAdvanceAll_'s round-robin start slot
    std::size_t           ringCursor_  = 0;   // E2 residual: the next slot the shared ring budget starts at
    std::size_t           ringBudgetLeft_ = RcState::kDefaultScanBytesPerPass;   // E2 residual: ring-rebuild bytes left this pass
    std::size_t           ringSlots_   = 0;   // E2 residual N5: memory-backed rcSlots_ (set on ownership apply)
    uint64_t              ringWalks_   = 0;   // N5: passes that walked the slots for ring work (test seam)
    // Review I-2: read_committed PushReads held while their partition's RC
    // state builds — answered on Ready (or unanswered on ownership loss).
    // Each owes one return: counted in reservedAckSlots_(), capped, reserved.
    std::vector<PushReadArgs> heldPushReads_ = [] { std::vector<PushReadArgs> v; v.reserve(kMaxHeldPushReads); return v; }();
    // F5: PushReads deferred to the next pass (budget / ring) — a fixed FIFO.
    std::unique_ptr<PushReadArgs[]> deferredPushReads_ = std::make_unique<PushReadArgs[]>(kMaxHeldPushReads);
    std::size_t                     deferredPushHead_  = 0;
    std::size_t                     deferredPushCount_ = 0;
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
                a.logPtr  = nullptr;   // never read a log this worker no longer owns
                a.replica = nullptr;   // E2: nor its ring (a transient read finds it through the replica)
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
                if (!r->isDiskBacked()) slot.rc->reserveRingRebuild();   // E2: memory-backed only
                slot.rc->setResidencyProbe(rcProbeFn_, rcProbeCtx_);
                // F15 S5 (I3): the build rebuilds THIS worker's producer state.
                slot.rc->bindProducerState(&psm_, r->topicId(), static_cast<int32_t>(r->partitionId()));
            }
            installReadAhead_(slot);
            next.push_back(std::move(slot));
        }
        rcSlots_ = std::move(next);
        ringSlots_ = 0;   // E2 residual N5: memory-backed slots (0 -> no ring walk at all)
        for (const auto& sl : rcSlots_) ringSlots_ += sl.replica->isDiskBacked() ? 0u : 1u;
        rcById_.assign(ownedInc_.size(), nullptr);
        for (auto& sl : rcSlots_)
            if (sl.inc != 0 && sl.replica->registryId() < rcById_.size()) rcById_[sl.replica->registryId()] = sl.rc.get();
        for (auto& sub : subs_) sub.rc = rcOf_(subReplica_(sub));
    }
    [[nodiscard]] static RcFacts rcFacts_(const rune::replication::PartitionReplica* r) noexcept {
        RcFacts f;
        f.leader               = r->role() == rune::replication::Role::Leader;
        f.logGeneration        = r->logGeneration();
        f.leadershipGeneration = r->leadershipGeneration();
        f.epoch                = static_cast<int32_t>(r->epoch());
        return f;
    }
    // Task 15: drive the installed views over every owned LEADER source
    // partition whose committed range may have advanced.  Additive and
    // non-disruptive: runs right after rcAdvanceAll_(), only when a view set is
    // attached.  The read-committed bound is rc->lso(liveHw) — the SAME idiom
    // the push path uses (Worker.h ~2788-2793) — so a view never folds an
    // uncommitted (open-txn) batch.  Disk sources fold through readNoFault_ (one
    // pread, no page fault); TRANSIENT sources (Phase 2b E) fold through the
    // replica's InMemoryLog::readView (zero-copy, same thread) after
    // WorkerViewSet::driveView_ / reconcileTransient_.  Returns the REAL progress count
    // (records folded + checkpoint deltas pushed/held across every driven
    // view) so the node's idle strategy never backs off while view work — a
    // fold that advanced, or a back-pressured delta still awaiting ring space —
    // is pending.
    int driveViews_(int64_t nowNs) {
        int work = 0;
        for (auto* replica : owned_) {
            if (replica == nullptr || replica->topicId() < 0) continue;
            const int32_t topicId = replica->topicId();
            // Short-circuit: skip the rc/bound computation for an owned
            // partition no installed view is interested in.
            if (!viewSet_->interestedIn(topicId)) continue;
            if (replica->role() != rune::replication::Role::Leader) continue;
            const RcState* rcState = rcServe_(replica, rcOf_(replica));
            if (rcState == nullptr) continue;         // RC not Ready: fail closed, no bound
            const int64_t liveHw = replica->highWatermark();
            const int64_t bound  = rcState->lso(liveHw);
            const int32_t partition = static_cast<int32_t>(replica->partitionId());
            // N-C1: right after an election the HW floor can lag what the old leader had committed; a table is
            // Ready under this leadership only once HW has reached the leadership's start offset.
            const bool hwConfirmed = liveHw >= replica->leadershipStartOffset();
            if (replica->isDiskBacked()) {
                rune::storage::Log* log = replica->log();
                if (log == nullptr) continue;
                auto readRange = [this, log](int64_t from, int64_t to) -> rune::storage::LogReadView {
                    return readNoFault_(log, from, kPushMaxChunkBytes, to);
                };
                work += driveViewsOf_(topicId, partition, readRange, bound, nowNs, nullptr,
                                      replica->leadershipGeneration(), hwConfirmed);
            } else {
                // Phase 2b E: a TRANSIENT source — zero-copy view into the
                // replica's own ring (same thread).  The fold itself stops at its
                // passBound, so the ring's view needs no upper bound.
                rune::storage::InMemoryLog* mem = replica->memoryLog();
                if (mem == nullptr) continue;
                auto readRange = [mem](int64_t from, int64_t /*to*/) -> rune::storage::LogReadView {
                    return mem->readView(from, kPushMaxChunkBytes);
                };
                work += driveViewsOf_(topicId, partition, readRange, bound, nowNs, replica,
                                      replica->leadershipGeneration(), hwConfirmed);
            }
        }
        return work;
    }

    // One owned source partition's view work.  Order matters: materialize a
    // recovery instance from its checkpoint FIRST, then rebuild/fold.  For a
    // TRANSIENT source the per-instance cursor reconcile happens INSIDE the fold
    // (WorkerViewSet::driveView_) on the instance already held.  `transientRep`
    // is the owning replica of a TRANSIENT source (its ring's bounds + identity:
    // incarnation, logGeneration, last cut), nullptr for a disk source.
    // Task 19b (restart recovery): materializeRecoveryInstancesFor + driveRebuild
    // are gated on hasRecovery() — a broker with no durable views runs neither.
    template <class ReadRangeFn>
    int driveViewsOf_(int32_t topicId, int32_t partition, ReadRangeFn& readRange,
                      int64_t bound, int64_t nowNs,
                      const rune::replication::PartitionReplica* transientRep,
                      uint64_t leaderGen, bool leaderHwConfirmed) {
        int work = 0;
        TransientBounds tb;
        const TransientBounds* tbp = nullptr;
        if (transientRep != nullptr) {
            const rune::storage::InMemoryLog* mem = transientRep->memoryLog();
            tb.logStart      = mem->logStartOffset();
            tb.logEnd        = mem->logEndOffset();
            tb.incarnation   = transientRep->incarnation();     // E2: the ring's identity
            tb.logGeneration = transientRep->logGeneration();
            tb.cutOffset     = transientRep->lastTruncationOffset();
            tbp              = &tb;
        }
        if (viewSet_->hasRecovery()) [[unlikely]] {
            viewSet_->materializeRecoveryInstancesFor(topicId, partition, bound);
            work += viewSet_->driveRebuild(topicId, partition, readRange, bound, nowNs, tbp, leaderGen, leaderHwConfirmed);
        }
        work += viewSet_->driveFolds(topicId, partition, readRange, bound, nowNs, tbp, leaderGen, leaderHwConfirmed);
        return work;
    }

    // One §2.3 driver step for one partition.
    // D3b fix round 1 (cleaner invariant): the cleaner-visible LSO is 0 (clean
    // nothing) whenever the RC state is not Ready — it is published BEFORE any
    // rebuild step (revalidate may reset a Ready state in this very call, and the
    // inline produce gate rcGate_ steps here too) and the real value after.
    std::size_t rcAdvance_(rune::replication::PartitionReplica* r, RcState& rc) {
        if (!rc.revalidate(rcFacts_(r))) publishCleanerLso_(r, rc);   // 0 before scanning
        std::size_t n = 0;
        if (r->isDiskBacked()) {
            n = rc.advance(rcFacts_(r), r->log(), rcScanBytesPerPass_);
        } else {
            // E2 final fix wave (residual): a memory-backed rebuild step draws on
            // the pass's SHARED ring budget (at most ~kDefaultScanBytesPerPass of
            // ring rebuild per worker pass, however many partitions rebuild).
            // Spent: no step this pass (the state stays not Ready — reads held,
            // gated produce refused NotLeader, as before).
            const RcFacts f = rcFacts_(r);
            if (rc.needsRingWork(f)) {
                if (ringBudgetLeft_ > 0) {
                    n = rc.advance(f, nullptr, static_cast<int32_t>(ringBudgetLeft_), r->memoryLog());
                    ringBudgetLeft_ -= std::min(n, ringBudgetLeft_);
                }
            } else {
                n = rc.advance(f, nullptr, rcScanBytesPerPass_, r->memoryLog());   // Ready / follower: no ring work
            }
        }
        publishCleanerLso_(r, rc);
        return n;
    }
    // E2 final fix wave (residual): memory-backed and in need of ring-rebuild work.
    [[nodiscard]] bool ringWork_(rune::replication::PartitionReplica* r, const RcState& rc) const noexcept {
        return !r->isDiskBacked() && rc.needsRingWork(rcFacts_(r));
    }
    // D3b fix round 1: a cleaner swap (rewritten / emptied segment) is NOT applied
    // while this partition's RC state is SCANNING the log: the rebuild walks the
    // in-memory segments (the pre-swap inodes stay mapped), so it sees ONE
    // consistent version of the log — never a txn's data from before a cleaner
    // pass and the (removed) marker state from after it, which would leave the
    // txn open.  The swap stays queued (the cleaner retries a full ring) and is
    // applied once the state is Ready.  Together with "LSO 0 while not Ready"
    // (no NEW cleaner pass starts during a rebuild) a pass already in flight can
    // never land mid-rebuild.
    [[nodiscard]] bool cleanerSwapsDeferred_(const rune::replication::PartitionReplica* r) const noexcept {
        const RcState* rc = rcOf_(r);
        return rc != nullptr && rc->scanning();
    }
    // §2.4: the RC state to serve read_committed from — only if Ready AND still
    // valid for the replica's live role / generations (a demotion can land
    // between the per-pass driver and this read).  nullptr → serve nothing.
    [[nodiscard]] const RcState* rcServe_(rune::replication::PartitionReplica* r, RcState* rc) noexcept {
        if (rc == nullptr) return nullptr;
        if (!rc->revalidate(rcFacts_(r))) {
            publishCleanerLso_(r, *rc);   // D3b: a reset publishes 0
            return nullptr;
        }
        return rc;
    }
    // Per pass, every owned user partition (state-driven: no role-op hooks).
    // Fix round 4 (r3-1): the walk starts one slot later each pass (round-robin
    // cursor, no allocation), so the kMaxRcHintsInFlight hint slots rotate over
    // the building partitions — every one is first in line once per
    // rcSlots_.size() passes, not only the first ones in a fixed order.
    //
    // E2 final fix wave (residual): the memory-backed (ring) rebuilds share ONE
    // budget per pass (ringBudgetLeft_), handed out round-robin from a
    // persistent cursor (ringCursor_): the partition that spent the last of it
    // is followed by the next one on the following pass, so a rebuilding
    // partition waits at most K-1 passes for its slice (K = rebuilding
    // partitions) and none starves.  Disk partitions and ring partitions with
    // no rebuild work keep the per-slot walk.  No allocation.
    int rcAdvanceAll_() {
        const std::size_t n = rcSlots_.size();
        if (n == 0) return 0;
        if (rcWalkStart_ >= n) rcWalkStart_ = 0;
        int work = 0;
        for (std::size_t k = 0, i = rcWalkStart_; k < n; ++k) {
            RcSlot& sl = rcSlots_[i];
            if (!ringWork_(sl.replica, *sl.rc))
                work += rcAdvance_(sl.replica, *sl.rc) > 0 ? 1 : 0;   // publishes the cleaner LSO (D3b)
            if (++i == n) i = 0;
        }
        if (++rcWalkStart_ == n) rcWalkStart_ = 0;
        if (ringSlots_ == 0) return work;   // N5: a disk-only worker pays nothing for the ring budget
        ++ringWalks_;
        if (ringCursor_ >= n) ringCursor_ = 0;
        for (std::size_t k = 0, i = ringCursor_; k < n && ringBudgetLeft_ > 0; ++k) {
            RcSlot& sl = rcSlots_[i];
            const std::size_t next = i + 1 == n ? 0 : i + 1;
            if (ringWork_(sl.replica, *sl.rc)) {
                work += rcAdvance_(sl.replica, *sl.rc) > 0 ? 1 : 0;
                if (ringBudgetLeft_ == 0) ringCursor_ = next;   // the next pass starts after it
            }
            i = next;
        }
        return work;
    }
    // D3b: publish the partition's first unstable offset to the cleaner (only on
    // change).  Called on every RC transition: the per-pass driver (Ready / reset)
    // and right after each txn data batch / marker the RC state applies.
    static void publishCleanerLso_(rune::replication::PartitionReplica* r, RcState& rc) noexcept {
        int64_t v = 0;
        if (rc.takeCleanerLsoChange(v)) r->publishCleanerLso(v);
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
        return r == nullptr || r->acceptsProduce();   // DUR-1 B1: role AND completeness
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
        {   // (topicId, partition) index: load factor <= 0.5; capacity persists (no alloc unless it grows)
            std::size_t cap = 2;
            while (cap < 2 * owned_.size()) cap <<= 1;
            tpIndex_.assign(cap, TpSlot{kTpEmpty, nullptr});
            tpMask_ = cap - 1;
            for (auto* r : owned_) {
                if (r == nullptr) continue;
                const uint64_t k = tpKey_(r->topicId(), static_cast<int32_t>(r->partitionId()));
                for (uint64_t i = tpHash_(k) & tpMask_;; i = (i + 1) & tpMask_) {
                    if (tpIndex_[i].key == k) break;   // duplicate: keep the FIRST (linear-scan semantics)
                    if (tpIndex_[i].key == kTpEmpty) { tpIndex_[i] = TpSlot{k, r}; break; }
                }
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
            WorkerReturn ret = produceAnswer_(ProduceOrigin::GatewayOp, op.sessionId(), op.correlationId(),
                                              op.topicId(), op.partition());
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
            if (replica && !replica->isDiskBacked()) {
                // D15 m5: a memory replica's epoch-state write whose completion
                // is not routed here (a previous owner's): outcome unknown —
                // rewritten (no-op when nothing is in flight).
                bool memRouted = false;
                for (const TokenRoute& rt : routes_)
                    memRouted = memRouted || (rt.token != kNoToken && rt.kind == RouteKind::MemEpochState &&
                                              sameReplica_(rt, replica));
                if (!memRouted) replica->resetEpochStateForNewOwner();
                continue;
            }
            if (!replica) continue;
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
                // N-5 (cold): a log re-owned while an fsync this worker issued on it
                // is still in flight keeps its one-in-flight gate — seeded from the
                // route table (cleared by that completion through slotOf_(log)).
                // Cross-WORKER safety does not come from here: it rests on F7 (the
                // router is fixed for the pool's lifetime — a runtime move halts the
                // broker) and the two-phase migration's drain-before-grant contract
                // (WorkerPool.h), so another worker never fsyncs a log while this
                // worker's fsync on it is in flight.
                bool rpRouted = false, dirRouted = false, esRouted = false;
                std::vector<const rune::storage::LogSegment*> sealsRouted;   // cold (ownership change)
                for (const TokenRoute& rt : routes_) {
                    if (rt.token == kNoToken || rt.log != log) continue;
                    if (rt.kind == RouteKind::Durable)    fresh.durableToken = rt.token;
                    if (rt.kind == RouteKind::Background) fresh.bgToken      = rt.token;
                    if (rt.kind == RouteKind::RecoveryPoint) rpRouted = true;
                    if (rt.kind == RouteKind::DirSync) dirRouted = true;
                    if (rt.kind == RouteKind::EpochState && sameReplica_(rt, replica)) esRouted = true;
                    if (rt.kind == RouteKind::Seal) sealsRouted.push_back(rt.seg);
                }
                // E1d F7 (cold): a checkpoint / seal whose completion went to a
                // PREVIOUS owner (unobserved) must not pin R forever: replay —
                // the checkpoint stage resets (outcome unknown: rewritten with a
                // higher seq), every not-yet-durable sealed segment is re-sealed.
                if (!rpRouted) log->resetRecoveryCheckpointForNewOwner();
                if (!dirRouted) log->resetDirectorySyncForNewOwner();   // MEDIUM-2 (F7 parity)
                if (!esRouted) replica->resetEpochStateForNewOwner();   // hot-path B F7 (same rule)
                // MEDIUM-1: ALWAYS replay — skip only the segments whose seal is
                // routed here (their completion still arrives on this worker).
                (void)log->requeueUndurableSeals(sealsRouted.data(), sealsRouted.size());
                // LOW-B: the routed seals still gate Strict acks on this log.
                fresh.sealsInFlight = static_cast<uint32_t>(sealsRouted.size());
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
        (void)pendingAcksAll_.sweep([&](PendingAcksAll& e) {
            if (stillOwned(e)) return false;
            e.ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
            enqueueReturn_(e.ret);
            return true;
        });
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
        // E1d F8: the due-checkpoint queue holds only owned logs; capacity for
        // every slot (queueRecoveryCheckpoint_ never allocates).
        rpQueue_.clear();
        rpQueue_.reserve(slots_.size());
        for (auto& sl : slots_) {
            sl.rpQueued = false;
            if (sl.log->recoveryCheckpointDue() || sl.log->directorySyncDue()) { sl.rpQueued = true; rpQueue_.push_back(sl.log); }
        }
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
    /// D23: a route slot is free (cold — a scan of <= 2x the io depth).
    [[nodiscard]] bool routeHasRoom_() const noexcept {
        for (const TokenRoute& r : routes_) if (r.token == kNoToken) return true;
        return false;
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
        const int64_t boundary = log->flushBoundary();
        const CompletionToken t = io_.submit(seg->logFd(), boundary);
        if (t == kNoToken) { if (pin) log->unpinSegmentFlush(seg); return kNoToken; }
        if (!pin) seg = nullptr;
        if (!routeInsert_(TokenRoute{t, log, seg, epoch, kind, boundary})) {
            spdlog::critical("Worker {}: io token route table full (token={})", workerId_, t);
        }
        return t;
    }

    // F30 fix round 1 (F9): the ONE background disk-work step, shared by the
    // shard and thread-placement passes so they cannot drift.  Per owned log:
    // reconcile a truncation first (clamps lastSyncedLeo / bgLeo, so neither the
    // timed sync nor a coalesced re-request skips re-appended bytes), then submit
    // queued roll seals; then each replica's follower flush request.  Every
    // disk call is an io_ submission (spec §3.1).  Allocation-free.
    int submitBackgroundDiskWork_() {
        int work = 0;
        for (auto& slot : slots_) {
            reconcileTruncation_(slot);
            // Hot-path B F7: a truncation armed an async recovery-point lowering.
            if (slot.log->takeCheckpointKick()) [[unlikely]] queueRecoveryCheckpoint_(slot);
            if (slot.log->pendingSealCount() > 0) work += submitPendingSeals_(slot);
        }
        work += submitRecoveryCheckpoints_();
        for (auto* replica : owned_) {
            if (replica && replica->epochStateWriteDue()) [[unlikely]] work += submitEpochStateWrite_(*replica);
            if (replica && replica->takeFollowerFlushRequest() && replica->isDiskBacked()) {
                if (LogFlushSlot* slot = slotOf_(replica->log())) work += requestBgFlush_(*slot) ? 1 : 0;
            }
        }
        return work;
    }

    // Hot-path B F7: a replica's leader-epoch durability point (restart fence /
    // epoch-cache image) — never an fsync on this loop.  The slot image is
    // written into the pre-sized leader-epoch-state file's page cache (one
    // small pwrite inside the file's size — a sparse slot's blocks are
    // allocated at writeback, by the io backend's fdatasync; the write+sync
    // request carries at most kWriteSyncMaxBlock = 64 bytes) and its fdatasync
    // goes to the io backend; the completion confirms it (and may complete a
    // deferred leadership).  Cold: role changes / epoch transitions only.
    [[gnu::cold]] int submitEpochStateWrite_(rune::replication::PartitionReplica& r) noexcept {
        // D15 m5: a memory replica's fence goes through the same async writer
        // (its leader-epoch-state lives in its own dir; no Log / flush slot).
        const bool disk = r.isDiskBacked();
        LogFlushSlot* slot = disk ? slotOf_(r.log()) : nullptr;
        if (disk && slot == nullptr) return 0;             // not (yet) owned here: next pass
        rune::replication::PartitionReplica::EpochStateWrite w;
        std::size_t need = 0;
        if (!r.prepareEpochStateWrite(esScratch_.data(), esScratch_.size(), w, need)) {
            if (need <= esScratch_.size()) return 0;
            esScratch_.resize(need);                       // cold: the history outgrew the scratch
            if (!r.prepareEpochStateWrite(esScratch_.data(), esScratch_.size(), w, need)) return 0;
        }
        // D23 (D21 review L1a): its completion must be routable, so the room
        // is checked BEFORE the slot write and its fsync (cold: a scan of the route
        // table) — a full table refuses the write (rewritten next pass), never
        // an unobservable completion whose dup would be left open.
        if (forceRouteFailForTest_ > 0 || !routeHasRoom_()) [[unlikely]] {
            if (forceRouteFailForTest_ > 0) --forceRouteFailForTest_;
            r.epochStateWriteNotSubmitted();   // Prepared -> Idle, still due
            ++epochStateNotRouted_;
            return 0;                          // back-pressure (like a refused submit)
        }
        // The image is copied into the page cache here: the worker's scratch is
        // free again when pwrite returns (review Md4: no per-replica image).
        if (::pwrite(w.fd, w.data, w.len, static_cast<off_t>(w.offset)) != static_cast<ssize_t>(w.len)) {
            const int err = errno != 0 ? errno : ENOSPC;
            r.epochStateWriteSubmitted();
            (void)r.epochStateWriteCompleted(w.seq, err);   // the failure path (retry / fail-stop)
            ++epochStateSubmits_;
            return 1;
        }
        // The fsync runs on a DUP owned by its route (closed on completion):
        // the replica may close / reopen its own fd at any time — an fd number
        // is never closed under an in-flight fsync (and then reused).
        const int syncFd = ::fcntl(w.fd, F_DUPFD_CLOEXEC, 0);
        if (syncFd >= 0) ++epochStateSyncFdsOpen_;
        if (syncFd < 0) {
            const int err = errno != 0 ? errno : EMFILE;
            r.epochStateWriteSubmitted();
            (void)r.epochStateWriteCompleted(w.seq, err);   // the failure path (retry / fail-stop)
            ++epochStateSubmits_;
            return 1;
        }
        const CompletionToken t = io_.submit(syncFd, 0);
        if (t == kNoToken) {                               // back-pressure: retry next pass
            ::close(syncFd);                               // never submitted
            --epochStateSyncFdsOpen_;
            r.epochStateWriteNotSubmitted();
            return 0;
        }
        r.epochStateWriteSubmitted();
        lastEpochStateToken_ = t;
        lastEpochStateSyncFd_ = syncFd;
        TokenRoute route;
        route.token = t;
        route.aux   = static_cast<int64_t>(w.seq);
        route.replica = &r;
        route.incarnation = r.incarnation();
        route.syncFd = syncFd;
        if (disk) {
            route.log = r.log(); route.epoch = slot->epoch; route.kind = RouteKind::EpochState;
        } else {
            route.kind = RouteKind::MemEpochState;
        }
        // Cannot fail: the room was checked above and nothing ran in between
        // on this thread (D23).  Defensive only — its dup is then left open
        // (never closed under the in-flight fsync) and the write is redone.
        if (!routeInsert_(route)) [[unlikely]] {
            spdlog::critical("Worker {}: io token route table full (token={}) — epoch-state write re-queued",
                             workerId_, t);
            r.epochStateWriteNotRouted();
            ++epochStateNotRouted_;
            return 1;
        }
        ++epochStateSubmits_;
        return 1;
    }

    // Async roll seals queued by the log (every worker-driven log — the replica
    // runs with setExternalFollowerFlush).  A refused submit leaves the seal
    // queued (still pinned) for the next pass.
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

    // E1d: recovery-point checkpoints, EVENT-DRIVEN (F8): a completion that
    // makes a log's checkpoint due queues that log once (rpQueued); this pass
    // touches only queued logs.  Each checkpoint is ONE io-backend request
    // (write the alternate pre-allocated slot + fdatasync): no syscall, no
    // allocation on the loop.  A refused submit stays queued (next pass).
    void queueRecoveryCheckpoint_(LogFlushSlot& slot) noexcept {
        if (slot.rpQueued) return;
        slot.rpQueued = true;
        rpQueue_.push_back(slot.log);   // capacity reserved = slots_.size() (no allocation)
    }
    int submitRecoveryCheckpoints_() noexcept {
        if (rpQueue_.empty()) [[likely]] return 0;
        int work = 0;
        std::size_t keep = 0;
        for (std::size_t i = 0; i < rpQueue_.size(); ++i) {
            rune::storage::Log* log = rpQueue_[i];
            LogFlushSlot* slot = slotOf_(log);
            if (slot == nullptr) continue;                     // purged: dropped
            // MEDIUM-2: a blocked R first needs ONE batched directory fsync
            // (io backend, full fsync of the dir fd opened at recovery).
            bool dirRefused = false;
            rune::storage::Log::DirSync ds;
            if (log->prepareDirectorySync(ds)) {
                const CompletionToken dt = io_.submitFsync(ds.fd);
                if (dt == kNoToken) {
                    log->directorySyncNotSubmitted();
                    dirRefused = true;
                } else {
                    if (!routeInsert_(TokenRoute{dt, log, nullptr, slot->epoch, RouteKind::DirSync,
                                                 static_cast<int64_t>(ds.gen)})) {
                        spdlog::critical("Worker {}: io token route table full (token={})", workerId_, dt);
                    }
                    ++dirSyncSubmits_;
                    ++work;
                }
            }
            rune::storage::Log::RpWrite w;
            if (!log->prepareRecoveryCheckpoint(w)) {
                if (dirRefused) { rpQueue_[keep++] = log; continue; }   // back-pressure: retry next pass
                slot->rpQueued = false;
                continue;
            }
            const CompletionToken t = io_.submitWriteSync(w.fd, w.data, w.len, w.offset);
            if (t == kNoToken) {                               // back-pressure: retry next pass
                log->recoveryCheckpointNotSubmitted();
                rpQueue_[keep++] = log;
                continue;
            }
            log->recoveryCheckpointSubmitted();
            slot->rpQueued = false;
            if (!routeInsert_(TokenRoute{t, log, nullptr, slot->epoch, RouteKind::RecoveryPoint,
                                         static_cast<int64_t>(w.seq)})) {
                spdlog::critical("Worker {}: io token route table full (token={})", workerId_, t);
            }
            ++rpSubmits_;
            ++work;
        }
        rpQueue_.resize(keep);
        return work;
    }

    // Request a background flush of `slot`'s log.  Returns true iff submitted now.
    bool requestBgFlush_(LogFlushSlot& slot) noexcept {
        rune::storage::Log* log = slot.log;
        if (log->storageFailed()) return false;                   // fail-stop: never again
        // I-3: a Strict waiter is queued on this log — its durable fsync goes
        // next and covers this request; a background fsync submitted now
        // would defer the Strict waiter again (starvation when fsync >= the
        // sync interval).  Remembered in bgAgain, served by the durable.
        if (slot.strictWaiting) {
            if (log->logEndOffset() > slot.lastSyncedLeo) slot.bgAgain = true;
            return false;
        }
        if (slot.bgToken != kNoToken) {                           // coalesce
            if (log->logEndOffset() > slot.bgLeo) slot.bgAgain = true;
            return false;
        }
        // M-6: never a background fsync concurrent with the log's durable one
        // (one fsync in flight per log); re-requested when the durable completes.
        if (slot.durableToken != kNoToken) {
            if (log->logEndOffset() > slot.lastSyncedLeo) slot.bgAgain = true;
            return false;
        }
        const CompletionToken t = submitActive_(log, RouteKind::Background, slot.epoch);
        if (t == kNoToken) {
            // Back-pressure (io in-flight depth): no retry table, no per-pass
            // spin — the next owned-log sync interval (syncOwnedLogsAsync_) re-requests
            // it (LEO > lastSyncedLeo); with the interval 0 (default) it is not retried.
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

    // Group-commit sync interval (mirrors gateway's kGroupCommitIntervalNs = 10 ms).
    static constexpr int64_t kWorkerSyncIntervalNs = 10'000'000LL; // 10 ms (library default)
    int64_t syncIntervalNs_ = kWorkerSyncIntervalNs;   ///< owned-log background sync; 0 = off
public:
    /// Owned-log background sync interval (ns); 0 = never (durability via replication, Kafka model).
    /// Strict-tier acks keep their own coalesced fsync.  Before start.
    void setBackgroundSyncIntervalNs(int64_t ns) noexcept { syncIntervalNs_ = ns; }
    /// LAT-1 (f): CPUs for this worker's io_uring io-wq threads.  Stored here (control thread, before the
    /// runner starts); APPLIED on the worker thread at its first pass (io-wq is per task).
    void setIoWorkerCpus(std::vector<int32_t> cpus) { ioWorkerCpus_ = std::move(cpus); }
    /// Result of applying it on the worker thread: -1 not yet, 0 ok, else errno.  TEST-ONLY: written by
    /// the worker thread; read it only from the thread that drives doWork (tests drive it inline).
    [[nodiscard]] int ioWorkerCpusResult() const noexcept { return ioWorkerCpusResult_; }
    [[nodiscard]] const std::vector<int32_t>& ioWorkerCpus() const noexcept { return ioWorkerCpus_; }
private:

    // Time-gated owned-log sync (every mode, F30): same 10 ms gate and the same
    // "unchanged log = no work" rule, but each due log is an io_ submission.
    int syncOwnedLogsAsync_(int64_t nowNs) {
        if (syncIntervalNs_ <= 0) return 0;   // off: durability via replication (Kafka model)
        if (lastSyncNs_ == 0) { lastSyncNs_ = nowNs; return 0; }
        if (nowNs - lastSyncNs_ < syncIntervalNs_) return 0;
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
            // Strict acks=all: durable locally — now the HW gate.  (M4: for a
            // single-ISR Strict log, HW / consumer visibility can precede the
            // fsync — Kafka semantics; the PRODUCER ack never does.)
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
        (void)durablePending_.sweep([&](DurablePending& entry) {
            if (!entry.awaitSeal || entry.logPtr != slot.log) return false;
            fireDurable_(entry);
            return true;
        });
    }

    // ---- fsyncgate fail-stop -------------------------------------------------
    // L2 bypass (StorageFailure / OwnershipAck / EgressReleased): straight to ret_, else to
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
        (void)durablePending_.sweep([&](DurablePending& e) {
            if (e.logPtr != log) return false;
            e.ret.errorCode = code;
            enqueueReturn_(e.ret);
            return true;
        });
        if (!includeAcksAll) return;
        (void)pendingAcksAll_.sweep([&](PendingAcksAll& e) {
            auto* r = e.replica;
            if (!(r && r->isDiskBacked() && r->log() == log)) return false;   // log() is immutable: safe to read
            e.ret.errorCode = code;
            enqueueReturn_(e.ret);
            return true;
        });
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
    // F30 fix round 1: clamp to the LOWEST point truncated to since the last
    // pass (Log::takeTruncationLowWater), not the current LEO — a follower
    // truncates and re-appends in the same op, so the LEO can already be back
    // above the cut while the re-appended bytes are unflushed.
    void reconcileTruncation_(LogFlushSlot& slot) {
        const int64_t low = slot.log->takeTruncationLowWater();
        if (low != rune::storage::Log::kNoTruncation) [[unlikely]] {
            if (slot.lastSyncedLeo > low) slot.lastSyncedLeo = low;
            if (slot.bgLeo > low) slot.bgLeo = low;
        }
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

        // F15 S5: order-preserving removal — the answers of pipelined acks=all
        // batches of one partition leave in append order.
        return static_cast<int>(pendingAcksAll_.sweep([&](PendingAcksAll& entry) {
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
                return false;
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
            return true;
        }));
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
    [[nodiscard]] bool ackReservationAvailable_() const noexcept {
        return ret_.freeSlotsForMsgSize(ackSlotBytes_) >
               reservedAckSlots_() + static_cast<int64_t>(retFifo_.size()) &&
               owedCapAllows_();
    }
    // F15 S3 (addendum S3): the STATIC owed-return cap — the answer parked now
    // (+1) must stay within maxOwedReturns_, so pendingAcksAll_ /
    // durablePending_ never outgrow their reservation and the §6 FIFO bound
    // holds.
    [[nodiscard]] bool owedCapAllows_() const noexcept {
        return reservedAckSlots_() + 1 <= static_cast<int64_t>(maxOwedReturns_);
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
    // Worker-thread only.  O(1) via findOwnedReplica_ (index rebuilt cold on every
    // ownership change).  A TRANSIENT (InMemoryLog) replica returns {nullptr,
    // replica}: produceCore_ appends to replica->memoryLog() (Phase 2b E2).
    // Returns {nullptr, nullptr} if the partition is not owned by this worker.
    // -----------------------------------------------------------------------
    std::pair<rune::storage::Log*, rune::replication::PartitionReplica*>
    getLogAndReplicaForPartition_(int32_t topicId, int32_t partition) const noexcept {
        auto* r = findOwnedReplica_(topicId, partition);
        if (!r) return {nullptr, nullptr};
        return {r->isDiskBacked() ? r->log() : nullptr, r};
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
               static_cast<int64_t>(deferredPushCount_);        // F5: so do budget-deferred ones
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
        // Task 13: every polled frame answers into the session ack queue and
        // every owed waiter fires into it later — reserve for both (G-A6).
        const int64_t ackRoom   = ackSink_ ? std::max<int64_t>(0, ackRoom_()) : ringRoom;
        return std::min({static_cast<int64_t>(desiredFragmentLimit_), ringRoom, capRoom, ackRoom});
    }
    // Task 13 fix round 1 (C1b): free session-ack slots after reserving every
    // owed return (each fires into the ack queue later).  Every intake step
    // (op drain, client poll) is limited by it, so A + O <= kAckCapacity always.
    [[nodiscard]] int64_t ackRoom_() const noexcept {
        return static_cast<int64_t>(ackSink_->freeAckSlots()) - reservedAckSlots_();
    }

    int pollWorkerIngress_(int64_t nowNs) {
        if (!workerIngress_ || !workerIngress_->resolved()) return 0;

        const int32_t K = static_cast<int32_t>(ingressPollBudget_());
        if (K <= 0) [[unlikely]] {   // no room — skip poll (G-A6 preserved)
            if (reservedAckSlots_() >= static_cast<int64_t>(maxOwedReturns_)) noteOwedCapThrottle_("shard poll");
            return 0;
        }

        int polled = 0;
        if (ackSink_) {
            // Task 13 fix round 1 (C1c, spec G-A6): pause intake from THAT client
            // only.  A frame of a session still holding acks is ABORTed: its
            // image (one client publication) stops at that frame and the poll
            // moves on to the other images — Aeron's own per-publisher back-
            // pressure (§1a.3); the client's offers back-pressure until it reads
            // its acks.
            // D8d: the same ABORT when the session is at its direct budget, or the
            // worker's direct share is full / congested (stalled sessions first);
            // an admitted frame is counted until its ack leaves the sink.  One
            // hash probe per frame (admitDirect).
            workerIngress_->controlledPoll(
                K,
                [this, nowNs, &polled]
                (const uint8_t* data, std::size_t len, aeron::Header& /*hdr*/) {
                    uint8_t direct = SessionAckSink::kDirectRefused;   // < 16 B: undecodable, never answered
                    if (len >= kProduceSessionIdEnd) [[likely]] {
                        direct = ackSink_->admitDirect(produceSessionIdOf_(data));
                        if (direct == SessionAckSink::kDirectRefused) [[unlikely]] {
                            ++intakePausedFrames_;
                            return aeron::ControlledPollAction::ABORT;
                        }
                        // Review MEDIUM-2: a CLOSED session's buffered frame is consumed,
                        // never appended (the gateway's UnknownSession drop).
                        if (direct == SessionAckSink::kDirectDropClosed) [[unlikely]]
                            return aeron::ControlledPollAction::CONTINUE;
                    }
                    const int r = handleWorkerIngressFrame_(data, len, nowNs, direct);
                    // hotpath-A F2/F3: the partition's roll is deferred (no ready
                    // segment / seal queue full) — hold THIS frame in the image.
                    if (r < 0) [[unlikely]] return aeron::ControlledPollAction::ABORT;
                    polled += r;
                    return aeron::ControlledPollAction::CONTINUE;
                });
            return polled;
        }
        workerIngress_->controlledPoll(
            K,   // E5.1.3: poll up to K frames (reservation guarantees K ack slots)
            [this, nowNs, &polled]
            (const uint8_t* data, std::size_t len, aeron::Header& /*hdr*/) {
                const int r = handleWorkerIngressFrame_(data, len, nowNs);
                if (r < 0) [[unlikely]] return aeron::ControlledPollAction::ABORT;   // hotpath-A: held
                polled += r;
                return aeron::ControlledPollAction::CONTINUE;
            });
        return polled;
    }

    /// hotpath-A F2/F3 — returned by handleWorkerIngressFrame_ for a frame the
    /// worker HOLDS (controlled-poll ABORT: the frame stays in its image and is
    /// re-delivered on a later pass): its partition's roll is deferred because
    /// no pre-provisioned segment is ready or the async seal queue is full.
    /// Nothing was decoded into state, answered or appended.  Never blocks.
    static constexpr int kIngressHeld = -1;

    // Client MessageHeader: sessionId is LE64 at byte 8 (blockLength, templateId,
    // schemaId, version: 4 × u16) — read without decoding the whole frame.
    static constexpr std::size_t kProduceSessionIdEnd = 16;
    static_assert(rune::protocol::client::MessageHeader::ENCODED_SIZE >= kProduceSessionIdEnd);
    [[nodiscard]] static int64_t produceSessionIdOf_(const uint8_t* data) noexcept {
        uint64_t v = 0;
        for (int i = 7; i >= 0; --i) v = (v << 8) | data[8 + i];
        return static_cast<int64_t>(v);
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
    //   #3: acksAll derivation.  The Produce wire acks byte is two-valued:
    //       0 = Leader/None (immediate ack), 1 = All (deferred until the ISR HW);
    //       Produce::decode rejects any other value (no-hang wave), so acks != 0
    //       and the gateway's acks == 1 agree.
    // -----------------------------------------------------------------------
    // `directAck` (D8d): the admission (SessionAckSink::admitDirect) of this
    // frame — carried by its answer (exactly one: produceCore_ answers every
    // path once, now or parked) and given back here if it is undecodable.
    // Authentication Task 13: answer a shard-mode produce the session may not WRITE: ProduceResponse{NotAuthorized}
    // on its correlation id (the frame is consumed, never appended), counted, logged rate-limited.
    [[gnu::cold]] [[gnu::noinline]] int refuseNotAuthorized_(const rune::protocol::client::Produce& msg, uint8_t directAck) {
        if (workerNotAuthorized_++ % 1024 == 0)
            spdlog::warn("Worker {}: WorkerIngress: produce refused NotAuthorized (session={}, topic={}); total {}",
                         workerId_, msg.sessionId, msg.topicId, workerNotAuthorized_);
        WorkerReturn ret = produceAnswer_(ProduceOrigin::WorkerIngress, msg.sessionId, msg.correlationId,
                                          msg.topicId, msg.partition);
        ret.directAck = directAck;
        ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotAuthorized);
        enqueueReturn_(ret);
        return 1;
    }

    int handleWorkerIngressFrame_(const uint8_t* data, std::size_t len,
                                  int64_t nowNs, uint8_t directAck = 0) {
        RUNE_FR_SPAN_PRODUCE(rune_fr::Site::WorkerIngressRecv, 0);
        namespace pc = rune::protocol::client;

        // Task 22 (shard worker decrypts produce / per-worker ingress,
        // 2026-10-08): peek the CLEAR sessionId (the MessageHeader is always
        // plaintext — it forms part of the AEAD AAD) and look up this
        // session's c2b ingress ctx BEFORE touching the rest of the frame.
        // An entry here exists IFF the session is encrypted (see
        // executeSessionAnnounceWire_ / eraseSessionCrypto_: a plaintext
        // session never gets one, and SessionClose/re-announce-as-plaintext
        // erase it) — so a hit means "decrypt or drop", NEVER "maybe
        // plaintext". sessionCrypto_ is empty for an all-plaintext
        // deployment, so this is a cheap no-op scan for the common case:
        // the decode path below is byte-identical to pre-Task-22 when no
        // entry is found.
        const uint8_t* decData = data;
        std::size_t    decLen  = len;
        // Lows batch C (D8c R3): the seq of a decrypted frame is committed to
        // the replay window only once the frame is CONSUMED — a frame HELD
        // below (deferred roll: ABORT + redelivery) must re-open on redelivery.
        RecordsOpenCtx openCtx;
        uint64_t       openSeq = 0;
        if (!sessionCrypto_.empty() && len >= kProduceSessionIdEnd) [[unlikely]] {
            WorkerSessionCrypto* cr = findSessionCrypto_(produceSessionIdOf_(data));
            if (cr) [[unlikely]] {
                openCtx.recv       = cr->recv.has_value() ? &*cr->recv : nullptr;
                openCtx.recvSeq    = &cr->recvSeq;
                openCtx.recvWindow = &cr->recvWindow;
                // The reconstructed plaintext frame is always kCryptoOverhead
                // bytes SHORTER than the wire frame — sizing to `len` is
                // always enough. Grow-once (capacity persists across calls).
                if (workerIngressOpenScratch_.size() < len) workerIngressOpenScratch_.resize(len);
                const std::size_t n = openProduceFrameUncommitted(openCtx, data, len,
                                                                   workerIngressOpenScratch_.data(),
                                                                   workerIngressOpenScratch_.size(), openSeq);
                if (n == 0) [[unlikely]] {
                    // Fail-closed: no key yet / tamper / replay / too-old —
                    // NEVER decode the ciphertext as plaintext. Same drop
                    // contract as an undecodable plaintext frame below.
                    if (directAck != 0) ackSink_->releaseDirect(produceSessionIdOf_(data), directAck);
                    spdlog::warn("Worker {}: WorkerIngress: produce decrypt failed (session={}, "
                                 "len={}); dropping (fail-closed)",
                                 workerId_, produceSessionIdOf_(data), len);
                    return 0;
                }
                decData = workerIngressOpenScratch_.data();
                decLen  = n;
            }
        }

        // Decode the produce frame (plaintext path: decData/decLen ==
        // data/len, byte-identical to pre-Task-22; encrypted path: the
        // reconstructed [24B header][plaintext body] from above).
        auto msgOpt = pc::Produce::decode(decData, decLen);
        if (!msgOpt) [[unlikely]] {
            if (openSeq != 0) [[unlikely]] commitProduceSeq(openCtx, openSeq);   // consumed (answered)
            // No-hang (R2): a frame whose MessageHeader decodes is ANSWERED
            // ProduceResponse{InvalidRequest} on its correlationId (the answer
            // carries this frame's direct-budget unit, like every produce
            // answer) — never dropped while the client waits out its timeout.
            if (const auto hdr = pc::MessageHeader::decode(decData, decLen)) {
                if (undecodableProduceAnswers_++ % 1024 == 0)
                    spdlog::warn("Worker {}: WorkerIngress: Produce body undecodable (len={}, session={}, "
                                 "corr={}); answered InvalidRequest (total {})", workerId_, decLen,
                                 hdr->sessionId, hdr->correlationId, undecodableProduceAnswers_);
                WorkerReturn ret = produceAnswer_(ProduceOrigin::WorkerIngress, hdr->sessionId,
                                                  hdr->correlationId, /*topicId=*/-1, /*partition=*/-1);
                ret.directAck = directAck;
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
                enqueueReturn_(ret);
                return 1;
            }
            if (directAck != 0) ackSink_->releaseDirect(produceSessionIdOf_(data), directAck);
            spdlog::warn("Worker {}: WorkerIngress: Produce frame header undecodable "
                         "len={}; dropping (unanswerable)", workerId_, decLen);
            return 0;
        }
        const pc::Produce& msg = *msgOpt;

        // Authentication Task 13 (spec §6, D30): the shard-mode WRITE check - ONE slot probe, ONE bounds check, ONE
        // bit load.  Workers without worker-side acks keep no session table (unit rigs) and enforce nothing.  A
        // session this worker has no slot for is denied (fail closed).
        if (ackSink_) {
            const WriteGrantSlot* grants = ackSink_->writeGrants(msg.sessionId);
            if (grants == nullptr || !allowsWrite(*grants, msg.topicId)) [[unlikely]] {
                // The login's grants are enqueued on the lifecycle ring BEFORE the client learns it may produce, but
                // this pass may have drained that ring before the enqueue: hold the frame one pass (the controlled
                // poll re-delivers it after the next drain) rather than answer a stale NotAuthorized.
                if (sessionRing_.producerPosition() > sessionRing_.consumerPosition()) [[unlikely]] {
                    if (directAck != 0) ackSink_->releaseDirect(produceSessionIdOf_(data), directAck);
                    ++intakeGrantHolds_;
                    return kIngressHeld;                             // seq NOT committed: it re-opens
                }
                if (openSeq != 0) [[unlikely]] commitProduceSeq(openCtx, openSeq);   // consumed (answered)
                return refuseNotAuthorized_(msg, directAck);
            }
        }

        // Look up the owned Log + replica for this partition.
        auto [log, replica] = getLogAndReplicaForPartition_(msg.topicId, msg.partition);

        // hotpath-A F2/F3: back-pressure — the append would need a roll that
        // cannot happen without blocking this pass (no ready segment: never an
        // inline create; seal queue full: never an inline fsync).  Hold the
        // frame (the caller ABORTs the controlled poll); its admission unit is
        // given back.  O(1): one size compare (+ one acquire load when full).
        // Review m1: the hold is image-wide (a controlled-poll ABORT stops every
        // later frame of the same publisher on this worker until the roll can
        // happen — bounded by the provisioner's refill / a reported failure /
        // the seal-queue drain).  Frames that would be REFUSED anyway (not the
        // leader, oversize) are never held: they are answered at once.
        if (log != nullptr && log->rollDeferred() && produceRoleGatePasses_(replica) &&
            msg.recordBatch.size() <= maxIntakeBatchBytes()) [[unlikely]] {
            if (directAck != 0) ackSink_->releaseDirect(produceSessionIdOf_(data), directAck);
            ++intakeRollHolds_;
            return kIngressHeld;                                     // seq NOT committed: it re-opens
        }
        if (openSeq != 0) [[unlikely]] commitProduceSeq(openCtx, openSeq);   // consumed from here on

        // E5.1.3 review fix #3: acksAll = (acks != 0).
        // The Produce wire acks byte is 0 (Leader/None) or 1 (All); decode
        // rejects anything else (answered InvalidRequest above).
        ProduceFields f;
        f.sessionId     = msg.sessionId;
        f.correlationId = msg.correlationId;
        f.producerId    = msg.producerId;
        f.topicId       = msg.topicId;
        f.partition     = msg.partition;
        f.baseSequence  = msg.baseSequence;
        f.producerEpoch = msg.producerEpoch;
        f.durabilityTier = replica ? replica->durabilityTier() : uint8_t{1};
        f.acksAll       = (msg.acks != 0);
        f.fromOp        = false;   // the poll budget (ingressPollBudget_) reserved its answer
        f.directAck     = directAck;
        return produceCore_(f, log, replica, msg.recordBatch, nowNs);
    }

    // -----------------------------------------------------------------------
    // Task 13 (spec §3.4): THE produce path of a worker shard — worker-ingress
    // frames AND, with worker-side acks, gateway-received user produces
    // (executeProduceWire_).  The worker psm_ is the single idempotency owner
    // (validate → append → update in handleProduceBatch): the runtime no longer
    // consults its mirror for user partitions in shard mode.
    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // D8c fix round 1 (MEDIUM-1): THE constructor of every produce ANSWER shell —
    // the only place WorkerReturn::gatewayCredit is set.  Every answer of a
    // produce (produceCore_'s shell, which every refusal / parked waiter / HW
    // fire / timeout / §3.2 failure / fsync completion / purge copies, and
    // refuseNotOwned_) starts here, and the origin has no default, so a new
    // answer site cannot forget the runtime credit an op-ring produce holds
    // (returned when the sink CLAIMS the ack).  Direct worker-ingress produces
    // hold none.
    // -----------------------------------------------------------------------
    enum class ProduceOrigin : uint8_t { GatewayOp, WorkerIngress };
    [[nodiscard]] static WorkerReturn produceAnswer_(ProduceOrigin origin, int64_t sessionId, int64_t correlationId,
                                                     int32_t topicId, int32_t partition) noexcept {
        WorkerReturn ret;
        ret.type          = WorkerReturn::Type::ProduceResult;
        ret.sessionId     = sessionId;
        ret.correlationId = correlationId;
        ret.topicId       = topicId;
        ret.partition     = partition;
        ret.deferAck      = false;   // an answer, never facts-only (F15 S2 fix round 2, B1)
        ret.gatewayCredit = origin == ProduceOrigin::GatewayOp;
        return ret;
    }

    struct ProduceFields {
        int64_t sessionId     = 0;
        int64_t correlationId = 0;
        int64_t producerId    = -1;
        int32_t topicId       = 0;
        int32_t partition     = 0;
        int32_t baseSequence  = 0;
        int32_t minIsr        = 0;      ///< admission policy override (0 = the replica's)
        int16_t producerEpoch = 0;
        uint8_t durabilityTier = 1;     ///< 2 = Strict (ack after fsync)
        bool    acksAll       = false;
        bool    fromOp        = false;  ///< op-ring produce: reserve a parked answer here
        bool    crcValidated  = false;  ///< the op producer validated this batch's CRC (M-3)
        uint8_t directAck     = 0;      ///< D8d: direct-budget admission (worker ingress only)
        int64_t enqueuedAtNs  = 0;      ///< gateway enqueue time (op path; latency attribution)
    };

    int produceCore_(const ProduceFields& f, rune::storage::Log* log,
                     rune::replication::PartitionReplica* replica,
                     std::span<const uint8_t> rbSpan, int64_t nowNs) {
        namespace pc = rune::protocol::client;

        // Build a WorkerReturn shell for this ack.
        WorkerReturn ret = produceAnswer_(f.fromOp ? ProduceOrigin::GatewayOp : ProduceOrigin::WorkerIngress,
                                          f.sessionId, f.correlationId, f.topicId, f.partition);
        // D8d: every answer of a direct produce (refusal, append, parked waiter,
        // HW fire, timeout, §3.2 failure, fsync completion, purge) copies this
        // shell, so its direct-budget unit returns when THE answer leaves the sink.
        ret.directAck     = f.directAck;
        ret.producerId    = f.producerId;
        ret.producerEpoch = f.producerEpoch;
        ret.baseSequence  = f.baseSequence;
        ret.enqueuedAtNs  = f.enqueuedAtNs;
        ret.replica       = replica;
        if (replica) { ret.targetReplicaId = replica->registryId(); ret.incarnation = replica->incarnation(); }

        // Phase 2b E2: a TRANSIENT partition (owned replica, no disk Log) appends
        // to the replica's own InMemoryLog through this same produce core.
        rune::storage::InMemoryLog* const mem =
            (log == nullptr && replica != nullptr) ? replica->memoryLog() : nullptr;
        if (log == nullptr && mem == nullptr) {
            // Not owned by this worker (getLogAndReplicaForPartition_ → {nullptr,
            // nullptr}): retriable NotLeader so the client refreshes metadata and
            // re-routes.  Shared-nothing: no cross-worker forwarding per
            // [[rune-shared-nothing-law]].
            spdlog::debug("Worker {}: produce for a partition this worker does not own (topicId={} "
                          "partition={}); returning NotLeader (retriable)",
                          workerId_, f.topicId, f.partition);
            // R-M1: a topic the catalog dropped answers UnknownTopic, like the gateway.
            ret.errorCode = static_cast<int16_t>(topicDropped_(f.topicId) ? pc::ErrorCode::UnknownTopic
                                                                          : pc::ErrorCode::NotLeader);
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
        // F15 §2.3 produce gate (see executeProduceWire_) + PSM <-> log
        // consistency: an idempotent batch is validated only against a producer
        // state proven consistent with this log (see psm_).
        RcState* rc = nullptr;
        if (f.producerId >= 0) [[unlikely]] {
            if (replica != nullptr && f.topicId >= 0) {
                rc = rcGate_(replica);
                if (rc == nullptr) {
                    ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                    enqueueReturn_(ret);
                    return 1;
                }
            } else if (replica != nullptr) {
                // Internal partitions are written by coordinators with pid -1
                // only; no producer state is ever built for them.
                ret.errorCode = static_cast<int16_t>(pc::ErrorCode::Internal);
                enqueueReturn_(ret);
                return 1;
            } else {
                const PsmGate g = provePassthroughPsm_(f.topicId, f.partition, *log);
                if (g != PsmGate::Proven) [[unlikely]] {
                    // Unproven: never OOS ("NOT appended"), never an append —
                    // retriable while the budgeted build runs, outcome unknown
                    // when it failed.
                    ret.errorCode = static_cast<int16_t>(g == PsmGate::NotLeader ? pc::ErrorCode::NotLeader
                                                                                 : pc::ErrorCode::Internal);
                    enqueueReturn_(ret);
                    return 1;
                }
            }
        }

        // Zero-copy: handleProduceBatch now takes a READ-ONLY view and the log
        // restamps the base offset in place via pwritev — no mutable local copy.
        if (rbSpan.empty()) {
            spdlog::warn("Worker {}: WorkerIngress: empty recordBatch topicId={} "
                         "partition={}; answered InvalidRequest",
                         workerId_, f.topicId, f.partition);
            // Fix round 2: a malformed request is InvalidRequest (39), not Internal.
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
            enqueueReturn_(ret);
            return 1;
        }
        // F15 S3 (review I-1, re-review 1 I-3): MessageTooLarge (non-retriable,
        // never appended) above min(the gateway's max batch, what THIS
        // worker's return ring can carry in one PushReadResult) — so the Aeron
        // term size never raises the largest storable batch past the ring.
        // F18 (merged): the same LOUD intake guard as the gateway path — one limit
        // (maxAcceptableBatchBytes), so an accepted batch always fits one frame on
        // every hop (op/return ring, replication outbound ring + term, egress).
        // F3 round 5 C2 (same guard): the per-worker ingress path enforces the SAME
        // limit as the gateway — a UDP client's own term governs the image frame,
        // so nothing upstream bounds it; covers idempotent/transactional produce.
        if (rbSpan.size() > maxIntakeBatchBytes_) [[unlikely]] {
            // F3 round 6, item 2: rate-limited (first, then every 1024th) —
            // worker-thread-owned counters.
            if (oversizeProduceRejects_++ % 1024 == 0) {
                ++oversizeProduceRejectLogs_;
                spdlog::warn("Worker {}: WorkerIngress: produce batch too large ({} > {}) "
                             "topicId={} partition={}; rejected (MessageTooLarge, rejects={})",
                             workerId_, rbSpan.size(), maxIntakeBatchBytes_,
                             f.topicId, f.partition, oversizeProduceRejects_);
            }
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::MessageTooLarge);
            enqueueReturn_(ret);
            return 1;
        }

        // Task 7 schema validation — the SAME hook as executeProduceWire_ (fix on
        // sight, Task 13: the worker-ingress path skipped it, so a schema'd topic
        // accepted invalid records over shard ingress).
        if (schemaRejects_(f.topicId, rbSpan.data(), rbSpan.size())) [[unlikely]] {
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::SchemaValidationFailed);
            enqueueReturn_(ret);
            return 1;
        }

        const bool acksAll = f.acksAll;

        // ISR size for the min-ISR gate.
        const int isrSize = replica ? static_cast<int>(replica->isr().size()) : 0;
        const int minIsr  = f.minIsr > 0 ? f.minIsr : (replica ? replica->minIsr() : 1);

        // Durability tier — honor the topic/replica's configured tier so a
        // tier=strict (D2) topic actually acks after fdatasync.  Worker ingress:
        // the replica's tier (Majority when no replica is wired); op path: the
        // tier the gateway stamped from the same replica view (F15 S5).
        const uint8_t durabilityTier = f.durabilityTier;
        // Task 10 (spec §3.1) + F30 fix round 1 (F1/F2) + F15 S5: a worker never
        // fsyncs inline — in EVERY placement and on every intake path.  Strict
        // appends page-cache only and acks from its own io_ completion, gated on
        // the log's outstanding roll seals (onFlushCompletion_ / fireSealGated_),
        // then the HW gate for acks=all.  Only a disk-backed log has an fd to fsync.
        // Phase 2b E2: a transient ring has no fd — tier=strict acks as Majority.
        const bool asyncStrict = durabilityTier == 2 && log != nullptr && log->isDiskBacked();
        // The tier handleProduceBatch sees: never an inline sync for an op or an
        // async-Strict append.
        const uint8_t helperTier = (asyncStrict || f.fromOp) ? uint8_t{1} : durabilityTier;

        // Op-ring produce (gateway-received, Task 13): its parked answer was not
        // covered by a poll budget — reserve it here BEFORE the append (G-A6, the
        // F15 S2 R-4c rule); no slot → retriable NotLeader, log untouched.
        if (f.fromOp && (acksAll || asyncStrict) && !ackReservationAvailable_()) [[unlikely]] {
            ++acksAllRefusedNoReservation_;
            if (!owedCapAllows_()) noteOwedCapThrottle_("gateway-routed produce");
            if (acksAllRefusedNoReservation_ == 1 || (acksAllRefusedNoReservation_ & 1023u) == 0)
                spdlog::warn("Worker {}: parked (acks=all / Strict) produce refused NotLeader — return-ring reservation "
                             "exhausted (owed={}, total refused={})", workerId_, reservedAckSlots_(),
                             acksAllRefusedNoReservation_);
            // NotLeader: the only retriable "nothing was appended, retry" code
            // the client protocol has without a protocol change (review m11).
            ret.errorCode = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            enqueueReturn_(ret);
            return 1;
        }

        // Call the shared produce helper — over the disk Log, or (Phase 2b E2) a
        // TRANSIENT partition's ring: one memcpy into the ring, base offset +
        // leader epoch patched there; the ring never evicts at/above the HW
        // (prepareTransientAppend) — full → TransientLogFull (42, retriable),
        // nothing appended.
        // M-3: skip the repeat CRC pass ONLY when the op's producer says it
        // validated these exact bytes (the gateway, right after ensureValid).
        const bool crcPreverified = f.fromOp && f.crcValidated;
        rune::storage::ProduceOutcome outcome;
        if (log != nullptr) {
            outcome = rune::storage::handleProduceBatch(
                rbSpan.data(), rbSpan.size(), f.topicId, f.partition,
                f.producerId, f.producerEpoch, f.baseSequence,
                *log, psm_, acksAll, isrSize, minIsr, helperTier, crcPreverified);
        } else {
            replica->prepareTransientAppend();
            outcome = rune::storage::handleProduceBatch(
                rbSpan.data(), rbSpan.size(), f.topicId, f.partition,
                f.producerId, f.producerEpoch, f.baseSequence,
                *mem, psm_, acksAll, isrSize, minIsr, helperTier, crcPreverified);
            if (outcome.logFull) [[unlikely]] noteTransientFull_(f.topicId, f.partition, outcome.error);
        }

        // A duplicate answers its ORIGINAL base offset (-1 for a KNOWN-OLD
        // duplicate, EOS-OOS: OK, offset unknown — Kafka DUPLICATE_SEQUENCE_NUMBER)
        // and its answer waits on outcome.lastOffset: the original's last offset,
        // or for a known-old duplicate the last offset of the batch holding the
        // producer's lastSequence (all its records are at or below it).  Rule:
        // a known-old duplicate is answered once THAT batch meets the ack
        // condition (HW past it for acks=all, fsync for Strict) — never earlier.
        ret.baseOffset  = outcome.baseOffset;
        ret.lastOffset  = outcome.lastOffset;
        ret.recordCount = outcome.recordCount;
        ret.batchAttrs  = outcome.batchAttrs;

        if (outcome.identityMismatch) [[unlikely]] {
            // EOS-DUP defense: frame identity != CRC-covered batch header.
            // Rate-limited (first, then every 1024th) — worker-owned counter.
            if (identityMismatchRejects_++ % 1024 == 0)
                spdlog::warn("Worker {}: produce rejected — frame producer identity (pid={} epoch={} seq={}) "
                             "!= RecordBatch header; topicId={} partition={} (SchemaValidationFailed, rejects={})",
                             workerId_, f.producerId, f.producerEpoch, f.baseSequence,
                             f.topicId, f.partition, identityMismatchRejects_);
        }

        if (!outcome.isOK()) {
            ret.errorCode = static_cast<int16_t>(
                static_cast<int>(outcome.error));
            // D6: UnknownProducerId — the ProduceResult carries the partition's
            // log start in baseOffset (the WorkerReturn carrier does not grow,
            // F15 §7); every answer site maps it onto ProduceResponse.logStartOffset.
            if (outcome.error == pc::ErrorCode::UnknownProducerId) ret.baseOffset = outcome.logStartOffset;
            enqueueReturn_(ret);
            return 1;
        }

        // Waterfall: leader appended [baseOffset..lastOffset].  Keyed by lastOffset.
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplLeaderAppend,
            (static_cast<uint32_t>(f.partition) << 24) |
                (static_cast<uint32_t>(ret.lastOffset) & 0xFFFFFFu));   // a duplicate: its original range
        // GR-4 Mode A: advance the replica's LEO/HW after successful append.
        if (replica) {
            replica->onLeaderLogAppended();
        }

        // F15 §2.1 (C1): RC update in the same invocation as the append; a
        // duplicate retry was registered with its original.
        if (rc != nullptr && !outcome.isDuplicate && (outcome.batchAttrs & kTxnAttrBit) != 0) [[unlikely]] {
            rc->onTxnBatch(f.producerId, outcome.baseOffset);
            if (replica) publishCleanerLso_(replica, *rc);   // D3b: an open txn caps the cleaner
        }

        // F15 S5 (Kafka parity; re-review 1 I1 residual, re-review 2): a
        // DUPLICATE is answered only under its original batch's ack condition —
        // Strict: after an fsync covering it; acks=all: when the HW passes it
        // with the §3.2 re-validation — exactly like a fresh batch, never "OK at
        // once" for data that is not durable / not on the ISR yet.  (Kafka
        // DelayedProduce waits on the duplicate's original offsets too.)
        const int64_t lastOff = ret.lastOffset;

        if (asyncStrict) {
            // Park on the coalesced fsync (submitCoalescedFlushes_ stamps the token
            // this pass).  A duplicate retry is parked too: its original may still
            // be awaiting its own fsync, and a Strict ack is never sent before the
            // data it acknowledges is durable.
            ret.errorCode = 0;
            DurablePending dp;
            dp.token       = kNoToken;
            dp.lastOffset  = lastOff;
            dp.logPtr      = log;
            dp.ret         = std::move(ret);
            dp.thenAwaitHw = acksAll ? replica : nullptr;
            dp.deadlineNs  = nowNs + ackTimeoutNs_;
            dp.replica     = replica;
            dp.logGen      = replica ? replica->logGeneration() : 0;
            dp.minIsr      = minIsr;
            durablePending_.push_back(std::move(dp));
            if (std::find(dirtyThisCycle_.begin(), dirtyThisCycle_.end(), log) == dirtyThisCycle_.end())
                dirtyThisCycle_.push_back(log);
            return 1;
        }

        if (!acksAll) {
            // acks=leader (its original was answered on append — a duplicate's
            // condition holds already): ack immediately.
            ret.errorCode = 0;
            enqueueReturn_(ret);
            return 1;
        }

        // acks=all (fresh or duplicate): register a pending ack; fire when HW passes.
        // Timeout: nowNs + ackTimeoutNs_ (generous — accounts for fetch-handling
        // latency added by E5.1.2c, where fetch waits a worker cycle).
        // Keyed by (partition<<24)|offset (see ReplPendingAckFire).
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplPendingAckEnqueue,
            (static_cast<uint32_t>(ret.partition) << 24) |
                (static_cast<uint32_t>(lastOff) & 0xFFFFFFu));   // a duplicate: its ORIGINAL range
        PendingAcksAll pending;
        pending.replica    = replica;
        pending.lastOffset = lastOff;
        pending.deadlineNs = nowNs + ackTimeoutNs_;
        pending.logGen     = replica->logGeneration();
        pending.minIsr     = minIsr;
        pending.ret        = std::move(ret);
        pendingAcksAll_.push_back(std::move(pending));
        return 1;
    }

    // No-hang wave (R2): undecodable worker-ingress Produce frames answered
    // InvalidRequest (worker-thread owned; kept LAST so no hot member moves).
    uint64_t undecodableProduceAnswers_ = 0;
    int64_t     nextNoHangStatsLogNs_ = 0;   // fix round 4 (item 6): periodic no-hang line
    NoHangStats lastNoHangStats_{};
};

} // namespace rune::node
