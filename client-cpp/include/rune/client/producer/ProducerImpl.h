// include/rune/client/producer/ProducerImpl.h
//
// CB-2 — ProducerImpl: agent-side single-writer accumulator + ring handoff.
//
// This class is NOT part of the public API.  It is shared between Producer
// (app thread) and ClientAgent (agent thread) via a shared_ptr.
//
// App threads:
//   submitSendRecord() — lock-free ring write (the zero-alloc handoff).
//   submitFlush()      — lock-free ring write (zero bytes, flushFlag).
//   registerCallback() / registerFuture() — control-plane only (mutex-guarded).
//
// Agent thread (doWork / drainAndFlush):
//   drainRing()    — reads SEND_RECORD ring entries, appends to builders.
//   flushBatches() — encodes + offers batches that hit linger/size/flush triggers.
//   onProduceResponse() — resolves pending acks by correlationId.
//   NanoClock is sampled once per doWork cycle and passed in (never per-send).
//
// Ring message format (kMsgSendRecord payload):
//   [0..3]   topicId   : int32 LE
//   [4..7]   partition : int32 LE
//   [8]      keyLen    : uint8  (≤16)
//   [9..24]  keyBytes  : uint8[keyLen] (inline)
//   [25]     flags     : uint8  (bit0=wantAck)
//   [26..33] corrId    : int64 LE (only meaningful when wantAck)
//   [34..37] valueLen  : uint32 LE
//   [38..]   valueBytes
//
// SEND_FLUSH payload: zero bytes (kMsgFlush typeId).
//
// ProducerImpl owns its own ManyToOneRingBuffer (separate from the ClientAgent
// command ring) so SEND_RECORD messages are not confused with agent commands.

#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>

#include <rune/protocol/RecordBatch.h>

#include <concurrent/AtomicBuffer.h>
#include <rune/client/detail/M2ORingBuffer.h> // internal MPSC ring (controlledRead/ABORT, templated read)

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// Ring message type IDs used by the Producer (distinct from ClientAgent IDs).
// ---------------------------------------------------------------------------
inline constexpr int32_t kMsgSendRecord  = 1; // SEND_RECORD (producer-ring local ID)
inline constexpr int32_t kMsgFlush       = 2; // flush trigger (producer-ring local ID)
inline constexpr int32_t kMsgTier2Batch  = 3; // Tier-2 explicit batch token

// ---------------------------------------------------------------------------
// SEND_RECORD layout constants
// ---------------------------------------------------------------------------
inline constexpr int kRecTopicIdOff  = 0;   // int32 LE
inline constexpr int kRecPartOff     = 4;   // int32 LE
inline constexpr int kRecKeyLenOff   = 8;   // uint8
inline constexpr int kRecKeyBytesOff = 9;   // uint8[keyLen] (≤16)
inline constexpr int kRecFlagsOff    = 25;  // uint8 (bit0=wantAck)
inline constexpr int kRecCorrIdOff   = 26;  // int64 LE (only when wantAck)
inline constexpr int kRecValLenOff   = 34;  // uint32 LE
inline constexpr int kRecValBytesOff = 38;  // value bytes
inline constexpr int kRecHeaderSize  = 38;  // bytes before value

// kMsgTier2Batch token layout constants (21 bytes total)
// ---------------------------------------------------------------------------
// [0..3]   topicId     : int32 LE
// [4..7]   partition   : int32 LE
// [8]      flags       : uint8 (bit0=wantAck — always set for Tier-2)
// [9..16]  corrId      : int64 LE
// [17..20] recordCount : int32 LE (number of records in the pre-built batch)
inline constexpr int kT2TopicIdOff    = 0;   // int32 LE
inline constexpr int kT2PartOff       = 4;   // int32 LE
inline constexpr int kT2FlagsOff      = 8;   // uint8
inline constexpr int kT2CorrIdOff     = 9;   // int64 LE
inline constexpr int kT2RecCountOff   = 17;  // int32 LE
inline constexpr int kT2TokenSize     = 21;  // total bytes

// Flags bits
inline constexpr uint8_t kFlagWantAck = 0x01;

// Default ring capacity for a producer (per-producer, separate from agent ring).
// This is the app→agent handoff buffer (Kafka's buffer.memory analogue): sized to
// absorb produce bursts before Producer::send() back-pressures.  2 MiB holds
// several thousand in-flight SEND_RECORDs so a bursty app is not throttled by the
// agent's drain cadence.
inline constexpr int32_t kDefaultProducerRingCap = 32 * 1024 * 1024; // 32 MiB

// Per-partition ack-ring capacity (entries; MUST be a power of two).  Holds the
// corrIds of outstanding (sent, not-yet-acked) records for one partition so
// ProduceResponse can fan out baseOffset+i to each.  Sized to comfortably exceed
// the in-flight record count for a partition (maxInFlight batches × batch size);
// if it ever fills, drainRing force-flushes + stops draining so the producer ring
// applies backpressure (no corrId is ever dropped).  8 bytes/entry.
inline constexpr uint32_t kAckRingCapacity = 16384; // 128 KiB/partition

// ---------------------------------------------------------------------------
// Tier2Entry — pre-built RecordBatch bytes waiting for agent-thread processing.
//
// The app thread builds the batch bytes (without baseSequence — left 0) and
// stores them here.  The ring carries only a small token (kMsgTier2Batch);
// the agent pops from this queue in FIFO order when it processes the token,
// stamps the correct baseSequence from PartitionState, then encodes+sends.
// ---------------------------------------------------------------------------
struct Tier2Entry {
    int32_t topicId     = 0;
    int32_t partition   = 0;
    int64_t corrId      = 0;
    int32_t recordCount = 0;
    std::vector<uint8_t> batchBytes; // pre-built RecordBatch (baseSequence=0, patched by agent)
};

// ---------------------------------------------------------------------------
// PartitionState — per-(topicId,partition) accumulator (agent-thread only).
// ---------------------------------------------------------------------------
struct PartitionState {
    int32_t topicId        = 0;
    int32_t partition      = 0;

    /// E5.1: per-worker Aeron ingress stream id from metadata.
    /// 0 = legacy / not yet known — falls back to the default ingress stream.
    int32_t ingressStreamId = 0;

    // Idempotence sequence: monotonically increasing baseSequence.
    int32_t nextBaseSequence = 0;

    // In-flight count: batches sent but not yet acked.
    int32_t inFlight = 0;

    // RecordBatchBuilder (reused; reset after each flush).
    // Lazy-initialised on first append.
    std::unique_ptr<rune::protocol::RecordBatchBuilder> builder;
    int32_t builderBytes = 0; // approximate accumulated value bytes

    // Timestamp when first record was appended to the current batch (for linger).
    int64_t batchStartNs = 0; // 0 = empty

    // Wall-clock base timestamp of the current RecordBatchBuilder (wallNs() at
    // builder creation / last reset).  Used to compute per-record tsDelta for
    // the frame-cap pre-flight check (task 3).
    int64_t builderBaseWallNs = 0;

    // Force-flush flag (set by flush() call from app thread, read here).
    bool flushRequested = false;

    // Pending correlationId for the current batch (0 = acks::None or not set).
    // This is the WIRE corrId the Produce frame carries — the FIRST ack-wanting
    // record's corrId in the current batch.  The broker echoes it on the
    // ProduceResponse, which the client uses to look up the batch and fan out.
    int64_t pendingCorrId = 0;

    // Per-record ack fan-out, ZERO-ALLOC ring.  corrIds are NOT contiguous within
    // a partition (a producer spread across N partitions strides corrIds), so we
    // cannot use a (base,count) pair — we must remember each record's corrId.
    // ackRing is a per-partition ring of corrIds in batch-append order,
    // preallocated ONCE (never per-batch); each flushed batch owns the contiguous
    // slice [batchAckStart, batchAckStart+batchAckCount).  On ProduceResponse the
    // slice is walked: corrId = ackRing[(start+i) & ackMask] resolves at
    // baseOffset + i.  ackTail advances as batches ack (FIFO per partition), so
    // (ackHead - ackTail) is the outstanding count for overflow backpressure.
    std::vector<int64_t> ackRing;        // preallocated corrId ring (agent-thread only)
    uint32_t             ackMask       = 0; // ackRing.size() - 1 (size is pow2)
    uint64_t             ackHead       = 0; // next write index (monotonic)
    uint64_t             ackTail       = 0; // oldest unacked index (monotonic)
    uint64_t             batchAckStart = 0; // ackHead at start of current batch
    int32_t              batchAckCount = 0; // ack-wanting records in current batch

    // Tier-2 batches queued for this partition (agent-thread only, no mutex).
    // Populated by drainRing() when kMsgTier2Batch token is processed.
    // Drained by flushBatches() in FIFO order, after any pending Tier-1 batch.
    std::deque<Tier2Entry> pendingTier2Entries;

    PartitionState() = default;
    PartitionState(int32_t tid, int32_t part)
        : topicId(tid), partition(part) {}

    // Non-copyable (owns the builder).
    PartitionState(const PartitionState&) = delete;
    PartitionState& operator=(const PartitionState&) = delete;
    PartitionState(PartitionState&&) = default;
    PartitionState& operator=(PartitionState&&) = default;
};

// ---------------------------------------------------------------------------
// PendingAck — pending ack entry keyed by correlationId.
// ---------------------------------------------------------------------------
struct PendingAck {
    enum class Kind { Callback, Future };
    Kind kind = Kind::Callback;

    // Callback path.
    std::function<void(int64_t)> callback;

    // Future path.
    std::promise<int64_t> promise;

    // Deadline for timeout sweep (steady_clock).
    std::chrono::steady_clock::time_point deadline;
};

// ---------------------------------------------------------------------------
// ProducerImpl
// ---------------------------------------------------------------------------
class ProducerImpl {
public:
    /// Construct a ProducerImpl with its own ring buffer.
    /// ringCap: power-of-two usable capacity in bytes (default 256 KiB).
    explicit ProducerImpl(ProducerConfig cfg,
                          std::atomic<int64_t>& nextCorrId,
                          int32_t ringCap = kDefaultProducerRingCap);

    ~ProducerImpl() = default;

    // Non-copyable, non-movable (owns the ring buffer).
    ProducerImpl(const ProducerImpl&) = delete;
    ProducerImpl& operator=(const ProducerImpl&) = delete;
    ProducerImpl(ProducerImpl&&) = delete;
    ProducerImpl& operator=(ProducerImpl&&) = delete;

    // ---- App-thread API (zero-alloc void path) --------------------------------

    /// Write a SEND_RECORD into the ring.  Called from the app thread.
    /// NO heap allocation, NO lock.  Returns false if the ring is full.
    bool submitSendRecord(int32_t topicId,
                          int32_t partition,
                          const Key& key,
                          std::span<const uint8_t> value,
                          bool wantAck,
                          int64_t correlationId) noexcept;

    /// Write a flush trigger into the ring.  Zero-alloc.
    void submitFlush() noexcept;

    // ---- App-thread API (Tier-2 explicit batch) --------------------------------

    /// Submit a pre-built Tier-2 batch (from BatchBuilder::send()).
    ///
    /// Pushes the batch bytes to tier2Queue_ (mutex-guarded) and writes a
    /// kMsgTier2Batch token into the ring.  The agent thread processes the
    /// token in order with Tier-1 records, stamping baseSequence at that point.
    ///
    /// corrId: 0 = acks::None; non-zero = pending ack already registered.
    /// recordCount: number of records in the pre-built batch.
    /// batchBytes: pre-built RecordBatch bytes (baseSequence field = 0; will be
    ///             patched by the agent).
    ///
    /// Returns false if the ring is full (caller should handle as an error).
    bool submitTier2Batch(int32_t topicId,
                          int32_t partition,
                          int64_t corrId,
                          int32_t recordCount,
                          std::vector<uint8_t> batchBytes);

    // ---- App-thread API (opt-in ack — may allocate) --------------------------

    /// Allocate a correlationId + register a callback.  Returns the correlationId
    /// to stamp on the SEND_RECORD.
    int64_t registerCallback(std::function<void(int64_t)> cb);

    /// Allocate a correlationId + register a promise.  Returns (correlationId, future).
    std::pair<int64_t, std::future<int64_t>> registerFuture();

    // ---- Agent-thread API (single-writer) ------------------------------------

    /// Drain SEND_RECORD ring messages, append to builders.
    /// Does NOT flush — call flushBatches() after.  Applies a drain gate: if any
    /// partition is at batchBytes AND at maxInFlight, returns 0 without reading so
    /// the producer ring back-pressures the app (bounds every builder to a
    /// sendable size).
    /// nowNs: current time (NanoClock, sampled once per doWork cycle).
    /// Returns count of records drained.
    int drainRing(int64_t nowNs);

    /// Flush partition batches that are ready (linger/batchBytes/flush/flushFlag).
    /// Calls encodeAndSend() for each ready batch.
    /// sendFn: (ingressStreamId, buf, len) → bool (E5.1: per-worker routing).
    /// Returns count of batches flushed.
    int flushBatches(int64_t nowNs, int64_t sessionId,
                     std::function<bool(int32_t, const uint8_t*, std::size_t)> sendFn);

    /// Called by the agent when a ProduceResponse arrives.  Resolves the pending
    /// ack (callback/future) by correlationId.
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Called on agent close/stop: fail all pending acks.
    void failAllPending(const std::string& reason);

    /// Fail a single pending ack by correlationId (CB-5 A3: ring-full fast-fail).
    void failPending(int64_t corrId, const std::string& reason);

    /// CB-5 B: attempt to resend the wire frame for a redirected Produce.
    /// Returns true if resent, false if corrId unknown / max redirects exceeded.
    /// On failure the pending ack is failed immediately.
    static constexpr int kMaxRedirects = 3;
    bool retryRedirectedProduce(int64_t corrId,
                                std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn);

    /// CB-5 B: resend EVERY in-flight (still-unacked) Produce frame after a
    /// leader redirect + reconnect.  Each stored wire frame is re-stamped with
    /// the NEW sessionId (bytes [8..15] of the MessageHeader) so the leader —
    /// which just assigned a fresh session — accepts it instead of dropping it
    /// as unknown-session.  Bounded per-frame by kMaxRedirects; frames that
    /// exceed the budget are failed.  Returns the number of frames resent.
    int resendAllRedirected(int64_t newSessionId,
                            std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn);

    /// CB-5 B: true if there is at least one in-flight batch awaiting an ack
    /// (i.e. a redirect resend would have something to flush).
    [[nodiscard]] bool hasInFlight() const noexcept { return !batchMap_.empty(); }

    /// MD1 review IMPORTANT-1: look up the (topicId, partition) for an in-flight
    /// corrId.  Returns true and sets topicId+partition if found; false if the
    /// corrId is not in batchMap_.  Called on the agent thread from the redirect
    /// handler so the MetadataCache can be updated with the affected partition.
    [[nodiscard]] bool partitionForCorrId(int64_t corrId,
                                          int32_t& topicIdOut,
                                          int32_t& partitionOut) const noexcept {
        const auto it = batchMap_.find(corrId);
        if (it == batchMap_.end()) return false;
        topicIdOut   = it->second.topicId;
        partitionOut = it->second.partition;
        return true;
    }

    /// MD1 review IMPORTANT-2: return the (topicId, partition) of the first
    /// partition that currently has produce work — either accumulated in a
    /// PartitionState (records appended / Tier-2 queued) or already in-flight
    /// (batchMap_).  Returns true and sets the out params if any such partition
    /// exists; false otherwise.  Called on the agent thread so the produce path
    /// can resolve the partition leader's endpoint via the PartitionRouter and
    /// route the produce DIRECT to that leader using cached metadata.
    [[nodiscard]] bool firstPendingPartition(int32_t& topicIdOut,
                                             int32_t& partitionOut) const noexcept {
        // Prefer an accumulated (not-yet-sent) partition: routing it BEFORE the
        // frame goes out is the zero-extra-hop path.
        for (const auto& [key, ps] : partitions_) {
            const bool hasTier1 = (ps.batchStartNs != 0) || ps.flushRequested;
            const bool hasTier2 = !ps.pendingTier2Entries.empty();
            if (hasTier1 || hasTier2) {
                topicIdOut   = ps.topicId;
                partitionOut = ps.partition;
                return true;
            }
        }
        // Fall back to an in-flight batch (already sent, awaiting ack).
        if (!batchMap_.empty()) {
            const auto& bi = batchMap_.begin()->second;
            topicIdOut   = bi.topicId;
            partitionOut = bi.partition;
            return true;
        }
        return false;
    }

    /// E5.1: Update the per-worker ingressStreamId for a (topicId, partition).
    /// Called on the agent thread when a MetadataResponse is processed.
    /// Must be called BEFORE the next flushBatches() cycle so the routing is
    /// applied to the very next Produce frame for that partition.
    ///
    /// This touches PartitionState directly — same threading contract as
    /// drainRing/flushBatches (agent thread only, no lock needed).
    void updateIngressRoute(int32_t topicId, int32_t partition,
                            int32_t ingressStreamId) noexcept;

    /// Called from Client::sweepTimeouts(): fail acks older than the deadline.
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    // ---- Accessors -----------------------------------------------------------

    const ProducerConfig& config() const noexcept { return cfg_; }

    /// Total Aeron frames offered to transport (agent-thread only; plain uint64_t).
    uint64_t framesSent()  const noexcept { return framesSent_; }

    /// Total records accepted via drainRing (agent-thread only; plain uint64_t).
    uint64_t recordsSent() const noexcept { return recordsSent_; }

    // ---- Global flush flag (written by app thread, read by agent) -------------
    // Set by submitFlush(); cleared by flushBatches() after processing.
    std::atomic<bool> globalFlushPending_ {false};

private:
    // ---- Ring write helpers --------------------------------------------------

    bool writeToRing(int32_t msgTypeId,
                     const uint8_t* payload,
                     std::size_t len) noexcept;

    // ---- Agent-side helpers --------------------------------------------------

    PartitionState& getOrCreatePartition(int32_t topicId, int32_t partition);

    /// Double a partition's ack-ring, preserving live entries [ackTail, ackHead).
    /// Called only on overflow (deep single-partition pipeline); steady state
    /// never reallocs.
    void growAckRing(PartitionState& ps);

    bool encodeAndSend(PartitionState& ps,
                       int64_t nowNs,
                       int64_t sessionId,
                       int64_t correlationId,
                       std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn);

    /// Encode + send a Tier-2 pre-built batch (agent thread only).
    /// Patches baseSequence into the provided batch bytes, then encodes a
    /// Produce wire frame and offers it to transport.
    bool encodeAndSendTier2(PartitionState& ps,
                             Tier2Entry& entry,
                             int64_t sessionId,
                             std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn);

    // ---- Members -------------------------------------------------------------

    ProducerConfig cfg_;

    // M2: frame-cap values derived from cfg_ at construction time.
    // frameCapped_ = maxFrameBytes is a tighter bound than batchBytes.
    // effCap_ = the effective per-frame flush threshold.
    // Both are immutable after construction (cfg_ is immutable).
    bool    frameCapped_ = false;
    int32_t effCap_      = 0;

    // Shared correlationId counter (also used by Client for ping).
    std::atomic<int64_t>& nextCorrId_;

    // ---- Instrumentation counters (agent-thread only — plain uint64_t) -------
    // Single-writer (agent thread): no atomics needed per shared-nothing law.
    uint64_t framesSent_  = 0; // total Aeron frames offered to transport
    uint64_t recordsSent_ = 0; // total records drained from the ring

    // ---- Own ring buffer (MPSC: many app threads write, one agent thread reads)
    // ProducerImpl owns its ring so SEND_RECORD messages are separate from the
    // ClientAgent command ring (agent command type IDs 1-4 are unrelated).
    static constexpr std::size_t kRingAlignment = 64; // cache-line aligned
    std::unique_ptr<uint8_t, void(*)(void*)> ringBuf_;
    rune::client::detail::M2ORingBuffer      ring_;

    // ---- Agent-thread only (no lock needed — single-writer) ------------------

    // Per-((int64_t(topicId)<<32)|uint32_t(partition)) → PartitionState (collision-free).
    std::unordered_map<int64_t, PartitionState> partitions_;

    // Encode scratch buffers (reused per batch — zero per-flush heap allocation).
    // batchScratch_: RecordBatch bytes (cleared + refilled by buildInto()).
    // encodeScratch_: Produce wire frame (grown once; steady-state = no alloc).
    std::vector<uint8_t> batchScratch_;
    std::vector<uint8_t> encodeScratch_;

    // Stub producerId for M-B: local random-ish value.
    // TODO(milestone-c): real InitProducerId + zombie-fencing.
    // NOTE: every ProducerImpl instance (and every Client instance) shares
    // this same (producerId, epoch) pair.  On the broker, the idempotent-dedup
    // key is (producerId, partition, baseSequence).  Two concurrent producers
    // writing to the SAME partition with the same producerId WILL collide and
    // deduplicate incorrectly.  M-B assumes a single producer per broker
    // partition at a time; multi-producer safety requires per-producer unique
    // producerIds via InitProducerId (Milestone-C TODO).
    int64_t producerId_  = 0x4352554E'4543422DULL; // "RUNECB-" magic stub
    int16_t producerEpoch_ = 0;

    // ---- In-flight batch tracking (agent-thread only; no mutex needed) -------
    struct BatchInfo {
        int32_t              topicId;
        int32_t              partition;
        int32_t              ingressStreamId = 0; // E5.1: per-worker stream for redirect resend
        std::vector<uint8_t> wireFrame;   // CB-5 B: encoded Produce bytes for redirect resend
        int32_t              redirectCount = 0; // bounded redirect attempts
        // Per-record ack fan-out (zero-alloc): this batch owns the ackRing slice
        // [ackStart, ackStart+ackCount).  onProduceResponse resolves
        // corrId = ackRing[(ackStart+i) & ackMask] with (baseOffset+i).
        // ackCount==0 for acks=None batches.
        uint64_t             ackStart = 0;
        int32_t              ackCount = 0;
    };
    std::map<int64_t, BatchInfo> batchMap_;

    // ---- Tier-2 batch queue (mutex-guarded — app threads push, agent pops) ----
    // The ring carries a small kMsgTier2Batch token; the actual batch bytes live
    // here.  Agent pops in FIFO order when it processes the ring token.
    std::mutex               tier2Mtx_;
    std::deque<Tier2Entry>   tier2Queue_;

    // ---- Pending acks (mutex-guarded — control-plane only) -------------------
    std::mutex               ackMtx_;
    std::map<int64_t, PendingAck> pendingAcks_;
};

} // namespace rune::client::producer
