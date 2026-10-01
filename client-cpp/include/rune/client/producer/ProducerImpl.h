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
//   registerCallback() / registerFuture() — opt-in: allocate a PendingAck whose
//     address (the "ack handle") travels to the agent inside the SEND_RECORD.
//
// Agent thread (doWork / drainAndFlush):
//   drainRing()    — reads SEND_RECORD ring entries, appends to builders.
//   flushBatches() — encodes + offers batches that hit linger/size/flush triggers.
//   onProduceResponse() — completes the batch's ack-ring slice (no lookup, no lock).
//   NanoClock is sampled once per doWork cycle and passed in (never per-send).
//
// Ring message format (kMsgSendRecord payload):
//   [0..3]   topicId   : int32 LE
//   [4..7]   partition : int32 LE
//   [8]      keyLen    : uint8  (≤16)
//   [9..24]  keyBytes  : uint8[keyLen] (inline)
//   [25]     flags     : uint8  (bit0=wantAck)
//   [26..33] ackHandle : int64 LE PendingAck* (only meaningful when wantAck;
//                        ownership passes to the agent on ring commit)
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
#include <rune/client/detail/FlatIndexMap.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/detail/M2ORingBuffer.h> // internal MPSC ring (controlledRead/ABORT, templated read)

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <span>
#include <string>
#include <string_view>
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

// kMsgTier2Batch token: 8 bytes = a heap Tier2Entry* (int64 LE).  The entry's
// ownership travels through the ring (spec §1a.2) — no side queue, no mutex.
inline constexpr int kT2TokenSize = 8;

// Flags bits
inline constexpr uint8_t kFlagWantAck = 0x01;

// Default ring capacity for a producer (per-producer, separate from agent ring).
// This is the app→agent handoff buffer (Kafka's buffer.memory analogue): sized to
// absorb produce bursts before Producer::send() back-pressures.  2 MiB holds
// several thousand in-flight SEND_RECORDs so a bursty app is not throttled by the
// agent's drain cadence.
inline constexpr int32_t kDefaultProducerRingCap = 32 * 1024 * 1024; // 32 MiB

// Per-partition ack-ring capacity (entries; MUST be a power of two).  Holds the
// PendingAck handles of outstanding (sent, not-yet-acked) records for one
// partition at their record index so ProduceResponse can fan out baseOffset+i.  Sized to comfortably exceed
// the in-flight record count for a partition (maxInFlight batches × batch size);
// if it ever fills, drainRing force-flushes + stops draining so the producer ring
// applies backpressure (no handle is ever dropped).  8 bytes/entry.
inline constexpr uint32_t kAckRingCapacity = 16384; // 128 KiB/partition

// ---------------------------------------------------------------------------
// Tier2Entry — pre-built RecordBatch bytes waiting for agent-thread processing.
//
// The app thread builds the batch bytes (without baseSequence — left 0) into a
// heap entry and writes its address into the ring (kMsgTier2Batch); the agent
// takes ownership when it drains the token, stamps the correct baseSequence
// from PartitionState, then encodes+sends.
// ---------------------------------------------------------------------------
struct Tier2Entry {
    int32_t  topicId     = 0;
    int32_t  partition   = 0;
    uint64_t ackHandle   = 0;  // PendingAck* of the batch future (0 = none)
    int32_t  recordCount = 0;
    int64_t  enqueuedNs  = 0;  // agent clock when drained (delivery deadline if never sent)
    std::vector<uint8_t> batchBytes; // pre-built RecordBatch (baseSequence=0, patched by agent)
};

// ---------------------------------------------------------------------------
// PartitionState — per-(topicId,partition) accumulator (agent-thread only).
// ---------------------------------------------------------------------------
struct PartitionState {
    int32_t topicId        = 0;
    int32_t partition      = 0;
    uint32_t slotIndex = 0;  ///< dense index inside the owning producer (stable)

    /// E5.1: per-worker Aeron ingress stream id from metadata.
    /// 0 = legacy / not yet known — falls back to the default ingress stream.
    int32_t ingressStreamId = 0;

    // Idempotence sequence: monotonically increasing baseSequence.
    int32_t nextBaseSequence = 0;

    // Fixed ring of in-flight (sent, un-acked) batches; capacity = maxInFlight
    // rounded up to a power of two (spec §4.3).
    InFlightRing inflight;

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

    // Per-record ack fan-out, ZERO-ALLOC ring (spec §4.3).  ackRing holds
    // PendingAck handles at their record's OWN index within the batch (void
    // records hold 0; zeros are back-filled lazily, trailing void records take no
    // slot), preallocated ONCE (never per-batch).  Each flushed batch owns the
    // contiguous slice [batchAckStart, batchAckStart+batchAckCount): on
    // ProduceResponse ackRing[(start+i) & ackMask] completes at baseOffset + i.
    // ackTail = start of the oldest un-acked slice (oldest live in-flight batch,
    // else the current builder), so out-of-order acks never reclaim a live slice.
    std::vector<uint64_t> ackRing;       // PendingAck* handles at record index (0 = void record)
    uint32_t             ackMask       = 0; // ackRing.size() - 1 (size is pow2)
    uint64_t             ackHead       = 0; // next write index (monotonic)
    uint64_t             ackTail       = 0; // oldest unacked index (monotonic)
    uint64_t             batchAckStart = 0; // ackHead at start of current batch
    int32_t              batchAckCount = 0; // slice length of current batch (incl. back-filled 0s)

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
// PendingAck — heap object created on the opt-in ack paths.  Owned by the app
// thread until the SEND_RECORD carrying its address is committed to the ring,
// by the duty-cycle thread afterwards.  complete() fires it exactly once and
// deletes it.
// ---------------------------------------------------------------------------
struct PendingAck {
    enum class Kind : uint8_t { Callback, Future };
    Kind                         kind = Kind::Callback;
    std::function<void(int64_t)> callback;
    std::promise<int64_t>        promise;

    /// error empty = success (callback gets offset, future gets value);
    /// otherwise callback(-1) / future exception(error).
    static void complete(uint64_t handle, int64_t offset, std::string_view error) noexcept;
};

// ---------------------------------------------------------------------------
// ProducerImpl
// ---------------------------------------------------------------------------
class ProducerImpl {
public:
    /// Sink concept: `bool operator()(int32_t topicId, int32_t partition,
    /// int32_t ingressStreamId, const uint8_t* buf, std::size_t len) const`.
    using SendFn = std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)>;

    /// Construct a ProducerImpl with its own ring buffer.
    /// ringCap: power-of-two usable capacity in bytes (default 256 KiB).
    explicit ProducerImpl(ProducerConfig cfg,
                          std::atomic<int64_t>& nextCorrId,
                          int32_t ringCap = kDefaultProducerRingCap);

    /// Frees (without firing) every ack handle still owned by this producer.
    ~ProducerImpl();

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

    /// Non-blocking submit (ZERO allocation, lock-free): validates the value
    /// size, then writes the SEND_RECORD. Never drops silently — see SendResult.
    [[nodiscard]] SendResult trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value, int64_t ackHandle) noexcept;

    /// Slow path of the void send: retries until accepted; throws on non-retryable
    /// results. In Invoker mode (pumpFn_ wired by Client::producer()), a
    /// BackPressured retry pumps the owning Client's duty cycle instead of idling
    /// on the ring's consumer heartbeat (the owner thread IS that consumer, so
    /// waiting on it would wait on itself forever); the hook declines (returns
    /// false) when pumping would be unsafe (inside a duty-cycle callback, or off
    /// the owner thread), in which case this throws immediately rather than spin.
    /// With no pump hook wired (Thread mode, or a bare ProducerImpl in a test),
    /// it backs off and bounds the wait by the ring's consumerHeartbeatTime vs.
    /// agentLivenessTimeoutMs.
    void sendBlocking(SendResult first, int32_t topicId, int32_t partition, const Key& key,
                      std::span<const uint8_t> value);

    using ClockFn = int64_t (*)() noexcept;
    void setClockForTesting(ClockFn fn) noexcept { clock_ = fn; }

    /// Invoker-mode pump hook, wired by Client::producer(): called from
    /// sendBlocking() on BackPressured. Returns true if it pumped the duty cycle
    /// (retry immediately), false if pumping is unsafe right now (the hook
    /// enforces its own gate — owner thread, not already inside a duty-cycle
    /// callback) — sendBlocking then fails fast instead of spinning. A plain
    /// function pointer + opaque context: no std::function, no virtual call,
    /// no new atomics (shared-nothing: this is set once, before any send(), by
    /// the single thread that owns the producer).
    using PumpFn = bool (*)(void* ctx) noexcept;
    void setPumpFn(PumpFn fn, void* ctx) noexcept { pumpFn_ = fn; pumpCtx_ = ctx; }

    /// Write a flush trigger into the ring.  Zero-alloc.
    void submitFlush() noexcept;

    // ---- App-thread API (Tier-2 explicit batch) --------------------------------

    /// Submit a pre-built Tier-2 batch (from BatchBuilder::send()).
    ///
    /// Moves the batch into a heap Tier2Entry and writes its address into the
    /// ring (kMsgTier2Batch); ownership passes to the agent on commit.  The agent
    /// processes the token in order with Tier-1 records, stamping baseSequence.
    ///
    /// ackHandle: PendingAck handle from registerFuture() (0 = none).  On false
    /// the caller still owns it and must failUnsubmitted() it.
    /// recordCount: number of records in the pre-built batch.
    /// batchBytes: pre-built RecordBatch bytes (baseSequence field = 0; will be
    ///             patched by the agent).
    ///
    /// Returns false if the ring is full (caller should handle as an error).
    bool submitTier2Batch(int32_t topicId,
                          int32_t partition,
                          int64_t ackHandle,
                          int32_t recordCount,
                          std::vector<uint8_t> batchBytes);

    // ---- App-thread API (opt-in ack — may allocate) --------------------------

    /// Allocate a callback PendingAck.  Returns the owning ack handle to stamp on
    /// the SEND_RECORD (ownership passes on a successful submit; otherwise the
    /// caller must failUnsubmitted() it).
    int64_t registerCallback(std::function<void(int64_t)> cb);

    /// Allocate a future PendingAck.  Returns (owning ack handle, future).
    std::pair<int64_t, std::future<int64_t>> registerFuture();

    /// App thread: fail an ack handle that never reached the ring (ring full).
    void failUnsubmitted(int64_t ackHandle, const std::string& reason) noexcept;

    /// Per-batch acknowledgements (setup-phase; see Producer::onBatchAck).
    void onBatchAck(std::function<void(const BatchAck&)> cb) { batchAckCb_ = std::move(cb); }
    /// Invoker mode: buffer BatchAcks for drainBatchAcks() instead of (or as well
    /// as) calling the callback.
    void collectBatchAcks(bool on) noexcept {
        collectBatchAcks_ = on;
        if (on && completedAcks_.capacity() < 4096) completedAcks_.reserve(4096);
    }
    template <class H>
    int drainBatchAcks(H&& h) {
        const int n = static_cast<int>(completedAcks_.size());
        for (const BatchAck& a : completedAcks_) h(a);
        completedAcks_.clear();  // keeps capacity
        return n;
    }

    // ---- Agent-thread API (single-writer) ------------------------------------

    /// Drain SEND_RECORD ring messages, append to builders.
    /// Does NOT flush — call flushBatches() after.  Applies a drain gate: if any
    /// partition is at batchBytes AND at maxInFlight, returns 0 without reading so
    /// the producer ring back-pressures the app (bounds every builder to a
    /// sendable size).
    /// nowNs: current time (NanoClock, sampled once per doWork cycle).
    /// Returns count of records drained.
    int drainRing(int64_t nowNs);

    /// Sink concept: `bool operator()(int32_t topicId, int32_t partition,
    /// int32_t ingressStreamId, const uint8_t* buf, std::size_t len) const`.
    /// Explicitly instantiated in ProducerImpl.cpp for SendFn (tests, cold
    /// paths), agent::AgentProduceSink (production) and CountingSink (benches);
    /// no std::function is built or called on the production flush path.
    template <class Sink>
    int flushBatches(int64_t nowNs, int64_t sessionId, const Sink& sink);

    template <class Sink>
    int retransmitStalled(int64_t nowNs, const Sink& sink);

    /// Called by the agent when a ProduceResponse arrives.  Completes the batch's
    /// ack-ring slice (record i -> baseOffset + i) and reports its BatchAck.
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Called on close / redirect-reconnect failure (duty-cycle thread, or after
    /// it stopped): fails AND drops every accepted record — still in the app ring,
    /// in a builder, queued Tier-2, or in flight.
    void failAllPending(const std::string& reason);

    /// Duty-cycle thread: fail every record of an in-flight batch (wire id).
    /// Non-batch ids are ignored (they can come from any ErrorResponse).
    void failBatch(int64_t batchCorrId, const std::string& reason);

    /// CB-5 B: attempt to resend the wire frame for a redirected Produce.
    /// Returns true if resent, false if corrId unknown / max redirects exceeded.
    /// On failure the pending ack is failed immediately.
    static constexpr int kMaxRedirects = 3;
    bool retryRedirectedProduce(int64_t corrId,
                                SendFn& sendFn);

    /// CB-5 B: resend EVERY in-flight (still-unacked) Produce frame after a
    /// leader redirect + reconnect.  Each stored wire frame is re-stamped with
    /// the NEW sessionId (bytes [8..15] of the MessageHeader) so the leader —
    /// which just assigned a fresh session — accepts it instead of dropping it
    /// as unknown-session.  Bounded per-frame by kMaxRedirects; frames that
    /// exceed the budget are failed.  Returns the number of frames resent.
    int resendAllRedirected(int64_t newSessionId,
                            SendFn& sendFn);

    /// Task 1.4 (redirect convergence): re-offer every in-flight frame through its
    /// partition-leader route (multi-node).  The sendFn re-stamps the correct
    /// per-leader sessionId itself, so — unlike resendAllRedirected — this does NOT
    /// re-stamp a global sessionId and does NOT consume the redirect budget (a held
    /// offer is not a redirect).  Returns the number of frames actually offered.
    int resendAllViaRoute(SendFn& sendFn);

    /// CB-5 B: true if there is at least one in-flight batch awaiting an ack
    /// (i.e. a redirect resend would have something to flush).
    [[nodiscard]] bool hasInFlight() const noexcept {
        for (const auto& up : partitions_) if (!up->inflight.empty()) return true;
        return false;
    }

    /// MD1 review IMPORTANT-1: look up the (topicId, partition) for an in-flight
    /// batch corrId (O(1): the id addresses its partition slot directly).  Returns
    /// true and sets topicId+partition if a live batch has exactly this id.
    /// Called on the agent thread from the redirect handler so the MetadataCache
    /// can be updated with the affected partition.
    [[nodiscard]] bool partitionForCorrId(int64_t corrId, int32_t& topicIdOut,
                                          int32_t& partitionOut) noexcept {
        if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return false;
        const uint32_t s = batchcorr::slot(corrId);
        if (s >= partitions_.size() || partitions_[s]->inflight.find(corrId) == nullptr) return false;
        topicIdOut   = partitions_[s]->topicId;
        partitionOut = partitions_[s]->partition;
        return true;
    }

    /// MD1 review IMPORTANT-2: return the (topicId, partition) of the first
    /// partition that currently has produce work — either accumulated in a
    /// PartitionState (records appended / Tier-2 queued) or already in-flight
    /// (its InFlightRing).  Returns true and sets the out params if any such partition
    /// exists; false otherwise.  Called on the agent thread so the produce path
    /// can resolve the partition leader's endpoint via the PartitionRouter and
    /// route the produce DIRECT to that leader using cached metadata.
    [[nodiscard]] bool firstPendingPartition(int32_t& topicIdOut,
                                             int32_t& partitionOut) const noexcept {
        // Prefer an accumulated (not-yet-sent) partition: routing it BEFORE the
        // frame goes out is the zero-extra-hop path.
        for (const auto& up : partitions_) {
            const PartitionState& ps = *up;
            const bool hasTier1 = (ps.batchStartNs != 0) || ps.flushRequested;
            const bool hasTier2 = !ps.pendingTier2Entries.empty();
            if (hasTier1 || hasTier2) {
                topicIdOut   = ps.topicId;
                partitionOut = ps.partition;
                return true;
            }
        }
        // Fall back to an in-flight batch (already sent, awaiting ack).
        for (const auto& up : partitions_) {
            if (!up->inflight.empty()) {
                topicIdOut   = up->topicId;
                partitionOut = up->partition;
                return true;
            }
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

    /// Called from Client::sweepTimeouts(): fail in-flight batches past the
    /// delivery timeout, and fail + drop never-sent records past it.
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    // ---- Accessors -----------------------------------------------------------

    const ProducerConfig& config() const noexcept { return cfg_; }

    /// Diagnostics / tests (agent thread only): the accumulator of
    /// (topicId, partition), or nullptr if never seen.  Read-only; no allocation.
    [[nodiscard]] const PartitionState* partitionState(int32_t topicId,
                                                       int32_t partition) const noexcept;

    /// Ordinal inside the owning ProducerHub; encoded into batch correlation ids.
    void setOrdinal(uint32_t o) noexcept { ordinal_ = o; }
    [[nodiscard]] uint32_t ordinal() const noexcept { return ordinal_; }

    /// Total Aeron frames offered to transport (agent-thread only; plain uint64_t).
    uint64_t framesSent()  const noexcept { return framesSent_; }

    /// Total records accepted via drainRing (agent-thread only; plain uint64_t).
    uint64_t recordsSent() const noexcept { return recordsSent_; }

    /// This instance's current producerId / epoch (diagnostics + tests; F5).
    /// Both are agent-thread state but only ever change on the agent thread
    /// (construction and the epoch-overflow path), so a racy read from another
    /// thread is stale-but-safe for test assertions taken after a pump.
    [[nodiscard]] int64_t producerId() const noexcept { return producerId_; }
    [[nodiscard]] int16_t producerEpoch() const noexcept { return producerEpoch_; }

    /// Test-only seam (F5 regression coverage): force the current epoch so a
    /// test can drive the INT16_MAX overflow path without sending 32767 real
    /// batches to get there. Never called from production code.
    void debugSetEpochForTest(int16_t epoch) noexcept { producerEpoch_ = epoch; }

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

    template <class Sink>
    bool encodeAndSend(PartitionState& ps,
                       int64_t nowNs,
                       int64_t sessionId,
                       int64_t correlationId,
                       const Sink& sink);

    /// Encode + send a Tier-2 pre-built batch (agent thread only).
    /// Patches baseSequence into the provided batch bytes, then encodes a
    /// Produce wire frame and offers it to transport.
    template <class Sink>
    bool encodeAndSendTier2(PartitionState& ps,
                            Tier2Entry& entry,
                            int64_t nowNs,
                            int64_t sessionId,
                            const Sink& sink);

    [[nodiscard]] bool canFlight_(const PartitionState& ps) const noexcept {
        return static_cast<int32_t>(ps.inflight.live()) < cfg_.maxInFlight && !ps.inflight.full();
    }
    /// ackTail = start of the oldest un-acked slice (in-flight or current builder).
    void reclaimAckRing_(PartitionState& ps) noexcept;
    /// Consumes every message left in the app->agent ring, handing each ack
    /// handle it carries (SEND_RECORD wantAck / Tier-2 entry) to onHandle.
    template <class OnHandle>
    void drainAppRingHandles_(OnHandle&& onHandle);
    void pushAck_(PartitionState& ps, uint64_t handle);
    /// Completes the handles of slice [ackStart, ackStart+ackCount): record i ->
    /// baseOffset + i, or error.  Zeroes each slot so nothing completes twice.
    void completeSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount,
                        int64_t baseOffset, std::string_view error) noexcept;
    /// acks=None: completes the just-sent builder slice with -1 ("sent, offset
    /// unknown") and reclaims it — nothing waits for a response.
    void completeSentUnacked_(PartitionState& ps) noexcept;
    /// Unresolved sequence (Kafka semantics): a sequenced batch stamped with the
    /// CURRENT epoch was failed to the user while its broker outcome is unknown or
    /// it was rejected, so its sequences are a same-epoch gap the broker would
    /// reject forever.  Bumps producerEpoch_ and restarts every partition at
    /// baseSequence 0 (the broker accepts a higher epoch only at 0 and resets its
    /// state).  Batches of an older epoch do not bump again (one bump per gap
    /// event).  Epoch overflow: at INT16_MAX a FRESH unique producerId is drawn
    /// (never the old id+1 — see generateProducerId() in ProducerImpl.cpp) and
    /// the epoch restarts at 0 — the broker treats it as a new producer
    /// (trust-on-first-use at baseSequence 0).
    void onUnresolvedSequence_(int16_t batchEpoch) noexcept;
    void emitBatchAck_(const PartitionState& ps, int32_t baseSequence, int32_t recordCount,
                       int64_t baseOffset, int16_t errorCode);
    /// Fails a live batch's records and releases its slot.
    void failBatch_(PartitionState& ps, InFlightBatch& b, const std::string& reason);
    /// Always-on invariant check: the id encoded into the sent frame must be the
    /// id of the slot just claimed; on mismatch logs loudly and fails the batch.
    void verifyClaimedCorrId_(PartitionState& ps, InFlightBatch& b, int64_t wireCorrId);

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

    // Flat partition table (spec §1a.5): FlatIndexMap key -> dense slot, slots own
    // their PartitionState (stable address; slotIndex == position).
    detail::FlatIndexMap                          partitionIndex_;
    std::vector<std::unique_ptr<PartitionState>>  partitions_;

    // Encode scratch buffers (reused per batch — zero per-flush heap allocation).
    // batchScratch_: RecordBatch bytes (cleared + refilled by buildInto()).
    // encodeScratch_: Produce wire frame (grown once; steady-state = no alloc).
    std::vector<uint8_t> batchScratch_;
    std::vector<uint8_t> encodeScratch_;

    // Per-instance producerId (F5 fix): drawn fresh at construction by
    // generateProducerId() (ProducerImpl.cpp) — 63 random bits mixed from
    // std::random_device, std::chrono::steady_clock, and the process id, never
    // the old shared magic-stub value.  On the broker, the idempotent-dedup key
    // is (producerId, partition, baseSequence); two concurrent producers on the
    // SAME partition with the SAME producerId would collide and deduplicate
    // incorrectly, which is exactly what the old hardcoded stub caused.
    // producerId == -1 is reserved by the wire protocol for "non-idempotent"
    // (see acks=None handling in ProducerImpl.cpp) and is never assigned here.
    // TODO(milestone-c): the durable fix is a broker-allocated InitProducerId
    // with zombie-fencing; this client-drawn random id only makes an
    // accidental collision between two live instances astronomically unlikely.
    int64_t producerId_;
    int16_t producerEpoch_ = 0;

    // ---- In-flight batch tracking (agent-thread only; no mutex needed) -------
    // In-flight batches live in each PartitionState::inflight ring; a batch
    // correlation id encodes (ordinal_, partition slot, ring sequence).
    uint32_t ordinal_ = 0;
    // Lower bound of every live batch's attemptDeadlineNs: retransmitStalled()
    // returns in O(1) until it passes (spec §1a.5; replaces the ordered map front).
    int64_t earliestAttemptDeadlineNs_ = INT64_MAX;

    // ---- Per-batch acks (duty-cycle thread only) ------------------------------
    std::function<void(const BatchAck&)> batchAckCb_;   // set before the first send
    bool                                 collectBatchAcks_ = false;
    std::vector<BatchAck>                completedAcks_;

    // ---- sendBlocking() support (app thread only) -----------------------------
    static int64_t steadyNowNs_() noexcept;
    ClockFn clock_   = &steadyNowNs_;   // overridable by setClockForTesting()
    PumpFn  pumpFn_  = nullptr;         // wired by Client::producer() in Invoker mode
    void*   pumpCtx_ = nullptr;
};

} // namespace rune::client::producer
