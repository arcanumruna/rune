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
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/ringbuffer/RingBufferDescriptor.h>

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
inline constexpr int32_t kDefaultProducerRingCap = 256 * 1024; // 256 KiB

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
    int32_t topicId   = 0;
    int32_t partition = 0;

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

    // Force-flush flag (set by flush() call from app thread, read here).
    bool flushRequested = false;

    // Pending correlationId for the current batch (0 = acks::None or not set).
    // Populated from the SEND_RECORD's corrId field when wantAck=true.
    // Used in flushBatches so the Produce frame carries the app-registered corrId.
    int64_t pendingCorrId = 0;

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
    /// Does NOT flush — call flushBatches() after.
    /// nowNs: current time (NanoClock, sampled once per doWork cycle).
    /// Returns count of records drained.
    int drainRing(int64_t nowNs);

    /// Flush partition batches that are ready (linger/batchBytes/flush/flushFlag).
    /// Calls encodeAndSend() for each ready batch.
    /// Returns count of batches flushed.
    int flushBatches(int64_t nowNs, int64_t sessionId,
                     std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// Called by the agent when a ProduceResponse arrives.  Resolves the pending
    /// ack (callback/future) by correlationId.
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Called on agent close/stop: fail all pending acks.
    void failAllPending(const std::string& reason);

    /// Called from Client::sweepTimeouts(): fail acks older than the deadline.
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    // ---- Accessors -----------------------------------------------------------

    const ProducerConfig& config() const noexcept { return cfg_; }

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

    bool encodeAndSend(PartitionState& ps,
                       int64_t nowNs,
                       int64_t sessionId,
                       int64_t correlationId,
                       std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    /// Encode + send a Tier-2 pre-built batch (agent thread only).
    /// Patches baseSequence into the provided batch bytes, then encodes a
    /// Produce wire frame and offers it to transport.
    bool encodeAndSendTier2(PartitionState& ps,
                             Tier2Entry& entry,
                             int64_t sessionId,
                             std::function<bool(const uint8_t*, std::size_t)>& sendFn);

    // ---- Members -------------------------------------------------------------

    ProducerConfig cfg_;

    // Shared correlationId counter (also used by Client for ping).
    std::atomic<int64_t>& nextCorrId_;

    // ---- Own ring buffer (MPSC: many app threads write, one agent thread reads)
    // ProducerImpl owns its ring so SEND_RECORD messages are separate from the
    // ClientAgent command ring (agent command type IDs 1-4 are unrelated).
    static constexpr std::size_t kRingAlignment = 64; // cache-line aligned
    std::unique_ptr<uint8_t, void(*)(void*)> ringBuf_;
    aeron::concurrent::AtomicBuffer          ringAtomicBuf_;
    aeron::concurrent::ringbuffer::ManyToOneRingBuffer ring_;

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
    int64_t producerId_  = 0x4352554E'4543422DULL; // "RUNECB-" magic stub
    int16_t producerEpoch_ = 0;

    // ---- In-flight batch tracking (agent-thread only; no mutex needed) -------
    struct BatchInfo { int32_t topicId; int32_t partition; };
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
