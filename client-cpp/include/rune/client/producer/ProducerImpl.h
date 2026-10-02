// include/rune/client/producer/ProducerImpl.h
//
// CB-2 — ProducerImpl: agent-side single-writer accumulator + ring handoff.
//
// This class is NOT part of the public API.  F8c C7: owned jointly by the
// Producer handle (app thread) and the duty side (ProducerHub) — raw pointers,
// the §3.3 release protocol (the last of the two to detach frees it).
//
// App threads:
//   submitSendRecord() — lock-free ring write (the zero-alloc handoff).
//   submitFlush()      — lock-free ring write (zero bytes, flushFlag).
//   F8c C6: an acked send stamps a tagged 64-bit ACK HANDLE (ackhandle::, below:
//     a Completions target's CqRef + Token, or the onAck handler + Token) into
//     the SEND_RECORD — no allocation, nothing to own or free.
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
//   [26..33] ackHandle : int64 LE ackhandle:: tagged token (only meaningful
//                        when wantAck)
//   [34..37] valueLen  : uint32 LE
//   [38..]   valueBytes
//
// SEND_FLUSH payload: zero bytes (kMsgFlush typeId).
//
// ProducerImpl owns its own ManyToOneRingBuffer (separate from the ClientAgent
// command ring) so SEND_RECORD messages are not confused with agent commands.

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/producer/Producer.h>

#include <rune/protocol/RecordBatch.h>

#include <concurrent/AtomicBuffer.h>
#include <concurrent/BackOffIdleStrategy.h>
#include <rune/client/detail/FlatIndexMap.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/detail/M2ORingBuffer.h> // internal MPSC ring (controlledRead/ABORT, templated read)

#include <thread>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <variant>
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

// ---------------------------------------------------------------------------
// F8c C6 — the 64-bit ack handle (SEND_RECORD ackHandle, PartitionState::
// ackRing slot, Tier2Entry::ackHandle).  0 = no ack (void record).  Otherwise
// a TAGGED TOKEN, never a pointer (nothing is allocated, owned or freed):
//   bit 63 = 1            Completions target: bits 48..62 = its CqRef (15 bits:
//                         10-bit registry index + 5-bit generation),
//                         bits 0..47 = the Token;
//   bit 63 = 0, bit 62 = 1  the producer's onAck handler: bits 0..47 = the Token.
// (Spec §2.4 sketched bit 63 = "ack" + bit 62 = target + a 15-bit CqRef + a
// 48-bit Token — 65 bits.  Here the target bit itself marks "ack": a CQ handle
// always has bit 63 set and a handler handle bit 62, so neither is ever 0 and
// every CqRef and every Token fit.)
// ---------------------------------------------------------------------------
namespace ackhandle {
inline constexpr uint64_t kCqBit      = uint64_t{1} << 63;
inline constexpr uint64_t kHandlerBit = uint64_t{1} << 62;
inline constexpr int      kRefShift   = 48;
inline constexpr uint64_t kRefMask    = 0x7FFF;
[[nodiscard]] constexpr uint64_t forCq(CqRef ref, Token t) noexcept {
    return kCqBit | ((static_cast<uint64_t>(ref) & kRefMask) << kRefShift) | (t & kMaxToken);
}
[[nodiscard]] constexpr uint64_t forHandler(Token t) noexcept { return kHandlerBit | (t & kMaxToken); }
[[nodiscard]] constexpr bool  isCq(uint64_t h) noexcept { return (h & kCqBit) != 0; }
[[nodiscard]] constexpr CqRef cqRef(uint64_t h) noexcept {
    return static_cast<CqRef>((h >> kRefShift) & kRefMask);
}
[[nodiscard]] constexpr Token token(uint64_t h) noexcept { return h & kMaxToken; }
}  // namespace ackhandle

/// The reservation (and returned credit) of one producer completion record: a
/// per-record / Tier-2 ack, and each credit unit of a BatchAck record.
inline constexpr uint32_t kAckRecordBytes = kBatchAckUnitBytes;  // CompletionRing::completionRecordBytes(0)

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

// Task 14: a Direct-ingress (RunnerMode::Invoker) producer never writes a record
// into its ring — it only keeps the ring's trailer words (closed / heartbeat), so
// the ring is the smallest power of two instead of 32 MiB per producer.
inline constexpr int32_t kDirectProducerRingCap = 1024;

// Per-partition ack-ring capacity (entries; MUST be a power of two).  Holds the
// ack handles (ackhandle::) of outstanding (sent, not-yet-acked) records for one
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
    uint64_t ackHandle   = 0;  // ackhandle:: tagged token of the batch (0 = none)
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

    // EOS follow-ups I-4 (Kafka TransactionManager, per partition): the
    // (producerId, epoch) this partition's sequences belong to.  Every live
    // batch of the partition carries exactly this identity (a reset happens
    // only with nothing in flight, or re-stamps every live batch).  A bump of
    // the PRODUCER epoch moves only the partition that needs it; the others
    // keep theirs (the broker tracks (producerId, partition)), so no
    // already-stored retransmit elsewhere is ever fenced.
    int64_t  producerId   = -1;
    int16_t  epoch        = 0;
    int32_t  lastAckedSeq = -1;     // last sequence acked OK under (producerId, epoch)
    uint64_t lastAckTick  = 0;      // ProducerImpl::sendTick_ when the latest OK was processed
    bool     unresolved   = false;  // a batch failed with its broker outcome unknown

    // Fixed ring of in-flight (sent, un-acked) batches; capacity = maxInFlight
    // rounded up to a power of two (spec §4.3).
    InFlightRing inflight;
    // EOS-OOS: live batches parked on OutOfOrderSequence (InFlightBatch::oosParked)
    // and whether their older batches have all resolved, so they must be re-sent
    // — in sequence order, BEFORE any new batch of this partition — by the next
    // flushBatches visit.
    uint32_t parkedCount  = 0;
    bool     resendParked = false;

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

    // F12: this partition is on its producer's dirty list (ProducerImpl::dirty_)
    // — it has (or recently had) builder records / queued Tier-2 batches, so
    // flushBatches must examine it.  Cleared lazily by the flush walk once the
    // partition has no work left.  Owner/duty thread only.
    bool dirty = false;

    // Task 14 fix2: records of THIS partition waiting in the producer's
    // DeferredArena (owner thread only).  >0 gates this partition's Direct
    // sends (per-partition order) without touching any other partition.
    uint32_t deferredCount    = 0;
    uint32_t deferredBytes    = 0;  // arena bytes held (per-partition cap)
    uint32_t deferBlockedPass = 0;  // apply pass in which this partition ABORTed

    // Per-record ack fan-out, ZERO-ALLOC ring (spec §4.3).  ackRing holds
    // ack handles (ackhandle::) at their record's OWN index within the batch (void
    // records hold 0; zeros are back-filled lazily, trailing void records take no
    // slot), preallocated ONCE (never per-batch).  Each flushed batch owns the
    // contiguous slice [batchAckStart, batchAckStart+batchAckCount): on
    // ProduceResponse ackRing[(start+i) & ackMask] completes at baseOffset + i.
    // ackTail = start of the oldest un-acked slice (oldest live in-flight batch,
    // else the current builder), so out-of-order acks never reclaim a live slice.
    std::vector<uint64_t> ackRing;       // ackhandle:: tokens at record index (0 = void record)
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
// DeferredArena — Task 14 fix1 C1 / fix2: Direct-ingress sends made while the
// producer is inside its OWN duty work (a user callback it fires mid-walk /
// mid-reset) are queued here and applied after that work, PER-PARTITION in
// submission order (a stalled partition never holds back another one).
//
// NOT a cross-thread ring: a plain single-owner (Invoker owner thread) byte
// arena — no atomics, no locks — preallocated once (enableDirectIngress); push()
// never allocates.  Records are [DeferredHdr][payload], 8-byte aligned, appended
// at the tail.  Records leave by being applied / failed (dead: type 0); the
// apply pass compacts in place with a read/write cursor (kept records slide
// forward), and a push that does not fit compacts dead space first when no scan
// is in progress.  Full: push() returns nullptr (the caller back-pressures).
// ---------------------------------------------------------------------------
struct DeferredHdr {
    uint32_t len       = 0;  ///< payload bytes
    uint8_t  type      = 0;  ///< kMsgSendRecord / kMsgTier2Batch; 0 = dead
    uint8_t  flags     = 0;  ///< kDefUnknownPartition | kDefFlushOnApply
    uint16_t reserved  = 0;
    int32_t  topicId   = 0;
    int32_t  partition = 0;
    int64_t  submitNs  = 0;  ///< duty clock at acceptance: delivery deadline + batch start
};
static_assert(sizeof(DeferredHdr) == 24);
inline constexpr uint8_t kDefUnknownPartition = 0x01;  ///< counted in deferredUnknown_ (no PartitionState yet)
inline constexpr uint8_t kDefFlushOnApply     = 0x02;  ///< flush() was called after acceptance

class DeferredArena {
public:
    static constexpr std::size_t align8(std::size_t n) noexcept { return (n + 7u) & ~std::size_t{7}; }
    static constexpr std::size_t recordSize(std::size_t payloadLen) noexcept {
        return align8(sizeof(DeferredHdr) + payloadLen);
    }

    void init(std::size_t capacity) {
        buf_.assign(align8(capacity), 0);
        tail_ = 0;
        live_ = 0;
    }
    [[nodiscard]] uint32_t    live() const noexcept { return live_; }
    [[nodiscard]] bool        empty() const noexcept { return live_ == 0; }
    [[nodiscard]] std::size_t tail() const noexcept { return tail_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return buf_.size(); }

    /// Payload pointer for a new record, or nullptr when it does not fit.
    uint8_t* push(const DeferredHdr& h) noexcept {
        const std::size_t need = recordSize(h.len);
        if (tail_ + need > buf_.size()) return nullptr;
        uint8_t* p = buf_.data() + tail_;
        std::memcpy(p, &h, sizeof h);
        tail_ += need;
        ++live_;
        return p + sizeof(DeferredHdr);
    }
    // Offset-based access (stable while no compaction runs: the buffer never moves).
    [[nodiscard]] DeferredHdr* hdr(std::size_t off) noexcept {
        return reinterpret_cast<DeferredHdr*>(buf_.data() + off);
    }
    [[nodiscard]] uint8_t* payload(std::size_t off) noexcept {
        return buf_.data() + off + sizeof(DeferredHdr);
    }
    [[nodiscard]] std::size_t sizeAt(std::size_t off) noexcept { return recordSize(hdr(off)->len); }
    void kill(std::size_t off) noexcept {
        hdr(off)->type = 0;
        --live_;
    }
    /// Move [from, from+n) to `to` (to <= from) — the apply pass's write cursor.
    void move(std::size_t to, std::size_t from, std::size_t n) noexcept {
        if (to != from && n != 0) std::memmove(buf_.data() + to, buf_.data() + from, n);
    }
    void setTail(std::size_t t) noexcept { tail_ = t; }
    /// fix3: mark [from, to) — stale bytes behind the apply pass's write cursor
    /// — as ONE dead record, so the arena stays parseable at every instant.
    void writeFiller(std::size_t from, std::size_t to) noexcept {
        DeferredHdr f;
        f.len  = static_cast<uint32_t>(to - from - sizeof(DeferredHdr));
        f.type = 0;
        std::memcpy(buf_.data() + from, &f, sizeof f);
    }
    /// Slide live records forward over dead ones (order preserved).  Only when
    /// no scan holds an offset.
    void compactDead() noexcept {
        std::size_t w = 0;
        for (std::size_t r = 0; r < tail_;) {
            const std::size_t n = sizeAt(r);
            if (hdr(r)->type != 0) {
                move(w, r, n);
                w += n;
            }
            r += n;
        }
        tail_ = w;
    }

private:
    std::vector<uint8_t> buf_;
    std::size_t          tail_ = 0;
    uint32_t             live_ = 0;
};

// ---------------------------------------------------------------------------
// PumpResult — fix1 C1: outcome of ProducerImpl's pump hook (see PumpFn below).
// A 3-way result so a back-pressured blocking send() never waits on itself AND
// never silently spins forever when pumping would re-enter the agent's own poll:
//   Pumped          — the hook ran one duty cycle; retry trySubmit immediately.
//   DeclinedInCycle — called from INSIDE a duty-cycle callback (Invoker owner's
//                     or Thread mode's runner thread, already mid-cycle):
//                     pumping would recursively re-enter the agent's own poll.
//                     sendBlocking throws immediately — no spin, no waiting out
//                     its own heartbeat (which it IS, so it would never move).
//   NotOwner        — the calling thread is not the duty-cycle thread (Thread
//                     mode's ordinary app thread, or a foreign thread touching
//                     an Invoker producer off its owner thread): a genuinely
//                     separate thread may still drain the ring in real time, so
//                     sendBlocking falls back to the heartbeat-bounded wait.
//
// fix2 Important-2: NotOwner is decided by sendBlocking() itself (comparing the
// calling thread against the dutyThreadId_ copy from setPumpFn), BEFORE the
// pump hook (fn, below) is ever called — so a non-duty thread never touches
// the hook's context (the Client), which could otherwise race a concurrent
// Client destruction. The hook itself therefore only ever returns Pumped or
// DeclinedInCycle; it is never asked to decide NotOwner.
// ---------------------------------------------------------------------------
enum class PumpResult : uint8_t { Pumped, DeclinedInCycle, NotOwner };

// ---------------------------------------------------------------------------
// RejectReason — Task 14 fix4: WHY a submit was not accepted, RETURNED to the
// caller (trySubmit's out-param, submitTier2Batch's return value) — never kept
// as producer member state (which a foreign thread would read while the owner
// writes it, and which goes stale).
// ---------------------------------------------------------------------------
enum class RejectReason : uint8_t {
    None,               ///< accepted
    Closed,             ///< the producer's Client was closed
    WrongThread,        ///< Direct (Invoker) producer used off its owner thread
    ValueTooLarge,
    BackPressured,      ///< ring full / builder full (retry)
    DeferredFull,       ///< Direct: in-duty deferral arena / partition share full (retry later)
    PartitionDeferred,  ///< Direct: this partition still has deferred records that cannot go yet
    Unaddressable,      ///< Direct: the producer's partition table is full
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
                          int32_t ringCap = kDefaultProducerRingCap);

    /// Frees every Tier-2 entry still owned by this producer (ack handles are
    /// plain tokens: nothing to free, nothing fired).
    ~ProducerImpl();

    // ---- F8c C7: ownership (spec §3.3 — no shared_ptr) --------------------------
    //
    // Owned jointly by the Producer handle and the duty side (the ProducerHub,
    // from the registration's adoption until the handle's HandleRelease was
    // applied and the producer is quiescent — or until Client close hands it
    // back).  Each side detaches its share ONCE on the ring's ownership word
    // (one RMW); the side that detaches LAST frees the impl.  Nobody waits.

    /// Duty side: detach the duty share.  True: the handle already detached —
    /// the caller frees the impl.  The duty side never touches it afterwards.
    [[nodiscard]] bool detachDuty() noexcept { return ring_.detachConsumer(); }
    /// Handle side: detach the handle's share (its last access).  True: the
    /// duty side already detached — the caller frees the impl.
    [[nodiscard]] bool detachHandle() noexcept { return ring_.detachProducer(); }
    /// Either side (acquire): the duty side detached (Client close handed the
    /// impl back, or it was never registered) — the handle owns it alone and
    /// must never touch the Client again.
    [[nodiscard]] bool dutyDetached() const noexcept { return ring_.consumerDetached(); }
    /// Either side (acquire): the handle detached (debug check of a
    /// BatchBuilder that outlived its Producer while the duty side still holds
    /// the impl).
    [[nodiscard]] bool handleDetached() const noexcept { return ring_.producerDetached(); }
    /// F8c C7 fix round 2 (F1): the handle's share handed over with its release
    /// ORPHANED (the command ring was full).  setOrphanFence first (plain
    /// field), then the RMW publishes it.  True: the duty side had already
    /// detached — the caller frees the impl.
    void setOrphanFence(int64_t fence) noexcept { orphanFence_ = fence; }
    [[nodiscard]] bool detachHandleOrphaned() noexcept { return ring_.detachProducerOrphaned(); }
    /// Duty side (acquire): the handle orphaned its release; and its fence.
    [[nodiscard]] bool releaseOrphaned() const noexcept { return ring_.producerOrphaned(); }
    [[nodiscard]] int64_t orphanFence() const noexcept { return orphanFence_; }
    /// Duty side: the release was applied / queued (never twice).
    bool releaseNoted = false;
    /// Duty side: the handle's destructor semantics (closeImmediately unless a
    /// close — begin-close, closeImmediately, Client close — already started).
    void applyHandleRelease() {
        if (lifeOpen()) applyCloseImmediately(OpStatus::Closed);
    }
    /// Duty side: nothing of this producer is pending any more — no open /
    /// draining life, no duty work in progress (a handler of it is not
    /// running), every accepted record completed, the ring consumed to its
    /// closed tail.  A released producer is forgotten by its hub only then.
    /// Duty side: inside this producer's own duty work (a handler it fired).
    [[nodiscard]] bool inDutyWork() const noexcept { return inDuty_; }
    [[nodiscard]] bool releasable() const noexcept {
        return !lifeOpen() && !draining() && !inDuty_ && !pendingFail_.armed && fullyDrained_();
    }
    /// Registration refused (the Client was closing): no duty side will ever
    /// hold it — the intake is closed, the Client is "closed" for the handle,
    /// and the duty share is detached (the handle frees it alone).
    void markBornClosed() noexcept {
        markRingClosed();
        if (mode_ == IngressMode::Direct) clientClosedDirect_ = true;
        else ring_.consumerFinish();
        (void)ring_.detachConsumer();
    }

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

    /// Non-blocking submit (ZERO allocation, lock-free): checks the ring's closed
    /// word FIRST (fix1 C2/I3 — a Producer outliving its closed Client gets
    /// Closed, never touching the pump hook's now-dangling context), then
    /// validates the value size against both kMaxSendValueBytes AND the ring's
    /// own maxMsgLength (fix1 minor 11 — a message too big for a small ring maps
    /// to ValueTooLarge, not BackPressured, so it can never livelock a retry),
    /// then writes the SEND_RECORD. Never drops silently — see SendResult.
    /// fix4: `why` (optional) receives the reject reason, set before every
    /// return (None when accepted).
    [[nodiscard]] SendResult trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value, int64_t ackHandle,
                                       RejectReason* why = nullptr) noexcept;

    /// Slow path of the void send: retries until accepted; throws on non-retryable
    /// results (see Producer.h's send() doc for the exact exception per case).
    /// On BackPressured: if a pump hook is wired, it decides (see PumpResult) —
    /// Pumped retries immediately (no idle), DeclinedInCycle throws immediately
    /// (never spins, never waits out its own heartbeat), NotOwner falls back to
    /// the heartbeat-bounded wait below, same as "no hook wired" (Thread mode's
    /// ordinary app thread, or a bare ProducerImpl in a test). The ring's closed
    /// word is checked before EVERY pump attempt and in the wait loop, so a
    /// concurrent close() is decided immediately — never by waiting out the
    /// timeout (fix1 C2/I3).
    void sendBlocking(SendResult first, int32_t topicId, int32_t partition, const Key& key,
                      std::span<const uint8_t> value);

    /// F8c C6: the pieces of sendBlocking() for the token forms (Producer.cpp):
    /// one back-pressure wait step (pump on the duty thread / heartbeat-bounded
    /// idle elsewhere; throws exactly as sendBlocking does), and the exception
    /// for a non-retryable SendResult (Ok / BackPressured: no-op).
    struct BlockingWait {
        aeron::concurrent::BackoffIdleStrategy idle;
        int64_t                                startNs = 0;
    };
    [[nodiscard]] BlockingWait beginBlockingWait() const;
    void blockingWaitStep(BlockingWait& w);
    static void throwForSendResult(SendResult r);

    using ClockFn = int64_t (*)() noexcept;
    void setClockForTesting(ClockFn fn) noexcept { clock_ = fn; }

    /// Pump hook, wired by Client::producer() in BOTH runner modes (fix1 C1):
    /// called from sendBlocking() on BackPressured, checked AFTER the ring's
    /// closed word (never invoked once closed — the hook's context, the Client,
    /// may be destroyed by then). A plain function pointer + opaque context: no
    /// std::function, no virtual call, no new atomics (shared-nothing: this is
    /// set once, before any send(), by the single thread that owns the producer).
    ///
    /// fix2 Important-2: `dutyThreadId` is the Client's OWN duty-cycle thread id
    /// (the Invoker owner, or the Thread-mode runner), copied in BY VALUE here —
    /// it is immutable for the Client's whole lifetime once set (established
    /// before any producer() call can run), so this copy never goes stale and
    /// never requires touching the Client again to read it. sendBlocking()
    /// compares the CALLING thread against this copy BEFORE ever invoking `fn`:
    /// a thread that is not the duty thread (Thread mode's app thread; a
    /// foreign thread) is decided as NotOwner entirely inside ProducerImpl,
    /// without dereferencing `ctx` (the Client) — so it can never race the
    /// Client's destruction (the duty thread itself cannot race the destructor:
    /// the runner is joined before teardown in Thread mode, and the owner IS
    /// the destroyer in the Invoker off-owner-thread-destroy pattern).
    /// Task 14 fix1 I2: NOT noexcept — an exception thrown by the pumped duty
    /// cycle propagates out of send() exactly as it would out of doWork().
    using PumpFn = PumpResult (*)(void* ctx);
    void setPumpFn(PumpFn fn, void* ctx, std::thread::id dutyThreadId) noexcept {
        pumpFn_ = fn;
        pumpCtx_ = ctx;
        dutyThreadId_ = dutyThreadId;
    }

    /// Task 14 (spec §4.1): ingress mode.
    ///   Ring (default): app threads hand records to the duty-cycle thread
    ///     through this producer's MPSC ring (Thread mode — the runner drains it).
    ///   Direct (RunnerMode::Invoker): the owner thread IS the duty-cycle thread,
    ///     so trySubmit() appends straight into the partition builder — no ring,
    ///     no CAS, no second copy.  The records of one pass accumulate and leave
    ///     at the end of the pass (flushBatches), one frame per partition.
    enum class IngressMode : uint8_t { Ring, Direct };
    /// Direct ingress is the SAME pump hook / duty-thread binding as setPumpFn()
    /// (one mechanism, not two): `owner` becomes dutyThreadId_ (the only thread
    /// allowed to send; others get WrongThread), `pump`/`pumpCtx` become
    /// pumpFn_/pumpCtx_ (sendBlocking's way to run the owner's duty cycle; null
    /// = nothing can drain, so a back-pressured blocking send throws).
    struct DirectIngress {
        std::thread::id owner;
        PumpFn          pump    = nullptr;
        void*           pumpCtx = nullptr;
        int64_t         nowNs   = 0;  ///< duty-cycle clock now: the append clock until the first pass
    };
    /// Set once, before any send(), by the thread that creates the producer.
    /// Preallocates the deferred buffer (cfg.directDeferredBytes, clamped up to
    /// hold one max-size record) — the only allocation; may throw bad_alloc.
    void enableDirectIngress(const DirectIngress& d);
    [[nodiscard]] IngressMode ingressMode() const noexcept { return mode_; }

    /// Diagnostics / tests (owner thread): records + Tier-2 tokens deferred.
    [[nodiscard]] uint32_t deferredCount() const noexcept { return deferred_.live(); }
    /// Void deferred records failed by the delivery timeout (counted + logged).
    [[nodiscard]] uint64_t deferredTimedOutVoid() const noexcept { return deferredTimedOutVoid_; }

    /// fix1 minor 9: publish the ring's consumer-liveness heartbeat ONCE per
    /// duty cycle (called by ProducerHub::drainAndFlush before its drainRing()
    /// retry loop), not once per drainRing() call (which can run up to 256x/cycle
    /// per producer) — a relaxed store either way (see M2ORingBuffer), just
    /// cheaper to do it once.
    /// Task 14: no-op for a Direct producer (nobody waits on its ring).
    void publishHeartbeat(int64_t nowNs) noexcept {
        if (mode_ != IngressMode::Direct) ring_.consumerHeartbeatTime(nowNs);
    }

    /// fix1 C2/I3: mark this producer's ring closed. Called by Client::closeImpl_
    /// on every still-registered producer WHILE the Client is fully valid (before
    /// the pump hook's context, `this` Client, can ever dangle). Idempotent;
    /// safe to call from the duty-cycle thread only (matches close()'s contract).
    ///
    /// Task 14: a Direct producer has no ring traffic, and its closed state is
    /// PURE OWNER-THREAD state (a plain bool, no atomic): written here by
    /// closeImpl_ (Invoker: the owner thread, or the destroying thread after the
    /// app stopped driving — the same contract as doWork()), and by producer()'s
    /// lost-the-close-race path before the producer is handed to anyone.  Read
    /// only by the owner thread (trySubmit decides WrongThread first).
    void markRingClosed() noexcept {
        if (mode_ == IngressMode::Direct) {
            directClosed_ = true;
        } else {
            ring_.close();  // F8c C1(a): linearizable with every tryClaim()
        }
    }

    /// Write a flush trigger into the ring.  Zero-alloc.
    void submitFlush() noexcept;

    // ---- App-thread API (Tier-2 explicit batch) --------------------------------

    /// Submit a pre-built Tier-2 batch (from BatchBuilder::send()).
    ///
    /// Moves the batch into a heap Tier2Entry and writes its address into the
    /// ring (kMsgTier2Batch); ownership passes to the agent on commit.  The agent
    /// processes the token in order with Tier-1 records, stamping baseSequence.
    ///
    /// ackHandle: an ackhandle:: tagged token (0 = none).  recordCount: records
    /// in the pre-built batch.  batchBytes: pre-built RecordBatch bytes
    /// (baseSequence field = 0; patched by the agent) — MOVED FROM only when
    /// accepted (F8c C6: a rejected batch stays with the caller for a retry).
    ///
    /// fix4: returns RejectReason::None when accepted; otherwise why not.
    RejectReason submitTier2Batch(int32_t topicId,
                                  int32_t partition,
                                  uint64_t ackHandle,
                                  int32_t recordCount,
                                  std::vector<uint8_t>& batchBytes);

    // ---- F8c C6: ack routing (setup phase; read on the duty thread) ----------

    /// The Client's seams (both runner modes; absent in bare unit rigs):
    ///   cqDeliver — duty side: deliver one producer completion (per-record /
    ///               Tier-2 ack, BatchAck) to registry slot `ref` (generation
    ///               check + producer-op accounting, see Client).  Absent: CQ
    ///               completions are dropped.
    ///   sink      — duty side: the begin-close token (a generic target).
    ///   issueClose — app side: begin-close (target != nullptr) or
    ///               closeImmediately (target == nullptr): validate + reserve,
    ///               close the intake, apply on the duty thread or submit the
    ///               lifecycle command.  Absent (bare rig): the caller is the
    ///               duty thread and it is applied in place.
    ///   andWait   — the blocking close() (Client::andWait_).
    struct Hooks {
        void* ctx = nullptr;  ///< the Client (identity for Completions::client())
        void (*cqDeliver)(void* ctx, CqRef ref, const Completion& c) noexcept = nullptr;
        detail::CompletionSink sink{};
        OpResult (*issueClose)(void* ctx, ProducerImpl& impl, const CompletionTarget* target,
                               Token token) = nullptr;
        detail::AndWaitSink andWait = nullptr;
        /// F8c C7: the handle's §3.3 release (HubCommand::HandleRelease).
        /// Absent (bare rig): no duty side — the handle frees the impl alone.
        detail::HubCommandSink release = nullptr;
    };
    /// Set once by Client::producer() before the handle is returned.
    void setHooks(const Hooks& h) noexcept { hooks_ = h; }
    [[nodiscard]] const Hooks& hooks() const noexcept { return hooks_; }

    /// Setup phase: the per-record ack handler (AckToken sends).  Duty thread
    /// reads it (the first record that needs it was sent after this write:
    /// ring order publishes it).
    void setOnAck(AckFn fn, void* ctx) noexcept {
        onAckFn_  = fn;
        onAckCtx_ = ctx;
    }
    /// Setup phase: per-batch acks to a handler (exclusive with the CQ form).
    /// Throws std::logic_error after a send / when the CQ form is set.
    void setOnBatchAck(BatchAckFn fn, void* ctx);
    /// Setup phase: per-batch acks into a Completions (registry ref `ref`); every
    /// send then reserves one credit unit on `cq` (Producer.cpp).  Throws
    /// std::logic_error after a send / on a second call / when the handler form
    /// is set.
    void setBatchAckCq(Completions* cq, CqRef ref);
    /// App side: the batch-ack queue whose credit every send reserves (nullptr: none).
    [[nodiscard]] Completions* batchAckCq() const noexcept { return batchAckCq_; }

    /// fix2 minor / fix3 I3: user-callback exceptions caught on the duty thread.
    /// F8c C6: the ack and batch-ack handlers are noexcept function pointers
    /// (no try/catch on the hot path), so nothing is counted any more — kept for
    /// the existing read-safety contract and tests (always 0).
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept { return callbackExceptions_; }

    // ---- F8c C6a: close (rich-state lifecycle) ----------------------------------
    //
    //   Open ──begin-close──> Draining{target, token, deadline}
    //   Draining ──every accepted record completed AND ring consumed to the
    //              closed tail──> Closed            (token Ok)
    //   Draining ──deadline (deliveryTimeoutMs)──> Closed   (token Timeout)
    //   Open / Draining / Closed ──closeImmediately / failClose / Client close──>
    //              Discarding{status}               (token Closed; every record
    //              not yet acknowledged, and every one read later from the
    //              closed ring, completes `status` exactly once)
    //
    // Closed: the intake is closed and the token (if any) completed; records
    // still in flight complete normally.

    /// App side: begin-close (Producer::close(target, token)).
    OpResult beginClose(const CompletionTarget& target, Token token);
    /// App side: Producer::closeImmediately().
    OpResult closeImmediately();

    /// Close the INTAKE (any thread; Ring: the tail-bit CAS, linearizable with
    /// every claim; Direct: owner-local).  True iff THIS call closed it.
    bool closeIntake() noexcept;
    /// Duty side: begin draining (the intake was closed by the issuer).  The
    /// target is already pinned by the Client.
    void applyBeginClose(const detail::OpTarget& target, Token token, int64_t nowNs);
    /// Duty side: closeImmediately (status Closed) / the duty-thread failure
    /// path (any status): tail-close the intake (idempotent), complete a pending
    /// begin-close token Closed, fail every record not yet acknowledged with
    /// `st`, and complete every record later read from the closed ring with
    /// `st`.  Idempotent.
    void applyCloseImmediately(OpStatus st);
    /// Alias for the duty-thread failure path (T15: Aeron client closed).
    void failClose(OpStatus st) { applyCloseImmediately(st); }
    /// Duty side, once per cycle while not Open: Draining's completion /
    /// deadline check.  Returns work done.
    int closeDuty(int64_t nowNs);
    /// Duty side, Client close (after the ring is closed): fail everything with
    /// `st` and drain the closed ring up to its tail — waiting past records
    /// claimed but not yet committed, bounded by the close-drain unblock rule
    /// (yield cap, then Aeron's unblock(); a hang is worse than the documented
    /// shutdown loss of a writer stalled for the whole cap).
    void closeFinal(OpStatus st, int stalledYieldsBeforeUnblock = 1 << 20);
    /// True once Client close completed this producer (closeFinal): Ring — the
    /// ring's consumer-finished word (any thread); Direct — owner state.
    [[nodiscard]] bool clientClosed() const noexcept;
    [[nodiscard]] bool lifeOpen() const noexcept { return std::holds_alternative<LifeOpen>(life_); }
    [[nodiscard]] bool draining() const noexcept { return std::holds_alternative<LifeDraining>(life_); }
    [[nodiscard]] bool discarding() const noexcept { return std::holds_alternative<LifeDiscarding>(life_); }

    // ---- Test seams (F8c C6a): split a SEND_RECORD claim from its commit -------
    /// Claims + writes a SEND_RECORD WITHOUT committing it (Ring mode).  Returns
    /// the claim index (< 0: not claimed — back-pressure / closed).
    [[nodiscard]] aeron::util::index_t testClaimSendRecord(int32_t topicId, int32_t partition,
                                                           const Key& key,
                                                           std::span<const uint8_t> value,
                                                           uint64_t ackHandle) noexcept;
    void testCommitClaim(aeron::util::index_t idx) { ring_.commit(idx); }
    /// Ring head / (closed) tail positions (diagnostics / tests).
    [[nodiscard]] int64_t testRingHead() const noexcept { return ring_.headPosition(); }
    [[nodiscard]] int64_t testRingTail() const noexcept { return ring_.tailPosition(); }

    // ---- Agent-thread API (single-writer) ------------------------------------

    /// Drain up to `messageLimit` ring messages, appending records to builders.
    /// Does NOT flush — call flushBatches() after.  The drain stops EARLY (see
    /// lastDrainStop()) at a batch boundary that only a flush can resolve:
    ///   * a flush() token (consumed — records after it must not join the
    ///     batches it flushes);
    ///   * a record its partition cannot take yet (builder at batchBytes /
    ///     batchRecords / the frame cap, or a Tier-2 batch queued ahead of it):
    ///     the record stays UNCONSUMED in the ring (ABORT, in-order
    ///     back-pressure), so no builder ever grows past a sendable frame.
    /// nowNs: current time (NanoClock, sampled once per doWork cycle).
    /// Returns the number of ring messages consumed.
    int drainRing(int64_t nowNs, int messageLimit = 1);

    /// F12: why the last drainRing() stopped (duty thread only).
    enum class DrainStop : uint8_t {
        Drained,    ///< ring empty or messageLimit reached (Direct: always)
        FlushToken, ///< consumed a flush() token: flush now, then drain on
        Blocked,    ///< a record waits for a flush (ABORT; left in the ring)
    };
    [[nodiscard]] DrainStop lastDrainStop() const noexcept { return lastDrainStop_; }

    /// Sink concept: `bool operator()(int32_t topicId, int32_t partition,
    /// int32_t ingressStreamId, const uint8_t* buf, std::size_t len) const`.
    /// Explicitly instantiated in ProducerImpl.cpp for SendFn (tests, cold
    /// paths), agent::AgentProduceSink (production) and CountingSink (benches);
    /// no std::function is built or called on the production flush path.
    ///
    /// F12: examines ONLY the dirty partitions (those with builder records or
    /// queued Tier-2 batches) — O(dirty), never O(partitions); a partition
    /// leaves the dirty list once it has no work left.
    template <class Sink>
    int flushBatches(int64_t nowNs, int64_t sessionId, const Sink& sink);

    template <class Sink>
    int retransmitStalled(int64_t nowNs, const Sink& sink);

    /// Called by the agent when a ProduceResponse arrives.  errorCode 0 completes
    /// the batch's ack-ring slice (record i -> baseOffset + i) and reports its
    /// BatchAck; baseOffset < 0 with errorCode 0 is a known-old duplicate (Kafka
    /// DUPLICATE_SEQUENCE parity: the batch IS in the log, its offset is unknown)
    /// — every record completes Ok with offset -1.  errorCode != 0 fails the
    /// batch, except OutOfOrderSequence on a batch that is not the oldest live
    /// batch of its epoch: an earlier frame was lost, so it is parked and re-sent
    /// once the older batches resolve (Kafka parity).
    void onProduceResponse(int64_t correlationId, int64_t baseOffset, int16_t errorCode);

    /// Called on redirect-reconnect failure (SessionLost) and by the close paths
    /// (duty-cycle thread, or after it stopped): fails AND drops every accepted
    /// record — in the app ring (committed so far), the deferred arena, a
    /// builder, queued Tier-2, or in flight — with `st`, exactly once.
    /// noexcept (fix round 2, N1): run from ~DutyScope (the deferred fail-all
    /// of a close applied inside a handler).  Everything it calls is non-
    /// throwing (handlers are noexcept function pointers; no allocation).
    void failAllPending(OpStatus st) noexcept;

    /// Duty-cycle thread: fail every record of an in-flight batch (wire id)
    /// with DeliveryFailed.  Non-batch ids are ignored (they can come from any
    /// ErrorResponse).
    void failBatch(int64_t batchCorrId);

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
    /// F8c C7 fix round 2 (F2): the ordinal plus its sequence base — past the
    /// highest sequence any previous producer on this ordinal used (the hub
    /// carries it forward) — so a late response for a previous incarnation
    /// never matches one of this producer's batches.
    void setOrdinal(uint32_t o, uint64_t seqBase) noexcept {
        ordinal_ = o;
        seqBase_ = seqBase;
        for (auto& up : partitions_) up->inflight.rebase(seqBase_);  // created before adoption
    }
    /// Cold (ProducerHub::remove): one past the highest sequence any of this
    /// producer's partitions ever claimed (at least its own base).
    [[nodiscard]] uint64_t sequenceEnd() const noexcept {
        uint64_t end = seqBase_;
        for (const auto& up : partitions_) end = up->inflight.endSeq() > end ? up->inflight.endSeq() : end;
        return end;
    }
    /// Test seam (F2): start every partition's sequences at `base` (before the
    /// producer's first batch) — stands in for a long-lived producer.
    void testRebaseSequences(uint64_t base) noexcept {
        seqBase_ = base;
        for (auto& up : partitions_) up->inflight.rebase(base);
    }
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
    /// batches to get there (partitions with nothing in flight adopt it).
    /// Never called from production code.
    void debugSetEpochForTest(int16_t epoch) noexcept {
        producerEpoch_ = epoch;
        for (auto& up : partitions_) {
            if (up->inflight.empty()) up->epoch = epoch;
        }
    }

    /// Test-only seam (Task 14 fix2): lower the partition-table cap (default
    /// batchcorr::kMaxSlot + 1) so the "unaddressable partition" path can be
    /// exercised without creating 262144 partitions. Never called in production.
    void debugSetMaxPartitionsForTest(std::size_t n) noexcept { maxPartitions_ = n; }
    /// Task 14 fix2: accepted deferred records that could not be applied (and
    /// were failed through their handle / counted + logged if void). Owner thread.
    [[nodiscard]] uint64_t deferredApplyFailures() const noexcept { return deferredApplyFailures_; }

    // ---- Task 14 fix3 test seams (owner thread; never used in production) ----
    /// Parses the whole deferred arena and cross-checks it against the live
    /// count, the unknown-partition counters and every partition's deferred
    /// counters.  true = consistent.
    [[nodiscard]] bool debugArenaConsistent() noexcept;
    /// The NEXT apply pass's apply ATTEMPTS (Tier-1 records it tries to append,
    /// numbered from 1; records kept without an attempt are not counted) in
    /// [from, from + count) fail as if appendRecord_ threw (the loss path).
    /// from = 0: off.
    void debugFailDeferredApplies(uint32_t from, uint32_t count) noexcept {
        debugFailAt_    = from;
        debugFailCount_ = count;
    }
    /// Apply passes run so far.
    [[nodiscard]] uint64_t debugDeferPasses() const noexcept { return deferPasses_; }
    [[nodiscard]] std::size_t debugDeferPartitionCap() const noexcept { return deferPartitionCap_; }
    /// F12: partitions examined by flushBatches so far (owner/duty thread; a
    /// plain counter — the per-cycle flush must scale with dirty partitions).
    [[nodiscard]] uint64_t debugFlushVisits() const noexcept { return flushVisits_; }

private:
    // ---- Ring write helpers --------------------------------------------------

    bool writeToRing(int32_t msgTypeId,
                     const uint8_t* payload,
                     std::size_t len) noexcept;

    // ---- Agent-side helpers --------------------------------------------------

    PartitionState& getOrCreatePartition(int32_t topicId, int32_t partition);

    /// F12: put `ps` on the dirty list (idempotent).  Never allocates: dirty_'s
    /// capacity is reserved with the partition table (each slot appears once).
    void markDirty_(PartitionState& ps) noexcept {
        if (!ps.dirty) {
            ps.dirty = true;
            dirty_.push_back(ps.slotIndex);
        }
    }

    /// Closed state for this producer's ingress mode (Direct: owner-thread bool;
    /// Ring: the ring trailer's closed word).
    [[nodiscard]] bool closed_() const noexcept {
        return mode_ == IngressMode::Direct ? directClosed_ : ring_.closed();
    }
    /// Task 14: trySubmit's Direct path (owner thread; no ring, no atomic).
    /// Ring-mode body of trySubmit (unchanged hot path).
    SendResult trySubmitRing_(int32_t topicId, int32_t partition, const Key& key,
                              std::span<const uint8_t> value, int64_t ackHandle) noexcept;
    SendResult tryAppendDirect_(int32_t topicId, int32_t partition, const Key& key,
                                std::span<const uint8_t> value, int64_t ackHandle,
                                RejectReason& why) noexcept;

    /// One record into its partition builder — the single append path shared by
    /// the ring drain (Ring) and trySubmit (Direct).  Owner/duty thread only.
    /// ABORT = back-pressure: the record was NOT appended (builder at its cap and
    /// the window full, or — Direct, where no flush runs between sends — builder
    /// at its cap at all; or the frame-cap pre-flight).  Ownership of a wantAck
    /// handle passes to the producer only on CONTINUE.
    rune::client::detail::ControlledAction appendRecord_(int32_t topicId, int32_t partition,
        const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* valBytes, uint32_t valueLen,
        uint8_t flags, uint64_t ackHandle, int64_t nowNs);

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

    /// EOS-OOS: OutOfOrderSequence on a batch with an older live batch of its
    /// epoch — park it (no completion, no epoch bump) and fast-retransmit the
    /// older batches whose last offer was not after its own (the lost frame).
    void parkOutOfOrder_(PartitionState& ps, InFlightBatch& b) noexcept;
    /// An OK for a later batch of the same identity proves this one is stored
    /// (the broker appends sequences contiguously): its own answer is pending.
    [[nodiscard]] static bool storedByAck_(const PartitionState& ps, const InFlightBatch& b) noexcept {
        return b.baseSequence >= 0 && b.producerId == ps.producerId && b.producerEpoch == ps.epoch &&
               b.baseSequence + b.recordCount - 1 <= ps.lastAckedSeq;
    }
    /// A parked batch that was re-sent (or resolved) is no longer parked.
    static void unpark_(PartitionState& ps, InFlightBatch& b) noexcept {
        if (b.oosParked) {
            b.oosParked = false;
            --ps.parkedCount;
        }
    }
    /// Releases a resolved batch's slot; when the oldest live batch left is a
    /// parked one, its older batches all resolved: arm resendParked.
    void releaseResolved_(PartitionState& ps, InFlightBatch& b) noexcept;
    /// flushBatches: re-send every parked batch of `ps` oldest-first (stored
    /// frames).  False on transport back-pressure (retried next pass).
    template <class Sink>
    bool resendParked_(PartitionState& ps, int64_t nowNs, const Sink& sink);

    [[nodiscard]] bool canFlight_(const PartitionState& ps) const noexcept {
        return static_cast<int32_t>(ps.inflight.live()) < cfg_.maxInFlight && !ps.inflight.full();
    }
    /// ackTail = start of the oldest un-acked slice (in-flight or current builder).
    void reclaimAckRing_(PartitionState& ps) noexcept;
    /// Reads up to `limit` committed messages of the app->agent ring and
    /// completes each record / Tier-2 batch with `st` (its ack handle, and a
    /// one-unit BatchAck) — never sends it.  Frees Tier-2 entries.  Returns
    /// messages read.
    int discardRing_(OpStatus st, int limit);
    void pushAck_(PartitionState& ps, uint64_t handle);
    /// F8c C6: deliver ONE ack handle's completion (0 = void: nothing) —
    /// the onAck handler, or the Client's CQ registry.  Duty thread; never
    /// allocates, never throws.
    void completeHandle_(uint64_t h, OpKind op, int64_t value, int64_t aux, OpStatus st,
                         uint16_t errorCode) noexcept;
    /// Completes the handles of slice [ackStart, ackStart+ackCount): record i ->
    /// baseOffset + i (Ok), or -1 with `st` / `errorCode`.  `tier2`: the slice
    /// is one Tier-2 batch (op Tier2Batch, aux = recordCount).  Zeroes each
    /// slot so nothing completes twice.
    void completeSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount, int64_t baseOffset,
                        OpStatus st, uint16_t errorCode, bool tier2, int32_t recordCount) noexcept;
    /// acks=None: completes the just-sent builder slice with -1 ("sent, offset
    /// unknown") and reclaims it — nothing waits for a response.
    void completeSentUnacked_(PartitionState& ps) noexcept;
    // ---- EOS follow-ups I-4: Kafka's idempotent-producer sequence rules ------
    // (TransactionManager.maybeResolveSequences / requestEpochBumpForPartition /
    // startSequencesAtBeginning, RecordAccumulator.shouldStopDrainBatchesFor-
    // Partition), per partition — the exact rule set is in ProducerImpl.cpp,
    // "Sequence resolution".
    /// A sequenced batch of the partition's identity failed with its broker
    /// outcome unknown: the partition holds new batches until it is resolved.
    void markUnresolved_(PartitionState& ps, const InFlightBatch& b) noexcept;
    /// With nothing in flight: an unresolved partition is resolved — kept (every
    /// sequence it sent was acked after all) or reset to a new epoch at 0.
    void maybeResolve_(PartitionState& ps) noexcept;
    /// The partition moves to a new identity at sequence 0: a producer epoch
    /// bump if it is on the current one (else it adopts the current one).
    /// Epoch overflow: at INT16_MAX a FRESH unique producerId is drawn (never
    /// the old id+1 — see generateProducerId()) and the epoch restarts at 0.
    void resetPartition_(PartitionState& ps) noexcept;
    /// resetPartition_ + every live batch re-stamped (frame + header + CRC) in
    /// order from sequence 0 and queued for re-send (parked), new attempts.
    void resequence_(PartitionState& ps) noexcept;
    /// Re-stamp a stored frame's attempt tag (MessageHeader correlationId).
    static void stampAttempt_(InFlightBatch& b, uint8_t tag) noexcept;
    /// One offer of a live batch's stored frame as a NEW attempt (its earlier
    /// attempts' error answers are ignored from now on).  Reverted when the
    /// transport refuses it.
    template <class Send>
    bool offerAttempt_(PartitionState& ps, InFlightBatch& b, int64_t nowNs, Send&& send);
    /// One BatchAck to the batch-ack target (handler or CQ; none: nothing).
    /// `units`: the batch-ack credit units it returns (records of a Tier-1
    /// batch; 1 for a Tier-2 batch / a single never-sent record).
    void emitBatchAck_(int32_t topicId, int32_t partition, int32_t baseSequence, int32_t recordCount,
                       int64_t baseOffset, int16_t errorCode, OpStatus st, uint32_t units,
                       int16_t producerEpoch, int64_t producerId = -1) noexcept;
    /// Records that never reached a batch (failed in the ring / a builder /
    /// the Tier-2 queue / the arena): a BatchAck{baseSequence -1, kClientFailure}.
    void emitUnsentBatchAck_(int32_t topicId, int32_t partition, int32_t recordCount, OpStatus st,
                             uint32_t units) noexcept {
        emitBatchAck_(topicId, partition, -1, recordCount, -1, BatchAck::kClientFailure, st, units, -1);
    }
    /// Fails a live batch's records and releases its slot.
    void failBatch_(PartitionState& ps, InFlightBatch& b, OpStatus st);
    /// Fails the never-sent builder records of `ps` (handles + one BatchAck).
    void failBuilder_(PartitionState& ps, OpStatus st);
    /// Draining: every accepted record completed and the ring consumed to the
    /// closed tail.
    [[nodiscard]] bool fullyDrained_() const noexcept;
    /// Complete the begin-close token (if Draining) with `st`; life -> next.
    void completeCloseToken_(OpStatus st);
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

    // F8c C0: a flush() requested ON the duty thread (inside a callback) while
    // the ring is full cannot wait for ring room (it would wait on itself).  It
    // is recorded here instead and consumed by the next flushBatches() pass.
    // Duty-thread state only: written by submitFlush() on the duty thread and
    // read/cleared by flushBatches() on the same thread.  (Replaces the
    // cross-thread std::atomic<bool> globalFlushPending_ latch.)
    bool dutyFlushPending_ = false;

    // ---- Instrumentation counters (agent-thread only — plain uint64_t) -------
    // Single-writer (agent thread): no atomics needed per shared-nothing law.
    uint64_t framesSent_  = 0; // total Aeron frames offered to transport
    uint64_t recordsSent_ = 0; // total records drained from the ring
    uint64_t flushVisits_ = 0; // F12: partitions examined by flushBatches (debugFlushVisits)

    // ---- Own ring buffer (MPSC: many app threads write, one agent thread reads)
    // ProducerImpl owns its ring so SEND_RECORD messages are separate from the
    // ClientAgent command ring (agent command type IDs 1-4 are unrelated).
    static constexpr std::size_t kRingAlignment = 64; // cache-line aligned
    // F8c C7 (perf): the fields EVERY app-thread send reads — the ring object
    // (buffer pointer, capacity, index words), the ingress mode and the
    // batch-ack queue pointer — sit on cache lines of their own, written only
    // at setup.  Next to a field the duty thread writes per record
    // (recordsSent_, lastNowNs_) each send paid a cross-core miss (false
    // sharing that depended on member layout).
    alignas(64) std::unique_ptr<uint8_t, void(*)(void*)> ringBuf_;
    rune::client::detail::M2ORingBuffer      ring_;
    IngressMode  mode_       = IngressMode::Ring;  // set once before any send (Task 14)
    Completions* batchAckCq_ = nullptr;            // app side: reserve one unit per send (C6)

    // ---- Agent-thread only (no lock needed — single-writer) ------------------

    // Flat partition table (spec §1a.5): FlatIndexMap key -> dense slot, slots own
    // their PartitionState (stable address; slotIndex == position).
    alignas(64) detail::FlatIndexMap              partitionIndex_;
    std::vector<std::unique_ptr<PartitionState>>  partitions_;
    // F12: slots of the partitions with flush work (PartitionState::dirty), in
    // the order they became dirty.  capacity() >= partitions_.size() always
    // (reserved in getOrCreatePartition), so markDirty_ never allocates.
    std::vector<uint32_t>                         dirty_;
    DrainStop                                     lastDrainStop_ = DrainStop::Drained;

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
    // EOS follow-ups: +1 per frame offer (first send or re-send) — orders sends
    // against answers without clocks (fast retransmit; OOS-vs-ack ordering).
    uint64_t sendTick_ = 0;

    // ---- In-flight batch tracking (agent-thread only; no mutex needed) -------
    // In-flight batches live in each PartitionState::inflight ring; a batch
    // correlation id encodes (ordinal_, partition slot, ring sequence).
    uint32_t ordinal_ = 0;
    uint64_t seqBase_ = 0;  // F8c C7 F2: the ordinal's carried-forward sequence base
    int64_t  orphanFence_ = 0;  // F8c C7 F1: written by the handle before its orphan RMW
    // Lower bound of every live batch's attemptDeadlineNs: retransmitStalled()
    // returns in O(1) until it passes (spec §1a.5; replaces the ordered map front).
    int64_t earliestAttemptDeadlineNs_ = INT64_MAX;

    // ---- F8c C6: ack targets (setup phase; read on the duty thread) ------------
    Hooks        hooks_{};
    AckFn        onAckFn_       = nullptr;
    void*        onAckCtx_      = nullptr;
    BatchAckFn   batchAckFn_    = nullptr;
    void*        batchAckCtx_   = nullptr;
    CqRef        batchAckCqRef_ = kNoCqRef;   // duty side: the BatchAck queue's registry ref
    // fix2 minor: plain counter (see testCallbackExceptionCount()).
    uint64_t     callbackExceptions_ = 0;

    // ---- F8c C6a: lifecycle (duty-thread state; rich states) ---------------------
    struct LifeOpen {};
    struct LifeDraining {
        detail::OpTarget target{};
        Token            token      = 0;
        int64_t          deadlineNs = 0;
    };
    struct LifeClosed {};
    struct LifeDiscarding {
        OpStatus status = OpStatus::Closed;
    };
    std::variant<LifeOpen, LifeDraining, LifeClosed, LifeDiscarding> life_{};
    // Duty side: the intake is closed and the duty side knows it (Draining /
    // Closed / Discarding) — every pass flushes at once (no linger: nothing
    // more can join a batch).
    bool flushAll_ = false;
    // F8c fix round 1 (I1): a closeImmediately / failClose applied INSIDE this
    // producer's own duty work (from a handler it fired) — the intake is closed
    // and the life is Discarding at once, but the fail-all (and the aborted
    // begin-close token) waits for the outermost DutyScope's end.
    struct PendingFail {
        bool         armed    = false;
        OpStatus     status   = OpStatus::Closed;
        bool         hadToken = false;
        LifeDraining drain{};
    };
    PendingFail pendingFail_{};
    void runPendingFail_() noexcept;
    bool clientClosedDirect_ = false;  // Direct only: closeFinal ran (owner state)

    // ---- sendBlocking() support (app thread only) -----------------------------
    static int64_t steadyNowNs_() noexcept;
    ClockFn clock_   = &steadyNowNs_;   // overridable by setClockForTesting()
    PumpFn  pumpFn_  = nullptr;         // wired by Client::producer(), both modes (fix1 C1)
    void*   pumpCtx_ = nullptr;
    // fix2 Important-2: the Client's duty-cycle thread id, copied in by
    // setPumpFn(); set-once / immutable thereafter. Compared against the
    // calling thread BEFORE pumpFn_ is ever invoked, so a non-duty thread
    // never touches pumpCtx_ (the Client) at all.
    std::thread::id dutyThreadId_{};

    // ---- Task 14: ingress mode (set once before any send; owner thread only) --
    bool        directClosed_ = false;  // Direct only: see markRingClosed()
    // Duty-cycle clock of the last drainRing()/flushBatches() (Direct appends
    // stamp batchStartNs with it — the clock is sampled per pass, never per send).
    int64_t     lastNowNs_ = 0;

    // ---- Task 14 fix1 C1: re-entrancy deferral (duty/owner thread only) -------
    // inDuty_ is set (RAII, nesting-safe) while this producer walks / resets its
    // own partition state and fires user callbacks from it: flushBatches,
    // retransmitStalled, onProduceResponse, sweepTimedOut, failAllPending,
    // failBatch, retryRedirectedProduce, resendAll*.  A Direct send made while it
    // is set goes to deferred_ and is applied by applyDeferred_() when the
    // OUTERMOST scope ends (fix2: per-partition; outside a scope, a send to a
    // partition with deferred records applies them inline first).
    std::size_t  maxPartitions_ = static_cast<std::size_t>(batchcorr::kMaxSlot) + 1;
    uint64_t     deferredApplyFailures_ = 0;
    bool         inDuty_ = false;
    DeferredArena deferred_;
    uint32_t     deferredUnknown_ = 0;   // deferred records for not-yet-created partitions
    std::size_t  deferredUnknownBytes_ = 0;  // fix3: their arena bytes (aggregate cap = deferPartitionCap_)
    std::size_t  deferPartitionCap_ = 0; // per-partition arena share (bytes)
    uint32_t     deferPass_ = 0;         // apply-pass id (PartitionState::deferBlockedPass)
    // A scan holds arena offsets: push() must not compact.  fix3 (minor 5):
    //  * During an APPLY pass no push can happen at all — no user code runs
    //    inside it (lost handles are completed after the slide-down), and the
    //    stale gap [w, r) is always covered by one dead filler record — so the
    //    free space in [w, end) is never needed by a concurrent push.
    //  * During the fail / expire scans (failDeferred_, expireDeferred_) user
    //    callbacks DO run, but those scans only kill records in place (no
    //    gap); a push from a callback appends at the tail without compacting
    //    (and back-pressures if the tail is full) — the dead space is reclaimed
    //    by the next pass / non-scanning push.  Reusing [w, end) is therefore
    //    neither needed nor attempted.
    bool         deferScanning_ = false;
    uint64_t     deferredTimedOutVoid_ = 0;
    uint64_t     deferPasses_ = 0;           // apply passes run (diagnostics / tests)
    uint32_t     debugFailAt_ = 0;           // test seam: see debugFailDeferredApplies
    uint32_t     debugFailCount_ = 0;

    struct DutyScope {
        ProducerImpl& p;
        bool          prev;
        explicit DutyScope(ProducerImpl& pi) noexcept : p(pi), prev(pi.inDuty_) { p.inDuty_ = true; }
        ~DutyScope() {
            if (!prev) {
                // F8c fix round 1 (I1): a close applied from a handler this
                // producer fired runs its fail-all HERE — after the walk that
                // fired the handler finished (no live batch / ring read / slot
                // is ever failed twice) — and before the deferred records are
                // applied (they are failed with everything else).
                while (p.pendingFail_.armed) p.runPendingFail_();
                if (!p.deferred_.empty()) p.applyDeferred_();
                while (p.pendingFail_.armed) p.runPendingFail_();  // a lost-record handler closed
                p.inDuty_ = false;
            }
        }
        DutyScope(const DutyScope&) = delete;
        DutyScope& operator=(const DutyScope&) = delete;
    };
    /// One read/write-cursor pass over the arena (records present on entry):
    /// each record is applied through appendRecord_ unless its partition
    /// ABORTed earlier in this pass — then it (and every later record of that
    /// partition) is kept, slid forward.  Per-partition order is preserved; no
    /// partition blocks another.  Records deferred by callbacks fired from here
    /// wait for the next pass.  Never throws: a record that cannot be applied
    /// is failed via its handle (void: counted + logged) and dropped.
    void applyDeferred_() noexcept;
    /// failAllPending: fail + drop every record deferred before the call.
    void failDeferred_(OpStatus st);
    /// sweepTimedOut: fail deferred records accepted >= deliveryNs ago.
    void expireDeferred_(int64_t nowNs, int64_t deliveryNs);
    /// Accept one record / Tier-2 token into the arena (inDuty_).  Validates the
    /// partition first (an unaddressable one is InvalidTopic at send time).
    SendResult deferRecord_(int32_t topicId, int32_t partition, std::span<const uint8_t> key,
                            std::span<const uint8_t> value, uint8_t flags, int64_t ackHandle,
                            RejectReason& why) noexcept;
    RejectReason deferTier2_(std::unique_ptr<Tier2Entry>& entry) noexcept;
    uint8_t* pushDeferred_(const DeferredHdr& h) noexcept;
    PartitionState* findPartition_(int32_t topicId, int32_t partition) noexcept;
    /// Removes a record's per-partition / unknown accounting.
    void unaccountDeferred_(const DeferredHdr& h) noexcept;
    /// The record's ack handle (record: wantAck handle; Tier-2: takes + frees
    /// the entry and returns its handle).  0 = void.
    uint64_t takeDeferredHandle_(std::size_t off, bool* tier2 = nullptr,
                                 int32_t* recordCount = nullptr) noexcept;
    /// Outside inDuty_: run the apply pass if `ps`'s (or unknown partitions')
    /// deferred records must go first.  Returns false if this partition still
    /// has deferred records afterwards (the new send must not overtake them).
    bool settleDeferredFor_(int32_t topicId, int32_t partition) noexcept;
    void noteDeferredLoss_(const char* what) noexcept;
};

} // namespace rune::client::producer
