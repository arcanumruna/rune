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
#include <rune/client/producer/CoordinatorLink.h>
#include <rune/client/producer/InFlightRing.h>
#include <rune/client/producer/ProduceFrameBuilder.h>
#include <flight_recorder/FlightRecorder.h>
#include <rune/client/detail/M2ORingBuffer.h> // internal MPSC ring (controlledRead/ABORT, templated read)
#include <rune/client/detail/OneToOneRingBuffer.h> // Tier-2 free-slot ring (duty -> app)
#include <rune/client/detail/StatusRing.h>       // FD9: identity snapshots (duty -> app handle)

#include <cassert>
#include <thread>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
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
inline constexpr int32_t kMsgClaimBatch  = 4; // Phase 2b B: claimBatch — n records, ONE RecordBatch
inline constexpr int32_t kMsgTxnControl  = 5; // Phase 2b F: a transaction control op, FIFO with the records before it

// TXN_CONTROL layout (kMsgTxnControl payload, little endian, Phase 2b F Task 8):
//   [0]      kind          : u8  (TxnControl::Kind)
//   [1..3]   pad
//   [4..7]   groupIdLen    : u32
//   [8..15]  token         : u64 (HubCommand word a)
//   [16..23] target word b : i64 (detail::encodeTarget)
//   [24..31] target word c : i64
//   [32..35] offsetCount   : u32
//   [36..39] generationId  : i32 (#125: the consumer-group member's generation; -1 = none, the legacy form)
//   [40..43] memberIdLen   : u32 (#125)
//   [44..47] zero
//   [48..]   groupId bytes; then, from the next multiple of 8 (zero padded), memberId bytes; then, from the next
//            multiple of 8, offsetCount x 16-byte entries (topicId i32, partition i32, offset i64) - 8-aligned, so
//            the duty side reads them in place.
// A SendOffsets whose counts are over the caps (kMaxTxnOffsets / a 1..255-byte group id / a member id longer than
// kMaxGroupMemberIdBytes) travels as the 48-byte header ONLY: the duty side completes it InvalidArgument without
// reading any data.
inline constexpr std::size_t kTxnCtlHeaderBytes = 48;

// CLAIM_BATCH layout (kMsgClaimBatch payload): [0..33] exactly as SEND_RECORD
// (topicId, partition, keyLen, inline key, flags = 0, ackHandle = 0), then
//   [34..37] n      : uint32 LE (records)
//   [38..41] recLen : uint32 LE (value bytes per record)
//   [42..]          : n x recLen value bytes, contiguous
inline constexpr int kBatchCountOff   = 34;
inline constexpr int kBatchRecLenOff  = 38;
inline constexpr int kBatchRecsOff    = 42;
inline constexpr int kBatchHeaderSize = 42;

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
// ackRing slot, Tier2Slot::ackHandle).  0 = no ack (void record).  Otherwise
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

// kMsgTier2Batch token: 8 bytes = the pooled Tier2Slot's index (int64 LE, in-process).
// The slot's ownership travels through the ring (spec §1a.2) — no side queue, no
// mutex, no heap: the slot returns to the app over the SPSC free ring (tier2Free_).
inline constexpr int kT2TokenSize = 8;

/// "No Tier-2 slot" (an empty FIFO link, a builder that could not get a slot).
inline constexpr uint32_t kNoTier2Slot = 0xFFFF'FFFFu;

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
// Tier2Slot - one preallocated Tier-2 batch.  The pool (ProducerImpl::tier2Pool_)
// is sized ONCE at construction; a slot's RecordBatchBuilder body and its
// RecordBatch bytes keep their capacity for ever (tier2SlotBytes), so building and
// sending a batch allocates nothing.
//
// Ownership moves app -> duty -> app and is NEVER shared: the app thread owns a
// slot from tier2Acquire() until the kMsgTier2Batch token (carrying its index) is
// committed to the producer ring; the duty thread then owns it (queued on the
// partition's intrusive FIFO, `next`) until the batch is encoded+sent, failed or
// discarded, and returns the index over the SPSC free ring.  The ring's
// release/acquire is the only synchronisation (the slot has no atomics).
// ---------------------------------------------------------------------------
struct Tier2Slot {
    int32_t  topicId     = 0;
    int32_t  partition   = 0;
    uint64_t ackHandle   = 0;  // ackhandle:: tagged token of the batch (0 = none)
    int32_t  recordCount = 0;
    int64_t  enqueuedNs  = 0;  // agent clock when drained (delivery deadline if never sent)
    uint32_t next        = kNoTier2Slot;  // intrusive FIFO link (duty only)
    rune::protocol::RecordBatchBuilder builder{0, 0};  // app: accumulates records (body capacity persists)
    std::vector<uint8_t> batchBytes; // pre-built RecordBatch (baseSequence=0, patched by agent); capacity persists
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
    // D6 (Kafka TopicPartitionEntry.lastAckedOffset): the max LAST offset any OK
    // of this partition reported (-1 = none; a known-old OK -1 never moves it).
    // Rule U: lastAckedOffset < ProduceResponse.logStartOffset proves every
    // record we had acked was removed by retention.
    int64_t  lastAckedOffset = -1;
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
    // Task 16 fix round 1 (C1): the partition's owner answered NotLeader (retriable,
    // Kafka NOT_LEADER_OR_FOLLOWER parity).  New batches are held (never sent to the
    // old owner) while stored ones are in flight; the stored ones are re-offered,
    // unchanged, once metadata names the new owner (routeResend).  Cleared when
    // the re-offer completes or nothing is in flight any more.
    bool     routeParked  = false;
    // Every in-flight batch flagged routeResendPending is re-offered, oldest
    // first, on the next flushBatches visit — before anything new.
    bool     routeResend  = false;
    /// Re-review 1 NEW-4(b): bumped whenever the partition's produce route may
    /// have changed (a different publication / endpoint = a different lane).
    uint32_t laneGen      = 0;
    /// An OOS on the oldest batch is waiting for earlier-lane attempts to be
    /// answered before it may be taken as proof (see "Sequence resolution" C2b).
    bool     laneWait     = false;

    // The Tier-1 batch, built IN PLACE as its Produce wire frame (backlog B1/B4:
    // ProduceFrameBuilder.h) — reused; reset after each flush; its storage is
    // swapped into the in-flight slot on an accepted acked offer.
    // Lazy-initialised on first append.
    std::unique_ptr<ProduceFrameBuilder> builder;
    int32_t builderBytes = 0; // approximate accumulated value bytes

    // Timestamp when first record was appended to the current batch (for linger).
    int64_t batchStartNs = 0; // 0 = empty

    // Wall-clock base timestamp of the current RecordBatchBuilder (wallMs() at
    // builder creation / last reset).  Used to compute per-record tsDelta for
    // the frame-cap pre-flight check (task 3).
    int64_t builderBaseWallMs = 0;

    // Force-flush flag (set by flush() call from app thread, read here).
    bool flushRequested = false;

    // F12: this partition is on its producer's dirty list (ProducerImpl::dirty_)
    // — it has (or recently had) builder records / queued Tier-2 batches, so
    // flushBatches must examine it.  Cleared lazily by the flush walk once the
    // partition has no work left.  Owner/duty thread only.
    bool dirty = false;

    // Phase 2b F Task 6: membership in the current transaction (CoordinatorLink.h).  Only an Added partition's
    // NEW batches are sent: a transactional partition waits for its AddPartitionsToTxn OK; a plain producer's
    // partitions are created Added (flushBatches pays one index compare).  Duty thread only.
    TxnMembership txn{};

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

    // Tier-2 batches queued for this partition (agent-thread only, no mutex): an
    // intrusive FIFO of pool slot indices linked through Tier2Slot::next.
    // Populated by drainRing() when a kMsgTier2Batch token is processed.
    // Drained by flushBatches() in FIFO order, after any pending Tier-1 batch.
    uint32_t t2Head = kNoTier2Slot;
    uint32_t t2Tail = kNoTier2Slot;
    [[nodiscard]] bool hasTier2() const noexcept { return t2Head != kNoTier2Slot; }

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
    ValueTooLarge,      ///< can never fit this producer's ring (tiny rings)
    MessageTooLarge,    ///< key + value > Producer::kMaxRecordBytes (backlog B6)
    BackPressured,      ///< ring full / builder full (retry)
    DeferredFull,       ///< Direct: in-duty deferral arena / partition share full (retry later)
    PartitionDeferred,  ///< Direct: this partition still has deferred records that cannot go yet
    Unaddressable,      ///< Direct: the producer's partition table is full
    ClaimHeld,          ///< Direct: an open claim owns the append path (Phase 2b B)
    NotInTransaction,   ///< transactional producer, handle gate Closed (Phase 2b F)
};

// ---------------------------------------------------------------------------
// Phase 2b F Task 5: one transaction control op (the kMsgTxnControl payload decoded, Task 8, or an Invoker
// direct call), applied on the duty thread by ProducerImpl::applyTxnControl.
// ---------------------------------------------------------------------------
/// sendOffsetsToTransaction limits (Task 7): more offsets / a longer group id completes InvalidArgument, never truncated.
inline constexpr std::size_t kMaxTxnOffsets      = 1024;
inline constexpr std::size_t kMaxTxnGroupIdBytes = 255;

/// #125: the longest member id a SendOffsets carries (GroupMetadata).
inline constexpr std::size_t kMaxTxnMemberIdBytes = kMaxGroupMemberIdBytes;

/// The largest kMsgTxnControl payload: the header, a group id of kMaxTxnGroupIdBytes padded to 8, a member id of
/// kMaxTxnMemberIdBytes padded to 8, then kMaxTxnOffsets 16-byte entries.  ONE constant for the issue side's buffer
/// and the duty side's decode.
inline constexpr std::size_t kTxnCtlMaxBytes =
    ((kTxnCtlHeaderBytes + kMaxTxnGroupIdBytes + 7) & ~std::size_t{7}) + ((kMaxTxnMemberIdBytes + 7) & ~std::size_t{7}) +
    kMaxTxnOffsets * sizeof(OffsetEntry);
namespace detail_txnctl {
constexpr std::size_t align8(std::size_t n) { return (n + 7u) & ~std::size_t{7}; }
/// Where the entries start for a group id of `g` bytes and a member id of `m` bytes.
constexpr std::size_t entriesAt(std::size_t g, std::size_t m) { return align8(align8(kTxnCtlHeaderBytes + g) + m); }
constexpr bool entriesEndWithinMax() {
    for (std::size_t g = 0; g <= kMaxTxnGroupIdBytes; ++g) {
        if (entriesAt(g, kMaxTxnMemberIdBytes) + kMaxTxnOffsets * sizeof(OffsetEntry) > kTxnCtlMaxBytes) return false;
    }
    return true;
}
}  // namespace detail_txnctl
static_assert(detail_txnctl::entriesEndWithinMax(), "every legal group id length fits kTxnCtlMaxBytes");
static_assert(static_cast<std::size_t>(kDefaultProducerRingCap) / 8 >= kTxnCtlMaxBytes,
              "the default producer ring carries the largest control message (maxMsgLength = capacity / 8)");

struct TxnControl {
    enum class Kind : uint8_t { Init, Begin, SendOffsets, Commit, Abort, Flush };
    Kind             kind = Kind::Begin;
    detail::OpTarget target{};   ///< Begin: ignored (beginTransaction() is void; a refused one is counted)
    Token            token = 0;
    std::span<const OffsetEntry> offsets{};   ///< SendOffsets (Task 7)
    std::string_view groupId{};
    int32_t          generationId = -1;       ///< SendOffsets (#125): the member's generation; -1 = no member block
    std::string_view memberId{};
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
        return !lifeOpen() && !draining() && !endingTxn_() && !inDuty_ && !pendingFail_.armed && !txnHasLiveOp_() &&
               fullyDrained_();
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

    // ---- Phase 2b F: the handle gate (issuing-thread state) -------------------
    //
    // Written only by the producer's single issuing thread (FD2/FD3): shared-nothing,
    // no atomic.  NotTransactional = a plain producer (the hot path's only check,
    // [[likely]]); Closed = transactional, outside a transaction; Open = between
    // beginTransaction() and commit / abort / close.
    enum class TxnGate : uint8_t { NotTransactional, Closed, Open };
    [[nodiscard]] TxnGate txnGate() const noexcept { return txnGate_; }
    void setTxnGate(TxnGate g) noexcept { txnGate_ = g; }

    /// Non-blocking submit (ZERO allocation, lock-free): checks the ring's closed
    /// word FIRST (fix1 C2/I3 — a Producer outliving its closed Client gets
    /// Closed, never touching the pump hook's now-dangling context), then
    /// validates the record size (key + value <= Producer::kMaxRecordBytes, B6) AND the ring's
    /// own maxMsgLength (fix1 minor 11 — a message too big for a small ring maps
    /// to ValueTooLarge, not BackPressured, so it can never livelock a retry),
    /// then writes the SEND_RECORD. Never drops silently — see SendResult.
    /// fix4: `why` (optional) receives the reject reason, set before every
    /// return (None when accepted).
    [[nodiscard]] SendResult trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                       std::span<const uint8_t> value, int64_t ackHandle,
                                       RejectReason* why = nullptr) noexcept;

    // ---- Phase 2b Part B: zero-copy claims (Producer::tryClaim / claimBatch) ----

    /// What a granted claim hands to Claim / BatchClaim.
    struct ClaimGrant {
        SendResult      result  = SendResult::BackPressured;
        int64_t         token   = 0;        ///< Ring: the M2O claim index; Direct: the claim generation
        uint8_t*        base    = nullptr;  ///< value bytes of record 0
        const uint32_t* offsets = nullptr;  ///< Direct batch: record i at base + offsets[i]; nullptr: stride
    };
    /// Reserve one record of `valueLen` value bytes: Ring — a SEND_RECORD slot
    /// in the producer ring with its header written; the caller writes the
    /// value and commitClaim()s.  ZERO allocation.  Results as trySubmit.
    [[nodiscard]] ClaimGrant claimRecord(int32_t topicId, int32_t partition, const Key& key,
                                         uint32_t valueLen, uint64_t ackHandle) noexcept;
    /// Reserve n records of recLen bytes appended as ONE RecordBatch: Ring — a
    /// CLAIM_BATCH ring message (header written, records contiguous at `base`).
    /// Refusals: InvalidArgument (n == 0), MessageTooLarge (per-record cap,
    /// maxFrameBytes bound, ring maxMsgLength), Closed, BackPressured.
    [[nodiscard]] ClaimGrant claimBatch(int32_t topicId, int32_t partition, const Key& key, uint32_t n,
                                        uint32_t recLen) noexcept;
    /// Publish a granted claim.  False: it was released under the caller
    /// (Ring: Client close's drain unblock) — nothing was accepted.
    [[nodiscard]] bool commitClaim(int64_t token) noexcept;
    /// Discard a granted claim (Ring: the slot becomes padding).
    void abortClaim(int64_t token) noexcept;
    /// Phase 2b B (owner thread): Invoker claims abandoned while open - their
    /// partition's builder was failed / the producer closed; commit() returned false.
    [[nodiscard]] uint64_t claimsAbandoned() const noexcept { return claimsAbandoned_; }
    /// Client close drain (Direct mode, owner thread): the thread running close()
    /// cannot settle a claim it holds, and the open claim's reserved record would
    /// keep the drain waiting until deliveryTimeoutMs.  Abandons it now (its
    /// commit() returns false, nothing is sent).  No-op unless a claim is open.
    /// Phase 2b F Task 5: also applied when a commit / abort (or close's own abort) ends the transaction - a
    /// claim open across the end of its transaction never commits into it, nor into the next one.
    void abandonOpenClaimForClose() noexcept;

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
    /// The DEFAULT liveness clock (steady nanoseconds — the unit of the ring heartbeat and livenessNs).
    [[nodiscard]] static int64_t defaultClockNowForTest() noexcept { return steadyNowNs_(); }

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
    /// The batch lives in pool slot `slot` (tier2Acquire(); its batchBytes is the
    /// sealed RecordBatch, baseSequence = 0, patched by the agent).  The slot's
    /// INDEX is written into the ring (kMsgTier2Batch); ownership passes to the
    /// duty side on acceptance - the slot returns to the app over tier2Free_ once
    /// the batch is sent / failed / discarded.  The agent processes the token in
    /// order with Tier-1 records, stamping baseSequence.
    ///
    /// ackHandle: an ackhandle:: tagged token (0 = none).  recordCount: records
    /// in the pre-built batch.  A rejected batch stays with the caller (the slot
    /// is still the caller's) for a retry (F8c C6).
    ///
    /// fix4: returns RejectReason::None when accepted; otherwise why not.
    RejectReason submitTier2Batch(int32_t topicId,
                                  int32_t partition,
                                  uint64_t ackHandle,
                                  int32_t recordCount,
                                  uint32_t slot);

    /// Pre-sealed bytes in, for tests / tools that build a RecordBatch themselves: takes a
    /// pool slot, copies `bytes` into it, submits.  BackPressured when no slot is free; a
    /// rejected batch returns its slot (nothing is retained).
    RejectReason submitTier2Bytes(int32_t topicId, int32_t partition, uint64_t ackHandle,
                                  int32_t recordCount, std::span<const uint8_t> bytes);

    // ---- Tier-2 slot pool (app side: the single batch-issuing thread) --------

    /// A free pool slot (its builder and bytes are the caller's until submitTier2Batch
    /// accepts it or tier2Release returns it), or kNoTier2Slot when every slot is in
    /// use (the caller reports BackPressured).  Never allocates.
    [[nodiscard]] uint32_t tier2Acquire() noexcept;
    /// Return a slot the caller still owns (a builder dropped before / without an
    /// accepted send) - exactly once per acquired slot.
    void tier2Release(uint32_t slot) noexcept {
        assert(tier2AppFreeN_ < tier2Pool_.size() && t2Held_ > 0 && "Tier-2 slot released twice");
        --t2Held_;
        tier2AppFree_[tier2AppFreeN_++] = slot;
    }
    /// The slot was accepted by submitTier2Batch: it is the duty side's now.
    void tier2Handed() noexcept { --t2Held_; }

    // ---- Tier-2 issuing thread (single-thread per producer; Phase 2b F Task 3 / FD3) ----
    // The slot pool's app side (free stack + SPSC free-ring consumer) is owned by ONE thread: the
    // thread that created the producer (Invoker mode: the owner thread), or the one that called
    // bindBatchThread().  Every Tier-2 entry compares against it BEFORE touching the pool.
    [[nodiscard]] bool onBatchThread() const noexcept { return std::this_thread::get_id() == t2Thread_; }
    /// Rebind the issuing thread to the caller.  Call before the first batch() from the new
    /// thread with no BatchBuilder holding a slot (asserted in debug).
    void bindBatchThread() noexcept {
        assert(t2Held_ == 0 && "bindBatchThread with a live BatchBuilder");
        t2Thread_ = std::this_thread::get_id();
    }
    [[nodiscard]] Tier2Slot& tier2Slot(uint32_t slot) noexcept { return tier2Pool_[slot]; }
    /// Largest RecordBatch (header + records) a slot holds.
    [[nodiscard]] std::size_t tier2SlotBytes() const noexcept { return tier2SlotBytes_; }
    /// Pool introspection (tests; BATCH-ISSUING THREAD ONLY - it drains the free ring): total
    /// slots, and slots not owned by a builder or the duty side (app-local free +
    /// returned-but-undrained).
    [[nodiscard]] std::size_t tier2SlotCount() const noexcept { return tier2Pool_.size(); }
    [[nodiscard]] std::size_t tier2FreeCount() noexcept;

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
        /// Phase 2b F Task 8 (duty side): pin a CQ target's registry slot before its completion is delivered
        /// (Client::pinCqSlot_; an op read from the producer ring has no command-ring fence).  Absent (bare rig):
        /// targets are used as they are.
        void (*pinTarget)(void* ctx, detail::OpTarget& t) noexcept = nullptr;
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
    void applyCloseImmediately(OpStatus st, uint16_t errorCode = 0);
    /// Alias for the duty-thread failure path (T15: Aeron client closed).
    void failClose(OpStatus st, uint16_t errorCode = 0) { applyCloseImmediately(st, errorCode); }
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
    /// 2b-B Task 4 fix round (M1): runs the close-drain's unblock() on the ring's
    /// head claim — the interleaving of Client close against a writer that sits
    /// between a commit's verify and its publish.  Test seam; Ring mode.
    [[nodiscard]] bool testUnblockHeadClaim() { return ring_.unblock(); }
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
    /// once the older batches resolve (Kafka parity).  D6: `logStartOffset` is
    /// the partition's log start carried with UnknownProducerId (-1 = not
    /// carried) — rule U decides retention (re-sequence) from loss (unknown).
    void onProduceResponse(int64_t correlationId, int64_t baseOffset, int16_t errorCode,
                           int64_t logStartOffset = -1);

    /// Called on redirect-reconnect failure (SessionLost) and by the close paths
    /// (duty-cycle thread, or after it stopped): fails AND drops every accepted
    /// record — in the app ring (committed so far), the deferred arena, a
    /// builder, queued Tier-2, or in flight — with `st`, exactly once.
    /// noexcept (fix round 2, N1): run from ~DutyScope (the deferred fail-all
    /// of a close applied inside a handler).  Everything it calls is non-
    /// throwing (handlers are noexcept function pointers; no allocation).
    void failAllPending(OpStatus st, uint16_t errorCode = 0) noexcept;
    /// Authentication (D26): every batch IN FLIGHT (already sent, unanswered) completes BrokerError `errorCode`
    /// (SessionRevoked) — the answer the broker's closing session can no longer give.  Records not yet sent
    /// are left to the caller's terminal failure path.  Duty thread.
    void failInFlightBroker(uint16_t errorCode) noexcept;
    /// Review B I1: a partition whose route is Rejected (its leader node's session was revoked / refused): `pred`
    /// returns the error code (46 / 44) for a partition on such a route, 0 otherwise.  For each such partition every
    /// in-flight batch AND the unsent builder complete BrokerError with that code, at once — never held until the
    /// delivery timeout.  Idempotent; cheap when nothing matches.  Duty thread.
    using RouteFailFn = uint16_t (*)(void* ctx, int32_t topicId, int32_t partition) noexcept;
    void failRoutedBroker(RouteFailFn pred, void* ctx) noexcept;

    /// Duty-cycle thread: fail every record of an in-flight batch (wire id)
    /// with DeliveryFailed.  Non-batch ids are ignored (they can come from any
    /// ErrorResponse).
    void failBatch(int64_t batchCorrId);

    /// CB-5 B: attempt to resend the wire frame for a redirected Produce.
    /// Returns true if resent, false if corrId unknown / max redirects exceeded.
    /// On failure the pending ack is failed immediately.
    /// DR-7 review M2: an attempt timed out since the last call (Kafka: a request
    /// timeout requests a metadata update).  Duty thread; clears the flag.
    [[nodiscard]] bool takeMetadataWanted() noexcept {
        const bool w = metadataWanted_;
        metadataWanted_ = false;
        return w;
    }
    bool metadataWanted_ = false;  // see takeMetadataWanted()
    static constexpr int kMaxRedirects = 3;
    /// N-e: the backoff before a retriable error (MinIsrViolation) is re-sent
    /// (Kafka retry.backoff.ms default), capped by produceTimeoutMs.
    static constexpr int64_t kRetriableBackoffNs = 100'000'000LL;
    /// E2 final fix wave (M3), kept from the transactional producer: a batch answered TransientLogFull (42) is
    /// re-offered after this much DUTY-CLOCK time (capped by produceTimeoutMs) - never a cycle count.
    static constexpr int64_t kTransientLogFullBackoffNs = kRetriableBackoffNs;
    /// The error a transactional producer's transaction fails with when its sequences must restart (wire values of
    /// ErrorCode::OutOfOrderSequence / ::UnknownProducerId; the second is the app-visible code when the broker
    /// forgot the producer).
    static constexpr uint16_t kTxnResetOos        = 20;
    static constexpr uint16_t kTxnResetUnknownPid = 28;
    /// DR-15 review M1: a Redirect for `corrId` that is not followed (target
    /// unreachable) is still an answer for the in-flight accounting (NEW-4(b)).
    void noteRedirectAnswer(int64_t corrId) noexcept;
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

    /// Task 16: queue every in-flight batch of ONE partition for a re-offer through
    /// its (new) route — used when metadata moves the partition's ingress endpoint
    /// (another worker / back to the node) or names a new owner after NotLeader.
    /// Same frame, same batch id, same producer identity: the broker dedups by
    /// (producerId, epoch, sequence); a late ack from the old owner is ignored.
    /// The re-offer itself runs in the next flushBatches pass (same duty cycle),
    /// oldest first, stopping at the first refused offer (order is kept; the rest
    /// go on the next pass), before any new batch.  Returns # batches queued.
    int resendPartitionViaRoute(int32_t topicId, int32_t partition) noexcept;
    /// Task 16 (C1): resendPartitionViaRoute for every route-parked partition
    /// (fresh metadata arrived after a NotLeader).  Returns # batches queued.
    int resendRouteParked() noexcept;
    /// Re-review 1 NEW-4(b): the partition's produce route changed without a
    /// re-offer (e.g. a first node -> worker install): later offers of its live
    /// batches go on a new lane.  Cold path.
    void noteRouteChanged(int32_t topicId, int32_t partition) noexcept;
    /// Test/diagnostics: true while (topicId, partition) is route-parked.
    [[nodiscard]] bool routeParked(int32_t topicId, int32_t partition) const noexcept;

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
            const bool hasTier2 = ps.hasTier2();
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

    // ---- Phase 2b F Task 2: the coordinator link (duty thread only) -----------
    /// Emplaces the link with this producer's ordinal (ProducerHub::add, right after setOrdinal) and
    /// the sink to offer coordinator frames through.  Before it, a bare rig has no link.
    void enableCoordinator(CoordinatorSink sink, uint32_t seqBase = 0);
    /// FD4: this producer's id comes from the broker, not the rig default.  Client::producer calls it on the
    /// creating thread, before the impl is shared, so the handle reports -1 until the id is assigned.
    void expectBrokerId();
    /// Cold (ProducerHub::remove): the link's sequence, carried to the ordinal's next incarnation.
    [[nodiscard]] uint32_t coordinatorSeq() const noexcept { return coord_ ? coord_->seq() : 0; }
    [[nodiscard]] bool hasCoordinator() const noexcept { return coord_.has_value(); }
    [[nodiscard]] CoordinatorLink* coordinatorLink() noexcept { return coord_ ? &*coord_ : nullptr; }
    /// An answer routed here by ordinal (ProducerHub::onCoordinatorResponse).  The machines of Tasks
    /// 4-7 consume the settled slot; until then it is released here.
    void onCoordinatorAnswer(const CoordAnswer& a, int64_t nowNs) noexcept;
    /// One duty pass of the link: offers, retries, deadline expiry (expired slots are released).
    int pumpCoordinator(int64_t nowNs) noexcept;
    /// FD4: one duty pass of the identity machine (ProducerHub::drainAndFlush, before the flush).  An
    /// IdAssigned producer pays one variant-index compare; the rest is cold.
    int identityStep(int64_t nowNs) noexcept {
        if (identity_.index() == kIdAssignedIndex) [[likely]] return 0;
        return identityStepCold_(nowNs);
    }
    [[nodiscard]] bool identityAssigned() const noexcept { return identity_.index() == kIdAssignedIndex; }
    [[nodiscard]] const ProducerIdentity& identity() const noexcept { return identity_; }
    void onCoordinatorMoved() noexcept { if (coord_) coord_->onCoordinatorMoved(); }
    void onCoordinatorSessionChanged() noexcept { if (coord_) coord_->onSessionChanged(); }
    /// Diagnostics / tests: answers routed to this producer / the id of the last one.
    [[nodiscard]] uint64_t coordAnswersSeen() const noexcept { return coordAnswersSeen_; }
    [[nodiscard]] int64_t  lastCoordAnswerCorrId() const noexcept { return lastCoordCorrId_; }

    // ---- Phase 2b F Task 5: the transaction machine (duty thread only; ProducerTxn.cpp) --------------
    //
    //   Uninitialized -init-> Initializing{op, slot} -InitProducerId OK-> Ready -begin-> InTransaction{started}
    //   InTransaction -commit-> Committing{op, phase, started} -EndTxn OK-> Ready
    //   InTransaction | AbortableError -abort-> Aborting{op, phase, started, kind} -EndTxn OK [-bump OK]-> Ready
    //   a refused leg / lost data -> AbortableError{status, code, started, kind} (commit completes with it)
    //   17 / 18, 19 or 45 on EndTxn, a non-retriable InitProducerId, a revoked session -> FatalError (terminal)
    //
    // Semantics are Kafka's TransactionManager's: state gating (begin only in
    // Ready; commit / sendOffsets only InTransaction; abort in InTransaction / AbortableError; init only
    // Uninitialized; FatalError: every op completes with its status), misuse completes BrokerError(19), an
    // unstarted transaction ends locally (B-1), an op timeout leaves Committing / Aborting InDoubt and the SAME
    // call re-sends EndTxn (B-2), a sequence gap bumps the epoch on abort (B-3), the local state resets only on
    // EndTxn's final answer.  Every op completes exactly once, bounded by transactionOpTimeoutMs.  Coordinator
    // requests are acquired only by txnStep (never inside an apply / answer), so a completion handler that
    // issues the next op never races the slot its answer is still using.
    void applyTxnControl(const TxnControl& c, int64_t nowNs);
    /// Phase 2b F Task 8 (ISSUING thread): issue one control op.  Ring mode: a kMsgTxnControl message in this
    /// producer's ring, FIFO with the records before it (the drain applies it at its place); a full ring is
    /// BackPressured, a closed intake Closed.  Direct (Invoker) mode: applied at once on the owner thread
    /// (WrongThread elsewhere).  Gate rules (FD2): Commit / Abort close the gate BEFORE the write (restored if
    /// nothing was written), Begin opens it AFTER.  The caller (Producer) has resolved and reserved the target;
    /// on any result but Accepted nothing will complete.  A SendOffsets over the caps is Accepted and completes
    /// InvalidArgument (in ring order).  Zero allocation.
    [[nodiscard]] OpResult issueTxnControl(const TxnControl& c) noexcept;
    /// True when control ops travel the producer ring (Thread mode): the issuer must count a CQ-targeted one in
    /// Completions::producerIssued_ (C6); a Direct (Invoker) op is applied in place and pinned instead.
    [[nodiscard]] bool txnOpsOnRing() const noexcept { return mode_ != IngressMode::Direct; }
    [[nodiscard]] TxnState txnStateDuty() const noexcept { return static_cast<TxnState>(txn_.index()); }
    [[nodiscard]] const TxnMachine& txnMachine() const noexcept { return txn_; }
    /// One duty pass of the machine (ProducerHub::drainAndFlush, after identityStep): sends the InitProducerId /
    /// EndTxn / bump that is due, waits for the data, expires op deadlines, decides pending flushes.  A producer
    /// with nothing pending pays one index compare.
    int txnStep(int64_t nowNs) noexcept {
        if (txnQuiet_()) [[likely]] return 0;
        return txnStepCold_(nowNs);
    }
    /// Authentication (D26): the session was revoked - terminal.  In-flight batches complete BrokerError
    /// `errorCode`; a transactional producer goes FatalError(BrokerError, errorCode): every pending op completes
    /// with it and no coordinator request is ever sent or re-sent.  Duty thread.
    void onSessionRevoked(uint16_t errorCode) noexcept;
    /// beginTransaction() calls the machine refused (outside Ready): counted, no side effect.
    [[nodiscard]] uint64_t txnRefusedBegins() const noexcept { return txnRefusedBegins_; }
    /// FD2 duty re-check (Task 6): void records that reached the duty side while the machine was not InTransaction
    /// - never produced, counted here (they fail the transaction: commit completes with its error).  Duty thread.
    [[nodiscard]] uint64_t txnRecordsRefusedDuty() const noexcept { return txnRecordsRefusedDuty_; }
    /// Partitions in the current transaction (left NotInTxn since it began).  Duty thread; diagnostics / tests.
    [[nodiscard]] std::size_t txnPartitionCount() const noexcept { return txnSeen_.size(); }
    /// Ring messages the drain read that were neither a record nor a flush token (Task 8: control ops).  Duty thread.
    [[nodiscard]] uint64_t ringForeignMessages() const noexcept { return ringForeign_; }
    /// Test seam: write one raw message of `type` into the producer ring (Ring mode).
    bool testWriteRing(int32_t type, std::span<const uint8_t> payload) noexcept {
        return writeToRing(type, payload.data(), payload.size());
    }
    /// Test seams (duty thread): the data / offsets legs of Tasks 6 / 7 drive these through the same paths.
    void testMarkStarted(TxnStarted s = TxnStarted::Yes) noexcept { markStarted_(s); }
    void testPoison(OpStatus st, uint16_t errorCode, AbortKind kind) noexcept { poison_(st, errorCode, kind); }
    /// A transaction leg (AddPartitions / AddOffsets / TxnOffsetCommit) answered `errorCode`, as the link hands
    /// it over (Retry / Rediscover never reach the machine).
    void testLegAnswer(ReqKind kind, int16_t errorCode) noexcept { onTxnLegAnswer_(kind, errorCode); }

    /// Total Aeron frames offered to transport (agent-thread only; plain uint64_t).
    uint64_t framesSent()  const noexcept { return framesSent_; }
    /// Stored frames offered AGAIN (timer / route / parked / redirect retransmits); duty side (Invoker owner in tests).
    uint64_t framesResent() const noexcept { return framesResent_; }

    /// Total records accepted via drainRing (agent-thread only; plain uint64_t).
    uint64_t recordsSent() const noexcept { return recordsSent_; }

    /// This producer's current identity (duty-thread state; diagnostics + tests).  -1 until assigned.
    /// The app-side handle reads it through appProducerId() / appProducerEpoch() (FD9: a StatusRing).
    [[nodiscard]] int64_t producerId() const noexcept {
        const auto* a = std::get_if<IdAssigned>(&identity_);
        return a != nullptr ? a->producerId : -1;
    }
    [[nodiscard]] int16_t producerEpoch() const noexcept {
        const auto* a = std::get_if<IdAssigned>(&identity_);
        return a != nullptr ? a->epoch : int16_t{-1};
    }

    /// FD9: the latest identity the duty thread published (ONE app thread; drains the status ring).
    struct ProducerStatus {
        int64_t producerId    = -1;
        int16_t producerEpoch = -1;
        uint8_t identityState = 0;
        uint8_t txnState      = 0;   ///< TxnState (Task 5)
        uint8_t txnActive     = 0;   ///< 1: InTransaction / Committing / Aborting / AbortableError
    };
    /// BOUND THREAD ONLY (the creating thread, or bindBatchThread()'s): the cache and the ring's consumer side
    /// belong to it.  Any other thread gets -1 and touches nothing (a single-consumer ring must not be drained
    /// by two threads).
    [[nodiscard]] int64_t appProducerId() noexcept {
        if (!onBatchThread()) return -1;
        drainStatus_();
        return appStatus_.producerId;
    }
    [[nodiscard]] int16_t appProducerEpoch() noexcept {
        if (!onBatchThread()) return -1;
        drainStatus_();
        return appStatus_.producerEpoch;
    }
    /// FD9 (bound thread only; elsewhere Uninitialized / false, nothing touched).
    [[nodiscard]] TxnState appTxnState() noexcept {
        if (!onBatchThread()) return TxnState::Uninitialized;
        drainStatus_();
        return static_cast<TxnState>(appStatus_.txnState);
    }
    [[nodiscard]] bool appTxnActive() noexcept {
        if (!onBatchThread()) return false;
        drainStatus_();
        return appStatus_.txnActive != 0;
    }

    /// Test-only seam (F5 regression coverage): force the current epoch so a
    /// test can drive the INT16_MAX overflow path without sending 32767 real
    /// batches to get there (partitions with nothing in flight adopt it).
    /// Never called from production code.
    void debugSetEpochForTest(int16_t epoch) noexcept {
        if (auto* a = std::get_if<IdAssigned>(&identity_)) a->epoch = epoch;
        for (auto& up : partitions_) {
            if (up->inflight.empty()) up->epoch = epoch;
        }
    }

    /// Test-only seam (sequence overflow regression): set a partition's next
    /// baseSequence (created on demand) so a test reaches INT32_MAX without
    /// sending 2^31 records.  Never called from production code.
    void debugSetNextSequenceForTest(int32_t topicId, int32_t partition, int32_t seq) {
        PartitionState& ps = getOrCreatePartition(topicId, partition);
        ps.nextBaseSequence = seq;
        ps.lastAckedSeq     = seq - 1;
    }

    /// Test-only seam (re-review 1 N-a): make the stored frame of the n-th live
    /// batch (oldest = 0) of a partition undecodable (truncated).  Never called
    /// from production code.
    void debugMangleStoredFrameForTest(int32_t topicId, int32_t partition, int n) {
        PartitionState& ps = getOrCreatePartition(topicId, partition);
        int i = 0;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (i++ == n) b.wireFrame.resize(20);
        });
    }

    /// Test-only seam (Task 14 fix2): lower the partition-table cap (default
    /// batchcorr::kMaxSlot + 1) so the "unaddressable partition" path can be
    /// exercised without creating 262144 partitions. Never called in production.
    void debugSetMaxPartitionsForTest(std::size_t n) noexcept { maxPartitions_ = n; }
    /// Task 14 fix2: accepted deferred records that could not be applied (and
    /// were failed through their handle / counted + logged if void). Owner thread.
    [[nodiscard]] uint64_t deferredApplyFailures() const noexcept { return deferredApplyFailures_; }
    /// D6 review M1 (diagnostic): UnknownProducerId answers where this producer
    /// had an acked record at/above the answer's logStartOffset — the broker
    /// LOST producer state it should have kept (not retention).  The batch was
    /// still provably not appended and is re-sequenced (rule U); never a failure.
    [[nodiscard]] uint64_t unknownProducerIdStateLoss() const noexcept { return unknownPidStateLoss_; }

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
    // Task 10 (perf): the per-record helpers below are force-inlined into the
    // send path (appendRecord_) — out of line they cost ~4 ns/record on the
    // plain send path (two extra calls + reloads per record).  Their rare
    // branches (builder creation, the oversize-record warning) are cold,
    // non-inlined helpers so the inlined body stays small.
    /// Create the partition's builder once (reused across batches).
    [[gnu::always_inline]] void ensureBuilder_(PartitionState& ps);
    [[gnu::cold, gnu::noinline]] void createBuilder_(PartitionState& ps);
    /// Every check before ONE record may join `ps`'s builder (Tier-2 queued
    /// ahead, frame cap, batchBytes, batchRecords), the builder created and the
    /// record's ack-ring room reserved.  ABORT = back-pressure (nothing changed).
    [[gnu::always_inline]] rune::client::detail::ControlledAction admitRecord_(PartitionState& ps, uint8_t keyLen,
                                                                               uint32_t valueLen, uint8_t flags,
                                                                               uint64_t ackHandle);
    [[gnu::cold, gnu::noinline]] void warnOversizeRecord_(const PartitionState& ps, int32_t soloContrib) const;
    /// Same for a claimBatch of n x recLen: it joins a non-empty builder only
    /// when it fits whole (else flushRequested + ABORT); builder capacity for
    /// all n is reserved so appending them cannot throw (all or none).
    rune::client::detail::ControlledAction admitBatch_(PartitionState& ps, uint8_t keyLen, uint32_t n,
                                                       uint32_t recLen);
    /// Append an admitted CLAIM_BATCH's n records (one timestamp), then account them.
    void appendBatch_(PartitionState& ps, const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* recs,
                      uint32_t n, uint32_t recLen, int64_t nowNs);
    /// Accounting after `count` records starting at offsetDelta `firstIndex`
    /// were written into ps.builder: batch clock, dirty list, the ack handle of
    /// an acked single record (room reserved by admitRecord_), wire size, count.
    [[gnu::always_inline]] void noteAppended_(PartitionState& ps, int32_t firstIndex, int32_t count, uint8_t flags,
                                              uint64_t ackHandle, int64_t nowNs);
    /// claimBatch's size rules, shared by both ingress modes.
    [[nodiscard]] SendResult checkBatchClaim_(const Key& key, uint32_t n, uint32_t recLen) const noexcept;

    // ---- Phase 2b B: the Invoker (Direct) claim state machine (owner thread) ---
    //   NoDirectClaim --claimRecord / claimBatch--> DirectClaimOpen
    //   DirectClaimOpen --commit--> NoDirectClaim   (records join the batch: noteAppended_)
    //   DirectClaimOpen --abort--> NoDirectClaim    (builder rolled back to `mark`)
    //   DirectClaimOpen --abandon--> DirectClaimAbandoned (failBuilder_ / close:
    //       rolled back, and the builder's storage - the caller's span - moved
    //       into claimQuarantine_)
    //   DirectClaimAbandoned --commit (false) / abort / ~Claim--> NoDirectClaim
    // While open: every other append to the producer is ClaimHeld (trySubmit) or
    // ABORTed (admitRecord_ - the deferred apply pass), and the partition is not
    // flushed (encodeAndSend); failBuilder_ abandons it.
    // While abandoned (final review I1): the caller may still write its span,
    // harmlessly - those bytes are in claimQuarantine_, outside every builder
    // and in-flight frame.  Sends proceed (a failure handler's retries too);
    // another claim is ClaimHeld until this one is settled (one quarantine).
    struct NoDirectClaim {};
    struct DirectClaimOpen {
        uint32_t                  slot       = 0;  ///< PartitionState::slotIndex
        ProduceFrameBuilder::Mark mark{};          ///< the builder before the reservation
        int32_t                   firstIndex = 0;  ///< offsetDelta of the first reserved record
        int32_t                   count      = 0;  ///< 1 (tryClaim) or n (claimBatch)
        uint8_t                   flags      = 0;  ///< kFlagWantAck for an acked tryClaim
        uint64_t                  ackHandle  = 0;
        uint64_t                  gen        = 0;  ///< the Claim's token
    };
    struct DirectClaimAbandoned {
        uint64_t gen = 0;  ///< the Claim's token (its span is in claimQuarantine_)
    };
    [[nodiscard]] bool directClaimOn_(const PartitionState& ps) const noexcept {
        const auto* o = std::get_if<DirectClaimOpen>(&directClaim_);
        return o != nullptr && o->slot == ps.slotIndex;
    }
    /// The open claim on `ps` (if any) is rolled back to its mark and becomes
    /// DirectClaimAbandoned; true when it was.  The caller then moves the
    /// builder's storage to claimQuarantine_ (detachStorage*) - failBuilder_
    /// and abandonOpenClaimForClose.
    [[nodiscard]] bool abandonDirectClaimOn_(PartitionState& ps) noexcept;
    /// commit / abort of an abandoned claim `gen`: settled (NoDirectClaim).
    [[gnu::cold, gnu::noinline]] void settleAbandonedClaim_(uint64_t gen) noexcept;
    /// Owner-thread / closed / held / in-duty / deferred-ahead checks of a claim.
    [[nodiscard]] SendResult directClaimGate_(int32_t topicId, int32_t partition) noexcept;
    [[nodiscard]] ClaimGrant claimRecordDirect_(int32_t topicId, int32_t partition, const Key& key,
                                                uint32_t valueLen, uint64_t ackHandle) noexcept;
    [[nodiscard]] ClaimGrant claimBatchDirect_(int32_t topicId, int32_t partition, const Key& key, uint32_t n,
                                               uint32_t recLen) noexcept;
    [[nodiscard]] bool commitDirectClaim_(uint64_t gen) noexcept;
    void abortDirectClaim_(uint64_t gen) noexcept;

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
                            Tier2Slot& entry,
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
    /// flushBatches: Task 16 route re-offer of `ps`'s flagged in-flight batches,
    /// oldest first.  False on a refused offer (the rest wait for the next pass).
    template <class Sink>
    bool resendRoute_(PartitionState& ps, int64_t nowNs, const Sink& sink);
    void queueRouteResend_(PartitionState& ps) noexcept;

    [[nodiscard]] bool canFlight_(const PartitionState& ps) const noexcept {
        return static_cast<int32_t>(ps.inflight.live()) < cfg_.maxInFlight && !ps.inflight.full();
    }
    /// ackTail = start of the oldest un-acked slice (in-flight or current builder).
    void reclaimAckRing_(PartitionState& ps) noexcept;
    /// Reads up to `limit` committed messages of the app->agent ring and
    /// completes each record / Tier-2 batch with `st` (its ack handle, and a
    /// one-unit BatchAck) — never sends it.  Frees Tier-2 entries.  Returns
    /// messages read.
    int discardRing_(OpStatus st, int limit, uint16_t errorCode = 0);
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
    /// Epoch overflow: at INT16_MAX the identity machine asks the broker for a NEW id
    /// (IdUnassigned -> InitProducerId, FD4); the partition holds (producerId -1) until it arrives.
    /// `cause`: the error a TRANSACTIONAL producer's transaction fails with when the sequences must restart
    /// (OutOfOrderSequence; UnknownProducerId when the broker forgot the producer - the app-visible code 28).
    void resetPartition_(PartitionState& ps, uint16_t cause = kTxnResetOos) noexcept;
    /// Sequence space (int32; the client chooses never to wrap — N-c: since
    /// rune-broker 37564ba the broker accepts the Kafka wrap INT32_MAX -> 0 in
    /// order and refuses a batch crossing INT32_MAX; not wrapping is compatible
    /// and costs one drain + epoch move per 2^31 records of a partition): true
    /// when a batch of `n` records
    /// may be stamped at ps.nextBaseSequence with its last sequence AND the
    /// following nextBaseSequence still <= INT32_MAX.  When it may not: with
    /// nothing in flight the partition moves to a new identity at sequence 0
    /// (resetPartition_: an epoch bump BEFORE the overflow) and it may; with
    /// batches in flight it waits for them (a bump now would fence their
    /// retransmits — EOS I-4) and returns false (the batch is kept, retried
    /// next pass).
    [[nodiscard]] bool sequenceRoom_(PartitionState& ps, int32_t n) noexcept;
    /// resetPartition_ + every live batch re-stamped (frame + header + CRC) in
    /// order from sequence 0 and queued for re-send (parked), new attempts.
    void resequence_(PartitionState& ps, uint16_t cause = kTxnResetOos) noexcept;
    /// Re-stamp a stored frame's attempt tag (MessageHeader correlationId).
    static void stampAttempt_(InFlightBatch& b, uint8_t tag) noexcept;
    /// NEW-4(b): an answer (any outcome) to attempt `tag` of `b` arrived — one
    /// fewer outstanding attempt on its lane.
    static void noteAnswer_(InFlightBatch& b, uint8_t tag) noexcept;
    /// NEW-4(b): a live batch of b's identity has an unanswered attempt on an
    /// earlier lane (it may still be appended).
    [[nodiscard]] bool staleLaneInFlight_(PartitionState& ps, const InFlightBatch& b) noexcept;
    /// NEW-4(b): a waiting OOS head is re-sent once no earlier-lane attempt of
    /// its identity is outstanding.
    void maybeEndLaneWait_(PartitionState& ps) noexcept;
    /// One offer of a live batch's stored frame as a NEW attempt (its earlier
    /// attempts' error answers are ignored from now on).  Reverted when the
    /// transport refuses it.
    template <class Send>
    bool offerAttempt_(PartitionState& ps, InFlightBatch& b, int64_t nowNs, Send&& send, uint16_t frReason = 0);
    /// One BatchAck to the batch-ack target (handler or CQ; none: nothing).
    /// `units`: the batch-ack credit units it returns (records of a Tier-1
    /// batch; 1 for a Tier-2 batch / a single never-sent record).
    void emitBatchAck_(int32_t topicId, int32_t partition, int32_t baseSequence, int32_t recordCount,
                       int64_t baseOffset, int16_t errorCode, OpStatus st, uint32_t units,
                       int16_t producerEpoch, int64_t producerId = -1) noexcept;
    /// Records that never reached a batch (failed in the ring / a builder /
    /// the Tier-2 queue / the arena): a BatchAck{baseSequence -1, kClientFailure}.
    /// Phase 2b F Task 6: every record failure goes through here or txnNoteLoss_ directly - a transactional
    /// producer's lost record poisons its transaction (a commit never sends EndTxn(commit) over lost data).
    void emitUnsentBatchAck_(int32_t topicId, int32_t partition, int32_t recordCount, OpStatus st,
                             uint32_t units, uint16_t errorCode = 0) noexcept {
        emitBatchAck_(topicId, partition, -1, recordCount, -1, BatchAck::kClientFailure, st, units, -1);
        txnNoteLoss_(st, errorCode, AbortKind::Plain);   // never sent: no sequence gap
    }
    /// A record of a transactional producer failed (`kind`: BumpEpoch when it was sent - its sequence outcome is
    /// unknown).  Plain producers: one compare on a failure path only.
    void txnNoteLoss_(OpStatus st, uint16_t errorCode, AbortKind kind) noexcept {
        if (txnBatchAttrs_ != 0) [[unlikely]] txnLost_(st, errorCode, kind);
    }
    [[gnu::cold, gnu::noinline]] void txnLost_(OpStatus st, uint16_t errorCode, AbortKind kind) noexcept;
    /// Fails a live batch's records and releases its slot.
    void failBatch_(PartitionState& ps, InFlightBatch& b, OpStatus st, uint16_t errorCode = 0);
    /// Fails the never-sent builder records of `ps` (handles + one BatchAck).
    void failBuilder_(PartitionState& ps, OpStatus st, uint16_t errorCode = 0);
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
    uint64_t framesResent_ = 0; // retransmits (offerAttempt_)
    // FR (fr-hop-instrumentation): coalescers for conditions retried every duty
    // cycle (empty when FR is compiled out).
    [[no_unique_address]] flight_recorder::Throttle frOfferRefused_{};
    [[no_unique_address]] flight_recorder::Throttle frWindowFull_{};
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
    TxnGate      txnGate_    = TxnGate::NotTransactional;  // Phase 2b F: issuing thread only (FD2)

    // ---- Tier-2 slot pool (preallocated once; see Tier2Slot) ------------------
    // App side (the single batch-issuing thread): tier2AppFree_[0, tier2AppFreeN_) are
    // free slot indices it owns outright; when empty it drains tier2Free_ (the duty
    // side's returns) into it.  Duty side: t2Return_ writes tier2Free_.
    std::vector<Tier2Slot>                    tier2Pool_;
    std::size_t                               tier2SlotBytes_ = 0;
    std::unique_ptr<uint8_t, void(*)(void*)>  tier2FreeBuf_;
    rune::client::detail::OneToOneRingBuffer  tier2Free_;
    std::vector<uint32_t>                     tier2AppFree_;
    std::size_t                               tier2AppFreeN_ = 0;
    std::size_t                               t2Held_ = 0;  ///< slots held by live builders (issuing thread)
    std::thread::id                           t2Thread_ = std::this_thread::get_id();  ///< the bound issuing thread

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

    // encodeScratch_: the Produce wire frame of an acks=None Tier-2 batch (nothing
    // retained; grown once, steady-state = no alloc).  Tier-1 batches are built
    // in place (ProduceFrameBuilder) and acked Tier-2 batches are encoded into
    // their in-flight slot's retained buffer (backlog B4).
    FrameBytes encodeScratch_;

    // ---- Phase 2b F (FD4): the producer identity machine (duty thread only) ----
    // IdUnassigned -> IdRequested -> IdAssigned | IdFailed.  A bare rig (no link wired) starts IdAssigned
    // with kRigProducerId; enableCoordinator() moves an acked, non-transactional producer to IdUnassigned
    // (a transactional one waits for initTransactions, Task 5).  The id comes from the broker
    // (InitProducerId, empty transactional id); batches are HELD (flushBatches returns) until IdAssigned,
    // never sent with a guessed id.  Partitions copy (producerId, epoch) at first use (EOS I-4); a
    // partition created or reset while the id is pending carries producerId -1 and adopts on assignment.
    ProducerIdentity identity_ = IdAssigned{kRigProducerId, 0};
    [[nodiscard]] IdAssigned& assigned_() noexcept { return *std::get_if<IdAssigned>(&identity_); }
    int  identityStepCold_(int64_t nowNs) noexcept;
    /// InitProducerId into a fresh slot (transactional: the transactional id + transactionTimeoutMs; plain: an
    /// empty id), identity -> IdRequested.  nullptr when no slot is free (the caller retries next pass).
    CoordinatorLink::Slot* sendInitProducerId_(int64_t deadlineNs) noexcept;
    void onInitAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s) noexcept;
    void onInitExpired_(CoordinatorLink::Slot& s) noexcept;
    void assignIdentity_(int64_t pid, int16_t epoch) noexcept;
    void restampInflight_(PartitionState& ps) noexcept;
    void publishStatus_() noexcept;
    void drainStatus_() noexcept { statusRing_.drainLatest(appStatus_); }
    // 4096 bytes (~170 snapshots): every identity AND transaction transition publishes one (FD9).
    detail::StatusRing<ProducerStatus, 4096> statusRing_;
    ProducerStatus appStatus_{};   // the bound app thread's cache (it alone drains the ring)
    // Duty side: is the newest snapshot in the ring?  A refused tryPublish keeps the snapshot here and it is
    // re-offered from cold points (sweepTimedOut / the identity step) until accepted; only the newest matters.
    struct StatusSynced {};
    struct StatusPending { ProducerStatus snap{}; };
    std::variant<StatusSynced, StatusPending> statusSync_{};
    void retryStatus_() noexcept;

    // ---- Phase 2b F Task 5: the transaction machine (duty thread only; ProducerTxn.cpp) ----------------
    static constexpr std::size_t kInitializingIdx = static_cast<std::size_t>(TxnState::Initializing);
    static constexpr std::size_t kCommittingIdx   = static_cast<std::size_t>(TxnState::CommittingTransaction);
    static constexpr std::size_t kAbortingIdx     = static_cast<std::size_t>(TxnState::AbortingTransaction);
    /// Pending flush(target, token) ops: a compact prefix [0, txnFlushN_) (zero allocation; one more completes
    /// BrokerError(19)).
    static constexpr std::size_t kMaxTxnFlushes = 8;
    TxnMachine                          txn_{Uninitialized{}};
    std::array<TxnOp, kMaxTxnFlushes>   txnFlushes_{};
    uint8_t                             txnFlushN_        = 0;
    uint64_t                            txnRefusedBegins_ = 0;
    [[nodiscard]] bool transactional_() const noexcept { return !cfg_.transactionalId.empty(); }
    [[nodiscard]] int64_t txnOpTimeoutNs_() const noexcept {
        return (cfg_.transactionOpTimeoutMs > 0 ? cfg_.transactionOpTimeoutMs : 1) * 1'000'000LL;
    }
    [[nodiscard]] bool txnQuiet_() const noexcept {
        const std::size_t i = txn_.index();
        return txnFlushN_ == 0 && i != kInitializingIdx && i != kCommittingIdx && i != kAbortingIdx &&
               txnAddQueue_.empty() && offsetsOp_.index() == 0;
    }
    // ---- Phase 2b F Task 7: sendOffsetsToTransaction (ProducerTxn.cpp; duty thread only) ----
    /// The copy of the call's offsets + group id the op runs from (the caller's span / the Thread-mode ring payload
    /// is only valid during the apply).  Allocated once at construction of a transactional producer (never on a
    /// plain one), reused by every op: ZERO allocation per sendOffsets.
    struct OffsetsScratch {
        std::array<OffsetEntry, kMaxTxnOffsets> entries{};
        std::array<char, kMaxTxnGroupIdBytes>   group{};
        std::array<char, kMaxTxnMemberIdBytes>  member{};       ///< #125
        uint16_t                                n        = 0;
        uint8_t                                 groupLen = 0;
        uint16_t                                memberLen    = 0;
        int32_t                                 generationId = -1;   ///< -1: no member block (legacy)
    };
    std::unique_ptr<OffsetsScratch> offsetsScratch_;
    OffsetsOp                       offsetsOp_{OffsetsIdle{}};
    /// How the op ended.  Failed (a non-retriable answer) and TimedOut (offsets in doubt) poison the transaction (it
    /// can only abort); TimedOut and Cancelled (an abort / close) may leave a leg on the wire, so the coordinator may
    /// have started the transaction (TxnStarted::Maybe, m15).  Done changes nothing.
    enum class OffsetsEnd : uint8_t { Done, Failed, TimedOut, Cancelled };
    void applySendOffsets_(const TxnControl& c, const TxnOp& op);
    int  offsetsStep_(int64_t nowNs) noexcept;
    void onOffsetsAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s) noexcept;
    /// The op's deadline passed (`fromLink`: the link expired that slot itself and releases it afterwards).
    void offsetsExpired_(CoordinatorLink::Slot* fromLink) noexcept;
    /// End the live op: Idle and every slot released first, then (handlers may run user code) the completion.
    /// Returns whether an op was live.
    bool offsetsEnd_(OpStatus st, uint16_t errorCode, std::string_view text, OffsetsEnd how) noexcept;
    // ---- Phase 2b F Task 6: transactional data (ProducerTxn.cpp; duty thread only) ----
    // txnSeen_: the partitions that left NotInTxn in the current transaction (dense slot list, each at most once);
    // txnAddQueue_: those queued (Adding{nullptr}) for the next AddPartitionsToTxn.  Both reserved with the
    // partition table (getOrCreatePartition), so pushing never allocates; reset on EndTxn's final answer.
    std::vector<uint32_t> txnSeen_;
    std::vector<uint32_t> txnAddQueue_;
    uint64_t              txnRecordsRefusedDuty_ = 0;
    uint64_t              ringForeign_ = 0;   ///< see ringForeignMessages()
    /// kAttrTransactional for a transactional producer, 0 for a plain one (immutable after construction): the
    /// attributes a Tier-2 restamp sets, and the cheap "transactional" test of the failure / intake paths.
    uint16_t              txnBatchAttrs_ = 0;
    /// Data of the transaction may flow: InTransaction, or Committing while it waits for the data (AwaitData).
    [[nodiscard]] bool txnFlowing_() const noexcept;
    /// The machine is not InTransaction: a record reaching the duty side now is refused (FD2).  Plain: false.
    [[nodiscard]] bool txnIntakeShut_() const noexcept {
        return txnBatchAttrs_ != 0 && !std::holds_alternative<InTransaction>(txn_);
    }
    /// The (status, code) a record refused / discarded outside a transaction completes with (FD2).
    [[nodiscard]] TxnOutcome txnRefusal_() const noexcept;
    /// flushBatches, a partition that is not Added (cold): a NotInTxn one with data is queued for AddPartitions
    /// while the data flows; everything else is held.  True: it still has work (stays dirty).
    [[gnu::cold, gnu::noinline]] bool txnHold_(PartitionState& ps) noexcept;
    void txnEnroll_(PartitionState& ps) noexcept;
    /// txnStep: AddPartitionsToTxn for the queued partitions (split across slots per kCoordFrameCap).
    int  addStep_(int64_t nowNs) noexcept;
    void onAddPartitionsAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s) noexcept;
    void onAddExpired_(CoordinatorLink::Slot& s) noexcept;
    /// No AddPartitionsToTxn is on the link (a stale one re-sent after EndTxn could open a new transaction).
    [[nodiscard]] bool txnAddsSettled_() const noexcept;
    /// EndTxn's final answer / a terminal end: every partition of the transaction back to NotInTxn.
    void txnResetMembership_() noexcept;
    /// Abort (AwaitData): every record of the producer not yet sent completes `o`, never produced; an
    /// AddPartitionsToTxn still on the link is cancelled.  True when anything was discarded / cancelled.
    bool txnDiscardUnsent_(const TxnOutcome& o) noexcept;
    /// FD2 duty re-check, Ring mode: reads up to `limit` ring messages and refuses each record (never produced).
    /// Stops (lastDrainStop_ Blocked, the message left in the ring) at any message that is not a record / flush.
    [[gnu::cold, gnu::noinline]] int refuseRingTxn_(int limit) noexcept;
    /// Sequence reset of a transactional partition (KIP-360: the coordinator owns the epoch - never bumped
    /// locally): the partition sends nothing new, the transaction must abort and the abort bumps the epoch.
    [[gnu::cold, gnu::noinline]] void txnResetPartition_(PartitionState& ps, uint16_t cause) noexcept;
    /// A txn op (or flush) still owes its completion: the producer is not releasable before it is decided.
    [[nodiscard]] bool txnHasLiveOp_() const noexcept;
    int  txnStepCold_(int64_t nowNs) noexcept;
    int  endStep_(int64_t nowNs) noexcept;
    int  flushStep_(int64_t nowNs) noexcept;
    /// Every record the producer accepted has completed (builders, Tier-2, in flight, deferred).  The ring is
    /// NOT consulted: records ahead of a control op in the ring were drained before it applied (FD1).
    [[nodiscard]] bool txnDataSettled_() const noexcept;
    void completeTxn_(const TxnOp& op, OpKind kind, OpStatus st, uint16_t errorCode, std::string_view text,
                      int64_t value = 0, int64_t aux = 0) noexcept;
    void completeJoined_(const TxnJoiners& j, OpKind kind, OpStatus st, uint16_t errorCode,
                         std::string_view text) noexcept;
    void refuseTxn_(const TxnControl& c, OpStatus st, uint16_t errorCode, std::string_view text) noexcept;
    /// Decode a kMsgTxnControl payload (duty thread); a CQ target is marked producerOp.  False: the message is
    /// shorter than its header (nothing can be completed).  A SendOffsets over the caps / truncated decodes with
    /// empty offsets and group id, so applyTxnControl refuses it InvalidArgument.  offsets / groupId point INTO the ring buffer (valid inside the read callback only).
    [[nodiscard]] bool decodeTxnControl_(const uint8_t* p, std::size_t len, TxnControl& out) noexcept;
    [[gnu::noinline]] OpResult writeTxnControl_(const TxnControl& c, bool overCap) noexcept;
    void markStarted_(TxnStarted s) noexcept;
    void poison_(OpStatus st, uint16_t errorCode, AbortKind kind) noexcept;
    void onTxnLegAnswer_(ReqKind kind, int16_t errorCode) noexcept;
    void enterFatal_(OpStatus st, uint16_t errorCode, std::string_view text) noexcept;
    /// EndTxn's final OK (or an unstarted transaction): the epoch bump first when required, else Ready.
    void endDone_(int64_t nowNs) noexcept;
    /// Committing / Aborting -> Ready; the op and its joiners complete Ok.
    void finishTxn_() noexcept;
    /// The pending end op's deadline: InDoubt, the op completes Timeout.  `releaseSlot`: false when the link
    /// itself expired the slot (it releases it afterwards).
    void endExpired_(bool releaseSlot) noexcept;
    void initExpired_(bool releaseSlot) noexcept;
    void onTxnSlotExpired_(CoordinatorLink::Slot& s) noexcept;
    void onTxnInitAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s, int64_t nowNs) noexcept;
    void onEndTxnAnswer_(const CoordAnswer& a, CoordinatorLink::Slot& s, int64_t nowNs) noexcept;
    /// EndTxn(commit) offered more than once by the link: EndSent{.., Resend} (an earlier offer may have landed).
    void noteEndReoffer_() noexcept;
    /// closeImmediately on a transactional producer: every pending txn op / flush completes `st`, every slot is
    /// released, nothing more is sent (the coordinator's transaction timeout aborts an open transaction).
    void txnDiscard_(OpStatus st, uint16_t errorCode) noexcept;
    /// close(): the open transaction is aborted (or the running op waited for) before the drain.
    [[nodiscard]] bool txnOpenForClose_() const noexcept;
    int  closeTxnStep_(int64_t nowNs) noexcept;
    void startCloseAbort_(int64_t nowNs) noexcept;
    void finishTxnClose_(OpStatus st, uint16_t errorCode) noexcept;
    int  beginDrain_(const detail::OpTarget& target, Token token, int64_t nowNs);
    // EOS follow-ups: +1 per frame offer (first send or re-send) — orders sends
    // against answers without clocks (fast retransmit; OOS-vs-ack ordering).
    uint64_t sendTick_ = 0;

    // ---- In-flight batch tracking (agent-thread only; no mutex needed) -------
    // In-flight batches live in each PartitionState::inflight ring; a batch
    // correlation id encodes (ordinal_, partition slot, ring sequence).
    uint32_t ordinal_ = 0;
    std::optional<CoordinatorLink> coord_;   // Phase 2b F: duty thread only
    uint64_t coordAnswersSeen_  = 0;
    int64_t  lastCoordCorrId_   = 0;
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
        OpStatus status    = OpStatus::Closed;
        uint16_t errorCode = 0;   // authentication (D26): BrokerError + SessionRevoked for a revoked session
    };
    // Phase 2b F Task 5: close() of a transactional producer with a transaction open (or an op running) ends it
    // first - its own EndTxn(abort), or the running commit / abort / init - then drains as LifeDraining.
    struct CloseAbortNone {};
    struct CloseAbortRunning {};
    struct CloseAbortDone { OpStatus status = OpStatus::Ok; uint16_t errorCode = 0; };
    struct LifeEndingTxn {
        detail::OpTarget target{};
        Token            token      = 0;
        int64_t          deadlineNs = 0;   ///< transactionOpTimeoutMs
        std::variant<CloseAbortNone, CloseAbortRunning, CloseAbortDone> own{};
    };
    std::variant<LifeOpen, LifeDraining, LifeClosed, LifeDiscarding, LifeEndingTxn> life_{};
    [[nodiscard]] bool endingTxn_() const noexcept { return std::holds_alternative<LifeEndingTxn>(life_); }
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
        uint16_t     errorCode = 0;
        bool         hadToken = false;
        LifeDraining drain{};
    };
    PendingFail pendingFail_{};
    void runPendingFail_() noexcept;
    bool clientClosedDirect_ = false;  // Direct only: closeFinal ran (owner state)

    // ---- sendBlocking() support (app thread only) -----------------------------
    static int64_t steadyNowNs_() noexcept;
    ClockFn clock_   = &steadyNowNs_;   // overridable by setClockForTesting()
    uint64_t unknownPidStateLoss_ = 0;  // D6 review M1: rule U2 occurrences (diagnostic)
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

    // ---- Phase 2b B: Invoker claims (owner thread only) -----------------------
    std::variant<NoDirectClaim, DirectClaimOpen, DirectClaimAbandoned> directClaim_{};
    // DirectClaimAbandoned's storage: the abandoned claim's frame bytes (the
    // caller's span) while that state holds; otherwise the spare an abandon
    // swaps into the builder (capacity >= kRecordsStart from construction and
    // persisting, so an abandon never allocates).
    FrameBytes            claimQuarantine_;
    uint64_t              claimGen_        = 0;
    uint64_t              claimsAbandoned_ = 0;
    std::vector<uint32_t> claimOffsets_;  ///< Direct claimBatch value offsets (capacity persists)

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
    void failDeferred_(OpStatus st, uint16_t errorCode = 0);
    /// sweepTimedOut: fail deferred records accepted >= deliveryNs ago.
    void expireDeferred_(int64_t nowNs, int64_t deliveryNs);
    /// Accept one record / Tier-2 token into the arena (inDuty_).  Validates the
    /// partition first (an unaddressable one is InvalidTopic at send time).
    SendResult deferRecord_(int32_t topicId, int32_t partition, std::span<const uint8_t> key,
                            std::span<const uint8_t> value, uint8_t flags, int64_t ackHandle,
                            RejectReason& why) noexcept;
    RejectReason deferTier2_(uint32_t slot) noexcept;
    void tier2Collect_() noexcept;
    // ---- Tier-2 partition FIFO + pool return (duty side) -----------------------
    void t2Push_(PartitionState& ps, uint32_t slot) noexcept {
        tier2Pool_[slot].next = kNoTier2Slot;
        if (ps.t2Tail == kNoTier2Slot) ps.t2Head = slot; else tier2Pool_[ps.t2Tail].next = slot;
        ps.t2Tail = slot;
    }
    /// Unlink the FIFO head; the slot is still the duty side's (t2Return_ gives it back).
    uint32_t t2Pop_(PartitionState& ps) noexcept {
        const uint32_t slot = ps.t2Head;
        ps.t2Head = tier2Pool_[slot].next;
        if (ps.t2Head == kNoTier2Slot) ps.t2Tail = kNoTier2Slot;
        return slot;
    }
    /// Give a finished slot back to the app (SPSC free ring; capacity = pool size, so
    /// the write cannot fail while each slot is returned exactly once).
    void t2Return_(uint32_t slot) noexcept;
    uint8_t* pushDeferred_(const DeferredHdr& h) noexcept;
    PartitionState* findPartition_(int32_t topicId, int32_t partition) noexcept;
    /// Removes a record's per-partition / unknown accounting.
    void unaccountDeferred_(const DeferredHdr& h) noexcept;
    /// The record's ack handle (record: wantAck handle; Tier-2: returns the slot
    /// to the pool and returns its handle).  0 = void.
    uint64_t takeDeferredHandle_(std::size_t off, bool* tier2 = nullptr,
                                 int32_t* recordCount = nullptr) noexcept;
    /// Outside inDuty_: run the apply pass if `ps`'s (or unknown partitions')
    /// deferred records must go first.  Returns false if this partition still
    /// has deferred records afterwards (the new send must not overtake them).
    bool settleDeferredFor_(int32_t topicId, int32_t partition) noexcept;
    void noteDeferredLoss_(const char* what) noexcept;
};

// Phase 2b F Task 5: the machine's alternative index IS the TxnState value.
static_assert(std::variant_size_v<TxnMachine> == static_cast<std::size_t>(TxnState::FatalError) + 1,
              "TxnState is the machine's index");
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::Initializing), TxnMachine>, Initializing>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::Ready), TxnMachine>, Ready>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::InTransaction), TxnMachine>, InTransaction>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::CommittingTransaction), TxnMachine>, Committing>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::AbortingTransaction), TxnMachine>, Aborting>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::AbortableError), TxnMachine>, AbortableError>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(TxnState::FatalError), TxnMachine>, FatalError>);

} // namespace rune::client::producer
