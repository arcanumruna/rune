// src/producer/ProducerImpl.cpp
//
// CB-2 — ProducerImpl: agent-side single-writer accumulator.
//
// Hot path discipline:
//   submitSendRecord(): NO heap allocation, NO lock.  Writes a compact
//   SEND_RECORD into the pre-allocated ring (one memcpy into the ring slot).
//   All batching, encoding, and ack routing happen on the agent thread.

#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/agent/AgentProduceSink.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/Completions.h>
#include <rune/client/detail/Crc32cPatch.h>
#include <rune/client/transport/FrameCrc.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune_fr/FlightRecorder.h>

#include <concurrent/BackOffIdleStrategy.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <climits>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <type_traits>

#if defined(_WIN32)
#include <process.h> // _getpid
#else
#include <unistd.h>  // getpid
#endif

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

// Unique key for the partition map: collision-free bit-packing.
// Upper 32 bits = topicId, lower 32 bits = partition.
// This is provably collision-free for all (topicId, partition) pairs,
// unlike the old topicId*100000+partition formula which collides for
// partition >= 100000 or high topicIds.
inline int64_t partKey(int32_t topicId, int32_t partition) noexcept {
    return (static_cast<int64_t>(topicId) << 32) | static_cast<uint32_t>(partition);
}

// Write a 4-byte LE int32 at buf[off].
inline void writeLE32(uint8_t* buf, int off, int32_t v) noexcept {
    const uint32_t u = static_cast<uint32_t>(v);
    buf[off+0] = static_cast<uint8_t>(u);
    buf[off+1] = static_cast<uint8_t>(u >> 8);
    buf[off+2] = static_cast<uint8_t>(u >> 16);
    buf[off+3] = static_cast<uint8_t>(u >> 24);
}

// Write a 4-byte LE uint32 at buf[off].
inline void writeLE32u(uint8_t* buf, int off, uint32_t v) noexcept {
    buf[off+0] = static_cast<uint8_t>(v);
    buf[off+1] = static_cast<uint8_t>(v >> 8);
    buf[off+2] = static_cast<uint8_t>(v >> 16);
    buf[off+3] = static_cast<uint8_t>(v >> 24);
}

// Write an 8-byte LE int64 at buf[off].
inline void writeLE64(uint8_t* buf, int off, int64_t v) noexcept {
    const uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i)
        buf[off+i] = static_cast<uint8_t>(u >> (i * 8));
}

// Read a 4-byte LE int32 from buf[off].
inline int32_t readLE32(const uint8_t* buf, int off) noexcept {
    return static_cast<int32_t>(
        static_cast<uint32_t>(buf[off+0])       |
        (static_cast<uint32_t>(buf[off+1]) << 8)  |
        (static_cast<uint32_t>(buf[off+2]) << 16) |
        (static_cast<uint32_t>(buf[off+3]) << 24));
}

// Read a 4-byte LE uint32 from buf[off].
inline uint32_t readLE32u(const uint8_t* buf, int off) noexcept {
    return static_cast<uint32_t>(buf[off+0])       |
           (static_cast<uint32_t>(buf[off+1]) << 8)  |
           (static_cast<uint32_t>(buf[off+2]) << 16) |
           (static_cast<uint32_t>(buf[off+3]) << 24);
}

// Read an 8-byte LE int64 from buf[off].
inline int64_t readLE64(const uint8_t* buf, int off) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(buf[off+i]) << (i * 8);
    return static_cast<int64_t>(v);
}

// Current wall-clock timestamp: epoch MILLISECONDS (the record-timestamp wire contract, Kafka parity — D7).
inline int64_t wallMs() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Varint / varlong length helpers — exact byte count for RecordBatch encoding.
// Used by recordBodyContrib() to pre-flight the frame-cap check (task 3).
// ZERO-COST on the normal path: only called when frameCapped=true.
// ---------------------------------------------------------------------------

inline int32_t varlongLen(int64_t v) noexcept {
    // Zigzag encoding: matches writeVarlong() in RecordBatch.h.
    const uint64_t u = static_cast<uint64_t>((v << 1) ^ (v >> 63));
    uint64_t tmp = u;
    int n = 1;
    while (tmp >= 0x80u) { tmp >>= 7; ++n; }
    return n;
}

inline int32_t varintLen(int32_t v) noexcept {
    // C1 FIX: RecordBatch.h::writeVarint() is ZIGZAG (it delegates to writeVarlong,
    // see Varint.h).  The pre-flight model MUST match writeVarint exactly, or it
    // under-counts (e.g. writeVarint(64) = 2 bytes, plain-varint = 1) and emitted
    // frames overshoot maxFrameBytes.  Delegate to varlongLen for an exact match.
    return varlongLen(static_cast<int64_t>(v));
}

// ---------------------------------------------------------------------------
// recordBodyContrib — exact bytes that RecordBatchBuilder::append() adds to
// its internal records_ buffer for one record.
//
// Matches RecordBatch.h::append() exactly:
//   contribution = varint(bodySize) + bodySize
//   bodySize     = 1(attrs) + varlong(tsDelta) + varint(offsetDelta)
//               + varint(keyLen) + keyLen
//               + varint(valueLen) + valueLen
//               + 1(headerCount=0)
// ---------------------------------------------------------------------------
inline int32_t recordBodyContrib(int32_t keyLen, int32_t valueLen,
                                 int64_t tsDelta, int32_t offsetDelta) noexcept {
    const int32_t bodySize =
        1 +                        // attributes byte
        varlongLen(tsDelta) +      // tsDelta (zigzag varlong)
        varintLen(offsetDelta) +   // offsetDelta
        varintLen(keyLen) +        // keyLen varint
        keyLen +                   // key bytes
        varintLen(valueLen) +      // valueLen varint
        valueLen +                 // value bytes
        1;                         // headerCount = 0 → varint(0) = 1 byte
    return varintLen(bodySize) + bodySize;
}

// Backlog B1: offer a freshly encoded frame whose RecordBatch CRC is still
// unset.  A sink that seals on accept (the Aeron transport via
// AgentProduceSink, CountingSink) gets the FrameCrc and computes the CRC only
// once the frame is accepted; any other sink (the std::function SendFn test /
// cold seams) gets a sealed frame, as before.
template <class Sink>
inline bool offerFresh(const Sink& sink, int32_t topicId, int32_t partition, int32_t streamId,
                       uint8_t* buf, std::size_t len, transport::FrameCrc crc) {
    if constexpr (std::is_invocable_r_v<bool, const Sink&, int32_t, int32_t, int32_t, uint8_t*,
                                        std::size_t, transport::FrameCrc>) {
        return sink(topicId, partition, streamId, buf, len, crc);
    } else {
        (void)transport::sealFrame(buf, len, crc);
        return sink(topicId, partition, streamId, static_cast<const uint8_t*>(buf), len);
    }
}

// Backlog B2: re-stamps producerId / producerEpoch / baseSequence (14 contiguous
// bytes) of a SEALED RecordBatch at `batch` (`len` bytes) and patches its CRC
// incrementally — bit-exact with a full recompute, without rescanning the batch.
inline void restampBatchIdentity(uint8_t* batch, std::size_t len, int64_t producerId,
                                 int16_t producerEpoch, int32_t baseSequence) noexcept {
    namespace rb = rune::protocol::batch;
    constexpr std::size_t kOff = rb::kProducerIdOffset;
    constexpr std::size_t kN   = rb::kRecordCountOffset - rb::kProducerIdOffset;
    static_assert(rb::kProducerEpochOffset == rb::kProducerIdOffset + 8 &&
                  rb::kBaseSequenceOffset == rb::kProducerIdOffset + 10 && kN == 14,
                  "identity fields must stay contiguous");
    uint8_t old[kN];
    std::memcpy(old, batch + kOff, kN);
    auto put = [](uint8_t* p, uint64_t v, int bytes) noexcept {
        for (int i = 0; i < bytes; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i));
    };
    put(batch + rb::kProducerIdOffset, static_cast<uint64_t>(producerId), 8);
    put(batch + rb::kProducerEpochOffset, static_cast<uint16_t>(producerEpoch), 2);
    put(batch + rb::kBaseSequenceOffset, static_cast<uint32_t>(baseSequence), 4);
    const uint32_t crc = rune::protocol::detail::getLE32(batch + rb::kCrcOffset);
    put(batch + rb::kCrcOffset,
        rune::client::detail::crc32cPatch(crc, len - rb::kAttributesOffset, kOff - rb::kAttributesOffset,
                                          old, batch + kOff, kN),
        4);
}

// ---------------------------------------------------------------------------
// producerId generation (F5 fix) — see ProducerImpl.h's producerId_ comment.
//
// The old code shared one hardcoded magic-stub constant across every
// ProducerImpl instance; the broker dedups idempotent batches by
// (producerId, partition, baseSequence), so two producers on the same
// partition collided and silently deduplicated each other's records.  This
// generator draws a fresh, effectively-unique, non-negative 64-bit id per
// instance (and per epoch-overflow rotation) instead.
//
// TODO(milestone-c): a client-guessed id is only a stopgap. The durable fix
// is a broker-allocated InitProducerId RPC with zombie-fencing (a monotonic
// epoch the broker itself hands out and validates), which this local
// generator cannot provide (no protection against two processes drawing the
// same id, however astronomically unlikely).
// ---------------------------------------------------------------------------

// SplitMix64 — fast, well-distributed 64-bit mixer (public-domain algorithm;
// used here purely to combine entropy sources, not as a CSPRNG).
inline uint64_t splitmix64(uint64_t x) noexcept {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// The old shared magic-stub producerId — the generator below must never
// reissue it (that would silently resurrect the collision bug this fix
// removes).
inline constexpr int64_t kLegacyStubProducerId =
    static_cast<int64_t>(0x4352554E'4543422DULL); // "RUNECB-"

// Draws a fresh, non-negative 64-bit producerId by mixing three independent,
// cheap entropy sources: std::random_device (OS entropy), steady_clock (a
// monotonic per-call timestamp), and the process id (separates concurrent
// processes started at the same instant). The result is masked to 63 bits
// (always >= 0 — the wire protocol reserves -1 for "non-idempotent"); on the
// vanishingly unlikely chance it comes out as 0 or the old stub, it redraws.
//
// std::random_device may throw (no entropy source / sandboxed /dev/urandom), and
// this function is noexcept (a throw would std::terminate the producer ctor).  On
// failure the OS-entropy term falls back to steady_clock ^ pid; the clock and pid
// terms are mixed in below regardless, and the redraw loop still advances
// because steady_clock moves between iterations.
uint64_t osEntropyOrFallback(uint64_t clockBits, uint64_t pidBits, uint64_t selfBits) noexcept {
    try {
        std::random_device rd;
        return (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    } catch (...) {
        return splitmix64(clockBits ^ splitmix64(pidBits) ^ splitmix64(selfBits ^ 0xA5A5A5A5A5A5A5A5ULL));
    }
}

// `self` (the ProducerImpl address) separates two producers of one process
// constructed within the same steady_clock tick when OS entropy is unavailable.
int64_t generateProducerId(const void* self) noexcept {
    const uint64_t selfBits = static_cast<uint64_t>(reinterpret_cast<std::uintptr_t>(self));
    for (;;) {
        const uint64_t clockBits = static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        const uint64_t pidBits = static_cast<uint64_t>(
#if defined(_WIN32)
            ::_getpid()
#else
            ::getpid()
#endif
        );
        const uint64_t rdBits = osEntropyOrFallback(clockBits, pidBits, selfBits);

        uint64_t mixed = splitmix64(rdBits);
        mixed = splitmix64(mixed ^ splitmix64(clockBits));
        mixed = splitmix64(mixed ^ splitmix64(pidBits));
        mixed = splitmix64(mixed ^ splitmix64(selfBits));

        const int64_t candidate =
            static_cast<int64_t>(mixed & 0x7FFFFFFFFFFFFFFFULL); // mask to non-negative
        if (candidate != 0 && candidate != kLegacyStubProducerId) {
            return candidate;
        }
        // 0 or an exact collision with the retired stub: redraw (astronomically
        // unlikely — loop rather than special-case it).
    }
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Ring buffer helpers (mirrors ClientAgent pattern)
// ---------------------------------------------------------------------------

namespace {

std::unique_ptr<uint8_t, void(*)(void*)>
makeRingBuffer(std::size_t size, std::size_t align)
{
    void* raw = nullptr;
    if (align < sizeof(void*)) align = sizeof(void*);
    if (::posix_memalign(&raw, align, size) != 0) raw = nullptr;
    if (!raw) throw std::bad_alloc{};
    ::memset(raw, 0, size);
    return {static_cast<uint8_t*>(raw), [](void* p){ ::free(p); }};
}

int32_t ringTotalBytes(int32_t cap) noexcept {
    return cap + rune::client::detail::M2ORingBuffer::trailerLength();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

ProducerImpl::ProducerImpl(ProducerConfig cfg,
                            int32_t ringCap)
    : cfg_(cfg)
    , ringBuf_(makeRingBuffer(static_cast<std::size_t>(ringTotalBytes(ringCap)),
                              kRingAlignment))
    , ring_(ringBuf_.get(), static_cast<std::size_t>(ringTotalBytes(ringCap)))
    , producerId_(generateProducerId(this))  // F5: unique per instance, never the old shared stub
{
    // Pre-reserve partition map and encode scratch to avoid allocs on steady-state.
    partitions_.reserve(16);
    encodeScratch_.reserve(64 * 1024); // Produce wire frame (grown once)
    // M2: frame-cap threshold computed ONCE (cfg_ is immutable).  With the default
    // config (maxFrameBytes == batchBytes) frameCapped_ is FALSE, so the per-record
    // pre-flight collapses out entirely — zero happy-path cost.  It only engages when
    // a user sets batchBytes larger than maxFrameBytes.
    frameCapped_ = (cfg_.maxFrameBytes > 0 && cfg_.maxFrameBytes < cfg_.batchBytes);
    effCap_      = frameCapped_ ? cfg_.maxFrameBytes : cfg_.batchBytes;
    // EOS-OOS: an idempotent producer never has more batches of one partition
    // outstanding than the broker's dedup window (the shared protocol constant):
    // a retransmit older than the window could otherwise not be recognised as a
    // duplicate (Kafka caps max.in.flight at 5 for the same reason).
    constexpr int32_t kWindow = rune::protocol::client::kIdempotentMaxInFlightBatches;
    // N-2: the in-flight ring rounds its capacity up to a power of two and
    // full() bounds the span INCLUDING holes by it — only a power-of-two window
    // keeps every retransmit inside the broker's window.
    static_assert(kWindow > 0 && (kWindow & (kWindow - 1)) == 0,
                  "kIdempotentMaxInFlightBatches must be a power of two");
    if (cfg_.acks != Acks::None && cfg_.maxInFlight > kWindow) {
        std::fprintf(stderr,
                     "ProducerImpl: maxInFlight=%d clamped to %d (idempotent producer: the broker's "
                     "dedup window, rune::protocol::client::kIdempotentMaxInFlightBatches)\n",
                     static_cast<int>(cfg_.maxInFlight), static_cast<int>(kWindow));
        cfg_.maxInFlight = kWindow;
    }
}

// ---------------------------------------------------------------------------
// App-thread: zero-alloc void fast path
// ---------------------------------------------------------------------------

bool ProducerImpl::submitSendRecord(int32_t topicId,
                                     int32_t partition,
                                     const Key& key,
                                     std::span<const uint8_t> value,
                                     bool wantAck,
                                     int64_t correlationId) noexcept
{
    const std::size_t keyLen   = key.size();
    const std::size_t valueLen = value.size();
    const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + valueLen;

    // Public contract (backlog B6): a record is capped at kMaxRecordBytes
    // (key + value).  Returns false (not accepted) on an oversized record;
    // trySubmit checks first and reports SendResult::MessageTooLarge — never a
    // silent drop (fix1 minor 12).
    if (keyLen + valueLen > rune::client::Producer::kMaxRecordBytes) {
        return false;
    }

    // ZERO-COPY (#3): claim the ring slot and write the SEND_RECORD DIRECTLY into
    // it — the value is copied ONCE (app buffer → ring), versus the old path which
    // built the record in a stack scratch and then copied scratch → ring (value
    // copied twice).  No stack scratch, no intermediate copy, no heap, no lock.
    //
    // Task 13 review carry-over: "never an orphaned claim" is STRUCTURAL — once a
    // claim succeeded, every exit that does not commit it aborts it (an orphaned
    // claim would wedge the consumer at this record forever).
    aeron::util::index_t idx = -1;
    bool committed = false;
    try {
        idx = ring_.tryClaim(kMsgSendRecord, totalLen);
        if (idx < 0) return false; // INSUFFICIENT_CAPACITY → back-pressure

        uint8_t* dst = ring_.base() + idx;
        writeLE32(dst, kRecTopicIdOff, topicId);
        writeLE32(dst, kRecPartOff,    partition);
        dst[kRecKeyLenOff] = static_cast<uint8_t>(keyLen);
        std::memset(dst + kRecKeyBytesOff, 0, Key::kMaxInline); // pad inline key
        if (keyLen > 0) std::memcpy(dst + kRecKeyBytesOff, key.bytes().data(), keyLen);
        dst[kRecFlagsOff] = wantAck ? kFlagWantAck : 0u;
        writeLE64(dst, kRecCorrIdOff, correlationId);
        writeLE32u(dst, kRecValLenOff, static_cast<uint32_t>(valueLen));
        if (valueLen > 0) std::memcpy(dst + kRecValBytesOff, value.data(), valueLen);

        ring_.commit(idx);
        committed = true;
        // App-thread: record handed to the app→agent ring (corr=correlationId).
        RUNE_FR_MARK_PRODUCE(rune_fr::Site::ClientAppSend,
                             static_cast<uint32_t>(correlationId));
        return true;
    } catch (...) {
        if (committed) return true;  // published; only the trailing mark threw
        if (idx >= 0) {
            try {
                ring_.abort(idx);  // the claim becomes padding: the consumer skips it
            } catch (...) {
                // abort() refuses only a claim the consumer already released
                // (unblock()), i.e. one that is no longer ours to abort.
            }
        }
        return false; // preserve noexcept contract
    }
}

int64_t ProducerImpl::steadyNowNs_() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

SendResult ProducerImpl::trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                   std::span<const uint8_t> value, int64_t ackHandle,
                                   RejectReason* why) noexcept {
    if (mode_ == IngressMode::Direct) {
        RejectReason w = RejectReason::None;
        const SendResult r = tryAppendDirect_(topicId, partition, key, value, ackHandle, w);
        if (why != nullptr) *why = w;
        return r;
    }
    const SendResult r = trySubmitRing_(topicId, partition, key, value, ackHandle);
    if (why != nullptr) [[unlikely]] {
        *why = r == SendResult::Ok            ? RejectReason::None
             : r == SendResult::Closed        ? RejectReason::Closed
             : r == SendResult::ValueTooLarge ? RejectReason::ValueTooLarge
             : r == SendResult::MessageTooLarge ? RejectReason::MessageTooLarge
                                              : RejectReason::BackPressured;
    }
    return r;
}

SendResult ProducerImpl::trySubmitRing_(int32_t topicId, int32_t partition, const Key& key,
                                        std::span<const uint8_t> value, int64_t ackHandle) noexcept {
    // fix1 C2/I3 + F8c C1(a): closed is a terminal state, decided by the claim
    // itself — the ring's closed bit lives in the tail word tryClaim() CASes, so
    // a send is either accepted before Client::closeImpl_ closed the ring (and
    // drained by its close path) or rejected Closed, never accepted after it
    // (the old separate pre-check could pass just before the close).  No extra
    // load on the hot path: the rejected claim is told apart from back-pressure
    // only on the (cold) failure branch below.  sendBlocking() still checks
    // closed_() before ever touching the pump hook's context.
    // Backlog B6: one O(1) compare at append (key + value <= 16 KiB).
    if (key.size() + value.size() > rune::client::Producer::kMaxRecordBytes) [[unlikely]] {
        return SendResult::MessageTooLarge;
    }
    // fix1 minor 11: a value that can NEVER fit this ring (maxMsgLength is a
    // function of ringCap, which can be far smaller than kMaxRecordBytes —
    // e.g. a small test ring) must not be reported as retryable back-pressure:
    // no amount of draining ever makes room for a message the ring cannot hold,
    // so BackPressured here would livelock sendBlocking's retry loop forever.
    const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + value.size();
    if (totalLen > static_cast<std::size_t>(ring_.maxMsgLength())) return SendResult::ValueTooLarge;
    if (submitSendRecord(topicId, partition, key, value, ackHandle != 0, ackHandle)) [[likely]] {
        return SendResult::Ok;
    }
    return ring_.closed() ? SendResult::Closed : SendResult::BackPressured;
}

SendResult ProducerImpl::tryAppendDirect_(int32_t topicId, int32_t partition, const Key& key,
                                          std::span<const uint8_t> value, int64_t ackHandle,
                                          RejectReason& why) noexcept {
    // Task 14: the owner thread IS the duty-cycle thread — append straight into
    // the partition builder (no ring, no CAS, no second copy).  Every piece of
    // state touched here is owner-thread-only; any other thread is rejected
    // FIRST, before it reads any of it (incl. the closed bool).
    if (std::this_thread::get_id() != dutyThreadId_) {
        why = RejectReason::WrongThread;
        return SendResult::WrongThread;
    }
    // Closed is terminal, checked before anything else on the owner thread: a
    // Producer outliving its Client never appends, never reaches the pump hook.
    if (directClosed_) {
        why = RejectReason::Closed;
        return SendResult::Closed;
    }
    if (key.size() + value.size() > rune::client::Producer::kMaxRecordBytes) [[unlikely]] {  // B6
        why = RejectReason::MessageTooLarge;
        return SendResult::MessageTooLarge;
    }
    const auto kb = key.bytes();
    const uint8_t flags = ackHandle != 0 ? kFlagWantAck : uint8_t{0};
    // Task 14 fix1 C1 / fix2: inside this producer's own duty work (a callback
    // it fired mid-walk / mid-reset) the record is DEFERRED into the arena and
    // applied when that work completes.
    if (inDuty_) [[unlikely]] return deferRecord_(topicId, partition, kb, value, flags, ackHandle, why);
    // Outside: only THIS partition's deferred records (and, conservatively,
    // records for not-yet-created partitions) must go first — applied inline
    // now.  If this partition is still stalled the send must not overtake them.
    if (!deferred_.empty()) [[unlikely]] {
        if (!settleDeferredFor_(topicId, partition)) {
            why = RejectReason::PartitionDeferred;
            return SendResult::BackPressured;
        }
    }
    try {
        const auto a = appendRecord_(topicId, partition, kb.data(), static_cast<uint8_t>(kb.size()),
                                     value.data(), static_cast<uint32_t>(value.size()), flags,
                                     static_cast<uint64_t>(ackHandle), lastNowNs_);
        if (a == rune::client::detail::ControlledAction::ABORT) {
            why = RejectReason::BackPressured;
            return SendResult::BackPressured;
        }
    } catch (const std::length_error&) {
        // getOrCreatePartition's partition-table cap (nothing was appended; the
        // caller still owns the ack handle): a partition this producer can never
        // address — reported, never retried as back-pressure.
        why = RejectReason::Unaddressable;
        return SendResult::InvalidTopic;
    } catch (...) {
        // fix1 M1: anything else (bad_alloc growing a builder / the ack ring) —
        // nothing was committed (appendRecord_'s ordering), the caller still owns
        // the handle: "not accepted right now".
        why = RejectReason::BackPressured;
        return SendResult::BackPressured;
    }
    why = RejectReason::None;
    RUNE_FR_MARK_PRODUCE(rune_fr::Site::ClientAppSend, static_cast<uint32_t>(ackHandle));
    return SendResult::Ok;
}

void ProducerImpl::enableDirectIngress(const DirectIngress& d) {
    // Clamps: the arena must fit two max-size records and one partition's share
    // must fit one (a legal deferred send can never livelock as back-pressure),
    // and one partition may hold at most its share (default min(arena/2,
    // 4 x batchBytes)) so a stalled partition cannot exhaust the arena.
    constexpr std::size_t kMaxRec = DeferredArena::recordSize(
        static_cast<std::size_t>(kRecHeaderSize) + rune::client::Producer::kMaxRecordBytes);
    const std::size_t want = cfg_.directDeferredBytes > 0
                                 ? static_cast<std::size_t>(cfg_.directDeferredBytes) : 0u;
    deferred_.init(std::max(want, 2 * kMaxRec));
    const std::size_t arena = deferred_.capacity();
    std::size_t share = cfg_.directDeferredPartitionBytes > 0
        ? static_cast<std::size_t>(cfg_.directDeferredPartitionBytes)
        : std::min(arena / 2, 4 * static_cast<std::size_t>(std::max(cfg_.batchBytes, 1)));
    deferPartitionCap_ = std::clamp(share, kMaxRec, arena);
    setPumpFn(d.pump, d.pumpCtx, d.owner);
    mode_      = IngressMode::Direct;
    lastNowNs_ = d.nowNs;
}

PartitionState* ProducerImpl::findPartition_(int32_t topicId, int32_t partition) noexcept {
    const uint32_t i = partitionIndex_.find(partKey(topicId, partition));
    return (i == detail::FlatIndexMap::kNone) ? nullptr : partitions_[i].get();
}

uint8_t* ProducerImpl::pushDeferred_(const DeferredHdr& h) noexcept {
    uint8_t* d = deferred_.push(h);
    if (d == nullptr && !deferScanning_) {
        deferred_.compactDead();  // reclaim records applied / failed since the last pass
        d = deferred_.push(h);
    }
    return d;
}

SendResult ProducerImpl::deferRecord_(int32_t topicId, int32_t partition,
                                      std::span<const uint8_t> key, std::span<const uint8_t> value,
                                      uint8_t flags, int64_t ackHandle, RejectReason& why) noexcept {
    PartitionState* ps = findPartition_(topicId, partition);
    // fix2 minor 4: validate BEFORE accepting — a partition the table can never
    // hold is reported now, never accepted and then lost at apply time.
    // Conservative: every unknown deferred record counts as a new partition.
    if (ps == nullptr && partitions_.size() + deferredUnknown_ >= maxPartitions_) {
        why = RejectReason::Unaddressable;
        return SendResult::InvalidTopic;
    }
    const uint32_t len = static_cast<uint32_t>(kRecHeaderSize + value.size());
    const std::size_t size = DeferredArena::recordSize(len);
    // Per-partition share; records for not-yet-created partitions share ONE
    // aggregate share (fix3) — neither can exhaust the arena.
    if (ps != nullptr ? ps->deferredBytes + size > deferPartitionCap_
                      : deferredUnknownBytes_ + size > deferPartitionCap_) {
        why = RejectReason::DeferredFull;
        return SendResult::BackPressured;
    }
    DeferredHdr h;
    h.len       = len;
    h.type      = static_cast<uint8_t>(kMsgSendRecord);
    h.flags     = ps == nullptr ? kDefUnknownPartition : uint8_t{0};
    h.topicId   = topicId;
    h.partition = partition;
    h.submitNs  = lastNowNs_;
    uint8_t* d = pushDeferred_(h);
    if (d == nullptr) {
        why = RejectReason::DeferredFull;
        return SendResult::BackPressured;
    }
    writeLE32(d, kRecTopicIdOff, topicId);
    writeLE32(d, kRecPartOff, partition);
    d[kRecKeyLenOff] = static_cast<uint8_t>(key.size());
    std::memset(d + kRecKeyBytesOff, 0, Key::kMaxInline);
    if (!key.empty()) std::memcpy(d + kRecKeyBytesOff, key.data(), key.size());
    d[kRecFlagsOff] = flags;
    writeLE64(d, kRecCorrIdOff, ackHandle);
    writeLE32u(d, kRecValLenOff, static_cast<uint32_t>(value.size()));
    if (!value.empty()) std::memcpy(d + kRecValBytesOff, value.data(), value.size());
    if (ps != nullptr) {
        ++ps->deferredCount;
        ps->deferredBytes += static_cast<uint32_t>(size);
    } else {
        ++deferredUnknown_;
        deferredUnknownBytes_ += size;
    }
    why = RejectReason::None;
    return SendResult::Ok;  // the handle (if any) is owned by the producer now
}

RejectReason ProducerImpl::deferTier2_(std::unique_ptr<Tier2Entry>& entry) noexcept {
    PartitionState* ps = findPartition_(entry->topicId, entry->partition);
    if (ps == nullptr && partitions_.size() + deferredUnknown_ >= maxPartitions_) {
        return RejectReason::Unaddressable;
    }
    const std::size_t size = DeferredArena::recordSize(kT2TokenSize);
    if (ps != nullptr ? ps->deferredBytes + size > deferPartitionCap_
                      : deferredUnknownBytes_ + size > deferPartitionCap_) {
        return RejectReason::DeferredFull;
    }
    DeferredHdr h;
    h.len       = kT2TokenSize;
    h.type      = static_cast<uint8_t>(kMsgTier2Batch);
    h.flags     = ps == nullptr ? kDefUnknownPartition : uint8_t{0};
    h.topicId   = entry->topicId;
    h.partition = entry->partition;
    h.submitNs  = lastNowNs_;
    uint8_t* d = pushDeferred_(h);
    if (d == nullptr) return RejectReason::DeferredFull;
    writeLE64(d, 0, static_cast<int64_t>(reinterpret_cast<uintptr_t>(entry.release())));
    if (ps != nullptr) {
        ++ps->deferredCount;
        ps->deferredBytes += static_cast<uint32_t>(size);
    } else {
        ++deferredUnknown_;
        deferredUnknownBytes_ += size;
    }
    return RejectReason::None;
}

void ProducerImpl::unaccountDeferred_(const DeferredHdr& h) noexcept {
    if (h.flags & kDefUnknownPartition) {
        --deferredUnknown_;
        deferredUnknownBytes_ -= DeferredArena::recordSize(h.len);
        return;
    }
    if (PartitionState* ps = findPartition_(h.topicId, h.partition)) {
        --ps->deferredCount;
        ps->deferredBytes -= static_cast<uint32_t>(DeferredArena::recordSize(h.len));
    }
}

uint64_t ProducerImpl::takeDeferredHandle_(std::size_t off, bool* tier2, int32_t* recordCount) noexcept {
    const DeferredHdr* h = deferred_.hdr(off);
    const uint8_t* p = deferred_.payload(off);
    if (h->type == kMsgTier2Batch) {
        std::unique_ptr<Tier2Entry> e(
            reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
        if (tier2 != nullptr) *tier2 = true;
        if (recordCount != nullptr) *recordCount = e->recordCount;
        return e->ackHandle;
    }
    if (tier2 != nullptr) *tier2 = false;
    if (recordCount != nullptr) *recordCount = 1;
    return (p[kRecFlagsOff] & kFlagWantAck) ? static_cast<uint64_t>(readLE64(p, kRecCorrIdOff)) : 0u;
}

void ProducerImpl::noteDeferredLoss_(const char* what) noexcept {
    ++deferredApplyFailures_;
    const uint64_t n = deferredApplyFailures_;
    if ((n & (n - 1)) == 0) {  // rate-limited: 1st, 2nd, 4th, 8th, ...
        std::fprintf(stderr, "rune-client: deferred producer record %s (total %llu)\n", what,
                     static_cast<unsigned long long>(n));
    }
}

bool ProducerImpl::settleDeferredFor_(int32_t topicId, int32_t partition) noexcept {
    const PartitionState* ps = findPartition_(topicId, partition);
    if (deferredUnknown_ == 0 && (ps == nullptr || ps->deferredCount == 0)) return true;
    // fix3: this partition's deferred records already failed to apply in the
    // LATEST pass.  Outside duty work its state cannot have changed since:
    // everything that can unblock it (an ack, a flush, a sweep, failAll) runs in
    // a DutyScope whose end runs a new pass (deferPass_ moves on).  So another
    // pass now is futile — refuse in O(1).
    if (ps != nullptr && ps->deferredCount > 0 && ps->deferBlockedPass == deferPass_) return false;
    { const DutyScope apply(*this); }  // outermost: its end runs the apply pass
    ps = findPartition_(topicId, partition);
    return ps == nullptr || ps->deferredCount == 0;
}

void ProducerImpl::applyDeferred_() noexcept {
    using rune::client::detail::ControlledAction;
    // Runs at the end of the OUTERMOST DutyScope: no walk over partitions_ /
    // ackRing / a builder is in progress.  inDuty_ is still set, so a callback
    // fired from here (a failure completion) defers again — appended past `end`,
    // processed by the next pass.
    const bool prevScan = deferScanning_;
    deferScanning_ = true;
    ++deferPasses_;
    if (++deferPass_ == 0) deferPass_ = 1;
    const uint32_t failAt = debugFailAt_;
    const uint32_t failEnd = debugFailAt_ + debugFailCount_;
    debugFailAt_ = 0;
    debugFailCount_ = 0;
    uint32_t visited = 0;
    // fix3: user code (a lost record's failure callback) NEVER runs inside the
    // pass — [w, r) holds stale bytes until the slide-down.  Lost handles are
    // collected here (fixed, no allocation) and completed after setTail(); if
    // more records fail than fit, the rest of the pass keeps its records
    // (applied by the next pass).
    static constexpr std::size_t kMaxLost = 16;
    struct Lost {
        uint64_t handle      = 0;
        int32_t  topicId     = 0;
        int32_t  partition   = 0;
        int32_t  recordCount = 1;
        bool     tier2       = false;
    };
    std::array<Lost, kMaxLost> lost{};
    std::size_t lostN = 0;
    const std::size_t end = deferred_.tail();
    std::size_t w = 0;
    for (std::size_t r = 0; r < end;) {
        const std::size_t size = deferred_.sizeAt(r);
        DeferredHdr* h = deferred_.hdr(r);
        if (h->type == 0) { r += size; continue; }  // dead: dropped by compaction
        PartitionState* ps = findPartition_(h->topicId, h->partition);
        const bool lostFull = (lostN == kMaxLost);
        bool keep = lostFull || (ps != nullptr && ps->deferBlockedPass == deferPass_);
        bool applied = false;
        Lost lostRec{0, h->topicId, h->partition, 1, false};
        const char* lostWhat = nullptr;
        if (!keep && h->type == kMsgTier2Batch) {
            std::unique_ptr<Tier2Entry> e(reinterpret_cast<Tier2Entry*>(
                static_cast<uintptr_t>(readLE64(deferred_.payload(r), 0))));
            try {
                PartitionState& tps = getOrCreatePartition(e->topicId, e->partition);
                e->enqueuedNs = h->submitNs;
                const int32_t rc = e->recordCount;
                tps.pendingTier2Entries.push_back(std::move(*e));
                markDirty_(tps);
                if (tps.builder && tps.builder->recordCount() > 0) tps.flushRequested = true;
                recordsSent_ += static_cast<uint64_t>(rc);
                applied = true;
            } catch (...) {
                lostRec.handle      = e->ackHandle;
                lostRec.tier2       = true;
                lostRec.recordCount = e->recordCount;
                lostWhat = "(Tier-2 batch) could not be queued";
            }
        } else if (!keep) {
            const uint8_t* p = deferred_.payload(r);
            const uint8_t flags = p[kRecFlagsOff];
            const uint64_t handle = static_cast<uint64_t>(readLE64(p, kRecCorrIdOff));
            try {
                if (failAt != 0 && ++visited >= failAt && visited < failEnd) [[unlikely]] {
                    throw std::runtime_error("test seam: forced deferred apply failure");
                }
                const auto a = appendRecord_(h->topicId, h->partition, p + kRecKeyBytesOff,
                                             p[kRecKeyLenOff], p + kRecValBytesOff,
                                             readLE32u(p, kRecValLenOff), flags, handle, h->submitNs);
                if (a == ControlledAction::ABORT) keep = true;
                else applied = true;
            } catch (...) {
                // nothing committed (appendRecord_ ordering): we own the handle
                lostRec.handle = (flags & kFlagWantAck) ? handle : 0u;
                lostWhat = "could not be applied";
            }
        }
        if (keep) {
            ps = findPartition_(h->topicId, h->partition);  // may have been created this pass
            if (ps != nullptr) {
                // later records of it: kept too (order).  Not when kept only
                // because the lost-handle buffer is full (it is not blocked).
                if (!lostFull) ps->deferBlockedPass = deferPass_;
                if (h->flags & kDefUnknownPartition) {  // now a known (stalled) partition
                    h->flags = static_cast<uint8_t>(h->flags & ~kDefUnknownPartition);
                    --deferredUnknown_;
                    deferredUnknownBytes_ -= size;
                    ++ps->deferredCount;
                    ps->deferredBytes += static_cast<uint32_t>(size);
                }
            }
            deferred_.move(w, r, size);
            w += size;
            r += size;
            if (w < r) deferred_.writeFiller(w, r);  // stale gap: one dead record
            continue;
        }
        const DeferredHdr done = *h;
        unaccountDeferred_(done);
        deferred_.kill(r);
        if (applied && (done.flags & kDefFlushOnApply)) {
            if (PartitionState* aps = findPartition_(done.topicId, done.partition)) aps->flushRequested = true;
        }
        r += size;
        if (w < r) deferred_.writeFiller(w, r);  // stale gap: one dead record
        if (lostWhat != nullptr) {
            noteDeferredLoss_(lostWhat);  // counter + stderr only: no user code
            lost[lostN++] = lostRec;      // F8c C6: completed (and batch-acked) after the slide
        }
    }
    // No push can happen during the pass (no user code runs in it), so [end,
    // tail) is empty here — kept general (slide down whatever is there).
    const std::size_t extra = deferred_.tail() - end;
    deferred_.move(w, end, extra);
    deferred_.setTail(w + extra);
    deferScanning_ = prevScan;
    // The arena is compact and consistent again: now user code may run (it may
    // flush, send — deferred, inDuty_ is still set — or close the producer).
    for (std::size_t i = 0; i < lostN; ++i) {
        const Lost& l = lost[i];
        emitUnsentBatchAck_(l.topicId, l.partition, l.recordCount, OpStatus::DeliveryFailed, 1);
        completeHandle_(l.handle, l.tier2 ? OpKind::Tier2Batch : OpKind::Send, -1,
                        l.tier2 ? l.recordCount : 0, OpStatus::DeliveryFailed, 0);
    }
}

bool ProducerImpl::debugArenaConsistent() noexcept {
    uint32_t live = 0, unknown = 0;
    std::size_t r = 0;
    const std::size_t tail = deferred_.tail();
    while (r < tail) {
        const DeferredHdr* h = deferred_.hdr(r);
        const std::size_t n = DeferredArena::recordSize(h->len);
        if (n < sizeof(DeferredHdr) || r + n > tail) return false;
        if (h->type != 0 && h->type != kMsgSendRecord && h->type != kMsgTier2Batch) return false;
        if (h->type != 0) {
            ++live;
            if (h->flags & kDefUnknownPartition) {
                ++unknown;
            } else if (findPartition_(h->topicId, h->partition) == nullptr) {
                return false;  // a known record for a partition that does not exist
            }
        }
        r += n;
    }
    if (r != tail || live != deferred_.live() || unknown != deferredUnknown_) return false;
    for (const auto& up : partitions_) {
        uint32_t cnt = 0;
        for (std::size_t o = 0; o < tail; o += deferred_.sizeAt(o)) {
            const DeferredHdr* h = deferred_.hdr(o);
            if (h->type != 0 && !(h->flags & kDefUnknownPartition) &&
                h->topicId == up->topicId && h->partition == up->partition) {
                ++cnt;
            }
        }
        if (cnt != up->deferredCount) return false;
    }
    return true;
}

void ProducerImpl::failDeferred_(OpStatus st) {
    const bool prevScan = deferScanning_;
    deferScanning_ = true;  // callbacks below may push: never compact under our offsets
    const std::size_t end = deferred_.tail();  // only those deferred before the call
    for (std::size_t r = 0; r < end; r += deferred_.sizeAt(r)) {
        if (deferred_.hdr(r)->type == 0) continue;
        const DeferredHdr h = *deferred_.hdr(r);
        bool tier2 = false;
        int32_t rc = 1;
        const uint64_t handle = takeDeferredHandle_(r, &tier2, &rc);
        unaccountDeferred_(h);
        deferred_.kill(r);
        emitUnsentBatchAck_(h.topicId, h.partition, rc, st, 1);
        completeHandle_(handle, tier2 ? OpKind::Tier2Batch : OpKind::Send, -1, tier2 ? rc : 0, st, 0);
    }
    deferScanning_ = prevScan;
}

void ProducerImpl::expireDeferred_(int64_t nowNs, int64_t deliveryNs) {
    if (deferred_.empty()) return;
    const bool prevScan = deferScanning_;
    deferScanning_ = true;
    const std::size_t end = deferred_.tail();
    for (std::size_t r = 0; r < end; r += deferred_.sizeAt(r)) {
        const DeferredHdr* hp = deferred_.hdr(r);
        if (hp->type == 0 || nowNs - hp->submitNs < deliveryNs) continue;
        const DeferredHdr h = *hp;
        bool tier2 = false;
        int32_t rc = 1;
        const uint64_t handle = takeDeferredHandle_(r, &tier2, &rc);
        unaccountDeferred_(h);
        deferred_.kill(r);
        emitUnsentBatchAck_(h.topicId, h.partition, rc, OpStatus::DeliveryFailed, 1);
        if (handle != 0) {
            completeHandle_(handle, tier2 ? OpKind::Tier2Batch : OpKind::Send, -1, tier2 ? rc : 0,
                            OpStatus::DeliveryFailed, 0);
        } else {
            ++deferredTimedOutVoid_;
            const uint64_t n = deferredTimedOutVoid_;
            if ((n & (n - 1)) == 0) {
                std::fprintf(stderr,
                    "rune-client: deferred void producer record delivery timed out (never "
                    "sent; total %llu)\n", static_cast<unsigned long long>(n));
            }
        }
    }
    deferScanning_ = prevScan;
}

void ProducerImpl::throwForSendResult(SendResult r) {
    switch (r) {
    case SendResult::Ok:
    case SendResult::BackPressured:
        return;
    case SendResult::ValueTooLarge:
        throw std::length_error("Producer::send: record can never fit this producer's ring maxMsgLength");
    case SendResult::MessageTooLarge:
        throw std::length_error("Producer::send: MessageTooLarge: key + value exceeds "
                                "Producer::kMaxRecordBytes (16 KiB)");
    case SendResult::InvalidTopic:
        throw std::invalid_argument("Producer::send: invalid topic handle");
    case SendResult::WrongThread:
        throw std::logic_error("Producer::send: invoker-mode producer used off its owner thread");
    case SendResult::WrongShard:
        throw std::logic_error("Producer::send: partition owned by another shard");
    case SendResult::Closed:
        // fix1 C2: consistent with producer()-after-close (Client::producer()
        // throws logic_error("client closed") once stopped_ is set).  F8c C6a:
        // also a producer whose intake was closed (close / closeImmediately).
        throw std::logic_error("Producer::send: client closed (or this producer was closed)");
    case SendResult::QueueFull:
        throw std::runtime_error("Producer::send: completion queue has no credit (poll it)");
    case SendResult::InvalidArgument:
        throw std::invalid_argument("Producer::send: invalid token or completion queue");
    }
}

ProducerImpl::BlockingWait ProducerImpl::beginBlockingWait() const {
    return BlockingWait{aeron::concurrent::BackoffIdleStrategy(
                            100, 10, std::chrono::duration<long, std::nano>(1'000),
                            std::chrono::duration<long, std::nano>(100'000)),
                        clock_()};
}

void ProducerImpl::blockingWaitStep(BlockingWait& w) {
    const int64_t livenessNs = cfg_.agentLivenessTimeoutMs * 1'000'000LL;
    // fix1 C2/I3: re-check closed every iteration, BEFORE the pump attempt —
    // decided immediately, never by waiting out agentLivenessTimeoutMs, and
    // the (possibly by-now-dangling) pump context is never touched once closed.
    if (closed_()) {
        throw std::logic_error("Producer::send: client closed (or this producer was closed)");
    }
    if (mode_ == IngressMode::Direct && pumpFn_ == nullptr) {
        // Task 14: a Direct producer is drained ONLY by its owner's duty cycle
        // (this thread — off-owner callers got WrongThread above).  With no
        // pump hook nothing can ever make room: fail fast, never wait.
        throw std::runtime_error(
            "Producer::send: back-pressured and no duty cycle to pump (direct ingress "
            "without a pump hook) — use trySend instead");
    }
    // fix2 Important-2: NotOwner is decided HERE, by comparing the calling
    // thread against the duty-thread id copied in by setPumpFn(), BEFORE
    // ever calling pumpFn_ — so a thread that is not the duty thread never
    // dereferences pumpCtx_ (the Client), and can never race a concurrent
    // Client destruction on that thread.
    if (pumpFn_ != nullptr && std::this_thread::get_id() == dutyThreadId_) {
        // fix1 C1: DeclinedInCycle means pumping would re-enter the agent's own
        // poll (we ARE the duty-cycle thread, already mid-cycle) — fail fast.
        // fix3 M2: covers two conditions (inside a duty-cycle callback, or the
        // Thread-mode runner itself outside its cycle) — the message says so.
        if (pumpFn_(pumpCtx_) == PumpResult::DeclinedInCycle) {
            throw std::runtime_error(
                "Producer::send: back-pressured, and pumping the duty cycle from here is "
                "not safe right now (already inside a duty-cycle callback, or this is the "
                "duty-cycle thread itself outside any cycle) — use trySend instead");
        }
        return;  // pumped: retry immediately (no idle)
    }
    // NotOwner (no hook wired, or a genuinely separate thread): the
    // heartbeat-bounded wait.
    w.idle.idle(0);
    const int64_t nowNs = clock_();
    if (closed_()) {
        throw std::logic_error("Producer::send: client closed (or this producer was closed)");
    }
    if (nowNs - std::max(ring_.consumerHeartbeatTime(), w.startNs) > livenessNs) {
        throw std::runtime_error(
            "Producer::send: back-pressured and the runner stopped draining for "
            "> agentLivenessTimeoutMs (client closed?)");
    }
}

void ProducerImpl::sendBlocking(SendResult r, int32_t topicId, int32_t partition, const Key& key,
                                std::span<const uint8_t> value) {
    BlockingWait w = beginBlockingWait();
    for (;;) {
        if (r == SendResult::Ok) return;
        throwForSendResult(r);  // every non-retryable result throws
        blockingWaitStep(w);
        r = trySubmit(topicId, partition, key, value, 0);
    }
}

void ProducerImpl::submitFlush() noexcept {
    if (mode_ == IngressMode::Direct) {
        // Task 14: owner-thread state only (no ring, no atomic latch): mark every
        // partition; the end of the owner's pass flushes them.  Off the owner
        // thread nothing can have been sent through this producer: a no-op.
        if (std::this_thread::get_id() != dutyThreadId_ || directClosed_) return;
        // F12: only partitions with buffered work can have anything to flush —
        // O(dirty), not O(partitions).
        // Only partitions still ON the dirty list: a flush() from a callback during
        // the flushBatches walk sees the half-compacted list, whose tail holds stale
        // slots of partitions already removed (dirty=false) — flagging those would
        // leave a flag nothing clears (later record skips linger; spurious pending).
        for (const uint32_t slot : dirty_) {
            auto& ps = *partitions_[slot];
            if (ps.dirty) ps.flushRequested = true;
        }
        // fix2 minor 3: flush() covers records already accepted into the
        // deferred arena too — each is flushed when it is applied.
        for (std::size_t r = 0; r < deferred_.tail(); r += deferred_.sizeAt(r)) {
            DeferredHdr* h = deferred_.hdr(r);
            if (h->type != 0) h->flags = static_cast<uint8_t>(h->flags | kDefFlushOnApply);
        }
        return;
    }
    // F8c C0 (shared-nothing): the flush request reaches the duty thread ONLY as
    // a kMsgFlush ring token — ring order is the synchronisation, no atomic
    // latch.  The token must never be lost to a full ring:
    //   * duty thread (a flush() from an ack/batch-ack callback): it cannot wait
    //     for room (it is the consumer) — record it in duty-owned state; the
    //     next flushBatches() pass flushes every builder (the old latch's
    //     semantics, without the cross-thread flag);
    //   * any other thread: back off until the ring accepts the token (the
    //     duty cycle drains the ring, so room appears), the producer is closed,
    //     or the ring consumer's heartbeat goes stale for agentLivenessTimeoutMs
    //     (the same failure detector send() uses; flush() is noexcept, so a
    //     wedged runner is logged, not thrown).
    if (writeToRing(kMsgFlush, nullptr, 0)) return;
    if (std::this_thread::get_id() == dutyThreadId_) {
        dutyFlushPending_ = true;
        return;
    }
    aeron::concurrent::BackoffIdleStrategy idle(
        100, 10, std::chrono::duration<long, std::nano>(1'000),
        std::chrono::duration<long, std::nano>(100'000));
    const int64_t startNs    = clock_();
    const int64_t livenessNs = cfg_.agentLivenessTimeoutMs * 1'000'000LL;
    while (!writeToRing(kMsgFlush, nullptr, 0)) {
        if (closed_()) return;
        idle.idle(0);
        if (clock_() - std::max(ring_.consumerHeartbeatTime(), startNs) > livenessNs) {
            std::fprintf(stderr,
                "rune-client: Producer::flush: ring full and the runner stopped draining for "
                "> agentLivenessTimeoutMs; flush request dropped (records still flush on "
                "linger / batch size)\n");
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// App-thread: Tier-2 explicit batch submission
// ---------------------------------------------------------------------------

RejectReason ProducerImpl::submitTier2Batch(int32_t topicId,
                                     int32_t partition,
                                     uint64_t ackHandle,
                                     int32_t recordCount,
                                     std::vector<uint8_t>& batchBytes)
{
    if (mode_ == IngressMode::Direct) {
        // Task 14: queue straight onto the partition (owner thread only — the same
        // state the ring drain's kMsgTier2Batch branch would touch).  Closed (or a
        // foreign thread): not accepted, the caller fails the handle.
        // Foreign thread decided FIRST, before any member read (owner-thread state).
        if (std::this_thread::get_id() != dutyThreadId_) return RejectReason::WrongThread;
        if (directClosed_) return RejectReason::Closed;
        if (inDuty_) {
            // fix1 C1 / fix2: deferred like a Tier-1 record (same per-partition
            // order, same validation and full policy).
            auto entry = std::make_unique<Tier2Entry>();
            entry->topicId     = topicId;
            entry->partition   = partition;
            entry->ackHandle   = ackHandle;
            entry->recordCount = recordCount;
            entry->batchBytes  = std::move(batchBytes);
            const RejectReason why = deferTier2_(entry);
            if (why != RejectReason::None) batchBytes = std::move(entry->batchBytes);  // C6: retryable
            return why;
        }
        if (!deferred_.empty() && !settleDeferredFor_(topicId, partition)) {
            return RejectReason::PartitionDeferred;  // its deferred records must go first
        }
        PartitionState* psp = nullptr;
        try {
            psp = &getOrCreatePartition(topicId, partition);
        } catch (const std::length_error&) {
            return RejectReason::Unaddressable;
        }
        PartitionState& ps = *psp;
        Tier2Entry e;
        e.topicId     = topicId;
        e.partition   = partition;
        e.ackHandle   = ackHandle;
        e.recordCount = recordCount;
        e.enqueuedNs  = lastNowNs_;
        e.batchBytes  = std::move(batchBytes);
        try {
            ps.pendingTier2Entries.push_back(std::move(e));
        } catch (...) {
            batchBytes = std::move(e.batchBytes);  // nothing changed: the caller keeps the batch
            throw;
        }
        markDirty_(ps);
        if (ps.builder && ps.builder->recordCount() > 0) ps.flushRequested = true;
        recordsSent_ += static_cast<uint64_t>(recordCount);
        return RejectReason::None;
    }
    if (ring_.closed()) return RejectReason::Closed;
    auto entry = std::make_unique<Tier2Entry>();
    entry->topicId     = topicId;
    entry->partition   = partition;
    entry->ackHandle   = ackHandle;
    entry->recordCount = recordCount;
    entry->batchBytes  = std::move(batchBytes);
    uint8_t tok[kT2TokenSize];
    writeLE64(tok, 0, static_cast<int64_t>(reinterpret_cast<uintptr_t>(entry.get())));
    if (!writeToRing(kMsgTier2Batch, tok, kT2TokenSize)) {
        // F8c fix round 1 (review m1): a close that landed after the pre-check
        // above is reported as Closed, not back-pressure (the claim failed on
        // the ring's closed bit).  C6: the caller keeps the batch (retryable).
        batchBytes = std::move(entry->batchBytes);
        return ring_.closed() ? RejectReason::Closed : RejectReason::BackPressured;
    }
    entry.release();  // ownership now travels through the ring
    return RejectReason::None;
}

// ---------------------------------------------------------------------------
// F8c C6: ack targets
// ---------------------------------------------------------------------------

void ProducerImpl::setOnBatchAck(BatchAckFn fn, void* ctx) {
    if (batchAckCq_ != nullptr) {
        throw std::logic_error("Producer::onBatchAck: batchAcksTo() already set (exclusive)");
    }
    // Review m1: setup phase only (the duty thread reads it on every batch).
    if (mode_ == IngressMode::Direct ? (recordsSent_ != 0 || !deferred_.empty())
                                     : (ring_.tailPosition() != 0)) {
        throw std::logic_error("Producer::onBatchAck: called after a send (setup phase only)");
    }
    batchAckFn_  = fn;
    batchAckCtx_ = ctx;
}

void ProducerImpl::setBatchAckCq(Completions* cq, CqRef ref) {
    if (batchAckCq_ != nullptr) throw std::logic_error("Producer::batchAcksTo: already set");
    if (batchAckFn_ != nullptr) {
        throw std::logic_error("Producer::batchAcksTo: onBatchAck() already set (exclusive)");
    }
    // Every record must carry exactly one credit unit, so no record may have
    // been accepted before the queue is bound (setup phase).  Ring: nothing
    // ever claimed (the tail word, a ring word any thread may read); Direct:
    // the owner's own counters.
    const bool sentBefore = mode_ == IngressMode::Direct
                                ? (recordsSent_ != 0 || !deferred_.empty())
                                : (ring_.tailPosition() != 0);
    if (sentBefore) throw std::logic_error("Producer::batchAcksTo: called after a send (setup phase only)");
    batchAckCqRef_ = ref;
    batchAckCq_    = cq;
}

void ProducerImpl::completeHandle_(uint64_t h, OpKind op, int64_t value, int64_t aux, OpStatus st,
                                   uint16_t errorCode) noexcept {
    if (h == 0) return;
    const Token t = ackhandle::token(h);
    if (ackhandle::isCq(h)) {
        if (hooks_.cqDeliver == nullptr) return;  // bare rig: no registry
        Completion c;
        c.token         = t;
        c.value         = value;
        c.aux           = aux;
        c.op            = op;
        c.status        = st;
        c.errorCode     = errorCode;
        c.reservedBytes = kAckRecordBytes;
        hooks_.cqDeliver(hooks_.ctx, ackhandle::cqRef(h), c);
        return;
    }
    if (onAckFn_ != nullptr) onAckFn_(onAckCtx_, t, value, st, errorCode);  // noexcept by type
}

// ---------------------------------------------------------------------------
// Agent-thread: ring helpers
// ---------------------------------------------------------------------------

bool ProducerImpl::writeToRing(int32_t msgTypeId,
                                const uint8_t* payload,
                                std::size_t len) noexcept
{
    if (len > static_cast<std::size_t>(ring_.maxMsgLength())) {
        return false;
    }

    // Aeron ring requires a non-null, non-zero-length buffer pointer even for
    // zero-length payloads.  Use a static dummy byte.
    static const uint8_t kDummy[1] = {};
    const uint8_t* ptr = (len > 0 && payload != nullptr) ? payload : kDummy;
    const std::size_t sz = (len > 0) ? len : 1;

    try {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(ptr),
            static_cast<aeron::util::index_t>(sz));
        return ring_.write(msgTypeId, ab, 0,
                           static_cast<aeron::util::index_t>(len));
    } catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// E5.1: Agent-thread: update per-worker ingressStreamId routing
// ---------------------------------------------------------------------------

void ProducerImpl::updateIngressRoute(int32_t topicId, int32_t partition,
                                       int32_t ingressStreamId) noexcept
{
    PartitionState& ps = getOrCreatePartition(topicId, partition);
    ps.ingressStreamId = ingressStreamId;
}

// ---------------------------------------------------------------------------
// Agent-thread: drain ring
// ---------------------------------------------------------------------------

PartitionState& ProducerImpl::getOrCreatePartition(int32_t topicId, int32_t partition) {
    const int64_t k = partKey(topicId, partition);
    const uint32_t idx = partitionIndex_.find(k);
    if (idx != detail::FlatIndexMap::kNone) return *partitions_[idx];
    auto ps = std::make_unique<PartitionState>(topicId, partition);
    ps->slotIndex = static_cast<uint32_t>(partitions_.size());
    ps->producerId = producerId_;  // I-4: the identity its sequences start under
    ps->epoch      = producerEpoch_;
    ps->ackRing.assign(kAckRingCapacity, 0);  // preallocated ONCE (zero per-batch alloc)
    ps->ackMask = kAckRingCapacity - 1;
    ps->inflight = InFlightRing(static_cast<uint32_t>(cfg_.maxInFlight > 0 ? cfg_.maxInFlight : 1));
    ps->inflight.rebase(seqBase_);  // F8c C7 F2: the ordinal's carried-forward base
    if (partitions_.size() >= maxPartitions_) {
        throw std::length_error("ProducerImpl: more than 262144 partitions");
    }
    // F12: the dirty list holds each slot at most once — keep its capacity >=
    // the partition count (geometric, BEFORE the table grows: a bad_alloc here
    // leaves no half-created partition), so markDirty_ never allocates.
    if (dirty_.capacity() < partitions_.size() + 1) {
        dirty_.reserve(std::max<std::size_t>(64, 2 * (partitions_.size() + 1)));
    }
    partitions_.push_back(std::move(ps));
    partitionIndex_.insert(k, partitions_.back()->slotIndex);
    return *partitions_.back();
}

const PartitionState* ProducerImpl::partitionState(int32_t topicId,
                                                   int32_t partition) const noexcept {
    const uint32_t i = partitionIndex_.find(partKey(topicId, partition));
    return (i == detail::FlatIndexMap::kNone) ? nullptr : partitions_[i].get();
}

void ProducerImpl::growAckRing(PartitionState& ps) {
    const uint32_t oldCap = static_cast<uint32_t>(ps.ackRing.size());
    const uint32_t newCap = oldCap << 1; // stays a power of two
    const uint32_t oldMask = ps.ackMask;
    const uint32_t newMask = newCap - 1;
    std::vector<uint64_t> next(newCap, 0);
    // Re-place the live window [ackTail, ackHead) at its new masked slots.  head/
    // tail/batchAckStart are absolute monotonic indices, so they remain valid.
    for (uint64_t i = ps.ackTail; i < ps.ackHead; ++i) {
        next[i & newMask] = ps.ackRing[i & oldMask];
    }
    ps.ackRing = std::move(next);
    ps.ackMask = newMask;
    std::fprintf(stderr,
                 "ProducerImpl: ack-ring topic=%d part=%d grown %u->%u "
                 "(deep single-partition pipeline; raise kAckRingCapacity to avoid)\n",
                 ps.topicId, ps.partition, oldCap, newCap);
}

int ProducerImpl::drainRing(int64_t nowNs, int messageLimit) {
    using rune::client::detail::ControlledAction;
    lastDrainStop_ = DrainStop::Drained;
    // fix1 minor 9: the heartbeat is published once per duty cycle by
    // ProducerHub::drainAndFlush (publishHeartbeat()), not here — drainRing can
    // run up to 256x per producer per cycle.
    lastNowNs_ = nowNs;  // Task 14: the pass clock Direct appends stamp batches with
    // Task 14: a Direct producer's records were appended by trySubmit already;
    // its ring never carries anything — skip the ring's head/tail reads entirely.
    if (mode_ == IngressMode::Direct) return 0;
    // F8c C6a: closeImmediately / failure path / Client close — every record
    // read from the closed ring completes with the Discarding status.
    if (const auto* dc = std::get_if<LifeDiscarding>(&life_)) [[unlikely]] {
        return discardRing_(dc->status, messageLimit);
    }

    // F12: drain up to messageLimit messages in ONE pass; ProducerHub flushes
    // once after it (not once per record).  The pass stops early at a batch
    // boundary only a flush can resolve (lastDrainStop_):
    //   * a flush() token — BREAK (consumed): the hub flushes, then drains on,
    //     so records after the token never join the batches it flushes;
    //   * a record its partition cannot take yet — appendRecord_ ABORTs (builder
    //     full / batchRecords / frame cap / Tier-2 queued ahead).  ABORT leaves
    //     the record UNCONSUMED in the ring (precise, in-order backpressure):
    //     the hub flushes and drains again; if the partition is also at
    //     maxInFlight the ring fills → Producer::send() back-pressures the app.
    //     No builder ever grows past a sendable frame size (the fix for the 5s
    //     produceTimeout tail).
    // Unlike Aeron's ManyToOneRingBuffer, this ring both supports ABORT and
    // dispatches the handler inline (no std::function).
    return ring_.controlledRead(
        [this, nowNs](
            int32_t                              msgTypeId,
            aeron::concurrent::AtomicBuffer&     buf,
            aeron::util::index_t                 offset,
            aeron::util::index_t                 len) -> ControlledAction
        {
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
            if (msgTypeId == kMsgFlush) {
                // F12: only dirty partitions have anything to flush (O(dirty)).
                for (const uint32_t slot : dirty_) partitions_[slot]->flushRequested = true;
                lastDrainStop_ = DrainStop::FlushToken;
                return ControlledAction::BREAK;  // flush at this boundary, then drain on
            }

            if (msgTypeId == kMsgTier2Batch) {
                if (len < kT2TokenSize) return ControlledAction::CONTINUE;
                // Take ownership of the heap entry whose address travelled the ring.
                std::unique_ptr<Tier2Entry> entry(
                    reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
                PartitionState& ps = getOrCreatePartition(entry->topicId, entry->partition);
                if (ps.builder && ps.builder->recordCount() > 0) ps.flushRequested = true;
                recordsSent_ += static_cast<uint64_t>(entry->recordCount);
                entry->enqueuedNs = nowNs;
                ps.pendingTier2Entries.push_back(std::move(*entry));
                markDirty_(ps);
                return ControlledAction::CONTINUE;
            }

            if (msgTypeId != kMsgSendRecord) return ControlledAction::CONTINUE;

            const std::size_t sz = static_cast<std::size_t>(len);
            if (sz < static_cast<std::size_t>(kRecHeaderSize)) return ControlledAction::CONTINUE;

            const int32_t topicId   = readLE32(p, kRecTopicIdOff);
            const int32_t partition = readLE32(p, kRecPartOff);
            const uint8_t keyLen    = p[kRecKeyLenOff];
            const uint8_t flags     = p[kRecFlagsOff];
            const uint64_t ackHandle = static_cast<uint64_t>(readLE64(p, kRecCorrIdOff));
            const uint32_t valueLen = readLE32u(p, kRecValLenOff);
            // Agent-thread: record dequeued from the ring (gap ClientAppSend→here =
            // app→agent ring handoff).  corr=ack handle (low bits); value=record bytes.
            RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientAgentDrain,
                                     static_cast<uint32_t>(ackHandle),
                                     static_cast<double>(valueLen));

            if (sz < static_cast<std::size_t>(kRecHeaderSize) + valueLen)
                return ControlledAction::CONTINUE;

            const ControlledAction a =
                appendRecord_(topicId, partition, p + kRecKeyBytesOff, keyLen,
                              p + kRecValBytesOff, valueLen, flags, ackHandle, nowNs);
            if (a == ControlledAction::ABORT) lastDrainStop_ = DrainStop::Blocked;
            return a;
        },
        messageLimit);
}

// ---------------------------------------------------------------------------
// Owner/duty thread: append one record to its partition builder (Task 14: the
// single append path — the ring drain and Direct trySubmit both land here).
// ---------------------------------------------------------------------------

rune::client::detail::ControlledAction ProducerImpl::appendRecord_(
    int32_t topicId, int32_t partition, const uint8_t* keyBytes, uint8_t keyLen,
    const uint8_t* valBytes, uint32_t valueLen, uint8_t flags, uint64_t ackHandle, int64_t nowNs)
{
    using rune::client::detail::ControlledAction;
    PartitionState& ps = getOrCreatePartition(topicId, partition);

    // Task 14 fix1 I1: a record must never overtake a Tier-2 batch queued before
    // it on the same partition.  flushBatches sends Tier-1 before Tier-2, so a
    // record appended while a Tier-2 entry waits (e.g. window full) would leave
    // first.  Hold it (ABORT: Ring keeps it in the ring, Direct back-pressures /
    // keeps it deferred) until the Tier-2 queue is empty.
    if (!ps.pendingTier2Entries.empty()) return ControlledAction::ABORT;

    // Effective per-frame cap: use maxFrameBytes when it is a tighter bound
    // than batchBytes (i.e. maxFrameBytes > 0 and < batchBytes).  Zero-cost
    // on the normal path (maxFrameBytes >= batchBytes): the min-style compare
    // collapses to batchBytes with no new branch taken.
    // M2: use the frame-cap values computed once at construction (cfg_ immutable).
    const bool frameCapped = frameCapped_;
    const int32_t effCap = effCap_;

    // BACKPRESSURE / FRAME-SPLIT: ABORT before appending when accumulating
    // this record would push the frame past effCap.
    //
    // Normal path (frameCapped=false, effCap=batchBytes):
    //   ABORT only when ALSO at maxInFlight (can't flush; classic backpressure).
    //   Zero new hot-path branch: frameCapped=false → condition is identical
    //   to the old `builderBytes >= batchBytes && inFlight >= maxInFlight`.
    //
    // Frame-cap path (frameCapped=true, effCap=maxFrameBytes < batchBytes):
    //   Use an exact pre-flight check: compute the wire contribution of this
    //   record (recordBodyContrib) and ABORT if appending it would push
    //   builderBytes past effCap.  The check uses wallMs() for tsDelta, which
    //   is the same function the builder calls inside append() — so the
    //   predicted size matches the actual encoded size.  The builder is reset
    //   to builderBaseWallMs on each frame boundary.
    //   ABORT even when inFlight has room so the current frame is flushed
    //   first; this guarantees NO emitted RecordBatch exceeds effCap wire.
    if (frameCapped && ps.builder && ps.builder->recordCount() > 0) {
        // Compute exact wire contribution of this next record.
        // wallMs() here and inside builder->append() may differ by one ms tick,
        // which can shift varlongLen(tsDelta) by +1 byte at boundary values.
        // Adding 1 byte of slack makes the check conservative (splits ≤1 byte
        // early at boundary values) but guarantees NO overshoot.
        const int64_t tsDelta = wallMs() - ps.builderBaseWallMs;
        const int32_t contrib = recordBodyContrib(
            static_cast<int32_t>(keyLen),
            static_cast<int32_t>(valueLen),
            tsDelta,
            ps.builder->recordCount() /* offsetDelta = current record count */)
            + 1; // +1 byte slack for wallMs() jitter between pre-flight and append

        if (ps.builderBytes + contrib > effCap) {
            // Record would overshoot: ABORT.  flushBatches will fire on
            // sizeFull (builderBytes >= effCap) or via flushRequested=true
            // when builderBytes < effCap but still needs to flush.
            if (ps.builderBytes < effCap) {
                // sizeFull won't trigger (builder not yet at cap); force flush
                // via otherTrigger so the current batch is sent this cycle.
                ps.flushRequested = true;
            }
            return ControlledAction::ABORT;
        }
    }
    // Normal-path backpressure: a full builder ABORTs, regardless of the window.
    // No flush runs between appends in either mode (F12: Ring drains a whole pass
    // and the hub flushes once after it; Direct (Task 14) flushes at the end of
    // the owner's pass) — so without this a builder would grow past batchBytes
    // into an oversized frame.  Ring: the ABORT ends the drain pass, the hub
    // flushes the full builder and drains again; if the window is full too, the
    // record stays in the ring (back-pressure).
    if (!frameCapped && ps.builderBytes >= effCap) {
        return ControlledAction::ABORT;
    }
    // Task 14 fix1 M2 / F12: batchRecords is enforced at append time too, in
    // both modes (no flush runs between appends).
    if (cfg_.batchRecords > 0 && ps.builder &&
        ps.builder->recordCount() >= cfg_.batchRecords) {
        return ControlledAction::ABORT;
    }

    // Create the builder ONCE per partition; reused (reset in place) across
    // batches so there is no per-batch heap allocation.
    if (!ps.builder) {
        const int64_t baseWall = wallMs();
        ps.builder = std::make_unique<ProduceFrameBuilder>(0, baseWall);
        ps.builderBaseWallMs = baseWall;
    }
    // I1 FIX (frame cap): builderBaseWallMs is never re-sampled here — it already
    // equals the builder's actual baseTimestamp (set at construction above and at
    // the reset(0, resetWall) in encodeAndSend), so the pre-flight tsDelta is
    // computed against the SAME base the builder encodes with.
    const int32_t index = ps.builder->recordCount();  // this record's offsetDelta

    // Per-record acks at the record's OWN index (spec §4.3): the slice of a
    // batch is aligned with its records, void records hold 0.  Zeros are
    // back-filled lazily so batches without callbacks cost nothing.
    //
    // Task 14 fix1 M1: exception safety.  Everything that can throw (builder
    // creation above, ack-ring growth here, the builder append below) happens
    // BEFORE anything is committed; the ack slots are reserved first and only
    // written after the append succeeded.  So on a throw the record is not in
    // the builder AND the handle is not in the ack ring — the caller still owns
    // it (exactly once).
    const bool wantAck = (flags & kFlagWantAck) && ackHandle != 0;
    if (wantAck) {
        const uint64_t need = static_cast<uint64_t>(index - ps.batchAckCount) + 1u;
        while (ps.ackHead - ps.ackTail + need > ps.ackRing.size()) growAckRing(ps);
    }

    // I2: a single record whose own wire contribution exceeds the frame cap
    // cannot be split.  It is still emitted (alone) to guarantee progress, but
    // we surface it loudly so an over-cap frame is never produced silently.
    if (frameCapped && ps.builder->recordCount() == 0) {
        const int32_t soloContrib = recordBodyContrib(
            static_cast<int32_t>(keyLen), static_cast<int32_t>(valueLen),
            /*tsDelta=*/0, /*offsetDelta=*/0);
        if (soloContrib > effCap) {
            std::fprintf(stderr,
                "ProducerImpl: single record wire=%d exceeds maxFrameBytes=%d "
                "(topic=%d part=%d); emitting oversize frame (broker may reject)\n",
                soloContrib, effCap, topicId, partition);
        }
    }

    const std::string_view kv(reinterpret_cast<const char*>(keyBytes),
                              static_cast<std::size_t>(keyLen));
    const std::string_view vv(reinterpret_cast<const char*>(valBytes),
                              static_cast<std::size_t>(valueLen));
    ps.builder->append(kv, vv, wallMs());

    // ---- committed: nothing below throws (ack slots reserved above) ----------
    // fix2: the batch clock is the EARLIEST record's acceptance time (a deferred
    // record carries its own; Kafka accumulator semantics).
    if (index == 0 || nowNs < ps.batchStartNs) ps.batchStartNs = nowNs;
    markDirty_(ps);  // F12: no-throw (capacity reserved with the partition)
    if (wantAck) {
        if (ps.batchAckCount == 0) ps.batchAckStart = ps.ackHead;
        while (ps.batchAckCount < index) pushAck_(ps, 0);
        pushAck_(ps, ackHandle);
    }
    // Track the TRUE encoded WIRE size (fixed batch header + accumulated
    // record bodies), not just value bytes — so the batch flushes at
    // batchBytes of WIRE and never overshoots the broker's max produce
    // batch size (the old value-only count undercounted per-record framing
    // by ~20B/record, producing ~70KB wire for a "64KB" batch → rejected).
    ps.builderBytes = static_cast<int32_t>(
        rune::protocol::batch::kRecordBatchOverhead + ps.builder->bodyBytes());
    ++recordsSent_;
    return ControlledAction::CONTINUE;
}

// ---------------------------------------------------------------------------
// Agent-thread: encode and offer one batch
// ---------------------------------------------------------------------------

template <class Sink>
bool ProducerImpl::encodeAndSend(
    PartitionState& ps,
    int64_t nowNs,
    int64_t sessionId,
    int64_t correlationId,
    const Sink& sink)
{
    if (!ps.builder || ps.builder->recordCount() == 0) return false;

    // acks=None: nothing waits for a response, so nothing can detect (or ever
    // retransmit) a lost frame — sequencing a batch nobody tracks just leaves a
    // permanent, silent gap in the idempotent stream the moment one is dropped.
    // Encode these batches as NON-idempotent instead: producerId=-1 (the wire
    // sentinel the broker uses to skip all sequence validation) and leave
    // producerEpoch/baseSequence at the builder's reset() defaults (-1/-1) —
    // see Producer.h's "acks=None" note for the documented convention.
    // acks=Leader/All: stamp this instance's real id and the next sequence.
    // baseSequence is stamped but NOT advanced yet — the advance happens only
    // after a successful offer (below), so a back-pressured retry re-sends the
    // SAME sequence rather than opening a gap.
    if (cfg_.acks != Acks::None) {
        if (!sequenceRoom_(ps, ps.builder->recordCount())) return false;  // int32 sequence space
        ps.builder->setProducerId(ps.producerId);  // the PARTITION's identity (I-4)
        ps.builder->setProducerEpoch(ps.epoch);
        ps.builder->setBaseSequence(ps.nextBaseSequence);
    }

    // Backlog B1/B4: the batch already IS its wire frame (ProduceFrameBuilder —
    // each record was written once, in place, at append).  Sealing writes only
    // the Produce header and the RecordBatch header; the RecordBatch CRC is
    // computed by the sink once the frame is accepted (tryClaim / the offer
    // reserved-value supplier), so a back-pressured offer costs ~110 header
    // bytes — no record copy, no CRC — and records keep joining the held batch.
    namespace rpc = rune::protocol::client;
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = ps.topicId;
    msg.partition      = ps.partition;
    msg.partitionKnown = 1;
    msg.acks           = (cfg_.acks == Acks::All) ? uint8_t{1} : uint8_t{0};  // B2: wire 1=All, 0=Leader/None
    // EOS-DUP: the broker dedups on the FRAME's identity, not the batch header's
    // — carry exactly what was stamped into the header above (acks=None keeps the
    // -1 sentinels: non-idempotent).  Retransmits re-send this frame verbatim.
    if (cfg_.acks != Acks::None) {
        msg.producerId    = ps.producerId;
        msg.producerEpoch = ps.epoch;
        msg.baseSequence  = ps.nextBaseSequence;
    } else {
        msg.producerEpoch = -1;  // the header's -1 (Produce defaults epoch to 0): all three match
    }
    const std::size_t written = ps.builder->seal(msg);

    // Offer to transport — Task 1.4: route by (topicId,partition); E5.1: pass
    // per-partition ingressStreamId as the transport stream.
    const bool ok = offerFresh(sink, ps.topicId, ps.partition, ps.ingressStreamId,
                               ps.builder->data(), written, ProduceFrameBuilder::frameCrc());

    if (!ok) {
        RUNE_FR_THROTTLED_PRODUCE(frOfferRefused_, rune_fr::Site::ClientOfferRefused, rune_fr::foldCorr(correlationId),
                                  static_cast<uint16_t>(ps.partition));
        // Aeron ingress back-pressure (term buffer momentarily full / not yet
        // connected).  Leave the batch FULLY intact — builder records, stamped
        // baseSequence, and the ack-ring slice (batchAckStart/Count) are all
        // preserved — and return false so flushBatches retries on the next agent
        // cycle.  The frame stays unsealed (no CRC computed); the retry re-seals
        // its headers only.
        //
        // This is the fix for the intermittent 5s tail: the old code reset the
        // builder + advanced nextBaseSequence UNCONDITIONALLY, so a single
        // back-pressured offer silently dropped the batch's records while their
        // callbacks waited for an ack → produceTimeout (5s), and also opened
        // an idempotent-sequence gap.
        return false;
    }

    ++framesSent_;                                       // one Aeron frame offered
    // Agent-thread: batch frame offered to the broker.  corr=batch correlationId;
    // value=in-flight depth AFTER this offer (how deep the un-acked pipeline is).
    // corr = rune_fr::foldCorr (the key the broker's hop sites use for this batch).
    RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientBatchOffer,
                             rune_fr::foldCorr(correlationId),
                             static_cast<double>(ps.inflight.live() + 1));
    // The batch's age at its first offer (first record appended -> offer): the
    // client-side linger / window / back-pressure wait.  arg = record count.
    RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientBatchSeal, rune_fr::foldCorr(correlationId),
                                 static_cast<uint16_t>(std::min<int32_t>(ps.builder->recordCount(), 65535)),
                                 static_cast<double>(nowNs - ps.batchStartNs));
    const int32_t batchRecords = ps.builder->recordCount();
    // acks=None batches are non-idempotent (producerId=-1, above) and nothing
    // tracks their sequence, so nextBaseSequence is never stamped, read, or
    // advanced for them — it stays exactly where the last idempotent batch (if
    // any) left it, ready to resume cleanly if acks is ever changed back.
    const int32_t batchSeq = (cfg_.acks != Acks::None) ? ps.nextBaseSequence : -1;
    if (cfg_.acks != Acks::None) {
        ps.nextBaseSequence += batchRecords;             // advance ONLY on success
    }
    if (correlationId != 0) {
        // Only ack-expecting batches occupy the in-flight window (acks=None: corrId 0).
        InFlightBatch& b = ps.inflight.claim(ordinal_, ps.slotIndex);
        b.firstSendNs       = nowNs;
        b.lastSendNs        = nowNs;
        b.oosParked         = false;
        b.attemptDeadlineNs = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
        b.attempts          = 1;
        b.redirectCount     = 0;
        b.ackStart          = (ps.batchAckCount > 0) ? ps.batchAckStart : ps.ackHead;
        b.ackCount          = ps.batchAckCount;
        b.recordCount       = batchRecords;
        b.baseSequence      = batchSeq;
        b.producerEpoch     = ps.epoch;
        b.producerId        = ps.producerId;
        b.lastSendTick      = ++sendTick_;
        b.ingressStreamId   = ps.ingressStreamId;
        b.tier2             = false;
        b.laneGen           = ps.laneGen;  // NEW-4(b): attempt 0 on the current lane
        b.laneFirstTag      = 0;
        b.laneOut           = 1;
        b.staleLaneOut      = 0;
        // B4: the accepted (now sealed) frame becomes the retained retransmit
        // frame by a buffer swap — no copy; the builder takes the slot's old
        // storage for the next batch.
        ps.builder->swapFrameInto(b.wireFrame);
        if (b.attemptDeadlineNs < earliestAttemptDeadlineNs_) {
            earliestAttemptDeadlineNs_ = b.attemptDeadlineNs;
        }
        verifyClaimedCorrId_(ps, b, correlationId);  // always on (not an assert)
    } else {
        // acks=None: no response is awaited (the wire id is 0 — see onProduceResponse),
        // so the batch's acks complete now, "sent, offset unknown" (-1), and its
        // ack-ring slice is reclaimed immediately — nothing is retained.
        emitBatchAck_(ps.topicId, ps.partition, batchSeq, batchRecords, -1, 0, OpStatus::Ok,
                      static_cast<uint32_t>(batchRecords), -1);
        completeSentUnacked_(ps);
    }

    // Reset builder IN PLACE for the next batch (success path only; keep the
    // object + its records_ capacity — no per-batch heap alloc).
    const int64_t resetWall = wallMs();
    ps.builder->reset(0, resetWall);
    ps.builderBytes  = 0;
    ps.builderBaseWallMs = resetWall;  // I1: keep the pre-flight base in lockstep with
                                       // the builder's actual baseTimestamp.
    ps.batchStartNs  = 0;
    ps.flushRequested = false;
    ps.batchAckCount  = 0; // ackHead/ackTail persist; only the current-batch slice resets
    // batchAckStart is set afresh when the next ack-wanting record arrives.

    return true;
}

// ---------------------------------------------------------------------------
// Agent-thread: encode and send a Tier-2 pre-built batch
// ---------------------------------------------------------------------------

template <class Sink>
bool ProducerImpl::encodeAndSendTier2(
    PartitionState& ps,
    Tier2Entry& entry,
    int64_t nowNs,
    int64_t sessionId,
    const Sink& sink)
{
    if (entry.batchBytes.empty() || entry.recordCount == 0) return false;

    namespace rb = rune::protocol::batch;
    auto& b = entry.batchBytes;
    if (b.size() < static_cast<std::size_t>(rb::kRecordBatchOverhead)) {
        return false; // truncated batch — should not happen
    }

    // acks=None: nothing waits for a response, so a lost frame can never be
    // detected or retransmitted — sequencing (and tracking) a batch nobody
    // tracks just leaves a permanent gap the moment one is dropped.  Encode
    // these as NON-idempotent instead: producerId=-1 (the wire sentinel that
    // tells the broker to skip sequence validation entirely).  BatchBuilder
    // never stamps a pre-built Tier-2 batch (RecordBatchBuilder defaults
    // producerId_/producerEpoch_/baseSequence_ to -1 — see RecordBatch.h), so
    // entry.batchBytes is ALREADY -1/-1/-1 with a CRC computed over exactly
    // those values: nothing to patch, and ps.nextBaseSequence is left
    // untouched (see the Tier-1 path in encodeAndSend for the same rule).
    //
    // acks=Leader/All: patch this instance's real id and the next sequence
    // into the pre-built RecordBatch bytes.  Use the named constants from
    // rune::protocol::batch (RecordBatch.h) so a layout change on the C++
    // server side causes a compile-time break rather than silent CRC/seq
    // corruption.
    const bool idempotent = (cfg_.acks != Acks::None);
    if (idempotent && !sequenceRoom_(ps, entry.recordCount)) return false;  // int32 sequence space
    if (idempotent) {
        // Backlog B2: stamp producerId / epoch / baseSequence and patch the CRC
        // (computed once over the -1/-1/-1 batch by BatchBuilder) INCREMENTALLY —
        // no rescan of the batch.  NOT advanced until the offer succeeds (below):
        // a back-pressured retry re-stamps the SAME values (a zero delta).
        restampBatchIdentity(b.data(), b.size(), ps.producerId, ps.epoch, ps.nextBaseSequence);
    }

    // Encode the Produce wire frame.
    namespace rpc = rune::protocol::client;
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    // Wire id = the in-flight slot this batch will claim (the Tier-2 future's
    // handle goes into the ack slice, record 0, below).  acks=None: id 0 and no
    // slot, exactly like Tier-1 — no response is awaited.
    const bool    awaitAck   = idempotent;
    const int64_t wireCorrId = awaitAck
                             ? batchcorr::make(ordinal_, ps.slotIndex, ps.inflight.nextSeq())
                             : 0;
    msg.correlationId  = wireCorrId;
    msg.topicId        = entry.topicId;
    msg.partition      = entry.partition;
    msg.partitionKnown = 1;
    msg.acks           = (cfg_.acks == Acks::All) ? uint8_t{1} : uint8_t{0};  // B2: wire 1=All, 0=Leader/None
    // EOS-DUP: the frame carries the identity patched into the header above.
    if (idempotent) {
        msg.producerId    = ps.producerId;
        msg.producerEpoch = ps.epoch;
        msg.baseSequence  = ps.nextBaseSequence;
    } else {
        msg.producerEpoch = -1;  // the pre-built header's -1: all three match
    }
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(b);

    const std::size_t needed = msg.encodedSize();
    if (needed == 0) return false;

    // Backlog B4: an acked batch is encoded straight into the buffer of the
    // in-flight slot it will claim (free: the caller checked canFlight_), so the
    // retained retransmit frame is the encoded frame — no second copy.
    // acks=None retains nothing: the scratch buffer.
    std::vector<uint8_t>& out = awaitAck ? ps.inflight.peekNext().wireFrame : encodeScratch_;
    out.resize(needed);
    const std::size_t written = msg.encode(out.data());
    if (written == 0) return false;

    // Task 1.4: route by (topicId,partition); E5.1: pass per-partition
    // ingressStreamId as the per-worker transport stream.  The batch is already
    // sealed (B2 patch above): nothing for the transport to compute.
    const bool ok = sink(ps.topicId, ps.partition, ps.ingressStreamId, out.data(), written);

    if (!ok) {
        RUNE_FR_THROTTLED_PRODUCE(frOfferRefused_, rune_fr::Site::ClientOfferRefused, rune_fr::foldCorr(wireCorrId),
                                  static_cast<uint16_t>(ps.partition));
        // Back-pressure: leave the entry + sequence untouched so the caller can
        // retry (flushBatches must NOT pop it).  No sequence advance, no drop.
        return false;
    }

    ++framesSent_; // one Aeron frame offered to transport (Tier-2)
    RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientBatchOffer, rune_fr::foldCorr(wireCorrId),
                             static_cast<double>(ps.inflight.live() + 1));
    RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientBatchSeal, rune_fr::foldCorr(wireCorrId),
                                 static_cast<uint16_t>(std::min<int32_t>(entry.recordCount, 65535)),
                                 static_cast<double>(nowNs - entry.enqueuedNs));
    // acks=None: non-idempotent (see above) — sequence stays untouched, matching
    // the Tier-1 rule in encodeAndSend.
    const int32_t batchSeq = idempotent ? ps.nextBaseSequence : -1;
    if (idempotent) {
        ps.nextBaseSequence += entry.recordCount;  // advance ONLY on success
    }
    if (!awaitAck) {
        emitBatchAck_(ps.topicId, ps.partition, batchSeq, entry.recordCount, -1, 0, OpStatus::Ok, 1, -1);
        const uint64_t h = entry.ackHandle;
        entry.ackHandle = 0;
        completeHandle_(h, OpKind::Tier2Batch, -1, entry.recordCount, OpStatus::Ok, 0);  // "sent, offset unknown"
    } else {
        if (ps.ackHead - ps.ackTail >= ps.ackRing.size()) growAckRing(ps);
        const uint64_t slice = ps.ackHead;
        ps.ackRing[ps.ackHead & ps.ackMask] = entry.ackHandle;  // record 0 -> the batch's token
        ++ps.ackHead;
        entry.ackHandle = 0;  // ownership moved to the ack ring
        InFlightBatch& ib = ps.inflight.claim(ordinal_, ps.slotIndex);
        // Tier-2 gets the same retransmit/delivery clock as Tier-1 (fixed on sight:
        // the old BatchInfo left firstSendNs = 0, so the first retransmit scan gave
        // it up immediately).
        ib.firstSendNs       = nowNs;
        ib.lastSendNs        = nowNs;
        ib.oosParked         = false;
        ib.attemptDeadlineNs = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
        if (ib.attemptDeadlineNs < earliestAttemptDeadlineNs_) {
            earliestAttemptDeadlineNs_ = ib.attemptDeadlineNs;
        }
        ib.attempts          = 1;
        ib.redirectCount     = 0;
        ib.ackStart          = slice;
        ib.ackCount          = 1;
        ib.recordCount       = entry.recordCount;
        ib.baseSequence      = batchSeq;
        ib.producerEpoch     = ps.epoch;
        ib.producerId        = ps.producerId;
        ib.lastSendTick      = ++sendTick_;
        ib.ingressStreamId   = ps.ingressStreamId;
        ib.tier2             = true;
        ib.laneGen           = ps.laneGen;  // NEW-4(b)
        ib.laneFirstTag      = 0;
        ib.laneOut           = 1;
        ib.staleLaneOut      = 0;  // wireFrame: encoded in place above (B4)
        verifyClaimedCorrId_(ps, ib, wireCorrId);  // always on (not an assert)
    }

    return true;
}

// ---------------------------------------------------------------------------
// EOS-OOS: re-send parked batches (their older batches all resolved)
// ---------------------------------------------------------------------------

template <class Send>
bool ProducerImpl::offerAttempt_(PartitionState& ps, InFlightBatch& b, int64_t nowNs, Send&& send,
                                 [[maybe_unused]] uint16_t frReason) {
    const uint8_t prev = b.attemptTag;
    const uint8_t next = static_cast<uint8_t>(prev + 1);
    stampAttempt_(b, next);
    if (!send(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(), b.wireFrame.size())) {
        stampAttempt_(b, prev);  // not on the wire: the previous attempt is still the current one
        return false;
    }
    b.attemptTag   = next;
    b.lastSendNs   = nowNs;
    b.lastSendTick = ++sendTick_;
    ++b.attempts;
    if (b.laneGen != ps.laneGen) {  // NEW-4(b): the first attempt on a new lane
        b.staleLaneOut = static_cast<uint16_t>(b.staleLaneOut + b.laneOut);
        b.laneOut      = 0;
        b.laneGen      = ps.laneGen;
        b.laneFirstTag = next;
    }
    ++b.laneOut;
    // A stored frame went out again (reason: 1 timer, 2 route, 3 parked, 4 redirect).
    RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientRetransmit, rune_fr::foldCorr(b.corrId), frReason,
                                 static_cast<double>(b.attempts));
    unpark_(ps, b);  // re-sent: its next answer decides (parked again only on another OOS)
    return true;
}

template <class Sink>
bool ProducerImpl::resendRoute_(PartitionState& ps, int64_t nowNs, const Sink& sink) {
    const int64_t attemptNs = cfg_.produceTimeoutMs * 1'000'000LL;
    bool all = true;
    ps.inflight.forEachLive([&](InFlightBatch& b) {
        if (!all || !b.routeResendPending) return;
        // A NEW attempt (same frame, same identity): the old owner's late
        // answers to the earlier attempts are ignored from now on (EOS I-2).
        if (!offerAttempt_(ps, b, nowNs, sink, 2)) {
            all = false;  // new publication resolving / back-pressure: keep order
            return;
        }
        b.routeResendPending = false;
        b.attemptDeadlineNs  = nowNs + attemptNs;
        if (b.attemptDeadlineNs < earliestAttemptDeadlineNs_) earliestAttemptDeadlineNs_ = b.attemptDeadlineNs;
    });
    if (all) ps.routeResend = false;  // N-b: routeParked is a NEW park if set (see queueRouteResend_)
    return all;
}

template <class Sink>
bool ProducerImpl::resendParked_(PartitionState& ps, int64_t nowNs, const Sink& sink) {
    const int64_t attemptNs = cfg_.produceTimeoutMs * 1'000'000LL;
    bool all  = true;
    bool stop = false;  // reached a possible gap: the parked batches behind it wait for it
    ps.inflight.forEachLive([&](InFlightBatch& b) {
        if (!all || stop) return;
        if (!b.oosParked) {
            if (!storedByAck_(ps, b)) stop = true;
            return;
        }
        if (!offerAttempt_(ps, b, nowNs, sink, 3)) {
            all = false;  // back-pressure: keep order — the rest wait for the next pass
            return;
        }
        b.attemptDeadlineNs = nowNs + attemptNs;
        if (b.attemptDeadlineNs < earliestAttemptDeadlineNs_) earliestAttemptDeadlineNs_ = b.attemptDeadlineNs;
    });
    if (all) ps.resendParked = false;
    return all;
}

// ---------------------------------------------------------------------------
// Agent-thread: flush batches
// ---------------------------------------------------------------------------

template <class Sink>
int ProducerImpl::flushBatches(
    int64_t nowNs,
    int64_t sessionId,
    const Sink& sink)
{
    lastNowNs_ = nowNs;  // Task 14: the pass clock Direct appends stamp batches with
    const DutyScope duty(*this);  // fix1 C1: callbacks below (acks=None, batch acks) defer sends

    // M1 FIX [CB-5]: Do NOT emit any frame while the session is not established
    // (sessionId == 0 means Connecting / post-forceReconnect window).  Records
    // stay in the builder / Tier-2 queue and will flush naturally once
    // ConnectResponse{OK} arrives and drainAndFlush is called with the real
    // leader-assigned sessionId.  This prevents Produce frames from being
    // stamped sessionId=0 and silently dropped by the broker.
    if (sessionId == 0) return 0;

    const int64_t lingerNs = cfg_.lingerUs * 1000LL;
    // F8c C0: a duty-thread flush() that met a full ring (see submitFlush) is
    // consumed here, read-and-clear, by ONE pass — duty-thread state, no atomic.
    // Every other flush() reaches the duty thread as a kMsgFlush ring token.
    // F8c C6a: once the intake is closed nothing more can join a batch — flush
    // every builder at once (no linger).
    const bool globalFlush = dutyFlushPending_ || flushAll_;
    dutyFlushPending_ = false;

    int flushed = 0;

    // F12: walk ONLY the dirty partitions (O(dirty), not O(partitions)), compacting
    // the list in place: a partition with no work left after its visit leaves the
    // list (dirty=false); one still holding records / Tier-2 batches (linger not
    // expired, window full, transport back-pressure) stays for the next cycle.
    // Indexed (never iterators/references into dirty_): user callbacks fired by a
    // send below can only APPEND to it (markDirty_ — no reallocation, capacity is
    // reserved), and those late entries are carried over after the walk.
    const std::size_t walkEnd = dirty_.size();
    std::size_t keep = 0;
    for (std::size_t di = 0; di < walkEnd; ++di) {
        const uint32_t slot = dirty_[di];
        PartitionState& ps = *partitions_[slot];
        ++flushVisits_;
        // ---- EOS-OOS: parked batches whose older batches all resolved go out
        // again FIRST, oldest-first — a new batch sent ahead of them would only
        // meet the same sequence gap.  Back-pressure: nothing new this pass.
        // ---- Task 16: a moved route re-offers the stored batches first, in
        // order; a NotLeader-parked partition sends nothing new while stored
        // batches are in flight (they would only meet the old owner again).
        if (ps.routeResend && !resendRoute_(ps, nowNs, sink)) {
            dirty_[keep++] = slot;
            continue;
        }
        if (ps.routeParked) {  // N-b: also a park that arrived while the re-offer drained
            if (!ps.inflight.empty()) {
                dirty_[keep++] = slot;
                continue;
            }
            ps.routeParked = false;  // everything stored resolved: nothing to re-offer
        }
        if (ps.resendParked && !resendParked_(ps, nowNs, sink)) {
            dirty_[keep++] = slot;
            continue;
        }
        // ---- EOS follow-ups M-1 / I-4 (Kafka shouldStopDrainBatchesForPartition):
        // no NEW batch of a partition while it has parked batches (they would
        // only meet the same gap) or an unresolved sequence (its outcome decides
        // the identity the next batch gets).  Kept dirty: re-examined every pass.
        if (ps.parkedCount > 0 || ps.unresolved) {
            dirty_[keep++] = slot;
            continue;
        }
        // ---- Tier-1: flush pending auto-batch if triggered -------------------
        //
        // CB-5 A1 FIX: Tier-2 entries must NEVER be dispatched while a
        // Tier-1 auto-batch is blocked in the builder (i.e. there are
        // accumulated records that haven't been sent yet).  If a Tier-1 batch
        // is stuck behind maxInFlight, Tier-2 entries for the same partition
        // must also wait — jumping ahead would violate submission order.

        bool tier1Pending = (ps.builder && ps.builder->recordCount() > 0);

        if (tier1Pending) {
            const bool lingerExpired  = (nowNs - ps.batchStartNs) >= lingerNs;
            // M2: effective per-frame cap computed once at construction (effCap_).
            // Zero-cost on the normal path (maxFrameBytes >= batchBytes): effCap_ ==
            // batchBytes so sizeFull is identical to the pre-frame-cap behavior.
            const int32_t effCap = effCap_;
            const bool sizeFull       = (ps.builderBytes >= effCap) ||
                                        (cfg_.batchRecords > 0 &&
                                         ps.builder->recordCount() >= cfg_.batchRecords);
            const bool otherTrigger   = lingerExpired || ps.flushRequested || globalFlush ||
                                        !ps.pendingTier2Entries.empty();
            const bool canFlight      = canFlight_(ps);

            // maxInFlight gates ALL flushes (bounds outstanding un-acked batches —
            // the durability/idempotent window; the MaxInFlightBound contract).
            // The builder can never grow past effCap because drainRing's drain
            // gate STOPS draining a partition once it is size-full (F12: regardless
            // of the in-flight window), so no oversized (unsendable) frame is built.
            const bool doFlush        = canFlight && (sizeFull || otherTrigger);
            // FR: a batch is due but the in-flight window is full (waits on acks).
            if (!canFlight && (sizeFull || otherTrigger))
                RUNE_FR_THROTTLED_PRODUCE(frWindowFull_, rune_fr::Site::ClientWindowFull,
                                          static_cast<uint32_t>(ps.partition),
                                          static_cast<uint16_t>(std::min<std::size_t>(ps.inflight.live(), 65535)));

            if (doFlush) {
                const int64_t corrId = (cfg_.acks != Acks::None)
                    ? batchcorr::make(ordinal_, ps.slotIndex, ps.inflight.nextSeq())
                    : 0;
                if (encodeAndSend(ps, nowNs, sessionId, corrId, sink)) {
                    ++flushed;
                    tier1Pending = false;
                }
                // encodeAndSend false (transport back-pressure): batch kept intact,
                // tier1Pending stays true, Tier-2 blocked below — retry next cycle.
            }
            // else: not triggered yet (partial batch under maxInFlight) — keep
            // accumulating; Tier-2 blocked to preserve submission order.
        } else {
            // No pending Tier-1 records; clear flush flags.
            if (ps.flushRequested) ps.flushRequested = false;
        }

        // ---- Tier-2: flush any pending explicit batches (FIFO order) ---------
        // Tier-2 entries are sent after any pending Tier-1 records are flushed.
        // CB-5 A1: skip Tier-2 entirely if a Tier-1 batch is still pending
        // (either not triggered, blocked by maxInFlight, or send failed).
        // This enforces strict submission order: no T2 can jump ahead of T1.
        if (!tier1Pending) {
            while (!ps.pendingTier2Entries.empty()) {
                // maxInFlight guard (shared with Tier-1 for this partition).
                if (!canFlight_(ps)) break;

                Tier2Entry& entry = ps.pendingTier2Entries.front();

                if (!encodeAndSendTier2(ps, entry, nowNs, sessionId, sink)) {
                    break; // back-pressure: keep the entry, retry next cycle (no drop)
                }
                ++flushed;
                ps.pendingTier2Entries.pop_front();
            }
        }

        // F12: still has work -> stays dirty; otherwise leaves the list.  Re-read
        // the state (a callback fired above may have reset this partition).
        if ((ps.builder && ps.builder->recordCount() > 0) || !ps.pendingTier2Entries.empty() ||
            ps.resendParked) {
            dirty_[keep++] = slot;
        } else {
            ps.dirty          = false;
            ps.flushRequested = false;
        }
    }
    // Entries appended during the walk (callbacks) slide down behind the kept ones.
    const std::size_t total = dirty_.size();
    for (std::size_t di = walkEnd; di < total; ++di) dirty_[keep++] = dirty_[di];
    dirty_.resize(keep);  // shrink only: never allocates

    return flushed;
}

// ---------------------------------------------------------------------------
// Agent-thread: ProduceResponse routing
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Sequence resolution — EOS follow-ups fix round 1 (I-2, I-4, M-1).  The
// idempotent producer's rules, per partition, after Kafka's TransactionManager
// (maybeResolveSequences, requestEpochBumpForPartition / startSequences-
// AtBeginning, canRetry) and RecordAccumulator.shouldStopDrainBatchesFor-
// Partition.  Every live batch of a partition carries the partition's
// identity (producerId, epoch).
//
//  A. Attempts.  Every offer of a batch's stored frame is a new attempt (its
//     tag rides in the frame's correlationId).  An ERROR answer of a
//     superseded attempt is ignored (Kafka: a new correlation id per request;
//     a stale response is never matched).  An OK of any attempt is a success.
//  B. OK: complete (baseOffset -1 = a known-old duplicate: stored, offset
//     unknown); lastAckedSeq advances.  DuplicateSequence (Kafka
//     DUPLICATE_SEQUENCE_NUMBER) is the same outcome: an OK with offset -1.
//  The OutOfOrderSequence CONTRACT (Kafka's; re-review 1 NEW-4): the broker
//  answers OOS only for a batch it did NOT append, and a batch it did append
//  is answered OK (its original offset in the dedup window, -1 beyond it) or
//  DuplicateSequence — never OOS.  C3/C4 below re-sequence on that proof.  A
//  broker that cannot vouch for a sequence (a producer state not proven
//  consistent with its log) must answer an outcome-unknown error (-> D), never
//  OOS: an OOS for a stored batch makes C4 report it failed and re-stamp
//  possibly stored followers under a new epoch (written twice).  (rune-broker
//  37564ba: the no-worker-pool gateway INLINE path still answers a known-old
//  duplicate OOS — a broker fix; the worker path keeps the contract.)
//  C. OutOfOrderSequence on batch b (current attempt):
//     C1. an older live batch of b's identity exists -> b is PARKED (retried
//         once the older ones resolve; Kafka: !isNextSequence -> retry).  The
//         frame right before the gap is fast-retransmitted once.
//     C2. b is the oldest, but its attempt was offered before the partition's
//         latest OK was processed -> the answer may predate that OK (answers
//         reorder: errors are immediate, acks=all OKs deferred, routes differ):
//         b is re-sent (C applies again to that attempt's answer).
//     C2b. (NEW-4(b)) b is the oldest, but a live batch of b's identity still
//         has an UNANSWERED attempt on an EARLIER LANE (PartitionState::laneGen:
//         the route moved / a redirect / a retarget since it was offered).
//         Attempts on one lane reach the broker in order, so every same-lane
//         attempt offered before b's was processed before b's OOS; an
//         earlier lane's attempt may still be appended AFTER it.  The OOS is
//         not proof yet: b is parked (PartitionState::laneWait) until every
//         earlier-lane attempt is answered (any outcome; an OK of any attempt
//         completes its batch), then re-sent on the current lane — that
//         attempt's answer decides (C again).  Bounded by the attempt timer
//         and the delivery timeout (D) when an old lane never answers.
//     C3. b is the oldest and b.baseSequence != lastAckedSeq + 1 -> a failed
//         predecessor left a gap; the OOS proves it was never written (else b
//         would have been accepted), so b was not either: the partition is
//         RE-SEQUENCED (new epoch, sequences from 0; b and every batch behind it
//         re-stamped and re-sent) — nothing fails.
//     C4. otherwise the broker rejected the next-in-line batch: b FAILS
//         (BrokerError 20) and the batches behind it are re-sequenced (Kafka:
//         requestEpochBumpForPartition; startSequencesAtBeginning re-stamps the
//         in-flight batches).
//  R. Retriable, same identity (no completion, no bump): NotLeader (park,
//     re-offer on the new owner's route — Task 16 C1) and MinIsrViolation
//     (re-sent after kRetriableBackoffNs — Kafka NOT_ENOUGH_REPLICAS, N-e)
//     and TransientLogFull (42, Phase 2b E2: same retry).
//  D. Any other error answer, the delivery timeout, an exhausted redirect
//     budget, a close: b FAILS with its outcome UNKNOWN (an earlier attempt
//     may be stored) -> the partition is UNRESOLVED.
//  E. An unresolved partition sends no new batch (re-sends of live ones go on)
//     until nothing of it is in flight; then (Kafka maybeResolveSequences):
//     lastAckedSeq + 1 == nextBaseSequence (later batches were acked: the gap
//     was filled after all) -> resolved, same identity; else -> reset to a new
//     identity at sequence 0.
//  F. A new identity bumps the PRODUCER epoch only if the partition was on
//     the current one (else it adopts it); other partitions keep theirs, so an
//     already-stored retransmit elsewhere is never fenced.
//  G. No new batch of a partition goes out while it has parked batches (C1)
//     or is unresolved (E) — Kafka stops draining a partition with a retry
//     outstanding.
//  U. D6 — UnknownProducerId (Kafka UNKNOWN_PRODUCER_ID, KIP-360 canRetry) on
//     batch b (current attempt): the broker's PROVEN producer state has no
//     entry for this producer and b does not start at sequence 0 — b was NOT
//     appended.  C1 / C2 / C2b / C3 apply first, exactly as for an OOS (the
//     answer may be a gap in front of b, predate an OK, or be explained by a
//     failed predecessor's gap).  Then, in place of C4 (b is next in line):
//     U1. lastAckedOffset < the answer's logStartOffset (every record this
//         partition had acked is below the log start: retention removed the
//         producer state legitimately) -> the partition is RE-SEQUENCED as in
//         C3 (new identity per F; b and every batch behind it re-stamped and
//         re-sent) — nothing fails.
//     U2. otherwise (the broker should still know us: an acked record of ours
//         is at/above the log start, or nothing was acked and the log start is
//         0) -> ALSO re-sequenced (D6 review M1, Kafka canRetry's idempotent
//         branch: UNKNOWN_PRODUCER_ID always retries with an epoch bump).  Any
//         attempt of b would sit above lastAckedOffset >= logStart, inside the
//         rebuilt range, and would have created an entry — so the re-send is
//         provably duplicate-free (no window at all, unlike U1).  The acked-
//         data case is counted (unknownProducerIdStateLoss: the broker lost
//         state it should have kept); it is never an app-visible failure.
//     lastAckedOffset (per partition) = the max last offset of every OK (B).
// ---------------------------------------------------------------------------

void ProducerImpl::onProduceResponse(int64_t correlationId, int64_t baseOffset, int16_t errorCode,
                                     int64_t logStartOffset)
{
    const DutyScope duty(*this);  // fix1 C1
    if (!batchcorr::isBatch(correlationId) || batchcorr::ordinal(correlationId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(correlationId);
    if (s >= partitions_.size()) return;
    PartitionState& ps = *partitions_[s];
    // Hop chain: the ProduceResponse reached the producer (arg = its attempt).
    RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientAckRecv, rune_fr::foldCorr(correlationId),
                                 batchcorr::attempt(correlationId), static_cast<double>(errorCode));
    InFlightBatch* b = ps.inflight.find(correlationId);
    if (b == nullptr) return;  // duplicate / stale (slot reused) / already expired: ignore
    noteAnswer_(*b, batchcorr::attempt(correlationId));  // NEW-4(b): one fewer outstanding attempt
    // NEW-4(a): DuplicateSequence (Kafka DUPLICATE_SEQUENCE_NUMBER) = stored,
    // offset unknown — the same outcome as the broker's OK -1 known-old answer.
    const bool dupSeq =
        errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::DuplicateSequence);
    const int16_t ec = dupSeq ? int16_t{0} : errorCode;
    if (dupSeq) baseOffset = -1;
    const uint32_t units = b->tier2 ? 1u : static_cast<uint32_t>(b->recordCount);
    if (ec == 0) {  // B
        // EOS-OOS: baseOffset < 0 with OK = a known-old duplicate (the broker
        // wrote it earlier; Kafka DUPLICATE_SEQUENCE parity): success, offset
        // unknown — every record completes Ok with -1, exactly once.
        const int64_t off = baseOffset < 0 ? -1 : baseOffset;
        emitBatchAck_(ps.topicId, ps.partition, b->baseSequence, b->recordCount, off, 0,
                      OpStatus::Ok, units, b->producerEpoch, b->producerId);
        completeSlice_(ps, b->ackStart, b->ackCount, off, OpStatus::Ok, 0, b->tier2, b->recordCount);
        if (b->baseSequence >= 0 && b->producerId == ps.producerId && b->producerEpoch == ps.epoch) {
            ps.lastAckedSeq = std::max(ps.lastAckedSeq, b->baseSequence + b->recordCount - 1);
        }
        // D6: the max acked LAST offset (a known-old OK -1 carries none).
        if (off >= 0) ps.lastAckedOffset = std::max(ps.lastAckedOffset, off + b->recordCount - 1);
        ps.lastAckTick = sendTick_;
        releaseResolved_(ps, *b);
        reclaimAckRing_(ps);
        maybeEndLaneWait_(ps);
        return;
    }
    if (batchcorr::attempt(correlationId) != b->attemptTag) {  // A: a superseded attempt's error
        maybeEndLaneWait_(ps);  // ... is still an answer (NEW-4(b))
        return;
    }
    if (ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader)) {
        // Task 16 fix round 1 (C1): the partition moved (Kafka NOT_LEADER_OR_FOLLOWER
        // parity): retriable — no completion, no epoch bump, the SAME identity.
        // The partition parks (no new batches to the old owner); the owner
        // (Client) asks for metadata at once and, when it names the new owner,
        // every stored batch is re-offered unchanged as a new attempt
        // (resendPartitionViaRoute / resendRouteParked) — the new owner dedups
        // by (producerId, epoch, sequence).  Only the CURRENT attempt's NotLeader
        // gets here (A): a superseded attempt's late NotLeader is ignored.  The
        // attempt timer stays the backstop; the delivery timeout bounds it.
        RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientNotLeader, rune_fr::foldCorr(correlationId),
                                     static_cast<uint16_t>(ps.partition), 0.0);
        ps.routeParked = true;
        markDirty_(ps);
        return;
    }
    // D6: UnknownProducerId shares C1 / C2 / C2b with OOS, then rule U.
    const bool unknownPid = ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownProducerId);
    if ((ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::OutOfOrderSequence) || unknownPid) &&
        b->baseSequence >= 0) {
        const bool olderLive = ps.inflight.anyOlderLive(*b, [&](const InFlightBatch& o) {
            return o.producerId == b->producerId && o.producerEpoch == b->producerEpoch;
        });
        if (olderLive) {  // C1
            parkOutOfOrder_(ps, *b);
            RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientOosPark, rune_fr::foldCorr(correlationId), 1,
                                         static_cast<double>(ps.parkedCount));
            return;
        }
        if (b->lastSendTick <= ps.lastAckTick) {  // C2: may predate the latest OK — re-send
            if (!b->oosParked) {
                b->oosParked = true;
                ++ps.parkedCount;
            }
            RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientOosPark, rune_fr::foldCorr(correlationId), 2,
                                         static_cast<double>(ps.parkedCount));
            ps.resendParked = true;  // b is the oldest: nothing to wait for
            markDirty_(ps);
            return;
        }
        if (staleLaneInFlight_(ps, *b)) {  // C2b (NEW-4(b)): not conclusive yet — wait
            if (!b->oosParked) {
                b->oosParked = true;
                ++ps.parkedCount;
            }
            ps.laneWait = true;
            RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientOosPark, rune_fr::foldCorr(correlationId), 3,
                                         static_cast<double>(ps.parkedCount));
            RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientLaneWait, rune_fr::foldCorr(correlationId),
                                         static_cast<uint16_t>(ps.partition), 0.0);
            markDirty_(ps);
            return;
        }
        if (b->baseSequence != ps.lastAckedSeq + 1) {  // C3: a failed predecessor's gap
            resequence_(ps);                             // (an UnknownProducerId too: the gap explains it)
            return;
        }
        if (!unknownPid) {
            // C4: the next-in-line batch was rejected — it fails, the rest move on.
            emitBatchAck_(ps.topicId, ps.partition, b->baseSequence, b->recordCount, -1, ec,
                          OpStatus::BrokerError, units, b->producerEpoch, b->producerId);
            completeSlice_(ps, b->ackStart, b->ackCount, -1, OpStatus::BrokerError,
                           static_cast<uint16_t>(ec), b->tier2, b->recordCount);
            unpark_(ps, *b);
            ps.inflight.release(*b);
            resequence_(ps);
            reclaimAckRing_(ps);
            return;
        }
        // U1 (lastAckedOffset < logStartOffset: retention removed our state).
        // Accepted window (D6 review, same as Kafka KIP-360): U1 proves only THIS attempt was not appended.
        // Earlier attempts on the same lane were answered first (one FIFO publication); cross-lane ones park
        // via C2b above.  Left: an earlier attempt whose OK is lost outright AND whose data was then retained /
        // evicted away before a producer-state rebuild — then the re-sequenced re-send duplicates it.
        // U2 (D6 review M1): otherwise — b provably not stored (see U2 above): re-sequenced too, no window.
        if (ps.lastAckedOffset >= 0 && logStartOffset >= 0 && ps.lastAckedOffset >= logStartOffset) [[unlikely]] {
            ++unknownPidStateLoss_;
            std::fprintf(stderr,
                         "ProducerImpl: topic=%d part=%d UnknownProducerId with acked offset %lld >= log start %lld: "
                         "the broker lost producer state (batch not appended; re-sequenced)\n",
                         ps.topicId, ps.partition, static_cast<long long>(ps.lastAckedOffset),
                         static_cast<long long>(logStartOffset));
        }
        resequence_(ps);
        return;
    }
    // Phase 2b E2 (ruling 2): TransientLogFull (42) — the leader's transient
    // ring is full of not-yet-replicated records; definitely not appended, the
    // route is right (no park, no metadata) — exactly MinIsrViolation's retry.
    if (ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::MinIsrViolation) ||
        ec == static_cast<int16_t>(rune::protocol::client::ErrorCode::TransientLogFull)) {
        // N-e (Kafka NOT_ENOUGH_REPLICAS / _AFTER_APPEND: both retriable): the
        // ISR is below minIsr.  Re-sent unchanged (same identity, a new attempt)
        // after a backoff by the attempt timer — safe whether or not it was
        // appended (the broker's ProducePath refuses BEFORE the append; the
        // gateway's full pending-ack queue answers it AFTER: the retry is then a
        // window duplicate, OK with the original offset).  Bounded by the
        // delivery timeout.  Followers meet the gap (C1) and wait.
        RUNE_FR_MARK_ARG_VAL_PRODUCE(rune_fr::Site::ClientRetriable, rune_fr::foldCorr(correlationId),
                                     static_cast<uint16_t>(ec), 0.0);
        const int64_t backoffNs = std::min<int64_t>(kRetriableBackoffNs, cfg_.produceTimeoutMs * 1'000'000LL);
        b->attemptDeadlineNs = lastNowNs_ + backoffNs;
        if (b->attemptDeadlineNs < earliestAttemptDeadlineNs_) earliestAttemptDeadlineNs_ = b->attemptDeadlineNs;
        return;
    }
    // D: rejected, but an earlier attempt may be stored — outcome unknown.
    emitBatchAck_(ps.topicId, ps.partition, b->baseSequence, b->recordCount, -1, ec,
                  OpStatus::BrokerError, units, b->producerEpoch, b->producerId);
    completeSlice_(ps, b->ackStart, b->ackCount, -1, OpStatus::BrokerError,
                   static_cast<uint16_t>(ec), b->tier2, b->recordCount);
    markUnresolved_(ps, *b);
    releaseResolved_(ps, *b);
    reclaimAckRing_(ps);
}

void ProducerImpl::parkOutOfOrder_(PartitionState& ps, InFlightBatch& b) noexcept {
    if (!b.oosParked) {
        b.oosParked = true;
        ++ps.parkedCount;
    }
    // Still retried on its own attempt timer (a lost resend never strands it);
    // the delivery timeout still bounds it.
    // Fast retransmit (M-1): the gap sits right before this batch — at the
    // NEWEST older batch of its identity, unless a parked batch lies between
    // (then the gap is upstream of that parked batch: already being handled).
    // Re-sent only if that frame's latest offer went out before this batch's
    // (a stale OOS of a batch sent before the resend leaves it alone).
    InFlightBatch* gap = nullptr;
    ps.inflight.anyOlderLive(b, [&](InFlightBatch& o) {
        if (o.producerId != b.producerId || o.producerEpoch != b.producerEpoch) return false;
        gap = o.oosParked ? nullptr : &o;
        return false;
    });
    if (gap != nullptr && gap->lastSendTick <= b.lastSendTick) {
        gap->attemptDeadlineNs     = 0;
        earliestAttemptDeadlineNs_ = 0;
    }
}

void ProducerImpl::noteAnswer_(InFlightBatch& b, uint8_t tag) noexcept {
    // Attempts laneFirstTag..attemptTag (mod 256) went out on the current lane.
    const auto onLane = static_cast<uint8_t>(tag - b.laneFirstTag) <=
                        static_cast<uint8_t>(b.attemptTag - b.laneFirstTag);
    if (onLane) {
        if (b.laneOut > 0) --b.laneOut;
    } else if (b.staleLaneOut > 0) {
        --b.staleLaneOut;
    }
}

bool ProducerImpl::staleLaneInFlight_(PartitionState& ps, const InFlightBatch& b) noexcept {
    bool stale = false;
    ps.inflight.forEachLive([&](InFlightBatch& o) {
        if (o.producerId == b.producerId && o.producerEpoch == b.producerEpoch && o.staleLaneOut != 0) stale = true;
    });
    return stale;
}

void ProducerImpl::maybeEndLaneWait_(PartitionState& ps) noexcept {
    if (!ps.laneWait) return;
    bool stale = false;
    ps.inflight.forEachLive([&](InFlightBatch& o) { stale |= o.staleLaneOut != 0; });
    if (stale) return;
    ps.laneWait = false;
    if (ps.parkedCount != 0) {  // the waiting head goes out again; its answer decides
        ps.resendParked = true;
        markDirty_(ps);
    }
}

void ProducerImpl::releaseResolved_(PartitionState& ps, InFlightBatch& b) noexcept {
    unpark_(ps, b);
    ps.inflight.release(b);
    // The parked batches go out again (flushBatches, before new ones) once every
    // live batch older than the first parked one is either acked-by-implication
    // (an OK proves every lower sequence is stored: its own answer is just
    // pending) — i.e. the gap in front of them is closed or failed.
    if (ps.parkedCount != 0 && !ps.resendParked) {
        bool ready = true, seenParked = false;
        ps.inflight.forEachLive([&](InFlightBatch& o) {
            if (seenParked) return;
            if (o.oosParked) seenParked = true;
            else if (!storedByAck_(ps, o)) ready = false;
        });
        if (ready) {
            ps.resendParked = true;
            markDirty_(ps);
        }
    }
    maybeResolve_(ps);
}

void ProducerImpl::markUnresolved_(PartitionState& ps, const InFlightBatch& b) noexcept {
    if (b.baseSequence >= 0 && b.producerId == ps.producerId && b.producerEpoch == ps.epoch) {
        ps.unresolved = true;
    }
}

void ProducerImpl::maybeResolve_(PartitionState& ps) noexcept {  // E
    if (!ps.unresolved || !ps.inflight.empty()) return;
    if (ps.lastAckedSeq + 1 == ps.nextBaseSequence) {
        ps.unresolved = false;  // every sequence sent was acked after all
    } else {
        resetPartition_(ps);
    }
    markDirty_(ps);  // held records may go now
}

void ProducerImpl::resetPartition_(PartitionState& ps) noexcept {  // F
    if (ps.producerId == producerId_ && ps.epoch == producerEpoch_) {
        if (producerEpoch_ == INT16_MAX) {
            // Epoch space exhausted: rotate to a FRESH unique id (never producerId_+1 —
            // an incremented id is just as likely to collide with another live
            // producer's id as the original was, and gives the broker no reason to
            // believe this is a distinct producer).  Fresh id + epoch 0 is the same
            // trust-on-first-use the broker already grants any never-seen producerId.
            producerId_    = generateProducerId(this);
            producerEpoch_ = 0;
        } else {
            ++producerEpoch_;
        }
    }
    ps.producerId       = producerId_;
    ps.epoch            = producerEpoch_;
    ps.nextBaseSequence = 0;
    ps.lastAckedSeq     = -1;
    ps.unresolved       = false;
    std::fprintf(stderr,
                 "ProducerImpl: topic=%d part=%d sequences restart at 0 under producerId=%lld epoch=%d\n",
                 ps.topicId, ps.partition, static_cast<long long>(producerId_), static_cast<int>(producerEpoch_));
}

bool ProducerImpl::sequenceRoom_(PartitionState& ps, int32_t n) noexcept {
    if (static_cast<int64_t>(ps.nextBaseSequence) + n <= INT32_MAX) return true;
    if (!ps.inflight.empty()) return false;  // drain first: never fence a stored retransmit
    resetPartition_(ps);                     // new identity at 0, before the overflow
    return static_cast<int64_t>(n) <= INT32_MAX;
}

namespace {
constexpr std::size_t kHdrCorrIdOff  = 16;  // MessageHeader.correlationId LE64
constexpr std::size_t kHdrBytes      = 24;  // MessageHeader
constexpr std::size_t kFramePidOff   = kHdrBytes + 10;  // Produce body: producerId LE64
constexpr std::size_t kFrameEpochOff = kHdrBytes + 18;  //               producerEpoch LE16
constexpr std::size_t kFrameSeqOff   = kHdrBytes + 20;  //               baseSequence LE32
inline void putLE(uint8_t* p, uint64_t v, int bytes) noexcept {
    for (int i = 0; i < bytes; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i));
}
}  // namespace

void ProducerImpl::stampAttempt_(InFlightBatch& b, uint8_t tag) noexcept {
    if (b.wireFrame.size() < kHdrCorrIdOff + 8) return;
    putLE(b.wireFrame.data() + kHdrCorrIdOff,
          static_cast<uint64_t>(batchcorr::withAttempt(b.corrId, tag)), 8);
}

void ProducerImpl::resequence_(PartitionState& ps) noexcept {
    namespace rb = rune::protocol::batch;
    resetPartition_(ps);
    int32_t seq = 0;
    ps.inflight.forEachLive([&](InFlightBatch& b) {
        // Re-stamp the stored frame: Produce body + the CRC-covered batch header.
        auto& f = b.wireFrame;
        const auto view = rune::protocol::client::Produce::decode(f.data(), f.size());
        if (!view || view->recordBatch.size() < static_cast<std::size_t>(rb::kRecordBatchOverhead)) [[unlikely]] {
            // N-a: never re-send a frame whose identity could not be re-stamped
            // (the broker would reject it SchemaValidationFailed).  Unreachable
            // (the frame was encoded and sent by this producer): fail it loudly;
            // its sequences are not consumed.
            std::fprintf(stderr,
                         "ProducerImpl: BUG stored frame of corrId=%lld (topic=%d part=%d) does not decode; "
                         "failing the batch instead of re-sequencing it\n",
                         static_cast<long long>(b.corrId), ps.topicId, ps.partition);
            failBatch_(ps, b, OpStatus::DeliveryFailed);
            return;
        }
        {
            uint8_t* batch = f.data() + (view->recordBatch.data() - f.data());
            const std::size_t len = view->recordBatch.size();
            putLE(f.data() + kFramePidOff, static_cast<uint64_t>(ps.producerId), 8);
            putLE(f.data() + kFrameEpochOff, static_cast<uint16_t>(ps.epoch), 2);
            putLE(f.data() + kFrameSeqOff, static_cast<uint32_t>(seq), 4);
            // B2: incremental CRC patch (bit-exact), not a rescan of the batch.
            restampBatchIdentity(batch, len, ps.producerId, ps.epoch, seq);
        }
        b.producerId    = ps.producerId;
        b.producerEpoch = ps.epoch;
        b.baseSequence  = seq;
        seq += b.recordCount;
        // Every earlier attempt is superseded (their answers are about the old identity).
        b.attemptTag = static_cast<uint8_t>(b.attemptTag + 1);
        stampAttempt_(b, b.attemptTag);
        if (!b.oosParked) {
            b.oosParked = true;
            ++ps.parkedCount;
        }
    });
    ps.nextBaseSequence = seq;
    if (ps.parkedCount != 0) {
        ps.resendParked = true;
        markDirty_(ps);
    }
}

void ProducerImpl::verifyClaimedCorrId_(PartitionState& ps, InFlightBatch& b, int64_t wireCorrId) {
    if (b.corrId == wireCorrId) return;
    // Invariant broken: the frame on the wire carries an id no live slot answers
    // to, so its ack could never resolve these records.  Fail them loudly now
    // rather than let them hang until the delivery timeout (release builds too).
    std::fprintf(stderr,
                 "ProducerImpl: BUG wire corrId=%lld != claimed slot corrId=%lld "
                 "(topic=%d part=%d); failing the batch\n",
                 static_cast<long long>(wireCorrId), static_cast<long long>(b.corrId),
                 ps.topicId, ps.partition);
    failBatch_(ps, b, OpStatus::DeliveryFailed);  // internal error: batch correlation id mismatch
}

void ProducerImpl::reclaimAckRing_(PartitionState& ps) noexcept {
    if (const InFlightBatch* o = ps.inflight.oldestLive()) {
        ps.ackTail = o->ackStart;
    } else {
        ps.ackTail = (ps.batchAckCount > 0) ? ps.batchAckStart : ps.ackHead;
    }
}

void ProducerImpl::pushAck_(PartitionState& ps, uint64_t handle) {
    if (ps.ackHead - ps.ackTail >= ps.ackRing.size()) growAckRing(ps);
    ps.ackRing[ps.ackHead & ps.ackMask] = handle;
    ++ps.ackHead;
    ++ps.batchAckCount;
}

void ProducerImpl::completeSlice_(PartitionState& ps, uint64_t ackStart, int32_t ackCount,
                                  int64_t baseOffset, OpStatus st, uint16_t errorCode, bool tier2,
                                  int32_t recordCount) noexcept {
    const bool ok = st == OpStatus::Ok;
    for (int32_t i = 0; i < ackCount; ++i) {
        // Copy + zero the slot BEFORE the delivery: a handler may send (deferred
        // in Direct mode) — never hold a reference into ackRing across user code.
        uint64_t& slot = ps.ackRing[(ackStart + static_cast<uint64_t>(i)) & ps.ackMask];
        const uint64_t h = slot;
        slot = 0;
        if (h == 0) continue;
        // baseOffset < 0 on Ok: offset unknown (known-old duplicate) — -1 for every record.
        const int64_t value = (ok && baseOffset >= 0) ? baseOffset + (tier2 ? 0 : i) : -1;
        RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientAckComplete, static_cast<uint32_t>(h),
                                 static_cast<double>(value));
        completeHandle_(h, tier2 ? OpKind::Tier2Batch : OpKind::Send, value, tier2 ? recordCount : 0,
                        st, errorCode);
    }
}

void ProducerImpl::completeSentUnacked_(PartitionState& ps) noexcept {
    if (ps.batchAckCount <= 0) return;
    for (int32_t i = 0; i < ps.batchAckCount; ++i) {
        uint64_t& slot = ps.ackRing[(ps.batchAckStart + static_cast<uint64_t>(i)) & ps.ackMask];
        const uint64_t h = slot;
        slot = 0;
        completeHandle_(h, OpKind::Send, -1, 0, OpStatus::Ok, 0);  // "sent, offset unknown"
    }
    // The builder slice is always the newest one and acks=None never has an
    // in-flight slice, so the slice is reclaimed by rewinding the head.
    ps.ackHead       = ps.batchAckStart;
    ps.batchAckCount = 0;
    reclaimAckRing_(ps);
}

void ProducerImpl::emitBatchAck_(int32_t topicId, int32_t partition, int32_t baseSequence,
                                 int32_t recordCount, int64_t baseOffset, int16_t errorCode,
                                 OpStatus st, uint32_t units, int16_t producerEpoch,
                                 int64_t producerId) noexcept {
    if (batchAckFn_ != nullptr) {
        const BatchAck a{topicId, partition, errorCode == 0 ? baseOffset : -1, baseSequence,
                         recordCount, errorCode, producerEpoch, producerId};
        batchAckFn_(batchAckCtx_, a);  // noexcept by type
        return;
    }
    if (batchAckCqRef_ == kNoCqRef || hooks_.cqDeliver == nullptr || units == 0) return;
    Completion c;
    c.token = (static_cast<uint64_t>(static_cast<uint32_t>(topicId)) << 32) |
              static_cast<uint32_t>(partition);
    c.value = errorCode == 0 ? baseOffset : -1;
    c.aux   = static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(baseSequence)) << 32) |
                                   static_cast<uint32_t>(recordCount));
    c.op            = OpKind::BatchAck;
    c.status        = st;
    c.errorCode     = static_cast<uint16_t>(errorCode);
    c.reservedBytes = units * kAckRecordBytes;  // the credit of every record it covers (batchAckUnits)
    c.reserved0     = static_cast<uint16_t>(producerEpoch);  // batchAckOf() -> BatchAck::producerEpoch
    hooks_.cqDeliver(hooks_.ctx, batchAckCqRef_, c);
}

void ProducerImpl::failBatch_(PartitionState& ps, InFlightBatch& b, OpStatus st) {
    emitBatchAck_(ps.topicId, ps.partition, b.baseSequence, b.recordCount, -1, BatchAck::kClientFailure,
                  st, b.tier2 ? 1u : static_cast<uint32_t>(b.recordCount), b.producerEpoch, b.producerId);
    completeSlice_(ps, b.ackStart, b.ackCount, -1, st, 0, b.tier2, b.recordCount);
    markUnresolved_(ps, b);  // D: outcome at the broker unknown
    releaseResolved_(ps, b);
    reclaimAckRing_(ps);
}

void ProducerImpl::failBuilder_(PartitionState& ps, OpStatus st) {
    if (!ps.builder || ps.builder->recordCount() == 0) return;
    const int32_t n = ps.builder->recordCount();
    emitUnsentBatchAck_(ps.topicId, ps.partition, n, st, static_cast<uint32_t>(n));
    if (ps.batchAckCount > 0) {
        completeSlice_(ps, ps.batchAckStart, ps.batchAckCount, -1, st, 0, false, 0);
        ps.ackHead = ps.batchAckStart;  // reclaim the unsent (newest) slice
    }
    ps.batchAckCount = 0;
    const int64_t resetWall = wallMs();
    ps.builder->reset(0, resetWall);
    ps.builderBaseWallMs = resetWall;
    ps.builderBytes      = 0;
    ps.batchStartNs      = 0;
    ps.flushRequested    = false;
    reclaimAckRing_(ps);
}

void ProducerImpl::failBatch(int64_t corrId) {
    const DutyScope duty(*this);  // fix1 C1
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return;
    if (InFlightBatch* b = partitions_[s]->inflight.find(corrId)) {
        noteAnswer_(*b, batchcorr::attempt(corrId));  // NEW-4(b)
        if (batchcorr::attempt(corrId) != b->attemptTag) {  // A: a superseded attempt's error
            maybeEndLaneWait_(*partitions_[s]);
            return;
        }
        failBatch_(*partitions_[s], *b, OpStatus::DeliveryFailed);
        maybeEndLaneWait_(*partitions_[s]);
    }
}

void ProducerImpl::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    const int64_t nowNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    lastNowNs_ = nowNs;  // the duty-cycle clock (Client passes its own now)
    const DutyScope duty(*this);  // fix1 C1: failure callbacks below defer their retries
    const int64_t deliveryNs = cfg_.deliveryTimeoutMs * 1'000'000LL;
    // fix2: deferred records are accepted records — same delivery deadline,
    // measured from acceptance.  First, so an expired one is never applied.
    expireDeferred_(nowNs, deliveryNs);
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (nowNs - b.firstSendNs >= deliveryNs) {
                std::fprintf(stderr, "ProducerImpl: batch corrId=%lld (topic=%d part=%d) failed: no answer within the "
                             "delivery timeout (%lldms, %d attempts)\n", static_cast<long long>(b.corrId), ps.topicId,
                             ps.partition, static_cast<long long>(cfg_.deliveryTimeoutMs), b.attempts);
                failBatch_(ps, b, OpStatus::DeliveryFailed);  // no ProduceResponse in time
            }
        });
        // Records never sent (no session / no route) past the delivery deadline are
        // failed AND dropped — an ack is never reported failed for a record that
        // could still be written later.
        if (ps.builder && ps.builder->recordCount() > 0 && ps.batchStartNs != 0 &&
            nowNs - ps.batchStartNs >= deliveryNs) {
            std::fprintf(stderr, "ProducerImpl: topic=%d part=%d records never sent within the delivery timeout "
                         "(%lldms; no route / session)\n", ps.topicId, ps.partition,
                         static_cast<long long>(cfg_.deliveryTimeoutMs));
            failBuilder_(ps, OpStatus::DeliveryFailed);  // never sent
        }
        // Tier-2 batches never sent past the deadline: same rule (FIFO, so the
        // expired ones are at the front).  Popped BEFORE the delivery (a handler
        // may queue another Tier-2 batch — deferred in Direct mode).
        while (!ps.pendingTier2Entries.empty() &&
               nowNs - ps.pendingTier2Entries.front().enqueuedNs >= deliveryNs) {
            const uint64_t h  = ps.pendingTier2Entries.front().ackHandle;
            const int32_t  rc = ps.pendingTier2Entries.front().recordCount;
            ps.pendingTier2Entries.pop_front();
            emitUnsentBatchAck_(ps.topicId, ps.partition, rc, OpStatus::DeliveryFailed, 1);
            completeHandle_(h, OpKind::Tier2Batch, -1, rc, OpStatus::DeliveryFailed, 0);
        }
    }
}

template <class Sink>
int ProducerImpl::retransmitStalled(int64_t nowNs, const Sink& sink)
{
    if (nowNs < earliestAttemptDeadlineNs_) return 0;  // O(1) on the happy path
    const DutyScope duty(*this);  // fix1 C1: failBatch_ callbacks below
    const int64_t deliveryNs = cfg_.deliveryTimeoutMs * 1'000'000LL;
    const int64_t attemptNs  = cfg_.produceTimeoutMs * 1'000'000LL;
    int64_t nextEarliest = INT64_MAX;
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (nowNs - b.firstSendNs >= deliveryNs) {
                std::fprintf(stderr,
                    "ProducerImpl: batch corrId=%lld undelivered after %d attempts / %lldms "
                    "(topic=%d part=%d); giving up\n",
                    static_cast<long long>(b.corrId), b.attempts,
                    static_cast<long long>(cfg_.deliveryTimeoutMs), ps.topicId, ps.partition);
                failBatch_(ps, b, OpStatus::DeliveryFailed);  // no ProduceResponse in time
                return;
            }
            if (nowNs >= b.attemptDeadlineNs && !b.routeResendPending) metadataWanted_ = true;  // review M2
            if (nowNs >= b.attemptDeadlineNs && !b.routeResendPending &&
                offerAttempt_(ps, b, nowNs, sink, 1)) {
                b.attemptDeadlineNs = nowNs + attemptNs;  // same frame, a new attempt: broker dedups
                ++resent;
            }
            if (b.attemptDeadlineNs < nextEarliest) nextEarliest = b.attemptDeadlineNs;
        });
    }
    earliestAttemptDeadlineNs_ = nextEarliest;
    return resent;
}

// ---------------------------------------------------------------------------
// CB-5 B: re-stamp the sessionId (MessageHeader int64 LE at byte offset 8) in a
// pre-encoded wire frame.  Used on redirect resend: the leader assigned a NEW
// sessionId, but the stored frame still carries the OLD (follower) sessionId.
// ---------------------------------------------------------------------------
namespace {
constexpr std::size_t kHeaderSessionIdOff = 8; // MessageHeader.sessionId LE64

inline void restampSessionId(std::vector<uint8_t>& frame, int64_t sessionId) noexcept {
    if (frame.size() < kHeaderSessionIdOff + 8) return;
    const uint64_t u = static_cast<uint64_t>(sessionId);
    for (int i = 0; i < 8; ++i)
        frame[kHeaderSessionIdOff + i] = static_cast<uint8_t>(u >> (i * 8));
}
} // anonymous namespace

void ProducerImpl::noteRedirectAnswer(int64_t corrId) noexcept
{
    const DutyScope duty(*this);
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return;
    PartitionState& ps = *partitions_[s];
    if (InFlightBatch* b = ps.inflight.find(corrId)) {
        noteAnswer_(*b, batchcorr::attempt(corrId));
        maybeEndLaneWait_(ps);
    }
}

bool ProducerImpl::retryRedirectedProduce(int64_t corrId, SendFn& sendFn)
{
    const DutyScope duty(*this);  // fix1 C1
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return false;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return false;
    PartitionState& ps = *partitions_[s];
    InFlightBatch* b = ps.inflight.find(corrId);
    if (b == nullptr) return false;
    noteAnswer_(*b, batchcorr::attempt(corrId));  // NEW-4(b)
    if (batchcorr::attempt(corrId) != b->attemptTag) {  // A: a superseded attempt's redirect
        maybeEndLaneWait_(ps);
        return true;
    }
    if (b->redirectCount >= kMaxRedirects) {
        std::fprintf(stderr, "ProducerImpl: batch corrId=%lld (topic=%d part=%d) failed: redirect budget (%d) exhausted\n",
                     static_cast<long long>(b->corrId), ps.topicId, ps.partition, kMaxRedirects);
        failBatch_(ps, *b, OpStatus::DeliveryFailed);  // redirect budget exhausted
        maybeEndLaneWait_(ps);
        return false;
    }
    ++b->redirectCount;
    ++ps.laneGen;  // NEW-4(b): the redirect target is another lane
    if (ps.routeResend) {
        // Review MEDIUM-1: an older batch's re-offer is queued — join it (the
        // per-pass re-offer sends oldest first); never overtake it.
        if (ps.ingressStreamId != 0) b->ingressStreamId = ps.ingressStreamId;
        b->routeResendPending = true;
        markDirty_(ps);
        return false;
    }
    if (offerAttempt_(ps, *b, lastNowNs_, sendFn, 4)) return true;
    // DR-2 (2026-10-05): refused (the leader's publication is still connecting —
    // its session may already be Connected, so no connect event follows): the
    // per-pass route re-offer drives it as soon as the publication accepts.
    queueRouteResend_(ps);
    return false;
}

int ProducerImpl::resendAllRedirected(int64_t newSessionId, SendFn& sendFn)
{
    const DutyScope duty(*this);  // fix1 C1
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ++ps.laneGen;  // NEW-4(b): the retargeted transport is another lane
        bool refused = ps.routeResend;  // review MEDIUM-2: behind a queued / refused re-offer, in order
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (b.redirectCount >= kMaxRedirects) {
                std::fprintf(stderr, "ProducerImpl: batch corrId=%lld (topic=%d part=%d) failed: redirect budget (%d) "
                             "exhausted on resend\n", static_cast<long long>(b.corrId), ps.topicId, ps.partition,
                             kMaxRedirects);
                failBatch_(ps, b, OpStatus::DeliveryFailed);  // redirect budget exhausted
                return;
            }
            restampSessionId(b.wireFrame, newSessionId);
            ++b.redirectCount;
            if (!refused && offerAttempt_(ps, b, lastNowNs_, sendFn, 4)) {
                ++resent;
                return;
            }
            // DR-2: refused (or behind a refused one) — the per-pass route re-offer
            // drives it, oldest first (review LOW-1: on the partition's current stream).
            refused = true;
            if (ps.ingressStreamId != 0) b.ingressStreamId = ps.ingressStreamId;
            b.routeResendPending = true;
            ps.routeResend = true;
            markDirty_(ps);
        });
    }
    return resent;
}

int ProducerImpl::resendAllViaRoute(SendFn& sendFn)
{
    const DutyScope duty(*this);  // fix1 C1
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (offerAttempt_(ps, b, lastNowNs_, sendFn, 2)) ++resent;
        });
    }
    return resent;
}

void ProducerImpl::queueRouteResend_(PartitionState& ps) noexcept
{
    if (ps.inflight.empty()) {  // nothing stored: just stop holding new batches
        ps.routeParked = false;
        ps.routeResend = false;
        return;
    }
    ps.inflight.forEachLive([&](InFlightBatch& b) {
        if (ps.ingressStreamId != 0) b.ingressStreamId = ps.ingressStreamId;  // the new owner's stream
        b.routeResendPending = true;
    });
    ps.routeResend = true;
    // N-b: this re-offer consumes the park; a NotLeader arriving while it drains
    // (for an attempt it already re-offered) parks the partition anew.
    ps.routeParked = false;
    markDirty_(ps);
}

int ProducerImpl::resendPartitionViaRoute(int32_t topicId, int32_t partition) noexcept
{
    const uint32_t idx = partitionIndex_.find(partKey(topicId, partition));
    if (idx == detail::FlatIndexMap::kNone) return 0;
    PartitionState& ps = *partitions_[idx];
    ++ps.laneGen;  // NEW-4(b): the route moved — another lane
    queueRouteResend_(ps);
    return ps.routeResend ? static_cast<int>(ps.inflight.live()) : 0;
}

void ProducerImpl::noteRouteChanged(int32_t topicId, int32_t partition) noexcept
{
    const uint32_t idx = partitionIndex_.find(partKey(topicId, partition));
    if (idx != detail::FlatIndexMap::kNone) ++partitions_[idx]->laneGen;
}

int ProducerImpl::resendRouteParked() noexcept
{
    int queued = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        if (!ps.routeParked || ps.routeResend) continue;
        queueRouteResend_(ps);
        if (ps.routeResend) queued += static_cast<int>(ps.inflight.live());
    }
    return queued;
}

bool ProducerImpl::routeParked(int32_t topicId, int32_t partition) const noexcept
{
    const uint32_t idx = partitionIndex_.find(partKey(topicId, partition));
    return idx != detail::FlatIndexMap::kNone && partitions_[idx]->routeParked;
}

int ProducerImpl::discardRing_(OpStatus st, int limit) {
    // Records still in the app->agent ring (single consumer: we are it) are
    // completed with `st` and never sent.  Tier-2 entries are freed.  NOTE:
    // the ring head advances only when read() returns, so a handler run here
    // must never re-enter a read of this ring — the only ways (closeImmediately
    // / failClose from a handler) are deferred to the DutyScope end (fix round 1,
    // I1), and discardRing_'s callers hold a DutyScope or are Discarding already.
    return ring_.read(
        [&](int32_t type, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
            aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
            if (type == kMsgSendRecord && len >= kRecHeaderSize) {
                const uint64_t h = (p[kRecFlagsOff] & kFlagWantAck)
                                       ? static_cast<uint64_t>(readLE64(p, kRecCorrIdOff)) : 0u;
                const int32_t tid  = readLE32(p, kRecTopicIdOff);
                const int32_t part = readLE32(p, kRecPartOff);
                emitUnsentBatchAck_(tid, part, 1, st, 1);
                completeHandle_(h, OpKind::Send, -1, 0, st, 0);
            } else if (type == kMsgTier2Batch && len >= kT2TokenSize) {
                std::unique_ptr<Tier2Entry> e(
                    reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
                if (!e) return;
                const uint64_t h  = e->ackHandle;
                const int32_t  rc = e->recordCount;
                const int32_t  tid = e->topicId, part = e->partition;
                e.reset();
                emitUnsentBatchAck_(tid, part, rc, st, 1);
                completeHandle_(h, OpKind::Tier2Batch, -1, rc, st, 0);
            }
            // kMsgFlush: nothing to complete.
        },
        limit);
}

void ProducerImpl::failAllPending(OpStatus st) noexcept {
    const DutyScope duty(*this);  // fix1 C1: sends from these callbacks are deferred
    // Records committed to the app ring so far (Ring mode; a closed ring's
    // later commits are completed by the Discarding drain / closeFinal).
    if (mode_ == IngressMode::Ring) {
        for (int pass = 0; pass < 64; ++pass) {
            if (discardRing_(st, INT_MAX) == 0) break;
        }
    }
    failDeferred_(st);  // records deferred before this call were accepted: fail them too
    // Everything the producer accepted: in-flight batches + the current builder
    // + queued Tier-2 batches.  Indexed walk: a handler may append partitions
    // (Ring mode sends from a handler only write the ring; Direct ones defer).
    for (std::size_t i = 0; i < partitions_.size(); ++i) {
        PartitionState& ps = *partitions_[i];
        ps.inflight.forEachLive([&](InFlightBatch& b) { failBatch_(ps, b, st); });
        failBuilder_(ps, st);
        while (!ps.pendingTier2Entries.empty()) {
            const uint64_t h  = ps.pendingTier2Entries.front().ackHandle;
            const int32_t  rc = ps.pendingTier2Entries.front().recordCount;
            ps.pendingTier2Entries.pop_front();
            emitUnsentBatchAck_(ps.topicId, ps.partition, rc, st, 1);
            completeHandle_(h, OpKind::Tier2Batch, -1, rc, st, 0);
        }
        ps.builderBytes   = 0;
        ps.batchStartNs   = 0;
        ps.flushRequested = false;
        ps.resendParked   = false;  // N-3: nothing in flight is left to re-send
        ps.laneWait       = false;  // NEW-4(b): likewise
        reclaimAckRing_(ps);
    }
}

// ---------------------------------------------------------------------------
// F8c C6a: close
// ---------------------------------------------------------------------------

bool ProducerImpl::closeIntake() noexcept {
    if (mode_ == IngressMode::Direct) {
        if (directClosed_) return false;  // owner-thread state (callers are the owner)
        directClosed_ = true;
        return true;
    }
    return ring_.close();  // C1 tail bit: linearizable with every claim, idempotent
}

bool ProducerImpl::clientClosed() const noexcept {
    // Ring: the consumer-finished ring word (closeFinal sets it last, release);
    // Direct: owner-thread state (callers are the owner).
    return mode_ == IngressMode::Direct ? clientClosedDirect_ : ring_.consumerFinished();
}

OpResult ProducerImpl::beginClose(const CompletionTarget& target, Token token) {
    if (mode_ == IngressMode::Direct && std::this_thread::get_id() != dutyThreadId_) {
        return OpResult::WrongThread;
    }
    // The Client is closed (its close completed everything): never touch the
    // hooks' context, which may be gone.
    if (clientClosed()) return OpResult::Closed;
    if (hooks_.issueClose != nullptr) return hooks_.issueClose(hooks_.ctx, *this, &target, token);
    // Bare rig (no Client): the caller IS the duty thread; handler targets only.
    if (token > kMaxToken || target.cq != nullptr || target.handler.fn == nullptr) {
        return OpResult::InvalidArgument;
    }
    if (!closeIntake()) return OpResult::Closed;
    detail::OpTarget t;
    t.handler = target.handler;
    applyBeginClose(t, token, lastNowNs_);
    return OpResult::Accepted;
}

OpResult ProducerImpl::closeImmediately() {
    if (mode_ == IngressMode::Direct && std::this_thread::get_id() != dutyThreadId_) {
        return OpResult::WrongThread;
    }
    if (clientClosed()) return OpResult::Closed;
    if (hooks_.issueClose != nullptr) return hooks_.issueClose(hooks_.ctx, *this, nullptr, 0);
    applyCloseImmediately(OpStatus::Closed);  // bare rig: the caller IS the duty thread
    return OpResult::Accepted;
}

void ProducerImpl::applyBeginClose(const detail::OpTarget& target, Token token, int64_t nowNs) {
    if (!lifeOpen()) {
        // Reachable: a closeImmediately / failClose applied before the CAS
        // winner's BeginClose command.  The token still completes exactly once.
        Completion c;
        c.token  = token;
        c.op     = OpKind::ProducerClose;
        c.status = OpStatus::Closed;
        hooks_.sink.deliver(target, c);
        return;
    }
    LifeDraining d;
    d.target     = target;
    d.token      = token;
    d.deadlineNs = nowNs + cfg_.deliveryTimeoutMs * 1'000'000LL;
    life_        = d;
    flushAll_    = true;  // nothing more can join a batch: flush every builder at once
    if (!inDuty_) (void)closeDuty(nowNs);  // inside a handler: the next hub pass decides
}

void ProducerImpl::applyCloseImmediately(OpStatus st) {
    (void)closeIntake();  // idempotent: the failure path may be the first closer
    if (discarding()) return;
    const bool         wasDraining = draining();
    const LifeDraining d = wasDraining ? std::get<LifeDraining>(life_) : LifeDraining{};
    life_     = LifeDiscarding{st};
    flushAll_ = true;
    if (inDuty_) {
        // F8c fix round 1 (I1): called from a handler this producer fired
        // (an ack / batch-ack / failure completion): the walk that fired it
        // still holds live state (a batch not yet released, a ring read not
        // yet consumed).  Fail-all at the outermost DutyScope's end instead.
        pendingFail_.armed    = true;
        pendingFail_.status   = st;
        pendingFail_.hadToken = wasDraining;
        pendingFail_.drain    = d;
        return;
    }
    failAllPending(st);
    if (wasDraining) {
        Completion c;
        c.token  = d.token;
        c.op     = OpKind::ProducerClose;
        c.status = OpStatus::Closed;
        hooks_.sink.deliver(d.target, c);
    }
}

void ProducerImpl::runPendingFail_() noexcept {
    const PendingFail pf = pendingFail_;
    pendingFail_ = PendingFail{};
    failAllPending(pf.status);  // nested DutyScope: inDuty_ is still set
    if (pf.hadToken) {
        Completion c;
        c.token  = pf.drain.token;
        c.op     = OpKind::ProducerClose;
        c.status = OpStatus::Closed;
        hooks_.sink.deliver(pf.drain.target, c);
    }
}

bool ProducerImpl::fullyDrained_() const noexcept {
    if (mode_ == IngressMode::Ring && ring_.headPosition() < ring_.tailPosition()) return false;
    if (!deferred_.empty()) return false;
    for (const auto& up : partitions_) {
        const PartitionState& ps = *up;
        if ((ps.builder && ps.builder->recordCount() > 0) || !ps.pendingTier2Entries.empty() ||
            !ps.inflight.empty()) {
            return false;
        }
    }
    return true;
}

int ProducerImpl::closeDuty(int64_t nowNs) {
    if (!draining()) return 0;
    const LifeDraining d = std::get<LifeDraining>(life_);
    OpStatus st;
    if (fullyDrained_()) {
        st = OpStatus::Ok;
    } else if (nowNs >= d.deadlineNs) {
        st = OpStatus::Timeout;
    } else {
        return 0;
    }
    // Before user code.  A handler that then calls closeImmediately() moves
    // Closed -> Discarding and fails whatever is still outstanding (nothing
    // after Ok; the stragglers once after Timeout) — idempotent, never twice.
    life_ = LifeClosed{};
    Completion c;
    c.token  = d.token;
    c.op     = OpKind::ProducerClose;
    c.status = st;
    hooks_.sink.deliver(d.target, c);
    return 1;
}

void ProducerImpl::closeFinal(OpStatus st, int stalledYieldsBeforeUnblock) {
    applyCloseImmediately(st);
    if (mode_ != IngressMode::Ring) {
        clientClosedDirect_ = true;
        return;
    }
    // Drain the closed ring up to its frozen tail: a record claimed before the
    // close but committed after it is still read (and completed `st`) — the
    // fix5 close drain (ClientAgent::drainRegistrationsForClose) rule: wait on
    // progress, then Aeron's unblock() after a yield cap; stop only when
    // unblock() cannot make progress (a hang is the worse failure).
    const int64_t target = ring_.tailPosition();
    // Review m9: an earlier failClose(st') set the status the ring's records
    // complete with — keep it.
    const OpStatus ringSt = discarding() ? std::get<LifeDiscarding>(life_).status : st;
    int stalled = 0;
    int failedUnblocks = 0;
    while (ring_.headPosition() < target) {
        const int64_t before = ring_.headPosition();
        const int n = discardRing_(ringSt, INT_MAX);
        if (n > 0 || ring_.headPosition() != before) {
            stalled = 0;
            failedUnblocks = 0;
            continue;
        }
        if (++stalled >= stalledYieldsBeforeUnblock) {
            stalled = 0;
            if (ring_.unblock()) {
                failedUnblocks = 0;
                continue;
            }
            if (++failedUnblocks >= 3) {
                std::fprintf(stderr,
                             "rune-client: producer close: ring blocked by an unrecoverable claim at "
                             "position %lld; records behind it are lost\n",
                             static_cast<long long>(ring_.headPosition()));
                break;
            }
        }
        std::this_thread::yield();
    }
    // Nobody reads this ring again (release): a Producer that outlives its
    // Client sees it and never touches the hooks' (gone) context.
    ring_.consumerFinish();
}

aeron::util::index_t ProducerImpl::testClaimSendRecord(int32_t topicId, int32_t partition,
                                                       const Key& key, std::span<const uint8_t> value,
                                                       uint64_t ackHandle) noexcept {
    try {
        const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + value.size();
        const aeron::util::index_t idx = ring_.tryClaim(kMsgSendRecord, totalLen);
        if (idx < 0) return idx;
        uint8_t* dst = ring_.base() + idx;
        writeLE32(dst, kRecTopicIdOff, topicId);
        writeLE32(dst, kRecPartOff, partition);
        dst[kRecKeyLenOff] = static_cast<uint8_t>(key.size());
        std::memset(dst + kRecKeyBytesOff, 0, Key::kMaxInline);
        if (key.size() > 0) std::memcpy(dst + kRecKeyBytesOff, key.bytes().data(), key.size());
        dst[kRecFlagsOff] = ackHandle != 0 ? kFlagWantAck : 0u;
        writeLE64(dst, kRecCorrIdOff, static_cast<int64_t>(ackHandle));
        writeLE32u(dst, kRecValLenOff, static_cast<uint32_t>(value.size()));
        if (!value.empty()) std::memcpy(dst + kRecValBytesOff, value.data(), value.size());
        return idx;
    } catch (...) {
        return -1;
    }
}

ProducerImpl::~ProducerImpl() {
    if (detail::testOnImplFree != nullptr) detail::testOnImplFree(detail::HandleKind::Producer, this);
    // Last owner gone (no other thread can reach this producer): free every
    // Tier-2 entry still owned here WITHOUT completing anything (a handler's
    // context may already be dead).  Ack handles are plain tokens: nothing to free.
    try {
        for (int pass = 0; pass < 64; ++pass) {
            const int n = ring_.read(
                [](int32_t type, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                   aeron::util::index_t len) {
                    if (type == kMsgTier2Batch && len >= kT2TokenSize) {
                        const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
                        delete reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0)));
                    }
                },
                INT_MAX);
            if (n == 0) break;
        }
    } catch (...) {}
    for (std::size_t r = 0; r < deferred_.tail(); r += deferred_.sizeAt(r)) {  // fix1/2: arena
        if (deferred_.hdr(r)->type != 0) (void)takeDeferredHandle_(r);  // frees a Tier-2 entry
    }
}

// ---------------------------------------------------------------------------
// Explicit instantiations — the only sink types the producer is built for.
// ---------------------------------------------------------------------------
template int ProducerImpl::flushBatches<ProducerImpl::SendFn>(int64_t, int64_t,
                                                             const ProducerImpl::SendFn&);
template int ProducerImpl::flushBatches<agent::AgentProduceSink>(
    int64_t, int64_t, const agent::AgentProduceSink&);
template int ProducerImpl::flushBatches<CountingSink>(int64_t, int64_t, const CountingSink&);
template int ProducerImpl::retransmitStalled<ProducerImpl::SendFn>(int64_t,
                                                                  const ProducerImpl::SendFn&);
template int ProducerImpl::retransmitStalled<agent::AgentProduceSink>(
    int64_t, const agent::AgentProduceSink&);
template int ProducerImpl::retransmitStalled<CountingSink>(int64_t, const CountingSink&);

} // namespace rune::client::producer
