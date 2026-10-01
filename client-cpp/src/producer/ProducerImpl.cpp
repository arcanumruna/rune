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
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune_fr/FlightRecorder.h>

#include <concurrent/BackOffIdleStrategy.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <climits>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <random>
#include <stdexcept>

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

// Current wall-clock timestamp (ns) for batch baseTimestamp.
inline int64_t wallNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
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

// Copies a sent frame into an in-flight slot's reusable buffer.  Capacity grows
// to the next power of two, so frames that differ by a few bytes (timestamp-delta
// varint width, value length) reuse it instead of reallocating in steady state
// (vector::assign would size it to the exact first frame).
inline void keepFrame(std::vector<uint8_t>& dst, const uint8_t* src, std::size_t n) {
    if (dst.capacity() < n) dst.reserve(std::bit_ceil(n));
    dst.assign(src, src + n);
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
                            std::atomic<int64_t>& nextCorrId,
                            int32_t ringCap)
    : cfg_(cfg)
    , nextCorrId_(nextCorrId)
    , ringBuf_(makeRingBuffer(static_cast<std::size_t>(ringTotalBytes(ringCap)),
                              kRingAlignment))
    , ring_(ringBuf_.get(), static_cast<std::size_t>(ringTotalBytes(ringCap)))
    , producerId_(generateProducerId(this))  // F5: unique per instance, never the old shared stub
{
    // Pre-reserve partition map and encode scratch to avoid allocs on steady-state.
    partitions_.reserve(16);
    batchScratch_.reserve(64 * 1024);  // RecordBatch bytes (reused by buildInto)
    encodeScratch_.reserve(64 * 1024); // Produce wire frame (grown once)
    // M2: frame-cap threshold computed ONCE (cfg_ is immutable).  With the default
    // config (maxFrameBytes == batchBytes) frameCapped_ is FALSE, so the per-record
    // pre-flight collapses out entirely — zero happy-path cost.  It only engages when
    // a user sets batchBytes larger than maxFrameBytes.
    frameCapped_ = (cfg_.maxFrameBytes > 0 && cfg_.maxFrameBytes < cfg_.batchBytes);
    effCap_      = frameCapped_ ? cfg_.maxFrameBytes : cfg_.batchBytes;
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

    // Public contract: a single SEND_RECORD is capped at kMaxSendValueBytes
    // (a fixed 8 KiB message, independent of the ring's larger maxMsgLength).
    // Returns false (not accepted) on an oversized value; callers (trySubmit /
    // Producer::trySend / Producer::send) map that to SendResult::ValueTooLarge
    // — never a silent drop (fix1 minor 12).
    if (valueLen > rune::client::Producer::kMaxSendValueBytes) {
        return false;
    }

    // ZERO-COPY (#3): claim the ring slot and write the SEND_RECORD DIRECTLY into
    // it — the value is copied ONCE (app buffer → ring), versus the old path which
    // built the record in a stack scratch and then copied scratch → ring (value
    // copied twice).  No stack scratch, no intermediate copy, no heap, no lock.
    try {
        const aeron::util::index_t idx =
            ring_.tryClaim(kMsgSendRecord, totalLen);
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
        // App-thread: record handed to the app→agent ring (corr=correlationId).
        RUNE_FR_MARK_PRODUCE(rune_fr::Site::ClientAppSend,
                             static_cast<uint32_t>(correlationId));
        return true;
    } catch (...) {
        return false; // preserve noexcept contract
    }
}

int64_t ProducerImpl::steadyNowNs_() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

SendResult ProducerImpl::trySubmit(int32_t topicId, int32_t partition, const Key& key,
                                   std::span<const uint8_t> value, int64_t ackHandle) noexcept {
    // fix1 C2/I3: closed is a terminal state, checked first — a Producer whose
    // Client has gone through close() never reaches submitSendRecord(), and
    // (critically) sendBlocking() never gets far enough to touch the pump hook's
    // context (which may by then be a destroyed Client).
    if (ring_.closed()) return SendResult::Closed;
    if (value.size() > rune::client::Producer::kMaxSendValueBytes) return SendResult::ValueTooLarge;
    // fix1 minor 11: a value that can NEVER fit this ring (maxMsgLength is a
    // function of ringCap, which can be far smaller than kMaxSendValueBytes —
    // e.g. a small test ring) must not be reported as retryable back-pressure:
    // no amount of draining ever makes room for a message the ring cannot hold,
    // so BackPressured here would livelock sendBlocking's retry loop forever.
    const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + value.size();
    if (totalLen > static_cast<std::size_t>(ring_.maxMsgLength())) return SendResult::ValueTooLarge;
    return submitSendRecord(topicId, partition, key, value, ackHandle != 0, ackHandle)
               ? SendResult::Ok
               : SendResult::BackPressured;
}

void ProducerImpl::sendBlocking(SendResult r, int32_t topicId, int32_t partition, const Key& key,
                                std::span<const uint8_t> value) {
    aeron::concurrent::BackoffIdleStrategy idle(
        100, 10, std::chrono::duration<long, std::nano>(1'000),
        std::chrono::duration<long, std::nano>(100'000));
    const int64_t startNs    = clock_();
    const int64_t livenessNs = cfg_.agentLivenessTimeoutMs * 1'000'000LL;
    for (;;) {
        switch (r) {
        case SendResult::Ok:            return;
        case SendResult::BackPressured: break;
        case SendResult::ValueTooLarge:
            throw std::length_error("Producer::send: value exceeds Producer::kMaxSendValueBytes "
                                     "or this producer's ring maxMsgLength");
        case SendResult::InvalidTopic:
            throw std::invalid_argument("Producer::send: invalid topic handle");
        case SendResult::WrongThread:
            throw std::logic_error("Producer::send: invoker-mode producer used off its owner thread");
        case SendResult::WrongShard:
            throw std::logic_error("Producer::send: partition owned by another shard");
        case SendResult::Closed:
            // fix1 C2: consistent with producer()-after-close (Client::producer()
            // throws logic_error("client closed") once stopped_ is set).
            throw std::logic_error("Producer::send: client closed");
        }
        // fix1 C2/I3: re-check closed every iteration, BEFORE the pump attempt —
        // decided immediately, never by waiting out agentLivenessTimeoutMs, and
        // the (possibly by-now-dangling) pump context is never touched once closed.
        if (ring_.closed()) {
            throw std::logic_error("Producer::send: client closed");
        }
        bool pumped = false;
        // fix2 Important-2: NotOwner is decided HERE, by comparing the calling
        // thread against the duty-thread id copied in by setPumpFn(), BEFORE
        // ever calling pumpFn_ — so a thread that is not the duty thread never
        // dereferences pumpCtx_ (the Client), and can never race a concurrent
        // Client destruction on that thread. The duty thread itself cannot race
        // the destructor (Thread mode joins the runner before teardown; Invoker
        // mode's owner thread IS the one destroying it in the off-owner-thread
        // pattern).
        if (pumpFn_ != nullptr && std::this_thread::get_id() == dutyThreadId_) {
            // fix1 C1: a 2-way result from here (NotOwner is pre-decided above).
            // DeclinedInCycle means pumping would re-enter the agent's own poll
            // (we ARE the duty-cycle thread, already mid-cycle) — fail fast,
            // never spin, never wait on a heartbeat that is literally this same
            // stalled call.
            const PumpResult pr = pumpFn_(pumpCtx_);
            if (pr == PumpResult::DeclinedInCycle) {
                throw std::runtime_error(
                    "Producer::send: back-pressured inside a duty-cycle callback — cannot "
                    "pump (would re-enter the agent's own poll); use trySend there instead");
            }
            pumped = (pr == PumpResult::Pumped);
        }
        // else: NotOwner (no hook wired, or a genuinely separate thread) — falls
        // back to the heartbeat-bounded wait below, same as "no hook wired".
        if (!pumped) {
            idle.idle(0);
            const int64_t nowNs = clock_();
            if (ring_.closed()) {
                throw std::logic_error("Producer::send: client closed");
            }
            if (nowNs - std::max(ring_.consumerHeartbeatTime(), startNs) > livenessNs) {
                throw std::runtime_error(
                    "Producer::send: back-pressured and the runner stopped draining for "
                    "> agentLivenessTimeoutMs (client closed?)");
            }
        }
        r = trySubmit(topicId, partition, key, value, 0);
    }
}

void ProducerImpl::submitFlush() noexcept {
    globalFlushPending_.store(true, std::memory_order_release);
    // Write a kMsgFlush with zero payload to wake the agent ring drain.
    writeToRing(kMsgFlush, nullptr, 0);
}

// ---------------------------------------------------------------------------
// App-thread: Tier-2 explicit batch submission
// ---------------------------------------------------------------------------

bool ProducerImpl::submitTier2Batch(int32_t topicId,
                                     int32_t partition,
                                     int64_t ackHandle,
                                     int32_t recordCount,
                                     std::vector<uint8_t> batchBytes)
{
    auto entry = std::make_unique<Tier2Entry>();
    entry->topicId     = topicId;
    entry->partition   = partition;
    entry->ackHandle   = static_cast<uint64_t>(ackHandle);
    entry->recordCount = recordCount;
    entry->batchBytes  = std::move(batchBytes);
    uint8_t tok[kT2TokenSize];
    writeLE64(tok, 0, static_cast<int64_t>(reinterpret_cast<uintptr_t>(entry.get())));
    if (!writeToRing(kMsgTier2Batch, tok, kT2TokenSize)) return false;  // caller fails the handle
    entry.release();  // ownership now travels through the ring
    return true;
}

// ---------------------------------------------------------------------------
// App-thread: opt-in ack registration (allocates one PendingAck; its address is
// the ack handle whose ownership travels through the ring — no map, no lock)
// ---------------------------------------------------------------------------

void PendingAck::complete(uint64_t handle, int64_t offset, std::string_view error,
                          uint64_t* exceptionCounter) noexcept {
    std::unique_ptr<PendingAck> pa(reinterpret_cast<PendingAck*>(static_cast<uintptr_t>(handle)));
    if (!pa) return;
    try {
        if (pa->kind == Kind::Callback) {
            if (pa->callback) pa->callback(error.empty() ? offset : -1);
        } else if (error.empty()) {
            pa->promise.set_value(offset);
        } else {
            pa->promise.set_exception(
                std::make_exception_ptr(std::runtime_error(std::string(error))));
        }
    } catch (const std::exception& e) {
        // fix2 minor: a throwing user callback must not escape the duty cycle
        // — caught, counted, and logged instead of silently swallowed.
        if (exceptionCounter) ++*exceptionCounter;
        std::fprintf(stderr,
            "rune-client: producer ack callback threw: %s (caught; the duty cycle continues)\n",
            e.what());
    } catch (...) {
        if (exceptionCounter) ++*exceptionCounter;
        std::fprintf(stderr,
            "rune-client: producer ack callback threw a non-std::exception "
            "(caught; the duty cycle continues)\n");
    }
}

int64_t ProducerImpl::registerCallback(std::function<void(int64_t)> cb) {
    auto* pa = new PendingAck{};
    pa->kind = PendingAck::Kind::Callback;
    pa->callback = std::move(cb);
    return static_cast<int64_t>(reinterpret_cast<uintptr_t>(pa));
}

std::pair<int64_t, std::future<int64_t>> ProducerImpl::registerFuture() {
    auto* pa = new PendingAck{};
    pa->kind = PendingAck::Kind::Future;
    auto fut = pa->promise.get_future();
    return {static_cast<int64_t>(reinterpret_cast<uintptr_t>(pa)), std::move(fut)};
}

void ProducerImpl::failUnsubmitted(int64_t ackHandle, const std::string& reason) noexcept {
    PendingAck::complete(static_cast<uint64_t>(ackHandle), -1, reason, &callbackExceptions_);
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
    ps->ackRing.assign(kAckRingCapacity, 0);  // preallocated ONCE (zero per-batch alloc)
    ps->ackMask = kAckRingCapacity - 1;
    ps->inflight = InFlightRing(static_cast<uint32_t>(cfg_.maxInFlight > 0 ? cfg_.maxInFlight : 1));
    if (partitions_.size() > batchcorr::kMaxSlot) {
        throw std::length_error("ProducerImpl: more than 262144 partitions");
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

int ProducerImpl::drainRing(int64_t nowNs) {
    using rune::client::detail::ControlledAction;
    // fix1 minor 9: the heartbeat is published once per duty cycle by
    // ProducerHub::drainAndFlush (publishHeartbeat()), not here — drainRing can
    // run up to 256x per producer per cycle.

    // controlledRead with limit=1: drain ONE message per call so ProducerHub can
    // interleave flushBatches() between records (keeps every builder ≤ batchBytes).
    // The handler returns ABORT when the target partition is BLOCKED — its builder
    // has hit batchBytes but cannot flush because it is at maxInFlight.  ABORT
    // leaves the record UNCONSUMED in the ring (precise, in-order backpressure):
    // the producer ring fills → Producer::send() back-pressures the app, and no
    // builder ever grows past a sendable frame size (the fix for the 5s
    // produceTimeout tail).  Unlike Aeron's ManyToOneRingBuffer, this ring both
    // supports ABORT and dispatches the handler inline (no std::function).
    return ring_.controlledRead(
        [this, nowNs](
            int32_t                              msgTypeId,
            aeron::concurrent::AtomicBuffer&     buf,
            aeron::util::index_t                 offset,
            aeron::util::index_t                 len) -> ControlledAction
        {
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
            if (msgTypeId == kMsgFlush) {
                for (auto& up : partitions_) up->flushRequested = true;
                globalFlushPending_.store(false, std::memory_order_relaxed);
                return ControlledAction::CONTINUE;
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

            PartitionState& ps = getOrCreatePartition(topicId, partition);

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
            //   builderBytes past effCap.  The check uses wallNs() for tsDelta, which
            //   is the same function the builder calls inside append() — so the
            //   predicted size matches the actual encoded size.  The builder is reset
            //   to builderBaseWallNs on each frame boundary.
            //   ABORT even when inFlight has room so the current frame is flushed
            //   first; this guarantees NO emitted RecordBatch exceeds effCap wire.
            if (frameCapped && ps.builder && ps.builder->recordCount() > 0) {
                // Compute exact wire contribution of this next record.
                // wallNs() here and inside builder->append() may differ by ≤1µs,
                // which can shift varlongLen(tsDelta) by +1 byte at boundary values.
                // Adding 1 byte of slack makes the check conservative (splits ≤1 byte
                // early at boundary values) but guarantees NO overshoot.
                const int64_t tsDelta = wallNs() - ps.builderBaseWallNs;
                const int32_t contrib = recordBodyContrib(
                    static_cast<int32_t>(keyLen),
                    static_cast<int32_t>(valueLen),
                    tsDelta,
                    ps.builder->recordCount() /* offsetDelta = current record count */)
                    + 1; // +1 byte slack for wallNs() jitter between pre-flight and append

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
            // Normal-path backpressure: ABORT when full AND at maxInFlight.
            if (!frameCapped && ps.builderBytes >= effCap && !canFlight_(ps)) {
                return ControlledAction::ABORT;
            }

            const uint8_t* keyBytes = p + kRecKeyBytesOff;
            const uint8_t* valBytes = p + kRecValBytesOff;

            // Create the builder ONCE per partition; reused (reset in place) across
            // batches so there is no per-batch heap allocation.
            if (!ps.builder) {
                const int64_t baseWall = wallNs();
                ps.builder = std::make_unique<rune::protocol::RecordBatchBuilder>(0, baseWall);
                ps.builderBaseWallNs = baseWall;
            }
            if (ps.builder->recordCount() == 0) {
                ps.builderBytes = 0;
                ps.batchStartNs = nowNs;
                // I1 FIX: do NOT re-sample wallNs() here.  builderBaseWallNs already
                // equals the builder's actual baseTimestamp (set at construction above
                // and at the reset(0, resetWall) in encodeAndSend), so the pre-flight
                // tsDelta is computed against the SAME base the builder encodes with.
                // Re-sampling here made the base newer than the builder's, under-counting
                // tsDelta by up to a varlong byte and compounding the frame-cap overshoot.
            }

            // Per-record acks at the record's OWN index (spec §4.3): the slice of a
            // batch is aligned with its records, void records hold 0.  Zeros are
            // back-filled lazily so batches without callbacks cost nothing.  The
            // handle's ownership passed to this thread with the ring commit.
            if ((flags & kFlagWantAck) && ackHandle != 0) {
                const int32_t index = ps.builder->recordCount();  // this record's offsetDelta
                if (ps.batchAckCount == 0) ps.batchAckStart = ps.ackHead;
                while (ps.batchAckCount < index) pushAck_(ps, 0);
                pushAck_(ps, ackHandle);
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
            ps.builder->append(kv, vv, wallNs());
            // Track the TRUE encoded WIRE size (fixed batch header + accumulated
            // record bodies), not just value bytes — so the batch flushes at
            // batchBytes of WIRE and never overshoots the broker's max produce
            // batch size (the old value-only count undercounted per-record framing
            // by ~20B/record, producing ~70KB wire for a "64KB" batch → rejected).
            ps.builderBytes = static_cast<int32_t>(
                rune::protocol::batch::kRecordBatchOverhead + ps.builder->bodyBytes());
            ++recordsSent_;
            return ControlledAction::CONTINUE;
        },
        /*messageCountLimit=*/1);
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
        ps.builder->setProducerId(producerId_);
        ps.builder->setProducerEpoch(producerEpoch_);
        ps.builder->setBaseSequence(ps.nextBaseSequence);
    }

    // Zero-copy encode: build the Produce frame header (with an EMPTY recordBatch)
    // then append the RecordBatch DIRECTLY after it, into the same wire buffer.
    // This removes the old two-pass cost — buildInto(batchScratch_) followed by a
    // full batch memcpy inside Produce::encode — which dominated the agent thread.
    // The batch's records are now gathered into the frame exactly once.
    namespace rpc = rune::protocol::client;
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = ps.topicId;
    msg.partition      = ps.partition;
    msg.partitionKnown = 1;
    msg.acks           = (cfg_.acks == Acks::All) ? uint8_t{1} : uint8_t{0};  // B2: wire 1=All, 0=Leader/None
    msg.key            = {};
    msg.recordBatch    = {};   // appended directly below (no intermediate copy)

    const std::size_t hdrLen = msg.encodedSize();   // header + empty key + empty rb prefix
    if (hdrLen == 0) return false;
    encodeScratch_.clear();
    encodeScratch_.resize(hdrLen);
    if (msg.encode(encodeScratch_.data()) == 0) return false;

    // Append the serialized batch right after the header (single gather copy).
    const std::size_t batchLen = ps.builder->buildAppend(encodeScratch_);
    // Patch the recordBatch u32 length prefix (the last 4 bytes of the header).
    rpc::detail::putLE32(encodeScratch_.data(),
                         static_cast<int>(hdrLen) - 4,
                         static_cast<int32_t>(batchLen));

    const std::size_t written = encodeScratch_.size();
    if (written == 0) return false;

    // Offer to transport — Task 1.4: route by (topicId,partition); E5.1: pass
    // per-partition ingressStreamId as the transport stream.
    const bool ok = sink(ps.topicId, ps.partition, ps.ingressStreamId,
                        encodeScratch_.data(), written);

    if (!ok) {
        // Aeron ingress back-pressure (term buffer momentarily full / not yet
        // connected).  Leave the batch FULLY intact — builder records, stamped
        // baseSequence, and the ack-ring slice (batchAckStart/Count) are all
        // preserved — and return false so flushBatches retries on the next agent
        // cycle.  buildAppend() serialised into encodeScratch_ but did NOT consume
        // the builder, so the retry re-encodes identically.
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
    RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientBatchOffer,
                             static_cast<uint32_t>(correlationId),
                             static_cast<double>(ps.inflight.live() + 1));
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
        b.attemptDeadlineNs = nowNs + cfg_.produceTimeoutMs * 1'000'000LL;
        b.attempts          = 1;
        b.redirectCount     = 0;
        b.ackStart          = (ps.batchAckCount > 0) ? ps.batchAckStart : ps.ackHead;
        b.ackCount          = ps.batchAckCount;
        b.recordCount       = batchRecords;
        b.baseSequence      = batchSeq;
        b.producerEpoch     = producerEpoch_;
        b.ingressStreamId   = ps.ingressStreamId;
        keepFrame(b.wireFrame, encodeScratch_.data(), written);
        if (b.attemptDeadlineNs < earliestAttemptDeadlineNs_) {
            earliestAttemptDeadlineNs_ = b.attemptDeadlineNs;
        }
        verifyClaimedCorrId_(ps, b, correlationId);  // always on (not an assert)
    } else {
        // acks=None: no response is awaited (the wire id is 0 — see onProduceResponse),
        // so the batch's acks complete now, "sent, offset unknown" (-1), and its
        // ack-ring slice is reclaimed immediately — nothing is retained.
        emitBatchAck_(ps, batchSeq, batchRecords, -1, 0);
        completeSentUnacked_(ps);
    }

    // Reset builder IN PLACE for the next batch (success path only; keep the
    // object + its records_ capacity — no per-batch heap alloc).
    const int64_t resetWall = wallNs();
    ps.builder->reset(0, resetWall);
    ps.builderBytes  = 0;
    ps.builderBaseWallNs = resetWall;  // I1: keep the pre-flight base in lockstep with
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
    if (idempotent) {
        // Patch producerId (int64 LE at kProducerIdOffset).
        const uint64_t pid = static_cast<uint64_t>(producerId_);
        for (int i = 0; i < 8; ++i)
            b[rb::kProducerIdOffset + i] = static_cast<uint8_t>(pid >> (i * 8));

        // Patch producerEpoch (int16 LE at kProducerEpochOffset).
        const uint16_t ep = static_cast<uint16_t>(producerEpoch_);
        b[rb::kProducerEpochOffset]     = static_cast<uint8_t>(ep);
        b[rb::kProducerEpochOffset + 1] = static_cast<uint8_t>(ep >> 8);

        // Patch baseSequence (int32 LE at kBaseSequenceOffset).  NOT advanced
        // until the offer succeeds (below) — a back-pressured retry must
        // re-send the SAME sequence.
        const uint32_t seq = static_cast<uint32_t>(ps.nextBaseSequence);
        b[rb::kBaseSequenceOffset]     = static_cast<uint8_t>(seq);
        b[rb::kBaseSequenceOffset + 1] = static_cast<uint8_t>(seq >> 8);
        b[rb::kBaseSequenceOffset + 2] = static_cast<uint8_t>(seq >> 16);
        b[rb::kBaseSequenceOffset + 3] = static_cast<uint8_t>(seq >> 24);

        // Recompute the CRC-32C over the CRC-covered region
        // [kAttributesOffset .. end], stored as uint32 LE at kCrcOffset.
        // Must be recomputed after patching producerId/epoch/baseSequence.
        const uint32_t crc = rune::protocol::crc32c(
            b.data() + rb::kAttributesOffset, b.size() - rb::kAttributesOffset);
        b[rb::kCrcOffset]     = static_cast<uint8_t>(crc);
        b[rb::kCrcOffset + 1] = static_cast<uint8_t>(crc >> 8);
        b[rb::kCrcOffset + 2] = static_cast<uint8_t>(crc >> 16);
        b[rb::kCrcOffset + 3] = static_cast<uint8_t>(crc >> 24);
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
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(b);

    const std::size_t needed = msg.encodedSize();
    if (needed == 0) return false;

    if (encodeScratch_.size() < needed) {
        encodeScratch_.resize(needed);
    }

    const std::size_t written = msg.encode(encodeScratch_.data());
    if (written == 0) return false;

    // Task 1.4: route by (topicId,partition); E5.1: pass per-partition
    // ingressStreamId as the per-worker transport stream.
    const bool ok = sink(ps.topicId, ps.partition, ps.ingressStreamId,
                        encodeScratch_.data(), written);

    if (!ok) {
        // Back-pressure: leave the entry + sequence untouched so the caller can
        // retry (flushBatches must NOT pop it).  No sequence advance, no drop.
        return false;
    }

    ++framesSent_; // one Aeron frame offered to transport (Tier-2)
    // acks=None: non-idempotent (see above) — sequence stays untouched, matching
    // the Tier-1 rule in encodeAndSend.
    const int32_t batchSeq = idempotent ? ps.nextBaseSequence : -1;
    if (idempotent) {
        ps.nextBaseSequence += entry.recordCount;  // advance ONLY on success
    }
    if (!awaitAck) {
        emitBatchAck_(ps, batchSeq, entry.recordCount, -1, 0);
        const uint64_t h = entry.ackHandle;
        entry.ackHandle = 0;
        PendingAck::complete(h, -1, {}, &callbackExceptions_);  // "sent, offset unknown"
    } else {
        if (ps.ackHead - ps.ackTail >= ps.ackRing.size()) growAckRing(ps);
        const uint64_t slice = ps.ackHead;
        ps.ackRing[ps.ackHead & ps.ackMask] = entry.ackHandle;  // record 0 -> the batch future
        ++ps.ackHead;
        entry.ackHandle = 0;  // ownership moved to the ack ring
        InFlightBatch& ib = ps.inflight.claim(ordinal_, ps.slotIndex);
        // Tier-2 gets the same retransmit/delivery clock as Tier-1 (fixed on sight:
        // the old BatchInfo left firstSendNs = 0, so the first retransmit scan gave
        // it up immediately).
        ib.firstSendNs       = nowNs;
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
        ib.producerEpoch     = producerEpoch_;
        ib.ingressStreamId   = ps.ingressStreamId;
        keepFrame(ib.wireFrame, encodeScratch_.data(), written);
        verifyClaimedCorrId_(ps, ib, wireCorrId);  // always on (not an assert)
    }

    return true;
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
    // M1 FIX [CB-5]: Do NOT emit any frame while the session is not established
    // (sessionId == 0 means Connecting / post-forceReconnect window).  Records
    // stay in the builder / Tier-2 queue and will flush naturally once
    // ConnectResponse{OK} arrives and drainAndFlush is called with the real
    // leader-assigned sessionId.  This prevents Produce frames from being
    // stamped sessionId=0 and silently dropped by the broker.
    if (sessionId == 0) return 0;

    const int64_t lingerNs = cfg_.lingerUs * 1000LL;
    // Consume the global-flush latch atomically: read-and-clear.  Relying on the
    // drainRing kMsgFlush token to clear it (the old design) left the flag stuck
    // TRUE forever whenever submitFlush()'s token write hit a full ring — which
    // forced a flush on every drained record and destroyed batching.  Clearing it
    // here makes one flushBatches cycle consume the flush; no token dependence.
    const bool globalFlush = globalFlushPending_.exchange(false, std::memory_order_acq_rel);

    int flushed = 0;

    for (auto& up : partitions_) {
        PartitionState& ps = *up;
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
            // gate STOPS draining a partition once it is size-full AND at
            // maxInFlight, so no oversized (unsendable) frame is ever built.
            const bool doFlush        = canFlight && (sizeFull || otherTrigger);

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
        if (tier1Pending) continue;

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

    return flushed;
}

// ---------------------------------------------------------------------------
// Agent-thread: ProduceResponse routing
// ---------------------------------------------------------------------------

void ProducerImpl::onProduceResponse(int64_t correlationId, int64_t baseOffset)
{
    if (!batchcorr::isBatch(correlationId) || batchcorr::ordinal(correlationId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(correlationId);
    if (s >= partitions_.size()) return;
    PartitionState& ps = *partitions_[s];
    InFlightBatch* b = ps.inflight.find(correlationId);
    if (b == nullptr) return;  // duplicate / stale (slot reused) / already expired: ignore
    // DR-BUG-5: a negative baseOffset is the -errorCode sentinel for the whole batch.
    const int16_t ec = baseOffset < 0 ? static_cast<int16_t>(-baseOffset) : int16_t{0};
    emitBatchAck_(ps, b->baseSequence, b->recordCount, baseOffset, ec);
    if (ec == 0) {
        completeSlice_(ps, b->ackStart, b->ackCount, baseOffset, {});
    } else {
        const std::string err = "produce rejected by broker (errorCode=" + std::to_string(ec) + ")";
        completeSlice_(ps, b->ackStart, b->ackCount, -1, err);
        onUnresolvedSequence_(b->producerEpoch);  // rejected: its sequences were never written
    }
    ps.inflight.release(*b);
    reclaimAckRing_(ps);
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
    failBatch_(ps, b, "internal error: batch correlation id mismatch");
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
                                  int64_t baseOffset, std::string_view error) noexcept {
    for (int32_t i = 0; i < ackCount; ++i) {
        uint64_t& slot = ps.ackRing[(ackStart + static_cast<uint64_t>(i)) & ps.ackMask];
        const uint64_t h = slot;
        slot = 0;
        if (h == 0) continue;
        RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientAckComplete, static_cast<uint32_t>(h),
                                 static_cast<double>(baseOffset + i));
        PendingAck::complete(h, baseOffset + i, error, &callbackExceptions_);
    }
}

void ProducerImpl::completeSentUnacked_(PartitionState& ps) noexcept {
    if (ps.batchAckCount <= 0) return;
    for (int32_t i = 0; i < ps.batchAckCount; ++i) {
        uint64_t& slot = ps.ackRing[(ps.batchAckStart + static_cast<uint64_t>(i)) & ps.ackMask];
        const uint64_t h = slot;
        slot = 0;
        if (h != 0) PendingAck::complete(h, -1, {}, &callbackExceptions_);
    }
    // The builder slice is always the newest one and acks=None never has an
    // in-flight slice, so the slice is reclaimed by rewinding the head.
    ps.ackHead       = ps.batchAckStart;
    ps.batchAckCount = 0;
    reclaimAckRing_(ps);
}

void ProducerImpl::emitBatchAck_(const PartitionState& ps, int32_t baseSequence,
                                 int32_t recordCount, int64_t baseOffset, int16_t errorCode) {
    if (!batchAckCb_ && !collectBatchAcks_) return;
    const BatchAck a{ps.topicId, ps.partition, errorCode == 0 ? baseOffset : -1,
                     baseSequence, recordCount, errorCode};
    if (batchAckCb_) {
        // fix2 minor: a throwing user callback must not escape the duty cycle
        // — caught, counted, and logged instead of silently swallowed.
        try {
            batchAckCb_(a);
        } catch (const std::exception& e) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: producer onBatchAck callback threw: %s (caught; the duty "
                "cycle continues)\n", e.what());
        } catch (...) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: producer onBatchAck callback threw a non-std::exception "
                "(caught; the duty cycle continues)\n");
        }
    }
    if (collectBatchAcks_) completedAcks_.push_back(a);
}

void ProducerImpl::failBatch_(PartitionState& ps, InFlightBatch& b, const std::string& reason) {
    emitBatchAck_(ps, b.baseSequence, b.recordCount, -1, BatchAck::kClientFailure);
    completeSlice_(ps, b.ackStart, b.ackCount, -1, reason);
    onUnresolvedSequence_(b.producerEpoch);  // outcome at the broker unknown
    ps.inflight.release(b);
    reclaimAckRing_(ps);
}

void ProducerImpl::onUnresolvedSequence_(int16_t batchEpoch) noexcept {
    if (batchEpoch != producerEpoch_) return;  // gap already resolved by an earlier bump
    if (producerEpoch_ == INT16_MAX) {
        // Epoch space exhausted: rotate to a FRESH unique id (never producerId_+1 —
        // an incremented id is just as likely to collide with another live
        // producer's id as the original was, and gives the broker no reason to
        // believe this is a distinct producer).  Fresh id + epoch 0 is the same
        // trust-on-first-use the broker already grants any never-seen producerId.
        producerId_ = generateProducerId(this);
        producerEpoch_ = 0;
    } else {
        ++producerEpoch_;
    }
    for (auto& up : partitions_) up->nextBaseSequence = 0;
    std::fprintf(stderr,
                 "ProducerImpl: unresolved sequence -> producerId=%lld epoch=%d, sequences "
                 "restart at 0\n",
                 static_cast<long long>(producerId_), static_cast<int>(producerEpoch_));
}

void ProducerImpl::failBatch(int64_t corrId, const std::string& reason) {
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return;
    if (InFlightBatch* b = partitions_[s]->inflight.find(corrId)) failBatch_(*partitions_[s], *b, reason);
}

void ProducerImpl::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    const int64_t nowNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    const int64_t deliveryNs = cfg_.deliveryTimeoutMs * 1'000'000LL;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (nowNs - b.firstSendNs >= deliveryNs) {
                failBatch_(ps, b, "produce delivery timed out (no ProduceResponse)");
            }
        });
        // Records never sent (no session / no route) past the delivery deadline are
        // failed AND dropped — an ack is never reported failed for a record that
        // could still be written later.
        if (ps.builder && ps.builder->recordCount() > 0 && ps.batchStartNs != 0 &&
            nowNs - ps.batchStartNs >= deliveryNs) {
            if (ps.batchAckCount > 0) {
                completeSlice_(ps, ps.batchAckStart, ps.batchAckCount, -1,
                               "produce delivery timed out (never sent)");
                ps.ackHead = ps.batchAckStart;  // reclaim the unsent (newest) slice
            }
            ps.batchAckCount = 0;
            const int64_t resetWall = wallNs();
            ps.builder->reset(0, resetWall);
            ps.builderBaseWallNs = resetWall;
            ps.builderBytes      = 0;
            ps.batchStartNs      = 0;
            ps.flushRequested    = false;
            reclaimAckRing_(ps);
        }
        // Tier-2 batches never sent past the deadline: same rule (FIFO, so the
        // expired ones are at the front).
        while (!ps.pendingTier2Entries.empty() &&
               nowNs - ps.pendingTier2Entries.front().enqueuedNs >= deliveryNs) {
            PendingAck::complete(ps.pendingTier2Entries.front().ackHandle, -1,
                                 "produce delivery timed out (never sent)", &callbackExceptions_);
            ps.pendingTier2Entries.pop_front();
        }
    }
}

template <class Sink>
int ProducerImpl::retransmitStalled(int64_t nowNs, const Sink& sink)
{
    if (nowNs < earliestAttemptDeadlineNs_) return 0;  // O(1) on the happy path
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
                failBatch_(ps, b, "produce delivery timed out (no ProduceResponse)");
                return;
            }
            if (nowNs >= b.attemptDeadlineNs &&
                sink(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(),
                     b.wireFrame.size())) {
                ++b.attempts;
                b.attemptDeadlineNs = nowNs + attemptNs;  // same frame, same corrId: broker dedups
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

bool ProducerImpl::retryRedirectedProduce(int64_t corrId, SendFn& sendFn)
{
    if (!batchcorr::isBatch(corrId) || batchcorr::ordinal(corrId) != ordinal_) return false;
    const uint32_t s = batchcorr::slot(corrId);
    if (s >= partitions_.size()) return false;
    PartitionState& ps = *partitions_[s];
    InFlightBatch* b = ps.inflight.find(corrId);
    if (b == nullptr) return false;
    if (b->redirectCount >= kMaxRedirects) {
        failBatch_(ps, *b, "Redirect exhausted: max redirect attempts reached");
        return false;
    }
    ++b->redirectCount;
    return sendFn(ps.topicId, ps.partition, b->ingressStreamId, b->wireFrame.data(),
                  b->wireFrame.size());
}

int ProducerImpl::resendAllRedirected(int64_t newSessionId, SendFn& sendFn)
{
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (b.redirectCount >= kMaxRedirects) {
                failBatch_(ps, b, "Redirect exhausted: max redirect attempts reached");
                return;
            }
            restampSessionId(b.wireFrame, newSessionId);
            ++b.redirectCount;
            if (sendFn(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(),
                       b.wireFrame.size())) {
                ++resent;
            }
        });
    }
    return resent;
}

int ProducerImpl::resendAllViaRoute(SendFn& sendFn)
{
    int resent = 0;
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            if (sendFn(ps.topicId, ps.partition, b.ingressStreamId, b.wireFrame.data(),
                       b.wireFrame.size())) {
                ++resent;
            }
        });
    }
    return resent;
}

template <class OnHandle>
void ProducerImpl::drainAppRingHandles_(OnHandle&& onHandle) {
    // Records still in the app->agent ring (single consumer: we are it).  A
    // padding record at the wrap reads as 0 messages, so stop after two empty
    // passes; hard-capped so concurrent writers cannot keep us here.
    auto h = [&](int32_t type, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                 aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
        if (type == kMsgSendRecord && len >= kRecHeaderSize && (p[kRecFlagsOff] & kFlagWantAck)) {
            onHandle(static_cast<uint64_t>(readLE64(p, kRecCorrIdOff)));
        } else if (type == kMsgTier2Batch && len >= kT2TokenSize) {
            std::unique_ptr<Tier2Entry> e(
                reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
            if (e) onHandle(e->ackHandle);
        }
    };
    int empty = 0;
    for (int pass = 0; pass < 64 && empty < 2; ++pass) {
        empty = (ring_.read(h, INT_MAX) == 0) ? empty + 1 : 0;
    }
}

void ProducerImpl::failAllPending(const std::string& reason) {
    drainAppRingHandles_([&](uint64_t handle) {
        PendingAck::complete(handle, -1, reason, &callbackExceptions_);
    });
    // Everything the producer accepted: in-flight slices + the current builder slice.
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        ps.inflight.forEachLive([&](InFlightBatch& b) {
            emitBatchAck_(ps, b.baseSequence, b.recordCount, -1, BatchAck::kClientFailure);
            onUnresolvedSequence_(b.producerEpoch);  // released with an unknown outcome
            ps.inflight.release(b);
        });
        for (uint64_t i = ps.ackTail; i < ps.ackHead; ++i) {
            uint64_t& h = ps.ackRing[i & ps.ackMask];
            if (h != 0) {
                PendingAck::complete(h, -1, reason, &callbackExceptions_);
                h = 0;
            }
        }
        ps.ackTail = ps.ackHead;
        ps.batchAckCount = 0;
        for (Tier2Entry& e : ps.pendingTier2Entries) {
            PendingAck::complete(e.ackHandle, -1, reason, &callbackExceptions_);
        }
        ps.pendingTier2Entries.clear();
        if (ps.builder) {
            const int64_t resetWall = wallNs();
            ps.builder->reset(0, resetWall);
            ps.builderBaseWallNs = resetWall;
        }
        ps.builderBytes   = 0;
        ps.batchStartNs   = 0;
        ps.flushRequested = false;
    }
}

ProducerImpl::~ProducerImpl() {
    // Last owner gone (no other thread can reach this producer): free every
    // handle still owned here WITHOUT invoking user callbacks (their captures may
    // already be dead) — a future's promise is destroyed, so its get() throws
    // broken_promise, exactly as when the old ack map was destroyed.
    auto discard = [](uint64_t h) {
        delete reinterpret_cast<PendingAck*>(static_cast<uintptr_t>(h));
    };
    try {
        drainAppRingHandles_(discard);
    } catch (...) {}
    for (auto& up : partitions_) {
        PartitionState& ps = *up;
        for (uint64_t i = ps.ackTail; i < ps.ackHead; ++i) {
            uint64_t& h = ps.ackRing[i & ps.ackMask];
            if (h != 0) { discard(h); h = 0; }
        }
        for (Tier2Entry& e : ps.pendingTier2Entries) discard(e.ackHandle);
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
