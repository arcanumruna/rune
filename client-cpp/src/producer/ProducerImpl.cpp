// src/producer/ProducerImpl.cpp
//
// CB-2 — ProducerImpl: agent-side single-writer accumulator.
//
// Hot path discipline:
//   submitSendRecord(): NO heap allocation, NO lock.  Writes a compact
//   SEND_RECORD into the pre-allocated ring (one memcpy into the ring slot).
//   All batching, encoding, and ack routing happen on the agent thread.

#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/client/DataMessages.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <concurrent/ringbuffer/RingBufferDescriptor.h>

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
    return cap + aeron::concurrent::ringbuffer::RingBufferDescriptor::TRAILER_LENGTH;
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
    , ringAtomicBuf_(ringBuf_.get(), ringTotalBytes(ringCap))
    , ring_(ringAtomicBuf_)
{
    // Pre-reserve partition map and encode scratch to avoid allocs on steady-state.
    partitions_.reserve(16);
    batchScratch_.reserve(64 * 1024);  // RecordBatch bytes (reused by buildInto)
    encodeScratch_.reserve(64 * 1024); // Produce wire frame (grown once)
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
    // Build SEND_RECORD in a stack buffer.
    // Max = kRecHeaderSize + key::kMaxInline + value (capped at ring's maxMsgLength).
    // We use a fixed 8KB stack scratch — ring maxMsgLength is typically 8192.
    constexpr std::size_t kMaxStackBuf = 8192;
    uint8_t scratch[kMaxStackBuf];

    const std::size_t keyLen   = key.size();
    const std::size_t valueLen = value.size();
    const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + valueLen;

    if (totalLen > kMaxStackBuf) {
        // Value too large for a single ring write. Caller should chunk.
        return false;
    }

    // Write fixed fields.
    writeLE32(scratch, kRecTopicIdOff,  topicId);
    writeLE32(scratch, kRecPartOff,     partition);
    scratch[kRecKeyLenOff] = static_cast<uint8_t>(keyLen);
    std::memset(scratch + kRecKeyBytesOff, 0, Key::kMaxInline); // pad
    if (keyLen > 0) {
        std::memcpy(scratch + kRecKeyBytesOff, key.bytes().data(), keyLen);
    }
    scratch[kRecFlagsOff] = wantAck ? kFlagWantAck : 0u;
    writeLE64(scratch, kRecCorrIdOff, correlationId);
    writeLE32u(scratch, kRecValLenOff, static_cast<uint32_t>(valueLen));
    if (valueLen > 0) {
        std::memcpy(scratch + kRecValBytesOff, value.data(), valueLen);
    }

    return writeToRing(kMsgSendRecord, scratch, totalLen);
}

void ProducerImpl::submitFlush() noexcept {
    globalFlushPending_.store(true, std::memory_order_release);
    // Write a kMsgFlush with zero payload to wake the agent ring drain.
    writeToRing(kMsgFlush, nullptr, 0);
}

// ---------------------------------------------------------------------------
// App-thread: opt-in ack registration (may allocate)
// ---------------------------------------------------------------------------

int64_t ProducerImpl::registerCallback(std::function<void(int64_t)> cb) {
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(ackMtx_);
    PendingAck pa;
    pa.kind     = PendingAck::Kind::Callback;
    pa.callback = std::move(cb);
    pa.deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(cfg_.produceTimeoutMs);
    pendingAcks_.emplace(corrId, std::move(pa));
    return corrId;
}

std::pair<int64_t, std::future<int64_t>> ProducerImpl::registerFuture() {
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(ackMtx_);
    PendingAck pa;
    pa.kind    = PendingAck::Kind::Future;
    pa.deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(cfg_.produceTimeoutMs);
    auto fut = pa.promise.get_future();
    pendingAcks_.emplace(corrId, std::move(pa));
    return {corrId, std::move(fut)};
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
// Agent-thread: drain ring
// ---------------------------------------------------------------------------

PartitionState& ProducerImpl::getOrCreatePartition(int32_t topicId,
                                                    int32_t partition) {
    const int64_t k = partKey(topicId, partition);
    auto it = partitions_.find(k);
    if (it == partitions_.end()) {
        auto [ins, ok] = partitions_.emplace(k, PartitionState(topicId, partition));
        return ins->second;
    }
    return it->second;
}

int ProducerImpl::drainRing(int64_t nowNs) {
    int drained = 0;

    // Drain ONE message at a time so that flush boundaries (kMsgFlush or batchBytes)
    // are respected between individual records.  ProducerHub calls drainRing in a
    // loop, interleaving flushBatches between iterations.
    ring_.read(
        [this, nowNs, &drained](
            int32_t                              msgTypeId,
            aeron::concurrent::AtomicBuffer&     buf,
            aeron::util::index_t                 offset,
            aeron::util::index_t                 len)
        {
            if (msgTypeId == kMsgFlush) {
                // Signal all partitions to flush.
                for (auto& [k, ps] : partitions_) {
                    ps.flushRequested = true;
                }
                globalFlushPending_.store(false, std::memory_order_relaxed);
                ++drained;
                return;
            }

            if (msgTypeId != kMsgSendRecord) return;

            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
            const std::size_t sz = static_cast<std::size_t>(len);
            if (sz < static_cast<std::size_t>(kRecHeaderSize)) return;

            const int32_t topicId   = readLE32(p, kRecTopicIdOff);
            const int32_t partition = readLE32(p, kRecPartOff);
            const uint8_t keyLen    = p[kRecKeyLenOff];
            const uint8_t flags     = p[kRecFlagsOff];
            const int64_t corrId    = readLE64(p, kRecCorrIdOff);
            const uint32_t valueLen = readLE32u(p, kRecValLenOff);

            if (sz < static_cast<std::size_t>(kRecHeaderSize) + valueLen) return;

            const uint8_t* keyBytes = p + kRecKeyBytesOff;
            const uint8_t* valBytes = p + kRecValBytesOff;

            PartitionState& ps = getOrCreatePartition(topicId, partition);

            // Initialise builder on first record in a batch.
            if (!ps.builder) {
                ps.builder = std::make_unique<rune::protocol::RecordBatchBuilder>(
                    0, wallNs());
                ps.builderBytes = 0;
                ps.batchStartNs = nowNs;
            }

            // Mark batch as wanting ack if any record in it does.
            if ((flags & kFlagWantAck) && corrId != 0) {
                // The correlationId is per-batch (last one wins for batch-level acks).
                // Store it on the partition so flushBatches can stamp it on the Produce.
                // TODO(milestone-c): per-record acks.
                ps.pendingCorrId = corrId;
                ps.flushRequested = true; // force-flush so each ack-send gets its own batch
            }

            // Append record to builder.
            const std::string_view kv(reinterpret_cast<const char*>(keyBytes),
                                      static_cast<std::size_t>(keyLen));
            const std::string_view vv(reinterpret_cast<const char*>(valBytes),
                                      static_cast<std::size_t>(valueLen));
            ps.builder->append(kv, vv, wallNs());
            ps.builderBytes += static_cast<int32_t>(valueLen);

            ++drained;
        },
        /*limit=*/1); // One message at a time: see ProducerHub drain+flush loop.

    return drained;
}

// ---------------------------------------------------------------------------
// Agent-thread: encode and offer one batch
// ---------------------------------------------------------------------------

bool ProducerImpl::encodeAndSend(
    PartitionState& ps,
    int64_t nowNs,
    int64_t sessionId,
    int64_t correlationId,
    std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    if (!ps.builder || ps.builder->recordCount() == 0) return false;

    // Stamp idempotent header fields.
    ps.builder->setProducerId(producerId_);
    ps.builder->setProducerEpoch(producerEpoch_);
    ps.builder->setBaseSequence(ps.nextBaseSequence);

    // Build the RecordBatch bytes into the reused member scratch buffer
    // (batchScratch_ is cleared + refilled; capacity is retained across flushes —
    // zero per-flush heap allocation on the agent produce hot path).
    ps.builder->buildInto(batchScratch_);

    // Stamp the baseSequence advance.
    ps.nextBaseSequence += ps.builder->recordCount();

    // Encode into the Produce wire frame.
    // The key field on the Produce frame is left empty (partitionKnown=1,
    // partition is computed client-side; the broker uses the partition field).
    //
    // Use the protocol struct directly to get encodedSize() without a cap=0
    // probe (encodeProduce(nullptr,0,...) always returns 0, not needed size).
    namespace rpc = rune::protocol::client;
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = ps.topicId;
    msg.partition      = ps.partition;
    msg.partitionKnown = 1;
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(batchScratch_);

    const std::size_t needed = msg.encodedSize();
    if (needed == 0) return false;

    // Grow scratch if needed (one-time growth; steady-state = no alloc).
    if (encodeScratch_.size() < needed) {
        encodeScratch_.resize(needed);
    }

    const std::size_t written = msg.encode(encodeScratch_.data());
    if (written == 0) return false;

    // Offer to transport (tryClaim path via AeronTransport).
    const bool ok = sendFn(encodeScratch_.data(), written);

    if (ok) {
        if (correlationId != 0) {
            // Increment in-flight counter (decremented on ProduceResponse).
            // Only track ack-expecting batches; acks=None (corrId==0) are
            // fire-and-forget and must NOT consume inFlight slots.
            ++ps.inFlight;
            // Record correlationId → (topicId, partition) for ProduceResponse routing.
            batchMap_[correlationId] = BatchInfo{ps.topicId, ps.partition};
        }
    }

    // Reset builder for next batch regardless (the batch bytes are now in flight).
    ps.builder.reset();
    ps.builderBytes  = 0;
    ps.batchStartNs  = 0;
    ps.flushRequested = false;
    ps.pendingCorrId  = 0;

    return ok;
}

// ---------------------------------------------------------------------------
// Agent-thread: flush batches
// ---------------------------------------------------------------------------

int ProducerImpl::flushBatches(
    int64_t nowNs,
    int64_t sessionId,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    const int64_t lingerNs = cfg_.lingerUs * 1000LL;
    const bool globalFlush = globalFlushPending_.load(std::memory_order_acquire);

    int flushed = 0;

    for (auto& [k, ps] : partitions_) {
        if (!ps.builder || ps.builder->recordCount() == 0) continue;

        // maxInFlight guard: skip if too many outstanding batches.
        if (ps.inFlight >= cfg_.maxInFlight) continue;

        // Check flush triggers.
        const bool lingerExpired = (nowNs - ps.batchStartNs) >= lingerNs;
        const bool sizeFull      = (ps.builderBytes >= cfg_.batchBytes);
        const bool doFlush       = lingerExpired || sizeFull ||
                                   ps.flushRequested || globalFlush;

        if (!doFlush) continue;

        // Determine correlationId:
        //   - acks=None → 0 (fire-and-forget, no ProduceResponse expected).
        //   - acks=Leader/All + wantAck record → use ps.pendingCorrId (registered
        //     by the app thread in registerFuture/registerCallback before the send).
        //   - acks=Leader/All + no wantAck → allocate a fresh corrId so the broker
        //     can route a ProduceResponse (needed to decrement inFlight).
        int64_t corrId = 0;
        if (cfg_.acks != Acks::None) {
            if (ps.pendingCorrId != 0) {
                corrId = ps.pendingCorrId;
            } else {
                // No app-registered ack; allocate internally to track inFlight.
                corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        if (encodeAndSend(ps, nowNs, sessionId, corrId, sendFn)) {
            ++flushed;
            ps.pendingCorrId = 0; // reset after flush
        }
    }

    return flushed;
}

// ---------------------------------------------------------------------------
// Agent-thread: ProduceResponse routing
// ---------------------------------------------------------------------------

void ProducerImpl::onProduceResponse(int64_t correlationId, int64_t baseOffset)
{
    // Resolve pending ack (if any) — under mutex.
    if (correlationId != 0) {
        // Look up (topicId, partition) from batchMap_ (agent-thread only; no mutex).
        auto bit = batchMap_.find(correlationId);
        if (bit != batchMap_.end()) {
            const int64_t k = partKey(bit->second.topicId, bit->second.partition);
            auto pit = partitions_.find(k);
            if (pit != partitions_.end()) {
                PartitionState& ps = pit->second;
                if (ps.inFlight > 0) --ps.inFlight;
            }
            batchMap_.erase(bit);
        }

        // Fire user callback / resolve future (under mutex).
        std::lock_guard<std::mutex> lock(ackMtx_);
        auto ait = pendingAcks_.find(correlationId);
        if (ait != pendingAcks_.end()) {
            PendingAck& pa = ait->second;
            if (pa.kind == PendingAck::Kind::Callback) {
                if (pa.callback) pa.callback(baseOffset);
            } else {
                try { pa.promise.set_value(baseOffset); } catch (...) {}
            }
            pendingAcks_.erase(ait);
        }
    }
}

void ProducerImpl::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    std::lock_guard<std::mutex> lock(ackMtx_);
    for (auto it = pendingAcks_.begin(); it != pendingAcks_.end(); ) {
        if (now >= it->second.deadline) {
            PendingAck& pa = it->second;
            if (pa.kind == PendingAck::Kind::Future) {
                try {
                    pa.promise.set_exception(
                        std::make_exception_ptr(
                            std::runtime_error("produce ack timed out (no ProduceResponse)")));
                } catch (...) {}
            } else if (pa.kind == PendingAck::Kind::Callback && pa.callback) {
                pa.callback(-1); // -1 signals error to callback
            }
            it = pendingAcks_.erase(it);
        } else {
            ++it;
        }
    }
}

void ProducerImpl::failAllPending(const std::string& reason) {
    std::lock_guard<std::mutex> lock(ackMtx_);
    for (auto& [corrId, pa] : pendingAcks_) {
        if (pa.kind == PendingAck::Kind::Future) {
            try {
                pa.promise.set_exception(
                    std::make_exception_ptr(std::runtime_error(reason)));
            } catch (...) {}
        }
        // Callbacks: fire with offset=-1 to signal error (best-effort).
        if (pa.kind == PendingAck::Kind::Callback && pa.callback) {
            pa.callback(-1);
        }
    }
    pendingAcks_.clear();
}

} // namespace rune::client::producer
