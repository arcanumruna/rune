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
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <stdexcept>

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
    const std::size_t keyLen   = key.size();
    const std::size_t valueLen = value.size();
    const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + valueLen;

    // Public contract: a single SEND_RECORD is capped at kMaxSendValueBytes
    // (a fixed 8 KiB message, independent of the ring's larger maxMsgLength).
    // Oversized values are silently dropped (return false).
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
        return true;
    } catch (...) {
        return false; // preserve noexcept contract
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
                                     int64_t corrId,
                                     int32_t recordCount,
                                     std::vector<uint8_t> batchBytes)
{
    // Push the batch bytes to the side queue BEFORE writing the ring token.
    // The agent pops from this queue when it processes the token.  The mutex
    // ensures correct ordering between app thread(s) and the agent thread.
    {
        std::lock_guard<std::mutex> lock(tier2Mtx_);
        Tier2Entry entry;
        entry.topicId     = topicId;
        entry.partition   = partition;
        entry.corrId      = corrId;
        entry.recordCount = recordCount;
        entry.batchBytes  = std::move(batchBytes);
        tier2Queue_.push_back(std::move(entry));
    }

    // Build the ring token (21 bytes).
    uint8_t tok[kT2TokenSize];
    writeLE32(tok, kT2TopicIdOff,   topicId);
    writeLE32(tok, kT2PartOff,      partition);
    tok[kT2FlagsOff] = kFlagWantAck; // Tier-2 always expects an ack
    writeLE64(tok, kT2CorrIdOff,    corrId);
    writeLE32(tok, kT2RecCountOff,  recordCount);

    const bool ok = writeToRing(kMsgTier2Batch, tok, kT2TokenSize);

    if (!ok) {
        // Ring full: remove the entry we just pushed to keep the queue/ring in sync.
        std::lock_guard<std::mutex> lock(tier2Mtx_);
        if (!tier2Queue_.empty()) {
            tier2Queue_.pop_back();
        }
    }

    return ok;
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

PartitionState& ProducerImpl::getOrCreatePartition(int32_t topicId,
                                                    int32_t partition) {
    const int64_t k = partKey(topicId, partition);
    auto it = partitions_.find(k);
    if (it == partitions_.end()) {
        auto [ins, ok] = partitions_.emplace(k, PartitionState(topicId, partition));
        PartitionState& ps = ins->second;
        // Preallocate the per-partition ack-ring ONCE (zero per-batch alloc).
        ps.ackRing.assign(kAckRingCapacity, 0);
        ps.ackMask = kAckRingCapacity - 1;
        return ps;
    }
    return it->second;
}

void ProducerImpl::growAckRing(PartitionState& ps) {
    const uint32_t oldCap = static_cast<uint32_t>(ps.ackRing.size());
    const uint32_t newCap = oldCap << 1; // stays a power of two
    const uint32_t oldMask = ps.ackMask;
    const uint32_t newMask = newCap - 1;
    std::vector<int64_t> next(newCap, 0);
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
            if (msgTypeId == kMsgFlush) {
                for (auto& [k, ps] : partitions_) ps.flushRequested = true;
                globalFlushPending_.store(false, std::memory_order_relaxed);
                return ControlledAction::CONTINUE;
            }

            if (msgTypeId == kMsgTier2Batch) {
                Tier2Entry entry;
                {
                    std::lock_guard<std::mutex> lock(tier2Mtx_);
                    if (tier2Queue_.empty()) return ControlledAction::CONTINUE; // ring/queue race
                    entry = std::move(tier2Queue_.front());
                    tier2Queue_.pop_front();
                }
                PartitionState& ps = getOrCreatePartition(entry.topicId, entry.partition);
                if (ps.builder && ps.builder->recordCount() > 0) ps.flushRequested = true;
                recordsSent_ += static_cast<uint64_t>(entry.recordCount);
                ps.pendingTier2Entries.push_back(std::move(entry));
                return ControlledAction::CONTINUE;
            }

            if (msgTypeId != kMsgSendRecord) return ControlledAction::CONTINUE;

            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
            const std::size_t sz = static_cast<std::size_t>(len);
            if (sz < static_cast<std::size_t>(kRecHeaderSize)) return ControlledAction::CONTINUE;

            const int32_t topicId   = readLE32(p, kRecTopicIdOff);
            const int32_t partition = readLE32(p, kRecPartOff);
            const uint8_t keyLen    = p[kRecKeyLenOff];
            const uint8_t flags     = p[kRecFlagsOff];
            const int64_t corrId    = readLE64(p, kRecCorrIdOff);
            const uint32_t valueLen = readLE32u(p, kRecValLenOff);

            if (sz < static_cast<std::size_t>(kRecHeaderSize) + valueLen)
                return ControlledAction::CONTINUE;

            PartitionState& ps = getOrCreatePartition(topicId, partition);

            // BACKPRESSURE: if this partition's builder is already at batchBytes and
            // it cannot flush (at maxInFlight), ABORT — leave the record in the ring
            // for a later cycle.  Decided BEFORE any state mutation below.
            if (ps.builderBytes >= cfg_.batchBytes && ps.inFlight >= cfg_.maxInFlight) {
                return ControlledAction::ABORT;
            }

            const uint8_t* keyBytes = p + kRecKeyBytesOff;
            const uint8_t* valBytes = p + kRecValBytesOff;

            // Create the builder ONCE per partition; reused (reset in place) across
            // batches so there is no per-batch heap allocation.
            if (!ps.builder) {
                ps.builder = std::make_unique<rune::protocol::RecordBatchBuilder>(0, wallNs());
            }
            if (ps.builder->recordCount() == 0) {
                ps.builderBytes = 0;
                ps.batchStartNs = nowNs;
            }

            // Batch acks (Kafka-style, ZERO-ALLOC): record this ack-wanting record's
            // corrId in the per-partition ack-ring so onProduceResponse can fan out
            // baseOffset+i to every callback/future.  corrIds need NOT be contiguous
            // (a producer spread over N partitions strides them).
            if ((flags & kFlagWantAck) && corrId != 0) {
                if (ps.ackHead - ps.ackTail >= ps.ackRing.size()) growAckRing(ps);
                if (ps.batchAckCount == 0) {
                    ps.batchAckStart = ps.ackHead;
                    ps.pendingCorrId = corrId; // wire corrId = first ack record in batch
                }
                ps.ackRing[ps.ackHead & ps.ackMask] = corrId;
                ++ps.ackHead;
                ++ps.batchAckCount;
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

bool ProducerImpl::encodeAndSend(
    PartitionState& ps,
    int64_t nowNs,
    int64_t sessionId,
    int64_t correlationId,
    std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn)
{
    if (!ps.builder || ps.builder->recordCount() == 0) return false;

    // Stamp idempotent header fields.  baseSequence is stamped but NOT advanced
    // yet — the advance happens only after a successful offer (below), so a
    // back-pressured retry re-sends the SAME sequence rather than opening a gap.
    ps.builder->setProducerId(producerId_);
    ps.builder->setProducerEpoch(producerEpoch_);
    ps.builder->setBaseSequence(ps.nextBaseSequence);

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

    // Offer to transport — E5.1: pass per-partition ingressStreamId.
    const bool ok = sendFn(ps.ingressStreamId, encodeScratch_.data(), written);

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
        // callbacks waited in pendingAcks_ → produceTimeout (5s), and also opened
        // an idempotent-sequence gap.
        return false;
    }

    ++framesSent_;                                       // one Aeron frame offered
    ps.nextBaseSequence += ps.builder->recordCount();    // advance ONLY on success
    if (correlationId != 0) {
        // Increment in-flight counter (decremented on ProduceResponse).
        // Only track ack-expecting batches; acks=None (corrId==0) are
        // fire-and-forget and must NOT consume inFlight slots.
        ++ps.inFlight;
        // CB-5 B: store wire frame bytes for redirect resend + ack fan-out slice.
        BatchInfo bi;
        bi.topicId        = ps.topicId;
        bi.partition      = ps.partition;
        bi.ingressStreamId = ps.ingressStreamId;  // E5.1: preserve stream for resend
        bi.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);
        bi.ackStart       = ps.batchAckStart; // zero-alloc per-record fan-out slice
        bi.ackCount       = ps.batchAckCount;
        batchMap_[correlationId] = std::move(bi);
    }

    // Reset builder IN PLACE for the next batch (success path only; keep the
    // object + its records_ capacity — no per-batch heap alloc).
    ps.builder->reset(0, wallNs());
    ps.builderBytes  = 0;
    ps.batchStartNs  = 0;
    ps.flushRequested = false;
    ps.pendingCorrId  = 0;
    ps.batchAckCount  = 0; // ackHead/ackTail persist; only the current-batch slice resets
    // batchAckStart is set afresh when the next ack-wanting record arrives.

    return true;
}

// ---------------------------------------------------------------------------
// Agent-thread: encode and send a Tier-2 pre-built batch
// ---------------------------------------------------------------------------

bool ProducerImpl::encodeAndSendTier2(
    PartitionState& ps,
    Tier2Entry& entry,
    int64_t sessionId,
    std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn)
{
    if (entry.batchBytes.empty() || entry.recordCount == 0) return false;

    // Patch the baseSequence, producerId, and producerEpoch into the pre-built
    // RecordBatch bytes.  Use the named constants from rune::protocol::batch
    // (RecordBatch.h) so a layout change on the C++ server side causes a
    // compile-time break rather than silent CRC/seq corruption.
    namespace rb = rune::protocol::batch;
    auto& b = entry.batchBytes;
    if (b.size() < static_cast<std::size_t>(rb::kRecordBatchOverhead)) {
        return false; // truncated batch — should not happen
    }

    // Patch producerId (int64 LE at kProducerIdOffset).
    const uint64_t pid = static_cast<uint64_t>(producerId_);
    for (int i = 0; i < 8; ++i)
        b[rb::kProducerIdOffset + i] = static_cast<uint8_t>(pid >> (i * 8));

    // Patch producerEpoch (int16 LE at kProducerEpochOffset).
    const uint16_t ep = static_cast<uint16_t>(producerEpoch_);
    b[rb::kProducerEpochOffset]     = static_cast<uint8_t>(ep);
    b[rb::kProducerEpochOffset + 1] = static_cast<uint8_t>(ep >> 8);

    // Patch baseSequence (int32 LE at kBaseSequenceOffset).
    const uint32_t seq = static_cast<uint32_t>(ps.nextBaseSequence);
    b[rb::kBaseSequenceOffset]     = static_cast<uint8_t>(seq);
    b[rb::kBaseSequenceOffset + 1] = static_cast<uint8_t>(seq >> 8);
    b[rb::kBaseSequenceOffset + 2] = static_cast<uint8_t>(seq >> 16);
    b[rb::kBaseSequenceOffset + 3] = static_cast<uint8_t>(seq >> 24);

    // Recompute the CRC-32C over the CRC-covered region
    // [kAttributesOffset .. end], stored as uint32 LE at kCrcOffset.
    // Must be recomputed after patching producerId/epoch/baseSequence.
    {
        const uint32_t crc = rune::protocol::crc32c(
            b.data() + rb::kAttributesOffset, b.size() - rb::kAttributesOffset);
        b[rb::kCrcOffset]     = static_cast<uint8_t>(crc);
        b[rb::kCrcOffset + 1] = static_cast<uint8_t>(crc >> 8);
        b[rb::kCrcOffset + 2] = static_cast<uint8_t>(crc >> 16);
        b[rb::kCrcOffset + 3] = static_cast<uint8_t>(crc >> 24);
    }

    // NOTE: baseSequence is patched above but NOT advanced until the offer
    // succeeds (below) — a back-pressured retry must re-send the SAME sequence.

    // Encode the Produce wire frame.
    namespace rpc = rune::protocol::client;
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = entry.corrId;
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

    // E5.1: pass per-partition ingressStreamId for per-worker routing.
    const bool ok = sendFn(ps.ingressStreamId, encodeScratch_.data(), written);

    if (!ok) {
        // Back-pressure: leave the entry + sequence untouched so the caller can
        // retry (flushBatches must NOT pop it).  No sequence advance, no drop.
        return false;
    }

    ++framesSent_; // one Aeron frame offered to transport (Tier-2)
    ps.nextBaseSequence += entry.recordCount; // advance ONLY on success
    if (entry.corrId != 0) {
        ++ps.inFlight;
        // CB-5 B: store wire frame bytes for redirect resend.
        BatchInfo bi;
        bi.topicId        = entry.topicId;
        bi.partition      = entry.partition;
        bi.ingressStreamId = ps.ingressStreamId;  // E5.1: preserve stream for resend
        bi.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);
        batchMap_[entry.corrId] = std::move(bi);
    }

    return true;
}

// ---------------------------------------------------------------------------
// Agent-thread: flush batches
// ---------------------------------------------------------------------------

int ProducerImpl::flushBatches(
    int64_t nowNs,
    int64_t sessionId,
    std::function<bool(int32_t, const uint8_t*, std::size_t)> sendFn)
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

    for (auto& [k, ps] : partitions_) {
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
            const bool sizeFull       = (ps.builderBytes >= cfg_.batchBytes) ||
                                        (cfg_.batchRecords > 0 &&
                                         ps.builder->recordCount() >= cfg_.batchRecords);
            const bool otherTrigger   = lingerExpired || ps.flushRequested || globalFlush ||
                                        !ps.pendingTier2Entries.empty();
            const bool canFlight      = (ps.inFlight < cfg_.maxInFlight);

            // maxInFlight gates ALL flushes (bounds outstanding un-acked batches —
            // the durability/idempotent window; the MaxInFlightBound contract).
            // The builder can never grow past batchBytes because drainRing's drain
            // gate STOPS draining a partition once it is size-full AND at
            // maxInFlight, so no oversized (unsendable) frame is ever built.
            const bool doFlush        = canFlight && (sizeFull || otherTrigger);

            if (doFlush) {
                int64_t corrId = 0;
                if (cfg_.acks != Acks::None) {
                    corrId = (ps.pendingCorrId != 0)
                                 ? ps.pendingCorrId
                                 : nextCorrId_.fetch_add(1, std::memory_order_relaxed);
                }
                if (encodeAndSend(ps, nowNs, sessionId, corrId, sendFn)) {
                    ++flushed;
                    ps.pendingCorrId = 0;
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
            if (ps.inFlight >= cfg_.maxInFlight) break;

            Tier2Entry& entry = ps.pendingTier2Entries.front();

            if (!encodeAndSendTier2(ps, entry, sessionId, sendFn)) {
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
    if (correlationId == 0) return;

    // Look up the batch (agent-thread only; no mutex).  The wire correlationId is
    // the batch's FIRST ack-wanting record corrId.  The batch owns the ack-ring
    // slice [ackStart, ackStart+ackCount); record i landed at offset baseOffset+i.
    PartitionState* ps       = nullptr;
    uint64_t        ackStart = 0;
    int32_t         ackCount = 0;
    auto bit = batchMap_.find(correlationId);
    if (bit != batchMap_.end()) {
        ackStart = bit->second.ackStart;
        ackCount = bit->second.ackCount;
        const int64_t k = partKey(bit->second.topicId, bit->second.partition);
        auto pit = partitions_.find(k);
        if (pit != partitions_.end()) {
            ps = &pit->second;
            if (ps->inFlight > 0) --ps->inFlight;    // one batch, one in-flight slot
            ps->ackTail += static_cast<uint64_t>(ackCount); // FIFO reclaim of ring slots
        }
        batchMap_.erase(bit);
    }
    if (!ps || ackCount == 0) {
        // Fallback (should not happen): resolve just the wire corrId.
        ps = nullptr; ackStart = 0; ackCount = 0;
    }

    // Fan out to every record's callback/future (under mutex).
    //
    // DR-BUG-5: a negative baseOffset is a sentinel from ClientAgent indicating a
    // broker-side non-OK errorCode (e.g. MinIsrViolation) for the whole batch —
    // reject every record's future/callback rather than resolving with garbage.
    std::lock_guard<std::mutex> lock(ackMtx_);
    const bool err = (baseOffset < 0);
    const std::string msg = err
        ? ("produce rejected by broker (errorCode=" + std::to_string(-baseOffset) + ")")
        : std::string();

    auto resolveOne = [&](int64_t corrId, int64_t off) {
        auto ait = pendingAcks_.find(corrId);
        if (ait == pendingAcks_.end()) return;
        PendingAck& pa = ait->second;
        if (err) {
            if (pa.kind == PendingAck::Kind::Callback) {
                if (pa.callback) pa.callback(-1); // -1 signals error to callback
            } else {
                try { pa.promise.set_exception(
                    std::make_exception_ptr(std::runtime_error(msg))); } catch (...) {}
            }
        } else {
            if (pa.kind == PendingAck::Kind::Callback) {
                if (pa.callback) pa.callback(off);
            } else {
                try { pa.promise.set_value(off); } catch (...) {}
            }
        }
        pendingAcks_.erase(ait);
    };

    if (ps && ackCount > 0) {
        for (int32_t i = 0; i < ackCount; ++i) {
            const int64_t corrId = ps->ackRing[(ackStart + static_cast<uint64_t>(i)) & ps->ackMask];
            resolveOne(corrId, baseOffset + i);
        }
    } else {
        resolveOne(correlationId, baseOffset); // degenerate fallback
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

bool ProducerImpl::retryRedirectedProduce(
    int64_t corrId,
    std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn)
{
    auto bit = batchMap_.find(corrId);
    if (bit == batchMap_.end()) return false; // unknown corrId

    BatchInfo& bi = bit->second;
    if (bi.redirectCount >= kMaxRedirects) {
        // Exhausted redirect budget — fail the promise.
        failPending(corrId, "Redirect exhausted: max redirect attempts reached");
        batchMap_.erase(bit);
        return false;
    }

    if (bi.wireFrame.empty()) {
        failPending(corrId, "Redirect: wire frame not available for resend");
        batchMap_.erase(bit);
        return false;
    }

    ++bi.redirectCount;
    // E5.1: pass ingressStreamId so resend goes to the same per-worker stream.
    return sendFn(bi.ingressStreamId, bi.wireFrame.data(), bi.wireFrame.size());
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

int ProducerImpl::resendAllRedirected(
    int64_t newSessionId,
    std::function<bool(int32_t, const uint8_t*, std::size_t)>& sendFn)
{
    int resent = 0;
    for (auto it = batchMap_.begin(); it != batchMap_.end(); ) {
        BatchInfo& bi = it->second;

        if (bi.wireFrame.empty()) {
            const int64_t corrId = it->first;
            it = batchMap_.erase(it);
            failPending(corrId, "Redirect: wire frame not available for resend");
            continue;
        }

        if (bi.redirectCount >= kMaxRedirects) {
            const int64_t corrId = it->first;
            it = batchMap_.erase(it);
            failPending(corrId, "Redirect exhausted: max redirect attempts reached");
            continue;
        }

        // Re-stamp the frame header with the leader's freshly assigned sessionId
        // so the leader accepts the resend (unknown-session would be dropped).
        restampSessionId(bi.wireFrame, newSessionId);

        ++bi.redirectCount;
        // E5.1: pass ingressStreamId so resend goes to the per-worker stream.
        if (sendFn(bi.ingressStreamId, bi.wireFrame.data(), bi.wireFrame.size())) {
            ++resent;
        }
        ++it;
    }
    return resent;
}

void ProducerImpl::failPending(int64_t corrId, const std::string& reason) {
    std::lock_guard<std::mutex> lock(ackMtx_);
    auto it = pendingAcks_.find(corrId);
    if (it == pendingAcks_.end()) return;
    PendingAck& pa = it->second;
    if (pa.kind == PendingAck::Kind::Future) {
        try {
            pa.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
    } else if (pa.kind == PendingAck::Kind::Callback && pa.callback) {
        pa.callback(-1);
    }
    pendingAcks_.erase(it);
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
