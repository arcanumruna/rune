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

            if (msgTypeId == kMsgTier2Batch) {
                // Pop the pre-built batch from tier2Queue_ (FIFO order).
                // NOTE: drainRing is called with limit=1; this handler is
                // invoked on the agent thread (single-writer).  The tier2Mtx_
                // guards access from the app thread side only.
                Tier2Entry entry;
                {
                    std::lock_guard<std::mutex> lock(tier2Mtx_);
                    if (tier2Queue_.empty()) return; // queue/ring race (shouldn't happen)
                    entry = std::move(tier2Queue_.front());
                    tier2Queue_.pop_front();
                }

                // Get or create the PartitionState to access idempotent seq.
                PartitionState& ps = getOrCreatePartition(entry.topicId, entry.partition);

                // If there are pending Tier-1 records in the builder, mark
                // flushRequested so flushBatches() sends them before the Tier-2
                // entry (preserving submission order).
                // CB-5 A1/A3: ordering is enforced by flushBatches's tier1Pending guard;
                // the dead dual-branch is collapsed here.
                if (ps.builder && ps.builder->recordCount() > 0) {
                    ps.flushRequested = true;
                }

                // Enqueue the Tier-2 entry for deferred dispatch in flushBatches.
                ps.pendingTier2Entries.push_back(std::move(entry));

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
            // CB-5 B: store wire frame bytes for redirect resend.
            BatchInfo bi;
            bi.topicId   = ps.topicId;
            bi.partition = ps.partition;
            bi.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);
            batchMap_[correlationId] = std::move(bi);
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
// Agent-thread: encode and send a Tier-2 pre-built batch
// ---------------------------------------------------------------------------

bool ProducerImpl::encodeAndSendTier2(
    PartitionState& ps,
    Tier2Entry& entry,
    int64_t sessionId,
    std::function<bool(const uint8_t*, std::size_t)>& sendFn)
{
    if (entry.batchBytes.empty() || entry.recordCount == 0) return false;

    // Patch the baseSequence, producerId, and producerEpoch into the pre-built
    // RecordBatch bytes.  The fields are at fixed offsets defined in RecordBatch.h:
    //   producerId    : int64 LE at offset 43
    //   producerEpoch : int16 LE at offset 51
    //   baseSequence  : int32 LE at offset 53
    // We write directly into the bytes vector (already allocated on app thread).
    auto& b = entry.batchBytes;
    if (b.size() < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        return false; // truncated batch — should not happen
    }

    // Patch producerId (int64 LE at offset 43).
    const uint64_t pid = static_cast<uint64_t>(producerId_);
    for (int i = 0; i < 8; ++i)
        b[43 + i] = static_cast<uint8_t>(pid >> (i * 8));

    // Patch producerEpoch (int16 LE at offset 51).
    const uint16_t ep = static_cast<uint16_t>(producerEpoch_);
    b[51] = static_cast<uint8_t>(ep);
    b[52] = static_cast<uint8_t>(ep >> 8);

    // Patch baseSequence (int32 LE at offset 53).
    const uint32_t seq = static_cast<uint32_t>(ps.nextBaseSequence);
    b[53] = static_cast<uint8_t>(seq);
    b[54] = static_cast<uint8_t>(seq >> 8);
    b[55] = static_cast<uint8_t>(seq >> 16);
    b[56] = static_cast<uint8_t>(seq >> 24);

    // Recompute the CRC-32C over the CRC-covered region.
    // The CRC at offset 17 (uint32 LE) covers bytes [21..end].
    // We must recompute after patching producerId/epoch/sequence.
    // Use the same CRC library the RecordBatchBuilder uses.
    {
        const std::size_t crcStart = 21; // attributes field (kAttributesOffset)
        const uint32_t crc = rune::protocol::crc32c(
            b.data() + crcStart, b.size() - crcStart);
        b[17] = static_cast<uint8_t>(crc);
        b[18] = static_cast<uint8_t>(crc >> 8);
        b[19] = static_cast<uint8_t>(crc >> 16);
        b[20] = static_cast<uint8_t>(crc >> 24);
    }

    // Advance baseSequence by the record count.
    ps.nextBaseSequence += entry.recordCount;

    // Encode the Produce wire frame.
    namespace rpc = rune::protocol::client;
    rpc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = entry.corrId;
    msg.topicId        = entry.topicId;
    msg.partition      = entry.partition;
    msg.partitionKnown = 1;
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(b);

    const std::size_t needed = msg.encodedSize();
    if (needed == 0) return false;

    if (encodeScratch_.size() < needed) {
        encodeScratch_.resize(needed);
    }

    const std::size_t written = msg.encode(encodeScratch_.data());
    if (written == 0) return false;

    const bool ok = sendFn(encodeScratch_.data(), written);

    if (ok && entry.corrId != 0) {
        ++ps.inFlight;
        // CB-5 B: store wire frame bytes for redirect resend.
        BatchInfo bi;
        bi.topicId   = entry.topicId;
        bi.partition = entry.partition;
        bi.wireFrame.assign(encodeScratch_.data(), encodeScratch_.data() + written);
        batchMap_[entry.corrId] = std::move(bi);
    }

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
        // ---- Tier-1: flush pending auto-batch if triggered -------------------
        //
        // CB-5 A1 FIX: Tier-2 entries must NEVER be dispatched while a
        // Tier-1 auto-batch is blocked in the builder (i.e. there are
        // accumulated records that haven't been sent yet).  If a Tier-1 batch
        // is stuck behind maxInFlight, Tier-2 entries for the same partition
        // must also wait — jumping ahead would violate submission order.

        bool tier1Pending = (ps.builder && ps.builder->recordCount() > 0);

        if (tier1Pending) {
            // maxInFlight guard: only attempt flush if there's capacity.
            if (ps.inFlight < cfg_.maxInFlight) {
                // Check flush triggers.
                const bool lingerExpired = (nowNs - ps.batchStartNs) >= lingerNs;
                const bool sizeFull      = (ps.builderBytes >= cfg_.batchBytes);
                const bool doFlush       = lingerExpired || sizeFull ||
                                           ps.flushRequested || globalFlush ||
                                           !ps.pendingTier2Entries.empty();

                if (doFlush) {
                    int64_t corrId = 0;
                    if (cfg_.acks != Acks::None) {
                        if (ps.pendingCorrId != 0) {
                            corrId = ps.pendingCorrId;
                        } else {
                            corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }

                    if (encodeAndSend(ps, nowNs, sessionId, corrId, sendFn)) {
                        ++flushed;
                        ps.pendingCorrId = 0;
                        // After a successful Tier-1 flush, tier1Pending is now false.
                        tier1Pending = false;
                    }
                    // If encodeAndSend returns false (transport back-pressure),
                    // tier1Pending remains true and Tier-2 will be blocked below.
                }
                // else: flush not triggered yet — Tier-1 still accumulating.
                // tier1Pending remains true; Tier-2 blocked to preserve order.
            }
            // else: maxInFlight exceeded — Tier-1 blocked, so Tier-2 also blocked.
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

            if (encodeAndSendTier2(ps, entry, sessionId, sendFn)) {
                ++flushed;
            }
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

bool ProducerImpl::retryRedirectedProduce(
    int64_t corrId,
    std::function<bool(const uint8_t*, std::size_t)>& sendFn)
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
    return sendFn(bi.wireFrame.data(), bi.wireFrame.size());
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
    std::function<bool(const uint8_t*, std::size_t)>& sendFn)
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
        if (sendFn(bi.wireFrame.data(), bi.wireFrame.size())) {
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
