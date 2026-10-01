#pragma once
// OneToOneByteRing.h — SPSC lock-free byte ring backed by the internal
// rune::node::detail::O2ORingBuffer (a namespace-isolated copy of common's agrona
// O2O ring, retargeted onto Aeron's AtomicBuffer + RecordDescriptor — no external
// "common" dependency).
//
// Why O2ORingBuffer instead of Aeron's stock OneToOneRingBuffer: the stock ring
// exposes only write(msgTypeId, src, ...) — a scratch→ring COPY that returns only a
// bool.  O2ORingBuffer adds tryClaim()/commit()/abort() so a producer can encode a
// message DIRECTLY into the claimed ring slot (ZERO extra copy) and learn exactly
// when it is back-pressured (claim < 0).  read()/drain() are TEMPLATED so the
// handler inlines (no per-message std::function).
//
// Owned zeroed byte storage = payloadCapacity + TRAILER_LENGTH.
// Hot path: tryClaim()+encode-in-place+commit() = 1 copy (the encode itself);
//           drain() = zero-copy view into the ring.
// Back-pressure: tryClaim() returns < 0 (and write() returns false) when full.

#include <rune/node/detail/O2ORingBuffer.h>

#include <cstdint>
#include <cstddef>
#include <vector>

namespace rune::node {

/// Minimum payload capacity accepted by the ring: power-of-two bytes.
/// Ring total allocation = capacity + TRAILER_LENGTH.
inline constexpr int kDefaultWorkerRingCapacity = 256 * 1024; // 256 KiB payload

/// OneToOneByteRing — SPSC lock-free ring for flat byte messages.
///
/// Thread-safety: producer methods (write/tryClaim/commit/abort) MUST be called
/// from exactly one producer thread; drain() from exactly one consumer thread.
/// Non-copyable, non-movable (the ring holds a view into owned storage).
class OneToOneByteRing {
public:
    using index_t = aeron::util::index_t;

    /// Construct with payload capacity in bytes (must be power-of-two; >= 256).
    explicit OneToOneByteRing(int payloadCapacity = kDefaultWorkerRingCapacity)
        : bufferStorage_(
              static_cast<std::size_t>(payloadCapacity) +
              static_cast<std::size_t>(detail::O2ORingBuffer::trailerLength()),
              std::byte{0})
        , ring_(reinterpret_cast<uint8_t*>(bufferStorage_.data()), bufferStorage_.size())
    {}

    OneToOneByteRing(const OneToOneByteRing&)            = delete;
    OneToOneByteRing& operator=(const OneToOneByteRing&) = delete;
    OneToOneByteRing(OneToOneByteRing&&)                 = delete;
    OneToOneByteRing& operator=(OneToOneByteRing&&)      = delete;

    // -----------------------------------------------------------------------
    // Producer API (single producer thread only)
    // -----------------------------------------------------------------------

    /// Zero-copy claim: reserve `len` bytes for a message of the given msgTypeId and
    /// return the encoded-message offset (relative to base()) to write into, or a
    /// NEGATIVE value if the ring is full (back-pressure — the producer must hold and
    /// retry next cycle; it must NOT drop).  Pair with base() + commit()/abort().
    [[nodiscard]] index_t tryClaim(int32_t msgTypeId, std::size_t len) {
        return ring_.tryClaim(msgTypeId, len);
    }

    /// Writable base pointer of the ring storage.  Add a tryClaim() offset to get
    /// the destination for an in-place encode.
    [[nodiscard]] uint8_t* base() noexcept { return ring_.base(); }

    /// Publish a message previously reserved with tryClaim().
    void commit(index_t claimIndex) { ring_.commit(claimIndex); }

    /// Discard a message previously reserved with tryClaim() (becomes padding).
    void abort(index_t claimIndex) { ring_.abort(claimIndex); }

    /// Convenience: claim + copy + commit.  Returns false if the ring is full
    /// (back-pressure — the producer must hold and retry, never drop).
    bool write(const uint8_t* data, std::size_t len, int32_t msgTypeId) {
        aeron::concurrent::AtomicBuffer srcBuf{const_cast<uint8_t*>(data), len};
        return ring_.write(msgTypeId, srcBuf, /*srcIndex=*/0,
                           static_cast<index_t>(len));
    }

    // -----------------------------------------------------------------------
    // Consumer API (single consumer thread only)
    // -----------------------------------------------------------------------

    /// Drain up to `limit` messages, invoking handler for each.  Templated so the
    /// handler inlines (no per-message std::function).  Handler signature:
    ///   void(int32_t msgTypeId, AtomicBuffer& buf, index_t offset, index_t length)
    /// The buffer view is valid only for the duration of the handler call.
    template <typename Handler>
    int drain(Handler&& handler, int limit = 16) {
        return ring_.read(std::forward<Handler>(handler), limit);
    }

    // -----------------------------------------------------------------------
    // Introspection / back-pressure gates
    // -----------------------------------------------------------------------

    /// Consumer side: nothing unconsumed (tail == head, acquire loads — ring
    /// internals).  Task 13: a shard polls client ingress only once every op
    /// the runtime enqueued before it is executed (announce-before-accept).
    [[nodiscard]] bool empty() const {
        return const_cast<detail::O2ORingBuffer&>(ring_).size() == 0;
    }

    /// Maximum single-message payload the ring can hold (capacity / 8).
    index_t maxMsgLength() const { return ring_.maxMsgLength(); }

    /// True when at least one max-message-length frame can still be written.
    bool hasSpaceForMaxMsg() const {
        const int64_t capacity = static_cast<int64_t>(ring_.maxMsgLength()) * 8LL;
        const int64_t used     = static_cast<int64_t>(const_cast<detail::O2ORingBuffer&>(ring_).size());
        const int64_t needed   = static_cast<int64_t>(ring_.maxMsgLength()) * 2LL;
        return used + needed <= capacity;
    }

    /// Number of max-message-length slots that can still be written (each slot
    /// accounts for 2×maxMsgLength bytes — same safety factor as hasSpaceForMaxMsg).
    [[nodiscard]] int64_t freeMaxMsgSlots() const noexcept {
        const int64_t capacity = static_cast<int64_t>(ring_.maxMsgLength()) * 8LL;
        const int64_t used     = static_cast<int64_t>(const_cast<detail::O2ORingBuffer&>(ring_).size());
        const int64_t free     = capacity - used;
        const int64_t slotSize = static_cast<int64_t>(ring_.maxMsgLength()) * 2LL;
        if (slotSize <= 0) return 0;
        const int64_t slots = free / slotSize;
        return slots > 0 ? slots : 0;
    }

    /// Number of `msgBytes`-sized messages that still fit, accounting for the
    /// per-record framing (8-byte header, 8-byte aligned).  Use this to reserve
    /// return-ring space for SMALL fixed acks (ProduceResult ≈ 150 B) — reserving
    /// by 2×maxMsgLength (freeMaxMsgSlots) drastically over-counts (an 8 MB ring
    /// reports ~4 slots), throttling produce intake.
    [[nodiscard]] int64_t freeSlotsForMsgSize(int64_t msgBytes) const noexcept {
        const int64_t capacity = static_cast<int64_t>(ring_.maxMsgLength()) * 8LL;
        const int64_t used     = static_cast<int64_t>(const_cast<detail::O2ORingBuffer&>(ring_).size());
        const int64_t free     = capacity - used;
        const int64_t slotSize = ((msgBytes + 8 + 7) / 8) * 8; // header + 8-byte align
        if (slotSize <= 0) return 0;
        const int64_t slots = free / slotSize;
        return slots > 0 ? slots : 0;
    }

    /// True when at least one `msgBytes`-sized (framed) message still fits.
    [[nodiscard]] bool hasSpaceForMsg(int64_t msgBytes) const noexcept {
        return freeSlotsForMsgSize(msgBytes) > 0;
    }

private:
    // Raw storage — zero-initialised so the ring descriptor fields start at 0.
    std::vector<std::byte> bufferStorage_;
    // The underlying SPSC ring (internal O2O port with tryClaim/commit/abort).
    detail::O2ORingBuffer ring_;
};

} // namespace rune::node
