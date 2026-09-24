#pragma once
// OneToOneByteRing.h — SPSC lock-free byte ring backed by Aeron's OneToOneRingBuffer.
//
// Design (cross-thread-ring refactor):
//   - Single producer thread: write() encodes a flat byte message → ring.
//   - Single consumer thread: drain() reads up to limit frames (zero-copy view).
//
// Mirrors libs/consensus/include/rune/consensus/IngressRing.h exactly, but
// wraps aeron::concurrent::ringbuffer::OneToOneRingBuffer (SPSC, no locking)
// instead of ManyToOneRingBuffer (MPSC).
//
// Owned zeroed byte storage = payloadCapacity + TRAILER_LENGTH.
// Non-owning AtomicBuffer view + OneToOneRingBuffer over it.
// Hot path: write() = 1 copy (src→ring); drain() = zero-copy view into ring.
// Back-pressure: write() returns false when full (producer must hold + retry).

#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/OneToOneRingBuffer.h>
#include <concurrent/ringbuffer/RingBufferDescriptor.h>

#include <cstdint>
#include <cstddef>
#include <vector>
#include <stdexcept>

namespace rune::node {

/// Minimum payload capacity accepted by OneToOneRingBuffer: power-of-two bytes.
/// Ring total allocation = capacity + TRAILER_LENGTH.
inline constexpr int kDefaultWorkerRingCapacity = 256 * 1024; // 256 KiB payload

/// OneToOneByteRing — SPSC lock-free ring for flat byte messages.
///
/// Thread-safety: write() MUST be called from exactly one producer thread.
///                drain()  MUST be called from exactly one consumer thread.
/// Non-copyable, non-movable (AtomicBuffer is a non-owning view into owned storage).
class OneToOneByteRing {
public:
    using handler_t = aeron::concurrent::ringbuffer::handler_t;
    // handler_t signature (agrona):
    //   void(int32_t msgTypeId, AtomicBuffer& buf, index_t offset, index_t length)
    // The handler receives a zero-copy view into the ring buffer — valid only
    // for the duration of the handler call; do NOT retain references.

    /// Construct with payload capacity in bytes (must be power-of-two; >= 256).
    explicit OneToOneByteRing(int payloadCapacity = kDefaultWorkerRingCapacity)
        : bufferStorage_(
              static_cast<std::size_t>(payloadCapacity) +
              static_cast<std::size_t>(
                  aeron::concurrent::ringbuffer::RingBufferDescriptor::TRAILER_LENGTH),
              std::byte{0})
        , atomicBuffer_(
              reinterpret_cast<uint8_t*>(bufferStorage_.data()),
              bufferStorage_.size())
        , ring_(atomicBuffer_)
    {}

    // Not copyable or movable — AtomicBuffer is a non-owning view into bufferStorage_.
    OneToOneByteRing(const OneToOneByteRing&)            = delete;
    OneToOneByteRing& operator=(const OneToOneByteRing&) = delete;
    OneToOneByteRing(OneToOneByteRing&&)                 = delete;
    OneToOneByteRing& operator=(OneToOneByteRing&&)      = delete;

    // -----------------------------------------------------------------------
    // Producer API (single producer thread only)
    // -----------------------------------------------------------------------

    /// Write a raw byte frame into the ring with the given msgTypeId.
    /// msgTypeId MUST be >= 1 (agrona RecordDescriptor requirement).
    /// Returns true on success; false if the ring is full (back-pressure —
    /// the producer must NOT drop; it holds and retries next duty-cycle).
    bool write(const uint8_t* data, std::size_t len, int32_t msgTypeId) {
        aeron::concurrent::AtomicBuffer srcBuf{
            const_cast<uint8_t*>(data),
            len};
        return ring_.write(
            msgTypeId,
            srcBuf,
            /*srcIndex=*/0,
            static_cast<aeron::util::index_t>(len));
    }

    // -----------------------------------------------------------------------
    // Consumer API (single consumer thread only)
    // -----------------------------------------------------------------------

    /// Drain up to limit messages, invoking handler for each.
    /// Returns the number of messages read.
    int drain(const handler_t& handler, int limit = 16) {
        return ring_.read(handler, limit);
    }

    // -----------------------------------------------------------------------
    // Introspection (tests / debugging)
    // -----------------------------------------------------------------------

    int64_t producerPosition() const { return ring_.producerPosition(); }
    int64_t consumerPosition() const { return ring_.consumerPosition(); }

    /// Maximum single-message payload the ring can hold (capacity / 8).
    aeron::util::index_t maxMsgLength() const { return ring_.maxMsgLength(); }

    /// True when at least one max-message-length slot is available in the ring.
    /// Used by the G-A6 backpressure guard in Worker::doWork to check whether
    /// the return ring has capacity before polling the WorkerIngress for more
    /// produce frames — preventing ack drops under back-pressure.
    bool hasSpaceForMaxMsg() const {
        // A message of maxMsgLength() bytes needs (maxMsgLength() + header overhead).
        // Simpler proxy: the ring has capacity when producerPosition advances by at
        // most (total_capacity - TRAILER) over consumerPosition.  Since agrona's
        // internal size field = producerPosition - consumerPosition, we compare
        // against (capacity / 8) * 8 = capacity which is (total - TRAILER).
        // Equivalently: if we can still write one maxMsgLength frame, there is space.
        // agrona ring_.tryClaim returns < 0 on full; we approximate with positions.
        // capacity() is payload bytes = total - TRAILER; maxMsg = capacity / 8.
        // used = producerPosition - consumerPosition (in payload-space, not index).
        const int64_t capacity = static_cast<int64_t>(maxMsgLength()) * 8LL;
        const int64_t used     = ring_.producerPosition() - ring_.consumerPosition();
        // Leave room for a max-message frame + minimal alignment overhead (×2 safety).
        const int64_t needed   = static_cast<int64_t>(maxMsgLength()) * 2LL;
        return used + needed <= capacity;
    }

    /// Number of max-message-length slots that can still be written to the ring.
    ///
    /// Used by the E5.1.3 reservation-before-poll logic in Worker::pollWorkerIngress_:
    ///   K = min(desiredFragmentLimit, freeMaxMsgSlots() − retOverflow_.size())
    /// This allows K > 1 produce frames per doWork cycle while preserving G-A6
    /// (never poll more frames than there are available return slots).
    ///
    /// Each "slot" accounts for 2×maxMsgLength bytes of ring space (same ×2 safety
    /// factor as hasSpaceForMaxMsg so the results are consistent).
    [[nodiscard]] int64_t freeMaxMsgSlots() const noexcept {
        const int64_t capacity = static_cast<int64_t>(maxMsgLength()) * 8LL;
        const int64_t used     = ring_.producerPosition() - ring_.consumerPosition();
        const int64_t free     = capacity - used;
        const int64_t slotSize = static_cast<int64_t>(maxMsgLength()) * 2LL;
        if (slotSize <= 0) return 0;
        const int64_t slots = free / slotSize;
        return slots > 0 ? slots : 0;
    }

private:
    // Raw storage — zero-initialised so Aeron ring descriptor fields start at 0.
    std::vector<std::byte> bufferStorage_;

    // Non-owning view used by OneToOneRingBuffer.
    aeron::concurrent::AtomicBuffer atomicBuffer_;

    // The underlying SPSC ring.
    aeron::concurrent::ringbuffer::OneToOneRingBuffer ring_;
};

} // namespace rune::node
