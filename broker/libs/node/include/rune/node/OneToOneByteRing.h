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

private:
    // Raw storage — zero-initialised so Aeron ring descriptor fields start at 0.
    std::vector<std::byte> bufferStorage_;

    // Non-owning view used by OneToOneRingBuffer.
    aeron::concurrent::AtomicBuffer atomicBuffer_;

    // The underlying SPSC ring.
    aeron::concurrent::ringbuffer::OneToOneRingBuffer ring_;
};

} // namespace rune::node
