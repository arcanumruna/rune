#pragma once
// IngressRing.h — MPSC lock-free ingress ring backed by Aeron's ManyToOneRingBuffer.
//
// Design (§D of phase2-notes):
//   - Producers (network receiver threads): write() encoded SBE frames → ring.
//   - Consumer (single agent thread):        drain() reads up to limit frames.
//
// Backed by aeron::concurrent::ringbuffer::ManyToOneRingBuffer which is a
// battle-tested MPSC ring with correct acquire/release semantics and TRAILER
// metadata.  All allocation happens at construction; hot path is lock-free.
//
// Message type IDs: use 1 (consensus frames) — RecordDescriptor requires
// typeId >= 1 (0 is reserved for padding).

#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/ringbuffer/RingBufferDescriptor.h>

#include <functional>
#include <memory>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <stdexcept>

namespace rune::consensus {

/// Minimum capacity that ManyToOneRingBuffer accepts: power-of-2 payload area.
/// Must be a power of two; ring total = capacity + TRAILER_LENGTH.
inline constexpr int kDefaultIngressCapacity = 64 * 1024; // 64 KiB payload

/// Message type id for consensus SBE frames in the ring.
inline constexpr int32_t kConsensusMsgTypeId = 1;

/// IngressRing — MPSC lock-free ring for encoded consensus frames.
///
/// Thread-safety: write() may be called from multiple producer threads
/// concurrently.  drain() MUST be called from exactly one consumer thread
/// (the agent duty-cycle thread).
class IngressRing {
public:
    using DrainHandler = aeron::concurrent::ringbuffer::handler_t;
    // handler_t signature:
    //   void(int32_t msgTypeId, AtomicBuffer& buf, index_t offset, index_t length)

    /// Construct with payload capacity in bytes (must be power-of-two; ≥256).
    explicit IngressRing(int payloadCapacity = kDefaultIngressCapacity)
        : bufferStorage_(
              static_cast<std::size_t>(payloadCapacity) +
              static_cast<std::size_t>(aeron::concurrent::ringbuffer::RingBufferDescriptor::TRAILER_LENGTH),
              std::byte{0})
        , atomicBuffer_(
              reinterpret_cast<uint8_t*>(bufferStorage_.data()),
              bufferStorage_.size())
        , ring_(atomicBuffer_)
    {}

    // Not copyable or movable (AtomicBuffer is a non-owning view).
    IngressRing(const IngressRing&)            = delete;
    IngressRing& operator=(const IngressRing&) = delete;

    // -----------------------------------------------------------------------
    // Producer API (multi-threaded, lock-free)
    // -----------------------------------------------------------------------

    /// Write a raw SBE frame into the ring.
    /// Returns true on success; false if ring is full (producer should retry or drop).
    /// Thread-safe: may be called from multiple threads concurrently.
    bool write(const uint8_t* data, std::size_t len) {
        // Wrap caller's buffer as a read-only AtomicBuffer view for ManyToOneRingBuffer.
        aeron::concurrent::AtomicBuffer srcBuf{
            const_cast<uint8_t*>(data),
            len};
        return ring_.write(
            kConsensusMsgTypeId,
            srcBuf,
            /*srcIndex=*/0,
            static_cast<aeron::util::index_t>(len));
    }

    // -----------------------------------------------------------------------
    // Consumer API (single-thread: the agent duty-cycle thread)
    // -----------------------------------------------------------------------

    /// Drain up to limit messages, invoking handler for each.
    /// Returns the count of messages read.
    int drain(const DrainHandler& handler, int limit = 10) {
        return ring_.read(handler, limit);
    }

    // -----------------------------------------------------------------------
    // Introspection (tests / debugging)
    // -----------------------------------------------------------------------

    int64_t producerPosition() const { return ring_.producerPosition(); }
    int64_t consumerPosition() const { return ring_.consumerPosition(); }

private:
    // Raw storage — owned by IngressRing; zero-initialised so Aeron ring
    // descriptor fields start at 0 (tail=0, head=0, etc.).
    std::vector<std::byte> bufferStorage_;

    // Non-owning view used by ManyToOneRingBuffer.
    aeron::concurrent::AtomicBuffer atomicBuffer_;

    // The ring buffer itself.
    aeron::concurrent::ringbuffer::ManyToOneRingBuffer ring_;
};

} // namespace rune::consensus
