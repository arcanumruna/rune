#pragma once
// IngressRing.h — MPSC lock-free ingress ring.
//
// Design (§D of phase2-notes):
//   - Producers (network receiver threads): write()/tryClaim() encoded SBE frames.
//   - Consumer (single agent thread):        drain() reads up to limit frames.
//
// Backed by rune::consensus::detail::M2ORingBuffer — a namespace-isolated copy of
// common's agrona MULTI-producer/single-consumer ring, retargeted onto Aeron's
// AtomicBuffer + RecordDescriptor.  Unlike Aeron's stock ManyToOneRingBuffer (which
// exposes only a scratch→ring write() copy) it adds tryClaim()/commit()/abort() so a
// producer can encode a message DIRECTLY into the claimed ring slot (ZERO extra copy)
// and learn immediately when it is back-pressured (claim == INSUFFICIENT_CAPACITY).
// All allocation happens at construction; the hot path is lock-free.
//
// Message type IDs: use 1 (consensus frames) — RecordDescriptor requires
// typeId >= 1 (0 is reserved for padding).

#include <rune/consensus/detail/M2ORingBuffer.h>

#include <concurrent/AtomicBuffer.h>

#include <functional>
#include <utility>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <memory>
#include <vector>

namespace rune::consensus {

/// Minimum capacity accepted: power-of-2 payload area.
/// Must be a power of two; ring total = capacity + TRAILER_LENGTH.
inline constexpr int kDefaultIngressCapacity = 64 * 1024; // 64 KiB payload

/// Message type id for consensus SBE frames in the ring.
inline constexpr int32_t kConsensusMsgTypeId = 1;

/// Sentinel returned by tryClaim() when the ring is full (back-pressure).
inline constexpr aeron::util::index_t kIngressInsufficientCapacity = -2;

/// IngressRing — MPSC lock-free ring for encoded consensus / outbound frames.
///
/// Thread-safety: write()/tryClaim() may be called from multiple producer threads
/// concurrently.  drain() MUST be called from exactly one consumer thread.
class IngressRing {
public:
    // (msgTypeId, AtomicBuffer& buf, encodedOffset, length) — same shape as
    // agrona's handler_t, defined here so we don't depend on the stock ring header.
    // Legacy alias (callers may still name the shape; drain() takes any callable).
    using DrainHandler = std::function<void(std::int32_t,
                                            aeron::concurrent::AtomicBuffer&,
                                            aeron::util::index_t,
                                            aeron::util::index_t)>;

    /// Construct with payload capacity in bytes (must be power-of-two; ≥256).
    explicit IngressRing(int payloadCapacity = kDefaultIngressCapacity)
        : bufferBytes_(static_cast<std::size_t>(payloadCapacity) +
                       static_cast<std::size_t>(detail::M2ORingBuffer::trailerLength()))
        , bufferStorage_(zeroedStorage_(bufferBytes_))
        , ring_(reinterpret_cast<uint8_t*>(bufferStorage_.get()), bufferBytes_)
    {}

    // Not copyable or movable (the ring holds a view into owned storage).
    IngressRing(const IngressRing&)            = delete;
    IngressRing& operator=(const IngressRing&) = delete;

    // -----------------------------------------------------------------------
    // Producer API (multi-threaded, lock-free)
    // -----------------------------------------------------------------------

    /// Write a raw SBE frame into the ring (claim + copy + commit).
    /// Returns true on success; false if the ring is full (retry or drop).
    /// Thread-safe: may be called from multiple threads concurrently.
    bool write(const uint8_t* data, std::size_t len) {
        aeron::concurrent::AtomicBuffer srcBuf{const_cast<uint8_t*>(data), len};
        return ring_.write(kConsensusMsgTypeId, srcBuf, /*srcIndex=*/0,
                           static_cast<aeron::util::index_t>(len));
    }

    /// Zero-copy claim: reserve `len` bytes and return the encoded-message offset to
    /// write into (relative to base()), or a NEGATIVE value (kIngressInsufficient-
    /// Capacity) on back-pressure.  Encode in place at base()+offset, then commit().
    /// Thread-safe (multi-producer).
    aeron::util::index_t tryClaim(std::size_t len) {
        return ring_.tryClaim(kConsensusMsgTypeId, len);
    }
    /// Publish a slot previously reserved with tryClaim().
    void commit(aeron::util::index_t index) { ring_.commit(index); }
    /// Discard a slot previously reserved with tryClaim() (becomes padding).
    void abort(aeron::util::index_t index) { ring_.abort(index); }
    /// Base pointer for zero-copy in-place encoding at a tryClaim() offset.
    [[nodiscard]] uint8_t* base() noexcept { return ring_.base(); }

    // -----------------------------------------------------------------------
    // Consumer API (single-thread: the agent duty-cycle thread)
    // -----------------------------------------------------------------------

    /// Drain up to limit messages, invoking handler for each.  Returns count read.
    /// TEMPLATED (Task 11 fix 2, B1): the handler is inlined into the ring read —
    /// never wrapped in a std::function on the per-cycle drain.
    template <class Handler>
    int drain(Handler&& handler, int limit = 10) {
        return ring_.read(std::forward<Handler>(handler), limit);
    }

    /// Controlled drain: handler returns true to consume, false to ABORT (the
    /// message stays in the ring and is re-read next drain).  Returns consumed.
    template <class Handler>
    int controlledDrain(Handler&& handler, int limit = 10) {
        return ring_.controlledRead(std::forward<Handler>(handler), limit);
    }

    // -----------------------------------------------------------------------
    // Introspection (tests / debugging)
    // -----------------------------------------------------------------------

    int64_t producerPosition() const { return ring_.producerPosition(); }
    int64_t consumerPosition() const { return ring_.consumerPosition(); }

    /// Payload capacity in bytes (power of two).
    std::size_t capacity() const { return static_cast<std::size_t>(ring_.capacity()); }

    /// Largest single message the ring accepts (agrona: capacity / 8).  write()/
    /// tryClaim() reject a larger message — callers that frame variable-length
    /// payloads must bound them to this value first.
    std::size_t maxMsgLength() const {
        return static_cast<std::size_t>(ring_.maxMsgLength());
    }

private:
    // Raw storage — owned by IngressRing; zero-initialised so ring descriptor
    // fields start at 0 (tail=0, head=0, etc.).
    /// Zeroed storage, every page written once at construction (as a value-filled vector did).  One memset,
    /// not a per-element construct / destroy: an unoptimized (Debug / sanitizer) build walked a std::vector's
    /// bytes one by one at every ring's construction and destruction.
    static std::unique_ptr<std::byte[]> zeroedStorage_(std::size_t n) {
        std::unique_ptr<std::byte[]> p(new std::byte[n]);
        std::memset(p.get(), 0, n);
        return p;
    }

    std::size_t                  bufferBytes_;
    std::unique_ptr<std::byte[]> bufferStorage_;

    // The ring buffer (holds a non-owning AtomicBuffer view into bufferStorage_).
    detail::M2ORingBuffer ring_;
};

} // namespace rune::consensus
