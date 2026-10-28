#pragma once
// OneToOneRingBuffer — internal single-producer / single-consumer byte ring.
//
// A faithful port of Aeron's concurrent::ringbuffer::OneToOneRingBuffer
// (aeron-client C++, include/concurrent/ringbuffer/OneToOneRingBuffer.h) into
// rune::client::detail, with the same three changes M2ORingBuffer makes to
// Aeron's ManyToOneRingBuffer:
//
//   1. read() is TEMPLATED (the handler is inlined) — Aeron's read() takes a
//      `const handler_t&` (std::function): a per-message indirect call.
//   2. The synchronisation words (record header, head, tail) use this class's
//      own __atomic release/acquire accessors instead of Aeron's
//      putInt64Ordered/getInt64Volatile (plain volatile + asm barrier), which
//      are invisible to ThreadSanitizer.  Same machine code on x86-64.
//   3. The trailer is extended with two Rune-only "closed" words, ONE WRITER
//      EACH (F8a fix round 1, I3/minor):
//        * PRODUCER_CLOSED (CL*12) — written only by the producer side ("no
//          more messages will ever be published; the last one is final");
//        * CONSUMER_CLOSED (CL*14) — written only by the consumer side ("the
//          consumer abandoned this channel" — e.g. a handle whose registration
//          was never/indeterminately delivered).
//      closed() reports either.  Aeron's RingBufferDescriptor has neither word
//      (its trailer ends at CL*12); nothing outside rune::client::detail ever
//      reads this ring, so extending it is safe.
//
// Everything else — record framing (8-byte header: length low word, type high
// word, written as ONE 64-bit store), alignment, end-of-buffer padding, the
// producer's head cache (no per-publish read of the consumer's head: the
// consumer's head is re-read only when the cached view says the record does not
// fit), capacity/maxMsgLength rules — is Aeron's, line for line.
//
// Threading: exactly ONE producer thread may call write(); exactly ONE consumer
// thread may call read().

#include <climits>
#include <cstdint>
#include <string>

#include <concurrent/AtomicBuffer.h>
#include "rune/protocol/ring/RecordDescriptor.h"
#include <util/BitUtil.h>
#include <util/Index.h>
#include <util/Exceptions.h>

namespace rune::client::detail {

class OneToOneRingBuffer {
public:
    /// buffer/length describe a region of (capacity == pow2) + trailerLength() bytes.
    OneToOneRingBuffer(std::uint8_t* buffer, std::size_t length)
        : m_buffer(buffer, static_cast<aeron::util::index_t>(length)) {
        m_capacity = m_buffer.capacity() - TRAILER_LENGTH;
        checkCapacity(m_capacity);
        m_maxMsgLength              = m_capacity / 8;
        m_tailPositionIndex         = m_capacity + TAIL_POSITION_OFFSET;
        m_headCachePositionIndex    = m_capacity + HEAD_CACHE_POSITION_OFFSET;
        m_headPositionIndex         = m_capacity + HEAD_POSITION_OFFSET;
        m_correlationIdCounterIndex = m_capacity + CORRELATION_COUNTER_OFFSET;
        m_consumerHeartbeatIndex    = m_capacity + CONSUMER_HEARTBEAT_OFFSET;
        m_producerClosedIndex       = m_capacity + PRODUCER_CLOSED_OFFSET;
        m_consumerClosedIndex       = m_capacity + CONSUMER_CLOSED_OFFSET;
        m_detachIndex               = m_capacity + DETACH_OFFSET;
    }

    OneToOneRingBuffer(const OneToOneRingBuffer&)            = delete;
    OneToOneRingBuffer& operator=(const OneToOneRingBuffer&) = delete;

    [[nodiscard]] aeron::util::index_t capacity()     const { return m_capacity; }
    [[nodiscard]] aeron::util::index_t maxMsgLength() const { return m_maxMsgLength; }
    static constexpr aeron::util::index_t trailerLength() { return TRAILER_LENGTH; }

    // ------------------------------------------------------------------
    // Producer side (one thread).
    // ------------------------------------------------------------------

    /// Aeron OneToOneRingBuffer::write: false on insufficient capacity.
    bool write(std::int32_t msgTypeId, const aeron::concurrent::AtomicBuffer& srcBuffer,
               aeron::util::index_t srcIndex, aeron::util::index_t length) {
        return writeWith(msgTypeId, length,
                         [&](aeron::concurrent::AtomicBuffer& dst, aeron::util::index_t at) {
                             dst.putBytes(at, srcBuffer, srcIndex, length);
                         });
    }

    /// Same as write(), but `fill(buffer, encodedOffset)` copies the payload in
    /// place (templated: no std::function, no intermediate buffer).  The header
    /// is published only after fill() returns; if fill() throws nothing is
    /// published (the tail is not advanced) and the exception propagates.
    template <typename Fill>
    bool writeWith(std::int32_t msgTypeId, aeron::util::index_t length, Fill&& fill) {
        namespace RD = rune::protocol::ring;
        RD::RecordDescriptor::checkMsgTypeId(msgTypeId);
        checkMsgLength(length);

        const aeron::util::index_t recordLength = length + RD::RecordDescriptor::HEADER_LENGTH;
        const aeron::util::index_t requiredCapacity =
            aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);
        const aeron::util::index_t mask = m_capacity - 1;

        // Producer-owned words: the head CACHE and the tail are written only by
        // this thread, so plain reads of them are exact.
        std::int64_t head = m_buffer.getInt64(m_headCachePositionIndex);
        const std::int64_t tail = m_buffer.getInt64(m_tailPositionIndex);
        const aeron::util::index_t availableCapacity =
            m_capacity - static_cast<aeron::util::index_t>(tail - head);

        if (requiredCapacity > availableCapacity) {
            head = loadAcquire64_(m_headPositionIndex);
            if (requiredCapacity > (m_capacity - static_cast<aeron::util::index_t>(tail - head))) {
                return false;
            }
            m_buffer.putInt64(m_headCachePositionIndex, head);
        }

        aeron::util::index_t padding = 0;
        auto recordIndex = static_cast<aeron::util::index_t>(tail & mask);
        const aeron::util::index_t toBufferEndLength = m_capacity - recordIndex;

        if (requiredCapacity > toBufferEndLength) {
            auto headIndex = static_cast<std::int32_t>(head & mask);
            if (requiredCapacity > headIndex) {
                head = loadAcquire64_(m_headPositionIndex);
                headIndex = static_cast<std::int32_t>(head & mask);
                if (requiredCapacity > headIndex) {
                    return false;
                }
                m_buffer.putInt64(m_headCachePositionIndex, head);
            }
            padding = toBufferEndLength;
        }

        // The payload is filled BEFORE the padding header is published (Aeron
        // writes the padding first; its putBytes cannot fail).  A caller-supplied
        // fill may throw: published padding without an advanced tail would let
        // the consumer skip past the tail.  Filling first publishes nothing on a
        // throw.  The target region is free either way (checked above).
        const aeron::util::index_t writeIndex = (0 != padding) ? 0 : recordIndex;
        fill(m_buffer, RD::RecordDescriptor::encodedMsgOffset(writeIndex));

        if (0 != padding) {
            storeRelease64_(recordIndex,
                            RD::RecordDescriptor::makeHeader(padding, RD::RecordDescriptor::PADDING_MSG_TYPE_ID));
            recordIndex = 0;
        }

        storeRelease64_(recordIndex, RD::RecordDescriptor::makeHeader(recordLength, msgTypeId));
        storeRelease64_(m_tailPositionIndex, tail + requiredCapacity + padding);
        return true;
    }

    /// Rune extension (producer side): would `bytes` of records (already
    /// aligned, padding included) fit right now?  Uses the producer's head
    /// cache exactly like write(): the consumer's head is re-read (and the
    /// cache refreshed) only when the cached view says no.
    [[nodiscard]] bool hasCapacityFor(aeron::util::index_t bytes) {
        std::int64_t head = m_buffer.getInt64(m_headCachePositionIndex);
        const std::int64_t tail = m_buffer.getInt64(m_tailPositionIndex);
        if (bytes <= m_capacity - static_cast<aeron::util::index_t>(tail - head)) return true;
        head = loadAcquire64_(m_headPositionIndex);
        m_buffer.putInt64(m_headCachePositionIndex, head);
        return bytes <= m_capacity - static_cast<aeron::util::index_t>(tail - head);
    }

    /// Rune extension: the producer will publish nothing more (release).  Idempotent.
    void producerClose() noexcept { storeRelease64_(m_producerClosedIndex, 1); }

    // ------------------------------------------------------------------
    // Consumer side (one thread).
    // ------------------------------------------------------------------

    /// Aeron OneToOneRingBuffer::read with a templated handler
    /// (msgTypeId, AtomicBuffer&, encodedOffset, length).  Like Aeron, one call
    /// consumes at most the contiguous block up to the buffer end; a call that
    /// only consumed end-of-buffer padding returns 0 having advanced the head.
    template <typename Handler>
    int read(Handler&& handler, int messageCountLimit) {
        namespace RD = rune::protocol::ring;
        const std::int64_t head = m_buffer.getInt64(m_headPositionIndex);  // consumer-owned
        const auto headIndex = static_cast<std::int32_t>(head & (m_capacity - 1));
        const std::int32_t contiguousBlockLength = m_capacity - headIndex;
        int messagesRead = 0;
        int bytesRead = 0;

        ScopeExit cleanup([&]() {
            if (bytesRead != 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(m_headPositionIndex, head + bytesRead);
            }
        });

        while ((bytesRead < contiguousBlockLength) && (messagesRead < messageCountLimit)) {
            const std::int32_t recordIndex = headIndex + bytesRead;
            const std::int64_t header = loadAcquire64_(recordIndex);
            const std::int32_t recordLength = RD::RecordDescriptor::recordLength(header);
            if (recordLength <= 0) break;

            bytesRead += aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);

            const std::int32_t msgTypeId = RD::RecordDescriptor::messageTypeId(header);
            if (RD::RecordDescriptor::PADDING_MSG_TYPE_ID == msgTypeId) continue;

            ++messagesRead;
            handler(msgTypeId, m_buffer, RD::RecordDescriptor::encodedMsgOffset(recordIndex),
                    recordLength - RD::RecordDescriptor::HEADER_LENGTH);
        }
        return messagesRead;
    }

    template <typename Handler>
    int read(Handler&& handler) { return read(std::forward<Handler>(handler), INT_MAX); }

    /// Rune extension: the consumer abandoned the channel (release).  Idempotent.
    void consumerClose() noexcept { storeRelease64_(m_consumerClosedIndex, 1); }

    // ------------------------------------------------------------------
    // Either side.
    // ------------------------------------------------------------------

    [[nodiscard]] bool producerClosed() const noexcept { return loadAcquire64_(m_producerClosedIndex) != 0; }
    [[nodiscard]] bool consumerClosed() const noexcept { return loadAcquire64_(m_consumerClosedIndex) != 0; }
    [[nodiscard]] bool closed() const noexcept { return producerClosed() || consumerClosed(); }

    /// F8c fix round 2 (I5): last-one-frees ownership handshake on ONE word.
    /// Each side detaches exactly once with one RMW; the call that finds the
    /// OTHER side already detached returns true — its caller frees the ring.
    /// Exactly one of the two calls returns true; neither side ever waits.
    bool detachProducer() noexcept {
        return (__atomic_fetch_or(word64_(m_detachIndex), kDetachedProducer, __ATOMIC_ACQ_REL) &
                kDetachedConsumer) != 0;
    }
    bool detachConsumer() noexcept {
        return (__atomic_fetch_or(word64_(m_detachIndex), kDetachedConsumer, __ATOMIC_ACQ_REL) &
                kDetachedProducer) != 0;
    }
    /// F8c C7 fix round 2 (F1): the consumer side (the handle) detaches AND
    /// marks its release ORPHANED in one RMW (see M2ORingBuffer).  True iff
    /// the producer side had already detached (the caller frees the owner).
    bool detachConsumerOrphaned() noexcept {
        return (__atomic_fetch_or(word64_(m_detachIndex), kDetachedConsumer | kOrphanRelease,
                                  __ATOMIC_ACQ_REL) & kDetachedProducer) != 0;
    }
    [[nodiscard]] bool consumerOrphaned() const noexcept {
        return (loadAcquire64_(m_detachIndex) & kOrphanRelease) != 0;
    }
    /// F8c C7: either side (acquire) — has the producer side detached?  Once
    /// true, the producer side never touches the ring (or its owner) again.
    [[nodiscard]] bool producerDetached() const noexcept {
        return (loadAcquire64_(m_detachIndex) & kDetachedProducer) != 0;
    }

    [[nodiscard]] std::int64_t producerPosition() const noexcept { return loadAcquire64_(m_tailPositionIndex); }
    [[nodiscard]] std::int64_t consumerPosition() const noexcept { return loadAcquire64_(m_headPositionIndex); }

    /// Aeron OneToOneRingBuffer::size (consistent head/tail snapshot).
    [[nodiscard]] std::int32_t size() const noexcept {
        std::int64_t headBefore;
        std::int64_t tail;
        std::int64_t headAfter = loadAcquire64_(m_headPositionIndex);
        do {
            headBefore = headAfter;
            tail = loadAcquire64_(m_tailPositionIndex);
            headAfter = loadAcquire64_(m_headPositionIndex);
        } while (headAfter != headBefore);
        const std::int64_t size = tail - headAfter;
        if (size < 0) return 0;
        if (size > m_capacity) return static_cast<std::int32_t>(m_capacity);
        return static_cast<std::int32_t>(size);
    }

    void consumerHeartbeatTime(std::int64_t timeNs) noexcept {
        __atomic_store_n(word64_(m_consumerHeartbeatIndex), timeNs, __ATOMIC_RELAXED);
    }
    [[nodiscard]] std::int64_t consumerHeartbeatTime() const noexcept {
        return __atomic_load_n(word64_(m_consumerHeartbeatIndex), __ATOMIC_RELAXED);
    }

    /// Aeron: a one-to-one ring can never be blocked by another producer.
    bool unblock() noexcept { return false; }

private:
    static constexpr aeron::util::index_t CL = aeron::util::BitUtil::CACHE_LINE_LENGTH;
    // CL*0..CL*10: Aeron RingBufferDescriptor.  CL*12/CL*14: Rune extensions.
    static constexpr std::int64_t kDetachedProducer = 1;
    static constexpr std::int64_t kDetachedConsumer = 2;
    static constexpr std::int64_t kOrphanRelease    = 4;
    static constexpr aeron::util::index_t TAIL_POSITION_OFFSET       = CL * 2;
    static constexpr aeron::util::index_t HEAD_CACHE_POSITION_OFFSET = CL * 4;
    static constexpr aeron::util::index_t HEAD_POSITION_OFFSET       = CL * 6;
    static constexpr aeron::util::index_t CORRELATION_COUNTER_OFFSET = CL * 8;
    static constexpr aeron::util::index_t CONSUMER_HEARTBEAT_OFFSET  = CL * 10;
    static constexpr aeron::util::index_t PRODUCER_CLOSED_OFFSET     = CL * 12;
    static constexpr aeron::util::index_t CONSUMER_CLOSED_OFFSET     = CL * 14;
    static constexpr aeron::util::index_t DETACH_OFFSET              = CL * 15;  // spare line (F8c fix2)
    static constexpr aeron::util::index_t TRAILER_LENGTH             = CL * 16;

    template <typename F>
    class ScopeExit {
    public:
        explicit ScopeExit(F&& f) noexcept : f_(std::move(f)) {}
        ~ScopeExit() noexcept { f_(); }
        ScopeExit(const ScopeExit&) = delete;
        ScopeExit& operator=(const ScopeExit&) = delete;
    private:
        F f_;
    };

    [[nodiscard]] std::int64_t* word64_(aeron::util::index_t off) const noexcept {
        return reinterpret_cast<std::int64_t*>(m_buffer.buffer() + off);
    }
    [[nodiscard]] std::int64_t loadAcquire64_(aeron::util::index_t off) const noexcept {
        return __atomic_load_n(word64_(off), __ATOMIC_ACQUIRE);
    }
    void storeRelease64_(aeron::util::index_t off, std::int64_t v) noexcept {
        __atomic_store_n(word64_(off), v, __ATOMIC_RELEASE);
    }

    static void checkCapacity(aeron::util::index_t capacity) {
        if (!aeron::util::BitUtil::isPowerOfTwo(capacity)) {
            throw aeron::util::IllegalArgumentException(
                "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=" +
                    std::to_string(capacity), SOURCEINFO);
        }
    }

    void checkMsgLength(aeron::util::index_t length) const {
        if (length < 0) {
            throw aeron::util::IllegalArgumentException(
                "invalid message length=" + std::to_string(length), SOURCEINFO);
        }
        if (length > m_maxMsgLength) {
            throw aeron::util::IllegalArgumentException(
                "encoded message exceeds maxMsgLength of " + std::to_string(m_maxMsgLength) +
                    " length=" + std::to_string(length), SOURCEINFO);
        }
    }

    aeron::concurrent::AtomicBuffer m_buffer;
    aeron::util::index_t m_capacity                  = 0;
    aeron::util::index_t m_maxMsgLength              = 0;
    aeron::util::index_t m_headPositionIndex         = 0;
    aeron::util::index_t m_headCachePositionIndex    = 0;
    aeron::util::index_t m_tailPositionIndex         = 0;
    aeron::util::index_t m_correlationIdCounterIndex = 0;
    aeron::util::index_t m_consumerHeartbeatIndex    = 0;
    aeron::util::index_t m_producerClosedIndex       = 0;
    aeron::util::index_t m_consumerClosedIndex       = 0;
    aeron::util::index_t m_detachIndex               = 0;
};

} // namespace rune::client::detail
