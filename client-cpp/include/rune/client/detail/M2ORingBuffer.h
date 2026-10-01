#pragma once
// M2ORingBuffer — internal multi-producer / single-consumer byte ring.
//
// A namespace-isolated copy of the broker's agrona M2O ring, retargeted onto
// Aeron's own AtomicBuffer + RecordDescriptor (already a client dependency) so it
// pulls in NO external "common" library.  It exists ONLY inside
// rune::client::detail and is never exposed across the client's public API.
//
// Why this instead of aeron::concurrent::ringbuffer::ManyToOneRingBuffer:
//   1. controlledRead() with ControlledAction::ABORT lets the consumer leave a
//      message UNCONSUMED (in-order backpressure) — Aeron's ring consumes on read
//      and has no abort, forcing a coarse "check-before-read" drain gate.
//   2. read()/controlledRead() are TEMPLATED (the handler is inlined) — Aeron's
//      read() dispatches every message through a std::function, a per-message
//      indirect call + potential heap alloc on the hot path.  Templating removes
//      that entirely (ULL).
//
// Layout + wire format are byte-identical to Aeron's ManyToOneRingBuffer, so
// write()/read() interoperate with the same RecordDescriptor framing.
//
// Threading: many producer threads may call tryClaim()/write(); exactly ONE
// consumer thread may call read()/controlledRead().

#include <climits>
#include <cstdint>
#include <string>

#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/RecordDescriptor.h>
#include <util/BitUtil.h>
#include <util/Index.h>
#include <util/Exceptions.h>

namespace rune::client::detail {

/// Consumer decision returned from a controlledRead() handler.
enum class ControlledAction {
    ABORT,    ///< leave THIS message (and the rest) unconsumed; stop.
    BREAK,    ///< consume up to and including this message; stop.
    COMMIT,   ///< consume everything so far immediately; continue.
    CONTINUE  ///< consume this message; continue.
};

class M2ORingBuffer {
public:
    /// buffer/length describe a region of (capacity == pow2) + TRAILER_LENGTH bytes.
    M2ORingBuffer(std::uint8_t* buffer, std::size_t length)
        : m_buffer(buffer, static_cast<aeron::util::index_t>(length)) {
        m_capacity = m_buffer.capacity() - TRAILER_LENGTH;
        checkCapacity(m_capacity);
        m_maxMsgLength              = m_capacity / 8;
        m_tailPositionIndex         = m_capacity + TAIL_POSITION_OFFSET;
        m_headCachePositionIndex    = m_capacity + HEAD_CACHE_POSITION_OFFSET;
        m_headPositionIndex         = m_capacity + HEAD_POSITION_OFFSET;
        m_correlationIdCounterIndex = m_capacity + CORRELATION_COUNTER_OFFSET;
        m_consumerHeartbeatIndex    = m_capacity + CONSUMER_HEARTBEAT_OFFSET;
    }

    M2ORingBuffer(const M2ORingBuffer&)            = delete;
    M2ORingBuffer& operator=(const M2ORingBuffer&) = delete;

    [[nodiscard]] aeron::util::index_t capacity()     const { return m_capacity; }
    [[nodiscard]] aeron::util::index_t maxMsgLength() const { return m_maxMsgLength; }

    /// Raw writable base pointer — combined with a tryClaim() offset for a
    /// zero-copy in-place write (producer writes the message directly into the
    /// claimed ring slot, then commit()s).  Advanced use; single producer per claim.
    [[nodiscard]] std::uint8_t* base() noexcept { return m_buffer.buffer(); }

    /// Total trailer bytes to add on top of the pow2 payload capacity.
    static constexpr aeron::util::index_t trailerLength() { return TRAILER_LENGTH; }

    /// Aeron ManyToOneRingBuffer::consumerHeartbeatTime: the single consumer
    /// publishes its liveness (release store) so producers can tell a slow
    /// consumer from a dead one — the ring primitive's own atomic, no other
    /// shared state (spec §1a.1).  Uses storeRelease64_/loadAcquire64_ (the
    /// __atomic builtins this class already uses for head/tail/length), NOT
    /// AtomicBuffer::putInt64Ordered/getInt64Volatile (plain volatile + asm
    /// barrier): the latter is invisible to ThreadSanitizer, which reported a
    /// data race between this store and the cross-thread load in
    /// ProducerImpl::sendBlocking even though the ordering was correct on
    /// x86-64 — same reasoning as the class-level comment below.
    void consumerHeartbeatTime(std::int64_t timeNs) noexcept {
        storeRelease64_(m_consumerHeartbeatIndex, timeNs);
    }
    [[nodiscard]] std::int64_t consumerHeartbeatTime() const noexcept {
        return loadAcquire64_(m_consumerHeartbeatIndex);
    }

    // ------------------------------------------------------------------
    // Producer side (multi-thread): claim/commit or the write() convenience.
    // ------------------------------------------------------------------

    /// Reserve space for a `length`-byte message; returns the encoded-message
    /// offset to write into, or a NEGATIVE value on insufficient capacity.
    aeron::util::index_t tryClaim(std::int32_t msgTypeId, std::size_t length) {
        namespace RD = aeron::concurrent::ringbuffer;
        RD::RecordDescriptor::checkMsgTypeId(msgTypeId);
        checkMsgLength(static_cast<aeron::util::index_t>(length));

        const aeron::util::index_t recordLength =
            static_cast<aeron::util::index_t>(length) + RD::RecordDescriptor::HEADER_LENGTH;
        const aeron::util::index_t recordIndex = claimCapacity(recordLength);
        if (INSUFFICIENT_CAPACITY == recordIndex) return recordIndex;

        storeRelease32_(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
        std::atomic_thread_fence(std::memory_order_release);
        m_buffer.putInt32(RD::RecordDescriptor::typeOffset(recordIndex), msgTypeId);
        return RD::RecordDescriptor::encodedMsgOffset(recordIndex);
    }

    /// Publish a message previously reserved with tryClaim().
    void commit(aeron::util::index_t index) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = computeRecordIndex(index);
        const std::int32_t recordLength = verifyClaimedSpaceNotReleased(recordIndex);
        storeRelease32_(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
    }

    /// Discard a message previously reserved with tryClaim() (becomes padding).
    void abort(aeron::util::index_t index) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = computeRecordIndex(index);
        const std::int32_t recordLength = verifyClaimedSpaceNotReleased(recordIndex);
        m_buffer.putInt32(RD::RecordDescriptor::typeOffset(recordIndex),
                          RD::RecordDescriptor::PADDING_MSG_TYPE_ID);
        storeRelease32_(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
    }

    /// Convenience: claim + copy + commit in one call (matches Aeron's write()).
    /// Returns false on insufficient capacity (caller applies back-pressure).
    bool write(std::int32_t msgTypeId, const aeron::concurrent::AtomicBuffer& src,
               aeron::util::index_t srcIndex, aeron::util::index_t length) {
        const aeron::util::index_t claimIndex =
            tryClaim(msgTypeId, static_cast<std::size_t>(length));
        if (claimIndex < 0) return false;
        m_buffer.putBytes(claimIndex, src, srcIndex, length);
        commit(claimIndex);
        return true;
    }

    // ------------------------------------------------------------------
    // Consumer side (single thread): templated read / controlledRead.
    // ------------------------------------------------------------------

    /// Drain up to messageCountLimit messages; handler is
    /// (msgTypeId, AtomicBuffer&, encodedOffset, length).  Returns messages read.
    template <typename Handler>
    int read(Handler&& handler, int messageCountLimit) {
        namespace RD = aeron::concurrent::ringbuffer;
        int messagesRead = 0;

        const aeron::util::index_t headPositionIndex = m_headPositionIndex;
        const std::int64_t head = m_buffer.getInt64(headPositionIndex);
        const aeron::util::index_t capacity = m_capacity;
        const aeron::util::index_t headIndex = static_cast<std::int32_t>(head) & (capacity - 1);
        const aeron::util::index_t maxBlockLength = capacity - headIndex;
        aeron::util::index_t bytesRead = 0;

        ScopeExit cleanup([&]() {
            if (bytesRead != 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
            }
        });

        while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
            const aeron::util::index_t recordIndex = headIndex + bytesRead;
            const std::int32_t recordLength =
                loadAcquire32_(RD::RecordDescriptor::lengthOffset(recordIndex));
            if (recordLength <= 0) break;

            bytesRead += aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);

            const std::int32_t msgTypeId =
                m_buffer.getInt32(RD::RecordDescriptor::typeOffset(recordIndex));
            if (RD::RecordDescriptor::PADDING_MSG_TYPE_ID == msgTypeId) continue;

            handler(msgTypeId, m_buffer,
                    RD::RecordDescriptor::encodedMsgOffset(recordIndex),
                    recordLength - RD::RecordDescriptor::HEADER_LENGTH);
            ++messagesRead;
        }
        return messagesRead;
    }

    template <typename Handler>
    int read(Handler&& handler) { return read(std::forward<Handler>(handler), INT_MAX); }

    /// Like read(), but the handler returns a ControlledAction; ABORT leaves the
    /// message (and remainder) unconsumed for a later cycle (in-order backpressure).
    template <typename Handler>
    int controlledRead(Handler&& handler, int messageCountLimit = INT_MAX) {
        namespace RD = aeron::concurrent::ringbuffer;
        int messagesRead = 0;

        const aeron::util::index_t headPositionIndex = m_headPositionIndex;
        std::int64_t head = m_buffer.getInt64(headPositionIndex);
        const aeron::util::index_t capacity = m_capacity;
        aeron::util::index_t headIndex = static_cast<std::int32_t>(head) & (capacity - 1);
        const aeron::util::index_t maxBlockLength = capacity - headIndex;
        aeron::util::index_t bytesRead = 0;

        ScopeExit cleanup([&]() {
            if (bytesRead > 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
            }
        });

        while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
            const aeron::util::index_t recordIndex = headIndex + bytesRead;
            const std::int32_t recordLength =
                loadAcquire32_(RD::RecordDescriptor::lengthOffset(recordIndex));
            if (recordLength <= 0) break;

            const aeron::util::index_t alignedLength =
                aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);
            bytesRead += alignedLength;

            const std::int32_t msgTypeId =
                m_buffer.getInt32(RD::RecordDescriptor::typeOffset(recordIndex));
            if (RD::RecordDescriptor::PADDING_MSG_TYPE_ID == msgTypeId) continue;

            const ControlledAction action =
                handler(msgTypeId, m_buffer,
                        RD::RecordDescriptor::encodedMsgOffset(recordIndex),
                        recordLength - RD::RecordDescriptor::HEADER_LENGTH);

            if (action == ControlledAction::ABORT) { bytesRead -= alignedLength; break; }
            ++messagesRead;
            if (action == ControlledAction::BREAK) break;
            if (action == ControlledAction::COMMIT) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
                headIndex += bytesRead;
                head      += bytesRead;
                bytesRead = 0;
            }
        }
        return messagesRead;
    }

private:
    // Trailer layout — byte-identical to Aeron's RingBufferDescriptor.
    static constexpr aeron::util::index_t CL = aeron::util::BitUtil::CACHE_LINE_LENGTH;
    static constexpr aeron::util::index_t TAIL_POSITION_OFFSET       = CL * 2;
    static constexpr aeron::util::index_t HEAD_CACHE_POSITION_OFFSET = CL * 4;
    static constexpr aeron::util::index_t HEAD_POSITION_OFFSET       = CL * 6;
    static constexpr aeron::util::index_t CORRELATION_COUNTER_OFFSET = CL * 8;
    static constexpr aeron::util::index_t CONSUMER_HEARTBEAT_OFFSET  = CL * 10;
    static constexpr aeron::util::index_t TRAILER_LENGTH             = CL * 12;
    static constexpr aeron::util::index_t INSUFFICIENT_CAPACITY      = -2;

    // Minimal template-based scope guard (no std::function, no heap alloc).
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

    // Synchronisation words (record length, head, head cache, tail) use C++
    // __atomic builtins rather than Aeron's x86-64 accessors (plain volatile +
    // asm compiler barriers).  Same machine code on x86-64 (a release store and
    // an acquire load are plain MOVs; the CAS is LOCK CMPXCHG), correct on weakly
    // ordered CPUs by construction, and visible to ThreadSanitizer — which could
    // not see the publication edge, so every message handed through this ring
    // (and everything it points to) was reported as a data race.
    [[nodiscard]] std::int32_t* word32_(aeron::util::index_t off) const noexcept {
        return reinterpret_cast<std::int32_t*>(m_buffer.buffer() + off);
    }
    [[nodiscard]] std::int64_t* word64_(aeron::util::index_t off) const noexcept {
        return reinterpret_cast<std::int64_t*>(m_buffer.buffer() + off);
    }
    [[nodiscard]] std::int32_t loadAcquire32_(aeron::util::index_t off) const noexcept {
        return __atomic_load_n(word32_(off), __ATOMIC_ACQUIRE);
    }
    void storeRelease32_(aeron::util::index_t off, std::int32_t v) noexcept {
        __atomic_store_n(word32_(off), v, __ATOMIC_RELEASE);
    }
    [[nodiscard]] std::int64_t loadAcquire64_(aeron::util::index_t off) const noexcept {
        return __atomic_load_n(word64_(off), __ATOMIC_ACQUIRE);
    }
    void storeRelease64_(aeron::util::index_t off, std::int64_t v) noexcept {
        __atomic_store_n(word64_(off), v, __ATOMIC_RELEASE);
    }
    bool compareAndSet64_(aeron::util::index_t off, std::int64_t expected,
                          std::int64_t desired) noexcept {
        return __atomic_compare_exchange_n(word64_(off), &expected, desired, /*weak=*/false,
                                           __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
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

    [[nodiscard]] aeron::util::index_t computeRecordIndex(aeron::util::index_t index) const {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = index - RD::RecordDescriptor::HEADER_LENGTH;
        if (recordIndex < 0 ||
            recordIndex > (m_capacity - RD::RecordDescriptor::HEADER_LENGTH)) {
            throw aeron::util::IllegalArgumentException(
                "Invalid message index: " + std::to_string(index), SOURCEINFO);
        }
        return recordIndex;
    }

    [[nodiscard]] std::int32_t verifyClaimedSpaceNotReleased(aeron::util::index_t recordIndex) const {
        namespace RD = aeron::concurrent::ringbuffer;
        const std::int32_t recordLength =
            m_buffer.getInt32(RD::RecordDescriptor::lengthOffset(recordIndex));
        if (recordLength < 0) return recordLength;
        throw aeron::util::IllegalStateException("Claimed space previously released", SOURCEINFO);
    }

    aeron::util::index_t claimCapacity(aeron::util::index_t recordLength) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t requiredCapacity =
            aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);
        const aeron::util::index_t capacity = m_capacity;
        const aeron::util::index_t tailPositionIndex = m_tailPositionIndex;
        const aeron::util::index_t headCachePositionIndex = m_headCachePositionIndex;
        const aeron::util::index_t mask = capacity - 1;

        std::int64_t head = loadAcquire64_(headCachePositionIndex);
        std::int64_t tail, newTail;
        aeron::util::index_t tailIndex, padding, writeIndex;

        do {
            tail = loadAcquire64_(tailPositionIndex);
            const aeron::util::index_t availableCapacity =
                capacity - static_cast<aeron::util::index_t>(tail - head);

            if (requiredCapacity > availableCapacity) {
                head = loadAcquire64_(m_headPositionIndex);
                if (requiredCapacity > (capacity - static_cast<aeron::util::index_t>(tail - head))) {
                    return INSUFFICIENT_CAPACITY;
                }
                storeRelease64_(headCachePositionIndex, head);
            }

            newTail   = tail + requiredCapacity;
            padding   = 0;
            tailIndex = static_cast<aeron::util::index_t>(tail) & mask;
            writeIndex = tailIndex;
            const aeron::util::index_t toBufferEndLength = capacity - tailIndex;

            if (requiredCapacity > toBufferEndLength) {
                aeron::util::index_t headIndex = static_cast<aeron::util::index_t>(head) & mask;
                writeIndex = 0;
                if (requiredCapacity > headIndex) {
                    head = loadAcquire64_(m_headPositionIndex);
                    headIndex = static_cast<aeron::util::index_t>(head) & mask;
                    if (requiredCapacity > headIndex) {
                        writeIndex = INSUFFICIENT_CAPACITY;
                        newTail = tail; // only pad to the buffer end; claim no space
                    }
                    storeRelease64_(headCachePositionIndex, head);
                }
                padding = toBufferEndLength;
                newTail += padding;
            }
        } while (!compareAndSet64_(tailPositionIndex, tail, newTail));

        if (0 != padding) {
            storeRelease32_(RD::RecordDescriptor::lengthOffset(tailIndex), -padding);
            std::atomic_thread_fence(std::memory_order_release);
            m_buffer.putInt32(RD::RecordDescriptor::typeOffset(tailIndex),
                              RD::RecordDescriptor::PADDING_MSG_TYPE_ID);
            storeRelease32_(RD::RecordDescriptor::lengthOffset(tailIndex), padding);
        }
        return writeIndex;
    }

    aeron::concurrent::AtomicBuffer m_buffer;
    aeron::util::index_t m_capacity                 = 0;
    aeron::util::index_t m_maxMsgLength             = 0;
    aeron::util::index_t m_headPositionIndex        = 0;
    aeron::util::index_t m_headCachePositionIndex   = 0;
    aeron::util::index_t m_tailPositionIndex        = 0;
    aeron::util::index_t m_correlationIdCounterIndex = 0;
    aeron::util::index_t m_consumerHeartbeatIndex   = 0;
};

} // namespace rune::client::detail
