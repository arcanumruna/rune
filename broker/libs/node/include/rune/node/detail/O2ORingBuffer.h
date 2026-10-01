#pragma once
// O2ORingBuffer — internal single-producer / single-consumer byte ring.
//
// A namespace-isolated copy of common's agrona O2O ring, retargeted onto Aeron's
// own AtomicBuffer + RecordDescriptor (already a broker dependency) so it pulls in
// NO external "common" library.  It lives ONLY inside rune::node::detail.
//
// Why this instead of aeron::concurrent::ringbuffer::OneToOneRingBuffer:
//   1. tryClaim()/commit()/abort() let the producer encode a message DIRECTLY into
//      the claimed ring slot (ZERO extra copy) and learn immediately whether it was
//      back-pressured (claim < 0) — Aeron's OneToOneRingBuffer exposes only
//      write(msgTypeId, src, ...), which forces a second scratch→ring copy and
//      returns only a bool.
//   2. read()/controlledRead() are TEMPLATED (the handler is inlined) — Aeron's
//      read() dispatches every message through a std::function (per-message indirect
//      call + potential heap alloc on the hot path).  Templating removes that (ULL).
//   3. controlledRead()'s ControlledAction::ABORT leaves a message UNCONSUMED for a
//      later cycle (in-order back-pressure) — Aeron's read consumes on read.
//
// Layout + wire format are byte-identical to Aeron's OneToOneRingBuffer, so the
// RecordDescriptor framing interoperates.
//
// Threading: EXACTLY ONE producer thread may call tryClaim()/write(); EXACTLY ONE
// consumer thread may call read()/controlledRead().  claimCapacity() is
// single-producer (no CAS — the producer owns the tail).

#include <climits>
#include <cstdint>
#include <string>

#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/RecordDescriptor.h>
#include <util/BitUtil.h>
#include <util/Index.h>
#include <util/Exceptions.h>
#include <rune/consensus/detail/TsanAnnotations.h>

namespace rune::node::detail {

/// Consumer decision returned from a controlledRead() handler.
enum class ControlledAction {
    ABORT,    ///< leave THIS message (and the rest) unconsumed; stop.
    BREAK,    ///< consume up to and including this message; stop.
    COMMIT,   ///< consume everything so far immediately; continue.
    CONTINUE  ///< consume this message; continue.
};

class O2ORingBuffer {
public:
    /// buffer/length describe a region of (capacity == pow2) + TRAILER_LENGTH bytes.
    O2ORingBuffer(std::uint8_t* buffer, std::size_t length)
        : m_buffer(buffer, static_cast<aeron::util::index_t>(length)) {
        m_capacity = m_buffer.capacity() - TRAILER_LENGTH;
        checkCapacity(m_capacity);
        m_maxMsgLength           = m_capacity / 8;
        m_tailPositionIndex      = m_capacity + TAIL_POSITION_OFFSET;
        m_headCachePositionIndex = m_capacity + HEAD_CACHE_POSITION_OFFSET;
        m_headPositionIndex      = m_capacity + HEAD_POSITION_OFFSET;
    }

    O2ORingBuffer(const O2ORingBuffer&)            = delete;
    O2ORingBuffer& operator=(const O2ORingBuffer&) = delete;

    [[nodiscard]] aeron::util::index_t capacity()     const { return m_capacity; }
    [[nodiscard]] aeron::util::index_t maxMsgLength() const { return m_maxMsgLength; }

    /// Raw writable base pointer — combine with a tryClaim() offset for a zero-copy
    /// in-place write (encode the message directly into the claimed slot, commit()).
    [[nodiscard]] std::uint8_t* base() noexcept { return m_buffer.buffer(); }

    /// Total trailer bytes to add on top of the pow2 payload capacity.
    static constexpr aeron::util::index_t trailerLength() { return TRAILER_LENGTH; }

    // ------------------------------------------------------------------
    // Producer side (single thread): claim/commit or the write() convenience.
    // ------------------------------------------------------------------

    /// Reserve space for a `length`-byte message; returns the encoded-message offset
    /// to write into (relative to base()), or a NEGATIVE value on insufficient
    /// capacity (back-pressure — the caller must retry later).
    aeron::util::index_t tryClaim(std::int32_t msgTypeId, std::size_t length) {
        namespace RD = aeron::concurrent::ringbuffer;
        RD::RecordDescriptor::checkMsgTypeId(msgTypeId);
        checkMsgLength(static_cast<aeron::util::index_t>(length));

        const aeron::util::index_t recordLength =
            static_cast<aeron::util::index_t>(length) + RD::RecordDescriptor::HEADER_LENGTH;
        const aeron::util::index_t recordIndex = claimCapacity(recordLength);
        if (INSUFFICIENT_CAPACITY == recordIndex) return recordIndex;

        m_buffer.putInt32Ordered(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
        std::atomic_thread_fence(std::memory_order_release);
        m_buffer.putInt32(RD::RecordDescriptor::typeOffset(recordIndex), msgTypeId);
        return RD::RecordDescriptor::encodedMsgOffset(recordIndex);
    }

    /// Publish a message previously reserved with tryClaim().
    void commit(aeron::util::index_t index) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = computeRecordIndex(index);
        const std::int32_t recordLength = verifyClaimedSpaceNotReleased(recordIndex);
        RUNE_TSAN_RELEASE(m_buffer.buffer() + recordIndex);
        m_buffer.putInt32Ordered(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
    }

    /// Discard a message previously reserved with tryClaim() (becomes padding).
    void abort(aeron::util::index_t index) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = computeRecordIndex(index);
        const std::int32_t recordLength = verifyClaimedSpaceNotReleased(recordIndex);
        RUNE_TSAN_RELEASE(m_buffer.buffer() + recordIndex);
        m_buffer.putInt32(RD::RecordDescriptor::typeOffset(recordIndex),
                          RD::RecordDescriptor::PADDING_MSG_TYPE_ID);
        m_buffer.putInt32Ordered(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
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
                RUNE_TSAN_RELEASE(m_buffer.buffer() + headPositionIndex);
                m_buffer.putInt64Ordered(headPositionIndex, head + bytesRead);
            }
        });

        while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
            const aeron::util::index_t recordIndex = headIndex + bytesRead;
            const std::int32_t recordLength =
                m_buffer.getInt32Volatile(RD::RecordDescriptor::lengthOffset(recordIndex));
            if (recordLength <= 0) break;
            RUNE_TSAN_ACQUIRE(m_buffer.buffer() + recordIndex);

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
    /// message (and remainder) unconsumed for a later cycle (in-order back-pressure).
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
                RUNE_TSAN_RELEASE(m_buffer.buffer() + headPositionIndex);
                m_buffer.putInt64Ordered(headPositionIndex, head + bytesRead);
            }
        });

        while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
            const aeron::util::index_t recordIndex = headIndex + bytesRead;
            const std::int32_t recordLength =
                m_buffer.getInt32Volatile(RD::RecordDescriptor::lengthOffset(recordIndex));
            if (recordLength <= 0) break;
            RUNE_TSAN_ACQUIRE(m_buffer.buffer() + recordIndex);

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
                RUNE_TSAN_RELEASE(m_buffer.buffer() + headPositionIndex);
                m_buffer.putInt64Ordered(headPositionIndex, head + bytesRead);
                headIndex += bytesRead;
                head      += bytesRead;
                bytesRead = 0;
            }
        }
        return messagesRead;
    }

    /// Bytes currently unconsumed in the ring (tail - head), clamped to [0, capacity].
    [[nodiscard]] aeron::util::index_t size() {
        const std::int64_t tail = m_buffer.getInt64Volatile(m_tailPositionIndex);
        const std::int64_t head = m_buffer.getInt64Volatile(m_headPositionIndex);
        const std::int64_t sz = tail - head;
        if (sz < 0) return 0;
        if (sz > m_capacity) return m_capacity;
        return static_cast<aeron::util::index_t>(sz);
    }

private:
    // Trailer layout — byte-identical to Aeron's RingBufferDescriptor.
    static constexpr aeron::util::index_t CL = aeron::util::BitUtil::CACHE_LINE_LENGTH;
    static constexpr aeron::util::index_t TAIL_POSITION_OFFSET       = CL * 2;
    static constexpr aeron::util::index_t HEAD_CACHE_POSITION_OFFSET = CL * 4;
    static constexpr aeron::util::index_t HEAD_POSITION_OFFSET       = CL * 6;
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

    // Single-producer claim (no CAS — the producer owns the tail).  Ported from
    // common's O2ORingBuffer::claimCapacity, retargeted to Aeron's AtomicBuffer.
    aeron::util::index_t claimCapacity(aeron::util::index_t recordLength) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t alignedRecordLength =
            aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);
        const aeron::util::index_t requiredCapacity =
            alignedRecordLength + RD::RecordDescriptor::HEADER_LENGTH;
        const aeron::util::index_t capacity = m_capacity;
        const aeron::util::index_t tailPositionIndex = m_tailPositionIndex;
        const aeron::util::index_t headCachePositionIndex = m_headCachePositionIndex;
        const aeron::util::index_t mask = capacity - 1;

        std::int64_t head = m_buffer.getInt64(headCachePositionIndex);
        const std::int64_t tail = m_buffer.getInt64(tailPositionIndex);
        const aeron::util::index_t availableCapacity =
            capacity - static_cast<aeron::util::index_t>(tail - head);

        if (requiredCapacity > availableCapacity) {
            head = m_buffer.getInt64Volatile(m_headPositionIndex);
            RUNE_TSAN_ACQUIRE(m_buffer.buffer() + m_headPositionIndex);
            if (requiredCapacity > (capacity - static_cast<aeron::util::index_t>(tail - head))) {
                return INSUFFICIENT_CAPACITY;
            }
            m_buffer.putInt64(headCachePositionIndex, head);
        }

        aeron::util::index_t padding = 0;
        const aeron::util::index_t recordIndex = static_cast<aeron::util::index_t>(tail) & mask;
        const aeron::util::index_t toBufferEndLength = capacity - recordIndex;
        aeron::util::index_t writeIndex = recordIndex;
        std::int64_t nextTail = tail + alignedRecordLength;

        if (alignedRecordLength == toBufferEndLength) {
            m_buffer.putInt64Ordered(tailPositionIndex, nextTail);
            m_buffer.putInt64(0, 0L); // pre-zero the wrapped next-message header
            return recordIndex;
        } else if (requiredCapacity > toBufferEndLength) {
            writeIndex = 0;
            aeron::util::index_t headIndex = static_cast<aeron::util::index_t>(head) & mask;
            if (requiredCapacity > headIndex) {
                head = m_buffer.getInt64Volatile(m_headPositionIndex);
                RUNE_TSAN_ACQUIRE(m_buffer.buffer() + m_headPositionIndex);
                headIndex = static_cast<aeron::util::index_t>(head) & mask;
                if (requiredCapacity > headIndex) {
                    writeIndex = INSUFFICIENT_CAPACITY;
                    nextTail = tail; // claim no space; only pad to the buffer end
                }
                m_buffer.putInt64(headCachePositionIndex, head);
            }
            padding = toBufferEndLength;
            nextTail += padding;
        }

        m_buffer.putInt64Ordered(tailPositionIndex, nextTail);

        if (0 != padding) {
            m_buffer.putInt64(0, 0L);
            m_buffer.putInt32Ordered(RD::RecordDescriptor::lengthOffset(recordIndex), -padding);
            std::atomic_thread_fence(std::memory_order_release);
            m_buffer.putInt32(RD::RecordDescriptor::typeOffset(recordIndex),
                              RD::RecordDescriptor::PADDING_MSG_TYPE_ID);
            RUNE_TSAN_RELEASE(m_buffer.buffer() + recordIndex);
            m_buffer.putInt32Ordered(RD::RecordDescriptor::lengthOffset(recordIndex), padding);
        }

        if (INSUFFICIENT_CAPACITY != writeIndex) {
            m_buffer.putInt64(writeIndex + alignedRecordLength, 0L); // pre-zero next header
        }

        return writeIndex;
    }

    aeron::concurrent::AtomicBuffer m_buffer;
    aeron::util::index_t m_capacity               = 0;
    aeron::util::index_t m_maxMsgLength           = 0;
    aeron::util::index_t m_headPositionIndex      = 0;
    aeron::util::index_t m_headCachePositionIndex = 0;
    aeron::util::index_t m_tailPositionIndex      = 0;
};

} // namespace rune::node::detail
