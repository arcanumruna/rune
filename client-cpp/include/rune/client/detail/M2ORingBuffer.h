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
// Record framing (length/type headers, alignment, padding) is byte-identical
// to Aeron's ManyToOneRingBuffer, so write()/read() interoperate with the same
// RecordDescriptor framing, and the TRAILER layout (tail, head cache, head,
// correlation counter, consumer heartbeat — CL*0..CL*12) is Aeron's
// RingBufferDescriptor exactly.
//
// Rune extension (F8c C1(a), user-approved deviation from Aeron's
// ManyToOneRingBuffer, which has no close): a LINEARIZABLE close.  The closed
// flag is the top (sign) bit of the tail position word — the word tryClaim()
// already CASes — so every claim is totally ordered with the close: a claim
// whose CAS precedes the close is drained by the consumer (even if it commits
// afterwards), a claim after it fails with kClaimClosed, and the tail never
// moves again.  The claim hot path pays one sign test on the tail value it has
// already loaded.  (The previous separate `closed` trailer word could not
// order a writer that checked it before the close but claimed after.)
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
        m_consumerFinishedIndex     = m_capacity + CONSUMER_FINISHED_OFFSET;
        m_detachIndex               = m_capacity + DETACH_OFFSET;
        m_orphanHintIndex           = m_capacity + ORPHAN_HINT_OFFSET;
    }

    M2ORingBuffer(const M2ORingBuffer&)            = delete;
    M2ORingBuffer& operator=(const M2ORingBuffer&) = delete;

    [[nodiscard]] aeron::util::index_t capacity()     const { return m_capacity; }
    [[nodiscard]] aeron::util::index_t maxMsgLength() const { return m_maxMsgLength; }

    /// fix3 I1: read-only views of the ring's own head/tail position counters
    /// (acquire) — ring-protocol words that already exist for tryClaim()/
    /// read()'s own bookkeeping, exposed so a close-time drain can tell "ring
    /// truly empty" (head caught up to a snapshotted tail) apart from "stuck
    /// behind a claimed-but-not-yet-committed record" (head has not caught up,
    /// even though read() currently has nothing it can consume). No new shared
    /// state is introduced — shared-nothing is preserved.
    [[nodiscard]] std::int64_t headPosition() const noexcept {
        return loadAcquire64_(m_headPositionIndex);
    }
    /// The tail POSITION (the closed bit masked off — see close()).
    [[nodiscard]] std::int64_t tailPosition() const noexcept {
        return loadAcquire64_(m_tailPositionIndex) & TAIL_POSITION_MASK;
    }

    /// Raw writable base pointer — combined with a tryClaim() offset for a
    /// zero-copy in-place write (producer writes the message directly into the
    /// claimed ring slot, then commit()s).  Advanced use; single producer per claim.
    [[nodiscard]] std::uint8_t* base() noexcept { return m_buffer.buffer(); }

    /// Total trailer bytes to add on top of the pow2 payload capacity.
    static constexpr aeron::util::index_t trailerLength() { return TRAILER_LENGTH; }

    /// Aeron ManyToOneRingBuffer::consumerHeartbeatTime: the single consumer
    /// publishes its liveness so producers can tell a slow consumer from a dead
    /// one — a backstop ONLY for a wedged runner (fix1 I5: a cleanly closed
    /// Client is decided by closed(), below, never by this timer).  It publishes
    /// nothing else (no other memory is ordered against it), so both sides use a
    /// RELAXED __atomic op (fix1 minor 9) — still visible to ThreadSanitizer,
    /// unlike AtomicBuffer::putInt64Ordered/getInt64Volatile (plain volatile +
    /// asm barrier), which is invisible to it (see the class-level comment
    /// below) and caused a reported data race against the correct ordering.
    void consumerHeartbeatTime(std::int64_t timeNs) noexcept {
        storeRelaxed64_(m_consumerHeartbeatIndex, timeNs);
    }
    [[nodiscard]] std::int64_t consumerHeartbeatTime() const noexcept {
        return loadRelaxed64_(m_consumerHeartbeatIndex);
    }

    /// F8c fix round 1 (I2): the consumer will never read this ring again
    /// (release).  Set after a close drain finished — even an ABANDONED one
    /// (fix5): a producer still waiting for the outcome of a record it
    /// committed observes this (acquire) and knows the record will never be
    /// read.  A Rune extension in the trailer's spare cache line (the second
    /// line of Aeron's padded heartbeat slot); the trailer length is unchanged.
    void consumerFinish() noexcept { storeRelease64_(m_consumerFinishedIndex, 1); }
    [[nodiscard]] bool consumerFinished() const noexcept {
        return loadAcquire64_(m_consumerFinishedIndex) != 0;
    }

    /// F8c C7: last-one-frees ownership hand-off of the OBJECT that owns this
    /// ring (a ProducerImpl) between its two holders — the consumer side (the
    /// duty registry) and the producer side (the app-thread handle).  Each side
    /// detaches exactly once with ONE RMW on one word; the call that finds the
    /// OTHER side already detached returns true and its caller frees the
    /// owner.  Exactly one of the two calls returns true; nobody waits.  The
    /// same handshake as OneToOneRingBuffer::detachProducer/detachConsumer
    /// (F8c fix round 2, I5), in the trailer's spare line after the
    /// correlation counter (Aeron pads it; nothing else uses it).
    bool detachConsumer() noexcept {
        return (__atomic_fetch_or(word64_(m_detachIndex), kDetachedConsumer, __ATOMIC_ACQ_REL) &
                kDetachedProducer) != 0;
    }
    bool detachProducer() noexcept {
        return (__atomic_fetch_or(word64_(m_detachIndex), kDetachedProducer, __ATOMIC_ACQ_REL) &
                kDetachedConsumer) != 0;
    }
    /// Either side (acquire): has the consumer side (the duty registry)
    /// detached?  Once true, the consumer never touches the owner again.
    [[nodiscard]] bool consumerDetached() const noexcept {
        return (loadAcquire64_(m_detachIndex) & kDetachedConsumer) != 0;
    }
    /// Either side (acquire): has the producer side (the handle) detached?
    [[nodiscard]] bool producerDetached() const noexcept {
        return (loadAcquire64_(m_detachIndex) & kDetachedProducer) != 0;
    }
    /// F8c C7 fix round 2 (F1): the producer side (the handle) detaches AND
    /// marks its release as ORPHANED (it could not be put on the command ring:
    /// full) in the same RMW — the duty side discovers it (orphanHint below)
    /// and applies the release itself.  Returns true iff the consumer side had
    /// already detached (the caller frees the owner).
    bool detachProducerOrphaned() noexcept {
        return (__atomic_fetch_or(word64_(m_detachIndex), kDetachedProducer | kOrphanRelease,
                                  __ATOMIC_ACQ_REL) & kDetachedConsumer) != 0;
    }
    [[nodiscard]] bool producerOrphaned() const noexcept {
        return (loadAcquire64_(m_detachIndex) & kOrphanRelease) != 0;
    }

    /// F8c C7 fix round 2 (F1), on a COMMAND ring: "an orphaned handle release
    /// exists — scan for it".  Any thread raises it (release) AFTER marking its
    /// impl orphaned; the consumer takes it (one acquire load per duty cycle;
    /// the RMW only when raised), then scans.  A raise racing the take is
    /// either seen by this scan or leaves the hint up for the next one.
    void raiseOrphanHint() noexcept { __atomic_store_n(word64_(m_orphanHintIndex), 1, __ATOMIC_RELEASE); }
    [[nodiscard]] bool takeOrphanHint() noexcept {
        if (loadAcquire64_(m_orphanHintIndex) == 0) [[likely]] return false;
        return __atomic_exchange_n(word64_(m_orphanHintIndex), 0, __ATOMIC_ACQ_REL) != 0;
    }

    /// Test seam (fix5 / F8c fix round 1): turn a claim returned by tryClaim()
    /// into a HEADER-LESS claim (length + type words zeroed) — the state of a
    /// writer that won the tail CAS but never wrote its record header.  Uses
    /// the ring's own release stores, so a concurrently polling consumer
    /// (TSan) sees a well-defined value.
    void testZeroClaimHeader(aeron::util::index_t claimIndex) noexcept {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = claimIndex - RD::RecordDescriptor::HEADER_LENGTH;
        storeRelease32_(RD::RecordDescriptor::typeOffset(recordIndex), 0);
        storeRelease32_(RD::RecordDescriptor::lengthOffset(recordIndex), 0);
    }

    /// tryClaim() result when the ring is closed (INSUFFICIENT_CAPACITY is -2).
    static constexpr aeron::util::index_t kClaimClosed = -3;

    /// F8c C1(a): close the ring to producers — LINEARIZABLE with tryClaim().
    /// Sets the closed bit in the tail word with one RMW (fetch_or, seq_cst),
    /// which is totally ordered with every claim's tail CAS: a claim whose CAS
    /// came first is a normal record (the consumer drains it — a close drain
    /// reads up to tailPosition(), waiting past claims not yet committed), and
    /// every claim after it returns kClaimClosed.  The tail position is frozen
    /// from here on.  Idempotent; any thread (a producer, the consumer, or a
    /// closer).  Returns true iff THIS call closed the ring.
    ///
    /// (fix1 C2/I3 use: Client::closeImpl_ closes each producer ring while the
    /// Client is still fully valid, so a Producer that outlives its Client gets
    /// SendResult::Closed and never touches the by-then-invalid pump hook.)
    bool close() noexcept {
        const std::int64_t prev =
            __atomic_fetch_or(word64_(m_tailPositionIndex), TAIL_CLOSED_BIT, __ATOMIC_SEQ_CST);
        return (prev & TAIL_CLOSED_BIT) == 0;
    }
    [[nodiscard]] bool closed() const noexcept {
        return (loadAcquire64_(m_tailPositionIndex) & TAIL_CLOSED_BIT) != 0;
    }

    // ------------------------------------------------------------------
    // Producer side (multi-thread): claim/commit or the write() convenience.
    // ------------------------------------------------------------------

    /// Reserve space for a `length`-byte message; returns the encoded-message
    /// offset to write into, or a NEGATIVE value: INSUFFICIENT_CAPACITY (-2,
    /// back-pressure) or kClaimClosed (-3, the ring is closed — see close()).
    aeron::util::index_t tryClaim(std::int32_t msgTypeId, std::size_t length) {
        namespace RD = aeron::concurrent::ringbuffer;
        RD::RecordDescriptor::checkMsgTypeId(msgTypeId);
        checkMsgLength(static_cast<aeron::util::index_t>(length));

        const aeron::util::index_t recordLength =
            static_cast<aeron::util::index_t>(length) + RD::RecordDescriptor::HEADER_LENGTH;
        const aeron::util::index_t recordIndex = claimCapacity(recordLength);
        if (recordIndex < 0) return recordIndex;  // INSUFFICIENT_CAPACITY / kClaimClosed

        storeRelease32_(RD::RecordDescriptor::lengthOffset(recordIndex), -recordLength);
        std::atomic_thread_fence(std::memory_order_release);
        m_buffer.putInt32(RD::RecordDescriptor::typeOffset(recordIndex), msgTypeId);
        return RD::RecordDescriptor::encodedMsgOffset(recordIndex);
    }

    /// Publish a message previously reserved with tryClaim().  Throws
    /// IllegalStateException when the claim was released under the writer
    /// (unblock() turned it into padding): nothing is published.
    ///
    /// The publish is ONE compare-exchange on the record's length word
    /// (-len -> +len), not a read-verify then a plain store: an unblock() that
    /// lands between the two steps used to be overwritten by an equal store and
    /// commit() reported success for a record that was already padding (a
    /// silently lost record).  With the CAS exactly one of {commit, unblock}
    /// flips the word; the loser sees it and commit() throws.
    void commit(aeron::util::index_t index) { commitWith(index, [] {}); }

    /// commit() with a test seam run between the verify and the publish step
    /// (deterministic interleaving of unblock() against a commit).
    template <typename Between>
    void commitWith(aeron::util::index_t index, Between&& between) {
        namespace RD = aeron::concurrent::ringbuffer;
        const aeron::util::index_t recordIndex = computeRecordIndex(index);
        std::int32_t recordLength = verifyClaimedSpaceNotReleased(recordIndex);
        between();
        if (!__atomic_compare_exchange_n(word32_(RD::RecordDescriptor::lengthOffset(recordIndex)), &recordLength,
                                         -recordLength, false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
            throw aeron::util::IllegalStateException("Claimed space previously released", SOURCEINFO);
        }
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
        return writeWith(msgTypeId, length,
                         [&](aeron::concurrent::AtomicBuffer& dst, aeron::util::index_t at) {
                             dst.putBytes(at, src, srcIndex, length);
                         });
    }

    /// fix5 Important-2(a): claim + fill + commit with the tryClaim()/commit()/
    /// abort() contract Aeron documents for its claim API (Java
    /// ManyToOneRingBuffer.tryClaim: "the claimed space MUST be committed or
    /// aborted"): once tryClaim() succeeds, the claim is ALWAYS resolved — if
    /// `fill(buffer, encodedOffset)` throws (e.g. AtomicBuffer::putBytes's
    /// bounds check), the claim is abort()ed (turned into padding the consumer
    /// skips) BEFORE the exception propagates, so a throw can never leave an
    /// orphaned claim that blocks the single consumer forever.
    ///
    /// commit() itself throws only when the claimed space was already released
    /// (verifyClaimedSpaceNotReleased — i.e. unblock() converted it to padding
    /// while this writer stalled): nothing is orphaned in that case (the
    /// consumer already skips it), and abort() would throw the same error, so
    /// that exception propagates unchanged.
    ///
    /// Templated (`fill` is inlined): no std::function, no allocation.
    template <typename Fill>
    bool writeWith(std::int32_t msgTypeId, aeron::util::index_t length, Fill&& fill) {
        const aeron::util::index_t claimIndex =
            tryClaim(msgTypeId, static_cast<std::size_t>(length));
        if (claimIndex < 0) return false;
        try {
            fill(m_buffer, claimIndex);
        } catch (...) {
            abort(claimIndex);
            throw;
        }
        commit(claimIndex);
        return true;
    }

    /// fix5 Important-2(b): faithful port of Aeron's
    /// ManyToOneRingBuffer::unblock() (Java agrona / C++ aeron-client). Called
    /// by the single consumer when it has decided a claimed-but-never-committed
    /// record at the consumer position is blocking progress:
    ///   * length < 0 at head: a claim whose header was written but never
    ///     committed — rewrite it as PADDING of the same length.
    ///   * length == 0 at head: a claim whose tail CAS happened but whose header
    ///     was never written — scan forward (ALIGNMENT steps, up to the
    ///     producer index, or the buffer end if the tail has wrapped) for the
    ///     first non-zero length word; if every word between head and it is
    ///     still zero (scanBackToConfirmStillZeroed), pad head up to it.
    /// Returns true if it converted a region to padding (the next read() then
    /// skips it). Consumer thread only.
    ///
    /// Two deliberate deviations, both from the C++ port toward the Java
    /// original: (1) the forward-scan limit for the wrapped case is the DATA
    /// capacity (`m_capacity`, Java's `capacity`), not the C++ port's
    /// `m_buffer.capacity()` (which includes the trailer and would scan into
    /// the head/tail words); (2) the padding header is written as Java does —
    /// type word, then a release store of the (positive) length word — using
    /// this class's own __atomic accessors (TSan-visible) instead of
    /// putInt64Ordered.
    ///
    /// Hazard (inherent to Aeron's unblock, documented there too): if the
    /// "blocking" writer was merely stalled rather than gone, its later
    /// commit() throws ("Claimed space previously released") or, worse, its
    /// payload write races the consumer: it can land on the region just turned
    /// into padding, on bytes the consumer is reading in the same drain, or on
    /// space the consumer has since recycled — a torn read is possible. Never
    /// "harmless": a caller unblocks only when it accepts that risk. This
    /// client's sole use is ClientAgent::drainRegistrationsForClose(), which
    /// accepts it at close() after an iteration cap (a hang is worse).
    ///
    /// Not every stall is unblockable: a claim whose tail CAS covered an
    /// end-of-buffer wrap but whose writer stalled before writing the padding
    /// header leaves only zero words from head to the buffer end — there is no
    /// later non-zero length to pad up to, so this returns false (as Aeron's
    /// does) and anything behind that claim stays unreachable.
    bool unblock() {
        namespace RD = aeron::concurrent::ringbuffer;
        const std::int64_t headPosition = loadAcquire64_(m_headPositionIndex);
        const std::int64_t tailPosition = loadAcquire64_(m_tailPositionIndex) & TAIL_POSITION_MASK;

        if (tailPosition == headPosition) return false;

        const aeron::util::index_t mask = m_capacity - 1;
        const auto consumerIndex = static_cast<std::int32_t>(headPosition & mask);
        const auto producerIndex = static_cast<std::int32_t>(tailPosition & mask);

        bool unblocked = false;
        std::int32_t length = loadAcquire32_(RD::RecordDescriptor::lengthOffset(consumerIndex));
        if (length < 0) {
            m_buffer.putInt32(RD::RecordDescriptor::typeOffset(consumerIndex),
                              RD::RecordDescriptor::PADDING_MSG_TYPE_ID);
            storeRelease32_(RD::RecordDescriptor::lengthOffset(consumerIndex), -length);
            unblocked = true;
        } else if (0 == length) {
            // go from (consumerIndex to producerIndex) or (consumerIndex to capacity)
            const std::int32_t limit = producerIndex > consumerIndex ? producerIndex : m_capacity;
            std::int32_t i = consumerIndex + RD::RecordDescriptor::ALIGNMENT;
            do {
                // read the length word of every ALIGNMENT-aligned slot
                length = loadAcquire32_(RD::RecordDescriptor::lengthOffset(i));
                if (0 != length) {
                    if (scanBackToConfirmStillZeroed(i, consumerIndex)) {
                        m_buffer.putInt32(RD::RecordDescriptor::typeOffset(consumerIndex),
                                          RD::RecordDescriptor::PADDING_MSG_TYPE_ID);
                        storeRelease32_(RD::RecordDescriptor::lengthOffset(consumerIndex),
                                        i - consumerIndex);
                        unblocked = true;
                    }
                    break;
                }
                i += RD::RecordDescriptor::ALIGNMENT;
            } while (i < limit);
        }
        return unblocked;
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
        std::int64_t head = m_buffer.getInt64(headPositionIndex);
        const aeron::util::index_t capacity = m_capacity;
        aeron::util::index_t headIndex = static_cast<std::int32_t>(head) & (capacity - 1);
        aeron::util::index_t maxBlockLength = capacity - headIndex;
        aeron::util::index_t bytesRead = 0;
        // fix2 Important-1: same continuation as controlledRead() — at most ONE
        // end-of-buffer padding region can exist per wrap (claimCapacity only
        // pads the tail-end gap once), so at most one same-call continuation is
        // ever needed. Without this, a call that consumes only wrap padding
        // reports 0 ("nothing to do") even though a real message — e.g. a
        // producer registration — sits right past the wrap, which left
        // ClientAgent::drainRegistrationsForClose() able to miss a registration
        // at close time (leaked holder, ring never marked closed).
        bool canWrapOnce = true;

        ScopeExit cleanup([&]() {
            if (bytesRead != 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
            }
        });

        for (;;) {
            bool stop = false;
            while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
                const aeron::util::index_t recordIndex = headIndex + bytesRead;
                const std::int32_t recordLength =
                    loadAcquire32_(RD::RecordDescriptor::lengthOffset(recordIndex));
                if (recordLength <= 0) { stop = true; break; }

                bytesRead += aeron::util::BitUtil::align(recordLength, RD::RecordDescriptor::ALIGNMENT);

                const std::int32_t msgTypeId =
                    m_buffer.getInt32(RD::RecordDescriptor::typeOffset(recordIndex));
                if (RD::RecordDescriptor::PADDING_MSG_TYPE_ID == msgTypeId) continue;

                handler(msgTypeId, m_buffer,
                        RD::RecordDescriptor::encodedMsgOffset(recordIndex),
                        recordLength - RD::RecordDescriptor::HEADER_LENGTH);
                ++messagesRead;
            }
            if (stop) break;
            if (canWrapOnce && messagesRead == 0 && bytesRead == maxBlockLength && bytesRead > 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
                head          += bytesRead;
                headIndex      = 0;
                maxBlockLength = capacity;
                bytesRead      = 0;
                canWrapOnce    = false;
                continue;
            }
            break;
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
        aeron::util::index_t maxBlockLength = capacity - headIndex;
        aeron::util::index_t bytesRead = 0;
        // fix1 I6: at most ONE end-of-buffer padding region can exist per wrap
        // (claimCapacity only pads the tail-end gap once), so at most one
        // same-call continuation past it is ever needed.
        bool canWrapOnce = true;

        ScopeExit cleanup([&]() {
            if (bytesRead > 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
            }
        });

        for (;;) {
            bool stop = false;
            while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit)) {
                const aeron::util::index_t recordIndex = headIndex + bytesRead;
                const std::int32_t recordLength =
                    loadAcquire32_(RD::RecordDescriptor::lengthOffset(recordIndex));
                if (recordLength <= 0) { stop = true; break; }

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

                if (action == ControlledAction::ABORT) { bytesRead -= alignedLength; stop = true; break; }
                ++messagesRead;
                if (action == ControlledAction::BREAK) { stop = true; break; }
                if (action == ControlledAction::COMMIT) {
                    m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                    storeRelease64_(headPositionIndex, head + bytesRead);
                    headIndex += bytesRead;
                    head      += bytesRead;
                    bytesRead = 0;
                }
            }
            if (stop) break;
            // The block [headIndex, headIndex+maxBlockLength) is fully consumed
            // with budget left (messagesRead < messageCountLimit) and NOTHING
            // real was found (messagesRead == 0): it was pure wrap-around
            // padding. Continue from index 0 in THIS SAME call instead of
            // returning 0 — a caller that treats 0 as "ring empty, nothing to
            // do this cycle" would otherwise wait a full extra cycle for
            // records that are already sitting right past the wrap.
            if (canWrapOnce && messagesRead == 0 && bytesRead == maxBlockLength && bytesRead > 0) {
                m_buffer.setMemory(headIndex, static_cast<std::size_t>(bytesRead), 0);
                storeRelease64_(headPositionIndex, head + bytesRead);
                head          += bytesRead;
                headIndex      = 0;
                maxBlockLength = capacity;
                bytesRead      = 0;
                canWrapOnce    = false;
                continue;
            }
            break;
        }
        return messagesRead;
    }

private:
    // Trailer layout: byte-identical to Aeron's RingBufferDescriptor (tail,
    // head cache, head, correlation counter, consumer heartbeat; CL*12 long).
    // The closed flag lives in the tail word's top bit (F8c C1(a)), not in a
    // trailer word of its own.
    static constexpr aeron::util::index_t CL = aeron::util::BitUtil::CACHE_LINE_LENGTH;
    static constexpr aeron::util::index_t TAIL_POSITION_OFFSET       = CL * 2;
    static constexpr aeron::util::index_t HEAD_CACHE_POSITION_OFFSET = CL * 4;
    static constexpr aeron::util::index_t HEAD_POSITION_OFFSET       = CL * 6;
    static constexpr aeron::util::index_t CORRELATION_COUNTER_OFFSET = CL * 8;
    static constexpr aeron::util::index_t CONSUMER_HEARTBEAT_OFFSET  = CL * 10; // matches Aeron
    static constexpr aeron::util::index_t CONSUMER_FINISHED_OFFSET   = CL * 11; // Rune (I2): spare line
    static constexpr aeron::util::index_t DETACH_OFFSET              = CL * 9;  // Rune (C7): spare line
    static constexpr aeron::util::index_t ORPHAN_HINT_OFFSET         = CL * 1;  // Rune (C7 F1): spare line
    static constexpr std::int64_t kDetachedProducer = 1;
    static constexpr std::int64_t kDetachedConsumer = 2;
    static constexpr std::int64_t kOrphanRelease    = 4;
    static constexpr aeron::util::index_t TRAILER_LENGTH             = CL * 12; // matches Aeron
    static constexpr aeron::util::index_t INSUFFICIENT_CAPACITY      = -2;
    // The tail word: bit 63 = closed (positions never reach 2^63), bits 0..62 =
    // the tail position.  A closed tail is negative: one sign test.
    static constexpr std::int64_t TAIL_CLOSED_BIT    = std::int64_t{-1} << 63;  // INT64_MIN
    static constexpr std::int64_t TAIL_POSITION_MASK = ~TAIL_CLOSED_BIT;

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
    // fix1 minor 9: the heartbeat word orders nothing else, so RELAXED is
    // sufficient — still an __atomic op (visible to TSan), just the cheapest one.
    [[nodiscard]] std::int64_t loadRelaxed64_(aeron::util::index_t off) const noexcept {
        return __atomic_load_n(word64_(off), __ATOMIC_RELAXED);
    }
    void storeRelaxed64_(aeron::util::index_t off, std::int64_t v) noexcept {
        __atomic_store_n(word64_(off), v, __ATOMIC_RELAXED);
    }
    /// CAS that reports the observed value in `expected` on failure (the witness).
    bool compareAndSwap64_(aeron::util::index_t off, std::int64_t& expected,
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

    // Aeron ManyToOneRingBuffer::scanBackToConfirmStillZeroed (for unblock()).
    [[nodiscard]] bool scanBackToConfirmStillZeroed(std::int32_t from, std::int32_t limit) const noexcept {
        namespace RD = aeron::concurrent::ringbuffer;
        std::int32_t i = from - RD::RecordDescriptor::ALIGNMENT;
        bool allZeroes = true;
        while (i >= limit) {
            if (0 != loadAcquire32_(RD::RecordDescriptor::lengthOffset(i))) {
                allZeroes = false;
                break;
            }
            i -= RD::RecordDescriptor::ALIGNMENT;
        }
        return allZeroes;
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
        std::int64_t newTail;
        aeron::util::index_t tailIndex, padding, writeIndex;

        // F8c C1(a): the closed flag is bit 63 of the tail word.  The CAS's
        // EXPECTED value is always the masked (open) position, so a closed word
        // can never match: a claim succeeds only on an open tail, and every
        // claim is totally ordered with close() (one RMW on the same word).
        // The success path is load, AND, compute, CAS — the closed test runs
        // only on a failed CAS (already the contended path) and on the cold
        // full-ring return.  Do NOT drop the mask: an expected value carrying
        // the bit would let the CAS move a closed tail.
        for (;;) {
            const std::int64_t raw  = loadAcquire64_(tailPositionIndex);
            const std::int64_t tail = raw & TAIL_POSITION_MASK;
            const aeron::util::index_t availableCapacity =
                capacity - static_cast<aeron::util::index_t>(tail - head);

            if (requiredCapacity > availableCapacity) {
                head = loadAcquire64_(m_headPositionIndex);
                if (requiredCapacity > (capacity - static_cast<aeron::util::index_t>(tail - head))) {
                    // Cold: report closed from the value already loaded (closed
                    // is sticky), so a writer retrying on -2 can never livelock
                    // on a closed, full ring.
                    return raw < 0 ? kClaimClosed : INSUFFICIENT_CAPACITY;
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
            std::int64_t witness = tail;
            if (compareAndSwap64_(tailPositionIndex, witness, newTail)) break;
            if (witness < 0) [[unlikely]] return kClaimClosed;
        }

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
    aeron::util::index_t m_consumerFinishedIndex = 0;
    aeron::util::index_t m_detachIndex           = 0;
    aeron::util::index_t m_orphanHintIndex       = 0;
};

} // namespace rune::client::detail
