#pragma once
// CompletionRing — F8c C1(b): the single-producer / single-consumer channel
// that carries Completion records from the duty thread (the ONE producer) to
// the app thread that owns a rune::client::Completions (the ONE consumer).
//
// A thin typed layer over detail::OneToOneRingBuffer (the Aeron
// OneToOneRingBuffer port): each record is one Completion (40 bytes) followed
// inline by its payload.  The only cross-thread memory is the ring's own
// (record header / head / tail and the producer-closed word) — no std::atomic,
// no lock, no allocation after construction, no std::function (the read
// handler is a template).
//
// Credit contract (spec §2.2): the issuing thread reserved Completion::
// reservedBytes (an aligned record size) on the Completions BEFORE the op was
// accepted, and the sum of the outstanding reservations plus one maximal
// record (the at-most-one end-of-buffer padding) never exceeds the capacity —
// so deliver() can never meet a full ring.  deliver() writes each completion
// within its own reservation (error text truncated; an oversize result becomes
// OpStatus::ResultTooLarge).  A failed write would be a bug: it is counted in
// lostCompletions() (the tests assert it stays 0).

#include <rune/client/Completion.h>
#include <rune/client/detail/OneToOneRingBuffer.h>

#include <concurrent/AtomicBuffer.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>

namespace rune::client::detail {

class CompletionRing {
public:
    static constexpr std::int32_t kMsgCompletion = 1;
    static constexpr std::uint32_t kRecordHeaderBytes = 8;  // Aeron RecordDescriptor::HEADER_LENGTH
    static constexpr std::uint32_t kCompletionBytes = sizeof(Completion);

    /// Aligned ring footprint of a record whose message is `msgLen` bytes.
    static constexpr std::uint32_t recordBytes(std::uint32_t msgLen) noexcept {
        return (msgLen + kRecordHeaderBytes + 7u) & ~7u;
    }
    /// Aligned ring footprint of a completion carrying `payloadLen` bytes.
    static constexpr std::uint32_t completionRecordBytes(std::uint32_t payloadLen) noexcept {
        return recordBytes(kCompletionBytes + payloadLen);
    }

    /// `capacityBytes`: the ring's usable capacity (a power of two).
    explicit CompletionRing(std::uint32_t capacityBytes)
        : storage_(new std::uint8_t[totalBytes(capacityBytes)]())  // zeroed: the ring requires it
        , ring_(storage_.get(), totalBytes(capacityBytes)) {}

    CompletionRing(const CompletionRing&)            = delete;
    CompletionRing& operator=(const CompletionRing&) = delete;

    /// Test seam (F8c C2 release-protocol tests): called by the destructor
    /// when set — counts frees.  Set it before any thread that may free a
    /// ring runs and clear it after they are joined (never written while
    /// another thread reads it).  nullptr in production.
    static inline void (*testOnDestroy)(const CompletionRing*) noexcept = nullptr;
    ~CompletionRing() {
        if (testOnDestroy != nullptr) testOnDestroy(this);
    }

    [[nodiscard]] std::uint32_t capacity() const noexcept {
        return static_cast<std::uint32_t>(ring_.capacity());
    }
    [[nodiscard]] std::uint32_t maxMsgLength() const noexcept {
        return static_cast<std::uint32_t>(ring_.maxMsgLength());
    }

    // ---- Producer side (the duty thread only) ------------------------------

    /// What deliver() does with a payload larger than the reservation allows.
    enum class Overflow : std::uint8_t {
        Truncate,  ///< error text: keep the first bytes that fit
        TooLarge,  ///< a result blob: status = ResultTooLarge, value = real size, no payload
    };

    /// Writes `c` + `payload` within c.reservedBytes.  Returns false only on a
    /// credit-contract violation (counted in lostCompletions()).
    bool deliver(Completion c, std::span<const std::uint8_t> payload,
                 Overflow overflow = Overflow::Truncate) noexcept {
        if (c.reservedBytes < completionRecordBytes(0)) {
            ++lostCompletions_;
            return false;
        }
        std::uint32_t room = c.reservedBytes - kRecordHeaderBytes - kCompletionBytes;
        if (room > maxMsgLength() - kCompletionBytes) room = maxMsgLength() - kCompletionBytes;
        std::size_t len = payload.size();
        if (len > room) {
            if (overflow == Overflow::TooLarge) {
                c.status = OpStatus::ResultTooLarge;
                c.value  = static_cast<std::int64_t>(len);
                len = 0;
            } else {
                len = room;
            }
        }
        c.payloadLen = static_cast<std::uint32_t>(len);
        bool ok = false;
        try {
            ok = ring_.writeWith(kMsgCompletion,
                                 static_cast<aeron::util::index_t>(kCompletionBytes + len),
                                 [&](aeron::concurrent::AtomicBuffer& dst, aeron::util::index_t at) {
                                     std::uint8_t* p = dst.buffer() + at;
                                     std::memcpy(p, &c, kCompletionBytes);
                                     if (len != 0) std::memcpy(p + kCompletionBytes, payload.data(), len);
                                 });
        } catch (...) {
            ok = false;
        }
        if (!ok) ++lostCompletions_;
        return ok;
    }

    /// The producer will deliver nothing more (release).  Idempotent.
    void producerClose() noexcept { ring_.producerClose(); }

    /// Credit-contract violations so far.  Producer-side state: read it on the
    /// duty thread, or after the duty thread is gone.
    [[nodiscard]] std::uint64_t lostCompletions() const noexcept { return lostCompletions_; }

    // ---- Consumer side (the owning app thread only) ------------------------

    /// Reads up to `limit` completions: f(const Completion&, std::span<const
    /// uint8_t> payload) — the span is valid only during the call.  Continues
    /// past end-of-buffer padding in the same call.  Returns completions read.
    template <class F>
    int read(F&& f, int limit) {
        int n = 0;
        while (n < limit) {
            const std::int64_t before = ring_.consumerPosition();
            n += ring_.read(
                [&f](std::int32_t, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                     aeron::util::index_t) {
                    Completion c;
                    const std::uint8_t* p = buf.buffer() + offset;
                    std::memcpy(&c, p, kCompletionBytes);
                    f(c, std::span<const std::uint8_t>(p + kCompletionBytes, c.payloadLen));
                },
                limit - n);
            if (ring_.consumerPosition() == before) break;  // nothing (not even padding) consumed
        }
        return n;
    }

    /// Either side: has the producer closed (acquire)?  A consumer that
    /// observes true and THEN drains sees every completion ever delivered.
    [[nodiscard]] bool producerClosed() const noexcept { return ring_.producerClosed(); }

    /// Consumer side: nothing unread right now.
    [[nodiscard]] bool empty() const noexcept {
        return ring_.consumerPosition() == ring_.producerPosition();
    }

private:
    static std::size_t totalBytes(std::uint32_t capacityBytes) {
        if (capacityBytes == 0 || (capacityBytes & (capacityBytes - 1)) != 0) {
            throw std::invalid_argument("CompletionRing: capacityBytes must be a power of two");
        }
        return static_cast<std::size_t>(capacityBytes) +
               static_cast<std::size_t>(OneToOneRingBuffer::trailerLength());
    }

    std::unique_ptr<std::uint8_t[]> storage_;
    OneToOneRingBuffer               ring_;
    std::uint64_t                    lostCompletions_ = 0;  // producer (duty) side only
};

} // namespace rune::client::detail
