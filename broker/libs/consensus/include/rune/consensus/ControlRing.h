#pragma once
// ControlRing.h — F9a: the ONE cross-thread control/observer channel.
//
// Rune law (user, 2026-10-01): no std::atomic / mutex anywhere outside the ring
// implementations and the LogCleaner channel.  Every lifecycle command (Stop),
// signal event (Stop / FrDump / Halt) and observer snapshot (leader view, driver
// misuse event) crosses threads as a RING MESSAGE on one of these.
//
// A tiny fixed-capacity wrapper over rune::consensus::detail::M2ORingBuffer (the
// namespace-isolated copy of Aeron/agrona's ManyToOneRingBuffer — the memory
// ordering lives INSIDE the ring implementation, which is the only allowed place).
//
//   - Storage is INLINE (no heap): Capacity payload bytes + the ring trailer.
//   - A message is a fixed 16-byte {int64 a, int64 b} body + a msgTypeId (>0)
//     (24-byte record with the 8-byte header; Capacity/24 messages fit).
//   - offer(): any number of producer threads (M2O claims the tail with the
//     ring's own CAS).  Returns false when full — the caller decides (retry next
//     cycle, conflate, or drop + log); offer() never blocks and never allocates.
//   - poll(): EXACTLY ONE consumer thread.  Templated handler (inlined — no
//     std::function, no virtual call), so draining an EMPTY ring on a duty cycle
//     is a few loads (head positions + the record length), no stores.
//
// Cost on an idle duty cycle: poll() on an empty ring ≈ 4 loads, no stores.

#include <rune/consensus/detail/M2ORingBuffer.h>

#include <array>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::consensus {

template <std::size_t Capacity = 1024>
class ControlRing {
    static_assert(Capacity >= 256 && (Capacity & (Capacity - 1)) == 0,
                  "ControlRing capacity must be a power of two >= 256");

public:
    static constexpr std::size_t kBodyBytes = 16;

    ControlRing() : ring_(storage_.data(), storage_.size()) {}

    ControlRing(const ControlRing&)            = delete;
    ControlRing& operator=(const ControlRing&) = delete;
    ControlRing(ControlRing&&)                 = delete;
    ControlRing& operator=(ControlRing&&)      = delete;

    /// Producer side (any thread).  msgTypeId must be > 0.  false = ring full.
    [[nodiscard]] bool offer(std::int32_t msgTypeId, std::int64_t a = 0, std::int64_t b = 0) noexcept {
        const aeron::util::index_t idx = ring_.tryClaim(msgTypeId, kBodyBytes);
        if (idx < 0) return false;
        std::uint8_t* p = ring_.base() + idx;
        std::memcpy(p, &a, sizeof(a));
        std::memcpy(p + sizeof(a), &b, sizeof(b));
        ring_.commit(idx);
        return true;
    }

    /// Consumer side (exactly one thread).  handler(int32 msgTypeId, int64 a, int64 b).
    /// Returns the number of messages consumed.
    ///
    /// Agrona's read() never crosses the buffer end in one call: a poll that
    /// consumes up to the end (e.g. only the padding record before a wrap) would
    /// otherwise report 0 while a message waits at index 0.  So when one read
    /// ends exactly at a wrap boundary, poll() reads once more.
    template <typename Handler>
    int poll(Handler&& handler, int limit = INT_MAX) noexcept(noexcept(handler(0, 0, 0))) {
        auto decode = [&handler](std::int32_t type, aeron::concurrent::AtomicBuffer& buf,
                                 aeron::util::index_t off, aeron::util::index_t /*len*/) {
            std::int64_t a = 0;
            std::int64_t b = 0;
            std::memcpy(&a, buf.buffer() + off, sizeof(a));
            std::memcpy(&b, buf.buffer() + off + sizeof(a), sizeof(b));
            handler(type, a, b);
        };
        const std::int64_t before = ring_.consumerPosition();
        int n = ring_.read(decode, limit);
        const std::int64_t after = ring_.consumerPosition();
        if (after != before && (after & static_cast<std::int64_t>(Capacity - 1)) == 0 && n < limit) {
            n += ring_.read(decode, limit - n);
        }
        return n;
    }

    /// One message's ring footprint: the 8-byte record header + the 16-byte body (8-aligned).
    static constexpr std::size_t kRecordBytes = 24;
    /// Committed-or-claimed bytes not yet consumed (producer position - consumer position).  Any thread;
    /// approximate while the other side runs (a wrap's padding record counts too).  Diagnostics and
    /// depth estimates only — never a correctness decision.
    [[nodiscard]] std::int64_t pendingBytes() const noexcept {
        return ring_.producerPosition() - ring_.consumerPosition();
    }
    /// The ring's producer / consumer positions (bytes; monotonic).  Approximate
    /// while the other side runs — estimates only (e.g. "is a record I offered
    /// still queued": its end position > consumerPosition()).
    [[nodiscard]] std::int64_t producerPosition() const noexcept { return ring_.producerPosition(); }
    [[nodiscard]] std::int64_t consumerPosition() const noexcept { return ring_.consumerPosition(); }
    /// What a doorbell watch needs (node/detail/RingDoorbell.h RingWatch): buffer, its length (capacity +
    /// trailer), the pow2 payload capacity and the head-position index.  Startup wiring only.
    struct WatchInfo {
        std::uint8_t*        buffer;
        aeron::util::index_t length;
        aeron::util::index_t capacity;
        aeron::util::index_t headPositionIndex;
    };
    [[nodiscard]] WatchInfo watchInfo() noexcept {
        return {storage_.data(), static_cast<aeron::util::index_t>(storage_.size()),
                static_cast<aeron::util::index_t>(Capacity), ring_.headPositionIndex()};
    }

private:
    alignas(64) std::array<std::uint8_t,
                           Capacity + static_cast<std::size_t>(detail::M2ORingBuffer::trailerLength())>
        storage_{};
    detail::M2ORingBuffer ring_;
};

} // namespace rune::consensus
