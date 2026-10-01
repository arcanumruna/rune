#pragma once
// StatusRing — F8a single-producer / single-consumer "latest status" channel.
//
// Shared-nothing law: a value the duty thread owns (a subscription's bound
// subscriptionId / resume position, a consumer's liveness) is NEVER read by an
// app thread directly.  The duty thread PUBLISHES snapshots of it into this
// ring; the app-side handle (the ring's single consumer) drains the ring and
// keeps the latest snapshot in its own, app-owned cache.  The only cross-thread
// memory ordering is the ring's own (M2ORingBuffer head/tail/length/closed
// words) — no std::atomic, no lock, anywhere in this class.
//
// The ring's `closed` trailer word doubles as the "this channel is finished"
// signal: the duty thread (or Client::close()) publishes the final snapshot and
// THEN closes the ring.  An app thread that observes closed() (acquire) and
// drains afterwards therefore always sees that final snapshot, and knows its
// handle must never again submit a command to a Client that may be gone.
//
// Back-pressure (latest-value semantics, never a lost final state):
//   - tryPublish(): ordinary updates (e.g. a position advance per Records
//     frame).  Written only while at least kReserveRecords record slots remain
//     free; otherwise refused (the caller marks itself dirty and retries on a
//     later duty cycle — a newer snapshot supersedes it anyway).
//   - publishFinal(): lifecycle snapshots (bind, final/close).  Always fits:
//     ordinary updates never consume the reserve, and at most a couple of
//     lifecycle snapshots are ever written per channel.

#include <rune/client/detail/M2ORingBuffer.h>

#include <concurrent/AtomicBuffer.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

namespace rune::client::detail {

template <class T, std::int32_t Capacity = 1024>
class StatusRing {
    static_assert(std::is_trivially_copyable_v<T>, "StatusRing payload must be trivially copyable");
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

public:
    static constexpr std::int32_t kMsgType = 1;
    // One record = 8-byte Aeron record header + payload, 8-byte aligned.
    static constexpr std::int32_t kRecordBytes =
        static_cast<std::int32_t>(((sizeof(T) + 8u) + 7u) & ~std::size_t{7});
    // Slots kept free for lifecycle snapshots (bind + final), plus worst-case
    // end-of-buffer padding for each.
    static constexpr std::int32_t kReserveRecords = 4;

    StatusRing()
        : storage_(new std::uint8_t[totalBytes()]())  // zeroed: the ring requires it
        , ring_(storage_.get(), totalBytes()) {}

    StatusRing(const StatusRing&)            = delete;
    StatusRing& operator=(const StatusRing&) = delete;

    // ---- Producer side (the duty thread only) ------------------------------

    /// Ordinary update.  false = refused (ring near-full: the consumer has not
    /// drained for a while) or the channel is already closed.
    bool tryPublish(const T& v) {
        if (ring_.closed()) return false;
        const std::int64_t used = ring_.tailPosition() - ring_.headPosition();
        if (static_cast<std::int64_t>(Capacity) - used <
            static_cast<std::int64_t>(kRecordBytes) * (2 + kReserveRecords)) {
            return false;
        }
        return write_(v);
    }

    /// Lifecycle update — uses the reserve.  false only if already closed (or,
    /// impossibly given the reserve, the ring is completely full).
    bool publishFinal(const T& v) {
        if (ring_.closed()) return false;
        return write_(v);
    }

    /// Mark the channel finished (release).  Idempotent.  Any thread that
    /// established it is the channel's last writer (duty thread, or the
    /// closing thread after the duty thread is gone).
    void close() noexcept { ring_.closed(true); }

    // ---- Consumer side (the app-side handle only) --------------------------

    /// Drain everything published so far; `latest` receives the newest
    /// snapshot.  Returns true if anything was read.
    bool drainLatest(T& latest) {
        bool any = false;
        ring_.read([&](std::int32_t, aeron::concurrent::AtomicBuffer& buf,
                       aeron::util::index_t offset, aeron::util::index_t len) {
            if (len == static_cast<aeron::util::index_t>(sizeof(T))) {
                std::memcpy(&latest, buf.buffer() + offset, sizeof(T));
                any = true;
            }
        });
        return any;
    }

    /// Either side: has the channel been closed (acquire)?
    [[nodiscard]] bool closed() const noexcept { return ring_.closed(); }

private:
    static constexpr std::size_t totalBytes() noexcept {
        return static_cast<std::size_t>(Capacity) +
               static_cast<std::size_t>(M2ORingBuffer::trailerLength());
    }

    bool write_(const T& v) {
        return ring_.writeWith(kMsgType, static_cast<aeron::util::index_t>(sizeof(T)),
                               [&](aeron::concurrent::AtomicBuffer& dst, aeron::util::index_t at) {
                                   std::memcpy(dst.buffer() + at, &v, sizeof(T));
                               });
    }

    std::unique_ptr<std::uint8_t[]> storage_;
    M2ORingBuffer                   ring_;
};

} // namespace rune::client::detail
