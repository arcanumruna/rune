#pragma once
// StatusRing — F8a single-producer / single-consumer "latest status" channel.
//
// Shared-nothing law: a value the duty thread owns (a subscription's bound
// subscriptionId / resume position, a consumer's generation) is NEVER read by an
// app thread directly.  The duty thread (the ONE producer) PUBLISHES snapshots
// of it into this ring; the app-side handle (the ONE consumer) drains the ring
// and keeps the latest snapshot in its own, app-owned cache.  The only cross-
// thread memory ordering is the ring's own (OneToOneRingBuffer header/head/tail
// and its two closed words) — no std::atomic, no lock, anywhere in this class.
//
// Two "finished" words, ONE WRITER EACH (fix round 1):
//   - producer side: close() — the duty thread (or Client::close() after the
//     duty thread is gone) publishes the FINAL snapshot and THEN closes.  An app
//     thread that observes producerClosed() (acquire) and drains afterwards sees
//     that final snapshot, and knows no callback for this channel will run again.
//   - consumer side: abandon() — the app thread that created the handle marks a
//     registration that was never (or indeterminately) delivered.
//
// Back-pressure (latest-value semantics, never a lost final state):
//   - tryPublish(): ordinary updates.  Written only while kReserveRecords slots
//     stay free (checked through the producer's head cache — no per-publish read
//     of the consumer's head); otherwise refused (the caller marks itself dirty
//     and retries on a following duty cycle — a newer snapshot supersedes it).
//   - publishFinal(): lifecycle snapshots.  Always fits: ordinary updates never
//     consume the reserve, and at most a couple are written per channel.

#include <rune/client/detail/OneToOneRingBuffer.h>

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
    /// drained for a while) or the channel is already finished.
    bool tryPublish(const T& v) {
        if (ring_.closed()) return false;
        if (!ring_.hasCapacityFor(kRecordBytes * (2 + kReserveRecords))) return false;
        return write_(v);
    }

    /// Lifecycle update — uses the reserve.  false only if already closed.
    bool publishFinal(const T& v) {
        if (ring_.producerClosed()) return false;
        return write_(v);
    }

    /// Producer side: the channel is finished (release).  Idempotent.
    void close() noexcept { ring_.producerClose(); }

    // ---- Consumer side (the app-side handle only) --------------------------

    /// Drain everything published so far; `latest` receives the newest
    /// snapshot.  Returns true if anything was read.  Loops until the head stops
    /// moving, so a read that consumed only end-of-buffer padding is followed
    /// by the one that reaches the record past the wrap.
    bool drainLatest(T& latest) {
        bool any = false;
        for (;;) {
            const std::int64_t before = ring_.consumerPosition();
            ring_.read([&](std::int32_t, aeron::concurrent::AtomicBuffer& buf,
                           aeron::util::index_t offset, aeron::util::index_t len) {
                if (len == static_cast<aeron::util::index_t>(sizeof(T))) {
                    std::memcpy(&latest, buf.buffer() + offset, sizeof(T));
                    any = true;
                }
            });
            if (ring_.consumerPosition() == before) break;
        }
        return any;
    }

    /// T15b: drain everything published so far IN ORDER: h(const T&) per
    /// snapshot (an event log, not only the latest).  Returns the count.
    template <class H>
    int drainAll(H&& h) {
        int n = 0;
        for (;;) {
            const std::int64_t before = ring_.consumerPosition();
            ring_.read([&](std::int32_t, aeron::concurrent::AtomicBuffer& buf,
                           aeron::util::index_t offset, aeron::util::index_t len) {
                if (len == static_cast<aeron::util::index_t>(sizeof(T))) {
                    T v;
                    std::memcpy(&v, buf.buffer() + offset, sizeof(T));
                    ++n;
                    h(static_cast<const T&>(v));
                }
            });
            if (ring_.consumerPosition() == before) break;
        }
        return n;
    }

    /// Consumer side: abandon the channel (release).  Idempotent.
    void abandon() noexcept { ring_.consumerClose(); }

    /// F8c C7: last-one-frees ownership hand-off of the impl that owns this
    /// ring, between the duty side (the ring's producer: the hub that owns the
    /// impl) and the app-side handle (its consumer).  Each side detaches once;
    /// the call that finds the other side already detached returns true and
    /// its caller frees the impl.  Nobody waits (OneToOneRingBuffer's I5 word).
    bool detachProducer() noexcept { return ring_.detachProducer(); }
    bool detachConsumer() noexcept { return ring_.detachConsumer(); }
    /// F8c C7 fix round 2 (F1): the handle's detach with its release ORPHANED
    /// (the command ring was full) — the duty side applies it.
    bool detachConsumerOrphaned() noexcept { return ring_.detachConsumerOrphaned(); }
    [[nodiscard]] bool consumerOrphaned() const noexcept { return ring_.consumerOrphaned(); }
    /// Either side (acquire): the duty side detached — it never touches the
    /// impl (or its Client) again; the handle owns it alone.
    [[nodiscard]] bool producerDetached() const noexcept { return ring_.producerDetached(); }

    /// Either side: finished by the producer (no further callbacks; the last
    /// published snapshot is final)?
    [[nodiscard]] bool producerClosed() const noexcept { return ring_.producerClosed(); }
    /// Either side: finished by either side?
    [[nodiscard]] bool closed() const noexcept { return ring_.closed(); }

private:
    static constexpr std::size_t totalBytes() noexcept {
        return static_cast<std::size_t>(Capacity) +
               static_cast<std::size_t>(OneToOneRingBuffer::trailerLength());
    }

    bool write_(const T& v) {
        return ring_.writeWith(kMsgType, static_cast<aeron::util::index_t>(sizeof(T)),
                               [&](aeron::concurrent::AtomicBuffer& dst, aeron::util::index_t at) {
                                   std::memcpy(dst.buffer() + at, &v, sizeof(T));
                               });
    }

    std::unique_ptr<std::uint8_t[]> storage_;
    OneToOneRingBuffer              ring_;
};

} // namespace rune::client::detail
