#pragma once
// Completions — F8c: the polled completion queue (spec §2.2, §2.3, §3.3).
//
// Owned by ONE app thread.  Wraps a detail::CompletionRing whose single
// producer is the duty thread and whose single consumer is the owner, which
// drains it with poll().  Ops that target a Completions reserve their
// worst-case record size here, on the issuing (= owning) thread, BEFORE they
// are accepted; the duty thread then writes each completion within its own
// reservation, so the ring can never be full when the duty thread delivers.
//
// Every counter here is plain owner-thread state (no atomics): the issuing
// thread IS the polling thread (single-owner contract).  The only cross-thread
// memory is the ring's own.
//
// Two forms:
//   * Completions(cfg): standalone (C1) — no Client; the duty side reaches the
//     ring via ring() (tests, custom producers).
//   * Completions(client, cfg) (C2): registered with the Client.  The
//     constructor hands the ring to the duty thread (CqAdd on the agent
//     command ring) and waits for its CqRef (a registration record in its own
//     ring: one duty round trip, cold path).  Ops then carry that CqRef; the
//     duty thread resolves it in its registry and checks the generation, so a
//     completion for a released / reused slot is dropped, never misdelivered.
//
// Lifetime (§3.3 release protocol, no atomics outside rings).  The ring block
// has exactly ONE owner at a time:
//   * the duty registry, from CqAdd until (a) it applies this queue's
//     CqRelease (it then frees the block), or (b) Client close transfers the
//     block back (it delivers every outstanding `Closed` completion, then
//     producer-closes the ring — the last thing it ever does with it);
//   * this object otherwise.
// The destructor decides from ring words only: producer-closed -> free it
// here; else submit CqRelease: accepted -> the duty side frees it; refused
// because the Client is closing -> wait (idle strategy) until close
// producer-closes it, then free it here.  A Completions may outlive its
// Client (after close it holds only `Closed` completions, then finished()).

#include <rune/client/Completion.h>
#include <rune/client/detail/CompletionRing.h>

#include <concurrent/BackOffIdleStrategy.h>

#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <thread>
#include <utility>

namespace rune::client {

struct CompletionsConfig {
    uint32_t capacityBytes  = 1u << 20;   ///< ring capacity (power of two)
    uint32_t maxResultBytes = 16u << 10;  ///< reservation for a variable result (schema blob)
    uint32_t maxErrorText   = 192;        ///< reservation for BrokerError text (truncated)
    /// Clock for waitFor() deadlines (ns).  nullptr = steady_clock.  A test
    /// seam: tests drive deadlines with a frozen / stepped clock, never sleeps.
    int64_t (*nanoClock)() noexcept = nullptr;
};

/// The default waitFor() idle strategy (§2.3): spin 10, yield 5, park 1 µs → 1 ms.
struct DefaultWaitIdle : aeron::concurrent::BackoffIdleStrategy {
    DefaultWaitIdle()
        : aeron::concurrent::BackoffIdleStrategy(
              10, 5, std::chrono::duration<long, std::nano>(1'000), std::chrono::milliseconds(1)) {}
};

/// waitFor() pump for a thread that is NOT the duty thread (Thread mode): the
/// runner makes progress on its own, so nothing is pumped.
struct NoPump {
    int operator()() const noexcept { return 0; }
};

class Client;

static_assert(kBatchAckUnitBytes == detail::CompletionRing::completionRecordBytes(0),
              "a BatchAck credit unit is one producer completion record");

class Completions {
public:
    /// Standalone queue (no Client).
    explicit Completions(CompletionsConfig cfg = {})
        : cfg_(validate(cfg))
        , ring_(new detail::CompletionRing(cfg_.capacityBytes))
        , stash_(new uint8_t[cfg_.capacityBytes])
        , maxRecordBytes_(detail::CompletionRing::completionRecordBytes(
              cfg_.maxResultBytes > cfg_.maxErrorText ? cfg_.maxResultBytes : cfg_.maxErrorText))
        , owner_(std::this_thread::get_id()) {}

    /// Registered with `client` (C2).  The constructing thread becomes the
    /// owner: the only thread that may issue ops on, poll, wait on, or destroy
    /// this queue.  Blocks for one duty round trip (Invoker owner / a duty-
    /// cycle callback: applied directly, no wait).  Throws std::logic_error in
    /// Invoker mode off the owner thread, std::runtime_error when the Client's
    /// registry is full (kMaxCqs).  If the Client is already closed the queue
    /// is born finished (every op on it is rejected with OpResult::Closed).
    explicit Completions(Client& client, CompletionsConfig cfg = {});

    /// Release protocol (§3.3).  Never blocks: during another thread's close()
    /// the ring is freed by whichever of the two lets go last (one ring word).
    /// In Invoker mode, destroying a Completions is duty work (same contract as
    /// doWork(): never concurrently with doWork()/close() on another thread).
    /// If an op for this queue is still in the command ring, the release is
    /// deferred until the drain passes it — the slot stays held meanwhile, so
    /// the registry may report full until then; a late op completes into this
    /// queue's own (unpolled) ring, never into another queue.
    ~Completions();

    Completions(const Completions&)            = delete;
    Completions& operator=(const Completions&) = delete;
    /// Moving keeps the ring in place (heap) and the owner thread.  A
    /// moved-from Completions has no ring and must not be used again (review
    /// m10) — only destroyed/assigned.
    Completions(Completions&& o) noexcept { moveFrom_(o); }
    Completions& operator=(Completions&& o) noexcept {
        if (this != &o) {
            release_();
            moveFrom_(o);
        }
        return *this;
    }

    /// The Client this queue is registered with (nullptr: standalone).
    [[nodiscard]] Client* client() const noexcept { return client_; }
    /// The duty-registry reference (kNoCqRef: standalone, or born closed).
    [[nodiscard]] CqRef cqRef() const noexcept { return cqRef_; }
    /// False for a moved-from queue (no ring).
    [[nodiscard]] bool valid() const noexcept { return ring_ != nullptr; }
    /// True on the owner (constructing) thread.
    [[nodiscard]] bool onOwnerThread() const noexcept { return std::this_thread::get_id() == owner_; }

    // ---- Issuing side (owner thread) --------------------------------------

    /// The reservation (an aligned ring record size) an op of `kind` needs.
    /// `payloadBytes` is the op's own variable payload where it has one (the
    /// ping echo length).
    [[nodiscard]] uint32_t reservationFor(OpKind kind, uint32_t payloadBytes = 0) const noexcept {
        switch (kind) {
        case OpKind::Send:
        case OpKind::Tier2Batch:
        case OpKind::Flush:
        case OpKind::BatchAck:
        case OpKind::ProducerClose:
            return detail::CompletionRing::completionRecordBytes(0);
        case OpKind::Ping:
            // The echo, or the error text of a failed ping, whichever is longer.
            return detail::CompletionRing::completionRecordBytes(
                payloadBytes > cfg_.maxErrorText ? payloadBytes : cfg_.maxErrorText);
        case OpKind::GetTopicSchema:
        case OpKind::ListTopics:
            return detail::CompletionRing::completionRecordBytes(cfg_.maxResultBytes);
        default:
            return detail::CompletionRing::completionRecordBytes(cfg_.maxErrorText);
        }
    }

    /// Reserve `bytes` of credit for one op about to be issued.  Accepted: the
    /// op may be issued (and must complete exactly once, or be unreserve()d if
    /// it is then rejected).  QueueFull: poll() first.  Closed: the producer
    /// side closed this queue.  InvalidArgument: not an 8-aligned record size
    /// (use reservationFor()) or larger than one maximal record.  WrongThread
    /// (debug builds): called off the owner thread — the credit counters are
    /// owner-thread state (spec risk 3).
    [[nodiscard]] OpResult reserve(uint32_t bytes) noexcept {
#ifndef NDEBUG
        if (!onOwnerThread()) return OpResult::WrongThread;
#endif
        if (ring_->producerClosed()) return OpResult::Closed;
        // A reservation is a ring record size: 8-aligned (review m2) — an
        // unaligned one could under-reserve the aligned record by up to 7 bytes.
        if (bytes < detail::CompletionRing::completionRecordBytes(0) || bytes > maxRecordBytes_ ||
            (bytes & 7u) != 0) {
            return OpResult::InvalidArgument;
        }
        // The slack of one maximal record covers the at-most-one end-of-buffer
        // padding record the ring can hold.
        if (static_cast<uint64_t>(reservedTotal_) + bytes + maxRecordBytes_ > cfg_.capacityBytes) {
            return OpResult::QueueFull;
        }
        reservedTotal_ += bytes;
        ++outstanding_;
        return OpResult::Accepted;
    }

    /// Return the credit of a reservation whose op was then rejected (no
    /// completion will ever arrive for it).
    void unreserve(uint32_t bytes) noexcept {
        reservedTotal_ -= bytes;
        --outstanding_;
    }

    // ---- Producer side (the duty thread) -----------------------------------

    /// The duty side's handle.  It writes Completion records whose
    /// reservedBytes is the reservation made for that op.
    [[nodiscard]] detail::CompletionRing& ring() noexcept { return *ring_; }

    // ---- Consumer side (owner thread) --------------------------------------

    /// Deliver up to `limit` completions, oldest first:
    /// h(const Completion&, std::span<const uint8_t> payload) — the span is
    /// valid only during h.  Completions stashed by waitFor() come first (they
    /// were read from the ring before anything still in it).  Returns the
    /// number delivered.
    template <class H>
    int poll(H&& h, int limit = INT_MAX) {
        int n = 0;
        while (n < limit && stashHead_ < stashTail_) {
            uint8_t* e = stash_.get() + stashHead_;
            StashHdr hdr;
            std::memcpy(&hdr, e, sizeof hdr);
            stashHead_ += hdr.size;
            if (hdr.live == 0) continue;
            --stashLive_;
            Completion c;
            std::memcpy(&c, e + sizeof(StashHdr), sizeof c);
            consumeCredit_(c);
            ++n;
            h(c, std::span<const uint8_t>(e + sizeof(StashHdr) + sizeof c, c.payloadLen));
        }
        if (stashHead_ == stashTail_) stashHead_ = stashTail_ = 0;
        if (stashLive_ != 0 || n >= limit) return n;
        n += ring_->read(
            [this, &h](const Completion& c, std::span<const uint8_t> payload) {
                consumeCredit_(c);
                h(c, payload);
            },
            limit - n);
        return n;
    }

    enum class Wait : uint8_t {
        Done,        ///< onDone ran for the token
        TimedOut,    ///< stopped waiting; the op is still accepted and WILL complete — poll it
        NotPending,  ///< the producer closed and the token is nowhere (never accepted / already delivered)
    };

    /// Blocking convenience (§2.3): pump-until-done, never a condvar.
    ///   loop { w = pump(); w += read the ring (other tokens -> stash);
    ///          done? -> Done;  deadline? -> TimedOut;  idle.idle(w); }
    /// `pump` drives the duty cycle when the caller IS the duty thread (Invoker
    /// owner: pump = doWork); a Thread-mode app thread passes NoPump (the
    /// runner progresses by itself).  The pump is never called once the
    /// producer side has closed (its owner may be gone).  A TimedOut wait only
    /// stops waiting: the op will still complete and must still be polled.
    /// onDone(const Completion&, std::span<const uint8_t>) runs on this thread.
    template <class H, class Pump = NoPump, class Idle = DefaultWaitIdle>
    Wait waitFor(Token t, std::chrono::nanoseconds timeout, H&& onDone, Pump&& pump = Pump{},
                 Idle&& idle = Idle{}) {
        if (takeFromStash_(t, onDone)) return Wait::Done;
        const int64_t deadline = now_() + timeout.count();
        for (;;) {
            const bool closedBefore = ring_->producerClosed();
            int work = closedBefore ? 0 : pump();
            bool done = false;
            work += ring_->read(
                [&](const Completion& c, std::span<const uint8_t> payload) {
                    // Review m6: a BatchAck's token field packs (topic, partition),
                    // never an app token — never taken for one.
                    if (!done && c.token == t && c.op != OpKind::BatchAck) {
                        done = true;
                        consumeCredit_(c);
                        onDone(c, payload);
                    } else {
                        pushStash_(c, payload);
                    }
                },
                INT_MAX);
            if (done) return Wait::Done;
            if (closedBefore && ring_->empty()) return Wait::NotPending;
            if (now_() >= deadline) return Wait::TimedOut;
            idle.idle(work);
        }
    }

    /// Accepted ops not yet delivered (reserved and neither polled nor unreserved).
    [[nodiscard]] uint32_t outstanding() const noexcept { return outstanding_; }
    /// Credit currently reserved (bytes).
    [[nodiscard]] uint32_t reservedBytes() const noexcept { return reservedTotal_; }
    /// Completions read from the ring by waitFor() and not yet delivered.
    [[nodiscard]] uint32_t stashed() const noexcept { return stashLive_; }
    /// The producer side closed AND everything delivered so far was drained.
    [[nodiscard]] bool finished() const noexcept {
        return ring_->producerClosed() && ring_->empty() && stashLive_ == 0;
    }
    [[nodiscard]] const CompletionsConfig& config() const noexcept { return cfg_; }
    [[nodiscard]] uint32_t maxRecordBytes() const noexcept { return maxRecordBytes_; }

    /// Token of the internal registration record (outside the app's 48-bit space).
    static constexpr Token kRegistrationToken = ~Token{0};

private:
    friend class Client;
    friend class Producer;      // C6: per-record / batch-ack credit + producer-op accounting
    friend class BatchBuilder;  // C6: Tier-2 ack credit

    void moveFrom_(Completions& o) noexcept {
        cfg_            = o.cfg_;
        ring_           = std::exchange(o.ring_, nullptr);
        stash_          = std::move(o.stash_);
        maxRecordBytes_ = o.maxRecordBytes_;
        reservedTotal_  = o.reservedTotal_;
        outstanding_    = o.outstanding_;
        stashHead_      = o.stashHead_;
        stashTail_      = o.stashTail_;
        stashLive_      = o.stashLive_;
        client_         = std::exchange(o.client_, nullptr);
        owner_          = o.owner_;
        cqRef_          = std::exchange(o.cqRef_, kNoCqRef);
        issueFence_     = o.issueFence_;
        producerIssued_ = o.producerIssued_;
    }
    /// The §3.3 release (out of line: it talks to the Client).
    void release_() noexcept;

    struct StashHdr {
        uint32_t size;  // whole entry, 8-aligned (== the ring record size)
        uint32_t live;  // 0 once taken by waitFor()
    };
    static_assert(sizeof(StashHdr) == detail::CompletionRing::kRecordHeaderBytes);

    static CompletionsConfig validate(CompletionsConfig c) {
        if (c.capacityBytes < 1024 || (c.capacityBytes & (c.capacityBytes - 1)) != 0) {
            throw std::invalid_argument("Completions: capacityBytes must be a power of two >= 1024");
        }
        const uint32_t maxPayload = c.maxResultBytes > c.maxErrorText ? c.maxResultBytes : c.maxErrorText;
        // The ring's maxMsgLength is capacity / 8 (Aeron).
        if (static_cast<uint64_t>(detail::CompletionRing::kCompletionBytes) + maxPayload > c.capacityBytes / 8) {
            throw std::invalid_argument(
                "Completions: maxResultBytes / maxErrorText exceed capacityBytes / 8 - 40");
        }
        return c;
    }

    int64_t now_() const noexcept {
        if (cfg_.nanoClock != nullptr) return cfg_.nanoClock();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    void consumeCredit_(const Completion& c) noexcept {
        reservedTotal_ -= c.reservedBytes;
        // C6: one BatchAck record returns the credit of every record of its
        // batch (reservedBytes / kBatchAckUnitBytes reservations; EOS-OOS:
        // reserved0 carries the batch's producerEpoch).
        outstanding_ -= (c.op == OpKind::BatchAck) ? batchAckUnits(c) : 1u;
    }

    // Copy a completion read from the ring into the stash (FIFO).  The stash is
    // as large as the ring and holds only records that were in the ring, each
    // the same size as its ring record, so the live entries always fit; it is
    // compacted when the free tail is too short.
    void pushStash_(const Completion& c, std::span<const uint8_t> payload) {
        const uint32_t size = detail::CompletionRing::completionRecordBytes(c.payloadLen);
        if (stashTail_ + size > cfg_.capacityBytes) compactStash_();
        uint8_t* e = stash_.get() + stashTail_;
        const StashHdr hdr{size, 1};
        std::memcpy(e, &hdr, sizeof hdr);
        std::memcpy(e + sizeof hdr, &c, sizeof c);
        if (c.payloadLen != 0) std::memcpy(e + sizeof hdr + sizeof c, payload.data(), c.payloadLen);
        stashTail_ += size;
        ++stashLive_;
    }

    void compactStash_() noexcept {
        uint32_t out = 0;
        for (uint32_t in = stashHead_; in < stashTail_;) {
            StashHdr hdr;
            std::memcpy(&hdr, stash_.get() + in, sizeof hdr);
            if (hdr.live != 0) {
                if (out != in) std::memmove(stash_.get() + out, stash_.get() + in, hdr.size);
                out += hdr.size;
            }
            in += hdr.size;
        }
        stashHead_ = 0;
        stashTail_ = out;
    }

    template <class H>
    bool takeFromStash_(Token t, H& onDone) {
        for (uint32_t at = stashHead_; at < stashTail_;) {
            uint8_t* e = stash_.get() + at;
            StashHdr hdr;
            std::memcpy(&hdr, e, sizeof hdr);
            if (hdr.live != 0) {
                Completion c;
                std::memcpy(&c, e + sizeof hdr, sizeof c);
                if (c.token == t && c.op != OpKind::BatchAck) {
                    hdr.live = 0;
                    std::memcpy(e, &hdr, sizeof hdr);
                    --stashLive_;
                    consumeCredit_(c);
                    onDone(c, std::span<const uint8_t>(e + sizeof hdr + sizeof c, c.payloadLen));
                    return true;
                }
            }
            at += hdr.size;
        }
        return false;
    }

    CompletionsConfig          cfg_;
    detail::CompletionRing*    ring_ = nullptr;  // owned per the §3.3 protocol (see the class doc)
    std::unique_ptr<uint8_t[]> stash_;
    uint32_t maxRecordBytes_ = 0;
    uint32_t reservedTotal_  = 0;  // owner thread only
    uint32_t outstanding_    = 0;  // owner thread only
    uint32_t stashHead_      = 0;  // owner thread only
    uint32_t stashTail_      = 0;
    uint32_t stashLive_      = 0;
    Client*         client_ = nullptr;   // registered with (nullptr: standalone)
    std::thread::id owner_;              // the constructing thread
    CqRef           cqRef_  = kNoCqRef;  // duty-assigned at registration
    std::int64_t    issueFence_ = 0;     // I3': command-ring tail after its last ring-submitted op (owner)
    /// C6: producer completions (per-record / Tier-2 acks, batch-ack credit
    /// units) accepted for this queue so far — owner-thread state.  They reach
    /// the duty side through the PRODUCER ring, not the command ring, so the
    /// I3' fence cannot order them against this queue's release: the release
    /// carries this count and the duty side quarantines the slot until it has
    /// delivered (or dropped) exactly this many producer completions.
    std::uint64_t   producerIssued_ = 0;
};

/// Where an op's single completion goes (spec §2.1): a Completions queue the
/// issuing thread owns and polls, or a CompletionHandler called on the duty
/// thread.  Implicit from either, so every op takes `CompletionTarget`.
struct CompletionTarget {
    Completions*      cq = nullptr;
    CompletionHandler handler{};
    CompletionTarget(Completions& q) noexcept : cq(&q) {}             // NOLINT(google-explicit-constructor)
    CompletionTarget(CompletionHandler h) noexcept : handler(h) {}    // NOLINT(google-explicit-constructor)
};

} // namespace rune::client
