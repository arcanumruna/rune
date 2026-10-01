// include/rune/client/transport/AeronTransport.h
//
// AeronTransport — client-side Aeron duplex transport.
//
// Encapsulates:
//   - An embedded Aeron media driver (C API, own thread).
//   - One outbound ExclusivePublication  (client → gateway ingress).
//   - One inbound Subscription + FragmentAssembler (gateway → client egress).
//
// Threading model (SINGLE-WRITER):
//   offer() and poll() MUST be called from ONE thread (the session-layer thread,
//   CC-5).  The transport itself acquires no locks on the hot path.  start() and
//   stop() are NOT thread-safe with respect to offer/poll; call them from the same
//   thread or provide external synchronisation.
//
// Zero-copy handler contract:
//   The buffer pointer passed to the Handler is valid ONLY for the duration of
//   the callback.  Do not store it; copy the data if you need it to outlive the
//   call.

#pragma once

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <Subscription.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <rune/client/detail/FlatIndexMap.h>
#include <rune/client/transport/Resolution.h>
#include <rune/client/transport/TransportConfig.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace rune::client::transport {

/// dutyCycle() step, reported to the test-only pump observer (T15 R2 seam).
enum class PumpStep : uint8_t { Driver, Conductor };

// ---------------------------------------------------------------------------
// AeronTransport
// ---------------------------------------------------------------------------

class AeronTransport {
public:
    /// Zero-copy fragment handler.  buf/len are valid ONLY during this call.
    using Handler = std::function<void(const uint8_t* buf, std::size_t len)>;

    explicit AeronTransport(TransportConfig cfg);
    ~AeronTransport();

    // Non-copyable, non-movable (owns driver thread + C resources).
    AeronTransport(const AeronTransport&)            = delete;
    AeronTransport& operator=(const AeronTransport&) = delete;
    AeronTransport(AeronTransport&&)                 = delete;
    AeronTransport& operator=(AeronTransport&&)      = delete;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /// Start the embedded driver, connect the Aeron client, and resolve the
    /// ExclusivePublication (ingress) and Subscription (egress).
    /// Blocks until both are resolved or throws on failure.
    /// Bounded resolution (kResolveMaxMs hard cap + ASSERT — no busy-spin).
    void start();

    /// Release pub/sub → close aeron client → stop+join driver thread →
    /// drain CnC → close driver.  Idempotent (CAS flag).
    /// Teardown order follows BrokerNode BR-3b to prevent use-after-free.
    void stop() noexcept;

    /// Invoker mode: one pass of this transport's OWN embedded driver duty cycle
    /// (conductor + sender + receiver, aeron_driver.c:712-722) followed by the Aeron
    /// client conductor (useConductorAgentInvoker).  Thread mode: returns 0 — the
    /// driver thread and conductor thread own those duty cycles.
    int dutyCycle() noexcept;

    /// True once the Aeron client closed itself (inter-service timeout, driver
    /// timeout).  Offers then fail; the owner decides whether to rebuild.
    [[nodiscard]] bool closed() const noexcept { return aeron_ && aeron_->isClosed(); }

    /// True when a background driver thread exists (Thread mode after start()).
    [[nodiscard]] bool usesDriverThread() const noexcept { return driverThread_.joinable(); }

    /// True when the Aeron client conductor is invoker-driven (no conductor thread).
    [[nodiscard]] bool usesConductorInvoker() const noexcept {
        return aeron_ && aeron_->usesAgentInvoker();
    }

    /// The mediaDriverTimeout (ms) carried by the Aeron client's Context (0 before
    /// start()).  See mediaDriverTimeoutMsFor().
    [[nodiscard]] long aeronMediaDriverTimeoutMs() const noexcept {
        return aeron_ ? aeron_->context().mediaDriverTimeout() : 0;
    }

    /// Test-only seam (T15 R2): fn(ctx, step) runs after each dutyCycle() step, so
    /// a test can assert the driver is pumped before the client conductor.  Owner
    /// thread only; nullptr (the default) = off, one predictable branch per step.
    using PumpStepObserver = void (*)(void* ctx, PumpStep step) noexcept;
    void testSetPumpStepObserver(PumpStepObserver fn, void* ctx) noexcept {
        pumpObserver_    = fn;
        pumpObserverCtx_ = ctx;
    }

    // -------------------------------------------------------------------------
    // I/O — single-writer, called from ONE thread only
    // -------------------------------------------------------------------------

    /// Publish buf[0..len-1] to the default ingress ExclusivePublication
    /// (cfg_.ingressStreamId).  Uses tryClaim for zero-copy; falls back to
    /// offer if tryClaim is not supported.  Returns true iff accepted.
    /// No busy-spin — call site retries if needed.
    bool offer(const uint8_t* buf, std::size_t len) noexcept;

    /// F8a fix3: the 3-way verdict of an offer.
    ///   Accepted — published.
    ///   Retry    — transient: back-pressured, not connected, admin action, or
    ///              the publication is not resolved yet.  Offer it again later.
    ///   Fatal    — will never succeed: publication closed, max position
    ///              exceeded, or Aeron threw (e.g. the frame exceeds the
    ///              publication's max message length).  Drop it.
    enum class OfferResult : uint8_t { Accepted, Retry, Fatal };
    OfferResult offerResult(const uint8_t* buf, std::size_t len) noexcept;

    /// F8a fix3: the largest frame any ingress publication of this transport
    /// can carry (Aeron: termLength/8, capped at 16 MiB), computed once from
    /// the configuration (the smaller of the driver term length and a
    /// `term-length=` on the ingress channel).  Immutable: safe from any thread.
    [[nodiscard]] std::size_t maxMessageLength() const noexcept { return maxMessageLength_; }

    /// Publish buf[0..len-1] to the per-worker ingress ExclusivePublication
    /// for the given ingressStreamId (0 or the default stream id -> the default
    /// publication).  The (currentChannel, ingressStreamId) publication resolves
    /// asynchronously (never sleeps).  While it is not Ready this returns false
    /// (back-pressure: the caller keeps the batch and retries) — it NEVER falls
    /// back to the default publication: with maxInFlight > 1 that would split one
    /// partition's consecutive batches across two Aeron streams and let the
    /// broker process them out of order (ordering + idempotence break).
    /// E5.1: called from the ProducerImpl flush path with the per-partition
    /// worker stream id decoded from the metadata response.
    bool offer(int32_t ingressStreamId, const uint8_t* buf, std::size_t len) noexcept;

    /// Task 1.3 (multi-node): publish buf[0..len-1] to the ingress publication for
    /// an EXPLICIT (channel, ingressStreamId) pair — the per-leader-node routing
    /// primitive.  Uses publicationHandle(channel, streamId) on the ONE shared
    /// driver.  This REPLACES the single-transport retarget model: instead of
    /// swapping ingressPub_ to a leader, each leader session offers directly to
    /// its own endpoint.  Returns false while the publication resolves (never
    /// sleeps) or is back-pressured — the caller retries on the next duty cycle.
    /// Agent-thread only.
    bool offerTo(const std::string& channel, int32_t ingressStreamId,
                 const uint8_t* buf, std::size_t len) noexcept;

    /// Stable handle of the publication for (channel, streamId).  Cold path: the
    /// first call creates the slot.  Resolution advances only inside offerOn()
    /// (and offer(streamId)/offerTo(), which call it) — never blocking; in
    /// invoker mode the owner's dutyCycle() is what lets the driver + client
    /// conductor answer the registration between those calls.
    using PubHandle = int32_t;
    static constexpr PubHandle kNoPub = -1;
    PubHandle publicationHandle(const std::string& channel, int32_t streamId);

    /// Hot path: one bounds check + one variant check + tryClaim.  Returns false
    /// while the publication resolves or is back-pressured (retry next cycle).
    bool offerOn(PubHandle h, const uint8_t* buf, std::size_t len) noexcept;

    /// Test/simulation seam: the clock that drives every Resolution deadline and
    /// retry backoff (default: steady_clock).  pumpUntil_'s own wall-time bound
    /// in start()/retarget() stays on the real clock.  nullptr restores default.
    void setResolveClock(int64_t (*nowNs)() noexcept) noexcept {
        resolveNow_ = nowNs != nullptr ? nowNs : &monoNs_;
    }

    [[nodiscard]] bool publicationReady(PubHandle h) const noexcept {
        return h >= 0 && static_cast<std::size_t>(h) < pubSlots_.size() &&
               pubSlots_[static_cast<std::size_t>(h)].res.ready() != nullptr;
    }

    /// Polls the default egress subscription and every resolved per-worker egress
    /// subscription.  Unfragmented messages (every control frame / ProduceResponse)
    /// go straight to `handler` — an inlined template call, no std::function; only
    /// multi-fragment messages pass through Aeron's FragmentAssembler (the same
    /// UNFRAGMENTED test Aeron uses, FragmentAssembler.h:92).  handler:
    /// void(const uint8_t* buf, std::size_t len); buf valid only during the call.
    template <class H>
    int poll(H&& handler, int fragmentLimit) {
        using HT = std::remove_reference_t<H>;
        pollCtx_   = const_cast<void*>(static_cast<const void*>(&handler));
        pollThunk_ = [](void* ctx, const uint8_t* b, std::size_t n) { (*static_cast<HT*>(ctx))(b, n); };
        auto onFragment = [&handler](aeron::FragmentAssembler& assembler) {
            return [&handler, &assembler](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t offset, aeron::util::index_t length,
                                          aeron::Header& header) {
                namespace FrameDescriptor = aeron::FrameDescriptor;
                if ((header.flags() & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED) {
                    handler(reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                            static_cast<std::size_t>(length));
                } else {
                    assembler.handler()(buf, offset, length, header);
                }
            };
        };
        // fragmentLimit is the TOTAL budget across the default egress subscription
        // and every per-worker subscription (the caller sizes it to its ack-buffer
        // room).  The starting source rotates each poll so no subscription starves.
        int frags = 0;
        const std::size_t sources = 1 + subSlots_.size();
        const std::size_t start   = pollStart_ < sources ? pollStart_ : 0;
        pollStart_ = (start + 1 == sources) ? 0 : start + 1;
        for (std::size_t k = 0; k < sources && frags < fragmentLimit; ++k) {
            std::size_t i = start + k;
            if (i >= sources) i -= sources;
            if (i == 0) {
                if (egressSub_ && assembler_)
                    frags += egressSub_->poll(onFragment(*assembler_), fragmentLimit - frags);
                continue;
            }
            SubSlot& s = subSlots_[i - 1];
            aeron::Subscription* sub = s.res.ready();
            if (sub == nullptr) sub = resolveSubSlot_(s);
            if (sub != nullptr) frags += sub->poll(onFragment(*s.assembler), fragmentLimit - frags);
        }
        pollCtx_   = nullptr;
        pollThunk_ = nullptr;
        return frags;
    }

    /// read-rework I4: open (idempotently) a per-worker dynamic-MDC egress
    /// Subscription on controlChannel + streamId so the client receives Records
    /// pushed by the OWNING worker's egress publication.  Learned from the
    /// MetadataResponse (control host = leader broker host; port/stream per
    /// partition).  Agent-thread only (single-writer, same contract as poll()).
    /// Returns immediately: the subscription resolves inside poll() (never
    /// sleeps; a timed-out registration is retried after kResolveRetryNs) — the
    /// worker's cursor only advances on a successful send, so a delayed/absent
    /// subscription delays but never loses records.  Records route by
    /// subscriptionId in the SubscriberHub regardless of which sub delivered them.
    void addEgressSubscription(const std::string& controlChannel, int32_t streamId) noexcept;

    // -------------------------------------------------------------------------
    // Readiness
    // -------------------------------------------------------------------------

    /// True once the ingress ExclusivePublication is connected to at least one
    /// subscriber.  Safe to call before start() (returns false).
    [[nodiscard]] bool ingressConnected() const noexcept;

    /// True once the egress Subscription has at least one image (publisher
    /// connected).  Safe to call before start() (returns false).
    [[nodiscard]] bool egressConnected() const noexcept;

    /// Regression hook (spec §3.6): number of times the CALLER thread pumped
    /// aeron_driver_main_do_work() while driverThread_ was running.  A driver
    /// must be driven by exactly one thread, so this must always be 0.
    [[nodiscard]] int callerPumpViolations() const noexcept { return callerPumpViolations_; }

    /// Max single-frame payload of the default ingress publication
    /// (= mtuLength - 32 for UDP).  0 before start().
    [[nodiscard]] int32_t ingressMaxPayloadLength() const noexcept {
        return ingressPub_ ? ingressPub_->maxPayloadLength() : 0;
    }

    // -------------------------------------------------------------------------
    // retarget — single-writer, agent-thread only
    // -------------------------------------------------------------------------

    /// Create a new ingress ExclusivePublication on newIngressChannel (same
    /// stream id as the original config) and swap it in for the current one.
    /// Per-stream publications of offer(streamId) are lazily re-resolved on the
    /// new channel; the stream ids are PRESERVED (E5.1: REDIRECT changes channel,
    /// keeps per-partition worker stream ids).
    /// Bounded resolution (kResolveMaxMs hard cap); pumps this transport's own
    /// driver + conductor in invoker mode (never waits on another thread).
    /// Called only from the agent thread (single-writer contract).
    ///
    /// FIX F8: noexcept + non-throwing.  Resolves the NEW publication BEFORE
    /// releasing the old one, so a failed retarget (resolve timeout / not
    /// started) leaves the OLD publication intact and usable — offer() keeps
    /// working on the previous endpoint.  Returns true on success, false on
    /// failure (transport left consistent, no exception, no std::terminate even
    /// when reached from the agent's noexcept dispatchEgressFrame path).
    bool retarget(const std::string& newIngressChannel) noexcept;

private:
    // ---- Configuration -------------------------------------------------------
    TransportConfig cfg_;

    // ---- Embedded driver (C API) --------------------------------------------
    aeron_driver_context_t* driverCtx_     = nullptr;
    aeron_driver_t*         driver_        = nullptr;
    std::atomic<bool>       driverRunning_ {false};
    std::thread             driverThread_;

    void startEmbeddedDriver();
    void stopEmbeddedDriver() noexcept;

    /// Every caller-thread duty cycle of the driver goes through here.  Counts a
    /// violation when driverThread_ is concurrently driving the same driver.
    int pumpDriverFromCaller_() noexcept;
    int callerPumpViolations_ = 0;  // written/read by the owning (caller) thread only
    PumpStepObserver pumpObserver_    = nullptr;  // test seam (owner thread only)
    void*            pumpObserverCtx_ = nullptr;

    /// Pumps THIS transport's driver + conductor (Invoker) or backs off while the
    /// background agents work (Thread) until ready() or timeoutNs elapses.  Never
    /// waits on another component of the calling thread (spec §3.6).
    template <class Ready>
    bool pumpUntil_(Ready&& ready, int64_t timeoutNs);
    static int64_t monoNs_() noexcept;
    int64_t (*resolveNow_)() noexcept = &monoNs_;  // Resolution clock (injectable)

    // ---- Aeron C++ client + pub/sub -----------------------------------------
    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::ExclusivePublication> ingressPub_;
    std::shared_ptr<aeron::Subscription>         egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    // ---- Lifecycle CAS flags (idempotent stop) --------------------------------
    std::atomic<bool> started_ {false};
    std::atomic<bool> stopped_ {false};

    // ---- Poll handler thunk (set during poll(), cleared after) ---------------
    // Single-writer: only one thread calls poll() so this is never concurrent.
    // Reassembled (multi-fragment) messages reach the current poll() handler via
    // this thunk; set only for the duration of poll().
    void* pollCtx_ = nullptr;
    std::size_t pollStart_ = 0;  // round-robin first source for poll() (single-writer)
    void (*pollThunk_)(void*, const uint8_t*, std::size_t) = nullptr;

    // ---- Internal helpers ----------------------------------------------------
    /// Offer buf to a specific ExclusivePublication (shared implementation).
    std::size_t maxMessageLength_ = 0;  // immutable after construction
    static std::size_t computeMaxMessageLength(const TransportConfig& c) noexcept;
    OfferResult offerToPub(aeron::ExclusivePublication& pub,
                    const uint8_t* buf, std::size_t len) noexcept;

    // ---- Resolution constants ------------------------------------------------
    static constexpr int kResolveMaxMs = 5'000; // hard cap for pub/sub resolve

    // ---- Resolved-on-demand publications / subscriptions (spec §3.6) ---------
    // Single-writer (owner thread only).  Cleared in stop() before aeron_ release
    // (their dtors deregister through the client conductor).
    struct PubSlot {
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::ExclusivePublication> res;
    };
    struct SubSlot {
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::Subscription> res;
        std::unique_ptr<aeron::FragmentAssembler> assembler;  // built on Ready
    };
    std::vector<PubSlot>  pubSlots_;   // index == PubHandle (stable, never erased)
    detail::FlatIndexMap  streamPub_;  // default-channel streamId -> PubHandle
    std::vector<SubSlot>  subSlots_;   // per-worker dynamic-MDC egress subscriptions
    /// Advances a pending egress SubSlot; on Ready builds its assembler.
    aeron::Subscription* resolveSubSlot_(SubSlot& s) noexcept;

    static constexpr int64_t kResolveTimeoutNs = static_cast<int64_t>(kResolveMaxMs) * 1'000'000LL;
    static constexpr int64_t kResolveRetryNs   = 100'000'000LL;  // 100 ms before re-registering
};

} // namespace rune::client::transport
