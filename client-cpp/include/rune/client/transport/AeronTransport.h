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
#include <concurrent/logbuffer/FrameDescriptor.h>

extern "C" {
#include <aeronmd/aeronmd.h>
#include "rune/protocol/net/rune_gso_bindings.h"
}

#include <rune/client/detail/FlatIndexMap.h>
#include <rune/client/detail/OneToOneRingBuffer.h>
#include <rune/client/transport/DriverDirLock.h>
#include <rune/client/transport/FrameCrc.h>
#include <rune/client/transport/Resolution.h>
#include <rune/client/transport/ResponseChannel.h>
#include <rune/client/transport/TransportConfig.h>
#include <flight_recorder/FlightRecorder.h>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <variant>
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
    /// #510: a start() that threw leaves the transport StartFailed - every later
    /// start() rethrows that original error (stop() still frees what it created);
    /// a start() after stop() throws std::logic_error.
    void start();

    /// Release pub/sub → close aeron client → stop+join driver thread →
    /// drain CnC → close driver.  Idempotent (plain owner flag).
    /// Teardown order follows BrokerNode BR-3b to prevent use-after-free.
    /// Lifecycle call: only when no other thread pumps this transport (the duty
    /// thread is joined / this IS the owner); the calling thread becomes the
    /// pump owner for the final driver drain.
    void stop() noexcept;

    /// F8c C0 — single pump owner, enforced structurally (no atomics):
    /// exactly ONE thread at a time may pump this transport's embedded driver /
    /// client conductor (dutyCycle(), and start()/stop()'s own pumps).  start()
    /// makes the calling thread the owner; a duty thread that takes over after a
    /// happens-before handoff (thread creation, join) calls bindPumpThread()
    /// first.  Debug builds abort if any other thread pumps (a second pumper);
    /// with DriverMode::Thread the driver thread is the driver's only pumper and
    /// a caller pump while it runs is counted in callerPumpViolations().
    void bindPumpThread() noexcept { pumpOwner_ = std::this_thread::get_id(); }

    /// Invoker mode: one pass of this transport's OWN embedded driver duty cycle
    /// (conductor + sender + receiver, aeron_driver.c:712-722) followed by the Aeron
    /// client conductor (useConductorAgentInvoker).  Thread mode: returns 0 — the
    /// driver thread and conductor thread own those duty cycles.
    int dutyCycle() noexcept;

    /// True once the Aeron client closed itself (inter-service timeout, driver
    /// timeout).  Offers then fail; the owner decides whether to rebuild.
    [[nodiscard]] bool closed() const noexcept {
        return conductorFault_ != nullptr || (aeron_ && aeron_->isClosed());
    }

    /// The default ingress publication's current channel (changes on retarget()).
    /// Owner thread only.
    [[nodiscard]] const std::string& ingressChannel() const noexcept { return cfg_.ingressChannel; }

    /// True when a background driver thread exists (Thread mode after start()).
    [[nodiscard]] bool usesDriverThread() const noexcept { return driverThread_.joinable(); }
    /// A7 park-until-work: this INVOKER driver's socket-readiness fds (its receiver / sender transport
    /// pollers' epoll instances, rune_driver_wake.h) into `out` (at most `max`); returns how many.  0 in
    /// Thread mode (the driver thread consumes the sockets), before start() / after stop(), or off Linux.
    /// Owned by the driver: valid until stop().  The pumping owner parks in ppoll() on them between passes.
    /// 0 as well when epoll-first polling is off (review C1: Aeron's stock poller can leave an EPOLLERR socket
    /// ready, so only epoll-first pollers, which consume every readiness, may be parked on).
    [[nodiscard]] int wakeFds(int* out, int max) const noexcept;
    /// TEST SEAM (pump thread only): poller slots an epoll_wait selected since the last reset
    /// (rune_driver_epoll_selected_for_test; 0 = the stock recvmmsg-every-socket path ran).
    [[nodiscard]] int epollSelectedForTest(bool reset) noexcept;

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
    ///
    /// Backlog B1 (CRC on accept): a PENDING `crc` names the RecordBatch CRC of
    /// a frame the producer left unsealed.  It is computed only once Aeron has
    /// accepted the frame — inside a successful tryClaim, or (frame larger than
    /// one MTU payload) inside the offer's reserved-value supplier, after Aeron's
    /// position < limit check and before the first fragment commits — and is
    /// written into the claimed term bytes AND into `buf` (the producer's
    /// retained frame, which must then be writable).  A refused offer computes
    /// nothing.  Default {}: the frame is sent as is.
    bool offer(const uint8_t* buf, std::size_t len, FrameCrc crc = {}) noexcept;

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
    bool offer(int32_t ingressStreamId, const uint8_t* buf, std::size_t len,
               FrameCrc crc = {}) noexcept;

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

    /// NAT-safe egress: the handle of a REQUEST publication to (channel, streamId) correlated with a response
    /// subscription on `responseControlChannel` (stream = the session's egressStreamId).  Two phases, both inside
    /// offerOn() and never blocking: the response subscription first (a Live SubSlot, polled like the MDC ones),
    /// then — once it is Ready — the publication on `channel|response-correlation-id=<its registration id>`.  offerOn
    /// returns false until the publication is Ready.  An empty `responseControlChannel` is publicationHandle().
    /// (channel, streamId) stays the lookup key: a plain and a correlated slot for one key never both exist — asking
    /// for a correlated one when a plain one exists returns the plain one unchanged (logged once).  Cold path.
    PubHandle responsePublicationHandle(const std::string& channel, int32_t streamId,
                                        std::string_view responseControlChannel);
    /// The publication of `h` is a response-correlated request publication whose response subscription exists
    /// (Ready or still resolving the publication).  False for a plain / unknown handle or a response subscription
    /// not yet Ready.  Cold path (session connect, bind duty).
    [[nodiscard]] bool publicationResponseCorrelated(PubHandle h) const noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return false;
        return std::holds_alternative<PubSlot::Correlated>(pubSlots_[static_cast<std::size_t>(h)].mode);
    }

    /// True iff the DEFAULT ingress publication is response-correlated (the seed's response pair exists).
    [[nodiscard]] bool seedResponseCorrelated() const noexcept { return respSeedSlot_ >= 0; }
    /// TEST SEAMS: the channel string the default ingress publication was actually registered with, and the
    /// registration id of the seed's response subscription (0 = none).
    [[nodiscard]] const std::string& seedPublicationChannelForTest() const noexcept { return ingressPubChannel_; }
    [[nodiscard]] int64_t seedResponseSubscriptionIdForTest() const noexcept {
        if (respSeedSlot_ < 0) return 0;
        const aeron::Subscription* sub = subSlots_[static_cast<std::size_t>(respSeedSlot_)].res.ready();
        return sub != nullptr ? sub->registrationId() : 0;
    }

    /// Hot path: one bounds check + one variant check + tryClaim.  Returns false
    /// while the publication resolves or is back-pressured (retry next cycle).
    bool offerOn(PubHandle h, const uint8_t* buf, std::size_t len, FrameCrc crc = {}) noexcept;

    /// client<->broker TLS/AEAD (Task 6): the reusable seal scratch for the
    /// data-plane encrypt path (ClientAgent::offerProduceToLeader_).  Owned by
    /// the transport (single-writer, same duty thread as offer/offerOn — never
    /// two seals in flight at once), so one buffer suffices across every
    /// session; grows to the largest frame sealed so far and never shrinks
    /// (zero steady-state allocation once warmed up).  Returns nullptr if
    /// minCap cannot be allocated (caller must then drop the frame).
    [[nodiscard]] uint8_t* sealScratch(std::size_t minCap) noexcept;

    /// Test/simulation seam: the clock that drives every Resolution deadline and
    /// retry backoff (default: steady_clock).  pumpUntil_'s own wall-time bound
    /// in start()/retarget() stays on the real clock.  nullptr restores default.
    void setResolveClock(int64_t (*nowNs)() noexcept) noexcept {
        resolveNow_ = nowNs != nullptr ? nowNs : &monoNs_;
    }

    /// Task 16 fix round 1 (M4): close the publication of `h` when it is Ready (an
    /// unreferenced route's old worker endpoint).  The slot stays (handles are
    /// stable); a later publicationHandle()/offerOn() on it re-resolves.  A slot
    /// still Resolving is left alone (resetting it would orphan its pending
    /// driver registration).  Cold path, owner thread.
    void releasePublication(PubHandle h) noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return;
        PubSlot& s = pubSlots_[static_cast<std::size_t>(h)];
        if (s.res.ready() != nullptr) s.res.reset();
    }
    /// The channel of `h` (nullptr for an unknown handle).  Cold path.
    [[nodiscard]] const std::string* publicationChannel(PubHandle h) const noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return nullptr;
        return &pubSlots_[static_cast<std::size_t>(h)].channel;
    }
    /// Test/diagnostics: a Ready publication exists for (channel, streamId).
    [[nodiscard]] bool publicationOpen(const std::string& channel, int32_t streamId) const noexcept {
        for (const PubSlot& s : pubSlots_) {
            if (s.streamId == streamId && s.channel == channel && s.res.ready() != nullptr) return true;
        }
        return false;
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
                namespace FrameDescriptor = aeron::concurrent::logbuffer::FrameDescriptor;
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
        if (retiredSubs_ != 0) [[unlikely]] reapRetiredSubs_();   // never inside a poll callback: here is safe
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
            if (s.state != SubSlot::State::Live) continue;   // retired (reaped next poll) or free
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
    /// F8c C0: decided from owner-thread state (the driver thread is launched /
    /// joined by the owner), not by sampling a flag the driver thread shares —
    /// deterministic, no atomic.  Read on the owner thread.
    [[nodiscard]] int callerPumpViolations() const noexcept { return callerPumpViolations_; }

    /// UDP GSO send / GRO receive installed in the embedded driver (stays reportable after stop()).
    [[nodiscard]] bool udpGsoActive() const noexcept { return (gsoActiveMask_ & 1) != 0; }
    [[nodiscard]] bool udpGroActive() const noexcept { return (gsoActiveMask_ & 2) != 0; }
    /// network_publication_max_messages_per_send the embedded driver started with (0 before start()).
    [[nodiscard]] uint32_t maxMessagesPerSend() const noexcept { return maxMessagesPerSend_; }
    /// GSO/GRO totals of every transport the driver closed; complete after stop().
    [[nodiscard]] rune_gso_totals_t gsoTotals() const noexcept { return gsoTotals_; }
    /// A7: the embedded driver polls its transports epoll-first (rune_driver_wake.h).
    [[nodiscard]] bool epollPollActive() const noexcept { return epollPollActive_; }
    /// A7 review L2: rune_driver_wake.c reads Aeron driver INTERNALS (the transport poller's epoll fd, its
    /// bindings clientd epoll_event[] array, the receiver / sender poller fields), verified against exactly
    /// this Aeron release.  Any other runtime Aeron keeps Aeron's stock polling (no wake fds: the owner
    /// backs off as before A7) and logs why — re-verify rune_driver_wake.c, then move the pin.  Same pin as
    /// the broker (rune-broker 3763230).
    static constexpr int kEpollPollAeronPin[3] = {1, 51, 0};
    [[nodiscard]] static constexpr bool epollPollSupportsAeron(int major, int minor, int patch) noexcept {
        return major == kEpollPollAeronPin[0] && minor == kEpollPollAeronPin[1] && patch == kEpollPollAeronPin[2];
    }
    /// TEST SEAM — call before start(): the driver's next `count` GSO sendmsg calls fail with errno `err`.
    void injectGsoSendErrorsForTest(int err, int count) noexcept { gsoInjectErr_ = err; gsoInjectCount_ = count; }

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
    ///
    /// NAT-safe egress: `responsePort` (the new gateway's response-control port, 0 / a non-UDP channel = none) selects
    /// the response pairing — a new response subscription FIRST, then the publication correlated with it; the old
    /// pair (publication, and the old response subscription, retired and released at the next poll) is released only
    /// after both new ones are Ready.  A failure keeps the old pair.
    bool retarget(const std::string& newIngressChannel, int32_t responsePort = 0) noexcept;

private:
    // ---- Configuration -------------------------------------------------------
    TransportConfig cfg_;

    // ---- Embedded driver (C API) --------------------------------------------
    std::optional<DriverDirLock> dirLock_;   // held from startEmbeddedDriver to the end of stopEmbeddedDriver
    aeron_driver_context_t* driverCtx_     = nullptr;
    aeron_driver_t*         driver_        = nullptr;
    rune_gso_bindings_t*    gso_           = nullptr;   // owned; outlives driverCtx_ (Aeron points at it)
    rune_gso_totals_t       gsoTotals_{};
    void*                   epollBindings_   = nullptr;   // owned stock-bindings copy (no GSO); outlives driverCtx_
    bool                    epollPollActive_ = false;
    int                     gsoActiveMask_ = 0;      // rune_gso_bindings_active(): 1 = GSO, 2 = GRO
    uint32_t                maxMessagesPerSend_ = 0;
    int                     gsoInjectErr_   = 0;      // test seam (injectGsoSendErrorsForTest)
    int                     gsoInjectCount_ = 0;
    void releaseGso_() noexcept;   // after aeron_driver_context_close
    std::thread             driverThread_;
    // F8c C0: true from the owner launching driverThread_ until it joined it —
    // owner-thread state only (the driver thread never reads it).
    bool                    driverThreadLaunched_ = false;
    // The driver thread's stop signal: a one-to-one ring, owner = producer,
    // driver thread = consumer.  Stop = producerClose() (release); the driver
    // loop runs while !producerClosed() (acquire).  The ring's own closed word is
    // the only cross-thread memory — no std::atomic in this class.
    static constexpr std::size_t kDriverCtlCapacity = 64;
    std::unique_ptr<uint8_t[]>                  driverCtlStorage_;
    std::unique_ptr<detail::OneToOneRingBuffer> driverCtl_;
    // The one thread allowed to pump (see bindPumpThread()).  Debug-checked.
    std::thread::id         pumpOwner_{};
    /// Set (once, by the duty/pump thread) when the Aeron client conductor threw: a conductor that has thrown
    /// is half-updated and is never invoked again; closed() reports it.
    std::exception_ptr      conductorFault_{};

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
    /// The channel `ingressPub_` was registered with (== cfg_.ingressChannel, plus the response-correlation-id when
    /// the seed has a response pair).  cfg_.ingressChannel stays the PLAIN identity channel.  Owner thread only.
    std::string                                  ingressPubChannel_;
    /// Index into subSlots_ of the seed's response subscription (-1 = none): the seed's rich response state.
    int32_t                                      respSeedSlot_ = -1;
    std::shared_ptr<aeron::Subscription>         egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    // ---- Lifecycle (idempotent start/stop) --------------------------------------
    // F8c C0: plain owner-thread state — start()/stop() are lifecycle calls made
    // only by the single owner (see stop()), never concurrently with a pumper.
    // #510: a start() that threw is its own state holding the original error, so a
    // later start() rethrows it instead of reporting success.
    enum class Lifecycle : uint8_t { Idle, Started, StartFailed, Stopped };
    void startBody_();   // start()'s steps; start() owns the lifecycle state around it
    Lifecycle          state_ = Lifecycle::Idle;
    std::exception_ptr startError_;   // cold: set only by a failed start()

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
                    const uint8_t* buf, std::size_t len, FrameCrc crc = {}) noexcept;
    /// B1: the frame whose CRC the reserved-value supplier seals (armed only for
    /// the duration of one multi-fragment offer; single-writer).
    struct PendingSeal {
        uint8_t*    frame = nullptr;
        std::size_t len   = 0;
        FrameCrc    crc{};
        bool        armed = false;
    };
    PendingSeal seal_{};
    /// client<->broker TLS/AEAD (Task 6): backing storage for sealScratch()
    /// above.  Grow-only (std::vector::resize never shrinks capacity on grow;
    /// we never call shrink_to_fit) — see sealScratch()'s doc comment.
    std::vector<uint8_t> cryptoSealScratch_;
    /// Aeron's offer() takes its supplier as a std::function (Publication.h);
    /// built ONCE here (captures `this`; the transport is immovable) so the hot
    /// path never constructs one.  Seals on the first (BEGIN) fragment.
    aeron::on_reserved_value_supplier_t sealSupplier_;
    std::int64_t sealInTerm_(aeron::concurrent::AtomicBuffer& term, aeron::util::index_t termOffset) noexcept;
    /// FR: coalescer for refused offers (retried every duty cycle; empty when compiled out).
    [[no_unique_address]] flight_recorder::Throttle frOfferRefused_{};

    // ---- Resolution constants ------------------------------------------------
    static constexpr int kResolveMaxMs = 5'000; // hard cap for pub/sub resolve

    // ---- Resolved-on-demand publications / subscriptions (spec §3.6) ---------
    // Single-writer (owner thread only).  Cleared in stop() before aeron_ release
    // (their dtors deregister through the client conductor).
    struct PubSlot {
        /// Response pairing (NAT-safe egress), one explicit state — never scattered flags:
        ///   Plain                  an ordinary publication on `channel`;
        ///   AwaitResponseSub{idx}  its response subscription (subSlots_[idx]) is still resolving: no publication yet;
        ///   Correlated{idx}        the publication is registered (or resolving/ready) on `addChannel`, which names
        ///                          that subscription's registration id.
        struct Plain {};
        struct AwaitResponseSub { int32_t subSlot; };
        struct Correlated { int32_t subSlot; };
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::ExclusivePublication> res;
        std::variant<Plain, AwaitResponseSub, Correlated> mode{Plain{}};
        std::string addChannel;   ///< the registered channel when Correlated (cold: built once)
    };
    struct SubSlot {
        enum class State : uint8_t { Live, Retired, Free };
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::Subscription> res;
        std::unique_ptr<aeron::FragmentAssembler> assembler;  // built on Ready
        State       state = State::Live;   ///< Retired: not polled, released by reapRetiredSubs_(); Free: reusable
    };
    /// Open (idempotently, among Live slots) a subscription slot; its index, or -1 on allocation failure.  Cold.
    int32_t addSubSlot_(const std::string& channel, int32_t streamId) noexcept;
    /// A response subscription retired by retarget() may be the one whose poll callback is running: it is only
    /// marked here and released (with its assembler) at the start of the next poll().
    void retireSubSlot_(int32_t idx) noexcept;
    void reapRetiredSubs_() noexcept;
    uint32_t retiredSubs_ = 0;   // Retired slots awaiting reapRetiredSubs_()
    bool     warnedPlainResponse_ = false;   // responsePublicationHandle: logged once
    std::vector<PubSlot>  pubSlots_;   // index == PubHandle (stable, never erased)
    detail::FlatIndexMap  streamPub_;  // default-channel streamId -> PubHandle
    std::vector<SubSlot>  subSlots_;   // per-worker dynamic-MDC egress subscriptions
    /// Advances a pending egress SubSlot; on Ready builds its assembler.
    aeron::Subscription* resolveSubSlot_(SubSlot& s) noexcept;

    static constexpr int64_t kResolveTimeoutNs = static_cast<int64_t>(kResolveMaxMs) * 1'000'000LL;
    static constexpr int64_t kResolveRetryNs   = 100'000'000LL;  // 100 ms before re-registering
};

} // namespace rune::client::transport
