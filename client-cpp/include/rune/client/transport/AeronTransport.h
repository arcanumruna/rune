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

#include <array>
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

    /// TEST SEAM (cold; owner thread): while stalled, dutyCycle() pumps nothing, so a registration sent meanwhile stays
    /// Resolving; a retarget's publication resolve bound is shortened to `boundNs` (resolve clock) so a stalled
    /// retarget fails promptly.  Stall off / bound 0 restores the defaults.  The driver is parked aside rather than
    /// flagged, so dutyCycle()'s existing `driver_ == nullptr` check stalls it and the hot path carries no test-only
    /// branch.
    void setDriverStalledForTest(bool stalled, int64_t retargetBoundNs = 0) noexcept {
        if (stalled && driver_ != nullptr) { stalledDriver_ = driver_; driver_ = nullptr; }
        if (!stalled && stalledDriver_ != nullptr) { driver_ = stalledDriver_; stalledDriver_ = nullptr; }
        retargetPubBoundNs_ = retargetBoundNs > 0 ? retargetBoundNs : kResolveTimeoutNs;
    }

    /// True once the Aeron client closed itself (inter-service timeout, driver
    /// timeout), or while a failed rebuild() left no driver and no client.  Offers then fail;
    /// the owner decides whether to rebuild.
    [[nodiscard]] bool closed() const noexcept {
        return state_ == Lifecycle::Down || conductorFault_ != nullptr || (aeron_ && aeron_->isClosed());
    }

    /// The Aeron client closed (closed()) or the owner wants a fresh one: every Aeron object is released in stop()'s
    /// order (BR-3b), the embedded driver is closed, and a fresh driver + client are started; the seed pair is resolved
    /// again on the CURRENT ingress channel (where a failover moved it) with `seedResponsePort` (the seed's current
    /// response-control port).  Every PubHandle and sub slot keeps its key and re-resolves lazily; a response pair gets a
    /// fresh subscription (the old one was pinned to a dead image).  An attached transport (cfg.aeronDir) leaves the
    /// external driver alone and only reconnects.  true = up again; false = still closed (Down, cause logged), the owner
    /// retries after its backoff.  Blocking and bounded like start(); owner thread only, never from a poll() callback.
    bool rebuild(int32_t seedResponsePort) noexcept;

    /// The default ingress publication's current channel (changes when a retarget completes).
    /// Owner thread only.
    [[nodiscard]] const std::string& ingressChannel() const noexcept { return cfg_.ingressChannel; }

    /// True when a background driver thread exists (Thread mode after start()).
    [[nodiscard]] bool usesDriverThread() const noexcept { return driverThread_.joinable(); }
    /// A7 park-until-work: this INVOKER driver's socket-readiness fds (its receiver / sender transport
    /// pollers' epoll instances, rune_driver_wake.h) into `out` (at most `max`); returns how many.  0 in
    /// Thread mode (the driver thread consumes the sockets), before start() / after stop(), or off Linux.
    /// Owned by the driver: valid until the next rebuild() or stop(): re-query before each park.  The pumping owner parks in ppoll() on them between passes.
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
    /// I6: like responsePublicationHandle, but a PLAIN publication already on the key is converted to a
    /// response-correlated one (released and re-registered once its response subscription is Ready).  Cold.
    PubHandle upgradeToResponsePublication(const std::string& channel, int32_t streamId,
                                           std::string_view responseControlChannel);
    PubHandle responsePublicationHandle(const std::string& channel, int32_t streamId,
                                        std::string_view responseControlChannel);
    /// #751: release a response-paired publication whose last user is gone: its response subscription retires (#764),
    /// its request publication is released or drained (#763), and the slot becomes Released (handles are stable; the
    /// next responsePublicationHandle / publicationHandle on the same key re-inits it).  A plain or unknown handle is
    /// left alone.  Cold path, owner thread.
    void releaseResponsePublication(PubHandle h) noexcept;
    /// #751 review M1: the pair's last bind reference is gone but a plain route still offers on this slot: keep it as a
    /// plain publication.  The response subscription retires, the correlated request publication is released or
    /// drained (#763), and the slot (mode Plain, fresh Resolution) re-resolves lazily on the next offerOn /
    /// publicationHandle.  Plain or Released: no-op.  Cold path, owner thread.
    void demoteResponsePublication(PubHandle h) noexcept;
    [[nodiscard]] bool publicationReleasedForTest(PubHandle h) const noexcept {
        return h >= 0 && static_cast<std::size_t>(h) < pubSlots_.size() &&
               std::holds_alternative<PubSlot::Released>(pubSlots_[static_cast<std::size_t>(h)].mode);
    }
    /// TEST SEAM: publication slots that are not Released (a leak shows as growth).
    [[nodiscard]] std::size_t activePublicationSlotsForTest() const noexcept {
        std::size_t n = 0;
        for (const PubSlot& s : pubSlots_) if (!std::holds_alternative<PubSlot::Released>(s.mode)) ++n;
        return n;
    }
    [[nodiscard]] std::size_t pubSlotCountForTest() const noexcept { return pubSlots_.size(); }
    /// The publication of `h` is a response-correlated request publication whose response subscription exists
    /// (Ready or still resolving the publication).  False for a plain / unknown handle or a response subscription
    /// not yet Ready.  Cold path (session connect, bind duty).
    [[nodiscard]] bool publicationResponseCorrelated(PubHandle h) const noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return false;
        return std::holds_alternative<PubSlot::Correlated>(pubSlots_[static_cast<std::size_t>(h)].mode);
    }

    /// True iff the DEFAULT ingress publication is response-correlated (the seed's response pair exists).
    [[nodiscard]] bool seedResponseCorrelated() const noexcept { return respSeedSlot_ >= 0; }
    /// TEST SEAM (owner thread, no I/O): move the seed's PHYSICAL ingress identity, as a completed retarget does.
    void setIngressChannelForTest(const std::string& channel) { cfg_.ingressChannel = channel; }
    /// TEST SEAM (call before start()): the seed's response subscription channel start() registers, in place of the one
    /// derived from the ingress channel and the response port; a channel the driver refuses forces the fallback to the
    /// declared path.  Empty = off.
    void setSeedResponseChannelForTest(std::string channel) { seedResponseChannelForTest_ = std::move(channel); }
    [[nodiscard]] bool publicationIsResponsePairedForTest(PubHandle h) const noexcept {   // awaiting or correlated
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return false;
        return !std::holds_alternative<PubSlot::Plain>(pubSlots_[static_cast<std::size_t>(h)].mode);
    }
    /// TEST SEAMS: the channel string the default ingress publication was actually registered with, and the
    /// registration id of the seed's response subscription (0 = none).
    /// TEST SEAMS (owner thread; cold): the response subscription slot of the seed pair, its Live state and owner
    /// count (-1 / false / 0 when none).
    [[nodiscard]] int32_t seedResponseSlotForTest() const noexcept { return respSeedSlot_; }
    [[nodiscard]] bool subSlotLiveForTest(int32_t slot) const noexcept {
        return slot >= 0 && static_cast<std::size_t>(slot) < subSlots_.size() &&
               subSlots_[static_cast<std::size_t>(slot)].state == SubSlot::State::Live;
    }
    [[nodiscard]] uint32_t subSlotMdcRefsForTest(int32_t slot) const noexcept {
        return slot >= 0 && static_cast<std::size_t>(slot) < subSlots_.size()
                   ? subSlots_[static_cast<std::size_t>(slot)].mdcRefs : 0;
    }
    /// TEST SEAMS (cold): the Live shared-MDC slot for (channel, stream) or -1; the response subscription slot a
    /// response-paired publication names (-1 = none); and a release of one owner of a slot (what a user's release does).
    [[nodiscard]] int32_t mdcSubSlotForTest(const std::string& channel, int32_t streamId) const noexcept {
        for (std::size_t i = 0; i < subSlots_.size(); ++i)
            if (subSlots_[i].state == SubSlot::State::Live && subSlots_[i].kind == SubSlot::Kind::SharedMdc &&
                subSlots_[i].streamId == streamId && subSlots_[i].channel == channel) return static_cast<int32_t>(i);
        return -1;
    }
    [[nodiscard]] int32_t responseSubSlotForTest(PubHandle h) const noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return -1;
        const auto& m = pubSlots_[static_cast<std::size_t>(h)].mode;
        if (const auto* a = std::get_if<PubSlot::AwaitResponseSub>(&m)) return a->subSlot;
        if (const auto* c = std::get_if<PubSlot::Correlated>(&m)) return c->subSlot;
        return -1;
    }
    [[nodiscard]] std::string responseSubChannelForTest(PubHandle h) const {
        const int32_t slot = responseSubSlotForTest(h);
        return slot < 0 ? std::string{} : subSlots_[static_cast<std::size_t>(slot)].channel;
    }
    void releaseSubSlotOwnerForTest(int32_t slot) noexcept { retireSubSlot_(slot); }
    /// TEST SEAMS (cold): publication Resolutions parked by drainPub_ (#763); the registration id of the response
    /// subscription a response-paired handle names (0 = none Ready); and the driver's publication-limit counters (one
    /// per live publication) whose label ends with `channelSuffix` (the registered channel, e.g. a plain endpoint).
    [[nodiscard]] std::size_t drainingPubsForTest() const noexcept { return drainingPubs_.size(); }
    [[nodiscard]] bool subSlotFreeForTest(int32_t slot) const noexcept {
        return slot >= 0 && static_cast<std::size_t>(slot) < subSlots_.size() &&
               subSlots_[static_cast<std::size_t>(slot)].state == SubSlot::State::Free;
    }
    [[nodiscard]] int64_t responseSubscriptionIdForTest(PubHandle h) const noexcept {
        const int32_t slot = responseSubSlotForTest(h);
        if (slot < 0) return 0;
        const aeron::Subscription* sub = subSlots_[static_cast<std::size_t>(slot)].res.ready();
        return sub != nullptr ? sub->registrationId() : 0;
    }
    [[nodiscard]] int publisherLimitCountersForTest(const std::string& channelSuffix) const {
        int n = 0;
        if (!aeron_) return 0;
        aeron_->countersReader().forEach([&](std::int32_t, std::int32_t typeId, const aeron::concurrent::AtomicBuffer&,
                                             const std::string& label) {
            constexpr std::int32_t kPublisherLimitTypeId = 1;   // AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID
            if (typeId == kPublisherLimitTypeId && label.size() >= channelSuffix.size() &&
                label.compare(label.size() - channelSuffix.size(), channelSuffix.size(), channelSuffix) == 0) ++n;
        });
        return n;
    }
    /// TEST SEAM: the sub slot table's size (slots are never erased: a released one is Retired, then Free).
    [[nodiscard]] std::size_t subSlotCountForTest() const noexcept { return subSlots_.size(); }
    [[nodiscard]] const std::string& seedPublicationChannelForTest() const noexcept { return ingressPubChannel_; }
    /// TEST SEAMS (cold): fresh response subscriptions registered since start (a re-pair adds one), and the identity of
    /// the publication a handle currently holds (null = none Ready), to prove a slot was left alone.
    [[nodiscard]] uint64_t responseSubsAddedForTest() const noexcept { return freshSubsAdded_; }
    [[nodiscard]] const void* publicationIdentityForTest(PubHandle h) const noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return nullptr;
        return pubSlots_[static_cast<std::size_t>(h)].res.ready();
    }
    [[nodiscard]] bool seedRePairingForTest() const noexcept { return std::holds_alternative<SeedRePairing>(seedPair_); }
    /// TEST SEAM: the existing slot for (channel, stream) or kNoPub - never creates one.
    [[nodiscard]] PubHandle findPublicationForTest(const std::string& channel, int32_t streamId) const noexcept {
        for (std::size_t i = 0; i < pubSlots_.size(); ++i)
            if (pubSlots_[i].streamId == streamId && pubSlots_[i].channel == channel) return static_cast<PubHandle>(i);
        return kNoPub;
    }
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
    /// in start() stays on the real clock.  nullptr restores default.
    void setResolveClock(int64_t (*nowNs)() noexcept) noexcept {
        resolveNow_ = nowNs != nullptr ? nowNs : &monoNs_;
    }

    /// Task 16 fix round 1 (M4): close the publication of `h` when it is Ready (an
    /// unreferenced route's old worker endpoint).  The slot stays (handles are
    /// stable); a later publicationHandle()/offerOn() on it re-resolves.  A slot
    /// still Resolving/TimedOut is parked in the drain set (#763 sibling: resetting
    /// it would orphan its pending driver registration) and the slot starts afresh.
    /// Cold path, owner thread.
    void releasePublication(PubHandle h) noexcept {
        if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return;
        PubSlot& s = pubSlots_[static_cast<std::size_t>(h)];
        drainPub_(std::move(s.res));
        s.res = Resolution<aeron::ExclusivePublication>{kResolveTimeoutNs, kResolveRetryNs};
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
        // #763: registrations whose owner left them pending
        if (!drainingPubs_.empty()) [[unlikely]] serviceDraining_();
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

    /// Move the seed's default ingress publication to newIngressChannel (same stream id as the original config).  The
    /// move is a STATE on the duty thread, never a blocking call (#767): beginRetarget only registers the new response
    /// subscription (when the channel has a response control) and returns Pending; serviceRetarget(), once per duty
    /// cycle from the owner, resolves the response subscription, then the response-correlated publication (each bounded
    /// by the resolve clock: kResolveTimeoutNs for the subscription, retargetPubBoundNs_ for the publication), and
    /// swaps when both are Ready.  Per-stream publications of offer(streamId) are lazily re-resolved on the new channel
    /// (E5.1: REDIRECT changes channel, keeps per-partition worker stream ids).  Nothing here pumps the driver or the
    /// conductor and nothing sleeps.
    ///
    /// Meanwhile the OLD pair stays in force but quiet: offer()/offerResult() on the default publication report "not
    /// sent" (Retry), exactly as for a not-connected publication, so nothing reaches the old gateway; offerTo(channel,
    /// ...) (leader sessions) is unaffected and ingressChannel() keeps the old value until the swap.  A failed resolve
    /// (rejected / past its bound) leaves the old pair intact and usable.  Either way one outcome {token, ok} is
    /// queued for takeRetargetOutcome().  A newer beginRetarget to ANOTHER target supersedes a pending one (its outcome
    /// is {token, false} and its resources are released first); one to the SAME target (channel and response port)
    /// joins it: no new registration, the pending move's token is returned.  It also abandons a pending seed re-pair.
    ///
    /// NAT-safe egress: `responsePort` (the new gateway's response-control port, 0 / a non-UDP channel = none) selects
    /// the response pairing — a new response subscription FIRST, then the publication correlated with it; the old
    /// pair (publication, and the old response subscription, retired and released at the next poll) is released only
    /// after both new ones are Ready.
    ///
    /// Failed = not started / no slot / an exception (old pair intact, no outcome queued).  Called only from the agent
    /// thread (single-writer contract); noexcept (reached from the agent's noexcept dispatch path).
    enum class RetargetStart : uint8_t { Pending, Failed };
    struct RetargetOutcome {
        uint64_t token = 0;
        bool     ok    = false;
    };
    RetargetStart beginRetarget(const std::string& newIngressChannel, int32_t responsePort, uint64_t& token) noexcept;
    /// Advance a pending retarget by one step; a no-op (one predictable branch) when none is pending.  Once per duty
    /// cycle, from the owner, outside any poll callback.
    void serviceRetarget() noexcept {
        if (seedPair_.index() == kSeedRetargetingIndex) [[unlikely]] serviceRetarget_();
    }
    /// The oldest undelivered retarget outcome, if any.
    [[nodiscard]] std::optional<RetargetOutcome> takeRetargetOutcome() noexcept;
    /// Abandon the pending retarget, if any (a request to stay where the seed is): its outcome is {token, false},
    /// what it registered is released, the old pair stays.
    void cancelRetarget() noexcept;
    [[nodiscard]] bool retargetOutcomePending() const noexcept { return retargetOutcomeCount_ != 0; }
    [[nodiscard]] bool seedRetargetPending() const noexcept { return seedPair_.index() == kSeedRetargetingIndex; }
    /// TEST SEAM: outcomes dropped because 4 were undrained (cannot happen with one pending move at a time).
    [[nodiscard]] uint64_t retargetOutcomesDroppedForTest() const noexcept { return retargetOutcomesDropped_; }
    /// TEST SEAM: beginRetarget calls that joined the move already pending to the same target.
    [[nodiscard]] uint64_t retargetsJoinedForTest() const noexcept { return retargetsJoined_; }
    /// TEST SEAM: conductor / driver pump steps taken so far (dutyCycle() calls that pumped); a call that must not
    /// block shows no change across it.
    [[nodiscard]] uint64_t dutyCyclePumpsForTest() const noexcept { return dutyCyclePumps_; }
    /// A response subscription is ONE-SHOT: Aeron pins it to the first response publication session that reaches it, so
    /// a second response publication to the same request image (a reconnect, a rebind of a new session) or to ANOTHER
    /// request publication correlated with the same subscription can never connect.  Every response pair therefore owns
    /// its subscription (never shared, never deduplicated), and every new connect episode / session bind builds a FRESH
    /// pair (new response subscription + new request publication, hence a new image).  rePairSeedResponse: the seed's
    /// pair; rePairResponsePublication: the pair of the handle `h` (the handle stays valid and offerOn re-resolves it).
    /// Both cold; false = not response-paired / failed (the old pair is intact).
    /// Asynchronous and cold: only registers the new response subscription (never pumps, never sleeps, never touches
    /// another publication); the following offer() calls build the new request publication once it is Ready and swap
    /// the pair (see SeedRePairing).  True = a re-pair is in flight (or already was); false = nothing to pair.
    bool rePairSeedResponse(int32_t responsePort) noexcept;
    bool rePairResponsePublication(PubHandle h) noexcept;
    /// #862: the same, onto another response control channel (the peer's response port moved).
    bool rePairResponsePublication(PubHandle h, std::string_view responseControlChannel) noexcept;
    /// #989: the gateway's response publication of the seed's pair in force / of the pair of handle `h` connected and
    /// then closed (the gateway closed it or died: its image went unavailable).  True once per pair, at that edge.  A restarted gateway can never reach
    /// that subscription again (see above), so its session is gone: the caller reconnects, as Aeron Cluster's client
    /// does when its egress image closes (AeronCluster.pollEgress).  False while a seed re-pair is in flight.  Hot-path
    /// safe: one image check of one subscription.
    bool seedResponseEgressLost() noexcept;
    bool responseEgressLost(PubHandle h) noexcept;

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
    aeron_driver_t*  stalledDriver_       = nullptr;  // test seam: the driver parked by setDriverStalledForTest
    // the retarget's publication resolve bound (resolve clock); test-shortened
    int64_t          retargetPubBoundNs_   = kResolveTimeoutNs;
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
    std::string                                  seedResponseChannelForTest_;   // test seam, see setSeedResponseChannelForTest
    /// Index into subSlots_ of the seed's response subscription (-1 = none): the seed's rich response state.
    int32_t                                      respSeedSlot_ = -1;
    /// The seed pair's re-pair state, one explicit state (mirrors PubSlot::AwaitResponseSub -> Correlated):
    ///   SeedSteady     the pair in force (ingressPub_ + respSeedSlot_) is the only one;
    ///   SeedRePairing  a fresh response subscription (subSlots_[subSlot]) is resolving, then the request publication
    ///                  correlated with it on `addChannel`; offer() advances it and swaps when both are Ready.
    ///   SeedRetargeting the seed moves to ANOTHER channel (#767): see beginRetarget().
    struct SeedSteady {};
    struct SeedRePairing {
        int32_t     subSlot;
        std::string addChannel;
        Resolution<aeron::ExclusivePublication> res;
        int64_t     deadlineNs;   // #755: the re-pair is abandoned (old pair kept) at this resolve-clock time
    };
    struct SeedRetargeting {
        std::string channel;           // the new plain ingress channel (becomes cfg_.ingressChannel)
        int32_t     responsePort;
        int32_t     newRespSlot;       // -1 = no response pairing
        std::string pubChannel;        // the channel the publication registers with (correlated once the sub is Ready)
        Resolution<aeron::ExclusivePublication> pubRes;
        bool        awaitingSub;       // true: the response subscription resolves first; false: the publication
        int64_t     stageStartNs;      // resolve-clock time the current stage began
        uint64_t    token;
    };
    static constexpr std::size_t kSeedRetargetingIndex = 2;
    std::variant<SeedSteady, SeedRePairing, SeedRetargeting> seedPair_{SeedSteady{}};
    uint64_t retargetsJoined_ = 0;
    void serviceRetarget_() noexcept;
    void completeRetarget_(SeedRetargeting& rt) noexcept;
    /// Leave SeedRetargeting: queue {token, ok}; on failure release what the move registered (the old pair stays).
    void finishRetarget_(bool ok) noexcept;
    void queueRetargetOutcome_(uint64_t token, bool ok) noexcept;
    static constexpr std::size_t kMaxRetargetOutcomes = 4;
    RetargetOutcome retargetOutcomes_[kMaxRetargetOutcomes]{};
    std::size_t     retargetOutcomeHead_  = 0;
    std::size_t     retargetOutcomeCount_ = 0;
    uint64_t        retargetOutcomesDropped_ = 0;
    uint64_t        retargetTokens_ = 0;
    uint64_t        dutyCyclePumps_ = 0;
    /// Advance a pending seed re-pair; true = swapped (the new pair is in force), false = still pending or abandoned.
    bool advanceSeedRePair_(SeedRePairing& rp) noexcept;
    /// Drop a pending seed re-pair: its subscription slot is retired, the old pair stays in force.
    void abandonSeedRePair_() noexcept;
    std::shared_ptr<aeron::Subscription>         egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    // ---- Lifecycle (idempotent start/stop) --------------------------------------
    // F8c C0: plain owner-thread state — start()/stop() are lifecycle calls made
    // only by the single owner (see stop()), never concurrently with a pumper.
    // #510: a start() that threw is its own state holding the original error, so a
    // later start() rethrows it instead of reporting success.
    // Down: started, but the last rebuild() failed (no driver, no client); only rebuild() and stop() apply.
    enum class Lifecycle : uint8_t { Idle, Started, StartFailed, Down, Stopped };
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
        struct AwaitResponseSub { int32_t subSlot; int64_t deadlineNs; };   // #755: gives up at deadlineNs (resolve clock)
        struct Correlated { int32_t subSlot; };
        /// #751: the owner released the pair (releaseResponsePublication): no publication, no subscription, offers fail.
        /// The slot keeps its key so a later responsePublicationHandle / publicationHandle on it re-inits it in place.
        struct Released {};
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::ExclusivePublication> res;
        std::variant<Plain, AwaitResponseSub, Correlated, Released> mode{Plain{}};
        std::string addChannel;   ///< the registered channel when Correlated (cold: built once)
    };
    struct SubSlot {
        enum class State : uint8_t { Live, Retired, Free };
        /// What the slot is, one explicit state (#764): SharedMdc is deduplicated among Live slots and refcounted
        /// (`mdcRefs` users); ResponsePair is ONE-SHOT (Aeron pins it to the first response session), never shared,
        /// has exactly one owner and no count, and retires on its own release.
        enum class Kind : uint8_t { SharedMdc, ResponsePair };
        std::string channel;
        int32_t     streamId;
        Resolution<aeron::Subscription> res;
        std::unique_ptr<aeron::FragmentAssembler> assembler;  // built on Ready
        Kind        kind = Kind::SharedMdc;
        uint32_t    mdcRefs = 0;   ///< SharedMdc only: users of this Live slot (addSubSlot_ ++; retireSubSlot_ --, Retired at 0)
        State       state = State::Live;   ///< Retired: not polled, released by reapRetiredSubs_(); Free: reusable
        /// #989, ResponsePair only: the gateway's response publication seen from here.  AwaitImage = none connected yet;
        /// Connected = one was; Lost = it closed after that (terminal: Aeron pins this subscription to that one
        /// publication, so nothing can reach it again).  Advanced by responseEgressLost_().
        enum class Egress : uint8_t { AwaitImage, Connected, Lost };
        Egress      egress = Egress::AwaitImage;
    };
    /// #989: advance subSlots_[idx]'s Egress state; true exactly once, at Connected -> Lost.  -1 / not Ready / not a
    /// response pair = false.
    bool responseEgressLost_(int32_t idx) noexcept;
    /// Open a subscription slot; its index, or -1 on allocation failure.  Cold.  SharedMdc is idempotent among Live
    /// SharedMdc slots (one more user); ResponsePair always opens a fresh slot.  Every successful call is one OWNER of
    /// the slot and must be matched by one retireSubSlot_ on its release path.
    int32_t addSubSlot_(SubSlot::Kind kind, const std::string& channel, int32_t streamId) noexcept;
    /// A response subscription retired by retarget() may be the one whose poll callback is running: it is only
    /// marked here and released (with its assembler) at the start of the next poll().
    void retireSubSlot_(int32_t idx) noexcept;
    void reapRetiredSubs_() noexcept;
    /// Release the slot's subscription and assembler now (rebuild() / a failed start()): the slot is Free.
    void freeSubSlot_(SubSlot& s) noexcept;
    /// #763: a publication Resolution that leaves its owner goes through here, never `reset()`.  Ready: released now.
    /// Resolving / TimedOut: its driver registration is still pending and would later complete with nobody to release
    /// it, so it is parked in drainingPubs_ and polled (serviceDraining_, from poll()) until it is Ready (released) or
    /// Rejected (dropped).  Unstarted / Rejected: nothing registered.  Cold, never blocks.  Every pending
    /// registration is tracked (the set grows by allocation on this cold path; only an allocation failure drops one,
    /// logged once).
    void drainPub_(Resolution<aeron::ExclusivePublication>&& r) noexcept;
    void serviceDraining_() noexcept;
    static constexpr std::size_t kMaxDraining = 16;   // capacity reserved up front, not a cap
    std::vector<Resolution<aeron::ExclusivePublication>> drainingPubs_;
    bool        warnedDrainOverflow_ = false;   // drainPub_ could not allocate: logged once
    uint32_t retiredSubs_ = 0;   // Retired slots awaiting reapRetiredSubs_()
    uint64_t rebuildFailures_ = 0;   // failed rebuild() attempts; logged on the first and every 64th
    uint64_t freshSubsAdded_ = 0;   // fresh (response-pair) subscription slots added; test seam
    bool     warnedPlainResponse_ = false;   // responsePublicationHandle: logged once
    bool     warnedResponseGiveUp_ = false;   // offerOn fell back from a response pair to plain: logged once
    std::vector<PubSlot>  pubSlots_;   // index == PubHandle (stable, never erased)
    detail::FlatIndexMap  streamPub_;  // default-channel streamId -> PubHandle
    std::vector<SubSlot>  subSlots_;   // per-worker dynamic-MDC egress subscriptions
    /// Advances a pending egress SubSlot; on Ready builds its assembler.
    aeron::Subscription* resolveSubSlot_(SubSlot& s) noexcept;

    static constexpr int64_t kResolveTimeoutNs = static_cast<int64_t>(kResolveMaxMs) * 1'000'000LL;
    static constexpr int64_t kResolveRetryNs   = 100'000'000LL;  // 100 ms before re-registering
};

} // namespace rune::client::transport
