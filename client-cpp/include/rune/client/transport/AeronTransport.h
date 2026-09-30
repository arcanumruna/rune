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

#include <rune/client/transport/TransportConfig.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

namespace rune::client::transport {

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

    // -------------------------------------------------------------------------
    // I/O — single-writer, called from ONE thread only
    // -------------------------------------------------------------------------

    /// Publish buf[0..len-1] to the default ingress ExclusivePublication
    /// (cfg_.ingressStreamId).  Uses tryClaim for zero-copy; falls back to
    /// offer if tryClaim is not supported.  Returns true iff accepted.
    /// No busy-spin — call site retries if needed.
    bool offer(const uint8_t* buf, std::size_t len) noexcept;

    /// Publish buf[0..len-1] to the per-worker ingress ExclusivePublication
    /// for the given ingressStreamId.  Resolves / caches a publication for the
    /// pair (currentChannel, ingressStreamId) on first use.  Falls back to the
    /// default publication if ingressStreamId == 0 or resolution fails.
    /// E5.1: called from the ProducerImpl flush path with the per-partition
    /// worker stream id decoded from the metadata response.
    bool offer(int32_t ingressStreamId, const uint8_t* buf, std::size_t len) noexcept;

    /// Task 1.3 (multi-node): publish buf[0..len-1] to the ingress publication for
    /// an EXPLICIT (channel, ingressStreamId) pair — the per-leader-node routing
    /// primitive.  Resolves/caches a publication keyed by "channel#streamId" on the
    /// ONE shared driver (no new driver — reuses the existing multiplexing exactly
    /// like pubCache_).  This REPLACES the single-transport retarget model: instead
    /// of swapping ingressPub_ to a leader, each leader session offers directly to
    /// its own endpoint.  On resolution failure returns false (the caller retries on
    /// the next duty cycle — no busy-spin, no throw).  Agent-thread only.
    bool offerTo(const std::string& channel, int32_t ingressStreamId,
                 const uint8_t* buf, std::size_t len) noexcept;

    /// Poll the default egress Subscription AND every per-worker dynamic-MDC egress
    /// Subscription through their FragmentAssemblers.  For each fully reassembled
    /// frame calls handler(buf, len) with a zero-copy view.  Returns the total
    /// number of fragments processed across all egress subscriptions.
    int poll(const Handler& handler, int fragmentLimit);

    /// read-rework I4: open (idempotently) a per-worker dynamic-MDC egress
    /// Subscription on controlChannel + streamId so the client receives Records
    /// pushed by the OWNING worker's egress publication.  Learned from the
    /// MetadataResponse (control host = leader broker host; port/stream per
    /// partition).  Agent-thread only (single-writer, same contract as poll()).
    /// Bounded resolve; on failure it logs nothing and skips (no throw) — the
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
    /// All per-worker publications in pubCache_ are also re-resolved on the new
    /// channel; the stream ids are PRESERVED (E5.1: REDIRECT changes channel,
    /// keeps per-partition worker stream ids).
    /// Bounded resolution (kResolveMaxMs hard cap + no busy-spin).
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

    /// Pumps THIS transport's driver + conductor (Invoker) or backs off while the
    /// background agents work (Thread) until ready() or timeoutNs elapses.  Never
    /// waits on another component of the calling thread (spec §3.6).
    template <class Ready>
    bool pumpUntil_(Ready&& ready, int64_t timeoutNs);
    static int64_t monoNs_() noexcept;

    // ---- Aeron C++ client + pub/sub -----------------------------------------
    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::ExclusivePublication> ingressPub_;
    std::shared_ptr<aeron::Subscription>         egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    // ---- E5.1: per-worker ingress publication cache -------------------------
    // Maps ingressStreamId → ExclusivePublication on cfg_.ingressChannel.
    // Populated on first use in offer(streamId, buf, len); entries are rebuilt
    // on retarget() so the channel change is transparent to the caller.
    // Single-writer (agent thread only — same threading contract as offer/poll).
    std::unordered_map<int32_t, std::shared_ptr<aeron::ExclusivePublication>> pubCache_;

    // ---- Task 1.3: per-(channel,streamId) ingress publication cache ----------
    // Multi-node routing: a leader is addressed by (channel, ingressStreamId).
    // Keyed "channel#streamId" so N leader nodes multiplex over the ONE driver
    // (same pattern as pubCache_/egressSubs_ — no new driver).  Populated lazily
    // in offerTo(); NOT cleared on retarget() (retarget is the legacy single-node
    // model; multi-node addresses each node explicitly and never retargets these).
    // Single-writer (agent thread only).  Cleared in stop() before aeron_ release.
    std::unordered_map<std::string, std::shared_ptr<aeron::ExclusivePublication>>
        endpointPubCache_;

    // ---- read-rework I4: per-worker dynamic-MDC egress subscription cache -------
    // The client opens one Subscription per owning-worker control endpoint (from
    // the MetadataResponse) and polls them all in poll().  Each Subscription needs
    // its OWN FragmentAssembler — per-image reassembly state must not be shared
    // across subscriptions.  Keyed by "channel#streamId".  Single-writer (agent
    // thread only).  Cleared in stop() before the aeron client is released.
    struct EgressSubscription {
        std::shared_ptr<aeron::Subscription>      sub;
        std::unique_ptr<aeron::FragmentAssembler> assembler;
    };
    std::unordered_map<std::string, EgressSubscription> egressSubs_;

    // ---- Lifecycle CAS flags (idempotent stop) --------------------------------
    std::atomic<bool> started_ {false};
    std::atomic<bool> stopped_ {false};

    // ---- Poll handler pointer (set during poll(), cleared after) -------------
    // Single-writer: only one thread calls poll() so this is never concurrent.
    const Handler* pollHandler_ = nullptr;

    // ---- Internal helpers ----------------------------------------------------
    /// Offer buf to a specific ExclusivePublication (shared implementation).
    bool offerToPub(aeron::ExclusivePublication& pub,
                    const uint8_t* buf, std::size_t len) noexcept;

    /// Resolve or retrieve a cached ExclusivePublication for ingressStreamId on
    /// cfg_.ingressChannel.  Returns nullptr on failure (resolve timeout / not
    /// started).
    std::shared_ptr<aeron::ExclusivePublication>
    resolveWorkerPub(int32_t ingressStreamId) noexcept;

    /// Task 1.3: resolve or retrieve a cached ExclusivePublication for an explicit
    /// (channel, ingressStreamId) pair.  Returns nullptr on failure.
    std::shared_ptr<aeron::ExclusivePublication>
    resolveEndpointPub(const std::string& channel, int32_t ingressStreamId) noexcept;

    // ---- Resolution constants ------------------------------------------------
    static constexpr int kResolveMaxMs   = 5'000; // hard cap for pub/sub resolve
    static constexpr int kResolveSliceUs = 1'000; // sleep between polls (1 ms)
};

} // namespace rune::client::transport
