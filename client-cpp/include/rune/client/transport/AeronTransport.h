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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace rune::client::transport {

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for AeronTransport.
/// dataDir is the root directory for the embedded media driver.
/// The driver's aeron dir will be dataDir/aeron (the aeronDir gotcha:
/// the aeron::Context aeronDir MUST equal the driver's aeron dir).
struct TransportConfig {
    std::string ingressChannel;   ///< Channel the client publishes to (→ gateway)
    int32_t     ingressStreamId;  ///< Stream id for ingress publication
    std::string egressChannel;    ///< Channel the client subscribes on (← gateway)
    int32_t     egressStreamId;   ///< Stream id for egress subscription
    std::string dataDir;          ///< Root dir; driver lives at dataDir/aeron
    /// Aeron term buffer length (bytes) for the embedded driver — sizes every
    /// publication/subscription log buffer on this driver.  Must be a power of
    /// two in [64 KiB, 1 GiB].  Default 4 MiB (deployments raise it via env, e.g.
    /// 32 MiB, for deep in-flight on high-throughput small-record producing;
    /// a 64 KiB buffer caps in-flight to ~50 × 1 KiB).
    std::size_t termBufferLength = 4u * 1024 * 1024;
};

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

    // -------------------------------------------------------------------------
    // I/O — single-writer, called from ONE thread only
    // -------------------------------------------------------------------------

    /// Publish buf[0..len-1] to the ingress ExclusivePublication.
    /// Uses tryClaim for zero-copy; falls back to offer if tryClaim is not
    /// supported (e.g. MTU-crossing).  Returns true iff accepted (position > 0).
    /// No busy-spin — call site retries if needed.
    bool offer(const uint8_t* buf, std::size_t len) noexcept;

    /// Poll the egress Subscription through the FragmentAssembler.
    /// For each fully reassembled frame calls handler(buf, len) with a zero-copy
    /// view.  Returns the number of fragments processed.
    int poll(const Handler& handler, int fragmentLimit);

    // -------------------------------------------------------------------------
    // Readiness
    // -------------------------------------------------------------------------

    /// True once the ingress ExclusivePublication is connected to at least one
    /// subscriber.  Safe to call before start() (returns false).
    [[nodiscard]] bool ingressConnected() const noexcept;

    /// True once the egress Subscription has at least one image (publisher
    /// connected).  Safe to call before start() (returns false).
    [[nodiscard]] bool egressConnected() const noexcept;

    // -------------------------------------------------------------------------
    // retarget — single-writer, agent-thread only
    // -------------------------------------------------------------------------

    /// Create a new ingress ExclusivePublication on newIngressChannel (same
    /// stream id as the original config) and swap it in for the current one.
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

    // ---- Aeron C++ client + pub/sub -----------------------------------------
    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::ExclusivePublication> ingressPub_;
    std::shared_ptr<aeron::Subscription>         egressSub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    // ---- Lifecycle CAS flags (idempotent stop) --------------------------------
    std::atomic<bool> started_ {false};
    std::atomic<bool> stopped_ {false};

    // ---- Poll handler pointer (set during poll(), cleared after) -------------
    // Single-writer: only one thread calls poll() so this is never concurrent.
    const Handler* pollHandler_ = nullptr;

    // ---- Resolution constants ------------------------------------------------
    static constexpr int kResolveMaxMs   = 5'000; // hard cap for pub/sub resolve
    static constexpr int kResolveSliceUs = 1'000; // sleep between polls (1 ms)
};

} // namespace rune::client::transport
