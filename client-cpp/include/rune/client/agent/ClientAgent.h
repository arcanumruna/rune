// include/rune/client/agent/ClientAgent.h
//
// CC-5 — Single-writer client agent duty-cycle.
//
// Wires CC-3 AeronTransport + CC-4 SessionMachine into a single duty-cycle
// driven by doWork(nowNs).  App threads submit commands (connect / disconnect /
// close / send) via a lock-free ManyToOneRingBuffer; the agent thread drains
// the ring, polls transport egress, and ticks the session machine.
//
// Threading model — SINGLE-WRITER:
//   The ring drain + transport poll + session tick ALL run on ONE thread
//   (the doWork caller).  App threads ONLY touch the ManyToOneRingBuffer.
//   No locks protect session_ or transport_ — the ring is the sync primitive.
//
// doWork contract:
//   Returns total work count (ring records + fragments + session tick).
//   Returns 0 when nothing happened (idle); the caller's IdleStrategy backs
//   off.  No busy-spin on the agent side.
//
// Egress dispatch order:
//   checkFrame → peekTemplate → decode → route:
//     ConnectResponse → session_.onConnectResponse + onActivity + observer
//     ErrorResponse   → session_.onErrorResponse   + onActivity + observer
//     Pong            → session_.onActivity         + observer
//     unknown/bad     → drop (never crash)

#pragma once

#include <rune/client/transport/AeronTransport.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/client/codec/Codec.h>

#include <concurrent/AtomicBuffer.h>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/ringbuffer/RingBufferDescriptor.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace rune::client::agent {

// ---------------------------------------------------------------------------
// AgentConfig — full configuration for the ClientAgent.
// ---------------------------------------------------------------------------
struct AgentConfig {
    // Transport configuration (injected into AeronTransport).
    transport::TransportConfig transport;

    // Session configuration (injected into SessionMachine).
    session::SessionConfig session;

    // Ring buffer capacity — must be a power of two (bytes of usable space).
    // Default: 64 KiB of usable space.
    int32_t ringCapacity = 64 * 1024;

    // Maximum command records to drain from the ring per doWork call (bounded).
    int ringDrainLimit = 16;

    // Maximum fragments to poll from egress per doWork call (bounded).
    int egressPollLimit = 16;
};

// ---------------------------------------------------------------------------
// ClientAgent
// ---------------------------------------------------------------------------

class ClientAgent : public session::SessionIo {
public:
    // Egress observer: called for every decoded egress frame from the gateway.
    // (ConnectResponse, ErrorResponse, Pong — plus any others you forward.)
    // Signature: (templateId, raw frame bytes, frame length)
    using EgressObserver = std::function<void(
        codec::TemplateId, const uint8_t*, std::size_t)>;

    // -------------------------------------------------------------------------
    // Construction / destruction
    // -------------------------------------------------------------------------

    explicit ClientAgent(AgentConfig cfg);
    ~ClientAgent() override;

    // Non-copyable, non-movable (owns transport, ring buffer).
    ClientAgent(const ClientAgent&)            = delete;
    ClientAgent& operator=(const ClientAgent&) = delete;
    ClientAgent(ClientAgent&&)                 = delete;
    ClientAgent& operator=(ClientAgent&&)      = delete;

    // -------------------------------------------------------------------------
    // Lifecycle (single-writer; call from the agent thread or before threads start)
    // -------------------------------------------------------------------------

    /// Start the underlying AeronTransport (embedded driver, pub/sub resolution).
    void start();

    /// Stop the agent (idempotent).  Stops transport.  Dtor calls stop.
    void stop() noexcept;

    // -------------------------------------------------------------------------
    // doWork — single-writer duty cycle
    // -------------------------------------------------------------------------

    /// Drive one duty-cycle iteration.
    ///  1. Drain the command ring (bounded).
    ///  2. Poll transport egress (bounded).
    ///  3. Tick the session machine.
    /// Returns total work count (0 = nothing happened → IdleStrategy backs off).
    /// Must be called from ONE thread only (the agent thread).
    int doWork(int64_t nowNs) noexcept;

    // -------------------------------------------------------------------------
    // Thread-safe submission API (callable from ANY thread)
    // -------------------------------------------------------------------------

    /// Submit a CONNECT command.  Returns false if the ring is full.
    bool submitConnect() noexcept;

    /// Submit a DISCONNECT command.  Returns false if the ring is full.
    bool submitDisconnect() noexcept;

    /// Submit a CLOSE command.  Returns false if the ring is full.
    bool submitClose() noexcept;

    /// Submit a pre-encoded frame for transmission.  Returns false if ring full.
    /// The frame bytes are copied into the ring (the caller's buffer is safe to
    /// reuse immediately after this call returns).
    bool submitSend(const uint8_t* frame, std::size_t len) noexcept;

    // -------------------------------------------------------------------------
    // Observers
    // -------------------------------------------------------------------------

    /// Set (replace) the egress observer.  Safe to call before start().
    /// Called from the agent thread only once the agent is running.
    void setEgressObserver(EgressObserver obs) noexcept;

    /// Delegate to SessionMachine::state().
    [[nodiscard]] session::SessionState sessionState() const noexcept;

    /// Delegate to SessionMachine::sessionId().
    [[nodiscard]] int64_t sessionId() const noexcept;

private:
    // -------------------------------------------------------------------------
    // SessionIo implementation (called only on the agent thread)
    // -------------------------------------------------------------------------
    bool sendFrame(const uint8_t* buf, std::size_t len) override;
    void retarget(const std::string& newIngressChannel) override;

    // -------------------------------------------------------------------------
    // Command message type IDs for the ring
    // -------------------------------------------------------------------------
    static constexpr int32_t kMsgConnect    = 1;
    static constexpr int32_t kMsgDisconnect = 2;
    static constexpr int32_t kMsgClose      = 3;
    static constexpr int32_t kMsgSend       = 4;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    bool writeCommand(int32_t msgTypeId, const uint8_t* payload, std::size_t payloadLen) noexcept;
    void dispatchEgressFrame(int64_t nowNs, const uint8_t* buf, std::size_t len) noexcept;

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    AgentConfig cfg_;

    // ---- Transport + session (agent-thread owned, single-writer) ------------
    transport::AeronTransport transport_;
    session::SessionMachine   session_;

    // ---- Ring buffer (MPSC: many app threads write, one agent thread reads) -
    // Buffer size = power-of-two capacity + trailer.
    // Stored as aligned raw bytes; AtomicBuffer wraps it.
    static constexpr std::size_t kRingAlignment = 64; // cache-line aligned
    std::unique_ptr<uint8_t, void(*)(void*)> ringBuf_;
    aeron::concurrent::AtomicBuffer          ringAtomicBuf_;
    aeron::concurrent::ringbuffer::ManyToOneRingBuffer ring_;

    // ---- Lifecycle -----------------------------------------------------------
    std::atomic<bool> started_ {false};
    std::atomic<bool> stopped_ {false};

    // ---- Egress observer (agent thread only after construction) --------------
    EgressObserver egressObserver_;

    // ---- Scratch buffer for ring write of send payloads ---------------------
    // AtomicBuffer requires a non-const buffer pointer; we wrap a stack frame
    // at write time using this pattern (the ring copies the bytes internally).
};

} // namespace rune::client::agent
