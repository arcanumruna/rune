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
//   Returns total work count (ring records drained + egress fragments polled).
//   Session ticks are always performed but not counted as work.
//   Returns 0 when nothing happened (idle); the caller's IdleStrategy backs
//   off.  No busy-spin on the agent side.
//
// Egress dispatch order:
//   checkFrame → peekTemplate → decode → route:
//     ConnectResponse → session_.onConnectResponse + onActivity + observer
//     ErrorResponse   → session_.onErrorResponse   + onActivity + observer
//     Pong            → session_.onActivity         + observer
//     ProduceResponse → producerHub_.onProduceResponse  (CB-2)
//     unknown/bad     → drop (never crash)

#pragma once

#include <rune/client/transport/AeronTransport.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/client/codec/Codec.h>

#include <concurrent/AtomicBuffer.h>
#include <rune/client/detail/FlatIndexMap.h>
#include <rune/client/detail/M2ORingBuffer.h> // internal MPSC ring (templated read, no std::function)

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// CB-2 forward declaration — allow Client to access ring_ for ProducerImpl.
// ---------------------------------------------------------------------------
namespace rune::client { class Client; }

namespace rune::client::agent {

// ---------------------------------------------------------------------------
// Session — endpoint-keyed per-node session state (multi-node Task 1.1).
//
// Bundles the state that is specific to ONE broker node/leader:
//   - the SessionMachine (connect/keepalive/redirect FSM for that node), and
//   - that node's data-op redirect state (redirectPending / currentLeader*).
//
// The single AeronTransport is SHARED across all sessions (it already
// multiplexes N ingress publications + N egress subscriptions over one embedded
// media driver); a Session does NOT own a transport — it references the shared
// one via the owning ClientAgent.  A Session carries its own sessionId (owned by
// its SessionMachine) and, later, its own stream ids.
//
// Shared-nothing: every field here is owned by the AGENT thread only.  The
// redirect fields are plain data (no atomics) — they are read/written solely on
// the agent/runner thread (SessionMachine already carries no atomics for the
// same reason).
//
// Task 1.1 scope: the ClientAgent holds EXACTLY ONE Session (the seed).  The
// pool-of-1 path is byte-identical to the previous single-session flow.  Routing
// / multi-session-open is Task 1.3.
// ---------------------------------------------------------------------------
// Forward declaration — the agent owns the shared transport; a Session's IO
// adapter targets that transport at the session's own endpoint channel.
class ClientAgent;

// ---------------------------------------------------------------------------
// SessionEndpointIo — per-session SessionIo bound to the session's endpoint.
//
// Task 1.3: each Session sends its control frames (ConnectRequest / Keepalive /
// Disconnect) to ITS OWN broker-node endpoint over the SHARED AeronTransport
// (offerTo(channel, ...)), instead of the legacy single retarget()ed ingress
// pub.  The seed session's endpoint == the transport's default ingress channel,
// so its offerTo() fast-paths straight back to the byte-identical default offer.
// Multi-node: each leader session offers to its own channel.  Retarget on this
// adapter is a NO-OP (multi-node addresses endpoints explicitly — it never
// retargets the shared default pub).  Agent-thread only.
// ---------------------------------------------------------------------------
struct SessionEndpointIo : session::SessionIo {
    ClientAgent* agent   = nullptr;    // owner (shared transport lives here)
    std::string  endpoint;             // this session's ingress channel

    SessionEndpointIo(ClientAgent* a, std::string ep)
        : agent(a), endpoint(std::move(ep)) {}

    bool sendFrame(const uint8_t* buf, std::size_t len) override;
    bool retarget(const std::string& newIngressChannel) noexcept override;
};

struct Session {
    /// This session's endpoint channel (broker-node ingress).  For the seed this
    /// equals the transport default ingress channel.
    std::string endpoint;

    /// Per-session IO adapter — routes control frames to `endpoint`.  Held by
    /// pointer/value BEFORE `machine` so it outlives the machine's reference.
    SessionEndpointIo io;

    /// The per-node session state machine.  Non-movable (holds a SessionIo&),
    /// so a Session is always held by pointer in the agent's collection.
    session::SessionMachine machine;

    // ---- Per-node data-op redirect state (agent-thread-only; plain data) -----
    // Set when a Produce/Subscribe on THIS node's session is rejected by a
    // FOLLOWER with ErrorResponse{Redirect,leaderEndpoint}: the transport is
    // retarget()ed to the leader and a fresh reconnect is submitted; the pending
    // redirected data ops are re-stamped + resent on the next ConnectResponse{OK}.
    // Single-owner (runner/agent thread) → NO atomic (shared-nothing law).
    bool        redirectPending      = false;

    /// The channel URI this session is currently connected to.  Seeded from the
    /// config gateway channel and updated on every retarget (redirect or route).
    std::string currentLeaderChannel;

    /// The broker nodeId this session is currently believed to be on, or -1 when
    /// unknown (the initial gateway connection — we don't know which node it is
    /// until a REDIRECT teaches us).
    int32_t     currentLeaderNode    = -1;

    /// Task 1.3: has this session's connect been submitted yet?  Lazy multi-node
    /// sessions are opened async — connect is submitted once; frames for the
    /// session's partitions park in the producer builders until ConnectResponse{OK}.
    bool        connectSubmitted     = false;

    Session(ClientAgent* agent, session::SessionConfig cfg,
            std::string sessionEndpoint)
        : endpoint(sessionEndpoint)
        , io(agent, sessionEndpoint)
        , machine(io, std::move(cfg))
        , currentLeaderChannel(sessionEndpoint)
    {}
};

// ---------------------------------------------------------------------------
// AgentConfig — full configuration for the ClientAgent.
// ---------------------------------------------------------------------------
struct AgentConfig {
    // Transport configuration (injected into AeronTransport).
    transport::TransportConfig transport;

    // Session configuration (injected into SessionMachine).
    session::SessionConfig session;

    // Ring buffer capacity — must be a power of two (bytes of usable space).
    // Default: 32 MiB — absorbs command/send bursts without back-pressuring.
    int32_t ringCapacity = 32 * 1024 * 1024;

    // Maximum command records to drain from the ring per doWork call (bounded).
    int ringDrainLimit = 128;

    // Maximum fragments to poll from egress per doWork call (bounded).  This is the
    // ack-return drain width — raised so a burst of ProduceResponses is drained in
    // one cycle instead of trickling out 16 at a time.
    int egressPollLimit = 256;
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
    ///  4. CB-2: drain producer ring + flush batches (via ProducerHub).
    /// Returns total work count (0 = nothing happened → IdleStrategy backs off).
    /// Must be called from ONE thread only (the agent thread).
    int doWork(int64_t nowNs) noexcept;

    // -------------------------------------------------------------------------
    // Thread-safe submission API (callable from ANY thread)
    // -------------------------------------------------------------------------

    /// Submit a CONNECT command.  Returns false if the ring is full.
    bool submitConnect() noexcept;

    /// Submit a RECONNECT command (CB-5 B data-op redirect).  Forces a fresh
    /// ConnectRequest even when the session is currently Connected — used after
    /// retarget()ing the transport to a new leader.  Returns false if ring full.
    bool submitReconnect() noexcept;

    /// Submit a DISCONNECT command.  Returns false if the ring is full.
    bool submitDisconnect() noexcept;

    /// Submit a CLOSE command.  Returns false if the ring is full.
    bool submitClose() noexcept;

    /// Submit a pre-encoded frame for transmission.  Returns false if ring full.
    /// The frame bytes are copied into the ring (the caller's buffer is safe to
    /// reuse immediately after this call returns).
    bool submitSend(const uint8_t* frame, std::size_t len) noexcept;

    /// Shared-nothing producer registration: hand a heap-allocated
    /// std::shared_ptr<ProducerImpl>* ("holder") to the RUNNER thread via the
    /// command ring.  The runner drains kMsgRegisterProducer, invokes the register
    /// hook (which adopts the shared_ptr into the single-owner ProducerHub list and
    /// deletes the holder), so the producer list is never touched cross-thread.
    /// Returns false if the ring is momentarily full (caller retries).  The holder
    /// pointer is only its 8-byte address — no producer bytes cross the ring.
    bool submitRegisterProducer(void* holder) noexcept;

    /// read-rework I4: open (idempotently) a per-worker dynamic-MDC egress
    /// subscription so the client receives Records pushed by the owning worker.
    /// AGENT-THREAD ONLY — call from onMetadataResponse (which runs on the agent
    /// thread during egress poll dispatch).  Forwards to the transport.
    void addEgressSubscription(const std::string& controlChannel, int32_t streamId) noexcept {
        transport_.addEgressSubscription(controlChannel, streamId);
    }

    // -------------------------------------------------------------------------
    // Task 1.3: multi-node session registry + partition routing table
    // -------------------------------------------------------------------------

    /// Return the Session for `endpoint`, lazily opening a NEW one if absent.
    /// AGENT-THREAD ONLY.  A new session gets its own SessionMachine bound to a
    /// per-session IO adapter (offerTo(endpoint,...)) over the SHARED transport —
    /// its own ConnectRequest → its own sessionId + keepalive on the one driver.
    /// The open is ASYNC/non-blocking: connect() is submitted here and the session
    /// transitions Disconnected→Connecting; callers park frames (in the producer
    /// builders) until this session's ConnectResponse{OK} assigns a sessionId.
    /// The seed session's endpoint == the transport default ingress channel.
    Session& sessionForEndpoint(const std::string& endpoint) noexcept;

    /// Task-1.3 form: session and produce data share `endpoint`.
    void setRoute(int32_t topicId, int32_t partition, const std::string& endpoint) noexcept {
        (void)setRoute(topicId, partition, endpoint, endpoint);
    }
    /// Routes (topicId, partition): Connect / session id on `sessionEndpoint` (the
    /// node), Produce frames on `dataChannel` (the owning worker's ingress, or the
    /// node).  Both equal to the seed endpoint (or empty) removes the route so the
    /// partition takes the byte-identical default path.  Cold path (metadata).
    /// Returns true when the route changed.
    bool setRoute(int32_t topicId, int32_t partition, const std::string& sessionEndpoint,
                  const std::string& dataChannel) noexcept;

    /// Task 1.3: number of currently open sessions (test/observer helper).
    [[nodiscard]] std::size_t sessionCount() const noexcept { return sessions_.size(); }

    [[nodiscard]] std::string routeEndpointFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] std::string dataChannelFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] bool multiNodeRouting() const noexcept { return routeIndex_.size() != 0; }

    /// ProduceResponses decoded this duty cycle, for the owner to route with plain
    /// calls (buffered only when no produceResponseFn_ test hook is installed).
    struct ProduceAck {
        int64_t correlationId;
        int64_t baseOffsetOrError;  ///< < 0 = -ErrorCode
    };
    [[nodiscard]] std::span<const ProduceAck> produceAcks() const noexcept {
        return {produceAcks_.data(), produceAcks_.size()};
    }
    void clearProduceAcks() noexcept { produceAcks_.clear(); }

    /// Forward ProduceResponse frames to the egress observer (transactional
    /// producers need them).  Default true (historical behaviour).
    void setForwardProduceResponses(bool on) noexcept { forwardProduceResponses_ = on; }

    [[nodiscard]] bool transportClosed() const noexcept { return transport_.closed(); }

    /// Task 1.4 (redirect convergence): route ONE pre-encoded produce frame to its
    /// partition leader session — the SAME routing the produce drain path uses
    /// (consult the route slots, lazily open the leader session, re-stamp the
    /// leader sessionId, offerTo).  Returns false (hold) when the leader session
    /// is not yet Connected; the caller retains the frame for a later resend.
    /// Used by the Client-level redirect handler and connect-triggered resend.
    /// AGENT-THREAD ONLY.
    bool offerToPartition(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                          const uint8_t* buf, std::size_t len) noexcept {
        return offerProduceToLeader_(topicId, partition, ingressStreamId, buf, len);
    }

    /// Task 1.4 (redirect convergence): hook fired on the agent thread whenever a
    /// (non-seed or seed) session reaches Connected, when multi-node routing is
    /// active.  The Client wires this to re-offer all in-flight produce frames via
    /// their (now possibly reachable) partition-leader routes.  Set before start().
    using ProduceResendFn = std::function<void()>;
    void setProduceResendFn(ProduceResendFn fn) noexcept {
        produceResendFn_ = std::move(fn);
    }

    /// Shared-nothing producer registration hook: invoked on the RUNNER thread
    /// when a kMsgRegisterProducer command is drained.  The argument is the holder
    /// pointer passed to submitRegisterProducer (a std::shared_ptr<ProducerImpl>*).
    /// The Client wires this to adopt the producer into the ProducerHub list and
    /// delete the holder.  Set before start().
    using ProducerRegisterFn = std::function<void(void*)>;
    void setProducerRegisterFn(ProducerRegisterFn fn) noexcept {
        producerRegisterFn_ = std::move(fn);
    }

    // -------------------------------------------------------------------------
    // Observers
    // -------------------------------------------------------------------------

    /// Set (replace) the egress observer.
    /// MUST be called before start() (single-threaded setup phase only).
    /// The observer is read on the agent thread; changing it after start() is
    /// a data race.  CC-6 wires the observer once before start() — that is the
    /// only supported usage pattern.
    void setEgressObserver(EgressObserver obs) noexcept;

    // -------------------------------------------------------------------------
    // CB-2: Producer duty-cycle hooks (setup-phase only; call before start())
    // -------------------------------------------------------------------------

    /// Send function type: (topicId, partition, ingressStreamId, frame, len) -> bool.
    /// ingressStreamId=0 means "use the default ingress stream".
    /// E5.1: carries the per-worker Aeron stream id for per-partition routing.
    /// Task 1.4: (topicId, partition) is the routing key — ingressStreamId is only
    /// the transport stream to offer on (it REPEATS across nodes, so it cannot be a
    /// routing key without cross-node collision).
    using SendFn = std::function<bool(int32_t /*topicId*/, int32_t /*partition*/,
                                      int32_t /*ingressStreamId*/,
                                      const uint8_t*, std::size_t)>;

    /// Producer drain+flush callback: invoked on the agent thread every doWork
    /// cycle.  Signature: (nowNs, sessionId, sendFn) -> workCount.
    using ProducerDrainFn =
        std::function<int(int64_t /*nowNs*/, int64_t /*sessionId*/, SendFn)>;

    /// ProduceResponse routing callback: invoked on the agent thread when a
    /// ProduceResponse frame arrives.  Signature: (correlationId, baseOffset).
    using ProduceResponseFn = std::function<void(int64_t, int64_t)>;

    /// Set the producer drain+flush hook.  MUST be called before start().
    void setProducerDrainFn(ProducerDrainFn fn) noexcept;

    /// Set the ProduceResponse routing hook.  MUST be called before start().
    void setProduceResponseFn(ProduceResponseFn fn) noexcept;

    // -------------------------------------------------------------------------
    // MD1 Task 10: MetadataResponse hook (setup-phase only; call before start())
    // -------------------------------------------------------------------------

    /// Callback invoked on the agent thread when a MetadataResponse arrives.
    /// Signature: (decoded MetadataResponse) -> void.
    using MetadataResponseFn =
        std::function<void(const codec::MetadataResponseDecoded&)>;

    /// Set the MetadataResponse routing hook.  MUST be called before start().
    void setMetadataResponseFn(MetadataResponseFn fn) noexcept;

    /// Return the session state snapshot published by the agent thread.
    /// Thread-safe (atomic acquire load).  Eventually-consistent: reflects the
    /// state at the end of the most recent doWork() call.  Safe to call from
    /// ANY thread — this is the cross-thread observer path used by CC-6.
    [[nodiscard]] session::SessionState sessionState() const noexcept;

    /// Return the session-id snapshot published by the agent thread.
    /// Thread-safe (atomic acquire load).  Eventually-consistent: reflects the
    /// session-id at the end of the most recent doWork() call.
    [[nodiscard]] int64_t sessionId() const noexcept;

    // -------------------------------------------------------------------------
    // Coherent (state, sessionId) snapshot — FIX F3
    // -------------------------------------------------------------------------

    /// A coherent (state, sessionId) pair read from a SINGLE atomic load.
    /// Neither field can be torn against the other: they are published together
    /// (packed into one uint64_t) by the agent thread, so a caller never sees
    /// state==Connected paired with a sessionId from a different session epoch.
    struct StateSnapshot {
        session::SessionState state;
        int64_t               sessionId;
    };

    /// Return a coherent (state, sessionId) pair in ONE atomic acquire load.
    /// This is the accessor Client::ping() MUST use so the sessionId it stamps
    /// on the Ping always belongs to the same session epoch as the observed
    /// Connected state.  (Two independent atomic loads could tear across a
    /// resume — FIX F3.)
    [[nodiscard]] StateSnapshot stateAndSessionId() const noexcept;

    // CB-2: Allow Client to access ring_ for ProducerImpl construction.
    friend class ::rune::client::Client;

    // Internal accessor for producer tests (CB-2): returns the ring buffer so
    // ProducerImpl can be constructed directly in test harnesses.
    // NOT part of the public API; production code must use Client::producer().
    rune::client::detail::M2ORingBuffer& ringForTesting() noexcept {
        return ring_;
    }

private:
    // -------------------------------------------------------------------------
    // SessionIo implementation (called only on the agent thread)
    // -------------------------------------------------------------------------
    bool sendFrame(const uint8_t* buf, std::size_t len) override;
    bool retarget(const std::string& newIngressChannel) noexcept override;

    // -------------------------------------------------------------------------
    // Command message type IDs for the ring
    // -------------------------------------------------------------------------
    static constexpr int32_t kMsgConnect    = 1;
    static constexpr int32_t kMsgDisconnect = 2;
    static constexpr int32_t kMsgClose      = 3;
    static constexpr int32_t kMsgSend       = 4;
    static constexpr int32_t kMsgReconnect  = 5; // CB-5 B: force fresh reconnect
    static constexpr int32_t kMsgRegisterProducer = 6; // shared-nothing producer registration

    // Task 1.3: per-session ConnectRequest correlationId range stride.  Session i
    // starts its correlationId counter at i*stride so concurrent connects demux
    // unambiguously off the one shared egress subscription.  1e12 is far above any
    // realistic per-session connect-retry count and stays well within int64.
    static constexpr int64_t kSessionCorrIdStride = 1'000'000'000'000LL;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    bool writeCommand(int32_t msgTypeId, const uint8_t* payload, std::size_t payloadLen) noexcept;
    void dispatchEgressFrame(int64_t nowNs, const uint8_t* buf, std::size_t len) noexcept;

    /// Task 1.3: resolve the Session whose IN-FLIGHT ConnectRequest carries
    /// `corrId` (its SessionMachine's current correlationId while Connecting).
    /// Falls back to the seed session when no session matches (single-node, a
    /// steady-state frame, or an already-established session).  Agent-thread only.
    Session& sessionForConnectCorrId(int64_t corrId) noexcept;

    /// Publish the current (state, sessionId) as ONE packed atomic store
    /// (release).  Called on the agent thread (a) after each frame is fed to the
    /// session and BEFORE the egress observer runs (FIX F1), and (b) at the end
    /// of each doWork() as the catch-all for tick-driven transitions.
    void publishSnapshot() noexcept;

    // ---- Packed (state, sessionId) snapshot codec (FIX F1 + F3) -------------
    // Layout: high 16 bits = SessionState (fits in 8 bits), low 48 bits =
    // sessionId.  sessionId is monotonic from 1 (server-assigned) and 0 when no
    // session — it comfortably fits in 48 bits.  Packing both into ONE uint64_t
    // makes the pair readable in a single atomic load with no torn read.
    static constexpr int      kStateShift    = 48;
    static constexpr uint64_t kSessionIdMask = (uint64_t{1} << kStateShift) - 1;

    static uint64_t packSnapshot(session::SessionState st, int64_t sid) noexcept {
        const uint64_t usid = static_cast<uint64_t>(sid) & kSessionIdMask;
        return (static_cast<uint64_t>(static_cast<uint8_t>(st)) << kStateShift) | usid;
    }
    static session::SessionState unpackState(uint64_t v) noexcept {
        return static_cast<session::SessionState>(
            static_cast<uint8_t>(v >> kStateShift));
    }
    static int64_t unpackSessionId(uint64_t v) noexcept {
        return static_cast<int64_t>(v & kSessionIdMask);
    }

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    AgentConfig cfg_;

    // ---- Transport (SHARED across all sessions; agent-thread owned) ----------
    // One embedded media driver multiplexes every session's ingress/egress
    // streams (Task 1.1: do NOT create per-session transports/drivers).
    transport::AeronTransport transport_;

    // ---- Endpoint-keyed sessions (agent-thread owned, single-writer) ---------
    // Held by pointer because SessionMachine is non-movable (it holds a SessionIo&
    // to the Session's own io adapter).  sessions_[0] is ALWAYS the seed (the
    // transport default ingress channel); Task 1.3 lazily appends one Session per
    // additional leader endpoint via sessionForEndpoint().  A plain vector keeps
    // the pool-of-1 hot path a single deref (no string hashing) while the small N
    // multi-node case is a short linear scan (N ≤ cluster size).  Agent-thread only.
    std::vector<std::unique_ptr<Session>> sessions_;

    /// The seed (single) session — sessions_[0].  Pool-of-1 accessor: identical to
    /// the previous single-session member for both agent-internal doWork and the
    /// Client-level redirect/dispatch paths.  Agent-thread only.
    Session&       seedSession()       noexcept { return *sessions_.front(); }
    const Session& seedSession() const noexcept { return *sessions_.front(); }

    // ---- Task 1.4: partition routing table (agent-owned plain data) ----------
    // Maps a (topicId, partition) → its leader endpoint channel.  Rebuilt in place
    // on metadata change (setRoute).  Read by the produce send lambda on the hot
    // path: a FlatIndexMap probe keyed by an int64_t packed key (no string hashing
    // on the key).  The key is ((int64_t)topicId<<32) | (uint32_t)partition — unique
    // per partition cluster-wide, so it never collides across nodes (ingressStreamId
    // does — each node reuses the same small per-worker stream ids).  EMPTY on
    // single-node (no per-worker routes advertised) ⇒ the send lambda takes the
    // byte-identical default offer path (no lookup cost beyond one size check).
    // Single-writer (agent thread).
    static constexpr uint32_t kNoSession = 0xFFFFFFFFu;
    // One slot per routed partition: strings are cold (setRoute / first offer);
    // the hot path touches only sessionIndex / streamId / pub.
    struct PartitionRoute {
        std::string sessionEndpoint;
        std::string dataChannel;
        uint32_t    sessionIndex = kNoSession;  // resolved on first offer
        int32_t     streamId     = -1;          // stream `pub` was built for
        transport::AeronTransport::PubHandle pub = transport::AeronTransport::kNoPub;
    };
    detail::FlatIndexMap        routeIndex_;   // routeKey(t,p) -> routes_ index
    std::vector<PartitionRoute> routes_;
    std::vector<uint32_t>       freeRoutes_;
    std::vector<ProduceAck>     produceAcks_;  // reserved in the ctor; owner clears
    bool                        forwardProduceResponses_ = true;
    bool                        mnDebug_ = false;  // RUNE_MN_DEBUG, read ONCE
    uint32_t sessionIndexFor_(const std::string& endpoint) noexcept;

    // Pack (topicId, partition) into the routing-table key.  partition is masked to
    // its unsigned 32-bit value so negative partitions (never valid) can't alias.
    static constexpr int64_t routeKey(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<int64_t>(topicId) << 32)
             |  static_cast<int64_t>(static_cast<uint32_t>(partition));
    }

    // ---- Task 1.3: control-frame offer to a specific endpoint ----------------
    // Used by SessionEndpointIo::sendFrame — offers a session's control frame to
    // that session's endpoint over the shared transport (default ingress stream id).
    friend struct SessionEndpointIo;
    bool offerControlTo(const std::string& channel,
                        const uint8_t* buf, std::size_t len) noexcept;

    // ---- Task 1.4: shared produce→leader routing (drain path + redirect resend) --
    // Probe the route slot, resolve its session / pub once, hold if the session is not
    // Connected, else re-stamp the leader sessionId (header offset 8) and offerOn.
    // Single-writer (agent thread).  Definition mirrors the produce drain send-lambda.
    bool offerProduceToLeader_(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                               const uint8_t* buf, std::size_t len) noexcept;

    // ---- Ring buffer (MPSC: many app threads write, one agent thread reads) -
    // Buffer size = power-of-two capacity + trailer.
    // Stored as aligned raw bytes; AtomicBuffer wraps it.
    static constexpr std::size_t kRingAlignment = 64; // cache-line aligned
    std::unique_ptr<uint8_t, void(*)(void*)> ringBuf_;
    rune::client::detail::M2ORingBuffer      ring_;

    // ---- Lifecycle -----------------------------------------------------------
    std::atomic<bool> started_ {false};
    std::atomic<bool> stopped_ {false};

    // ---- Egress observer (agent thread only; must be set before start()) ----
    EgressObserver egressObserver_;

    // ---- CB-2: Producer hooks (agent thread only; set before start()) --------
    ProducerDrainFn    producerDrainFn_;
    ProduceResponseFn  produceResponseFn_;
    ProduceResendFn    produceResendFn_;    // Task 1.4: fired on session-connect (multi-node)
    ProducerRegisterFn producerRegisterFn_; // shared-nothing producer registration (ring-drained)

    // ---- MD1 Task 10: Metadata response hook (agent thread only) -------------
    MetadataResponseFn metadataResponseFn_;

    // ---- Thread-safe observer snapshot (BrokerNode mirror pattern) ----------
    // FIX F1 + F3: (state, sessionId) are packed into ONE atomic uint64_t and
    // published together (release) so cross-thread readers get a COHERENT pair
    // in a single acquire load — no torn read across a resume, and the snapshot
    // is fresh at the moment the egress observer resolves the connect promise.
    // Written on the agent thread; read by sessionState()/sessionId()/
    // stateAndSessionId() from any thread.
    std::atomic<uint64_t> snapshot_ {
        packSnapshot(session::SessionState::Disconnected, 0)
    };
};

} // namespace rune::client::agent
