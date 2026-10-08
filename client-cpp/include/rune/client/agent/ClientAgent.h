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
#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/M2ORingBuffer.h> // internal MPSC ring (templated read, no std::function)

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
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
struct Session;

struct SessionEndpointIo : session::SessionIo {
    ClientAgent* agent   = nullptr;    // owner (shared transport lives here)
    std::string  endpoint;             // this session's ingress channel
    /// D4 r2: the Session this adapter belongs to — its control frames are
    /// sealed under ITS crypto state and routed by ITS identity (seed = the
    /// default publication), never looked up by endpoint string (a seed moved
    /// off its bootstrap and a session to that bootstrap share the string).
    Session*     owner   = nullptr;

    SessionEndpointIo(ClientAgent* a, std::string ep)
        : agent(a), endpoint(std::move(ep)) {}

    bool sendFrame(const uint8_t* buf, std::size_t len) override;
    bool retarget(const std::string& newIngressChannel) noexcept override;
    /// T15a fix1 I1: the seed's bootstrap fallback (leader sessions: no-op).
    bool retargetToBootstrap() noexcept override;
    /// T15a fix2 A-B1: the seed's return to the last-known leader (leader sessions: no-op).
    bool retargetToLastKnownLeader() noexcept override;
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

    /// T15a fix2 A-B1 (seed only): the leader the seed was stuck to when a Backoff
    /// attempt fell back to the bootstrap ("" = none).  Captured at the fallback,
    /// NEVER cleared by it, so the next odd Backoff attempt can return there:
    /// Backoff alternates bootstrap <-> last-known leader.  A later redirect while
    /// on the bootstrap moves the transport, and the next fallback captures that.
    std::string lastKnownLeaderChannel;
    int32_t     lastKnownLeaderNode  = -1;

    /// Task 1.3: has this session's connect been submitted yet?  Lazy multi-node
    /// sessions are opened async — connect is submitted once; frames for the
    /// session's partitions park in the producer builders until ConnectResponse{OK}.
    bool        connectSubmitted     = false;

    /// Connect epoch: incremented on every ConnectResponse{OK} that leaves this
    /// session Connected (a resume, a reconnect, a node restart that re-used the
    /// broker session id — MEDIUM-1: the broker id alone cannot tell them apart).
    uint64_t    connectEpoch         = 0;
    /// The node's ConnectResponse.brokerVersion of the latest accepted connect
    /// (A-MEDIUM-3: every node a group partition is read from must be >= 3).
    int32_t     brokerVersion        = 0;

    Session(ClientAgent* agent, session::SessionConfig cfg,
            std::string sessionEndpoint)
        : endpoint(sessionEndpoint)
        , io(agent, sessionEndpoint)
        , machine(io, std::move(cfg))
        , currentLeaderChannel(sessionEndpoint)
    { io.owner = this; }
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
    /// Single-threaded lifecycle call: only when no thread is inside doWork()
    /// (after the runner is joined / on the Invoker owner) — never as a
    /// cross-thread stop signal (use the runner shutdown path or submitClose()).
    void stop() noexcept;

    /// F8c C0: the calling thread becomes the single thread that drives this
    /// agent's duty cycle (and so pumps the transport's driver/conductor in
    /// invoker mode).  Call once at the top of a duty thread that takes over
    /// after a happens-before handoff (the Thread-mode runner; the closing
    /// thread after the join).  See AeronTransport::bindPumpThread().
    void bindDutyThread() noexcept { transport_.bindPumpThread(); }

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

    /// F8c C2: verdict of a command-ring submit.  Closed is LINEARIZABLE with
    /// closeCommandRing() (the closed bit lives in the ring's tail word, C1):
    /// a command accepted before the close is drained by the close path, a
    /// command refused as Closed was never written.
    enum class Submit : uint8_t { Accepted, Full, Closed };

    /// F8c C2: close the command ring to producers (any thread; one RMW in
    /// ring internals).  True only for the call that closed it.  Every command
    /// accepted before it is still in the ring (drainRegistrationsForClose()
    /// reads up to the frozen tail); every later submit returns Closed.
    bool closeCommandRing() noexcept { return ring_.close(); }
    /// Any thread: has closeCommandRing() run?  (acquire)
    [[nodiscard]] bool commandRingClosed() const noexcept { return ring_.closed(); }
    /// F8c fix round 1 (I2): close() marks the command ring's consumer
    /// finished once nothing will ever read it again (after its drain, even an
    /// abandoned one); any thread may observe it (acquire).
    void finishCommandRingConsumer() noexcept { ring_.consumerFinish(); }
    [[nodiscard]] bool commandRingConsumerFinished() const noexcept { return ring_.consumerFinished(); }
    /// The command ring's consumer position (duty thread: its own progress).
    [[nodiscard]] std::int64_t commandRingHead() const noexcept { return ring_.headPosition(); }
    /// F8c C7 fix round 2 (F1): orphaned handle releases exist (any thread
    /// raises; the duty thread takes, then scans).
    void raiseOrphanHint() noexcept { ring_.raiseOrphanHint(); }
    [[nodiscard]] bool takeOrphanHint() noexcept { return ring_.takeOrphanHint(); }
    /// Any thread: the command ring's tail position (acquire).
    [[nodiscard]] std::int64_t commandRingTail() const noexcept { return ring_.tailPosition(); }
    /// The largest command record (header + payload) the ring accepts.
    [[nodiscard]] std::size_t commandMsgLimit() const noexcept {
        const auto m = static_cast<std::size_t>(ring_.maxMsgLength());
        return (testCommandMsgCap_ != 0 && testCommandMsgCap_ < m) ? testCommandMsgCap_ : m;
    }
    /// Test seams (set before the threads they affect run).
    void testCapCommandMsgLength(std::size_t cap) noexcept { testCommandMsgCap_ = cap; }
    void testDisableCloseDrainUnblock() noexcept { testCloseDrainNoUnblock_ = true; }

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

    /// Shared-nothing producer registration: hand the ProducerImpl* to the
    /// RUNNER thread via the command ring.  The runner drains
    /// kMsgRegisterProducer and invokes the register hook, which adopts it into
    /// the single-owner ProducerHub list (F8c C7: the duty side's share of the
    /// impl — no shared_ptr holder), so the producer list is never touched
    /// cross-thread.  Returns false if the ring is momentarily full (caller
    /// retries).  Only the 8-byte address crosses the ring.
    Submit submitRegisterProducer(void* impl) noexcept;

    /// Same shared-nothing hand-off for a transactional producer: `impl` is the
    /// TransactionalProducerImpl* adopted on the duty-cycle thread.
    Submit submitRegisterTxnProducer(void* impl) noexcept;

    /// F8a: hand a subscriber / group-consumer / completion-queue command to
    /// the duty thread (kMsgHubCommand, SBE ring_msg::HubCommandMsg).  The duty
    /// thread drains it and invokes the hub-command hook.
    Submit submitHubCommand(const ::rune::client::detail::HubCommand& cmd) noexcept;

    /// F8c C3: hand a client control op (connect / ping / defineTopic /
    /// getTopicSchema / listTopics) to the duty thread: `hdr` (a trivially
    /// copyable header, `hdrLen` bytes) followed by the op's wire frame, copied
    /// into ONE ring record (kMsgControlOp).  The duty thread invokes the
    /// control-op hook with the whole record.
    Submit submitControlOp(const void* hdr, std::size_t hdrLen,
                           std::span<const uint8_t> frame) noexcept;

    /// close() path, single-threaded (runner joined / Invoker owner): drain the
    /// WHOLE command ring once, adopting producer + txn-producer registrations
    /// and applying F8a hub commands through their hooks (so their holders are
    /// freed and the owner can fail them — a commitSync token or subscribe
    /// still in the ring is never stranded) and dropping every other command
    /// (sends carry no completion).
    ///
    /// fix5 Important-2(c): always terminates. If the drain makes no progress
    /// for kCloseDrainUnblockAfterYields consecutive yields (stuck behind a
    /// claimed-but-never-committed record), it calls M2ORingBuffer::unblock()
    /// (Aeron's own recovery for exactly this), logs one warning, and
    /// continues; if unblock() cannot make progress kCloseDrainMaxFailedUnblocks
    /// times in a row, it gives up (warning) rather than wedge close().
    int drainRegistrationsForClose() noexcept;

    /// fix5 Important-2(c): consecutive no-progress yields before
    /// drainRegistrationsForClose() unblocks the ring. An ITERATION cap, not a
    /// time: the claim->commit window it waits out is a handful of noexcept
    /// instructions (writeCommand -> M2ORingBuffer::writeWith, which aborts its
    /// claim on any throw), so only a writer that is descheduled mid-window —
    /// or a bug that orphaned a claim — can reach it. 1M yields is many
    /// scheduler quanta even on a loaded box, so a merely-preempted writer
    /// commits long before it; a real orphan costs close() one bounded wait.
    static constexpr int kCloseDrainUnblockAfterYields = 1'000'000;
    /// fix5 Important-2(c): consecutive unblock() calls that make no progress
    /// (nothing unblockable: e.g. a header-less claim with nothing committed
    /// behind it) before drainRegistrationsForClose() stops waiting.
    static constexpr int kCloseDrainMaxFailedUnblocks = 2;

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

    /// Task 16 fix round 1 (M6): two endpoint strings name the same destination —
    /// equal, or both Aeron channels whose `endpoint=host:port` values match
    /// (other URI params ignored), or a bare "host:port" equal to the other's
    /// endpoint value.  Hostnames are not resolved.  Cold path.
    [[nodiscard]] static bool sameEndpoint(std::string_view a, std::string_view b) noexcept;
    /// Test hook: a Ready ingress publication exists for (channel, streamId).
    [[nodiscard]] bool testPublicationOpen(const std::string& channel, int32_t streamId) const noexcept {
        return transport_.publicationOpen(channel, streamId);
    }

    // ---- Consumer-group fetch routing (cold path; agent thread only) ---------
    // A group member's Subscribe for a partition must reach that partition's
    // LEADER node on that node's own session (the broker refuses a Subscribe for
    // a partition it does not lead).  Sessions are append-only (sessions_ never
    // shrinks), so an index stays valid for the agent's lifetime.

    /// Session `index`'s connect epoch while it is Connected; 0 otherwise.
    [[nodiscard]] uint64_t connectedEpoch(uint32_t index) const noexcept {
        if (index >= sessions_.size()) return 0;
        const Session& s = *sessions_[index];
        return s.machine.state() == session::SessionState::Connected ? s.connectEpoch : 0;
    }
    /// Session `index`'s node broker version (latest accepted connect; 0 = unknown).
    [[nodiscard]] int32_t sessionBrokerVersion(uint32_t index) const noexcept {
        return index < sessions_.size() ? sessions_[index]->brokerVersion : 0;
    }
    /// Session `index`'s state (Disconnected when out of range).
    /// DR-7 (rerun): produce offers held because the partition's leader session is not
    /// Connected — monotonic; each increment is an event (Kafka: a leader that is not
    /// ready requests a metadata update, paced by the retry backoff).
    [[nodiscard]] uint64_t heldOffers() const noexcept { return heldOffers_; }
    /// DR-7: connect attempts of session `index` that ended without a session.
    [[nodiscard]] uint64_t sessionConnectFailuresAt(uint32_t index) const noexcept {
        return index < sessions_.size() ? sessions_[index]->machine.connectFailures() : 0;
    }
    /// DR-7: some partition route sends through session `index` (control path).
    [[nodiscard]] bool routesUseSession(uint32_t index) const noexcept {
        for (const PartitionRoute& r : routes_)
            if (r.sessionIndex == index) return true;
        return false;
    }
    [[nodiscard]] session::SessionState sessionStateAt(uint32_t index) const noexcept {
        return index < sessions_.size() ? sessions_[index]->machine.state() : session::SessionState::Disconnected;
    }
    /// M1: re-drive a fresh connect on a NON-seed session that is terminally
    /// Disconnected (its first-connect / resume budget ran out).  True if started.
    bool reviveSession(uint32_t index, int64_t nowNs) noexcept;
    /// MAJOR-2: the ConnectResponse being dispatched to the egress observer
    /// right now — did its session machine ACCEPT it (awaited correlationId,
    /// now Connected)?  Which session?  Was it awaited at all (a stale /
    /// duplicate one is not)?  Valid inside the observer call.
    [[nodiscard]] bool     lastConnectAccepted() const noexcept { return lastConnectAccepted_; }
    [[nodiscard]] bool     lastConnectAwaited() const noexcept { return lastConnectAwaited_; }
    [[nodiscard]] uint32_t lastConnectSession() const noexcept { return lastConnectSession_; }

    /// Index of the session leading (topicId, partition): its route's node
    /// session, lazily opened (async connect); 0 = the seed (no route).
    [[nodiscard]] uint32_t leaderSessionIndex(int32_t topicId, int32_t partition) noexcept;
    /// Session `index`'s id while it is Connected; 0 otherwise (or out of range).
    [[nodiscard]] int64_t connectedSessionId(uint32_t index) const noexcept;
    /// Offer a control frame (already stamped with that session's id) on
    /// session `index`'s node ingress: the seed's default publication, or the
    /// node endpoint's.  Retry = transiently refused (the owner retries).
    transport::AeronTransport::OfferResult offerOnSession(uint32_t index, const uint8_t* buf,
                                                          std::size_t len) noexcept;

    /// D4 (coordinator session — Kafka's coordinator connection): the index of
    /// the session to the group / txn COORDINATOR at `channel`: that node's ONE
    /// session (sessionForEndpoint: lazily opened, async connect, its own crypto
    /// state and disjoint correlationId range — the same session partition routes
    /// to that node use); 0 when `channel` is empty or names the seed's node.  A
    /// terminally Disconnected one is re-driven (its own connect budget paces it).
    /// The seed never moves for it.  Agent thread only; cold path.
    [[nodiscard]] uint32_t coordinatorSessionIndex(const std::string& channel, int64_t nowNs) noexcept;
    /// D4 r2: where the seed session physically is — the channel the default
    /// ingress publication points at (its bootstrap identity unless a data-op
    /// redirect / stick-to-leader retarget moved it).
    [[nodiscard]] const std::string& seedChannel() const noexcept { return transport_.ingressChannel(); }
    /// D4 r2 LOW-2: (topicId, partition) has a partition route (else it rides the seed).
    [[nodiscard]] bool hasRoute(int32_t topicId, int32_t partition) const noexcept {
        return routeIndex_.size() != 0 && routeIndex_.find(routeKey(topicId, partition)) != detail::FlatIndexMap::kNone;
    }
    /// D4: offer ONE coordinator frame (group / non-Produce txn) on session
    /// `index`: the header sessionId is re-stamped with that session's id (as
    /// offerProduceToLeader_ does for a leader), then sealed under that session's
    /// CryptoState and offered on its node ingress.  Retry (= hold; the owner
    /// re-sends on that session's connect) while it is not Connected.
    transport::AeronTransport::OfferResult offerToCoordinator(uint32_t index, const uint8_t* buf,
                                                              std::size_t len) noexcept;
    /// D4: session `index`'s machine (read-only; the seed when out of range).
    [[nodiscard]] const session::SessionMachine& sessionMachineAt(uint32_t index) const noexcept {
        return (index < sessions_.size() ? *sessions_[index] : *sessions_[0]).machine;
    }

    [[nodiscard]] std::string routeEndpointFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] std::string dataChannelFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] bool multiNodeRouting() const noexcept { return routeIndex_.size() != 0; }

    /// ProduceResponses decoded this duty cycle, for the owner to route with plain
    /// calls (buffered only when no produceResponseFn_ test hook is installed).
    /// EOS-OOS: the error code travels on its own — an OK answer may carry
    /// baseOffset -1 (a known-old duplicate: written, offset unknown), which the
    /// old "< 0 = -ErrorCode" sentinel turned into error 1.
    struct ProduceAck {
        int64_t correlationId;
        int64_t baseOffset;  ///< meaningful when errorCode == 0 (-1: offset unknown)
        int16_t errorCode;   ///< rune::protocol::client::ErrorCode (0 = OK)
        int64_t logStartOffset = -1;  ///< D6: the partition's log start (with UnknownProducerId)
    };
    [[nodiscard]] std::span<const ProduceAck> produceAcks() const noexcept {
        return {produceAcks_.data(), produceAcks_.size()};
    }
    void clearProduceAcks() noexcept { produceAcks_.clear(); }

    /// Forward ProduceResponse frames to the egress observer (transactional
    /// producers need them).  Default true (historical behaviour).
    void setForwardProduceResponses(bool on) noexcept { forwardProduceResponses_ = on; }

    /// The Aeron client closed itself — or the T15b test fault was applied.
    [[nodiscard]] bool transportClosed() const noexcept { return testFault_ || transport_.closed(); }

    /// T15b: the Client failed for good (Aeron client closed / seed Rejected).
    /// From now on doWork() only drains the command ring: no driver / conductor
    /// pumping, no egress poll, no session ticks, no producer drain, and every
    /// offer is refused — no I/O at all.  Duty thread only.  Idempotent.
    void enterFailedMode() noexcept { failed_ = true; }
    [[nodiscard]] bool failedMode() const noexcept { return failed_; }

    /// T15b test seam (any thread): put the transport-fault command on the ring
    /// (the duty thread applies it: transportClosed() turns true).
    bool submitTestFault() noexcept { return writeCommand(kMsgTestFault, nullptr, 0); }

    /// T15b: the routed leader-node sessions (every session but the seed) and
    /// which of them are not Connected (bit i = session i + 1; the first 64).
    /// Duty thread only.
    [[nodiscard]] uint32_t leaderSessionCount() const noexcept {
        return static_cast<uint32_t>(sessions_.size() - 1);
    }
    [[nodiscard]] uint64_t leaderSessionsDownMask() const noexcept {
        uint64_t mask = 0;
        const std::size_t n = sessions_.size() < 65 ? sessions_.size() : 65;
        for (std::size_t i = 1; i < n; ++i) {
            if (sessions_[i]->machine.state() != session::SessionState::Connected) mask |= uint64_t{1} << (i - 1);
        }
        return mask;
    }
    /// T15b: the seed session's machine (read-only; duty thread only).
    [[nodiscard]] const session::SessionMachine& seedMachine() const noexcept { return seedSession().machine; }
    /// Test hook (T15a): read-only access to the transport's thread-model observers.
    [[nodiscard]] const transport::AeronTransport& testTransport() const noexcept { return transport_; }
    /// Test hook (T15a fix1): retarget the seed's default ingress publication, as
    /// the Client's stick-to-leader redirect path does.  Agent thread only.
    bool testRetargetSeed(const std::string& channel) noexcept { return retarget(channel); }
    /// Test hooks (T15a r2-m1): the seed's two Backoff fallbacks, and where the
    /// seed's default publication points now.  Agent thread only.
    bool testRetargetSeedToBootstrap() noexcept { return retargetSeedToBootstrap_(); }
    bool testRetargetSeedToLastKnownLeader() noexcept { return retargetSeedToLastKnownLeader_(); }
    [[nodiscard]] const std::string& testSeedIngressChannel() const noexcept { return transport_.ingressChannel(); }

    /// Test hooks.  Reserved ack-buffer capacity (the hot path must never grow it).
    [[nodiscard]] std::size_t testProduceAckCapacity() const noexcept { return produceAcks_.capacity(); }
    /// Fill the command ring with no-op commands until no registration fits
    /// (cold, allocates a scratch buffer).  The duty cycle drops no-ops.
    void testFillCommandRing();
    /// fix2 Important-1: write exactly ONE no-op command with the given payload
    /// length (ring record size = align(8 + payloadLen, 8)), for tests that
    /// need to position the ring's tail at a precise byte offset (e.g. to land
    /// a subsequent registration exactly on a wrap boundary). Returns false if
    /// the ring has no room. The duty cycle drops no-ops, same as the filler
    /// above.
    bool testWriteNoopCommand(std::size_t payloadLen) noexcept {
        std::vector<uint8_t> scratch(payloadLen, 0);
        return writeCommand(kMsgTestNoop, scratch.data(), payloadLen);
    }
    /// fix3 I1: claim ring space for a no-op WITHOUT committing — the record's
    /// length stays negative (claimed, not yet readable) until
    /// testCommitClaim() is called with the returned index. For regression
    /// tests of drainRegistrationsForClose()'s wait-past-an-uncommitted-claim
    /// behavior only. Returns a negative value on failure to claim (see
    /// M2ORingBuffer::tryClaim()).
    aeron::util::index_t testClaimNoopWithoutCommitting(std::size_t payloadLen) {
        return ring_.tryClaim(kMsgTestNoop, payloadLen);
    }
    /// fix3 I1: commit a claim previously returned by
    /// testClaimNoopWithoutCommitting().
    void testCommitClaim(aeron::util::index_t claimIndex) { ring_.commit(claimIndex); }
    /// fix5 Important-2(c): turn a claim from testClaimNoopWithoutCommitting()
    /// into a HEADER-LESS claim (length + type words zeroed) — the state of a
    /// writer that won the tail CAS but never wrote its record header.
    void testZeroClaimHeader(aeron::util::index_t claimIndex) noexcept {
        ring_.testZeroClaimHeader(claimIndex);
    }
    /// fix3 I1: called (if set) every time drainRegistrationsForClose() yields
    /// waiting on an uncommitted claim — lets a test commit the claim
    /// deterministically, from inside the wait loop, on the SAME thread,
    /// instead of needing a second thread and a real-time wait. Empty by
    /// default (zero cost in production).
    void testSetCloseDrainYieldHook(std::function<void()> fn) noexcept {
        testCloseDrainYieldHook_ = std::move(fn);
    }
    /// fix5 Important-2(c): override kCloseDrainUnblockAfterYields so a test
    /// can reach the unblock path without a million real yields. Set before
    /// close(); single-threaded.
    void testSetCloseDrainUnblockAfterYields(int n) noexcept { closeDrainUnblockAfterYields_ = n; }
    /// The close-drain yield cap (also used by the producers' closed-ring drain, C6a).
    [[nodiscard]] int closeDrainUnblockAfterYields() const noexcept { return closeDrainUnblockAfterYields_; }
    /// fix5 Important-2(c): number of times drainRegistrationsForClose() has
    /// called M2ORingBuffer::unblock() (owner / post-join reads only).
    [[nodiscard]] int testCloseDrainUnblockCalls() const noexcept { return closeDrainUnblockCalls_; }

    /// Task 1.4 (redirect convergence): route ONE pre-encoded produce frame to its
    /// partition leader session — the SAME routing the produce drain path uses
    /// (consult the route slots, lazily open the leader session, re-stamp the
    /// leader sessionId, offerTo).  Returns false (hold) when the leader session
    /// is not yet Connected; the caller retains the frame for a later resend.
    /// Used by the Client-level redirect handler and connect-triggered resend.
    /// AGENT-THREAD ONLY.
    bool offerToPartition(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                          const uint8_t* buf, std::size_t len) noexcept {
        if (failed_) [[unlikely]] return false;  // T15b: a failed client does no I/O
        return offerProduceToLeader_(topicId, partition, ingressStreamId, buf, len, {});
    }
    /// Backlog B1: a freshly encoded frame whose RecordBatch CRC is still unset —
    /// the transport seals it (into the term AND `buf`) only once Aeron accepted
    /// it (AeronTransport::offer).  AGENT-THREAD ONLY.
    bool offerToPartition(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                          uint8_t* buf, std::size_t len, transport::FrameCrc crc) noexcept {
        if (failed_) [[unlikely]] return false;  // T15b: a failed client does no I/O
        return offerProduceToLeader_(topicId, partition, ingressStreamId, buf, len, crc);
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
    /// when a kMsgRegisterProducer command is drained.  The argument is the impl
    /// pointer passed to submitRegisterProducer (F8c C7: a ProducerImpl*).  The
    /// Client wires this to adopt the producer into the ProducerHub list.  Set
    /// before start().
    using ProducerRegisterFn = std::function<void(void*)>;
    void setProducerRegisterFn(ProducerRegisterFn fn) noexcept {
        producerRegisterFn_ = std::move(fn);
    }
    void setTxnProducerRegisterFn(ProducerRegisterFn fn) noexcept {
        txnProducerRegisterFn_ = std::move(fn);
    }

    /// F8a fix2: control frames dropped because their session no longer exists,
    /// and frames the transport refused — ring-drained or duty-thread direct
    /// sends (each retried later).
    /// Duty thread only (or after the runner is joined).
    [[nodiscard]] uint64_t staleFramesDropped() const noexcept { return staleFramesDropped_; }
    [[nodiscard]] uint64_t ingressOfferRefused() const noexcept { return ingressOfferRefused_; }
    [[nodiscard]] uint64_t ingressFatalDropped() const noexcept { return ingressFatalDropped_; }
    /// F8a fix3: the transport's max frame length (immutable; any thread).
    [[nodiscard]] std::size_t maxFrameLength() const noexcept {
        const std::size_t m = transport_.maxMessageLength();
        return (testMaxFrameCap_ != 0 && testMaxFrameCap_ < m) ? testMaxFrameCap_ : m;
    }
    /// fix4 test seam (Invoker tests, owner thread only — a plain field):
    /// cap maxFrameLength() below the transport's (0 = uncapped).
    void testCapMaxFrameLength(std::size_t cap) noexcept { testMaxFrameCap_ = cap; }
    /// fix3 test seam (duty thread): the next `n` ring-drained kMsgSend offers
    /// return `r` instead of reaching the transport.
    void testForceIngressOffers(transport::AeronTransport::OfferResult r, int n) noexcept {
        testForcedOffer_ = r;
        testForcedOffers_ = n;
    }

    /// F8a fix2/fix4: duty-thread direct send on the default ingress
    /// publication (the one kMsgSend frames use) — for frames whose owner
    /// retries them itself instead of going through the ring.  Returns the
    /// 3-way verdict: Retry = transiently refused (the owner retries), Fatal =
    /// never deliverable (counted in ingressFatalDropped, rate-limited log; the
    /// owner drops it).  Honours testForceIngressOffers.
    transport::AeronTransport::OfferResult offerDefault(const uint8_t* buf, std::size_t len) noexcept;

    /// F8a: hook invoked (duty thread, or close()'s drain) for every drained
    /// kMsgHubCommand.  Set before start().  Cold path (control-plane commands).
    using HubCommandFn = std::function<void(const ::rune::client::detail::HubCommand&)>;
    void setHubCommandFn(HubCommandFn fn) noexcept {
        hubCommandFn_ = std::move(fn);
    }

    /// F8c C3: hook invoked (duty thread, or close()'s drain) for every drained
    /// kMsgControlOp with the record bytes (header + frame).  Set before start().
    using ControlOpFn = std::function<void(const uint8_t*, std::size_t)>;
    void setControlOpFn(ControlOpFn fn) noexcept { controlOpFn_ = std::move(fn); }

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
    /// ProduceResponse frame arrives.  Signature: (correlationId, baseOffset,
    /// errorCode).
    using ProduceResponseFn = std::function<void(int64_t, int64_t, int16_t)>;

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

    /// The seed session's state.  F8c C3: DUTY-THREAD ONLY (the thread that
    /// drives doWork(), or the closing thread once no duty thread runs) — a
    /// plain read of duty-owned state.  The cross-thread atomic snapshot is
    /// gone: an app thread learns the session state from Client::state()
    /// (a duty-published StatusRing) and op outcomes from completions.
    [[nodiscard]] session::SessionState sessionState() const noexcept;

    /// The seed session's id.  Duty-thread only (see sessionState()).
    [[nodiscard]] int64_t sessionId() const noexcept;

    /// (state, sessionId) of the seed session.  Duty-thread only — both are
    /// read from the one duty-owned SessionMachine, so they are coherent.
    struct StateSnapshot {
        session::SessionState state;
        int64_t               sessionId;
    };
    [[nodiscard]] StateSnapshot stateAndSessionId() const noexcept;

    // CB-2: Allow Client to access ring_ for ProducerImpl construction.
    friend class ::rune::client::Client;

    // Internal accessor for producer tests (CB-2): returns the ring buffer so
    // ProducerImpl can be constructed directly in test harnesses.
    // NOT part of the public API; production code must use Client::producer().
    rune::client::detail::M2ORingBuffer& ringForTesting() noexcept {
        return ring_;
    }

    // TEST SEAMS (duty thread only; A10 session-liveness regression): inject one
    // reassembled egress frame / tick every session machine with an explicit clock.
    void dispatchEgressFrameForTest(int64_t nowNs, const uint8_t* buf, std::size_t len) noexcept {
        dispatchEgressFrame(nowNs, buf, len);
    }
    void tickSessionsForTest(int64_t nowNs) noexcept {
        for (auto& s : sessions_) s->machine.tick(nowNs);
    }
    session::SessionMachine& sessionMachineForTest(std::size_t i) noexcept { return sessions_[i]->machine; }
    session::SessionMachine& sessionForEndpointForTest(const std::string& endpoint) noexcept {
        return sessionForEndpoint(endpoint).machine;
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
    static constexpr int32_t kMsgRegisterTxnProducer = 7; // shared-nothing txn producer registration
    static constexpr int32_t kMsgHubCommand = 8;          // F8a subscriber / group-consumer command
    static constexpr int32_t kMsgControlOp  = 9;          // F8c C3 client control op (header + frame)
    static constexpr int32_t kMsgTestFault = 98;          // T15b test seam: transportClosed() -> true
    static constexpr int32_t kMsgTestNoop = 99;           // testFillCommandRing filler (dropped)

    // Task 1.3: per-session ConnectRequest correlationId range stride.  Session i
    // starts its correlationId counter at i*stride so concurrent connects demux
    // unambiguously off the one shared egress subscription.  1e12 is far above any
    // realistic per-session connect-retry count and stays well within int64.
    static constexpr int64_t kSessionCorrIdStride = 1'000'000'000'000LL;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    bool writeCommand(int32_t msgTypeId, const uint8_t* payload, std::size_t payloadLen) noexcept;
    /// writeCommand() with the Full / Closed distinction (C2).
    Submit writeCommand3_(int32_t msgTypeId, const uint8_t* payload, std::size_t payloadLen) noexcept;
    void dispatchEgressFrame(int64_t nowNs, const uint8_t* buf, std::size_t len) noexcept;

    /// Task 1.3: resolve the Session whose IN-FLIGHT ConnectRequest carries
    /// `corrId` (its SessionMachine's current correlationId while Connecting).
    /// Falls back to the seed session when no session matches (single-node, a
    /// steady-state frame, or an already-established session).  Agent-thread only.
    Session& sessionForConnectCorrId(int64_t corrId) noexcept;

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
    uint64_t                    heldOffers_ = 0;  // DR-7: see heldOffers()
    std::vector<PartitionRoute> routes_;
    std::vector<uint32_t>       freeRoutes_;
    std::vector<ProduceAck>     produceAcks_;  // reserved in the ctor; owner clears
    bool                        forwardProduceResponses_ = true;
    bool                        mnDebug_ = false;  // RUNE_MN_DEBUG, read ONCE

    /// client<->broker TLS/AEAD (Task 6): reusable decrypt scratch for
    /// dispatchEgressFrame's data-plane open path — holds the reconstructed
    /// [24B header][plaintext body] a decrypted egress frame is checkFrame'd /
    /// switched on.  Single-writer (the agent's own doWork thread), grows to
    /// the largest egress frame decrypted so far and never shrinks (zero
    /// steady-state allocation once warmed up) — same shape as AeronTransport's
    /// cryptoSealScratch_ on the encrypt side.
    std::vector<uint8_t>        egressDecryptScratch_;
    uint32_t sessionIndexFor_(const std::string& endpoint) noexcept;
    void releaseRoutePub_(transport::AeronTransport::PubHandle h) noexcept;  // M4

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
    bool offerControlTo(Session& owner, const uint8_t* buf, std::size_t len) noexcept;
    /// Open a NEW session to `endpoint` (async connect, disjoint corrId range).
    Session& openSession_(const std::string& endpoint) noexcept;
    /// T15a fix1 I1: point the default ingress publication (the seed session's
    /// control path) back at the configured gateway and forget the stuck-to
    /// leader.  No-op when already there.  Agent thread only (cold path).
    bool retargetSeedToBootstrap_() noexcept;
    /// T15a fix2 A-B1: the inverse — back to seed.lastKnownLeaderChannel (no-op
    /// when none is known or already there).  Agent thread only (cold path).
    bool retargetSeedToLastKnownLeader_() noexcept;

    // ---- Task 1.4: shared produce→leader routing (drain path + redirect resend) --
    // Probe the route slot, resolve its session / pub once, hold if the session is not
    // Connected, else re-stamp the leader sessionId (header offset 8) and offerOn.
    // Single-writer (agent thread).  Definition mirrors the produce drain send-lambda.
    bool offerProduceToLeader_(int32_t topicId, int32_t partition, int32_t ingressStreamId,
                               const uint8_t* buf, std::size_t len,
                               transport::FrameCrc crc) noexcept;

    // ---- client<->broker TLS/AEAD (Task 6b): seal every OTHER non-handshake --
    // client->broker send site under its DESTINATION session's CryptoState.
    // Shared by offerControlTo / offerIngress_ / offerOnSession (every
    // non-Produce publication write) — offerProduceToLeader_ already seals on
    // its own (via sealForOffer) and does not call this.  No-op (buf/len
    // unchanged) when `crypto.encrypted` is false — byte-identical plaintext
    // path.  Reseats buf/len to the transport's reusable seal scratch on
    // success; returns false (DROP, never transmit) on seal failure.
    [[nodiscard]] bool sealForDestination_(session::CryptoState& crypto,
                                           const uint8_t*& buf, std::size_t& len) noexcept;

    // ---- Ring buffer (MPSC: many app threads write, one agent thread reads) -
    // Buffer size = power-of-two capacity + trailer.
    // Stored as aligned raw bytes; AtomicBuffer wraps it.
    static constexpr std::size_t kRingAlignment = 64; // cache-line aligned
    std::unique_ptr<uint8_t, void(*)(void*)> ringBuf_;
    rune::client::detail::M2ORingBuffer      ring_;

    // ---- Lifecycle -----------------------------------------------------------
    // F8c C0: plain owner state, no atomics.  start() runs on the constructing
    // thread before any duty thread exists; stop() runs only once no duty
    // thread can be inside doWork() — Client::closeImpl_ after joining the
    // runner (Thread mode) / on the owner thread (Invoker), or the destructor.
    // A cross-thread stop REQUEST never touches these: it goes through the
    // runner shutdown path (Client::close(): stop the runner loop, join it, then
    // stop()) or the command ring (submitClose()).  The thread creation / join
    // edges order every access, so doWork()'s read below is race-free.
    bool started_ = false;
    bool stopped_ = false;
    bool failed_    = false;  // T15b failed mode (duty thread only)
    bool testFault_ = false;  // T15b test seam (duty thread only)

    // ---- Egress observer (agent thread only; must be set before start()) ----
    EgressObserver egressObserver_;

    // ---- CB-2: Producer hooks (agent thread only; set before start()) --------
    ProducerDrainFn    producerDrainFn_;
    ProduceResponseFn  produceResponseFn_;
    ProduceResendFn    produceResendFn_;    // Task 1.4: fired on session-connect (multi-node)
    ProducerRegisterFn producerRegisterFn_; // shared-nothing producer registration (ring-drained)
    ProducerRegisterFn txnProducerRegisterFn_; // same, transactional producers
    HubCommandFn       hubCommandFn_;          // F8a subscriber / group-consumer commands
    ControlOpFn        controlOpFn_;           // F8c C3 client control ops
    // F8a fix2: duty-thread counters for the kMsgSend drain rule (plain; read
    // only on the duty thread / after the runner is joined).
    uint64_t staleFramesDropped_  = 0;
    uint64_t ingressOfferRefused_ = 0;
    uint64_t ingressFatalDropped_ = 0;
    // fix3 test seam: force the next N ring-drained offers' verdicts.
    int testForcedOffers_ = 0;
    transport::AeronTransport::OfferResult testForcedOffer_ = transport::AeronTransport::OfferResult::Accepted;
    std::size_t testMaxFrameCap_ = 0;  // fix4 test seam (0 = uncapped)
    bool     lastConnectAccepted_ = false;  // MAJOR-2 (see lastConnectAccepted())
    bool     lastConnectAwaited_  = false;
    uint32_t lastConnectSession_  = 0;
    std::size_t testCommandMsgCap_ = 0;  // F8c fix1 test seam (0 = uncapped)
    bool        testCloseDrainNoUnblock_ = false;  // F8c fix1 test seam
    transport::AeronTransport::OfferResult offerIngress_(const uint8_t* buf, std::size_t len) noexcept;
    void noteFatalDrop_(std::size_t len) noexcept;

    // fix3 I1: test-only observability hook for drainRegistrationsForClose()'s
    // wait loop (see testSetCloseDrainYieldHook()). Empty by default.
    std::function<void()> testCloseDrainYieldHook_;
    // fix5 Important-2(c): close-drain unblock threshold (test-overridable)
    // and how many times it fired. Close-path / owner only — plain ints.
    int closeDrainUnblockAfterYields_ = kCloseDrainUnblockAfterYields;
    int closeDrainUnblockCalls_       = 0;

    // ---- MD1 Task 10: Metadata response hook (agent thread only) -------------
    MetadataResponseFn metadataResponseFn_;

};

} // namespace rune::client::agent
