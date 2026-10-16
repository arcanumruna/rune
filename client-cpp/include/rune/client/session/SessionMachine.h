// rune/client/session/SessionMachine.h
//
// CC-4 — Pure, clock-injected client session state machine.
// Namespace: rune::client::session
//
// Design:
//   - Transport-agnostic: the machine NEVER touches Aeron directly.
//     It calls an injected SessionIo for sendFrame / retarget.
//   - Clock-injected: tick(nowNs) uses ONLY the passed nowNs; no wall-clock.
//   - Single-writer: all methods called from ONE thread. No locks.
//   - Zero per-tick heap steady-state: encodes into a member scratch buffer.
//   - Bounded redirect/retry loops (configurable cap; default 5).
//   - Uses CC-2 codec adapters (rune::client::codec) for all wire I/O.
//   - Per-subscription offset tracking is Milestone-B scaffold only.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <rune/client/codec/Codec.h>

// client<->broker TLS/AEAD (Task 5) — cold-path handshake drive.  TlsSession/AeadCipher
// are forward-declared-opaque wrappers around OpenSSL; including them here does NOT
// pull OpenSSL into this header's callers' include surface (see rune-crypto headers).
#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/crypto/TlsSession.h>

namespace rune::client::session {

// ---------------------------------------------------------------------------
// Session state enum
// ---------------------------------------------------------------------------
enum class SessionState : uint8_t {
    Disconnected,
    Connecting,
    Connected,
    Resuming,  // real waiting state: entered, then doResume() fires on the NEXT tick
    Closed,    // terminal; idempotent after this
    // T15a R1 — reachable ONLY with SessionConfig::reconnectForeverAfterLive:
    Backoff,   // was Live, retry budget exhausted: waits until nextAttemptNs(), then
               // a resume-first ConnectRequest.  Capped exponential, never gives up.
    Rejected,  // terminal: a non-retriable answer (rejectCode(), e.g. ProtocolVersion)
    // client<->broker TLS/AEAD (Task 5): entered from Connecting when the broker's
    // ConnectResponse carries a chosenCipher != 0 (crypto negotiated).  A TLS 1.3
    // handshake is driven over HandshakeContinue/HandshakeContinueResponse legs; on
    // done() the session derives keys and moves to Connected.  A verify/export
    // failure is non-retriable -> Rejected (ErrorCode::HandshakeFailed).
    Handshaking,
};

// ---------------------------------------------------------------------------
// SessionIo — narrow transport interface injected into the machine.
//
// Implementations: real Aeron (CC-5) or a fake (tests).
// All methods called from the single-writer thread.
// ---------------------------------------------------------------------------
struct SessionIo {
    /// Send one encoded client→gateway frame.
    /// Returns true if the frame was accepted by the underlying transport.
    virtual bool sendFrame(const uint8_t* buf, std::size_t len) = 0;

    /// Retarget the transport at a new ingress endpoint (redirect / notleader).
    /// FIX F8: noexcept + returns success.  A false return means the retarget
    /// failed (e.g. resolve timeout) and the transport kept its previous
    /// ingress endpoint; the machine still proceeds to resend the
    /// ConnectRequest (a failed retarget does not throw and does not crash).
    virtual bool retarget(const std::string& newIngressChannel) noexcept = 0;

    /// T15a fix1 I1: point this session back at its configured BOOTSTRAP endpoint
    /// (the gateway it was built with), undoing any redirect / stick-to-leader
    /// retarget.  Called by a Backoff attempt (reconnectForeverAfterLive) so a
    /// session whose redirected leader died re-enters the cluster through the
    /// bootstrap, which redirects it to the current leader.  Cold path.  Default:
    /// no-op success (an IO whose endpoint never moves).
    virtual bool retargetToBootstrap() noexcept { return true; }

    /// T15a fix2 A-B1: point this session back at the last-known leader it was
    /// stuck to before the most recent retargetToBootstrap() (no-op when none is
    /// known).  Odd Backoff attempts >= 3 call it, so Backoff genuinely alternates
    /// bootstrap <-> last-known leader and converges if EITHER is alive.  Cold
    /// path.  Default: no-op success.
    virtual bool retargetToLastKnownLeader() noexcept { return true; }

    virtual ~SessionIo() = default;
};

// ---------------------------------------------------------------------------
// SessionConfig — machine configuration with sensible defaults.
// ---------------------------------------------------------------------------
struct SessionConfig {
    std::string egressChannel  = "aeron:udp?endpoint=localhost:40200";
    int32_t     egressStreamId = 1001;
    // D6: announces UnknownProducerId + ProduceResponse.logStartOffset support
    // (a broker answers 28 only at >= kClientVersionUnknownProducerId).
    int32_t     clientVersion  = rune::protocol::client::kClientVersion;

    // Timeouts / intervals (nanoseconds).
    int64_t connectTimeoutNs    = 5'000'000'000LL;   // 5 s  — give up one connect attempt
    int64_t keepaliveIntervalNs = 1'000'000'000LL;   // 1 s  — send keepalive this often
    int64_t responseTimeoutNs   = 10'000'000'000LL;  // 10 s — stale session threshold
    /// A10: correlationId of this session's keepalive Ping.  The agent gives each session its own negative
    /// value so the Pong is credited to exactly the session it answers (broker session ids are per node and
    /// collide across nodes; application pings use ids >= 1).
    int64_t keepaliveCorrelationId = 0;

    // DR-BUG-2 FIX: fast-retry cadence for ConnectRequest when the ingress
    // publication was not-connected or back-pressured at send time.  The
    // SessionMachine retries the ConnectRequest at this interval (default 200 ms)
    // without waiting the full connectTimeoutNs.  Bounded: at most
    // connectTimeoutNs / connectRetryIntervalNs retries per connect attempt
    // (capped by maxConnectRetries episodes).  Connect-path only — no effect on
    // the steady-state produce/subscribe hot path.
    int64_t connectRetryIntervalNs = 200'000'000LL;  // 200 ms fast-retry cadence

    // Redirect/retry caps — prevent infinite loops.
    int maxRedirects     = 5;  // per connect() call
    int maxConnectRetries = 3;  // connect-timeout retries before → Disconnected
    int maxResumeRetries  = 3;  // Resuming → Connecting retries before → Disconnected

    // Task 1.3 (multi-node): starting value for this session's ConnectRequest
    // correlationId counter.  A client opening N sessions (one per leader node)
    // over the ONE shared egress subscription demuxes each ConnectResponse by its
    // correlationId; giving each session a DISJOINT correlationId range guarantees
    // no cross-session collision while multiple sessions connect concurrently.
    // Default 0 (first corrId = 1) → single-session behaviour is byte-identical.
    int64_t correlationIdBase = 0;

    // T15a R1 — "never give up once Live" (shard clients set it; a plain Client
    // keeps false = today's behaviour).  Once this session has been Live (first
    // ConnectResponse{OK}), every retry-budget exhaustion above (maxRedirects,
    // maxConnectRetries, maxResumeRetries) and every retriable error answer goes
    // to Backoff instead of terminal Disconnected; the budgets then mean
    // "attempts before Backoff" (the fast first retries are kept).  A
    // non-retriable answer (ProtocolVersion) goes to terminal Rejected.  A
    // session that was never Live keeps today's first-connect budget.
    bool    reconnectForeverAfterLive = false;
    int64_t backoffInitialNs = 10'000'000LL;     // 10 ms first Backoff delay, doubling...
    int64_t backoffMaxNs     = 1'000'000'000LL;  // ...capped at 1 s

    // client<->broker TLS/AEAD (Task 5) — cold-path handshake configuration.
    // Toggle (design spec §11, RUNE_TLS): false = today's plaintext connect, byte-
    // identical to pre-Task-5 (every existing SessionMachineTest keeps passing,
    // including ones that pin an explicit pre-crypto clientVersion like 1).  true
    // = connect() offers a v4 ConnectRequest carrying cipherList/handshakeBytes/
    // authToken and drives the TLS handshake described below.
    bool offerCrypto = false;
    // trust: nullopt = trust the system store (production default); "" = trust
    // nothing (tests, deliberately untrusted); non-empty = a PEM CA bundle.
    std::optional<std::string> tlsTrustPem;
    // Hostname checked against the broker cert's SAN (SNI + hostname verification).
    // "" installs NO hostname check (TlsSession::makeClient contract) — set this to
    // the broker's DNS name in production; left empty only for loopback/tests.
    std::string tlsExpectHost;
    // Client authZ token carried inside the (now-encrypted-by-TLS) Connect handshake
    // payload; "" = none offered.  Validated by the broker's existing auth path.
    std::string authToken;

    // Task 9 (downgrade safety, spec §11 require_tls's client-side mirror):
    // when true, a ConnectResponse{OK} that negotiates NO cipher (chosenCipher
    // == 0 — a plaintext broker, or a MITM that stripped the crypto offer off
    // the wire before the broker saw it) must NOT silently proceed to a
    // plaintext Connected session.  onConnectResponse instead calls
    // onHandshakeFailed_() -> terminal Rejected(HandshakeFailed), exactly like
    // a real TLS handshake failure.  Meaningless (and left false) unless
    // offerCrypto is also true.  Default false (back-compat).
    bool requireCrypto = false;
};

// ---------------------------------------------------------------------------
// CryptoState — per-session crypto state (client<->broker TLS/AEAD, Task 5).
// Populated once the TLS 1.3 handshake completes (see SessionMachine::connect /
// onHandshakeBytes); `send`/`recv` are the two directional AeadCiphers derived
// from the TLS exporter.  Exactly the variables THIS state needs (rule 3) — no
// scattered booleans elsewhere in SessionMachine track crypto readiness.
//
// Per-sender egress (Task 20 / per-worker-egress-crypto-addendum): in shard
// mode a session has MULTIPLE independent broker->client senders — the
// gateway (senderId 0, `recv`/`recvSeq`/`recvWindow` below, UNCHANGED from
// pre-Task-20) and each shard worker i (senderId i+1). Workers seal with a
// key the client never receives over the wire; the client re-derives it
// locally from the retained `b2cKey`/`b2cPrefix` via
// rune::crypto::deriveSenderKey (same derivation the broker used), so `b2cKey`
// (the raw b2c master key) and `b2cPrefix` must be retained here — they used
// to be consumed directly into `recv` and discarded. `workerRecv[senderId-1]`
// is a Rule-3 slot holding EXACTLY the three things a non-gateway sender's
// receive stream needs — its own lazily-derived+cached AeadCipher and its own
// INDEPENDENT replay window (recvSeq/recvWindow) — so a replay/forged seq on
// one worker's stream can never affect the gateway's or another worker's.
// Fixed-size array (senders are few: the gateway + up to kMaxShardWorkers
// shard workers) — no heap, no map, zero per-frame allocation; a cipher is
// constructed only once, on first sight of that senderId.
// ---------------------------------------------------------------------------
struct CryptoState {
    bool                  encrypted  = false;
    rune::crypto::Cipher  cipher{};
    uint16_t              epoch      = 0;
    uint64_t              sendSeq    = 0;
    uint64_t              recvSeq    = 0;      // sender 0 (gateway) — UNCHANGED pre-Task-20 field
    uint64_t              recvWindow = 0;      // sender 0 (gateway) — UNCHANGED pre-Task-20 field
    std::optional<rune::crypto::AeadCipher> send, recv;  // recv == sender 0 (gateway)

    // Retained raw b2c key + nonce prefix (Task 20): lets the client re-derive
    // any worker's per-sender key locally, matching the broker's derivation.
    // No key material ever crosses the wire. Only meaningful when `recv` is
    // engaged (i.e. `encrypted`); populated alongside `recv` in finishHandshake_.
    rune::crypto::AeadKey b2cKey{};
    uint8_t               b2cPrefix[rune::crypto::kNoncePrefix] = {};

    // One slot per non-gateway sender (senderId k >= 1, indexed k-1).
    struct WorkerRecv {
        std::optional<rune::crypto::AeadCipher> cipher;
        uint64_t recvSeq    = 0;
        uint64_t recvWindow = 0;
    };
    // CROSS-REPO INVARIANT: must stay >= rune::node::NodeConfig::kMaxShardWorkers
    // (rune-broker-tls, libs/node/include/rune/node/NodeConfig.h — currently 64,
    // enforced by NodeConfig::validate). Worker i seals with senderId = i+1, so a
    // broker legitimately configured with up to NodeConfig::kMaxShardWorkers
    // workers can produce senderIds up to that same count; this table must have a
    // slot for every one of them or the client's selectRecvSlot fail-closed path
    // (idx >= kMaxShardWorkers) silently (and permanently) drops that worker's
    // Records. The two repos can't share a header, so this value is NOT derived
    // from NodeConfig — if the broker's max ever changes, mirror it here too.
    static constexpr uint8_t kMaxShardWorkers = 64;
    std::array<WorkerRecv, kMaxShardWorkers> workerRecv{};
};

// ---------------------------------------------------------------------------
// SubscriptionOffset — Milestone-B scaffold only.
// Tracks the last acknowledged offset per topic/subscription key so that
// re-connects can carry a resume hint.
//
// TODO(Milestone B): populate from actual subscription acknowledgements,
//                   and encode in ConnectRequest.resumeFromOffset once the
//                   server-side resume protocol is defined.
// ---------------------------------------------------------------------------
struct SubscriptionOffset {
    int64_t key        = 0;  // topic / subscription id
    int64_t lastOffset = 0;  // last acknowledged offset
};

// ---------------------------------------------------------------------------
// SessionMachine — the state machine.
// ---------------------------------------------------------------------------
class SessionMachine {
public:
    // Scratch buffer for encoding frames (no per-tick heap).
    // 512 bytes is enough for any control message (ConnectRequest with a
    // typical channel string is ≤ 256 bytes; Keepalive/Disconnect are 24-26).
    static constexpr std::size_t kScratchCap = 512;

    // ------------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------------
    explicit SessionMachine(SessionIo& io, SessionConfig cfg = {}) noexcept
        : io_(io)
        , cfg_(std::move(cfg))
        , correlationIdCounter_(cfg_.correlationIdBase) // Task 1.3: disjoint per-session range
    {}

    // Non-copyable, non-movable (holds reference to io_).
    SessionMachine(const SessionMachine&)            = delete;
    SessionMachine& operator=(const SessionMachine&) = delete;
    SessionMachine(SessionMachine&&)                 = delete;
    SessionMachine& operator=(SessionMachine&&)      = delete;

    // ------------------------------------------------------------------------
    // Observers
    // ------------------------------------------------------------------------
    [[nodiscard]] SessionState state()         const noexcept { return state_; }
    [[nodiscard]] int64_t      sessionId()     const noexcept { return sessionId_; }
    [[nodiscard]] int64_t      keepaliveCorrelationId() const noexcept { return cfg_.keepaliveCorrelationId; }
    [[nodiscard]] int64_t      correlationId() const noexcept { return correlationId_; }
    [[nodiscard]] int           redirectCount() const noexcept { return redirectCount_; }
    /// T15a R1: latched on the first ConnectResponse{OK}.
    [[nodiscard]] bool    everLive()       const noexcept { return everLive_; }
    /// Backoff state: consecutive Backoff entries since the last Live (0 when Live).
    [[nodiscard]] int     backoffAttempt() const noexcept { return backoff_.attempt; }
    /// DR-7: connect attempts that ended without a session (timed out / refused);
    /// monotonic — each increment is an event (the owner asks for fresh metadata).
    [[nodiscard]] uint64_t connectFailures() const noexcept { return connectFailures_; }
    /// No-hang fix round 4 (item 6): Connected → Resuming edges caused by THIS
    /// client's own liveness check (response timeout / own stall) — the session
    /// is treated as lost, exactly as if the broker had sent SessionExpired.
    [[nodiscard]] uint64_t staleSessionLosses() const noexcept { return staleSessionLosses_; }
    /// DR client bugs fix round 1 (M1): the subset of staleSessionLosses() caused
    /// by the CLIENT's own stall (a tick gap > responseTimeoutNs) — the broker
    /// was not shown dead.
    [[nodiscard]] uint64_t ownStallLosses() const noexcept { return ownStallLosses_; }
    /// DR client bugs fix round 1 (I2): the error code of the latest connect
    /// attempt refused by the broker (0 = none since the last ConnectResponse{OK}),
    /// or kHandshakeTimedOut (fix round 3, M-2: client-local, never on the wire,
    /// retriable).
    [[nodiscard]] int16_t lastConnectError() const noexcept { return lastConnectError_; }
    static constexpr int16_t kHandshakeTimedOut = -2;   ///< client-local lastConnectError(): never on the wire
    /// Retrying cannot change these answers (public for the owner's failover).
    [[nodiscard]] static bool nonRetriableConnectError(int16_t code) noexcept {
        return isNonRetriable_(static_cast<rune::protocol::client::ErrorCode>(static_cast<uint16_t>(code)));
    }
    /// Backoff state: injected-clock time of the next attempt.
    [[nodiscard]] int64_t nextAttemptNs()  const noexcept { return backoff_.nextAttemptNs; }
    /// Rejected state: the non-retriable error code (0 otherwise).
    [[nodiscard]] int16_t rejectCode()     const noexcept { return rejectCode_; }
    /// client<->broker TLS/AEAD (Task 5): the session's crypto state.  `encrypted`
    /// is false until the handshake completes (or if no cipher was negotiated).
    [[nodiscard]] const CryptoState& cryptoState() const noexcept { return cryptoState_; }
    /// Mutable overload — AeadCipher::seal/open are non-const (they advance the
    /// reused EVP_CIPHER_CTX per call), so the Task 6 data-path seal/open seams
    /// (and this task's own round-trip test) need write access to send/recv.
    [[nodiscard]] CryptoState& cryptoState() noexcept { return cryptoState_; }

    // ------------------------------------------------------------------------
    // External API — called by the application / CC-5 agent.
    // ------------------------------------------------------------------------

    /// Fix round 3 (N-I1): the seed's publication moved to ANOTHER broker while this
    /// session is not Connected — a resume hint (and its keys) is meaningless there: a
    /// resume is valid only on the broker that holds the session (another broker
    /// answers SessionExpired, or — require_tls — rejects the plaintext resume as
    /// HandshakeFailed, terminally).  The next request is a fresh connect, with a
    /// fresh TLS episode.  No-op while Connected / Handshaking / terminal.
    void abandonResumeHint() noexcept {
        if (state_ == SessionState::Connected || state_ == SessionState::Handshaking ||
            state_ == SessionState::Closed || state_ == SessionState::Rejected) return;
        sessionId_   = 0;
        cryptoState_ = CryptoState{};
        tls_.reset();
        hsBytes_.clear();
    }

    /// Transition Disconnected → Connecting (or re-try from Resuming).
    /// Encodes and sends a ConnectRequest, seeds the connect timer.
    /// No-op from any other state.
    void connect(int64_t nowNs) noexcept {
        if (state_ != SessionState::Disconnected) return;
        // Task 5: fresh TlsSession + ClientHello for this episode — begun by doConnect
        // (fix round 3 N-I1: every connect with no session to resume negotiates afresh).
        doConnect(nowNs, /*resetSessionId=*/true, /*freshEpisode=*/true);
    }

    /// CB-5 B: force a FRESH reconnect regardless of the current state.
    ///
    /// Used by the data-op redirect path: a Produce/Subscribe was rejected by a
    /// FOLLOWER with ErrorResponse{Redirect,leaderEndpoint}.  The agent has
    /// already retarget()ed the transport to the leader.  The session is still
    /// logically Connected to the OLD (follower) sessionId, so connect() would
    /// no-op.  forceReconnect() drops the old session and issues a brand-new
    /// ConnectRequest (sessionId reset to 0) so the LEADER assigns a fresh
    /// sessionId.  Pending redirected data ops are resent — re-stamped with the
    /// new sessionId — once the ConnectResponse{OK} arrives (see Client.cpp).
    ///
    /// No-op only from Closed and Rejected (both terminal).
    void forceReconnect(int64_t nowNs) noexcept {
        if (state_ == SessionState::Closed || state_ == SessionState::Rejected) return;
        // Force to Disconnected first so doConnect's Connecting transition is
        // a clean fresh episode (reset sessionId + all retry/redirect counters).
        state_ = SessionState::Disconnected;
        // Task 5: a fresh TlsSession (any prior one is abandoned) — begun by doConnect.
        doConnect(nowNs, /*resetSessionId=*/true, /*freshEpisode=*/true);
    }

    /// Connected → Closed: send a Disconnect frame (clean shutdown).
    /// Resuming / Backoff → Closed: send a Disconnect frame if a session exists.
    /// No-op from any other state.
    void disconnect() noexcept {
        if (state_ == SessionState::Connected) {
            sendDisconnect();
            transitionTo(SessionState::Closed);
            return;
        }
        if (state_ == SessionState::Resuming || state_ == SessionState::Backoff) {
            // A session was established before; send a polite goodbye if possible.
            if (sessionId_ != 0) {
                sendDisconnect();
            }
            transitionTo(SessionState::Closed);
            return;
        }
        if (state_ == SessionState::Handshaking) {
            // Task 5: sessionId_ was already assigned (the broker's first
            // ConnectResponse leg), so a polite goodbye is possible even though
            // the TLS handshake never finished; abandon the in-flight TlsSession.
            if (sessionId_ != 0) {
                sendDisconnect();
            }
            tls_.reset();
            transitionTo(SessionState::Closed);
            return;
        }
        // All other states: no-op.
    }

    /// From ANY state → Closed, idempotent.  No frame sent.
    void close() noexcept {
        transitionTo(SessionState::Closed);
    }

    // ------------------------------------------------------------------------
    // Inbound — fed by the CC-5 agent after decoding an egress frame.
    // ------------------------------------------------------------------------

    /// A ConnectResponse arrived from the gateway.
    /// Must be called in Connecting; ignored from all other states.
    void onConnectResponse(int16_t  code,
                           int32_t  leaderMemberId,
                           int64_t  assignedSessionId,
                           int64_t  incomingCorrelationId,
                           int64_t  nowNs,
                           // client<->broker TLS/AEAD (Task 5) — additive trailing
                           // params; default-0/empty keeps every pre-Task-5 call
                           // site (5 args) byte-for-byte unchanged in behaviour.
                           uint8_t  chosenCipher = 0,
                           std::string_view handshakeBytes = {}) noexcept {
        // FIX E: remove the redundant Closed guard before the !=Connecting guard.
        if (state_ != SessionState::Connecting) return;  // ignore in wrong state

        // Stale / mismatched correlationId — ignore.
        if (incomingCorrelationId != correlationId_) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));

        if (ec == rpc::ErrorCode::OK) {
            sessionId_      = assignedSessionId;
            lastActivityNs_ = nowNs;

            // Task 5: the broker chose a cipher -> crypto negotiated, continue the
            // TLS handshake instead of completing the connect.  tls_ must be the
            // live TlsSession this episode's ClientHello came from (beginCryptoHandshake_
            // in connect()/forceReconnect()); if it's missing (e.g. local TLS init
            // failed) a cipher-bearing answer can't be driven -> treat as a failure.
            if (chosenCipher != 0) {
                if (!tls_) {
                    onHandshakeFailed_(nowNs);
                    return;
                }
                chosenCipher_ = static_cast<rune::crypto::Cipher>(chosenCipher);
                transitionTo(SessionState::Handshaking);
                onHandshakeBytes(reinterpret_cast<const uint8_t*>(handshakeBytes.data()),
                                  handshakeBytes.size(), nowNs);
                return;
            }

            // Task 9 (downgrade safety): requireCrypto means a plaintext
            // ConnectResponse{OK} (no cipher chosen) must NOT be treated as a
            // happy path — fail terminally exactly like a real handshake
            // failure, never silently proceed plaintext.  This is the
            // client-side mirror of the broker's require_tls: either side
            // alone defeats a MITM that strips the crypto offer off the wire.
            if (cfg_.requireCrypto && cfg_.offerCrypto) {
                onHandshakeFailed_(nowNs);
                return;
            }

            // Happy path — no crypto chosen (plaintext session, or a pre-v4 broker).
            tls_.reset();
            finishConnected_(nowNs);
            return;
        }

        if (ec == rpc::ErrorCode::Redirect || ec == rpc::ErrorCode::NotLeader) {
            // FIX B: ConnectResponse{Redirect/NotLeader} carries only a memberId.
            // There is NO member→endpoint map in Milestone A.
            // Resend to the same endpoint, bounded by the cap.
            // TODO(Milestone B): member-id→endpoint map for ConnectResponse-based redirect.
            (void)leaderMemberId;
            (void)assignedSessionId;

            if (redirectCount_ >= cfg_.maxRedirects) {
                onRetryBudgetExhausted_(nowNs);
                return;
            }
            redirectCount_++;
            // Resend ConnectRequest to the same endpoint (no retarget — no endpoint string).
            doConnect(nowNs, /*resetSessionId=*/false, /*freshEpisode=*/false);
            return;
        }

        // Any other error code (ProtocolVersion, SessionExpired, etc.) — disconnect
        // (T15a R1, flag on: Rejected if non-retriable, Backoff once Live).
        onConnectError_(ec, nowNs);
    }

    /// ConnectResponse with a redirect endpoint string.
    /// This is the primary overload used by tests and by CC-5 when the agent
    /// maps leaderMemberId → channel string.
    /// FIX B: calls io_.retarget(endpoint) BEFORE resending the ConnectRequest.
    void onConnectResponseRedirect(std::string_view newEndpoint,
                                   int64_t          incomingCorrelationId,
                                   int64_t          nowNs) noexcept {
        if (state_ != SessionState::Connecting) return;
        if (incomingCorrelationId != correlationId_) return;

        if (redirectCount_ >= cfg_.maxRedirects) {
            onRetryBudgetExhausted_(nowNs);
            return;
        }

        redirectCount_++;
        // FIX B: retarget BEFORE resending.
        // FIX F8: retarget is noexcept and returns success; a failed retarget
        // keeps the previous ingress endpoint — we still resend (bounded by the
        // redirect cap) rather than crash.
        (void)io_.retarget(std::string(newEndpoint));
        doConnect(nowNs, /*resetSessionId=*/false, /*freshEpisode=*/false);
    }

    // ------------------------------------------------------------------------
    // client<->broker TLS/AEAD (Task 5) — handshake drive.
    // ------------------------------------------------------------------------

    /// A HandshakeContinueResponse arrived (a later leg of a multi-round TLS
    /// handshake, after the first ConnectResponse leg).  Must be called in
    /// Handshaking; ignored from all other states or a stale correlationId.
    /// code == ErrorCode::OK means "continue" (handshakeBytes carries the next
    /// server flight, possibly empty if the client still owes output); any other
    /// code (notably HandshakeFailed) ends the handshake, non-retriably.
    void onHandshakeContinueResponse(int16_t          code,
                                      std::string_view handshakeBytes,
                                      int64_t          incomingCorrelationId,
                                      int64_t          nowNs) noexcept {
        if (state_ != SessionState::Handshaking) return;
        if (incomingCorrelationId != correlationId_) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));
        if (ec == rpc::ErrorCode::HandshakeFailed) {
            onHandshakeFailed_(nowNs);   // a TLS failure: non-retriable
            return;
        }
        if (ec != rpc::ErrorCode::OK) {
            // No-hang fix round 5 (N17): any other code (e.g. Internal: the
            // broker's pending-connect table was full and evicted this
            // handshake) is a CONNECT failure, not a TLS one — the same path
            // as a ConnectResponse error in Connecting (retriable unless
            // ProtocolVersion), never a terminal mislabelled HandshakeFailed.
            tls_.reset();
            hsBytes_.clear();
            onConnectError_(ec, nowNs);
            return;
        }
        onHandshakeBytes(reinterpret_cast<const uint8_t*>(handshakeBytes.data()),
                          handshakeBytes.size(), nowNs);
    }

    /// Feed inbound TLS handshake bytes (from ConnectResponse.handshakeBytes on the
    /// first leg, or HandshakeContinueResponse.handshakeBytes on later legs) into
    /// the in-flight TlsSession and drive it one step: emits a HandshakeContinue
    /// carrying any bytes TlsSession now has pending for the peer, and on done()
    /// exports keys -> builds CryptoState -> Connected.  A TLS verify/protocol
    /// failure is non-retriable -> Rejected (ErrorCode::HandshakeFailed).
    /// No-op outside Handshaking or with no in-flight TlsSession (defensive —
    /// onConnectResponse/onHandshakeContinueResponse already guard both before
    /// calling this).
    void onHandshakeBytes(const uint8_t* buf, std::size_t len, int64_t nowNs) noexcept {
        if (state_ != SessionState::Handshaking || !tls_) return;

        const auto hs = tls_->readInbound(buf, len);
        if (hs == rune::crypto::HsState::Failed) {
            onHandshakeFailed_(nowNs);
            return;
        }

        drainHandshakeOutbound_();
        if (!hsBytes_.empty()) {
            sendHandshakeContinue_();
        }

        if (tls_->done()) {
            finishHandshake_(nowNs);
        }
        // Else: waiting for more input from the broker (another
        // HandshakeContinueResponse) — nothing further to do this call.
    }

    /// An ErrorResponse arrived.
    /// SessionExpired / UnknownSession in Connected → Resuming.
    /// Redirect/NotLeader in Connecting with an endpoint string → retarget + resend.
    /// Redirect/NotLeader in Connecting without endpoint → resend to same endpoint.
    void onErrorResponse(int16_t          code,
                         std::string_view message,
                         int64_t          incomingCorrelationId,
                         int64_t          nowNs,
                         int64_t          noticeSessionId = 0) noexcept {
        if (state_ == SessionState::Closed) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));

        if (state_ == SessionState::Connected) {
            // F-A C1 (defence in depth): a session-fatal notice naming ANOTHER session (one this machine already
            // left) never moves this one.  0 = the notice names no session (kept: the old behaviour).
            if (noticeSessionId != 0 && sessionId_ != 0 && noticeSessionId != sessionId_) return;
            if (ec == rpc::ErrorCode::SessionExpired || ec == rpc::ErrorCode::UnknownSession) {
                transitionTo(SessionState::Resuming);
                // doResume fires on the next tick (FIX D: Resuming is a real waiting state).
                resumeEntryNs_ = nowNs;
            }
            return;
        }

        if (state_ == SessionState::Connecting) {
            if (incomingCorrelationId != correlationId_) return;

            if (ec == rpc::ErrorCode::Redirect || ec == rpc::ErrorCode::NotLeader) {
                if (redirectCount_ >= cfg_.maxRedirects) {
                    onRetryBudgetExhausted_(nowNs);
                    return;
                }
                redirectCount_++;

                // FIX B: if the message field carries a non-empty endpoint string,
                // this is the real broker-gateway redirect path — retarget.
                // FIX F8: retarget is noexcept; a failed retarget keeps the old
                // endpoint and we still resend (no throw into noexcept dispatch).
                if (!message.empty()) {
                    (void)io_.retarget(std::string(message));
                }
                // Resend ConnectRequest (retargeted if message was an endpoint).
                doConnect(nowNs, /*resetSessionId=*/false, /*freshEpisode=*/false);
            } else {
                onConnectError_(ec, nowNs);
            }
        }
    }

    /// Any inbound frame refreshes the activity timestamp (liveness proof).
    void onActivity(int64_t nowNs) noexcept {
        if (state_ == SessionState::Connected) {
            lastActivityNs_ = nowNs;
        }
    }

    /// The egress link dropped (e.g., Aeron image went unavailable).
    /// Connected → Resuming.  doResume fires on the NEXT tick (FIX D).
    void onTransportDisconnected(int64_t nowNs) noexcept {
        if (state_ != SessionState::Connected) return;
        transitionTo(SessionState::Resuming);
        // FIX D: enter Resuming as a real state; doResume fires on next tick.
        resumeEntryNs_ = nowNs;
    }

    // ------------------------------------------------------------------------
    // Clock-driven — called every agent cycle.
    // ------------------------------------------------------------------------

    /// Drive keepalive cadence, connect timeout, and response timeout.
    /// Uses ONLY the injected nowNs — no wall-clock.
    void tick(int64_t nowNs) noexcept {
        // A10: a gap between this machine's own ticks longer than the response
        // timeout is a stall of the CLIENT (SIGSTOP, a long pause): the broker may
        // have expired the session meanwhile, and frames buffered during the stall
        // (read just before this tick) prove nothing — treat the session as stale.
        const bool ownStall = lastTickNs_ != 0 && nowNs - lastTickNs_ > cfg_.responseTimeoutNs;
        lastTickNs_ = nowNs;
        if (ownStall && state_ == SessionState::Connected) [[unlikely]] {
            ++staleSessionLosses_;
            ++ownStallLosses_;   // M1: the client stalled, not the broker
            transitionTo(SessionState::Resuming);
            resumeEntryNs_ = nowNs;
            return;
        }
        switch (state_) {
        case SessionState::Connecting:
            tickConnecting(nowNs);
            break;
        case SessionState::Handshaking:
            tickHandshaking_(nowNs);
            break;
        case SessionState::Connected:
            tickConnected(nowNs);
            break;
        case SessionState::Resuming:
            tickResuming(nowNs);
            break;
        case SessionState::Backoff:
            tickBackoff_(nowNs);
            break;
        case SessionState::Disconnected:
        case SessionState::Closed:
        case SessionState::Rejected:
            break;
        }
    }

    // ------------------------------------------------------------------------
    // Milestone-B scaffold — per-subscription offset tracking.
    // See SubscriptionOffset above.
    // ------------------------------------------------------------------------

    /// Record the last acknowledged offset for a subscription key.
    /// TODO(Milestone B): wire into resume-hint encoding.
    void noteOffset(int64_t key, int64_t offset) noexcept {
        offsets_[key] = SubscriptionOffset{key, offset};
    }

    /// Return the latest tracked offset for a key, or -1 if not known.
    /// TODO(Milestone B): encode in ConnectRequest on resume.
    [[nodiscard]] int64_t resumeHint(int64_t key) const noexcept {
        auto it = offsets_.find(key);
        return it != offsets_.end() ? it->second.lastOffset : -1LL;
    }

private:
    // ------------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------------

    void transitionTo(SessionState next) noexcept {
        if (state_ == SessionState::Closed) return;  // terminal
        state_ = next;
    }

    /// Generate a monotonically increasing correlationId.
    /// Simple counter — no atomics needed (single-writer).
    int64_t nextCorrelationId() noexcept {
        return ++correlationIdCounter_;
    }

    /// Encode + send a ConnectRequest using the current config.
    /// If resetSessionId, sessionId_ is set to 0 (fresh connect).
    /// Otherwise the existing sessionId_ is used as a resume hint scaffold.
    /// If freshEpisode, reset BOTH retry counters and redirectCount_.
    /// FIX A: redirectCount_ is NOT reset on timeout-retry paths.
    /// FIX C: connectRetryCount_ and resumeRetryCount_ are reset only on a
    ///         fresh external connect() (freshEpisode=true).
    void doConnect(int64_t nowNs, bool resetSessionId, bool freshEpisode) noexcept {
        if (resetSessionId) {
            sessionId_ = 0;
        }
        // Fix round 3 (N-I1): a connect that cannot resume on the same broker (no
        // session id to resume) negotiates crypto AFRESH — never a plaintext request
        // carrying, or a Connected session keeping, the OLD session's keys (a
        // Backoff attempt's SessionExpired -> fresh connect, a shard failover move,
        // a connect-time redirect of a fresh connect).  No-op when !offerCrypto.
        if (sessionId_ == 0) beginCryptoHandshake_();
        if (freshEpisode) {
            // FIX A + FIX C: reset all counters only on a fresh external connect().
            redirectCount_     = 0;
            connectRetryCount_ = 0;
            resumeRetryCount_  = 0;
        }
        correlationId_       = nextCorrelationId();
        connectStartNs_      = nowNs;
        lastConnectSentNs_   = nowNs;   // DR-BUG-2: track last send time for fast-retry

        // DR-BUG-2: record whether the initial send succeeded so tickConnecting
        // can fast-retry when the ingress pub was not-connected at offer time.
        sendConnectRequest_();
        transitionTo(SessionState::Connecting);
    }

    /// Initiate a resume attempt from Resuming state.
    /// FIX C: resumeRetryCount_ is reset on onConnectResponse{OK}, not here.
    void doResume(int64_t nowNs) noexcept {
        if (resumeRetryCount_ >= cfg_.maxResumeRetries) {
            onRetryBudgetExhausted_(nowNs);
            return;
        }
        resumeRetryCount_++;
        // Resend ConnectRequest with the last sessionId (resume hint scaffold).
        correlationId_       = nextCorrelationId();
        connectStartNs_      = nowNs;
        lastConnectSentNs_   = nowNs;   // DR-BUG-2: track send time for fast-retry

        // Milestone-B scaffold: resume never carries crypto fields (tls_ is always
        // empty here — it's reset once Connected, and Resuming is only reachable
        // from Connected) — sendConnectRequest_() naturally takes the plain path.
        sendConnectRequest_();
        transitionTo(SessionState::Connecting);
    }

    /// Encode + send the current ConnectRequest.  Branches on whether a crypto
    /// handshake is in flight (tls_ engaged): with crypto, offers
    /// cipherList={bestLocalCipher()} + the pending ClientHello bytes (hsBytes_,
    /// drained by beginCryptoHandshake_()) + authToken, via the GROWABLE
    /// hsFrameBuf_ (kScratchCap=512 is not guaranteed to fit a TLS flight);
    /// without crypto, the original fixed-scratch encode (byte-identical to
    /// pre-Task-5 behaviour).  Sets connectSentOk_ either way.
    void sendConnectRequest_() noexcept {
        // Fix round 3 (N-I1): ANY connect request with no session to resume (a resume
        // hint abandoned by a move to another broker, a timeout retry after the
        // handshake state was dropped) carries a fresh ClientHello — never plaintext
        // under offerCrypto.
        if (cfg_.offerCrypto && sessionId_ == 0 && !tls_) beginCryptoHandshake_();
        if (tls_) {
            const uint8_t cipherByte = static_cast<uint8_t>(rune::crypto::bestLocalCipher());
            const std::string_view hs(reinterpret_cast<const char*>(hsBytes_.data()), hsBytes_.size());
            const std::size_t n = rune::client::codec::encodeConnectRequestCrypto(
                hsFrameBuf_,
                cfg_.egressChannel, cfg_.egressStreamId,
                // Wire gate: crypto fields are dropped on decode if clientVersion <
                // kClientVersionCrypto — force it when offering crypto regardless
                // of what cfg_.clientVersion was configured to.
                (cfg_.clientVersion > rune::protocol::client::kClientVersionCrypto
                     ? cfg_.clientVersion
                     : rune::protocol::client::kClientVersionCrypto),
                sessionId_, correlationId_,
                std::span<const uint8_t>(&cipherByte, 1),
                hs, cfg_.authToken);
            connectSentOk_ = n > 0 && io_.sendFrame(hsFrameBuf_.data(), n);
            return;
        }
        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,
            correlationId_);
        connectSentOk_ = n > 0 && io_.sendFrame(scratch_.data(), n);
    }

    /// client<->broker TLS/AEAD (Task 5): construct a fresh client-role TlsSession
    /// for a new connect episode and pump its ClientHello into hsBytes_.  Cold
    /// path (once per connect episode) — heap growth here (hsBytes_/hsFrameBuf_)
    /// is explicitly out of the zero-alloc hot-path rule, which covers only the
    /// steady-state produce/subscribe path, not the once-per-connection handshake.
    /// If local TLS construction fails (e.g. a misconfigured trust bundle), tls_
    /// stays empty and sendConnectRequest_() silently falls back to a plaintext
    /// offer — degrade, don't hard-fail a connect attempt over local TLS init.
    void beginCryptoHandshake_() noexcept {
        if (!cfg_.offerCrypto) return;  // toggle off — byte-identical pre-Task-5 connect
        cryptoState_ = CryptoState{};   // N-I1: no key of an earlier session survives a fresh episode
        tls_.emplace(rune::crypto::TlsSession::makeClient(
            cfg_.tlsTrustPem ? cfg_.tlsTrustPem->c_str() : nullptr,
            cfg_.tlsExpectHost.c_str()));
        if (!tls_->valid()) {
            tls_.reset();
            hsBytes_.clear();
            return;
        }
        cryptoState_ = CryptoState{};
        chosenCipher_ = rune::crypto::Cipher::None;
        tls_->readInbound(nullptr, 0);  // kick off ClientHello generation
        drainHandshakeOutbound_();
    }

    /// Pull every pending outbound handshake byte out of tls_ into hsBytes_
    /// (growable — see beginCryptoHandshake_ comment).  Clears hsBytes_ first.
    void drainHandshakeOutbound_() noexcept {
        hsBytes_.clear();
        uint8_t chunk[4096];
        for (;;) {
            const std::size_t n = tls_->writeOutbound(chunk, sizeof(chunk));
            if (n == 0) break;
            hsBytes_.insert(hsBytes_.end(), chunk, chunk + n);
        }
    }

    /// Send hsBytes_ (already drained) as a HandshakeContinue frame, via the
    /// same growable hsFrameBuf_ used by sendConnectRequest_().
    void sendHandshakeContinue_() noexcept {
        const std::string_view hs(reinterpret_cast<const char*>(hsBytes_.data()), hsBytes_.size());
        const std::size_t n = rune::client::codec::encodeHandshakeContinue(
            hsFrameBuf_, hs, sessionId_, correlationId_);
        if (n > 0) {
            io_.sendFrame(hsFrameBuf_.data(), n);
        }
        hsBytes_.clear();
    }

    /// tls_->done(): export keys, build CryptoState.send/recv AeadCiphers, and
    /// complete the connect episode.  A None cipher from exportKeys (TlsSession's
    /// documented fail-closed signal) is a handshake failure, not a connect.
    void finishHandshake_(int64_t nowNs) noexcept {
        const rune::crypto::SessionKeys keys = tls_->exportKeys(chosenCipher_, sessionId_);
        if (keys.cipher == rune::crypto::Cipher::None) {
            onHandshakeFailed_(nowNs);
            return;
        }
        cryptoState_.cipher = keys.cipher;
        cryptoState_.send.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
        cryptoState_.recv.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
        // Task 20: retain the raw b2c key + prefix so per-sender (shard worker)
        // keys can be re-derived locally later, lazily, on first sight of each
        // senderId — see CryptoState::workerRecv.
        cryptoState_.b2cKey = keys.b2c;
        std::memcpy(cryptoState_.b2cPrefix, keys.b2cPrefix, sizeof(cryptoState_.b2cPrefix));
        cryptoState_.encrypted = true;
        tls_.reset();  // handshake object no longer needed once keys are exported
        finishConnected_(nowNs);
    }

    /// Shared happy-path bookkeeping for "this connect episode is complete,
    /// transition to Connected" — reached either directly (no crypto chosen) or
    /// after a successful TLS handshake (finishHandshake_).
    void finishConnected_(int64_t nowNs) noexcept {
        lastKeepaliveSentNs_ = nowNs;
        lastActivityNs_      = nowNs;
        redirectCount_       = 0;
        // FIX C: reset both retry counters on successful connect/resume.
        connectRetryCount_   = 0;
        resumeRetryCount_    = 0;
        everLive_            = true;   // T15a R1 latch
        backoff_             = {};
        lastConnectError_    = 0;      // I2: refused attempts are history now
        transitionTo(SessionState::Connected);
    }

    /// A TLS handshake failure (verify failure, protocol error, local export
    /// failure, or a broker-sent HandshakeContinueResponse{HandshakeFailed}).
    /// Non-retriable — Rejected, mirroring onConnectError_'s ProtocolVersion path.
    void onHandshakeFailed_(int64_t nowNs) noexcept {
        (void)nowNs;
        tls_.reset();
        hsBytes_.clear();
        rejectCode_ = static_cast<int16_t>(
            static_cast<uint16_t>(rune::protocol::client::ErrorCode::HandshakeFailed));
        lastConnectError_ = rejectCode_;   // I2
        transitionTo(SessionState::Rejected);
    }

    void sendDisconnect() noexcept {
        const std::size_t n = rune::client::codec::encodeDisconnect(
            scratch_.data(), kScratchCap,
            static_cast<int16_t>(-1),  // reason: normal close
            sessionId_,
            correlationId_);
        if (n > 0) {
            io_.sendFrame(scratch_.data(), n);
        }
    }

    void tickConnecting(int64_t nowNs) noexcept {
        // Guard: only fast-retry if the full timeout has NOT elapsed yet.
        if (nowNs - connectStartNs_ <= cfg_.connectTimeoutNs) {
            // DR-BUG-2 FIX: fast-retry the ConnectRequest when the ingress
            // publication was not-connected at offer time (connectSentOk_==false).
            // The ingress UDP pub to the broker needs time to establish under
            // concurrent-client load; retrying at connectRetryIntervalNs (200ms)
            // ensures the broker receives the ConnectRequest once the UDP link
            // is up, without waiting the full connectTimeoutNs (5s default).
            //
            // We do NOT generate a new correlationId on fast-retry: the broker
            // may have already created a PendingConnect for an earlier send that
            // got through partially.  Keeping the same correlationId means:
            //   - the broker's pending entry (if it exists) sends ConnectResponse
            //     with the matching correlationId → client accepts it;
            //   - if the broker never received it, it creates a new PendingConnect
            //     with the same correlationId and sends ConnectResponse.
            // Either way, the client's onConnectResponse check passes.
            //
            // If the send already succeeded (connectSentOk_==true), the broker has
            // the ConnectRequest.  Its DR-BUG-2 fix retries sending ConnectResponse
            // until the egress link warms up — no client action needed beyond waiting.
            if (!connectSentOk_ && (nowNs - lastConnectSentNs_ >= cfg_.connectRetryIntervalNs)) {
                lastConnectSentNs_ = nowNs;
                sendConnectRequest_();
            }
            return;
        }

        // Connect timed out.
        ++connectFailures_;  // DR-7: an event the owner reacts to (metadata refresh)
        if (connectRetryCount_ >= cfg_.maxConnectRetries) {
            onRetryBudgetExhausted_(nowNs);
            return;
        }
        connectRetryCount_++;
        // FIX A: do NOT reset redirectCount_ here — the redirect cap must span
        //        the whole connect episode (reset only on fresh connect() or OK response).
        correlationId_       = nextCorrelationId();
        connectStartNs_      = nowNs;
        lastConnectSentNs_   = nowNs;

        sendConnectRequest_();
    }

    /// client<->broker TLS/AEAD (Task 5): bound how long a stuck handshake (broker
    /// never answers a HandshakeContinue) can hold the session.  Reuses
    /// connectStartNs_/connectTimeoutNs — the handshake is the tail of the same
    /// connect episode the timer was seeded for.  No retry semantics defined for
    /// a half-completed TLS exchange, so a timeout here is treated the same as a
    /// verify failure: non-retriable.
    void tickHandshaking_(int64_t nowNs) noexcept {
        if (nowNs - connectStartNs_ > cfg_.connectTimeoutNs) {
            onHandshakeTimedOut_(nowNs);   // fix round 3 (M-2): transient, not a verdict
        }
    }

    /// Fix round 3 (M-2, ruling): the broker never finished the handshake in time —
    /// transient (a slow / recovering broker), never a verdict on its certificate.
    /// A connect failure: retried with backoff (Backoff once Live with
    /// reconnectForeverAfterLive; Disconnected otherwise — the seed failover moves to
    /// its next candidate).  A broker-sent HandshakeFailed / a verify failure stays
    /// terminal Rejected (onHandshakeFailed_).
    void onHandshakeTimedOut_(int64_t nowNs) noexcept {
        tls_.reset();
        hsBytes_.clear();
        cryptoState_      = CryptoState{};
        sessionId_        = 0;                 // the half-made broker session is abandoned: the retry is fresh
        lastConnectError_ = kHandshakeTimedOut;
        ++connectFailures_;
        onRetryBudgetExhausted_(nowNs);        // Backoff (forever & Live) / Disconnected
    }

    // ------------------------------------------------------------------------
    // T15a R1 — never give up once Live (reconnectForeverAfterLive).
    // ------------------------------------------------------------------------

    /// Non-retriable broker answers: retrying cannot change them.
    /// client<->broker TLS/AEAD (Task 5): HandshakeFailed (cert verify failure,
    /// bad token, decrypt failure, ...) joins ProtocolVersion here — retrying the
    /// same handshake against the same broker identity cannot change the outcome.
    [[nodiscard]] static bool isNonRetriable_(rune::protocol::client::ErrorCode ec) noexcept {
        return ec == rune::protocol::client::ErrorCode::ProtocolVersion ||
               ec == rune::protocol::client::ErrorCode::HandshakeFailed;
    }

    [[nodiscard]] bool reconnectsForever_() const noexcept {
        return cfg_.reconnectForeverAfterLive && everLive_;
    }

    /// Every retry-budget exhaustion goes through here: terminal Disconnected
    /// (flag off, or never Live — today's behaviour), else Backoff.
    void onRetryBudgetExhausted_(int64_t nowNs) noexcept {
        if (reconnectsForever_()) {
            enterBackoff_(nowNs);
        } else {
            transitionTo(SessionState::Disconnected);
        }
    }

    /// A ConnectResponse / ErrorResponse error while Connecting that is neither
    /// OK nor a redirect.
    void onConnectError_(rune::protocol::client::ErrorCode ec, int64_t nowNs) noexcept {
        namespace rpc = rune::protocol::client;
        lastConnectError_ = static_cast<int16_t>(static_cast<uint16_t>(ec));   // I2
        ++connectFailures_;
        if (!cfg_.reconnectForeverAfterLive) {
            transitionTo(SessionState::Disconnected);  // today's behaviour, unchanged
            return;
        }
        if (isNonRetriable_(ec)) {
            rejectCode_ = static_cast<int16_t>(static_cast<uint16_t>(ec));
            transitionTo(SessionState::Rejected);
            return;
        }
        if (!everLive_) {
            transitionTo(SessionState::Disconnected);  // first-connect budget unchanged
            return;
        }
        if ((ec == rpc::ErrorCode::SessionExpired || ec == rpc::ErrorCode::UnknownSession) &&
            sessionId_ != 0) {
            // The broker no longer knows this session: the resume cannot succeed,
            // so connect FRESH now (bounded: the sid is 0 on the next answer).
            doConnect(nowNs, /*resetSessionId=*/true, /*freshEpisode=*/false);
            return;
        }
        enterBackoff_(nowNs);
    }

    /// Enter Backoff{attempt+1, now + min(initial * 2^attempt, max)}.  Overflow-
    /// free: doubles step by step and stops at the cap.
    void enterBackoff_(int64_t nowNs) noexcept {
        const int64_t cap   = cfg_.backoffMaxNs > 0 ? cfg_.backoffMaxNs : 1;
        int64_t       delay = cfg_.backoffInitialNs > 0 ? cfg_.backoffInitialNs : 1;
        for (int i = 0; i < backoff_.attempt && delay < cap; ++i) {
            delay = delay > cap / 2 ? cap : delay * 2;
        }
        if (delay > cap) delay = cap;
        ++backoff_.attempt;
        backoff_.nextAttemptNs = nowNs + delay;
        transitionTo(SessionState::Backoff);
    }

    /// Backoff: nothing before nextAttemptNs; then one resume-first attempt (the
    /// last sessionId is the hint; a SessionExpired answer drops it); even
    /// attempts go through the bootstrap endpoint (retargetToBootstrap), odd
    /// attempts >= 3 back through the last-known leader (retargetToLastKnownLeader).  The
    /// redirect budget is renewed so the attempt can follow a leader change; the
    /// connect/resume budgets stay spent, so a failed attempt backs off again.
    void tickBackoff_(int64_t nowNs) noexcept {
        if (nowNs < backoff_.nextAttemptNs) return;
        // fix1 I1 + fix2 A-B1 (Kafka-style bootstrap fallback, real alternation):
        //   attempt 1     the current (possibly redirected) endpoint — a fast
        //                 resume when only the link blipped;
        //   even attempts the configured bootstrap — the leader may be dead for
        //                 good, and only the bootstrap can redirect us;
        //   odd >= 3      back to the last-known leader — the BOOTSTRAP may be
        //                 the dead one.
        // Either live endpoint is reached within two attempts, whatever the
        // outage length.  Driven by the Backoff attempt count on the injected
        // clock — no timers of its own.
        if ((backoff_.attempt % 2) == 0) {
            (void)io_.retargetToBootstrap();
        } else if (backoff_.attempt >= 3) {
            (void)io_.retargetToLastKnownLeader();
        }
        redirectCount_ = 0;
        doConnect(nowNs, /*resetSessionId=*/false, /*freshEpisode=*/false);
    }

    void tickConnected(int64_t nowNs) noexcept {
        // Check for stale session first.
        if (nowNs - lastActivityNs_ > cfg_.responseTimeoutNs) {
            ++staleSessionLosses_;
            transitionTo(SessionState::Resuming);
            // FIX D: enter Resuming as a real state; doResume fires on next tick.
            resumeEntryNs_ = nowNs;
            return;
        }

        // Keepalive cadence.
        if (nowNs - lastKeepaliveSentNs_ >= cfg_.keepaliveIntervalNs) {
            lastKeepaliveSentNs_ = nowNs;

            // A10: the keepalive is a Ping — the broker touches the session AND
            // answers with a Pong, so even an idle session proves liveness inside
            // responseTimeoutNs.  Its correlationId (<= 0, per session) is never an
            // application ping's id (those start at 1) and routes the Pong back.
            const std::size_t n = rune::client::codec::encodePing(
                scratch_.data(), kScratchCap, std::string_view{},
                sessionId_,
                cfg_.keepaliveCorrelationId);
            if (n > 0) {
                io_.sendFrame(scratch_.data(), n);
            }
        }
    }

    /// FIX D: tickResuming is now live code.
    /// Resuming is a real observable waiting state — the machine enters it
    /// (so callers can observe state()==Resuming) and doResume() fires on the
    /// first tick after entry.  This also enables disconnect() to be handled
    /// while in Resuming.
    void tickResuming(int64_t nowNs) noexcept {
        // resumeEntryNs_ is set when entering Resuming.  Fire doResume()
        // on the very first tick (nowNs >= resumeEntryNs_).
        (void)nowNs;
        doResume(nowNs);
    }

    // ------------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------------
    SessionIo&    io_;
    SessionConfig cfg_;

    SessionState  state_            = SessionState::Disconnected;
    int64_t       sessionId_        = 0;
    int64_t       correlationId_    = 0;
    int64_t       correlationIdCounter_ = 0;

    // Timers (injected clock, nanoseconds).
    int64_t connectStartNs_      = 0;
    int64_t lastKeepaliveSentNs_ = 0;
    int64_t lastActivityNs_      = 0;
    int64_t lastTickNs_          = 0;   ///< A10: own-stall detection (gap between ticks)
    int64_t resumeEntryNs_       = 0;  // FIX D: timestamp when Resuming was entered
    // DR-BUG-2: fast-retry state for the connect path.
    uint64_t connectFailures_ = 0;   // DR-7: see connectFailures()
    uint64_t staleSessionLosses_ = 0;   // fix round 4: see staleSessionLosses()
    uint64_t ownStallLosses_     = 0;   // M1: see ownStallLosses()
    int16_t  lastConnectError_   = 0;   // I2: see lastConnectError()
    int64_t lastConnectSentNs_   = 0;  // when the last ConnectRequest was offered
    bool    connectSentOk_       = false; // true if the last offer() succeeded

    // T15a R1 — Backoff state's own variables + the Live latch + Rejected's code.
    struct BackoffState {
        int     attempt       = 0;  // Backoff entries since the last Live
        int64_t nextAttemptNs = 0;  // injected-clock deadline of the next attempt
    };
    BackoffState backoff_{};
    bool         everLive_   = false;
    int16_t      rejectCode_ = 0;

    // Counters.
    int redirectCount_     = 0;
    int connectRetryCount_ = 0;
    int resumeRetryCount_  = 0;

    // Encode scratch buffer — reused each tick, zero per-tick heap allocation.
    std::array<uint8_t, kScratchCap> scratch_{};

    // Milestone-B scaffold: per-subscription offset map.
    // TODO(Milestone B): integrate into ConnectRequest resume-hint encoding.
    std::unordered_map<int64_t, SubscriptionOffset> offsets_;

    // ------------------------------------------------------------------------
    // client<->broker TLS/AEAD (Task 5) — cold-path handshake state.
    // Engaged only across one connect episode's Connecting->Handshaking->
    // Connected run; empty (tls_ == nullopt) in every other state, including
    // the whole steady-state Connected/Resuming/Backoff path.
    // ------------------------------------------------------------------------
    std::optional<rune::crypto::TlsSession> tls_;
    rune::crypto::Cipher chosenCipher_ = rune::crypto::Cipher::None;  // from ConnectResponse.chosenCipher
    // Growable (cold path only — see beginCryptoHandshake_ comment): pending
    // outbound handshake bytes pulled from tls_, and the frame-encode scratch
    // for any message that carries them (kScratchCap=512 is not sized for a
    // TLS flight).
    std::vector<uint8_t> hsBytes_;
    std::vector<uint8_t> hsFrameBuf_;
    CryptoState cryptoState_;
};

} // namespace rune::client::session
