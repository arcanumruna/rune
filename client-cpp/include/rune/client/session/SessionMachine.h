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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <rune/client/codec/Codec.h>
#include <rune/client/detail/SecureZero.h>
#include <rune/client/session/ScramClient.h>
#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

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
    // Authentication (spec §5, D11): entered from Connecting / Handshaking when the ConnectResponse says
    // authMode Required and authState MustAuthenticate.  One SCRAM-SHA-512 exchange (AuthClientFirst,
    // AuthClientFinal) runs inside the session — sealed once TLS is up; the state holds exactly that exchange
    // (SessionMachine::scram_).  Connected only after the broker proved the password (AuthServerFinal{OK} with
    // a valid server signature); no data frame leaves the session before.  A refusal is terminal Rejected
    // (AuthenticationFailed) unless the broker rate-limited it (D27).
    Authenticating,
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

    /// Issue #115: this session's CURRENT ingress channel ("" = unknown).  A connect with no configured
    /// tlsExpectHost verifies the broker certificate against this channel's host (tlsHostOfChannel).  Cold path
    /// (once per connect episode).  Default: unknown.
    [[nodiscard]] virtual std::string ingressChannel() const { return {}; }

    /// Edge notification (cold path): the machine just entered terminal Rejected (once per session; Rejected is
    /// never left).  Default: ignored.  Lets the owner keep an O(1) rejected-session count instead of rescanning.
    virtual void onRejected() noexcept {}

    virtual ~SessionIo() = default;
};

/// Issue #115: the host of an Aeron UDP channel ("aeron:udp?endpoint=host:port|..." or a bare "host:port"), without
/// the port and IPv6 brackets; "" when the channel names no host (aeron:ipc, no endpoint).  The TLS name a session
/// verifies when ClientConfig::tlsExpectHost is empty.
[[nodiscard]] inline std::string tlsHostOfChannel(std::string_view ch) {
    if (ch.rfind("aeron:", 0) == 0) {
        const auto ep = ch.find("endpoint=");
        if (ep == std::string_view::npos) return {};
        ch = ch.substr(ep + 9);
        if (const auto end = ch.find_first_of("|&"); end != std::string_view::npos) ch = ch.substr(0, end);
    }
    if (!ch.empty() && ch.front() == '[') {   // [v6]:port
        const auto close = ch.find(']');
        return close == std::string_view::npos ? std::string{} : std::string(ch.substr(1, close - 1));
    }
    const auto colon = ch.rfind(':');
    if (colon != std::string_view::npos && ch.find(':') == colon) ch = ch.substr(0, colon);   // host:port (not a bare v6)
    return std::string(ch);
}

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
    /// The default responseTimeoutNs, readable without constructing a SessionConfig (its std::string member
    /// allocates - a hot caller must not build one to read a constant).
    static constexpr int64_t kDefaultResponseTimeoutNs = 10'000'000'000LL;
    int64_t responseTimeoutNs   = kDefaultResponseTimeoutNs;  // 10 s — stale session threshold
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
    int maxUnavailableRetries = 8;  // Issues #461/#519: transient refusals (48, rate-limited 44) retried with Backoff before Live (10 ms doubling to 1 s: ~2.3 s)
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
    // Issue #481: a node session the agent opens and revives on demand (not one the app connected).  Its unproven
    // final-leg exhaustion keeps the paced Backoff: Disconnected would be revived at once, every duty cycle.
    bool    revivedOnDemand = false;
    int64_t backoffInitialNs = 10'000'000LL;     // 10 ms first Backoff delay, doubling...
    int64_t backoffMaxNs     = 1'000'000'000LL;  // ...capped at 1 s

    // client<->broker TLS/AEAD (Task 5) — cold-path handshake configuration.
    // Toggle (design spec §11, RUNE_TLS): false = today's plaintext connect, byte-
    // identical to pre-Task-5 (every existing SessionMachineTest keeps passing,
    // including ones that pin an explicit pre-crypto clientVersion like 1).  true
    // = connect() offers a v4 ConnectRequest carrying cipherList/handshakeBytes/
    // (an empty, reserved authToken) and drives the TLS handshake described below.
    bool offerCrypto = false;
    // trust: nullopt = trust the system store (production default); "" = trust
    // nothing (tests, deliberately untrusted); non-empty = a PEM CA bundle.
    std::optional<std::string> tlsTrustPem;
    // Hostname checked against the broker cert's SAN (SNI + hostname verification).
    // "" installs NO hostname check (TlsSession::makeClient contract) — set this to
    // the broker's DNS name in production; left empty only for loopback/tests.
    std::string tlsExpectHost;
    // Authentication (spec §5): the SCRAM credentials.  username "" = no credentials — the broker answers
    // AuthenticationFailed (the client never short-circuits: the broker enforces).  The password is never
    // logged and never sent (only a proof derived from it).  The wire's ConnectRequest.authToken field stays
    // reserved and is always sent empty (fix-on-sight: it was never validated).
    std::string username;
    std::string password;
    // Review B I3: with credentials a session is Connected only after a real login (server signature verified) or
    // the resume of a session THIS client authenticated.  A broker that says "no login needed" (dev mode, pre-auth,
    // Required+Anonymous before bootstrap) is refused Rejected(AuthenticationFailed) unless this opt-in is set.
    // Default false.  A client WITHOUT credentials is unaffected.
    bool allowUnauthenticatedBroker = false;
    // Issue #121: this session exists only to send `rune security bootstrap` (no credentials; the request proves
    // itself with the cluster-key MAC).  A broker whose bootstrap window was REOPENED (its last admin disabled or
    // dropped) answers MustAuthenticate, since its other users still log in: such a session then skips the login and
    // is Connected in the anonymous-bootstrap mode, exactly as on a never-bootstrapped cluster (Required+Anonymous).
    // Ignored with credentials.  Default false: a credential-less client otherwise starts the (failing) login.
    bool bootstrapOnly = false;
    // Issue #235: the most PBKDF2 iterations this client runs for a login (the broker names the count).  A count
    // above it is refused before any hashing: a rogue broker cannot stall the duty thread.  Clamped to 4096..1e6.
    uint32_t maxAcceptedIterations = 100'000;

    // Task 9 (downgrade safety, spec §11 require_tls's client-side mirror):
    // when true, a ConnectResponse{OK} that negotiates NO cipher (chosenCipher
    // == 0 — a plaintext broker, or a MITM that stripped the crypto offer off
    // the wire before the broker saw it) must NOT silently proceed to a
    // plaintext Connected session.  Issue #14: every answer before the TLS keys
    // exist is unauthenticated, so such a refusal (and any pre-TLS ConnectResponse
    // / HandshakeContinueResponse error or verify failure) is a TRANSIENT connect
    // failure (onPreTlsRefusal_: retried, then Backoff / Disconnected), never a
    // terminal Rejected a forged packet could cause.  Meaningless (and left false)
    // unless offerCrypto is also true.  Default false; Client sets it for a
    // credentialed client (ClientConfig::insecurePlaintextAuth).
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

    // Issue #116 (per-receiver client->broker keys): true when this session's client (ConnectRequest.clientVersion)
    // and broker (ConnectResponse.brokerVersion) both speak receiver keys (rpc::sessionUsesReceiverKeys).  Then each
    // c2b frame is sealed for ONE receiver: receiver 0 (the gateway) uses `send`/`sendSeq` above, unchanged; receiver
    // k >= 1 (shard worker k-1) uses workerSend[k-1] - its own cipher under deriveReceiverKey(c2bKey, k) and its own
    // seq space, so a frame sealed for one receiver never opens at another (and a receiver's window never lags behind
    // a seq another receiver consumed).  false (legacy) = every frame on `send`/`sendSeq` with receiver id 0.
    bool                  receiverKeys = false;
    // Retained raw c2b key + nonce prefix: a receiver slot's key is derived from them lazily, on first use (cold, once
    // per receiver per session).  Populated alongside `send` in finishHandshake_.
    rune::crypto::AeadKey c2bKey{};
    uint8_t               c2bPrefix[rune::crypto::kNoncePrefix] = {};
    struct ReceiverSend {
        std::optional<rune::crypto::AeadCipher> cipher;
        uint64_t sendSeq = 0;
        bool     deriveFailed = false;   // #673: this receiver's key failed to derive: held until the session reconnects
    };
    // One slot per worker receiver (id k >= 1, indexed k-1); same bound as workerRecv (the broker's max workers).
    std::array<ReceiverSend, kMaxShardWorkers> workerSend{};
    uint32_t receiverDeriveFailures = 0;   // #673: receiver keys that failed to derive this episode (each counted once)

    /// #535 / #675: derive receiver `receiverId`'s c2b key (selectSendSlot's cold path).  The derivation function is
    /// a test seam (CryptoStateTestAccess, tests only); production always uses rune::crypto::deriveReceiverKey.
    [[nodiscard]] bool deriveReceiverKeyFor(uint8_t receiverId, rune::crypto::AeadKey& out) const noexcept {
        return deriveFn_(c2bKey, receiverId, out);
    }

private:
    friend struct CryptoStateTestAccess;
    bool (*deriveFn_)(const rune::crypto::AeadKey&, uint8_t, rune::crypto::AeadKey&) noexcept =
        &rune::crypto::deriveReceiverKey;
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
    {
        // A password over kCapacity leaves the secret empty (the login then fails AuthenticationFailed); Client and
        // ClientAgent refuse it earlier with std::invalid_argument.
        (void)ownSecret_.assign(cfg_.password);
        detail::wipeString(cfg_.password);
    }

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
    /// #426: true when `sid` names the session whose keys this machine discarded while it was encrypted.
    [[nodiscard]] bool namesRetiredSealedSession(int64_t sid) const noexcept { return sid != 0 && sid == retiredSealedSid_; }
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
    /// Issues #15 / #111: AuthUnavailable (48) answers / session ends seen (each a retriable failure, never Rejected).
    [[nodiscard]] uint64_t authUnavailableCount() const noexcept { return authUnavailable_; }
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
    /// Authentication: only a Connected session may carry data frames (Authenticating / Handshaking /
    /// Connecting never do).
    [[nodiscard]] bool canSendData() const noexcept { return state_ == SessionState::Connected; }
    /// Authentication: the broker's authMode from the latest accepted ConnectResponse (InProcess before any).
    [[nodiscard]] rune::protocol::client::AuthMode brokerAuthMode() const noexcept { return authInfo_.brokerMode; }
    /// Authentication: AuthServerFinal carried kAuthFlagMustChangePassword (bootstrap one-time password) —
    /// the session is Connected but the broker only accepts a password change on it.
    [[nodiscard]] bool passwordChangeRequired() const noexcept { return authInfo_.passwordChangeRequired; }
    /// Review B I3: this Connected session was earned — a completed login, or the resume of a session this machine
    /// authenticated.  False for a session on a broker that skipped the login (allowUnauthenticatedBroker) and for
    /// a client without credentials.
    [[nodiscard]] bool authenticated() const noexcept { return authInfo_.authenticated; }
    /// Review B I5: the broker answered Required + Anonymous (a cluster with no security admin yet): this session may
    /// carry ONLY the bootstrap — the broker answers any other frame AuthenticationFailed and closes it, so the
    /// machine sends no keepalive and the Client no connect-time metadata request on it.  Nothing keeps such a
    /// session alive: the broker's login deadline ends it, and an accepted bootstrap restarts that deadline so its
    /// answer arrives first (#240).
    [[nodiscard]] bool anonymousBootstrap() const noexcept { return authInfo_.anonymousBootstrap; }
    /// #514: Connected, able to be pinged (not a bootstrap-only session), and no inbound frame for more than `ns`.
    /// A live node answers every keepalive Ping, so silence past two keepalive intervals means it is suspect -
    /// long before the response timeout declares the session stale.
    [[nodiscard]] bool silentLongerThan(int64_t nowNs, int64_t ns) const noexcept {
        return state_ == SessionState::Connected && !authInfo_.anonymousBootstrap && nowNs - lastActivityNs_ > ns;
    }
    [[nodiscard]] int64_t keepaliveIntervalNs() const noexcept { return cfg_.keepaliveIntervalNs; }

    /// Issue #114: sign, in place, a SecurityDdl frame that changes this session's OWN credential (ALTER USER <self>
    /// ADD / DROP / SET PASSWORD carrying the CredentialProof flag): its last kCredentialProofBytes become the SCRAM
    /// proof of the password this machine logged in with, over credentialChangeMessage(the login's combined nonce,
    /// the request) - so it is valid on THIS broker session only.  Called on every offer of the frame, after the
    /// session id is stamped (a redirect or a reconnect re-signs it for the session it now rides).  Any other frame,
    /// or a session this machine did not log in on, is left untouched (the broker then refuses the change).  Duty
    /// thread; cold path (PBKDF2 only if the cache lost the entry).
    void signOwnCredentialChange(uint8_t* buf, std::size_t len) noexcept {
        namespace rpc = rune::protocol::client;
        try {
            if (!loginCredential_ || loginCredential_->sessionId != sessionId_ || authenticatedSessionId_ != sessionId_) return;
            const auto m = rpc::SecurityDdl::decode(buf, len);
            if (!m || (m->flags & rpc::kSecurityDdlFlagCredentialProof) == 0 || !rpc::isOwnCredentialChange(*m, cfg_.username))
                return;
            const LoginCredential& c = *loginCredential_;
            const auto salt = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(c.salt.data()), c.salt.size());
            const auto* sp = cache_->saltedPassword(secret_->view(), salt, c.iterations);
            if (sp == nullptr) return;
            const auto proof = rune::crypto::scram::clientProof(rune::crypto::Digest::Sha512, *sp,
                                                                rpc::credentialChangeMessage(c.loginNonce, *m));
            if (!proof || proof->size != rpc::kCredentialProofBytes) return;
            std::memcpy(buf + len - rpc::kCredentialProofBytes, proof->bytes.data(), rpc::kCredentialProofBytes);
        } catch (...) {
            // allocation failure: the frame goes unsigned and the broker refuses the change (explicitly)
        }
    }
    /// Why the machine refused the broker (Rejected after an OK ConnectResponse that skipped the login); "" if not.
    [[nodiscard]] const std::string& rejectDetail() const noexcept { return rejectDetail_; }
    /// Review B I4: the session user changed their own password - the next login uses the new one, and a one-time
    /// (must-change) session is now a normal one.  Duty thread.  For a standalone machine (its own secret); the agent
    /// updates its shared secret and calls passwordChanged().  false (nothing changed): shared secret, or over the cap.
    [[nodiscard]] bool installPassword(std::string_view newPassword) noexcept {
        if (secret_ != &ownSecret_ || !ownSecret_.assign(newPassword)) return false;
        passwordChanged();
        return true;
    }
    /// The credential changed (the agent already updated the shared secret): the one-time flag is cleared and the
    /// login proof is dropped.
    void passwordChanged() noexcept {
        authInfo_.passwordChangeRequired = false;
        // Issue #114: the login's password may be gone (SET replaces every verifier): this session can no longer prove a
        // further own-credential change (the broker answers "log in again") - never sign with a stale salt, which
        // would also cost a PBKDF2 on the duty thread.
        loginCredential_.reset();
    }
    /// Authentication: the retryAfterMs of the AuthServerFinal{44} that last refused this session (0 = none).
    [[nodiscard]] uint32_t authRetryAfterMs() const noexcept { return authInfo_.retryAfterMs; }
    [[nodiscard]] int      unavailableRetries() const noexcept { return unavailableRetries_; }
    /// Issue #461: in Backoff because a transient refusal (48 / rate-limited 44) on a connect is being retried within
    /// maxUnavailableRetries - not because the machine gave up.
    [[nodiscard]] bool     retryingTransientRefusal() const noexcept {
        return state_ == SessionState::Backoff && unavailableRetries_ > 0 &&
               (lastConnectError_ == static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthUnavailable) ||
                lastConnectError_ == static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthenticationFailed));
    }
    /// C4 review (#461): the caller's connect deadline passed while a transient refusal (48 / rate-limited 44) on a
    /// connect was being retried.  The retry ends here (-> Disconnected) so nothing reconnects behind a connect that
    /// already failed; lastConnectError() and authRetryAfterMs() keep the refusal for the report.  Returns whether it
    /// abandoned anything.  A reconnect-forever session (Live before) is never stopped here.
    bool abandonTransientRetry() noexcept {
        if (reconnectsForever_() || unavailableRetries_ == 0) return false;
        if (lastConnectError_ != static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthUnavailable) &&
            lastConnectError_ != static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthenticationFailed)) {
            return false;
        }
        switch (state_) {
            case SessionState::Backoff:
            case SessionState::Connecting:
            case SessionState::Handshaking:
            case SessionState::Authenticating:
                hs_.reset();
                transitionTo(SessionState::Disconnected);
                return true;
            default:
                return false;
        }
    }
    /// Authentication: the salted-password cache is the duty thread's (the agent's), shared by its sessions;
    /// a machine built alone (tests) uses its own.  Duty thread, before the first connect.
    void useScramCache(ScramCache* cache) noexcept { cache_ = cache != nullptr ? cache : &ownCache_; }
    /// The agent's shared secret (one copy for every session); nullptr = this machine's own.  Duty thread, before
    /// the first connect.  Wipes the machine's own copy.
    void useSharedSecret(const detail::FixedSecret* s) noexcept {
        secret_ = s != nullptr ? s : &ownSecret_;
        if (s != nullptr) ownSecret_.wipe();
    }
    /// client<->broker TLS/AEAD (Task 5): the session's crypto state.  `encrypted`
    /// is false until the handshake completes (or if no cipher was negotiated).
    [[nodiscard]] const CryptoState& cryptoState() const noexcept { return cryptoState_; }
    /// Mutable overload — AeadCipher::seal/open are non-const (they advance the
    /// reused EVP_CIPHER_CTX per call), so the Task 6 data-path seal/open seams
    /// (and this task's own round-trip test) need write access to send/recv.
    [[nodiscard]] CryptoState& cryptoState() noexcept { return cryptoState_; }
    /// Fix round 4: this session offers crypto — its negotiated mode is only known
    /// once Connected, so no non-handshake frame may leave it before then.
    [[nodiscard]] bool offersCrypto() const noexcept { return cfg_.offerCrypto; }
    /// PR #476 review: a frame sealed under this session's keys opened - the broker has proven the keys, so an
    /// unauthenticated handshake failure naming this session is a forgery from now on (onHandshakeContinueResponse).
    void onSealedFrameOpened() noexcept {
        if (brokerKeysProven_) return;
        brokerKeysProven_ = true;
        unprovenFinalLegRetries_ = 0;
        backoff_ = {};   // the connect really succeeded: renew the Backoff schedule (finishConnected_ deferred it)
    }
    [[nodiscard]] bool brokerKeysProven() const noexcept { return brokerKeysProven_; }
    /// Fix round 5 (N4-I1): a HandshakeContinueResponse{HandshakeFailed} naming this Connected
    /// session's current id and connect corrId — its final handshake leg failed broker-side.
    [[nodiscard]] bool isFinalLegHandshakeFailure(int16_t code, int64_t corrId, int64_t noticeSessionId) const noexcept {
        // Review B I2: with authentication a TLS 1.3 client that has sent its Finished is Authenticating (not yet
        // Connected) when the broker's final-leg failure arrives — the same episode, the same verdict.
        return (state_ == SessionState::Connected || state_ == SessionState::Authenticating) &&
               static_cast<rune::protocol::client::ErrorCode>(static_cast<uint16_t>(code)) ==
                   rune::protocol::client::ErrorCode::HandshakeFailed &&
               corrId == correlationId_ && noticeSessionId != 0 && noticeSessionId == sessionId_;
    }

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
            state_ == SessionState::Authenticating ||
            state_ == SessionState::Closed || state_ == SessionState::Rejected) return;
        discardCrypto_();   // before the id clears: it records the sealed episode's id (#426)
        sessionId_   = 0;
        // Fix round 5 (N4-M1): Connecting keeps its episode — the next NEW request replaces
        // it (a different requestCorrelationId); dropping it here would make tickConnecting's
        // requireCrypto-without-TLS rule turn a transient move into a terminal Rejected.
        if (state_ != SessionState::Connecting) hs_.reset();
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
        scram_.reset();   // Authentication: an abandoned exchange
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
        if (state_ == SessionState::Handshaking || state_ == SessionState::Authenticating) {
            // Task 5: sessionId_ was already assigned (the broker's first
            // ConnectResponse leg), so a polite goodbye is possible even though
            // the TLS handshake never finished; abandon the in-flight TlsSession.
            if (sessionId_ != 0) {
                sendDisconnect();
            }
            hs_.reset();
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
                           std::string_view handshakeBytes = {},
                           // Authentication (v9): defaults = a broker without authentication, so every
                           // pre-v9 call site keeps its behaviour (Connected at once).
                           rune::protocol::client::AuthMode  authMode  = rune::protocol::client::AuthMode::InProcess,
                           rune::protocol::client::AuthState authState = rune::protocol::client::AuthState::Anonymous,
                           // Issue #116: ConnectResponse.brokerVersion (0 = a broker that predates the field) - with
                           // this client's own version it decides whether the session uses receiver keys.
                           int32_t brokerVersion = 0) noexcept {
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
            // TLS handshake instead of completing the connect.  hs_ must be the
            // live TlsSession this episode's ClientHello came from (beginCryptoHandshake_
            // in connect()/forceReconnect()); if it's missing (e.g. local TLS init
            // failed) a cipher-bearing answer can't be driven -> treat as a failure.
            if (chosenCipher != 0) {
                if (!hs_) {
                    onHandshakeFailed_(nowNs);
                    return;
                }
                hs_->chosenCipher = static_cast<rune::crypto::Cipher>(chosenCipher);
                hs_->authMode     = authMode;     // the auth step follows the handshake's done()
                hs_->authState    = authState;
                hs_->brokerVersion = brokerVersion;   // issue #116: receiver keys are decided at done()
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

            // Happy path — no crypto chosen (plaintext session, or a pre-v4 broker): authenticate when the
            // broker requires it, else Connected.
            hs_.reset();
            beginAuthOrFinish_(nowNs, authMode, authState);
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
        notePreTlsRedirect_(newEndpoint);   // issue #115: unauthenticated - the TLS name does not follow it
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
                                      int64_t          nowNs,
                                      int64_t          noticeSessionId = 0) noexcept {
        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));
        // Fix round 5 (N4-I1, ruling): the broker failed the handshake on its FINAL leg (the
        // client's last flight did not verify, or its key export failed) — after this machine
        // already went Connected (a TLS 1.3 client is done once it has SENT its Finished; on
        // success the broker sends nothing more).  The same verdict as on any leg: terminal.
        // Only for THIS session's last handshake: its current id AND connect corrId.
        if (isFinalLegHandshakeFailure(code, incomingCorrelationId, noticeSessionId)) {
            // PR #476 review (issues #14 / #469, supersedes N4-I1): the frame is in clear and names only guessable ids.
            // Once the broker has PROVEN the session's keys (a sealed frame opened) it is a forgery: ignored.  Before
            // that it is a transient connect failure, retried within the connect budget - never terminal, so one
            // packet can never end a session for good.
            if (brokerKeysProven_) return;
            // The session reached Connected / Authenticating, which renewed the connect budget: these retries have
            // their own count, renewed only once a sealed frame proves the keys - a broker that fails every final leg
            // is retried maxConnectRetries times, then Backoff / Disconnected, never a reconnect storm.
            hs_.reset();
            discardCrypto_();
            lastConnectError_ = code;
            ++connectFailures_;
            if (unprovenFinalLegRetries_ < cfg_.maxConnectRetries) {
                ++unprovenFinalLegRetries_;
                doConnect(nowNs, /*resetSessionId=*/true, /*freshEpisode=*/false);
                return;
            }
            // Issue #481: exhausted like any connect budget - Disconnected for a session the app connected, Backoff
            // once Live with reconnectForeverAfterLive.  A node session revived on demand keeps Backoff, whose delay
            // keeps growing until a sealed frame proves the keys (finishConnected_ does not renew it for an unproven
            // TLS session): a broker failing every final leg is retried at most once per Backoff delay, never a storm.
            sessionId_ = 0;
            if (cfg_.revivedOnDemand) {
                enterBackoff_(nowNs);
            } else {
                onRetryBudgetExhausted_(nowNs);
            }
            return;
        }
        if (state_ != SessionState::Handshaking) return;
        if (incomingCorrelationId != correlationId_) return;
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
            hs_.reset();
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
        if (state_ != SessionState::Handshaking || !hs_) return;

        const auto hs = hs_->tls.readInbound(buf, len);
        if (hs == rune::crypto::HsState::Failed) {
            onHandshakeFailed_(nowNs);
            return;
        }

        drainHandshakeOutbound_();
        if (!hs_->outbound.empty()) {
            sendHandshakeContinue_();
        }

        if (hs_->tls.done()) {
            finishHandshake_(nowNs);
        }
        // Else: waiting for more input from the broker (another
        // HandshakeContinueResponse) — nothing further to do this call.
    }

    // ------------------------------------------------------------------------
    // Authentication (spec §5, D5) — the SCRAM exchange of the Authenticating state.
    // ------------------------------------------------------------------------

    /// AuthServerFirst arrived: answer one RFC 5802 proof per active verifier (1 or 2) over its own AuthMessage,
    /// salting the password through the agent's ScramCache (PBKDF2 only on a cold (salt, iterations)).  Ignored
    /// outside Authenticating / ScramSentFirst.  A combined nonce that does not extend ours is a broker fault:
    /// Rejected(AuthenticationFailed).
    void onAuthServerFirst(const rune::protocol::client::AuthServerFirst& sf, int64_t nowNs) noexcept {
        namespace rpc = rune::protocol::client;
        namespace rs  = rune::crypto::scram;
        if (state_ != SessionState::Authenticating || !scram_) return;
        auto* first = std::get_if<ScramSentFirst>(&*scram_);
        if (first == nullptr) return;   // a duplicate AuthServerFirst: the proof already went out
        const std::string_view cn = first->clientNonce;
        if (sf.combinedNonce.size() <= cn.size() || sf.combinedNonce.substr(0, cn.size()) != cn ||
            sf.combinedNonce.find(',') != std::string_view::npos) {
            failAuth_(rpc::ErrorCode::AuthenticationFailed, 0, nowNs);   // "broker nonce does not extend ours"
            return;
        }
        if (sf.count < 1 || sf.count > rpc::kAuthMaxVerifiers) {
            failAuth_(rpc::ErrorCode::AuthenticationFailed, 0, nowNs);
            return;
        }
        const uint32_t cap = std::clamp(cfg_.maxAcceptedIterations, rpc::kAuthIterationsMin, rpc::kAuthIterationsMax);
        for (uint8_t i = 0; i < sf.count; ++i) {
            if (sf.salts[i].iterations > cap) {
                failAuth_(rpc::ErrorCode::AuthenticationFailed, 0, nowNs);
                try {
                    rejectDetail_ = "the broker asked for " + std::to_string(sf.salts[i].iterations) +
                                    " SCRAM iterations, above maxAcceptedScramIterations " + std::to_string(cap);
                } catch (...) {}
                return;
            }
        }
        const std::string combined(sf.combinedNonce);
        const std::string cfwp = rs::clientFinalWithoutProof(combined);
        ScramSentFinal fin;
        fin.count = sf.count;
        fin.combinedNonce = combined;
        rpc::AuthClientFinal out;
        out.sessionId     = sessionId_;
        out.correlationId = correlationId_;
        out.count         = sf.count;
        for (uint8_t i = 0; i < sf.count; ++i) {
            const auto salt = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sf.salts[i].salt.data()),
                                                       sf.salts[i].salt.size());
            const auto* sp = cache_->saltedPassword(secret_->view(), salt, sf.salts[i].iterations);
            if (sp == nullptr) { failAuthLocal_(); return; }
            fin.authMessages[i] = rs::authMessage(first->clientFirstBare,
                                                  rs::serverFirst(combined, salt, sf.salts[i].iterations), cfwp);
            const auto proof = rs::clientProof(rune::crypto::Digest::Sha512, *sp, fin.authMessages[i]);
            const auto sig   = rs::expectedServerSignature(rune::crypto::Digest::Sha512, *sp, fin.authMessages[i]);
            if (!proof || !sig || proof->size != rpc::kScramProofBytes) { failAuthLocal_(); return; }
            std::memcpy(out.proofs[i].data(), proof->bytes.data(), rpc::kScramProofBytes);
            fin.expected[i] = *sig;
            fin.salts[i].assign(sf.salts[i].salt);
            fin.iterations[i] = sf.salts[i].iterations;
        }
        hsFrameBuf_.resize(out.encodedSize());
        const std::size_t n = out.encode(hsFrameBuf_.data());
        fin.sent = n > 0 && io_.sendFrame(hsFrameBuf_.data(), n);   // refused: tickAuthenticating_ re-offers it
        scram_.emplace(std::move(fin));   // ScramSentFirst -> ScramSentFinal: the same state, its next variables
    }

    /// AuthServerFinal arrived — the verdict.  OK needs a server signature equal to one of the expected ones
    /// (constant time): then Connected (finishConnected_); a missing / wrong signature means the broker could
    /// not prove the password: Rejected(AuthenticationFailed).  An error code: 44 is terminal (Rejected) unless
    /// rate-limited — with retryAfterMs > 0 and reconnectForeverAfterLive the session backs off at least that
    /// long (D27).  The kAuthFlagMustChangePassword flag marks a one-time-password session.
    void onAuthServerFinal(const rune::protocol::client::AuthServerFinal& f, int64_t nowNs) noexcept {
        namespace rpc = rune::protocol::client;
        if (state_ != SessionState::Authenticating || !scram_) return;
        if (f.errorCode != 0) {
            const auto ec = static_cast<rpc::ErrorCode>(f.errorCode);
            if (ec == rpc::ErrorCode::AuthenticationFailed) failAuth_(ec, f.retryAfterMs, nowNs);
            else if (ec == rpc::ErrorCode::AuthUnavailable) onAuthUnavailable_(nowNs);   // issues #15 / #111
            else onConnectError_(ec, nowNs);   // anything else (Internal ...): a connect failure
            return;
        }
        const auto* fin = std::get_if<ScramSentFinal>(&*scram_);
        if (fin == nullptr || f.serverSignature.size() != rpc::kAuthSignatureBytes) {
            failAuth_(rpc::ErrorCode::AuthenticationFailed, 0, nowNs);   // no proof owed yet / no signature
            return;
        }
        const auto got = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(f.serverSignature.data()),
                                                  f.serverSignature.size());
        bool match = false;
        int matched = -1;
        for (uint8_t i = 0; i < fin->count; ++i) {   // no early exit: constant time over the verifiers
            const bool m = rune::crypto::constantTimeEqual(fin->expected[i].view(), got);
            match |= m;
            if (m && matched < 0) matched = i;
        }
        if (!match) {
            failAuth_(rpc::ErrorCode::AuthenticationFailed, 0, nowNs);   // "broker failed to prove the password"
            return;
        }
        authInfo_.passwordChangeRequired = (f.flags & rpc::kAuthFlagMustChangePassword) != 0;
        authInfo_.retryAfterMs           = 0;
        authInfo_.authenticated          = true;
        authenticatedSessionId_          = sessionId_;   // a later resume of THIS id is this machine's own login
        loginCredential_ = LoginCredential{sessionId_, fin->combinedNonce, fin->salts[static_cast<std::size_t>(matched)],
                                           fin->iterations[static_cast<std::size_t>(matched)]};
        finishConnected_(nowNs);
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

        // A session-end notice (corr 0, SessionRevoked 46 / NamespaceSuspended 51) naming THIS session is terminal in every state
        // that holds the session id and speaks over an established session: Authenticating, Resuming, Backoff (Connected below).
        // Rejected, never resumed, never retried - also with reconnectForeverAfterLive.  Connecting / Handshaking hold no
        // established session (the ConnectResponse / the TLS keys are still owed; a plaintext notice proves nothing): ignored.
        if ((ec == rpc::ErrorCode::SessionRevoked || ec == rpc::ErrorCode::NamespaceSuspended) && incomingCorrelationId == 0 &&
            (state_ == SessionState::Authenticating || state_ == SessionState::Resuming || state_ == SessionState::Backoff)) {
            if (sessionId_ != 0 && noticeSessionId == sessionId_) rejectSessionEnd_(ec);
            return;
        }

        if (state_ == SessionState::Connected) {
            // F-A C1 (defence in depth): a session-fatal notice naming ANOTHER session (one this machine already
            // left) never moves this one.  0 = the notice names no session (kept: the old behaviour).
            if (noticeSessionId != 0 && sessionId_ != 0 && noticeSessionId != sessionId_) return;
            if (ec == rpc::ErrorCode::SessionExpired || ec == rpc::ErrorCode::UnknownSession) {
                transitionTo(SessionState::Resuming);
                // doResume fires on the next tick (FIX D: Resuming is a real waiting state).
                resumeEntryNs_ = nowNs;
            } else if (ec == rpc::ErrorCode::AuthUnavailable && incomingCorrelationId == 0) {
                // Issues #15 / #111: the broker's security state stayed stale and it ended this session - retriable:
                // reconnect FRESH elsewhere (the seed failover rotates the candidates), never resume here.
                onAuthUnavailable_(nowNs);
            } else if ((ec == rpc::ErrorCode::SessionRevoked || ec == rpc::ErrorCode::NamespaceSuspended) &&
                       incomingCorrelationId == 0) {
                // Authentication (D26): the session-level ErrorResponse{SessionRevoked, corr 0} — the user was
                // dropped / disabled — or {NamespaceSuspended 51, corr 0}: a RejectAll suspend of the user's namespace
                // (client version 14; an older client gets 46).  The broker closes the session:
                // terminal, never resumed or reconnected (a reconnect is AuthenticationFailed anyway).
                rejectSessionEnd_(ec);
            }
            return;
        }

        if (state_ == SessionState::Authenticating) {
            // Authentication: the broker ended the exchange with an ErrorResponse (its auth deadline, D13)
            // instead of an AuthServerFinal.  Only the answer to THIS episode counts.
            // Review B M2: a SESSION-level refusal (the auth deadline, D13) carries correlation id 0 and names the session.
            const bool sessionLevel = incomingCorrelationId == 0 && ec == rpc::ErrorCode::AuthenticationFailed &&
                                      (noticeSessionId == 0 || noticeSessionId == sessionId_);
            if (incomingCorrelationId != correlationId_ && !sessionLevel) return;
            if (ec == rpc::ErrorCode::AuthenticationFailed) {
                failAuth_(ec, /*retryAfterMs=*/0, nowNs);
            } else if (ec == rpc::ErrorCode::SessionExpired || ec == rpc::ErrorCode::UnknownSession) {
                onConnectError_(ec, nowNs);
            } else if (ec == rpc::ErrorCode::AuthUnavailable) {
                // C4 review (#461): the broker could not check security for this login - transient, retried (paced,
                // bounded) like a 48 on the ConnectResponse, never waited out to the handshake timeout.
                scram_.reset();
                onAuthUnavailable_(nowNs);
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
                    notePreTlsRedirect_(message);   // issue #115: unauthenticated - the TLS name does not follow it
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
        case SessionState::Authenticating:
            tickAuthenticating_(nowNs);
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
        if (next != SessionState::Authenticating) scram_.reset();   // the exchange's variables live in that state only
        const bool intoRejected = next == SessionState::Rejected && state_ != SessionState::Rejected;
        state_ = next;
        if (intoRejected) io_.onRejected();   // edge: once per session (Rejected is terminal)
    }

    /// Generate a monotonically increasing correlationId.
    /// Simple counter — no atomics needed (single-writer).
    int64_t nextCorrelationId() noexcept {
        return ++correlationIdCounter_;
    }

    /// #426: the id of the encrypted session whose keys are about to be dropped is remembered, so a frame the broker
    /// still seals under that discarded key (its lease has not expired) is never mistaken for a clear frame.
    void retireSealedSessionId_() noexcept {
        if (cryptoState_.encrypted && sessionId_ != 0) retiredSealedSid_ = sessionId_;
    }
    /// Every crypto reset goes through here; it runs BEFORE any `sessionId_ = 0` on the same path.
    void discardCrypto_() noexcept {
        retireSealedSessionId_();
        cryptoState_ = CryptoState{};
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
            retireSealedSessionId_();   // #426: the crypto reset below comes after the id clears
            sessionId_ = 0;
            authenticatedSessionId_ = 0;   // a fresh episode: no earlier login can be resumed
        }
        // Fix round 3 (N-I1) / round 4 (N3-I1): the request below negotiates crypto
        // afresh under offerCrypto (sendConnectRequest_: one TLS episode per request).
        if (freshEpisode) {
            // FIX A + FIX C: reset all counters only on a fresh external connect().
            redirectCount_     = 0;
            connectRetryCount_ = 0;
            resumeRetryCount_  = 0;
            unavailableRetries_ = 0;
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

        // Fix round 4 (N3-I1): under offerCrypto the resume request begins a fresh TLS
        // episode in sendConnectRequest_ (the broker allocates a new session anyway).
        sendConnectRequest_();
        transitionTo(SessionState::Connecting);
    }

    /// Encode + send the current ConnectRequest.  Branches on whether a crypto
    /// handshake is in flight (hs_ engaged): with crypto, offers
    /// cipherList={bestLocalCipher()} + the pending ClientHello bytes (hs_->outbound,
    /// drained by beginCryptoHandshake_()) + an empty authToken, via the GROWABLE
    /// hsFrameBuf_ (kScratchCap=512 is not guaranteed to fit a TLS flight);
    /// without crypto, the original fixed-scratch encode (byte-identical to
    /// pre-Task-5 behaviour).  Sets connectSentOk_ either way.
    void sendConnectRequest_() noexcept {
        // Fix round 4 (N3-I1): the broker never resumes — handleConnect allocates a
        // NEW session on every connect, a resume too — so EVERY ConnectRequest under
        // offerCrypto negotiates crypto afresh: never plaintext, never the old keys
        // (the resume hint stays in the request; the broker ignores it).  A retry
        // A fast retry of the SAME request (same correlationId: the broker may hold
        // its pending connect) keeps its episode; every new request — a timeout
        // retry, a redirect resend, a resume — begins its own.
        if (cfg_.offerCrypto && (!hs_ || hs_->requestCorrelationId != correlationId_)) {
            beginCryptoHandshake_();
            if (hs_) hs_->requestCorrelationId = correlationId_;
        }
        if (cfg_.offerCrypto && cfg_.requireCrypto && !hs_) [[unlikely]] {
            // Local TLS init failed while crypto is REQUIRED: never a plaintext
            // request — tickConnecting turns this into terminal Rejected.
            connectSentOk_ = false;
            return;
        }
        if (hs_) {
            const uint8_t cipherByte = static_cast<uint8_t>(rune::crypto::bestLocalCipher());
            const std::string_view hs(reinterpret_cast<const char*>(hs_->outbound.data()), hs_->outbound.size());
            const std::size_t n = rune::client::codec::encodeConnectRequestCrypto(
                hsFrameBuf_,
                cfg_.egressChannel, cfg_.egressStreamId,
                // Wire gate: crypto fields are dropped on decode if clientVersion <
                // kClientVersionCrypto — force it when offering crypto regardless
                // of what cfg_.clientVersion was configured to.
                announcedClientVersion_(),
                sessionId_, correlationId_,
                std::span<const uint8_t>(&cipherByte, 1),
                hs, std::string_view{});   // ConnectRequest.authToken: reserved, always empty
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

    /// The ConnectRequest.clientVersion a crypto offer announces: crypto fields are dropped on decode below
    /// kClientVersionCrypto, so an offer always announces at least that (issue #116: the broker gates receiver keys
    /// on this same value).
    [[nodiscard]] int32_t announcedClientVersion_() const noexcept {
        return cfg_.clientVersion > rune::protocol::client::kClientVersionCrypto
                   ? cfg_.clientVersion : rune::protocol::client::kClientVersionCrypto;
    }

    /// client<->broker TLS/AEAD (Task 5): construct a fresh client-role TlsSession
    /// for a new connect episode and pump its ClientHello into hs_->outbound.  Cold
    /// path (once per connect episode) — heap growth here (hs_->outbound/hsFrameBuf_)
    /// is explicitly out of the zero-alloc hot-path rule, which covers only the
    /// steady-state produce/subscribe path, not the once-per-connection handshake.
    /// If local TLS construction fails (e.g. a misconfigured trust bundle), hs_
    /// stays empty and sendConnectRequest_() silently falls back to a plaintext
    /// offer — degrade, don't hard-fail a connect attempt over local TLS init.
    void beginCryptoHandshake_() noexcept {
        if (!cfg_.offerCrypto) return;  // toggle off — byte-identical pre-Task-5 connect
        discardCrypto_();   // N-I1: no key of an earlier session survives a fresh episode
        brokerKeysProven_ = false;      // PR #476 review: proven again only by a sealed frame of THIS episode
        hs_.reset();
        std::string host;
        try {
            host = tlsHostForEpisode_();
        } catch (...) {
            return;   // allocation failure (cold): no TLS this episode - requireCrypto fails it, never plaintext creds
        }
        auto t = rune::crypto::TlsSession::makeClient(cfg_.tlsTrustPem ? cfg_.tlsTrustPem->c_str() : nullptr,
                                                      host.c_str());
        if (!t.valid()) return;
        hs_.emplace(std::move(t));
        hs_->tls.readInbound(nullptr, 0);  // kick off ClientHello generation
        drainHandshakeOutbound_();
    }

    /// Issue #115: the certificate name this connect episode verifies.  The configured tlsExpectHost when set; else
    /// the host of the session's current endpoint - except an endpoint reached by a PRE-TLS redirect (a ConnectResponse
    /// / ErrorResponse redirect in Connecting is unauthenticated: any UDP sender can forge one).  Such an endpoint is
    /// bound for the session's whole life to the name of the endpoint it was redirected from, whatever path brings the
    /// session back to it (the Backoff walk's last-known leader, a resume, a later redirect): a forged redirect to a
    /// host the attacker holds a certificate for never verifies.  More than kMaxRedirectPins bound endpoints (or an
    /// allocation failure) fails closed: every unbound endpoint then verifies the session's anchor (the name of the
    /// first endpoint it ever checked).  Cold path.
    [[nodiscard]] std::string tlsHostForEpisode_() {
        if (!cfg_.tlsExpectHost.empty()) return cfg_.tlsExpectHost;
        const std::string ch = io_.ingressChannel();
        if (const RedirectPin* pin = findRedirectPin_(ch)) return pin->host;
        if (!anchorSet_) { anchorHost_ = tlsHostOfChannel(ch); anchorSet_ = true; }
        if (redirectPinsOverflowed_) return anchorHost_;
        return tlsHostOfChannel(ch);
    }

    struct RedirectPin { std::string channel, host; };
    static constexpr std::size_t kMaxRedirectPins = 16;
    /// The pin binding `channel`, refreshed to most-recently-used; null when none.
    RedirectPin* findRedirectPin_(const std::string& channel) {
        for (std::size_t k = 0; k < redirectPins_.size(); ++k) {
            if (redirectPins_[k].channel != channel) continue;
            if (k + 1 != redirectPins_.size()) {
                RedirectPin p = std::move(redirectPins_[k]);
                redirectPins_.erase(redirectPins_.begin() + static_cast<std::ptrdiff_t>(k));
                redirectPins_.push_back(std::move(p));
            }
            return &redirectPins_.back();
        }
        return nullptr;
    }

    /// Issue #115: a pre-TLS redirect is about to retarget this session - bind the target endpoint to the name the
    /// session verifies now (the name its current endpoint is itself bound to, else that endpoint's own host).  A
    /// target already bound keeps its first binding.
    void notePreTlsRedirect_(std::string_view newEndpoint) noexcept {
        if (!cfg_.offerCrypto || !cfg_.tlsExpectHost.empty()) return;
        try {
            const std::string host = tlsHostForEpisode_();   // the current endpoint's binding (sets the anchor)
            std::string target = newEndpoint.rfind("aeron:", 0) == 0 ? std::string(newEndpoint)
                                                                     : "aeron:udp?endpoint=" + std::string(newEndpoint);
            if (findRedirectPin_(target) != nullptr) return;
            if (redirectPins_.size() == kMaxRedirectPins) {
                redirectPins_.erase(redirectPins_.begin());   // least recently used ...
                redirectPinsOverflowed_ = true;               // ... and every unbound endpoint verifies the anchor
            }
            redirectPins_.push_back(RedirectPin{std::move(target), host});
        } catch (...) {
            redirectPinsOverflowed_ = true;   // fail closed: unbound endpoints verify the anchor
        }
    }

    /// Pull every pending outbound handshake byte out of hs_->tls into hs_->outbound
    /// (growable — see beginCryptoHandshake_ comment).  Clears hs_->outbound first.
    void drainHandshakeOutbound_() noexcept {
        hs_->outbound.clear();
        uint8_t chunk[4096];
        for (;;) {
            const std::size_t n = hs_->tls.writeOutbound(chunk, sizeof(chunk));
            if (n == 0) break;
            hs_->outbound.insert(hs_->outbound.end(), chunk, chunk + n);
        }
    }

    /// Send hs_->outbound (already drained) as a HandshakeContinue frame, via the
    /// same growable hsFrameBuf_ used by sendConnectRequest_().
    void sendHandshakeContinue_() noexcept {
        const std::string_view hs(reinterpret_cast<const char*>(hs_->outbound.data()), hs_->outbound.size());
        const std::size_t n = rune::client::codec::encodeHandshakeContinue(
            hsFrameBuf_, hs, sessionId_, correlationId_);
        if (n > 0) {
            io_.sendFrame(hsFrameBuf_.data(), n);
        }
        hs_->outbound.clear();
    }

    /// hs_->tls.done(): export keys, build CryptoState.send/recv AeadCiphers, and
    /// complete the connect episode.  A None cipher from exportKeys (TlsSession's
    /// documented fail-closed signal) is a handshake failure, not a connect.
    void finishHandshake_(int64_t nowNs) noexcept {
        const rune::crypto::SessionKeys keys = hs_->tls.exportKeys(hs_->chosenCipher, sessionId_);
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
        // Issue #116: per-receiver c2b keys when both sides speak them (the version this episode's ConnectRequest
        // announced - see sendConnectRequest_ - and the broker's ConnectResponse.brokerVersion).  The raw c2b key is
        // retained to derive each worker receiver's key on first use.
        cryptoState_.receiverKeys = rune::protocol::client::sessionUsesReceiverKeys(announcedClientVersion_(),
                                                                                   hs_->brokerVersion);
        cryptoState_.c2bKey = keys.c2b;
        std::memcpy(cryptoState_.c2bPrefix, keys.c2bPrefix, sizeof(cryptoState_.c2bPrefix));
        cryptoState_.encrypted = true;
        const auto authMode  = hs_->authMode;
        const auto authState = hs_->authState;
        hs_.reset();  // handshake object no longer needed once keys are exported
        beginAuthOrFinish_(nowNs, authMode, authState);   // SCRAM runs sealed (D11), then Connected
    }

    /// Authentication (D11): the connect episode's last step.  The broker requires a login of this session
    /// (authMode Required + authState MustAuthenticate) -> Authenticating, AuthClientFirst sent; anything else
    /// (dev mode, an in-process broker, a resumed authenticated session, a pre-v9 broker) -> Connected.
    void beginAuthOrFinish_(int64_t nowNs, rune::protocol::client::AuthMode mode,
                            rune::protocol::client::AuthState state) noexcept {
        namespace rpc = rune::protocol::client;
        authInfo_.brokerMode = mode;
        authInfo_.authenticated = false;
        authInfo_.anonymousBootstrap = mode == rpc::AuthMode::Required && state == rpc::AuthState::Anonymous;
        const bool credentialed = !cfg_.username.empty();
        if (mode == rpc::AuthMode::Required && state == rpc::AuthState::MustAuthenticate) {
            if (!credentialed && cfg_.bootstrapOnly) {   // issue #121: a reopened bootstrap window
                authInfo_.anonymousBootstrap = true;
                finishConnected_(nowNs);
                return;
            }
            beginAuth_(nowNs);
            return;
        }
        // Review B I3: a resume is believed only for a session this machine itself authenticated.
        if (credentialed && mode == rpc::AuthMode::Required && state == rpc::AuthState::ResumedAuthenticated &&
            authenticatedSessionId_ != 0 && sessionId_ == authenticatedSessionId_) {
            authInfo_.authenticated = true;
            finishConnected_(nowNs);
            return;
        }
        if (!credentialed || cfg_.allowUnauthenticatedBroker) {
            finishConnected_(nowNs);   // nothing to prove (no credentials), or the operator accepted it
            return;
        }
        // Credentials but no login: a broker that skips the SCRAM exchange could be anything.  Refused, explicitly.
        if (mode == rpc::AuthMode::Required && state == rpc::AuthState::Anonymous) {
            rejectDetail_ = "the cluster has no security admin yet: it accepts only `rune security bootstrap` and "
                            "authenticated nothing for this client";
        } else if (mode == rpc::AuthMode::Required && state == rpc::AuthState::ResumedAuthenticated) {
            rejectDetail_ = "the broker claims an authenticated session this client never established";
        } else {
            rejectDetail_ = "the broker did not authenticate this session (dev mode / a pre-authentication broker): "
                            "set allowUnauthenticatedBroker to accept it";
        }
        lastConnectError_ = static_cast<int16_t>(static_cast<uint16_t>(rpc::ErrorCode::AuthenticationFailed));
        rejectCode_       = lastConnectError_;
        ++connectFailures_;
        discardCrypto_();
        transitionTo(SessionState::Rejected);
    }

    void beginAuth_(int64_t nowNs) noexcept {
        namespace rpc = rune::protocol::client;
        std::string nonce = rune::crypto::scram::makeNonce();
        if (nonce.empty()) { failAuthLocal_(); return; }   // no randomness: fail closed
        ScramSentFirst first;
        first.clientFirstBare = rune::crypto::scram::clientFirstBare(cfg_.username, nonce);
        first.clientNonce     = std::move(nonce);
        connectStartNs_       = nowNs;                      // the login has its own connectTimeoutNs
        authInfo_.passwordChangeRequired = false;
        rpc::AuthClientFirst m;
        m.sessionId     = sessionId_;
        m.correlationId = correlationId_;
        m.username      = cfg_.username;       // "" = no credentials: the broker answers 44, we never short-circuit
        m.clientNonce   = first.clientNonce;
        scram_.emplace(std::move(first));
        transitionTo(SessionState::Authenticating);
        hsFrameBuf_.resize(m.encodedSize());
        const std::size_t n = m.encode(hsFrameBuf_.data());
        if (n == 0) { failAuthLocal_(); return; }   // a field that does not fit the wire: never a garbled login
        if (io_.sendFrame(hsFrameBuf_.data(), n)) std::get<ScramSentFirst>(*scram_).sent = true;
        // else: back-pressured — the frame stays in hsFrameBuf_ and tickAuthenticating_ re-offers it
    }

    /// The broker refused the login (or proved nothing): terminal Rejected(code) — except a rate-limited 44
    /// (retryAfterMs > 0): before Live a bounded Backoff (Issue #519), of a reconnect-forever session that was Live
    /// Backoff without bound; both wait at least that long (D27).
    void failAuth_(rune::protocol::client::ErrorCode ec, uint32_t retryAfterMs, int64_t nowNs) noexcept {
        lastConnectError_ = static_cast<int16_t>(static_cast<uint16_t>(ec));
        authInfo_.retryAfterMs = retryAfterMs;
        ++connectFailures_;
        discardCrypto_();
        if (retryAfterMs > 0 &&   // Issue #519: a rate-limited login before Live is a bounded retry, not a verdict
            retryTransientRefusal_(static_cast<int16_t>(static_cast<uint16_t>(ec)),
                                      nowNs + static_cast<int64_t>(retryAfterMs) * 1'000'000LL, nowNs)) {
            return;
        }
        if (retryAfterMs > 0 && reconnectsForever_()) {
            sessionId_ = 0;                       // the refused broker session is gone: the retry is fresh
            authenticatedSessionId_ = 0;
            enterBackoff_(nowNs);
            const int64_t notBefore = nowNs + static_cast<int64_t>(retryAfterMs) * 1'000'000LL;
            if (backoff_.nextAttemptNs < notBefore) backoff_.nextAttemptNs = notBefore;   // max(retryAfter, backoff)
            return;
        }
        rejectCode_ = static_cast<int16_t>(static_cast<uint16_t>(ec));
        transitionTo(SessionState::Rejected);
    }

    /// Issues #461 / #519 / #613: a transient refusal on a connect (the broker's security state is catching up, its
    /// unauthenticated-session cap is full, the login throttle asks for a pause): retried with Backoff, at least
    /// `notBeforeNs`, within maxUnavailableRetries (reset by a fresh connect() and by Live) - then Disconnected with the
    /// code, never Rejected.  C4 review: this holds on EVERY connect of a session that does not reconnect forever (a
    /// re-connect() after Live too); a reconnect-forever session that was Live keeps its own unbounded Backoff path
    /// (false: the caller goes on).  A session the client revives on demand (node sessions) ends its budget in
    /// Backoff, never Disconnected: a revive from Disconnected is immediate, so it would storm the refusing broker.
    bool retryTransientRefusal_(int16_t code, int64_t notBeforeNs, int64_t nowNs) noexcept {
        // A live session the broker ended (ErrorResponse 48, corr 0) is no connect in progress: the caller's path.
        if (reconnectsForever_() || state_ == SessionState::Connected) return false;
        hs_.reset();
        discardCrypto_();
        scram_.reset();
        sessionId_              = 0;     // the refused broker session is gone: the retry is fresh
        authenticatedSessionId_ = 0;
        lastConnectError_       = code;
        ++connectFailures_;
        if (unavailableRetries_ >= cfg_.maxUnavailableRetries && !cfg_.revivedOnDemand) {
            transitionTo(SessionState::Disconnected);
            return true;
        }
        if (unavailableRetries_ < cfg_.maxUnavailableRetries) ++unavailableRetries_;
        enterBackoff_(nowNs);
        if (backoff_.nextAttemptNs < notBeforeNs) backoff_.nextAttemptNs = notBeforeNs;
        return true;
    }

    /// Issues #15 / #111: AuthUnavailable (48) - THIS broker cannot vouch for its security state right now (catching
    /// up, partitioned): never a verdict on the credentials.  The broker closed the session, so the retry is a fresh
    /// connect; a retriable connect failure (Backoff for a reconnect-forever session that was Live - the seed failover
    /// moves it to another broker - Disconnected otherwise, the caller's connect retry).
    void onAuthUnavailable_(int64_t nowNs) noexcept {
        discardCrypto_();
        sessionId_              = 0;
        authenticatedSessionId_ = 0;
        ++authUnavailable_;
        onConnectError_(rune::protocol::client::ErrorCode::AuthUnavailable, nowNs);
    }

    /// A LOCAL failure (no randomness, PBKDF2 refused): not the broker's verdict, but never a plaintext login.
    void failAuthLocal_() noexcept {
        lastConnectError_ = static_cast<int16_t>(static_cast<uint16_t>(rune::protocol::client::ErrorCode::Internal));
        rejectCode_       = lastConnectError_;
        discardCrypto_();
        transitionTo(SessionState::Rejected);
    }

    void tickAuthenticating_(int64_t nowNs) noexcept {
        // A login frame the transport refused (back-pressure) is re-offered every tick until accepted — exactly
        // once on the wire (a refused offer delivered nothing).  hsFrameBuf_ still holds it: nothing else in this
        // state writes it.
        if (scram_ && !hsFrameBuf_.empty()) {
            bool* sent = std::visit([](auto& st) { return &st.sent; }, *scram_);
            if (!*sent && io_.sendFrame(hsFrameBuf_.data(), hsFrameBuf_.size())) *sent = true;
        }
        if (nowNs - connectStartNs_ > cfg_.connectTimeoutNs) {
            onHandshakeTimedOut_(nowNs);   // a broker that never answers the login: transient, like a stuck handshake
        }
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
        unavailableRetries_  = 0;
        everLive_            = true;   // T15a R1 latch
        // PR #476 review: an encrypted session's Backoff is renewed only once a sealed frame proves the keys
        // (onSealedFrameOpened) - an optimistic Connected followed by a final-leg failure keeps its growing delay.
        if (!cryptoState_.encrypted) {
            backoff_ = {};
            if (sessionId_ == retiredSealedSid_) retiredSealedSid_ = 0;   // #426: a plaintext episode resuming the SAME id: its frames are legitimately clear; a new id never reads the old sealed ack
        }
        lastConnectError_    = 0;      // I2: refused attempts are history now
        transitionTo(SessionState::Connected);
    }

    /// A TLS handshake failure (verify failure, protocol error, local export
    /// failure, or a broker-sent HandshakeContinueResponse{HandshakeFailed}).
    /// Non-retriable — Rejected, mirroring onConnectError_'s ProtocolVersion path —
    /// unless crypto is REQUIRED: then every such answer arrived before the session had
    /// keys, so nothing authenticates it (issue #14: any UDP sender can forge one) and it is
    /// a transient connect failure (onPreTlsRefusal_), never a terminal verdict.
    void onHandshakeFailed_(int64_t nowNs) noexcept {
        if (cfg_.offerCrypto && cfg_.requireCrypto) {
            onPreTlsRefusal_(static_cast<int16_t>(static_cast<uint16_t>(rune::protocol::client::ErrorCode::HandshakeFailed)),
                             nowNs);
            return;
        }
        rejectHandshake_();
    }

    /// Terminal Rejected(HandshakeFailed): a TLS failure that retrying cannot change.
    void rejectHandshake_() noexcept {
        hs_.reset();
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
        if (cfg_.offerCrypto && cfg_.requireCrypto && !hs_) [[unlikely]] {
            rejectHandshake_();   // fix round 4: required crypto, no local TLS — a local verdict, terminal
            return;
        }
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
        hs_.reset();
        discardCrypto_();
        sessionId_        = 0;                 // the half-made broker session is abandoned: the retry is fresh
        lastConnectError_ = kHandshakeTimedOut;
        ++connectFailures_;
        onRetryBudgetExhausted_(nowNs);        // Backoff (forever & Live) / Disconnected
    }

    // ------------------------------------------------------------------------
    // T15a R1 — never give up once Live (reconnectForeverAfterLive).
    // ------------------------------------------------------------------------

    /// The broker ended this session for good (46 / 51): terminal Rejected(code).
    void rejectSessionEnd_(rune::protocol::client::ErrorCode ec) noexcept {
        rejectCode_       = static_cast<int16_t>(static_cast<uint16_t>(ec));
        lastConnectError_ = rejectCode_;
        transitionTo(SessionState::Rejected);
    }

    /// Non-retriable broker answers: retrying cannot change them.
    /// client<->broker TLS/AEAD (Task 5): HandshakeFailed (cert verify failure,
    /// bad token, decrypt failure, ...) joins ProtocolVersion here — retrying the
    /// same handshake against the same broker identity cannot change the outcome.
    [[nodiscard]] static bool isNonRetriable_(rune::protocol::client::ErrorCode ec) noexcept {
        return ec == rune::protocol::client::ErrorCode::ProtocolVersion ||
               ec == rune::protocol::client::ErrorCode::HandshakeFailed ||
               ec == rune::protocol::client::ErrorCode::AuthenticationFailed ||   // credentials are cluster-wide (D27)
               ec == rune::protocol::client::ErrorCode::SessionRevoked ||         // the user is gone / disabled
               ec == rune::protocol::client::ErrorCode::NamespaceSuspended;       // RejectAll suspend of the user's namespace
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

    /// Issue #14: an answer that arrived before this session's TLS keys existed while crypto is REQUIRED - a
    /// ConnectResponse / ErrorResponse refusal, a plaintext OK (a stripped offer), a HandshakeContinueResponse
    /// failure, a failed verify of the server's flight.  Nothing authenticates it (any UDP sender can forge one), so
    /// it is a transient connect failure: retried on a fresh episode within the connect budget, then
    /// onRetryBudgetExhausted_ (Backoff once Live with reconnectForeverAfterLive, else Disconnected), never terminal
    /// Rejected.  lastConnectError() reports the code.
    void onPreTlsRefusal_(int16_t code, int64_t nowNs) noexcept {
        hs_.reset();
        discardCrypto_();
        lastConnectError_ = code;
        ++connectFailures_;
        if (state_ == SessionState::Connecting || state_ == SessionState::Handshaking) {
            if (connectRetryCount_ < cfg_.maxConnectRetries) {
                ++connectRetryCount_;
                doConnect(nowNs, /*resetSessionId=*/true, /*freshEpisode=*/false);
                return;
            }
        }
        sessionId_ = 0;   // a half-made (or forged-away) session is abandoned: the retry is fresh
        onRetryBudgetExhausted_(nowNs);
    }

    /// A ConnectResponse / ErrorResponse error while Connecting that is neither
    /// OK nor a redirect.
    void onConnectError_(rune::protocol::client::ErrorCode ec, int64_t nowNs) noexcept {
        namespace rpc = rune::protocol::client;
        if (ec == rpc::ErrorCode::AuthUnavailable &&   // Issue #461: a transient refusal on a connect is retried
            retryTransientRefusal_(static_cast<int16_t>(static_cast<uint16_t>(ec)), 0, nowNs)) return;
        if (cfg_.offerCrypto && cfg_.requireCrypto &&
            (state_ == SessionState::Connecting || state_ == SessionState::Handshaking)) {
            onPreTlsRefusal_(static_cast<int16_t>(static_cast<uint16_t>(ec)), nowNs);   // issue #14: unauthenticated
            return;
        }
        lastConnectError_ = static_cast<int16_t>(static_cast<uint16_t>(ec));   // I2
        ++connectFailures_;
        if (ec == rpc::ErrorCode::SessionRevoked || ec == rpc::ErrorCode::NamespaceSuspended) {
            rejectSessionEnd_(ec);   // the login itself was answered "this user / namespace is ended": terminal, flag or not
            return;
        }
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

        // Keepalive cadence (not on a bootstrap-only session: the broker closes it on any other frame).
        if (!authInfo_.anonymousBootstrap && nowNs - lastKeepaliveSentNs_ >= cfg_.keepaliveIntervalNs) {
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
    /// The session id of the last episode whose keys were discarded while encrypted; a frame naming it that no live
    /// key opens is old-episode ciphertext and is never read in clear (#426).
    int64_t       retiredSealedSid_ = 0;
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
    uint64_t authUnavailable_ = 0;   // issues #15 / #111: see authUnavailableCount()
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
    // Connected run; empty (hs_ == nullopt) in every other state, including
    // the whole steady-state Connected/Resuming/Backoff path.
    // ------------------------------------------------------------------------
    // Fix round 4 (TLS class): ONE handshake episode's own data — the TLS
    // transcript (the in-flight TlsSession), the cipher the broker chose, and the
    // pending outbound handshake bytes — engaged from the ConnectRequest that
    // starts the episode through Handshaking, and gone in every other state
    // (Connected keeps only the derived keys, cryptoState_).  Rule 3: the
    // Handshaking state's variables, never scattered members.
    struct HandshakeEpisode {
        rune::crypto::TlsSession tls;
        rune::crypto::Cipher     chosenCipher = rune::crypto::Cipher::None;  // from ConnectResponse.chosenCipher
        std::vector<uint8_t>     outbound;   // pending outbound handshake bytes (cold path, growable)
        int64_t                  requestCorrelationId = 0;   // the ONE ConnectRequest this episode answers
        // Authentication: the first leg's verdict on how this session logs in, applied when done() (D11).
        rune::protocol::client::AuthMode  authMode  = rune::protocol::client::AuthMode::InProcess;
        rune::protocol::client::AuthState authState = rune::protocol::client::AuthState::Anonymous;
        int32_t                           brokerVersion = 0;   // issue #116: the first leg's ConnectResponse.brokerVersion
        explicit HandshakeEpisode(rune::crypto::TlsSession t) noexcept : tls(std::move(t)) {}
    };
    std::optional<HandshakeEpisode> hs_;
    // Issue #115 (tlsHostForEpisode_): every endpoint a pre-TLS redirect moved this session to, bound to the
    // certificate name it keeps verifying there (most recently used last); the anchor name, and the fail-closed flag.
    bool                     brokerKeysProven_ = false;   // PR #476 review: see onSealedFrameOpened
    int                      unprovenFinalLegRetries_ = 0;   // final-leg failures retried since keys were last proven
    int                      unavailableRetries_ = 0;   // transient refusals retried since the last fresh connect() / Live
    std::vector<RedirectPin> redirectPins_;
    std::string              anchorHost_;
    bool                     anchorSet_ = false;
    bool                     redirectPinsOverflowed_ = false;
    // Growable frame-encode scratch for any message carrying handshake bytes
    // (kScratchCap=512 is not sized for a TLS flight); capacity persists.
    std::vector<uint8_t> hsFrameBuf_;
    CryptoState cryptoState_;

    // ------------------------------------------------------------------------
    // Authentication (Task 7) — the Authenticating state's own variable and the broker-mode observation.
    // ------------------------------------------------------------------------
    // The SCRAM exchange: engaged ONLY while Authenticating (transitionTo clears it on every other edge).
    std::optional<ScramExchange> scram_;
    // What the session learnt about authentication: the broker's mode (latched at the ConnectResponse that
    // decided it), the one-time-password flag of the latest AuthServerFinal, the latest rate-limit.
    struct AuthInfo {
        rune::protocol::client::AuthMode brokerMode = rune::protocol::client::AuthMode::InProcess;
        bool     passwordChangeRequired = false;
        uint32_t retryAfterMs = 0;
        bool     authenticated = false;   // review B I3: earned by a login (or an earned resume)
        bool     anonymousBootstrap = false;   // review B I5: Required + Anonymous — only the bootstrap may be sent
    };
    int64_t     authenticatedSessionId_ = 0;   // the broker session id this machine logged in on (0 = none)
    // Issue #114: the login that authenticated `sessionId` - what signOwnCredentialChange proves with.  Replaced by
    // every login; used only while that session is the current one.
    struct LoginCredential { int64_t sessionId = 0; std::string loginNonce; std::string salt; uint32_t iterations = 0; };
    std::optional<LoginCredential> loginCredential_;
    std::string rejectDetail_;
    AuthInfo     authInfo_{};
    ScramCache   ownCache_;
    ScramCache*  cache_ = &ownCache_;
    detail::FixedSecret        ownSecret_;
    const detail::FixedSecret* secret_ = &ownSecret_;
};

} // namespace rune::client::session
