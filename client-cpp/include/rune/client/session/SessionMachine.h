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
#include <string>
#include <string_view>
#include <unordered_map>

#include <rune/client/codec/Codec.h>

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

    virtual ~SessionIo() = default;
};

// ---------------------------------------------------------------------------
// SessionConfig — machine configuration with sensible defaults.
// ---------------------------------------------------------------------------
struct SessionConfig {
    std::string egressChannel  = "aeron:udp?endpoint=localhost:40200";
    int32_t     egressStreamId = 1001;
    int32_t     clientVersion  = 1;

    // Timeouts / intervals (nanoseconds).
    int64_t connectTimeoutNs    = 5'000'000'000LL;   // 5 s  — give up one connect attempt
    int64_t keepaliveIntervalNs = 1'000'000'000LL;   // 1 s  — send keepalive this often
    int64_t responseTimeoutNs   = 10'000'000'000LL;  // 10 s — stale session threshold

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
    [[nodiscard]] int64_t      correlationId() const noexcept { return correlationId_; }
    [[nodiscard]] int           redirectCount() const noexcept { return redirectCount_; }

    // ------------------------------------------------------------------------
    // External API — called by the application / CC-5 agent.
    // ------------------------------------------------------------------------

    /// Transition Disconnected → Connecting (or re-try from Resuming).
    /// Encodes and sends a ConnectRequest, seeds the connect timer.
    /// No-op from any other state.
    void connect(int64_t nowNs) noexcept {
        if (state_ != SessionState::Disconnected) return;
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
    /// No-op only from Closed (terminal).
    void forceReconnect(int64_t nowNs) noexcept {
        if (state_ == SessionState::Closed) return;
        // Force to Disconnected first so doConnect's Connecting transition is
        // a clean fresh episode (reset sessionId + all retry/redirect counters).
        state_ = SessionState::Disconnected;
        doConnect(nowNs, /*resetSessionId=*/true, /*freshEpisode=*/true);
    }

    /// Connected → Closed: send a Disconnect frame (clean shutdown).
    /// Resuming → Closed: send a Disconnect frame if a session exists.
    /// No-op from any state other than Connected or Resuming.
    void disconnect() noexcept {
        if (state_ == SessionState::Connected) {
            sendDisconnect();
            transitionTo(SessionState::Closed);
            return;
        }
        if (state_ == SessionState::Resuming) {
            // A session was established before; send a polite goodbye if possible.
            if (sessionId_ != 0) {
                sendDisconnect();
            }
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
                           int64_t  nowNs) noexcept {
        // FIX E: remove the redundant Closed guard before the !=Connecting guard.
        if (state_ != SessionState::Connecting) return;  // ignore in wrong state

        // Stale / mismatched correlationId — ignore.
        if (incomingCorrelationId != correlationId_) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));

        if (ec == rpc::ErrorCode::OK) {
            // Happy path — session established.
            sessionId_           = assignedSessionId;
            lastKeepaliveSentNs_ = nowNs;
            lastActivityNs_      = nowNs;
            redirectCount_       = 0;
            // FIX C: reset both retry counters on successful connect/resume.
            connectRetryCount_   = 0;
            resumeRetryCount_    = 0;
            transitionTo(SessionState::Connected);
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
                transitionTo(SessionState::Disconnected);
                return;
            }
            redirectCount_++;
            // Resend ConnectRequest to the same endpoint (no retarget — no endpoint string).
            doConnect(nowNs, /*resetSessionId=*/false, /*freshEpisode=*/false);
            return;
        }

        // Any other error code (ProtocolVersion, SessionExpired, etc.) — disconnect.
        transitionTo(SessionState::Disconnected);
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
            transitionTo(SessionState::Disconnected);
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

    /// An ErrorResponse arrived.
    /// SessionExpired / UnknownSession in Connected → Resuming.
    /// Redirect/NotLeader in Connecting with an endpoint string → retarget + resend.
    /// Redirect/NotLeader in Connecting without endpoint → resend to same endpoint.
    void onErrorResponse(int16_t          code,
                         std::string_view message,
                         int64_t          incomingCorrelationId,
                         int64_t          nowNs) noexcept {
        if (state_ == SessionState::Closed) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));

        if (state_ == SessionState::Connected) {
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
                    transitionTo(SessionState::Disconnected);
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
                transitionTo(SessionState::Disconnected);
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
        switch (state_) {
        case SessionState::Connecting:
            tickConnecting(nowNs);
            break;
        case SessionState::Connected:
            tickConnected(nowNs);
            break;
        case SessionState::Resuming:
            tickResuming(nowNs);
            break;
        case SessionState::Disconnected:
        case SessionState::Closed:
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
        if (freshEpisode) {
            // FIX A + FIX C: reset all counters only on a fresh external connect().
            redirectCount_     = 0;
            connectRetryCount_ = 0;
            resumeRetryCount_  = 0;
        }
        correlationId_       = nextCorrelationId();
        connectStartNs_      = nowNs;
        lastConnectSentNs_   = nowNs;   // DR-BUG-2: track last send time for fast-retry

        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,
            correlationId_);

        if (n > 0) {
            // DR-BUG-2: record whether the initial send succeeded so tickConnecting
            // can fast-retry when the ingress pub was not-connected at offer time.
            connectSentOk_ = io_.sendFrame(scratch_.data(), n);
        }
        transitionTo(SessionState::Connecting);
    }

    /// Initiate a resume attempt from Resuming state.
    /// FIX C: resumeRetryCount_ is reset on onConnectResponse{OK}, not here.
    void doResume(int64_t nowNs) noexcept {
        if (resumeRetryCount_ >= cfg_.maxResumeRetries) {
            transitionTo(SessionState::Disconnected);
            return;
        }
        resumeRetryCount_++;
        // Resend ConnectRequest with the last sessionId (resume hint scaffold).
        correlationId_       = nextCorrelationId();
        connectStartNs_      = nowNs;
        lastConnectSentNs_   = nowNs;   // DR-BUG-2: track send time for fast-retry

        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,   // carry last sessionId as Milestone-B resume hint
            correlationId_);

        if (n > 0) {
            connectSentOk_ = io_.sendFrame(scratch_.data(), n);
        }
        transitionTo(SessionState::Connecting);
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
                const std::size_t n = rune::client::codec::encodeConnectRequest(
                    scratch_.data(), kScratchCap,
                    cfg_.egressChannel,
                    cfg_.egressStreamId,
                    cfg_.clientVersion,
                    sessionId_,
                    correlationId_);
                if (n > 0) {
                    connectSentOk_ = io_.sendFrame(scratch_.data(), n);
                }
            }
            return;
        }

        // Connect timed out.
        if (connectRetryCount_ >= cfg_.maxConnectRetries) {
            transitionTo(SessionState::Disconnected);
            return;
        }
        connectRetryCount_++;
        // FIX A: do NOT reset redirectCount_ here — the redirect cap must span
        //        the whole connect episode (reset only on fresh connect() or OK response).
        correlationId_       = nextCorrelationId();
        connectStartNs_      = nowNs;
        lastConnectSentNs_   = nowNs;

        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,
            correlationId_);
        if (n > 0) {
            connectSentOk_ = io_.sendFrame(scratch_.data(), n);
        }
    }

    void tickConnected(int64_t nowNs) noexcept {
        // Check for stale session first.
        if (nowNs - lastActivityNs_ > cfg_.responseTimeoutNs) {
            transitionTo(SessionState::Resuming);
            // FIX D: enter Resuming as a real state; doResume fires on next tick.
            resumeEntryNs_ = nowNs;
            return;
        }

        // Keepalive cadence.
        if (nowNs - lastKeepaliveSentNs_ >= cfg_.keepaliveIntervalNs) {
            lastKeepaliveSentNs_ = nowNs;

            const std::size_t n = rune::client::codec::encodeKeepalive(
                scratch_.data(), kScratchCap,
                sessionId_,
                correlationId_);
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
    int64_t resumeEntryNs_       = 0;  // FIX D: timestamp when Resuming was entered
    // DR-BUG-2: fast-retry state for the connect path.
    int64_t lastConnectSentNs_   = 0;  // when the last ConnectRequest was offered
    bool    connectSentOk_       = false; // true if the last offer() succeeded

    // Counters.
    int redirectCount_     = 0;
    int connectRetryCount_ = 0;
    int resumeRetryCount_  = 0;

    // Encode scratch buffer — reused each tick, zero per-tick heap allocation.
    std::array<uint8_t, kScratchCap> scratch_{};

    // Milestone-B scaffold: per-subscription offset map.
    // TODO(Milestone B): integrate into ConnectRequest resume-hint encoding.
    std::unordered_map<int64_t, SubscriptionOffset> offsets_;
};

} // namespace rune::client::session
