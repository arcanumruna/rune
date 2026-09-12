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
    Resuming,
    Closed,  // terminal; idempotent after this
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
    virtual void retarget(const std::string& newIngressChannel) = 0;

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

    // Redirect/retry caps — prevent infinite loops.
    int maxRedirects     = 5;  // per connect() call
    int maxConnectRetries = 3;  // connect-timeout retries before → Disconnected
    int maxResumeRetries  = 3;  // Resuming → Connecting retries before → Disconnected
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
        doConnect(nowNs, /*resetSessionId=*/true);
    }

    /// Connected → Closed: send a Disconnect frame (clean shutdown).
    /// No-op from any state other than Connected.
    void disconnect() noexcept {
        if (state_ != SessionState::Connected) return;
        sendDisconnect();
        transitionTo(SessionState::Closed);
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
        if (state_ == SessionState::Closed) return;
        if (state_ != SessionState::Connecting) return;  // ignore in wrong state

        // Stale / mismatched correlationId — ignore.
        if (incomingCorrelationId != correlationId_) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));

        if (ec == rpc::ErrorCode::OK) {
            // Happy path — session established.
            sessionId_          = assignedSessionId;
            lastKeepaliveSentNs_ = nowNs;
            lastActivityNs_      = nowNs;
            redirectCount_       = 0;
            connectRetryCount_   = 0;
            transitionTo(SessionState::Connected);
            return;
        }

        if (ec == rpc::ErrorCode::Redirect || ec == rpc::ErrorCode::NotLeader) {
            // leaderMemberId carries the new endpoint hint for redirect/notleader.
            // The actual channel string is conventionally embedded in the
            // assignedSessionId field (reused as string handle) — but the brief
            // specifies that the machine receives the endpoint as a string.
            // Since the codec gives us leaderMemberId + assignedSessionId
            // (no endpoint string in ConnectResponse), we interpret:
            //   assignedSessionId == 0 → no endpoint known → fall through to Disconnected
            //   assignedSessionId != 0 → treat as opaque channel id (real use: CC-5 agent
            //     resolves endpoint from a cluster-member map and calls the overload below).
            // The test-facing overload onConnectResponseRedirect() carries the string.
            (void)leaderMemberId;

            if (redirectCount_ >= cfg_.maxRedirects) {
                transitionTo(SessionState::Disconnected);
                return;
            }
            // Stay in Connecting; caller must re-issue with an endpoint.
            // (No-op retarget here — this overload has no channel string.)
            redirectCount_++;
            // Resend ConnectRequest (with same sessionId/correlationId reset).
            doConnect(nowNs, /*resetSessionId=*/false);
            return;
        }

        // Any other error code (ProtocolVersion, SessionExpired, etc.) — disconnect.
        transitionTo(SessionState::Disconnected);
    }

    /// ConnectResponse with a redirect endpoint string.
    /// This is the primary overload used by tests and by CC-5 when the agent
    /// maps leaderMemberId → channel string.
    void onConnectResponseRedirect(std::string_view newEndpoint,
                                   int64_t          incomingCorrelationId,
                                   int64_t          nowNs) noexcept {
        if (state_ == SessionState::Closed) return;
        if (state_ != SessionState::Connecting) return;
        if (incomingCorrelationId != correlationId_) return;

        if (redirectCount_ >= cfg_.maxRedirects) {
            transitionTo(SessionState::Disconnected);
            return;
        }

        redirectCount_++;
        io_.retarget(std::string(newEndpoint));
        doConnect(nowNs, /*resetSessionId=*/false);
    }

    /// An ErrorResponse arrived.
    /// SessionExpired / UnknownSession in Connected → Resuming.
    /// Redirect in Connecting → handled like onConnectResponseRedirect (no endpoint variant).
    void onErrorResponse(int16_t          code,
                         std::string_view /*message*/,
                         int64_t          incomingCorrelationId,
                         int64_t          nowNs) noexcept {
        if (state_ == SessionState::Closed) return;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(static_cast<uint16_t>(code));

        if (state_ == SessionState::Connected) {
            if (ec == rpc::ErrorCode::SessionExpired || ec == rpc::ErrorCode::UnknownSession) {
                transitionTo(SessionState::Resuming);
                // Immediately try to resume.
                doResume(nowNs);
            }
            return;
        }

        if (state_ == SessionState::Connecting) {
            if (incomingCorrelationId != correlationId_) return;
            // Redirect or NotLeader without endpoint → stay connecting, retry.
            if (ec == rpc::ErrorCode::Redirect || ec == rpc::ErrorCode::NotLeader) {
                if (redirectCount_ >= cfg_.maxRedirects) {
                    transitionTo(SessionState::Disconnected);
                    return;
                }
                redirectCount_++;
                doConnect(nowNs, /*resetSessionId=*/false);
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
    /// Connected → Resuming.
    void onTransportDisconnected(int64_t nowNs) noexcept {
        if (state_ != SessionState::Connected) return;
        transitionTo(SessionState::Resuming);
        doResume(nowNs);
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
    void doConnect(int64_t nowNs, bool resetSessionId) noexcept {
        if (resetSessionId) {
            sessionId_ = 0;
        }
        correlationId_   = nextCorrelationId();
        connectStartNs_  = nowNs;

        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,
            correlationId_);

        if (n > 0) {
            io_.sendFrame(scratch_.data(), n);
        }
        transitionTo(SessionState::Connecting);
    }

    /// Initiate a resume attempt from Resuming state.
    void doResume(int64_t nowNs) noexcept {
        if (resumeRetryCount_ >= cfg_.maxResumeRetries) {
            transitionTo(SessionState::Disconnected);
            return;
        }
        resumeRetryCount_++;
        // Resend ConnectRequest with the last sessionId (resume hint scaffold).
        correlationId_  = nextCorrelationId();
        connectStartNs_ = nowNs;

        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,   // carry last sessionId as Milestone-B resume hint
            correlationId_);

        if (n > 0) {
            io_.sendFrame(scratch_.data(), n);
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
        if (nowNs - connectStartNs_ <= cfg_.connectTimeoutNs) return;

        // Connect timed out.
        if (connectRetryCount_ >= cfg_.maxConnectRetries) {
            transitionTo(SessionState::Disconnected);
            return;
        }
        connectRetryCount_++;
        redirectCount_ = 0;
        // Re-send ConnectRequest with fresh correlationId, reset timer.
        correlationId_  = nextCorrelationId();
        connectStartNs_ = nowNs;

        const std::size_t n = rune::client::codec::encodeConnectRequest(
            scratch_.data(), kScratchCap,
            cfg_.egressChannel,
            cfg_.egressStreamId,
            cfg_.clientVersion,
            sessionId_,
            correlationId_);
        if (n > 0) {
            io_.sendFrame(scratch_.data(), n);
        }
    }

    void tickConnected(int64_t nowNs) noexcept {
        // Check for stale session first.
        if (nowNs - lastActivityNs_ > cfg_.responseTimeoutNs) {
            transitionTo(SessionState::Resuming);
            doResume(nowNs);
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

    void tickResuming(int64_t nowNs) noexcept {
        // If we transitioned to Resuming and haven't yet sent a ConnectRequest
        // (i.e. connectStartNs_ is stale), kick off a resume attempt.
        // This covers the case where Resuming was entered without doResume()
        // being called immediately (e.g. from a direct tick after transport-disconnect).
        if (nowNs - connectStartNs_ > cfg_.connectTimeoutNs) {
            doResume(nowNs);
        }
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
