// src/Client.cpp
//
// CC-6 — Client implementation: connect()/ping() futures + background runner.
//
// Threading safety summary:
//   - The runner thread (runLoop) is the ONLY caller of agent_.doWork().
//     It also calls sweepTimeouts() and onEgressFrame() (via the egress
//     observer that the agent invokes from within doWork()).
//   - The app thread calls connect() and ping() which:
//       1. Create a promise, insert into the pending map (under mtx_).
//       2. Submit a command to the agent's thread-safe ring.
//   - mtx_ protects pendingConnect_ and pendingPings_.  It is only held for
//     short critical sections; never held across agent calls or I/O.
//   - The egress observer (onEgressFrame) runs on the runner thread and
//     resolves/rejects promises while holding mtx_ for minimal scopes.

#include <rune/client/Client.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/codec/DataCodec.h>

#include <concurrent/BackOffIdleStrategy.h>
#include <concurrent/YieldingIdleStrategy.h>

#include <cassert>
#include <sstream>
#include <stdexcept>

namespace rune::client {

// ---------------------------------------------------------------------------
// Helper: build AgentConfig from ClientConfig
// ---------------------------------------------------------------------------

namespace {

agent::AgentConfig makeAgentConfig(const ClientConfig& c) {
    agent::AgentConfig acfg;
    acfg.transport.ingressChannel  = c.gatewayChannel;
    acfg.transport.ingressStreamId = c.gatewayStreamId;
    acfg.transport.egressChannel   = c.egressChannel;
    acfg.transport.egressStreamId  = c.egressStreamId;
    acfg.transport.dataDir         = c.dataDir;

    // Session egress config (for ConnectRequest body).
    acfg.session.egressChannel  = c.egressChannel;
    acfg.session.egressStreamId = c.egressStreamId;

    // Pass through keepalive interval if the caller overrides it.
    if (c.keepaliveIntervalNs > 0) {
        acfg.session.keepaliveIntervalNs = c.keepaliveIntervalNs;
    }

    // M2 FIX: propagate connectTimeoutMs → SessionMachine connectTimeoutNs.
    // Without this, the SessionMachine uses its default 5s per-retry timeout
    // regardless of the user-configured ClientConfig::connectTimeoutMs, which
    // means a dead-endpoint reconnect can take 5s × (maxRetries+1) = 20s before
    // the session transitions to Disconnected and sweep can promptly fail pending
    // produce futures and subscribes.  Map ms → ns; clamp to a 1ms minimum.
    if (c.connectTimeoutMs > 0) {
        acfg.session.connectTimeoutNs =
            static_cast<int64_t>(c.connectTimeoutMs) * 1'000'000LL;
    }

    return acfg;
}

} // namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

Client::Client(ClientConfig cfg)
    : cfg_(std::move(cfg))
    , agent_(makeAgentConfig(cfg_))
    , nextSweep_(std::chrono::steady_clock::now() +
                 std::chrono::milliseconds(kSweepIntervalMs))
{
    // Wire the egress observer BEFORE starting the agent (CC-5 contract).
    agent_.setEgressObserver([this](codec::TemplateId tid,
                                    const uint8_t*    buf,
                                    std::size_t       len) {
        onEgressFrame(tid, buf, len);
    });

    // CB-2: Wire the ProducerHub into the agent via function hooks so doWork
    // drives producer drain+flush and ProduceResponse frames are routed.
    // Using std::function keeps ClientAgent free of a header dependency on
    // ProducerHub (the agent links to no producer symbols at all).
    agent_.setProducerDrainFn(
        [this](int64_t nowNs, int64_t sessionId,
               agent::ClientAgent::SendFn sendFn) -> int {
            return producerHub_.drainAndFlush(nowNs, sessionId, std::move(sendFn));
        });
    agent_.setProduceResponseFn(
        [this](int64_t correlationId, int64_t baseOffset) {
            producerHub_.onProduceResponse(correlationId, baseOffset);
        });

    // Start the agent (transport + embedded driver).
    agent_.start();

    // Launch the background runner thread.
    runnerThread_ = std::thread([this]() { runLoop(); });
}

// ---------------------------------------------------------------------------
// Destruction
// ---------------------------------------------------------------------------

Client::~Client() {
    close();
}

// ---------------------------------------------------------------------------
// close() — idempotent shutdown
// ---------------------------------------------------------------------------

void Client::close() {
    // Atomically transition to stopped; only the first caller does the work.
    bool alreadyStopped = stopped_.exchange(true, std::memory_order_acq_rel);
    if (alreadyStopped) return;

    // Join the runner thread (it checks stopped_ in its loop condition).
    // CONTRACT: close() must NOT be called from within a Client callback
    // (onConnected/onDisconnected/onError run on the runner thread). Doing so
    // would self-join the runner (std::system_error) and re-enter teardown on the
    // runner's own doWork stack. Callbacks that want to close should hand off to
    // another thread. (Documented on the callback setters in Client.h.)
    if (runnerThread_.joinable()) {
        runnerThread_.join();
    }

    // Stop the agent (idempotent — CC-5 contract).
    agent_.stop();

    // CB-4: Deactivate all subscriptions so no more dispatch occurs.
    // MUST be called after agent_.stop() so no agent thread is in onRecords().
    subscriberHub_.deactivateAll();

    // M2 FIX: Fail any still-pending (unbound) subscribes so their active flag
    // is cleared and any polling app-code sees the subscription is dead.
    // Must be called after deactivateAll() (which already clears pending), but
    // failPendingSubscribes is idempotent (operates on pendingByCorrelation_
    // which deactivateAll already cleared — this is a belt-and-suspenders call
    // for any edge where deactivateAll was not yet called).
    subscriberHub_.failPendingSubscribes("Client closed");

    // Fail any still-pending producer acks (futures/callbacks) BEFORE returning.
    // producerHub_.failAll() calls ProducerImpl::failAllPending on every registered
    // producer — this resolves every pending future/callback with an error so they
    // never hang.  MUST be called after agent_.stop() so the agent thread is no
    // longer in drainAndFlush / onProduceResponse (no concurrent access to pendingAcks_).
    producerHub_.failAll("Client closed");

    // M2 FIX: clear redirectPending_ on close so it does not latch.
    redirectPending_.store(false, std::memory_order_release);

    // Fail any still-pending connect/ping futures.
    failAllPending("Client closed");

    // Fire onDisconnected once.
    bool expected = false;
    if (disconnectedFired_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        if (cbDisconnected_) {
            cbDisconnected_();
        }
    }
}

// ---------------------------------------------------------------------------
// Callback registration
// ---------------------------------------------------------------------------

// FIX F4: callback setters are setup-phase only.  Once connect() has been
// called the runner thread may read the callback std::functions, so mutating
// them here would be a data race.  Enforce the "set before connect()" contract:
// assert in debug builds; no-op safely in release.
void Client::onConnected(std::function<void(int64_t)> cb) {
    assert(!connectCalled_.load(std::memory_order_acquire) &&
           "onConnected() must be called BEFORE connect() (callback setters are "
           "setup-phase only; setting after connect() is a data race)");
    if (connectCalled_.load(std::memory_order_acquire)) return;
    cbConnected_ = std::move(cb);
}

void Client::onDisconnected(std::function<void()> cb) {
    assert(!connectCalled_.load(std::memory_order_acquire) &&
           "onDisconnected() must be called BEFORE connect()");
    if (connectCalled_.load(std::memory_order_acquire)) return;
    cbDisconnected_ = std::move(cb);
}

void Client::onError(std::function<void(const std::string&)> cb) {
    assert(!connectCalled_.load(std::memory_order_acquire) &&
           "onError() must be called BEFORE connect()");
    if (connectCalled_.load(std::memory_order_acquire)) return;
    cbError_ = std::move(cb);
}

// ---------------------------------------------------------------------------
// State observer
// ---------------------------------------------------------------------------

session::SessionState Client::state() const noexcept {
    return agent_.sessionState();
}

// ---------------------------------------------------------------------------
// connect() — app thread
// ---------------------------------------------------------------------------

std::future<void> Client::connect() {
    // FIX F4: mark the callback setup phase closed.  Release store so the runner
    // thread's acquire load sees all callbacks registered before this point.
    connectCalled_.store(true, std::memory_order_release);

    std::promise<void> prom;
    auto fut = prom.get_future();

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(cfg_.connectTimeoutMs);

    {
        std::lock_guard<std::mutex> lk(mtx_);
        // If there is already a pending connect, fail the old one.
        if (pendingConnect_) {
            try {
                pendingConnect_->promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("connect() called while another connect is pending")));
            } catch (...) { /* already satisfied */ }
            pendingConnect_.reset();
        }

        pendingConnect_ = std::make_unique<PendingConnect>(
            PendingConnect{std::move(prom), deadline});
    }

    // Submit the connect command to the ring (thread-safe).
    if (!agent_.submitConnect()) {
        // Ring full — fail the promise immediately.
        std::lock_guard<std::mutex> lk(mtx_);
        if (pendingConnect_) {
            try {
                pendingConnect_->promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("connect(): ring full, could not submit")));
            } catch (...) {}
            pendingConnect_.reset();
        }
    }

    return fut;
}

// ---------------------------------------------------------------------------
// ping() — app thread
// ---------------------------------------------------------------------------

std::future<Bytes> Client::ping(Bytes payload) {
    std::promise<Bytes> prom;
    auto fut = prom.get_future();

    // Require an established session (Milestone-A: reject, do not queue).
    // FIX F3: read (state, sessionId) as ONE coherent atomic snapshot so the
    // sessionId we stamp on the Ping always belongs to the same session epoch
    // as the observed Connected state (two independent loads could tear across
    // a resume and stamp a stale sessionId).
    const auto snap    = agent_.stateAndSessionId();
    const auto st      = snap.state;
    const auto sessId  = snap.sessionId;

    if (st != session::SessionState::Connected) {
        prom.set_exception(std::make_exception_ptr(
            std::runtime_error("ping(): not connected (state != Connected)")));
        return fut;
    }

    // Allocate a fresh correlationId.
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);

    // Encode the Ping frame into a scratch buffer.
    // Max frame size: header(24) + varLen(2) + payload <= 24+2+payload.size().
    // Use an on-stack buffer for typical payloads; heap for large ones.
    const std::size_t neededCap =
        26u + payload.size() + 32u; // 32 bytes headroom
    std::vector<uint8_t> scratch(neededCap);

    // Build string_view over payload bytes (binary-safe — std::string_view
    // does not require NUL termination; Codec's encodePing also uses
    // string_view, so NUL bytes are preserved).
    const std::string_view echoView(
        reinterpret_cast<const char*>(payload.data()), payload.size());

    const std::size_t frameLen = codec::encodePing(
        scratch.data(), scratch.size(),
        echoView,
        sessId,
        corrId);

    if (frameLen == 0) {
        prom.set_exception(std::make_exception_ptr(
            std::runtime_error("ping(): failed to encode Ping frame")));
        return fut;
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(cfg_.pingTimeoutMs);

    // Insert into pending map BEFORE submitting (prevents a race where the
    // Pong arrives before we insert the promise).
    {
        std::lock_guard<std::mutex> lk(mtx_);
        pendingPings_.emplace(corrId,
            PendingPing{std::move(prom), deadline});
    }

    // Submit the encoded frame to the agent ring.
    if (!agent_.submitSend(scratch.data(), frameLen)) {
        // Ring full — remove and fail the promise.
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = pendingPings_.find(corrId);
        if (it != pendingPings_.end()) {
            try {
                it->second.promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("ping(): ring full, could not submit")));
            } catch (...) {}
            pendingPings_.erase(it);
        }
    }

    return fut;
}

// ---------------------------------------------------------------------------
// runLoop() — background runner thread
// ---------------------------------------------------------------------------

void Client::runLoop() {
    // Select idle strategy based on ClientConfig::idleMode.
    //
    // LowLatency — YieldingIdleStrategy: on idle cycles the runner yields the
    //   OS scheduler quantum instead of parking.  No park ≡ no OS wake-up
    //   latency; the runner picks up a submitted command / inbound frame in
    //   ~µs.  Cost: one CPU core at ~100% while the runner is alive.
    //
    // Balanced (default) — BackoffIdleStrategy: spins a few cycles, then
    //   yields, then parks up to 1 ms.  CPU-friendly for clients that do not
    //   need sub-millisecond reaction time.
    if (cfg_.idleMode == IdleMode::LowLatency) {
        aeron::concurrent::YieldingIdleStrategy idleStrategy;

        while (!stopped_.load(std::memory_order_acquire)) {
            const int64_t now = nowNs();
            const int work = agent_.doWork(now);
            idleStrategy.idle(work);

            const auto wallNow = std::chrono::steady_clock::now();
            if (wallNow >= nextSweep_) {
                sweepTimeouts();
                nextSweep_ = wallNow +
                             std::chrono::milliseconds(kSweepIntervalMs);
            }
        }
    } else {
        // Balanced: Aeron BackoffIdleStrategy: spins → yields → parks (max 1 ms).
        aeron::concurrent::BackoffIdleStrategy idleStrategy(
            /*maxSpins=*/10,
            /*maxYields=*/20,
            /*minParkNs=*/std::chrono::duration<long, std::nano>(1'000),
            /*maxParkNs=*/std::chrono::milliseconds(1));

        while (!stopped_.load(std::memory_order_acquire)) {
            const int64_t now = nowNs();
            const int work = agent_.doWork(now);

            // Idle strategy: backs off on 0 work, resets on positive work.
            idleStrategy.idle(work);

            // Periodic timeout sweep (bounded, steady clock).
            const auto wallNow = std::chrono::steady_clock::now();
            if (wallNow >= nextSweep_) {
                sweepTimeouts();
                nextSweep_ = wallNow +
                             std::chrono::milliseconds(kSweepIntervalMs);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// sweepTimeouts() — called from the runner thread
// ---------------------------------------------------------------------------

void Client::sweepTimeouts() {
    const auto now = std::chrono::steady_clock::now();

    // --- Pending producer ack timeouts (no mtx_ — ProducerImpl has its own ackMtx_) ---
    // Mirror the connect/ping sweep: any pending ProduceResponse ack older than
    // produceTimeoutMs is failed so futures/callbacks never hang.
    producerHub_.sweepTimedOut(now);

    // --- M2 FIX: Pending subscribe-request timeouts ---
    // A Subscribe that gets no SubscribeResponse within subscribeTimeoutMs is
    // failed (active cleared) so the app is never left polling forever.
    subscriberHub_.sweepTimedOut(now);

    // --- M2 FIX: Disconnected-while-redirectPending_ prompt fail ---------------
    // When a redirect-triggered reconnect exhausts its connect retries (SessionMachine
    // tickConnecting → Disconnected), the Client has no Disconnected callback wired
    // into the agent.  Detect the condition here: state==Disconnected while
    // redirectPending_ is set → the reconnect to the (possibly dead) leader failed.
    // Promptly fail all pending produces + subscribes and reset redirectPending_
    // so it does not latch into any later connect attempt.
    if (redirectPending_.load(std::memory_order_acquire)) {
        const auto snap = agent_.stateAndSessionId();
        if (snap.state == session::SessionState::Disconnected) {
            if (redirectPending_.exchange(false, std::memory_order_acq_rel)) {
                producerHub_.failAll("Redirect reconnect failed: endpoint unreachable");
                subscriberHub_.failPendingSubscribes(
                    "Redirect reconnect failed: endpoint unreachable");
                if (cbError_) {
                    cbError_("Redirect reconnect failed: endpoint unreachable");
                }
            }
        }
    }

    std::lock_guard<std::mutex> lk(mtx_);

    // --- Pending connect timeout ---
    if (pendingConnect_ && now >= pendingConnect_->deadline) {
        try {
            pendingConnect_->promise.set_exception(
                std::make_exception_ptr(
                    std::runtime_error("connect() timed out")));
        } catch (...) {}
        pendingConnect_.reset();

        // Fire onError.
        if (cbError_) {
            cbError_("connect() timed out");
        }
    }

    // --- Pending ping timeouts ---
    for (auto it = pendingPings_.begin(); it != pendingPings_.end(); ) {
        if (now >= it->second.deadline) {
            try {
                it->second.promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("ping() timed out (no Pong received)")));
            } catch (...) {}
            it = pendingPings_.erase(it);
        } else {
            ++it;
        }
    }
}

// ---------------------------------------------------------------------------
// onEgressFrame() — egress observer, runs on the runner/agent thread
// ---------------------------------------------------------------------------

void Client::onEgressFrame(codec::TemplateId tid,
                            const uint8_t*    buf,
                            std::size_t       len)
{
    switch (tid) {

    // ---- ConnectResponse ----
    case codec::TemplateId::ConnectResponse: {
        auto opt = codec::decodeConnectResponse(buf, len);
        if (!opt) break;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(
            static_cast<uint16_t>(opt->code));

        if (ec == rpc::ErrorCode::OK) {
            // Resolve the pending-connect promise.
            std::unique_ptr<PendingConnect> pc;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                pc = std::move(pendingConnect_);
            }
            if (pc) {
                try { pc->promise.set_value(); } catch (...) {}
            }

            // CB-5 B: if this ConnectResponse{OK} completes a redirect-triggered
            // reconnect, flush every pending redirected data op NOW, re-stamped
            // with the leader's freshly assigned sessionId.  This is deferred to
            // exactly here (rather than at redirect time) so the resent frames
            // carry the NEW session — the leader drops unknown-session ops.
            // Runs on the agent thread (single-writer): submitSend → agent ring.
            if (redirectPending_.exchange(false, std::memory_order_acq_rel)) {
                const int64_t newSessionId = opt->assignedSessionId;
                std::function<bool(const uint8_t*, std::size_t)> sendFn =
                    [this](const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    };
                producerHub_.resendAllRedirected(newSessionId, sendFn);
                subscriberHub_.resendPending(newSessionId, sendFn);
            }

            // Fire onConnected.
            if (cbConnected_) {
                cbConnected_(opt->assignedSessionId);
            }
        } else {
            // Error response via ConnectResponse — fail the promise and fire onError.
            std::unique_ptr<PendingConnect> pc;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                pc = std::move(pendingConnect_);
            }
            const std::string msg = "ConnectResponse error code: " +
                                    std::to_string(opt->code);
            if (pc) {
                try {
                    pc->promise.set_exception(
                        std::make_exception_ptr(std::runtime_error(msg)));
                } catch (...) {}
            }
            if (cbError_) {
                cbError_(msg);
            }
        }
        break;
    }

    // ---- ErrorResponse ----
    case codec::TemplateId::ErrorResponse: {
        auto opt = codec::decodeErrorResponse(buf, len);
        if (!opt) break;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(
            static_cast<uint16_t>(opt->code));

        // CB-5 B: Redirect handling for pending data ops (Produce/Subscribe).
        // When a follower returns ErrorResponse{Redirect, leaderClientEndpoint}
        // for a pending Produce, we retarget the transport to the new leader
        // endpoint, reconnect the session, and resend the affected op.
        // Bounded by kMaxRedirects (ProducerImpl caps per-corrId retries).
        if (ec == rpc::ErrorCode::Redirect && !opt->message.empty()) {
            const std::string newEndpoint(opt->message);
            // Retarget transport to the leader's client endpoint.
            // If the endpoint is already a full Aeron channel URI (starts with
            // "aeron:"), use it as-is (test / ipc scenarios).  Otherwise treat
            // it as host:port and build a UDP channel.  This allows both
            // production UDP redirects and in-process ipc test redirects.
            const std::string newChannel =
                (newEndpoint.rfind("aeron:", 0) == 0)
                    ? newEndpoint
                    : ("aeron:udp?endpoint=" + newEndpoint);

            // Retarget is noexcept; if it fails the old publication is kept.
            const bool retargeted = agent_.retarget(newChannel);

            if (retargeted) {
                // CB-5 B FIX (bugs 1-4): a data op (Produce OR Subscribe) was
                // rejected by a FOLLOWER.  We must:
                //   (1) reconnect to the leader with a FRESH session (the leader
                //       has no session for us — submitReconnect forces a new
                //       ConnectRequest even though we are still logically
                //       Connected to the follower), and
                //   (2) resend the affected op(s) — Produce AND Subscribe — but
                //       only AFTER the new ConnectResponse{OK} arrives, so they
                //       carry the leader's freshly assigned sessionId (re-stamped
                //       into the frame header).  We defer the resend by setting
                //       redirectPending_; the ConnectResponse{OK} handler flushes
                //       it (see above).  This fixes: subscribe never resent (1),
                //       stale sessionId (2), resend racing the reconnect (3).
                //
                // Stick-to-leader (4): the transport retarget persists and the
                // new session is the leader's, so every subsequent op reads the
                // leader session via stateAndSessionId() and lands directly on
                // the leader without re-redirecting.
                //
                // If nothing is actually in flight (e.g. a duplicate/late
                // Redirect after we already reconnected), avoid a spurious
                // reconnect that would churn the session.
                const bool haveWork =
                    producerHub_.hasInFlight() || subscriberHub_.hasPending();
                // Only initiate ONE reconnect per redirect episode: if a
                // reconnect is already pending (multiple in-flight ops each
                // redirected), coalesce — the single deferred resend on the next
                // ConnectResponse{OK} flushes ALL pending ops.  This prevents
                // session churn from a burst of redirects.
                bool expected = false;
                if (haveWork &&
                    redirectPending_.compare_exchange_strong(
                        expected, true, std::memory_order_acq_rel)) {
                    agent_.submitReconnect();
                }
            } else {
                // Retarget failed — fail the pending op(s) promptly.
                // M2 FIX: also fail pending subscribes (not just produces) and
                // reset redirectPending_ so it does not latch into a later connect.
                producerHub_.failAll("Redirect retarget failed");
                subscriberHub_.failPendingSubscribes("Redirect retarget failed");
                redirectPending_.store(false, std::memory_order_release);
                if (cbError_) {
                    cbError_("Redirect retarget failed for endpoint: " + newEndpoint);
                }
            }
            break; // Redirect is handled; don't fall through to the error path.
        }

        std::string msg(opt->message);
        if (msg.empty()) {
            msg = "ErrorResponse code: " + std::to_string(opt->code);
        }

        // FIX F2: fail in-flight pings on this error IMMEDIATELY rather than
        // letting them die by pingTimeoutMs (which loses the server error and
        // adds full-timeout latency).  Two cases, both under a single lock so
        // there is no double-resolve (extract-under-lock discipline):
        //   (a) the error carries a correlationId matching a pending ping →
        //       fail exactly that ping;
        //   (b) the error is session-fatal (SessionExpired / UnknownSession) →
        //       fail ALL pending pings for the (now dead) session.
        const bool sessionFatal =
            (ec == rpc::ErrorCode::SessionExpired ||
             ec == rpc::ErrorCode::UnknownSession);

        std::unique_ptr<PendingConnect>       pc;
        std::vector<std::promise<Bytes>>      failedPings;
        {
            std::lock_guard<std::mutex> lk(mtx_);

            // Always fail a pending connect (existing behaviour).
            pc = std::move(pendingConnect_);

            if (sessionFatal) {
                // Fail every pending ping for this session.
                for (auto& [corrId, pp] : pendingPings_) {
                    failedPings.push_back(std::move(pp.promise));
                }
                pendingPings_.clear();
            } else {
                // Fail only the ping whose correlationId matches (if any).
                auto it = pendingPings_.find(opt->correlationId);
                if (it != pendingPings_.end()) {
                    failedPings.push_back(std::move(it->second.promise));
                    pendingPings_.erase(it);
                }
            }
        }

        if (pc) {
            try {
                pc->promise.set_exception(
                    std::make_exception_ptr(std::runtime_error(msg)));
            } catch (...) {}
        }
        for (auto& prom : failedPings) {
            try {
                prom.set_exception(
                    std::make_exception_ptr(std::runtime_error(msg)));
            } catch (...) {}
        }
        if (cbError_) {
            cbError_(msg);
        }
        break;
    }

    // ---- Pong ----
    case codec::TemplateId::Pong: {
        auto opt = codec::decodePong(buf, len);
        if (!opt) break;

        // Copy the echo bytes (string_view is a view into the Aeron buffer
        // which is only valid for the duration of this callback).
        Bytes echoBytes(opt->echo.begin(), opt->echo.end());

        std::promise<Bytes> prom;
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = pendingPings_.find(opt->correlationId);
            if (it != pendingPings_.end()) {
                prom = std::move(it->second.promise);
                pendingPings_.erase(it);
                found = true;
            }
        }
        if (found) {
            try { prom.set_value(std::move(echoBytes)); } catch (...) {}
        }
        break;
    }

    // ---- SubscribeResponse (CB-4) ----
    case codec::TemplateId::SubscribeResponse: {
        auto opt = codec::decodeSubscribeResponse(buf, len);
        if (!opt) break;
        // CB-5 A2: pass a sendFn so SubscriberHub can emit a late Unsubscribe
        // if the app called unsubscribe() before the response arrived.
        {
            const int64_t unsubCorrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
            const auto snap = agent_.stateAndSessionId();
            subscriberHub_.onSubscribeResponse(
                *opt,
                unsubCorrId,
                snap.sessionId,
                [this](const uint8_t* fbuf, std::size_t flen) -> bool {
                    return agent_.submitSend(fbuf, flen);
                });
        }
        break;
    }

    // ---- Records push (CB-4) ----
    case codec::TemplateId::Records: {
        auto opt = codec::decodeRecords(buf, len);
        if (!opt) break;
        // Route to the subscriber hub (agent thread).
        subscriberHub_.onRecords(*opt);
        break;
    }

    default:
        // Unknown template — ignore (never crash).
        break;
    }
}

// ---------------------------------------------------------------------------
// failAllPending() — fail every pending promise with the given reason
// ---------------------------------------------------------------------------

void Client::failAllPending(const std::string& reason) {
    std::lock_guard<std::mutex> lk(mtx_);

    if (pendingConnect_) {
        try {
            pendingConnect_->promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
        pendingConnect_.reset();
    }

    for (auto& [corrId, pp] : pendingPings_) {
        try {
            pp.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
    }
    pendingPings_.clear();
}

// ---------------------------------------------------------------------------
// CB-2: topic() — static topic handle lookup
// ---------------------------------------------------------------------------

TopicHandle Client::topic(const std::string& name) const noexcept {
    for (const auto& spec : cfg_.topics) {
        if (spec.name == name) {
            return TopicHandle{spec.topicId, spec.partitionCount};
        }
    }
    return TopicHandle{-1, 1}; // invalid
}

// ---------------------------------------------------------------------------
// CB-2: producer() — create and register a Producer
// ---------------------------------------------------------------------------

Producer Client::producer(ProducerConfig cfg) {
    // ProducerImpl owns its own ring buffer; shares the correlationId counter.
    auto impl = std::make_shared<producer::ProducerImpl>(
        cfg,
        nextCorrId_);

    // Register with the hub (hub is wired into the agent before start()).
    producerHub_.add(impl);

    return Producer(std::move(impl));
}

// ---------------------------------------------------------------------------
// CB-4: subscribe() — create a new push subscription
// ---------------------------------------------------------------------------

Subscription Client::subscribe(TopicHandle      topic,
                                const Key&       key,
                                SubscribeOptions opts,
                                RecordHandler    handler)
{
    const int32_t partition = key.partition(topic.partitionCount);
    return subscribeImpl(topic, partition, /*partitionKnown=*/0,
                         key.bytes(), opts, std::move(handler));
}

Subscription Client::subscribe(TopicHandle      topic,
                                int32_t          partition,
                                SubscribeOptions opts,
                                RecordHandler    handler)
{
    return subscribeImpl(topic, partition, /*partitionKnown=*/1,
                         {}, opts, std::move(handler));
}

Subscription Client::subscribeImpl(TopicHandle              topic,
                                    int32_t                  partition,
                                    uint8_t                  partitionKnown,
                                    std::span<const uint8_t> keyBytes,
                                    SubscribeOptions         opts,
                                    RecordHandler            handler)
{
    // Allocate a fresh correlationId (app-thread, atomic).
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);

    // Snapshot the current session id (coherent pair — same as ping()).
    const auto snap    = agent_.stateAndSessionId();
    const int64_t sessId = snap.sessionId;

    // Build the shared impl.
    auto impl = std::make_shared<SubscriptionImpl>();
    impl->correlationId = corrId;
    impl->topicId       = topic.topicId;
    impl->partition     = partition;
    impl->handler       = std::move(handler);
    impl->active.store(true, std::memory_order_relaxed);
    impl->subscriptionId.store(0, std::memory_order_relaxed);
    impl->nextOffset.store(0, std::memory_order_relaxed);

    // Build the unsubscribe callback: submits Unsubscribe via the agent ring.
    // Capture the agent_ by reference (safe: Client owns the agent and outlives
    // any Subscription the app can hold — same lifetime guarantee as producers).
    auto unsubFn = [this](int64_t subId) {
        const int64_t corrId2 = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
        const auto s = agent_.stateAndSessionId();
        subscriberHub_.sendUnsubscribe(
            subId,
            s.sessionId,
            corrId2,
            [this](const uint8_t* buf, std::size_t len) -> bool {
                return agent_.submitSend(buf, len);
            });
    };

    // Register + submit Subscribe (encodes and writes to the ring).
    // M2 FIX: pass a deadline so sweepTimedOut() can fail this subscribe if
    // SubscribeResponse never arrives (mirrors produce-ack timeout sweep).
    const auto subscribeDL = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(cfg_.subscribeTimeoutMs);

    subscriberHub_.registerSubscribe(
        impl,
        sessId,
        corrId,
        topic.topicId,
        partition,
        partitionKnown,
        opts.from.toWire(),
        static_cast<uint8_t>(opts.isolation),
        keyBytes,
        subscribeDL,
        [this](const uint8_t* buf, std::size_t len) -> bool {
            return agent_.submitSend(buf, len);
        });

    return Subscription(std::move(impl), std::move(unsubFn));
}

} // namespace rune::client
