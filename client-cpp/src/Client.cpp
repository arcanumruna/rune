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

#include <concurrent/BackOffIdleStrategy.h>

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

    // Fail any still-pending futures.
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
    // Use Aeron BackoffIdleStrategy: spins → yields → parks (max 1 ms).
    // This satisfies the "NO busy-spin" requirement: after the spin/yield
    // phases it parks for up to 1 ms, backing off on idle cycles.
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

// ---------------------------------------------------------------------------
// sweepTimeouts() — called from the runner thread
// ---------------------------------------------------------------------------

void Client::sweepTimeouts() {
    const auto now = std::chrono::steady_clock::now();

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

} // namespace rune::client
