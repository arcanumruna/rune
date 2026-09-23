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
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>

#include <concurrent/BackOffIdleStrategy.h>
#include <concurrent/YieldingIdleStrategy.h>

#include <array>
#include <cassert>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

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
    , partitionRouter_(metadataCache_)
    , currentLeaderChannel_(cfg_.gatewayChannel)  // MD1 IMPORTANT-2: seed w/ initial gateway
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
            // MD1 IMPORTANT-2: before flushing produce frames, consult the
            // PartitionRouter.  If the pending produce partition's cached leader
            // endpoint differs from the current connection, retarget+reconnect so
            // the produce lands DIRECT on the partition leader.  When a retarget
            // is initiated we skip this drain cycle: drainAndFlush would emit onto
            // the old (follower) connection with a soon-to-be-stale session.  The
            // held frames flush on the next ConnectResponse{OK} (redirectPending_
            // machinery), re-stamped with the leader session — same as a redirect.
            if (routeProduceToLeader()) {
                return 1; // did routing work; defer the flush to post-reconnect
            }
            int work = producerHub_.drainAndFlush(nowNs, sessionId, sendFn);
            // TX-5: drain all transactional producers (rarely >1; mutex is fine).
            std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
            {
                std::lock_guard<std::mutex> lk(txnHubMtx_);
                txnSnap = txnProducers_;
            }
            for (auto& tp : txnSnap) {
                work += tp->doWork(sessionId, sendFn);
            }
            (void)nowNs;
            return work;
        });
    agent_.setProduceResponseFn(
        [this](int64_t correlationId, int64_t baseOffset) {
            producerHub_.onProduceResponse(correlationId, baseOffset);
        });

    // MD1 Task 10: Wire the MetadataResponse hook so the cache is populated
    // on the agent (runner) thread without any extra lock.
    agent_.setMetadataResponseFn(
        [this](const codec::MetadataResponseDecoded& resp) {
            onMetadataResponse(resp);
        });

    // CG-5: GroupConsumerHub is ticked from sweepTimeouts (100 ms cadence;
    // fine for heartbeat ~10 s and auto-commit ~5 s timescales).
    // Egress frame routing is wired in onEgressFrame below.

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

    // CG-5: Deactivate all group consumers (fire onRevoked, send LeaveGroup).
    // MUST be called after agent_.stop() so no agent thread is in onRecords().
    // sendFn here is a best-effort last send (transport may be stopping).
    {
        const auto snap = agent_.stateAndSessionId();
        const group::SendFn gcSendFn = [this](const uint8_t* fb, std::size_t fl) -> bool {
            return agent_.submitSend(fb, fl);
        };
        groupConsumerHub_.closeAll(snap.sessionId, gcSendFn);
    }

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

    // TX-5: fail all pending txn producer ops.
    {
        std::lock_guard<std::mutex> lk(txnHubMtx_);
        for (auto& tp : txnProducers_) {
            tp->failAll("Client closed");
        }
    }

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

    // --- CG-5: GroupConsumerHub duty cycle (heartbeat / auto-commit / rejoin) ---
    // The hub's doWork checks its own internal timers so calling it every
    // kSweepIntervalMs (100 ms) is sufficient for heartbeat (~10 s) and
    // auto-commit (~5 s) cadences.  Rejoining consumers send JoinGroup on the
    // very first doWork call after a state transition (no extra latency).
    if (!groupConsumerHub_.empty()) {
        const auto snap = agent_.stateAndSessionId();
        const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        const group::SendFn sendFn = [this](const uint8_t* fb, std::size_t fl) -> bool {
            return agent_.submitSend(fb, fl);
        };
        groupConsumerHub_.doWork(nowNs, snap.sessionId, sendFn);
    }

    // --- MD1 IMPORTANT-3: fire an armed background metadata refresh if due ------
    // No-op unless a refresh was armed (unknown leader / repeated redirects) and
    // its exponential-backoff window has elapsed while Connected.
    tickMetadataRefresh();

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

            // CG-5: Trigger group consumers to send JoinGroup on connect.
            if (!groupConsumerHub_.empty()) {
                const group::SendFn gcSendFn =
                    [this](const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    };
                groupConsumerHub_.onConnected(opt->assignedSessionId, gcSendFn);
            }

            // MD1 Task 10: on each successful connect, request a full metadata
            // refresh so the cache is populated for zero-RTT partition routing.
            sendMetadataRequest(opt->assignedSessionId);

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
            // DR-BUG-2 (redirect-reconnect robustness): a Redirect only concerns
            // THIS Client-level handler when there is an in-flight data op to
            // follow to the leader.  Determine that FIRST — before touching the
            // transport.
            //
            // Root cause of the follower connect() timeout: on the INITIAL
            // connect to a follower, the broker replies ErrorResponse{Redirect}.
            // That frame is dispatched to BOTH the SessionMachine (which, in
            // Connecting, already retarget()s to the leader and resends the
            // ConnectRequest WITH the DR-BUG-2 NOT_CONNECTED fast-retry) AND to
            // this observer.  If this observer ALSO retarget()ed, it created a
            // SECOND brand-new leader publication that DISCARDED the one the
            // SessionMachine had just offered its ConnectRequest on — throwing
            // away that offer and the pub's warm-up.  Under concurrent-client
            // load (slow pub warm-up) the reconnect then stalled until the
            // 15 s app-side connect deadline → "connect() timed out".
            //
            // Fix: at connect time (no work in flight) the SessionMachine OWNS
            // the redirect end-to-end; this observer must NOT retarget.  Only a
            // genuine established-session data-op redirect (haveWork) drives the
            // Client-level retarget + submitReconnect below.  This makes the
            // redirect-reconnect as robust as the initial connect (both now rely
            // on the single SessionMachine fast-retry publication) and removes
            // the pub-swap race — no busy-spin, connect/setup path only.
            const bool haveWork =
                producerHub_.hasInFlight() || subscriberHub_.hasPending();
            if (!haveWork) {
                // Connect-time (or duplicate/late) redirect: let the
                // SessionMachine handle it; do not churn the transport.
                break;
            }

            const std::string newEndpoint(opt->message);

            // MD1 IMPORTANT-1: zero-RTT metadata cache update from the REAL
            // redirect frame — no full MetadataRequest round-trip needed to
            // reroute.  MD1-9's broker REDIRECT carries the target ENDPOINT in
            // the message field (the {leaderId,leaderEpoch} were logged, not
            // wire-encoded).  So we recover the affected partition from OUR OWN
            // request context (the in-flight produce corrId → topicId/partition)
            // and resolve the endpoint back to an authoritative nodeId via the
            // broker set we already cached.  If both resolve, update the cache in
            // place so the very next route targets the new node immediately.  A
            // background refresh (with backoff) is also armed so the
            // authoritative {leaderNode,leaderEpoch} is reconciled shortly after.
            {
                int32_t rTopicId = -1, rPartition = -1;
                if (producerHub_.partitionForCorrId(
                        opt->correlationId, rTopicId, rPartition)) {
                    // Normalise the redirect target to the cache's "host:port"
                    // key: strip an "...endpoint=" prefix if present.
                    std::string cacheEp = newEndpoint;
                    const auto epPos = cacheEp.find("endpoint=");
                    if (epPos != std::string::npos) {
                        cacheEp = cacheEp.substr(epPos + 9u);
                    }
                    const int32_t newNodeId =
                        metadataCache_.nodeIdForEndpoint(cacheEp);
                    if (newNodeId != metadata::MetadataCache::kNoLeader) {
                        // Provisional epoch = current+1 so the staleness rule
                        // accepts it; the authoritative epoch overwrites it when
                        // the background MetadataResponse arrives.
                        const int32_t curEpoch =
                            metadataCache_.epochFor(rTopicId, rPartition);
                        const int32_t newEpoch = (curEpoch >= 0) ? (curEpoch + 1) : 0;
                        metadataCache_.updatePartitionLeader(
                            rTopicId, rPartition, newNodeId, newEpoch);
                        // IMPORTANT-2: we now KNOW which node the session is
                        // following — record it so proactive routing can avoid
                        // re-sending to the wrong node.
                        currentLeaderNode_ = newNodeId;
                    }
                }
                // No-progress (unknown corrId / endpoint not in broker set):
                // arm a background metadata refresh with exponential backoff so
                // the authoritative map is reconciled without storming.
                scheduleMetadataRefresh();
            }

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
                // MD1 IMPORTANT-2: record the connection's new leader channel so
                // proactive produce routing compares against the right target.
                currentLeaderChannel_ = newChannel;
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
                // Only initiate ONE reconnect per redirect episode: if a
                // reconnect is already pending (multiple in-flight ops each
                // redirected), coalesce — the single deferred resend on the next
                // ConnectResponse{OK} flushes ALL pending ops.  This prevents
                // session churn from a burst of redirects.
                bool expected = false;
                if (redirectPending_.compare_exchange_strong(
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

    // ---- Records push (CB-4 + CG-5) ----
    case codec::TemplateId::Records: {
        auto opt = codec::decodeRecords(buf, len);
        if (!opt) break;
        // Route to the subscriber hub first (direct subscriptions).
        subscriberHub_.onRecords(*opt);
        // Also route to group consumer hub (assigned partitions).
        groupConsumerHub_.onRecords(*opt);
        break;
    }

    // ---- CG-5: Consumer-group response frames --------------------------------

    case codec::TemplateId::JoinGroupResponse: {
        if (!groupConsumerHub_.empty()) {
            const auto snap = agent_.stateAndSessionId();
            const group::SendFn gcSendFn =
                [this](const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            groupConsumerHub_.onJoinGroupResponse(buf, len, snap.sessionId, gcSendFn);
        }
        break;
    }

    case codec::TemplateId::SyncGroupResponse: {
        if (!groupConsumerHub_.empty()) {
            const auto snap = agent_.stateAndSessionId();
            const group::SendFn gcSendFn =
                [this](const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            groupConsumerHub_.onSyncGroupResponse(buf, len, snap.sessionId, gcSendFn);
        }
        break;
    }

    case codec::TemplateId::HeartbeatResponse: {
        if (!groupConsumerHub_.empty()) {
            const auto snap = agent_.stateAndSessionId();
            const group::SendFn gcSendFn =
                [this](const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            groupConsumerHub_.onHeartbeatResponse(buf, len, snap.sessionId, gcSendFn);
        }
        break;
    }

    case codec::TemplateId::OffsetCommitResponse: {
        if (!groupConsumerHub_.empty()) {
            groupConsumerHub_.onOffsetCommitResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::OffsetFetchResponse: {
        if (!groupConsumerHub_.empty()) {
            groupConsumerHub_.onOffsetFetchResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::Assign: {
        if (!groupConsumerHub_.empty()) {
            const auto snap = agent_.stateAndSessionId();
            const group::SendFn gcSendFn =
                [this](const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            groupConsumerHub_.onAssign(buf, len, snap.sessionId, gcSendFn);
        }
        break;
    }

    // ---- TX-5: Transactional producer responses --------------------------------
    // ProduceResponse (tid 17) also lands here via the agent's egressObserver;
    // the producerHub_ regular path is driven via setProduceResponseFn (above).
    // Txn producers need the full buf+len+sendFn for their retry logic.

    case codec::TemplateId::ProduceResponse: {
        std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
        {
            std::lock_guard<std::mutex> lk(txnHubMtx_);
            txnSnap = txnProducers_;
        }
        if (!txnSnap.empty()) {
            std::function<bool(const uint8_t*, std::size_t)> sendFn =
                [this](const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            for (auto& tp : txnSnap) {
                tp->onProduceResponse(buf, len, sendFn);
            }
        }
        break;
    }

    case codec::TemplateId::InitProducerIdResponse: {
        std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
        {
            std::lock_guard<std::mutex> lk(txnHubMtx_);
            txnSnap = txnProducers_;
        }
        for (auto& tp : txnSnap) {
            tp->onInitProducerIdResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::AddPartitionsToTxnResponse: {
        std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
        {
            std::lock_guard<std::mutex> lk(txnHubMtx_);
            txnSnap = txnProducers_;
        }
        for (auto& tp : txnSnap) {
            tp->onAddPartitionsToTxnResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::AddOffsetsToTxnResponse: {
        std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
        {
            std::lock_guard<std::mutex> lk(txnHubMtx_);
            txnSnap = txnProducers_;
        }
        if (!txnSnap.empty()) {
            const auto snap = agent_.stateAndSessionId();
            std::function<bool(const uint8_t*, std::size_t)> sendFn =
                [this](const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            for (auto& tp : txnSnap) {
                tp->onAddOffsetsToTxnResponse(buf, len, snap.sessionId, sendFn);
            }
        }
        break;
    }

    case codec::TemplateId::TxnOffsetCommitResponse: {
        std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
        {
            std::lock_guard<std::mutex> lk(txnHubMtx_);
            txnSnap = txnProducers_;
        }
        for (auto& tp : txnSnap) {
            tp->onTxnOffsetCommitResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::EndTxnResponse: {
        std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnSnap;
        {
            std::lock_guard<std::mutex> lk(txnHubMtx_);
            txnSnap = txnProducers_;
        }
        for (auto& tp : txnSnap) {
            tp->onEndTxnResponse(buf, len);
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

    // Register with the hub — DR-BUG-1: producer() may legitimately be called
    // from the app thread at any time, including after start().  ProducerHub::add()
    // is thread-safe (cold path under addMtx_); the runner thread's hot-path
    // snapshot load is wait-free and sees the new entry on the next acquire.
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

// ---------------------------------------------------------------------------
// CG-5: subscribeGroup() — create and register a GroupConsumer
// ---------------------------------------------------------------------------

GroupConsumer Client::subscribeGroup(std::string              groupId,
                                      std::vector<std::string> topicNames,
                                      GroupConfig              cfg)
{
    // (#4) Guard: one group per Client.  Registering a second GroupConsumer
    // on the same Client would cause JoinGroup/SyncGroup/OffsetCommit
    // responses to be misrouted (all share correlationId=0 and are matched
    // by state to the first consumer in the matching state).
    // For multi-group support, add per-consumer correlationId tracking (CG-6).
    if (!groupConsumerHub_.empty()) {
        throw std::logic_error(
            "subscribeGroup: only one GroupConsumer per Client is supported. "
            "Use a separate Client instance for each consumer group. "
            "(Multi-group per connection is deferred to CG-6.)");
    }

    auto impl = std::make_shared<GroupConsumerImpl>();
    impl->groupId = std::move(groupId);
    impl->cfg     = cfg;

    // Resolve topicNames → topicIds using ClientConfig::topics.
    for (const auto& name : topicNames) {
        for (const auto& spec : cfg_.topics) {
            if (spec.name == name) {
                impl->topicIds.push_back(spec.topicId);
                break;
            }
        }
    }

    impl->state  = GroupConsumerState::Unjoined;
    impl->active.store(true, std::memory_order_relaxed);

    // Register with the hub — thread-safe cold path (under addMtx_).
    groupConsumerHub_.add(impl);

    return GroupConsumer(std::move(impl));
}

// ---------------------------------------------------------------------------
// TX-5: transactionalProducer() — create and register a TransactionalProducer
// ---------------------------------------------------------------------------

TransactionalProducer Client::transactionalProducer(const std::string& transactionalId,
                                                     ProducerConfig cfg)
{
    auto impl = std::make_shared<producer::TransactionalProducerImpl>(
        transactionalId,
        cfg,
        nextCorrId_);

    std::lock_guard<std::mutex> lk(txnHubMtx_);
    txnProducers_.push_back(impl);
    return TransactionalProducer(std::move(impl));
}

// ---------------------------------------------------------------------------
// MD1 Task 10: sendMetadataRequest() — request all-topic metadata from the broker
// ---------------------------------------------------------------------------

void Client::sendMetadataRequest(int64_t sessionId) noexcept {
    // Allocate a fresh correlationId.
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);
    pendingMetadataCorrId_ = corrId;

    // Encode a MetadataRequest (empty topicIds span = all topics).
    std::array<uint8_t, 256> buf{};
    const std::span<const int32_t> emptyTopics;
    const std::size_t n = codec::encodeMetadataRequest(
        buf.data(), buf.size(),
        emptyTopics,
        sessionId,
        corrId);
    if (n == 0) return;  // encoding failed — silently ignore

    // Submit to the agent ring (thread-safe, non-blocking).
    agent_.submitSend(buf.data(), n);
}

// ---------------------------------------------------------------------------
// MD1 Task 10: onMetadataResponse() — update MetadataCache from decoded response
// ---------------------------------------------------------------------------

void Client::onMetadataResponse(const codec::MetadataResponseDecoded& resp) noexcept {
    // Convert codec view types to metadata cache types.
    std::vector<metadata::BrokerEntry>    brokers;
    std::vector<metadata::PartitionEntry> partitions;

    brokers.reserve(resp.brokers.size());
    for (const auto& b : resp.brokers) {
        metadata::BrokerEntry e;
        e.nodeId = b.nodeId;
        e.port   = b.port;
        e.host   = std::string(b.host);
        brokers.push_back(std::move(e));
    }

    partitions.reserve(resp.partitions.size());
    for (const auto& p : resp.partitions) {
        metadata::PartitionEntry e;
        e.topicId     = p.topicId;
        e.partition   = p.partition;
        e.leaderId    = p.leaderId;
        e.leaderEpoch = p.leaderEpoch;
        partitions.push_back(e);
    }

    metadataCache_.updateFromResponse(brokers, partitions);

    // Update partitionRouter partition counts from the response.
    // Derive per-topic partition count from the maximum partition number seen.
    // (The server sends all partitions for each topic in the response.)
    std::unordered_map<int32_t, int32_t> maxPartition;
    for (const auto& p : partitions) {
        auto& maxP = maxPartition[p.topicId];
        if (p.partition > maxP) maxP = p.partition;
    }
    for (const auto& [topicId, maxP] : maxPartition) {
        partitionRouter_.setTopicPartitionCount(topicId, maxP + 1);
    }

    // MD1 IMPORTANT-3: a fresh authoritative map arrived — reset the backoff to
    // its initial interval so a future no-progress episode starts from the floor,
    // and disarm any pending refresh (we just refreshed).
    metadataBackoffNs_     = kMetadataBackoffInitNs;
    nextMetadataRefreshAt_ = std::chrono::steady_clock::time_point::max();
}

// ---------------------------------------------------------------------------
// MD1 IMPORTANT-3: scheduleMetadataRefresh() — arm a backoff-gated refresh
// ---------------------------------------------------------------------------
//
// Called on no-progress (unknown partition leader / repeated redirects).  Arms
// the next refresh at now+backoff, then DOUBLES the backoff (capped at
// kMetadataBackoffMaxNs) so a run of redirects can't storm the broker with
// MetadataRequests.  The armed refresh fires from tickMetadataRefresh().
void Client::scheduleMetadataRefresh() noexcept {
    // Idempotent while armed: the drain hook may call this every cycle on a
    // sustained no-progress condition.  Re-arming each call would push the
    // deadline forward forever (it would never fire) and blow the backoff to the
    // cap instantly.  So only arm+double when NOT already armed; the doubling
    // then happens once per fired-then-rescheduled episode.
    if (nextMetadataRefreshAt_ != std::chrono::steady_clock::time_point::max()) {
        return; // already armed — leave the pending deadline in place
    }
    nextMetadataRefreshAt_ = std::chrono::steady_clock::now() +
        std::chrono::nanoseconds(metadataBackoffNs_);

    if (metadataBackoffNs_ <= kMetadataBackoffMaxNs / 2) {
        metadataBackoffNs_ *= 2;
    } else {
        metadataBackoffNs_ = kMetadataBackoffMaxNs;
    }
}

// ---------------------------------------------------------------------------
// MD1 IMPORTANT-3: tickMetadataRefresh() — fire an armed refresh if due
// ---------------------------------------------------------------------------
//
// Called from sweepTimeouts() (~kSweepIntervalMs cadence).  Fires a
// MetadataRequest iff a refresh is armed, its backoff window has elapsed, and
// the session is Connected.  After firing it disarms (max()) so we don't spam
// until the next scheduleMetadataRefresh().
void Client::tickMetadataRefresh() noexcept {
    if (nextMetadataRefreshAt_ == std::chrono::steady_clock::time_point::max()) {
        return; // not armed
    }
    const auto snap = agent_.stateAndSessionId();
    if (snap.state != session::SessionState::Connected) return;
    if (snap.sessionId == 0) return;

    if (std::chrono::steady_clock::now() < nextMetadataRefreshAt_) return;

    nextMetadataRefreshAt_ = std::chrono::steady_clock::time_point::max(); // disarm
    sendMetadataRequest(snap.sessionId);
}

// ---------------------------------------------------------------------------
// MD1 IMPORTANT-2: routeProduceToLeader() — proactive per-partition routing
// ---------------------------------------------------------------------------
//
// If there is pending/in-flight produce work whose partition leader (per the
// cached metadata) lives on a DIFFERENT endpoint than the current connection,
// retarget the transport to that leader and reconnect so the produce lands
// DIRECT on the partition leader.  The held produce frames flush on the next
// ConnectResponse{OK} via the redirectPending_ machinery (re-stamped with the
// leader session).  Returns true if a retarget was initiated this cycle.
//
// PHASE-1 LIMITATION: the client transport maintains a SINGLE session /
// publication.  True multi-endpoint connection management (N simultaneous
// leader connections) is a larger change deferred beyond Phase 1.  Here,
// route-selection drives the target endpoint via cached metadata and reuses the
// existing connect/redirect machinery to REACH that leader on the one session.
bool Client::routeProduceToLeader() noexcept {
    // Only route when a reconnect isn't already in flight (avoid churn).
    if (redirectPending_.load(std::memory_order_acquire)) return false;

    int32_t topicId = -1, partition = -1;
    if (!producerHub_.firstPendingPartition(topicId, partition)) return false;

    // Resolve the partition leader's node from cached metadata.
    const int32_t leaderNode = metadataCache_.leaderFor(topicId, partition);
    if (leaderNode == metadata::MetadataCache::kNoLeader) {
        // Unknown leader for this partition → no-progress: arm a background
        // metadata refresh (backoff) and stay put; the broker will REDIRECT if
        // we're on the wrong node.
        scheduleMetadataRefresh();
        return false;
    }

    // Already following this leader (or we haven't yet learned which node the
    // initial gateway connection is — currentLeaderNode_ == -1)?  Do NOT
    // proactively churn the working connection.  When we ARE on the wrong node
    // the broker REDIRECT teaches us currentLeaderNode_ (see the redirect
    // handler) and the zero-RTT cache update keeps subsequent routes correct.
    if (currentLeaderNode_ < 0 || leaderNode == currentLeaderNode_) {
        return false;
    }

    // The cached leader differs from the node we're on → route DIRECT to it.
    const std::string endpoint = metadataCache_.endpointFor(leaderNode);
    if (endpoint.empty()) {
        scheduleMetadataRefresh();
        return false;
    }
    const std::string leaderChannel =
        (endpoint.rfind("aeron:", 0) == 0)
            ? endpoint
            : ("aeron:udp?endpoint=" + endpoint);
    if (leaderChannel == currentLeaderChannel_) return false;

    // Retarget to the partition leader + reconnect; defer the produce flush to
    // the next ConnectResponse{OK} (redirectPending_) so it carries the leader
    // session — identical to the redirect-follow contract.
    if (!agent_.retarget(leaderChannel)) return false;
    currentLeaderChannel_ = leaderChannel;
    currentLeaderNode_    = leaderNode;

    bool expected = false;
    if (redirectPending_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        agent_.submitReconnect();
    }
    return true;
}

} // namespace rune::client
