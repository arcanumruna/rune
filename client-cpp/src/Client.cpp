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
#include <rune/client/agent/AgentProduceSink.h>
#include <rune/client/detail/CpuAffinity.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <concurrent/BackOffIdleStrategy.h>
#include <concurrent/YieldingIdleStrategy.h>

#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace rune::client {

// ---------------------------------------------------------------------------
// Helper: build AgentConfig from ClientConfig
// ---------------------------------------------------------------------------

agent::AgentConfig toAgentConfig(const ClientConfig& c) {
    agent::AgentConfig acfg;
    acfg.transport.ingressChannel  = c.gatewayChannel;
    acfg.transport.ingressStreamId = c.gatewayStreamId;
    acfg.transport.egressChannel   = c.egressChannel;
    acfg.transport.egressStreamId  = c.egressStreamId;
    acfg.transport.dataDir         = c.dataDir;
    acfg.transport.termBufferLength = c.termBufferBytes;

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

    // Task 3: explicit Aeron knobs (spec §4.1/§4.3).  Env overrides win.
    acfg.transport.mtuLength         = c.mtuBytes;
    acfg.transport.socketSndBufBytes = c.socketBufferBytes;
    acfg.transport.socketRcvBufBytes = c.socketBufferBytes;
    // Task 12: RunnerMode::Invoker = no threads (invoker driver + conductor).
    acfg.transport.driverMode = (c.runnerMode == RunnerMode::Invoker)
                                    ? transport::DriverMode::Invoker
                                    : transport::DriverMode::Thread;
    acfg.transport.clientLivenessTimeoutNs = c.aeronClientLivenessTimeoutNs;
    transport::applyEnvOverrides(acfg.transport);

    return acfg;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

Client::Client(ClientConfig cfg)
    : cfg_(std::move(cfg))
    , agent_(toAgentConfig(cfg_))
    , partitionRouter_(metadataCache_)
{
    // MD1 IMPORTANT-2: the seed session's currentLeaderChannel is initialised to
    // the config ingress channel (== gatewayChannel) inside ClientAgent's Session
    // constructor — Task 1.1 (was Client::currentLeaderChannel_ here).
    // Wire the egress observer BEFORE starting the agent (CC-5 contract).
    agent_.setEgressObserver([this](codec::TemplateId tid,
                                    const uint8_t*    buf,
                                    std::size_t       len) {
        onEgressFrame(tid, buf, len);
    });

    // Produce + ack path: plain member calls from dutyCycle() (spec §4.3).  No
    // producer-drain / ProduceResponse std::function hooks are installed on the
    // agent (they remain only as test-harness seams).  ProduceResponses are
    // buffered by the agent (produceAcks()) and routed to the hub by dutyCycle().
    agent_.setForwardProduceResponses(false);  // re-enabled when a txn producer registers

    // Task 1.4 (multi-node redirect convergence): fired on the agent thread when a
    // partition-leader session reaches Connected.  Re-offer every in-flight frame
    // through its partition route.  Cold path (connect event), unchanged.
    agent_.setProduceResendFn([this]() {
        producer::ProducerImpl::SendFn sendFn =
            [this](int32_t t, int32_t p, int32_t sid, const uint8_t* fb, std::size_t fl) -> bool {
                return agent_.offerToPartition(t, p, sid, fb, fl);
            };
        producerHub_.resendAllViaRoute(sendFn);
    });

    // Shared-nothing producer registration: the duty-cycle thread drains a
    // kMsgRegisterProducer command and lands here, adopting the producer into the
    // single-owner ProducerHub list.  `holder` is a heap shared_ptr<ProducerImpl>*
    // created by Client::producer(); we move the shared_ptr out and free the holder.
    agent_.setProducerRegisterFn([this](void* holder) {
        auto* sp = static_cast<std::shared_ptr<producer::ProducerImpl>*>(holder);
        producerHub_.add(std::move(*sp));
        delete sp;
    });
    // Same hand-off for transactional producers (was a mutex-guarded vector that
    // the runner locked and copied every duty cycle).
    agent_.setTxnProducerRegisterFn([this](void* holder) {
        auto* sp = static_cast<std::shared_ptr<producer::TransactionalProducerImpl>*>(holder);
        txnProducers_.push_back(std::move(*sp));
        delete sp;
        agent_.setForwardProduceResponses(true);  // txn producers consume raw ProduceResponses
    });

    // MD1 Task 10: Wire the MetadataResponse hook so the cache is populated
    // on the duty-cycle thread without any extra lock.
    agent_.setMetadataResponseFn([this](const codec::MetadataResponseDecoded& resp) {
        onMetadataResponse(resp);
    });

    // Built ONCE (not per duty cycle / per frame).  TX-5 / Task 1.4: txn producers
    // keep their 3-arg SendFn (streamId, buf, len); the data path has no
    // per-partition route, so topicId/partition = 0 → the agent's default offer path.
    txnSendFn_ = [this](int32_t streamId, const uint8_t* buf, std::size_t len) -> bool {
        return agent_.offerToPartition(/*topicId=*/0, /*partition=*/0, streamId, buf, len);
    };
    txnControlSendFn_ = [this](int32_t, const uint8_t* buf, std::size_t len) -> bool {
        return agent_.submitSend(buf, len);
    };

    // CG-5: GroupConsumerHub is ticked from sweepTimeouts (100 ms cadence).

    agent_.start();  // Invoker: pumps its own driver while resolving (no threads)
    ownerThread_ = std::this_thread::get_id();
    nextSweepNs_ = now() + kSweepIntervalMs * 1'000'000LL;
    if (cfg_.runnerMode == RunnerMode::Thread) {
        runnerThread_ = std::thread([this]() { runLoop(); });
    }
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
        const group::SendFn gcSendFn = [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
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
    // longer in drainAndFlush / onProduceResponse (no concurrent access to the ack rings).
    producerHub_.failAll("Client closed");

    // TX-5: fail all pending txn producer ops.  No lock: the runner was joined
    // above (Thread mode) or this is the owner thread (Invoker mode).
    for (auto& tp : txnProducers_) {
        tp->failAll("Client closed");
    }

    // M2 FIX: clear the seed session's redirect latch on close so it does not
    // latch.  Safe as a plain write: the runner thread was joined above, so this
    // runs single-threaded (no concurrent agent-thread access) — Task 1.1.
    redirectPending() = false;

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
// defineTopic() — app thread (Task 10)
// ---------------------------------------------------------------------------

std::future<DefineTopicResult> Client::defineTopic(std::string              name,
                                                    uint32_t                 partitions,
                                                    std::string              keyField,
                                                    bool                     validate,
                                                    std::span<const uint8_t> descriptorBlob)
{
    namespace rpc = rune::protocol::client;

    std::promise<DefineTopicResult> prom;
    auto fut = prom.get_future();

    // Require an established session (mirror ping() semantics).
    const auto snap   = agent_.stateAndSessionId();
    const auto st     = snap.state;
    const auto sessId = snap.sessionId;

    if (st != session::SessionState::Connected) {
        prom.set_exception(std::make_exception_ptr(
            std::runtime_error("defineTopic(): not connected (state != Connected)")));
        return fut;
    }

    // Allocate a fresh correlationId.
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);

    // Encode the DefineTopic request.
    rpc::DefineTopic msg;
    msg.sessionId      = sessId;
    msg.correlationId  = corrId;
    msg.topicName      = name;
    msg.partitionCount = partitions;
    msg.keyField       = keyField;
    msg.validate       = validate;
    msg.descriptorBlob = descriptorBlob.data();
    msg.descriptorLen  = static_cast<uint32_t>(descriptorBlob.size());

    const std::size_t neededCap = msg.encodedSize() + 32u; // 32 bytes headroom
    std::vector<uint8_t> scratch(neededCap);
    const std::size_t frameLen = msg.encode(scratch.data());

    if (frameLen == 0) {
        prom.set_exception(std::make_exception_ptr(
            std::runtime_error("defineTopic(): failed to encode DefineTopic frame")));
        return fut;
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(cfg_.pingTimeoutMs);

    // Insert into pending map BEFORE submitting (prevents a race where the
    // response arrives before we insert the promise — mirrors ping()).
    {
        std::lock_guard<std::mutex> lk(mtx_);
        pendingDefine_.emplace(corrId,
            PendingDefine{std::move(prom), deadline, name});
    }

    // Submit the encoded frame to the agent ring.
    if (!agent_.submitSend(scratch.data(), frameLen)) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = pendingDefine_.find(corrId);
        if (it != pendingDefine_.end()) {
            try {
                it->second.promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("defineTopic(): ring full, could not submit")));
            } catch (...) {}
            pendingDefine_.erase(it);
        }
    }

    return fut;
}

// ---------------------------------------------------------------------------
// getTopicSchema() — app thread (Task 10)
// ---------------------------------------------------------------------------

std::future<TopicSchemaResult> Client::getTopicSchema(std::string name)
{
    namespace rpc = rune::protocol::client;

    std::promise<TopicSchemaResult> prom;
    auto fut = prom.get_future();

    // Require an established session.
    const auto snap   = agent_.stateAndSessionId();
    const auto st     = snap.state;
    const auto sessId = snap.sessionId;

    if (st != session::SessionState::Connected) {
        prom.set_exception(std::make_exception_ptr(
            std::runtime_error("getTopicSchema(): not connected (state != Connected)")));
        return fut;
    }

    // Allocate a fresh correlationId.
    const int64_t corrId = nextCorrId_.fetch_add(1, std::memory_order_relaxed);

    // Encode the GetTopicSchema request.
    rpc::GetTopicSchema msg;
    msg.sessionId     = sessId;
    msg.correlationId = corrId;
    msg.topicName     = name;

    const std::size_t neededCap = msg.encodedSize() + 32u;
    std::vector<uint8_t> scratch(neededCap);
    const std::size_t frameLen = msg.encode(scratch.data());

    if (frameLen == 0) {
        prom.set_exception(std::make_exception_ptr(
            std::runtime_error("getTopicSchema(): failed to encode GetTopicSchema frame")));
        return fut;
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(cfg_.pingTimeoutMs);

    {
        std::lock_guard<std::mutex> lk(mtx_);
        pendingGet_.emplace(corrId,
            PendingGet{std::move(prom), deadline});
    }

    if (!agent_.submitSend(scratch.data(), frameLen)) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = pendingGet_.find(corrId);
        if (it != pendingGet_.end()) {
            try {
                it->second.promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("getTopicSchema(): ring full, could not submit")));
            } catch (...) {}
            pendingGet_.erase(it);
        }
    }

    return fut;
}

// ---------------------------------------------------------------------------
// listTopics() — app thread (Task 9 CLI)
// ---------------------------------------------------------------------------

std::vector<std::pair<std::string, int32_t>> Client::listTopics()
{
    std::lock_guard<std::mutex> lk(mtx_);
    return knownTopics_;
}

// ---------------------------------------------------------------------------
// doWork() / dutyCycle() — the ONE duty cycle shared by both runner modes
// ---------------------------------------------------------------------------

int Client::doWork() {
    if (cfg_.runnerMode != RunnerMode::Invoker) {
        throw std::logic_error("Client::doWork(): only valid in RunnerMode::Invoker");
    }
    assert(std::this_thread::get_id() == ownerThread_ && "doWork() from a non-owner thread");
    if (inDutyCycle_) return 0;  // re-entered from a callback: never nest the duty cycle
    return dutyCycle(now());
}

int Client::dutyCycle(int64_t nowNs) {
    struct InCycle {
        bool& flag;
        explicit InCycle(bool& f) : flag(f) { flag = true; }
        ~InCycle() { flag = false; }
    } inCycle(inDutyCycle_);

    // 1. Agent: driver/conductor (invoker), command ring, egress, session ticks.
    //    ProduceResponses are decoded and buffered, not dispatched through hooks.
    int work = agent_.doWork(nowNs);

    // 2. ProduceResponses → hub with plain calls (O(1) ordinal routing).
    const auto acks = agent_.produceAcks();
    const int ackCount = static_cast<int>(acks.size());
    for (const auto& a : acks) producerHub_.onProduceResponse(a.correlationId, a.baseOffsetOrError);
    agent_.clearProduceAcks();

    // 3. Producer drain/flush through AgentProduceSink (no per-cycle std::function).
    work += ackCount + producerDuty_(nowNs);

    // 4. Periodic sweep on the duty-cycle clock.
    if (nowNs >= nextSweepNs_) {
        sweepTimeouts(nowNs);
        nextSweepNs_ = nowNs + kSweepIntervalMs * 1'000'000LL;
    }
    return work;
}

int Client::producerDuty_(int64_t nowNs) {
    const int64_t sessionId = agent_.seedSession().machine.sessionId();
    int work = producerHub_.drainAndFlush(nowNs, sessionId, agent::AgentProduceSink{&agent_});
    for (auto& tp : txnProducers_) work += tp->doWork(sessionId, txnSendFn_);
    return work;
}

// ---------------------------------------------------------------------------
// runLoop() — background runner thread (RunnerMode::Thread)
// ---------------------------------------------------------------------------

void Client::runLoop() {
    if (cfg_.runnerCore >= 0 && !detail::pinCurrentThreadToCore(cfg_.runnerCore)) {
        std::fprintf(stderr, "rune-client: could not pin the runner to core %d\n", cfg_.runnerCore);
    }
    if (cfg_.idleMode == IdleMode::LowLatency) {
        // spin 5, yield 3, park 1 µs .. 100 µs: the runner drives the producer drain
        // AND the ack poll, so its park ceiling is the client floor on ack latency.
        aeron::concurrent::BackoffIdleStrategy idle(
            5, 3, std::chrono::duration<long, std::nano>(1'000),
            std::chrono::duration<long, std::nano>(100'000));
        while (!stopped_.load(std::memory_order_acquire)) idle.idle(dutyCycle(now()));
    } else if (cfg_.idleMode == IdleMode::BusySpin) {
        // Never park or yield: pin this runner to a dedicated core.
        while (!stopped_.load(std::memory_order_acquire)) {
            if (dutyCycle(now()) == 0) {
#if defined(__x86_64__) || defined(__i386__)
                __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
                __asm__ volatile("yield" ::: "memory");
#else
                __asm__ volatile("" ::: "memory");
#endif
            }
        }
    } else {
        // Balanced: spins -> yields -> parks (max 1 ms).
        aeron::concurrent::BackoffIdleStrategy idle(
            10, 20, std::chrono::duration<long, std::nano>(1'000), std::chrono::milliseconds(1));
        while (!stopped_.load(std::memory_order_acquire)) idle.idle(dutyCycle(now()));
    }
}

// ---------------------------------------------------------------------------
// sweepTimeouts() — called from the runner thread
// ---------------------------------------------------------------------------

void Client::sweepTimeouts(int64_t nowNs) {
    // Connect/ping/subscribe/define/get promise deadlines are created on app
    // threads with steady_clock, so they keep their own steady_clock sample.
    const auto now = std::chrono::steady_clock::now();

    // --- Producer delivery timeouts (duty-cycle thread; no lock — ack rings are single-owner) ---
    // Producer deadlines are on the duty-cycle clock (injected in tests): any
    // pending ProduceResponse ack older than produceTimeoutMs is failed so
    // futures/callbacks never hang.
    producerHub_.sweepTimedOut(
        std::chrono::steady_clock::time_point{std::chrono::nanoseconds{nowNs}});

    // No-progress backstop (moved from the per-cycle drain hook; the refresh it arms
    // is itself backoff-gated at >= 100 ms, so the 100 ms sweep cadence loses nothing):
    // a pending produce partition with NO known leader arms a metadata refresh.
    {
        int32_t topicId = -1, partition = -1;
        if (producerHub_.firstPendingPartition(topicId, partition) &&
            metadataCache_.leaderFor(topicId, partition) == metadata::MetadataCache::kNoLeader) {
            scheduleMetadataRefresh();
        }
    }

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
        const int64_t wallNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        const group::SendFn sendFn = [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
            return agent_.submitSend(fb, fl);
        };
        groupConsumerHub_.doWork(wallNs, snap.sessionId, sendFn);
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
    if (redirectPending()) {
        const auto snap = agent_.stateAndSessionId();
        if (snap.state == session::SessionState::Disconnected) {
            redirectPending() = false;
            producerHub_.failAll("Redirect reconnect failed: endpoint unreachable");
            subscriberHub_.failPendingSubscribes(
                "Redirect reconnect failed: endpoint unreachable");
            if (cbError_) {
                cbError_("Redirect reconnect failed: endpoint unreachable");
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

    // --- Task 10: Pending defineTopic timeouts ---
    for (auto it = pendingDefine_.begin(); it != pendingDefine_.end(); ) {
        if (now >= it->second.deadline) {
            try {
                it->second.promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("defineTopic() timed out (no DefineTopicResponse received)")));
            } catch (...) {}
            it = pendingDefine_.erase(it);
        } else {
            ++it;
        }
    }

    // --- Task 10: Pending getTopicSchema timeouts ---
    for (auto it = pendingGet_.begin(); it != pendingGet_.end(); ) {
        if (now >= it->second.deadline) {
            try {
                it->second.promise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("getTopicSchema() timed out (no TopicSchemaResponse received)")));
            } catch (...) {}
            it = pendingGet_.erase(it);
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
            if (redirectPending()) {
                redirectPending() = false;
                const int64_t newSessionId = opt->assignedSessionId;
                // E5.1: resend uses per-worker streamId stored in BatchInfo.
                // submitSend goes through the agent ring → transport_.offer(streamId, ...)
                // but the ring kMsgSend path uses the default stream.  For correctness
                // the resend needs to carry the ingressStreamId — however the ring path
                // does not forward streamId.  As a pragmatic safe fallback we use
                // stream 0 (default) for the resend after redirect; per-worker routing
                // resumes on the next fresh produce cycle via flushBatches.
                std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn =
                    [this](int32_t /*topicId*/, int32_t /*partition*/,
                           int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    };
                producerHub_.resendAllRedirected(newSessionId, sendFn);
                subscriber::SubscriberHub::SendFn subSendFn =
                    [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    };
                subscriberHub_.resendPending(newSessionId, subSendFn);
            }

            // CG-5: Trigger group consumers to send JoinGroup on connect.
            if (!groupConsumerHub_.empty()) {
                const group::SendFn gcSendFn =
                    [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
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
            // Task 1.2: include group + txn pending state so that a redirect
            // arriving while a group join or txn op is in flight is treated as
            // a genuine data-op redirect rather than dropped as a connect-time
            // redirect.  All four components are agent-thread reads (no locks).
            const bool groupActive = !groupConsumerHub_.empty();
            const bool txnPending = !txnProducers_.empty();  // duty-cycle thread
            const bool haveWork =
                producerHub_.hasInFlight() || subscriberHub_.hasPending()
                || groupActive || txnPending;
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
            int32_t rTopicId = -1, rPartition = -1;
            const bool havePart = producerHub_.partitionForCorrId(
                    opt->correlationId, rTopicId, rPartition);
            {
                if (havePart) {
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
                        currentLeaderNode() = newNodeId;
                    }
                }
                // No-progress (unknown corrId / endpoint not in broker set):
                // arm a background metadata refresh with exponential backoff so
                // the authoritative map is reconciled without storming.
                scheduleMetadataRefresh();
            }

            // Task 1.4 (multi-node redirect convergence): when per-partition routing
            // is active, a per-partition Redirect must update THAT partition's route
            // to the real leader and resend the redirected frame to the leader's own
            // session — NOT retarget the shared transport / reconnect the seed (the
            // single-node model, which churns the seed and thrashes every partition).
            // The producer stores each in-flight frame; setRoute + offerToPartition
            // route it to the correct leader (held until that session connects, then
            // flushed by the connect-triggered produceResendFn_).  This is how the
            // client converges from the stale bootstrap image to the elected leaders.
            if (havePart && rTopicId >= 0 && rPartition >= 0 &&
                agent_.multiNodeRouting() && !newEndpoint.empty()) {
                const std::string leaderChannel =
                    (newEndpoint.rfind("aeron:", 0) == 0)
                        ? newEndpoint
                        : ("aeron:udp?endpoint=" + newEndpoint);
                agent_.setRoute(rTopicId, rPartition, leaderChannel);
                std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> mnSendFn =
                    [this](int32_t t, int32_t p, int32_t sid,
                           const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.offerToPartition(t, p, sid, fb, fl);
                    };
                // Re-offer the redirected batch to the (new) leader route now; if the
                // leader session is still connecting it holds and the connect-triggered
                // resend flushes it.  retryRedirectedProduce enforces the redirect
                // budget so a genuinely unroutable partition still fails eventually.
                producerHub_.retryRedirectedProduce(opt->correlationId, mnSendFn);
                break; // handled per-partition; skip the single-transport retarget.
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
                currentLeaderChannel() = newChannel;
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
                // Coalesce: only initiate ONE reconnect per redirect episode.
                // Single-owner agent thread → a plain check-and-set (no CAS).
                if (!redirectPending()) {
                    redirectPending() = true;
                    agent_.submitReconnect();
                }
            } else {
                // Retarget failed — fail ONLY the produce that triggered this
                // redirect (identified by opt->correlationId).  This is Fix 7b:
                // a failed retarget to a dead partition-leader must not cascade
                // into failAll() and kill produces for other (alive) partitions.
                // The transport stays on the current node; the next produce
                // attempt for an alive partition will succeed on the same session.
                //
                // We still call failPendingSubscribes() and reset redirectPending_
                // so the subscribe side and reconnect-pending latch are clean.
                const std::string retargetReason =
                    "Redirect retarget failed for endpoint: " + newEndpoint;
                producerHub_.failPending(opt->correlationId, retargetReason);
                subscriberHub_.failPendingSubscribes("Redirect retarget failed");
                redirectPending() = false;
                if (cbError_) {
                    cbError_(retargetReason);
                }
            }
            break; // Redirect is handled; don't fall through to the error path.
        }

        // ---- Note A: bare NotLeader is RETRIABLE (not a terminal error) -------
        // A follower that rejects a data op but does NOT know the leader endpoint
        // returns ErrorResponse{NotLeader} with an EMPTY message (no endpoint to
        // follow — contrast with Redirect above, which carries the leader's
        // endpoint).  This is the transient "leadership just moved / not yet
        // known" case: the correct client behaviour is to REFRESH metadata and
        // RETRY the same op with backoff, NOT to fire cbError_ immediately.
        //
        // Only handle the bare form here (empty message).  A NotLeader that DOES
        // carry an endpoint is treated as a Redirect by the broker; if one ever
        // arrives here it falls through to the generic path below.
        //
        // Retry is bounded by ProducerImpl::kMaxRedirects (per-corrId redirect
        // budget shared with the Redirect resend path).  Only when that budget is
        // exhausted does retryRedirectedProduce() fail the pending ack — so
        // cbError_ is never fired for a bare NotLeader until retries run out.
        if (ec == rpc::ErrorCode::NotLeader && opt->message.empty()) {
            // Task 1.2: mirror the Redirect haveWork expansion — include
            // group + txn pending so a bare NotLeader while a group or txn op
            // is in flight is retried rather than dropped.
            {
                bool groupActiveNL = !groupConsumerHub_.empty();
                const bool txnPendingNL = !txnProducers_.empty();  // duty-cycle thread
                const bool haveWork =
                    producerHub_.hasInFlight() || subscriberHub_.hasPending()
                    || groupActiveNL || txnPendingNL;
                if (!haveWork) {
                    // No in-flight op to retry (late/duplicate NotLeader for an
                    // already-resolved corrId): arm a background refresh so the map
                    // is reconciled, then drop it.  Do NOT fire cbError_.
                    scheduleMetadataRefresh();
                    break;
                }
            }

            // Mark the affected partition (from our own in-flight request
            // context) for metadata refresh so the very next route reconciles the
            // new leader.  We do not know the new leaderNodeId (no endpoint in the
            // frame), so we cannot update the cache in place — we arm a background
            // MetadataRequest (exponential-backoff-gated) instead.
            scheduleMetadataRefresh();

            // Re-enqueue the produce that hit the stale leader.  The stored wire
            // frame is resent on the SAME session (leadership is expected to have
            // moved back or the leader to have caught up by the time the retry
            // lands; the background refresh reroutes if it has genuinely moved).
            // retryRedirectedProduce() bumps the per-corrId redirect counter and,
            // on exhausting kMaxRedirects, fails the pending ack itself.
            const auto snap = agent_.stateAndSessionId();
            (void)snap;
            std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn =
                [this](int32_t /*topicId*/, int32_t /*partition*/,
                       int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            producerHub_.retryRedirectedProduce(opt->correlationId, sendFn);

            break; // bare NotLeader handled: refreshed + retried (or budget-failed).
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
                [this](int32_t /*streamId*/, const uint8_t* fbuf, std::size_t flen) -> bool {
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
                [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
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
                [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
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
                [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
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
                [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.submitSend(fb, fl);
                };
            groupConsumerHub_.onAssign(buf, len, snap.sessionId, gcSendFn);
        }
        break;
    }

    // ---- TX-5: Transactional producer responses --------------------------------
    // ProduceResponse (tid 17) lands here only once a txn producer registered
    // (setForwardProduceResponses(true)); the producerHub_ path is the plain-call
    // ack routing in dutyCycle().  Txn producers iterate the duty-cycle-owned list.
    // Txn producers need the full buf+len+sendFn for their retry logic.

    case codec::TemplateId::ProduceResponse: {
        for (auto& tp : txnProducers_) {
            tp->onProduceResponse(buf, len, txnControlSendFn_);
        }
        break;
    }

    case codec::TemplateId::InitProducerIdResponse: {
        for (auto& tp : txnProducers_) {
            tp->onInitProducerIdResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::AddPartitionsToTxnResponse: {
        for (auto& tp : txnProducers_) {
            tp->onAddPartitionsToTxnResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::AddOffsetsToTxnResponse: {
        if (!txnProducers_.empty()) {
            const auto snap = agent_.stateAndSessionId();
            for (auto& tp : txnProducers_) {
                tp->onAddOffsetsToTxnResponse(buf, len, snap.sessionId, txnControlSendFn_);
            }
        }
        break;
    }

    case codec::TemplateId::TxnOffsetCommitResponse: {
        for (auto& tp : txnProducers_) {
            tp->onTxnOffsetCommitResponse(buf, len);
        }
        break;
    }

    case codec::TemplateId::EndTxnResponse: {
        for (auto& tp : txnProducers_) {
            tp->onEndTxnResponse(buf, len);
        }
        break;
    }

    // ---- Task 10: DefineTopicResponse (65) ----
    case codec::TemplateId::DefineTopicResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::DefineTopicResponse::decode(buf, len);
        if (!opt) break;

        std::promise<DefineTopicResult> prom;
        std::string savedName;
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = pendingDefine_.find(opt->correlationId);
            if (it != pendingDefine_.end()) {
                prom      = std::move(it->second.promise);
                savedName = std::move(it->second.topicName);
                pendingDefine_.erase(it);
                found = true;
            }
        }
        if (found) {
            DefineTopicResult result;
            result.topicId   = opt->topicId;
            result.version   = opt->version;
            result.errorCode = opt->errorCode;

            // Task 9 (CLI): on success, register (name, topicId) so listTopics()
            // can enumerate topics that were applied via this client.
            if (result.errorCode == 0 && !savedName.empty() && result.topicId >= 0) {
                std::lock_guard<std::mutex> lk(mtx_);
                // Avoid duplicates (idempotent re-apply would re-register).
                bool already = false;
                for (const auto& [n, id] : knownTopics_) {
                    if (n == savedName) { already = true; break; }
                }
                if (!already) {
                    knownTopics_.emplace_back(savedName, result.topicId);
                }
            }

            try { prom.set_value(result); } catch (...) {}
        }
        break;
    }

    // ---- Task 10: TopicSchemaResponse (67) ----
    case codec::TemplateId::TopicSchemaResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::TopicSchemaResponse::decode(buf, len);
        if (!opt) break;

        std::promise<TopicSchemaResult> prom;
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = pendingGet_.find(opt->correlationId);
            if (it != pendingGet_.end()) {
                prom  = std::move(it->second.promise);
                pendingGet_.erase(it);
                found = true;
            }
        }
        if (found) {
            TopicSchemaResult result;
            result.topicId   = opt->topicId;
            result.version   = opt->version;
            result.errorCode = opt->errorCode;
            // Copy descriptorBlob out of the Aeron buffer (view is only valid
            // for the duration of this callback — same discipline as Pong echo).
            if (opt->descriptorBlob && opt->descriptorLen > 0) {
                result.descriptorBlob.assign(
                    opt->descriptorBlob,
                    opt->descriptorBlob + opt->descriptorLen);
            }
            try { prom.set_value(std::move(result)); } catch (...) {}
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

    // Task 10: fail any in-flight schema ops.
    for (auto& [corrId, pd] : pendingDefine_) {
        try {
            pd.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
    }
    pendingDefine_.clear();

    for (auto& [corrId, pg] : pendingGet_) {
        try {
            pg.promise.set_exception(
                std::make_exception_ptr(std::runtime_error(reason)));
        } catch (...) {}
    }
    pendingGet_.clear();
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

    // Shared-nothing registration: producer() may be called from any app thread,
    // including after start().  Rather than touch the runner-owned ProducerHub list
    // directly (which would need an atomic snapshot + mutex), hand the ProducerImpl
    // to the runner thread via the agent command ring.  The runner drains
    // kMsgRegisterProducer and calls producerHub_.add() itself, so the list stays
    // single-owner (no atomic, no lock).  `impl` is copied into a heap holder whose
    // address travels the ring; the runner adopts the shared_ptr and frees the
    // holder.  Both `impl` (returned to the caller) and the hub's entry keep the
    // ProducerImpl alive.  Retry on a transiently full ring (off the hot path).
    // Bounded setup-phase submit; in Invoker mode the duty cycle runs between
    // attempts (this thread is the ring's only consumer).
    auto* holder = new std::shared_ptr<producer::ProducerImpl>(impl);
    submitRegistration_([&] { return agent_.submitRegisterProducer(holder); },
                        [&] { delete holder; });

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
            [this](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
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
        [this](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
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

    // Shared-nothing registration through the agent ring (like producer()): the
    // duty-cycle thread adopts it into txnProducers_ — no mutex, no per-cycle copy.
    auto* holder = new std::shared_ptr<producer::TransactionalProducerImpl>(impl);
    submitRegistration_([&] { return agent_.submitRegisterTxnProducer(holder); },
                        [&] { delete holder; });
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
        e.topicId        = p.topicId;
        e.partition      = p.partition;
        e.leaderId       = p.leaderId;
        e.leaderEpoch    = p.leaderEpoch;
        e.ingressStreamId = p.ingressStreamId;  // E5.1: per-worker ingress stream
        partitions.push_back(e);
    }

    metadataCache_.updateFromResponse(brokers, partitions);

    // E5.1: propagate per-worker ingressStreamIds to all registered producers.
    // Called on the agent thread (single-writer) — ProducerImpl::updateIngressRoute
    // is agent-thread-only, so no lock is needed.
    for (const auto& p : partitions) {
        if (p.ingressStreamId != 0) {
            producerHub_.updateIngressRoute(p.topicId, p.partition, p.ingressStreamId);
        }
    }

    // Task 1.3: (re)build the agent-owned partition routing table IN PLACE.  For
    // each partition with a known leader + per-worker ingressStreamId, map the
    // stream id → the leader node's ingress endpoint channel.  setRoute() drops the
    // entry when the leader resolves to the seed/default endpoint (single-node =
    // every partition on the seed ⇒ empty route table ⇒ byte-identical hot path).
    // The producer's flush→sendFn boundary then routes each record to its leader
    // session by ingressStreamId — replacing the single-transport retarget model.
    const bool mnDbg = (std::getenv("RUNE_MN_DEBUG") != nullptr);
    int mnRoutes = 0;
    for (const auto& p : partitions) {
        const int32_t leaderNode = metadataCache_.leaderFor(p.topicId, p.partition);
        const std::string ep = (leaderNode == metadata::MetadataCache::kNoLeader)
                                   ? std::string() : metadataCache_.endpointFor(leaderNode);
        if (mnDbg) {
            std::fprintf(stderr, "[MNDBG route] topic=%d part=%d ingressStream=%d leaderNode=%d ep='%s'\n",
                         p.topicId, p.partition, p.ingressStreamId, leaderNode, ep.c_str());
        }
        if (p.ingressStreamId == 0) continue;
        if (leaderNode == metadata::MetadataCache::kNoLeader) continue;
        if (ep.empty()) continue;
        const std::string leaderChannel =
            (ep.rfind("aeron:", 0) == 0) ? ep : ("aeron:udp?endpoint=" + ep);
        agent_.setRoute(p.topicId, p.partition, leaderChannel);
        ++mnRoutes;
    }
    if (mnDbg) std::fprintf(stderr, "[MNDBG route] built %d routes from %zu partitions\n",
                            mnRoutes, partitions.size());

    // read-rework I4: open a per-worker dynamic-MDC egress subscription for each
    // partition whose owner advertises one (per-worker egress active on the broker).
    // Control host = the leader broker's host; port/stream are per-partition (shared
    // across partitions on the same worker → addEgressSubscription dedups).  Records
    // then arrive on these subs and route by subscriptionId in the SubscriberHub.
    // Agent-thread only (onMetadataResponse runs on the agent thread).
    // Fallback control host: the host of the broker the client is CURRENTLY
    // connected to (currentLeaderChannel_, "aeron:udp?endpoint=<host>:<port>").
    // Used when the MetadataResponse doesn't advertise the leader's client host
    // (e.g. a single-node config with no peer client port) — the worker's MDC
    // control publication binds on that same host.
    std::string fallbackHost;
    {
        const std::string& leaderChan = currentLeaderChannel();
        const auto ep = leaderChan.find("endpoint=");
        if (ep != std::string::npos) {
            const std::size_t hs = ep + 9;
            const std::size_t colon = leaderChan.rfind(':');
            if (colon != std::string::npos && colon > hs)
                fallbackHost = leaderChan.substr(hs, colon - hs);
        }
    }
    for (const auto& p : resp.partitions) {
        if (p.egressControlPort == 0 || p.egressStreamId == 0) continue;
        std::string_view host;
        for (const auto& b : resp.brokers) {
            if (b.nodeId == p.leaderId) { host = b.host; break; }
        }
        std::string ctrlHost = host.empty() ? fallbackHost : std::string(host);
        if (ctrlHost.empty()) continue;
        std::string ctrl = "aeron:udp?control-mode=dynamic|control=" +
                           ctrlHost + ":" +
                           std::to_string(p.egressControlPort) + "|fc=min";
        agent_.addEgressSubscription(ctrl, p.egressStreamId);
    }

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

} // namespace rune::client
