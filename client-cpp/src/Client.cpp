// src/Client.cpp
//
// CC-6 — Client implementation: control ops with completion tokens + runner.
//
// Threading summary (F8c C2/C3 — no futures, no atomics, no mutex):
//   - The duty thread (the Thread-mode runner, or the Invoker owner inside
//     doWork()) owns the agent, the session, every hub, the control-op pending
//     table, the correlation-id counter and the completion-queue registry.
//   - App threads hand it commands through the agent's M2O command ring
//     (control ops, registrations, CqAdd / CqRelease, callback setters); on the
//     duty thread itself they are applied directly.
//   - Results travel back as completions: into a Completions ring (duty thread
//     = its single producer) or to a CompletionHandler called on the duty
//     thread.  The seed session state reaches the owner through a StatusRing.
//   - close() closes the command ring (the closed bit is in its tail word:
//     linearizable with every submit), joins the runner, drains what was
//     accepted, completes everything Closed and hands every registered
//     Completions back (producer-close).

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

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace rune::client {

namespace {
// F8c fix round 1 (I1): the Client this thread is closing (set right after the
// runner join, cleared when closeImpl_ returns).  Per-THREAD state — never
// shared: it lets a Completions released by user code that close() itself runs
// (a callable destroyed by the final drain, a callback during teardown)
// recognise that its thread already owns every duty structure, and release
// directly instead of waiting for a hand-back only this thread would perform.
thread_local const Client* tClosing = nullptr;
// F8c fix round 1 (m6): a distinct token per *AndWait call on THIS thread (two
// threads may draw the same value; each call has its own fresh queue, and the
// I3 quarantine makes cross-queue delivery impossible, so this is hygiene).
thread_local Token tWaitToken = 0;
} // namespace

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
    // T15 D1: driverOnDutyThread also selects the invoker driver under
    // RunnerMode::Thread — the runner pumps driver + conductor in its duty cycle
    // (ClientAgent::doWork -> transport_.dutyCycle()): one thread per shard.
    acfg.transport.driverMode =
        (c.runnerMode == RunnerMode::Invoker || c.driverOnDutyThread)
            ? transport::DriverMode::Invoker
            : transport::DriverMode::Thread;
    // T15 R1: never give up once Live (shard clients); false = today's behaviour.
    acfg.session.reconnectForeverAfterLive = c.reconnectForeverAfterLive;
    acfg.transport.clientLivenessTimeoutNs = c.aeronClientLivenessTimeoutNs;
    // F12: malformed env values are reported (once per Client construction —
    // this runs once per Client, never per cycle), not silently dropped.
    for (const std::string& w : transport::applyEnvOverrides(acfg.transport)) {
        std::fprintf(stderr, "%s\n", w.c_str());
    }

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
        (void)routeBufferedAcks_();  // acks earlier in this poll first: never resend an acked batch
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

    // F8a: subscriber / group-consumer commands from app threads, applied on the
    // duty thread (or by close()'s final drain) by the hub that owns the state.
    agent_.setHubCommandFn([this](const detail::HubCommand& cmd) { applyHubCommand_(cmd); });
    // F8c C3: control ops (connect / ping / DDL / listTopics) from app threads.
    agent_.setControlOpFn([this](const uint8_t* rec, std::size_t len) { applyControlOpRecord_(rec, len); });
    pendingCtl_.reserve(64);
    ctlScratch_.reserve(512);
    cqSlots_.reserve(kMaxCqs);
    cqFree_.assign(kMaxCqs, 0);  // FIFO ring of released slots (I3)
    groupConsumerHub_.setSessionConnected(false);  // fix3: not connected until the first connect
    subscriberHub_.setSessionConnected(false);
    // F8a fix2: Unsubscribe / OffsetCommit frames go straight to the transport
    // from the duty thread (retried every duty cycle on refusal), never via the
    // command ring.
    subscriberHub_.setDirectSend(&Client::directSendTrampoline_, this);
    groupConsumerHub_.setDirectSend(&Client::directSendTrampoline_, this);

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
    ownerThread_  = std::this_thread::get_id();
    dutyThreadId_ = ownerThread_;
    nextSweepNs_  = now() + kSweepIntervalMs * 1'000'000LL;
    if (cfg_.runnerMode == RunnerMode::Thread) {
        // dutyThreadId_ is written BEFORE the runner runs its first duty cycle: the
        // runner blocks on `go` until the write is done, and set_value() → wait()
        // is a happens-before edge, so the runner's reads (submitRegistration_ /
        // ~Client from a callback) never race the write.  App threads see it via
        // the Client's construction (they cannot hold the Client earlier).
        std::promise<void> go;
        std::future<void>  goF = go.get_future();
        runnerThread_ = std::thread([this, goF = std::move(goF)]() mutable {
            goF.wait();
            runLoop();
        });
        dutyThreadId_ = runnerThread_.get_id();
        go.set_value();
    }
}

// ---------------------------------------------------------------------------
// Destruction
// ---------------------------------------------------------------------------

Client::~Client() {
    // Never throws (close()'s contract checks would std::terminate here).
    //  - Destroyed from inside its own duty-cycle callback (owner in Invoker mode,
    //    runner in Thread mode): every frame above us still uses `this` — a silent
    //    use-after-free.  Programming error: fail loud.
    //  - Invoker mode, destroyed OFF the owner thread: legitimate ("construct and
    //    drive on a worker, join it, destroy on main") — destruction ends the
    //    lifetime and the app guarantees no concurrent doWork(), so tear down.
    if (std::this_thread::get_id() == dutyThreadId_ && inDutyCycle_) {
        std::fprintf(stderr,
                     "rune-client: FATAL: Client destroyed from inside its own duty-cycle "
                     "callback (use-after-free); aborting\n");
        std::fflush(stderr);
        std::abort();
    }
    closeImpl_();
}

// ---------------------------------------------------------------------------
// close() — idempotent shutdown
// ---------------------------------------------------------------------------

void Client::close() {
    if (cfg_.runnerMode == RunnerMode::Invoker) {
        // Cold-path contract checks, enforced in release builds too.
        if (std::this_thread::get_id() != ownerThread_) [[unlikely]] {
            throw std::logic_error("Client::close(): Invoker mode — call from the owner thread");
        }
        if (inDutyCycle_) [[unlikely]] {
            throw std::logic_error(
                "Client::close(): called from a duty-cycle callback (would tear the agent "
                "down under its own poll)");
        }
    }
    closeImpl_();
}

void Client::closeImpl_() {
    // F8c C2: closing the agent command ring IS the stop signal (no stopped_
    // atomic).  The closed bit lives in the ring's tail word, so every submit
    // is linearized against this one RMW: a command accepted before it is
    // drained below; a command after it is refused (Closed) and never written
    // — no ClosedAfterSubmit window, no accepted leak.  Only the first caller
    // (the one that closed the ring) does the work.
    if (!agent_.closeCommandRing()) return;

    // Join the runner thread (it loops until it sees the closed ring).
    // CONTRACT: close() must NOT be called from within a Client callback
    // (onConnected/onDisconnected/onError run on the runner thread). Doing so
    // would self-join the runner (std::system_error) and re-enter teardown on the
    // runner's own doWork stack. Callbacks that want to close should hand off to
    // another thread. (Documented on the callback setters in Client.h.)
    if (runnerThread_.joinable()) {
        runnerThread_.join();
    }
    // F8c C0: no duty thread runs any more — this thread is the single pumper
    // for whatever the teardown below drives (it is already the owner in
    // Invoker mode, or the destroying thread in the off-owner-destroy pattern).
    agent_.bindDutyThread();
    // From here this thread owns every duty-side structure (the join / the
    // Invoker contract orders it after the last duty cycle).
    closing_ = true;
    struct ClosingMark {  // I1: this thread is the closer until closeImpl_ returns
        const Client* prev;
        explicit ClosingMark(const Client* c) : prev(tClosing) { tClosing = c; }
        ~ClosingMark() { tClosing = prev; }
    } closingMark(this);

    // Single-threaded from here.  Drain the command ring up to its (now frozen)
    // tail: registrations are adopted (their holders freed, the failAll below
    // reaches them), hub commands applied, completion queues added / released,
    // and every accepted control op completes — Closed (closing_).
    (void)agent_.drainRegistrationsForClose();
    // I3': the drain read up to the frozen tail (or was abandoned — nothing
    // behind the orphan is ever applied): every deferred release is safe now,
    // and must happen before the registry hand-back (the owner gave it up).
    (void)applyDeferredReleases_(true);

    // fix1 C2/I3: mark every producer's ring closed NOW — the Client (and its
    // pump-hook contexts, each `this`) is still fully valid, and this runs
    // before the transport is torn down. A Producer whose shared_ptr the app
    // kept past this point then gets SendResult::Closed, never touching the
    // (by-then-dangling) pump hook: no use-after-free, no silent accept.
    producerHub_.closeAllRings();

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
    // fix5: failAll() also markClosed()s each one (the txn analogue of
    // closeAllRings() above), so later operations fail at once, never hang.
    for (auto& tp : txnProducers_) {
        tp->failAll("Client closed");
    }

    // M2 FIX: clear the seed session's redirect latch on close so it does not
    // latch.  Safe as a plain write: the runner thread was joined above, so this
    // runs single-threaded (no concurrent agent-thread access) — Task 1.1.
    redirectPending() = false;

    // F8c (spec §4 order): after producers, txn producers and group consumers
    // have failed their ops, the control tables complete Closed, and only then
    // is every registered Completions handed back (producer-closed: its owner
    // sees every Closed record, then finished()) — so no completion delivered
    // during close is dropped.  A queue released by user code that this close
    // runs (above) was freed directly (tClosing, I1), never waited on.
    failAllControl_(OpStatus::Closed);
    closeCqRegistry_();
    // I2: nothing reads the command ring any more — even if the drain above
    // was abandoned behind an orphaned claim.  A Completions constructor still
    // waiting for a CqAdd stranded there sees this and is born finished.
    agent_.finishCommandRingConsumer();

    // F8c C3: handler-target completions deferred during the close (Closed),
    // now that every queue is handed back.
    for (DeferredHandler& d : closeDeferred_) {
        d.handler(d.c, std::span<const uint8_t>(d.payload));
    }
    closeDeferred_.clear();

    // Fire onDisconnected once (closeImpl_'s body runs once: the first closer).
    if (cbDisconnected_) {
        invokeUserCallback_("onDisconnected", [&] { cbDisconnected_(); });
    }
}

// ---------------------------------------------------------------------------
// Callback registration
// ---------------------------------------------------------------------------

// F8c C3: a callback setter is a command for the duty thread (no
// connectCalled_ flag, no setup-phase race).  On the Invoker owner OUTSIDE a
// duty cycle the callback is installed directly; anywhere else it goes through
// the ring (never replaced while it may be running: a duty-cycle callback that
// sets a callback queues the change for the next drain).  After close() the
// setter is a no-op.
template <class Fn>
void Client::setCallback_(int which, Fn cb) {
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && onDuty && !inDutyCycle_) {
        if (agent_.commandRingClosed()) return;
        auto* holder = new Fn(std::move(cb));
        applySetCallback_(which, holder);
        return;
    }
    if (agent_.commandRingClosed()) return;
    auto holder = std::make_unique<Fn>(std::move(cb));
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::ClientSetCallback;
    cmd.handle = reinterpret_cast<uint64_t>(holder.get());
    cmd.a      = which;
    try {
        if (submitRegistration_([&] { return agent_.submitHubCommand(cmd); }, [] {})) {
            (void)holder.release();  // the duty thread owns it now
        }
    } catch (const std::logic_error&) {
        if (!agent_.commandRingClosed()) throw;  // ring full inside a callback
    }
}

void Client::applySetCallback_(int which, void* holder) {
    switch (which) {
    case 0: {
        std::unique_ptr<std::function<void(int64_t)>> h(static_cast<std::function<void(int64_t)>*>(holder));
        cbConnected_ = std::move(*h);
        break;
    }
    case 1: {
        std::unique_ptr<std::function<void()>> h(static_cast<std::function<void()>*>(holder));
        cbDisconnected_ = std::move(*h);
        break;
    }
    default: {
        std::unique_ptr<std::function<void(const std::string&)>> h(
            static_cast<std::function<void(const std::string&)>*>(holder));
        cbError_ = std::move(*h);
        break;
    }
    }
}

void Client::onConnected(std::function<void(int64_t)> cb) { setCallback_(0, std::move(cb)); }
void Client::onDisconnected(std::function<void()> cb)     { setCallback_(1, std::move(cb)); }
void Client::onError(std::function<void(const std::string&)> cb) { setCallback_(2, std::move(cb)); }

// ---------------------------------------------------------------------------
// State observer
// ---------------------------------------------------------------------------

session::SessionState Client::state() const {
    const auto me = std::this_thread::get_id();
    // The duty thread itself (Invoker owner, a Thread-mode runner callback).
    if (me == dutyThreadId_) return agent_.sessionState();
    if (me != ownerThread_) {
        throw std::logic_error(
            "Client::state(): call from the owner (constructing) thread or a duty-cycle callback");
    }
    // Thread mode, owner thread: the latest status the duty thread published.
    (void)seedStatus_.drainLatest(seedStatusCache_);
    return static_cast<session::SessionState>(seedStatusCache_.state);
}

void Client::publishSeedStatus_(session::SessionState st, int64_t sid) noexcept {
    if (cfg_.runnerMode != RunnerMode::Thread) return;  // Invoker: the owner reads it directly
    seedStatusLast_.state     = static_cast<uint8_t>(st);
    seedStatusLast_.sessionId = sid;
    seedStatusDirty_ = !seedStatus_.tryPublish(seedStatusLast_);
}

uint64_t Client::testCallbackExceptionCount() const noexcept {
    return callbackExceptions_
         + subscriberHub_.testCallbackExceptionCount()
         + groupConsumerHub_.testCallbackExceptionCount();
}

// ---------------------------------------------------------------------------
// F8c C2: completion-queue registry (§2.2, §3.3)
// ---------------------------------------------------------------------------

void Client::registerCq_(Completions& q) {
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && !onDuty) {
        throw std::logic_error("Completions: Invoker mode — construct on the owner thread");
    }
    q.client_ = this;
    if (agent_.commandRingClosed()) {  // born finished: no duty side will ever own it
        (void)q.ring_->producerCloseAndDetach();  // the owner's detach then frees it
        return;
    }
    // The registration record is an ordinary completion: reserve its credit.
    const uint32_t rec = detail::CompletionRing::completionRecordBytes(0);
    (void)q.reserve(rec);  // a fresh queue on its owner thread: always Accepted
    if (onDuty) {
        applyCqAdd_(q.ring_);  // the duty thread itself (Invoker owner / a runner callback)
    } else {
        detail::HubCommand cmd;
        cmd.op     = detail::HubCommand::Op::CqAdd;
        cmd.handle = reinterpret_cast<uint64_t>(q.ring_);
        aeron::concurrent::BackoffIdleStrategy idle(
            10, 20, std::chrono::duration<long, std::nano>(1'000),
            std::chrono::duration<long, std::nano>(100'000));
        for (;;) {
            const auto r = agent_.submitHubCommand(cmd);
            if (r == agent::ClientAgent::Submit::Accepted) break;
            if (r == agent::ClientAgent::Submit::Closed) {  // never written: still ours
                q.unreserve(rec);
                (void)q.ring_->producerCloseAndDetach();
                return;
            }
            idle.idle(0);  // full: the duty thread drains it (progress-based, no time bound)
        }
    }
    // One duty round trip: the duty side answers in the queue's own ring.  The
    // wait ends on ring words only: the registration record, the hand-back
    // (producer-closed), or — I2 — the command ring's consumer finished with
    // the CqAdd never read (an abandoned close drain).  Never a time bound.
    int64_t answer = -2;
    auto onAnswer = [&](const Completion& c, std::span<const uint8_t>) { answer = c.value; };
    DefaultWaitIdle idle;
    Completions::Wait w = Completions::Wait::TimedOut;
    for (;;) {
        w = q.waitFor(Completions::kRegistrationToken, std::chrono::nanoseconds(0), onAnswer);
        if (w != Completions::Wait::TimedOut) break;
        if (agent_.commandRingConsumerFinished()) {
            // Everything close() delivered happens-before that word: one more
            // pass sees the answer if the CqAdd was ever applied.
            w = q.waitFor(Completions::kRegistrationToken, std::chrono::nanoseconds(0), onAnswer);
            break;
        }
        idle.idle(0);
    }
    if (w == Completions::Wait::Done && answer >= 0) {
        q.cqRef_ = static_cast<CqRef>(answer);
        return;
    }
    if (w == Completions::Wait::Done && answer == -1) {
        q.client_ = nullptr;  // never adopted: the destructor frees it locally
        throw std::runtime_error("Completions: the Client's completion-queue registry is full (" +
                                 std::to_string(kMaxCqs) + " queues)");
    }
    // Never registered: the Client closed and handed it back (NotPending), or
    // its CqAdd was stranded by an abandoned close drain (TimedOut).  Born
    // finished: the registration credit returns, no duty side owns the ring.
    q.unreserve(rec);
    // NotPending: the closer already handed it back (producer closed AND
    // detached).  TimedOut + consumer-finished: nobody ever adopted it.
    if (!q.ring_->producerClosed()) (void)q.ring_->producerCloseAndDetach();
}

void Client::releaseCq_(Completions& q) noexcept {
    detail::CompletionRing* ring = q.ring_;
    const CqRef             ref  = q.cqRef_;
    // I1: on the thread running this Client's close() — its drain already
    // read up to the frozen tail (or was abandoned: nothing behind the orphan
    // is ever applied), so no op of this queue is left unapplied.
    if (tClosing == this) {
        applyCqRelease_(ref, ring);
        return;
    }
    if (std::this_thread::get_id() == dutyThreadId_) {
        // Thread-mode runner: every op it issues is applied directly.
        if (cfg_.runnerMode == RunnerMode::Thread) {
            applyCqRelease_(ref, ring);
            return;
        }
        // I3' (fix round 3): the Invoker owner, inside or between cycles.  Its
        // ops issued between cycles may still sit in the command ring (past a
        // bounded drain, or behind a Retry head).  The queue records the ring
        // tail after its LAST ring-submitted op (owner-thread state, written
        // only by this thread): release directly once the drain has passed
        // that fence, otherwise defer the release until it does.  Duty-side
        // state only; nobody waits.
        const std::int64_t fence = q.issueFence_;
        if (agent_.commandRingHead() >= fence) {
            applyCqRelease_(ref, ring);
        } else {
            deferredReleases_.push_back(DeferredRelease{ring, fence, ref});
        }
        return;
    }
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::CqRelease;
    cmd.handle = reinterpret_cast<uint64_t>(ring);
    cmd.a      = ref;
    aeron::concurrent::BackoffIdleStrategy idle(
        10, 20, std::chrono::duration<long, std::nano>(1'000),
        std::chrono::duration<long, std::nano>(100'000));
    for (;;) {
        const auto r = agent_.submitHubCommand(cmd);
        if (r == agent::ClientAgent::Submit::Accepted) return;  // §3.3 (a): the duty side frees it
        if (r == agent::ClientAgent::Submit::Closed) break;
        idle.idle(0);  // full: another thread (the duty thread) drains it — progress-based
    }
    // §3.3 (b) + I5: close() is in progress on another thread.  Never wait for
    // its hand-back (the closer may be running user code blocked on THIS
    // thread): detach the consumer side; if the closer already handed the
    // ring back (producer detached), free it here, else the closer frees it.
    if (ring->consumerDetach()) delete ring;
}

int Client::applyDeferredReleases_(bool all) noexcept {
    if (deferredReleases_.empty()) return 0;
    const std::int64_t head = agent_.commandRingHead();
    int n = 0;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < deferredReleases_.size(); ++i) {
        const DeferredRelease d = deferredReleases_[i];
        if (all || head >= d.fence) {
            applyCqRelease_(d.ref, d.ring);  // every op it fenced has been applied (counted)
            ++n;
        } else {
            deferredReleases_[keep++] = d;
        }
    }
    deferredReleases_.resize(keep);
    return n;
}

void Client::applyCqAdd_(detail::CompletionRing* ring) {
    Completion reg;
    reg.token         = Completions::kRegistrationToken;
    reg.reservedBytes = detail::CompletionRing::completionRecordBytes(0);
    // I3 (fix round 2): a released slot returns to the free ring only once no
    // pending op targets it (CqSlot::targets == 0 — quarantine), so a stale op
    // can never match a recycled slot: correctness does not rest on the
    // generation width.  Fresh slots first, then the oldest freed (FIFO).
    uint32_t idx = 0;
    if (cqSlots_.size() < kMaxCqs) {
        idx = static_cast<uint32_t>(cqSlots_.size());
        cqSlots_.push_back({});
    } else if (cqFreeCount_ != 0) {
        idx = cqFree_[cqFreeHead_];
        cqFreeHead_ = (cqFreeHead_ + 1) & (kMaxCqs - 1);
        --cqFreeCount_;
    } else {
        reg.value = -1;  // full: not adopted — the queue keeps (and frees) its ring
        (void)ring->deliver(reg, {});
        return;
    }
    cqSlots_[idx].ring = ring;
    reg.value = makeCqRef(idx, cqSlots_[idx].gen);
    (void)ring->deliver(reg, {});
}

void Client::applyCqRelease_(CqRef ref, detail::CompletionRing* ring) {
    const uint32_t idx = cqRefIndex(ref);
    if (ref != kNoCqRef && idx < cqSlots_.size() && cqSlots_[idx].ring == ring &&
        cqSlots_[idx].gen == cqRefGen(ref)) {
        CqSlot& slot = cqSlots_[idx];
        slot.ring = nullptr;
        slot.gen  = static_cast<uint8_t>((slot.gen + 1) & kCqRefGenMask);
        if (slot.targets == 0) {
            freeCqSlot_(idx);
        } else {
            slot.quarantined = true;  // recycled when its last pending op completes
        }
    }
    // An accepted release always transfers the ring to this side (§3.3 (a)).
    delete ring;
}

void Client::pinCqSlot_(DutyTarget& t) noexcept {
    if (t.handler.fn != nullptr || t.cqRef == kNoCqRef) return;
    const uint32_t idx = cqRefIndex(t.cqRef);
    if (idx >= cqSlots_.size() || cqSlots_[idx].ring == nullptr || cqSlots_[idx].gen != cqRefGen(t.cqRef)) {
        return;  // already released: its completion is dropped (nothing to pin)
    }
    ++cqSlots_[idx].targets;
    t.counted = true;
}

void Client::freeCqSlot_(uint32_t idx) noexcept {
    cqFree_[(cqFreeHead_ + cqFreeCount_) & (kMaxCqs - 1)] = static_cast<uint16_t>(idx);
    ++cqFreeCount_;
}

void Client::closeCqRegistry_() {
    for (std::size_t i = 0; i < cqSlots_.size(); ++i) {
        CqSlot& slot = cqSlots_[i];
        if (slot.ring == nullptr) continue;
        detail::CompletionRing* ring = slot.ring;
        slot.ring = nullptr;
        slot.gen  = static_cast<uint8_t>((slot.gen + 1) & kCqRefGenMask);
        // The LAST access, I5 handshake: if the owner already detached (its
        // release found us closing) we free it; otherwise the owner does.
        if (ring->producerCloseAndDetach()) delete ring;
    }
}

std::size_t Client::testRegisteredCqs() const noexcept {
    std::size_t n = 0;
    for (const CqSlot& slot : cqSlots_) n += slot.ring != nullptr ? 1u : 0u;
    return n;
}

void Client::testDeliverToCqRef(CqRef ref, Token token, uint32_t reservedBytes) {
    DutyTarget t;
    t.cqRef         = ref;
    t.reservedBytes = reservedBytes;
    Completion c;
    c.token = token;
    c.op    = OpKind::Ping;
    deliver_(t, c, {});
}

void Client::deliver_(const DutyTarget& t, Completion c, std::span<const uint8_t> payload,
                      detail::CompletionRing::Overflow ov) {
    if (t.handler.fn != nullptr) {
        if (closing_) {
            // close(): after every queue is handed back (see closeImpl_).
            closeDeferred_.push_back(DeferredHandler{t.handler, c, {payload.begin(), payload.end()}});
            return;
        }
        c.payloadLen = static_cast<uint32_t>(payload.size());
        t.handler(c, payload);  // noexcept by type
        return;
    }
    c.reservedBytes = t.reservedBytes;
    const uint32_t idx = cqRefIndex(t.cqRef);
    if (t.cqRef != kNoCqRef && idx < cqSlots_.size() && cqSlots_[idx].ring != nullptr &&
        cqSlots_[idx].gen == cqRefGen(t.cqRef)) {
        (void)cqSlots_[idx].ring->deliver(c, payload, ov);
    } else {
        ++staleCqDrops_;  // released slot (quarantined, never reused yet): dropped
    }
    if (t.counted && idx < cqSlots_.size()) {  // I3: this op no longer pins its slot
        CqSlot& slot = cqSlots_[idx];
        if (--slot.targets == 0 && slot.quarantined) {
            slot.quarantined = false;
            freeCqSlot_(idx);
        }
    }
}

// ---------------------------------------------------------------------------
// F8c C3: control ops — app-thread issue path
// ---------------------------------------------------------------------------

OpResult Client::issueControl_(OpKind op, CompletionTarget target, Token token,
                               std::span<const uint8_t> frame, uint32_t payloadHint) {
    if (token > kMaxToken) return OpResult::InvalidArgument;
    if (target.cq == nullptr && target.handler.fn == nullptr) return OpResult::InvalidArgument;
    if (frame.size() > agent_.maxFrameLength()) return OpResult::InvalidArgument;
    // Liveness: a record the command ring can never carry is not "full" (a
    // retry would loop forever) — it is rejected here, synchronously.
    if (sizeof(ControlOpHeader) + frame.size() > agent_.commandMsgLimit()) return OpResult::InvalidArgument;
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && !onDuty) return OpResult::WrongThread;

    ControlOpHeader h;
    h.token = token;
    h.op    = op;
    Completions* q = target.cq;
    if (q != nullptr) {
        if (q->client_ != this) return OpResult::InvalidArgument;
        if (!q->onOwnerThread()) return OpResult::WrongThread;
        const uint32_t r  = q->reservationFor(op, payloadHint);
        const OpResult rr = q->reserve(r);
        if (rr != OpResult::Accepted) return rr;
        h.reservedBytes = r;
        h.cqRef         = q->cqRef_;
    } else {
        h.handler = target.handler;
    }
    auto unreserve = [&] { if (q != nullptr) q->unreserve(h.reservedBytes); };

    // Inside a duty cycle (a callback on the duty thread): the state is ours —
    // apply now.  Everywhere else (an app thread, the Invoker owner between
    // doWork() calls) it is a command, processed in ring order by the duty
    // thread's next drain.
    if (onDuty && inDutyCycle_) {
        if (agent_.commandRingClosed()) {
            unreserve();
            return OpResult::Closed;
        }
        applyControlOp_(h, frame);
        return OpResult::Accepted;
    }
    switch (agent_.submitControlOp(&h, sizeof h, frame)) {
    case agent::ClientAgent::Submit::Accepted:
        // I3': the queue's ring fence — every op of it lies before this tail.
        if (q != nullptr) q->issueFence_ = agent_.commandRingTail();
        return OpResult::Accepted;
    case agent::ClientAgent::Submit::Full:     unreserve(); return OpResult::BackPressured;
    case agent::ClientAgent::Submit::Closed:   break;
    }
    unreserve();
    return OpResult::Closed;
}

OpResult Client::connect(CompletionTarget target, Token token) {
    return issueControl_(OpKind::Connect, target, token, {}, 0);
}

OpResult Client::ping(std::span<const uint8_t> payload, CompletionTarget target, Token token) {
    // Encoded here (pure); the duty thread stamps sessionId + correlationId.
    std::array<uint8_t, 512> small{};
    std::vector<uint8_t>     big;
    uint8_t*                 buf = small.data();
    const std::size_t        cap = 26u + payload.size() + 32u;
    if (cap > small.size()) {
        big.resize(cap);
        buf = big.data();
    }
    const std::string_view echo(reinterpret_cast<const char*>(payload.data()), payload.size());
    const std::size_t n = codec::encodePing(buf, cap, echo, /*sessionId=*/0, /*corrId=*/0);
    if (n == 0) return OpResult::InvalidArgument;
    return issueControl_(OpKind::Ping, target, token, std::span<const uint8_t>(buf, n),
                         static_cast<uint32_t>(payload.size()));
}

OpResult Client::defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                             bool validate, std::span<const uint8_t> descriptorBlob,
                             CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::DefineTopic msg;
    msg.sessionId      = 0;
    msg.correlationId  = 0;
    msg.topicName      = name;
    msg.partitionCount = partitions;
    msg.keyField       = keyField;
    msg.validate       = validate;
    msg.descriptorBlob = descriptorBlob.data();
    msg.descriptorLen  = static_cast<uint32_t>(descriptorBlob.size());
    std::vector<uint8_t> frame(msg.encodedSize() + 32u);
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueControl_(OpKind::DefineTopic, target, token, std::span<const uint8_t>(frame.data(), n), 0);
}

OpResult Client::getTopicSchema(std::string_view name, CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::GetTopicSchema msg;
    msg.sessionId     = 0;
    msg.correlationId = 0;
    msg.topicName     = name;
    std::vector<uint8_t> frame(msg.encodedSize() + 32u);
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueControl_(OpKind::GetTopicSchema, target, token, std::span<const uint8_t>(frame.data(), n), 0);
}

OpResult Client::listTopics(CompletionTarget target, Token token) {
    return issueControl_(OpKind::ListTopics, target, token, {}, 0);
}

std::vector<std::pair<std::string, int32_t>> Client::decodeTopicList(std::span<const uint8_t> p) {
    std::vector<std::pair<std::string, int32_t>> out;
    std::size_t at = 0;
    auto need = [&](std::size_t n) { return at + n <= p.size(); };
    if (!need(4)) return out;
    uint32_t count = 0;
    std::memcpy(&count, p.data(), 4);
    at = 4;
    for (uint32_t i = 0; i < count && need(6); ++i) {
        int32_t  id  = 0;
        uint16_t len = 0;
        std::memcpy(&id, p.data() + at, 4);
        std::memcpy(&len, p.data() + at + 4, 2);
        at += 6;
        if (!need(len)) break;
        out.emplace_back(std::string(reinterpret_cast<const char*>(p.data() + at), len), id);
        at += len;
    }
    return out;
}

// ---------------------------------------------------------------------------
// F8c C3: control ops — duty side (pending table, completion, timeouts)
// ---------------------------------------------------------------------------

void Client::applyControlOpRecord_(const uint8_t* rec, std::size_t len) {
    if (len < sizeof(ControlOpHeader)) return;
    ControlOpHeader h;
    std::memcpy(&h, rec, sizeof h);
    applyControlOp_(h, std::span<const uint8_t>(rec + sizeof h, len - sizeof h));
}

void Client::applyControlOp_(const ControlOpHeader& h, std::span<const uint8_t> frame) {
    PendingCtl p;
    p.token                = h.token;
    p.op                   = h.op;
    p.target.handler       = h.handler;
    p.target.reservedBytes = h.reservedBytes;
    p.target.cqRef         = h.cqRef;
    pinCqSlot_(p.target);  // I3: the slot stays out of reuse until this op completes
    if (closing_) {  // close()'s final drain: an accepted op completes exactly once
        completeCtl_(p, OpStatus::Closed);
        return;
    }
    auto& machine = agent_.seedSession().machine;
    const int64_t nowNs = now();

    if (h.op == OpKind::Connect) {
        if (hasPendingConnect_) {  // the older one is superseded (exactly once)
            PendingCtl old = std::move(pendingConnect_);
            hasPendingConnect_ = false;
            completeCtl_(old, OpStatus::Superseded);
        }
        if (machine.state() == session::SessionState::Connected) {
            completeCtl_(p, OpStatus::Ok, machine.sessionId());
            return;
        }
        p.deadlineNs = nowNs + cfg_.connectTimeoutMs * 1'000'000LL;
        pendingConnect_    = std::move(p);
        hasPendingConnect_ = true;
        machine.connect(nowNs);
        return;
    }
    if (h.op == OpKind::ListTopics) {
        ctlScratch_.assign(4, 0);
        const auto count = static_cast<uint32_t>(knownTopics_.size());
        std::memcpy(ctlScratch_.data(), &count, 4);
        for (const auto& [name, id] : knownTopics_) {
            const auto len = static_cast<uint16_t>(std::min<std::size_t>(name.size(), 0xFFFF));
            uint8_t rec[6];
            std::memcpy(rec, &id, 4);
            std::memcpy(rec + 4, &len, 2);
            ctlScratch_.insert(ctlScratch_.end(), rec, rec + 6);
            ctlScratch_.insert(ctlScratch_.end(), name.begin(), name.begin() + len);
        }
        completeCtl_(p, OpStatus::Ok, count, 0, 0, std::span<const uint8_t>(ctlScratch_),
                     detail::CompletionRing::Overflow::TooLarge);
        return;
    }

    // Ping / DefineTopic / GetTopicSchema: decided HERE, on the duty thread.
    if (machine.state() != session::SessionState::Connected) {
        completeCtl_(p, OpStatus::NotConnected);
        return;
    }
    if (frame.size() < rune::protocol::client::MessageHeader::ENCODED_SIZE) {
        completeCtl_(p, OpStatus::NotConnected);
        return;
    }
    namespace rpc = rune::protocol::client;
    p.corrId     = nextCorrId_++;
    p.deadlineNs = nowNs + cfg_.pingTimeoutMs * 1'000'000LL;
    if (h.op == OpKind::DefineTopic) {
        if (auto d = rpc::DefineTopic::decode(frame.data(), frame.size())) p.topicName = std::string(d->topicName);
    }
    ctlScratch_.assign(frame.begin(), frame.end());
    rpc::detail::putLE64(ctlScratch_.data(), 8, machine.sessionId());
    rpc::detail::putLE64(ctlScratch_.data(), 16, p.corrId);
    switch (agent_.offerDefault(ctlScratch_.data(), ctlScratch_.size())) {
    case transport::AeronTransport::OfferResult::Accepted:
        break;
    case transport::AeronTransport::OfferResult::Retry:  // re-offered every duty cycle, in order
        p.unsentFrame = ctlScratch_;
        ++unsentCtl_;
        break;
    case transport::AeronTransport::OfferResult::Fatal:  // the transport can never carry it
        completeCtl_(p, OpStatus::NotConnected);
        return;
    }
    pendingCtl_.push_back(std::move(p));
}

void Client::completeCtl_(PendingCtl& p, OpStatus st, int64_t value, int64_t aux, uint16_t errorCode,
                          std::span<const uint8_t> payload, detail::CompletionRing::Overflow ov) {
    Completion c;
    c.token     = p.token;
    c.value     = value;
    c.aux       = aux;
    c.op        = p.op;
    c.status    = st;
    c.errorCode = errorCode;
    deliver_(p.target, c, payload, ov);
}

Client::PendingCtl Client::takeCtl_(std::size_t i) {
    // Ordered erase (m3): unsent frames keep their order; the table is small.
    PendingCtl p = std::move(pendingCtl_[i]);
    pendingCtl_.erase(pendingCtl_.begin() + static_cast<std::ptrdiff_t>(i));
    if (!p.unsentFrame.empty()) --unsentCtl_;
    return p;
}

std::size_t Client::findCtl_(int64_t corrId) const noexcept {
    for (std::size_t i = 0; i < pendingCtl_.size(); ++i) {
        if (pendingCtl_[i].corrId == corrId) return i;
    }
    return pendingCtl_.size();
}

void Client::completeConnect_(OpStatus st, int64_t value, uint16_t errorCode, std::string_view text) {
    if (!hasPendingConnect_) return;
    PendingCtl p = std::move(pendingConnect_);
    hasPendingConnect_ = false;
    completeCtl_(p, st, value, 0, errorCode,
                 std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(text.data()), text.size()));
}

int Client::retryUnsentControl_() {
    const auto& m = agent_.seedSession().machine;
    int work = 0;
    for (std::size_t i = 0; i < pendingCtl_.size() && unsentCtl_ != 0;) {
        PendingCtl& p = pendingCtl_[i];
        if (p.unsentFrame.empty()) { ++i; continue; }
        const int64_t frameSid = rune::protocol::client::detail::getLE64(p.unsentFrame.data(), 8);
        if (m.state() != session::SessionState::Connected || frameSid != m.sessionId()) {
            PendingCtl dead = takeCtl_(i);  // its session is gone: no answer possible
            completeCtl_(dead, OpStatus::SessionLost);
            ++work;
            continue;
        }
        switch (agent_.offerDefault(p.unsentFrame.data(), p.unsentFrame.size())) {
        case transport::AeronTransport::OfferResult::Accepted:
            p.unsentFrame.clear();
            p.unsentFrame.shrink_to_fit();
            --unsentCtl_;
            ++work;
            ++i;
            break;
        case transport::AeronTransport::OfferResult::Retry:
            return work;  // still refused: keep the order, retry next cycle
        case transport::AeronTransport::OfferResult::Fatal: {
            PendingCtl dead = takeCtl_(i);
            completeCtl_(dead, OpStatus::NotConnected);
            ++work;
            break;
        }
        }
    }
    return work;
}

void Client::sweepControl_(int64_t nowNs) {
    if (hasPendingConnect_ && nowNs >= pendingConnect_.deadlineNs) {
        completeConnect_(OpStatus::Timeout, 0, 0, {});
        if (cbError_) {
            invokeUserCallback_("onError", [&] { cbError_("connect() timed out"); });
        }
    }
    for (std::size_t i = 0; i < pendingCtl_.size();) {
        if (nowNs >= pendingCtl_[i].deadlineNs) {
            PendingCtl p = takeCtl_(i);  // removed first: the delivery may issue a new op
            completeCtl_(p, OpStatus::Timeout);
        } else {
            ++i;
        }
    }
}

void Client::failAllControl_(OpStatus st) {
    if (hasPendingConnect_) completeConnect_(st, 0, 0, {});
    while (!pendingCtl_.empty()) {
        PendingCtl p = takeCtl_(pendingCtl_.size() - 1);
        completeCtl_(p, st);
    }
}

// ---------------------------------------------------------------------------
// F8c C3: *AndWait — cold-path blocking conveniences (pump-until-done)
// ---------------------------------------------------------------------------

void Client::throwForStatus_(const char* what, const Completion& c, std::span<const uint8_t> payload) const {
    std::string msg(what);
    switch (c.status) {
    case OpStatus::Ok:             return;
    case OpStatus::Timeout:        msg += ": timed out"; break;
    case OpStatus::Closed:         msg += ": client closed"; break;
    case OpStatus::NotConnected:   msg += ": not connected (state != Connected)"; break;
    case OpStatus::SessionLost:    msg += ": session lost before the answer"; break;
    case OpStatus::Superseded:     msg += ": superseded by a newer call"; break;
    case OpStatus::ResultTooLarge: msg += ": result too large (" + std::to_string(c.value) + " bytes)"; break;
    case OpStatus::BrokerError:
        msg += ": broker error code " + std::to_string(c.errorCode);
        if (!payload.empty()) msg += ": " + std::string(reinterpret_cast<const char*>(payload.data()), payload.size());
        break;
    default:                       msg += ": failed (status " + std::to_string(static_cast<int>(c.status)) + ")"; break;
    }
    throw std::runtime_error(msg);
}

template <class Issue, class OnDone>
void Client::andWait_(const char* what, std::chrono::nanoseconds timeout, uint32_t maxResultBytes,
                      Issue&& issue, OnDone&& onDone) {
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker) {
        if (!onDuty) throw std::logic_error(std::string(what) + ": Invoker mode — call from the owner thread");
        if (inDutyCycle_) {
            throw std::logic_error(std::string(what) +
                                   ": called from a duty-cycle callback (it could never complete) — "
                                   "use the token form");
        }
    } else if (onDuty) {
        throw std::logic_error(std::string(what) +
                               ": called on the runner thread (a callback would wait for itself) — "
                               "use the token form");
    }
    CompletionsConfig qc;
    qc.maxResultBytes = std::max<uint32_t>(maxResultBytes, qc.maxErrorText);
    uint32_t cap = 1u << 16;
    while (cap / 8 < qc.maxResultBytes + 64u) cap <<= 1;
    qc.capacityBytes = cap;
    Completions q(*this, qc);

    // A distinct token per call (m6): a completion for an earlier call can
    // never be taken for this one's answer.
    tWaitToken = (tWaitToken % kMaxToken) + 1;
    const Token kToken = tWaitToken;
    // n1: ONE budget for issue + wait.
    const auto issueDeadline = std::chrono::steady_clock::now() + timeout;
    aeron::concurrent::BackoffIdleStrategy backoff(
        10, 20, std::chrono::duration<long, std::nano>(1'000),
        std::chrono::duration<long, std::nano>(100'000));
    for (;;) {
        const OpResult r = issue(q, kToken);
        if (r == OpResult::Accepted) break;
        if (r == OpResult::BackPressured) {  // the command ring is full: let the duty thread drain
            if (std::chrono::steady_clock::now() >= issueDeadline) {
                throw std::runtime_error(std::string(what) + ": timed out (command ring full)");
            }
            if (cfg_.runnerMode == RunnerMode::Invoker) (void)doWork(); else backoff.idle(0);
            continue;
        }
        if (r == OpResult::Closed) throw std::runtime_error(std::string(what) + ": client closed");
        throw std::runtime_error(std::string(what) + ": rejected (OpResult " +
                                 std::to_string(static_cast<int>(r)) + ")");
    }
    bool       ok = false;
    auto done = [&](const Completion& c, std::span<const uint8_t> payload) {
        ok = true;
        onDone(c, payload);
    };
    Completions::Wait w;
    const auto left = std::max(std::chrono::nanoseconds(0),
                               std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   issueDeadline - std::chrono::steady_clock::now()));
    if (cfg_.runnerMode == RunnerMode::Invoker) {
        w = q.waitFor(kToken, left, done, [this] { return doWork(); });
    } else {
        w = q.waitFor(kToken, left, done);
    }
    if (w == Completions::Wait::TimedOut) throw std::runtime_error(std::string(what) + ": timed out waiting");
    if (!ok) throw std::runtime_error(std::string(what) + ": client closed");
}

void Client::connectAndWait(std::chrono::nanoseconds timeout) {
    andWait_("connect()", timeout, 0,
             [&](Completions& q, Token t) { return connect(q, t); },
             [&](const Completion& c, std::span<const uint8_t> payload) { throwForStatus_("connect()", c, payload); });
}

Bytes Client::pingAndWait(std::span<const uint8_t> payload, std::chrono::nanoseconds timeout) {
    Bytes echo;
    andWait_("ping()", timeout, static_cast<uint32_t>(payload.size()),
             [&](Completions& q, Token t) { return ping(payload, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 throwForStatus_("ping()", c, p);
                 echo.assign(p.begin(), p.end());
             });
    return echo;
}

DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions,
                                             std::string_view keyField, bool validate,
                                             std::span<const uint8_t> descriptorBlob,
                                             std::chrono::nanoseconds timeout) {
    DefineTopicResult result;
    andWait_("defineTopic()", timeout, 0,
             [&](Completions& q, Token t) {
                 return defineTopic(name, partitions, keyField, validate, descriptorBlob, q, t);
             },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("defineTopic()", c, p);
                 result.topicId   = static_cast<int32_t>(c.value);
                 result.version   = static_cast<uint16_t>(c.aux);
                 result.errorCode = c.errorCode;
             });
    return result;
}

TopicSchemaResult Client::getTopicSchemaAndWait(std::string_view name, std::chrono::nanoseconds timeout) {
    TopicSchemaResult result;
    andWait_("getTopicSchema()", timeout, 64u << 10,
             [&](Completions& q, Token t) { return getTopicSchema(name, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("getTopicSchema()", c, p);
                 result.topicId   = static_cast<int32_t>(c.value);
                 result.version   = static_cast<uint16_t>(c.aux);
                 result.errorCode = c.errorCode;
                 if (c.status == OpStatus::Ok) result.descriptorBlob.assign(p.begin(), p.end());
             });
    return result;
}

std::vector<std::pair<std::string, int32_t>> Client::listTopics(std::chrono::nanoseconds timeout) {
    std::vector<std::pair<std::string, int32_t>> out;
    andWait_("listTopics()", timeout, 64u << 10,
             [&](Completions& q, Token t) { return listTopics(q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 throwForStatus_("listTopics()", c, p);
                 out = decodeTopicList(p);
             });
    return out;
}

// ---------------------------------------------------------------------------
// doWork() / dutyCycle() — the ONE duty cycle shared by both runner modes
// ---------------------------------------------------------------------------

int Client::doWork() {
    if (cfg_.runnerMode != RunnerMode::Invoker) {
        throw std::logic_error("Client::doWork(): only valid in RunnerMode::Invoker");
    }
    if (std::this_thread::get_id() != ownerThread_) [[unlikely]] {
        throw std::logic_error("Client::doWork(): call from the owner (constructing) thread");
    }
    if (inDutyCycle_) return 0;  // re-entered from a callback: never nest the duty cycle
    // fix2 minor: a no-op after close() — belt-and-suspenders against a stray
    // post-close call (e.g. from an app loop that has not yet noticed close()
    // happened).  F8c: "closed" is the command ring's closed bit.
    if (agent_.commandRingClosed()) return 0;
    return dutyCycle(now());
}

int Client::dutyCycle(int64_t nowNs) {
    struct InCycle {  // saves + restores: nesting-safe (a registration pump inside)
        bool& flag;
        bool  prev;
        explicit InCycle(bool& f) : flag(f), prev(f) { flag = true; }
        ~InCycle() { flag = prev; }
    } inCycle(inDutyCycle_);
    if (testDutyCycleHook_ != nullptr) [[unlikely]] testDutyCycleHook_();

    // 1. Agent: driver/conductor (invoker), command ring, egress, session ticks.
    //    ProduceResponses are decoded and buffered, not dispatched through hooks.
    int work = agent_.doWork(nowNs);
    // I3': owner releases fenced on the command ring apply once the drain passed them.
    if (!deferredReleases_.empty()) [[unlikely]] work += applyDeferredReleases_(false);

    // 2. ProduceResponses → hub with plain calls (O(1) ordinal routing).
    work += routeBufferedAcks_();

    // 3. Producer drain/flush through AgentProduceSink (no per-cycle std::function).
    work += producerDuty_(nowNs);

    // 3b. F8a fix1 I4 / fix2: status snapshots a near-full status ring refused,
    //     Unsubscribes / OffsetCommits the transport refused — retried every
    //     cycle while any exist (one branch when none; a group consumer's
    //     commit flags are checked while one is registered — at most one).
    if (((subscriberHub_.dirtyCount() | groupConsumerHub_.dirtyCount() |
          subscriberHub_.pendingUnsubscribes() | subscriberHub_.unsentSubscribes()) != 0) ||
        !groupConsumerHub_.empty()) [[unlikely]] {
        const int64_t sid = agent_.seedSession().machine.sessionId();
        if (subscriberHub_.dirtyCount() != 0)          work += subscriberHub_.republishDirty();
        if (groupConsumerHub_.dirtyCount() != 0)       work += groupConsumerHub_.republishDirty();
        if (subscriberHub_.pendingUnsubscribes() != 0) work += subscriberHub_.retryPendingUnsubscribes(sid);
        if (subscriberHub_.unsentSubscribes() != 0)    work += subscriberHub_.retryUnsentSubscribes();
        if (!groupConsumerHub_.empty()) {
            work += groupConsumerHub_.flushRequestedCommits(sid);
            if (groupConsumerHub_.pendingLeaves() != 0) work += groupConsumerHub_.retryPendingLeaves(sid);
        }
    }

    // 3b'. F8c C3: control frames the transport refused (Retry), in order.
    if (unsentCtl_ != 0) [[unlikely]] work += retryUnsentControl_();

    // 3c. F8a fix2/fix3/fix4 (IM-2, I2, M5): session state is LEVEL-triggered
    //     for commitSync — it fails on leaving a connected (state, sessionId,
    //     episode) (syncSeedSession_, also run at the transition itself by
    //     the egress handlers and before every hub command), and on every
    //     cycle the session is not Connected while one is pending (a
    //     commitSync applied while down fails at once inside the hub).
    syncSeedSession_();
    if (lastSeedState_ != session::SessionState::Connected && !groupConsumerHub_.empty() &&
        groupConsumerHub_.hasPendingCommitSync()) [[unlikely]] {
        groupConsumerHub_.onSessionDown();
    }

    // 4. Periodic sweep on the duty-cycle clock.
    if (nowNs >= nextSweepNs_) {
        sweepTimeouts(nowNs);
        nextSweepNs_ = nowNs + kSweepIntervalMs * 1'000'000LL;
    }
    return work;
}

void Client::syncSeedSession_() noexcept {
    const auto& m = agent_.seedSession().machine;
    const session::SessionState st  = m.state();
    const int64_t               sid = m.sessionId();
    // The connect correlationId changes on every connect / resume attempt and
    // never while Connected: it identifies the connect episode, so a resume
    // that gets the SAME sessionId back is still a transition.
    const int64_t episode = m.correlationId();
    if (st == lastSeedState_ && sid == lastSeedSessionId_ && episode == lastSeedEpisode_) [[likely]] {
        // F8c C3: an app-side status the ring refused (owner not draining) is
        // retried until it fits — the newest value always wins.
        if (seedStatusDirty_) [[unlikely]] publishSeedStatus_(st, sid);
        return;
    }
    // F8c C3: app threads (Client::state()) see the transition BEFORE any
    // completion or callback this transition triggers (ring order on this
    // thread: status first).
    if (st != lastSeedState_ || sid != lastSeedSessionId_) publishSeedStatus_(st, sid);
    const bool connected = st == session::SessionState::Connected;
    groupConsumerHub_.setSessionConnected(connected, sid, episode);
    subscriberHub_.setSessionConnected(connected);
    lastSeedState_     = st;
    lastSeedSessionId_ = sid;
    lastSeedEpisode_   = episode;
}

int Client::routeBufferedAcks_() {
    const auto acks = agent_.produceAcks();
    const int n = static_cast<int>(acks.size());
    for (const auto& a : acks) producerHub_.onProduceResponse(a.correlationId, a.baseOffsetOrError);
    agent_.clearProduceAcks();
    return n;
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
    agent_.bindDutyThread();  // F8c C0: the runner is now the single pumper
    if (cfg_.runnerCore >= 0 && !detail::pinCurrentThreadToCore(cfg_.runnerCore)) {
        std::fprintf(stderr, "rune-client: could not pin the runner to core %d\n", cfg_.runnerCore);
    }
    if (cfg_.idleMode == IdleMode::LowLatency) {
        // spin 5, yield 3, park 1 µs .. 100 µs: the runner drives the producer drain
        // AND the ack poll, so its park ceiling is the client floor on ack latency.
        aeron::concurrent::BackoffIdleStrategy idle(
            5, 3, std::chrono::duration<long, std::nano>(1'000),
            std::chrono::duration<long, std::nano>(100'000));
        while (!agent_.commandRingClosed()) idle.idle(dutyCycle(now()));
    } else if (cfg_.idleMode == IdleMode::BusySpin) {
        // Never park or yield: pin this runner to a dedicated core.
        while (!agent_.commandRingClosed()) {
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
        while (!agent_.commandRingClosed()) idle.idle(dutyCycle(now()));
    }
}

// ---------------------------------------------------------------------------
// sweepTimeouts() — called from the runner thread
// ---------------------------------------------------------------------------

void Client::sweepTimeouts(int64_t nowNs) {
    // Subscribe deadlines are created on app threads with steady_clock, so
    // they keep their own steady_clock sample (control-op deadlines are on the
    // duty clock: sweepControl_ below).
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
                invokeUserCallback_("onError", [&] {
                    cbError_("Redirect reconnect failed: endpoint unreachable");
                });
            }
        }
    }

    // --- F8c C3: control-op deadlines on the duty clock -> Timeout ---
    sweepControl_(nowNs);
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
        // F8a fix4 I-1: the hubs learn the session transition HERE, before the
        // resends / onConnected callback / later frames of this same poll (a
        // record handler subscribing) — never only at the end of the cycle.
        syncSeedSession_();
        auto opt = codec::decodeConnectResponse(buf, len);
        if (!opt) break;

        namespace rpc = rune::protocol::client;
        const auto ec = static_cast<rpc::ErrorCode>(
            static_cast<uint16_t>(opt->code));

        if (ec == rpc::ErrorCode::OK) {
            // F8c C3: complete the pending connect (state() was published by
            // syncSeedSession_ above, so a woken waiter reads Connected).
            completeConnect_(OpStatus::Ok, opt->assignedSessionId, 0, {});

            // CB-5 B: if this ConnectResponse{OK} completes a redirect-triggered
            // reconnect, flush every pending redirected data op NOW, re-stamped
            // with the leader's freshly assigned sessionId.  This is deferred to
            // exactly here (rather than at redirect time) so the resent frames
            // carry the NEW session — the leader drops unknown-session ops.
            // Runs on the agent thread (single-writer): submitSend → agent ring.
            if (redirectPending()) {
                redirectPending() = false;
                // Acks that arrived earlier in THIS poll reach the hub before the
                // resend, so an already-acked batch is never resent (T12 I4).
                (void)routeBufferedAcks_();
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
            } else if (subscriberHub_.hasPending()) {
                // F8a fix2: a pending Subscribe sent before this session existed
                // was dropped by the ring drain (stale session) — re-issue every
                // pending one stamped with the new session.
                subscriberHub_.resendPending(
                    opt->assignedSessionId,
                    [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    });
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

            // Fire onConnected. fix1 C1: this runs inside the noexcept
            // ClientAgent::doWork chain (dispatchEgressFrame -> egressObserver_ ->
            // onEgressFrame) — a throwing callback (e.g. a blocking send() that
            // throws because it is back-pressured inside this very callback) must
            // never escape it and terminate the process.
            if (cbConnected_) {
                invokeUserCallback_("onConnected", [&] { cbConnected_(opt->assignedSessionId); });
            }
        } else {
            // Error response via ConnectResponse — fail the connect and fire onError.
            const std::string msg = "ConnectResponse error code: " +
                                    std::to_string(opt->code);
            completeConnect_(OpStatus::BrokerError, 0, static_cast<uint16_t>(opt->code), msg);
            if (cbError_) {
                invokeUserCallback_("onError", [&] { cbError_(msg); });
            }
        }
        break;
    }

    // ---- ErrorResponse ----
    case codec::TemplateId::ErrorResponse: {
        syncSeedSession_();  // fix4: a SessionExpired just took the session down
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
                    invokeUserCallback_("onError", [&] { cbError_(retargetReason); });
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

        // FIX F2 / F8c C3: complete in-flight control ops on this error
        // IMMEDIATELY rather than letting them die by their deadline (which
        // loses the server error and adds full-timeout latency):
        //   (a) a pending connect always fails (BrokerError);
        //   (b) session-fatal (SessionExpired / UnknownSession) -> SessionLost
        //       for EVERY pending control op of the (now dead) session;
        //   (c) otherwise the op whose correlationId matches -> BrokerError.
        // Each entry is removed before it is delivered (exactly once).
        const bool sessionFatal =
            (ec == rpc::ErrorCode::SessionExpired ||
             ec == rpc::ErrorCode::UnknownSession);
        const auto ecode = static_cast<uint16_t>(opt->code);
        const auto text  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(msg.data()), msg.size());
        completeConnect_(OpStatus::BrokerError, 0, ecode, msg);
        if (sessionFatal) {
            while (!pendingCtl_.empty()) {
                PendingCtl p = takeCtl_(pendingCtl_.size() - 1);
                completeCtl_(p, OpStatus::SessionLost, 0, 0, ecode, text);
            }
        } else if (const std::size_t i = findCtl_(opt->correlationId); i < pendingCtl_.size()) {
            PendingCtl p = takeCtl_(i);
            const int64_t noTopic = (p.op == OpKind::Ping) ? 0 : -1;
            completeCtl_(p, OpStatus::BrokerError, noTopic, 0, ecode, text);
        }
        if (cbError_) {
            invokeUserCallback_("onError", [&] { cbError_(msg); });
        }
        break;
    }

    // ---- Pong ----
    case codec::TemplateId::Pong: {
        auto opt = codec::decodePong(buf, len);
        if (!opt) break;

        // F8c C3: the echo is written straight into the completion (the
        // Aeron buffer view is valid for this call only; no copy here).
        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() && pendingCtl_[i].op == OpKind::Ping) {
            PendingCtl p = takeCtl_(i);
            completeCtl_(p, OpStatus::Ok, 0, 0, 0,
                         std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(opt->echo.data()),
                                                  opt->echo.size()));
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
            const int64_t unsubCorrId = nextCorrId_++;
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

        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() && pendingCtl_[i].op == OpKind::DefineTopic) {
            PendingCtl p = takeCtl_(i);
            // Task 9 (CLI): on success, register (name, topicId) so listTopics()
            // can enumerate topics that were applied via this client.
            if (opt->errorCode == 0 && !p.topicName.empty() && opt->topicId >= 0) {
                const bool already = std::any_of(knownTopics_.begin(), knownTopics_.end(),
                                                 [&](const auto& e) { return e.first == p.topicName; });
                if (!already) knownTopics_.emplace_back(p.topicName, opt->topicId);
            }
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->topicId, opt->version, opt->errorCode);
        }
        break;
    }

    // ---- Task 10: TopicSchemaResponse (67) ----
    case codec::TemplateId::TopicSchemaResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::TopicSchemaResponse::decode(buf, len);
        if (!opt) break;

        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() && pendingCtl_[i].op == OpKind::GetTopicSchema) {
            PendingCtl p = takeCtl_(i);
            if (opt->errorCode == 0) {
                // Task 11 / F8c C3: the per-topic schema cache is written HERE,
                // on the duty thread (was an app-thread write under mtx_).
                topicSchemaCache_.put(opt->topicId,
                    metadata::TopicSchemaEntry{opt->version, /*validate=*/false, /*keyField=*/""});
            }
            const std::span<const uint8_t> blob(
                opt->descriptorBlob, (opt->descriptorBlob != nullptr) ? opt->descriptorLen : 0u);
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->topicId, opt->version, opt->errorCode, blob,
                         detail::CompletionRing::Overflow::TooLarge);
        }
        break;
    }

    default:
        // Unknown template — ignore (never crash).
        break;
    }
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
    // Task 14 (spec §4.1): in Invoker mode the owner thread IS the duty-cycle
    // thread — the producer appends straight into its partition builders (Direct
    // ingress); the ring hop (CAS + copy into a ring this same thread drains) is
    // pure overhead there.  Thread mode keeps the MPSC ring (unchanged).
    if (cfg_.runnerMode == RunnerMode::Invoker) {
        return producerWithRingCap_(cfg, producer::kDirectProducerRingCap, /*direct=*/true);
    }
    return producerWithRingCap_(cfg, producer::kDefaultProducerRingCap, /*direct=*/false);
}

Producer Client::testProducerWithRingCap(ProducerConfig cfg, int32_t ringCap) {
    // A RING-ingress producer in either runner mode: the seam that keeps the ring
    // paths (heartbeat wait, ring back-pressure, foreign app threads) testable
    // against an Invoker client too.
    return producerWithRingCap_(cfg, ringCap, /*direct=*/false);
}

Producer Client::producerWithRingCap_(ProducerConfig cfg, int32_t ringCap, bool direct) {
    // ProducerImpl owns its own ring buffer (its batch correlation ids are
    // self-tagged — batchcorr — so it takes no shared counter).
    auto impl = std::make_shared<producer::ProducerImpl>(cfg, ringCap);

    // fix1 C1: wire the pump hook in BOTH runner modes — a back-pressured
    // blocking send() must never wait on a ring consumer that IS the calling
    // thread (Invoker owner), and must never spin/wait-out-the-heartbeat when
    // called from inside a duty-cycle callback (either mode: Invoker owner or
    // Thread-mode runner). pumpTrampoline_ decides (PumpResult); Thread mode's
    // ordinary app thread still falls back to the heartbeat-bounded wait (a
    // genuinely separate runner thread may yet drain the ring for real). Plain
    // function pointer + `this`: no std::function, no virtual call, no new atomic.
    // fix2 Important-2: dutyThreadId_ is copied in by value — immutable for the
    // rest of this Client's lifetime (already finalized by the time any
    // producer() call can run), so ProducerImpl can decide NotOwner itself
    // without ever touching `this` from a non-duty thread.
    impl->setPumpFn(&Client::pumpTrampoline_, this, dutyThreadId_);

    // fix1 I4: clock coherence — when an injected clock is configured, the ring's
    // heartbeat writer (the duty cycle, via dutyCycle(now())) and its reader
    // (sendBlocking's liveness check) must agree on "now", or an injected-clock
    // test can see a stale heartbeat race a real steady_clock read.
    if (cfg_.nanoClock != nullptr) {
        impl->setClockForTesting(cfg_.nanoClock);
    }

    // Task 14: Direct ingress reuses the SAME pump hook + duty-thread binding as
    // setPumpFn() above (one mechanism): the owner (== dutyThreadId_ in Invoker
    // mode) is the only thread allowed to send; a back-pressured blocking send
    // pumps through pumpTrampoline_, which declines inside a duty cycle (an ack
    // callback) so sendBlocking throws rather than re-entering.  The append clock
    // is seeded from the duty-cycle clock (records sent before the producer's
    // first pass get a real batchStartNs for linger / delivery timeout).
    if (direct) {
        impl->enableDirectIngress({dutyThreadId_, &Client::pumpTrampoline_, this, now()});
    }

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
    // F8c C2: the submit is linearized against close() by the command ring's
    // closed bit (no post-submit stopped_ CAS, no indeterminate ownership):
    //   * accepted — before the close: the duty cycle, or close()'s final
    //     drain (which reads up to the frozen tail), adopts it and frees the
    //     holder; close() then marks its ring closed with every other one;
    //   * refused as Closed (close() raced this call) — never written: free
    //     the holder here and hand back a producer whose ring is closed (every
    //     send reports Closed), as the fix3 race path did.
    // (Closed on entry still throws logic_error("client closed") from
    // submitRegistration_, as before.)
    if (!submitRegistration_([&] { return agent_.submitRegisterProducer(holder); },
                             [&] { delete holder; })) {
        delete holder;
        impl->markRingClosed();
    }

    return Producer(std::move(impl));
}

producer::PumpResult Client::pumpTrampoline_(void* ctx) {
    auto* self = static_cast<Client*>(ctx);
    // fix2 Important-2: ProducerImpl::sendBlocking only ever calls this hook
    // when std::this_thread::get_id() == the dutyThreadId_ it copied from
    // setPumpFn() — NotOwner is decided there, without ever reaching here, so
    // this function is only ever entered ON the duty-cycle thread (the Invoker
    // owner, or the Thread-mode runner). inDutyCycle_ is duty-cycle-thread-only
    // state; reading it here is therefore safe (no cross-thread read).
    if (self->inDutyCycle_) {
        // We ARE the duty-cycle thread, already mid-cycle (this call came from
        // inside a user callback the cycle itself invoked). Pumping would
        // recursively re-enter the agent's own poll on the same thread —
        // decline so sendBlocking fails fast instead.
        return producer::PumpResult::DeclinedInCycle;
    }
    if (self->cfg_.runnerMode == RunnerMode::Invoker) {
        // The owner thread, outside any cycle: safe to drive one ourselves.
        // Task 14 fix1 I2: an exception from the cycle propagates out of
        // send() unchanged, exactly as out of doWork(); dutyCycle's InCycle
        // guard restores inDutyCycle_ on the way out.
        (void)self->dutyCycle(self->now());
        return producer::PumpResult::Pumped;
    }
    // Thread mode's runner thread calling from outside dutyCycle() should not
    // happen in practice (the runner's entire body IS the cycle loop) — never
    // pump proactively in Thread mode from here regardless; decline rather
    // than assume it is safe. fix3 M2: sendBlocking's thrown message for
    // DeclinedInCycle is phrased to cover this case too (it does not claim
    // "inside a callback" specifically), since this branch reuses the same
    // result for a technically distinct (if unreachable) condition.
    return producer::PumpResult::DeclinedInCycle;
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
    // Build the impl.  Its immutable fields are written HERE, before the
    // SubscribeAdd command is committed to the agent ring (which publishes them
    // to the duty thread); every other field is duty-thread owned from then on.
    // F8c C3: the correlationId (and the frame's sessionId) are assigned by the
    // duty thread when it adopts the request — no app-thread counter / snapshot.
    auto impl = std::make_shared<SubscriptionImpl>();
    impl->topicId       = topic.topicId;
    impl->partition     = partition;
    impl->handler       = std::move(handler);

    // M2 FIX: a deadline so sweepTimedOut() can fail this subscribe if
    // SubscribeResponse never arrives (mirrors produce-ack timeout sweep).
    const auto subscribeDL = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(cfg_.subscribeTimeoutMs);

    // F8a: encode on this thread (pure), then hand the request to the duty
    // thread, which registers it as pending and sends the Subscribe frame.
    auto req = subscriber::SubscriberHub::makeSubscribeRequest(
        impl, /*sessionId (stamped on adoption)=*/0, partitionKnown, opts.from.toWire(),
        static_cast<uint8_t>(opts.isolation), keyBytes, subscribeDL);
    // F8a fix4 M1: a Subscribe frame no ingress publication can carry fails
    // HERE, synchronously — it never reaches the duty thread (where it could
    // only be dropped as Fatal).
    if (req->subscribeFrame.empty() || req->subscribeFrame.size() > agent_.maxFrameLength()) {
        throw std::length_error(
            "subscribe(): Subscribe frame of " + std::to_string(req->subscribeFrame.size()) +
            " bytes cannot be sent (transport max message length " +
            std::to_string(agent_.maxFrameLength()) + ")");
    }

    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::SubscribeAdd;
    cmd.handle = reinterpret_cast<uint64_t>(req.get());
    switch (hubCommandTrampoline_(this, cmd)) {  // throws: req frees itself
    case detail::HubSubmit::Accepted:
        (void)req.release();      // the duty thread owns it now
        break;
    case detail::HubSubmit::Closed:
        impl->status.abandon();   // never submitted (req freed): born finished
        break;
    }

    return Subscription(std::move(impl), &Client::hubCommandTrampoline_, this, handleDutyThread_());
}

// ---------------------------------------------------------------------------
// F8a: hubCommandTrampoline_ — the HubCommandSink bound to every handle
// ---------------------------------------------------------------------------

std::exception_ptr Client::submitError_(const char* what, std::size_t frameLen) const {
    if (frameLen > agent_.maxFrameLength()) {
        return std::make_exception_ptr(std::length_error(
            std::string(what) + ": frame of " + std::to_string(frameLen) +
            " bytes exceeds the transport max message length of " +
            std::to_string(agent_.maxFrameLength())));
    }
    return std::make_exception_ptr(std::runtime_error(std::string(what) + ": ring full, could not submit"));
}

void Client::applyHubCommandDirect_(const detail::HubCommand& cmd) {
    syncSeedSession_();  // fix4 I-1: the hubs act on the session's CURRENT state
    // fix3 I3: the caller's handle holds the impl (cmd.handle) for the whole
    // call, so the hubs may act on the impl itself — even when its Add is still
    // queued in the ring (created on another thread) — and adoption honours it.
    using Op = detail::HubCommand::Op;
    if (cmd.op == Op::Unsubscribe && cmd.handle != 0) {
        const auto snap = agent_.stateAndSessionId();
        subscriberHub_.unsubscribeDirect(
            *reinterpret_cast<SubscriptionImpl*>(cmd.handle),
            nextCorrId_++, snap.sessionId,
            [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                return agent_.submitSend(fb, fl);
            });
        return;
    }
    if (cmd.op == Op::GroupClose || cmd.op == Op::GroupCommit || cmd.op == Op::GroupCommitSync) {
        groupConsumerHub_.applyCommandDirect(cmd);
        return;
    }
    applyHubCommand_(cmd);
}

detail::SendResult Client::directSendTrampoline_(void* ctx, const uint8_t* buf, std::size_t len) {
    using R = transport::AeronTransport::OfferResult;
    switch (static_cast<Client*>(ctx)->agent_.offerDefault(buf, len)) {
    case R::Accepted: return detail::SendResult::Accepted;
    case R::Retry:    return detail::SendResult::Retry;
    case R::Fatal:    break;
    }
    return detail::SendResult::Fatal;
}

void Client::applyHubCommand_(const detail::HubCommand& cmd) {
    syncSeedSession_();  // fix4 I-1: the hubs act on the session's CURRENT state
    using Op = detail::HubCommand::Op;
    switch (cmd.op) {  // F8c C2/C3: the Client's own commands
    case Op::CqAdd:
        applyCqAdd_(reinterpret_cast<detail::CompletionRing*>(cmd.handle));
        return;
    case Op::CqRelease:
        applyCqRelease_(static_cast<CqRef>(cmd.a), reinterpret_cast<detail::CompletionRing*>(cmd.handle));
        return;
    case Op::ClientSetCallback:
        applySetCallback_(static_cast<int>(cmd.a), reinterpret_cast<void*>(cmd.handle));
        return;
    default:
        break;
    }
    if (cmd.op == Op::SubscribeAdd && cmd.handle != 0) {
        // F8c C3: the Subscribe correlationId comes from the duty-owned counter
        // (header offset 16; the sessionId at offset 8 is stamped by the hub).
        auto* req = reinterpret_cast<subscriber::SubscribeRequest*>(cmd.handle);
        if (req->impl) {
            req->impl->correlationId = nextCorrId_++;
            if (req->subscribeFrame.size() >= rune::protocol::client::MessageHeader::ENCODED_SIZE) {
                rune::protocol::client::detail::putLE64(req->subscribeFrame.data(), 16,
                                                        req->impl->correlationId);
            }
        }
    }
    if (cmd.op == Op::SubscribeAdd || cmd.op == Op::Unsubscribe) {
        const auto snap = agent_.stateAndSessionId();
        const int64_t unsubCorrId = (cmd.op == Op::Unsubscribe) ? nextCorrId_++ : 0;
        subscriberHub_.applyCommand(
            cmd, unsubCorrId, snap.sessionId,
            [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                return agent_.submitSend(fb, fl);
            });
    } else {
        groupConsumerHub_.applyCommand(cmd);
    }
}

detail::HubSubmit Client::hubCommandTrampoline_(void* ctx, const detail::HubCommand& cmd) {
    using Op = detail::HubCommand::Op;
    auto* self = static_cast<Client*>(ctx);
    const bool onDuty = std::this_thread::get_id() == self->dutyThreadId_;

    // fix1 I6: a commitSync() future created inside a duty-cycle callback can
    // never resolve while that callback blocks the duty thread — fail loudly
    // instead of deadlocking.  (inDutyCycle_ is read on the duty thread only.)
    if (cmd.op == Op::GroupCommitSync && onDuty && self->inDutyCycle_) {
        throw std::logic_error(
            "GroupConsumer::commitSync() called from inside a duty-cycle callback: its "
            "future can never resolve while the callback runs — use commit()");
    }
    if (self->agent_.commandRingClosed()) return detail::HubSubmit::Closed;

    // fix1 C1(a): on the duty thread the state is ours — apply directly (never
    // through the ring, so never blocked by it).  fix2: the registrations
    // (SubscribeAdd / GroupAdd) too — otherwise a direct Unsubscribe / commit /
    // close on the owner could OVERTAKE its own still-queued registration and
    // be applied to a subscription/consumer the hub does not hold yet.  Not
    // GroupSetCallback (it could replace a running callback): ring order.
    if (onDuty && (cmd.op == Op::SubscribeAdd || cmd.op == Op::GroupAdd ||
                   cmd.op == Op::Unsubscribe || cmd.op == Op::GroupClose ||
                   cmd.op == Op::GroupCommit || cmd.op == Op::GroupCommitSync)) {
        self->applyHubCommandDirect_(cmd);
        return detail::HubSubmit::Accepted;
    }

    // F8c C2: every submit below is linearized against close() by the command
    // ring's closed bit: Accepted = before the close (applied by the duty
    // cycle or close()'s final drain), Closed = never written.
    using S = agent::ClientAgent::Submit;
    if (detail::isLifecycle(cmd.op)) {
        // fix1 C1(b): guaranteed delivery.  A full ring is waited out on the
        // duty thread's progress (it drains the ring), never a time bound and
        // never a throw.  The wait ends only by submitting or by close(), which
        // finishes the subscription / consumer itself.
        aeron::concurrent::BackoffIdleStrategy idle(
            10, 20, std::chrono::duration<long, std::nano>(1'000),
            std::chrono::duration<long, std::nano>(100'000));
        for (;;) {
            const S r = self->agent_.submitHubCommand(cmd);
            if (r == S::Accepted) return detail::HubSubmit::Accepted;
            if (r == S::Closed) return detail::HubSubmit::Closed;
            if (self->testHubCommandRingFullHook_) [[unlikely]] self->testHubCommandRingFullHook_();
            idle.idle(0);
        }
    }
    try {
        // Bounded back-pressure exactly like producer registration: Invoker
        // owner pumps the duty cycle, Thread-mode app threads park while
        // the runner drains; from a duty-cycle callback / non-owner Invoker
        // thread a FULL ring throws instead (never a silent drop).
        return self->submitRegistration_([&] { return self->agent_.submitHubCommand(cmd); }, [] {})
                   ? detail::HubSubmit::Accepted
                   : detail::HubSubmit::Closed;
    } catch (const std::logic_error&) {
        // close() won the race before the submit: nothing was written.
        if (self->agent_.commandRingClosed()) return detail::HubSubmit::Closed;
        throw;
    }
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
    // F8a: an app-side flag — the hub is duty-thread owned and never read here.
    if (groupSubscribed_) {
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

    impl->state = GroupConsumerState::Unjoined;

    // F8a: register through the agent ring — the duty thread adopts the impl
    // into the single-owner hub list and frees the holder.
    auto* holder = new std::shared_ptr<GroupConsumerImpl>(impl);
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupAdd;
    cmd.handle = reinterpret_cast<uint64_t>(holder);
    detail::HubSubmit r = detail::HubSubmit::Closed;
    try {
        r = hubCommandTrampoline_(this, cmd);
    } catch (...) {
        delete holder;  // never submitted
        throw;
    }
    switch (r) {
    case detail::HubSubmit::Accepted:
        groupSubscribed_ = true;
        break;
    case detail::HubSubmit::Closed:
        delete holder;            // never submitted
        impl->status.abandon();   // born finished
        break;
    }

    return GroupConsumer(std::move(impl), &Client::hubCommandTrampoline_, this, handleDutyThread_());
}

// ---------------------------------------------------------------------------
// TX-5: transactionalProducer() — create and register a TransactionalProducer
// ---------------------------------------------------------------------------

TransactionalProducer Client::transactionalProducer(const std::string& transactionalId,
                                                     ProducerConfig cfg)
{
    // nextCorrId_ is duty-owned: the txn producer only draws ids on the duty
    // thread (doWork / response handlers), so it shares the plain counter.
    auto impl = std::make_shared<producer::TransactionalProducerImpl>(
        transactionalId,
        cfg,
        nextCorrId_);

    // Shared-nothing registration through the agent ring (like producer()): the
    // duty-cycle thread adopts it into txnProducers_ — no mutex, no per-cycle copy.
    // F8c C2: linearized against close() by the ring (see producerWithRingCap_):
    // Closed = never written — free the holder and self-close the producer so
    // every later operation fails at once instead of queueing work no duty
    // cycle will ever process.
    auto* holder = new std::shared_ptr<producer::TransactionalProducerImpl>(impl);
    if (!submitRegistration_([&] { return agent_.submitRegisterTxnProducer(holder); },
                             [&] { delete holder; })) {
        delete holder;
        impl->markClosed("Client closed");
    }
    return TransactionalProducer(std::move(impl));
}

// ---------------------------------------------------------------------------
// MD1 Task 10: sendMetadataRequest() — request all-topic metadata from the broker
// ---------------------------------------------------------------------------

void Client::sendMetadataRequest(int64_t sessionId) noexcept {
    // Allocate a fresh correlationId (duty-owned counter).
    const int64_t corrId = nextCorrId_++;
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
