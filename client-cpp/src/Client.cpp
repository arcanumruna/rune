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
#include <rune/client/detail/OneToOneRingBuffer.h>
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
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <unordered_map>

namespace rune::client {

namespace {

ClientConfig validatedConfig(ClientConfig c) {
    if (c.traceDdlRetries && c.runnerMode != RunnerMode::Invoker)
        throw std::invalid_argument("ClientConfig::traceDdlRetries requires RunnerMode::Invoker "
                                    "(the notes are duty-thread state, read by the Invoker owner)");
    return c;
}
// F8c C8: the runner start latch (spec §3.4) — one empty record on a
// stack-local OneToOneRingBuffer of the smallest useful capacity.
constexpr std::size_t   kStartLatchCapacity = 64;
constexpr std::int32_t  kStartLatchMsgType  = 1;
// Catalog history: the completion value of a failed DDL — the pre-catalog ops
// (topicId / viewTopicId) report -1 as before; the new ops' objectId is 0.
int64_t ddlNoValue(rune::client::OpKind op) noexcept {
    using rune::client::OpKind;
    return op == OpKind::DefineTopic || op == OpKind::AlterTopic || op == OpKind::CreateView ? -1 : 0;   // GetViewRow: 0
}
// Task 16: "host:port" (or a full Aeron channel) -> an Aeron UDP channel.
std::string toChannel(const std::string& endpoint) {
    return endpoint.rfind("aeron:", 0) == 0 ? endpoint : ("aeron:udp?endpoint=" + endpoint);
}
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
    if (c.responseTimeoutNs > 0) {
        acfg.session.responseTimeoutNs = c.responseTimeoutNs;
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

    // Task 9 (RUNE_TLS enablement): map the public client<->broker TLS/AEAD
    // config straight into SessionConfig. offerCrypto defaults false, so a
    // caller who never touches these fields gets byte-identical pre-Task-5
    // plaintext behaviour.
    acfg.session.offerCrypto   = c.offerCrypto;
    acfg.session.tlsTrustPem   = c.tlsTrustPem;
    acfg.session.tlsExpectHost = c.tlsExpectHost;
    acfg.session.authToken     = c.authToken;
    acfg.session.requireCrypto = c.requireCrypto;
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
    : cfg_(validatedConfig(std::move(cfg)))
    , agent_(toAgentConfig(cfg_))
    , partitionRouter_(metadataCache_)
    , catalogReader_(std::make_unique<CatalogReader>(*this))   // eagerly, on the owning thread (I2)
{
    if (cfg_.traceDdlRetries) ddlRetryNotes_.reserve(kMaxDdlRetryNotes);   // push_back never reallocates on the duty thread
    // F8c review n1: releaseCq_ is noexcept and may defer a release; the list
    // never holds more than one entry per registry slot, so reserve it once
    // here and the push_back there can never allocate (or throw).
    deferredReleases_.reserve(kMaxCqs);
    // T15b: the shard-status ring exists before the runner starts (it is the
    // ring's producer from its first cycle).
    if (cfg_.shardStatus) {
        shardRing_ = std::make_unique<detail::StatusRing<shard::ShardStatus, 64 * 1024>>();
        shardDuty_.shard = shardPublished_.shard = cfg_.shardIndex;
    }
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
    // single-owner ProducerHub list.  F8c C7: the record carries the impl itself
    // (no shared_ptr holder) — from here the duty side holds its share (§3.3).
    agent_.setProducerRegisterFn([this](void* impl) {
        auto* p = static_cast<producer::ProducerImpl*>(impl);
        producerHub_.add(p);
        // Task 16 regression: a producer adopted AFTER a MetadataResponse learns
        // the advertised per-partition ingress streams now (updateIngressRoute in
        // onMetadataResponse reached only the producers registered back then).
        metadataCache_.forEachIngressStream([p](int32_t t, int32_t part, int32_t sid) {
            p->updateIngressRoute(t, part, sid);
        });
        // F1: its handle orphaned the release before this adoption: a scan
        // could not see it yet — queue it now (its fence keeps FIFO).
        if (p->releaseOrphaned() && !p->releaseNoted) {
            p->releaseNoted = true;
            noteOrphan_(detail::HandleKind::Producer, p, p->orphanFence());
        }
    });
    // Same hand-off for transactional producers (was a mutex-guarded vector that
    // the runner locked and copied every duty cycle).
    agent_.setTxnProducerRegisterFn([this](void* impl) {
        auto* t = static_cast<producer::TransactionalProducerImpl*>(impl);
        txnProducers_.push_back(t);
        if (t->status.consumerOrphaned() && !t->releaseNoted) {  // F1: see producers
            t->releaseNoted = true;
            noteOrphan_(detail::HandleKind::TxnProducer, t, t->orphanFence);
        }
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
    releases_.reserve(16);       // C7: handle releases (cold; grows only on a burst of releases)
    groupConsumerHub_.setSessionConnected(false);  // fix3: not connected until the first connect
    // F8c C4: commitSync completions go through the completion-queue registry.
    groupConsumerHub_.setCompletionSink(detail::CompletionSink{&Client::deliverTrampoline_, this});
    subscriberHub_.setSessionConnected(false);
    // F8a fix2: Unsubscribe / OffsetCommit frames go straight to the transport
    // from the duty thread (retried every duty cycle on refusal), never via the
    // command ring.
    subscriberHub_.setDirectSend(&Client::directSendTrampoline_, this);
    groupConsumerHub_.setDirectSend(&Client::coordDirectSendTrampoline_, this);  // D4: the coordinator
    // D4: every group frame goes to the COORDINATOR session — the seed's ring
    // path (unchanged) while the coordinator is the seed, else that node's own
    // session (re-stamped with its id; held = refused while it connects).
    coordSendFn_ = [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
        if (coordIdx_ == 0) return agent_.submitSend(fb, fl);
        return agent_.offerToCoordinator(coordIdx_, fb, fl) != transport::AeronTransport::OfferResult::Retry;
    };
    {
        group::GroupConsumerHub::FetchRouter fr;
        fr.ctx           = this;
        fr.leaderSession = &Client::fetchLeaderTrampoline_;
        fr.sessionId     = &Client::fetchSessionIdTrampoline_;
        fr.epoch         = &Client::fetchEpochTrampoline_;
        fr.revive        = &Client::fetchReviveTrampoline_;
        fr.brokerVersion = &Client::fetchBrokerVersionTrampoline_;
        fr.offer         = &Client::fetchOfferTrampoline_;
        groupConsumerHub_.setFetchRouter(fr);
        // E2 final fix wave (I2): a moved plain subscription lives on its
        // partition leader's own node session, reached the same way.
        subscriber::SubscriberHub::NodeRouter nr;
        nr.ctx           = this;
        nr.leaderSession = &Client::fetchLeaderTrampoline_;
        nr.sessionId     = &Client::fetchSessionIdTrampoline_;
        nr.epoch         = &Client::fetchEpochTrampoline_;
        nr.offer         = &Client::fetchOfferTrampoline_;
        nr.nextCorrId    = &Client::nextCorrTrampoline_;
        subscriberHub_.setNodeRouter(nr);
    }

    // MD1 Task 10: Wire the MetadataResponse hook so the cache is populated
    // on the duty-cycle thread without any extra lock.
    agent_.setMetadataResponseFn([this](const codec::MetadataResponseDecoded& resp) {
        onMetadataResponse(resp);
    });

    // Built ONCE (not per duty cycle / per frame).  TX-5 / Task 1.4: txn producers
    // keep their 3-arg SendFn (streamId, buf, len), stamped with the seed
    // sessionId.  C10: the route follows the frame —
    //   * Produce (txn data) -> ITS partition's leader route (re-stamped with
    //     that leader session's id; no route known = the seed, as for any produce);
    //   * every other txn frame is a COORDINATOR request (InitProducerId,
    //     AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn) -> the
    //     seed session, like the group coordinator's requests.  (It used to take
    //     topic 0 / partition 0's leader route: on a cluster whose partition
    //     leaders are spread over the nodes that is not the coordinator.)
    txnSendFn_ = [this](int32_t streamId, const uint8_t* buf, std::size_t len) -> bool {
        const auto tid = codec::peekTemplate(buf, len);
        if (tid && static_cast<codec::TemplateId>(*tid) == codec::TemplateId::Produce) {
            const auto pv = codec::decodeProduce(buf, len);
            const int32_t topicId   = pv ? pv->topicId : 0;
            const int32_t partition = pv ? pv->partition : 0;
            if (coordIdx_ != 0 && !agent_.hasRoute(topicId, partition)) [[unlikely]] {
                // D4: the txn producer stamps the COORDINATOR session's id; a
                // Produce without a partition route rides the seed — its id.
                // (A routed one is re-stamped with its leader's: r2 LOW-2, never
                // held on the seed.)
                const int64_t seedSid = agent_.seedSession().machine.sessionId();
                if (seedSid == 0) return false;  // held: re-offered next cycle
                rune::protocol::client::detail::putLE64(const_cast<uint8_t*>(buf), 8, seedSid);
            }
            return agent_.offerToPartition(topicId, partition, streamId, buf, len);
        }
        // Retry = transiently refused: false, re-offered next cycle.  Fatal =
        // never deliverable (counted + logged by the agent): consumed, the op
        // deadline completes the request.  D4: a coordinator request goes to the
        // coordinator session (the seed until a NotCoordinator names another
        // node); Retry there also covers "not connected yet" (re-sent once the
        // session changes — TransactionalProducerImpl::doWork).
        if (coordIdx_ != 0) [[unlikely]] {
            return agent_.offerToCoordinator(coordIdx_, buf, len) != transport::AeronTransport::OfferResult::Retry;
        }
        return agent_.offerDefault(buf, len) != transport::AeronTransport::OfferResult::Retry;
    };

    // CG-5: GroupConsumerHub is ticked from sweepTimeouts (100 ms cadence).

    agent_.start();  // Invoker: pumps its own driver while resolving (no threads)
    ownerThread_  = std::this_thread::get_id();
    dutyThreadId_ = ownerThread_;
    nextSweepNs_  = now() + kSweepIntervalMs * 1'000'000LL;
    if (cfg_.runnerMode == RunnerMode::Thread) {
        // F8c C8 (spec §3.4): the runner start latch — a stack-local 1-record
        // OneToOneRingBuffer.  The runner writes dutyThreadId_ (its OWN id) as
        // its first action, then publishes one record, then producer-closes the
        // ring as its LAST touch of it.  This thread spins until it has read the
        // record AND seen the producer-closed word: the acquire reads order the
        // runner's dutyThreadId_ write before every later read of it (app
        // threads hold the Client only after this constructor returns), and the
        // closed word proves the runner is done with the stack buffer (write()
        // stores the tail AFTER the record header, so returning on the record
        // alone would let that store land in a dead frame).  No timeout: the
        // runner's very first statements publish, so the wait ends as soon as
        // the thread runs — liveness does not depend on any time window.
        alignas(64) std::uint8_t startRingBuf[kStartLatchCapacity +
                                              detail::OneToOneRingBuffer::trailerLength()] = {};
        detail::OneToOneRingBuffer startRing(startRingBuf, sizeof(startRingBuf));
        runnerThread_ = std::thread([this, &startRing]() {
            dutyThreadId_ = std::this_thread::get_id();
            if (!startRing.writeWith(kStartLatchMsgType, 0,
                                     [](aeron::concurrent::AtomicBuffer&, aeron::util::index_t) {})) {
                std::abort();  // unreachable: an empty ring of capacity 64 takes one empty record
            }
            startRing.producerClose();  // the runner never touches `startRing` again
            runLoop();
        });
        bool started = false;
        aeron::concurrent::YieldingIdleStrategy idle;
        for (;;) {
            started = started ||
                      startRing.read([](std::int32_t, aeron::concurrent::AtomicBuffer&,
                                        aeron::util::index_t, aeron::util::index_t) {}, 1) == 1;
            if (started && startRing.producerClosed()) break;
            idle.idle();
        }
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

void Client::checkCloseCaller_(const char* what) const {
    if (cfg_.runnerMode == RunnerMode::Invoker) {
        // Cold-path contract checks, enforced in release builds too.
        if (std::this_thread::get_id() != ownerThread_) [[unlikely]] {
            throw std::logic_error(std::string(what) + ": Invoker mode — call from the owner thread");
        }
        if (inDutyCycle_) [[unlikely]] {
            throw std::logic_error(std::string(what) +
                                   ": called from a duty-cycle callback (would tear the agent "
                                   "down under its own poll)");
        }
    } else if (std::this_thread::get_id() == dutyThreadId_ && !agent_.commandRingClosed()) [[unlikely]] {
        throw std::logic_error(std::string(what) +
                               ": called from a runner callback (it would join its own thread)");
    }
}

void Client::close() {
    checkCloseCaller_("Client::close()");
    // F8c C7 (user decision 2): drain every producer (and close every
    // transactional producer) first — one token for all of them — then tear
    // down.  Each drain is bounded by its own deadline (deliveryTimeoutMs /
    // transactionOpTimeoutMs).
    //
    // DR client bug 2 (dr2 multiclient: close() hung > 200 s after a successful
    // run): the WHOLE drain phase — this queue's registration, the drain
    // command's issue, the wait for its answer — is bounded by ONE deadline,
    // ClientConfig::closeTimeoutMs.  Each of those steps needs the duty thread
    // to read the command ring; anything that keeps a command from being read
    // (a refused frame held at the ring head, a wedged duty step, a producer
    // drain that never settles) used to wait 100 years here.  At the deadline
    // the teardown below runs anyway: every pending operation — each producer's
    // unsent / unacknowledged record, the drain token, every control op —
    // completes exactly once (Closed), and close() returns.
    if (agent_.commandRingClosed()) {
        closeImpl_();
        return;
    }
    const int64_t closeMs  = cfg_.closeTimeoutMs > 0 ? cfg_.closeTimeoutMs : kDefaultCloseTimeoutMs;
    const auto    deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(closeMs);
    CompletionsConfig qc;                       // as andWait_'s queue
    qc.maxResultBytes = qc.maxErrorText;
    uint32_t cap = 1u << 16;
    while (cap / 8 < qc.maxResultBytes + 64u) cap <<= 1;
    qc.capacityBytes = cap;
    // Standalone first, registered under the deadline: an IN-DOUBT registration
    // (CqAdd still unread at the deadline) keeps the queue alive across the
    // teardown, which applies the CqAdd and hands the queue back.
    Completions q(qc);
    bool drained = false;
    try {
        checkBlockingCaller_("Client::close()");
        if (registerCq_(q, deadline) == CqRegistration::Registered) {
            constexpr Token kToken = 1;
            aeron::concurrent::BackoffIdleStrategy backoff(
                10, 20, std::chrono::duration<long, std::nano>(1'000),
                std::chrono::duration<long, std::nano>(100'000));
            OpResult r = OpResult::BackPressured;
            for (;;) {
                r = issueDrain_(q, kToken);
                if (r != OpResult::BackPressured || std::chrono::steady_clock::now() >= deadline) break;
                if (cfg_.runnerMode == RunnerMode::Invoker) (void)doWork(); else backoff.idle(0);
            }
            if (r == OpResult::Accepted) {
                const auto left = std::max(std::chrono::nanoseconds(0),
                                           std::chrono::duration_cast<std::chrono::nanoseconds>(
                                               deadline - std::chrono::steady_clock::now()));
                auto done = [&](const Completion&, std::span<const uint8_t>) { drained = true; };
                const Completions::Wait w = cfg_.runnerMode == RunnerMode::Invoker
                                                ? q.waitFor(kToken, left, done, [this] { return doWork(); })
                                                : q.waitFor(kToken, left, done);
                (void)w;
            }
        }
    } catch (const std::runtime_error&) {
        // Another thread closed the Client meanwhile (its close completes
        // everything): nothing left to drain.  (A full queue registry: the
        // teardown below completes everything, as before.)
        drained = agent_.commandRingClosed();
    }
    if (!drained && !agent_.commandRingClosed()) {
        std::fprintf(stderr,
                     "rune-client: close(): the drain did not finish within closeTimeoutMs=%lld — tearing "
                     "down now (every pending operation completes Closed)\n",
                     static_cast<long long>(closeMs));
        ++closeDeadlineHits_;
    }
    closeImpl_();
    // `q` is destroyed here, after the teardown handed it back (or it was never
    // adopted): its destructor never waits.
}

void Client::closeImmediately() {
    checkCloseCaller_("Client::closeImmediately()");
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

    // Single-threaded from here.  Drain the command ring up to its (now frozen)
    // tail: registrations are adopted (the hubs hold their share; the failAll below
    // reaches them), hub commands applied, completion queues added / released,
    // and every accepted control op completes — Closed (closing_).
    (void)agent_.drainRegistrationsForClose();
    // I3': the drain read up to the frozen tail (or was abandoned — nothing
    // behind the orphan is ever applied): every deferred release is safe now,
    // and must happen before the registry hand-back (the owner gave it up).
    (void)applyDeferredReleases_(true);

    // fix1 C2/I3: mark every producer's ring closed NOW — the Client (and its
    // pump-hook contexts, each `this`) is still fully valid, and this runs
    // before the transport is torn down. A Producer handle the app
    // kept past this point then gets SendResult::Closed, never touching the
    // (by-then-dangling) pump hook: no use-after-free, no silent accept.
    producerHub_.closeAllRings();

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

    // F8c C6/C6a: every producer completes everything it accepted with Closed,
    // exactly once — in its ring (drained to the frozen tail: a record claimed
    // before closeAllRings() but committed after it is still read; fix5 bound),
    // builders, the Tier-2 queue, the deferred arena, in flight — and a pending
    // begin-close token completes Closed.  Before the registry hand-back
    // (closeCqRegistry_, below), so no producer completion is a stale drop.
    // After agent_.stop(): nothing else touches the producers.
    producerHub_.closeFinalAll(OpStatus::Closed, agent_.closeDrainUnblockAfterYields());

    // TX-5 / F8c C5: every pending txn op completes Closed (its completion
    // reaches its queue before the registry hand-back below), and each
    // producer's status channel is finished, so later operations are rejected
    // Closed at once, never hang.  No lock: the runner was joined above
    // (Thread mode) or this is the owner thread (Invoker mode).
    for (producer::TransactionalProducerImpl* tp : txnProducers_) {
        tp->failAll();
    }

    // CG-5: Deactivate all group consumers (fire onRevoked, send LeaveGroup).
    // F8c fix round 1 (M-1): spec §4 order — producers, txn producers, THEN
    // group consumers, then the control tables.
    // MUST be called after agent_.stop() so no agent thread is in onRecords().
    // sendFn here is a best-effort last send (transport may be stopping).
    {
        const auto snap = agent_.stateAndSessionId();
        const group::SendFn gcSendFn = [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
            return agent_.submitSend(fb, fl);
        };
        groupConsumerHub_.closeAll(snap.sessionId, gcSendFn);
    }


    // M2 FIX: clear the seed session's redirect latch on close so it does not
    // stay latched.  Safe as a plain write: the runner thread was joined above, so this
    // runs single-threaded (no concurrent agent-thread access) — Task 1.1.
    redirectPending() = false;

    // F8c (spec §4 order): after producers, txn producers and group consumers
    // have failed their ops, the control tables complete Closed, and only then
    // is every registered Completions handed back (producer-closed: its owner
    // sees every Closed record, then finished()) — so no completion delivered
    // during close is dropped.  A queue released by user code that this close
    // runs (above) detached its side (I1), never waited on: the hand-back frees it.
    failAllControl_(OpStatus::Closed);
    // F8c C7: a Client::close() drain still pending (another thread's close
    // won, or this is the destructor) completes Closed, exactly once.
    for (const DrainWaiter& w : drainWaiters_) {
        Completion c;
        c.token  = w.token;
        c.op     = OpKind::ClientDrain;
        c.status = OpStatus::Closed;
        deliver_(w.target, c, {});
    }
    drainWaiters_.clear();
    // Task 5 fix round 1 (M3): belt-and-braces — any release deferred while
    // close ran is applied before the registry hand-back.
    (void)applyDeferredReleases_(true);
    closeCqRegistry_();
    // F8c C7 (§3.3 (b)): every handle impl the hubs still hold is handed back
    // — the duty side detaches its share (the handle frees it on release, or
    // it is freed here if its handle already let go).  After every hub
    // finished (above), before the command ring's consumer-finished word.
    handBackAll_();
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
    (void)registerCq_(q, std::chrono::steady_clock::time_point::max());
}

Client::CqRegistration Client::registerCq_(Completions& q, std::chrono::steady_clock::time_point deadline) {
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && !onDuty) {
        throw std::logic_error("Completions: Invoker mode — construct on the owner thread");
    }
    q.client_ = this;
    if (agent_.commandRingClosed()) {  // born finished: no duty side will ever own it
        (void)q.ring_->producerCloseAndDetach();  // the owner's detach then frees it
        return CqRegistration::BornFinished;
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
            // Closed: never written, still ours.  DR client bug 2: a bounded caller
            // (close()) past its deadline with the ring still full: never written
            // either — born finished, the same way.
            if (r == agent::ClientAgent::Submit::Closed || std::chrono::steady_clock::now() >= deadline) {
                q.unreserve(rec);
                (void)q.ring_->producerCloseAndDetach();
                return CqRegistration::BornFinished;
            }
            idle.idle(0);  // full: the duty thread drains it (progress-based; bounded only by `deadline`)
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
        // DR client bug 2: a bounded caller (close()) stops waiting at its deadline.
        // The CqAdd is still in the command ring: the queue is IN DOUBT — the
        // caller keeps it alive across closeImpl_(), whose drain applies the CqAdd
        // and whose hand-back finishes the queue (its destructor then frees it).
        if (std::chrono::steady_clock::now() >= deadline) return CqRegistration::InDoubt;
        idle.idle(0);
    }
    if (w == Completions::Wait::Done && answer >= 0) {
        q.cqRef_ = static_cast<CqRef>(answer);
        return CqRegistration::Registered;
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
    return CqRegistration::BornFinished;
}

void Client::releaseCq_(Completions& q) noexcept {
    detail::CompletionRing* ring = q.ring_;
    const CqRef             ref  = q.cqRef_;
    // I1: on the thread running this Client's close().  Usually its drain has
    // already read up to the frozen tail (or was abandoned: nothing behind the
    // orphan is ever applied).  A release made INSIDE drainRegistrationsForClose
    // can still have later ops of this queue in that drain: they are dropped as
    // stale by the generation check (misdelivery would need 32 generation wraps
    // of this one slot within a single drain — not reachable in practice).
    const uint64_t prodIssued = q.producerIssued_;  // C6: producer-op accounting
    // I1 (Task 5 fix round 0: no thread_local): a queue released by user code
    // that close() itself runs (a callable destroyed by the final drain) takes
    // the same closed-ring path as any thread below — it detaches its side and
    // never waits; closeCqRegistry_'s hand-back (later on this same thread)
    // frees it.
    if (std::this_thread::get_id() == dutyThreadId_) {
        // Thread-mode runner: every op it issues is applied directly.
        if (cfg_.runnerMode == RunnerMode::Thread) {
            applyCqRelease_(ref, ring, prodIssued);
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
        // Task 5 fix round 1 (M3): once close() runs (closing_, set by this
        // same thread before its drain), the drain is the last reader of the
        // command ring — done or abandoned, nothing behind it is ever applied
        // — so a release is never deferred past it (it would leak: no later
        // applyDeferredReleases_ runs, and the hand-back cannot free a ring
        // whose consumer never detached).
        if (closing_ || agent_.commandRingHead() >= fence) {
            applyCqRelease_(ref, ring, prodIssued);
        } else {
            // Never allocates: reserved to kMaxCqs in the constructor (one entry
            // per registry slot at most).  A deferred release keeps its slot (and
            // ring) until the drain passes the fence, so the registry can report
            // "full" meanwhile — bounded by that drain; a late op completes into
            // this queue's own (unpolled) ring, never into another queue.
            deferredReleases_.push_back(DeferredRelease{ring, fence, ref, prodIssued});
        }
        return;
    }
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::CqRelease;
    cmd.handle = reinterpret_cast<uint64_t>(ring);
    cmd.a      = ref;
    cmd.b      = static_cast<int64_t>(prodIssued);
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
            applyCqRelease_(d.ref, d.ring, d.prodIssued);  // every op it fenced has been applied (counted)
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
    cqSlots_[idx].ring          = ring;
    cqSlots_[idx].prodDelivered = 0;  // C6: a fresh owner's producer-op count
    reg.value = makeCqRef(idx, cqSlots_[idx].gen);
    (void)ring->deliver(reg, {});
}

void Client::applyCqRelease_(CqRef ref, detail::CompletionRing* ring, uint64_t prodIssued) {
    const uint32_t idx = cqRefIndex(ref);
    if (ref != kNoCqRef && idx < cqSlots_.size() && cqSlots_[idx].ring == ring &&
        cqSlots_[idx].gen == cqRefGen(ref)) {
        CqSlot& slot = cqSlots_[idx];
        slot.ring = nullptr;
        slot.gen  = static_cast<uint8_t>((slot.gen + 1) & kCqRefGenMask);
        // Quarantined until no pinned op targets it (I3) AND every producer op
        // its owner issued has been delivered or dropped here (C6) — so a
        // stale completion can never match a recycled slot.
        slot.quarantined = true;
        slot.prodIssued  = prodIssued;
        maybeRecycleCqSlot_(idx);
    }
    // An accepted release always transfers the ring to this side (§3.3 (a)).
    delete ring;
}

void Client::maybeRecycleCqSlot_(uint32_t idx) noexcept {
    CqSlot& slot = cqSlots_[idx];
    if (!slot.quarantined || slot.targets != 0 || slot.prodDelivered < slot.prodIssued) return;
    slot.quarantined   = false;
    slot.prodDelivered = 0;
    slot.prodIssued    = 0;
    freeCqSlot_(idx);
}

void Client::producerCqDeliverTrampoline_(void* ctx, CqRef ref, const Completion& c) noexcept {
    static_cast<Client*>(ctx)->producerCqDeliver_(ref, c);
}

void Client::producerCqDeliver_(CqRef ref, const Completion& c) noexcept {
    // Duty thread (or the closing thread).  One producer completion: a per-
    // record / Tier-2 ack (one producer op) or a BatchAck (batchAckUnits()
    // credit units, each one producer op).
    const uint32_t idx = cqRefIndex(ref);
    if (idx >= cqSlots_.size()) {
        ++staleCqDrops_;
        return;
    }
    CqSlot& slot = cqSlots_[idx];
    const uint64_t ops = c.op == OpKind::BatchAck ? batchAckUnits(c) : 1u;
    if (slot.ring != nullptr && slot.gen == cqRefGen(ref)) {
        (void)slot.ring->deliver(c, {});
        slot.prodDelivered += ops;
        return;
    }
    // Released: dropped.  If the slot is quarantined for THIS generation (the
    // one released just before its gen was bumped), count it so the slot can
    // be recycled once every producer op of that owner is accounted for.
    ++staleCqDrops_;
    if (slot.quarantined && ((cqRefGen(ref) + 1) & kCqRefGenMask) == slot.gen) {
        slot.prodDelivered += ops;
        maybeRecycleCqSlot_(idx);
    }
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
        --slot.targets;
        maybeRecycleCqSlot_(idx);
    }
}

void Client::deliverTrampoline_(void* ctx, const detail::OpTarget& t, const Completion& c,
                                std::span<const uint8_t> payload) noexcept {
    static_cast<Client*>(ctx)->deliver_(t, c, payload);
}

void Client::pinCommandTarget_(detail::HubCommand& cmd) noexcept {
    detail::OpTarget t = detail::decodeTarget(cmd);
    pinCqSlot_(t);
    detail::encodeTarget(t, cmd);
}

// ---------------------------------------------------------------------------
// F8c C4/C5: token ops issued through handles (commitSync, txn ops)
// ---------------------------------------------------------------------------

OpResult Client::targetedOpTrampoline_(void* ctx, detail::HubCommand& cmd, const CompletionTarget& target,
                                       Token token, OpKind op) {
    return static_cast<Client*>(ctx)->issueTargeted_(cmd, target, token, op);
}

OpResult Client::issueTargeted_(detail::HubCommand& cmd, const CompletionTarget& target, Token token, OpKind op,
                                bool viaRing) {
    if (token > kMaxToken) return OpResult::InvalidArgument;
    if (target.cq == nullptr && target.handler.fn == nullptr) return OpResult::InvalidArgument;
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && !onDuty) return OpResult::WrongThread;

    detail::OpTarget t;
    Completions* q = target.cq;
    if (q != nullptr) {
        if (q->client_ != this) return OpResult::InvalidArgument;
        if (!q->onOwnerThread()) return OpResult::WrongThread;
        const uint32_t r  = q->reservationFor(op);
        const OpResult rr = q->reserve(r);
        if (rr != OpResult::Accepted) return rr;
        t.reservedBytes = r;
        t.cqRef         = q->cqRef_;
    } else {
        t.handler = target.handler;
    }
    cmd.a = static_cast<int64_t>(token);
    detail::encodeTarget(t, cmd);
    auto unreserve = [&] { if (q != nullptr) q->unreserve(t.reservedBytes); };

    if (agent_.commandRingClosed()) {
        unreserve();
        return OpResult::Closed;
    }
    // The duty thread itself (the Invoker owner — in or outside a cycle — or
    // a Thread-mode runner callback): the state is ours, apply now (the
    // command is pinned and owned before this returns; nothing is queued).
    if (onDuty && !viaRing) {
        applyHubCommandDirect_(cmd);
        return OpResult::Accepted;
    }
    // fix5 test seam: close() racing this submit (the ring linearizes it).
    if (testBeforeRegistrationSubmitHook_) [[unlikely]] testBeforeRegistrationSubmitHook_();
    switch (agent_.submitHubCommand(cmd)) {
    case agent::ClientAgent::Submit::Accepted:
        // I3': the queue's ring fence — the op lies before this tail.
        if (q != nullptr) q->issueFence_ = agent_.commandRingTail();
        return OpResult::Accepted;
    case agent::ClientAgent::Submit::Full:
        unreserve();
        return OpResult::BackPressured;
    case agent::ClientAgent::Submit::Closed:
        break;
    }
    unreserve();
    return OpResult::Closed;
}

Completion Client::andWaitTrampoline_(void* ctx, const char* what, std::chrono::nanoseconds timeout,
                                      detail::AndWaitIssue issue, void* issueCtx, std::string* text) {
    auto* self = static_cast<Client*>(ctx);
    Completion out;
    self->andWait_(what, timeout, 0,
                   [&](Completions& q, Token t) { return issue(issueCtx, CompletionTarget(q), t); },
                   [&](const Completion& c, std::span<const uint8_t> p) {
                       out = c;
                       if (text != nullptr) text->assign(reinterpret_cast<const char*>(p.data()), p.size());
                   });
    return out;
}

// ---------------------------------------------------------------------------
// F8c C3: control ops — app-thread issue path
// ---------------------------------------------------------------------------

OpResult Client::issueControl_(OpKind op, CompletionTarget target, Token token,
                               std::span<const uint8_t> frame, uint32_t payloadHint, uint32_t requestIdAt) {
    if (token > kMaxToken) return OpResult::InvalidArgument;
    if (target.cq == nullptr && target.handler.fn == nullptr) return OpResult::InvalidArgument;
    if (frame.size() > agent_.maxFrameLength()) return OpResult::InvalidArgument;
    // Liveness: a record the command ring can never carry is not "full" (a
    // retry would loop forever) — it is rejected here, synchronously.
    if (sizeof(ControlOpHeader) + frame.size() > agent_.commandMsgLimit()) return OpResult::InvalidArgument;
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && !onDuty) return OpResult::WrongThread;

    ControlOpHeader h;
    h.token       = token;
    h.op          = op;
    h.requestIdAt = requestIdAt;
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
    return defineTopic(name, partitions, keyField, validate, descriptorBlob,
                       Persistence::Disk, target, token);
}

OpResult Client::defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                             bool validate, std::span<const uint8_t> descriptorBlob,
                             Persistence persistence, CompletionTarget target, Token token) {
    return defineTopic(name, partitions, keyField, descriptorBlob, validate, persistence, TopicOptions{},
                       DdlOptions{}, target, token);
}

OpResult Client::defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                             std::span<const uint8_t> descriptorBlob, bool validate, Persistence persistence,
                             const TopicOptions& topic, const DdlOptions& ddl, CompletionTarget target,
                             Token token) {
    namespace rpc = rune::protocol::client;
    if (persistence != Persistence::Disk && persistence != Persistence::Transient)
        return OpResult::InvalidArgument;   // wire decode rejects >1; never cast garbage
    rpc::DefineTopic msg;
    msg.sessionId         = 0;
    msg.correlationId     = 0;
    msg.topicName         = name;
    msg.partitionCount    = partitions;
    msg.keyField          = keyField;
    msg.validate          = validate;
    msg.descriptorBlob    = descriptorBlob.data();
    msg.descriptorLen     = static_cast<uint32_t>(descriptorBlob.size());   // 0 = an untyped topic
    msg.persistence       = static_cast<uint8_t>(persistence);
    msg.replicationFactor = topic.replicationFactor;
    msg.config            = rune::protocol::convertTopicConfig<rpc::TopicConfigWire>(topic.config);
    msg.hasExt            = true;   // catalog history: every DDL carries the extension
    msg.ext               = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;   // a field does not fit its wire width
    return issueDdl_(OpKind::DefineTopic, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

OpResult Client::getTopicSchema(std::string_view name, CompletionTarget target, Token token) {
    return getTopicSchema(name, /*requestedFingerprint=*/0, target, token);
}

OpResult Client::getTopicSchema(std::string_view name, uint64_t requestedFingerprint,
                                CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::GetTopicSchema msg;
    msg.sessionId             = 0;
    msg.correlationId         = 0;
    msg.topicName             = name;
    msg.requestedFingerprint  = requestedFingerprint;
    std::vector<uint8_t> frame(msg.encodedSize() + 32u);
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueControl_(OpKind::GetTopicSchema, target, token, std::span<const uint8_t>(frame.data(), n), 0);
}

OpResult Client::alterTopic(std::string_view name, uint16_t expectedVersion,
                            std::span<const uint8_t> opsBlob, CompletionTarget target, Token token) {
    return alterTopic(name, expectedVersion, opsBlob, DdlOptions{}, target, token);
}

OpResult Client::alterTopic(std::string_view name, uint16_t expectedVersion, std::span<const uint8_t> opsBlob,
                            const DdlOptions& ddl, CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::AlterTopic msg;
    msg.sessionId       = 0;
    msg.correlationId   = 0;
    msg.topicName       = name;
    msg.expectedVersion = expectedVersion;
    msg.opsBlob         = opsBlob.data();
    msg.opsLen          = static_cast<uint32_t>(opsBlob.size());
    msg.hasExt          = true;
    msg.ext             = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueDdl_(OpKind::AlterTopic, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

OpResult Client::listTopics(CompletionTarget target, Token token) {
    return issueControl_(OpKind::ListTopics, target, token, {}, 0);
}

OpResult Client::createView(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                            std::span<const uint8_t> planBlob, CompletionTarget target, Token token) {
    return createView(viewName, keyField, persistence, planBlob, DdlOptions{}, target, token);
}

OpResult Client::createView(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                            std::span<const uint8_t> planBlob, const DdlOptions& ddl, CompletionTarget target,
                            Token token) {
    namespace rpc = rune::protocol::client;
    rpc::CreateView msg;
    msg.sessionId     = 0;
    msg.correlationId = 0;
    msg.viewName      = viewName;
    msg.keyField      = keyField;
    msg.persistence   = persistence;
    msg.planBlob      = planBlob.data();
    msg.planLen       = static_cast<uint32_t>(planBlob.size());
    msg.hasExt        = true;
    msg.ext           = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueDdl_(OpKind::CreateView, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

// ---------------------------------------------------------------------------
// Catalog history (Task 5): the remaining DDL ops + the DDL extension
// ---------------------------------------------------------------------------

uint64_t Client::drawClientInstanceId_() {
    // F14: a random 64-bit id per Client instance (two 32-bit draws), never 0 —
    // the requestId's hi word; {0, 0} means "none" on the wire.
    std::random_device rd;
    uint64_t id = 0;
    while (id == 0) id = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    return id;
}

rune::protocol::client::DdlExtension Client::ddlExt_(const DdlOptions& ddl) const noexcept {
    rune::protocol::client::DdlExtension e;
    e.statement  = ddl.statement;
    e.clientName = ddl.clientName.empty() ? std::string_view(cfg_.clientName) : std::string_view(ddl.clientName);
    e.requestId  = ddl.requestId;   // none() = stamped by the duty thread (issueDdl_)
    if (ddl.replayedFrom) {
        e.hasReplayedFrom   = 1;
        e.replayedClusterId = ddl.replayedFrom->clusterId;
        e.replayedOffset    = ddl.replayedFrom->offset;
        e.replayedPrincipal = ddl.replayedFrom->principal;
    }
    return e;
}

namespace {
// Review M2: where the requestId of an encoded DDL frame of `op` sits, found by
// DECODING the frame (the decoded clientName view ends right before it) — the
// check on the encoder's extOffset() before the duty thread stamps there.
std::optional<std::size_t> decodedRequestIdOffset(rune::client::OpKind op, std::span<const uint8_t> f) {
    namespace rpc = rune::protocol::client;
    using rune::client::OpKind;
    std::optional<rpc::DdlExtension> ext;
    switch (op) {
    case OpKind::DefineTopic:      if (auto m = rpc::DefineTopic::decode(f.data(), f.size()); m && m->hasExt) ext = m->ext; break;
    case OpKind::AlterTopic:       if (auto m = rpc::AlterTopic::decode(f.data(), f.size()); m && m->hasExt) ext = m->ext; break;
    case OpKind::CreateView:       if (auto m = rpc::CreateView::decode(f.data(), f.size()); m && m->hasExt) ext = m->ext; break;
    case OpKind::DropTopic:        if (auto m = rpc::DropTopic::decode(f.data(), f.size())) ext = m->ext; break;
    case OpKind::DropView:         if (auto m = rpc::DropView::decode(f.data(), f.size())) ext = m->ext; break;
    case OpKind::AlterView:        if (auto m = rpc::AlterView::decode(f.data(), f.size())) ext = m->ext; break;
    case OpKind::AlterTopicConfig: if (auto m = rpc::AlterTopicConfig::decode(f.data(), f.size())) ext = m->ext; break;
    default: break;
    }
    if (!ext) return std::nullopt;
    const auto* end = reinterpret_cast<const uint8_t*>(ext->clientName.data()) + ext->clientName.size();
    return static_cast<std::size_t>(end - f.data());
}
} // namespace

OpResult Client::issueDdl_(OpKind op, CompletionTarget target, Token token, std::span<const uint8_t> frame,
                           const rune::protocol::client::DdlExtension& ext, std::size_t extOffset) {
    namespace rpc = rune::protocol::client;
    uint32_t requestIdAt = 0;
    if (ext.requestId.none()) {
        // The encoder says where its extension starts (M2) — never re-derived
        // here — and a decode of the very frame must agree before the duty
        // thread stamps 16 bytes there.
        const std::size_t at = extOffset + rpc::extRequestIdOffset(ext);
        const auto decoded = decodedRequestIdOffset(op, frame);
        if (!decoded || *decoded != at || at + 16u > frame.size()) return OpResult::InvalidArgument;
        requestIdAt = static_cast<uint32_t>(at);
        if (testSkewDdlRequestIdAt_ != 0) [[unlikely]] {   // F4 test seam (issuing thread only)
            requestIdAt = static_cast<uint32_t>(frame.size()) + testSkewDdlRequestIdAt_;
            testSkewDdlRequestIdAt_ = 0;
        }
    }
    return issueControl_(op, target, token, frame, 0, requestIdAt);
}

OpResult Client::dropTopic(std::string_view name, const DdlOptions& ddl, CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::DropTopic msg;
    msg.topicName = name;
    msg.ext       = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueDdl_(OpKind::DropTopic, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

OpResult Client::dropView(std::string_view name, const DdlOptions& ddl, CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::DropView msg;
    msg.viewName = name;
    msg.ext      = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueDdl_(OpKind::DropView, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

OpResult Client::alterView(std::string_view name, std::string_view keyField, uint8_t persistence,
                           uint32_t expectedVersion, std::span<const uint8_t> planBlob, const DdlOptions& ddl,
                           CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::AlterView msg;
    msg.viewName        = name;
    msg.keyField        = keyField;
    msg.persistence     = persistence;
    msg.expectedVersion = expectedVersion;
    msg.planBlob        = planBlob.data();
    msg.planLen         = static_cast<uint32_t>(planBlob.size());
    msg.ext             = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueDdl_(OpKind::AlterView, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

OpResult Client::alterTopicConfig(std::string_view name, uint32_t expectedVersion,
                                  std::span<const rune::protocol::client::ConfigChangeWire> changes,
                                  const DdlOptions& ddl, CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    if (changes.size() > 0xFFFFu) return OpResult::InvalidArgument;   // the count is a u16
    std::vector<uint8_t> block(rpc::configChangesEncodedSize(changes.size()));
    if (rpc::putConfigChanges(block.data(), changes) != block.size()) return OpResult::InvalidArgument;
    rpc::AlterTopicConfig msg;
    msg.topicName       = name;
    msg.expectedVersion = expectedVersion;
    msg.changes         = std::span<const uint8_t>(block.data(), block.size());
    msg.ext             = ddlExt_(ddl);
    std::vector<uint8_t> frame(msg.encodedSize());
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueDdl_(OpKind::AlterTopicConfig, target, token, std::span<const uint8_t>(frame.data(), n), msg.ext,
                     msg.extOffset());
}

OpResult Client::getViewRow(int32_t viewTopicId, uint64_t keyU64, uint64_t requestedFingerprint,
                            CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::GetViewRow msg;
    msg.sessionId            = 0;
    msg.correlationId        = 0;
    msg.viewTopicId          = viewTopicId;
    msg.keyU64               = keyU64;
    msg.requestedFingerprint = requestedFingerprint;
    uint8_t frame[rpc::GetViewRow::ENCODED_SIZE];
    const std::size_t n = msg.encode(frame);
    return issueControl_(OpKind::GetViewRow, target, token, std::span<const uint8_t>(frame, n), 0);
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
    // close()'s final drain, or T15b: failed for good (no session can answer) —
    // an accepted op completes exactly once, Closed.
    if (closing_ || transportFailed_) {
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
    // Catalog history (fix round 0, A): a DDL has its own deadline — the whole
    // op, its Redirect hops and its NotLeader budget (budgetEnd <= deadline).
    p.deadlineNs = nowNs + (isDdlOp(h.op) ? cfg_.ddlTimeoutMs : cfg_.pingTimeoutMs) * 1'000'000LL;
    if (h.op == OpKind::DefineTopic) {
        if (auto d = rpc::DefineTopic::decode(frame.data(), frame.size())) p.topicName = std::string(d->topicName);
    }
    ctlScratch_.assign(frame.begin(), frame.end());
    rpc::detail::putLE64(ctlScratch_.data(), 8, machine.sessionId());
    rpc::detail::putLE64(ctlScratch_.data(), 16, p.corrId);
    if (isRoutedCtlOp(h.op)) {
        // Catalog history (F14): the requestId is stamped HERE, by the duty
        // thread that owns the counter, then kept with the frame — every resend
        // (Redirect / NotLeader / a lost session) re-offers these exact bytes.
        DdlPending d;
        if (h.requestIdAt != 0) {
            if (std::size_t{h.requestIdAt} + 16u > ctlScratch_.size()) [[unlikely]] {
                // F4: the stamp would not fit (issue-time decode check bypassed):
                // fail the op explicitly — never send it as requestId {0,0},
                // which no leader could dedupe.
                constexpr std::string_view kWhy =
                    "DDL requestId stamp offset lies outside the frame: not sent (it would carry requestId {0,0})";
                completeCtl_(p, OpStatus::InvalidArgument, ddlNoValue(p.op), 0, 0,
                             std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(kWhy.data()), kWhy.size()));
                return;
            }
            if (++requestCounter_ == 0) ++requestCounter_;   // lo is never 0 (requestIdOf: 0 = explicit)
            d.requestLo = requestCounter_;
            rpc::detail::putLE64(ctlScratch_.data(), static_cast<int>(h.requestIdAt),
                                 static_cast<int64_t>(clientInstanceId_));
            rpc::detail::putLE64(ctlScratch_.data(), static_cast<int>(h.requestIdAt + 8u),
                                 static_cast<int64_t>(d.requestLo));
        }
        d.frame = ctlScratch_;
        DdlOfferedOn on;
        switch (offerDdl_(d, nowNs, on)) {
        case transport::AeronTransport::OfferResult::Accepted: d.retry = DdlSent{0, on}; break;
        case transport::AeronTransport::OfferResult::Retry:    d.retry = DdlUnsentOnSeed{}; break;   // the pump re-offers
        case transport::AeronTransport::OfferResult::Fatal:
            p.ddl = std::move(d);
            completeCtl_(p, OpStatus::NotConnected);
            return;
        }
        p.ddl = std::move(d);
        countDdl_(p, +1);
        pendingCtl_.push_back(std::move(p));
        return;
    }
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
                          std::span<const uint8_t> payload, detail::CompletionRing::Overflow ov,
                          uint32_t reserved0) {
    Completion c;
    c.token     = p.token;
    c.value     = value;
    c.aux       = aux;
    c.op        = p.op;
    c.status    = st;
    c.errorCode = errorCode;
    c.reserved0 = reserved0;   // GetTopicSchema: partitionCount (Phase 2b C)
    // Catalog history: a DDL reports its generated requestId's lo (requestIdOf).
    if (const auto* d = std::get_if<DdlPending>(&p.ddl)) c.reserved0 = d->requestLo;
    deliver_(p.target, c, payload, ov);
}

Client::PendingCtl Client::takeCtl_(std::size_t i) {
    // Ordered erase (m3): unsent frames keep their order; the table is small.
    PendingCtl p = std::move(pendingCtl_[i]);
    pendingCtl_.erase(pendingCtl_.begin() + static_cast<std::ptrdiff_t>(i));
    if (!p.unsentFrame.empty()) --unsentCtl_;
    countDdl_(p, -1);
    return p;
}

// ---------------------------------------------------------------------------
// Catalog history (Task 5): a pending DDL's answers — Redirect, NotLeader,
// resend pump.  Duty thread; cold.  Every path ends in exactly one completion.
// ---------------------------------------------------------------------------

void Client::setDdlRetry_(DdlPending& d, DdlRetry next) noexcept {
    ddlAwaitingResend_ += (ddlWaitsForResend(next) ? 1 : 0) - (ddlWaitsForResend(d.retry) ? 1 : 0);
    d.retry = std::move(next);
}

void Client::countDdl_(const PendingCtl& p, int sign) noexcept {
    const auto* d = std::get_if<DdlPending>(&p.ddl);
    if (d == nullptr) return;
    ddlPending_ += sign;
    if (ddlWaitsForResend(d->retry)) ddlAwaitingResend_ += sign;
}

void Client::failControlOnSeedLoss_(uint16_t errorCode, std::span<const uint8_t> text) {
    for (std::size_t i = pendingCtl_.size(); i-- > 0;) {
        if (std::holds_alternative<DdlPending>(pendingCtl_[i].ddl)) continue;   // follows its own session (I1)
        PendingCtl p = takeCtl_(i);
        completeCtl_(p, OpStatus::SessionLost, 0, 0, errorCode, text);
    }
}

transport::AeronTransport::OfferResult Client::offerDdl_(DdlPending& d, int64_t nowNs, DdlOfferedOn& on) {
    uint32_t idx = 0;
    const auto r = agent_.offerToEndpoint(d.endpoint, d.frame.data(), d.frame.size(), nowNs, idx);
    if (r == transport::AeronTransport::OfferResult::Accepted) on = DdlOfferedOn{idx, agent_.connectedEpoch(idx)};
    return r;
}

void Client::failDdl_(std::size_t i, uint16_t code, std::string_view detail, int64_t value, int64_t aux) {
    PendingCtl p = takeCtl_(i);   // removed first: the delivery may issue a new op
    // GetViewRow's errorCode namespace is the ViewRowResponse status, where 2 is
    // NotReady and 3 SchemaMismatch: a routing failure (NotLeader 2 / Redirect 3)
    // is the client-side kNotLeader, never mistaken for either.
    if (p.op == OpKind::GetViewRow && (code == 2 || code == 3)) code = ViewRowResult::kNotLeader;
    completeCtl_(p, OpStatus::BrokerError, value, aux, code,
                 std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(detail.data()), detail.size()));
}

namespace {
constexpr int64_t kDdlInitialBackoffNs = 100'000'000;     // 100 ms
constexpr int64_t kDdlMaxBackoffNs     = 1'000'000'000;   // 1 s
} // namespace

void Client::onDdlRedirect_(std::size_t i, std::string_view endpoint, int64_t nowNs) {
    using Offer = transport::AeronTransport::OfferResult;
    PendingCtl& p = pendingCtl_[i];
    DdlPending& d = std::get<DdlPending>(p.ddl);
    if (endpoint.empty()) {   // a Redirect that names no node: nothing to follow — retriable as NotLeader
        onDdlNotLeader_(i, "redirect without a leader endpoint", nowNs);
        return;
    }
    // Only an in-flight DDL is answered; one waiting for its resend has no
    // request out (a duplicate answer) — its own pump / deadline ends it.
    int redirects = 0;
    std::optional<DdlNotLeaderSeen> nl;
    if (const auto* st = std::get_if<DdlSent>(&d.retry)) redirects = st->redirects;
    else if (const auto* n = std::get_if<DdlNotLeaderSeen>(&d.retry)) { redirects = n->redirects; nl = *n; }
    else return;
    if (++redirects > kMaxDdlRedirects) {
        const std::string why = "redirected more than " + std::to_string(kMaxDdlRedirects) + " times";
        failDdl_(i, static_cast<uint16_t>(rune::protocol::client::ErrorCode::Redirect), why, ddlNoValue(p.op));
        return;
    }
    // The leader's own session (the seed never moves for a DDL: it carries the
    // data ops / subscriptions); the same frame, so the same requestId.
    d.endpoint = toChannel(std::string(endpoint));
    DdlOfferedOn on;
    switch (offerDdl_(d, nowNs, on)) {
    case Offer::Accepted:
        if (cfg_.traceDdlRetries) noteDdlRetry_({DdlRetryNote::Kind::Redirect, redirects, 0, std::string(endpoint)});
        if (nl) setDdlRetry_(d, DdlNotLeaderSeen{redirects, nl->budgetEndNs, nl->backoffNs, on});
        else    setDdlRetry_(d, DdlSent{redirects, on});
        return;
    case Offer::Retry:   // that node's session is still connecting: the pump re-offers it
        if (cfg_.traceDdlRetries) noteDdlRetry_({DdlRetryNote::Kind::Redirect, redirects, 0, std::string(endpoint)});
        if (nl) setDdlRetry_(d, DdlAwaitingResend{redirects, nl->budgetEndNs, nl->backoffNs, nowNs});
        else    setDdlRetry_(d, DdlHeldForNode{redirects});
        return;
    case Offer::Fatal: {   // the transport can never carry it
        PendingCtl dead = takeCtl_(i);
        completeCtl_(dead, OpStatus::NotConnected);
        return;
    }
    }
}

void Client::onDdlNotLeader_(std::size_t i, std::string_view detail, int64_t nowNs) {
    PendingCtl& p = pendingCtl_[i];
    DdlPending& d = std::get<DdlPending>(p.ddl);
    int     redirects   = 0;
    int64_t budgetEndNs = 0;
    int64_t backoffNs   = 0;
    if (const auto* st = std::get_if<DdlSent>(&d.retry)) {
        redirects   = st->redirects;
        budgetEndNs = std::min(p.deadlineNs, nowNs + static_cast<int64_t>(
                                                         std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                             cfg_.ddlRetryBudget).count()));
        backoffNs   = kDdlInitialBackoffNs;
    } else if (const auto* n = std::get_if<DdlNotLeaderSeen>(&d.retry)) {
        redirects   = n->redirects;
        budgetEndNs = n->budgetEndNs;
        backoffNs   = std::min(n->backoffNs * 2, kDdlMaxBackoffNs);
    } else {
        return;   // not in flight (a duplicate answer): its pump / deadline ends it
    }
    if (nowNs >= budgetEndNs) {   // spec §5.5: retried within the budget, then an explicit failure
        std::string why(detail);
        why += why.empty() ? "not leader: DDL retry budget exhausted" : " (DDL retry budget exhausted)";
        failDdl_(i, static_cast<uint16_t>(rune::protocol::client::ErrorCode::NotLeader), why, ddlNoValue(p.op));
        return;
    }
    // The last resend lands at the budget's end at the latest; its answer decides.
    if (cfg_.traceDdlRetries)
        noteDdlRetry_({DdlRetryNote::Kind::NotLeader, redirects, std::min(backoffNs, budgetEndNs - nowNs) / 1'000'000,
                       std::string(detail)});
    setDdlRetry_(d, DdlAwaitingResend{redirects, budgetEndNs, backoffNs, std::min(nowNs + backoffNs, budgetEndNs)});
}

void Client::noteDdlRetry_(DdlRetryNote note) {
    if (ddlRetryNotes_.size() >= kMaxDdlRetryNotes) { ++ddlRetryNotesDropped_; return; }
    ddlRetryNotes_.push_back(std::move(note));
}

std::size_t Client::takeDdlRetryNotes(std::vector<DdlRetryNote>& out) {
    if (cfg_.runnerMode != RunnerMode::Invoker || std::this_thread::get_id() != ownerThread_)
        throw std::logic_error("Client::takeDdlRetryNotes: Invoker mode, from the owner thread only");
    for (auto& n : ddlRetryNotes_) out.push_back(std::move(n));
    ddlRetryNotes_.clear();
    return std::exchange(ddlRetryNotesDropped_, 0);
}

int Client::driveDdl_(int64_t nowNs) {
    using Offer = transport::AeronTransport::OfferResult;
    int work = 0;
    for (std::size_t i = 0; i < pendingCtl_.size();) {
        PendingCtl& p = pendingCtl_[i];
        auto* d = std::get_if<DdlPending>(&p.ddl);
        if (d == nullptr) { ++i; continue; }
        // I1: an in-flight DDL whose session left its connect episode (a
        // session-fatal answer naming it, a stale loss, a transport drop, a
        // reconnect under a new id) can never be answered there: it is re-sent
        // toward the seed — the SAME frame, so the SAME requestId (the new
        // leader's dedupe answers the original result) — inside the NotLeader
        // budget (started now if no NotLeader was seen yet).
        const DdlOfferedOn* on = nullptr;
        int redirects = 0;
        int64_t budgetEndNs = 0, backoffNs = kDdlInitialBackoffNs;
        if (const auto* st = std::get_if<DdlSent>(&d->retry)) {
            on = &st->on;
            redirects = st->redirects;
            budgetEndNs = std::min(p.deadlineNs, nowNs + static_cast<int64_t>(
                                                             std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                                 cfg_.ddlRetryBudget).count()));
        } else if (const auto* n = std::get_if<DdlNotLeaderSeen>(&d->retry)) {
            on = &n->on;
            redirects = n->redirects;
            budgetEndNs = n->budgetEndNs;
            backoffNs = n->backoffNs;
        }
        if (on != nullptr) {
            if (agent_.connectedEpoch(on->sessionIndex) != on->sessionEpoch) [[unlikely]] {
                d->endpoint.clear();   // toward the seed: it redirects to the current leader
                setDdlRetry_(*d, DdlAwaitingResend{redirects, budgetEndNs, backoffNs, nowNs,
                                                   DdlResendCause::SessionLost});
                ++work;
            } else {
                ++i;
                continue;
            }
        }
        DdlOfferedOn sentOn;
        if (const auto* a = std::get_if<DdlAwaitingResend>(&d->retry)) {
            if (nowNs < a->resendAtNs) { ++i; continue; }
            const DdlAwaitingResend due = *a;
            switch (offerDdl_(*d, nowNs, sentOn)) {
            case Offer::Accepted:
                setDdlRetry_(*d, DdlNotLeaderSeen{due.redirects, due.budgetEndNs, due.backoffNs, sentOn});
                ++work;
                ++i;
                break;
            case Offer::Retry:   // refused (its node's session is not up): every cycle, inside the budget
                // A budget that ends at the DDL deadline itself is the deadline:
                // the sweep ends it Timeout (the DDL timed out); a shorter
                // budget ends it NotLeader here.
                if (nowNs >= due.budgetEndNs && nowNs < p.deadlineNs) {
                    if (due.cause == DdlResendCause::SessionLost) {
                        // F1: the DDL's session was lost and never came back
                        // inside the budget — a session loss, not a NotLeader.
                        constexpr std::string_view kWhy =
                            "session lost: the DDL's session was not re-established within the DDL retry budget "
                            "(it may have been applied; a retry with the same requestId is exact)";
                        PendingCtl dead = takeCtl_(i);
                        completeCtl_(dead, OpStatus::SessionLost, ddlNoValue(dead.op), 0, 0,
                                     std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(kWhy.data()),
                                                              kWhy.size()));
                    } else {
                        failDdl_(i, static_cast<uint16_t>(rune::protocol::client::ErrorCode::NotLeader),
                                 "not leader: DDL retry budget exhausted (the node could not be reached)",
                                 ddlNoValue(p.op));
                    }
                    ++work;
                } else {
                    ++i;
                }
                break;
            case Offer::Fatal: {
                PendingCtl dead = takeCtl_(i);
                completeCtl_(dead, OpStatus::NotConnected);
                ++work;
                break;
            }
            }
            continue;
        }
        // DdlHeldForNode / DdlUnsentOnSeed: re-offered every cycle; the DDL deadline bounds them.
        int heldRedirects = 0;
        if (const auto* hld = std::get_if<DdlHeldForNode>(&d->retry)) heldRedirects = hld->redirects;
        else if (!std::holds_alternative<DdlUnsentOnSeed>(d->retry)) { ++i; continue; }
        switch (offerDdl_(*d, nowNs, sentOn)) {
        case Offer::Accepted:
            setDdlRetry_(*d, DdlSent{heldRedirects, sentOn});
            ++work;
            ++i;
            break;
        case Offer::Retry:
            ++i;
            break;
        case Offer::Fatal: {
            PendingCtl dead = takeCtl_(i);
            completeCtl_(dead, OpStatus::NotConnected);
            ++work;
            break;
        }
        }
    }
    return work;
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
    // Follower-connect hang fix (whole class): a pending connect whose seed
    // machine GAVE UP — back in Disconnected after its redirect budget ran out,
    // a connect error, or its own connect timeout — can never be answered.  It
    // ends now, explicitly NotConnected, never by waiting out its deadline.
    // (connect() moves the machine out of Disconnected before the op is pending;
    // Backoff — reconnect-forever — keeps trying and stays deadline-bounded.)
    if (hasPendingConnect_ &&
        agent_.seedMachine().state() == session::SessionState::Disconnected) [[unlikely]] {
        static constexpr std::string_view kGaveUp =
            "connect() gave up (redirect budget exhausted or connect refused): not connected";
        completeConnect_(OpStatus::NotConnected, 0, 0, kGaveUp);
        if (cbError_) invokeUserCallback_("onError", [&] { cbError_(std::string(kGaveUp)); });
    }
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
    case OpStatus::InvalidArgument:
        msg += ": invalid argument";
        if (!payload.empty()) msg += ": " + std::string(reinterpret_cast<const char*>(payload.data()), payload.size());
        break;
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

void Client::checkBlockingCaller_(const char* what) const {
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
}

template <class Issue, class OnDone>
void Client::andWait_(const char* what, std::chrono::nanoseconds timeout, uint32_t maxResultBytes,
                      Issue&& issue, OnDone&& onDone) {
    checkBlockingCaller_(what);
    CompletionsConfig qc;
    qc.maxResultBytes = std::max<uint32_t>(maxResultBytes, qc.maxErrorText);
    uint32_t cap = 1u << 16;
    while (cap / 8 < qc.maxResultBytes + 64u) cap <<= 1;
    qc.capacityBytes = cap;
    Completions q(*this, qc);

    // m6: every call has its OWN fresh queue (above) and the I3 quarantine makes
    // cross-queue delivery impossible, so a fixed token can never be answered
    // by an earlier call (Task 5 fix round 0: no per-thread counter).
    constexpr Token kToken = 1;
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

// ---------------------------------------------------------------------------
// Catalog history (Task 13): catalogHistory / catalogAsOf / catalogDiff
// ---------------------------------------------------------------------------

CatalogReader& Client::catalogReader() {
    // Hard rule 1: the reader is built in the constructor and the pointer never changes
    // (const) — no lazy creation, no first-caller race.  Its owner thread is the
    // constructing thread; any other thread's call throws std::logic_error HERE, in
    // every runner mode (fix round 2: the reference itself is never handed out).
    static_assert(std::is_const_v<decltype(catalogReader_)>, "the catalog reader must be created eagerly and never reassigned");
    catalogReader_->checkOwner_();
    return *catalogReader_;
}

namespace {
CatalogReadError asOfError(const rune::catalog::ReplayError& e) {
    return CatalogReadError{CatalogReadError::Kind::Replay, rune::catalog::describe(e), e.offset, 0};
}
} // namespace

CatalogResult<std::vector<rune::catalog::RecordedEvent>> Client::catalogHistory(
    const rune::catalog::HistoryFilter& filter, std::chrono::milliseconds timeout) {
    CatalogReader& r = catalogReader();
    if (auto ok = r.refresh(timeout); !ok) return std::unexpected(std::move(ok.error()));
    const auto matches = rune::catalog::history(r.events(), filter);
    std::vector<rune::catalog::RecordedEvent> out;
    out.reserve(matches.size());
    for (const rune::catalog::RecordedEvent* e : matches) out.push_back(*e);
    return out;
}

CatalogResult<rune::catalog::CatalogState> Client::catalogAsOf(const rune::catalog::AsOf& at,
                                                               std::chrono::milliseconds timeout) {
    CatalogReader& r = catalogReader();
    if (auto ok = r.refresh(timeout); !ok) return std::unexpected(std::move(ok.error()));
    auto s = rune::catalog::stateAsOf(r.events(), at);
    if (!s) return std::unexpected(asOfError(s.error()));
    return std::move(*s);
}

CatalogResult<std::vector<rune::catalog::DiffEntry>> Client::catalogDiff(const rune::catalog::AsOf& a,
                                                                         const rune::catalog::AsOf& b,
                                                                         std::chrono::milliseconds timeout) {
    CatalogReader& r = catalogReader();
    if (auto ok = r.refresh(timeout); !ok) return std::unexpected(std::move(ok.error()));
    auto sa = rune::catalog::stateAsOf(r.events(), a);
    if (!sa) return std::unexpected(asOfError(sa.error()));
    auto sb = rune::catalog::stateAsOf(r.events(), b);
    if (!sb) return std::unexpected(asOfError(sb.error()));
    return rune::catalog::diff(*sa, *sb);
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
    return defineTopicAndWait(name, partitions, keyField, validate, descriptorBlob,
                              Persistence::Disk, timeout);
}

DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions,
                                             std::string_view keyField, bool validate,
                                             std::span<const uint8_t> descriptorBlob,
                                             Persistence persistence,
                                             std::chrono::nanoseconds timeout) {
    DefineTopicResult result;
    andWait_("defineTopic()", timeout, 0,
             [&](Completions& q, Token t) {
                 return defineTopic(name, partitions, keyField, validate, descriptorBlob,
                                    persistence, q, t);
             },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("defineTopic()", c, p);
                 result.topicId   = static_cast<int32_t>(c.value);
                 result.version   = static_cast<uint16_t>(c.aux);
                 result.errorCode = c.errorCode;
                 result.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
                 result.requestId = requestIdOf(c);
             });
    return result;
}

DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions, std::string_view keyField,
                                             std::span<const uint8_t> descriptorBlob, bool validate,
                                             Persistence persistence, const TopicOptions& topic,
                                             const DdlOptions& ddl, std::chrono::milliseconds timeout) {
    DefineTopicResult result;
    andWait_("defineTopic()", timeout, 0,
             [&](Completions& q, Token t) {
                 return defineTopic(name, partitions, keyField, descriptorBlob, validate, persistence, topic, ddl,
                                    q, t);
             },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("defineTopic()", c, p);
                 result.topicId   = static_cast<int32_t>(c.value);
                 result.version   = static_cast<uint16_t>(c.aux);
                 result.errorCode = c.errorCode;
                 result.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

TopicSchemaResult Client::getTopicSchemaAndWait(std::string_view name, std::chrono::nanoseconds timeout) {
    return getTopicSchemaAndWait(name, /*requestedFingerprint=*/0, timeout);
}

TopicSchemaResult Client::getTopicSchemaAndWait(std::string_view name, uint64_t requestedFingerprint,
                                                std::chrono::nanoseconds timeout) {
    TopicSchemaResult result;
    andWait_("getTopicSchema()", timeout, 64u << 10,
             [&](Completions& q, Token t) { return getTopicSchema(name, requestedFingerprint, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("getTopicSchema()", c, p);
                 result.topicId        = static_cast<int32_t>(c.value);
                 result.version        = static_cast<uint16_t>(c.aux);
                 result.errorCode      = c.errorCode;
                 result.partitionCount = static_cast<int32_t>(c.reserved0);  // Phase 2b C
                 if (c.status == OpStatus::Ok) result.descriptorBlob.assign(p.begin(), p.end());
             });
    return result;
}

AlterTopicResult Client::alterTopicAndWait(std::string_view name, uint16_t expectedVersion,
                                           std::span<const uint8_t> opsBlob,
                                           std::chrono::nanoseconds timeout) {
    AlterTopicResult result;
    andWait_("alterTopic()", timeout, 0,
             [&](Completions& q, Token t) { return alterTopic(name, expectedVersion, opsBlob, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("alterTopic()", c, p);
                 result.topicId    = static_cast<int32_t>(c.value);
                 result.newVersion = static_cast<uint16_t>(c.aux);
                 result.errorCode  = c.errorCode;
                 result.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
                 result.requestId = requestIdOf(c);
             });
    return result;
}

AlterTopicResult Client::alterTopicAndWait(std::string_view name, uint16_t expectedVersion,
                                           std::span<const uint8_t> opsBlob, const DdlOptions& ddl,
                                           std::chrono::milliseconds timeout) {
    AlterTopicResult result;
    andWait_("alterTopic()", timeout, 0,
             [&](Completions& q, Token t) { return alterTopic(name, expectedVersion, opsBlob, ddl, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("alterTopic()", c, p);
                 result.topicId    = static_cast<int32_t>(c.value);
                 result.newVersion = static_cast<uint16_t>(c.aux);
                 result.errorCode  = c.errorCode;
                 result.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

CreateViewResult Client::createViewAndWait(std::string_view viewName, std::string_view keyField,
                                           uint8_t persistence, std::span<const uint8_t> planBlob,
                                           std::chrono::nanoseconds timeout) {
    CreateViewResult result;
    andWait_("createView()", timeout, 0,
             [&](Completions& q, Token t) { return createView(viewName, keyField, persistence, planBlob, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("createView()", c, p);
                 result.viewTopicId       = static_cast<int32_t>(c.value);
                 result.outputFingerprint = static_cast<uint64_t>(c.aux);
                 result.errorCode         = c.errorCode;
                 result.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
                 result.requestId = requestIdOf(c);
             });
    return result;
}

CreateViewResult Client::createViewAndWait(std::string_view viewName, std::string_view keyField,
                                           uint8_t persistence, std::span<const uint8_t> planBlob,
                                           const DdlOptions& ddl, std::chrono::milliseconds timeout) {
    CreateViewResult result;
    andWait_("createView()", timeout, 0,
             [&](Completions& q, Token t) { return createView(viewName, keyField, persistence, planBlob, ddl, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("createView()", c, p);
                 result.viewTopicId       = static_cast<int32_t>(c.value);
                 result.outputFingerprint = static_cast<uint64_t>(c.aux);
                 result.errorCode         = c.errorCode;
                 result.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

namespace {
// Catalog history: the DdlResult of a DropTopic / DropView / AlterView /
// AlterTopicConfig completion (Ok or BrokerError).
DdlResult ddlResultOf(const Completion& c, std::span<const uint8_t> p) {
    DdlResult r;
    r.errorCode  = c.errorCode;
    r.objectId   = static_cast<uint64_t>(c.value);
    r.newVersion = static_cast<uint32_t>(c.aux);
    r.detail.assign(reinterpret_cast<const char*>(p.data()), p.size());
    return r;
}
} // namespace

DdlResult Client::dropTopicAndWait(std::string_view name, const DdlOptions& ddl, std::chrono::milliseconds timeout) {
    DdlResult result;
    andWait_("dropTopic()", timeout, 0,
             [&](Completions& q, Token t) { return dropTopic(name, ddl, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("dropTopic()", c, p);
                 result = ddlResultOf(c, p);
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

DdlResult Client::dropViewAndWait(std::string_view name, const DdlOptions& ddl, std::chrono::milliseconds timeout) {
    DdlResult result;
    andWait_("dropView()", timeout, 0,
             [&](Completions& q, Token t) { return dropView(name, ddl, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("dropView()", c, p);
                 result = ddlResultOf(c, p);
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

DdlResult Client::alterViewAndWait(std::string_view name, std::string_view keyField, uint8_t persistence,
                                   uint32_t expectedVersion, std::span<const uint8_t> planBlob, const DdlOptions& ddl,
                                   std::chrono::milliseconds timeout) {
    DdlResult result;
    andWait_("alterView()", timeout, 0,
             [&](Completions& q, Token t) {
                 return alterView(name, keyField, persistence, expectedVersion, planBlob, ddl, q, t);
             },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("alterView()", c, p);
                 result = ddlResultOf(c, p);
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

DdlResult Client::alterTopicConfigAndWait(std::string_view name, uint32_t expectedVersion,
                                          std::span<const rune::protocol::client::ConfigChangeWire> changes,
                                          const DdlOptions& ddl, std::chrono::milliseconds timeout) {
    DdlResult result;
    andWait_("alterTopicConfig()", timeout, 0,
             [&](Completions& q, Token t) { return alterTopicConfig(name, expectedVersion, changes, ddl, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("alterTopicConfig()", c, p);
                 result = ddlResultOf(c, p);
                 result.requestId = ddl.requestId.none() ? requestIdOf(c) : ddl.requestId;
             });
    return result;
}

ViewRowResult Client::getViewRowAndWait(int32_t viewTopicId, uint64_t keyU64, uint64_t requestedFingerprint,
                                        std::chrono::nanoseconds timeout) {
    ViewRowResult result;
    andWait_("getViewRow()", timeout, 64u << 10,
             [&](Completions& q, Token t) { return getViewRow(viewTopicId, keyU64, requestedFingerprint, q, t); },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 // A ViewRowResponse status != OK rides as BrokerError/errorCode
                 // (see the dispatch case below) — never thrown; only a real
                 // transport-level failure (Timeout/NotConnected/SessionLost/
                 // Closed) is.
                 if (c.status != OpStatus::BrokerError) throwForStatus_("getViewRow()", c, p);
                 result.status = (c.status == OpStatus::Ok) ? ViewRowResult::kOk : c.errorCode;
                 if (result.status == ViewRowResult::kOk) result.rowBytes.assign(p.begin(), p.end());
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
    if (transportFailed_) [[unlikely]] {
        const int w = failedDutyCycle_(nowNs);
        if (cfg_.shardStatus) publishShardStatus_();
        return w;
    }

    // 1. Agent: driver/conductor (invoker), command ring, egress, session ticks.
    //    ProduceResponses are decoded and buffered, not dispatched through hooks.
    int work = agent_.doWork(nowNs);
    // I3': owner releases fenced on the command ring apply once the drain passed them.
    if (!deferredReleases_.empty()) [[unlikely]] work += applyDeferredReleases_(false);
    // DR client bug 1: a seed whose broker died moves to a live broker.
    work += driveSeedFailover_(nowNs);

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
            // D4: a coordinator session that gave up falls back to the seed
            // BEFORE the session watch below could revive it.
            if (coordIdx_ != 0) [[unlikely]] syncCoordinatorSession_();
            const int64_t gsid = coordSessionId_();  // the coordinator's session (the seed's by default)
            work += groupConsumerHub_.flushRequestedCommits(gsid);
            if (groupConsumerHub_.pendingLeaves() != 0) work += groupConsumerHub_.retryPendingLeaves(gsid);
            // M2: refused fetch Unsubscribes are re-offered every pass until sent / moot.
            if (groupConsumerHub_.pendingFetchUnsubscribes() != 0) work += groupConsumerHub_.retryFetchUnsubscribes();
            // A parked partition wants fresh metadata (backoff-paced refresh).
            if (groupConsumerHub_.takeMetadataWanted()) scheduleMetadataRefresh();
            // M1: the leader-node sessions the group's partitions depend on.
            work += watchGroupSessions_(nowNs);
        }
    }
    // DR-7: a partition route's leader session that fails to connect asks for metadata.
    work += watchProduceRoutes_(nowNs);
    // DR-15: Subscribes redirected to an unreachable node, after fresh metadata.
    if (!redirectedSubscribes_.empty()) [[unlikely]] work += retryRedirectedSubscribes_(nowNs);
    // Catalog history (Task 5): pending DDLs — a lost session's DDL re-sent
    // toward the seed, due resends offered (one branch when none).
    if (ddlPending_ != 0) [[unlikely]] work += driveDdl_(nowNs);
    if (!heldRedirectFollows_.empty()) [[unlikely]] work += releaseHeldRedirectFollows_();   // R3
    // E2 final fix wave (I2): moved subscriptions on their leader node's own
    // session — held Subscribes flushed, a lost session's ones re-armed.
    if (subscriberHub_.homedCount() != 0) [[unlikely]] {
        work += subscriberHub_.driveHomed(
            agent_.seedSession().machine.sessionId(),
            std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.subscribeTimeoutMs));
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
    if (lastCoordState_ != session::SessionState::Connected && !groupConsumerHub_.empty() &&
        groupConsumerHub_.hasPendingCommitSync()) [[unlikely]] {
        groupConsumerHub_.onSessionDown();
    }

    // 4. Periodic sweep on the duty-cycle clock.
    if (nowNs >= nextSweepNs_) {
        sweepTimeouts(nowNs);
        nextSweepNs_ = nowNs + kSweepIntervalMs * 1'000'000LL;
    }

    // 5. F8c C7: Client::close()'s drain, then handle releases — at the very
    //    end of the cycle, outside every hub walk (a released impl is freed
    //    only once nothing references it: never under a running handler).
    if (!drainWaiters_.empty()) [[unlikely]] work += checkDrain_();
    // F8c C7 fix round 2 (F1): orphaned releases (one acquire load per cycle
    // when none exists), applied in FIFO order behind the handle's own ops.
    if (agent_.takeOrphanHint()) [[unlikely]] scanOrphans_();
    if (!orphans_.empty()) [[unlikely]] work += applyDueOrphans_();
    if (!releases_.empty()) [[unlikely]] work += reapReleases_();

    // 6. T15b: the failure path — the Aeron client closed itself (or the seed
    //    got a terminal Rejected answer): fail for good, once.
    if (agent_.transportClosed() || agent_.seedMachine().state() == session::SessionState::Rejected)
        [[unlikely]] {
        enterFailed_();
        ++work;
    }
    // 7. T15b: shard status (ClientShard) — derived and published on change.
    if (cfg_.shardStatus) [[unlikely]] publishShardStatus_();
    return work;
}

void Client::enterFailed_() {
    if (transportFailed_) return;
    transportFailed_ = true;
    const bool rejected = !agent_.transportClosed();
    std::fprintf(stderr,
                 "rune-client: %s — failing for good: every accepted record completes Closed, "
                 "no further I/O\n",
                 rejected ? "the broker rejected the session (non-retriable)" : "the Aeron client closed");
    // Stop all I/O first, so nothing below can reach the transport.
    agent_.enterFailedMode();
    // Tail-close every producer's intake (a claim before the close is still read
    // from the ring and completed) and complete every accepted, unacknowledged
    // record Closed — exactly once (ProducerImpl's Discarding life).
    producerHub_.failCloseAll(OpStatus::Closed);
    // Pending control ops can never be answered now.
    failAllControl_(OpStatus::Closed);
}

int Client::failedDutyCycle_(int64_t nowNs) {
    ++failedDutyCycles_;
    // Commands only (no driver pumping, no poll, no ticks; sends dropped).
    int work = agent_.doWork(nowNs);
    if (!deferredReleases_.empty()) [[unlikely]] work += applyDeferredReleases_(false);
    // Producers in Discarding: records still read from their closed rings
    // complete Closed; nothing is sent (every offer is refused).
    work += producerDuty_(nowNs);
    // A control op issued after the failure completes Closed at once.
    if (hasPendingConnect_ || !pendingCtl_.empty()) failAllControl_(OpStatus::Closed);
    if (!drainWaiters_.empty()) work += checkDrain_();
    if (agent_.takeOrphanHint()) scanOrphans_();
    if (!orphans_.empty()) work += applyDueOrphans_();
    if (!releases_.empty()) work += reapReleases_();
    return work;
}

void Client::publishShardStatus_() noexcept {
    const auto& m = agent_.seedMachine();
    DutyView    v;
    v.seedState          = m.state();
    v.seedEverLive       = m.everLive();
    v.seedRejectCode     = m.rejectCode();
    v.seedSessionId      = m.sessionId();
    v.seedBackoffAttempt = m.backoffAttempt();
    v.leaderSessions     = agent_.leaderSessionCount();
    v.leadersDownMask    = agent_.leaderSessionsDownMask();
    v.transportFailed    = transportFailed_ && agent_.transportClosed();
    v.producerClosing    = producerHub_.anyClosing();
    shardDuty_ = shard::deriveShardStatus(v, cfg_.shardIndex);
    if (!shardDirty_ && shard::sameStatus(shardDuty_, shardPublished_)) [[likely]] return;
    // Ordinary updates are latest-value (a refused one is retried next cycle and
    // superseded by a newer one); Failed is a lifecycle snapshot: it always fits
    // and does NOT finish the ring (Closed still follows at teardown).
    const bool ok = shardDuty_.state == shard::ShardState::Failed ? shardRing_->publishFinal(shardDuty_)
                                                                    : shardRing_->tryPublish(shardDuty_);
    if (ok) {
        shardPublished_ = shardDuty_;
        shardDirty_     = false;
    } else {
        shardDirty_ = true;
    }
}

void Client::publishShardClosed() noexcept {
    if (!shardRing_ || shardClosed_) return;
    shardClosed_         = true;
    shard::ShardStatus c = shardDuty_;
    c.state              = shard::ShardState::Closed;
    c.failure            = shard::ShardFailure::None;
    c.downMask           = 0;
    (void)shardRing_->publishFinal(c);
    shardRing_->close();
    shardDuty_ = c;
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
        syncCoordinatorSession_();  // D4: O(1) when unchanged
        return;
    }
    // F8c C3: app threads (Client::state()) see the transition BEFORE any
    // completion or callback this transition triggers (ring order on this
    // thread: status first).
    if (st != lastSeedState_ || sid != lastSeedSessionId_) publishSeedStatus_(st, sid);
    // No-hang fix round 4 (item 6): the client's OWN liveness check (response
    // timeout / own stall) declared the seed session lost — its in-flight
    // control ops can never be answered (the broker allocates a new session on
    // resume), so they complete SessionLost NOW, exactly as on a SessionExpired
    // from the broker — never later as Timeout by their per-op deadlines.
    if (const uint64_t losses = m.staleSessionLosses(); losses != lastSeedStaleLosses_) [[unlikely]] {
        lastSeedStaleLosses_ = losses;
        static constexpr std::string_view kWhy = "session lost: no answer within the response timeout";
        const auto text = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(kWhy.data()), kWhy.size());
        failControlOnSeedLoss_(0, text);   // a DDL follows its own session (I1)
    }
    // E2 FU1 (seed-session check): the seed LEFT its Connected session (stale
    // loss, SessionExpired / UnknownSession, transport drop, a forced redirect
    // reconnect).  Every way back is a NEW broker session (the broker's
    // handleConnect always allocates one; the old one's subscriptions die with
    // it), and plain subscriptions bound on the seed are never re-armed — each
    // ends explicitly (SessionLost), never a silent stall.  Runs before any
    // frame of a new session can bind (the first thing its ConnectResponse
    // does).  close() finishes everything itself (Closed is excluded).
    if (lastSeedState_ == session::SessionState::Connected && st != session::SessionState::Closed &&
        (st != session::SessionState::Connected || sid != lastSeedSessionId_ || episode != lastSeedEpisode_))
        [[unlikely]]
        (void)subscriberHub_.endSeedBound();
    const bool connected = st == session::SessionState::Connected;
    subscriberHub_.setSessionConnected(connected);
    lastSeedState_     = st;
    lastSeedSessionId_ = sid;
    lastSeedEpisode_   = episode;
    // D4: the group hub follows the COORDINATOR session (the seed by default:
    // the same (connected, sessionId, episode) transitions as before).
    syncCoordinatorSession_();
}

int Client::routeBufferedAcks_() {
    const auto acks = agent_.produceAcks();
    const int n = static_cast<int>(acks.size());
    bool notLeader = false;
    for (const auto& a : acks) {
        producerHub_.onProduceResponse(a.correlationId, a.baseOffset, a.errorCode, a.logStartOffset);
        notLeader |= a.errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
    }
    agent_.clearProduceAcks();
    // Task 16 fix round 1 (C1): a ProduceResponse{NotLeader} parked its partition
    // (retriable) — ask who owns it now (coalesced: once per poll).
    if (notLeader) [[unlikely]] requestMetadataNow_();
    return n;
}

int Client::producerDuty_(int64_t nowNs) {
    const int64_t sessionId = agent_.seedSession().machine.sessionId();
    int work = producerHub_.drainAndFlush(nowNs, sessionId, agent::AgentProduceSink{&agent_});
    // D4: txn producers run on the COORDINATOR session (== the seed by default):
    // a coordinator move is a session change — what was in flight is re-sent.
    if (!txnProducers_.empty()) {
        const int64_t coordSid = coordIdx_ == 0 ? sessionId : coordSessionId_();
        for (auto& tp : txnProducers_) work += tp->doWork(nowNs, coordSid, txnSendFn_);
    }
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
    // T15b: every loop also ends when the Client failed for good (M2): the
    // runner then parks 1 ms per cycle below — it only drains the command ring
    // (close, releases) and the producers' closed rings, with no I/O.
    if (cfg_.idleMode == IdleMode::LowLatency) {
        // spin 5, yield 3, park 1 µs .. 100 µs: the runner drives the producer drain
        // AND the ack poll, so its park ceiling is the client floor on ack latency.
        aeron::concurrent::BackoffIdleStrategy idle(
            5, 3, std::chrono::duration<long, std::nano>(1'000),
            std::chrono::duration<long, std::nano>(100'000));
        while (!agent_.commandRingClosed() && !transportFailed_) idle.idle(dutyCycle(now()));
    } else if (cfg_.idleMode == IdleMode::BusySpin) {
        // Never park or yield: pin this runner to a dedicated core.
        while (!agent_.commandRingClosed() && !transportFailed_) {
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
        while (!agent_.commandRingClosed() && !transportFailed_) idle.idle(dutyCycle(now()));
    }
    // T15b M2: failed for good — a parked loop until close() (no I/O, no spin).
    while (!agent_.commandRingClosed()) {
        (void)dutyCycle(now());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
    {
        const int timedOut = subscriberHub_.sweepTimedOut(now);
        // Silent-failure gap: the timeout is an explicit failure (failReason
        // TimedOut) AND reaches the app's onError, once per Subscribe.
        for (int i = 0; i < timedOut && cbError_; ++i) {
            invokeUserCallback_("onError", [&] {
                cbError_("Subscribe timed out: no SubscribeResponse within subscribeTimeoutMs");
            });
        }
    }

    // --- CG-5: GroupConsumerHub duty cycle (heartbeat / auto-commit / rejoin) ---
    // The hub's doWork checks its own internal timers so calling it every
    // kSweepIntervalMs (100 ms) is sufficient for heartbeat (~10 s) and
    // auto-commit (~5 s) cadences.  Rejoining consumers send JoinGroup on the
    // very first doWork call after a state transition (no extra latency).
    if (!groupConsumerHub_.empty()) {
        const int64_t gsid = coordSessionId_();  // D4: the coordinator session (the seed by default)
        const int64_t wallNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        const group::SendFn& sendFn = coordSendFn_;
        groupConsumerHub_.doWork(wallNs, gsid, sendFn);
        // Pass-driven re-offer of transiently refused Subscribes and of routes a
        // produce redirect moved (parked partitions wait for fresh metadata —
        // LOW-2), and MEDIUM-2: OffsetFetch for start offsets still unknown.
        (void)groupConsumerHub_.reconcileFetch();
        (void)groupConsumerHub_.retryOffsetFetch(nowNs, gsid, sendFn);
        // F8c C4: commitSync deadlines on the duty clock -> Timeout.
        (void)groupConsumerHub_.sweepCommitSyncTimeouts(nowNs);
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
            producerHub_.failAll(OpStatus::SessionLost);  // redirect reconnect failed: no answer possible
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

        // MAJOR-2: a ConnectResponse no session machine was waiting for (stale
        // correlationId, a duplicate, a late answer to a superseded attempt)
        // changes nothing at all.
        if (!agent_.lastConnectAwaited()) break;
        // A leader-node session's own connect: its partitions' group
        // subscriptions (and held produce, re-driven by the agent) follow it —
        // none of the seed-session logic below (connect completion, metadata,
        // JoinGroup, onConnected) concerns it.
        if (agent_.lastConnectSession() != 0) {
            if (ec == rpc::ErrorCode::OK && agent_.lastConnectAccepted() && !groupConsumerHub_.empty()) {
                // That session's event: its unanswered Subscribes are re-sent.
                (void)groupConsumerHub_.resendUnanswered(agent_.lastConnectSession());
            }
            break;
        }

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
                redirectedSubscribes_.clear();  // DR-15 re-review: re-sent here — never again on metadata
                heldRedirectFollows_.clear();   // R3: likewise (a new Redirect decides it again)
            } else if (subscriberHub_.hasPending()) {
                // F8a fix2: a pending Subscribe sent before this session existed
                // was dropped by the ring drain (stale session) — re-issue every
                // pending one stamped with the new session.
                subscriberHub_.resendPending(
                    opt->assignedSessionId,
                    [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    });
                redirectedSubscribes_.clear();  // DR-15 re-review: re-sent here — never again on metadata
                heldRedirectFollows_.clear();   // R3: likewise (a new Redirect decides it again)
            }

            // MAJOR-2 (round 3): the broker's capability, for every group
            // consumer — present or created later (joinIfConnected).
            groupConsumerHub_.setBrokerVersion(opt->brokerVersion);

            // CG-5: Trigger group consumers to send JoinGroup on connect — only
            // when the SEED (the coordinator's session) connected: a leader
            // node's session (opened for produce / group fetch routing) must
            // not send a JoinGroup stamped with its own id (the ring drops it
            // as stale and the member would sit in Joining until the deadline).
            // D4: only while the seed IS the coordinator — a coordinator
            // session of its own joins its members on ITS connect
            // (syncCoordinatorSession_).
            if (!groupConsumerHub_.empty()) {
                if (coordIdx_ == 0) groupConsumerHub_.onConnected(opt->assignedSessionId, coordSendFn_);
                // A (leader or seed) session (re)connected: partitions waiting
                // for it — or whose subscription died with its old session —
                // subscribe now.
                (void)groupConsumerHub_.reconcileFetch();
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

        // Catalog history (Task 5, spec §5.1/§5.2/§9): an answer to a pending
        // DDL is decided HERE, before every data-op branch below (which would
        // treat a DDL Redirect as a connect-time one and drop it): Redirect ->
        // re-sent on the named node's session; NotLeader -> re-sent after a
        // backoff inside the retry budget; anything else -> its BrokerError
        // with the message as detail.  Session-fatal codes take the shared
        // path below (every pending op of the dead session ends SessionLost).
        if (opt->correlationId != 0 && ec != rpc::ErrorCode::SessionExpired &&
            ec != rpc::ErrorCode::UnknownSession) {
            if (const std::size_t i = findCtl_(opt->correlationId);
                i < pendingCtl_.size() && isRoutedCtlOp(pendingCtl_[i].op)) {
                if (ec == rpc::ErrorCode::Redirect) {
                    onDdlRedirect_(i, opt->message, now());
                } else if (ec == rpc::ErrorCode::NotLeader) {
                    onDdlNotLeader_(i, opt->message, now());
                } else {
                    std::string msg(opt->message);
                    if (msg.empty()) msg = "ErrorResponse code: " + std::to_string(opt->code);
                    failDdl_(i, static_cast<uint16_t>(opt->code), msg, ddlNoValue(pendingCtl_[i].op));
                    if (cbError_) invokeUserCallback_("onError", [&] { cbError_(msg); });
                }
                break;
            }
        }

        // A group member's per-partition Subscribe refused by a non-leader:
        // re-route THAT partition (never a seed-level redirect — the seed
        // session is the group coordinator's).
        if ((ec == rpc::ErrorCode::Redirect || ec == rpc::ErrorCode::NotLeader) &&
            group::GroupConsumerHub::isFetchCorrId(opt->correlationId)) {
            onGroupFetchRefused_(opt->correlationId, ec == rpc::ErrorCode::Redirect, opt->message);
            break;
        }

        // Rune E2 Task 13 (Task 11 carry): a node that stops leading a partition
        // answers the Subscribe correlation id of a BOUND read_committed
        // subscription with NotLeader and drops it (it can no longer serve it —
        // it used to go silent).  The subscription moves: it becomes a PENDING
        // Subscribe again under a fresh correlationId, from its resume position
        // (same isolation, its bound partition), and takes the pending-Subscribe
        // paths below exactly like a refused new one — bare NotLeader: fresh
        // metadata + backoff, bounded budget, then an explicit failure; Redirect:
        // follow the leader (and the subscribe timeout fails it explicitly if no
        // answer ever comes).  `opt` is this handler's own decoded copy: from here
        // on it names the re-armed Subscribe.  Cold (a leadership move).
        // Only an answer of the CURRENT session moves anything (a stale one of
        // an earlier session concerns a subscription that died with it).
        // E2 final fix wave (I2): the answer may come on the seed or — for a
        // subscription that moved before — on its leader node's own session;
        // the hub checks it is the subscription's own home session.
        if ((ec == rpc::ErrorCode::NotLeader || ec == rpc::ErrorCode::Redirect) && opt->correlationId != 0)
            [[unlikely]] {
            const auto snap = agent_.stateAndSessionId();
            // E2 residual round 5 (R4): a Redirect for a PENDING Subscribe counts
            // only from the session that Subscribe is outstanding on (its node
            // session when homed, else the current seed session).  A late one
            // from an earlier session (the dead seed, before a reconnect re-sent
            // the Subscribe there) is ignored and counted — following it would
            // bind the subscription a second time on another node.
            if (ec == rpc::ErrorCode::Redirect &&
                !subscriberHub_.answeredOnItsSession(opt->correlationId, opt->sessionId, snap.sessionId)) {
                ++staleSubscribeRedirects_;
                break;
            }
        }
        // E2 residual round 6 (F1): a resumed Subscribe's claimed data lineage
        // did not survive (the transient partition's ring restarted): restart
        // it at the log start and surface the reset (Subscription::dataResets).
        if (ec == rpc::ErrorCode::PartitionDataReset && opt->correlationId != 0) [[unlikely]] {
            const auto snap = agent_.stateAndSessionId();
            if (subscriberHub_.answeredOnItsSession(opt->correlationId, opt->sessionId, snap.sessionId) &&
                subscriberHub_.restartFromLogStart(
                    opt->correlationId, snap.sessionId,
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.subscribeTimeoutMs))) {
                break;
            }
        }
        // Catalog history (Task 13): a BOUND catalog subscription answered
        // NotLeader was ended by a truncation below a served high watermark —
        // finished explicitly (CatalogReadEnded) for its reader to re-validate,
        // never re-armed from its position (what it delivered may be stale).
        if (ec == rpc::ErrorCode::NotLeader && opt->correlationId != 0) [[unlikely]] {
            const auto snap = agent_.stateAndSessionId();
            if (subscriberHub_.endCatalogRead(opt->correlationId, opt->sessionId, snap.sessionId)) break;
        }
        if ((ec == rpc::ErrorCode::NotLeader || ec == rpc::ErrorCode::Redirect) && opt->correlationId != 0)
            [[unlikely]] {
            const auto snap = agent_.stateAndSessionId();
            if (subscriberHub_.rearmMovedAway(
                    opt->correlationId, nextCorrId_, nextCorrId_ + 1, opt->sessionId, snap.sessionId,
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.subscribeTimeoutMs))) {
                opt->correlationId = nextCorrId_;
                nextCorrId_ += 2;
            }
            // A moved subscription (re-armed now, or pending again and answered
            // Redirect by the node it was re-sent to) follows the Redirect PER
            // PARTITION: never a seed retarget, which would strand every other
            // subscription bound on the seed (final review I2).
            if (ec == rpc::ErrorCode::Redirect && !opt->message.empty() &&
                followMovedSubscriptionRedirect_(opt->correlationId, opt->message)) {
                break;
            }
        }

        // D4 (coordinator redirect): a follower answers a group / txn
        // COORDINATOR request with ErrorResponse{NotCoordinator, coordinator
        // endpoint} (the coordinator is the consensus leader).  The coordinator
        // route follows it to that node's own session (Kafka's coordinator
        // connection) — the seed session never moves.  Group members re-join
        // there (onCoordinatorMoved → Rejoining → its connect); txn requests in
        // flight are re-sent there by the txn producer (a session change).
        if (ec == rpc::ErrorCode::NotCoordinator && !opt->message.empty()) {
            onCoordinatorRedirect_(std::string(opt->message));
            break;
        }

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
                        metadataCache_.updatePartitionLeaderProvisional(  // M5 / NEW-1
                            rTopicId, rPartition, newNodeId, newEpoch, nextCorrId_);
                        // IMPORTANT-2: we now KNOW which node the session is
                        // following — record it so proactive routing can avoid
                        // re-sending to the wrong node.
                        currentLeaderNode() = newNodeId;
                    }
                }
                // Reconcile with the authoritative map NOW (M5): the provisional
                // epoch above never blocks the answer to THIS (or any later)
                // request (MetadataCache confirmedEpoch), so a spurious redirect
                // does not strand the partition's worker route on the node relay —
                // the answer restores it; an answer to a request sent BEFORE the
                // redirect cannot undo it (NEW-1, provisionalSince).  Rate-limited;
                // falls back to the backoff refresh.
                requestMetadataNow_();
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
            // MEDIUM-4: while a consumer group is active the seed session is the
            // group coordinator's — never moved by a data-op redirect.  A produce
            // redirect then takes this per-partition route even before metadata
            // installed any route.
            if (havePart && rTopicId >= 0 && rPartition >= 0 &&
                (agent_.multiNodeRouting() || groupActive) && !newEndpoint.empty()) {
                const std::string leaderChannel = toChannel(newEndpoint);  // N2
                // R2-2: a changed route is a new lane even if the redirected batch
                // is not re-offered below (superseded / budget / unknown) — older
                // attempts must not count as the current lane (NEW-4(b)).
                if (agent_.setRoute(rTopicId, rPartition, leaderChannel, leaderChannel))
                    producerHub_.noteRouteChanged(rTopicId, rPartition);
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

            // E2 residual round 3 (O1): a NEW, never-bound plain Subscribe
            // answered Redirect follows it PER PARTITION on the leader node's
            // own session (as a moved one does, I2) — never a seed retarget,
            // which would strand every subscription already bound on the seed
            // (a silent stall).  Round 5 (R3): also before the first metadata
            // answer — the Redirect's endpoint installs the partition's route.
            if (!groupActive && subscriberHub_.isNewPendingSubscribe(opt->correlationId) &&
                !agent::ClientAgent::sameEndpoint(toChannel(newEndpoint), agent_.seedChannel())) {
                bool handled = false;
                switch (redirectFollowFor_(newEndpoint)) {
                case RedirectFollow::PerPartition:
                    handled = subscriberHub_.adoptRedirectedSubscribe(opt->correlationId) &&
                              followMovedSubscriptionRedirect_(opt->correlationId, opt->message);
                    break;
                case RedirectFollow::Hold:
                    // R3: no metadata answer yet — the broker set is unknown.
                    // Held (neither sent nor followed by moving the seed) until
                    // the next answer decides it; the subscribe deadline still
                    // bounds it (explicit failure, never a hang).
                    try {
                        heldRedirectFollows_.push_back(HeldRedirect{opt->correlationId, newEndpoint, metadataResponses_});
                        ++redirectFollowsHeld_;
                        requestMetadataNow_();
                        handled = true;
                    } catch (...) {
                        // allocation failure (cold): the seed path below
                    }
                    break;
                case RedirectFollow::BySeed:
                    break;
                }
                if (handled) break;
            }

            if (groupActive) {
                // MEDIUM-4: while a group is active the seed session (the group
                // coordinator's) never follows a data-op redirect.
                //  * A plain Subscribe: plain subscriptions are sent on the seed
                //    only (no per-partition routing for them — they have no
                //    position / leader-change model; ticketed), so one for a
                //    partition another node leads can never succeed here: it is
                //    finished with SubscriptionFailReason::
                //    LeaderElsewhereWhileGroupActive (documented on subscribe()).
                //  * A produce whose partition this handler could not map
                //    (havePart false) is retried through its partition route.
                //  * Anything else (a transactional producer's Produce, whose
                //    correlationId the ProducerHub does not know) is not retried
                //    here: it completes at its own op deadline.
                if (!subscriberHub_.failPendingSubscribe(
                        opt->correlationId, SubscriptionFailReason::LeaderElsewhereWhileGroupActive)) {
                    producerHub_.retryRedirectedProduce(
                        opt->correlationId,
                        [this](int32_t t, int32_t p, int32_t sid, const uint8_t* fb, std::size_t fl) -> bool {
                            return agent_.offerToPartition(t, p, sid, fb, fl);
                        });
                }
                break;
            }

            redirectBySeed_(opt->correlationId, newEndpoint);
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
            // frame), so we cannot update the cache in place — ask for metadata
            // now (rate-limited; falls back to the backoff refresh).
            requestMetadataNow_();

            // Silent-failure gap: a PENDING plain Subscribe answered bare
            // NotLeader (a topic whose definition has not committed yet, or a
            // leader not yet known) is retried like the Redirect branch: once per
            // pending resend, paced by fresh metadata + a doubling backoff, and
            // bounded by the per-corr budget.  Exhausted -> explicit failure
            // carrying the broker code, and the app's onError fires.
            if (subscriberHub_.isPending(opt->correlationId)) {
                // E2 final fix wave (I2): a MOVED subscription whose partition
                // routes to a node session retries THERE after a backoff (its
                // new leader may not have taken over yet) — never through the
                // seed, which may sit on the node that just went away.  Same
                // bounded budget; exhausted -> the explicit failure below.
                if (subscriberHub_.movedRoutesToANode(opt->correlationId) &&
                    subscriberHub_.chargeNotLeaderRetry(opt->correlationId, producer::ProducerImpl::kMaxRedirects)) {
                    (void)subscriberHub_.holdMovedForRetry(
                        opt->correlationId,
                        std::chrono::steady_clock::now() + std::chrono::nanoseconds(redirectedResendBackoffNs_));
                    redirectedResendBackoffNs_ =
                        std::min<int64_t>(redirectedResendBackoffNs_ * 2, kRedirectedResendMaxNs);
                    requestMetadataNow_();
                    break;
                }
                if (subscriberHub_.chargeNotLeaderRetry(opt->correlationId,
                                                        producer::ProducerImpl::kMaxRedirects)) {
                    if (std::find(redirectedSubscribes_.begin(), redirectedSubscribes_.end(),
                                  opt->correlationId) == redirectedSubscribes_.end()) {
                        try {
                            redirectedSubscribes_.push_back(opt->correlationId);
                        } catch (...) {
                            // allocation failure (cold path): it now times out explicitly
                        }
                    }
                    redirectMetaSeen_       = metadataResponses_;
                    nextRedirectedResendNs_ = now() + redirectedResendBackoffNs_;
                    redirectedResendBackoffNs_ =
                        std::min<int64_t>(redirectedResendBackoffNs_ * 2, kRedirectedResendMaxNs);
                } else if (subscriberHub_.failPendingSubscribe(
                               opt->correlationId, SubscriptionFailReason::RejectedByBroker,
                               static_cast<uint16_t>(rpc::ErrorCode::NotLeader))) {
                    if (cbError_) {
                        invokeUserCallback_("onError", [&] {
                            cbError_("Subscribe failed: NotLeader (retry budget exhausted)");
                        });
                    }
                }
                break;
            }

            // Re-enqueue the produce that hit the stale leader.  The stored wire
            // frame is resent on the SAME session (leadership is expected to have
            // moved back or the leader to have caught up by the time the retry
            // lands; the background refresh reroutes if it has genuinely moved).
            // retryRedirectedProduce() bumps the per-corrId redirect counter and,
            // on exhausting kMaxRedirects, fails the pending ack itself.
            // I1 (Task 16 fix round 1): through the partition's route — the
            // same rules as every other resend (worker data channel / leader
            // session, re-stamped session id, its stream) — never the seed's
            // default publication.
            std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn =
                [this](int32_t t, int32_t pp, int32_t sid, const uint8_t* fb, std::size_t fl) -> bool {
                    return agent_.offerToPartition(t, pp, sid, fb, fl);
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
        // D4 r3 MEDIUM-1: a session-fatal answer for ANOTHER session (the
        // coordinator session — the agent routes it there by its cluster-unique
        // header sessionId) is not the seed's: the seed's pending connect /
        // control ops and the app's onError stay untouched.
        if (sessionFatal && opt->sessionId != 0) {
            const int64_t seedSid = agent_.seedSession().machine.sessionId();
            if (seedSid != 0 && opt->sessionId != seedSid) break;
        }
        const auto ecode = static_cast<uint16_t>(opt->code);
        const auto text  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(msg.data()), msg.size());
        // No-hang fix round 2: only an answer to THE connect (its correlationId)
        // or a session-fatal one fails a pending connect — an error answering
        // some other request (e.g. an InvalidRequest Ping) no longer does.
        if (sessionFatal || opt->correlationId == agent_.seedSession().machine.correlationId())
            completeConnect_(OpStatus::BrokerError, 0, ecode, msg);
        if (sessionFatal) {
            failControlOnSeedLoss_(ecode, text);   // a DDL follows its own session (I1)
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
        // A group member's per-partition Subscribe (tagged correlationId).
        if (groupConsumerHub_.onFetchSubscribeResponse(opt->correlationId, opt->errorCode,
                                                       opt->subscriptionId)) {
            break;
        }
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
        // Demultiplexed by subscriptionId — cluster-unique (the broker puts
        // the node id in its high bits), whichever node / publication pushed it.
        // Route to the subscriber hub first (direct subscriptions).
        subscriberHub_.onRecords(*opt, agent_.seedSession().machine.sessionId());
        // Also route to group consumer hub (assigned partitions) — never a
        // broker POSITION NOTE (an empty batch, client version 6, E2 Task 13):
        // it concerns plain subscriptions; a member resumes from its own
        // position (an empty view would move it as one record).
        if (!opt->recordBatch.empty()) [[likely]] groupConsumerHub_.onRecords(*opt);
        break;
    }

    // ---- CG-5: Consumer-group response frames --------------------------------

    case codec::TemplateId::JoinGroupResponse: {
        if (!groupConsumerHub_.empty()) {
            groupConsumerHub_.onJoinGroupResponse(buf, len, coordSessionId_(), coordSendFn_);  // D4
        }
        break;
    }

    case codec::TemplateId::SyncGroupResponse: {
        if (!groupConsumerHub_.empty()) {
            groupConsumerHub_.onSyncGroupResponse(buf, len, coordSessionId_(), coordSendFn_);  // D4
        }
        break;
    }

    case codec::TemplateId::HeartbeatResponse: {
        if (!groupConsumerHub_.empty()) {
            groupConsumerHub_.onHeartbeatResponse(buf, len, coordSessionId_(), coordSendFn_);  // D4
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
            groupConsumerHub_.onAssign(buf, len, coordSessionId_(), coordSendFn_);  // D4
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
            tp->onProduceResponse(buf, len);
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
        for (auto& tp : txnProducers_) {
            tp->onAddOffsetsToTxnResponse(buf, len);
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
            if (opt->errorCode == static_cast<uint16_t>(rpc::ErrorCode::NotLeader)) {   // catalog history: retriable
                onDdlNotLeader_(i, {}, now());
                break;
            }
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
                         detail::CompletionRing::Overflow::TooLarge,
                         static_cast<uint32_t>(opt->partitionCount));  // Phase 2b C (<=0 = not reported)
        }
        break;
    }

    // ---- Task 18: AlterTopicResponse (69) ----
    case codec::TemplateId::AlterTopicResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::AlterTopicResponse::decode(buf, len);
        if (!opt) break;

        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() && pendingCtl_[i].op == OpKind::AlterTopic) {
            if (opt->errorCode == static_cast<uint16_t>(rpc::ErrorCode::NotLeader)) {   // catalog history: retriable
                onDdlNotLeader_(i, {}, now());
                break;
            }
            PendingCtl p = takeCtl_(i);
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->topicId, opt->newVersion, opt->errorCode);
        }
        break;
    }

    // ---- Task 21: CreateViewResponse (71) ----
    case codec::TemplateId::CreateViewResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::CreateViewResponse::decode(buf, len);
        if (!opt) break;

        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() && pendingCtl_[i].op == OpKind::CreateView) {
            if (opt->errorCode == static_cast<uint16_t>(rpc::ErrorCode::NotLeader)) {   // catalog history: retriable
                onDdlNotLeader_(i, {}, now());
                break;
            }
            PendingCtl p = takeCtl_(i);
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->viewTopicId, static_cast<int64_t>(opt->outputFingerprint), opt->errorCode);
        }
        break;
    }

    // ---- Catalog history (Task 5): DdlResponse (78) — DropTopic / DropView /
    //      AlterView / AlterTopicConfig.  errorCode 2 = NotLeader (retriable).
    case codec::TemplateId::DdlResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::DdlResponse::decode(buf, len);
        if (!opt) break;

        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() &&
            (pendingCtl_[i].op == OpKind::DropTopic || pendingCtl_[i].op == OpKind::DropView ||
             pendingCtl_[i].op == OpKind::AlterView || pendingCtl_[i].op == OpKind::AlterTopicConfig)) {
            if (opt->errorCode == static_cast<uint16_t>(rpc::ErrorCode::NotLeader)) {
                onDdlNotLeader_(i, opt->detail, now());
                break;
            }
            PendingCtl p = takeCtl_(i);
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         static_cast<int64_t>(opt->objectId), static_cast<int64_t>(opt->newVersion),
                         opt->errorCode,
                         std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(opt->detail.data()),
                                                  opt->detail.size()));
        }
        break;
    }

    // ---- Task 21: ViewRowResponse (73) ----
    case codec::TemplateId::ViewRowResponse: {
        namespace rpc = rune::protocol::client;
        auto opt = rpc::ViewRowResponse::decode(buf, len);
        if (!opt) break;

        if (const std::size_t i = findCtl_(opt->correlationId);
            i < pendingCtl_.size() && pendingCtl_[i].op == OpKind::GetViewRow) {
            PendingCtl p = takeCtl_(i);
            // status is a ViewRowResponse-specific outcome, not a broker
            // ErrorCode — reuse the errorCode field as its carrier (same
            // convention DefineTopic/AlterTopic use for their own errorCode).
            // rowBytes' OWN length (opt->rowBytesLen) is ground truth for the
            // row length (opt->rowLen is informational only, per the wire
            // comment in DataMessages.h) — the payload below carries exactly
            // rowBytesLen bytes, never opt->rowLen.
            const std::span<const uint8_t> row(
                opt->rowBytes, (opt->rowBytes != nullptr) ? opt->rowBytesLen : 0u);
            completeCtl_(p, opt->status == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->rowLen, 0, opt->status, row,
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
    // self-tagged — batchcorr — so it takes no shared counter).  F8c C7: held
    // here until the registration is accepted (then the duty side holds its
    // share and the handle the other — spec §3.3).
    auto impl = std::make_unique<producer::ProducerImpl>(cfg, ringCap);

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

    // F8c C6/C6a: ack delivery into the completion-queue registry, the close
    // token's generic delivery, the begin-close / closeImmediately issue path
    // and the blocking close() — plain function pointers + `this`.
    {
        producer::ProducerImpl::Hooks hk;
        hk.ctx        = this;
        hk.cqDeliver  = &Client::producerCqDeliverTrampoline_;
        hk.sink       = detail::CompletionSink{&Client::deliverTrampoline_, this};
        hk.issueClose = &Client::issueProducerCloseTrampoline_;
        hk.andWait    = &Client::andWaitTrampoline_;
        hk.release    = &Client::hubCommandTrampoline_;  // C7: HandleRelease
        impl->setHooks(hk);
    }

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
    // single-owner (no atomic, no lock).  F8c C7: the impl's own address travels
    // the ring (no shared_ptr holder): the hub holds the duty share, the returned
    // handle the other; whichever lets go last frees it (§3.3).  Retry on a
    // transiently full ring (off the hot path).  Bounded setup-phase submit; in
    // Invoker mode the duty cycle runs between attempts (this thread is the
    // ring's only consumer).
    // F8c C2: the submit is linearized against close() by the command ring's
    // closed bit (no post-submit stopped_ CAS, no indeterminate ownership):
    //   * accepted — before the close: the duty cycle, or close()'s final
    //     drain (which reads up to the frozen tail), adopts it; close() then
    //     marks its ring closed with every other one and hands it back;
    //   * refused as Closed (close() raced this call) — never written: the
    //     producer is born closed (every send reports Closed; the handle
    //     alone owns it).
    // (Closed on entry still throws logic_error("client closed") from
    // submitRegistration_, as before — the unique_ptr frees the impl.)
    producer::ProducerImpl* raw = impl.get();
    if (!submitRegistration_([&] { return agent_.submitRegisterProducer(raw); }, [] {})) {
        impl->markBornClosed();
    }
    return Producer(impl.release());
}

OpResult Client::issueProducerCloseTrampoline_(void* ctx, producer::ProducerImpl& impl,
                                              const CompletionTarget* target, Token token) {
    return static_cast<Client*>(ctx)->issueProducerClose_(impl, target, token);
}

OpResult Client::issueProducerClose_(producer::ProducerImpl& impl, const CompletionTarget* target,
                                     Token token) {
    using Op = detail::HubCommand::Op;
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (cfg_.runnerMode == RunnerMode::Invoker && !onDuty) return OpResult::WrongThread;

    detail::HubCommand cmd;
    cmd.handle = reinterpret_cast<uint64_t>(&impl);
    Completions*     q = nullptr;
    detail::OpTarget t;
    if (target == nullptr) {
        // closeImmediately: the intake closes now (idempotent; it may already
        // be closed by a begin-close whose drain this aborts).
        cmd.op = Op::ProducerCloseNow;
        (void)impl.closeIntake();
    } else {
        if (token > kMaxToken) return OpResult::InvalidArgument;
        if (target->cq == nullptr && target->handler.fn == nullptr) return OpResult::InvalidArgument;
        q = target->cq;
        if (q != nullptr) {
            if (q->client_ != this) return OpResult::InvalidArgument;
            if (!q->onOwnerThread()) return OpResult::WrongThread;
            const uint32_t r  = q->reservationFor(OpKind::ProducerClose);
            const OpResult rr = q->reserve(r);
            if (rr != OpResult::Accepted) return rr;
            t.reservedBytes = r;
            t.cqRef         = q->cqRef_;
        } else {
            t.handler = target->handler;
        }
        // The linearization point: the intake's tail-bit CAS.  Lost = already
        // closed (a second begin-close, closeImmediately, the failure path or
        // Client close): no completion.
        if (!impl.closeIntake()) {
            if (q != nullptr) q->unreserve(t.reservedBytes);
            return OpResult::Closed;
        }
        cmd.op = Op::ProducerBeginClose;
        cmd.a  = static_cast<int64_t>(token);
        detail::encodeTarget(t, cmd);
    }
    auto unreserve = [&] { if (q != nullptr) q->unreserve(t.reservedBytes); };
    if (agent_.commandRingClosed()) {
        // The Client is closing: its close completes every record (and the
        // intake is closed either way).
        unreserve();
        return OpResult::Closed;
    }
    if (onDuty) {  // the Invoker owner (in or outside a cycle) / a runner callback
        applyProducerCommand_(cmd);
        return OpResult::Accepted;
    }
    // A lifecycle command: guaranteed delivery — a full ring is waited out on
    // the runner's progress (it drains the ring), never a time bound.
    aeron::concurrent::BackoffIdleStrategy idle(
        10, 20, std::chrono::duration<long, std::nano>(1'000),
        std::chrono::duration<long, std::nano>(100'000));
    for (;;) {
        const auto r = agent_.submitHubCommand(cmd);
        if (r == agent::ClientAgent::Submit::Accepted) {
            if (q != nullptr) q->issueFence_ = agent_.commandRingTail();  // I3'
            return OpResult::Accepted;
        }
        if (r == agent::ClientAgent::Submit::Closed) break;
        idle.idle(0);
    }
    unreserve();
    return OpResult::Closed;
}

void Client::applyProducerCommand_(const detail::HubCommand& cmd) {
    // The impl is alive: the issuing handle (or the hub, which adopted it
    // through the ring ahead of this command) holds it.
    auto* impl = reinterpret_cast<producer::ProducerImpl*>(cmd.handle);
    if (cmd.op == detail::HubCommand::Op::ProducerBeginClose) {
        detail::HubCommand pinned = cmd;
        pinCommandTarget_(pinned);  // I3: counted before anything can complete it
        impl->applyBeginClose(detail::decodeTarget(pinned), static_cast<Token>(cmd.a), now());
        return;
    }
    impl->applyCloseImmediately(OpStatus::Closed);
}

// ---------------------------------------------------------------------------
// F8c C7: handle release (spec §3.3) — no shared_ptr, no refcount, no waits
// ---------------------------------------------------------------------------
//
// Every handle impl (Producer / TransactionalProducer / Subscription /
// GroupConsumer) is owned jointly by its handle and the duty side.  Each side
// detaches its share of the impl's ownership word (a ring word) exactly once;
// whichever detaches LAST frees the impl.  The duty side detaches when it
// reaps a released impl that nothing references any more (end of a duty
// cycle), or when Client close hands every still-held impl back.
//
// Known gap (review m1 — test seam only): a registration stranded behind an
// ABANDONED close drain (an orphaned command-ring claim that unblock() cannot
// clear — impossible in-process outside the testClaim* seams) is never adopted
// and so never handed back: its duty share is never detached.  A handle that
// outlives the Client then sees !dutyDetached() and its release calls the
// release hook — i.e. it TOUCHES THE DEAD Client (use-after-free), it does not
// merely leak.  Accepted for the crash-only shutdown path (fix5); a later fix
// can mark such impls in their own ring word when the drain is abandoned.

detail::HubSubmit Client::submitRelease_(const detail::HubCommand& cmd) {
    // I1 (Task 5 fix round 0: no thread_local): a release made by user code
    // that close() itself runs finds the ring closed below (Closed: the
    // hand-back, later on this same thread, owns the impl) — never a wait.
    const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
    if (onDuty) {
        // The duty thread itself — the Invoker owner (in or outside a cycle) or
        // a Thread-mode runner callback (the duty state is ours until close()
        // joins this thread, even once it closed the ring): apply it here.
        if (cfg_.runnerMode == RunnerMode::Invoker && agent_.commandRingClosed()) {
            return detail::HubSubmit::Closed;  // closed: the hand-back owns it
        }
        // I3': ops of this handle may still sit in the command ring (issued
        // off this thread, or past a bounded drain): its impl is not forgotten
        // before the drain passed them (the current tail is a safe fence).
        applyRelease_(cmd, agent_.commandRingTail(), /*closing=*/false);
        return detail::HubSubmit::Accepted;
    }
    // Any other thread (spec §3.3): a lifecycle command on the command ring —
    // FIFO behind this handle's own ops; it never touches duty state itself
    // (the Invoker owner may be pumping right now).  Closed: close() is in
    // progress elsewhere and hands the impl back.
    //
    // F8c C7 fix round 2 (F1): the ring is FULL — never wait (the head may sit
    // behind an ABORTed send for as long as the transport refuses it, or
    // nobody pumps at all: join-then-destroy).  The release is ORPHANED
    // instead: no command, no time bound, nothing touched on the duty side.
    // The duty side discovers it (the ring's orphan hint -> a scan, or at the
    // impl's adoption) and applies it through the same {impl, fence} path as
    // a direct release, so it still takes effect in FIFO order behind this
    // handle's queued ops.
    const auto r = agent_.submitHubCommand(cmd);
    if (r == agent::ClientAgent::Submit::Accepted) return detail::HubSubmit::Accepted;
    if (r == agent::ClientAgent::Submit::Closed) return detail::HubSubmit::Closed;
    if (testHubCommandRingFullHook_) [[unlikely]] testHubCommandRingFullHook_();
    orphanRelease_(cmd);
    return detail::HubSubmit::Orphaned;
}

void Client::orphanRelease_(const detail::HubCommand& cmd) noexcept {
    using K = detail::HandleKind;
    void* const        impl  = reinterpret_cast<void*>(static_cast<uintptr_t>(cmd.handle));
    const std::int64_t fence = agent_.commandRingTail();
    // The fence (a plain field) first, then ONE RMW that orphans the release
    // and detaches the handle's share (release-ordered: it publishes the
    // fence).  If the duty side already let go, this side frees the impl —
    // and raises no hint (review N3: nothing is left to scan for, and this
    // thread then never touches the Client after the free).
    bool freed = false;
    switch (static_cast<K>(cmd.a)) {
    case K::Producer: {
        auto* p = static_cast<producer::ProducerImpl*>(impl);
        p->setOrphanFence(fence);
        freed = p->detachHandleOrphaned();
        if (freed) delete p;
        break;
    }
    case K::TxnProducer: {
        auto* t = static_cast<producer::TransactionalProducerImpl*>(impl);
        t->orphanFence = fence;
        freed = t->status.detachConsumerOrphaned();
        if (freed) delete t;
        break;
    }
    case K::Subscription: {
        auto* sub = static_cast<SubscriptionImpl*>(impl);
        sub->orphanFence = fence;
        freed = sub->status.detachConsumerOrphaned();
        if (freed) delete sub;
        break;
    }
    case K::GroupConsumer: {
        auto* gc = static_cast<GroupConsumerImpl*>(impl);
        gc->orphanFence = fence;
        freed = gc->status.detachConsumerOrphaned();
        if (freed) delete gc;
        break;
    }
    }
    // After the RMW (never before: that would reopen the lost wakeup) — a scan
    // that takes the hint sees the mark.  This store is the handle thread's
    // last touch of the Client: a handle destructor must not race the
    // Client's destruction (the documented handle contract — the submit above
    // touches the Client too).
    if (!freed) agent_.raiseOrphanHint();
}

void Client::noteOrphan_(detail::HandleKind kind, void* impl, std::int64_t fence) {
    try {
        orphans_.push_back(PendingRelease{kind, impl, fence});
    } catch (...) {
        // Out of memory: the impl stays with the duty side until close hands it back.
    }
}

void Client::scanOrphans_() {
    // Cold: only when an orphan hint was taken.  Every impl the duty side holds
    // whose release is orphaned and not yet noted is queued (FIFO: applied
    // once the command-ring head passes its fence).
    using K = detail::HandleKind;
    producerHub_.forEach([&](producer::ProducerImpl& p) {
        if (!p.releaseNoted && p.releaseOrphaned()) {
            p.releaseNoted = true;
            noteOrphan_(K::Producer, &p, p.orphanFence());
        }
    });
    for (std::size_t i = 0; i < txnProducers_.size(); ++i) {
        producer::TransactionalProducerImpl* t = txnProducers_[i];
        if (!t->releaseNoted && t->status.consumerOrphaned()) {
            t->releaseNoted = true;
            noteOrphan_(K::TxnProducer, t, t->orphanFence);
        }
    }
    subscriberHub_.forEachOwned([&](SubscriptionImpl& sub) {
        if (!sub.releaseNoted && sub.status.consumerOrphaned()) {
            sub.releaseNoted = true;
            noteOrphan_(K::Subscription, &sub, sub.orphanFence);
        }
    });
    groupConsumerHub_.forEachOwned([&](GroupConsumerImpl& gc) {
        if (!gc.releaseNoted && gc.status.consumerOrphaned()) {
            gc.releaseNoted = true;
            noteOrphan_(K::GroupConsumer, &gc, gc.orphanFence);
        }
    });
}

int Client::applyDueOrphans_() {
    // An orphaned release takes effect once the command-ring head passed its
    // fence: everything the handle submitted before it was applied first.
    const std::int64_t head = agent_.commandRingHead();
    int n = 0;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < orphans_.size(); ++i) {  // applyRelease_ may run user code
        const PendingRelease o = orphans_[i];
        if (head >= o.fence) {
            detail::HubCommand cmd;
            cmd.op     = detail::HubCommand::Op::HandleRelease;
            cmd.handle = reinterpret_cast<uint64_t>(o.impl);
            cmd.a      = static_cast<int64_t>(o.kind);
            applyRelease_(cmd, 0, /*closing=*/false);
            ++n;
        } else {
            orphans_[keep++] = o;
        }
    }
    orphans_.resize(keep);
    return n;
}

void Client::applyRelease_(const detail::HubCommand& cmd, std::int64_t fence, bool closing) {
    using K = detail::HandleKind;
    const auto kind = static_cast<K>(cmd.a);
    void* const impl = reinterpret_cast<void*>(static_cast<uintptr_t>(cmd.handle));
    // The handle's destructor semantics.  During close() (`closing`) the close
    // path finishes every producer / txn producer itself (closeFinal /
    // failAll, before or after this point); subscriptions and consumers are
    // finished locally here so no further callback reaches a destroyed handle.
    switch (kind) {
    case K::Producer:
        static_cast<producer::ProducerImpl*>(impl)->releaseNoted = true;
        if (!closing) static_cast<producer::ProducerImpl*>(impl)->applyHandleRelease();
        break;
    case K::TxnProducer:
        static_cast<producer::TransactionalProducerImpl*>(impl)->releaseNoted = true;
        if (!closing) static_cast<producer::TransactionalProducerImpl*>(impl)->applyHandleRelease();
        break;
    case K::Subscription: {
        auto* sub = static_cast<SubscriptionImpl*>(impl);
        sub->releaseNoted = true;
        if (closing) {
            if (sub->active) subscriberHub_.finishLocal(*sub);
        } else {
            detail::HubCommand u;
            u.op     = detail::HubCommand::Op::Unsubscribe;
            u.handle = cmd.handle;
            applyHubCommandDirect_(u);  // the impl is alive: the duty share is still held
        }
        break;
    }
    case K::GroupConsumer: {
        auto* gc = static_cast<GroupConsumerImpl*>(impl);
        gc->releaseNoted = true;
        if (closing) {
            if (gc->active) groupConsumerHub_.finishLocal(*gc);
        } else {
            detail::HubCommand g;
            g.op     = detail::HubCommand::Op::GroupClose;
            g.handle = cmd.handle;
            groupConsumerHub_.applyCommandDirect(g, now());
        }
        break;
    }
    }
    // Reserved in the constructor; a burst beyond it allocates (cold path).
    try {
        releases_.push_back(PendingRelease{kind, impl, fence});
    } catch (...) {
        // Out of memory: the impl stays with the duty side until close hands it back.
    }
}

int Client::reapReleases_() noexcept {
    using K = detail::HandleKind;
    const std::int64_t head = agent_.commandRingHead();
    int n = 0;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < releases_.size(); ++i) {
        const PendingRelease r = releases_[i];
        bool done = false;
        if (head >= r.fence) {
            switch (r.kind) {
            case K::Producer: {
                auto* p = static_cast<producer::ProducerImpl*>(r.impl);
                if (p->releasable()) {
                    producerHub_.remove(p);
                    if (p->detachDuty()) delete p;  // the handle let go first
                    done = true;
                }
                break;
            }
            case K::TxnProducer: {
                auto* t = static_cast<producer::TransactionalProducerImpl*>(r.impl);
                if (t->closedDuty()) {
                    std::erase(txnProducers_, t);
                    if (t->status.detachProducer()) delete t;
                    done = true;
                }
                break;
            }
            case K::Subscription: {
                auto* sub = static_cast<SubscriptionImpl*>(r.impl);
                if (subscriberHub_.release(sub)) {
                    if (sub->status.detachProducer()) delete sub;
                    done = true;
                }
                break;
            }
            case K::GroupConsumer: {
                auto* gc = static_cast<GroupConsumerImpl*>(r.impl);
                if (groupConsumerHub_.release(gc)) {
                    if (gc->status.detachProducer()) delete gc;
                    done = true;
                }
                break;
            }
            }
        }
        if (done) ++n;
        else releases_[keep++] = r;
    }
    releases_.resize(keep);
    return n;
}

void Client::handBackAll_() noexcept {
    // Released impls are in the hubs' lists too (forgotten only by a reap);
    // orphaned releases not yet applied are handed back with them.
    releases_.clear();
    orphans_.clear();
    uint64_t n = 0;
    producerHub_.handBackAll([&n](producer::ProducerImpl* p) noexcept {
        ++n;
        if (p->detachDuty()) delete p;
    });
    std::vector<producer::TransactionalProducerImpl*> txns;
    txns.swap(txnProducers_);
    for (producer::TransactionalProducerImpl* t : txns) {
        ++n;
        if (t->status.detachProducer()) delete t;
    }
    subscriberHub_.handBackAll([&n](SubscriptionImpl* sub) noexcept {
        ++n;
        if (sub->status.detachProducer()) delete sub;
    });
    groupConsumerHub_.handBackAll([&n](GroupConsumerImpl* gc) noexcept {
        ++n;
        if (gc->status.detachProducer()) delete gc;
    });
    implsHandedBack_ += n;
    // A free above may have destroyed a user callable that owned another
    // handle; its release (this closing thread) queued an entry for an impl
    // the hand-back then let go: the list is dead state now.
    releases_.clear();
}

// ---------------------------------------------------------------------------
// F8c C7: Client::close()'s drain (user decision 2)
// ---------------------------------------------------------------------------

OpResult Client::issueDrain_(Completions& q, Token token) {
    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::ClientDrain;
    // Always through the ring (also on the Invoker owner): applied after every
    // registration accepted before it, so a producer created just before
    // close() is drained too, not only failed by the teardown.
    return issueTargeted_(cmd, CompletionTarget(q), token, OpKind::ClientDrain, /*viaRing=*/true);
}

void Client::drainPartDone_(void*, const Completion&, std::span<const uint8_t>) noexcept {
    // A producer's / txn producer's internal close token: its outcome is its
    // records' own (each completed with its own status); checkDrain_ decides.
}

void Client::applyClientDrain_(const detail::HubCommand& cmd) {
    detail::HubCommand pinned = cmd;
    pinCommandTarget_(pinned);  // I3: counted before anything can complete it
    DrainWaiter w;
    w.target = detail::decodeTarget(pinned);
    w.token  = static_cast<Token>(cmd.a);
    drainWaiters_.push_back(w);  // cold: one per close() call
    (void)checkDrain_();
}

int Client::checkDrain_() {
    // Begin-close every open producer and close every transactional producer
    // (also ones registered after the drain began); settled once none is open
    // or draining / closing.  Each settles within its own deadline.
    const CompletionHandler internal{&Client::drainPartDone_, nullptr};
    const int64_t t = now();
    bool settled = true;
    producerHub_.forEach([&](producer::ProducerImpl& p) {
        // closeIntake() lost: another closer (a begin-close / closeImmediately
        // still in the command ring) owns this producer's close — wait for it.
        if (p.lifeOpen() && p.closeIntake()) {
            detail::OpTarget tgt;
            tgt.handler = internal;
            p.applyBeginClose(tgt, 0, t);
        }
        if (p.lifeOpen() || p.draining()) settled = false;
    });
    for (std::size_t i = 0; i < txnProducers_.size(); ++i) {  // a handler may register one
        producer::TransactionalProducerImpl* tp = txnProducers_[i];
        if (tp->closedDuty()) continue;
        if (!tp->closingDuty()) {
            detail::OpTarget tgt;
            tgt.handler = internal;
            tp->beginClose(tgt, 0, t);
        }
        if (!tp->closedDuty()) settled = false;
    }
    if (!settled) return 0;
    std::vector<DrainWaiter> ws;
    ws.swap(drainWaiters_);
    for (const DrainWaiter& w : ws) {
        Completion c;
        c.token = w.token;
        c.op    = OpKind::ClientDrain;
        deliver_(w.target, c, {});
    }
    return static_cast<int>(ws.size());
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
    // F8c C7: held here until the SubscribeAdd is accepted; then the duty side
    // holds its share (from adoption) and the handle the other (§3.3).
    auto impl = std::make_unique<SubscriptionImpl>();
    impl->topicId       = topic.topicId;
    impl->partition     = partition;
    impl->fromWire      = opts.from.toWire();
    impl->isolation     = static_cast<uint8_t>(opts.isolation);
    impl->handler       = std::move(handler);
    // Catalog history (Task 13): the epoch note / caught-up marker callbacks (TopicHandle::catalog()).
    if (topic.topicId == rune::protocol::client::kCatalogTopicId) {
        impl->onCaughtUp     = std::move(opts.onCaughtUp);
        impl->onHistoryEpoch = std::move(opts.onHistoryEpoch);
    }

    // M2 FIX: a deadline so sweepTimedOut() can fail this subscribe if
    // SubscribeResponse never arrives (mirrors produce-ack timeout sweep).
    const auto subscribeDL = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(cfg_.subscribeTimeoutMs);

    // F8a: encode on this thread (pure), then hand the request to the duty
    // thread, which registers it as pending and sends the Subscribe frame.
    auto req = subscriber::SubscriberHub::makeSubscribeRequest(
        impl.get(), /*sessionId (stamped on adoption)=*/0, partitionKnown, opts.from.toWire(),
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
    case detail::HubSubmit::Orphaned:  // HandleRelease only — never for a SubscribeAdd
        impl->status.abandon();         // never submitted (req freed): born finished
        (void)impl->status.detachProducer();  // C7: no duty side will ever hold it
        break;
    }

    return Subscription(impl.release(), &Client::hubCommandTrampoline_, this, handleDutyThread_());
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
    if (cmd.op == Op::GroupCommitSync) {
        detail::HubCommand pinned = cmd;
        pinCommandTarget_(pinned);  // I3: counted before anything can complete it
        groupConsumerHub_.applyCommandDirect(pinned, now());
        return;
    }
    if (producer::TransactionalProducerImpl::isTxnOp(cmd.op)) {
        applyTxnCommand_(cmd, /*direct=*/true);
        return;
    }
    if (cmd.op == Op::GroupClose || cmd.op == Op::GroupCommit) {
        groupConsumerHub_.applyCommandDirect(cmd, now());
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

detail::SendResult Client::coordDirectSendTrampoline_(void* ctx, const uint8_t* buf, std::size_t len) {
    auto* self = static_cast<Client*>(ctx);
    if (self->coordIdx_ == 0) return directSendTrampoline_(ctx, buf, len);  // the seed: unchanged
    using R = transport::AeronTransport::OfferResult;
    switch (self->agent_.offerToCoordinator(self->coordIdx_, buf, len)) {
    case R::Accepted: return detail::SendResult::Accepted;
    case R::Retry:    return detail::SendResult::Retry;
    case R::Fatal:    break;
    }
    return detail::SendResult::Fatal;
}

uint32_t Client::fetchLeaderTrampoline_(void* ctx, int32_t topicId, int32_t partition) noexcept {
    return static_cast<Client*>(ctx)->agent_.leaderSessionIndex(topicId, partition);
}

int64_t Client::fetchSessionIdTrampoline_(void* ctx, uint32_t session) noexcept {
    return static_cast<Client*>(ctx)->agent_.connectedSessionId(session);
}

uint64_t Client::fetchEpochTrampoline_(void* ctx, uint32_t session) noexcept {
    return static_cast<Client*>(ctx)->agent_.connectedEpoch(session);
}

int32_t Client::fetchBrokerVersionTrampoline_(void* ctx, uint32_t session) noexcept {
    return static_cast<Client*>(ctx)->agent_.sessionBrokerVersion(session);
}

void Client::fetchReviveTrampoline_(void* ctx, uint32_t session) noexcept {
    auto* self = static_cast<Client*>(ctx);
    (void)self->agent_.reviveSession(session, self->now());
}

detail::SendResult Client::fetchOfferTrampoline_(void* ctx, uint32_t session, const uint8_t* buf,
                                                 std::size_t len) noexcept {
    using R = transport::AeronTransport::OfferResult;
    switch (static_cast<Client*>(ctx)->agent_.offerOnSession(session, buf, len)) {
    case R::Accepted: return detail::SendResult::Accepted;
    case R::Retry:    return detail::SendResult::Retry;
    case R::Fatal:    break;
    }
    return detail::SendResult::Fatal;
}

// DR-7 (dockerized test_dr7_leader_crash_restart_rejoins_follower): metadata read
// just before a failover routes a partition to the dead ex-leader.  Its session never
// connects, so no NotLeader / Redirect ever arrives — the only event is the failed
// connect attempt.  Each one of a session some partition routes to asks for fresh
// metadata (Kafka: a failed connection requests a metadata update; rate-limited by
// requestMetadataNow_); the answer re-routes the partition (and its held batches).
int Client::watchProduceRoutes_(int64_t nowNs) noexcept {
    // Review MEDIUM-2: a produce attempt timed out (Kafka: a request timeout
    // requests a metadata update) — the partition may have moved silently.
    bool ask = producerHub_.takeMetadataWanted();
    // DR-7 (rerun): a batch held on a not-yet-Connected leader session — its connect
    // timeout may exceed the delivery timeout, so a connect FAILURE can come too late.
    // The backoff-paced refresh (reset by every answer — Kafka retry.backoff): never
    // the urgent window a NotLeader / Redirect / failure uses.
    if (const uint64_t h = agent_.heldOffers(); h != heldOffersSeen_) [[unlikely]] {
        heldOffersSeen_ = h;
        scheduleMetadataRefresh();
    }
    const auto n = static_cast<uint32_t>(agent_.sessionCount());
    if (n > routeConnectFailuresSeen_.size()) [[unlikely]] {
        try {
            routeConnectFailuresSeen_.resize(n, 0);
            routeSessionUp_.resize(n, 0);
        } catch (...) {
            return 0;  // allocation failure on this cold path: the next pass retries
        }
    }
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t f  = agent_.sessionConnectFailuresAt(i);
        const auto     st = agent_.sessionStateAt(i);
        const uint8_t  up = st == session::SessionState::Connected ? 1 : 0;
        const bool failed = f != routeConnectFailuresSeen_[i];
        const bool lost   = routeSessionUp_[i] != 0 && up == 0;  // review MEDIUM-1: a Connected leader dropped
        routeConnectFailuresSeen_[i] = f;
        routeSessionUp_[i]           = up;
        if (i == 0) {
            // The seed has its own reconnect path (and the seed failover).  DR client
            // bug 1: its broker may lead partitions routed through it — a seed
            // that went down / failed to connect is a metadata event too.
            if ((failed || lost) && agent_.multiNodeRouting()) ask = true;
            continue;
        }
        if (!failed && !lost && st != session::SessionState::Disconnected) [[likely]] continue;
        if (!agent_.routesUseSession(i)) continue;
        ask = ask || failed || lost;
        // Review MAJOR-1: a routed leader session that gave up (first-connect budget
        // spent before the cluster elected a new leader) is re-driven; its own connect
        // budget paces the attempts and each failure is the next refresh event — the
        // route converges whenever the failover completes, however long it takes.
        if (st == session::SessionState::Disconnected) (void)agent_.reviveSession(i, nowNs);
    }
    if (!ask) return 0;
    requestMetadataNow_();
    return 1;
}

// Redirect of a data op followed by RETARGETING THE SEED (the single-node /
// bootstrap-through-a-follower model, CB-5 B; DR-15 when the target is
// unreachable).  Extracted from the ErrorResponse handler (E2 residual round 5,
// R3) so a held per-partition follow can fall back to it.
// E2 residual round 5 (R3): how a Redirect of a NEW plain Subscribe is
// followed.  Per partition (O1) whenever the client can place the target: a
// route exists, or the target is a known node of the cluster.  Before the
// first metadata answer nothing is known — HOLD until it arrives (never skip
// to a seed retarget, which would strand the subscriptions bound on the seed).
// A target the cluster's metadata does not know (a node that just stopped, an
// unresolvable host) keeps the seed path (DR-15: stay, ask, re-send).
Client::RedirectFollow Client::redirectFollowFor_(const std::string& endpoint) const {
    if (agent_.multiNodeRouting()) return RedirectFollow::PerPartition;
    std::string ep = endpoint;
    if (const auto at = ep.find("endpoint="); at != std::string::npos) ep = ep.substr(at + 9u);
    if (metadataCache_.nodeIdForEndpoint(ep) != metadata::MetadataCache::kNoLeader) return RedirectFollow::PerPartition;
    return metadataResponses_ == 0 ? RedirectFollow::Hold : RedirectFollow::BySeed;
}

// R3: a metadata answer arrived since the follow was held — decide it now.
int Client::releaseHeldRedirectFollows_() {
    int work = 0;
    std::size_t kept = 0;
    for (std::size_t i = 0; i < heldRedirectFollows_.size(); ++i) {
        HeldRedirect h = heldRedirectFollows_[i];
        if (!subscriberHub_.isNewPendingSubscribe(h.corrId)) { ++work; continue; }   // timed out / cancelled
        if (metadataResponses_ == h.metaSeen) { heldRedirectFollows_[kept++] = std::move(h); continue; }
        ++work;
        switch (redirectFollowFor_(h.endpoint)) {
        case RedirectFollow::PerPartition:
            if (subscriberHub_.adoptRedirectedSubscribe(h.corrId) && followMovedSubscriptionRedirect_(h.corrId, h.endpoint))
                break;
            redirectBySeed_(h.corrId, h.endpoint);
            break;
        case RedirectFollow::Hold:   // still no answer applied (cannot happen: metaSeen moved)
        case RedirectFollow::BySeed:
            redirectBySeed_(h.corrId, h.endpoint);
            break;
        }
    }
    heldRedirectFollows_.resize(kept);
    return work;
}

void Client::redirectBySeed_(int64_t corrId, const std::string& newEndpoint) {
    // Retarget transport to the leader's client endpoint.
    // If the endpoint is already a full Aeron channel URI (starts with
    // "aeron:"), use it as-is (test / ipc scenarios).  Otherwise treat
    // it as host:port and build a UDP channel.  This allows both
    // production UDP redirects and in-process ipc test redirects.
    const std::string newChannel = toChannel(newEndpoint);  // N2

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
        redirectedResendBackoffNs_ = kRedirectedResendInitNs;  // DR-15: a reachable target
    } else {
        // DR-15 (SIGTERM parity, Docker 2026-10-05): the redirect named a node
        // that is unreachable right now (it just stopped — its host is gone).
        // Never terminal: stay on the current node, ask for metadata, and
        // re-send the pending Subscribes on the answer (the cluster moves on;
        // the next answer redirects to the live leader or serves it).  The
        // redirected Produce stays in flight — its attempt timer re-offers it
        // (no redirect budget spent: nothing was re-sent).
        if ((unreachableRedirects_++ & 1023u) == 0)
            std::fprintf(stderr, "rune-client: redirect target %s unreachable — staying, asking for metadata "
                         "(%llu so far)\n", newEndpoint.c_str(),
                         static_cast<unsigned long long>(unreachableRedirects_));
        // Review M1: still an answer for the produce's in-flight accounting.
        producerHub_.noteRedirectAnswer(corrId);
        // Review MAJOR-1: only THIS Subscribe is re-sent (never the others,
        // which still await their own answer — a duplicate would register a
        // second subscription broker-side).
        if (subscriberHub_.isPending(corrId) &&
            std::find(redirectedSubscribes_.begin(), redirectedSubscribes_.end(), corrId) ==
                redirectedSubscribes_.end()) {  // re-review NEW-M1: once per pending resend
            try {
                redirectedSubscribes_.push_back(corrId);
            } catch (...) {
                // allocation failure (cold path): it times out as before
            }
        }
        // Review M3: paced — the re-send waits for a fresh metadata answer
        // AND a doubling backoff (100 ms .. 2 s); never a tight redirect loop.
        redirectMetaSeen_       = metadataResponses_;
        nextRedirectedResendNs_ = now() + redirectedResendBackoffNs_;
        redirectedResendBackoffNs_ = std::min<int64_t>(redirectedResendBackoffNs_ * 2, kRedirectedResendMaxNs);
        requestMetadataNow_();
    }
}

int Client::retryRedirectedSubscribes_(int64_t nowNs) {
    if (metadataResponses_ == redirectMetaSeen_ || nowNs < nextRedirectedResendNs_) return 0;  // no new answer yet
    const auto snap = agent_.stateAndSessionId();
    if (snap.state != session::SessionState::Connected || snap.sessionId == 0) {
        // Review MAJOR-2: the reconnect's resendPending re-sends every pending
        // Subscribe on the new session — never a second copy from here.
        redirectedSubscribes_.clear();
        return 0;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.subscribeTimeoutMs);
    int sent = 0;
    for (const int64_t corr : redirectedSubscribes_) {
        sent += subscriberHub_.resendOne(
                    corr, snap.sessionId,
                    [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                        return agent_.submitSend(fb, fl);
                    },
                    deadline) ? 1 : 0;
    }
    redirectedSubscribes_.clear();
    return sent;
}

int Client::watchGroupSessions_(int64_t nowNs) {
    const uint32_t leaders = agent_.leaderSessionCount();
    if (leaders == 0) return 0;
    uint64_t up = 0, disc = 0;
    for (uint32_t i = 1; i <= leaders && i <= 64; ++i) {
        const session::SessionState st = agent_.sessionStateAt(i);
        if (st == session::SessionState::Connected) up |= uint64_t{1} << (i - 1);
        else if (st == session::SessionState::Disconnected) disc |= uint64_t{1} << (i - 1);
    }
    const uint64_t lost      = groupUpMask_ & ~up;
    const uint64_t newlyDisc = disc & ~groupDiscMask_;
    groupUpMask_   = up;
    groupDiscMask_ = disc;
    const uint64_t events = lost | newlyDisc;
    if (events == 0) [[likely]] return 0;
    bool affected = false;
    for (uint32_t i = 1; i <= leaders && i <= 64; ++i) {
        const uint64_t bit = uint64_t{1} << (i - 1);
        if ((events & bit) == 0 || !groupConsumerHub_.fetchUses(i)) continue;
        affected = true;
        (void)groupConsumerHub_.resendUnanswered(i);  // that session's unanswered Subscribes
        // A terminally Disconnected node session the group depends on never
        // stays so: a fresh connect (its own connect budget paces the retries —
        // each failure is the next event).
        if ((newlyDisc & bit) != 0 && agent_.reviveSession(i, nowNs)) groupDiscMask_ &= ~bit;
    }
    if (!affected) return 0;
    // The leader may have moved: ask now (rate-limited, falls back to the
    // backoff refresh); the answer re-routes (onMetadataResponse -> reconcile).
    requestMetadataNow_();
    (void)groupConsumerHub_.reconcileFetch();
    return 1;
}

int64_t Client::nextCorrTrampoline_(void* ctx) noexcept {
    return static_cast<Client*>(ctx)->nextCorrId_++;
}

bool Client::followMovedSubscriptionRedirect_(int64_t corrId, std::string_view endpoint) noexcept {
    int32_t topicId = -1, partition = -1;
    if (!subscriberHub_.movedPendingPartition(corrId, topicId, partition)) return false;
    try {
        const std::string newEndpoint(endpoint);
        // As a produce / group fetch Redirect: the cache learns the leader
        // provisionally (a lagging answer cannot undo it) and the partition's
        // route follows it — only this partition, never the seed session.
        std::string cacheEp = newEndpoint;
        if (const auto at = cacheEp.find("endpoint="); at != std::string::npos) cacheEp = cacheEp.substr(at + 9u);
        const int32_t node = metadataCache_.nodeIdForEndpoint(cacheEp);
        if (node != metadata::MetadataCache::kNoLeader) {
            const int32_t cur = metadataCache_.epochFor(topicId, partition);
            metadataCache_.updatePartitionLeaderProvisional(topicId, partition, node, cur >= 0 ? cur + 1 : 0,
                                                            nextCorrId_);
        }
        const std::string ch = toChannel(newEndpoint);
        if (agent_.setRoute(topicId, partition, ch, ch)) producerHub_.noteRouteChanged(topicId, partition);
    } catch (...) {
        // Allocation failure on this cold path: routed by the current route;
        // the metadata answer below re-routes.
    }
    (void)subscriberHub_.homeMoved(corrId, agent_.stateAndSessionId().sessionId);
    requestMetadataNow_();
    return true;
}

void Client::onGroupFetchRefused_(int64_t corrId, bool redirect, std::string_view endpoint) noexcept {
    int32_t topicId = -1, partition = -1;
    // A bare NotLeader parks the partition until fresh metadata names the
    // leader (asked for below); a Redirect follows its endpoint at once.
    if (!groupConsumerHub_.onFetchSubscribeRefused(corrId, /*park=*/!redirect || endpoint.empty(),
                                                   topicId, partition)) {
        return;  // stale (re-routed / revoked since): nothing to follow
    }
    if (redirect && !endpoint.empty()) {
        try {
            const std::string newEndpoint(endpoint);
            // As a produce Redirect (CB-5 B / MD1 IMPORTANT-1): the cache learns
            // the leader provisionally (a lagging answer cannot undo it) and the
            // partition's route follows it — only this partition, never the seed
            // (= the group coordinator) session.
            std::string cacheEp = newEndpoint;
            if (const auto at = cacheEp.find("endpoint="); at != std::string::npos) cacheEp = cacheEp.substr(at + 9u);
            const int32_t node = metadataCache_.nodeIdForEndpoint(cacheEp);
            if (node != metadata::MetadataCache::kNoLeader) {
                const int32_t cur = metadataCache_.epochFor(topicId, partition);
                metadataCache_.updatePartitionLeaderProvisional(topicId, partition, node, cur >= 0 ? cur + 1 : 0,
                                                                nextCorrId_);
            }
            const std::string ch = toChannel(newEndpoint);
            if (agent_.setRoute(topicId, partition, ch, ch)) producerHub_.noteRouteChanged(topicId, partition);
        } catch (...) {
            // Allocation failure on this cold path: the metadata refresh below re-routes.
        }
    }
    requestMetadataNow_();
    (void)groupConsumerHub_.reconcileFetch();
}

void Client::applyHubCommand_(const detail::HubCommand& cmd) {
    syncSeedSession_();  // fix4 I-1: the hubs act on the session's CURRENT state
    using Op = detail::HubCommand::Op;
    switch (cmd.op) {  // F8c C2/C3: the Client's own commands
    case Op::CqAdd:
        applyCqAdd_(reinterpret_cast<detail::CompletionRing*>(cmd.handle));
        return;
    case Op::CqRelease:
        applyCqRelease_(static_cast<CqRef>(cmd.a), reinterpret_cast<detail::CompletionRing*>(cmd.handle),
                        static_cast<uint64_t>(cmd.b));
        return;
    case Op::ProducerBeginClose:
    case Op::ProducerCloseNow:
        applyProducerCommand_(cmd);
        return;
    case Op::HandleRelease:  // F8c C7: via the ring — FIFO behind the handle's own ops
        applyRelease_(cmd, 0, closing_);
        return;
    case Op::ClientDrain:    // F8c C7: Client::close()
        applyClientDrain_(cmd);
        return;
    case Op::ClientSetCallback:
        applySetCallback_(static_cast<int>(cmd.a), reinterpret_cast<void*>(cmd.handle));
        return;
    default:
        break;
    }
    if (producer::TransactionalProducerImpl::isTxnOp(cmd.op)) {  // F8c C5
        applyTxnCommand_(cmd, /*direct=*/false);
        return;
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
        SubscriptionImpl* adopted = nullptr;  // F1: check the adopted impl for an orphaned release
        if (cmd.op == Op::SubscribeAdd && cmd.handle != 0) {
            adopted = reinterpret_cast<subscriber::SubscribeRequest*>(cmd.handle)->impl;
        }
        const auto snap = agent_.stateAndSessionId();
        const int64_t unsubCorrId = (cmd.op == Op::Unsubscribe) ? nextCorrId_++ : 0;
        subscriberHub_.applyCommand(
            cmd, unsubCorrId, snap.sessionId,
            [this](int32_t /*streamId*/, const uint8_t* fb, std::size_t fl) -> bool {
                return agent_.submitSend(fb, fl);
            });
        if (adopted != nullptr && adopted->status.consumerOrphaned() && !adopted->releaseNoted) {
            adopted->releaseNoted = true;  // its handle orphaned the release before this adoption
            noteOrphan_(detail::HandleKind::Subscription, adopted, adopted->orphanFence);
        }
    } else if (cmd.op == Op::GroupCommitSync) {
        detail::HubCommand pinned = cmd;
        pinCommandTarget_(pinned);  // I3: counted before anything can complete it
        groupConsumerHub_.applyCommand(pinned, now());
    } else {
        groupConsumerHub_.applyCommand(cmd, now());
        if (cmd.op == Op::GroupAdd) {
            // MEDIUM-7: adopted after the seed connected — join now (the
            // connect-time onConnected already ran).
            // D4: on the coordinator session (the seed by default).
            if (agent_.sessionMachineAt(coordIdx_).state() == session::SessionState::Connected) {
                groupConsumerHub_.joinIfConnected(coordSessionId_(), coordSendFn_);
            }
        }
        if (cmd.op == Op::GroupAdd && cmd.handle != 0) {  // F1: as for subscriptions
            auto* gc = reinterpret_cast<GroupConsumerImpl*>(cmd.handle);
            if (gc->status.consumerOrphaned() && !gc->releaseNoted) {
                gc->releaseNoted = true;
                noteOrphan_(detail::HandleKind::GroupConsumer, gc, gc->orphanFence);
            }
        }
    }
}

detail::HubSubmit Client::hubCommandTrampoline_(void* ctx, const detail::HubCommand& cmd) {
    using Op = detail::HubCommand::Op;
    auto* self = static_cast<Client*>(ctx);
    // F8c C7: a handle's §3.3 release has its own routing (closer thread,
    // duty work, or the ring) — decided before the closed-ring check.
    if (cmd.op == Op::HandleRelease) return self->submitRelease_(cmd);
    const bool onDuty = std::this_thread::get_id() == self->dutyThreadId_;

    // F8c C4: GroupCommitSync is a token op (targetedOpTrampoline_), never
    // submitted here; the token form is allowed from a duty-cycle callback.
    if (self->agent_.commandRingClosed()) return detail::HubSubmit::Closed;

    // fix1 C1(a): on the duty thread the state is ours — apply directly (never
    // through the ring, so never blocked by it).  fix2: the registrations
    // (SubscribeAdd / GroupAdd) too — otherwise a direct Unsubscribe / commit /
    // close on the owner could OVERTAKE its own still-queued registration and
    // be applied to a subscription/consumer the hub does not hold yet.  Not
    // GroupSetCallback (it could replace a running callback): ring order.
    if (onDuty && (cmd.op == Op::SubscribeAdd || cmd.op == Op::GroupAdd ||
                   cmd.op == Op::Unsubscribe || cmd.op == Op::GroupClose ||
                   cmd.op == Op::GroupCommit || cmd.op == Op::TxnBegin || cmd.op == Op::TxnSend ||
                   cmd.op == Op::TxnCloseNow)) {
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

    auto impl = std::make_unique<GroupConsumerImpl>();  // C7: see subscribeImpl
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
    // into the single-owner hub list.  F8c C7: the impl's own address (no
    // shared_ptr holder); a throw before the submit frees it here.
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupAdd;
    cmd.handle = reinterpret_cast<uint64_t>(impl.get());
    switch (hubCommandTrampoline_(this, cmd)) {
    case detail::HubSubmit::Accepted:
        groupSubscribed_ = true;
        break;
    case detail::HubSubmit::Closed:
    case detail::HubSubmit::Orphaned:  // HandleRelease only — never for a GroupAdd
        impl->status.abandon();               // never submitted: born finished
        (void)impl->status.detachProducer();  // C7: no duty side will ever hold it
        break;
    }

    return GroupConsumer(impl.release(), &Client::hubCommandTrampoline_, this, handleDutyThread_(),
                         &Client::targetedOpTrampoline_, &Client::andWaitTrampoline_);
}

// ---------------------------------------------------------------------------
// TX-5: transactionalProducer() — create and register a TransactionalProducer
// ---------------------------------------------------------------------------

TransactionalProducer Client::transactionalProducer(const std::string& transactionalId,
                                                     ProducerConfig cfg)
{
    // nextCorrId_ is duty-owned: the txn producer only draws ids on the duty
    // thread (doWork / response handlers), so it shares the plain counter.
    auto impl = std::make_unique<producer::TransactionalProducerImpl>(  // C7: see producer()
        transactionalId,
        cfg,
        nextCorrId_);
    // F8c C5: its completions go through the completion-queue registry; F14:
    // NotCoordinator asks this Client to re-discover the coordinator.  Both
    // are set before the impl is published to the duty thread (the ring).
    impl->setCompletionSink(detail::CompletionSink{&Client::deliverTrampoline_, this});
    impl->setRediscoverHook(&Client::rediscoverTrampoline_, this);

    // Shared-nothing registration through the agent ring (like producer()): the
    // duty-cycle thread adopts it into txnProducers_ — no mutex, no per-cycle copy.
    // F8c C2: linearized against close() by the ring (see producerWithRingCap_):
    // Closed = never written — the producer is born closed: its status channel
    // abandoned (every later operation is rejected Closed at once instead of
    // queueing work no duty cycle will ever process) and the duty share
    // detached (F8c C7: the handle alone owns and frees it).
    producer::TransactionalProducerImpl* raw = impl.get();
    if (!submitRegistration_([&] { return agent_.submitRegisterTxnProducer(raw); }, [] {})) {
        impl->status.abandon();
        (void)impl->status.detachProducer();  // C7: no duty side will ever hold it
    }
    return TransactionalProducer(impl.release(), &Client::hubCommandTrampoline_, this, handleDutyThread_(),
                                 &Client::targetedOpTrampoline_, &Client::andWaitTrampoline_);
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
        e.ingressEndpoint = p.ingressEndpoint;  // Task 16: owning worker's ingress ("" = node)
        partitions.push_back(std::move(e));
    }

    // NEW-1: the answered request's correlationId orders it against redirects.
    metadataCache_.updateFromResponse(brokers, partitions, resp.correlationId);

    // Task 1.3: (re)build the agent-owned partition routing table IN PLACE.  For
    // each partition with a known leader + per-worker ingressStreamId, map the
    // stream id → the leader node's ingress endpoint channel.  setRoute() drops the
    // entry when the leader resolves to the seed/default endpoint (single-node =
    // every partition on the seed ⇒ empty route table ⇒ byte-identical hot path).
    // The producer's flush→sendFn boundary then routes each record to its leader
    // session by ingressStreamId — replacing the single-transport retarget model.
    //
    // Task 16 (shard-mode brokers, spec §3.3): a non-empty per-partition
    // ingressEndpoint routes Produce frames STRAIGHT to the owning worker's ingress
    // (no gateway -> runtime -> worker-ring relay); Connect / the session id stay
    // on the node endpoint.  Empty keeps the legacy routing.
    //
    // Only entries the cache accepted (current epoch) change anything (M1, N1): a
    // stale entry neither moves nor withdraws a route nor changes a stream.
    //
    // Re-offer rule (one rule, every branch — M2): the partition's in-flight
    // batches are re-offered at once, unchanged (same frame, same producer
    // identity; the broker dedups; a late ack from the old owner is ignored) when
    //   (a) the produce channel LEAVES a worker endpoint (worker -> other worker,
    //       or the endpoint withdrawn -> the node): the old worker no longer owns
    //       the partition and will not answer them; or
    //   (b) the partition is NotLeader-parked (C1): the old owner refused them.
    // Otherwise (a first node -> worker install, or a legacy node route) nothing
    // is re-offered: the batches already went to a node whose gateway relays them
    // to the owner (shard brokers, spec §3.3) and are acked there by batch id.
    // Trade-off: a direct batch N+1 can overtake a relayed batch N — idempotent
    // producers pay one OutOfOrderSequence park round trip (healed by
    // parkOutOfOrder_); non-idempotent ones may reorder across that one hand-over
    // (Kafka max.in.flight>1 without idempotence) rather than duplicate.  Other
    // resend mechanisms (Redirect retry; resendAllViaRoute when a session
    // reaches Connected) keep their own rules.
    const bool mnDbg = (std::getenv("RUNE_MN_DEBUG") != nullptr);  // cold: once per metadata response
    int mnRoutes = 0;
    std::vector<std::pair<int32_t, int32_t>> moved;  // partitions that left a worker endpoint
    for (const auto& p : partitions) {
        const bool current = metadataCache_.epochFor(p.topicId, p.partition) == p.leaderEpoch;
        if (!current) continue;  // stale (lower epoch than the cache): ignored entirely
        // E5.1: propagate the per-worker ingressStreamId to the registered producers.
        if (p.ingressStreamId != 0) {
            producerHub_.updateIngressRoute(p.topicId, p.partition, p.ingressStreamId);
        }
        const int32_t leaderNode = metadataCache_.leaderFor(p.topicId, p.partition);
        const std::string ep = (leaderNode == metadata::MetadataCache::kNoLeader)
                                   ? std::string() : metadataCache_.endpointFor(leaderNode);
        if (mnDbg) {
            std::fprintf(stderr, "[MNDBG route] topic=%d part=%d ingressStream=%d leaderNode=%d ep='%s' worker='%s'\n",
                         p.topicId, p.partition, p.ingressStreamId, leaderNode, ep.c_str(),
                         p.ingressEndpoint.c_str());
        }
        const std::string prevData    = agent_.dataChannelFor(p.topicId, p.partition);
        const std::string prevSession = agent_.routeEndpointFor(p.topicId, p.partition);
        const bool hadWorker = !prevData.empty() && !agent::ClientAgent::sameEndpoint(prevData, prevSession);
        std::string session, data;
        if (!p.ingressEndpoint.empty()) {
            session = ep.empty() ? currentLeaderChannel() : toChannel(ep);
            data    = toChannel(p.ingressEndpoint);
        } else if (p.ingressStreamId != 0 && !ep.empty()) {
            session = data = toChannel(ep);  // legacy Task-1.3 node route
        } else if (hadWorker) {
            session = data = prevSession;    // endpoint withdrawn, no node route: back to the node
        } else {
            continue;
        }
        const bool changed = agent_.setRoute(p.topicId, p.partition, session, data);
        // NEW-4(b): any route change is a new lane for the partition's later
        // offers (even when nothing is re-offered now — a first worker install).
        if (changed) producerHub_.noteRouteChanged(p.topicId, p.partition);
        // Review MEDIUM-2: any data-channel move (worker OR node route — a failover)
        // re-offers the stored batches on the new route now (same identity; the
        // new leader dedups), never waiting for the attempt timer.
        if (changed && !prevData.empty() && !agent::ClientAgent::sameEndpoint(prevData, data)) {
            moved.emplace_back(p.topicId, p.partition);
        }
        ++mnRoutes;
    }
    if (mnDbg) std::fprintf(stderr, "[MNDBG route] built %d routes from %zu partitions (%zu moved)\n",
                            mnRoutes, partitions.size(), moved.size());
    // The re-offers run in this duty cycle's flush pass (in order, on the new
    // route); acks that arrived earlier in this poll are applied first so an
    // acked batch is never re-offered.
    (void)routeBufferedAcks_();
    for (const auto& [t, pp] : moved) producerHub_.resendPartitionViaRoute(t, pp);
    producerHub_.resendRouteParked();  // (b): fresh metadata after a NotLeader
    ++metadataResponses_;


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
    nextMetadataRefreshAt_ = std::chrono::steady_clock::time_point::max();
    if (metadataCache_.anyProvisional()) {
        // R2-1: an answer that leaves a redirect's guess unconfirmed (requested
        // before a later redirect of the burst, or a partition the answer omits)
        // must not end the refresh cycle — re-arm WITHOUT resetting the backoff,
        // so a guess that is never confirmed backs off to the cap instead of
        // polling every 100 ms forever.
        scheduleMetadataRefresh();
    } else {
        metadataBackoffNs_ = kMetadataBackoffInitNs;
    }

    // A fresh cluster view: group members follow their partitions' leaders
    // (a leader change re-subscribes on the new leader from the position).
    if (!groupConsumerHub_.empty()) {
        groupConsumerHub_.unparkFetch();
        (void)groupConsumerHub_.reconcileFetch();
        // MEDIUM-1: the answer still names a leader whose session is down (the
        // client noticed before the cluster re-elected): keep asking
        // (backoff-paced) until a leader the client can reach is named.
        if (groupConsumerHub_.fetchWaitsForSession()) scheduleMetadataRefresh();
    }

    // F14: a fresh cluster view — txn requests waiting for the coordinator to
    // be re-discovered (NotCoordinator) back off and re-send.
    for (auto& tp : txnProducers_) tp->onCoordinatorRediscovered();
}

// D4: see the ErrorResponse handler.  Agent / duty thread.
void Client::onCoordinatorRedirect_(const std::string& endpoint) {
    const std::string channel = toChannel(endpoint);  // LOW-1 (N2)
    // Review MEDIUM-1: an answer naming the coordinator already in use (a late
    // answer to a frame sent before the move) changes nothing at all.  r2
    // MAJOR-1: "the seed" means where the seed PHYSICALLY is (its default
    // publication), not its bootstrap identity — a seed a data-op redirect moved
    // to L that hears NotCoordinator{bootstrap} must route to the bootstrap.
    const std::string& current = coordIdx_ == 0 ? agent_.seedChannel() : coordinatorChannel_;
    if (agent::ClientAgent::sameEndpoint(channel, current)) return;
    // r2 LOW-3: a coordinator whose session was Rejected / Closed is not
    // re-followed until fresh metadata arrives (the member waits its join
    // deadline, the txn op its retry) — never a 100 ms move/fallback loop.
    if (!coordRejectedChannel_.empty()) {
        if (metadataResponses_ == coordRejectedMeta_ &&
            agent::ClientAgent::sameEndpoint(channel, coordRejectedChannel_)) {
            scheduleMetadataRefresh();
            return;
        }
        coordRejectedChannel_.clear();
    }
    ++coordinatorRedirects_;
    // That node's ONE session (lazily opened / re-driven) — 0 = the seed is there.
    coordIdx_ = agent_.coordinatorSessionIndex(channel, now());
    if (coordIdx_ == 0) coordinatorChannel_.clear();
    else                coordinatorChannel_ = channel;
    coordFailBase_ = agent_.sessionConnectFailuresAt(coordIdx_);
    groupConsumerHub_.onCoordinatorMoved();   // members -> Rejoining (there)
    syncCoordinatorSession_();                // already connected: they join at once
}

// D4: see syncSeedSession_ / onCoordinatorRedirect_.  Duty thread.
void Client::syncCoordinatorSession_() noexcept {
    const session::SessionMachine& m = agent_.sessionMachineAt(coordIdx_);
    const session::SessionState st  = m.state();
    if (coordIdx_ != 0) [[unlikely]] {
        // Fallback: the coordinator session gave up (its connect budget is spent
        // — terminal Disconnected / Rejected / Closed — or a failed attempt sent
        // it to Backoff): back to the seed, whose node answers again with the
        // current coordinator.  Paced by that session's own connect / backoff
        // cycle (each failure is one event), never a spin.
        const bool gaveUp = st == session::SessionState::Disconnected ||
                            st == session::SessionState::Rejected ||
                            st == session::SessionState::Closed ||
                            (st == session::SessionState::Backoff &&
                             agent_.sessionConnectFailuresAt(coordIdx_) != coordFailBase_);
        if (gaveUp) {
            ++coordinatorFallbacks_;
            if (st == session::SessionState::Rejected || st == session::SessionState::Closed) {
                coordRejectedChannel_ = coordinatorChannel_;   // r2 LOW-3
                coordRejectedMeta_    = metadataResponses_;
            }
            coordinatorChannel_.clear();
            coordIdx_ = 0;
            groupConsumerHub_.onCoordinatorMoved();
            syncCoordinatorSession_();  // once: the seed's tuple (coordIdx_ == 0 now)
            return;
        }
    }
    const int64_t sid     = m.sessionId();
    const int64_t episode = m.correlationId();
    if (coordIdx_ == lastCoordIdx_ && st == lastCoordState_ && sid == lastCoordSid_ &&
        episode == lastCoordEpisode_) [[likely]] {
        return;
    }
    const bool connected = st == session::SessionState::Connected;
    groupConsumerHub_.setSessionConnected(connected, sid, episode);
    lastCoordIdx_     = coordIdx_;
    lastCoordState_   = st;
    lastCoordSid_     = sid;
    lastCoordEpisode_ = episode;
    // A coordinator session of its own (re)connected: its members join there at
    // once (the seed's own connect does this in the ConnectResponse handler; a
    // TLS session connects on a later handshake leg — this level check sees it).
    if (coordIdx_ != 0 && connected && !groupConsumerHub_.empty()) {
        groupConsumerHub_.setBrokerVersion(agent_.sessionBrokerVersion(coordIdx_));
        groupConsumerHub_.onConnected(sid, coordSendFn_);
        (void)groupConsumerHub_.reconcileFetch();
    }
}

void Client::rediscoverTrampoline_(void* ctx) noexcept {
    // F14: NotCoordinator -> re-discover the coordinator (== the consensus
    // leader today, F15 §5.1): ask for a fresh cluster view now.  Not
    // connected: the MetadataRequest sent on the next ConnectResponse{OK}
    // answers it instead.
    // DR client bug 1: through any Connected session (the seed may be the dead one).
    (void)static_cast<Client*>(ctx)->sendMetadataRequestAnywhere_();
}

producer::TransactionalProducerImpl* Client::findTxn_(uint64_t handle) const noexcept {
    for (producer::TransactionalProducerImpl* tp : txnProducers_) {
        if (reinterpret_cast<uint64_t>(tp) == handle) return tp;
    }
    return nullptr;
}

void Client::applyTxnCommand_(const detail::HubCommand& cmd, bool direct) {
    using Op = detail::HubCommand::Op;
    detail::HubCommand c = cmd;
    // I3: a token op's target is pinned when it is applied (before anything
    // can complete it).  Begin / Send carry no target.
    if (c.op == Op::TxnInit || c.op == Op::TxnSendOffsets || c.op == Op::TxnEnd || c.op == Op::TxnClose) {
        pinCommandTarget_(c);
    }
    // Ring path: resolve the identity against the duty-owned list (never
    // dereference an unknown one).  Direct path: the caller's handle holds
    // the impl for the whole call (it may still be awaiting adoption).
    producer::TransactionalProducerImpl* impl =
        direct ? reinterpret_cast<producer::TransactionalProducerImpl*>(c.handle) : findTxn_(c.handle);
    if (impl == nullptr) {
        producer::TransactionalProducerImpl::discardCommand(c, detail::CompletionSink{&Client::deliverTrampoline_, this});
        return;
    }
    impl->applyCommand(c, now());
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
// Task 16 fix round 1 (C1/I1/M5): an owner moved (NotLeader / Redirect) — ask for
// metadata NOW rather than after the >= 100 ms refresh backoff; at most one such
// request per kUrgentMetadataMinGapNs of the duty clock (a burst of NotLeaders —
// one per in-flight batch — sends one), the rest fall back to the backoff refresh.
void Client::requestMetadataNow_() noexcept {
    const int64_t t = now();
    const bool due = lastUrgentMetadataNs_ == INT64_MIN || t - lastUrgentMetadataNs_ >= kUrgentMetadataMinGapNs;
    // DR client bug 1: any Connected session asks (the seed's broker may be the
    // one that died) — never "only while the seed is Connected".
    if (!due || !sendMetadataRequestAnywhere_()) {
        scheduleMetadataRefresh();
        return;
    }
    lastUrgentMetadataNs_ = t;
}

void Client::tickMetadataRefresh() noexcept {
    if (nextMetadataRefreshAt_ == std::chrono::steady_clock::time_point::max()) {
        return; // not armed
    }
    if (std::chrono::steady_clock::now() < nextMetadataRefreshAt_) return;
    // DR client bug 1: through any Connected session; none -> stays armed (the
    // next sweep retries; a session that connects asks on its own).
    if (!sendMetadataRequestAnywhere_()) return;
    nextMetadataRefreshAt_ = std::chrono::steady_clock::time_point::max(); // disarm
}

bool Client::sendMetadataRequestAnywhere_() noexcept {
    const auto snap = agent_.stateAndSessionId();
    if (snap.state == session::SessionState::Connected && snap.sessionId != 0) {
        sendMetadataRequest(snap.sessionId);
        return true;
    }
    // The seed is down: the first Connected node session asks (duty-thread direct
    // send on that node's ingress, stamped with ITS session id, sealed under its
    // crypto state).  The answer is applied like any other (any session's
    // MetadataResponse reaches onMetadataResponse).
    const auto n = static_cast<uint32_t>(agent_.sessionCount());
    for (uint32_t i = 1; i < n; ++i) {
        const int64_t sid = agent_.connectedSessionId(i);
        if (sid == 0) continue;
        const int64_t corrId = nextCorrId_++;
        std::array<uint8_t, 256> buf{};
        const std::size_t len = codec::encodeMetadataRequest(buf.data(), buf.size(), std::span<const int32_t>{},
                                                             sid, corrId);
        if (len == 0) return false;
        if (agent_.offerOnSession(i, buf.data(), len) != transport::AeronTransport::OfferResult::Accepted) continue;
        pendingMetadataCorrId_ = corrId;
        ++metadataViaOtherSession_;
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// DR client bug 1: seed failover
// ---------------------------------------------------------------------------
//
// SeedSteady -> SeedFailingOver when the seed, after having been Live, lost its
// session to silence (staleSessionLosses() moved: no answer within the response
// timeout — its broker is presumed dead) or gave up reconnecting (Disconnected /
// Backoff).  Each attempt moves the seed to ONE candidate and connects fresh
// there; it ends Connected (-> SeedSteady), or fails (Disconnected / Backoff) or
// runs out its deadline (min(connect timeout, kSeedFailoverAttemptMaxNs)) -> the
// next candidate.  Closed / Rejected -> SeedSteady (terminal; nothing to move).
int Client::driveSeedFailover_(int64_t nowNs) noexcept {
    const session::SessionMachine& m = agent_.seedMachine();
    const session::SessionState    st = m.state();
    const uint64_t losses  = m.staleSessionLosses();
    const bool     newLoss = losses != seedFailoverLossesSeen_;
    seedFailoverLossesSeen_ = losses;
    if (auto* f = std::get_if<SeedFailingOver>(&seedFailover_)) [[unlikely]] {
        if (st == session::SessionState::Connected || st == session::SessionState::Closed ||
            st == session::SessionState::Rejected) {
            if (st == session::SessionState::Connected)
                std::fprintf(stderr, "rune-client: seed reconnected on %s after %u failover attempt(s)\n",
                             agent_.seedChannel().c_str(), f->attempt);
            seedFailover_ = SeedSteady{};
            return 1;
        }
        const bool gaveUp = st == session::SessionState::Disconnected || st == session::SessionState::Backoff;
        if (nowNs < f->notBeforeNs || (!gaveUp && nowNs < f->attemptDeadlineNs)) return 0;
        return startSeedFailoverAttempt_(*f, nowNs) ? 1 : 0;
    }
    if (st == session::SessionState::Connected) [[likely]] return 0;
    if (!m.everLive() || st == session::SessionState::Closed || st == session::SessionState::Rejected) return 0;
    // A data-op redirect episode owns the seed (CB-5 B: retargeted to the named
    // leader, reconnecting there); its failure is answered by the sweep (M2: the
    // pending produces / subscribes fail promptly, the latch clears) — only then
    // may the seed fail over.
    if (redirectPending()) return 0;
    const bool gaveUp = st == session::SessionState::Disconnected || st == session::SessionState::Backoff;
    if (!newLoss && !gaveUp) return 0;
    seedFailover_ = SeedFailingOver{};
    return startSeedFailoverAttempt_(std::get<SeedFailingOver>(seedFailover_), nowNs) ? 1 : 0;
}

bool Client::startSeedFailoverAttempt_(SeedFailingOver& f, int64_t nowNs) noexcept {
    const std::string here = agent_.seedChannel();
    std::string target;
    try {
        // 1. a broker a node session is Connected to RIGHT NOW (proven alive).
        const auto n = static_cast<uint32_t>(agent_.sessionCount());
        for (uint32_t i = 1; i < n && target.empty(); ++i) {
            if (agent_.connectedSessionId(i) == 0) continue;
            const std::string& ch = agent_.sessionChannelAt(i);
            if (!ch.empty() && !agent::ClientAgent::sameEndpoint(ch, here)) target = ch;
        }
        // 2. else rotate over the metadata's brokers and the bootstrap (node id
        //    order, the seed's current broker excluded; it alone when nothing else).
        if (target.empty()) {
            seedCandidates_.clear();
            std::vector<std::pair<int32_t, std::string>> brokers;   // cold: once per attempt
            metadataCache_.forEachBrokerEndpoint(
                [&](int32_t id, const std::string& ep) { brokers.emplace_back(id, ep); });
            std::sort(brokers.begin(), brokers.end());
            auto add = [&](const std::string& ch) {
                if (ch.empty() || agent::ClientAgent::sameEndpoint(ch, here)) return;
                for (const std::string& c : seedCandidates_)
                    if (agent::ClientAgent::sameEndpoint(c, ch)) return;
                seedCandidates_.push_back(ch);
            };
            for (const auto& [id, ep] : brokers) add(toChannel(ep));
            add(cfg_.gatewayChannel);
            target = seedCandidates_.empty() ? here : seedCandidates_[f.attempt % seedCandidates_.size()];
        }
    } catch (...) {
        target = here;  // allocation failure (cold path): reconnect where it is
    }
    ++f.attempt;
    f.notBeforeNs = nowNs + kSeedFailoverMinGapNs;   // never a tight loop over refusing brokers
    const int64_t connectNs = cfg_.connectTimeoutMs > 0 ? cfg_.connectTimeoutMs * 1'000'000LL : kSeedFailoverAttemptMaxNs;
    f.attemptDeadlineNs = nowNs + std::min(connectNs, kSeedFailoverAttemptMaxNs);
    if (!agent_.failoverSeedTo(target, nowNs)) {
        f.attemptDeadlineNs = f.notBeforeNs;  // did not resolve: the next candidate shortly
        return false;
    }
    if ((seedFailovers_++ & 63u) == 0)
        std::fprintf(stderr, "rune-client: seed broker %s presumed dead (no answer / gave up) — seed moved to %s "
                     "(attempt %u, %llu so far)\n", here.c_str(), target.c_str(), f.attempt,
                     static_cast<unsigned long long>(seedFailovers_));
    // The leader of what the seed's broker led must be learnt from a live broker now.
    requestMetadataNow_();
    return true;
}

} // namespace rune::client
