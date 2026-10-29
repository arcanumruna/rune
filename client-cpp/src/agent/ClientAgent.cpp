// src/agent/ClientAgent.cpp
//
// CC-5 — ClientAgent implementation.
//
// Single-writer duty cycle: ring drain + transport poll + session tick.
// App threads ONLY touch the ManyToOneRingBuffer (no locks around session/transport).
//
// doWork steps:
//   1. Drain the command ring (bounded, ringDrainLimit records).
//   2. Poll transport egress (bounded, egressPollLimit fragments).
//   3. Tick session machine (keepalive/timeouts).
// Returns total work count; returns 0 when idle (no busy-spin).

#include <rune/client/agent/ClientAgent.h>
#include <rune/client/agent/AgentRingMessages.h>
#include <rune/client/codec/CryptoFrame.h>  // Task 6: data-plane seal/open
#include <rune/client/codec/DataCodec.h>
#include <rune/crypto/Kdf.h>  // I3: deriveWorkerBindToken
#include <rune/protocol/client/MessageHeader.h>  // Task 1.3: re-stamp sessionId (offset 8)

#include <algorithm>
#include <cassert>
#include <chrono>     // Task 1.3: nowNs for lazy session connect
#include <cstdlib>    // aligned_alloc / free
#include <cstdio>     // MNDBG instrumentation
#include <cstring>    // memcpy
#include <stdexcept>
#include <thread>     // fix3 I1: std::this_thread::yield() in drainRegistrationsForClose

namespace rune::client::agent {

// ---------------------------------------------------------------------------
// Helpers — aligned allocation for the ring buffer
// ---------------------------------------------------------------------------

namespace {

/// Allocate |size| bytes aligned to |align| bytes.
/// Returns a unique_ptr with a custom deleter (::free).
std::unique_ptr<uint8_t, void(*)(void*)>
makeAlignedBuffer(std::size_t size, std::size_t align)
{
    void* raw = nullptr;
#if defined(_WIN32)
    raw = _aligned_malloc(size, align);
#else
    // posix_memalign requires alignment >= sizeof(void*).
    if (align < sizeof(void*)) align = sizeof(void*);
    if (::posix_memalign(&raw, align, size) != 0) raw = nullptr;
#endif
    if (!raw) throw std::bad_alloc{};
    // Zero-initialise so the ring sees a clean header/trailer region.
    ::memset(raw, 0, size);
    return {static_cast<uint8_t*>(raw), [](void* p){ ::free(p); }};
}

/// Compute the total ring buffer bytes needed for a given usable capacity.
/// = ringCapacity (power-of-two) + M2ORingBuffer trailer.
int32_t ringTotalBytes(int32_t cap) noexcept {
    return cap + rune::client::detail::M2ORingBuffer::trailerLength();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

namespace {
// #257 (C6 review): the password goes into the agent's fixed secret and is wiped from the config BEFORE anything can
// throw (an over-cap refusal, the transport, the ring, bad_alloc), so no unwiped heap copy is ever freed.
AgentConfig takingPassword(AgentConfig& cfg, detail::FixedSecret& secret) {
    const bool fits = secret.assign(cfg.session.password);
    detail::wipeString(cfg.session.password);
    if (!fits)
        throw std::invalid_argument("password longer than " + std::to_string(detail::FixedSecret::kCapacity) + " bytes");
    return std::move(cfg);
}

// #682: the same, for a password handed over beside the config (never inside an AgentConfig).
AgentConfig takingPasswordBeside(AgentConfig& cfg, std::string& password, detail::FixedSecret& secret) {
    const bool fits = secret.assign(password);
    detail::wipeString(password);
    detail::wipeString(cfg.session.password);
    if (!fits)
        throw std::invalid_argument("password longer than " + std::to_string(detail::FixedSecret::kCapacity) + " bytes");
    return std::move(cfg);
}
} // namespace

ClientAgent::ClientAgent(AgentConfig cfg, std::string& password)
    : cfg_(takingPasswordBeside(cfg, password, secret_))
    , transport_(cfg_.transport)
    , ringBuf_(makeAlignedBuffer(
          static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)),
          kRingAlignment))
    , ring_(ringBuf_.get(), static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)))
    , egressObserver_(nullptr)
{
    init();
}

ClientAgent::ClientAgent(AgentConfig cfg)
    : cfg_(takingPassword(cfg, secret_))
    , transport_(cfg_.transport)
    , ringBuf_(makeAlignedBuffer(
          static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)),
          kRingAlignment))
    , ring_(ringBuf_.get(), static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)))
    , egressObserver_(nullptr)
{
    init();
}

void ClientAgent::init() {
    mnDebug_ = std::getenv("RUNE_MN_DEBUG") != nullptr;
    produceAcks_.reserve(static_cast<std::size_t>(cfg_.egressPollLimit) * 4);
    // Seed the endpoint-keyed session collection with the seed session
    // (sessions_[0]).  Its IO adapter targets the transport default ingress
    // channel, so its control frames fast-path back to the default offer (the
    // byte-identical single-node path).  Task 1.3 lazily appends one Session per
    // additional leader endpoint via sessionForEndpoint().
    session::SessionConfig seedCfg = cfg_.session;
    seedCfg.keepaliveCorrelationId = -1;   // A10: sessions_[0]'s keepalive Pong routing
    sessions_.emplace_back(std::make_unique<Session>(
        this, seedCfg, cfg_.transport.ingressChannel));
    sessions_.back()->responsePort = cfg_.transport.gatewayResponseControlPort;   // NAT-safe egress: the seed's port
    workerBinds_.reserve(64);   // NAT-safe egress: one entry per worker publication; growth is cold
    sessions_.back()->machine.useScramCache(&scramCache_);   // authentication: one salted-password cache per duty thread
    sessions_.back()->machine.useSharedSecret(&secret_);
}

// ---------------------------------------------------------------------------
// Destruction
// ---------------------------------------------------------------------------

ClientAgent::~ClientAgent() {
    stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void ClientAgent::start() {
    switch (state_) {
    case Lifecycle::Started:
        return;  // already started (owner state, see ClientAgent.h)
    case Lifecycle::StartFailed:
        std::rethrow_exception(startError_);   // #510: the ORIGINAL error, every time
    case Lifecycle::Stopped:
        throw std::logic_error("ClientAgent: start() after stop()");
    case Lifecycle::Idle:
        break;
    }
    try {
        transport_.start();
    } catch (...) {
        startError_ = std::current_exception();
        state_      = Lifecycle::StartFailed;   // stop() still frees what the failed start created
        throw;
    }
    state_ = Lifecycle::Started;
}

void ClientAgent::stop() noexcept {
    if (state_ == Lifecycle::Stopped) return;  // already stopped (owner state, see ClientAgent.h)
    state_ = Lifecycle::Stopped;
    transport_.stop();
}

// ---------------------------------------------------------------------------
// doWork — single-writer duty cycle
// ---------------------------------------------------------------------------

int ClientAgent::doWork(int64_t nowNs) noexcept {
    // fix1 C2/I3: a no-op after stop() — belt-and-suspenders against any stray
    // call reaching here once the transport (and everything it touches) is
    // being/has been torn down.
    if (state_ == Lifecycle::Stopped) return 0;
    dutyNowNs_ = nowNs;  // the clock a session opened during this pass runs on

    // T15b failed mode: no driver / conductor pumping, no I/O of any kind.
    int totalWork = failed_ ? 0 : transport_.dutyCycle();  // invoker: driver + conductor; thread mode: 0

    // ---- Step 1: Drain the command ring (bounded) ---------------------------
    // fix5 Important-2(d): deliberately NO unblock-on-no-progress here (nor in
    // ProducerImpl::drainRing for the producer rings). Aeron's consumers
    // unblock because their producers are OTHER PROCESSES that can die
    // mid-claim, detected by a ~10 s client-liveness timeout. Here every
    // producer is a thread of this process: a claim can only be orphaned by an
    // exception (now impossible — M2ORingBuffer::writeWith() aborts the claim
    // on any throw, and ProducerImpl's in-place writers only memcpy between
    // tryClaim and commit) or by the process dying (which takes this consumer
    // with it). So a claim that is not yet committed is, by construction, a
    // LIVE writer that was preempted — and unblocking a live writer is
    // memory-unsafe: its later payload write lands on ring space the consumer
    // has already recycled (another record's bytes, or a registration's
    // holder pointer). A progress-based trigger ("head unmoved for N
    // non-empty polls") cannot tell "preempted" from "dead" — N polls of a
    // busy-spin runner is microseconds, far shorter than one scheduler
    // quantum — and on the producer rings an unmoved head is also the NORMAL
    // state under controlledRead ABORT back-pressure. Waiting costs latency
    // only while the writer is descheduled; unblocking risks corruption. The
    // one place that must terminate regardless — close(), after the runner is
    // joined — has its own bounded unblock (drainRegistrationsForClose).
    //
    // F8a fix2/fix3 (root cause): a wire frame accepted into the ring is NEVER
    // silently dropped by the drain.  Rule for kMsgSend (every app/hub control
    // frame on the default ingress publication: Ping, DefineTopic,
    // GetTopicSchema, MetadataRequest, JoinGroup/SyncGroup/Heartbeat/
    // OffsetFetch, group Subscribe, txn control frames, redirect/connect
    // resends of pending Subscribes):
    //   * seed session Connected AND the frame carries its sessionId -> offer:
    //       Accepted -> consumed;
    //       Retry (back-pressured / publication not connected / admin action /
    //         not resolved yet — transient) -> the record is LEFT at the head
    //         of the ring (ABORT) and offered again next duty cycle, in order;
    //         nothing behind it is read this cycle.  It is held exactly as
    //         long as the transport stays transiently unavailable — there is
    //         no time bound of its own;
    //       Fatal (publication closed / max position exceeded / Aeron threw,
    //         e.g. a frame over the publication max message length — permanent)
    //         -> dropped, logged, counted (ingressFatalDropped()).  Oversize
    //         frames are rejected synchronously by submitSend() already.
    //   * otherwise (session not Connected, or the frame carries an OLD
    //     sessionId): the frame belongs to a session that no longer exists —
    //     dropped deliberately, counted (staleFramesDropped()), logged
    //     rate-limited (first + every 1024th), never resent from here.  Owners
    //     re-issue what must survive a session change with the new sessionId
    //     (pending Subscribes on every connect, JoinGroup on connect/rejoin).
    // Unsubscribe / adoption-time Subscribe / OffsetCommit / LeaveGroup do
    // not use the ring at all (duty-thread direct send with per-cycle retry).
    // Every other command type is consumed (CONTINUE).
    int ringWork = ring_.controlledRead(
        [this, nowNs](
            std::int32_t                         msgTypeId,
            aeron::concurrent::AtomicBuffer&     buf,
            aeron::util::index_t                 offset,
            aeron::util::index_t                 len)
        {
            using CA = rune::client::detail::ControlledAction;
            switch (msgTypeId) {
            case kMsgConnect:
                if (failed_) break;  // T15b: a failed client never connects again
                seedSession().machine.connect(nowNs);
                break;

            case kMsgReconnect:
                // CB-5 B: force a fresh reconnect (data-op redirect).  The
                // transport was already retarget()ed to the leader; drop the
                // stale follower session and connect fresh so the leader
                // assigns a new sessionId.
                if (failed_) break;
                seedSession().machine.forceReconnect(nowNs);
                break;

            case kMsgTestFault:
                testFault_ = true;
                break;

            case kMsgDisconnect:
                seedSession().machine.disconnect();
                break;

            case kMsgClose:
                seedSession().machine.close();
                break;

            case kMsgSend:
                if (failed_) {  // T15b: no I/O — the frame's session is gone for good
                    ++staleFramesDropped_;
                    break;
                }
                if (len > 0) {
                    const auto* p = reinterpret_cast<const uint8_t*>(
                        buf.buffer() + offset);
                    const auto n = static_cast<std::size_t>(len);
                    const auto& m = seedSession().machine;
                    const int64_t frameSid = (n >= 16)
                        ? ::rune::protocol::client::detail::getLE64(p, 8) : 0;
                    if (m.state() != session::SessionState::Connected ||
                        frameSid != m.sessionId()) {
                        if ((staleFramesDropped_++ & 1023u) == 0) {
                            std::fprintf(stderr,
                                "rune-client: dropped control frame(s) of a session that no "
                                "longer exists (frame sessionId %lld, current %lld, "
                                "connected=%d; %llu dropped so far)\n",
                                static_cast<long long>(frameSid),
                                static_cast<long long>(m.sessionId()),
                                m.state() == session::SessionState::Connected ? 1 : 0,
                                static_cast<unsigned long long>(staleFramesDropped_));
                        }
                        sendHeadRefusedSinceNs_ = 0;  // the head record is consumed
                        break;
                    }
                    switch (offerIngress_(p, n)) {
                    case transport::AeronTransport::OfferResult::Accepted:
                        break;
                    case transport::AeronTransport::OfferResult::Retry:
                        ++ingressOfferRefused_;
                        // DR client bug 2: the head is held while the transport
                        // refuses it — but never for longer than the session's own
                        // stale threshold (responseTimeoutNs).  Every command
                        // behind it (a Completions registration, close()'s drain,
                        // handle releases, hub commands) waits on this one frame;
                        // a frame the transport refused that long is as dead as one
                        // of a lost session: dropped, counted, logged (rate-
                        // limited).  Its owner's own timer recovers it (a control
                        // op / Subscribe / group request times out or is re-sent;
                        // a MetadataRequest is asked again).
                        if (sendHeadRefusedSinceNs_ == 0) sendHeadRefusedSinceNs_ = nowNs != 0 ? nowNs : 1;
                        if (nowNs - sendHeadRefusedSinceNs_ < cfg_.session.responseTimeoutNs) {
                            return CA::ABORT;  // stays at the head; retried next cycle
                        }
                        if ((sendHeadExpired_++ & 1023u) == 0) {
                            std::fprintf(stderr,
                                "rune-client: dropped a control frame the transport refused for %lld ms "
                                "(the response timeout) at the command ring's head; %llu so far\n",
                                static_cast<long long>((nowNs - sendHeadRefusedSinceNs_) / 1'000'000),
                                static_cast<unsigned long long>(sendHeadExpired_));
                        }
                        break;
                    case transport::AeronTransport::OfferResult::Fatal:
                        noteFatalDrop_(n);  // fix4 M2: rate-limited log, every drop counted
                        break;
                    }
                    sendHeadRefusedSinceNs_ = 0;  // the head record is consumed
                }
                break;

            case kMsgRegisterProducer:
                // Shared-nothing producer registration: decode the SBE
                // RegisterProducer message and adopt the producer into the
                // single-owner ProducerHub list ON THIS (runner) thread via the
                // register hook (F8c C7: the impl itself — no holder).
                if (producerRegisterFn_) {
                    const auto* p = reinterpret_cast<const uint8_t*>(
                        buf.buffer() + offset);
                    auto msg = ring_msg::RegisterProducer::decode(
                        p, static_cast<std::size_t>(len));
                    if (msg) {
                        producerRegisterFn_(reinterpret_cast<void*>(msg->handle));
                    }
                }
                break;

            case kMsgHubCommand:
                // F8a: subscriber / group-consumer command, applied by the hub
                // that owns the state — on THIS (duty) thread.
                if (hubCommandFn_) {
                    const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                    auto cmd = ring_msg::HubCommandMsg::decode(p, static_cast<std::size_t>(len));
                    if (cmd) hubCommandFn_(*cmd);
                }
                break;

            case kMsgControlOp:
                // F8c C3: a client control op (its pending entry and its
                // completion are duty-thread owned from here on).
                if (controlOpFn_) {
                    controlOpFn_(reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                                 static_cast<std::size_t>(len));
                }
                break;

            default:
                // Unknown command: drop silently.
                break;
            }
            return CA::CONTINUE;
        },
        cfg_.ringDrainLimit);

    totalWork += ringWork;
    if (failed_) [[unlikely]] return totalWork;  // T15b: commands only — no poll, ticks or drain

    // ---- Step 2: Poll egress (bounded) --------------------------------------
    // The fragment budget never exceeds the free room in the reserved ack buffer
    // (one unfragmented ProduceResponse per fragment at most), so the hot path
    // never grows produceAcks_.  A full buffer pauses egress for this cycle; the
    // owner clears it and polling resumes next cycle (nothing is dropped).
    int pollLimit = cfg_.egressPollLimit;
    if (!produceResponseFn_) {
        const int room = static_cast<int>(produceAcks_.capacity() - produceAcks_.size());
        if (room < pollLimit) pollLimit = room;
    }
    int fragments = 0;
    if (pollLimit > 0) {
        fragments = transport_.poll(
            [this, nowNs](const uint8_t* buf, std::size_t len) {
                dispatchEgressFrame(nowNs, buf, len);
            },
            pollLimit);
    }

    totalWork += fragments;

    // ---- Step 3: Tick every session machine ---------------------------------
    // tick() drives keepalive cadence and timeout handling.  It may emit frames
    // via each session's own SessionEndpointIo → transport_.offerTo(endpoint,...).
    // We do not count ticks as work; ring + egress fragments are the sole
    // work-done signals (the IdleStrategy backs off on 0 for idle cycles).
    // Single-node: exactly one session (the seed) — same cost as before.
    for (auto& s : sessions_) {
        s->machine.tick(nowNs);
    }

    // ---- Step 3b: NAT-safe egress — WorkerBind for every worker publication whose session (re)connected ----
    // One bounded pass over the small bind table; empty (a no-op) unless Metadata advertised response-mode workers.
    if (pendingResponseUpgrades_ != 0) [[unlikely]] totalWork += serviceResponseUpgrades_();   // I6 (before the binds)
    if (!workerBinds_.empty()) totalWork += serviceWorkerBinds_();

    // ---- Step 4: CB-2 — Producer duty cycle (drain ring + flush batches) -----
    // Runs on the AGENT thread (single-writer); no lock needed.
    // NanoClock is already sampled as nowNs above — reused here.
    if (producerDrainFn_) {
        const int64_t sessId = seedSession().machine.sessionId();
        const int producerWork = producerDrainFn_(
            nowNs,
            sessId,
            [this](int32_t topicId, int32_t partition, int32_t ingressStreamId,
                   const uint8_t* buf, std::size_t len) -> bool {
                // Task 1.4: partition routing at the flush→sendFn boundary, keyed by
                // (topicId, partition).  Shared with the redirect resend path — see
                // offerProduceToLeader_.
                return offerProduceToLeader_(topicId, partition, ingressStreamId,
                                             buf, len, {});
            });
        totalWork += producerWork;
    }

    return totalWork;
}

// ---------------------------------------------------------------------------
// Thread-safe submission API
// ---------------------------------------------------------------------------

ClientAgent::Submit ClientAgent::writeCommand3_(int32_t msgTypeId, const uint8_t* payload,
                                                std::size_t payloadLen) noexcept {
    if (writeCommand(msgTypeId, payload, payloadLen)) return Submit::Accepted;
    // The closed bit is permanent: a refused write on a closed ring is Closed
    // (it was never written, whichever of full / closed refused it).
    return ring_.closed() ? Submit::Closed : Submit::Full;
}

ClientAgent::Submit ClientAgent::submitControlOp(const void* hdr, std::size_t hdrLen,
                                                 std::span<const uint8_t> frame) noexcept {
    const std::size_t total = hdrLen + frame.size();
    if (total > commandMsgLimit()) return Submit::Full;
    bool ok = false;
    try {
        ok = ring_.writeWith(kMsgControlOp, static_cast<aeron::util::index_t>(total),
                             [&](aeron::concurrent::AtomicBuffer& dst, aeron::util::index_t at) {
                                 uint8_t* p = dst.buffer() + at;
                                 std::memcpy(p, hdr, hdrLen);
                                 if (!frame.empty()) std::memcpy(p + hdrLen, frame.data(), frame.size());
                             });
    } catch (...) {
        ok = false;
    }
    if (ok) return Submit::Accepted;
    return ring_.closed() ? Submit::Closed : Submit::Full;
}

bool ClientAgent::writeCommand(int32_t msgTypeId,
                                const uint8_t* payload,
                                std::size_t payloadLen) noexcept
{
    // Guard: ManyToOneRingBuffer::write() throws IllegalArgumentException when
    // length > maxMsgLength() (= ringCapacity/8).  Because this function is
    // noexcept, an unguarded throw would call std::terminate.  Return false
    // (= backpressure / rejection) instead so the caller can retry or signal
    // an error without crashing the process.
    if (payloadLen > static_cast<std::size_t>(ring_.maxMsgLength())) {
        return false;
    }

    // Wrap the payload in an AtomicBuffer for ring_.write().
    // If payloadLen == 0 we still need a valid (non-null) buffer pointer for
    // AtomicBuffer; use a static dummy byte.
    static uint8_t kDummy[1] = {};
    uint8_t* ptr = (payloadLen > 0)
        ? const_cast<uint8_t*>(payload)
        : kDummy;

    aeron::concurrent::AtomicBuffer ab(ptr,
                                       static_cast<aeron::util::index_t>(
                                           payloadLen > 0 ? payloadLen : 1));

    // Defense-in-depth: if the ring throws for any other reason (e.g. a future
    // Aeron version introduces a new check), catch and return false rather than
    // propagating through noexcept. fix5 Important-2(a): write() (writeWith())
    // aborts its own claim before any such throw leaves it, so returning false
    // here never leaves an orphaned claim wedging the consumer (or close()).
    try {
        return ring_.write(msgTypeId, ab, 0,
                           static_cast<aeron::util::index_t>(payloadLen));
    } catch (...) {
        return false;
    }
}

bool ClientAgent::submitConnect() noexcept {
    return writeCommand(kMsgConnect, nullptr, 0);
}

bool ClientAgent::submitReconnect() noexcept {
    return writeCommand(kMsgReconnect, nullptr, 0);
}

bool ClientAgent::submitDisconnect() noexcept {
    return writeCommand(kMsgDisconnect, nullptr, 0);
}

bool ClientAgent::submitClose() noexcept {
    return writeCommand(kMsgClose, nullptr, 0);
}

bool ClientAgent::submitSend(const uint8_t* frame, std::size_t len) noexcept {
    // F8a fix3 I1(a): a frame no ingress publication can ever carry is
    // rejected HERE, synchronously — it must never reach the ring (where it
    // would be a permanent offer failure).
    if (len > maxFrameLength()) return false;
    return writeCommand(kMsgSend, frame, len);
}

transport::AeronTransport::OfferResult ClientAgent::offerDefault(const uint8_t* buf,
                                                                std::size_t len) noexcept {
    const auto r = offerIngress_(buf, len);
    if (r == transport::AeronTransport::OfferResult::Fatal) [[unlikely]] noteFatalDrop_(len);
    // F8c C3: a refusal of a duty-thread direct send (control ops, Subscribe,
    // Unsubscribe, ...) is counted like a ring-drained one (the owner retries).
    if (r == transport::AeronTransport::OfferResult::Retry) [[unlikely]] ++ingressOfferRefused_;
    return r;
}

void ClientAgent::noteFatalDrop_(std::size_t len) noexcept {
    // F8a fix4 M2: counted every time, logged on the first and every 1024th.
    if ((ingressFatalDropped_++ & 1023u) == 0) {
        std::fprintf(stderr,
            "rune-client: dropped a control frame the transport can never accept "
            "(len %zu; %llu dropped so far)\n",
            len, static_cast<unsigned long long>(ingressFatalDropped_));
    }
}

transport::AeronTransport::OfferResult ClientAgent::offerIngress_(const uint8_t* buf,
                                                                 std::size_t len) noexcept {
    if (failed_) [[unlikely]] return transport::AeronTransport::OfferResult::Fatal;  // T15b: no I/O
    if (testForcedOffers_ > 0) [[unlikely]] {
        --testForcedOffers_;
        return testForcedOffer_;
    }
    // client<->broker TLS/AEAD (Task 6b): this is the DEFAULT ingress path
    // (ring-drained kMsgSend frames, and every offerDefault() caller) — it
    // always targets the seed/bootstrap session, so seal under ITS
    // CryptoState.  Plaintext path (crypto off) is byte-identical to before
    // Task 6b.  Seal failure returns Retry (not Fatal): the frame was never
    // transmitted, and the caller's own retry (ring record stays at head /
    // Client's unsent-frame park) re-offers the ORIGINAL plaintext buf next
    // cycle — a fresh seal attempt, not a replay of a stale ciphertext.
    if (!sealForDestination_(seedSession().machine, buf, len)) {
        return transport::AeronTransport::OfferResult::Retry;  // seal failed — DROP, do not transmit
    }
    return transport_.offerResult(buf, len);
}

ClientAgent::Submit ClientAgent::submitRegisterProducer(void* impl) noexcept {
    // Encode an SBE RegisterProducer message (MessageHeader + handle) and enqueue
    // it as the command payload — self-describing/versioned, like every other ring
    // message, rather than a raw pointer memcpy.
    ring_msg::RegisterProducer msg;
    msg.handle = reinterpret_cast<uint64_t>(impl);
    uint8_t buf[ring_msg::RegisterProducer::ENCODED_SIZE];
    const std::size_t n = msg.encode(buf);
    return writeCommand3_(kMsgRegisterProducer, buf, n);
}

int ClientAgent::drainRegistrationsForClose() noexcept {
    // fix3 I1: ring_.read() stops dead at a record that is CLAIMED but not yet
    // COMMITTED (its length word is still negative) — it has no way to tell
    // "nothing more was ever written" apart from "another app thread is
    // mid-write, right now, on a producer() call that
    // raced this close()". The old `if (n == 0) break;` treated both the same
    // and could leave a fully-committed registration sitting UNREAD behind
    // that in-flight claim (leaked holder; its ring never marked closed).
    //
    // Fix: snapshot the ring's tail ONCE (acquire) as the drain target before
    // looping. `n == 0` now only means "stop" once head has caught up to that
    // snapshot — otherwise there is a committed-or-committing record still
    // ahead of head, and we yield + retry. writeCommand() (the only path that
    // claims on this ring) is claim+memcpy+commit via
    // M2ORingBuffer::writeWith(), which aborts its claim on any throw (fix5
    // Important-2(a)), so the claim->commit window is a handful of
    // instructions; this function itself only ever runs once, at close(). No
    // new shared state — head and tail are the ring's own existing protocol
    // words (shared-nothing preserved).
    //
    // fix5 Important-2(c): "a handful of instructions" is not a termination
    // proof — a writer descheduled mid-window, or any future bug that orphans
    // a claim, would wedge close() forever. So the wait is iteration-capped
    // (closeDrainUnblockAfterYields_ consecutive no-progress yields; see
    // kCloseDrainUnblockAfterYields for why an iteration count, not a time)
    // and then does what Aeron's own consumer does for a blocked
    // ManyToOneRingBuffer: unblock() — the stuck claim becomes padding and the
    // drain continues to whatever is committed behind it. If unblock() cannot
    // make progress (nothing unblockable) kCloseDrainMaxFailedUnblocks rounds
    // in a row, stop: close() must terminate. Any registration still behind
    // such a claim is then left unread — the same accepted shutdown leak as
    // Client::producerWithRingCap_'s fix3 M1 branch, never a hang.
    // Residual hazard (Aeron's own unblock() hazard, accepted here — NOT
    // harmless; see unblock()'s doc and Step 1 of doWork for why the runner
    // never does this): if the "orphan" was really a writer descheduled for the
    // whole cap, it can resume while THIS drain is still running. Its commit()
    // is refused (writeCommand returns false; nothing is published), but its
    // payload memcpy races this drain: it lands on bytes the drain has turned
    // into padding or may be reading right now, so the same drain can observe
    // a torn/overwritten region — exactly the corruption risk Aeron documents
    // for ManyToOneRingBuffer::unblock(). It is accepted only because it needs
    // a writer stalled for the whole ~1M-yield cap at close(), and close()
    // must terminate (a hang is the worse failure).
    //
    // A stall unblock() CANNOT clear: a writer that won the tail CAS for an
    // end-of-buffer wrap (padding to the buffer end + its record at index 0)
    // and stalled before writing the padding header leaves only zero words
    // from head to the buffer end — unblock()'s forward scan finds nothing to
    // pad up to and returns false (as Aeron's does). A registration committed
    // behind such a claim is never reached: after kCloseDrainMaxFailedUnblocks
    // rounds the drain stops, and that registration falls under the documented
    // accepted shutdown leak above (fix3 M1), never a hang.
    const std::int64_t targetTail = ring_.tailPosition();
    int total = 0;
    int stalledYields = 0;
    int failedUnblocks = 0;
    bool warned = false;
    for (;;) {
        const std::int64_t headBefore = ring_.headPosition();
        const int n = ring_.read(
            [this](std::int32_t msgTypeId, aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t offset, aeron::util::index_t len) {
                const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                if (msgTypeId == kMsgHubCommand) {
                    // F8a: adopt/apply so heap payloads (subscribe requests,
                    // commitSync tokens, group registrations, completion
                    // queues) are owned and then finished by the close path —
                    // never stranded.
                    if (!hubCommandFn_) return;
                    auto cmd = ring_msg::HubCommandMsg::decode(p, static_cast<std::size_t>(len));
                    if (cmd) hubCommandFn_(*cmd);
                    return;
                }
                if (msgTypeId == kMsgControlOp) {
                    // F8c C3: an accepted control op completes exactly once —
                    // here, with Closed (the hook knows the Client is closing).
                    if (controlOpFn_) controlOpFn_(p, static_cast<std::size_t>(len));
                    return;
                }
                const ProducerRegisterFn* fn = (msgTypeId == kMsgRegisterProducer) ? &producerRegisterFn_ : nullptr;
                if (fn == nullptr || !*fn) return;  // sends / lifecycle: dropped
                auto msg = ring_msg::RegisterProducer::decode(p, static_cast<std::size_t>(len));
                if (msg) (*fn)(reinterpret_cast<void*>(msg->handle));
            },
            cfg_.ringDrainLimit);
        total += n;
        const std::int64_t headAfter = ring_.headPosition();
        if (headAfter >= targetTail) break; // caught up to the snapshot: done
        if (n > 0 || headAfter != headBefore) {
            stalledYields  = 0;  // progress: reset both caps
            failedUnblocks = 0;
            continue;
        }
        // Nothing currently readable, but the snapshot says more exists: it
        // must be an in-flight (or orphaned) claim.
        if (++stalledYields >= closeDrainUnblockAfterYields_) {
            stalledYields = 0;
            ++closeDrainUnblockCalls_;
            if (!warned) {
                warned = true;
                std::fprintf(stderr,
                             "rune-client: close(): command ring blocked by a claimed-but-"
                             "uncommitted record at position %lld for %d yields; unblocking "
                             "(Aeron ManyToOneRingBuffer::unblock)\n",
                             static_cast<long long>(headAfter), closeDrainUnblockAfterYields_);
            }
            if (!testCloseDrainNoUnblock_ && ring_.unblock()) {
                failedUnblocks = 0;
                continue;
            }
            if (++failedUnblocks >= kCloseDrainMaxFailedUnblocks) {
                std::fprintf(stderr,
                             "rune-client: close(): command ring cannot be unblocked at "
                             "position %lld (target %lld); abandoning the close drain\n",
                             static_cast<long long>(headAfter),
                             static_cast<long long>(targetTail));
                break;
            }
        }
        // Test-only observability hook (fix3 I1 regression), then yield and retry.
        if (testCloseDrainYieldHook_) testCloseDrainYieldHook_();
        std::this_thread::yield();
    }
    return total;
}

void ClientAgent::testFillCommandRing() {
    std::vector<uint8_t> scratch(static_cast<std::size_t>(ring_.maxMsgLength()), 0);
    for (std::size_t len = scratch.size(); len >= 1; len /= 2) {
        while (writeCommand(kMsgTestNoop, scratch.data(), len)) {}
    }
}

ClientAgent::Submit ClientAgent::submitHubCommand(const ::rune::client::detail::HubCommand& cmd) noexcept {
    uint8_t buf[ring_msg::HubCommandMsg::ENCODED_SIZE];
    const std::size_t n = ring_msg::HubCommandMsg::encode(cmd, buf);
    return writeCommand3_(kMsgHubCommand, buf, n);
}

// ---------------------------------------------------------------------------
// Observers
// ---------------------------------------------------------------------------

void ClientAgent::setEgressObserver(EgressObserver obs) noexcept {
    egressObserver_ = std::move(obs);
}

void ClientAgent::setProducerDrainFn(ProducerDrainFn fn) noexcept {
    producerDrainFn_ = std::move(fn);
}

void ClientAgent::setProduceResponseFn(ProduceResponseFn fn) noexcept {
    produceResponseFn_ = std::move(fn);
}

void ClientAgent::setMetadataResponseFn(MetadataResponseFn fn) noexcept {
    metadataResponseFn_ = std::move(fn);
}

session::SessionState ClientAgent::sessionState() const noexcept {
    // F8c C3: duty-thread only (plain read of the duty-owned seed machine).
    return seedSession().machine.state();
}

int64_t ClientAgent::sessionId() const noexcept {
    return seedSession().machine.sessionId();
}

ClientAgent::StateSnapshot ClientAgent::stateAndSessionId() const noexcept {
    const auto& m = seedSession().machine;
    return StateSnapshot{m.state(), m.sessionId()};
}

// ---------------------------------------------------------------------------
// Seed-transport helpers (agent thread only)
// ---------------------------------------------------------------------------

bool ClientAgent::retarget(const std::string& newIngressChannel, int32_t responsePort) noexcept {
    // FIX F8: forward to the transport's noexcept, non-throwing retarget.
    if (responsePort == 0) responsePort = knownResponsePort_(newIngressChannel);
    const bool moved = transport_.retarget(newIngressChannel, responsePort);
    if (moved) {
        seedSession().responsePort = responsePort;
        seedSession().pairPinned   = false;   // the retarget built a fresh pair
        invalidateRouteSessions_();  // DR client bug 1: the seed moved
    }
    return moved;
}

bool ClientAgent::failoverSeedTo(const std::string& channel, int64_t nowNs, bool connectNow) noexcept {
    if (failed_ || channel.empty()) return false;
    try {
        if (transport_.ingressChannel() != channel) {
            const int32_t port = knownResponsePort_(channel);
            if (!transport_.retarget(channel, port)) return false;  // old pub intact: the owner tries the next one
            Session& seed             = seedSession();
            seed.currentLeaderChannel = channel;
            seed.currentLeaderNode    = -1;   // learnt from the next metadata answer
            seed.responsePort         = port;
            seed.pairPinned           = false;   // fresh pair
            invalidateRouteSessions_();
        }
    } catch (...) {
        return false;  // allocation failure (cold path): the next candidate / pass retries
    }
    // A fresh session there (resetting the old broker session id: that broker
    // is gone, a resume hint would only cost the new one a SessionExpired).
    if (connectNow) seedSession().machine.forceReconnect(nowNs);
    return true;
}

// ---------------------------------------------------------------------------
// dispatchEgressFrame — decode + route one reassembled egress frame
// ---------------------------------------------------------------------------

bool ClientAgent::answersAnInFlightConnect_(const uint8_t* buf) const noexcept {
    const int64_t corr = rune::protocol::client::detail::getLE64(buf, 16);
    for (const auto& sp : sessions_)
        if (sp->machine.state() == session::SessionState::Connecting && sp->machine.correlationId() == corr) return true;
    return false;
}

void ClientAgent::dispatchEgressFrame(int64_t nowNs,
                                       const uint8_t* buf,
                                       std::size_t    len) noexcept
{
    // client<->broker TLS/AEAD (Task 6): decrypt BEFORE checkFrame, when this
    // frame belongs to an encrypted session.  The 24B MessageHeader is ALWAYS
    // clear (it's the AEAD AAD), so templateId (offset 2) / sessionId (offset
    // 8) can be read directly off the wire — no decode needed to make this
    // decision.  ConnectResponse / HandshakeContinueResponse are exempt: they
    // precede or complete key establishment itself and are always plaintext,
    // even on an otherwise-encrypted session (e.g. a stale duplicate delivered
    // after a later reconnect re-encrypted the session).  Plaintext path
    // (crypto off, or no encrypted session claims this sessionId) is
    // byte-identical to before Task 6 — buf/len are reassigned only on a
    // successful decrypt.
    //
    // sessionId is NOT a safe disambiguator on its own in multi-node mode: the
    // broker assigns it from a plain per-node counter starting at 1 (no node
    // id / epoch folded in), so two sessions to two different leader nodes
    // commonly share the same sessionId (e.g. both get 1 on their first
    // connect).  Rather than guess, try EVERY session currently claiming this
    // sessionId: AeadCipher::open authenticates under each candidate's own
    // key, so a wrong candidate just fails closed (0, no side effects — the
    // anti-replay window is committed only after a tag verifies) and costs
    // nothing but one extra open() call; cluster size bounds the candidate
    // count.  If at least one encrypted candidate exists but NONE
    // authenticates, this is a genuine tamper/replay (or an unresolvable
    // collision) — drop rather than risk falling through to plaintext
    // dispatch on an encrypted session's frame.
    // Retired-episode rule (#426): a session id whose keys were discarded is remembered by its machine
    // (namesRetiredSealedSession); a frame naming it that no live key opens is dropped, except a clear ErrorResponse
    // answering the new episode's in-flight ConnectRequest.
    // I2: a WorkerBindAck (87) is plaintext BY CONTRACT even on an encrypted session (it carries only the session id and
    // the bind's correlation id, no secret) and is not an ingress template (checkFrame refuses it): handled here, before
    // the sealed-session filter, and only an exact (sid, corr) match of a Sent bind changes anything.
    if (len >= rune::protocol::client::MessageHeader::ENCODED_SIZE &&
        rune::protocol::client::detail::getLE16(buf, 2) == rune::protocol::client::WorkerBindAck::TEMPLATE_ID) {
        onWorkerBindAck_(buf, len);
        return;
    }
    if (len >= rune::protocol::client::MessageHeader::ENCODED_SIZE) {
        const auto peekedTid = static_cast<codec::TemplateId>(
            rune::protocol::client::detail::getLE16(buf, 2));
        if (peekedTid != codec::TemplateId::ConnectResponse &&
            peekedTid != codec::TemplateId::HandshakeContinueResponse) {
            const int64_t sid = rune::protocol::client::detail::getLE64(buf, 8);
            bool sawEncryptedCandidate = false;
            bool sawRetiredCandidate   = false;
            bool decrypted             = false;
            if (sid != 0) {
                for (auto& sp : sessions_) {
                    Session& cand = *sp;
                    if (cand.machine.namesRetiredSealedSession(sid)) sawRetiredCandidate = true;
                    if (cand.machine.sessionId() != sid || !cand.machine.cryptoState().encrypted) continue;
                    sawEncryptedCandidate = true;
                    // Plaintext is always kCryptoOverhead (28) bytes SHORTER than
                    // the wire frame it reconstructs from — `len` is always enough.
                    // noexcept contract: vector::resize can throw (bad_alloc); contained
                    // here the same way AeronTransport::sealScratch contains its own
                    // grow — a failed grow drops the frame instead of terminating.
                    if (egressDecryptScratch_.size() < len) {
                        try {
                            egressDecryptScratch_.resize(len);
                        } catch (...) {
                            return;  // scratch exhausted — drop
                        }
                    }
                    const std::size_t plainLen = codec::openEgressFrame(
                        cand.machine.cryptoState(), buf, len,
                        egressDecryptScratch_.data(), egressDecryptScratch_.size());
                    if (plainLen != 0) {
                        buf = egressDecryptScratch_.data();
                        len = plainLen;
                        decrypted = true;
                        cand.machine.onSealedFrameOpened();   // PR #476 review: the broker proved the keys
                        break;
                    }
                    // Wrong candidate (sessionId collision) or genuine tamper —
                    // try the next candidate before giving up.
                }
            }
            if (!decrypted && sawEncryptedCandidate) {
                return;  // every encrypted candidate failed auth — drop
            }
            // #426: a frame naming a session whose keys were discarded (a new connect episode began while the broker
            // still held the old session and sealed its answer under the old key) is old-episode ciphertext: no live
            // key opens it and it is never decoded as plaintext.  The only clear frame still dispatched is an
            // ErrorResponse answering the new episode's in-flight ConnectRequest.
            if (!decrypted && sawRetiredCandidate &&
                !(peekedTid == codec::TemplateId::ErrorResponse && answersAnInFlightConnect_(buf))) {
                return;
            }
            // PR #476 review (issues #14 / #115): with crypto REQUIRED nothing in clear is ever acted on - whatever
            // session id its header names (0, one nobody holds, a pre-key session's) - except the connect leg itself:
            // the ConnectResponse / HandshakeContinueResponse above, and an ErrorResponse answering a session's
            // in-flight ConnectRequest (a pre-TLS Redirect / NotLeader / refusal, handled as unauthenticated by the
            // session machine).  Any UDP sender can put a frame on this egress; a clear ProduceResponse{OK} would
            // otherwise complete a produce, a clear Redirect move a TLS session.
            if (!decrypted && cfg_.session.offerCrypto && cfg_.session.requireCrypto &&
                !(peekedTid == codec::TemplateId::ErrorResponse && answersAnInFlightConnect_(buf))) [[unlikely]] {
                ++clearFramesDropped_;
                return;
            }
        }
    }

    // Validate frame.
    const auto check = codec::checkFrame(buf, len);
    if (check.status != codec::FrameStatus::Ok) {
        return; // bad/truncated frame — drop silently
    }

    const auto tidOpt = codec::peekTemplate(buf, len);
    if (!tidOpt) {
        return; // should not happen after checkFrame Ok, but be safe
    }
    const auto tid = static_cast<codec::TemplateId>(*tidOpt);

    switch (tid) {
    case codec::TemplateId::ConnectResponse: {
        auto opt = codec::decodeConnectResponse(buf, len);
        if (!opt) break;
        // Task 1.3: per-session demux by the connect correlationId.  All sessions
        // share the one egress subscription (every session declares the same egress
        // channel/streamId), so a ConnectResponse is routed to the session whose
        // in-flight ConnectRequest correlationId matches.  Single-node → the seed.
        Session& s = sessionForConnectCorrId(opt->correlationId);
        if (mnDebug_) {
            std::fprintf(stderr, "[MNDBG conn] ConnectResponse corrId=%lld code=%d assignedSid=%lld -> session ep='%s' (isSeed=%d)\n",
                (long long)opt->correlationId, (int)opt->code, (long long)opt->assignedSessionId,
                s.endpoint.c_str(), (int)(&s == &seedSession()));
        }
        // MAJOR-2: only a ConnectResponse the machine ACCEPTS (it was
        // Connecting with this very correlationId and is Connected after) is a
        // new connect episode; a stale / duplicate / ignored one changes nothing.
        const bool awaited = s.machine.state() == session::SessionState::Connecting &&
                             s.machine.correlationId() == opt->correlationId;
        // A ConnectResponse that came back on the response pair spent it (see Session::pairPinned).
        if (awaited && s.responsePort != 0) s.pairPinned = true;
        s.machine.onConnectResponse(
            opt->code,
            opt->leaderMemberId,
            opt->assignedSessionId,
            opt->correlationId,
            nowNs,
            // client<->broker TLS/AEAD (Task 5): a chosenCipher != 0 sends the
            // machine into Handshaking instead of Connected — see
            // SessionMachine::onConnectResponse.  A pre-v4 broker's decode leaves
            // these at their defaults (0 / empty), so this is a no-op then.
            opt->chosenCipher,
            opt->handshakeBytes,
            // Authentication (v9): the machine logs in (Authenticating) when the broker requires it.
            opt->authMode,
            opt->authState,
            // Issue #116: with the client's own version, decides per-receiver c2b keys.
            opt->brokerVersion);
        s.machine.onActivity(nowNs);
        // Task 5: "accepted" now means reached Connected — true immediately for a
        // plaintext connect, true only once the TLS handshake finishes (possibly
        // on a LATER HandshakeContinueResponse leg, see that case below) for a
        // crypto one.
        const bool nowConnected = s.machine.state() == session::SessionState::Connected;
        lastConnectAccepted_ = awaited && opt->code == 0 && nowConnected;
        lastConnectSession_  = 0;
        for (uint32_t i = 0; i < sessions_.size(); ++i) {
            if (sessions_[i].get() == &s) { lastConnectSession_ = i; break; }
        }
        lastConnectAwaited_ = awaited;
        // brokerVersion is informational (a capability gate) and safe to record
        // as soon as the broker answers OK, even while a crypto handshake is
        // still pending — it doesn't depend on encryption being established.
        if (awaited && opt->code == 0) {
            s.brokerVersion = opt->brokerVersion;
            // NAT-safe egress: the worker bind token arrives on this first leg (also under TLS); an older broker has
            // no tail, so the token stays zero and nothing is bound (acks / Records ride the declared channel).
            s.bindToken    = opt->v16 ? opt->bindToken : std::array<uint8_t, 16>{};
            s.bindRequired = false;
            for (const uint8_t b : s.bindToken) s.bindRequired = s.bindRequired || b != 0;
            // I3: a TLS session's token never rides the wire (the v16 tail is all-zero): it is derived from the session
            // keys once the handshake completes (serviceWorkerBinds_), so a v16 TLS answer still requires a bind.
            if (opt->v16 && opt->chosenCipher != 0) s.bindRequired = true;
        }
        if (lastConnectAccepted_) {
            ++s.connectEpoch;                  // MEDIUM-1: a new connect episode
        }
        // (F8c C3: the observer — Client::onEgressFrame — publishes the new
        // state to app threads (Client::state()) BEFORE it completes the
        // connect, so a thread woken by that completion reads Connected.)
        // Task 1.4 (redirect convergence): a leader session just (re)connected.
        // In multi-node mode, re-drive all in-flight produce frames through their
        // partition routes — held batches whose leader is THIS newly-connected
        // session now send.  Fires only when a session actually reached Connected
        // and per-partition routing is active (no cost single-node).
        if (lastConnectAccepted_ && routeIndex_.size() != 0 && produceResendFn_) {
            produceResendFn_();
        }
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::HandshakeContinueResponse: {
        // client<->broker TLS/AEAD (Task 5): a later leg of a multi-round TLS
        // handshake started by the ConnectResponse case above.  Only meaningful
        // while the session's machine is Handshaking — a stale/duplicate leg
        // after the handshake already finished or failed is a safe no-op (the
        // machine itself ignores it outside Handshaking / on a stale correlationId).
        auto opt = codec::decodeHandshakeContinueResponse(buf, len);
        if (!opt) break;
        Session* sp = &sessionForConnectCorrId(opt->correlationId);
        // Fix round 5 (N4-I1): a FINAL-leg HandshakeFailed arrives after its session went
        // Connected — sessionForConnectCorrId (in-flight connects only) would fall back to the
        // seed.  Resolve it by the (session id, connect corrId) pair — never by id alone.
        if (!(sp->machine.correlationId() == opt->correlationId &&
              (sp->machine.state() == session::SessionState::Connecting ||
               sp->machine.state() == session::SessionState::Handshaking ||
               sp->machine.state() == session::SessionState::Authenticating))) {
            for (auto& cand : sessions_) {
                if ((cand->machine.state() == session::SessionState::Connected ||
                     cand->machine.state() == session::SessionState::Authenticating) &&   // review B I2
                    cand->machine.sessionId() == opt->sessionId &&
                    cand->machine.correlationId() == opt->correlationId) { sp = cand.get(); break; }
            }
        }
        Session& s = *sp;
        const bool awaited = s.machine.state() == session::SessionState::Handshaking &&
                             s.machine.correlationId() == opt->correlationId;
        const bool finalLegFailure =
            s.machine.isFinalLegHandshakeFailure(opt->code, opt->correlationId, opt->sessionId);
        s.machine.onHandshakeContinueResponse(opt->code, opt->handshakeBytes, opt->correlationId, nowNs,
                                              opt->sessionId);
        if (!finalLegFailure) s.machine.onActivity(nowNs);
        if (finalLegFailure) [[unlikely]] {
            if ((handshakeFinalLegFailures_++ & 1023u) == 0)
                std::fprintf(stderr,
                    "rune-client: the broker failed the TLS handshake on its final leg (session %lld) — "
                    "the connect is retried, then fails (HandshakeFailed); %llu so far\n",
                    static_cast<long long>(opt->sessionId),
                    static_cast<unsigned long long>(handshakeFinalLegFailures_));
        }
        // Fix round 4: the observer reads THIS leg's verdict — never one left over
        // from an earlier ConnectResponse (a non-final leg accepts nothing).
        // Fix round 5: a final-leg failure is this session's verdict too (onError once).
        lastConnectAwaited_  = awaited || finalLegFailure;
        lastConnectAccepted_ = awaited && s.machine.state() == session::SessionState::Connected;
        lastConnectSession_  = 0;
        for (uint32_t i = 0; i < sessions_.size(); ++i) {
            if (sessions_[i].get() == &s) { lastConnectSession_ = i; break; }
        }
        if (lastConnectAccepted_) {
            // The handshake just finished on this leg — mirror the ConnectResponse
            // case's "new connect episode" bookkeeping (brokerVersion was already
            // recorded on the first leg, above).
            ++s.connectEpoch;
            if (routeIndex_.size() != 0 && produceResendFn_) {
                produceResendFn_();
            }
        }
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    // ---- Authentication (Task 7): the SCRAM exchange of a session's Authenticating state ----
    case codec::TemplateId::AuthServerFirst: {
        auto opt = ::rune::protocol::client::AuthServerFirst::decode(buf, len);
        if (!opt) break;   // outside the wire bounds: the login then times out (never a proof to a malformed frame)
        Session& s = sessionForConnectCorrId(opt->correlationId);
        s.machine.onAuthServerFirst(*opt, nowNs);
        break;
    }

    case codec::TemplateId::AuthServerFinal: {
        auto opt = ::rune::protocol::client::AuthServerFinal::decode(buf, len);
        if (!opt) break;
        Session& s = sessionForConnectCorrId(opt->correlationId);
        const bool awaited = s.machine.state() == session::SessionState::Authenticating &&
                             s.machine.correlationId() == opt->correlationId;
        s.machine.onAuthServerFinal(*opt, nowNs);
        if (awaited) s.machine.onActivity(nowNs);
        // The same per-leg verdict the ConnectResponse / HandshakeContinueResponse cases publish: "accepted"
        // now means reached Connected — only an AuthServerFinal{OK} with a valid server signature gets there.
        lastConnectAwaited_  = awaited;
        lastConnectAccepted_ = awaited && s.machine.state() == session::SessionState::Connected;
        lastConnectSession_  = 0;
        for (uint32_t i = 0; i < sessions_.size(); ++i) {
            if (sessions_[i].get() == &s) { lastConnectSession_ = i; break; }
        }
        if (lastConnectAccepted_) {
            ++s.connectEpoch;   // a new connect episode (brokerVersion was recorded on the first leg)
            if (routeIndex_.size() != 0 && produceResendFn_) produceResendFn_();
        }
        if (egressObserver_) egressObserver_(tid, buf, len);
        break;
    }

    case codec::TemplateId::ErrorResponse: {
        auto opt = codec::decodeErrorResponse(buf, len);
        if (!opt) break;
        // Task 1.3: route to the session whose in-flight connect corrId matches
        // (a Redirect/NotLeader during a session's connect); otherwise the seed.
        // D4 r2 MEDIUM-1: a session-level answer (SessionExpired / UnknownSession,
        // the broker's sendSessionExpired_: corr 0, header = the dead session's
        // cluster-unique id) that matches no in-flight connect goes to the session
        // owning that id — e.g. the coordinator session — never the healthy seed.
        Session* sp = &sessionForConnectCorrId(opt->correlationId);
        if (sp == &seedSession() && opt->sessionId != 0 &&
            (opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::SessionExpired) ||
             opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::UnknownSession) ||
             opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::SessionRevoked) ||
             opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::NamespaceSuspended)) &&
            !(seedSession().machine.correlationId() == opt->correlationId &&
              (seedSession().machine.state() == session::SessionState::Connecting ||
               seedSession().machine.state() == session::SessionState::Handshaking ||
               seedSession().machine.state() == session::SessionState::Authenticating))) {
            for (std::size_t i = 1; i < sessions_.size(); ++i) {
                if (sessions_[i]->machine.sessionId() == opt->sessionId) { sp = sessions_[i].get(); break; }
            }
        }
        Session& s = *sp;
        // F-A C1: a session-fatal notice for a session this client already LEFT (a forceReconnect / seed failover /
        // resume that got a new id abandoned it; the broker's lease reap now tells it SessionExpired ~10 s later)
        // names an id no machine holds.  It must never tear down the session it was routed to.  Ids are cluster-unique
        // ((node+1)<<40 | n); sessionId 0 = a session-less answer, and the answer to THIS session's own in-flight
        // connect (matched by corrId while Connecting / Handshaking) is kept as before.
        {
            const bool fatal =
                opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::SessionExpired) ||
                opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::UnknownSession) ||
                opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::SessionRevoked) ||
                opt->code == static_cast<int16_t>(::rune::protocol::client::ErrorCode::NamespaceSuspended);
            const int64_t mine = s.machine.sessionId();
            const bool answersMyConnect =
                s.machine.correlationId() == opt->correlationId &&
                (s.machine.state() == session::SessionState::Connecting ||
                 s.machine.state() == session::SessionState::Handshaking ||
                 s.machine.state() == session::SessionState::Authenticating);
            // Fix round 2 (I-2): also while `mine` is 0 (a fresh connect holds no session a notice could name).
            if (fatal && opt->sessionId != 0 && mine != opt->sessionId && !answersMyConnect) [[unlikely]] {
                ++staleSessionNotices_;
                if (egressObserver_) egressObserver_(tid, buf, len);   // the Client's own guard drops it for its ops
                break;                                                  // no machine transition, no activity credit
            }
        }
        if (mnDebug_) {
            std::fprintf(stderr, "[MNDBG err] ErrorResponse corrId=%lld code=%d msg='%.*s' -> session ep='%s' (isSeed=%d)\n",
                (long long)opt->correlationId, (int)opt->code,
                (int)opt->message.size(), opt->message.data(),
                s.endpoint.c_str(), (int)(&s == &seedSession()));
        }
        // D4 (review MAJOR-3): NotCoordinator answers a group / txn COORDINATOR
        // request, never a Connect — its correlationId comes from another
        // namespace (the Client's / the txn producer's counters) and may equal a
        // session's in-flight connect corrId, which would fail that connect.  It
        // concerns only the Client (the coordinator route), never a machine.
        if (opt->code != static_cast<int16_t>(::rune::protocol::client::ErrorCode::NotCoordinator)) {
            s.machine.onErrorResponse(opt->code, opt->message, opt->correlationId, nowNs, opt->sessionId);
        }
        s.machine.onActivity(nowNs);
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::Pong: {
        // A10: a keepalive Pong carries its session's own negative correlationId
        // (-(index + 1)) — credited to exactly that session (broker session ids
        // collide across nodes, so the sessionId cannot route it).  Anything else
        // (an application ping) refreshes the seed, as before.
        {
            const auto pongOpt = codec::decodePong(buf, len);
            const int64_t corr = pongOpt ? pongOpt->correlationId : 0;
            const auto idx = static_cast<std::size_t>(-(corr + 1));
            (corr < 0 && idx < sessions_.size() ? *sessions_[idx] : seedSession()).machine.onActivity(nowNs);
        }
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::MetadataResponse: {
        // MD1 Task 10: route MetadataResponse to the cache-update hook.
        if (metadataResponseFn_) {
            auto opt = codec::decodeMetadataResponse(buf, len);
            if (opt) {
                metadataResponseFn_(*opt);
            }
        }
        // Also forward to egress observer if wired.
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::ProduceResponse: {
        // EOS-OOS: errorCode and baseOffset travel separately (an OK answer to a
        // known-old duplicate carries baseOffset -1; the old -errorCode sentinel
        // read it as error 1).
        if (auto opt = codec::decodeProduceResponse(buf, len)) {
            const int64_t off = opt->errorCode == 0 ? opt->baseOffset : -1;
            if (produceResponseFn_) {
                produceResponseFn_(opt->correlationId, off, opt->errorCode);  // test-harness hook
            } else {
                produceAcks_.push_back(ProduceAck{opt->correlationId, off, opt->errorCode, opt->logStartOffset});
            }
        }
        break;
    }

    default:
        // Forward any non-session frame to the egress observer so that higher
        // layers (Client) can handle subscribe-related frames:
        //   SubscribeResponse (tid 19) — routes to SubscriberHub
        //   Records           (tid 20) — routes to SubscriberHub
        // and any future messages added to the protocol without agent changes.
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }
}

// ---------------------------------------------------------------------------
// Task 1.3: multi-node session registry + partition routing table
// ---------------------------------------------------------------------------

bool SessionEndpointIo::sendFrame(const uint8_t* buf, std::size_t len) {
    return agent->offerControlTo(*owner, buf, len);
}

bool SessionEndpointIo::retarget(const std::string& newIngressChannel) noexcept {
    return retargetWithResponsePort(newIngressChannel, 0);
}

bool SessionEndpointIo::responseCorrelated() const noexcept {
    if (owner == &agent->seedSession()) return agent->transport_.seedResponseCorrelated();   // D4 r2: by identity
    return agent->transport_.publicationResponseCorrelated(owner->ctlPub);
}

bool SessionEndpointIo::retargetWithResponsePort(const std::string& newIngressChannel, int32_t responsePort) noexcept {
    // Follower-connect hang fix: the SEED's connect-time Redirect (its
    // SessionMachine, in Connecting, calls this before resending the Connect)
    // moves the seed's control path — the default publication — to the named
    // endpoint.  This used to be a no-op for every session, the seed included,
    // while Client::onEgressFrame deliberately leaves a connect-time Redirect to
    // the SessionMachine: nobody followed it, every resend went back to the
    // redirecting broker, the redirect budget ran out and the connect hung to
    // its deadline.  D4 r2: the seed by identity.
    if (owner == &agent->seedSession()) return agent->retargetSeedOnConnectRedirect_(newIngressChannel, responsePort);
    // NAT-safe egress: the redirect names ANOTHER node; remember its response port for the session opened to it.
    if (responsePort != 0) {
        agent->setSessionResponsePort(newIngressChannel.rfind("aeron:", 0) == 0
                                          ? newIngressChannel : "aeron:udp?endpoint=" + newIngressChannel,
                                      responsePort);
    }
    // A leader session: multi-node addresses each leader endpoint explicitly
    // (offerTo), so it never retargets the shared default pub; the Client-level
    // redirect handler re-routes via the metadata cache + routing table.  Report
    // success so the SessionMachine resends on the same (correct) endpoint.
    return true;
}

bool ClientAgent::retargetSeedOnConnectRedirect_(const std::string& endpoint, int32_t responsePort) noexcept {
    try {
        // "host:port" or a full Aeron channel (the broker sends the channel).
        const std::string channel = endpoint.rfind("aeron:", 0) == 0 ? endpoint : "aeron:udp?endpoint=" + endpoint;
        if (transport_.ingressChannel() == channel) return true;
        if (responsePort == 0) responsePort = knownResponsePort_(channel);   // Metadata may know it
        if (!transport_.retarget(channel, responsePort)) return false;   // old pub intact; the resend is budget-bounded
        Session& seed            = seedSession();
        seed.currentLeaderChannel = channel;
        seed.currentLeaderNode    = -1;   // the node id is learnt from the metadata answer
        seed.responsePort         = responsePort;
        seed.pairPinned           = false;   // the retarget built a fresh pair
        invalidateRouteSessions_();       // DR client bug 1: the seed moved
        return true;
    } catch (...) {
        return false;   // allocation failure (cold path): keep the old pub
    }
}

void SessionEndpointIo::onRejected() noexcept { agent->noteSessionRejected_(owner); }

std::string SessionEndpointIo::ingressChannel() const {
    if (owner == &agent->seedSession()) return agent->seedChannel();   // D4 r2: the seed by identity
    return endpoint;
}

bool SessionEndpointIo::retargetToBootstrap() noexcept {
    // Only the seed session's endpoint is the transport's default publication,
    // which redirects / stick-to-leader retarget.  A leader session addresses its
    // own endpoint explicitly and is re-routed by metadata: nothing to undo.
    if (owner != &agent->seedSession()) return true;   // D4 r2: the seed by identity (a session may share its bootstrap string)
    return agent->retargetSeedToBootstrap_();
}

bool SessionEndpointIo::retargetToLastKnownLeader() noexcept {
    if (owner != &agent->seedSession()) return true;  // leader session: no-op (D4 r2: by identity)
    return agent->retargetSeedToLastKnownLeader_();
}

bool ClientAgent::retargetSeedToBootstrap_() noexcept {
    const std::string& bootstrap = cfg_.transport.ingressChannel;  // never retargeted
    if (transport_.ingressChannel() == bootstrap) return true;
    // fix2 A-B1: remember where we were stuck BEFORE leaving — the bootstrap may
    // be the dead node, and the next odd Backoff attempt returns here.  Captured
    // from the transport (the real current channel) and copied, since retarget()
    // replaces the transport's channel string.
    const std::string leaving = transport_.ingressChannel();
    const int32_t bootstrapPort = cfg_.transport.gatewayResponseControlPort;
    if (!transport_.retarget(bootstrap, bootstrapPort)) return false;  // old pub intact; next attempt retries
    Session& seed               = seedSession();
    seed.lastKnownLeaderChannel = leaving;
    seed.lastKnownLeaderNode    = seed.currentLeaderNode;
    seed.lastKnownLeaderResponsePort = seed.responsePort;
    seed.responsePort           = bootstrapPort;
    seed.pairPinned             = false;   // fresh pair
    seed.currentLeaderChannel   = bootstrap;
    seed.currentLeaderNode      = -1;  // unknown until the gateway redirects us again
    invalidateRouteSessions_();        // DR client bug 1: the seed moved
    return true;
}

bool ClientAgent::retargetSeedToLastKnownLeader_() noexcept {
    Session& seed = seedSession();
    if (seed.lastKnownLeaderChannel.empty()) return true;  // never stuck to a leader
    // T15a r2-m1: only from the bootstrap.  Anywhere else the seed is on a leader
    // NEWER than lastKnownLeader (attempt 2's retarget failed, or a redirect
    // moved it) — never trade it for the stale one.
    if (transport_.ingressChannel() != cfg_.transport.ingressChannel) return true;
    if (transport_.ingressChannel() == seed.lastKnownLeaderChannel) return true;
    if (!transport_.retarget(seed.lastKnownLeaderChannel, seed.lastKnownLeaderResponsePort)) return false;  // next odd attempt retries
    seed.currentLeaderChannel = seed.lastKnownLeaderChannel;
    seed.currentLeaderNode    = seed.lastKnownLeaderNode;
    seed.responsePort         = seed.lastKnownLeaderResponsePort;
    seed.pairPinned           = false;   // fresh pair
    invalidateRouteSessions_();  // DR client bug 1: the seed moved
    return true;
}

bool ClientAgent::offerControlTo(Session& owner, const uint8_t* buf, std::size_t len) noexcept {
    if (failed_) [[unlikely]] return false;  // T15b: a failed client does no I/O
    // client<->broker TLS/AEAD (Task 6b): seal under the SENDING session's own
    // CryptoState (D4 r2: by identity — never a lookup by endpoint string, which
    // a seed moved off its bootstrap shares with a session to that bootstrap).
    // Covers every session-machine-originated control frame (ConnectRequest,
    // HandshakeContinue, Keepalive/Ping, Disconnect) on EVERY session.
    // ConnectRequest/HandshakeContinue are naturally plaintext here (sent
    // while encrypted==false) and are defensively exempt by templateId too
    // (see sealForDestination_).  Plaintext path (crypto off) is byte-
    // identical to before Task 6b.
    if (!sealForDestination_(owner.machine, buf, len)) {
        return false;  // seal failed — DROP, do not transmit
    }
    // The seed → the default publication (wherever it points: byte-identical
    // single-node fast path).  Any other session → its own endpoint over the
    // shared transport (default ingress stream id).
    // I2/reconnect: a new connect episode on a spent response pair first builds a fresh pair (the offer below then
    // waits for it: the machine's connect retry re-sends).
    if (owner.pairPinned && len >= 4 &&
        rune::protocol::client::detail::getLE16(buf, 2) == static_cast<uint16_t>(codec::TemplateId::Connect)) [[unlikely]] {
        rePairSessionResponse_(owner);
    }
    if (&owner == &seedSession()) return transport_.offer(buf, len);
    // NAT-safe egress: a session opened with a known response port rides its response-correlated publication.
    if (owner.ctlPub != transport::AeronTransport::kNoPub) return transport_.offerOn(owner.ctlPub, buf, len);
    return transport_.offerTo(owner.endpoint, cfg_.transport.ingressStreamId, buf, len);
}

void ClientAgent::rePairSessionResponse_(Session& s) noexcept {
    // The pair is spent only while a response port is in force; a failed re-pair keeps the flag (the next Connect send
    // tries again) and the old pair.
    if (s.responsePort == 0) { s.pairPinned = false; return; }
    const bool ok = (&s == &seedSession()) ? transport_.rePairSeedResponse(s.responsePort)
                                           : transport_.rePairResponsePublication(s.ctlPub);
    if (ok) s.pairPinned = false;
}

void ClientAgent::attachResponsePub_(Session& s, int32_t port, bool upgradePlain) noexcept {
    s.pairPinned = false;   // a freshly built pair is unused
    // The ONE place a leader session's control publication is built (openSession_, and the upgrade of a session that
    // was opened plain before Metadata / a Redirect taught the port).  port 0 = the declared path.
    s.responsePort = port;
    if (port == 0) return;
    try {
        const auto respCh = transport::responseControlChannelFor(s.endpoint, port);
        if (respCh) {
            s.ctlPub = upgradePlain
                ? transport_.upgradeToResponsePublication(s.endpoint, cfg_.transport.ingressStreamId, *respCh)
                : transport_.responsePublicationHandle(s.endpoint, cfg_.transport.ingressStreamId, *respCh);
        } else {
            s.responsePort = 0;   // not a UDP endpoint: nothing to pair
        }
    } catch (...) {
        s.ctlPub = transport::AeronTransport::kNoPub;   // allocation failure (cold): the declared path
    }
}

Session& ClientAgent::openSession_(const std::string& endpoint) noexcept {
    // Lazily open a NEW session to this endpoint over the SHARED transport.  Its
    // SessionMachine gets its own IO adapter (offerTo(endpoint,...)) → its own
    // ConnectRequest → its own sessionId on the one driver.  ASYNC: submit connect
    // now; the session transitions Disconnected→Connecting.  Frames for its
    // partitions park in the producer builders until its ConnectResponse{OK}.
    // Give this session a DISJOINT correlationId range so its ConnectResponse
    // demuxes unambiguously off the ONE shared egress subscription (per-session
    // demux by connect corrId — no image identity needed).
    session::SessionConfig scfg = cfg_.session;
    scfg.correlationIdBase =
        static_cast<int64_t>(sessions_.size()) * kSessionCorrIdStride;
    scfg.keepaliveCorrelationId = -(static_cast<int64_t>(sessions_.size()) + 1);   // A10: Pong routing
    scfg.revivedOnDemand = true;   // Issue #481: paced by Backoff, never a revive per duty cycle
    sessions_.emplace_back(std::make_unique<Session>(this, std::move(scfg), endpoint));
    Session& s = *sessions_.back();
    s.machine.useScramCache(&scramCache_);   // authentication: PBKDF2 once per verifier, not once per session
    s.machine.useSharedSecret(&secret_);
    // NAT-safe egress: with a known response port the control publication is response-correlated (response
    // subscription first, in AeronTransport::offerOn).  A session opened before Metadata taught the port stays plain.
    attachResponsePub_(s, knownResponsePort_(endpoint));
    // On the duty clock (the clock tick() runs on), never steady_clock: with an
    // injected nanoClock the two are different domains and the connect's
    // fast-retry / timeout would never (or only much later) come due.
    s.machine.connect(dutyNowNs_);
    s.connectSubmitted = true;
    if (mnDebug_) {
        std::fprintf(stderr, "[MNDBG new-session] ep='%s' corrIdBase=%lld nSessions=%zu connectCorrId=%lld\n",
            endpoint.c_str(), (long long)(static_cast<int64_t>(sessions_.size()-1) * kSessionCorrIdStride),
            sessions_.size(), (long long)s.machine.correlationId());
    }
    return s;
}

uint32_t ClientAgent::resolveSessionIndex_(const std::string& endpoint) const noexcept {
    // THE one routing order (sessionForEndpoint opens on kNoSession; findSessionIndex_ reports it): the session
    // that carries a frame for `endpoint` and the session a WorkerBind rides must be the same, so they share this.
    // Empty endpoint → the seed.  The seed still on its bootstrap (it never moved): its endpoint → the seed
    // (single-node hot path, no scan).
    if (endpoint.empty()) return 0;
    const std::string& physical = transport_.ingressChannel();
    const bool seedHome = physical == cfg_.transport.ingressChannel;
    if (seedHome && sameEndpoint(endpoint, sessions_[0]->endpoint)) return 0;
    for (std::size_t i = 1; i < sessions_.size(); ++i) {
        if (sameEndpoint(sessions_[i]->endpoint, endpoint)) return static_cast<uint32_t>(i);  // M6: one session per node
    }
    // DR client bug 1: the seed is the session of the node it PHYSICALLY sits on
    // (its default publication) — never of the bootstrap node it left (a failover
    // / redirect moved it): a route to that node gets that node's own session,
    // so no frame stamped with the seed's new session id reaches the old node.
    if (sameEndpoint(endpoint, physical)) return 0;
    return kNoSession;
}

Session& ClientAgent::sessionForEndpoint(const std::string& endpoint) noexcept {
    const uint32_t idx = resolveSessionIndex_(endpoint);
    if (idx != kNoSession) return *sessions_[idx];
    return openSession_(endpoint);
}

Session& ClientAgent::sessionForConnectCorrId(int64_t corrId) noexcept {
    for (auto& s : sessions_) {
        // A session awaiting its ConnectResponse holds the matching correlationId
        // on its SessionMachine while Connecting.  Match only non-seed or genuinely
        // in-flight sessions; the seed is the fallback below.
        // client<->broker TLS/AEAD (Task 5): Handshaking is ALSO an in-flight
        // connect episode (the SAME correlationId as the initiating
        // ConnectRequest, carried across every HandshakeContinue/
        // HandshakeContinueResponse leg) — without this, a multi-node client's
        // HandshakeContinueResponse for a NON-seed session would always fall
        // through to the seed below.
        // Authentication: Authenticating is the same connect episode too (the auth frames carry its corrId).
        if ((s->machine.state() == session::SessionState::Connecting ||
             s->machine.state() == session::SessionState::Handshaking ||
             s->machine.state() == session::SessionState::Authenticating) &&
            s->machine.correlationId() == corrId) {
            return *s;
        }
    }
    return seedSession();
}

uint32_t ClientAgent::sessionIndexFor_(const std::string& endpoint) noexcept {
    Session& s = sessionForEndpoint(endpoint);
    for (uint32_t i = 0; i < sessions_.size(); ++i) {
        if (sessions_[i].get() == &s) return i;
    }
    return 0;  // unreachable: sessionForEndpoint always returns a member
}

namespace {
// "aeron:udp?endpoint=host:port|..." -> "host:port"; a bare "host:port" as is;
// any other channel (aeron:ipc, no endpoint param) as is.
std::string_view endpointKey(std::string_view ch) noexcept {
    if (ch.rfind("aeron:", 0) != 0) return ch;
    const auto q = ch.find('?');
    if (q == std::string_view::npos) return ch;
    std::size_t pos = q + 1;
    while (pos < ch.size()) {
        std::size_t end = ch.find('|', pos);
        if (end == std::string_view::npos) end = ch.size();
        const std::string_view kv = ch.substr(pos, end - pos);
        if (kv.rfind("endpoint=", 0) == 0) return kv.substr(9);
        pos = end + 1;
    }
    return ch;
}
} // namespace

bool ClientAgent::sameEndpoint(std::string_view a, std::string_view b) noexcept {
    if (a == b) return true;
    if (a.empty() || b.empty()) return false;
    // Only UDP endpoints (an endpoint= value or a bare host:port) are compared by
    // host:port; any other channel (aeron:ipc, aliases) is its own destination.
    const std::string_view ka = endpointKey(a);
    const std::string_view kb = endpointKey(b);
    if (ka.rfind("aeron:", 0) == 0 || kb.rfind("aeron:", 0) == 0) return false;
    return ka == kb;
}

void ClientAgent::releaseRoutePub_(transport::AeronTransport::PubHandle h) noexcept {
    if (h == transport::AeronTransport::kNoPub) return;
    for (const PartitionRoute& r : routes_) {
        if (r.pub == h) return;  // still referenced by another route
    }
    for (const WorkerBindEntry& b : workerBinds_) {
        if (b.pub == h) return;  // a worker's bind entry owns its publication for the client's lifetime
    }
    // Never a session's own control publication or the default channel's: only a
    // route-only (worker data) publication is closed.
    const std::string* ch = transport_.publicationChannel(h);
    if (ch == nullptr || *ch == cfg_.transport.ingressChannel) return;
    for (const auto& s : sessions_) {
        if (s->endpoint == *ch) return;
    }
    transport_.releasePublication(h);
}

bool ClientAgent::setRoute(int32_t topicId, int32_t partition, const std::string& sessionEndpoint,
                           const std::string& dataChannel, uint8_t receiverId, int32_t streamId,
                           int32_t responsePort) noexcept {
    const int64_t  key  = routeKey(topicId, partition);
    const uint32_t idx  = routeIndex_.find(key);
    // Fix round 1 (C1): where the seed's default publication PHYSICALLY points —
    // never its bootstrap identity.  Invariant: a partition has no route entry iff
    // its leader is the broker every no-route offer reaches (offerProduceToLeader_'s
    // default branch, leaderSessionIndex -> 0, hasRoute false -> the seed).
    const std::string& seed = transport_.ingressChannel();
    const bool isDefault = (sessionEndpoint.empty() || sameEndpoint(sessionEndpoint, seed)) &&
                           (dataChannel.empty() || sameEndpoint(dataChannel, seed));
    if (isDefault) {
        if (idx == detail::FlatIndexMap::kNone) return false;
        const auto oldPub = routes_[idx].pub;
        routeIndex_.erase(key);
        routes_[idx] = PartitionRoute{};
        freeRoutes_.push_back(idx);
        releaseRoutePub_(oldPub);  // M4
        return true;
    }
    try {
        const std::string& data = dataChannel.empty() ? sessionEndpoint : dataChannel;
        // Issue #116: the route's receiver (cold: once per metadata change; the offer reads two plain fields).
        // NAT-safe egress: a worker route (data channel is not the node itself) with a response-control port gets a
        // bind entry — the publication every produce to that worker rides, and the one WorkerBind goes out on.
        const bool workerRoute = !sameEndpoint(data, sessionEndpoint);
        const int32_t bindStream = streamId != 0 ? streamId : cfg_.transport.ingressStreamId;
        const auto bindFor = [&]() -> uint32_t {
            return (workerRoute && responsePort != 0)
                       ? workerBindFor_(sessionEndpoint, data, bindStream, responsePort) : kNoBind;
        };
        const auto fresh = [&] {
            PartitionRoute nr{sessionEndpoint, data};
            nr.receiverId = receiverId;
            nr.dataOnNode = sameEndpoint(data, sessionEndpoint);
            nr.bindIdx    = bindFor();
            nr.bindStream = bindStream;
            nr.bindPort   = responsePort;
            if (nr.bindIdx != kNoBind) {   // the entry's publication IS the route's (never a separate plain one)
                nr.pub      = workerBinds_[nr.bindIdx].pub;
                nr.streamId = streamId;
            }
            return nr;
        };
        if (idx != detail::FlatIndexMap::kNone) {
            PartitionRoute& r = routes_[idx];
            if (r.sessionEndpoint == sessionEndpoint && r.dataChannel == data) {
                const bool bindChanged = r.bindStream != bindStream || r.bindPort != responsePort;
                if (r.receiverId == receiverId && !bindChanged) return false;
                r.receiverId = receiverId;   // same endpoint, another receiver: keep the publication
                if (bindChanged) {
                    // The worker's response port / stream moved (restart, rebalance): re-resolve the entry.
                    r.bindIdx = bindFor();
                    r.bindStream = bindStream;
                    r.bindPort = responsePort;
                    if (r.bindIdx != kNoBind) {
                        r.pub = workerBinds_[r.bindIdx].pub;
                        r.streamId = streamId;
                    }
                }
                return true;
            }
            const auto oldPub = r.pub;
            r = fresh();
            releaseRoutePub_(oldPub);  // M4: the old worker publication, once unreferenced
            return true;
        }
        uint32_t slot;
        if (!freeRoutes_.empty()) {
            slot = freeRoutes_.back();
            freeRoutes_.pop_back();
            routes_[slot] = fresh();
        } else {
            slot = static_cast<uint32_t>(routes_.size());
            routes_.push_back(fresh());
        }
        routeIndex_.insert(key, slot);
        return true;
    } catch (...) {
        return false;
    }
}

uint32_t ClientAgent::leaderSessionIndex(int32_t topicId, int32_t partition) noexcept {
    if (routeIndex_.size() == 0) return 0;
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    if (idx == detail::FlatIndexMap::kNone) return 0;
    PartitionRoute& r = routes_[idx];
    // LOW-6: may emplace_back into sessions_ (a lazily opened node session) —
    // also from inside an egress callback.  Safe: sessions are held by
    // unique_ptr (stable addresses) and no range-for over sessions_ is live on
    // any path that reaches here (doWork's tick loop never calls into the hubs).
    if (r.sessionIndex == kNoSession) r.sessionIndex = sessionIndexFor_(r.sessionEndpoint);  // once per route
    return r.sessionIndex;
}

int16_t ClientAgent::rejectedRouteCode(int32_t topicId, int32_t partition) const noexcept {
    if (routeIndex_.size() == 0) return 0;
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    if (idx == detail::FlatIndexMap::kNone) return 0;
    const PartitionRoute& r = routes_[idx];
    uint32_t si = r.sessionIndex;
    if (si == kNoSession) {   // not resolved yet: look the endpoint up WITHOUT opening a session
        for (std::size_t i = 1; i < sessions_.size(); ++i) {
            if (sameEndpoint(sessions_[i]->endpoint, r.sessionEndpoint)) { si = static_cast<uint32_t>(i); break; }
        }
    }
    if (si == kNoSession || si == 0 || si >= sessions_.size()) return 0;
    const session::SessionMachine& m = sessions_[si]->machine;
    return m.state() == session::SessionState::Rejected ? m.rejectCode() : int16_t{0};
}

int64_t ClientAgent::connectedSessionId(uint32_t index) const noexcept {
    if (index >= sessions_.size()) return 0;
    const session::SessionMachine& m = sessions_[index]->machine;
    return m.state() == session::SessionState::Connected ? m.sessionId() : 0;
}

transport::AeronTransport::OfferResult ClientAgent::offerOnSession(uint32_t index, const uint8_t* buf,
                                                                  std::size_t len) noexcept {
    using R = transport::AeronTransport::OfferResult;
    if (index == 0) return offerDefault(buf, len);  // the seed: offerDefault->offerIngress_ already seals
    if (failed_ || index >= sessions_.size()) [[unlikely]] return R::Fatal;
    // client<->broker TLS/AEAD (Task 6b): seal under session `index`'s OWN
    // CryptoState — its sessionId is already final in `buf` (every caller,
    // e.g. GroupConsumerHub's fetch routing, stamps it from
    // router_.sessionId(target) before encoding, same as offerControlTo's
    // callers).  Plaintext path (crypto off) is byte-identical to before.
    if (!sealForDestination_(sessions_[index]->machine, buf, len)) {
        ++ingressOfferRefused_;
        return R::Retry;  // seal failed — DROP, do not transmit
    }
    Session& target = *sessions_[index];
    const bool offered = target.ctlPub != transport::AeronTransport::kNoPub
        ? transport_.offerOn(target.ctlPub, buf, len)   // NAT-safe egress: the response-correlated publication
        : transport_.offerTo(target.endpoint, cfg_.transport.ingressStreamId, buf, len);
    if (offered) return R::Accepted;
    ++ingressOfferRefused_;
    return R::Retry;
}

uint32_t ClientAgent::coordinatorSessionIndex(const std::string& channel, int64_t nowNs) noexcept {
    if (channel.empty() || failed_) return 0;
    // D4 r2 MAJOR-1: the seed is the coordinator's session only where it
    // PHYSICALLY is (its default publication), not by its bootstrap identity.
    if (sameEndpoint(channel, transport_.ingressChannel())) return 0;
    uint32_t idx = 0;
    for (uint32_t i = 1; i < sessions_.size(); ++i) {   // that node's own session (M6), never the seed
        if (sameEndpoint(sessions_[i]->endpoint, channel)) { idx = i; break; }
    }
    if (idx == 0) {
        // None yet.  The seed's BOOTSTRAP node while the seed sits elsewhere
        // (moved by a data-op redirect) gets a session of its own: the seed stays
        // where its data ops are; sessionForEndpoint (identity lookup) would
        // return the seed, so open it directly.
        (void)openSession_(channel);
        idx = static_cast<uint32_t>(sessions_.size() - 1);
    }
    (void)reviveSession(idx, nowNs);   // a gave-up one connects again (no-op otherwise)
    return idx;
}

transport::AeronTransport::OfferResult ClientAgent::offerToCoordinator(uint32_t index, const uint8_t* buf,
                                                                      std::size_t len) noexcept {
    using R = transport::AeronTransport::OfferResult;
    if (failed_ || index >= sessions_.size()) [[unlikely]] return R::Fatal;
    session::SessionMachine& m = sessions_[index]->machine;
    // Not Connected: held — the owner re-sends on this session's connect (the
    // group hub re-joins; the txn producer re-sends on the session change).
    if (m.state() != session::SessionState::Connected || m.sessionId() == 0) return R::Retry;
    if (len >= ::rune::protocol::client::MessageHeader::ENCODED_SIZE) {
        ::rune::protocol::client::detail::putLE64(const_cast<uint8_t*>(buf), 8, m.sessionId());
        // Issue #114: an own-credential change is proven for the session it rides, re-signed on every offer.
        const auto tid = ::rune::protocol::client::detail::getLE16(buf, 2);
        if (tid == static_cast<uint16_t>(::rune::protocol::client::TemplateId::SecurityDdl))
            m.signOwnCredentialChange(const_cast<uint8_t*>(buf), len);
    }
    return offerOnSession(index, buf, len);   // seals under THIS session's crypto state
}

transport::AeronTransport::OfferResult ClientAgent::offerToEndpoint(const std::string& channel, uint8_t* buf,
                                                                   std::size_t len, int64_t nowNs,
                                                                   uint32_t& sessionIndex) noexcept {
    // The node's one session (0 = the seed: "" or where the seed physically is).
    sessionIndex = coordinatorSessionIndex(channel, nowNs);
    return offerToCoordinator(sessionIndex, buf, len);   // stamps that session's id; Retry while not Connected
}

bool ClientAgent::reviveSession(uint32_t index, int64_t nowNs) noexcept {
    if (failed_ || index == 0 || index >= sessions_.size()) return false;
    session::SessionMachine& m = sessions_[index]->machine;
    if (m.state() != session::SessionState::Disconnected) return false;
    m.connect(nowNs);
    return true;
}

std::string ClientAgent::routeEndpointFor(int32_t topicId, int32_t partition) const noexcept {
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    return idx == detail::FlatIndexMap::kNone ? std::string{} : routes_[idx].sessionEndpoint;
}

std::string ClientAgent::dataChannelFor(int32_t topicId, int32_t partition) const noexcept {
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    return idx == detail::FlatIndexMap::kNone ? std::string{} : routes_[idx].dataChannel;
}

namespace {
/// client<->broker TLS/AEAD (Task 6 / 6b): seal `buf`/`len` under `crypto`
/// into `transport`'s reusable seal scratch (AeronTransport::sealScratch —
/// single-writer, grows-only, no per-call heap allocation once warmed up).
/// Shared by EVERY non-handshake client->broker send site (offerProduceToLeader_,
/// offerControlTo, offerIngress_, offerOnSession) — all run on the single
/// agent/duty thread and each consumes the scratch synchronously (seal, then
/// immediately offer) before the next one runs, so one scratch buffer is safe
/// (no re-entrancy).  Returns {nullptr/scratch, 0} on failure (scratch
/// exhausted, or AeadCipher::seal's own 0 = "cipher error, do not transmit"
/// contract) — callers must treat a 0 length as DROP, never transmit.
/// Callers check `crypto.encrypted` first; this only runs the seal+scratch
/// mechanics for the encrypted path.
struct SealedFrame { uint8_t* buf; std::size_t len; };
SealedFrame sealForOffer(transport::AeronTransport& transport, session::CryptoState& crypto,
                         const uint8_t* buf, std::size_t len, uint8_t receiverId = 0) noexcept {
    const std::size_t need = len + rune::protocol::client::kCryptoOverhead;
    uint8_t* scratch = transport.sealScratch(need);
    if (scratch == nullptr) return {nullptr, 0};
    // Issue #116: receiverId = the endpoint this frame is offered to (0 = the gateway; every non-produce frame).
    return {scratch, codec::sealFrame(crypto, buf, len, scratch, need, receiverId)};
}
} // namespace

/// client<->broker TLS/AEAD (Task 6b): the single seal-or-passthrough decision
/// shared by every NON-Produce client->broker send site (offerControlTo,
/// offerIngress_, offerOnSession — offerProduceToLeader_ already seals
/// directly via sealForOffer and is NOT routed through here, to avoid a
/// double seal).  Plaintext path (`!crypto.encrypted`) leaves buf/len
/// untouched — byte-identical to pre-Task-6b behaviour.  When encrypted,
/// defensively exempts Connect(1)/HandshakeContinue(10) by templateId (belt-
/// and-suspenders: both are only ever sent while this session's
/// crypto.encrypted is still false, since the handshake itself is what flips
/// it to true — see finishHandshake_ in SessionMachine.h), then seals via the
/// shared sealForOffer/sealScratch mechanics.  Returns false iff the frame
/// must be DROPPED (seal failure) — callers must not transmit in that case.
bool ClientAgent::sealForDestination_(session::SessionMachine& m,
                                      const uint8_t*& buf, std::size_t& len) noexcept {
    session::CryptoState& crypto = m.cryptoState();
    const bool handshakeFrame =
        len >= rune::protocol::client::MessageHeader::ENCODED_SIZE &&
        [&] {
            const auto tid = static_cast<codec::TemplateId>(
                rune::protocol::client::detail::getLE16(buf, 2));
            return tid == codec::TemplateId::Connect || tid == codec::TemplateId::HandshakeContinue;
        }();
    if (handshakeFrame) return true;  // handshake frame: never sealed
    // Fix round 4 (TLS handshake class): under offerCrypto the session's mode is
    // negotiated by its handshake — until it is Connected no frame may leave it
    // (plaintext under require_tls is refused by the broker; under offered it
    // would bypass the mode the handshake is about to choose).  The caller treats
    // this like a refused offer: its own Connected-edge resend recovers it.
    // Authentication: the login's own frames (AuthClientFirst / AuthClientFinal) leave in Authenticating —
    // sealed by the keys the handshake just derived (crypto.encrypted) — and nothing else does.
    if (m.offersCrypto() && m.state() != session::SessionState::Connected &&
        m.state() != session::SessionState::Authenticating) [[unlikely]] return false;
    // Review B M1 (defence in depth): in Authenticating only the login's own frames (and a goodbye) may leave.
    if (m.state() == session::SessionState::Authenticating && len >= rune::protocol::client::MessageHeader::ENCODED_SIZE) [[unlikely]] {
        const auto tid = static_cast<codec::TemplateId>(rune::protocol::client::detail::getLE16(buf, 2));
        if (tid != codec::TemplateId::AuthClientFirst && tid != codec::TemplateId::AuthClientFinal &&
            tid != codec::TemplateId::Disconnect) return false;
    }
    if (!crypto.encrypted) return true;  // plaintext session: nothing to do
    const SealedFrame sealed = sealForOffer(transport_, crypto, buf, len);
    if (sealed.len == 0) return false;  // seal failed — DROP, do not transmit
    buf = sealed.buf;
    len = sealed.len;
    return true;
}

// ---------------------------------------------------------------------------
// NAT-safe egress (spec D2/D6a/D7): response ports, worker bind table, WorkerBind duty
// ---------------------------------------------------------------------------

int32_t ClientAgent::knownResponsePort_(const std::string& endpoint) const noexcept {
    for (const auto& [ep, port] : nodeResponsePorts_) {
        if (sameEndpoint(ep, endpoint)) return port;
    }
    // The bootstrap gateway's port is configuration, not Metadata.
    if (sameEndpoint(endpoint, cfg_.transport.ingressChannel)) return cfg_.transport.gatewayResponseControlPort;
    return 0;
}

void ClientAgent::setSessionResponsePort(const std::string& endpoint, int32_t port) noexcept {
    if (endpoint.empty() || port < 0) return;
    try {
        bool found = false;
        for (auto& [ep, known] : nodeResponsePorts_) {
            if (sameEndpoint(ep, endpoint)) { known = port; found = true; break; }
        }
        if (!found) nodeResponsePorts_.emplace_back(endpoint, port);
    } catch (...) {
        return;   // allocation failure (cold): the node stays on the declared path
    }
    // I6: a leader session opened PLAIN before the port was learnt is upgraded (next duty pass: never re-entrantly
    // from inside a machine call that may be this very session's redirect handler).
    if (port == 0) return;
    for (std::size_t i = 1; i < sessions_.size(); ++i) {
        Session& s = *sessions_[i];
        if (s.responsePort == 0 && s.ctlPub == transport::AeronTransport::kNoPub && !s.upgradeToResponse &&
            sameEndpoint(s.endpoint, endpoint)) {
            s.upgradeToResponse = true;
            ++pendingResponseUpgrades_;
        }
    }
}

int ClientAgent::serviceResponseUpgrades_() noexcept {
    int done = 0;
    for (std::size_t i = 1; i < sessions_.size(); ++i) {
        Session& s = *sessions_[i];
        if (!s.upgradeToResponse) continue;
        s.upgradeToResponse = false;
        --pendingResponseUpgrades_;
        if (s.responsePort != 0 || s.ctlPub != transport::AeronTransport::kNoPub) continue;   // upgraded meanwhile
        attachResponsePub_(s, knownResponsePort_(s.endpoint), /*upgradePlain=*/true);
        if (s.responsePort == 0) continue;
        // The response path needs a fresh connect: the gateway announces the response publication (and the
        // bind token) on the ConnectResponse of a response-correlated request.
        s.machine.forceReconnect(dutyNowNs_);
        s.connectSubmitted = true;
        ++done;
    }
    return done;
}

uint32_t ClientAgent::findSessionIndex_(const std::string& endpoint) const noexcept {
    // sessionForEndpoint() without opening a session: the SAME resolution order (C1).
    return resolveSessionIndex_(endpoint);
}

uint32_t ClientAgent::workerBindFor_(const std::string& sessionEndpoint, const std::string& dataChannel,
                                     int32_t streamId, int32_t responsePort) noexcept {
    try {
        for (std::size_t i = 0; i < workerBinds_.size(); ++i) {
            WorkerBindEntry& b = workerBinds_[i];
            if (b.streamId == streamId && b.dataChannel == dataChannel) return static_cast<uint32_t>(i);
        }
        const auto respCh = transport::responseControlChannelFor(dataChannel, responsePort);
        if (!respCh) return kNoBind;   // not a UDP worker endpoint: nothing to pair
        WorkerBindEntry e;
        e.sessionEndpoint = sessionEndpoint;
        e.dataChannel     = dataChannel;
        e.streamId        = streamId;
        e.pub             = transport_.responsePublicationHandle(dataChannel, streamId, *respCh);
        if (e.pub == transport::AeronTransport::kNoPub) return kNoBind;
        workerBinds_.push_back(std::move(e));
        return static_cast<uint32_t>(workerBinds_.size() - 1);
    } catch (...) {
        return kNoBind;
    }
}

int ClientAgent::serviceWorkerBinds_() noexcept {
    int sent = 0;
    if (holdWorkerBinds_) [[unlikely]] return 0;
    for (WorkerBindEntry& e : workerBinds_) {
        if (e.sessionIndex == kNoSession) {
            e.sessionIndex = findSessionIndex_(e.sessionEndpoint);
            if (e.sessionIndex == kNoSession) continue;   // the route's first offer opens it
        }
        if (e.sessionIndex >= sessions_.size()) { e.sessionIndex = kNoSession; continue; }
        Session& s = *sessions_[e.sessionIndex];
        if (!s.bindRequired || s.machine.state() != session::SessionState::Connected) continue;
        const int64_t sid = s.machine.sessionId();
        if (sid == 0) continue;
        if (const auto* bound = std::get_if<WorkerBindEntry::Bound>(&e.state); bound != nullptr && bound->sid == sid) continue;
        // I3: the TLS-derived token (same derivation as the gateway's), computed once per connect episode (the
        // ConnectResponse resets the token); never a WorkerBind with a zero token.
        if (s.machine.cryptoState().encrypted && std::all_of(s.bindToken.begin(), s.bindToken.end(), [](uint8_t b) { return b == 0; })) {
            const auto& c2b = s.machine.cryptoState().c2bKey;
            const auto tok = rune::crypto::deriveWorkerBindToken(std::span<const uint8_t>(c2b.bytes, sizeof c2b.bytes), sid);
            s.bindToken = tok;
        }
        {
            bool nonZero = false;
            for (const uint8_t b : s.bindToken) nonZero = nonZero || b != 0;
            if (!nonZero) continue;
        }
        // I2: an unacknowledged bind is resent at kBindRetryNs steps; after kBindMaxAttempts the session reconnects
        // (a fresh connect issues a fresh token and announce, the bind starts over).
        auto* pending = std::get_if<WorkerBindEntry::Sent>(&e.state);
        if (pending != nullptr && pending->sid == sid) {
            if (dutyNowNs_ < pending->nextRetryNs) continue;
            if (pending->attempts >= kBindMaxAttempts) {
                if ((workerBindFailures_++ & 1023u) == 0)
                    std::fprintf(stderr, "rune-client: WorkerBind for session %lld unacknowledged after %u attempts "
                                 "(%llu failures so far); reconnecting\n", static_cast<long long>(sid),
                                 static_cast<unsigned>(pending->attempts), static_cast<unsigned long long>(workerBindFailures_));
                e.state = WorkerBindEntry::Unbound{};
                s.machine.forceReconnect(dutyNowNs_);
                s.connectSubmitted = true;
                ++sent;
                continue;
            }
        }
        // The first bind of a NEW session on a spent pair: the old session's worker response publication pinned the
        // response subscription, so this session's bind rides a fresh pair (new request publication = new image).  The
        // offer below fails until the pair is Ready; the next pass sends.
        if ((pending == nullptr || pending->sid != sid) && e.pairUsed) {
            if (transport_.rePairResponsePublication(e.pub)) { e.pairUsed = false; e.state = WorkerBindEntry::Unbound{}; }
            else continue;   // could not re-pair now (cold failure): try again next pass
            pending = nullptr;
        }
        // The frame is PLAINTEXT, never sealed (the first frame on a worker publication for this session).
        uint8_t frame[rune::protocol::client::WorkerBind::ENCODED_SIZE];
        const int64_t corr = bindCorrId_ + 1;
        const std::size_t n = codec::encodeWorkerBind(frame, sizeof frame, sid, corr, s.bindToken);
        if (n == 0) continue;
        if (transport_.offerOn(e.pub, frame, n)) {
            bindCorrId_ = corr;
            e.pairUsed = true;
            const uint8_t attempts = (pending != nullptr && pending->sid == sid) ? static_cast<uint8_t>(pending->attempts + 1) : uint8_t{1};
            e.state = WorkerBindEntry::Sent{sid, corr, attempts, dutyNowNs_ + kBindRetryNs};
            ++workerBindsSent_;
            ++sent;
        }
    }
    return sent;
}

void ClientAgent::onWorkerBindAck_(const uint8_t* buf, std::size_t len) noexcept {
    const auto ack = codec::decodeWorkerBindAck(buf, len);
    if (ack) {
        for (WorkerBindEntry& e : workerBinds_) {
            const auto* sent = std::get_if<WorkerBindEntry::Sent>(&e.state);
            if (sent != nullptr && sent->sid == ack->sessionId && sent->corr == ack->correlationId) {
                e.state = WorkerBindEntry::Bound{ack->sessionId};
                ++workerBindAcksAccepted_;
                return;
            }
        }
    }
    ++workerBindAcksIgnored_;   // malformed, late (already Bound), stale sid / corr, or for no entry
}

bool ClientAgent::offerProduceToLeader_(int32_t topicId, int32_t partition,
                                        int32_t ingressStreamId,
                                        const uint8_t* buf, std::size_t len,
                                        transport::FrameCrc crc) noexcept {
    // Single-node / no routes: one size check, then the default offer.
    if (routeIndex_.size() != 0) {
        const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
        if (idx != detail::FlatIndexMap::kNone) {
            PartitionRoute& r = routes_[idx];
            if (r.sessionIndex == kNoSession) r.sessionIndex = sessionIndexFor_(r.sessionEndpoint);  // once
            Session& leader = *sessions_[r.sessionIndex];
            const int64_t sid = leader.machine.sessionId();
            // Leader session still connecting: hold the batch (retried next cycle;
            // produceResendFn_ re-drives it on connect).
            if (sid == 0 || leader.machine.state() != session::SessionState::Connected) {
                // Issue #60 invariant: a Rejected leader never carries a batch.  The sweep (watchProduceRoutes_)
                // runs once per Rejected edge; a batch routed here AFTER it raises this flag so the owner sweeps
                // once more and fails it - never re-swept per cycle.
                if (leader.machine.state() == session::SessionState::Rejected) [[unlikely]] rejectedRouteHit_ = true;
                ++heldOffers_;  // DR-7: the owner asks for metadata (the leader may have moved)
                return false;
            }
            if (r.bindIdx != kNoBind) {
                // NAT-safe egress: the worker drops produce from an image it has not bound this session to.  Hold
                // exactly like the not-Connected branch until the WorkerBind for THIS session id went out (a session
                // without a token is not response-mode: nothing to bind).  One branch + one compare.
                const WorkerBindEntry& wb = workerBinds_[r.bindIdx];
                const auto* bound = std::get_if<WorkerBindEntry::Bound>(&wb.state);
                if (leader.bindRequired && (bound == nullptr || bound->sid != sid)) {
                    ++heldOffers_;
                    return false;
                }
            }
            if (r.streamId != ingressStreamId) {  // once per route / stream change
                const int32_t stream = ingressStreamId != 0 ? ingressStreamId
                                                            : cfg_.transport.ingressStreamId;
                if (r.bindIdx != kNoBind) {
                    // the entry's publication serves one stream: a produce on another is never sent plain.
                    if (workerBinds_[r.bindIdx].streamId != stream) { ++heldOffers_; return false; }
                    r.pub = workerBinds_[r.bindIdx].pub;
                } else {
                    try {
                        r.pub = transport_.publicationHandle(r.dataChannel, stream);
                    } catch (...) {
                        return false;
                    }
                }
                r.streamId = ingressStreamId;
            }
            if (len >= 16) {
                ::rune::protocol::client::detail::putLE64(const_cast<uint8_t*>(buf), 8, sid);
            }
            // client<->broker TLS/AEAD (Task 6): seal under the LEADER session's
            // CryptoState — the sessionId just patched above belongs to it.
            // Plaintext path (crypto off) is byte-identical to before Task 6.
            if (leader.machine.cryptoState().encrypted) {
                // Backlog B1 interaction: a PENDING RecordBatch CRC is normally
                // computed lazily, only once Aeron accepts the frame (so a
                // back-pressured offer never wastes the computation) — it
                // patches crc.crcAt bytes inside the PLAINTEXT buf/term later.
                // Once sealed, buf's body is opaque ciphertext: that deferred
                // patch would corrupt the AEAD tag instead of writing a real
                // CRC.  So when encrypting, finalize the CRC in the plaintext
                // NOW (sealFrame is unconditional/idempotent, same bytes B1's
                // own lazy path would have written) and offer the sealed frame
                // with an EMPTY FrameCrc — nothing left to defer.
                if (crc.pending()) {
                    transport::sealFrame(const_cast<uint8_t*>(buf), len, crc);
                }
                // Issue #116: sealed for the receiver this offer actually reaches - the route's worker (its own
                // ingress, or its per-worker stream on the node), or the gateway (0) when the frame goes to the
                // node on the default ingress stream.  A legacy session ignores it (receiver 0).
                const uint8_t receiver =
                    (r.dataOnNode && (ingressStreamId == 0 || ingressStreamId == cfg_.transport.ingressStreamId))
                        ? uint8_t{0} : r.receiverId;
                const SealedFrame sealed = sealForOffer(transport_, leader.machine.cryptoState(), buf, len, receiver);
                if (sealed.len == 0) return false;  // seal failed — DROP, do not transmit
                // PR #528 review (mixed-version cluster): a worker route with NO receiver id came from a broker that
                // predates receiver keys (< 7) while this leader's session uses them.  The frame is sealed for the
                // gateway (receiver 0), so it goes to the leader node's gateway, which relays it - never to the worker,
                // which would drop it as addressed to another receiver (every such produce would time out).
                if (receiver == 0 && !r.dataOnNode && leader.machine.cryptoState().receiverKeys) [[unlikely]] {
                    if (&leader == &seedSession()) return transport_.offer(sealed.buf, sealed.len);
                    return transport_.offerTo(leader.endpoint, cfg_.transport.ingressStreamId, sealed.buf, sealed.len);
                }
                return transport_.offerOn(r.pub, sealed.buf, sealed.len, {});
            }
            return transport_.offerOn(r.pub, buf, len, crc);
        }
    }
    // Single-node / no matching route: default offer, on the SEED's session (its
    // CryptoState is the one to seal under).  Fix round 2 (C-1): a fresh encode
    // carries the seed's id already, but a STORED frame re-offered after the seed
    // got a new session (failover, redirect reconnect, a resume answered with a
    // new id) still carries the OLD one — the broker drops a Produce for an
    // unknown session unanswered, so every re-offer and retransmit would be lost
    // until the delivery timeout.  Hold while the seed is not Connected (Resuming
    // keeps the old id as a hint); restamp otherwise (a fresh frame: one load and
    // two compares, nothing written).
    Session& seed = seedSession();
    {
        const int64_t sid = seed.machine.sessionId();
        if (sid == 0 || seed.machine.state() != session::SessionState::Connected) [[unlikely]] {
            ++heldOffers_;
            return false;
        }
        if (len >= 16 && static_cast<int64_t>(::rune::protocol::client::detail::getLE64(buf, 8)) != sid) [[unlikely]]
            ::rune::protocol::client::detail::putLE64(const_cast<uint8_t*>(buf), 8, sid);
    }
    if (seed.machine.cryptoState().encrypted) {
        // Backlog B1 interaction — see the identical comment in the leader
        // branch above: finalize a pending CRC in the plaintext BEFORE
        // sealing; the sealed frame offers with an empty FrameCrc.
        if (crc.pending()) {
            transport::sealFrame(const_cast<uint8_t*>(buf), len, crc);
        }
        const SealedFrame sealed = sealForOffer(transport_, seed.machine.cryptoState(), buf, len);
        if (sealed.len == 0) return false;  // seal failed — DROP, do not transmit
        return transport_.offer(ingressStreamId, sealed.buf, sealed.len, {});
    }
    return transport_.offer(ingressStreamId, buf, len, crc);
}

} // namespace rune::client::agent
