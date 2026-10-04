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
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/client/MessageHeader.h>  // Task 1.3: re-stamp sessionId (offset 8)

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

ClientAgent::ClientAgent(AgentConfig cfg)
    : cfg_(cfg)
    , transport_(cfg_.transport)
    , ringBuf_(makeAlignedBuffer(
          static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)),
          kRingAlignment))
    , ring_(ringBuf_.get(), static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)))
    , egressObserver_(nullptr)
{
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
    if (started_) return;  // already started (owner state, see ClientAgent.h)
    started_ = true;
    transport_.start();
}

void ClientAgent::stop() noexcept {
    if (stopped_) return;  // already stopped (owner state, see ClientAgent.h)
    stopped_ = true;
    transport_.stop();
}

// ---------------------------------------------------------------------------
// doWork — single-writer duty cycle
// ---------------------------------------------------------------------------

int ClientAgent::doWork(int64_t nowNs) noexcept {
    // fix1 C2/I3: a no-op after stop() — belt-and-suspenders against any stray
    // call reaching here once the transport (and everything it touches) is
    // being/has been torn down.
    if (stopped_) return 0;

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
                        break;
                    }
                    switch (offerIngress_(p, n)) {
                    case transport::AeronTransport::OfferResult::Accepted:
                        break;
                    case transport::AeronTransport::OfferResult::Retry:
                        ++ingressOfferRefused_;
                        return CA::ABORT;  // stays at the head; retried next cycle
                    case transport::AeronTransport::OfferResult::Fatal:
                        noteFatalDrop_(n);  // fix4 M2: rate-limited log, every drop counted
                        break;
                    }
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

            case kMsgRegisterTxnProducer:
                if (txnProducerRegisterFn_) {
                    const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                    auto msg = ring_msg::RegisterProducer::decode(p, static_cast<std::size_t>(len));
                    if (msg) txnProducerRegisterFn_(reinterpret_cast<void*>(msg->handle));
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
    // mid-write, right now, on a producer()/transactionalProducer() call that
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
                const ProducerRegisterFn* fn =
                    (msgTypeId == kMsgRegisterProducer)      ? &producerRegisterFn_
                    : (msgTypeId == kMsgRegisterTxnProducer) ? &txnProducerRegisterFn_
                                                             : nullptr;
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

ClientAgent::Submit ClientAgent::submitRegisterTxnProducer(void* impl) noexcept {
    ring_msg::RegisterProducer msg;
    msg.handle = reinterpret_cast<uint64_t>(impl);
    uint8_t buf[ring_msg::RegisterProducer::ENCODED_SIZE];
    const std::size_t n = msg.encode(buf);
    return writeCommand3_(kMsgRegisterTxnProducer, buf, n);
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
// SessionIo implementation (agent thread only)
// ---------------------------------------------------------------------------

bool ClientAgent::sendFrame(const uint8_t* buf, std::size_t len) {
    if (failed_) [[unlikely]] return false;  // T15b: a failed client does no I/O
    return transport_.offer(buf, len);
}

bool ClientAgent::retarget(const std::string& newIngressChannel) noexcept {
    // FIX F8: forward to the transport's noexcept, non-throwing retarget.
    return transport_.retarget(newIngressChannel);
}

// ---------------------------------------------------------------------------
// dispatchEgressFrame — decode + route one reassembled egress frame
// ---------------------------------------------------------------------------

void ClientAgent::dispatchEgressFrame(int64_t nowNs,
                                       const uint8_t* buf,
                                       std::size_t    len) noexcept
{
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
        s.machine.onConnectResponse(
            opt->code,
            opt->leaderMemberId,
            opt->assignedSessionId,
            opt->correlationId,
            nowNs);
        s.machine.onActivity(nowNs);
        // (F8c C3: the observer — Client::onEgressFrame — publishes the new
        // state to app threads (Client::state()) BEFORE it completes the
        // connect, so a thread woken by that completion reads Connected.)
        // Task 1.4 (redirect convergence): a leader session just (re)connected.
        // In multi-node mode, re-drive all in-flight produce frames through their
        // partition routes — held batches whose leader is THIS newly-connected
        // session now send.  Fires only when a session actually reached Connected
        // and per-partition routing is active (no cost single-node).
        if (opt->code == 0 && routeIndex_.size() != 0 && produceResendFn_ &&
            s.machine.state() == session::SessionState::Connected) {
            produceResendFn_();
        }
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::ErrorResponse: {
        auto opt = codec::decodeErrorResponse(buf, len);
        if (!opt) break;
        // Task 1.3: route to the session whose in-flight connect corrId matches
        // (a Redirect/NotLeader during a session's connect); otherwise the seed.
        Session& s = sessionForConnectCorrId(opt->correlationId);
        if (mnDebug_) {
            std::fprintf(stderr, "[MNDBG err] ErrorResponse corrId=%lld code=%d msg='%.*s' -> session ep='%s' (isSeed=%d)\n",
                (long long)opt->correlationId, (int)opt->code,
                (int)opt->message.size(), opt->message.data(),
                s.endpoint.c_str(), (int)(&s == &seedSession()));
        }
        s.machine.onErrorResponse(opt->code, opt->message, opt->correlationId, nowNs);
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
        if (forwardProduceResponses_ && egressObserver_) egressObserver_(tid, buf, len);
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
    return agent->offerControlTo(endpoint, buf, len);
}

bool SessionEndpointIo::retarget(const std::string& /*newIngressChannel*/) noexcept {
    // Multi-node addresses each leader endpoint explicitly (offerTo), so a per-node
    // session never retargets the shared default pub.  A ConnectResponse-based
    // redirect on a leader session is a no-op here (the leader endpoint is already
    // the correct target); the Client-level redirect handler drives re-routing via
    // the metadata cache + routing table.  Report success so the SessionMachine
    // proceeds to resend its ConnectRequest on the same (correct) endpoint.
    return true;
}

bool SessionEndpointIo::retargetToBootstrap() noexcept {
    // Only the seed session's endpoint is the transport's default publication,
    // which redirects / stick-to-leader retarget.  A leader session addresses its
    // own endpoint explicitly and is re-routed by metadata: nothing to undo.
    if (endpoint != agent->cfg_.transport.ingressChannel) return true;
    return agent->retargetSeedToBootstrap_();
}

bool SessionEndpointIo::retargetToLastKnownLeader() noexcept {
    if (endpoint != agent->cfg_.transport.ingressChannel) return true;  // leader session: no-op
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
    if (!transport_.retarget(bootstrap)) return false;  // old pub intact; next attempt retries
    Session& seed               = seedSession();
    seed.lastKnownLeaderChannel = leaving;
    seed.lastKnownLeaderNode    = seed.currentLeaderNode;
    seed.currentLeaderChannel   = bootstrap;
    seed.currentLeaderNode      = -1;  // unknown until the gateway redirects us again
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
    if (!transport_.retarget(seed.lastKnownLeaderChannel)) return false;  // next odd attempt retries
    seed.currentLeaderChannel = seed.lastKnownLeaderChannel;
    seed.currentLeaderNode    = seed.lastKnownLeaderNode;
    return true;
}

bool ClientAgent::offerControlTo(const std::string& channel,
                                 const uint8_t* buf, std::size_t len) noexcept {
    if (failed_) [[unlikely]] return false;  // T15b: a failed client does no I/O
    // Seed endpoint == transport default ingress channel → byte-identical default
    // offer (single-node fast path).  Otherwise route the control frame to the
    // session's own endpoint over the shared transport (default ingress stream id).
    if (channel.empty() || channel == cfg_.transport.ingressChannel) {
        return transport_.offer(buf, len);
    }
    return transport_.offerTo(channel, cfg_.transport.ingressStreamId, buf, len);
}

Session& ClientAgent::sessionForEndpoint(const std::string& endpoint) noexcept {
    // Empty / seed endpoint → the seed session (single-node hot path, no scan cost
    // when there is only one session).
    if (endpoint.empty() || sameEndpoint(endpoint, seedSession().endpoint)) {
        return seedSession();
    }
    for (auto& s : sessions_) {
        if (sameEndpoint(s->endpoint, endpoint)) return *s;  // M6: one session per node
    }
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
    sessions_.emplace_back(std::make_unique<Session>(this, std::move(scfg), endpoint));
    Session& s = *sessions_.back();
    s.machine.connect(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
    s.connectSubmitted = true;
    if (mnDebug_) {
        std::fprintf(stderr, "[MNDBG new-session] ep='%s' corrIdBase=%lld nSessions=%zu connectCorrId=%lld\n",
            endpoint.c_str(), (long long)(static_cast<int64_t>(sessions_.size()-1) * kSessionCorrIdStride),
            sessions_.size(), (long long)s.machine.correlationId());
    }
    return s;
}

Session& ClientAgent::sessionForConnectCorrId(int64_t corrId) noexcept {
    for (auto& s : sessions_) {
        // A session awaiting its ConnectResponse holds the matching correlationId
        // on its SessionMachine while Connecting.  Match only non-seed or genuinely
        // in-flight sessions; the seed is the fallback below.
        if (s->machine.state() == session::SessionState::Connecting &&
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
                           const std::string& dataChannel) noexcept {
    const int64_t  key  = routeKey(topicId, partition);
    const uint32_t idx  = routeIndex_.find(key);
    const std::string& seed = seedSession().endpoint;
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
        if (idx != detail::FlatIndexMap::kNone) {
            PartitionRoute& r = routes_[idx];
            if (r.sessionEndpoint == sessionEndpoint && r.dataChannel == data) return false;
            const auto oldPub = r.pub;
            r = PartitionRoute{sessionEndpoint, data};
            releaseRoutePub_(oldPub);  // M4: the old worker publication, once unreferenced
            return true;
        }
        uint32_t slot;
        if (!freeRoutes_.empty()) {
            slot = freeRoutes_.back();
            freeRoutes_.pop_back();
            routes_[slot] = PartitionRoute{sessionEndpoint, data};
        } else {
            slot = static_cast<uint32_t>(routes_.size());
            routes_.push_back(PartitionRoute{sessionEndpoint, data});
        }
        routeIndex_.insert(key, slot);
        return true;
    } catch (...) {
        return false;
    }
}

std::string ClientAgent::routeEndpointFor(int32_t topicId, int32_t partition) const noexcept {
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    return idx == detail::FlatIndexMap::kNone ? std::string{} : routes_[idx].sessionEndpoint;
}

std::string ClientAgent::dataChannelFor(int32_t topicId, int32_t partition) const noexcept {
    const uint32_t idx = routeIndex_.find(routeKey(topicId, partition));
    return idx == detail::FlatIndexMap::kNone ? std::string{} : routes_[idx].dataChannel;
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
            if (sid == 0 || leader.machine.state() != session::SessionState::Connected) return false;
            if (r.streamId != ingressStreamId) {  // once per route / stream change
                const int32_t stream = ingressStreamId != 0 ? ingressStreamId
                                                            : cfg_.transport.ingressStreamId;
                try {
                    r.pub = transport_.publicationHandle(r.dataChannel, stream);
                } catch (...) {
                    return false;
                }
                r.streamId = ingressStreamId;
            }
            if (len >= 16) {
                ::rune::protocol::client::detail::putLE64(const_cast<uint8_t*>(buf), 8, sid);
            }
            return transport_.offerOn(r.pub, buf, len, crc);
        }
    }
    return transport_.offer(ingressStreamId, buf, len, crc);
}

} // namespace rune::client::agent
