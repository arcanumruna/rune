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
#include <rune/client/codec/DataCodec.h>
#include <rune/protocol/client/MessageHeader.h>  // Task 1.3: re-stamp sessionId (offset 8)

#include <cassert>
#include <chrono>     // Task 1.3: nowNs for lazy session connect
#include <cstdlib>    // aligned_alloc / free
#include <cstring>    // memcpy
#include <stdexcept>

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
    // Seed the endpoint-keyed session collection with the seed session
    // (sessions_[0]).  Its IO adapter targets the transport default ingress
    // channel, so its control frames fast-path back to the default offer (the
    // byte-identical single-node path).  Task 1.3 lazily appends one Session per
    // additional leader endpoint via sessionForEndpoint().
    sessions_.emplace_back(std::make_unique<Session>(
        this, cfg_.session, cfg_.transport.ingressChannel));
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
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // already started
    }
    transport_.start();

    // Publish initial snapshot so observers see a consistent value immediately.
    publishSnapshot();
}

void ClientAgent::stop() noexcept {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // already stopped
    }
    transport_.stop();
}

// ---------------------------------------------------------------------------
// doWork — single-writer duty cycle
// ---------------------------------------------------------------------------

int ClientAgent::doWork(int64_t nowNs) noexcept {
    int totalWork = 0;

    // ---- Step 1: Drain the command ring (bounded) ---------------------------
    int ringWork = ring_.read(
        [this, nowNs](
            std::int32_t                         msgTypeId,
            aeron::concurrent::AtomicBuffer&     buf,
            aeron::util::index_t                 offset,
            aeron::util::index_t                 len)
        {
            switch (msgTypeId) {
            case kMsgConnect:
                seedSession().machine.connect(nowNs);
                break;

            case kMsgReconnect:
                // CB-5 B: force a fresh reconnect (data-op redirect).  The
                // transport was already retarget()ed to the leader; drop the
                // stale follower session and connect fresh so the leader
                // assigns a new sessionId.
                seedSession().machine.forceReconnect(nowNs);
                break;

            case kMsgDisconnect:
                seedSession().machine.disconnect();
                break;

            case kMsgClose:
                seedSession().machine.close();
                break;

            case kMsgSend:
                if (len > 0) {
                    const auto* p = reinterpret_cast<const uint8_t*>(
                        buf.buffer() + offset);
                    transport_.offer(p, static_cast<std::size_t>(len));
                }
                break;

            default:
                // Unknown command: drop silently.
                break;
            }
        },
        cfg_.ringDrainLimit);

    totalWork += ringWork;

    // ---- Step 2: Poll egress (bounded) --------------------------------------
    int fragments = transport_.poll(
        [this, nowNs](const uint8_t* buf, std::size_t len) {
            dispatchEgressFrame(nowNs, buf, len);
        },
        cfg_.egressPollLimit);

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
            [this](int32_t ingressStreamId, const uint8_t* buf, std::size_t len) -> bool {
                // Task 1.3: partition routing at the flush→sendFn boundary.
                // Fast path (single-node / no per-worker routes): the route table
                // is empty ⇒ one empty-map check, then the byte-identical default
                // offer.  Multi-node: an int32_t-keyed find resolves the leader
                // endpoint; we re-stamp the frame's sessionId (header offset 8) to
                // that leader session's id, then offer directly to its endpoint.
                if (!routeByStream_.empty() && ingressStreamId != 0) {
                    const auto it = routeByStream_.find(ingressStreamId);
                    if (it != routeByStream_.end() && !it->second.empty()) {
                        Session& leader = sessionForEndpoint(it->second);
                        const int64_t sid = leader.machine.sessionId();
                        // Leader session not yet established (async open in flight):
                        // report a failed offer so the producer retains the batch and
                        // retries on the next duty cycle (hold-and-refresh, no drop).
                        if (sid == 0 ||
                            leader.machine.state() != session::SessionState::Connected) {
                            return false;
                        }
                        // Re-stamp sessionId in place so the leader accepts the frame
                        // (the producer stamped the seed session id at encode time).
                        // The buffer is the producer's mutable encodeScratch_/wireFrame.
                        if (len >= 16) {
                            ::rune::protocol::client::detail::putLE64(
                                const_cast<uint8_t*>(buf), 8, sid);
                        }
                        return transport_.offerTo(it->second, ingressStreamId, buf, len);
                    }
                }
                return transport_.offer(ingressStreamId, buf, len);
            });
        totalWork += producerWork;
    }

    // ---- Step 5: Publish observer snapshot (release) ------------------------
    // Catch-all publish for tick-driven transitions (keepalive/timeout/resume).
    // Frame-driven transitions are ALSO published inside dispatchEgressFrame
    // BEFORE the observer runs (FIX F1), so a thread woken by the observer sees
    // a snapshot at least as fresh as the state that woke it.
    publishSnapshot();

    return totalWork;
}

// ---------------------------------------------------------------------------
// Thread-safe submission API
// ---------------------------------------------------------------------------

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
    // propagating through noexcept.
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
    return writeCommand(kMsgSend, frame, len);
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

void ClientAgent::publishSnapshot() noexcept {
    // Pack (state, sessionId) into ONE atomic value and publish (release).
    // Guard the sessionId range so the packing assumption (48 bits) holds.
    const int64_t sid = seedSession().machine.sessionId();
    assert(sid >= 0 && (static_cast<uint64_t>(sid) & ~kSessionIdMask) == 0 &&
           "sessionId does not fit in 48 bits — packing invariant violated");
    snapshot_.store(packSnapshot(seedSession().machine.state(), sid),
                    std::memory_order_release);
}

session::SessionState ClientAgent::sessionState() const noexcept {
    // Reads the packed atomic snapshot (release-published on the agent thread).
    // Thread-safe: callable from any thread.
    return unpackState(snapshot_.load(std::memory_order_acquire));
}

int64_t ClientAgent::sessionId() const noexcept {
    // Reads the packed atomic snapshot (release-published on the agent thread).
    // Thread-safe: callable from any thread.
    return unpackSessionId(snapshot_.load(std::memory_order_acquire));
}

ClientAgent::StateSnapshot ClientAgent::stateAndSessionId() const noexcept {
    // Single atomic load → coherent (state, sessionId) pair (FIX F3).
    const uint64_t v = snapshot_.load(std::memory_order_acquire);
    return StateSnapshot{unpackState(v), unpackSessionId(v)};
}

// ---------------------------------------------------------------------------
// SessionIo implementation (agent thread only)
// ---------------------------------------------------------------------------

bool ClientAgent::sendFrame(const uint8_t* buf, std::size_t len) {
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
        s.machine.onConnectResponse(
            opt->code,
            opt->leaderMemberId,
            opt->assignedSessionId,
            opt->correlationId,
            nowNs);
        s.machine.onActivity(nowNs);
        // FIX F1: publish the fresh (state, sessionId) BEFORE waking any thread
        // via the observer, so a thread returning from connect().get() and
        // immediately calling ping() reads Connected (not the stale snapshot).
        publishSnapshot();
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
        s.machine.onErrorResponse(opt->code, opt->message, opt->correlationId, nowNs);
        s.machine.onActivity(nowNs);
        publishSnapshot();  // FIX F1: fresh snapshot before observer
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::Pong: {
        seedSession().machine.onActivity(nowNs);
        publishSnapshot();  // FIX F1: fresh snapshot before observer
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
        // Route to the producer response hook so pending acks are resolved.
        //
        // DR-BUG-5 client fix: always check errorCode before resolving.  A
        // non-zero errorCode (e.g. MinIsrViolation=10) means the broker
        // rejected the produce — the record was NOT written.  Pass a
        // negative sentinel (-errorCode) so ProducerImpl::onProduceResponse
        // can distinguish an error response from a genuine baseOffset=0.
        // Valid committed baseOffsets are always >= 0, so negative is unambiguous.
        if (produceResponseFn_) {
            auto opt = codec::decodeProduceResponse(buf, len);
            if (opt) {
                const int64_t sentinel =
                    (opt->errorCode != 0)
                        ? -static_cast<int64_t>(static_cast<uint16_t>(opt->errorCode))
                        : opt->baseOffset;
                produceResponseFn_(opt->correlationId, sentinel);
            }
        }
        // Also forward to egress observer so Client can chain-observe if needed.
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
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

bool ClientAgent::offerControlTo(const std::string& channel,
                                 const uint8_t* buf, std::size_t len) noexcept {
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
    if (endpoint.empty() || endpoint == seedSession().endpoint) {
        return seedSession();
    }
    for (auto& s : sessions_) {
        if (s->endpoint == endpoint) return *s;
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
    sessions_.emplace_back(std::make_unique<Session>(this, std::move(scfg), endpoint));
    Session& s = *sessions_.back();
    s.machine.connect(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
    s.connectSubmitted = true;
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

void ClientAgent::setRoute(int32_t ingressStreamId,
                           const std::string& endpoint) noexcept {
    if (ingressStreamId == 0) return;  // 0 = legacy/default stream — never routed
    if (endpoint.empty() || endpoint == seedSession().endpoint) {
        // Route resolves to the seed/default endpoint → drop the entry so the hot
        // path stays on the byte-identical default offer.
        routeByStream_.erase(ingressStreamId);
        return;
    }
    routeByStream_[ingressStreamId] = endpoint;
}

std::string ClientAgent::routeEndpointFor(int32_t ingressStreamId) const noexcept {
    const auto it = routeByStream_.find(ingressStreamId);
    return (it != routeByStream_.end()) ? it->second : std::string{};
}

} // namespace rune::client::agent
