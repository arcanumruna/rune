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

#include <cassert>
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
/// = ringCapacity (power-of-two) + RingBufferDescriptor::TRAILER_LENGTH.
int32_t ringTotalBytes(int32_t cap) noexcept {
    return cap + aeron::concurrent::ringbuffer::RingBufferDescriptor::TRAILER_LENGTH;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

ClientAgent::ClientAgent(AgentConfig cfg)
    : cfg_(cfg)
    , transport_(cfg_.transport)
    , session_(*this, cfg_.session)
    , ringBuf_(makeAlignedBuffer(
          static_cast<std::size_t>(ringTotalBytes(cfg_.ringCapacity)),
          kRingAlignment))
    , ringAtomicBuf_(ringBuf_.get(),
                     ringTotalBytes(cfg_.ringCapacity))
    , ring_(ringAtomicBuf_)
    , egressObserver_(nullptr)
{}

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
                session_.connect(nowNs);
                break;

            case kMsgReconnect:
                // CB-5 B: force a fresh reconnect (data-op redirect).  The
                // transport was already retarget()ed to the leader; drop the
                // stale follower session and connect fresh so the leader
                // assigns a new sessionId.
                session_.forceReconnect(nowNs);
                break;

            case kMsgDisconnect:
                session_.disconnect();
                break;

            case kMsgClose:
                session_.close();
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

    // ---- Step 3: Tick the session machine -----------------------------------
    // tick() drives keepalive cadence and timeout handling.  It may emit
    // frames via sendFrame() → transport_.offer().  We do not count ticks as
    // work; ring + egress fragments are the sole work-done signals (the
    // IdleStrategy backs off on 0, which is correct for idle cycles).
    session_.tick(nowNs);

    // ---- Step 4: CB-2 — Producer duty cycle (drain ring + flush batches) -----
    // Runs on the AGENT thread (single-writer); no lock needed.
    // NanoClock is already sampled as nowNs above — reused here.
    if (producerDrainFn_) {
        const int64_t sessId = session_.sessionId();
        const int producerWork = producerDrainFn_(
            nowNs,
            sessId,
            [this](const uint8_t* buf, std::size_t len) -> bool {
                return transport_.offer(buf, len);
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

void ClientAgent::publishSnapshot() noexcept {
    // Pack (state, sessionId) into ONE atomic value and publish (release).
    // Guard the sessionId range so the packing assumption (48 bits) holds.
    const int64_t sid = session_.sessionId();
    assert(sid >= 0 && (static_cast<uint64_t>(sid) & ~kSessionIdMask) == 0 &&
           "sessionId does not fit in 48 bits — packing invariant violated");
    snapshot_.store(packSnapshot(session_.state(), sid),
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
        session_.onConnectResponse(
            opt->code,
            opt->leaderMemberId,
            opt->assignedSessionId,
            opt->correlationId,
            nowNs);
        session_.onActivity(nowNs);
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
        session_.onErrorResponse(opt->code, opt->message, opt->correlationId, nowNs);
        session_.onActivity(nowNs);
        publishSnapshot();  // FIX F1: fresh snapshot before observer
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::Pong: {
        session_.onActivity(nowNs);
        publishSnapshot();  // FIX F1: fresh snapshot before observer
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::ProduceResponse: {
        // Route to the producer response hook so pending acks are resolved.
        if (produceResponseFn_) {
            auto opt = codec::decodeProduceResponse(buf, len);
            if (opt) {
                produceResponseFn_(opt->correlationId, opt->baseOffset);
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

} // namespace rune::client::agent
