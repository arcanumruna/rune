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
    // tick() may emit a keepalive frame via sendFrame (→ transport_.offer).
    // We count it as work if the session is in an active state so that we
    // don't return 0 spuriously while keepalives are being sent.
    // We use a simple proxy: if session is Connected or Connecting, count 1
    // when tick() would have done something — but the brief says we can just
    // always count the tick as 1 if the state is not Disconnected/Closed.
    // In practice: just call tick() and leave totalWork unchanged (tick()
    // is cheap and the IdleStrategy only backs off on 0, which is fine).
    session_.tick(nowNs);
    // NOTE: We do NOT increment totalWork for a tick that emits nothing, but
    // if a keepalive was sent, sendFrame() called transport_.offer() which is
    // outside our fragment counter.  To avoid "phantom work" on pure-idle
    // cycles we only count ring + egress fragments as real work.  The session
    // keepalive itself causes outbound traffic that will eventually produce
    // inbound fragments on subsequent calls — the loop self-regulates.

    return totalWork;
}

// ---------------------------------------------------------------------------
// Thread-safe submission API
// ---------------------------------------------------------------------------

bool ClientAgent::writeCommand(int32_t msgTypeId,
                                const uint8_t* payload,
                                std::size_t payloadLen) noexcept
{
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
    return ring_.write(msgTypeId, ab, 0,
                       static_cast<aeron::util::index_t>(payloadLen));
}

bool ClientAgent::submitConnect() noexcept {
    return writeCommand(kMsgConnect, nullptr, 0);
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

session::SessionState ClientAgent::sessionState() const noexcept {
    return session_.state();
}

int64_t ClientAgent::sessionId() const noexcept {
    return session_.sessionId();
}

// ---------------------------------------------------------------------------
// SessionIo implementation (agent thread only)
// ---------------------------------------------------------------------------

bool ClientAgent::sendFrame(const uint8_t* buf, std::size_t len) {
    return transport_.offer(buf, len);
}

void ClientAgent::retarget(const std::string& newIngressChannel) {
    transport_.retarget(newIngressChannel);
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
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    case codec::TemplateId::Pong: {
        session_.onActivity(nowNs);
        if (egressObserver_) {
            egressObserver_(tid, buf, len);
        }
        break;
    }

    default:
        // Unknown template — drop silently, never crash.
        break;
    }
}

} // namespace rune::client::agent
