// ClientGateway.cpp — implementation of the client-facing session gateway (BR-4).

#include <rune/gateway/ClientGateway.h>

#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <chrono>
#include <thread>

namespace rune::gateway {

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Resolution timeouts
// ---------------------------------------------------------------------------
static constexpr int kResolveMaxMs   = 5000; // 5 s
static constexpr int kResolveSliceMs = 1;

// ---------------------------------------------------------------------------
// ClientGateway constructor
// ---------------------------------------------------------------------------
ClientGateway::ClientGateway(std::shared_ptr<aeron::Aeron> aeron,
                             std::string                   clientChannel,
                             int32_t                       clientStreamId,
                             int32_t                       thisNodeId)
    : aeron_(std::move(aeron))
    , clientChannel_(std::move(clientChannel))
    , clientStreamId_(clientStreamId)
    , thisNodeId_(thisNodeId)
    , assembler_([this](aeron::concurrent::AtomicBuffer& buffer,
                        aeron::util::index_t             offset,
                        aeron::util::index_t             length,
                        aeron::Header&                   header) {
          onFragment(buffer, offset, length, header);
      })
{
    // Register the ingress subscription.
    int64_t regId = aeron_->addSubscription(clientChannel_, clientStreamId_);

    // Bounded resolve — same pattern as AeronIngress.
    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};
    while (clock::now() < deadline) {
        sub_ = aeron_->findSubscription(regId);
        if (sub_) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
    }
    if (!sub_) {
        throw std::runtime_error(
            "ClientGateway: ingress subscription not resolved within timeout");
    }
}

// ---------------------------------------------------------------------------
// doWork — poll ingress; returns fragment count
// ---------------------------------------------------------------------------
int ClientGateway::doWork(int64_t nowNs) {
    nowNs_ = nowNs;
    constexpr int kFragmentLimit = 10;
    return sub_->poll(assembler_.handler(), kFragmentLimit);
}

// ---------------------------------------------------------------------------
// onFragment — called by FragmentAssembler for each reassembled frame.
// Drop-safe: any exception or decode failure is caught and silently dropped.
// ---------------------------------------------------------------------------
void ClientGateway::onFragment(aeron::concurrent::AtomicBuffer& buffer,
                               aeron::util::index_t             offset,
                               aeron::util::index_t             length,
                               aeron::Header&                   /*header*/)
{
    const auto* buf = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
    const auto  len = static_cast<std::size_t>(length);

    // Step 1: validate frame with checkFrame — reject anything non-conformant.
    const auto fc = pc::checkFrame(buf, len);
    if (fc.status != pc::FrameStatus::Ok) {
        // Drop silently — bad/unknown frame never crashes the gateway.
        return;
    }

    // Step 2: dispatch on templateId.
    const auto tid = static_cast<pc::TemplateId>(fc.templateId);
    switch (tid) {
        case pc::TemplateId::Connect:
            handleConnect(buf, len, nowNs_);
            break;
        case pc::TemplateId::Keepalive:
            handleKeepalive(buf, len, nowNs_);
            break;
        case pc::TemplateId::Ping:
            handlePing(buf, len);
            break;
        case pc::TemplateId::Disconnect:
            handleDisconnect(buf, len);
            break;
        default:
            // ConnectResponse, ErrorResponse, Pong — server → client only.
            // A client publishing them is malformed; drop.
            break;
    }
}

// ---------------------------------------------------------------------------
// handleConnect
// ---------------------------------------------------------------------------
void ClientGateway::handleConnect(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto reqOpt = pc::ConnectRequest::decode(buf, len);
    if (!reqOpt) return; // truncated — drop

    const auto& req = *reqOpt;

    // Allocate a new sessionId.
    const int64_t sessionId = sessions_.nextSessionId();

    // Resolve an egress ExclusivePublication to the client's egress channel.
    auto pub = resolvePublication(std::string(req.egressChannel), req.egressStreamId);
    if (!pub) {
        // Cannot reach the client's egress — log and drop (no session created).
        return;
    }

    // Register the session.
    Session& session = sessions_.add(sessionId, std::move(pub), nowNs);

    // Encode and send ConnectResponse.
    pc::ConnectResponse resp;
    resp.sessionId         = sessionId;
    resp.correlationId     = req.correlationId;
    resp.code              = static_cast<int16_t>(pc::ErrorCode::OK);
    resp.leaderMemberId    = thisNodeId_;
    resp.assignedSessionId = sessionId;

    const std::size_t encoded = resp.encode(scratch_.data());
    sendTo(session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// handleKeepalive
// ---------------------------------------------------------------------------
void ClientGateway::handleKeepalive(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto msgOpt = pc::Keepalive::decode(buf, len);
    if (!msgOpt) return;
    sessions_.touch(msgOpt->sessionId, nowNs);
}

// ---------------------------------------------------------------------------
// handlePing
// ---------------------------------------------------------------------------
void ClientGateway::handlePing(const uint8_t* buf, std::size_t len)
{
    auto pingOpt = pc::Ping::decode(buf, len);
    if (!pingOpt) return;

    Session* session = sessions_.find(pingOpt->sessionId);
    if (!session) {
        // Unknown session — drop (no egress to reply on).
        return;
    }

    // Encode Pong with verbatim echo.
    pc::Pong pong;
    pong.sessionId     = pingOpt->sessionId;
    pong.correlationId = pingOpt->correlationId;
    pong.echo          = pingOpt->echo;

    // Guard: echo payload must fit in the scratch buffer.
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + 2 + pingOpt->echo.size();
    if (needed > scratch_.size()) return; // oversized echo — drop

    const std::size_t encoded = pong.encode(scratch_.data());
    sendTo(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// handleDisconnect
// ---------------------------------------------------------------------------
void ClientGateway::handleDisconnect(const uint8_t* buf, std::size_t len)
{
    auto msgOpt = pc::Disconnect::decode(buf, len);
    if (!msgOpt) return;
    sessions_.remove(msgOpt->sessionId);
}

// ---------------------------------------------------------------------------
// sendTo — tryClaim first, fallback to offer (mirrors AeronMessageSink).
// ---------------------------------------------------------------------------
bool ClientGateway::sendTo(Session& session, const uint8_t* buf, std::size_t len)
{
    auto& pub = *session.egress;
    if (!pub.isConnected()) return false;

    auto* mutableBuf = const_cast<uint8_t*>(buf);
    aeron::concurrent::AtomicBuffer atomicBuf(
        mutableBuf, static_cast<aeron::util::index_t>(len));

    if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
        aeron::concurrent::logbuffer::BufferClaim claim;
        const int64_t pos = pub.tryClaim(
            static_cast<aeron::util::index_t>(len), claim);
        if (pos > 0) {
            claim.buffer().putBytes(claim.offset(), atomicBuf, 0,
                                    static_cast<aeron::util::index_t>(len));
            claim.commit();
            return true;
        }
        // BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED — fall through to offer.
    }

    const int64_t result = pub.offer(atomicBuf, 0,
                                     static_cast<aeron::util::index_t>(len));
    return result > 0;
}

// ---------------------------------------------------------------------------
// resolvePublication — bounded wait for ExclusivePublication, mirrors
// AeronMessageSink::resolvePublications().
// ---------------------------------------------------------------------------
std::shared_ptr<aeron::ExclusivePublication>
ClientGateway::resolvePublication(const std::string& channel, int32_t streamId)
{
    const int64_t regId = aeron_->addExclusivePublication(channel, streamId);

    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};

    while (clock::now() < deadline) {
        auto pub = aeron_->findExclusivePublication(regId);
        if (pub) return pub;
        std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
    }
    return nullptr; // timed out
}

} // namespace rune::gateway
