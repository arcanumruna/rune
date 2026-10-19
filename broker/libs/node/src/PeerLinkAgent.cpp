// PeerLinkAgent.cpp — see PeerLinkAgent.h (authentication plan Task 15).
#include <rune/node/PeerLinkAgent.h>

#include <spdlog/spdlog.h>

#include <stdexcept>

namespace rune::node {

namespace pl = rune::peerlink;

namespace {

constexpr uint64_t kLogEvery = 64;

rune::consensus::UdpMessageSink makeSink(const std::shared_ptr<aeron::Aeron>& aeron, const PeerLinkAgent::Params& p,
                                         rune::consensus::PeerResolutionSource resolution, rune::aeronutil::AeronPump pump) {
    std::unordered_map<rune::consensus::NodeId, rune::consensus::UdpMessageSink::PeerEndpoint> eps;
    for (const auto& peer : p.peers) eps[static_cast<rune::consensus::NodeId>(peer.id)] = {peer.channel, p.streamId};
    return rune::consensus::UdpMessageSink(aeron, eps, pump, resolution);
}

} // namespace

PeerLinkAgent::PeerLinkAgent(std::shared_ptr<aeron::Aeron> aeron, Params params,
                             rune::consensus::PeerResolutionSource resolution, rune::aeronutil::AeronPump pump)
    : self_(params.selfId), aeron_(std::move(aeron)),
      sink_(makeSink(aeron_, params, resolution, pump)) {
    // Subscribe FIRST in spirit (the sink's publications resolve lazily): a peer's Hello must find a subscription.
    const int64_t regId = aeron_->addSubscription(params.bindChannel, params.streamId);
    sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); }, rune::aeronutil::AeronPump::kUntilAnswered);
    if (!sub_) throw std::runtime_error("PeerLinkAgent: the peer-auth subscription did not resolve");
    peers_.reserve(params.peers.size());
    for (const auto& peer : params.peers)
        peers_.push_back(PeerSlot{peer.id, pl::PeerHandshake(params.selfId, peer.id, params.incarnation, params.keys), 0, 0});
}

PeerLinkAgent::PeerSlot* PeerLinkAgent::slot_(int32_t id) noexcept {
    for (auto& p : peers_) if (p.id == id) return &p;
    return nullptr;
}

const pl::LinkState* PeerLinkAgent::stateOf(int32_t peer) const noexcept {
    for (const auto& p : peers_) if (p.id == peer) return &p.hs.state();
    return nullptr;
}

int64_t PeerLinkAgent::establishedPeers() const noexcept {
    int64_t n = 0;
    for (const auto& p : peers_) n += std::holds_alternative<pl::Established>(p.hs.state()) ? 1 : 0;
    return n;
}

void PeerLinkAgent::absorb_(PeerSlot& p, pl::PeerHandshake::Out&& o, int64_t nowNs) {
    if (o.log) {
        if (o.logIsError) {
            ++refusals_;   // counter 1'000'234 counts every refusal ...
            if (p.refusals++ % kLogEvery == 0)   // ... the log line is rate-limited (a retry every 200 ms would flood it)
                spdlog::error("BrokerNode[{}]: {}{}", self_, o.log->view(), p.refusals > 1 ? " (repeated)" : "");
        } else {
            spdlog::info("BrokerNode[{}]: {}", self_, o.log->view());
        }
    }
    if (o.frame) sink_.send(static_cast<rune::consensus::NodeId>(p.id), o.frame->data(), o.frame->size());
    if (o.established && sinkFn_ != nullptr) sinkFn_(sinkCtx_, p.id, *o.established, nowNs);
}

void PeerLinkAgent::onFrame_(const uint8_t* b, std::size_t len, int64_t nowNs) {
    // Route by the sender named in the frame (its from field leads each body at offset 1).
    if (len < 9) { ++malformed_; return; }
    const auto tag = pl::peekPeerMsg(b, len);
    if (tag == 0) { ++malformed_; return; }
    const int32_t from = static_cast<int32_t>(pl::detail::getLe(b + 1, 4));
    PeerSlot* p = slot_(from);
    if (p == nullptr) {
        if (malformed_++ % 1024 == 0)
            spdlog::warn("BrokerNode[{}]: peer-link frame from unknown node {} ignored", self_, from);
        return;
    }
    switch (static_cast<pl::PeerMsg>(tag)) {
    case pl::PeerMsg::Hello:     { pl::PeerHello m;     if (pl::PeerHello::decode(b, len, m))     absorb_(*p, p->hs.onHello(m, nowNs), nowNs);     else ++malformed_; break; }
    case pl::PeerMsg::Challenge: { pl::PeerChallenge m; if (pl::PeerChallenge::decode(b, len, m)) absorb_(*p, p->hs.onChallenge(m, nowNs), nowNs); else ++malformed_; break; }
    case pl::PeerMsg::Proof:     { pl::PeerProof m;     if (pl::PeerProof::decode(b, len, m))     absorb_(*p, p->hs.onProof(m, nowNs), nowNs);     else ++malformed_; break; }
    case pl::PeerMsg::Accept:    { pl::PeerAccept m;    if (pl::PeerAccept::decode(b, len, m))    absorb_(*p, p->hs.onAccept(m, nowNs), nowNs);    else ++malformed_; break; }
    case pl::PeerMsg::Refuse:    { pl::PeerRefuse m;    if (pl::PeerRefuse::decode(b, len, m))    absorb_(*p, p->hs.onRefuse(m, nowNs), nowNs);    else ++malformed_; break; }
    }
}

int PeerLinkAgent::doWork(int64_t nowNs) {
    lastNowNs_ = nowNs;
    int work = sub_->poll(
        [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
            onFrame_(reinterpret_cast<const uint8_t*>(buf.buffer()) + off, static_cast<std::size_t>(len), lastNowNs_);
        }, 8);
    for (auto& p : peers_) {
        auto o = p.hs.tick(nowNs);
        if (o.frame || o.log || o.established) { absorb_(p, std::move(o), nowNs); ++work; }
    }
    return work;
}

void PeerLinkAgent::onUnknownEpoch(int32_t peer, uint32_t epoch, int64_t nowNs) {
    if (PeerSlot* p = slot_(peer)) absorb_(*p, p->hs.onUnknownEpoch(epoch, nowNs), nowNs);
}

void PeerLinkAgent::observeAuthFrames(int32_t peer, uint64_t total, int64_t nowNs) noexcept {
    PeerSlot* p = slot_(peer);
    if (p == nullptr || total == p->authSeen) return;
    p->authSeen = total;
    p->hs.onAuthenticated(nowNs);
}

} // namespace rune::node
