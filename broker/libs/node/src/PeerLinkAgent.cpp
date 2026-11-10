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
    if (!sub_) throw std::runtime_error("PeerLinkAgent: the peer-auth subscription " + params.bindChannel + " stream " + std::to_string(params.streamId) +
                                                (pump.deadlineExpired() ? " was not answered by the driver within the startup deadline" : " did not resolve"));
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
    // A peer may seal under an epoch towards this node only once EVERY plane here can open it: the control-thread
    // transports, every worker and the admin thread (the last two get the keys over rings and drain them once per pass,
    // so the order of the calls proves nothing).  What lets the peer seal under it: our Accept (responder), our
    // Confirm and any frame we seal under it (initiator: the responder promotes on either).  So a new epoch is
    // installed RECEIVE-ONLY on every plane first; once every plane acknowledged that install (BrokerNode::
    // servicePeerLinks_ -> releaseHeldAt) the send side switches if the handshake granted it, and the held Accept /
    // Confirm (or its newest re-send) leaves.  Without other planes (the sink answers 0) the keys apply at once.
    //
    // Except a responder's send-and-receive Accept (a first link, or a restarted initiator - no live link): it leaves at
    // once with the keys as granted.  Holding it cannot be made time-independent: the initiator's ChallengeSent gives up
    // after kAwaitNs with no prior link to fall back on, retries with a NEW epoch, and planes slower than kAwaitNs would
    // hold every retry as long - the link never comes up (rule 16).  A rekey's receive-only Accept is held safely: an
    // initiator that gives up keeps its prior link.  The initiator always holds (the responder never gives up on a
    // Confirm - a late one still promotes it).
    const auto tag = o.frame ? static_cast<pl::PeerMsg>(pl::peekPeerMsg(o.frame->data(), o.frame->size())) : pl::PeerMsg{};
    if (o.established && sinkFn_ != nullptr) {
        if (tag == pl::PeerMsg::Accept && o.established->install == pl::LinkInstall::SendAndReceive) {
            p.await.reset();   // a fresh link supersedes any held exchange: its frame and keys are void
            (void)sinkFn_(sinkCtx_, p.id, *o.established, nowNs);
        } else {
            pl::LinkKeys ro = *o.established;   // ~LinkKeys scrubs
            ro.install = pl::LinkInstall::ReceiveOnly;
            const uint64_t seq = sinkFn_(sinkCtx_, p.id, ro, nowNs);
            if (seq == 0) {
                if (o.established->install == pl::LinkInstall::SendAndReceive) (void)sinkFn_(sinkCtx_, p.id, *o.established, nowNs);
            } else {
                // A newer epoch supersedes an exchange still waiting: only the newest switches send, only its frame leaves.
                p.await.emplace();
                p.await->seq = seq;
                if (o.established->install == pl::LinkInstall::SendAndReceive) p.await->promote = *o.established;
            }
        }
    }
    if (o.frame) {
        if (p.await && (tag == pl::PeerMsg::Accept || tag == pl::PeerMsg::Confirm)) p.await->held = std::move(o.frame);
        else sink_.send(static_cast<rune::consensus::NodeId>(p.id), o.frame->data(), o.frame->size());
    }
}

void PeerLinkAgent::releaseHeldAt(std::size_t i) {
    PeerSlot& p = peers_[i];
    if (!p.await) return;
    // Every plane opens the epoch now: switch the send side (its acknowledgement is not awaited - the peer holds the
    // epoch already, it is the one that offered it), then let the peer seal under it.
    if (p.await->promote && sinkFn_ != nullptr) (void)sinkFn_(sinkCtx_, p.id, *p.await->promote, lastNowNs_);
    if (p.await->held) sink_.send(static_cast<rune::consensus::NodeId>(p.id), p.await->held->data(), p.await->held->size());
    p.await.reset();
}

// Issue #554 / #683: charged AFTER decode and the crypto-free checks, per peer.  A Hello that fails the cheap checks is
// let through unbudgeted (it is refused without crypto); one that would cost crypto spends this peer's budget.
bool PeerLinkAgent::admitHello_(PeerSlot& p, int32_t from, int32_t to, uint64_t keyId) noexcept {
    if (!p.hs.helloNeedsCrypto(from, to, keyId)) return true;
    if (p.helloBudget == 0) { ++p.helloShed; ++helloShedTotal_; return false; }
    --p.helloBudget;
    ++helloAnswered_;
    return true;
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
    case pl::PeerMsg::Hello:     { pl::PeerHello m;     if (pl::PeerHello::decode(b, len, m)) { if (admitHello_(*p, m.from, m.to, m.keyId)) absorb_(*p, p->hs.onHello(m, nowNs), nowNs); } else ++malformed_; break; }
    case pl::PeerMsg::Challenge: { pl::PeerChallenge m; if (pl::PeerChallenge::decode(b, len, m)) absorb_(*p, p->hs.onChallenge(m, nowNs), nowNs); else ++malformed_; break; }
    case pl::PeerMsg::Proof:     { pl::PeerProof m;     if (pl::PeerProof::decode(b, len, m))     absorb_(*p, p->hs.onProof(m, nowNs), nowNs);     else ++malformed_; break; }
    case pl::PeerMsg::Accept:    { pl::PeerAccept m;    if (pl::PeerAccept::decode(b, len, m))    absorb_(*p, p->hs.onAccept(m, nowNs), nowNs);    else ++malformed_; break; }
    case pl::PeerMsg::Refuse:    { pl::PeerRefuse m;    if (pl::PeerRefuse::decode(b, len, m))    absorb_(*p, p->hs.onRefuse(m, nowNs), nowNs);    else ++malformed_; break; }
    case pl::PeerMsg::Confirm:   { pl::PeerConfirm m;   if (pl::PeerConfirm::decode(b, len, m))   absorb_(*p, p->hs.onConfirm(m, nowNs), nowNs);   else ++malformed_; break; }
    case pl::PeerMsg::HelloV2:     { pl::PeerHelloV2 m;     if (pl::PeerHelloV2::decode(b, len, m)) { if (admitHello_(*p, m.from, m.to, m.keyId)) absorb_(*p, p->hs.onHelloV2(m, nowNs), nowNs); } else ++malformed_; break; }
    case pl::PeerMsg::ChallengeV2: { pl::PeerChallengeV2 m; if (pl::PeerChallengeV2::decode(b, len, m)) absorb_(*p, p->hs.onChallengeV2(m, nowNs), nowNs); else ++malformed_; break; }
    case pl::PeerMsg::ProofV2:     { pl::PeerProofV2 m;     if (pl::PeerProofV2::decode(b, len, m))     absorb_(*p, p->hs.onProofV2(m, nowNs), nowNs);     else ++malformed_; break; }
    }
}

int PeerLinkAgent::doWork(int64_t nowNs) {
    lastNowNs_ = nowNs;
    for (auto& p : peers_) p.helloBudget = kHelloAnswersPerPeerPerCycle;   // issue #554: before the poll
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

void PeerLinkAgent::onPeerUsedEpoch(int32_t peer, uint32_t epoch, int64_t nowNs) {
    if (PeerSlot* p = slot_(peer)) absorb_(*p, p->hs.onPeerUsedEpoch(epoch, nowNs), nowNs);
}

bool PeerLinkAgent::forceRekeyForTest(int32_t peer, int64_t nowNs) {
    PeerSlot* p = slot_(peer);
    if (p == nullptr) return false;
    auto o = p->hs.rekeyForTest(nowNs);
    const bool started = o.frame.has_value();
    absorb_(*p, std::move(o), nowNs);
    return started;
}

void PeerLinkAgent::observeAuthFrames(int32_t peer, uint64_t total, int64_t nowNs) noexcept {
    PeerSlot* p = slot_(peer);
    if (p == nullptr || total == p->authSeen) return;
    p->authSeen = total;
    p->hs.onAuthenticated(nowNs);
}

} // namespace rune::node
