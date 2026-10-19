// PeerHandshake.cpp — the mutual HMAC challenge-response state machine (authentication plan Task 15, D33, D35).
#include <rune/peerlink/PeerHandshake.h>

#include <rune/crypto/Kdf.h>

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstring>

namespace rune::peerlink {

namespace {

void put32(uint8_t*& p, int32_t v) { detail::putLe(p, static_cast<uint32_t>(v), 4); p += 4; }
void put64(uint8_t*& p, uint64_t v) { detail::putLe(p, v, 8); p += 8; }
void putN(uint8_t*& p, const uint8_t* s, std::size_t n) { std::memcpy(p, s, n); p += n; }

constexpr char kLabelB[] = "rune-peer-v1|B";
constexpr char kLabelA[] = "rune-peer-v1|A";
constexpr char kLabelC[] = "rune-peer-v1|C";

/// macB = HMAC(master, "rune-peer-v1|B" || from=B || to=A || nonceA || nonceB || incA || incB || keyId)
/// macA = HMAC(master, "rune-peer-v1|A" || from=A || to=B || nonceB || nonceA || incB || incA || keyId)
/// confirm = HMAC(master, "rune-peer-v1|C" || from=B || to=A || nonceA || nonceB || epoch)  (32 bytes)
bool transcriptMac(const ClusterKey& key, bool isB, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
                   const std::array<uint8_t, 32>& nonceB, uint64_t incA, uint64_t incB, uint64_t keyId,
                   std::array<uint8_t, 64>& out) {
    uint8_t buf[sizeof kLabelB - 1 + 4 + 4 + 32 + 32 + 8 + 8 + 8];
    uint8_t* p = buf;
    const char* label = isB ? kLabelB : kLabelA;
    putN(p, reinterpret_cast<const uint8_t*>(label), sizeof kLabelB - 1);
    if (isB) {
        put32(p, idB); put32(p, idA); putN(p, nonceA.data(), 32); putN(p, nonceB.data(), 32);
        put64(p, incA); put64(p, incB);
    } else {
        put32(p, idA); put32(p, idB); putN(p, nonceB.data(), 32); putN(p, nonceA.data(), 32);
        put64(p, incB); put64(p, incA);
    }
    put64(p, keyId);
    rune::crypto::DigestBytes d;
    if (!rune::crypto::hmac(rune::crypto::Digest::Sha512, {key.master.data(), key.master.size()},
                            {buf, static_cast<std::size_t>(p - buf)}, d) || d.size != 64) return false;
    std::memcpy(out.data(), d.bytes.data(), 64);
    return true;
}

bool confirmMac(const ClusterKey& key, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
                const std::array<uint8_t, 32>& nonceB, uint32_t epoch, std::array<uint8_t, 32>& out) {
    uint8_t buf[sizeof kLabelC - 1 + 4 + 4 + 32 + 32 + 4];
    uint8_t* p = buf;
    putN(p, reinterpret_cast<const uint8_t*>(kLabelC), sizeof kLabelC - 1);
    put32(p, idB); put32(p, idA); putN(p, nonceA.data(), 32); putN(p, nonceB.data(), 32);
    detail::putLe(p, epoch, 4); p += 4;
    rune::crypto::DigestBytes d;
    if (!rune::crypto::hmac(rune::crypto::Digest::Sha512, {key.master.data(), key.master.size()},
                            {buf, static_cast<std::size_t>(p - buf)}, d) || d.size != 64) return false;
    std::memcpy(out.data(), d.bytes.data(), 32);
    return true;
}

bool macEq(const std::array<uint8_t, 64>& a, const std::array<uint8_t, 64>& b) {
    return rune::crypto::constantTimeEqual({a.data(), a.size()}, {b.data(), b.size()});
}

template <class M> PeerHandshake::Out frameOut(const M& m) {
    static_assert(M::kSize <= PeerHandshake::FrameBuf::kCap);
    PeerHandshake::Out o;
    PeerHandshake::FrameBuf f;
    m.encode(f.bytes.data());
    f.len = M::kSize;
    o.frame = f;
    return o;
}

PeerHandshake::LogLine logLine(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
PeerHandshake::LogLine logLine(const char* fmt, ...) {
    PeerHandshake::LogLine l;
    va_list ap;
    va_start(ap, fmt);
    const int n = std::vsnprintf(l.text.data(), l.text.size(), fmt, ap);
    va_end(ap);
    l.len = n < 0 ? 0 : std::min<std::size_t>(static_cast<std::size_t>(n), l.text.size() - 1);
    return l;
}

PeerHandshake::Out errorOut(PeerHandshake::LogLine l) {
    PeerHandshake::Out o;
    o.log = l;
    o.logIsError = true;
    return o;
}

} // namespace

PeerHandshake::PeerHandshake(int32_t self, int32_t peer, uint64_t selfIncarnation, const ClusterKeys& keys)
    : self_(self), peer_(peer), incarnation_(selfIncarnation), keys_(keys) {}

PeerHandshake::Out PeerHandshake::refuseOut_(RefuseReason r, uint64_t keyId) {
    PeerRefuse m; m.from = self_; m.to = peer_; m.reason = r;
    Out o = frameOut(m);
    // every refusal this side makes is logged (and counted) here
    o.log = logLine("peer link from node %d REFUSED: %s (key id %016llx)", peer_, refuseReasonText(r), static_cast<unsigned long long>(keyId));
    o.logIsError = true;
    return o;
}

void PeerHandshake::restorePrior_(const std::optional<Established>& prior, int64_t nowNs) noexcept {
    if (prior) state_ = *prior; else state_ = LinkIdle{nowNs};
}

void PeerHandshake::onAuthenticated(int64_t nowNs) noexcept {
    if (auto* e = std::get_if<Established>(&state_)) e->lastAuthNs = nowNs;
    else if (auto* h = std::get_if<HelloSent>(&state_)) { if (h->prior) h->prior->lastAuthNs = nowNs; }
    else if (auto* c = std::get_if<ChallengeSent>(&state_)) { if (c->prior) c->prior->lastAuthNs = nowNs; }
}

// A rekey: a fresh Hello while the live link keeps its keys.  Only the initiator, only when the peer has been silent,
// at most one new exchange per kRetryNs.
PeerHandshake::Out PeerHandshake::rekey_(int64_t nowNs) {
    if (!initiator()) return {};
    auto* e = std::get_if<Established>(&state_);
    if (!e || nowNs - e->lastAuthNs < kSilenceNs || nowNs - lastExchangeNs_ < kRetryNs) return {};
    HelloSent hs;
    if (!rune::crypto::randomBytes({hs.nonceA.data(), hs.nonceA.size()})) return {};
    hs.keyId = keys_.current.keyId; hs.sentNs = nowNs; hs.startNs = nowNs; hs.prior = *e;
    lastExchangeNs_ = nowNs;
    PeerHello m; m.from = self_; m.to = peer_; m.keyId = hs.keyId; m.incarnation = incarnation_; m.nonceA = hs.nonceA;
    state_ = hs;
    return frameOut(m);
}

PeerHandshake::Out PeerHandshake::tick(int64_t nowNs) {
    if (initiator()) {
        if (auto* idle = std::get_if<LinkIdle>(&state_)) {
            if (nowNs < idle->retryAtNs || nowNs - lastExchangeNs_ < kRetryNs) return {};
        } else if (auto* h = std::get_if<HelloSent>(&state_)) {
            if (h->prior && nowNs - h->startNs >= kAwaitNs) { restorePrior_(h->prior, nowNs); return {}; }
            if (nowNs - h->sentNs < kRetryNs) return {};
            // The SAME Hello again: a late Challenge for the first still matches.
            PeerHello m; m.from = self_; m.to = peer_; m.keyId = h->keyId; m.incarnation = incarnation_; m.nonceA = h->nonceA;
            h->sentNs = nowNs;
            return frameOut(m);
        } else if (auto* c = std::get_if<ChallengeSent>(&state_)) {
            if (nowNs - c->sentNs >= kAwaitNs) restorePrior_(c->prior, nowNs);   // the Proof or the Accept was lost
            return {};
        } else {
            return {};   // Established
        }
        // LinkIdle and due: a fresh Hello under our CURRENT key (D35).
        HelloSent hs;
        if (!rune::crypto::randomBytes({hs.nonceA.data(), hs.nonceA.size()})) {
            state_ = LinkIdle{nowNs + kRetryNs};
            return errorOut(logLine("peer link to node %d: no random bytes available", peer_));
        }
        hs.keyId = keys_.current.keyId; hs.sentNs = nowNs; hs.startNs = nowNs;
        lastExchangeNs_ = nowNs;
        PeerHello m; m.from = self_; m.to = peer_; m.keyId = hs.keyId; m.incarnation = incarnation_; m.nonceA = hs.nonceA;
        state_ = hs;
        return frameOut(m);
    }
    // Responder.
    if (auto* idle = std::get_if<LinkIdle>(&state_)) {
        if (nowNs < idle->retryAtNs) return {};
        idle->retryAtNs = nowNs + kRetryNs;
        PeerRefuse m; m.from = self_; m.to = peer_; m.reason = RefuseReason::NoLink;     // "start over": a poke, not a refusal
        return frameOut(m);
    }
    if (auto* c = std::get_if<ChallengeSent>(&state_))
        if (nowNs - c->sentNs >= kAwaitNs) restorePrior_(c->prior, nowNs);
    return {};
}

PeerHandshake::Out PeerHandshake::onHello(const PeerHello& h, int64_t nowNs) {
    if (initiator() || h.from != peer_ || h.to != self_) return refuseOut_(RefuseReason::NotAPeer, h.keyId);
    const ClusterKey* key = keys_.byId(h.keyId);
    if (!key) return refuseOut_(RefuseReason::NoCommonKey, h.keyId);           // D35

    // The same Hello again (our Challenge may have been lost): the SAME Challenge.
    std::optional<Established> prior;
    if (auto* c = std::get_if<ChallengeSent>(&state_)) {
        prior = c->prior;
        if (c->nonceA == h.nonceA && c->peerIncarnation == h.incarnation && c->keyId == h.keyId) {
            PeerChallenge m; m.from = self_; m.to = peer_; m.keyId = c->keyId; m.incarnation = incarnation_; m.nonceB = c->nonceB;
            if (!transcriptMac(*key, true, peer_, self_, c->nonceA, c->nonceB, c->peerIncarnation, incarnation_, c->keyId, m.macB))
                return {};
            return frameOut(m);
        }
    } else if (auto* e = std::get_if<Established>(&state_)) {
        prior = *e;   // the live link stays valid until the re-handshake completes
    }
    if (nowNs - lastExchangeNs_ < kRetryNs) return {};   // at most one NEW exchange per kRetryNs: a flood costs a compare
    ChallengeSent cs;
    cs.nonceA = h.nonceA; cs.keyId = h.keyId; cs.peerIncarnation = h.incarnation; cs.sentNs = nowNs; cs.prior = prior;
    if (!rune::crypto::randomBytes({cs.nonceB.data(), cs.nonceB.size()}))
        return errorOut(logLine("peer link from node %d: no random bytes available", peer_));
    PeerChallenge m; m.from = self_; m.to = peer_; m.keyId = h.keyId; m.incarnation = incarnation_; m.nonceB = cs.nonceB;
    if (!transcriptMac(*key, true, peer_, self_, cs.nonceA, cs.nonceB, cs.peerIncarnation, incarnation_, cs.keyId, m.macB)) return {};
    lastExchangeNs_ = nowNs;
    state_ = cs;
    return frameOut(m);
}

PeerHandshake::Out PeerHandshake::onChallenge(const PeerChallenge& c, int64_t nowNs) {
    if (!initiator() || c.from != peer_ || c.to != self_) return {};
    auto* h = std::get_if<HelloSent>(&state_);
    if (!h || c.keyId != h->keyId) return {};    // unsolicited / for another key: nothing
    const ClusterKey* key = keys_.byId(h->keyId);
    if (!key) return {};
    std::array<uint8_t, 64> want{};
    if (!transcriptMac(*key, true, self_, peer_, h->nonceA, c.nonceB, incarnation_, c.incarnation, h->keyId, want)) return {};
    if (!macEq(want, c.macB))   // not the responder (or a forgery): logged, the state stays, the Hello is retried on its timer
        return errorOut(logLine("peer link from node %d REFUSED: %s (key id %016llx)", peer_, refuseReasonText(RefuseReason::BadProof),
                                static_cast<unsigned long long>(h->keyId)));
    PeerProof p; p.from = self_; p.to = peer_;
    if (!transcriptMac(*key, false, self_, peer_, h->nonceA, c.nonceB, incarnation_, c.incarnation, h->keyId, p.macA)) return {};
    ChallengeSent cs;
    cs.nonceA = h->nonceA; cs.nonceB = c.nonceB; cs.keyId = h->keyId; cs.peerIncarnation = c.incarnation; cs.sentNs = nowNs;
    cs.prior = h->prior;
    state_ = cs;
    return frameOut(p);
}

PeerHandshake::Out PeerHandshake::onProof(const PeerProof& p, int64_t nowNs) {
    if (initiator() || p.from != peer_ || p.to != self_) return {};
    auto* cs = std::get_if<ChallengeSent>(&state_);
    if (!cs) return {};    // no Challenge outstanding: nothing to prove against
    const ClusterKey* key = keys_.byId(cs->keyId);
    if (!key) return {};
    std::array<uint8_t, 64> want{};
    if (!transcriptMac(*key, false, peer_, self_, cs->nonceA, cs->nonceB, cs->peerIncarnation, incarnation_, cs->keyId, want)) return {};
    if (!macEq(want, p.macA))   // a forgery: logged, the Challenge stays pending (it expires on its own)
        return errorOut(logLine("peer link from node %d REFUSED: %s (key id %016llx)", peer_, refuseReasonText(RefuseReason::BadProof),
                                static_cast<unsigned long long>(cs->keyId)));
    const uint32_t epoch = ++epochCounter_;
    auto keys = deriveLinkKeys(*key, self_, peer_, false, cs->nonceA, cs->nonceB, epoch);
    PeerAccept a; a.from = self_; a.to = peer_; a.linkEpoch = epoch;
    if (!keys || !confirmMac(*key, peer_, self_, cs->nonceA, cs->nonceB, epoch, a.confirm)) {
        restorePrior_(cs->prior, nowNs);
        return errorOut(logLine("peer link with node %d: key derivation failed", peer_));
    }
    Established e{*keys, cs->peerIncarnation, nowNs, nowNs};
    const uint64_t id = cs->keyId;
    state_ = e;
    Out o = frameOut(a);
    o.established = *keys;
    o.log = logLine("peer link with node %d established (epoch %u, key id %016llx)", peer_, epoch, static_cast<unsigned long long>(id));
    return o;
}

PeerHandshake::Out PeerHandshake::onAccept(const PeerAccept& a, int64_t nowNs) {
    if (!initiator() || a.from != peer_ || a.to != self_) return {};
    auto* cs = std::get_if<ChallengeSent>(&state_);
    if (!cs) return {};
    const ClusterKey* key = keys_.byId(cs->keyId);
    if (!key) return {};
    std::array<uint8_t, 32> want{};
    if (!confirmMac(*key, self_, peer_, cs->nonceA, cs->nonceB, a.linkEpoch, want) ||
        !rune::crypto::constantTimeEqual({want.data(), want.size()}, {a.confirm.data(), a.confirm.size()}))
        return {};    // a forged Accept: ignored, the real one may still come
    auto keys = deriveLinkKeys(*key, self_, peer_, true, cs->nonceA, cs->nonceB, a.linkEpoch);
    if (!keys) { restorePrior_(cs->prior, nowNs); return {}; }
    Established e{*keys, cs->peerIncarnation, nowNs, nowNs};
    const uint64_t id = cs->keyId;
    state_ = e;
    Out o;
    o.established = *keys;
    o.log = logLine("peer link with node %d established (epoch %u, key id %016llx)", peer_, a.linkEpoch, static_cast<unsigned long long>(id));
    return o;
}

PeerHandshake::Out PeerHandshake::onRefuse(const PeerRefuse& r, int64_t nowNs) {
    if (r.from != peer_ || r.to != self_) return {};
    if (r.reason == RefuseReason::NoLink) return rekey_(nowNs);   // the responder restarted - or a forgery: rekey_ is rate limited
    // A refusal carries no MAC: it is logged (counted) and nothing else changes - not the state, not the keys.
    return errorOut(logLine("peer link to node %d REFUSED by %d: %s", peer_, peer_, refuseReasonText(r.reason)));
}

PeerHandshake::Out PeerHandshake::onUnknownEpoch(uint32_t epoch, int64_t nowNs) {
    if (!initiator()) return {};
    const auto* e = std::get_if<Established>(&state_);
    if (!e || e->keys.epoch == epoch) return {};
    return rekey_(nowNs);
}

} // namespace rune::peerlink
