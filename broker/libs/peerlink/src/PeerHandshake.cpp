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
constexpr char kLabelD[] = "rune-peer-v1|D";

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

/// Issue #241 - the initiator's Confirm: HMAC(master, "rune-peer-v1|D" || from=A || to=B || nonceA || nonceB || epoch),
/// 32 bytes.  The sibling of confirmMac with its own label and the direction reversed: an Accept's MAC is never a Confirm.
bool initiatorConfirmMac(const ClusterKey& key, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
                         const std::array<uint8_t, 32>& nonceB, uint32_t epoch, std::array<uint8_t, 32>& out) {
    uint8_t buf[sizeof kLabelD - 1 + 4 + 4 + 32 + 32 + 4];
    uint8_t* p = buf;
    putN(p, reinterpret_cast<const uint8_t*>(kLabelD), sizeof kLabelD - 1);
    put32(p, idA); put32(p, idB); putN(p, nonceA.data(), 32); putN(p, nonceB.data(), 32);
    detail::putLe(p, epoch, 4); p += 4;
    rune::crypto::DigestBytes d;
    if (!rune::crypto::hmac(rune::crypto::Digest::Sha512, {key.master.data(), key.master.size()},
                            {buf, static_cast<std::size_t>(p - buf)}, d) || d.size != 64) return false;
    std::memcpy(out.data(), d.bytes.data(), 32);
    return true;
}

bool mac32Eq(const std::array<uint8_t, 32>& a, const std::array<uint8_t, 32>& b) {
    return rune::crypto::constantTimeEqual({a.data(), a.size()}, {b.data(), b.size()});
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

bool PeerHandshake::mac_(const ClusterKey& key, bool isB, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
                         const std::array<uint8_t, 32>& nonceB, uint64_t incA, uint64_t incB, uint64_t keyId,
                         std::array<uint8_t, 64>& out) {
    ++macOps_;
    return transcriptMac(key, isB, idA, idB, nonceA, nonceB, incA, incB, keyId, out);
}

PeerHandshake::Out PeerHandshake::refuseOut_(RefuseReason r, uint64_t keyId) {
    PeerRefuse m; m.from = self_; m.to = peer_; m.reason = r;
    Out o = frameOut(m);
    // every refusal this side makes is logged (and counted) here
    o.log = logLine("peer link from node %d REFUSED: %s (key id %016llx)", peer_, refuseReasonText(r), static_cast<unsigned long long>(keyId));
    o.logIsError = true;
    return o;
}

// `prior` is taken BY VALUE: callers hand in the copy stored inside the current state_ alternative, which the assignment
// below destroys - assigning from it by reference is a self-overlapping variant assignment.
void PeerHandshake::restorePrior_(std::optional<Established> prior, int64_t nowNs) noexcept {
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
    if (!e || nowNs - e->lastAuthNs < kSilenceNs || nowNs - lastExchangeNs_ < kRetryNs ||
        nowNs - lastHintRekeyNs_ < kHintRekeyGapNs) return {};
    return startRekey_(*e, nowNs);
}

PeerHandshake::Out PeerHandshake::rekeyForTest(int64_t nowNs) {
    auto* e = std::get_if<Established>(&state_);
    if (!initiator() || !e) return {};
    return startRekey_(*e, nowNs);
}

PeerHandshake::Out PeerHandshake::startRekey_(const Established& live, int64_t nowNs) {
    HelloSent hs;
    if (!rune::crypto::randomBytes({hs.nonceA.data(), hs.nonceA.size()})) return {};
    hs.keyId = keys_.current.keyId; hs.sentNs = nowNs; hs.startNs = nowNs; hs.prior = live;   // a copy, before state_ changes
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
    // Issue #241: an Accept not yet confirmed is re-sent (the same bytes) - retransmission only; the promotion waits for
    // the Confirm or for an authenticated frame under the epoch, never for a timer.
    if (auto* e = std::get_if<Established>(&state_); e && e->unconfirmedAccept && nowNs - e->acceptSentNs >= kRetryNs) {
        e->acceptSentNs = nowNs;
        return frameOut(*e->unconfirmedAccept);
    }
    return {};
}

PeerHandshake::Out PeerHandshake::onHello(const PeerHello& h, int64_t nowNs) {
    if (initiator() || h.from != peer_ || h.to != self_) return refuseOut_(RefuseReason::NotAPeer, h.keyId);
    const ClusterKey* key = keys_.byId(h.keyId);
    if (!key) return refuseOut_(RefuseReason::NoCommonKey, h.keyId);           // D35

    // The same Hello again (our Challenge may have been lost): the SAME Challenge from its pending slot.
    for (auto& c : pending_) {
        if (c.used && nowNs - c.sentNs < kAwaitNs && c.nonceA == h.nonceA && c.peerIncarnation == h.incarnation && c.keyId == h.keyId) {
            PeerChallenge m; m.from = self_; m.to = peer_; m.keyId = c.keyId; m.incarnation = incarnation_; m.nonceB = c.nonceB;
            if (!mac_(*key, true, peer_, self_, c.nonceA, c.nonceB, c.peerIncarnation, incarnation_, c.keyId, m.macB))
                return {};
            return frameOut(m);
        }
    }
    // A NEW nonce: nothing about the link state changes.  It takes a free (or expired) slot, else evicts a RANDOM live
    // one (oldest-first would let a flooder kill the real slot after exactly kPending forgeries; random survives n
    // forgeries with probability (7/8)^n).  No per-ms gate: a gate is a lottery a flooder wins.
    PendingChallenge* slot = nullptr;
    for (auto& c : pending_)
        if (!c.used || nowNs - c.sentNs >= kAwaitNs) { slot = &c; break; }
    if (slot == nullptr) {
        static_assert((kPending & (kPending - 1)) == 0, "random eviction index is a mask");
        uint8_t r = 0;
        if (!rune::crypto::randomBytes({&r, 1})) return {};
        slot = &pending_[r & (kPending - 1)];
    }
    PendingChallenge cs;
    cs.used = true; cs.nonceA = h.nonceA; cs.keyId = h.keyId; cs.peerIncarnation = h.incarnation; cs.sentNs = nowNs;
    if (!rune::crypto::randomBytes({cs.nonceB.data(), cs.nonceB.size()}))
        return errorOut(logLine("peer link from node %d: no random bytes available", peer_));
    PeerChallenge m; m.from = self_; m.to = peer_; m.keyId = h.keyId; m.incarnation = incarnation_; m.nonceB = cs.nonceB;
    if (!mac_(*key, true, peer_, self_, cs.nonceA, cs.nonceB, cs.peerIncarnation, incarnation_, cs.keyId, m.macB)) return {};
    if (!mac_(*key, false, peer_, self_, cs.nonceA, cs.nonceB, cs.peerIncarnation, incarnation_, cs.keyId, cs.wantMacA)) return {};
    *slot = cs;
    return frameOut(m);
}

PeerHandshake::Out PeerHandshake::onChallenge(const PeerChallenge& c, int64_t nowNs) {
    if (!initiator() || c.from != peer_ || c.to != self_) return {};
    auto* h = std::get_if<HelloSent>(&state_);
    if (!h || c.keyId != h->keyId) return {};    // unsolicited / for another key: nothing
    const ClusterKey* key = keys_.byId(h->keyId);
    if (!key) return {};
    std::array<uint8_t, 64> want{};
    if (!mac_(*key, true, self_, peer_, h->nonceA, c.nonceB, incarnation_, c.incarnation, h->keyId, want)) return {};
    if (!macEq(want, c.macB))   // not the responder (or a forgery): logged, the state stays, the Hello is retried on its timer
        return errorOut(logLine("peer link from node %d REFUSED: %s (key id %016llx)", peer_, refuseReasonText(RefuseReason::BadProof),
                                static_cast<unsigned long long>(h->keyId)));
    PeerProof p; p.from = self_; p.to = peer_;
    if (!mac_(*key, false, self_, peer_, h->nonceA, c.nonceB, incarnation_, c.incarnation, h->keyId, p.macA)) return {};
    ChallengeSent cs;
    cs.nonceA = h->nonceA; cs.nonceB = c.nonceB; cs.keyId = h->keyId; cs.peerIncarnation = c.incarnation; cs.sentNs = nowNs;
    cs.prior = h->prior;
    state_ = cs;
    return frameOut(p);
}

PeerHandshake::Out PeerHandshake::onProof(const PeerProof& p, int64_t nowNs) {
    if (initiator() || p.from != peer_ || p.to != self_) return {};
    PendingChallenge* cs = nullptr;
    const ClusterKey* key = nullptr;
    // No HMAC here: the expected Proof was computed when the Challenge was issued; a forgery costs only compares.
    for (auto& c : pending_) {
        if (!c.used || nowNs - c.sentNs >= kAwaitNs) continue;
        const ClusterKey* k = keys_.byId(c.keyId);
        if (!k) continue;
        if (macEq(c.wantMacA, p.macA) && cs == nullptr) { cs = &c; key = k; }   // no early exit: constant work per table
    }
    if (cs == nullptr)   // no pending challenge matches: a forgery or a stale Proof (a Proof names no key id) - logged, nothing changes
        return errorOut(logLine("peer link from node %d REFUSED: %s (no pending challenge matches)", peer_, refuseReasonText(RefuseReason::BadProof)));
    const uint32_t epoch = ++epochCounter_;
    auto keys = deriveLinkKeys(*key, self_, peer_, false, cs->nonceA, cs->nonceB, epoch);
    PeerAccept a; a.from = self_; a.to = peer_; a.linkEpoch = epoch;
    if (!keys || !confirmMac(*key, peer_, self_, cs->nonceA, cs->nonceB, epoch, a.confirm))
        return errorOut(logLine("peer link with node %d: key derivation failed", peer_));
    // Issue #241: a re-handshake with the SAME initiator incarnation is installed receive-first - that initiator holds
    // the old epoch and keeps opening it, while it cannot open the new one before it processed this Accept (which
    // travels on another stream, so nothing orders it before our next sealed frame).  A first link, or a restarted
    // initiator (a new incarnation holds no old epoch), installs send-and-receive at once: holding back would only
    // drop frames the new initiator cannot open either way.
    const Established* live = liveLink_();
    const bool receiveFirst = live != nullptr && live->peerIncarnation == cs->peerIncarnation;
    Established e{*keys, cs->peerIncarnation, nowNs, nowNs};
    if (receiveFirst) {
        if (!initiatorConfirmMac(*key, peer_, self_, cs->nonceA, cs->nonceB, epoch, e.wantConfirm))
            return errorOut(logLine("peer link with node %d: key derivation failed", peer_));
        e.unconfirmedAccept = a;
        e.acceptSentNs = nowNs;
    }
    const uint64_t id = cs->keyId;
    for (auto& c : pending_) c.used = false;
    state_ = e;
    Out o = frameOut(a);
    o.established = *keys;
    o.established->install = receiveFirst ? LinkInstall::ReceiveOnly : LinkInstall::SendAndReceive;
    o.log = logLine("peer link with node %d established (epoch %u, key id %016llx%s)", peer_, epoch,
                    static_cast<unsigned long long>(id), receiveFirst ? ", receive-first" : "");
    return o;
}

PeerHandshake::Out PeerHandshake::onAccept(const PeerAccept& a, int64_t nowNs) {
    if (!initiator() || a.from != peer_ || a.to != self_) return {};
    // Issue #241: the responder re-sent the Accept of the epoch we hold (our Confirm was lost): the Confirm again,
    // nothing else changes.  Any other Accept while Established is ignored.
    if (auto* e = std::get_if<Established>(&state_)) {
        if (e->confirm && a.linkEpoch == e->keys.epoch && mac32Eq(a.confirm, e->acceptConfirm)) return frameOut(*e->confirm);
        return {};
    }
    auto* cs = std::get_if<ChallengeSent>(&state_);
    if (!cs) return {};
    const ClusterKey* key = keys_.byId(cs->keyId);
    if (!key) return {};
    std::array<uint8_t, 32> want{};
    if (!confirmMac(*key, self_, peer_, cs->nonceA, cs->nonceB, a.linkEpoch, want) ||
        !rune::crypto::constantTimeEqual({want.data(), want.size()}, {a.confirm.data(), a.confirm.size()}))
        return {};    // a forged Accept: ignored, the real one may still come
    auto keys = deriveLinkKeys(*key, self_, peer_, true, cs->nonceA, cs->nonceB, a.linkEpoch);
    PeerConfirm c; c.from = self_; c.to = peer_; c.linkEpoch = a.linkEpoch;
    if (!keys || !initiatorConfirmMac(*key, self_, peer_, cs->nonceA, cs->nonceB, a.linkEpoch, c.confirm)) {
        restorePrior_(cs->prior, nowNs);
        return {};
    }
    Established e{*keys, cs->peerIncarnation, nowNs, nowNs};
    e.confirm = c;
    e.acceptConfirm = a.confirm;
    const uint64_t id = cs->keyId;
    // The hint-rekey cap is armed only by a rekey that COMPLETED against the same incarnation (the hint was false):
    // an attempt that timed out (a restarted responder not yet answering) must not delay the next one.
    if (cs->prior && cs->prior->peerIncarnation == cs->peerIncarnation) lastHintRekeyNs_ = nowNs;
    state_ = e;
    // The initiator seals under the new epoch at once (the responder already holds it receive-only) and tells the
    // responder so: the Confirm.  A legacy responder ignores it as a malformed frame.
    Out o = frameOut(c);
    o.established = *keys;   // SendAndReceive
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

const Established* PeerHandshake::liveLink_() const noexcept {
    if (const auto* e = std::get_if<Established>(&state_)) return e;
    if (const auto* h = std::get_if<HelloSent>(&state_)) return h->prior ? &*h->prior : nullptr;
    if (const auto* c = std::get_if<ChallengeSent>(&state_)) return c->prior ? &*c->prior : nullptr;
    return nullptr;
}

PeerHandshake::Out PeerHandshake::promote_(Established& e, const char* why) {
    e.unconfirmedAccept.reset();
    Out o;
    o.established = e.keys;
    o.established->install = LinkInstall::SendAndReceive;
    o.log = logLine("peer link with node %d: epoch %u confirmed (%s), sealing under it", peer_, e.keys.epoch, why);
    return o;
}

PeerHandshake::Out PeerHandshake::onConfirm(const PeerConfirm& c, int64_t /*nowNs*/) {
    if (initiator() || c.from != peer_ || c.to != self_) return {};
    auto* e = std::get_if<Established>(&state_);
    // A forged, stale or duplicate Confirm: silently nothing (like a forged Accept).  The MAC compare is constant time.
    if (!e || !e->unconfirmedAccept || c.linkEpoch != e->keys.epoch || !mac32Eq(c.confirm, e->wantConfirm)) return {};
    return promote_(*e, "confirm");
}

PeerHandshake::Out PeerHandshake::onPeerUsedEpoch(uint32_t epoch, int64_t /*nowNs*/) {
    if (initiator()) return {};
    auto* e = std::get_if<Established>(&state_);
    if (!e || !e->unconfirmedAccept || epoch != e->keys.epoch) return {};
    return promote_(*e, "authenticated frame");
}

} // namespace rune::peerlink
