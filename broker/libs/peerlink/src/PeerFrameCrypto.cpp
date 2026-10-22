// PeerFrameCrypto.cpp — see PeerFrameCrypto.h (authentication plan Task 16).
#include <rune/peerlink/PeerFrameCrypto.h>

#include <rune/crypto/Kdf.h>

#include <cstring>

namespace rune::peerlink {

namespace {

constexpr rune::crypto::Cipher kFrameCipher = rune::crypto::Cipher::Aes256Gcm;

/// One frame key + nonce prefix per (direction master, sender id): HKDF(SHA-512, ikm = master, salt, info = label ||
/// senderId) -> 32 key bytes + 4 nonce-prefix bytes.
bool deriveFrameKey(const std::array<uint8_t, 32>& master, uint8_t senderId, rune::crypto::AeadKey& key,
                    uint8_t prefix[rune::crypto::kNoncePrefix]) {
    static constexpr char kSalt[] = "rune-peer-frame-v1";
    static constexpr char kInfo[] = "rune-peer-frame|";
    uint8_t info[sizeof kInfo];
    std::memcpy(info, kInfo, sizeof kInfo - 1);
    info[sizeof kInfo - 1] = senderId;
    uint8_t out[32 + rune::crypto::kNoncePrefix];
    if (!rune::crypto::hkdf(rune::crypto::Digest::Sha512, {master.data(), master.size()},
                            {reinterpret_cast<const uint8_t*>(kSalt), sizeof kSalt - 1}, {info, sizeof info}, {out, sizeof out}))
        return false;
    std::memcpy(key.bytes, out, 32);
    std::memcpy(prefix, out + 32, rune::crypto::kNoncePrefix);
    return true;
}

inline void putLe(uint8_t* p, uint64_t v, std::size_t n) noexcept { for (std::size_t i = 0; i < n; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
inline uint64_t getLe(const uint8_t* p, std::size_t n) noexcept { uint64_t v = 0; for (std::size_t i = 0; i < n; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i); return v; }

/// The 64-wide replay window of the client CryptoState (gateway/CryptoFrame.h), verbatim: check READ-ONLY before the
/// open, commit ONLY after the tag verified (a forged seq never poisons the window).
[[nodiscard]] inline bool replayWindowCheck(uint64_t recvSeq, uint64_t recvWindow, uint64_t seq) noexcept {
    if (seq == 0) return false;
    if (seq > recvSeq) return true;
    const uint64_t diff = recvSeq - seq;
    if (diff >= 64) return false;
    return (recvWindow & (uint64_t{1} << diff)) == 0;
}
inline void replayWindowCommit(uint64_t& recvSeq, uint64_t& recvWindow, uint64_t seq) noexcept {
    if (seq > recvSeq) {
        const uint64_t diff = seq - recvSeq;
        recvWindow = diff >= 64 ? uint64_t{1} : ((recvWindow << diff) | uint64_t{1});
        recvSeq = seq;
    } else {
        recvWindow |= (uint64_t{1} << (recvSeq - seq));
    }
}

constexpr std::size_t kAadBytes = kPeerHeaderBytes + 2;
inline void buildAad(uint8_t* aad, const uint8_t* header, int32_t toNode) noexcept {
    std::memcpy(aad, header, kPeerHeaderBytes);
    putLe(aad + kPeerHeaderBytes, static_cast<uint16_t>(toNode), 2);
}

} // namespace

PeerFrameCrypto::PeerFrameCrypto(int32_t selfNode, uint8_t senderId) : self_(selfNode), sender_(senderId) {}

const char* PeerFrameCrypto::name(OpenResult r) noexcept {
    switch (r) {
        case OpenResult::Ok:           return "ok";
        case OpenResult::NotPeerFrame: return "not a peer frame";
        case OpenResult::UnknownPeer:  return "unknown peer (no link)";
        case OpenResult::UnknownEpoch: return "unknown link epoch";
        case OpenResult::BadTag:       return "bad tag";
        case OpenResult::Replay:       return "replay";
        case OpenResult::BadSender:    return "sender id is not this receiver's role";
    }
    return "?";
}

const PeerFrameCrypto::PeerSlot* PeerFrameCrypto::slotOf_(int32_t peer) const noexcept {
    for (std::size_t i = 0; i < used_; ++i) if (slots_[i].peer == peer) return &slots_[i];
    return nullptr;
}
PeerFrameCrypto::PeerSlot* PeerFrameCrypto::slotOf_(int32_t peer) noexcept {
    for (std::size_t i = 0; i < used_; ++i) if (slots_[i].peer == peer) return &slots_[i];
    return nullptr;
}

bool PeerFrameCrypto::hasLink(int32_t peer) const noexcept {
    const PeerSlot* s = slotOf_(peer);
    return s != nullptr && s->sendIdx != kNoSend;
}
uint32_t PeerFrameCrypto::currentEpoch(int32_t peer) const noexcept {
    const PeerSlot* s = slotOf_(peer);
    const EpochState* e = s != nullptr ? sendOf_(*s) : nullptr;
    return e != nullptr ? e->keys.epoch : 0u;
}
bool PeerFrameCrypto::holdsEpoch(int32_t peer, uint32_t epoch) const noexcept {
    const PeerSlot* s = slotOf_(peer);
    if (s == nullptr) return false;
    for (const auto& e : s->epochs) if (e && e->keys.epoch == epoch) return true;
    return false;
}
bool PeerFrameCrypto::takePeerUsedEpoch(int32_t& peer, uint32_t& epoch) noexcept {
    if (!peerUsed_) return false;
    peer = peerUsed_->peer; epoch = peerUsed_->epoch;
    peerUsed_.reset();
    return true;
}

bool PeerFrameCrypto::installLink(int32_t peer, const LinkKeys& keys, int64_t /*nowNs: unused, see the header*/) {
    if (peer < 0 || peer > 0xFFFF || self_ < 0 || self_ > 0xFFFF || keys.epoch == 0) return false;
    if (keys.install != LinkInstall::ReceiveOnly && keys.install != LinkInstall::SendAndReceive) return false;
    PeerSlot* s = slotOf_(peer);
    if (s == nullptr) {
        if (used_ == kMaxPeers) return false;
        s = &slots_[used_++];
        s->peer = peer;
    }
    auto promote = [&](std::size_t i) {
        if (keys.install != LinkInstall::SendAndReceive) return;
        s->sendIdx   = static_cast<uint8_t>(i);
        s->sendOrder = s->epochs[i]->installOrder;
    };
    // 1. The epoch is held already.
    for (std::size_t i = 0; i < kEpochSlots; ++i) {
        auto& e = s->epochs[i];
        if (!e || e->keys.epoch != keys.epoch) continue;
        if (e->keys.sendMaster == keys.sendMaster && e->keys.recvMaster == keys.recvMaster) {
            promote(i);   // the promotion path: the windows and sequence numbers are kept
            return true;
        }
        // A restarted responder reused the epoch number: replace that slot fresh (below), never mix two key sets.
        if (s->sendIdx == i) { s->sendIdx = kNoSend; s->sendOrder = 0; }
        e.reset();
    }
    // 2. A free slot, else evict the oldest install that is not the send epoch.
    std::size_t at = kEpochSlots;
    for (std::size_t i = 0; i < kEpochSlots && at == kEpochSlots; ++i) if (!s->epochs[i]) at = i;
    if (at == kEpochSlots) {
        uint64_t oldest = UINT64_MAX;
        for (std::size_t i = 0; i < kEpochSlots; ++i)
            if (i != s->sendIdx && s->epochs[i]->installOrder < oldest) { oldest = s->epochs[i]->installOrder; at = i; }
    }
    EpochState fresh;
    fresh.keys = keys;
    rune::crypto::AeadKey key{};
    uint8_t prefix[rune::crypto::kNoncePrefix];
    // BOTH ciphers at install (cold): a promotion is a field store.
    if (!deriveFrameKey(keys.sendMaster, sender_, key, prefix)) return false;
    fresh.send.emplace(kFrameCipher, key, prefix);
    // The symmetric deployment: the peer's transport of the same role sends with the same sender id, and it is the
    // ONLY sender id this receiver accepts (open() refuses any other before any crypto).
    if (!deriveFrameKey(keys.recvMaster, sender_, key, prefix)) return false;
    fresh.recv.senderId = sender_;
    fresh.recv.cipher.emplace(kFrameCipher, key, prefix);
    fresh.installOrder = ++installs_;
    if (s->sendIdx == at) { s->sendIdx = kNoSend; s->sendOrder = 0; }   // unreachable (never evicted); defensive
    s->epochs[at].emplace(std::move(fresh));
    promote(at);
    return true;
}

std::size_t PeerFrameCrypto::sealAt_(int32_t peer, const uint8_t* plain, std::size_t len, uint8_t* frame,
                                     std::size_t cap, bool inPlace) noexcept {
    PeerSlot* s = slotOf_(peer);
    if (s == nullptr || s->sendIdx == kNoSend) { ++drops_; ++noLinkDrops_; return 0; }
    if (cap < len + kPeerFrameOverhead) { ++drops_; return 0; }
    EpochState& e = *s->epochs[s->sendIdx];
    const uint64_t seq = ++e.sendSeq;
    frame[0] = kPeerFrameMagic;
    frame[1] = sender_;
    putLe(frame + 2, static_cast<uint16_t>(self_), 2);
    putLe(frame + 4, e.keys.epoch, 4);
    putLe(frame + 8, seq, 8);
    uint8_t aad[kAadBytes];
    buildAad(aad, frame, peer);
    const std::size_t n = e.send->seal(seq, aad, kAadBytes, inPlace ? frame + kPeerHeaderBytes : plain, len,
                                       frame + kPeerHeaderBytes);
    if (n == 0) { ++drops_; return 0; }   // a cipher error: NEVER transmit (the seq is burned, which is harmless)
    return kPeerHeaderBytes + n;
}

std::size_t PeerFrameCrypto::seal(int32_t peer, const uint8_t* plain, std::size_t len, uint8_t* out,
                                  std::size_t cap) noexcept {
    return sealAt_(peer, plain, len, out, cap, /*inPlace=*/false);
}
std::size_t PeerFrameCrypto::sealInPlace(int32_t peer, uint8_t* frame, std::size_t plainLen, std::size_t cap) noexcept {
    return sealAt_(peer, frame + kPeerHeaderBytes, plainLen, frame, cap, /*inPlace=*/true);
}

PeerFrameCrypto::EpochState* PeerFrameCrypto::epochOf_(PeerSlot& s, uint32_t epoch) noexcept {
    for (auto& e : s.epochs) if (e && e->keys.epoch == epoch) return &*e;   // kEpochSlots compares, no clock
    return nullptr;
}

uint64_t PeerFrameCrypto::authFrames(int32_t peer) const noexcept {
    const PeerSlot* s = slotOf_(peer);
    return s != nullptr ? s->authFrames : 0u;
}

void PeerFrameCrypto::commitOpened(int32_t fromNode, uint32_t epoch, uint64_t seq) noexcept {
    PeerSlot* s = slotOf_(fromNode);
    if (s == nullptr) return;
    EpochState* e = epochOf_(*s, epoch);
    if (e != nullptr) replayWindowCommit(e->recv.seq, e->recv.window, seq);
}

PeerFrameCrypto::OpenResult PeerFrameCrypto::open(const uint8_t* frame, std::size_t len, uint8_t* out, std::size_t cap,
                                                  std::size_t& outLen, int32_t& fromNode, uint32_t& epoch,
                                                  bool commitWindow) noexcept {
    outLen = 0;
    if (len < kPeerFrameOverhead || frame[0] != kPeerFrameMagic) return drop_(OpenResult::NotPeerFrame);
    if (frame[1] != sender_) return drop_(OpenResult::BadSender);   // before any lookup or crypto
    fromNode = static_cast<int32_t>(getLe(frame + 2, 2));
    epoch    = static_cast<uint32_t>(getLe(frame + 4, 4));
    const uint64_t seq = getLe(frame + 8, 8);
    PeerSlot* s = slotOf_(fromNode);
    if (s == nullptr) return drop_(OpenResult::UnknownPeer);
    EpochState* e = epochOf_(*s, epoch);
    if (e == nullptr) return drop_(OpenResult::UnknownEpoch);
    RecvSender* r = &e->recv;
    if (seq == 0) return drop_(OpenResult::Replay);   // the seal path pre-increments: 0 is never a real seq
    const std::size_t ctLen = len - kPeerHeaderBytes;
    if (cap < ctLen - rune::crypto::kTagSize) return drop_(OpenResult::BadTag);
    uint8_t aad[kAadBytes];
    buildAad(aad, frame, self_);
    // Authenticate FIRST, then consult the window: a frame that does not verify is BadTag whatever its seq (a
    // tampered copy of an already seen frame is not a "replay"), and the window is committed only after the tag
    // verified - a forged seq never poisons it.  The cost is one decrypt for a replayed frame.
    const long n = r->cipher->open(seq, aad, kAadBytes, frame + kPeerHeaderBytes, ctLen, out);
    if (n < 0) return drop_(OpenResult::BadTag);   // `out` is unauthenticated garbage: never used
    if (!replayWindowCheck(r->seq, r->window, seq)) return drop_(OpenResult::Replay);
    if (commitWindow) replayWindowCommit(r->seq, r->window, seq);
    ++s->authFrames;
    // Issue #241: an authenticated frame under an epoch installed after our send epoch proves the peer holds it.
    if (e->installOrder > s->sendOrder) [[unlikely]] peerUsed_ = PeerEpoch{fromNode, epoch};
    outLen = static_cast<std::size_t>(n);
    return OpenResult::Ok;
}

} // namespace rune::peerlink
