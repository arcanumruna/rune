// PeerLinkKeys.cpp — deriveLinkKeys (authentication plan D33; issue #242 forward secrecy).
#include <rune/peerlink/PeerLinkKeys.h>

#include <cstring>
#include <string>

namespace rune::peerlink {

namespace {
/// v1: ikm = the master, info "rune-link|<from>-><to>" (byte-identical to the pre-#242 derivation).
/// v2: ikm = master || dhShared, info "rune-link2|<from>-><to>" - a distinct label, so no v2 key is ever a v1 key.
bool deriveOne(const rune::crypto::ClusterKeyMaterial& key, std::span<const uint8_t, 32> a, std::span<const uint8_t, 32> b,
               int32_t from, int32_t to, const std::optional<std::span<const uint8_t, 32>>& dhShared,
               std::array<uint8_t, 32>& out) {
    uint8_t salt[64];
    std::memcpy(salt, a.data(), 32);
    std::memcpy(salt + 32, b.data(), 32);
    const std::string info = std::string(dhShared ? "rune-link2|" : "rune-link|") + std::to_string(from) + "->" + std::to_string(to);
    const std::span<const uint8_t> infoBytes{reinterpret_cast<const uint8_t*>(info.data()), info.size()};
    if (!dhShared)
        return rune::crypto::hkdf(rune::crypto::Digest::Sha512, {key.master.data(), key.master.size()}, {salt, sizeof salt},
                                  infoBytes, {out.data(), out.size()});
    uint8_t ikm[sizeof key.master + 32];
    std::memcpy(ikm, key.master.data(), key.master.size());
    std::memcpy(ikm + key.master.size(), dhShared->data(), 32);
    const bool ok = rune::crypto::hkdf(rune::crypto::Digest::Sha512, {ikm, sizeof ikm}, {salt, sizeof salt}, infoBytes,
                                       {out.data(), out.size()});
    rune::crypto::secureZero({ikm, sizeof ikm});
    return ok;
}
} // namespace

std::optional<LinkKeys> deriveLinkKeys(const rune::crypto::ClusterKeyMaterial& key, int32_t self, int32_t peer,
                                       bool /*selfIsInitiator*/, std::span<const uint8_t, 32> nonceA,
                                       std::span<const uint8_t, 32> nonceB, uint32_t epoch,
                                       std::optional<std::span<const uint8_t, 32>> dhShared) {
    LinkKeys k;
    k.epoch = epoch;
    if (!deriveOne(key, nonceA, nonceB, self, peer, dhShared, k.sendMaster)) return std::nullopt;
    if (!deriveOne(key, nonceA, nonceB, peer, self, dhShared, k.recvMaster)) return std::nullopt;
    return k;
}

} // namespace rune::peerlink
