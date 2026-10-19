// PeerLinkKeys.cpp — deriveLinkKeys (authentication plan D33).
#include <rune/peerlink/PeerLinkKeys.h>

#include <cstring>
#include <string>

namespace rune::peerlink {

namespace {
bool deriveOne(const rune::crypto::ClusterKeyMaterial& key, std::span<const uint8_t, 32> a, std::span<const uint8_t, 32> b,
               int32_t from, int32_t to, std::array<uint8_t, 32>& out) {
    uint8_t salt[64];
    std::memcpy(salt, a.data(), 32);
    std::memcpy(salt + 32, b.data(), 32);
    const std::string info = "rune-link|" + std::to_string(from) + "->" + std::to_string(to);
    return rune::crypto::hkdf(rune::crypto::Digest::Sha512, {key.master.data(), key.master.size()}, {salt, sizeof salt},
                              {reinterpret_cast<const uint8_t*>(info.data()), info.size()}, {out.data(), out.size()});
}
} // namespace

std::optional<LinkKeys> deriveLinkKeys(const rune::crypto::ClusterKeyMaterial& key, int32_t self, int32_t peer,
                                       bool /*selfIsInitiator*/, std::span<const uint8_t, 32> nonceA,
                                       std::span<const uint8_t, 32> nonceB, uint32_t epoch) {
    LinkKeys k;
    k.epoch = epoch;
    if (!deriveOne(key, nonceA, nonceB, self, peer, k.sendMaster)) return std::nullopt;
    if (!deriveOne(key, nonceA, nonceB, peer, self, k.recvMaster)) return std::nullopt;
    return k;
}

} // namespace rune::peerlink
