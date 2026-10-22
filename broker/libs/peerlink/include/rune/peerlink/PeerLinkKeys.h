#pragma once
// rune/peerlink/PeerLinkKeys.h — the directional link keys of an established peer link (D33).
//
//   v2 (issue #242, forward secrecy):
//   send = HKDF(SHA-512, ikm = master || X25519 shared secret, salt = nonceA || nonceB, info = "rune-link2|<self>-><peer>")
//   v1 (a legacy initiator's handshake, no shared secret):
//   send = HKDF(SHA-512, ikm = the cluster master, salt = nonceA || nonceB, info = "rune-link|<self>-><peer>")
//   recv = the mirror label ("...|<peer>-><self>")
// so one link yields one key per direction, bound to both handshake nonces (a replayed handshake yields other keys).
// The ephemeral X25519 secret is what a recording of the link plus a later-stolen cluster key cannot reproduce.
// Each SENDING transport derives its own frame key from its direction's master per sender id (PeerFrameCrypto).
#include <rune/crypto/Kdf.h>

#include <array>
#include <cstdint>
#include <optional>
#include <span>

namespace rune::peerlink {

/// How a transport installs a link's keys (issue #241).  ReceiveOnly: the epoch opens frames but this side keeps
/// SEALING under its current send epoch - the responder of a re-handshake installs the new epoch so until the initiator
/// proved (PeerConfirm, or an authenticated frame under it) that it holds the epoch too.  SendAndReceive: the epoch
/// also becomes the send epoch (a first link, a restarted initiator, the initiator itself, the promotion).
enum class LinkInstall : uint8_t { ReceiveOnly = 0, SendAndReceive = 1 };

struct LinkKeys {
    uint32_t                epoch = 0;
    std::array<uint8_t, 32> sendMaster{}, recvMaster{};
    LinkInstall             install = LinkInstall::SendAndReceive;
    bool operator==(const LinkKeys&) const = default;
};

/// `selfIsInitiator` is accepted for symmetry with the handshake's call sites: the labels are directional by node id,
/// so both sides derive the mirror pair from the same (nonceA, nonceB) with no role-dependent branch.
/// `dhShared`: the X25519 shared secret of a v2 handshake; nullopt = a v1 handshake (no forward secrecy), whose keys
/// are byte-identical to the pre-#242 derivation.
[[nodiscard]] std::optional<LinkKeys> deriveLinkKeys(const rune::crypto::ClusterKeyMaterial& key, int32_t self,
                                                     int32_t peer, bool selfIsInitiator,
                                                     std::span<const uint8_t, 32> nonceA,
                                                     std::span<const uint8_t, 32> nonceB, uint32_t epoch,
                                                     std::optional<std::span<const uint8_t, 32>> dhShared);

} // namespace rune::peerlink
