#pragma once
// rune/peerlink/PeerHandshake.h — one inter-broker handshake per peer (authentication plan Task 15, D33, D35).
//
// PURE: the clock is injected, frames leave through the returned Out, nothing here touches Aeron or a thread.  One
// instance per peer, owned by the control thread's PeerLinkAgent.  The lower node id initiates:
//
//   A (initiator)                                   B (responder)
//   Hello{keyId, incA, nonceA}          ------->    holds keyId (current or next)? else Refuse{NoCommonKey}
//                                       <-------    Challenge{keyId, incB, nonceB, macB}
//   verify macB; Proof{macA}            ------->    verify macA; epoch = counter + 1
//                                       <-------    Accept{epoch, confirm}        (both derive LinkKeys)
//
//   macB = HMAC-SHA-512(master, "rune-peer-v1|B" || from=B || to=A || nonceA || nonceB || incA || incB || keyId)
//   macA = HMAC-SHA-512(master, "rune-peer-v1|A" || from=A || to=B || nonceB || nonceA || incB || incA || keyId)
// The distinct labels make a reflected MAC useless; both nonces make a replayed Hello or Proof useless.
//
// Rule 3: the link is ONE variant, each alternative holding exactly what that state needs.
#include <rune/peerlink/ClusterKeys.h>
#include <rune/peerlink/PeerLinkKeys.h>
#include <rune/peerlink/PeerLinkMessages.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace rune::peerlink {

struct LinkIdle      { int64_t retryAtNs = 0; };
struct HelloSent     { std::array<uint8_t, 32> nonceA{}; uint64_t keyId = 0; int64_t sentNs = 0; };
struct Established   { LinkKeys keys; uint64_t peerIncarnation = 0; int64_t sinceNs = 0; };
/// Initiator: Challenge verified, Proof sent, awaiting Accept.  Responder: Challenge sent, awaiting Proof.  `prior`
/// is the responder's live link while a re-handshake is in flight (restored if the Proof never comes).
struct ChallengeSent {
    std::array<uint8_t, 32> nonceA{}, nonceB{}; uint64_t keyId = 0; uint64_t peerIncarnation = 0; int64_t sentNs = 0;
    std::optional<Established> prior;
};
struct Refused       { RefuseReason reason{}; int64_t retryAtNs = 0; };
using LinkState = std::variant<LinkIdle, HelloSent, ChallengeSent, Established, Refused>;

class PeerHandshake {
public:
    static constexpr int64_t kRetryNs        = 200'000'000;     // Hello retry / refusal retry / NoLink poke
    static constexpr int64_t kAwaitNs        = 1'000'000'000;   // a Challenge / Proof awaiting its answer
    static constexpr int64_t kNoLinkGuardNs  = 1'000'000'000;   // a NoLink poke right after Established is stale

    PeerHandshake(int32_t self, int32_t peer, uint64_t selfIncarnation, const ClusterKeys& keys);

    struct Out {
        std::optional<std::vector<uint8_t>> frame;
        std::optional<LinkKeys>             established;   // set on Established (also on a new epoch)
        std::optional<std::string>          log;
        bool                                logIsError = false;   // a refusal: the agent counts it (counter 234)
    };

    /// Initiator (self < peer): Hello from LinkIdle / Refused when due; the same Hello again while awaiting a
    /// Challenge.  Responder: the NoLink poke while it holds no link; the expiry of an unanswered Challenge.
    Out tick(int64_t nowNs);
    Out onHello(const PeerHello&, int64_t nowNs);          // responder
    Out onChallenge(const PeerChallenge&, int64_t nowNs);  // initiator
    Out onProof(const PeerProof&, int64_t nowNs);          // responder
    Out onAccept(const PeerAccept&, int64_t nowNs);        // initiator
    Out onRefuse(const PeerRefuse&, int64_t nowNs);
    /// A frame named an epoch we do not hold -> re-handshake (initiator, Established only; otherwise nothing).
    void onUnknownEpoch(uint32_t epoch, int64_t nowNs);

    [[nodiscard]] const LinkState& state() const noexcept { return state_; }
    [[nodiscard]] bool initiator() const noexcept { return self_ < peer_; }
    [[nodiscard]] int32_t peer() const noexcept { return peer_; }

private:
    Out refuseOut_(RefuseReason r, uint64_t keyId);
    [[nodiscard]] std::string refusedLocally_(RefuseReason r, uint64_t keyId) const;
    [[nodiscard]] std::string refusedByPeer_(RefuseReason r) const;
    [[nodiscard]] static std::string hex_(uint64_t v);

    int32_t     self_, peer_;
    uint64_t    incarnation_;
    ClusterKeys keys_;
    uint32_t    epochCounter_ = 0;      // the responder's link epochs (Accept{epoch = counter + 1})
    uint64_t    peerIncHighWater_ = 0;  // the highest peer incarnation ever ESTABLISHED (never set by an unproven Hello)
    LinkState   state_{LinkIdle{0}};
};

} // namespace rune::peerlink
