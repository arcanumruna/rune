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
//   Confirm{epoch, confirm}             ------->    (issue #241) promote the receive-only epoch
//
//   macB = HMAC-SHA-512(master, "rune-peer-v1|B" || from=B || to=A || nonceA || nonceB || incA || incB || keyId)
//   macA = HMAC-SHA-512(master, "rune-peer-v1|A" || from=A || to=B || nonceB || nonceA || incB || incA || keyId)
//   Accept.confirm  = HMAC(master, "rune-peer-v1|C" || from=B || to=A || nonceA || nonceB || epoch)   (32 bytes)
//   Confirm.confirm = HMAC(master, "rune-peer-v1|D" || from=A || to=B || nonceA || nonceB || epoch)   (32 bytes)
// The distinct labels make a reflected MAC useless; both nonces make a replayed Hello or Proof useless.
//
// Issue #241: the responder of a re-handshake with the SAME initiator incarnation installs the new epoch RECEIVE-ONLY
// (Out.established->install) and keeps sealing under the old one until the initiator proves it holds the new epoch:
// its Confirm, or (a legacy initiator) an authenticated frame under it (onPeerUsedEpoch).  The unconfirmed Accept is
// re-sent every kRetryNs and an Established initiator answers it with its Confirm again.
//
// Rule 3: the link is ONE variant, each alternative holding exactly what that state needs.
#include <rune/peerlink/ClusterKeys.h>
#include <rune/peerlink/PeerLinkKeys.h>
#include <rune/peerlink/PeerLinkMessages.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace rune::peerlink {

struct LinkIdle      { int64_t retryAtNs = 0; };
/// Issue #241 - the receive-first install of a re-handshake:
///   responder: `unconfirmedAccept` is the Accept of an epoch installed RECEIVE-ONLY, re-sent every kRetryNs until the
///              initiator's Confirm (MAC `wantConfirm`) or an authenticated frame under the epoch promotes it;
///   initiator: `confirm` is the Confirm it sent for this epoch (re-sent when the same Accept comes again) and
///              `acceptConfirm` the Accept MAC it verified (a re-sent Accept is recognised by a constant-time compare).
struct Established {
    LinkKeys keys; uint64_t peerIncarnation = 0; int64_t sinceNs = 0; int64_t lastAuthNs = 0;
    std::optional<PeerAccept>  unconfirmedAccept;          // responder awaiting the Confirm
    int64_t                    acceptSentNs = 0;
    std::array<uint8_t, 32>    wantConfirm{};
    std::optional<PeerConfirm> confirm;                    // initiator: kept to be re-sent
    std::array<uint8_t, 32>    acceptConfirm{};
};
/// `prior` is the live link while a rekey is in flight: it keeps sealing and opening until the exchange completes.
struct HelloSent     { std::array<uint8_t, 32> nonceA{}; uint64_t keyId = 0; int64_t sentNs = 0; int64_t startNs = 0;
                       std::optional<Established> prior; };
/// Initiator: Challenge verified, Proof sent, awaiting Accept.  Responder: Challenge sent, awaiting Proof.  `prior`
/// is the responder's live link while a re-handshake is in flight (restored if the Proof never comes).
struct ChallengeSent {
    std::array<uint8_t, 32> nonceA{}, nonceB{}; uint64_t keyId = 0; uint64_t peerIncarnation = 0; int64_t sentNs = 0;
    std::optional<Established> prior;
};
/// A Challenge the RESPONDER has sent and not yet seen proven.  Fixed table beside the link state: an unauthenticated
/// Hello never changes the state (an Established responder stays Established).
struct PendingChallenge {
    bool used = false;
    std::array<uint8_t, 32> nonceA{}, nonceB{}; uint64_t keyId = 0; uint64_t peerIncarnation = 0; int64_t sentNs = 0;
    std::array<uint8_t, 64> wantMacA{};   // the Proof we expect, computed once when the Challenge is issued
};
using LinkState = std::variant<LinkIdle, HelloSent, ChallengeSent, Established>;

class PeerHandshake {
public:
    static constexpr int64_t kRetryNs        = 200'000'000;     // Hello retry / refusal retry / NoLink poke
    static constexpr int64_t kAwaitNs        = 1'000'000'000;   // a Challenge / Proof awaiting its answer
    static constexpr std::size_t kPending     = 8;               // responder: challenges awaiting their Proof
    static constexpr int64_t kHintRekeyGapNs  = 30'000'000'000;  // unauthenticated hints rekey a link at most this often
    static constexpr int64_t kSilenceNs      = 1'000'000'000;   // no authenticated frame this long: an unauthenticated hint may rekey

    PeerHandshake(int32_t self, int32_t peer, uint64_t selfIncarnation, const ClusterKeys& keys);

    /// Fixed buffers (rule 4): a handshake frame and a log line never touch the heap.
    struct FrameBuf {
        static constexpr std::size_t kCap = 256;
        std::array<uint8_t, kCap> bytes{};
        std::size_t               len = 0;
        [[nodiscard]] const uint8_t* data() const noexcept { return bytes.data(); }
        [[nodiscard]] std::size_t size() const noexcept { return len; }
        [[nodiscard]] bool operator==(const FrameBuf& o) const noexcept { return len == o.len && std::memcmp(bytes.data(), o.bytes.data(), len) == 0; }
    };
    struct LogLine {
        static constexpr std::size_t kCap = 224;
        std::array<char, kCap> text{};
        std::size_t            len = 0;
        [[nodiscard]] std::string_view view() const noexcept { return {text.data(), len}; }
        [[nodiscard]] std::string str() const { return std::string(text.data(), len); }
        [[nodiscard]] std::size_t find(const char* needle) const { return view().find(needle); }
    };
    struct Out {
        std::optional<FrameBuf>    frame;
        std::optional<LinkKeys>    established;   // set on Established (also on a new epoch)
        std::optional<LogLine>     log;
        bool                       logIsError = false;   // a refusal: the agent counts it (counter 234)
    };

    /// Initiator (self < peer): Hello from LinkIdle / Refused when due; the same Hello again while awaiting a
    /// Challenge.  Responder: the NoLink poke while it holds no link; the expiry of an unanswered Challenge.
    Out tick(int64_t nowNs);
    Out onHello(const PeerHello&, int64_t nowNs);          // responder
    Out onChallenge(const PeerChallenge&, int64_t nowNs);  // initiator
    Out onProof(const PeerProof&, int64_t nowNs);          // responder
    Out onAccept(const PeerAccept&, int64_t nowNs);        // initiator
    /// Responder (issue #241): the initiator proved it installed the receive-only epoch - promote it (SendAndReceive).
    /// A forged, stale or duplicate Confirm changes nothing.
    Out onConfirm(const PeerConfirm&, int64_t nowNs);
    /// Responder (issue #241): an AUTHENTICATED frame under `epoch` arrived (a legacy initiator sends no Confirm):
    /// the same promotion as a valid Confirm when `epoch` is the unconfirmed one; otherwise nothing.
    Out onPeerUsedEpoch(uint32_t epoch, int64_t nowNs);
    /// Only logs, except a NoLink poke (see rekey).  A Refuse carries no MAC, so it never changes the link state.
    Out onRefuse(const PeerRefuse&, int64_t nowNs);
    /// A frame named an epoch we do not hold.  UNAUTHENTICATED: it changes nothing unless the peer has been silent
    /// (no authenticated frame) for kSilenceNs and the exchange rate limit allows; then (initiator, Established) a
    /// rekey Hello goes out while the old keys stay live.
    Out onUnknownEpoch(uint32_t epoch, int64_t nowNs);
    /// An authenticated frame from this peer arrived on a sealed transport.
    void onAuthenticated(int64_t nowNs) noexcept;

    /// TEST ONLY (issue #241): an Established initiator starts a same-incarnation rekey now, bypassing the silence and
    /// hint-rate gates (the exchange itself is the real one).  {} when not an Established initiator.
    Out rekeyForTest(int64_t nowNs);

    /// Test-visible: HMAC transcript computations performed so far (pins that a forged Proof costs none).
    [[nodiscard]] uint64_t macComputations() const noexcept { return macOps_; }
    [[nodiscard]] const LinkState& state() const noexcept { return state_; }
    [[nodiscard]] bool initiator() const noexcept { return self_ < peer_; }
    [[nodiscard]] int32_t peer() const noexcept { return peer_; }

private:
    bool mac_(const ClusterKey& key, bool isB, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
              const std::array<uint8_t, 32>& nonceB, uint64_t incA, uint64_t incB, uint64_t keyId, std::array<uint8_t, 64>& out);
    Out refuseOut_(RefuseReason r, uint64_t keyId);
    Out rekey_(int64_t nowNs);
    Out startRekey_(const Established& live, int64_t nowNs);
    void restorePrior_(std::optional<Established> prior, int64_t nowNs) noexcept;
    /// The live link: the Established state, or the `prior` a rekey in flight keeps (null: none).
    [[nodiscard]] const Established* liveLink_() const noexcept;
    /// Responder (issue #241): the unconfirmed epoch becomes the send epoch.
    Out promote_(Established& e, const char* why);

    int32_t     self_, peer_;
    uint64_t    incarnation_;
    ClusterKeys keys_;
    uint32_t    epochCounter_ = 0;      // the responder's link epochs (Accept{epoch = counter + 1})
    std::array<PendingChallenge, kPending> pending_{};
    uint64_t    macOps_ = 0;
    int64_t     lastHintRekeyNs_ = -1'000'000'000'000;
    int64_t     lastExchangeNs_ = -1'000'000'000'000;   // start of the last NEW exchange (<= 1 per kRetryNs)
    LinkState   state_{LinkIdle{0}};
};

} // namespace rune::peerlink
