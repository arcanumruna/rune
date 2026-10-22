#pragma once
// rune/peerlink/PeerHandshake.h — one inter-broker handshake per peer (authentication plan Task 15, D33, D35).
//
// PURE: the clock is injected, frames leave through the returned Out, nothing here touches Aeron or a thread.  One
// instance per peer, owned by the control thread's PeerLinkAgent.  The lower node id initiates:
//
//   A (initiator)                                   B (responder)
//   HelloV2{keyId, incA, nonceA, shareA} ------>    holds keyId (current or next)? else Refuse{NoCommonKey}
//                                       <-------    ChallengeV2{keyId, incB, nonceB, macB2, shareB}
//   verify macB2;                                   (issue #18: nonceB / shareB come from a cookie, nothing stored)
//   ProofV2{keyId, incA, nonceA, shareA,
//           nonceB, macA2}              ------->    re-derive the cookie; verify macA2; epoch = counter + 1
//                                       <-------    Accept{epoch, confirm}        (both derive LinkKeys)
//   Confirm{epoch, confirm}             ------->    (issue #241) promote the receive-only epoch
//
//   macB2 = HMAC-SHA-512(master, "rune-peer-v2|B" || from=B || to=A || nonceA || nonceB || shareA || shareB || incA || incB || keyId)
//   macA2 = HMAC-SHA-512(master, "rune-peer-v2|A" || from=A || to=B || nonceB || nonceA || shareB || shareA || incB || incA || keyId)
//   Accept.confirm  = HMAC(master, "rune-peer-v1|C" || from=B || to=A || nonceA || nonceB || epoch)   (32 bytes)
//   Confirm.confirm = HMAC(master, "rune-peer-v1|D" || from=A || to=B || nonceA || nonceB || epoch)   (32 bytes)
// The distinct labels make a reflected MAC useless; both nonces make a replayed Hello or Proof useless.
//
// Issue #242, forward secrecy: shareA / shareB are X25519 ephemeral public keys (one keypair per handshake attempt on
// each side; a repeated Hello / Challenge re-sends the SAME share).  The link keys are HKDF over master || X25519 shared
// secret (PeerLinkKeys.h), so a recording of the link plus a cluster key stolen later does not yield them.  The MACs
// cover both shares (a share swapped in transit fails the MAC); the Accept / Confirm MACs are unchanged and bound to the
// nonces of this v2 exchange.  The ephemeral private keys and the shared secret live only in HelloSent, ChallengeSent
// and (responder) the scope of one onHelloV2 / onProofV2 call, as EphSecret, scrubbed on every exit; the responder
// computes the DH only after the ProofV2's macA matched.
//
// Issue #18, a stateless cookie for the v2 responder.  A pending-challenge table was floodable: unauthenticated
// HelloV2s evicted the real initiator's slot.  The responder now keeps NO per-Hello state for v2:
//   d = HMAC-SHA-512(cookieSecret, "rune-peer-cookie-v2|" || from(LE4) || to(LE4) || keyId(LE8) || incA(LE8) || nonceA || shareA)
//   nonceB = d[0..32), ephPriv = d[32..64) (X25519 clamps it), shareB = X25519(ephPriv, 9)
// The ProofV2 echoes keyId, incA, nonceA, shareA and nonceB; the responder re-derives d, checks nonceB (constant time)
// and only then computes macA2 and the DH.  So a forged ProofV2 costs one or two cookie HMACs and NO transcript MAC, and
// the same HelloV2 again gets the same Challenge while the secret lives.
//   - two secrets, cookieCur_ / cookiePrev_, rotated on a kAwaitNs grid of the injected clock (tick, onHelloV2,
//     onProofV2): a Challenge stays answerable for at least kAwaitNs and at most 2 x kAwaitNs;
//   - replay: every successful establishment (v1 or v2) drops BOTH secrets and draws a fresh one, so every outstanding
//     Challenge - the one just consumed included - is void, as the pending table is cleared on success;
//   - forward secrecy: a cookie secret lives at most 2 x kAwaitNs and is scrubbed when it rotates out.  The ephemeral
//     private key is derivable from it only within that window, the same exposure as the pending slot it replaces.
// The v1 path (a legacy initiator) still uses the pending-challenge table, so a legacy pair stays floodable (#553).
//
// Version skew (no runtime downgrade path):
//   - a new RESPONDER still answers a v1 Hello (a legacy initiator) with a v1 Challenge and v1 keys (no forward secrecy):
//       macB = HMAC-SHA-512(master, "rune-peer-v1|B" || from=B || to=A || nonceA || nonceB || incA || incB || keyId)
//       macA = HMAC-SHA-512(master, "rune-peer-v1|A" || from=A || to=B || nonceB || nonceA || incB || incA || keyId)
//   - a new INITIATOR only ever sends HelloV2 and refuses a v1 Challenge (logged, rate-limited): falling back would let
//     an attacker who drops HelloV2 frames strip forward secrecy.  A legacy responder ignores tag 7, so that pair does
//     not link until the responder is upgraded.
//   - rolling upgrade: upgrade nodes in DESCENDING node id.  The lower id initiates, so every upgraded initiator then
//     faces an upgraded responder, and every legacy initiator a responder that still speaks v1.
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
/// Issue #242: an ephemeral X25519 secret (a private key or a DH shared secret).  Scrubbed when its holder dies, so every
/// state exit - restorePrior_, a timeout, a supersede, the handshake's destructor - wipes it with the variant
/// alternative that held it.  A copy-assignment overwrites the old bytes.  Copyable (PeerHandshake stays movable).
struct EphSecret {
    std::array<uint8_t, 32> bytes{};
    EphSecret() = default;
    EphSecret(const EphSecret&) = default;
    EphSecret& operator=(const EphSecret&) = default;
    ~EphSecret() { scrub(); }
    void scrub() noexcept { rune::crypto::secureZero({bytes.data(), bytes.size()}); }
};
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
/// `ephPriv` / `ephPub`: this attempt's X25519 keypair (issue #242); a retry re-sends the same Hello (same share).
struct HelloSent     { std::array<uint8_t, 32> nonceA{}; uint64_t keyId = 0; int64_t sentNs = 0; int64_t startNs = 0;
                       std::optional<Established> prior; EphSecret ephPriv; std::array<uint8_t, 32> ephPub{}; };
/// Initiator: Challenge verified, Proof sent, awaiting Accept.  Responder: Challenge sent, awaiting Proof.  `prior`
/// is the responder's live link while a re-handshake is in flight (restored if the Proof never comes).
/// `dhShared` (issue #242): the X25519 shared secret the Accept's link keys are derived with; nullopt = v1.
struct ChallengeSent {
    std::array<uint8_t, 32> nonceA{}, nonceB{}; uint64_t keyId = 0; uint64_t peerIncarnation = 0; int64_t sentNs = 0;
    std::optional<Established> prior;
    std::optional<EphSecret> dhShared;
};
/// A Challenge the RESPONDER has sent and not yet seen proven.  Fixed table beside the link state: an unauthenticated
/// Hello never changes the state (an Established responder stays Established).  Issue #18: only v1 Challenges are
/// stored; a v2 Challenge is built in a temporary PendingChallenge from the cookie and never kept.
struct PendingChallenge {
    bool used = false;
    std::array<uint8_t, 32> nonceA{}, nonceB{}; uint64_t keyId = 0; uint64_t peerIncarnation = 0; int64_t sentNs = 0;
    std::array<uint8_t, 64> wantMacA{};   // the Proof we expect, computed once when the Challenge is issued
    bool v2 = false;                      // issue #242: a HelloV2 - shareA / shareB / ephPriv are set
    std::array<uint8_t, 32> shareA{}, shareB{};
    EphSecret ephPriv;                    // v2 temporary only: the cookie-derived key, scrubbed with the temporary
};
using LinkState = std::variant<LinkIdle, HelloSent, ChallengeSent, Established>;

class PeerHandshake {
public:
    static constexpr int64_t kRetryNs        = 200'000'000;     // Hello retry / refusal retry / NoLink poke
    static constexpr int64_t kAwaitNs        = 1'000'000'000;   // a Challenge / Proof awaiting its answer
    static constexpr std::size_t kPending     = 8;               // responder: challenges awaiting their Proof
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
    Out onChallenge(const PeerChallenge&, int64_t nowNs);  // initiator: always refused (downgrade, issue #242)
    Out onHelloV2(const PeerHelloV2&, int64_t nowNs);          // responder (issue #242)
    Out onChallengeV2(const PeerChallengeV2&, int64_t nowNs);  // initiator (issue #242)
    Out onProof(const PeerProof&, int64_t nowNs);          // responder: a legacy (v1) initiator
    Out onProofV2(const PeerProofV2&, int64_t nowNs);      // responder (issue #18): checked against the cookie
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
    /// (no authenticated frame) for kSilenceNs and the exchange rate limit (kRetryNs) allows; then (initiator,
    /// Established) a rekey Hello goes out while the old keys stay live.  Issue #17: a live peer is never silent - every
    /// broker seals a keepalive to each idle peer every 250 ms on the consensus plane (UdpMessageSink) - so a forged
    /// hint cannot rekey a live link, and there is no further cap: a restarted peer relinks after kSilenceNs.
    Out onUnknownEpoch(uint32_t epoch, int64_t nowNs);
    /// An authenticated frame from this peer arrived on a sealed transport.
    void onAuthenticated(int64_t nowNs) noexcept;

    /// TEST ONLY (issue #241): an Established initiator starts a same-incarnation rekey now, bypassing the silence and
    /// retry gates (the exchange itself is the real one).  {} when not an Established initiator.
    Out rekeyForTest(int64_t nowNs);

    /// Test-visible: HMAC transcript computations performed so far (pins that a forged Proof costs none).
    [[nodiscard]] uint64_t macComputations() const noexcept { return macOps_; }
    [[nodiscard]] const LinkState& state() const noexcept { return state_; }
    [[nodiscard]] bool initiator() const noexcept { return self_ < peer_; }
    [[nodiscard]] int32_t peer() const noexcept { return peer_; }

private:
    /// v2 (issue #242) when both shares are given: the "rune-peer-v2|*" transcript, which covers them.
    bool mac_(const ClusterKey& key, bool isB, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
              const std::array<uint8_t, 32>& nonceB, uint64_t incA, uint64_t incB, uint64_t keyId, std::array<uint8_t, 64>& out,
              const std::array<uint8_t, 32>* shareA = nullptr, const std::array<uint8_t, 32>* shareB = nullptr);
    /// Issue #242: a fresh nonce and X25519 keypair for a new Hello; false when no randomness / key is available.
    [[nodiscard]] bool freshHello_(HelloSent& hs) const noexcept;
    [[nodiscard]] PeerHelloV2 helloFrame_(const HelloSent& hs) const noexcept;
    /// Responder: a v1 Hello - the pending-slot table.
    Out answerHello_(int32_t from, int32_t to, uint64_t keyId, uint64_t incarnation, const std::array<uint8_t, 32>& nonceA,
                     int64_t nowNs);
    /// Responder (issue #18): a v2 Hello - answered from the cookie, nothing stored.
    Out answerHelloV2_(const PeerHelloV2& h, int64_t nowNs);
    /// Issue #18: rotate the cookie secrets on the kAwaitNs grid (fail closed: no randomness, no cookieCur_).
    void rotateCookie_(int64_t nowNs) noexcept;
    /// Issue #18: drop both cookie secrets and draw a fresh one (after an establishment: every Challenge is void).
    void freshCookie_(int64_t nowNs) noexcept;
    /// Issue #18: nonceB and the ephemeral private key of a v2 Challenge, from cookie secret `k` (NOT a transcript MAC:
    /// not counted in macOps_).
    [[nodiscard]] static bool cookie_(const EphSecret& k, int32_t from, int32_t to, uint64_t keyId, uint64_t incA,
                                      const std::array<uint8_t, 32>& nonceA, const std::array<uint8_t, 32>& shareA,
                                      std::array<uint8_t, 32>& nonceB, EphSecret& ephPriv) noexcept;
    /// The responder's establishment once a Proof (v1) or ProofV2 matched: the epoch, the link keys (`shared` = the
    /// X25519 secret, null = v1), the Accept, issue #241's receive-first decision, and the reset of every outstanding
    /// Challenge (the v1 pending table and, issue #18, both cookie secrets).
    Out establishAsResponder_(const ClusterKey& key, const std::array<uint8_t, 32>& nonceA, const std::array<uint8_t, 32>& nonceB,
                              uint64_t peerIncarnation, uint64_t keyId, const EphSecret* shared, int64_t nowNs);
    Out challengeOut_(const ClusterKey& key, const PendingChallenge& c);
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
    std::array<PendingChallenge, kPending> pending_{};   // v1 only (issue #18)
    std::optional<EphSecret> cookieCur_, cookiePrev_;     // issue #18: the v2 cookie secrets
    int64_t     cookieRotatedNs_ = 0;                     // the grid point cookieCur_ belongs to
    uint64_t    macOps_ = 0;
    int64_t     lastExchangeNs_ = -1'000'000'000'000;   // start of the last NEW exchange (<= 1 per kRetryNs)
    LinkState   state_{LinkIdle{0}};
};

} // namespace rune::peerlink
