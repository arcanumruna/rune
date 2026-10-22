#pragma once
// rune/peerlink/PeerFrameCrypto.h — AEAD on every inter-broker frame (authentication plan Task 16, D33, D34).
//
// Frame: [magic 0xB7][senderId u8][fromNode u16][linkEpoch u32][seq u64][ciphertext][tag 16]   (32 bytes overhead)
// AAD  : the 16-byte header || toNode u16.  Frames are sealed WHOLE before Aeron fragmentation and opened after
// reassembly.  Cipher: AES-256-GCM on every broker (never "the local best": both ends must agree).
//
// ONE PeerFrameCrypto per transport instance, owned by that transport's thread (consensus sink / ingress and the
// legacy replication transport on the control thread, each worker's plane, the admin plane): shared-nothing, no
// locks.  The owner feeds it the link keys it received over a ring (installLink).  Each SENDER (D33 sender ids:
// consensus 0, legacy replication 1, admin plane 2, worker w -> 3 + w) derives its own frame key from the link's
// per-direction master, so no two senders ever share a (key, nonce) space; the receiver keeps a 64-wide replay
// window per (peer, epoch, senderId).
//
// Hot path: seal / open never allocate.  The cipher contexts are built in installLink (cold).
//
// Epochs (issue #241): each peer holds up to kEpochSlots link epochs, each with BOTH its ciphers, and ONE of them is
// the send epoch.  A re-handshaked epoch is installed RECEIVE-ONLY first (the responder keeps sealing under the old
// epoch until the initiator proved it holds the new one) and promoted by a second install of the same epoch.  An older
// epoch is never dropped on a timer or on an event: one instance receives one peer sender over several Aeron streams
// that are not ordered against each other, so "a newer epoch was seen" never proves no older frame is in flight.  A
// slot is evicted only under slot pressure (the oldest one that is not the send epoch), which the handshake's
// exchange rate limit makes rare.
#include <rune/crypto/Aead.h>
#include <rune/peerlink/PeerLinkKeys.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>

namespace rune::peerlink {

inline constexpr std::size_t kPeerHeaderBytes   = 16;
inline constexpr std::size_t kPeerFrameOverhead = kPeerHeaderBytes + rune::crypto::kTagSize;   // 32
inline constexpr uint8_t     kPeerFrameMagic    = 0xB7;

/// The steady clock in ns (callers pass it to installLink's unused `nowNs`; kept for its call sites).
[[nodiscard]] inline int64_t steadyNowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// A transport built PeerPlain sends and receives exactly the bytes it always did (InProcessUnauthenticated /
/// DevDisabled / single node).
struct PeerPlain {};

class PeerFrameCrypto {
public:
    static constexpr std::size_t kMaxPeers       = 16;   // == FollowerSlots::kMaxReplicas
    static constexpr std::size_t kEpochSlots     = 3;    // epochs held per peer (issue #241): no timer, slot pressure only

    PeerFrameCrypto(int32_t selfNode, uint8_t senderId);
    PeerFrameCrypto(PeerFrameCrypto&&) noexcept            = default;
    PeerFrameCrypto& operator=(PeerFrameCrypto&&) noexcept = default;
    PeerFrameCrypto(const PeerFrameCrypto&)                = delete;
    PeerFrameCrypto& operator=(const PeerFrameCrypto&)     = delete;

    /// COLD.  Install `keys.epoch` of the link to `peer` (issue #241):
    ///   - a slot already holding that epoch with the same masters keeps its windows and sequence numbers (the
    ///     promotion of a receive-only epoch);
    ///   - the same epoch number with other masters (a restarted responder reused it) replaces that slot fresh;
    ///   - otherwise a free slot, else the slot with the oldest install that is not the send epoch.
    /// keys.install == SendAndReceive then makes it the send epoch; ReceiveOnly leaves the send epoch alone.
    /// `nowNs` is unused (epochs never expire on a timer); kept for its many call sites.  False: the table is full, a
    /// node id does not fit the u16 header field, or a cipher could not be built - the link is then not usable
    /// (frames to `peer` drop, counted).
    bool installLink(int32_t peer, const LinkKeys& keys, int64_t nowNs);

    /// A send epoch is set for `peer`.
    [[nodiscard]] bool hasLink(int32_t peer) const noexcept;
    /// The SEND epoch of the link to `peer` (0 = none).
    [[nodiscard]] uint32_t currentEpoch(int32_t peer) const noexcept;
    /// Test-visible: `epoch` of `peer` is held in a slot (it opens frames).
    [[nodiscard]] bool holdsEpoch(int32_t peer, uint32_t epoch) const noexcept;
    /// The (peer, epoch) of the last authenticated frame under an epoch installed AFTER the peer's send epoch - the peer
    /// proved it holds an epoch we hold receive-only.  Consumed once.  The instance never promotes itself: the owner
    /// hands it to the PeerLinkAgent, whose promotion comes back through installLink(SendAndReceive).
    [[nodiscard]] bool takePeerUsedEpoch(int32_t& peer, uint32_t& epoch) noexcept;

    /// Seal `plain` for `peer` into `out` (cap >= len + kPeerFrameOverhead).  Returns the frame length, or 0 = no
    /// established link / too small / cipher error: the caller sends NOTHING (the drop is counted here).
    [[nodiscard]] std::size_t seal(int32_t peer, const uint8_t* plain, std::size_t len, uint8_t* out,
                                   std::size_t cap) noexcept;
    /// In place: the caller encoded its plaintext at frame + kPeerHeaderBytes; returns the sealed length
    /// (plainLen + kPeerFrameOverhead) or 0.
    [[nodiscard]] std::size_t sealInPlace(int32_t peer, uint8_t* frame, std::size_t plainLen, std::size_t cap) noexcept;

    /// BadSender: the sender id is not this receiver's role (consensus 0, legacy 1, admin 2, worker w -> 3+w): refused
    /// before any crypto, so a bogus id costs a compare.
    enum class OpenResult : uint8_t { Ok, NotPeerFrame, UnknownPeer, UnknownEpoch, BadTag, Replay, BadSender };
    /// Open into `out` (never in place: AES-GCM writes unauthenticated plaintext before it checks the tag).  Never
    /// dispatches anything that did not verify.  `fromNode` / `epoch` are reported from the header as soon as it
    /// parsed (UnknownEpoch included, so the control thread can re-handshake).  The epoch is looked up among the
    /// peer's slots (a scan of kEpochSlots).
    [[nodiscard]] OpenResult open(const uint8_t* frame, std::size_t len, uint8_t* out, std::size_t cap,
                                  std::size_t& outLen, int32_t& fromNode, uint32_t& epoch,
                                  bool commitWindow = true) noexcept;
    /// For a caller that opened with commitWindow = false: commit the replay window once the frame was DISPATCHED
    /// (not ABORTed), so a redelivery after back-pressure re-opens instead of reading as a replay.
    void commitOpened(int32_t fromNode, uint32_t epoch, uint64_t seq) noexcept;
    /// Frames from `peer` that authenticated (monotonic): the PeerLinkAgent's "the peer is alive" signal.
    [[nodiscard]] uint64_t authFrames(int32_t peer) const noexcept;

    /// A caller dropped a frame because it knows there is no link (before claiming anything): counted like seal's.
    void noteNoLink() noexcept { ++drops_; ++noLinkDrops_; }

    [[nodiscard]] uint64_t drops() const noexcept { return drops_; }
    [[nodiscard]] uint64_t dropsOf(OpenResult r) const noexcept { return openDrops_[static_cast<std::size_t>(r)]; }
    [[nodiscard]] uint64_t noLinkDrops() const noexcept { return noLinkDrops_; }
    [[nodiscard]] int32_t  selfNode() const noexcept { return self_; }
    [[nodiscard]] uint8_t  senderId() const noexcept { return sender_; }

    [[nodiscard]] static const char* name(OpenResult r) noexcept;

private:
    struct RecvSender {
        uint8_t                                  senderId = 0;
        std::optional<rune::crypto::AeadCipher>  cipher;
        uint64_t                                 seq = 0, window = 0;   // the 64-wide replay window (CryptoState logic)
    };
    struct EpochState {
        LinkKeys                                 keys;
        std::optional<rune::crypto::AeadCipher>  send;                    // THIS sender's key for this direction
        uint64_t                                 sendSeq = 0;
        RecvSender                               recv;                    // the ONE sender id this receiver's role accepts
        uint64_t                                 installOrder = 0;        // from installs_ (the eviction order)
    };
    static constexpr uint8_t kNoSend = 0xFF;
    struct PeerSlot {
        int32_t                                             peer = -1;
        std::array<std::optional<EpochState>, kEpochSlots>  epochs;
        uint8_t                                             sendIdx = kNoSend;   // the send epoch's slot
        uint64_t                                            sendOrder = 0;       // its installOrder (0 = none)
        uint64_t                                            authFrames = 0;
    };
    struct PeerEpoch { int32_t peer = -1; uint32_t epoch = 0; };

    [[nodiscard]] const PeerSlot* slotOf_(int32_t peer) const noexcept;
    [[nodiscard]] PeerSlot* slotOf_(int32_t peer) noexcept;
    [[nodiscard]] static EpochState* epochOf_(PeerSlot& s, uint32_t epoch) noexcept;
    [[nodiscard]] static const EpochState* sendOf_(const PeerSlot& s) noexcept {
        return s.sendIdx == kNoSend ? nullptr : &*s.epochs[s.sendIdx];
    }
    [[nodiscard]] std::size_t sealAt_(int32_t peer, const uint8_t* plain, std::size_t len, uint8_t* frame,
                                      std::size_t cap, bool inPlace) noexcept;
    OpenResult drop_(OpenResult r) noexcept { ++drops_; ++openDrops_[static_cast<std::size_t>(r)]; return r; }

    int32_t                    self_;
    uint8_t                    sender_;
    std::array<PeerSlot, kMaxPeers> slots_{};
    std::size_t                used_ = 0;
    uint64_t                   drops_ = 0, noLinkDrops_ = 0;
    std::array<uint64_t, 7>    openDrops_{};
    uint64_t                   installs_ = 0;
    std::optional<PeerEpoch>   peerUsed_;          // takePeerUsedEpoch's single-slot latch
};

/// Chosen at transport construction (Rule 3): PeerPlain = today's bytes, PeerFrameCrypto = sealed.
using PeerCryptoMode = std::variant<PeerPlain, PeerFrameCrypto>;

} // namespace rune::peerlink
