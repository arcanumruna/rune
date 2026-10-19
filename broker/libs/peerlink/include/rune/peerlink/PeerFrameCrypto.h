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
// Hot path: seal / open never allocate.  The cipher contexts are built in installLink (cold) and — for a sender id
// not seen before — on the first frame from it (the receiving side derives lazily; the owner's OWN sender id is
// derived at install time, so the steady state of the symmetric deployment never builds one).
#include <rune/crypto/Aead.h>
#include <rune/peerlink/PeerLinkKeys.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <variant>

namespace rune::peerlink {

inline constexpr std::size_t kPeerHeaderBytes   = 16;
inline constexpr std::size_t kPeerFrameOverhead = kPeerHeaderBytes + rune::crypto::kTagSize;   // 32
inline constexpr uint8_t     kPeerFrameMagic    = 0xB7;

/// The steady clock in ns - the ONE clock install / open measure the old epoch's grace period with.
[[nodiscard]] inline int64_t steadyNowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// A transport built PeerPlain sends and receives exactly the bytes it always did (InProcessUnauthenticated /
/// DevDisabled / single node).
struct PeerPlain {};

class PeerFrameCrypto {
public:
    static constexpr std::size_t kMaxPeers       = 16;   // == FollowerSlots::kMaxReplicas
    static constexpr int64_t     kRetainNs       = 2'000'000'000;   // the old epoch's grace period (D33)
    static constexpr int64_t     kClockNow       = std::numeric_limits<int64_t>::min();   // "read the clock yourself"

    PeerFrameCrypto(int32_t selfNode, uint8_t senderId);
    PeerFrameCrypto(PeerFrameCrypto&&) noexcept            = default;
    PeerFrameCrypto& operator=(PeerFrameCrypto&&) noexcept = default;
    PeerFrameCrypto(const PeerFrameCrypto&)                = delete;
    PeerFrameCrypto& operator=(const PeerFrameCrypto&)     = delete;

    /// COLD.  A link to `peer` was established (a new epoch replaces the current one; the old one stays valid at
    /// this receiver for kRetainNs).  False: the table is full, a node id does not fit the u16 header field, or a
    /// cipher could not be built - the link is then not usable (frames to `peer` drop, counted).
    bool installLink(int32_t peer, const LinkKeys& keys, int64_t nowNs);

    [[nodiscard]] bool hasLink(int32_t peer) const noexcept;
    /// The current epoch of the link to `peer` (0 = none).
    [[nodiscard]] uint32_t currentEpoch(int32_t peer) const noexcept;

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
    /// parsed (UnknownEpoch included, so the control thread can re-handshake).  `nowNs` is only read for a frame
    /// of a non-current epoch; kClockNow = the steady clock.
    [[nodiscard]] OpenResult open(const uint8_t* frame, std::size_t len, uint8_t* out, std::size_t cap,
                                  std::size_t& outLen, int32_t& fromNode, uint32_t& epoch,
                                  int64_t nowNs = kClockNow, bool commitWindow = true) noexcept;
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
    };
    struct Retired { EpochState state; int64_t untilNs = 0; };
    struct PeerSlot {
        int32_t                  peer = -1;
        std::optional<EpochState> current;
        std::optional<Retired>    previous;
        uint64_t                  authFrames = 0;
    };

    [[nodiscard]] const PeerSlot* slotOf_(int32_t peer) const noexcept;
    [[nodiscard]] PeerSlot* slotOf_(int32_t peer) noexcept;
    [[nodiscard]] EpochState* epochOf_(PeerSlot& s, uint32_t epoch, int64_t nowNs) noexcept;
    [[nodiscard]] std::size_t sealAt_(int32_t peer, const uint8_t* plain, std::size_t len, uint8_t* frame,
                                      std::size_t cap, bool inPlace) noexcept;
    OpenResult drop_(OpenResult r) noexcept { ++drops_; ++openDrops_[static_cast<std::size_t>(r)]; return r; }

    int32_t                    self_;
    uint8_t                    sender_;
    std::array<PeerSlot, kMaxPeers> slots_{};
    std::size_t                used_ = 0;
    uint64_t                   drops_ = 0, noLinkDrops_ = 0;
    std::array<uint64_t, 7>    openDrops_{};
};

/// Chosen at transport construction (Rule 3): PeerPlain = today's bytes, PeerFrameCrypto = sealed.
using PeerCryptoMode = std::variant<PeerPlain, PeerFrameCrypto>;

} // namespace rune::peerlink
