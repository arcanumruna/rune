#pragma once
// rune/peerlink/PeerLinkMessages.h — the inter-broker link handshake frames (authentication plan Task 15, D33).
//
// Hand-rolled little-endian, internal broker protocol: clients never see these.  A 1-byte PeerMsg tag leads every
// frame.  The frames travel on the peer-auth stream (RUNE_PEER_AUTH_STREAM_ID) of the peer's consensus endpoint,
// UNENCRYPTED by design: they carry only nonces and MACs, never a key.  Every frame is a fixed size.
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::peerlink {

enum class PeerMsg : uint8_t { Hello = 1, Challenge = 2, Proof = 3, Accept = 4, Refuse = 5 };

/// Why a handshake was refused.  NoLink (5) is a poke, not a refusal: a responder that holds no link says "start
/// over" (it restarted) — never logged as a refusal, never counted.
enum class RefuseReason : uint8_t { NoCommonKey = 1, BadProof = 2, StaleIncarnation = 3, NotAPeer = 4, NoLink = 5 };

[[nodiscard]] constexpr const char* refuseReasonText(RefuseReason r) noexcept {
    switch (r) {
        case RefuseReason::NoCommonKey:      return "no common cluster key";
        case RefuseReason::BadProof:         return "bad proof";
        case RefuseReason::StaleIncarnation: return "stale incarnation";
        case RefuseReason::NotAPeer:         return "not a peer";
        case RefuseReason::NoLink:           return "no link";
    }
    return "unknown";
}

namespace detail {
inline void putLe(uint8_t* p, uint64_t v, std::size_t n) noexcept { for (std::size_t i = 0; i < n; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
[[nodiscard]] inline uint64_t getLe(const uint8_t* p, std::size_t n) noexcept {
    uint64_t v = 0;
    for (std::size_t i = 0; i < n; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}
} // namespace detail

struct PeerHello {
    int32_t from = -1, to = -1; uint64_t keyId = 0; uint64_t incarnation = 0; std::array<uint8_t, 32> nonceA{};
    static constexpr std::size_t kSize = 1 + 4 + 4 + 8 + 8 + 32;
    void encode(uint8_t* b) const noexcept {
        b[0] = static_cast<uint8_t>(PeerMsg::Hello);
        detail::putLe(b + 1, static_cast<uint32_t>(from), 4); detail::putLe(b + 5, static_cast<uint32_t>(to), 4);
        detail::putLe(b + 9, keyId, 8); detail::putLe(b + 17, incarnation, 8);
        std::memcpy(b + 25, nonceA.data(), 32);
    }
    [[nodiscard]] static bool decode(const uint8_t* b, std::size_t len, PeerHello& m) noexcept {
        if (len != kSize || b[0] != static_cast<uint8_t>(PeerMsg::Hello)) return false;
        m.from = static_cast<int32_t>(detail::getLe(b + 1, 4)); m.to = static_cast<int32_t>(detail::getLe(b + 5, 4));
        m.keyId = detail::getLe(b + 9, 8); m.incarnation = detail::getLe(b + 17, 8);
        std::memcpy(m.nonceA.data(), b + 25, 32);
        return true;
    }
};

struct PeerChallenge {
    int32_t from = -1, to = -1; uint64_t keyId = 0; uint64_t incarnation = 0; std::array<uint8_t, 32> nonceB{};
    std::array<uint8_t, 64> macB{};
    static constexpr std::size_t kSize = 1 + 4 + 4 + 8 + 8 + 32 + 64;
    void encode(uint8_t* b) const noexcept {
        b[0] = static_cast<uint8_t>(PeerMsg::Challenge);
        detail::putLe(b + 1, static_cast<uint32_t>(from), 4); detail::putLe(b + 5, static_cast<uint32_t>(to), 4);
        detail::putLe(b + 9, keyId, 8); detail::putLe(b + 17, incarnation, 8);
        std::memcpy(b + 25, nonceB.data(), 32); std::memcpy(b + 57, macB.data(), 64);
    }
    [[nodiscard]] static bool decode(const uint8_t* b, std::size_t len, PeerChallenge& m) noexcept {
        if (len != kSize || b[0] != static_cast<uint8_t>(PeerMsg::Challenge)) return false;
        m.from = static_cast<int32_t>(detail::getLe(b + 1, 4)); m.to = static_cast<int32_t>(detail::getLe(b + 5, 4));
        m.keyId = detail::getLe(b + 9, 8); m.incarnation = detail::getLe(b + 17, 8);
        std::memcpy(m.nonceB.data(), b + 25, 32); std::memcpy(m.macB.data(), b + 57, 64);
        return true;
    }
};

struct PeerProof {
    int32_t from = -1, to = -1; std::array<uint8_t, 64> macA{};
    static constexpr std::size_t kSize = 1 + 4 + 4 + 64;
    void encode(uint8_t* b) const noexcept {
        b[0] = static_cast<uint8_t>(PeerMsg::Proof);
        detail::putLe(b + 1, static_cast<uint32_t>(from), 4); detail::putLe(b + 5, static_cast<uint32_t>(to), 4);
        std::memcpy(b + 9, macA.data(), 64);
    }
    [[nodiscard]] static bool decode(const uint8_t* b, std::size_t len, PeerProof& m) noexcept {
        if (len != kSize || b[0] != static_cast<uint8_t>(PeerMsg::Proof)) return false;
        m.from = static_cast<int32_t>(detail::getLe(b + 1, 4)); m.to = static_cast<int32_t>(detail::getLe(b + 5, 4));
        std::memcpy(m.macA.data(), b + 9, 64);
        return true;
    }
};

/// `confirm` is an addition to the plan's struct: HMAC-SHA-512 (truncated to 32 bytes) over the transcript and the
/// epoch under the cluster key — an off-path forger cannot hand the initiator a wrong epoch.
struct PeerAccept {
    int32_t from = -1, to = -1; uint32_t linkEpoch = 0; std::array<uint8_t, 32> confirm{};
    static constexpr std::size_t kSize = 1 + 4 + 4 + 4 + 32;
    void encode(uint8_t* b) const noexcept {
        b[0] = static_cast<uint8_t>(PeerMsg::Accept);
        detail::putLe(b + 1, static_cast<uint32_t>(from), 4); detail::putLe(b + 5, static_cast<uint32_t>(to), 4);
        detail::putLe(b + 9, linkEpoch, 4); std::memcpy(b + 13, confirm.data(), 32);
    }
    [[nodiscard]] static bool decode(const uint8_t* b, std::size_t len, PeerAccept& m) noexcept {
        if (len != kSize || b[0] != static_cast<uint8_t>(PeerMsg::Accept)) return false;
        m.from = static_cast<int32_t>(detail::getLe(b + 1, 4)); m.to = static_cast<int32_t>(detail::getLe(b + 5, 4));
        m.linkEpoch = static_cast<uint32_t>(detail::getLe(b + 9, 4)); std::memcpy(m.confirm.data(), b + 13, 32);
        return true;
    }
};

struct PeerRefuse {
    int32_t from = -1, to = -1; RefuseReason reason{};
    static constexpr std::size_t kSize = 1 + 4 + 4 + 1;
    void encode(uint8_t* b) const noexcept {
        b[0] = static_cast<uint8_t>(PeerMsg::Refuse);
        detail::putLe(b + 1, static_cast<uint32_t>(from), 4); detail::putLe(b + 5, static_cast<uint32_t>(to), 4);
        b[9] = static_cast<uint8_t>(reason);
    }
    [[nodiscard]] static bool decode(const uint8_t* b, std::size_t len, PeerRefuse& m) noexcept {
        if (len != kSize || b[0] != static_cast<uint8_t>(PeerMsg::Refuse)) return false;
        if (b[9] < 1 || b[9] > 5) return false;
        m.from = static_cast<int32_t>(detail::getLe(b + 1, 4)); m.to = static_cast<int32_t>(detail::getLe(b + 5, 4));
        m.reason = static_cast<RefuseReason>(b[9]);
        return true;
    }
};

inline constexpr std::size_t kMaxPeerLinkFrame = PeerChallenge::kSize;

/// The tag of a frame, or 0 when it is empty / unknown.
[[nodiscard]] inline uint8_t peekPeerMsg(const uint8_t* b, std::size_t len) noexcept {
    return (len > 0 && b[0] >= 1 && b[0] <= 5) ? b[0] : uint8_t{0};
}

} // namespace rune::peerlink
