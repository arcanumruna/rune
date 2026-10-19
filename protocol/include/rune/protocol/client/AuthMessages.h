#pragma once
// AuthMessages.h — SCRAM-SHA-512 authentication frames (client version 9, spec §5, D5/D11).
//
// Templates 12..15.  Every frame carries the session id and correlation id in the
// MessageHeader.  Encoders return 0 when a field does not fit its wire width (the
// house fits() idiom); decoders are bounds-checked, never over-read, and return
// nullopt for any field outside the bounds below (the gateway answers
// InvalidRequest / AuthenticationFailed).  Decoded string_views point into `buf`.
//
//   AuthClientFirst (12)  [mechanism u8] | username var16, clientNonce var16
//   AuthServerFirst (13)  []             | combinedNonce var16, count u8, count x (salt var16, iterations u32)
//   AuthClientFinal (14)  [count u8]     | count x 64 proof bytes
//   AuthServerFinal (15)  [errorCode u16][flags u8][retryAfterMs u32] | serverSignature var16, detail var16
//
// Bounds on decode: username 1..128, nonce 1..128, salt 16..64, count 1..2,
// iterations 4096..1e6, signature 0 or 64 bytes.

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

inline constexpr uint8_t     kMechanismScramSha512 = 1;
inline constexpr std::size_t kScramProofBytes      = 64;
inline constexpr uint8_t     kAuthFlagMustChangePassword = 0x01;

inline constexpr std::size_t kAuthNameMax        = 128;
inline constexpr std::size_t kAuthNonceMax       = 128;
inline constexpr std::size_t kAuthSaltMin        = 16;
inline constexpr std::size_t kAuthSaltMax        = 64;
inline constexpr uint32_t    kAuthIterationsMin  = 4096;
inline constexpr uint32_t    kAuthIterationsMax  = 1'000'000;
inline constexpr std::size_t kAuthSignatureBytes = 64;
inline constexpr uint8_t     kAuthMaxVerifiers   = 2;   // D5

namespace detail {
inline std::size_t putAuthHeader(uint8_t* buf, uint16_t tid, uint16_t blockLength,
                                 int64_t sessionId, int64_t correlationId) noexcept {
    MessageHeader hdr;
    hdr.blockLength   = blockLength;
    hdr.templateId    = tid;
    hdr.schemaId      = kSchemaId;
    hdr.version       = kProtocolVersion;
    hdr.sessionId     = sessionId;
    hdr.correlationId = correlationId;
    hdr.encode(buf);
    return MessageHeader::ENCODED_SIZE;
}

/// Validates the header against `tid` and the minimum fixed block; on success returns the
/// header and sets `pos` to the first var byte (header + blockLength).
inline std::optional<MessageHeader> getAuthHeader(const uint8_t* buf, std::size_t avail, uint16_t tid,
                                                  uint16_t minBlock, std::size_t& pos) noexcept {
    auto h = MessageHeader::decode(buf, avail);
    if (!h || h->schemaId != kSchemaId || h->templateId != tid || h->blockLength < minBlock) return std::nullopt;
    pos = MessageHeader::ENCODED_SIZE + h->blockLength;
    if (pos > avail) return std::nullopt;
    return h;
}

[[nodiscard]] constexpr bool authSaltOk(std::size_t n) noexcept { return n >= kAuthSaltMin && n <= kAuthSaltMax; }
[[nodiscard]] constexpr bool authIterationsOk(uint32_t i) noexcept {
    return i >= kAuthIterationsMin && i <= kAuthIterationsMax;
}
} // namespace detail

// ---------------------------------------------------------------------------
// AuthClientFirst (12)
// ---------------------------------------------------------------------------
struct AuthClientFirst {
    int64_t sessionId = 0, correlationId = 0;
    uint8_t mechanism = kMechanismScramSha512;
    std::string_view username;
    std::string_view clientNonce;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AuthClientFirst);
    static constexpr uint16_t BODY_FIXED  = 1;

    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + username.size() + 2u + clientNonce.size();
    }
    /// Returns bytes written, or 0 when a field does not fit.
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fitsVarLen16(username.size()) || !fitsVarLen16(clientNonce.size())) return 0;
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        buf[pos++] = mechanism;
        pos += putVarStr(buf + pos, username);
        pos += putVarStr(buf + pos, clientNonce);
        return pos;
    }
    static std::optional<AuthClientFirst> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        AuthClientFirst m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.mechanism = buf[MessageHeader::ENCODED_SIZE];
        if (!getVarStr(buf, avail, pos, m.username) || !getVarStr(buf, avail, pos, m.clientNonce)) return std::nullopt;
        if (m.username.empty() || m.username.size() > kAuthNameMax) return std::nullopt;
        if (m.clientNonce.empty() || m.clientNonce.size() > kAuthNonceMax) return std::nullopt;
        return m;
    }
};

// ---------------------------------------------------------------------------
// AuthServerFirst (13) — one (salt, iterations) per active verifier (D5), count 1..2
// ---------------------------------------------------------------------------
struct ScramSaltWire {
    std::string_view salt;
    uint32_t         iterations = 0;
    bool operator==(const ScramSaltWire&) const = default;
};

struct AuthServerFirst {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view combinedNonce;
    uint8_t count = 1;
    std::array<ScramSaltWire, 2> salts{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AuthServerFirst);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept {
        std::size_t n = MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + combinedNonce.size() + 1u;
        for (uint8_t i = 0; i < count && i < 2; ++i) n += 2u + salts[i].salt.size() + 4u;
        return n;
    }
    /// Returns bytes written, or 0 when count is not 1..2 or a field does not fit.
    std::size_t encode(uint8_t* buf) const noexcept {
        if (count < 1 || count > kAuthMaxVerifiers || !fitsVarLen16(combinedNonce.size())) return 0;
        for (uint8_t i = 0; i < count; ++i) if (!fitsVarLen16(salts[i].salt.size())) return 0;
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        pos += putVarStr(buf + pos, combinedNonce);
        buf[pos++] = count;
        for (uint8_t i = 0; i < count; ++i) {
            pos += putVarStr(buf + pos, salts[i].salt);
            detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(salts[i].iterations)); pos += 4;
        }
        return pos;
    }
    static std::optional<AuthServerFirst> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        AuthServerFirst m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.combinedNonce)) return std::nullopt;
        if (m.combinedNonce.empty() || m.combinedNonce.size() > kAuthNonceMax) return std::nullopt;
        if (pos + 1u > avail) return std::nullopt;
        m.count = buf[pos++];
        if (m.count < 1 || m.count > kAuthMaxVerifiers) return std::nullopt;
        for (uint8_t i = 0; i < m.count; ++i) {
            if (!getVarStr(buf, avail, pos, m.salts[i].salt) || !detail::authSaltOk(m.salts[i].salt.size()))
                return std::nullopt;
            if (pos + 4u > avail) return std::nullopt;
            m.salts[i].iterations = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
            if (!detail::authIterationsOk(m.salts[i].iterations)) return std::nullopt;
        }
        return m;
    }
};

// ---------------------------------------------------------------------------
// AuthClientFinal (14) — one 64-byte proof per verifier listed in AuthServerFirst
// ---------------------------------------------------------------------------
struct AuthClientFinal {
    int64_t sessionId = 0, correlationId = 0;
    uint8_t count = 1;
    std::array<std::array<uint8_t, kScramProofBytes>, 2> proofs{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AuthClientFinal);
    static constexpr uint16_t BODY_FIXED  = 1;

    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + static_cast<std::size_t>(count) * kScramProofBytes;
    }
    /// Returns bytes written, or 0 when count is not 1..2.
    std::size_t encode(uint8_t* buf) const noexcept {
        if (count < 1 || count > kAuthMaxVerifiers) return 0;
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        buf[pos++] = count;
        for (uint8_t i = 0; i < count; ++i) {
            std::memcpy(buf + pos, proofs[i].data(), kScramProofBytes);
            pos += kScramProofBytes;
        }
        return pos;
    }
    static std::optional<AuthClientFinal> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        AuthClientFinal m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.count = buf[MessageHeader::ENCODED_SIZE];
        if (m.count < 1 || m.count > kAuthMaxVerifiers) return std::nullopt;
        if (pos + static_cast<std::size_t>(m.count) * kScramProofBytes > avail) return std::nullopt;
        for (uint8_t i = 0; i < m.count; ++i) {
            std::memcpy(m.proofs[i].data(), buf + pos, kScramProofBytes);
            pos += kScramProofBytes;
        }
        return m;
    }
};

// ---------------------------------------------------------------------------
// AuthServerFinal (15) — the verdict.  errorCode 0 = authenticated (serverSignature is
// the 64-byte ServerSignature the client verifies); otherwise an ErrorCode (typically
// AuthenticationFailed) with an empty signature, optional retryAfterMs and detail.
// ---------------------------------------------------------------------------
struct AuthServerFinal {
    int64_t  sessionId = 0, correlationId = 0;
    uint16_t errorCode = 0;
    uint8_t  flags = 0;
    uint32_t retryAfterMs = 0;
    std::string_view serverSignature;
    std::string_view detail;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AuthServerFinal);
    static constexpr uint16_t BODY_FIXED  = 7;

    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + serverSignature.size() + 2u + detail.size();
    }
    /// Returns bytes written, or 0 when a field does not fit.
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fitsVarLen16(serverSignature.size()) || !fitsVarLen16(detail.size())) return 0;
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        buf[pos++] = flags;
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(retryAfterMs)); pos += 4;
        pos += putVarStr(buf + pos, serverSignature);
        pos += putVarStr(buf + pos, detail);
        return pos;
    }
    static std::optional<AuthServerFinal> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        AuthServerFinal m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        constexpr int fb = static_cast<int>(MessageHeader::ENCODED_SIZE);
        m.errorCode    = detail::getLE16(buf, fb);
        m.flags        = buf[fb + 2];
        m.retryAfterMs = static_cast<uint32_t>(detail::getLE32(buf, fb + 3));
        if (!getVarStr(buf, avail, pos, m.serverSignature) || !getVarStr(buf, avail, pos, m.detail)) return std::nullopt;
        if (!m.serverSignature.empty() && m.serverSignature.size() != kAuthSignatureBytes) return std::nullopt;
        return m;
    }
};

} // namespace rune::protocol::client
