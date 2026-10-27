#pragma once
// NamespaceUsageMessages.h — NamespaceUsage (84) and NamespaceUsageResponse (85) (plan 2026-10-26 Phase 2, D29).
// No client-version bump: older peers answer UnknownTemplate and the broker never sends 85 unasked.
//
//   NamespaceUsage (84)          header | name var16                      (BODY_FIXED 0; empty name refused)
//   NamespaceUsageResponse (85)  header | [fixed] code i16, state u8 | NamespaceLimits (kNamespaceLimitsBytes) |
//                                usage u64 x kNamespaceLimitFieldCount (same field order as the limits; 0 = n/a)
//                                state = catalog NsStateTag value.
// Decoders are bounds-checked, require the exact frame length and never read past `avail`.

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include "NamespaceMessages.h"

namespace rune::protocol::client {

struct NamespaceUsageRequest {
    int64_t sessionId = 0, correlationId = 0;
    std::string name;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::NamespaceUsage);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + name.size(); }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (name.empty() || !fitsVarLen16(name.size())) return 0;
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        return pos + putVarStr(buf + pos, name);
    }
    static std::optional<NamespaceUsageRequest> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        NamespaceUsageRequest m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        std::string_view nm;
        if (!getVarStr(buf, avail, pos, nm) || nm.empty() || pos != avail) return std::nullopt;
        m.name.assign(nm);
        return m;
    }
};

struct NamespaceUsageResponse {
    int64_t sessionId = 0, correlationId = 0;
    int16_t code = 0;
    uint8_t state = 0;
    NamespaceLimits limits;
    std::array<uint64_t, kNamespaceLimitFieldCount> usage{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::NamespaceUsageResponse);
    static constexpr uint16_t BODY_FIXED  = 3;

    [[nodiscard]] static constexpr std::size_t encodedSize() noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + kNamespaceLimitsBytes + 8u * kNamespaceLimitFieldCount;
    }
    /// Zero-allocation encode; returns 0 when `cap` < encodedSize().
    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (cap < encodedSize()) return 0;
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        detail::putLE16s(buf, static_cast<int>(pos), code);
        buf[pos + 2] = state;
        pos += BODY_FIXED;
        encodeLimits(limits, buf + pos); pos += kNamespaceLimitsBytes;
        for (const uint64_t u : usage) { detail::putLE64(buf, static_cast<int>(pos), static_cast<int64_t>(u)); pos += 8; }
        return pos;
    }
    static std::optional<NamespaceUsageResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h || h->blockLength != BODY_FIXED || avail != encodedSize()) return std::nullopt;
        NamespaceUsageResponse m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.code = detail::getLE16s(buf, static_cast<int>(MessageHeader::ENCODED_SIZE));
        m.state = buf[MessageHeader::ENCODED_SIZE + 2];
        if (!decodeLimits(buf + pos, avail - pos, m.limits)) return std::nullopt;
        pos += kNamespaceLimitsBytes;
        for (auto& u : m.usage) { u = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos))); pos += 8; }
        return m;
    }
};

} // namespace rune::protocol::client
