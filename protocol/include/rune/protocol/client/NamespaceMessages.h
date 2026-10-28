#pragma once
// NamespaceMessages.h — namespace DDL (81), ListNamespaces (82) and ListNamespacesResponse (83), client version 14
// (plan D13, spec §5.1/§5.2).  NamespaceDdl is answered by DdlResponse (78) (`objectId` = the namespace id).
//
// Layouts (LE; encoders return 0 when a field does not fit, decoders are bounds-checked, return nullopt for an
// out-of-range enum / reserved bit or trailing bytes, and never read past `avail`; DdlExtension string_views point
// into `buf`, names are copied):
//
//   NamespaceDdl (81)         header | ext (catalog DdlExtension layout) | kind u8 | name var16 |
//                             presentMask u8[4] | NamespaceLimits (kNamespaceLimitsBytes)
//                             kind 1 = Create, 2 = SetLimits (3..5 reserved for Phase 2).  presentMask bit i = limits
//                             field i was given (kNamespaceLimitFieldCount = 29 fields: 28 u64 in declaration order,
//                             then featureBits); bits 29..31 must be zero.
//   ListNamespaces (82)       header | [fixed] afterNsId u32, max u16
//   ListNamespacesResponse(83) header | [fixed] count u16 | count x { nsId u32, name var16, state u8, limits } |
//                             nextAfter u32

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include "AuthMessages.h"
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"
#include "SchemaMessages.h"
#include "../NamespaceLimits.h"

namespace rune::protocol::client {

enum class NamespaceDdlKind : uint8_t { Create = 1, SetLimits = 2, Suspend = 3, Resume = 4, Delete = 5 };

/// Suspend flavour (kind 3 only): ReadOnly refuses writes, RejectAll refuses every request.
enum class NsSuspendMode : uint8_t { ReadOnly = 1, RejectAll = 2 };

inline constexpr std::size_t kNamespacePresentMaskBytes = 4;

[[nodiscard]] constexpr bool validSuspendMode(uint8_t m) noexcept {
    return m >= static_cast<uint8_t>(NsSuspendMode::ReadOnly) && m <= static_cast<uint8_t>(NsSuspendMode::RejectAll);
}

/// True when `mask` has limit field `i` (0..kNamespaceLimitFieldCount-1) marked present.
[[nodiscard]] constexpr bool namespaceMaskHas(const std::array<uint8_t, kNamespacePresentMaskBytes>& mask,
                                              std::size_t i) noexcept {
    return i < kNamespaceLimitFieldCount && ((mask[i >> 3] >> (i & 7u)) & 1u) != 0;
}

struct NamespaceDdlRequest {
    int64_t sessionId = 0, correlationId = 0;
    DdlExtension ext;
    NamespaceDdlKind kind = NamespaceDdlKind::Create;
    std::string name;
    std::array<uint8_t, kNamespacePresentMaskBytes> presentMask{};   // bit i = limit field i given; 29 fields
    NamespaceLimits limits;
    NsSuspendMode mode = NsSuspendMode::ReadOnly;   // kind 3 only

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::NamespaceDdl);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept {
        std::size_t tail = 0;
        switch (kind) {
            case NamespaceDdlKind::Create:
            case NamespaceDdlKind::SetLimits: tail = kNamespacePresentMaskBytes + kNamespaceLimitsBytes; break;
            case NamespaceDdlKind::Suspend:   tail = 1u; break;
            case NamespaceDdlKind::Resume:
            case NamespaceDdlKind::Delete:    tail = 0; break;
        }
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + extEncodedSize(ext) + 1u + 2u + name.size() + tail;
    }
    std::size_t encode(uint8_t* buf) const noexcept;
    static std::optional<NamespaceDdlRequest> decode(const uint8_t* buf, std::size_t avail) noexcept;
};

struct NamespaceInfo {
    uint32_t nsId = 0;
    std::string name;
    uint8_t state = 0;
    NamespaceLimits limits;
    bool operator==(const NamespaceInfo&) const = default;
};

struct ListNamespaces {
    int64_t sessionId = 0, correlationId = 0;
    uint32_t afterNsId = 0;
    uint16_t max = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ListNamespaces);
    static constexpr uint16_t BODY_FIXED  = 6;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }
    std::size_t encode(uint8_t* buf) const noexcept {
        std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(afterNsId));
        detail::putLE16(buf, static_cast<int>(pos + 4), max);
        return pos + BODY_FIXED;
    }
    static std::optional<ListNamespaces> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        ListNamespaces m;
        m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        const std::size_t b = MessageHeader::ENCODED_SIZE;
        m.afterNsId = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(b)));
        m.max = detail::getLE16(buf, static_cast<int>(b + 4));
        if (pos != avail) return std::nullopt;
        return m;
    }
};

struct ListNamespacesResponse {
    int64_t sessionId = 0, correlationId = 0;
    std::vector<NamespaceInfo> namespaces;   // control plane: allocation is fine
    uint32_t nextAfter = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ListNamespacesResponse);
    static constexpr uint16_t BODY_FIXED  = 2;

    [[nodiscard]] std::size_t encodedSize() const noexcept {
        std::size_t n = MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u;
        for (const auto& i : namespaces) n += 4u + 2u + i.name.size() + 1u + kNamespaceLimitsBytes;
        return n;
    }
    std::size_t encode(uint8_t* buf) const noexcept;
    static std::optional<ListNamespacesResponse> decode(const uint8_t* buf, std::size_t avail) noexcept;
};

inline std::size_t NamespaceDdlRequest::encode(uint8_t* buf) const noexcept {
    const auto k = static_cast<uint8_t>(kind);
    if (k < static_cast<uint8_t>(NamespaceDdlKind::Create) || k > static_cast<uint8_t>(NamespaceDdlKind::Delete)) return 0;
    const bool hasLimits = kind == NamespaceDdlKind::Create || kind == NamespaceDdlKind::SetLimits;
    if (kind == NamespaceDdlKind::Suspend && !validSuspendMode(static_cast<uint8_t>(mode))) return 0;
    if (!extFits(ext) || !fitsVarLen16(name.size())) return 0;
    if (hasLimits && (presentMask[3] & 0xE0u) != 0) return 0;   // bits 29..31 are reserved
    std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
    pos += putExt(buf + pos, ext);
    buf[pos++] = k;
    pos += putVarStr(buf + pos, name);
    if (kind == NamespaceDdlKind::Suspend) { buf[pos++] = static_cast<uint8_t>(mode); return pos; }
    if (!hasLimits) return pos;
    for (std::size_t i = 0; i < kNamespacePresentMaskBytes; ++i) buf[pos++] = presentMask[i];
    encodeLimits(limits, buf + pos);
    return pos + kNamespaceLimitsBytes;
}

inline std::optional<NamespaceDdlRequest> NamespaceDdlRequest::decode(const uint8_t* buf, std::size_t avail) noexcept {
    std::size_t pos = 0;
    auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
    if (!h) return std::nullopt;
    NamespaceDdlRequest m;
    m.sessionId = h->sessionId; m.correlationId = h->correlationId;
    if (!getExt(buf, avail, pos, m.ext)) return std::nullopt;
    if (pos + 1u > avail) return std::nullopt;
    const uint8_t k = buf[pos++];
    if (k < static_cast<uint8_t>(NamespaceDdlKind::Create) || k > static_cast<uint8_t>(NamespaceDdlKind::Delete)) return std::nullopt;
    m.kind = static_cast<NamespaceDdlKind>(k);
    std::string_view nm;
    if (!getVarStr(buf, avail, pos, nm) || nm.empty()) return std::nullopt;
    m.name.assign(nm);
    if (m.kind == NamespaceDdlKind::Suspend) {
        if (pos + 1u > avail || !validSuspendMode(buf[pos])) return std::nullopt;
        m.mode = static_cast<NsSuspendMode>(buf[pos++]);
        return pos == avail ? std::optional<NamespaceDdlRequest>(std::move(m)) : std::nullopt;
    }
    if (m.kind == NamespaceDdlKind::Resume || m.kind == NamespaceDdlKind::Delete)
        return pos == avail ? std::optional<NamespaceDdlRequest>(std::move(m)) : std::nullopt;
    if (pos + kNamespacePresentMaskBytes + kNamespaceLimitsBytes > avail) return std::nullopt;
    for (std::size_t i = 0; i < kNamespacePresentMaskBytes; ++i) m.presentMask[i] = buf[pos++];
    if ((m.presentMask[3] & 0xE0u) != 0) return std::nullopt;
    if (!decodeLimits(buf + pos, avail - pos, m.limits)) return std::nullopt;
    pos += kNamespaceLimitsBytes;
    if (pos != avail) return std::nullopt;
    return m;
}

inline std::size_t ListNamespacesResponse::encode(uint8_t* buf) const noexcept {
    if (namespaces.size() > 0xFFFFu) return 0;
    for (const auto& i : namespaces) if (!fitsVarLen16(i.name.size())) return 0;
    std::size_t pos = detail::putAuthHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
    detail::putLE16(buf, static_cast<int>(pos), static_cast<uint16_t>(namespaces.size()));
    pos += BODY_FIXED;
    for (const auto& i : namespaces) {
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(i.nsId)); pos += 4;
        pos += putVarStr(buf + pos, i.name);
        buf[pos++] = i.state;
        encodeLimits(i.limits, buf + pos); pos += kNamespaceLimitsBytes;
    }
    detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(nextAfter)); pos += 4;
    return pos;
}

inline std::optional<ListNamespacesResponse> ListNamespacesResponse::decode(const uint8_t* buf, std::size_t avail) noexcept {
    std::size_t pos = 0;
    auto h = detail::getAuthHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
    if (!h) return std::nullopt;
    ListNamespacesResponse m;
    m.sessionId = h->sessionId; m.correlationId = h->correlationId;
    const uint16_t count = detail::getLE16(buf, static_cast<int>(MessageHeader::ENCODED_SIZE));
    constexpr std::size_t kMinEntry = 4u + 2u + 1u + kNamespaceLimitsBytes;
    if (static_cast<std::size_t>(count) * kMinEntry + 4u > avail - pos) return std::nullopt;   // bound before reserve
    m.namespaces.reserve(count);
    for (uint16_t e = 0; e < count; ++e) {
        NamespaceInfo i;
        if (pos + 4u > avail) return std::nullopt;
        i.nsId = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
        std::string_view nm;
        if (!getVarStr(buf, avail, pos, nm)) return std::nullopt;
        i.name.assign(nm);
        if (pos + 1u + kNamespaceLimitsBytes > avail) return std::nullopt;
        i.state = buf[pos++];
        if (!decodeLimits(buf + pos, avail - pos, i.limits)) return std::nullopt;
        pos += kNamespaceLimitsBytes;
        m.namespaces.push_back(std::move(i));
    }
    if (pos + 4u != avail) return std::nullopt;
    m.nextAfter = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos)));
    return m;
}

} // namespace rune::protocol::client
