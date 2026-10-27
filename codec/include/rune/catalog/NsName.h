#pragma once
// rune/catalog/NsName.h — namespace names and the qualified object-name form (plan D6, D7).
//
//   * A namespace name matches [a-z0-9][a-z0-9-]{0,62}, and "default" is reserved (nsId 0, predefined).
//   * An object's local name keeps today's rules (TopicName.h, user / role name rules).
//   * The qualified form is "ns/name" with exactly ONE '/'.  parseQualified only SPLITS: it never looks a
//     namespace up and never allocates (string_view over the input).  An unqualified name has an empty ns, which
//     the resolver reads as "the caller's namespace".
//
// Shared by the broker, the client and rune-ql; header-only, constexpr, allocation-free.
#include <cstdint>
#include <expected>
#include <string_view>

namespace rune::catalog {

inline constexpr uint32_t         kDefaultNsId   = 0;
inline constexpr std::string_view kDefaultNsName = "default";
inline constexpr std::size_t      kMaxNamespaceNameBytes = 63;

[[nodiscard]] constexpr bool validNamespaceName(std::string_view n) noexcept {
    if (n.empty() || n.size() > kMaxNamespaceNameBytes || n == kDefaultNsName) return false;
    for (std::size_t i = 0; i < n.size(); ++i) {
        const char c = n[i];
        const bool alnum = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
        if (!alnum && !(c == '-' && i != 0)) return false;
    }
    return true;
}

struct Qualified {
    std::string_view ns;      // empty = unqualified (the caller's namespace)
    std::string_view local;
};
enum class QualifyError : uint8_t { Malformed };

/// "t" -> {"", "t"}; "a/t" -> {"a", "t"}.  Two or more '/', or an empty part ("", "/", "/t", "a/") -> Malformed.
[[nodiscard]] constexpr std::expected<Qualified, QualifyError> parseQualified(std::string_view s) noexcept {
    const auto slash = s.find('/');
    if (slash == std::string_view::npos) {
        if (s.empty()) return std::unexpected(QualifyError::Malformed);
        return Qualified{std::string_view{}, s};
    }
    const std::string_view ns = s.substr(0, slash);
    const std::string_view local = s.substr(slash + 1);
    if (ns.empty() || local.empty() || local.find('/') != std::string_view::npos)
        return std::unexpected(QualifyError::Malformed);
    return Qualified{ns, local};
}

} // namespace rune::catalog
