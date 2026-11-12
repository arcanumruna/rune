#pragma once
// rune/coordination/NsScopedId.h — namespace-scoped group and transactional ids (namespaces Phase 2 Task 5, D19).
//
// A group id or transactional id is turned, at the service boundary, into an internal *scoped id*:
//   * nsId 0 (`default`): the id unchanged — so `default` keys, logs and hashes are byte-identical to the v1 bytes
//     (no migration: a v1 key IS a nsId-0 key);
//   * nsId != 0: kScopedIdMark + u32 nsId (little-endian, 4 raw bytes) + the local id.
// Every map, the offset-store key, the txn-state key and `coordinatorFor(groupId)` then separate namespaces with no
// other change.  Clients may not send an id that starts with kScopedIdMark (reservedClientId).
//
// Header-only; zero allocation except the std::string the *caller* hands to scopedIdInto / returns from scopedId.
#include <cstdint>
#include <string>
#include <string_view>

namespace rune::catalog { class NamespaceRegistry; }

namespace rune::coordination {

inline constexpr char kScopedIdMark = '\x1F';
inline constexpr std::size_t kScopedIdPrefix = 5;   // mark + u32 nsId

/// Appends the scoped form of (nsId, local) to `out` (cleared first).  No allocation when out's capacity suffices.
inline void scopedIdInto(uint32_t nsId, std::string_view local, std::string& out)
{
    out.clear();
    if (nsId != 0) {
        out.push_back(kScopedIdMark);
        for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((nsId >> (8 * i)) & 0xFFu));
    }
    out.append(local);
}

[[nodiscard]] inline std::string scopedId(uint32_t nsId, std::string_view local)
{
    std::string s;
    scopedIdInto(nsId, local, s);
    return s;
}

struct ScopedIdView {
    uint32_t         nsId = 0;
    std::string_view local;
    bool operator==(const ScopedIdView&) const = default;
};

/// No mark -> {0, scoped}; a mark with fewer than kScopedIdPrefix bytes -> {0, scoped} (never produced).
[[nodiscard]] inline ScopedIdView parseScopedId(std::string_view scoped) noexcept
{
    if (scoped.size() < kScopedIdPrefix || scoped[0] != kScopedIdMark) return ScopedIdView{0, scoped};
    uint32_t nsId = 0;
    for (int i = 0; i < 4; ++i) nsId |= static_cast<uint32_t>(static_cast<unsigned char>(scoped[1 + i])) << (8 * i);
    return ScopedIdView{nsId, scoped.substr(kScopedIdPrefix)};
}

[[nodiscard]] constexpr bool reservedClientId(std::string_view id) noexcept
{
    return !id.empty() && id[0] == kScopedIdMark;
}

/// A per-request filter on topic ids (no allocation, no std::function): `fn == nullptr` means every topic is
/// readable.  The gateway sets it for a fetch-all OffsetFetch so the coordinator lists only topics the caller may READ
/// BEFORE it applies its capacity cap.
struct TopicReadFilter {
    bool (*fn)(void* ctx, int32_t topicId) = nullptr;   ///< true = the caller may READ `topicId`
    void* ctx = nullptr;
    [[nodiscard]] bool readable(int32_t topicId) const noexcept { return fn == nullptr || fn(ctx, topicId); }
};

/// The namespace context of one client request: the session's login namespace, whether the session is root, and the
/// gateway's namespace registry mirror (null = no registry: every id is an opaque local id in the session namespace).
struct NsRequestContext {
    uint32_t sessionNsId = 0;
    bool     root        = false;
    const rune::catalog::NamespaceRegistry* reg = nullptr;
    TopicReadFilter readFilter{};   ///< fetch-all OffsetFetch only: the topics the caller may READ (null fn = all)
};

} // namespace rune::coordination
