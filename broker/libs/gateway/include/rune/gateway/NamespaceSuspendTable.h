#pragma once
// Namespaces Phase 2 Task 3 (D21, D30): what a suspended namespace still serves, per request template.
//
// One row per client->broker REQUEST template the gateway dispatch routes.  The gateway keeps each session's
// namespace gate (Session::nsGate) and asks suspendAction(tid, gate) - an O(1) index load - for the cold refuse path.
//   ReadOnly  : writes refused (Produce, OffsetCommit, the txn requests, every DDL); reads, subscriptions, group
//               membership and the control templates are served.
//   RejectAll : everything refused except the session-control templates (Connect, Keepalive, Disconnect, Ping,
//               HandshakeContinue, AuthClientFirst, AuthClientFinal: closing and login answer for themselves).
//   Deleting  : behaves as RejectAll.
// Cluster admins are exempt - the gateway applies that, not this table.
#include <array>
#include <cstddef>
#include <cstdint>

#include <rune/catalog/CatalogEvent.h>   // NsStateTag

namespace rune::gateway {

enum class SuspendAction : uint8_t { Allow, Refuse };
struct SuspendRow {
    uint16_t      templateId;
    SuspendAction ro;
    SuspendAction ra;
};

inline constexpr uint16_t kSuspendTableSpan = 96;   // template ids 0..95

inline constexpr std::array<uint16_t, 36> kDispatchedTemplates = {
    1, 3, 4, 6, 8, 10, 12, 14, 16, 18, 21, 32, 34, 36, 38, 40, 42, 45, 47, 49, 51, 53,
    64, 66, 68, 70, 72, 74, 75, 76, 77, 79, 80, 81, 82, 84};

namespace detail {
constexpr SuspendRow row(uint16_t tid) noexcept {
    constexpr SuspendAction A = SuspendAction::Allow;
    constexpr SuspendAction R = SuspendAction::Refuse;
    switch (tid) {
    // session control: always allowed (closing / login handle themselves)
    case 1: case 3: case 4: case 6: case 10: case 12: case 14: return {tid, A, A};
    // writes: refused under ReadOnly too
    case 16: case 40: case 45: case 47: case 49: case 51: case 53:
    case 64: case 68: case 70: case 74: case 75: case 76: case 77: case 79: case 80: case 81:
        return {tid, R, R};
    // reads: served under ReadOnly, refused under RejectAll
    default: return {tid, A, R};
    }
}
template <std::size_t... I>
constexpr std::array<SuspendRow, sizeof...(I)> makeTable(std::index_sequence<I...>) noexcept {
    return {row(kDispatchedTemplates[I])...};
}
}  // namespace detail

inline constexpr std::array<SuspendRow, kDispatchedTemplates.size()> kSuspendTable =
    detail::makeTable(std::make_index_sequence<kDispatchedTemplates.size()>{});

namespace detail {
constexpr bool strictlyAscendingAndInSpan() noexcept {
    for (std::size_t i = 0; i < kDispatchedTemplates.size(); ++i) {
        if (kDispatchedTemplates[i] >= kSuspendTableSpan) return false;
        if (i > 0 && kDispatchedTemplates[i - 1] >= kDispatchedTemplates[i]) return false;
    }
    return true;
}
constexpr bool tableMatchesTemplates() noexcept {
    for (std::size_t i = 0; i < kSuspendTable.size(); ++i)
        if (kSuspendTable[i].templateId != kDispatchedTemplates[i]) return false;
    return true;
}
static_assert(strictlyAscendingAndInSpan(), "kDispatchedTemplates: strictly ascending, every id < kSuspendTableSpan");
static_assert(tableMatchesTemplates(), "kSuspendTable: same size and order as kDispatchedTemplates");

struct Index {
    std::array<uint8_t, kSuspendTableSpan> ro{};   // 1 = Refuse
    std::array<uint8_t, kSuspendTableSpan> ra{};
    std::array<uint8_t, kSuspendTableSpan> checked{};
};
constexpr Index makeIndex() noexcept {
    Index x;
    for (const SuspendRow& r : kSuspendTable) {
        x.ro[r.templateId] = r.ro == SuspendAction::Refuse ? 1 : 0;
        x.ra[r.templateId] = r.ra == SuspendAction::Refuse ? 1 : 0;
        x.checked[r.templateId] = (x.ro[r.templateId] | x.ra[r.templateId]);
    }
    return x;
}
inline constexpr Index kIndex = makeIndex();
}  // namespace detail

/// O(1).  Active -> Allow; Deleting behaves as RejectAll; an unknown template -> Allow.
[[nodiscard]] constexpr SuspendAction suspendAction(uint16_t tid, rune::catalog::NsStateTag gate) noexcept {
    if (tid >= kSuspendTableSpan) return SuspendAction::Allow;
    switch (gate) {
    case rune::catalog::NsStateTag::Active:             return SuspendAction::Allow;
    case rune::catalog::NsStateTag::SuspendedReadOnly:  return detail::kIndex.ro[tid] ? SuspendAction::Refuse : SuspendAction::Allow;
    case rune::catalog::NsStateTag::SuspendedRejectAll:
    case rune::catalog::NsStateTag::Deleting:           return detail::kIndex.ra[tid] ? SuspendAction::Refuse : SuspendAction::Allow;
    }
    return SuspendAction::Allow;
}

/// True when some non-Active gate refuses `tid` - the dispatch only decodes and looks the session up for these.
[[nodiscard]] constexpr bool suspendGateChecked(uint16_t tid) noexcept {
    return tid < kSuspendTableSpan && detail::kIndex.checked[tid] != 0;
}

}  // namespace rune::gateway
