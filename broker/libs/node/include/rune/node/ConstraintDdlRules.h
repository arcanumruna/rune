#pragma once
// ConstraintDdlRules.h — checked writes Part 09 (P9-9): the limits a constrained (unique or check) view meets at DDL.
#include <rune/node/VerdictState.h>
#include <rune/storage/ControlRecordLayout.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace rune::node {

inline constexpr uint64_t    kMaxConstrainedViewObjectId = 65536;
inline constexpr std::size_t kMaxConstraintOrdinals      = 65535;

/// nullopt = allowed.  `membersAfter` counts this view, the key index and every other constrained view over the source.
[[nodiscard]] inline std::optional<std::string> constrainedViewRefusal(uint64_t viewObjectId, std::size_t constraintCount,
                                                                       std::size_t membersAfter, std::size_t planRecordBytes) {
    if (viewObjectId >= kMaxConstrainedViewObjectId || constraintCount > kMaxConstraintOrdinals) return "constraint ids exhausted";
    if (membersAfter > kMaxVerdictViewsPerSource) return "a topic feeds at most 32 verdict views (the key index counts as one)";
    if (planRecordBytes > rune::storage::control_record::kMaxVerdictPlanBytes)
        return "a constrained view's plan must encode in at most 65536 bytes";
    return std::nullopt;
}

}  // namespace rune::node
