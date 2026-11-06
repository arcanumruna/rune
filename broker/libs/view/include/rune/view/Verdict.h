// libs/view/include/rune/view/Verdict.h
#pragma once
#include <cstdint>
#include <optional>
#include <span>

#include <rune/view/ViewPlan.h>

namespace rune::view {

struct Verdict {
    uint16_t code         = 0;
    uint32_t constraintId = 0;
    [[nodiscard]] bool accepted() const noexcept { return code == 0; }
};

// viewObjectId << 16 | ordinal; ordinal 0 is `unique`.
[[nodiscard]] constexpr uint32_t constraintIdOf(uint64_t viewObjectId, uint16_t ordinal) noexcept {
    return (static_cast<uint32_t>(viewObjectId) << 16) | ordinal;
}

enum class VerdictViewKind : uint8_t { KeyIndex = 1, Unique = 2, Check = 3, TestReject = 200 };

/// A unique view judges ordinal 0 and then its checks; a view with only checks is a Check member; any other view is none.
[[nodiscard]] inline std::optional<VerdictViewKind> verdictKindOf(const CompiledViewPlan& plan) noexcept {
    if (plan.unique) return VerdictViewKind::Unique;
    if (!plan.constraints.empty()) return VerdictViewKind::Check;
    return std::nullopt;
}

/// FNV-1a 64 over the catalog's compiled plan blob.
[[nodiscard]] inline uint64_t verdictPlanFingerprint(std::span<const uint8_t> compiledPlanBlob) noexcept {
    uint64_t h = 0xcbf29ce484222325ull;
    for (const uint8_t b : compiledPlanBlob) h = (h ^ b) * 0x100000001b3ull;
    return h;
}

} // namespace rune::view
