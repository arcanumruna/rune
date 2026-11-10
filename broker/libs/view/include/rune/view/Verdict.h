// libs/view/include/rune/view/Verdict.h
#pragma once
#include <cstdint>
#include <optional>
#include <span>

#include <rune/view/ViewPlan.h>

namespace rune::view {

// Internal, never stored, never sent: the judge holds at a record (a lease record whose layout is not installed yet): no
// verdict was given.  Not accepted, and never recorded in the rejected index or a marker.
inline constexpr uint16_t kVerdictHeld = 0xFFFF;

struct Verdict {
    uint16_t code         = 0;
    uint32_t constraintId = 0;
    [[nodiscard]] bool accepted() const noexcept { return code == 0; }
    [[nodiscard]] bool isHeld() const noexcept { return code == kVerdictHeld; }
    [[nodiscard]] static constexpr Verdict held() noexcept { return Verdict{kVerdictHeld, 0}; }
};

// viewObjectId << 16 | ordinal; ordinal 0 is `unique`.
[[nodiscard]] constexpr uint32_t constraintIdOf(uint64_t viewObjectId, uint16_t ordinal) noexcept {
    return (static_cast<uint32_t>(viewObjectId) << 16) | ordinal;
}

// Internal, stored in the rejected index and in verdict markers only, never sent on the wire: a lease topic's stale broker
// record (an Expire or Checkpoint whose lease was revoked, renewed or re-granted meanwhile).  Broker records have no
// producer to answer.
inline constexpr uint16_t kVerdictStaleBroker = 0xFFFE;

enum class VerdictViewKind : uint8_t { KeyIndex = 1, Unique = 2, Check = 3, LeaseIndex = 4, TestReject = 200 };

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
