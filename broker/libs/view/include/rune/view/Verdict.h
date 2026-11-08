// libs/view/include/rune/view/Verdict.h
#pragma once
#include <cstdint>

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

} // namespace rune::view
