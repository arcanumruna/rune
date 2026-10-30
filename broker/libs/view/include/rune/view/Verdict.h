// libs/view/include/rune/view/Verdict.h
#pragma once
#include <cstdint>

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

} // namespace rune::view
