#pragma once
// AuthDevMode.h — the dev-mode (RUNE_AUTH=disabled) warning cadence (authentication spec §9, plan D31).
//
// Dev mode runs every request as 'anonymous' with full access, so it is shouted: once at start and again every five
// minutes (a control-thread timer).  A pure value type: the caller supplies the clock; no thread, no allocation.

#include <cstdint>

namespace rune::node {

/// The dev-mode warning, verbatim (spec §9).
inline constexpr const char* kDevModeWarning =
    "AUTHENTICATION DISABLED (RUNE_AUTH=disabled): every request runs as 'anonymous' with full access — dev mode only";
inline constexpr int64_t kDevModeWarnIntervalNs = 5LL * 60 * 1'000'000'000LL;

struct DevModeWarnTimer {
    int64_t nextNs = 0;   // 0 = never warned: the first call is due at once ("at start")
    /// true = warn now; re-arms for `kDevModeWarnIntervalNs` later.
    [[nodiscard]] bool due(int64_t nowNs) noexcept {
        if (nextNs != 0 && nowNs < nextNs) return false;
        nextNs = nowNs + kDevModeWarnIntervalNs;
        return true;
    }
};

} // namespace rune::node
