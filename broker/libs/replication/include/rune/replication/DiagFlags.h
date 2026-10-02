#pragma once
// Opt-in diagnostic switches for the replication hot path.
//
// Every diagnostic log on the replication path is OFF unless explicitly enabled:
// a default deployment must never pay for formatted logging per fetch / per
// state change.  RUNE_FS_LOG=<anything but "0" or empty> turns the FollowerSync
// trace on.

namespace rune::replication {

/// Parse an opt-in diagnostic env value: unset, empty or "0" = off.
[[nodiscard]] constexpr bool diagFlagFromEnv(const char* e) noexcept {
    return e != nullptr && e[0] != '\0' && !(e[0] == '0' && e[1] == '\0');
}

} // namespace rune::replication
