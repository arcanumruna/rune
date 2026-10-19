#pragma once
// rune/security/SecurityReplayer.h — THE verifying replay of a __security prefix (auth spec §3):
// every consumer (the broker's admin thread, the client, the rune CLI) folds the log through this
// one class.  Per event, in order: decode under its formatVer, refuse a gap (the offset must be
// exactly nextOffset()), apply under the frozen rules of its formatVer and verify its fingerprint.
// Any failure is an explicit ReplayError naming the offset, nothing of that event is applied, and
// the replayer still waits for that same offset — a later event can never be applied past a refused
// one (it is a Gap).  An unknown formatVer or kind stops the replay; nothing is ever skipped.
// Single-threaded, owned by whoever replays.  Cold path (control plane).
#include <rune/security/SecurityState.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>

namespace rune::security {

struct ReplayError {
    enum class Kind : uint8_t { Decode, Apply, Gap } kind = Kind::Apply;
    int64_t offset = -1;
    std::optional<DecodeError> decode;
    std::optional<ApplyError> apply;
};
/// "offset N: <Decode|Apply|Gap> [<decode / apply code>] [<refusal>] [expected=0x… actual=0x…] <detail>".
[[nodiscard]] std::string describe(const ReplayError&);

class SecurityReplayer {
public:
    /// Start from `start` — the empty state, or a cached prefix (its appliedOffset()) to resume from.
    explicit SecurityReplayer(SecurityState start = {}) : state_(std::move(start)) {}

    /// Decode `record` (the __security record at `offset`) and apply it.
    [[nodiscard]] std::expected<Applied, ReplayError> feed(int64_t offset, std::span<const uint8_t> record);
    /// Apply an already-decoded event at `offset` (same checks, minus the decode).
    [[nodiscard]] std::expected<Applied, ReplayError> feedDecoded(int64_t offset, const SecurityEvent& e);

    [[nodiscard]] const SecurityState& state() const noexcept { return state_; }
    /// The only offset the next feed accepts: appliedOffset() + 1.
    [[nodiscard]] int64_t nextOffset() const noexcept { return state_.appliedOffset() + 1; }

private:
    SecurityState state_;
};

} // namespace rune::security
