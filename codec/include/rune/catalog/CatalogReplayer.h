#pragma once
// rune/catalog/CatalogReplayer.h — THE verifying replay of a __catalog prefix (spec §6):
// every consumer (the broker's admin thread, the client, the rune CLI) folds the log
// through this one class.  Per event, in order: decode under its formatVer, refuse a gap
// (the offset must be exactly nextOffset()), apply under the frozen rules of its
// formatVer and verify its fingerprint.  Any failure is an explicit ReplayError naming
// the offset, and nothing of that event is applied — the replayer then still waits for
// that same offset, so a later event can never be applied past a refused one (it is a
// Gap).  An unknown formatVer or kind stops the replay; nothing is ever skipped.
//
// Single-threaded, owned by whoever replays.  Cold path (control plane).
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace rune::catalog {

/// One event of a recorded log, at its __catalog offset.
struct RecordedEvent {
    int64_t      offset = -1;
    CatalogEvent event;
};

struct ReplayError {
    /// Decode: the bytes do not decode (`decode` says why).  Gap: the offset is not
    /// nextOffset().  Apply: the event's rules refused it or its fingerprint does not
    /// match (`apply` says which; `apply` is empty only for a query's own refusal, e.g.
    /// stateAsOf's "no version n of …").
    enum class Code : uint8_t { Decode, Gap, Apply };
    int64_t                    offset = -1;
    Code                       code   = Code::Apply;
    std::optional<DecodeError> decode;
    std::optional<ApplyError>  apply;
    std::string                detail;
};
[[nodiscard]] std::string_view replayErrorName(ReplayError::Code c) noexcept;
/// "offset N: <Decode|Gap|Apply> [<code name>] [expected=0x… actual=0x…] <detail>", e.g.
/// "offset 17: Apply FingerprintMismatch expected=0x1f… actual=0x9c… AlterSchema 'players'".
/// A query's refusal that has no offset (offset < 0) omits the "offset N: " prefix.
[[nodiscard]] std::string describe(const ReplayError& e);

class CatalogReplayer {
public:
    /// Start from `start` — the empty catalog, or a cached prefix (its appliedOffset()) to resume from.
    explicit CatalogReplayer(CatalogState start = {}) : state_(std::move(start)) {}

    /// Decode `record` (the __catalog record at `offset`) and apply it.
    [[nodiscard]] std::expected<Applied, ReplayError> feed(int64_t offset, std::span<const uint8_t> record);
    /// Apply an already-decoded event at `offset` (same checks, minus the decode).
    [[nodiscard]] std::expected<Applied, ReplayError> feedDecoded(int64_t offset, const CatalogEvent& e);

    [[nodiscard]] const CatalogState& state() const noexcept { return state_; }
    /// The only offset the next feed accepts: appliedOffset() + 1.
    [[nodiscard]] int64_t nextOffset() const noexcept { return state_.appliedOffset() + 1; }

private:
    CatalogState state_;
};

} // namespace rune::catalog
