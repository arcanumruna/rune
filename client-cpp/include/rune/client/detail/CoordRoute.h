#pragma once
// CoordRoute.h - #362: the Client's transaction / group coordinator route as a variant (the seed, or another node's
// session) instead of scattered fields with a 0 "up" sentinel.  Duty thread only; header-only, no allocation beyond
// the remote's channel string (set on a move, never per message).

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace rune::client::detail {

/// D4 coordinator route (duty thread only): the seed, or another node's session.
struct CoordSeed {};
struct CoordRemote {
    uint32_t               idx = 0;        // its agent session index (never 0)
    std::string            channel;        // its channel
    uint64_t               failBase = 0;   // its connectFailures() when it became the coordinator
    std::optional<int64_t> downSinceNs;    // first seen NOT Connected (duty clock); empty = up
};
using CoordRoute = std::variant<CoordSeed, CoordRemote>;

[[nodiscard]] inline uint32_t coordIdxOf(const CoordRoute& r) noexcept {
    const auto* rm = std::get_if<CoordRemote>(&r);
    return rm != nullptr ? rm->idx : 0;
}

/// Phase 2b F Task 11 / #362: track the remote session's down time; true once it has been down longer than graceNs.
[[nodiscard]] inline bool coordDownTooLong(CoordRemote& r, bool connected, int64_t nowNs, int64_t graceNs) noexcept {
    if (connected) {
        r.downSinceNs.reset();
        return false;
    }
    if (!r.downSinceNs) r.downSinceNs = nowNs;
    return nowNs - *r.downSinceNs > graceNs;
}

/// r2 LOW-3: a coordinator whose session was Rejected / Closed, and the metadataResponses_ count then.
struct RejectedCoord {
    std::string channel;
    int         meta = 0;
};

}  // namespace rune::client::detail
