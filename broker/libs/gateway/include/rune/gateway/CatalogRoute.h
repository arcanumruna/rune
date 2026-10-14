#pragma once
// CatalogRoute.h — where this broker sends a DDL (catalog history spec §5.1,
// §5.2, §5.5, §9; Task 11).
//
// A CONTROL-thread value: BrokerNode computes it each control pass from the
// committed (-6, 0) assignment (single node: the consensus leader) and the
// latched kA2cCatalogHalted, and publishes it to the gateway — on the same
// thread — only when it changes.  The gateway never reads catalog state.
//
//   RouteLocalLeader — this broker leads __catalog: submit to the admin ring
//                      (the AdminWorker still answers NotLeader while catching up).
//   RouteRemote      — another broker leads: ErrorResponse{Redirect 3, endpoint}.
//   RouteUnknown     — no leader yet / not placed: ErrorResponse{NotLeader 2}.
//   RouteHalted      — this broker's applier halted at `offset`:
//                      ErrorResponse{Internal 6, "catalog apply halted at offset N"}
//                      — never a redirect (matrix R2).
//
// Namespace: rune::gateway.  C++23.

#include <cstdint>
#include <cstddef>
#include <string>
#include <type_traits>
#include <variant>

namespace rune::gateway {

struct RouteLocalLeader {
    friend bool operator==(const RouteLocalLeader&, const RouteLocalLeader&) = default;
};
struct RouteRemote {
    std::string leaderClientEndpoint;
    friend bool operator==(const RouteRemote&, const RouteRemote&) = default;
};
struct RouteUnknown {   // NO_LEADER / not yet placed
    friend bool operator==(const RouteUnknown&, const RouteUnknown&) = default;
};
struct RouteHalted {    // this broker's applier halted
    int64_t offset = -1;
    friend bool operator==(const RouteHalted&, const RouteHalted&) = default;
};

using CatalogRoute = std::variant<RouteLocalLeader, RouteRemote, RouteUnknown, RouteHalted>;

/// The route's kind — the variant's alternative, by name (review M6: no magic
/// indices; the static_asserts pin the order).
enum class RouteKind : uint8_t { LocalLeader = 0, Remote = 1, Unknown = 2, Halted = 3 };
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(RouteKind::LocalLeader), CatalogRoute>,
                             RouteLocalLeader>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(RouteKind::Remote), CatalogRoute>,
                             RouteRemote>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(RouteKind::Unknown), CatalogRoute>,
                             RouteUnknown>);
static_assert(std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(RouteKind::Halted), CatalogRoute>,
                             RouteHalted>);
[[nodiscard]] inline RouteKind routeKindOf(const CatalogRoute& r) noexcept { return static_cast<RouteKind>(r.index()); }

/// "LocalLeader" | "Remote" | "Unknown" | "Halted" (logs, tests).
[[nodiscard]] inline const char* catalogRouteName(const CatalogRoute& r) noexcept {
    switch (routeKindOf(r)) {
    case RouteKind::LocalLeader: return "LocalLeader";
    case RouteKind::Remote:      return "Remote";
    case RouteKind::Unknown:     return "Unknown";
    case RouteKind::Halted:      return "Halted";
    }
    return "Unknown";
}

} // namespace rune::gateway
