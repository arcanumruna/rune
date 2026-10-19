#pragma once
// SecurityRoute.h — where this broker sends a security DDL / bootstrap (authentication plan D36), the `__security`
// counterpart of CatalogRoute.h: a CONTROL-thread value BrokerNode computes each pass from the committed (-7, 0)
// assignment (single node: the consensus leader) and the latched kA2cSecurityHalted, and publishes to the gateway on
// the same thread only when it changes.  The alternatives are the catalog route's - RouteLocalLeader (submit to the
// admin ring), RouteRemote (Redirect 3 to the `__security` leader's client endpoint), RouteUnknown (NotLeader),
// RouteHalted (Internal) - so one value type serves both logs.  Reads (Subscribe -7) are never routed: any broker
// serves its own replica.

#include <rune/gateway/CatalogRoute.h>

namespace rune::gateway {

using SecurityRoute = CatalogRoute;

}  // namespace rune::gateway
