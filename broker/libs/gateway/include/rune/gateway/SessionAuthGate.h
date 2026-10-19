#pragma once
// SessionAuthGate.h — what an UNAUTHENTICATED session may do while the broker requires authentication (authentication
// plan D11 / D13; controller ruling for the bootstrap window).  Per-state data (hard rule 3), one alternative per
// state; Task 11 grows this into the full per-session auth variant (SCRAM exchange, authenticated user,
// must-change-password) - the two refusing states below are the ones it starts from.
//
//   SessionOpen              — no gating: InProcessUnauthenticated / DevDisabled brokers (anonymous, full access).
//   SessionBootstrapOnly     — the broker requires authentication, the session sent no credentials (it cannot: Connect
//                              carries none) and `__security` holds NO Bootstrap event yet: ConnectResponse.authState =
//                              Anonymous, and the ONLY request accepted is SecurityBootstrap (80), whose cluster-key MAC
//                              is verified (D4).  Once a Bootstrap event is applied no new session is ever offered it.
//   SessionMustAuthenticate  — authState = MustAuthenticate: every request except the session-level
//                              Keepalive / Ping / Disconnect (and, from Task 11, the SCRAM templates 12-15) is answered
//                              AuthenticationFailed (44) in its own response type and the session ends (D13).

#include <variant>

namespace rune::gateway {

struct SessionOpen {};
struct SessionBootstrapOnly {};
struct SessionMustAuthenticate {};
using SessionAuthGate = std::variant<SessionOpen, SessionBootstrapOnly, SessionMustAuthenticate>;

/// How the broker authenticates sessions (what ConnectResponse.authMode reports).  The gateway's own copy of
/// NodeConfig::authMode, set by BrokerNode at start.
enum class GatewayAuthMode : uint8_t { InProcess = 0, Dev = 1, Required = 2 };

}  // namespace rune::gateway
