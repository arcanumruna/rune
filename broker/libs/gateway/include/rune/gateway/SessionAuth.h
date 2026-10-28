#pragma once
// SessionAuth.h - a session's authentication state (authentication plan D11 / D13; hard rule 3: each state holds
// exactly the variables it needs, no scattered booleans).  Owned by the CONTROL thread with its Session.
//
//   AuthAwaitingFirst         - the broker requires a login and the session has sent nothing yet (also: the bootstrap
//                               window, where only SecurityBootstrap is accepted besides AuthClientFirst).
//   AuthAwaitingFinal         - AuthClientFirst answered; the (copied) verifiers of the exchange wait for the proofs.
//                               ALWAYS two verifier slots (review A, D5/D8): a user with one password, an unknown user
//                               and an empty name get a deterministic FAKE second slot whose keys nothing matches, so
//                               the wire shape never reveals which account exists or how many passwords it has.
//   AuthPasswordChangeRequired- logged in with a one-time password: only `ALTER USER <self> SET PASSWORD` is allowed.
//   AuthAuthenticated         - logged in; `perms` is what the session may do (Task 12 compiles it).
//   AuthAnonymous             - DevDisabled / InProcessUnauthenticated brokers: allow all, no login ever.
//   AuthAnonymousRead         - namespaces Task 9 (spec §11): an authenticating broker with RUNE_ANONYMOUS_READ on (off
//                               by default); a session that sent a request without logging in.  Namespace `default`
//                               (nsId 0, D12), `perms` = Describe on its topics / views only; anything else is
//                               NotAuthorized (45) and the session stays.
//
// Replaces the Task 10 SessionAuthGate (open / bootstrap-only / must-authenticate).

#include <rune/crypto/Scram.h>
#include <rune/gateway/PermissionPool.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/protocol/client/Protocol.h>

#include <array>
#include <cstdint>
#include <string>
#include <variant>

namespace rune::gateway {

/// `correlationId` = the Connect's: the client matches an AuthServerFinal to its login episode by it, so the auth
/// deadline's answer (no AuthClientFirst seen yet) must carry it.
struct AuthAwaitingFirst {
    int64_t deadlineNs = 0;
    int64_t correlationId = 0;
    /// Issue #240: an accepted SecurityBootstrap restarts the login deadline ONCE per session (its answer follows the
    /// `__security` commit); a later bootstrap on the same session never extends it again.
    bool deadlineRestartedByBootstrap = false;
};

struct AuthAwaitingFinal {
    int64_t deadlineNs = 0;
    int64_t correlationId = 0;                ///< the AuthClientFirst's (the episode the answers belong to)
    uint64_t userId = 0;                      ///< 0 = unknown user / empty name (the fake verifiers, D8)
    std::string user;                         ///< as the client sent it ("" = no credentials)
    std::string clientFirstBare;              ///< "n=<user>,r=<clientNonce>"
    std::string combinedNonce;
    static constexpr uint8_t kSlots = 2;
    uint8_t count = kSlots;                   ///< always 2 on the wire (review A)
    std::array<rune::crypto::scram::Verifier, 2> verifiers;   ///< copies (the view may change before the final leg)
    /// #205 (PR #516 review): the state the verifiers were copied from - the view's applied offset at the first leg and
    /// whether it was behind events this broker held then.  A failed proof against a state that was behind, or that
    /// the view has since moved past, is not a verdict (retriable AuthUnavailable, never the terminal 44).
    int64_t viewAppliedAtFirst = -1;
    bool    viewBehindAtFirst = false;
    /// Issue #132: an AuthClientFinal passed the throttle (its verdict is the episode's charge; a stale view answers it
    /// retriable, uncharged).  A session that leaves this state WITHOUT one (the deadline, a disconnect, a final the
    /// throttle refused, any close) abandoned the first leg and is charged to the login throttle as a failure, once:
    /// first-leg probes are never free.
    bool    finalLegSeen = false;
    uint32_t nsId = 0;                        ///< namespaces Task 9: the resolved user's namespace (0 for the fakes)
};

// `loginNonce` (issue #114): the SCRAM combined nonce of the login that authenticated this session - what a change of
// the user's own credential must be proven over (credentialChangeMessage).  "" = none (a test-set state): no such
// change can be proven.
// `nsId` (namespaces Task 9): the session's namespace - the logged-in user's.  `user` stays the LOCAL name.
struct AuthPasswordChangeRequired { uint64_t userId = 0; std::string user; std::string loginNonce; uint32_t nsId = 0; };
struct AuthAuthenticated { uint64_t userId = 0; std::string user; PermissionRef perms; std::string loginNonce;   // #259: a pooled table
                           uint32_t nsId = 0; };
struct AuthAnonymous {};
struct AuthAnonymousRead { PermissionRef perms; };   // namespaces Task 9: always nsId 0 (`default`)

/// How the broker authenticates sessions (what ConnectResponse.authMode reports).  The gateway's own copy of
/// NodeConfig::authMode, set by BrokerNode at start.
enum class GatewayAuthMode : uint8_t { InProcess = 0, Dev = 1, Required = 2 };

using SessionAuth = std::variant<AuthAwaitingFirst, AuthAwaitingFinal, AuthPasswordChangeRequired, AuthAuthenticated,
                                 AuthAnonymous, AuthAnonymousRead>;

/// What the gate does with a request in the session's auth state.
enum class AuthGate : uint8_t { Allow, AnswerAuthFailedAndEnd, AnswerPasswordChangeRequired, AnswerNotAuthorized };

/// THE rule of what a session may send in its auth state (the gate before the dispatch switch).  Session-level
/// frames (Connect / HandshakeContinue / Keepalive / Ping / Disconnect) are always allowed; AuthAwaitingFirst allows
/// AuthClientFirst (12) and, while `bootstrapOpen`, SecurityBootstrap (80); AuthAwaitingFinal allows AuthClientFinal
/// (14); anything else there is AuthenticationFailed and the session ends (D13).  AuthPasswordChangeRequired allows a
/// SecurityDdl only when `isSelfSetPassword` (ALTER USER <self> SET PASSWORD), else PasswordChangeRequired (47) and
/// the session stays.  AuthAnonymousRead allows the Describe-gated reads (MetadataRequest, GetTopicSchema), else
/// NotAuthorized (45) and the session stays (namespaces Task 9).  Authenticated / anonymous sessions pass (what they may DO is the Authorizer's - Task 12).
[[nodiscard]] AuthGate gateFor(const SessionAuth&, rune::protocol::client::TemplateId, bool bootstrapOpen,
                               bool isSelfSetPassword) noexcept;

} // namespace rune::gateway
