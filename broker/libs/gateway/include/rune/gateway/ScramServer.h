#pragma once
// ScramServer.h - the broker half of one SCRAM-SHA-512 exchange (RFC 5802 / 7677, authentication plan D5 / D7 / D8,
// review A).  PURE functions of (SecurityState, request): no I/O, no clock, no session table - the gateway owns the
// state (AuthAwaitingFinal) and the wire.  The server NEVER runs PBKDF2 (the client does, once per verifier, and
// caches it): verifying a proof is three hash / HMAC operations, microseconds - the control loop is never stalled by
// a login, which is what the plan's 5 ms gate bounds (the client's own PBKDF2 at 4096 iterations dominates it).
//
// Review A (binding): the broker ALWAYS lists TWO (salt, iterations) pairs.  A user with one password, an unknown
// user and an empty name get a deterministic fake second salt, so the AuthServerFirst frames are shape-identical.
// A fake slot's keys are random per exchange: no proof can match it.  The unknown-user salts are
// HMAC-SHA-512(fakeSaltSeed, "rune-fake-salt|"/"rune-fake-salt2|" + name)[0..16] where the 32-byte seed is the one
// the Bootstrap event carries (the same on every broker and across restarts); before the first bootstrap there are
// no users to hide and the seed is all zeros.

#include <rune/gateway/SessionAuth.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/security/SecurityState.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::gateway {

/// AuthClientFirst decoded leniently: the generated decoder refuses an EMPTY username (1..128), but a client without
/// credentials sends one and the plan requires the answer to be indistinguishable from an unknown user - so the
/// frame is parsed here with the same layout and an empty username allowed.  nullopt = structurally invalid (or a
/// username over 128 bytes / a nonce outside 1..128): the gateway answers AuthServerFinal{44} and ends the session.
[[nodiscard]] std::optional<rune::protocol::client::AuthClientFirst> decodeClientFirst(const uint8_t* buf, std::size_t len) noexcept;

struct ServerFirstPlan {
    AuthAwaitingFinal next;
    std::vector<uint8_t> frame;     ///< the encoded AuthServerFirst; EMPTY = local failure (no randomness): answer 44
};
/// Always succeeds in shape: a known user (enabled or not - a disabled account fails at the final leg, Review Focus 1)
/// gets its 1-2 verifiers (+ a fake second slot for one), an unknown / empty name gets two fake slots.
/// combinedNonce = clientNonce + a fresh server nonce.
[[nodiscard]] ServerFirstPlan scramServerFirst(const rune::security::SecurityState&,
                                               const rune::protocol::client::AuthClientFirst&, int64_t deadlineNs);

enum class LoginFailure : uint8_t { NoCredentials, UnknownUser, BadProof, Disabled, Throttled, Timeout, SecurityHalted };
struct LoginOk { uint64_t userId; std::string user; bool mustChangePassword; std::array<uint8_t, 64> serverSignature; };
/// Checks the proofs against the (copied) verifiers of `state`, re-reading the user from the CURRENT state: a user
/// dropped between the legs is UnknownUser, a user disabled in between is Disabled.  Every slot is verified (no early
/// exit) so the time does not depend on which slot - or whether any - matched.
[[nodiscard]] std::variant<LoginOk, LoginFailure> scramServerFinal(const rune::security::SecurityState&,
                                                                   const AuthAwaitingFinal&,
                                                                   const rune::protocol::client::AuthClientFinal&);
/// Logs only - NEVER on the wire (Review Focus 1): "no-credentials", "unknown-user", "bad-proof", "disabled",
/// "throttled", "timeout", "security-halted".
[[nodiscard]] std::string_view loginFailureName(LoginFailure) noexcept;

} // namespace rune::gateway
