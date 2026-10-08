#pragma once
// TlsStartupPolicy.h — Task 9 review fix round 2: BrokerNode::buildGateway()'s
// startup-time TLS config validation, factored into small, pure, Aeron-free
// functions (same reasoning/pattern as rune::gateway::ConnectDowngradePolicy.h's
// per-Connect decision logic) so the fail-fast behaviour is unit-testable
// without Aeron, and without calling buildGateway() itself (which needs a
// live Aeron client).
//
// The check below is a startup-time, cold-path refusal: throwing here means
// the broker never starts rather than silently serving a session in a way
// that contradicts its own configuration (closing a fail-open footgun the
// Task 9 review found). A second such check, mustRefuseShardWithRequireTls,
// was removed by Task 19 once shard mode could serve crypto (see its own
// removal note below).

#include <string>

namespace rune::node {

/// "TLS configured" = at least one of the cert/key FILE PATHS is non-empty.
/// A partial config (only RUNE_TLS_CERT_FILE or only RUNE_TLS_KEY_FILE set)
/// still counts as configured — it must fail fast below, never silently
/// degrade to plaintext as if TLS had never been mentioned.
[[nodiscard]] inline bool tlsConfigured(const std::string& certPath,
                                         const std::string& keyPath) noexcept {
    return !certPath.empty() || !keyPath.empty();
}

/// Review fix (Important): true iff BrokerNode::buildGateway() must THROW
/// instead of starting — independent of require_tls. Fires whenever TLS was
/// configured (see tlsConfigured) but the cert and key did NOT both load
/// into non-empty PEM text (missing file, unreadable, empty, or only one of
/// the two paths set — loadPemFile() returns false for an empty path too,
/// so `certLoaded`/`keyLoaded` already cover that case). Before this fix,
/// a configured-but-broken cert only threw when require_tls was ALSO set;
/// with require_tls off it logged and silently cleared to plaintext — a
/// fail-open footgun this closes.
[[nodiscard]] inline bool mustRefuseStartForUnloadableTls(bool configured,
                                                           bool certLoaded,
                                                           bool keyLoaded) noexcept {
    return configured && !(certLoaded && keyLoaded);
}

// Review fix (Minor, pairs with the above) — REMOVED by Task 19 (per-worker
// egress crypto addendum, 2026-10-08): mustRefuseShardWithRequireTls used to
// refuse starting a broker with both shard mode and require_tls set, because
// shard mode could not yet serve an encrypted session at all (Worker.h's
// per-worker Records egress was plaintext-only). Task 19 made the shard
// worker seal its own Records egress with its own per-sender key, so shard
// mode now serves crypto exactly like legacy mode — the combination is no
// longer a dead end and this startup refusal is gone.

} // namespace rune::node
