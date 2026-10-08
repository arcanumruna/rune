#pragma once
// TlsStartupPolicy.h — Task 9 review fix round 2: BrokerNode::buildGateway()'s
// startup-time TLS config validation, factored into small, pure, Aeron-free
// functions (same reasoning/pattern as rune::gateway::ConnectDowngradePolicy.h's
// per-Connect decision logic) so the fail-fast behaviour is unit-testable
// without Aeron, and without calling buildGateway() itself (which needs a
// live Aeron client).
//
// Both checks below are startup-time, cold-path refusals: throwing here
// means the broker never starts rather than silently serving a session in a
// way that contradicts its own configuration (closing two fail-open
// footguns the Task 9 review found).

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

/// Review fix (Minor, pairs with the above): shard mode can never serve an
/// encrypted session — Worker.h's per-worker Records egress stays plaintext
/// until Task 14 seals it (the ★ gating note; ClientGateway::handleConnect
/// already refuses any crypto offer in shard mode via
/// shardModeRefusesCryptoOffer). So shard mode + require_tls together would
/// reject EVERY Connect: shard mode refuses the crypto offer, and
/// require_tls refuses the plaintext fallback that would otherwise result —
/// a broker that silently bricks itself one Connect at a time. Fail loud at
/// startup instead of per-Connect.
[[nodiscard]] inline bool mustRefuseShardWithRequireTls(bool shardMode,
                                                         bool requireTls) noexcept {
    return shardMode && requireTls;
}

} // namespace rune::node
