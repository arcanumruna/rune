#pragma once
// ConnectDowngradePolicy.h — Task 9: require_tls hard-reject + shard-mode
// crypto gating, factored out of ClientGateway::handleConnect into small,
// pure, Aeron-free functions.
//
// WHY a separate header: ClientGateway.h transitively includes <Aeron.h>,
// which does not exist on this repo's macOS dev host (same reason Task 8
// extracted CryptoState.h / CryptoFrame.h out of SessionTable.h — see
// GatewayCryptoTest.cpp's file header). These decision functions carry no
// Aeron/session state at all, so they can be unit-tested directly with
// crafted booleans, standalone, without a real ClientGateway/Aeron.
//
// ★ shard-mode gating note (carried from the Task 8b review): per-worker
// SHARD-mode Records egress (Worker.h, the sendRecordsFrame call in the
// per-worker push loop) is still PLAINTEXT — sealing that path is Task 14's
// scope. An encrypted session accepted here would have its Records served
// in the CLEAR by a shard-mode worker: a silent plaintext leak the client
// believes is encrypted (its SessionMachine would hold b2c/c2b AEAD keys
// and try to open plaintext Records bytes under them). So shard mode
// REFUSES any crypto offer outright — handleConnect must mark the connect
// cryptoFailed (never silently fall back to "just serve it plaintext",
// which would defeat requireCrypto on the client with no signal at all).
// Shard + TLS needs Task 14 to seal Worker.h's Records egress before this
// gating can be lifted.

namespace rune::gateway {

/// Whether handleConnect should drive the server-role TLS handshake at all
/// for this Connect's crypto offer. false => TlsSession::makeServer /
/// readInbound is skipped entirely — no crypto engaged, either because the
/// client didn't offer it, this broker has no cert/key configured, or (Task
/// 9 gating, see file header) the broker is in shard mode.
[[nodiscard]] inline bool shouldEngageCrypto(bool shardMode,
                                              bool clientOffersCrypto,
                                              bool brokerHasTls) noexcept {
    return clientOffersCrypto && brokerHasTls && !shardMode;
}

/// Task 9 ★ gating: true => this Connect's crypto offer must be REFUSED
/// (handleConnect sets cryptoFailed; pollPendingConnects answers
/// ConnectResponse{HandshakeFailed}, unconditionally — never served
/// plaintext either) because a shard-mode worker would otherwise serve this
/// session's Records in the clear. Only fires when the offer would
/// otherwise have been accepted (clientOffersCrypto && brokerHasTls); a
/// plaintext offer in shard mode is unaffected (shard mode was
/// plaintext-only before Task 9 and stays so for a non-offering client).
[[nodiscard]] inline bool shardModeRefusesCryptoOffer(bool shardMode,
                                                        bool clientOffersCrypto,
                                                        bool brokerHasTls) noexcept {
    return shardMode && clientOffersCrypto && brokerHasTls;
}

/// require_tls (Task 9, spec §11 downgrade safety): true => the Connect
/// MUST be rejected with ConnectResponse{HandshakeFailed} rather than
/// served plaintext. `cryptoEstablished` is whether this connect ended up
/// with a live server-role TlsSession after shouldEngageCrypto/readInbound
/// ran — so a shard-mode-refused crypto offer (cryptoEstablished=false) is
/// correctly rejected too when require_tls is set, rather than silently
/// served plaintext (that would defeat require_tls exactly the way shard
/// mode's plaintext egress defeats requireCrypto).
[[nodiscard]] inline bool mustRejectForRequireTls(bool requireTls,
                                                    bool cryptoEstablished) noexcept {
    return requireTls && !cryptoEstablished;
}

} // namespace rune::gateway
