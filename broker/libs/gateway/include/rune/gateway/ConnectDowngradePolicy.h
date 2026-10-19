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
// ★ shard-mode gating note — HISTORICAL, resolved by Task 19 (per-worker
// egress crypto addendum, 2026-10-08): per-worker SHARD-mode Records egress
// (Worker.h, the sendRecordsFrame call in the per-worker push loop) used to
// be PLAINTEXT-only, so shard mode REFUSED any crypto offer outright (see
// the now-removed shardModeRefusesCryptoOffer / mustRefuseShardWithRequireTls).
// Task 19 made the shard worker SEAL its own Records egress with its own
// per-sender key (RecordsSeal.h's senderId, keys shipped via SessionAnnounce
// — see SessionAnnouncer::announce()), so shard mode now serves encrypted
// sessions exactly like legacy mode. This note stays only as a pointer for
// anyone finding references to the removed functions in old commits/briefs.

namespace rune::gateway {

/// Whether handleConnect should drive the server-role TLS handshake at all
/// for this Connect's crypto offer. false => TlsSession::makeServer /
/// readInbound is skipped entirely — no crypto engaged, because the client
/// didn't offer it or this broker has no cert/key configured. Task 19:
/// shard mode is no longer special-cased here (it used to refuse crypto
/// outright — see the file header note).
[[nodiscard]] inline bool shouldEngageCrypto(bool clientOffersCrypto,
                                              bool brokerHasTls) noexcept {
    return clientOffersCrypto && brokerHasTls;
}

/// require_tls (Task 9, spec §11 downgrade safety): true => the Connect
/// MUST be rejected with ConnectResponse{HandshakeFailed} rather than
/// served plaintext. `cryptoEstablished` is whether this connect ended up
/// with a live server-role TlsSession after shouldEngageCrypto/readInbound
/// ran — e.g. a plaintext client, no broker cert, or a local TLS init
/// failure (cryptoEstablished=false) is correctly rejected when require_tls
/// is set, rather than silently served plaintext.
[[nodiscard]] inline bool mustRejectForRequireTls(bool requireTls,
                                                    bool cryptoEstablished) noexcept {
    return requireTls && !cryptoEstablished;
}

} // namespace rune::gateway
