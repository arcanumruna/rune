#pragma once

// client<->broker TLS/AEAD (Task 7, extracted in Task 8) — rune::gateway::
// CryptoState, the broker's OWN per-session crypto state (NOT the client's
// rune::client::session::CryptoState — a separate type, same shape, by
// design: Rule 3, "each state holds exactly the variables it needs").
// Populated once a server-role TlsSession reaches done() and exportKeys()
// returns a real cipher (ClientGateway::handleHandshakeContinue_). Direction
// mapping is the MIRROR of the client's (task-7-interfaces.md's "DIRECTION
// MAPPING" trap):
//   recv <- c2b/c2bPrefix  (OPENS client->broker ingress)
//   send <- b2c/b2cPrefix  (SEALS broker->client egress)
//
// Lives in its own header (Task 8) so the data-plane codec
// (rune/gateway/CryptoFrame.h) — and anything that unit-tests it standalone —
// does NOT have to pull in <Aeron.h>: SessionTable.h (which embeds this
// struct inside Session) is part of rune_gateway's PUBLIC interface and
// already includes <Aeron.h>, but CryptoState itself has no Aeron dependency
// at all, so splitting it out keeps the aeron-free data-plane logic
// genuinely aeron-free (see GatewayCryptoTest.cpp's build/test split notes).

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>

#include <cstdint>
#include <optional>

namespace rune::gateway {

struct CryptoState {
    bool                                     encrypted  = false;
    rune::crypto::Cipher                     cipher{};
    uint16_t                                 epoch      = 0;
    uint64_t                                 sendSeq    = 0;
    uint64_t                                 recvSeq    = 0;
    uint64_t                                 recvWindow = 0;
    std::optional<rune::crypto::AeadCipher>  send, recv;

    // Task 19 (per-worker egress crypto addendum, 2026-10-08): the RAW b2c
    // key + 4-byte nonce prefix, retained alongside `send` (which already
    // holds the AeadCipher built FROM these same bytes) so the gateway can
    // derive each shard worker's own per-sender key
    // (rune::crypto::deriveSenderKey(b2c, workerId+1, ...)) at SessionAnnounce
    // time — shard mode only; legacy mode never reads these. Zero-valued
    // (all-zero key/prefix) for every plaintext session, exactly like every
    // other CryptoState field.
    rune::crypto::AeadKey                    b2c{};
    uint8_t                                  b2cPrefix[4] = {};

    // Task 22 (shard worker decrypts produce / per-worker ingress,
    // 2026-10-08): the RAW c2b key + 4-byte nonce prefix, retained alongside
    // `recv` (which already holds the AeadCipher built FROM these same
    // bytes) so the gateway can ship the SAME bytes to every shard worker at
    // SessionAnnounce time — ingress has a SINGLE sender (the client), so
    // (unlike b2c) no per-worker derivation: every worker opens produce
    // under this exact key. Shard mode only; legacy mode never reads this.
    // Zero-valued for every plaintext session.
    rune::crypto::AeadKey                    c2b{};
    uint8_t                                  c2bPrefix[4] = {};
};

} // namespace rune::gateway
