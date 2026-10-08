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
};

} // namespace rune::gateway
