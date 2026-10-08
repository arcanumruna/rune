#pragma once
// ---------------------------------------------------------------------------
// RecordsSeal.h — client<->broker TLS/AEAD (Task 8b) — the raw-crypto-params
// seal step for a Records push frame.
//
// Factored out of RecordsPush.h for TWO reasons:
//   1. Aeron-free: RecordsPush.h unconditionally pulls in <ExclusivePublication.h>
//      (it needs aeron::ExclusivePublication& for the real send path), so it
//      cannot be included on a host with no Aeron (this repo's test host —
//      see GatewayCryptoTest.cpp's build/test split notes). The seal step
//      itself has nothing to do with Aeron, so it lives here instead, where
//      it is unit-testable standalone.
//   2. libs/node must not depend on libs/gateway: this header takes RAW
//      crypto bits (an AeadCipher*, a sendSeq counter, a cipher id, an
//      epoch) instead of rune::gateway::CryptoState, so it has zero gateway
//      dependency. Both the gateway's ClientGateway::pushRecords_ (now) and
//      the per-worker egress (Task 14, shard mode, once per-worker session
//      keys are installed) reach the SAME sealRecordsFrame() with their own
//      crypto state's raw fields — the one seam both callers share.
//
// Mirrors rune::gateway::codec::sealResponseFrame (Task 8, CryptoFrame.h)
// EXACTLY: same wire layout ([24B MessageHeader][12B CryptoSubHeader]
// [ciphertext][16B tag], AAD = the 36 clear bytes), same seq pre-increment
// (seq = ++*sendSeq BEFORE encoding the subheader), same AeadCipher::seal
// call, same "0 == never transmit" contract. This is a deliberate MIRROR,
// not a third hand-rolled envelope: the two functions share the exact same
// underlying primitives (rune::protocol::client::CryptoSubHeader::encode +
// rune::crypto::AeadCipher::seal) in the exact same order — the only
// difference is which struct carries the crypto fields (CryptoState vs this
// header's raw RecordsSealCtx), the same split Task 7/8 already established
// between the gateway's own CryptoState and the client's CryptoState
// (Rule 3: each state holds exactly the variables it needs).
// ---------------------------------------------------------------------------
#include <rune/crypto/Aead.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/MessageHeader.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::node {

/// Raw crypto bits needed to seal a Records push frame (Rule 3 — exactly
/// what the seal step needs; no rune::gateway::CryptoState dependency).
/// `send`/`sendSeq` are non-owning pointers into the caller's own crypto
/// state (e.g. `&*session.crypto.send` / `&session.crypto.sendSeq` on the
/// gateway; a per-worker session's own fields for Task 14).
///
/// A default-constructed (all-null) ctx means "misconfigured" — NOT
/// plaintext. The discriminant between a plaintext and an encrypted Records
/// push is a null `RecordsSealCtx*` at the sendRecordsFrame() call site
/// (see RecordsPush.h), never a null field inside an otherwise-present ctx:
/// a caller that believes the session is encrypted but passes a ctx with a
/// null `send` (key not yet installed) must fail closed (sealRecordsFrame
/// returns 0, nothing is ever transmitted in the clear).
struct RecordsSealCtx {
    rune::crypto::AeadCipher* send    = nullptr;  ///< SEALs (b2c on the gateway)
    uint64_t*                 sendSeq = nullptr;  ///< ++*sendSeq per sealed frame
    uint8_t                   cipher  = 0;        ///< rune::crypto::Cipher value
    uint16_t                  epoch   = 0;
};

/// Seal a contiguous Records plaintext frame `plain` ([24B MessageHeader]
/// [body] — the caller's own Records::encode() already wrote the correct
/// sessionId, so the header is already part of the AAD; `plainLen` >=
/// MessageHeader::ENCODED_SIZE) into `sealed`
/// ([24B header][12B CryptoSubHeader][ciphertext][16B tag], `sealedCap` >=
/// plainLen + rune::protocol::client::kCryptoOverhead).
/// Returns the sealed length (== plainLen + kCryptoOverhead), or 0 on: bad
/// input, a misconfigured ctx (`send`/`sendSeq` null), or an AEAD seal
/// failure — the caller MUST NOT transmit on 0 (AeadCipher's own contract:
/// a cipher error must never be mistaken for success, which would risk
/// nonce/keystream reuse under the same key).
[[nodiscard]] inline std::size_t sealRecordsFrame(
    const RecordsSealCtx& ctx,
    const uint8_t* plain, std::size_t plainLen,
    uint8_t* sealed, std::size_t sealedCap) noexcept {
    namespace rpc = rune::protocol::client;
    if (!ctx.send || !ctx.sendSeq) return 0;  // misconfigured — never transmit plaintext
    if (plainLen < rpc::MessageHeader::ENCODED_SIZE) return 0;
    const std::size_t bodyLen = plainLen - rpc::MessageHeader::ENCODED_SIZE;
    const std::size_t outLen  = plainLen + rpc::kCryptoOverhead;
    if (sealedCap < outLen) return 0;

    // [0..23] header, clear — copy unchanged (part of the AAD).
    std::memcpy(sealed, plain, rpc::MessageHeader::ENCODED_SIZE);

    // [24..35] CryptoSubHeader, clear — also part of the AAD.
    const uint64_t seq = ++(*ctx.sendSeq);
    rpc::CryptoSubHeader sub;
    sub.version = 1;
    sub.cipher  = ctx.cipher;
    sub.epoch   = ctx.epoch;
    sub.seq     = seq;
    sub.encode(sealed + rpc::MessageHeader::ENCODED_SIZE);

    // [36..] sealed body || tag.
    const std::size_t n = ctx.send->seal(
        seq, sealed, rpc::kCryptoHeaderBytes,
        plain + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
        sealed + rpc::kCryptoHeaderBytes);
    if (n == 0) return 0;  // AeadCipher::seal contract: do not transmit on 0
    return rpc::kCryptoHeaderBytes + n;  // == outLen
}

} // namespace rune::node
