// include/rune/gateway/CryptoFrame.h
//
// client<->broker TLS/AEAD (Task 8) — gateway SERVER-ROLE data-plane AEAD
// envelope seal/open.  The MIRROR of the client's
// rune/client/codec/CryptoFrame.h (Task 6) — same wire layout, same
// anti-replay window, same check-then-authenticate-then-commit ordering —
// parameterized over the broker's OWN rune::gateway::CryptoState instead of
// the client's rune::client::session::CryptoState (Rule 3: each state holds
// exactly the variables it needs; this is a mirror, not a fork of the logic).
//
// Pure wire-level helpers: no Aeron dependency (rune::gateway::CryptoState
// lives in its own aeron-free header, CryptoState.h — see that header's doc
// comment for why), so this header is unit-testable standalone (native, no
// aeron, a real rune::crypto::AeadCipher + OpenSSL). ClientGateway::
// onClientFrame_ / sendTo are the thin callers of these two functions.
//
// Wire layout for an encrypted client<->broker frame (see
// rune/protocol/client/CryptoEnvelope.h):
//   [0..23]   MessageHeader   (24B, clear — forms part of the AEAD AAD)
//   [24..35]  CryptoSubHeader (12B, clear — also part of the AAD)
//   [36..]    ciphertext || 16B AEAD tag
//
// Direction mapping (the MIRROR of the client's — task-7-interfaces.md's ★
// DIRECTION MAPPING trap):
//   crypto.recv OPENS client->broker ingress  (keyed from c2b/c2bPrefix)
//   crypto.send SEALS broker->client egress   (keyed from b2c/b2cPrefix)
//
// Anti-replay: CryptoState::recvSeq/recvWindow implement the standard
// sliding-bitmap window (64 sequence numbers) — see the client's
// CryptoFrame.h for the full rationale, reproduced identically here:
//   - a seq AHEAD of the window is always acceptable (it slides the window
//     forward once committed);
//   - a seq INSIDE the window is acceptable exactly once (first time its bit
//     is clear) — a second delivery of the same seq is a replay;
//   - a seq more than 64 behind the window is too old, rejected.
// The window is consulted READ-ONLY before authentication (a cheap reject
// for a definitely-bad seq) and committed ONLY after the AEAD tag verifies —
// a tampered frame carrying a forged seq must never poison the window
// against a later, legitimate frame using that same seq.
#pragma once

#include <rune/gateway/CryptoState.h>
#include <rune/crypto/Aead.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/MessageHeader.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::gateway::codec {

namespace detail_crypto {

/// Read-only: would `seq` be accepted by the window (recvSeq, recvWindow)?
/// seq == 0 is never valid — the seal path pre-increments sendSeq
/// (`++crypto.sendSeq`), so the first real seq is 1; 0 stays free as
/// "nothing received yet" (CryptoState's default-constructed recvSeq).
[[nodiscard]] inline bool replayWindowCheck(uint64_t recvSeq, uint64_t recvWindow,
                                             uint64_t seq) noexcept {
    if (seq == 0) return false;
    if (seq > recvSeq) return true;  // ahead of the window — always acceptable
    const uint64_t diff = recvSeq - seq;
    if (diff >= 64) return false;    // too old — outside the window
    return (recvWindow & (uint64_t{1} << diff)) == 0;  // not yet seen
}

/// Commit `seq` into the window.  Call ONLY after the AEAD tag has verified.
inline void replayWindowCommit(uint64_t& recvSeq, uint64_t& recvWindow,
                                uint64_t seq) noexcept {
    if (seq > recvSeq) {
        const uint64_t diff = seq - recvSeq;
        recvWindow = diff >= 64 ? uint64_t{1} : ((recvWindow << diff) | uint64_t{1});
        recvSeq = seq;
    } else {
        const uint64_t diff = recvSeq - seq;
        recvWindow |= (uint64_t{1} << diff);
    }
}

}  // namespace detail_crypto

/// Seal an outbound broker->client response frame.  `plain` is
/// [24B MessageHeader][body] — the caller's existing encode() already wrote
/// the correct sessionId, so the header is already part of the AAD;
/// `plainLen` >= MessageHeader::ENCODED_SIZE. Writes
/// [24B header][12B CryptoSubHeader][sealed body][16B tag] into `scratch`
/// (caller-owned, reused across calls — needs plainLen +
/// rune::protocol::client::kCryptoOverhead bytes of capacity).
/// Returns the sealed frame length (== plainLen + kCryptoOverhead), or 0 on
/// bad input / seal failure — the caller MUST NOT transmit on 0 (AeadCipher's
/// own contract: a cipher error must never be mistaken for success, which
/// would risk nonce/keystream reuse under the same key).
[[nodiscard]] inline std::size_t sealResponseFrame(
    CryptoState& crypto,
    const uint8_t* plain, std::size_t plainLen,
    uint8_t* scratch, std::size_t scratchCap) noexcept {
    namespace rpc = rune::protocol::client;
    if (plainLen < rpc::MessageHeader::ENCODED_SIZE || !crypto.send.has_value()) return 0;
    const std::size_t bodyLen = plainLen - rpc::MessageHeader::ENCODED_SIZE;
    const std::size_t outLen  = plainLen + rpc::kCryptoOverhead;
    if (scratchCap < outLen) return 0;

    // [0..23] header, clear — copy unchanged (part of the AAD).
    std::memcpy(scratch, plain, rpc::MessageHeader::ENCODED_SIZE);

    // [24..35] CryptoSubHeader, clear — also part of the AAD.
    const uint64_t seq = ++crypto.sendSeq;
    rpc::CryptoSubHeader sub;
    sub.version = 1;
    sub.cipher  = static_cast<uint8_t>(crypto.cipher);
    sub.epoch   = crypto.epoch;
    sub.seq     = seq;
    sub.encode(scratch + rpc::MessageHeader::ENCODED_SIZE);

    // [36..] sealed body || tag.  crypto.send is keyed b2c — SEALS
    // broker->client egress (the mirror of the client's crypto.recv, which
    // OPENS this same frame with the same b2c key).
    const std::size_t n = crypto.send->seal(
        seq, scratch, rpc::kCryptoHeaderBytes,
        plain + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
        scratch + rpc::kCryptoHeaderBytes);
    if (n == 0) return 0;  // AeadCipher::seal contract: do not transmit on 0
    return rpc::kCryptoHeaderBytes + n;  // == outLen
}

/// Open an inbound client->broker ingress frame.  `wire` is the encrypted
/// frame exactly as it arrived: [24B header][12B CryptoSubHeader][ciphertext]
/// [16B tag]. Reconstructs [24B header][plaintext body] into `scratch`
/// (caller-owned, reused — `wireLen` bytes of capacity is always enough,
/// since the reconstructed frame is `kCryptoOverhead` (28) bytes SHORTER
/// than the wire frame it came from).
/// Returns the reconstructed length, or 0 on: a truncated/undecodable frame,
/// a seq the anti-replay window refuses (too old or already seen), or an AEAD
/// auth failure (tampered ciphertext OR tampered header — the header is part
/// of the AAD).  The window is committed ONLY after the tag verifies, so a
/// forged seq can never poison it against a later legitimate frame.
[[nodiscard]] inline std::size_t openIngressFrame(
    CryptoState& crypto,
    const uint8_t* wire, std::size_t wireLen,
    uint8_t* scratch, std::size_t scratchCap) noexcept {
    namespace rpc = rune::protocol::client;
    constexpr std::size_t kMinWire = rpc::kCryptoHeaderBytes + rune::crypto::kTagSize;
    if (wireLen < kMinWire || !crypto.recv.has_value()) return 0;

    const auto subOpt = rpc::CryptoSubHeader::decode(
        wire + rpc::MessageHeader::ENCODED_SIZE,
        wireLen - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return 0;
    const uint64_t seq = subOpt->seq;

    if (!detail_crypto::replayWindowCheck(crypto.recvSeq, crypto.recvWindow, seq)) return 0;

    const std::size_t ctLen    = wireLen - rpc::kCryptoHeaderBytes;
    const std::size_t plainCap = ctLen - rune::crypto::kTagSize;
    const std::size_t outLen   = rpc::MessageHeader::ENCODED_SIZE + plainCap;
    if (scratchCap < outLen) return 0;

    // crypto.recv is keyed c2b — OPENS client->broker ingress (the mirror of
    // the client's crypto.send, which SEALS this same frame with the same
    // c2b key).
    const long plainLen = crypto.recv->open(
        seq, wire, rpc::kCryptoHeaderBytes,
        wire + rpc::kCryptoHeaderBytes, ctLen,
        scratch + rpc::MessageHeader::ENCODED_SIZE);
    if (plainLen < 0) return 0;  // auth failure — discard; window NOT updated

    detail_crypto::replayWindowCommit(crypto.recvSeq, crypto.recvWindow, seq);

    std::memcpy(scratch, wire, rpc::MessageHeader::ENCODED_SIZE);  // clear header, unchanged
    return rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(plainLen);
}

}  // namespace rune::gateway::codec
