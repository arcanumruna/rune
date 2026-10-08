#pragma once
// ---------------------------------------------------------------------------
// RecordsOpen.h — client<->broker TLS/AEAD (Task 22, shard worker DECRYPTS
// produce / per-worker ingress) — the raw-crypto-params OPEN step for a
// worker-ingress Produce frame.
//
// This is the ingress counterpart to RecordsSeal.h's egress SEAL (Task 19):
// same reasons for living here, aeron-free and node-level:
//   1. Aeron-free: unit-testable standalone (this repo's test host has no
//      Aeron — see GatewayCryptoTest.cpp's build/test split notes).
//   2. libs/node must not depend on libs/gateway: this header takes RAW
//      crypto bits (an AeadCipher*, a recvSeq/recvWindow pair) instead of
//      rune::gateway::CryptoState.
//
// Mirrors rune::gateway::codec::openIngressFrame (CryptoFrame.h, Task 8)
// EXACTLY: same wire layout ([24B MessageHeader][12B CryptoSubHeader]
// [ciphertext][16B tag], AAD = the 36 clear bytes), same anti-replay
// sliding-bitmap window, same check-then-authenticate-then-commit ordering
// (replayWindowCheck READ-ONLY before open(); replayWindowCommit ONLY after
// the AEAD tag verifies — a forged seq must never poison the window against
// a later, legitimate frame). This is a deliberate MIRROR, not a third
// hand-rolled copy of the SHAPE: libs/node literally cannot call the
// gateway's function (no libs/gateway dependency allowed), so the logic is
// reproduced byte-for-byte here instead.
//
// DIRECTION: ingress is a SINGLE sender (the client) -> ONE c2b key per
// session, no per-sender split (unlike the b2c egress — see
// 2026-10-08-per-worker-egress-crypto-addendum.md). Each worker keeps its
// OWN replay window for this session's c2b stream (WorkerSessionCrypto's own
// recvSeq/recvWindow, Rule 3) — the client seals EVERY client->broker frame
// under one c2b cipher + a single monotonic sendSeq, so produce-to-worker
// and control-to-gateway frames never collide in seq space; the worker's
// window simply sees its own subset of that stream (forward gaps where the
// gateway-bound frames were) — never shared with the gateway's own c2b
// window for the same session.
// ---------------------------------------------------------------------------
#include <rune/crypto/Aead.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/MessageHeader.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::node {

/// Raw crypto bits needed to open a worker-ingress Produce frame (Rule 3 —
/// exactly what the open step needs; no rune::gateway::CryptoState
/// dependency). `recv`/`recvSeq`/`recvWindow` are non-owning pointers into
/// the caller's own per-session crypto state (a shard worker's own
/// WorkerSessionCrypto fields, Task 22).
///
/// A null `recv` means "no c2b key installed yet" (a pre-key encrypted
/// session) — NOT plaintext. The caller only builds a ctx at all once it
/// has already found a per-session crypto entry (an encrypted session is
/// known by the presence of that entry — see Worker.h's
/// handleWorkerIngressFrame_); a ctx with a null `recv` must fail closed
/// (openProduceFrame returns 0, nothing is ever decoded as plaintext).
struct RecordsOpenCtx {
    rune::crypto::AeadCipher* recv       = nullptr;  ///< OPENs client->broker c2b ingress (null = no key yet)
    uint64_t*                 recvSeq    = nullptr;  ///< this session's replay-window high-water seq (worker-owned)
    uint64_t*                 recvWindow = nullptr;  ///< this session's replay-window bitmap (worker-owned)
};

namespace detail_open {

/// Read-only: would `seq` be accepted by the window (recvSeq, recvWindow)?
/// Byte-for-byte the same sliding-bitmap anti-replay window logic as
/// rune::gateway::codec::detail_crypto::replayWindowCheck (CryptoFrame.h):
///   - a seq AHEAD of the window is always acceptable (slides it forward
///     once committed);
///   - a seq INSIDE the window is acceptable exactly once (first time its
///     bit is clear) — a second delivery of the same seq is a replay;
///   - a seq more than 64 behind the window is too old, rejected.
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

}  // namespace detail_open

/// Open an inbound client->worker Produce ingress frame.  `wire` is the
/// encrypted frame exactly as it arrived: [24B MessageHeader][12B
/// CryptoSubHeader][ciphertext][16B tag]. Reconstructs [24B header]
/// [plaintext body] into `scratch` (caller-owned, reused — `wireLen` bytes
/// of capacity is always enough, since the reconstructed frame is
/// `kCryptoOverhead` (28) bytes SHORTER than the wire frame it came from).
///
/// Returns the reconstructed length, or 0 on: a misconfigured ctx (`recv`/
/// `recvSeq`/`recvWindow` null — no key installed yet: fail closed, never a
/// plaintext fallback), a truncated/undecodable frame, a seq the
/// anti-replay window refuses (too old or already seen), or an AEAD auth
/// failure (tampered ciphertext OR tampered header — the header is part of
/// the AAD). The window is committed ONLY after the tag verifies, so a
/// forged seq can never poison it against a later legitimate frame.
/// Lows batch C (D8c R3): open WITHOUT committing `seq` (`seqOut`) — for a
/// frame the worker may HOLD (controlled-poll ABORT + redelivery: a deferred
/// roll).  Commit it with commitProduceSeq once the frame is consumed: a
/// committed-then-held frame would fail its own redelivery as a replay and be
/// dropped.  The window check is read-only, so an uncommitted frame re-opens.
[[nodiscard]] inline std::size_t openProduceFrameUncommitted(
    const RecordsOpenCtx& ctx,
    const uint8_t* wire, std::size_t wireLen,
    uint8_t* scratch, std::size_t scratchCap, uint64_t& seqOut) noexcept {
    namespace rpc = rune::protocol::client;
    if (!ctx.recv || !ctx.recvSeq || !ctx.recvWindow) return 0;  // no key yet — fail closed, never plaintext

    constexpr std::size_t kMinWire = rpc::kCryptoHeaderBytes + rune::crypto::kTagSize;
    if (wireLen < kMinWire) return 0;

    const auto subOpt = rpc::CryptoSubHeader::decode(
        wire + rpc::MessageHeader::ENCODED_SIZE,
        wireLen - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return 0;
    const uint64_t seq = subOpt->seq;

    if (!detail_open::replayWindowCheck(*ctx.recvSeq, *ctx.recvWindow, seq)) return 0;

    const std::size_t ctLen    = wireLen - rpc::kCryptoHeaderBytes;
    const std::size_t plainCap = ctLen - rune::crypto::kTagSize;
    const std::size_t outLen   = rpc::MessageHeader::ENCODED_SIZE + plainCap;
    if (scratchCap < outLen) return 0;

    // ctx.recv is keyed c2b — OPENS client->broker ingress.
    const long plainLen = ctx.recv->open(
        seq, wire, rpc::kCryptoHeaderBytes,
        wire + rpc::kCryptoHeaderBytes, ctLen,
        scratch + rpc::MessageHeader::ENCODED_SIZE);
    if (plainLen < 0) return 0;  // auth failure — discard; window NOT updated

    std::memcpy(scratch, wire, rpc::MessageHeader::ENCODED_SIZE);  // clear header, unchanged
    seqOut = seq;
    return rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(plainLen);
}

/// Commit an opened frame's seq (openProduceFrameUncommitted) once it is consumed.
inline void commitProduceSeq(const RecordsOpenCtx& ctx, uint64_t seq) noexcept {
    detail_open::replayWindowCommit(*ctx.recvSeq, *ctx.recvWindow, seq);
}

/// Open and commit at once (a frame that is always consumed).  The window is
/// committed ONLY after the tag verifies, so a forged seq can never poison it.
[[nodiscard]] inline std::size_t openProduceFrame(
    const RecordsOpenCtx& ctx,
    const uint8_t* wire, std::size_t wireLen,
    uint8_t* scratch, std::size_t scratchCap) noexcept {
    uint64_t seq = 0;
    const std::size_t n = openProduceFrameUncommitted(ctx, wire, wireLen, scratch, scratchCap, seq);
    if (n != 0) commitProduceSeq(ctx, seq);
    return n;
}

} // namespace rune::node
