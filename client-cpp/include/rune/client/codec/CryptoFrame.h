// include/rune/client/codec/CryptoFrame.h
//
// client<->broker TLS/AEAD (Task 6 / 6b) — data-plane AEAD envelope seal/open.
//
// Pure wire-level helpers that seal an outbound client->broker frame / open an
// inbound egress frame using a session's CryptoState (Task 5's
// rune::client::session::CryptoState).  No Aeron dependency — the only inputs
// are plain buffers and a CryptoState reference, so this header is
// unit-testable standalone (native, no aeron, a real rune::crypto::AeadCipher
// + OpenSSL).  sealFrame is generic — it seals ANY [24B header][body] frame,
// not just Produce (Task 6b: Keepalive/Disconnect/Subscribe/... reuse it too).
// ClientAgent::offerProduceToLeader_ / offerControlTo / offerIngress_ /
// offerOnSession / dispatchEgressFrame are thin callers of these two
// functions.
//
// Wire layout for an encrypted client<->broker frame (see
// rune/protocol/client/CryptoEnvelope.h):
//   [0..23]   MessageHeader   (24B, clear — forms part of the AEAD AAD)
//   [24..35]  CryptoSubHeader (12B, clear — also part of the AAD)
//   [36..]    ciphertext || 16B AEAD tag
//
// Anti-replay: CryptoState::recvSeq/recvWindow implement the standard
// sliding-bitmap window (64 sequence numbers, the Rule-3 "state holds exactly
// the variables it needs" shape already established by CryptoState itself):
//   - a seq AHEAD of the window is always acceptable (it will slide the
//     window forward once committed);
//   - a seq INSIDE the window is acceptable exactly once (first time its bit
//     is clear) — a second delivery of the same seq is a replay;
//   - a seq more than 64 behind the window is too old, rejected.
// The window is consulted READ-ONLY before authentication (a cheap reject for
// a definitely-bad seq) and committed ONLY after the AEAD tag verifies — a
// tampered frame carrying a forged seq must never poison the window against a
// later, legitimate frame using that same seq.
#pragma once

#include <rune/client/session/SessionMachine.h>
#include <rune/crypto/Aead.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/MessageHeader.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace rune::client::codec {

namespace detail_crypto {

/// Read-only: would `seq` be accepted by the window (recvSeq, recvWindow)?
/// seq == 0 is never valid — SessionMachine's seal path pre-increments
/// sendSeq (`++crypto.sendSeq`), so the first real seq is 1; 0 stays free as
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

/// Task 20 (per-sender broker->client recv): the {cipher, recvSeq, recvWindow}
/// triple openEgressFrame runs check->open->commit against for one senderId.
/// Non-owning — points INTO session::CryptoState (sender 0's existing fields,
/// or a workerRecv[] slot); `cipher == nullptr` means "fail closed, drop" (an
/// unencrypted session, an out-of-range senderId, or a key that failed to
/// derive). No field is ever swapped at the call site on a failed open — this
/// is purely cipher/window SELECTION, never a fallback/trial decrypt.
struct RecvSlot {
    rune::crypto::AeadCipher* cipher     = nullptr;
    uint64_t*                 recvSeq    = nullptr;
    uint64_t*                 recvWindow = nullptr;
};

/// Select (and, for a non-gateway sender, lazily derive+cache) the recv slot
/// for `senderId`. senderId 0 is the gateway — returns EXACTLY
/// crypto.recv/recvSeq/recvWindow, the pre-Task-20 fields, untouched: byte-
/// identical selection cost (one branch, no derivation, no cache lookup actually
/// IS a cache hit — it's the same fields openEgressFrame always used).
/// senderId k>=1 indexes crypto.workerRecv[k-1] (fixed-size, kMaxShardWorkers
/// wide): on first sight of that senderId, derives its key via
/// rune::crypto::deriveSenderKey(crypto.b2cKey, senderId, ...) and constructs
/// its AeadCipher ONCE (cold path); every subsequent frame from that sender
/// is an array-index cache hit — zero per-frame allocation. Fails closed
/// (returns a null RecvSlot) if the session isn't encrypted, senderId is out
/// of range, or derivation fails — the caller must drop rather than ever try
/// another sender's cipher.
[[nodiscard]] inline RecvSlot selectRecvSlot(session::CryptoState& crypto,
                                              uint8_t senderId) noexcept {
    if (!crypto.recv.has_value()) return {};  // not an encrypted session — fail closed
    if (senderId == 0) {
        return RecvSlot{&*crypto.recv, &crypto.recvSeq, &crypto.recvWindow};
    }
    const std::size_t idx = static_cast<std::size_t>(senderId) - 1;
    if (idx >= session::CryptoState::kMaxShardWorkers) return {};  // out of range — fail closed
    auto& slot = crypto.workerRecv[idx];
    if (!slot.cipher.has_value()) {
        rune::crypto::AeadKey derived{};
        if (!rune::crypto::deriveSenderKey(crypto.b2cKey, senderId, derived)) {
            return {};  // underivable — fail closed, no cipher cached
        }
        slot.cipher.emplace(crypto.cipher, derived, crypto.b2cPrefix);
    }
    return RecvSlot{&*slot.cipher, &slot.recvSeq, &slot.recvWindow};
}

}  // namespace detail_crypto

/// Issue #116: the {cipher, sendSeq} a c2b frame for `receiverId` is sealed with.  Receiver 0 (the gateway) - and every
/// frame of a legacy session (!crypto.receiverKeys) - is EXACTLY crypto.send/sendSeq, the pre-#116 fields.  Receiver
/// k >= 1 (shard worker k-1) is crypto.workerSend[k-1]: its cipher under deriveReceiverKey(c2bKey, k) is built ONCE,
/// on first use (cold); every later frame is an array-index hit (zero allocation).  A null cipher = refuse (an id above
/// the broker's max worker count, or a key that failed to derive) - never a fallback to another receiver's key.
struct SendSlot {
    rune::crypto::AeadCipher* cipher  = nullptr;
    uint64_t*                 sendSeq = nullptr;
    uint8_t                   receiverId = 0;   // what the subheader names (0 for every legacy frame)
};
[[nodiscard]] inline SendSlot selectSendSlot(session::CryptoState& crypto, uint8_t receiverId) noexcept {
    if (!crypto.send.has_value()) return {};
    if (!crypto.receiverKeys || receiverId == 0) return SendSlot{&*crypto.send, &crypto.sendSeq, 0};
    const std::size_t idx = static_cast<std::size_t>(receiverId) - 1;
    if (idx >= session::CryptoState::kMaxShardWorkers) return {};   // above the broker's max workers: refuse
    auto& slot = crypto.workerSend[idx];
    if (!slot.cipher.has_value()) {
        if (slot.deriveFailed) return {};   // #673: already failed this episode: not retried per offer
        rune::crypto::AeadKey derived{};
        if (!crypto.deriveReceiverKeyFor(receiverId, derived)) {   // fail closed
            slot.deriveFailed = true;
            ++crypto.receiverDeriveFailures;
            std::fprintf(stderr,
                         "rune client: receiver key %u failed to derive; frames for it are held until the session "
                         "reconnects\n",
                         static_cast<unsigned>(receiverId));
            return {};
        }
        slot.cipher.emplace(crypto.cipher, derived, crypto.c2bPrefix);
    }
    return SendSlot{&*slot.cipher, &slot.sendSeq, receiverId};
}

/// Seal an outbound client→broker frame.  NOT produce-specific — `plain` is a
/// generic [24B MessageHeader][body] for ANY non-handshake template (Produce,
/// Keepalive/Ping, Disconnect, Subscribe, Unsubscribe, Metadata, group/txn,
/// ...): the sessionId must already be patched in-place (every caller patches
/// — or already carries — the destination session's sessionId BEFORE calling
/// here, so the final header is part of the AAD); `plainLen` >=
/// MessageHeader::ENCODED_SIZE.  Writes
/// [24B header][12B CryptoSubHeader][sealed body][16B tag] into `scratch`
/// (caller-owned, reused across calls — needs plainLen +
/// rune::protocol::client::kCryptoOverhead bytes of capacity).
/// Issue #116: `receiverId` names the endpoint the frame is offered to - 0 the gateway (the node endpoint), k >= 1 shard
/// worker k-1 (its own ingress): on a receiver-key session the frame is sealed under that receiver's key and seq space
/// and the subheader's senderId carries the id; on a legacy session it is ignored (byte-identical pre-#116 frame).
/// Returns the sealed frame length (== plainLen + kCryptoOverhead), or 0 on
/// bad input / an unknown receiver / seal failure — the caller MUST NOT transmit on 0 (AeadCipher's
/// own contract: a cipher error must never be mistaken for success, which
/// would risk nonce/keystream reuse under the same key).
[[nodiscard]] inline std::size_t sealFrame(
    session::CryptoState& crypto,
    const uint8_t* plain, std::size_t plainLen,
    uint8_t* scratch, std::size_t scratchCap, uint8_t receiverId = 0) noexcept {
    namespace rpc = rune::protocol::client;
    if (plainLen < rpc::MessageHeader::ENCODED_SIZE || !crypto.send.has_value()) return 0;
    const std::size_t bodyLen = plainLen - rpc::MessageHeader::ENCODED_SIZE;
    const std::size_t outLen  = plainLen + rpc::kCryptoOverhead;
    if (scratchCap < outLen) return 0;
    // Issue #116: the receiver's own cipher and seq space (receiver 0 / a legacy session = crypto.send/sendSeq).
    const SendSlot slot = selectSendSlot(crypto, receiverId);
    if (slot.cipher == nullptr) return 0;   // unknown / underivable receiver: do not transmit

    // [0..23] header, clear — copy unchanged (part of the AAD).
    std::memcpy(scratch, plain, rpc::MessageHeader::ENCODED_SIZE);

    // [24..35] CryptoSubHeader, clear — also part of the AAD.  senderId names the RECEIVER on c2b (issue #116).
    const uint64_t seq = ++*slot.sendSeq;
    rpc::CryptoSubHeader sub;
    sub.version  = 1;
    sub.cipher   = static_cast<uint8_t>(crypto.cipher);
    sub.epoch    = crypto.epoch;
    sub.senderId = slot.receiverId;
    sub.seq      = seq;
    sub.encode(scratch + rpc::MessageHeader::ENCODED_SIZE);

    // [36..] sealed body || tag.
    const std::size_t n = slot.cipher->seal(
        seq, scratch, rpc::kCryptoHeaderBytes,
        plain + rpc::MessageHeader::ENCODED_SIZE, bodyLen,
        scratch + rpc::kCryptoHeaderBytes);
    if (n == 0) return 0;  // AeadCipher::seal contract: do not transmit on 0
    return rpc::kCryptoHeaderBytes + n;  // == outLen
}

/// Open an inbound egress frame.  `wire` is the encrypted frame exactly as it
/// arrived: [24B header][12B CryptoSubHeader][ciphertext][16B tag].
/// Reconstructs [24B header][plaintext body] into `scratch` (caller-owned,
/// reused — `wireLen` bytes of capacity is always enough, since the
/// reconstructed frame is `kCryptoOverhead` (28) bytes SHORTER than the wire
/// frame it came from).
/// Returns the reconstructed length, or 0 on: a truncated/undecodable frame,
/// a seq the anti-replay window refuses (too old or already seen), or an AEAD
/// auth failure (tampered ciphertext OR tampered header — the header is part
/// of the AAD).  The window is committed ONLY after the tag verifies, so a
/// forged seq can never poison it against a later legitimate frame.
/// Task 20: `wire`'s CryptoSubHeader.senderId (clear, authenticated-via-AAD)
/// selects WHICH sender's {cipher, recvSeq, recvWindow} this frame is opened
/// against — senderId 0 is the gateway (byte-identical to pre-Task-20: the
/// same crypto.recv/recvSeq/recvWindow fields, same checks, same order), and
/// senderId k>=1 is shard worker k-1's independently-keyed, independently-
/// windowed stream (see detail_crypto::selectRecvSlot). Exactly ONE open()
/// call is ever made, against the selected sender's cipher — an unknown or
/// underivable senderId drops the frame (fail closed); there is no second/
/// trial decrypt against any other sender's key.
[[nodiscard]] inline std::size_t openEgressFrame(
    session::CryptoState& crypto,
    const uint8_t* wire, std::size_t wireLen,
    uint8_t* scratch, std::size_t scratchCap) noexcept {
    namespace rpc = rune::protocol::client;
    constexpr std::size_t kMinWire = rpc::kCryptoHeaderBytes + rune::crypto::kTagSize;
    if (wireLen < kMinWire) return 0;

    const auto subOpt = rpc::CryptoSubHeader::decode(
        wire + rpc::MessageHeader::ENCODED_SIZE,
        wireLen - rpc::MessageHeader::ENCODED_SIZE);
    if (!subOpt) return 0;
    const uint64_t seq      = subOpt->seq;
    const uint8_t  senderId = subOpt->senderId;

    // One array-index selection on a byte already parsed (senderId 0 is a
    // direct reference to the pre-Task-20 fields; k>=1 is, after the cold
    // first-sight derivation, an O(1) cache hit) — no trial decrypt.
    const detail_crypto::RecvSlot slot = detail_crypto::selectRecvSlot(crypto, senderId);
    if (slot.cipher == nullptr) return 0;  // unknown/underivable sender — fail closed

    if (!detail_crypto::replayWindowCheck(*slot.recvSeq, *slot.recvWindow, seq)) return 0;

    const std::size_t ctLen    = wireLen - rpc::kCryptoHeaderBytes;
    const std::size_t plainCap = ctLen - rune::crypto::kTagSize;
    const std::size_t outLen   = rpc::MessageHeader::ENCODED_SIZE + plainCap;
    if (scratchCap < outLen) return 0;

    const long plainLen = slot.cipher->open(
        seq, wire, rpc::kCryptoHeaderBytes,
        wire + rpc::kCryptoHeaderBytes, ctLen,
        scratch + rpc::MessageHeader::ENCODED_SIZE);
    if (plainLen < 0) return 0;  // auth failure — discard; window NOT updated

    detail_crypto::replayWindowCommit(*slot.recvSeq, *slot.recvWindow, seq);

    std::memcpy(scratch, wire, rpc::MessageHeader::ENCODED_SIZE);  // clear header, unchanged
    return rpc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(plainLen);
}

}  // namespace rune::client::codec
