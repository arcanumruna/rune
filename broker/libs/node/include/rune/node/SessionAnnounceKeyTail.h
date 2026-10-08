#pragma once
// SessionAnnounceKeyTail.h — Task 19 (per-worker egress crypto addendum,
// 2026-10-08) + Task 22 (shard worker decrypts produce / per-worker
// ingress, 2026-10-08): the wire layout of the crypto material a
// SessionAnnounce carries to a shard worker, when the session is encrypted.
//
// WHY NO SBE SCHEMA CHANGE: WorkerOp is a PROCESS-LOCAL ring message (see
// rune-internal-ring.xml's own header comment) compiled into one binary —
// there is no cross-version wire-compat concern to protect, and the
// SessionAnnounce op already has unused scalar/tail capacity for exactly
// this (control-plane, cold, once per (session, worker)). Rather than add a
// new SBE field (and the cross-repo rune-protocol churn + a Java/sbe-tool
// regeneration this task cannot perform standalone), the crypto header rides
// as a fixed-size PREFIX of the SessionAnnounce op's EXISTING variable tail,
// ahead of the egress channel bytes — exactly the same "reuse what's already
// on the wire" discipline this codebase already uses for
// WorkerReturn::pushEffectiveBound()/pushRcNotReady() (F15 S4) and the
// WorkerOp `deferAck` flag byte (F15 S5).
//
// Layout: [cipher:u8][b2cPrefix:4 B][b2cKey:32 B][c2bPrefix:4 B][c2bKey:32 B][channel bytes...]
//   - cipher: rune::crypto::Cipher value — SHARED by both directions (the
//     session negotiates exactly one cipher; Task 22 adds no second byte).
//   - b2cPrefix/b2cKey (Task 19, egress / broker->client): the 4-byte AEAD
//     nonce prefix (the session's b2cPrefix — shared across every sender of
//     the session; only the KEY differs per sender) + the 32-byte key
//     deriveSenderKey(b2c, senderId) produced for THIS worker. senderId
//     itself does NOT ride here — the worker already knows its own identity
//     (workerId_ + 1) and stamps it locally at seal time.
//   - c2bPrefix/c2bKey (Task 22, ingress / client->broker): the session's
//     RAW c2b nonce prefix + key, UNCHANGED/UN-DERIVED and IDENTICAL across
//     every worker of the session — ingress has a SINGLE sender (the
//     client), so unlike b2c there is no per-worker derivation: every
//     worker opens the client's produce frames under the exact same c2b
//     key the gateway uses to open the client's control frames (each still
//     keeps its OWN replay window — see RecordsOpen.h's doc comment).
//
// Zero bytes added for a plaintext session: Worker::enqueueOp only calls
// encodeSessionAnnounceCryptoTail() when the op is encrypted; a plaintext
// SessionAnnounce's tail is the egress channel bytes alone, byte-identical
// to every pre-Task-19 build.
//
// Aeron-free (no Aeron/rune-crypto dependency at all — pure byte-copy
// functions) so this format is unit-testable standalone, same reasoning as
// RecordsSeal.h.
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>

namespace rune::node {

/// Fixed crypto-header length ahead of the channel bytes:
/// cipher(1) + b2cPrefix(4) + b2cKey(32) + c2bPrefix(4) + c2bKey(32).
inline constexpr std::size_t kSessionAnnounceCryptoHeaderBytes = 1 + 4 + 32 + 4 + 32;

/// Encode [cipher|b2cPrefix|b2cKey|c2bPrefix|c2bKey|channel] into `buf`
/// (capacity `bufCap`). Returns the total length written
/// (kSessionAnnounceCryptoHeaderBytes + channelLen), or 0 if `bufCap` is too
/// small — the caller MUST NOT enqueue a truncated/partial tail (same
/// "0 == do nothing" contract as RecordsSeal.h::sealRecordsFrame).
[[nodiscard]] inline std::size_t encodeSessionAnnounceCryptoTail(
        uint8_t* buf, std::size_t bufCap,
        uint8_t cipher,
        const uint8_t b2cPrefix[4], const uint8_t b2cKey[32],
        const uint8_t c2bPrefix[4], const uint8_t c2bKey[32],
        const char* channel, std::size_t channelLen) noexcept {
    const std::size_t total = kSessionAnnounceCryptoHeaderBytes + channelLen;
    if (!buf || bufCap < total) return 0;
    buf[0] = cipher;
    std::memcpy(buf + 1, b2cPrefix, 4);
    std::memcpy(buf + 5, b2cKey, 32);
    std::memcpy(buf + 37, c2bPrefix, 4);
    std::memcpy(buf + 41, c2bKey, 32);
    if (channelLen != 0) std::memcpy(buf + kSessionAnnounceCryptoHeaderBytes, channel, channelLen);
    return total;
}

struct SessionAnnounceCryptoTail {
    uint8_t           cipher      = 0;
    uint8_t           b2cPrefix[4] = {};
    uint8_t           b2cKey[32]   = {};
    uint8_t           c2bPrefix[4] = {};
    uint8_t           c2bKey[32]   = {};
    std::string_view  channel;   ///< view into the caller's buffer — same lifetime contract as the raw tail
};

/// Decode a tail previously written by encodeSessionAnnounceCryptoTail().
/// Returns nullopt if `len` is too short to hold the fixed header — fail
/// closed: the caller must NEVER install a key from a truncated/garbage
/// tail (treat as "no valid key", never as "plaintext").
[[nodiscard]] inline std::optional<SessionAnnounceCryptoTail> decodeSessionAnnounceCryptoTail(
        const char* buf, std::size_t len) noexcept {
    if (!buf || len < kSessionAnnounceCryptoHeaderBytes) return std::nullopt;
    SessionAnnounceCryptoTail t;
    t.cipher = static_cast<uint8_t>(buf[0]);
    std::memcpy(t.b2cPrefix, buf + 1, 4);
    std::memcpy(t.b2cKey, buf + 5, 32);
    std::memcpy(t.c2bPrefix, buf + 37, 4);
    std::memcpy(t.c2bKey, buf + 41, 32);
    t.channel = std::string_view(buf + kSessionAnnounceCryptoHeaderBytes, len - kSessionAnnounceCryptoHeaderBytes);
    return t;
}

} // namespace rune::node
