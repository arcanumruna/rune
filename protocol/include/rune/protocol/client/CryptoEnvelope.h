#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include "MessageHeader.h"   // detail::putLE16/getLE16/putLE64/getLE64

// ---------------------------------------------------------------------------
// client<->broker TLS/AEAD (Task 4) — crypto envelope wire format.
//
// This header is the pure wire FORMAT only: no OpenSSL, no aeron, no crypto
// operations. The actual AEAD seal/open lives in rune-crypto; this just
// defines the bytes that sit on the wire around the ciphertext.
//
// Frame layout for an encrypted client<->broker message:
//   [0..23]    MessageHeader      (24 bytes, clear — forms part of the AEAD AAD)
//   [24..35]   CryptoSubHeader    (12 bytes, clear — also part of the AAD)
//   [36..]     ciphertext || 16-byte AEAD tag
//
// kCryptoHeaderBytes (=36) is the AAD length (header + subheader).
// kCryptoOverhead (=28) is the total non-plaintext overhead added per frame
// (12-byte subheader + 16-byte tag) relative to the plaintext body size.
// ---------------------------------------------------------------------------
namespace rune::protocol::client {

// kCryptoSchemaId is defined in Protocol.h (included transitively via
// MessageHeader.h) — not redefined here to avoid an ODR clash.

/// CryptoSubHeader — 12 bytes, little-endian, written immediately after the
/// 24-byte MessageHeader (i.e. at frame offset 24).
///
/// Byte offsets (relative to the subheader itself):
///   version   uint8   0
///   cipher    uint8   1   (rune::crypto::Cipher value)
///   epoch     uint8   2   (low byte of the wire u16 @2; key rotation, 0 today)
///   senderId  uint8   3   (high byte of the wire u16 @2; 0 = gateway, i+1 = worker i)
///   seq       uint64  4
///
/// The wire format keeps a single little-endian uint16 at sub-offset 2 for
/// zero size growth: `epoch_lo | (senderId << 8)`. `epoch` and `senderId` are
/// therefore each limited to 0..255 on the wire — `epoch` stays a `uint16_t`
/// member for API continuity but must only ever hold its low byte.
/// Backward compatibility: with `senderId == 0` and `epoch == 0` (the shipped
/// default), the high byte is 0, so the encoded 12 bytes are byte-identical
/// to the pre-Task-18 format — see
/// CryptoEnvelopeTest.SubHeaderSenderIdZeroEpochZeroByteIdenticalToPreTask18.
struct CryptoSubHeader {
    uint8_t  version  = 1;
    uint8_t  cipher   = 0;   // rune::crypto::Cipher value
    uint16_t epoch    = 0;   // low byte only (0..255) — see class doc comment
    uint8_t  senderId = 0;   // 0 = gateway, i+1 = worker i
    uint64_t seq      = 0;

    static constexpr std::size_t ENCODED_SIZE = 12;

    /// Encodes all fields into at[0..11] as little-endian. `at` must be >= 12 bytes.
    void encode(uint8_t* at) const noexcept {
        at[0] = version;
        at[1] = cipher;
        detail::putLE16(at, 2, static_cast<uint16_t>(epoch) | (static_cast<uint16_t>(senderId) << 8));
        detail::putLE64(at, 4, static_cast<int64_t>(seq));
    }

    /// Decodes a subheader from buf[0..avail-1]. Returns nullopt if avail < 12.
    static std::optional<CryptoSubHeader> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;
        CryptoSubHeader h;
        h.version  = buf[0];
        h.cipher   = buf[1];
        const uint16_t raw = detail::getLE16(buf, 2);
        h.epoch    = raw & 0x00FF;
        h.senderId = static_cast<uint8_t>(raw >> 8);
        h.seq      = static_cast<uint64_t>(detail::getLE64(buf, 4));
        return h;
    }
};

static_assert(CryptoSubHeader::ENCODED_SIZE == 12);

/// AAD length: MessageHeader (24) + CryptoSubHeader (12).
inline constexpr std::size_t kCryptoHeaderBytes = MessageHeader::ENCODED_SIZE + CryptoSubHeader::ENCODED_SIZE; // 36

/// Per-frame overhead added by the crypto envelope beyond the plaintext body:
/// the 12-byte subheader plus a 16-byte AEAD tag.
inline constexpr std::size_t kCryptoOverhead = CryptoSubHeader::ENCODED_SIZE + 16; // 28

static_assert(kCryptoHeaderBytes == 36);
static_assert(kCryptoOverhead == 28);

} // namespace rune::protocol::client
