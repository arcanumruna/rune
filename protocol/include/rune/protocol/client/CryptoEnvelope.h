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
///   version  uint8   0
///   cipher   uint8   1   (rune::crypto::Cipher value)
///   epoch    uint16  2
///   seq      uint64  4
struct CryptoSubHeader {
    uint8_t  version = 1;
    uint8_t  cipher  = 0;   // rune::crypto::Cipher value
    uint16_t epoch   = 0;
    uint64_t seq     = 0;

    static constexpr std::size_t ENCODED_SIZE = 12;

    /// Encodes all fields into at[0..11] as little-endian. `at` must be >= 12 bytes.
    void encode(uint8_t* at) const noexcept {
        at[0] = version;
        at[1] = cipher;
        detail::putLE16(at, 2, epoch);
        detail::putLE64(at, 4, static_cast<int64_t>(seq));
    }

    /// Decodes a subheader from buf[0..avail-1]. Returns nullopt if avail < 12.
    static std::optional<CryptoSubHeader> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;
        CryptoSubHeader h;
        h.version = buf[0];
        h.cipher  = buf[1];
        h.epoch   = detail::getLE16(buf, 2);
        h.seq     = static_cast<uint64_t>(detail::getLE64(buf, 4));
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
