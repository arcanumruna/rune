#pragma once
#include <cstdint>
namespace rune::crypto {
enum class Cipher : uint8_t { None = 0, Aes256Gcm = 1, ChaCha20Poly1305 = 2 };
// True when OpenSSL will use a hardware AES-GCM implementation (AES-NI/VAES/ARMv8).
bool hasHardwareAes() noexcept;
// AES-256-GCM when hardware AES is present, else ChaCha20-Poly1305 (the no-HW-AES floor).
Cipher bestLocalCipher() noexcept;
}
