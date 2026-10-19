// rune-crypto Kdf: OpenSSL 3 EVP primitives; every step checked, any failure returns false (fail closed).
#include "rune/crypto/Kdf.h"
#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/rand.h>
#include <cstring>

namespace rune::crypto {
namespace {
const uint8_t kEmpty = 0;   // a non-null pointer for empty spans (OpenSSL dislikes null with len 0 in places)
const uint8_t* ptr(std::span<const uint8_t> s) noexcept { return s.empty() ? &kEmpty : s.data(); }
const EVP_MD* mdFor(Digest d) noexcept { return d == Digest::Sha256 ? EVP_sha256() : EVP_sha512(); }
char* nameFor(Digest d) noexcept { return const_cast<char*>(d == Digest::Sha256 ? "SHA256" : "SHA512"); }
std::span<const uint8_t> sv(std::string_view s) noexcept { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
} // namespace

bool DigestBytes::operator==(const DigestBytes& o) const noexcept {
    return size == o.size && CRYPTO_memcmp(bytes.data(), o.bytes.data(), size) == 0;
}

bool hash(Digest d, std::span<const uint8_t> msg, DigestBytes& out) noexcept {
    unsigned n = 0;
    if (EVP_Digest(ptr(msg), msg.size(), out.bytes.data(), &n, mdFor(d), nullptr) != 1 || n != digestSize(d)) { out.size = 0; return false; }
    out.size = static_cast<uint8_t>(n);
    return true;
}

bool hmac(Digest d, std::span<const uint8_t> key, std::span<const uint8_t> msg, DigestBytes& out) noexcept {
    out.size = 0;
    EVP_MAC* mac = EVP_MAC_fetch(nullptr, "HMAC", nullptr);
    if (!mac) return false;
    EVP_MAC_CTX* ctx = EVP_MAC_CTX_new(mac);
    EVP_MAC_free(mac);   // ctx holds its own reference
    if (!ctx) return false;
    OSSL_PARAM params[2] = {OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, nameFor(d), 0), OSSL_PARAM_construct_end()};
    size_t n = 0;
    const bool ok = EVP_MAC_init(ctx, ptr(key), key.size(), params) == 1 &&
                    EVP_MAC_update(ctx, ptr(msg), msg.size()) == 1 &&
                    EVP_MAC_final(ctx, out.bytes.data(), &n, out.bytes.size()) == 1 && n == digestSize(d);
    EVP_MAC_CTX_free(ctx);
    if (!ok) return false;
    out.size = static_cast<uint8_t>(n);
    return true;
}

bool pbkdf2(Digest d, std::string_view password, std::span<const uint8_t> salt, uint32_t iterations, DigestBytes& out) noexcept {
    out.size = 0;
    if (iterations == 0 || salt.size() > 0x7fffffff || password.size() > 0x7fffffff) return false;
    const char* pw = password.empty() ? "" : password.data();
    if (PKCS5_PBKDF2_HMAC(pw, static_cast<int>(password.size()), ptr(salt), static_cast<int>(salt.size()),
                          static_cast<int>(iterations), mdFor(d), static_cast<int>(digestSize(d)), out.bytes.data()) != 1) return false;
    out.size = static_cast<uint8_t>(digestSize(d));
    return true;
}

namespace {
bool hkdfMode(int mode, Digest d, std::span<const uint8_t> ikm, std::span<const uint8_t> salt,
              std::span<const uint8_t> info, uint8_t* out, size_t outLen) noexcept {
    EVP_KDF* kdf = EVP_KDF_fetch(nullptr, "HKDF", nullptr);
    if (!kdf) return false;
    EVP_KDF_CTX* kctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!kctx) return false;
    OSSL_PARAM params[6];
    int i = 0;
    params[i++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_MODE, &mode);
    params[i++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, nameFor(d), 0);
    params[i++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, const_cast<uint8_t*>(ptr(ikm)), ikm.size());
    params[i++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, const_cast<uint8_t*>(ptr(salt)), salt.size());
    if (mode != EVP_KDF_HKDF_MODE_EXTRACT_ONLY)
        params[i++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, const_cast<uint8_t*>(ptr(info)), info.size());
    params[i] = OSSL_PARAM_construct_end();
    const bool ok = EVP_KDF_derive(kctx, out, outLen, params) == 1;
    EVP_KDF_CTX_free(kctx);
    return ok;
}
} // namespace

bool hkdf(Digest d, std::span<const uint8_t> ikm, std::span<const uint8_t> salt, std::span<const uint8_t> info,
          std::span<uint8_t> out) noexcept {
    if (out.empty() || out.size() > 255 * digestSize(d)) return false;
    return hkdfMode(EVP_KDF_HKDF_MODE_EXTRACT_AND_EXPAND, d, ikm, salt, info, out.data(), out.size());
}

bool randomBytes(std::span<uint8_t> out) noexcept {
    if (out.empty()) return true;
    if (out.size() > 0x7fffffff) return false;
    return RAND_bytes(out.data(), static_cast<int>(out.size())) == 1;
}

bool constantTimeEqual(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
    if (a.size() != b.size()) return false;
    if (a.empty()) return true;
    return CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
}

std::string base64Encode(std::span<const uint8_t> in) {
    std::string out((in.size() + 2) / 3 * 4 + 1, '\0');   // EVP_EncodeBlock NUL-terminates
    const int n = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), ptr(in), static_cast<int>(in.size()));
    out.resize(n < 0 ? 0 : static_cast<std::size_t>(n));
    return out;
}

std::optional<std::vector<uint8_t>> base64Decode(std::string_view in) {
    if (in.size() % 4 != 0) return std::nullopt;
    std::size_t pad = 0;
    for (std::size_t k = 0; k < in.size(); ++k) {
        const char c = in[k];
        const bool alpha = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/';
        if (alpha) { if (pad) return std::nullopt; continue; }
        if (c != '=' || k < in.size() - 2) return std::nullopt;   // '=' only as the last one or two bytes
        ++pad;
    }
    if (in.empty()) return std::vector<uint8_t>{};
    std::vector<uint8_t> out(in.size() / 4 * 3);
    const int n = EVP_DecodeBlock(out.data(), reinterpret_cast<const unsigned char*>(in.data()), static_cast<int>(in.size()));
    if (n < 0 || static_cast<std::size_t>(n) < pad) return std::nullopt;
    out.resize(static_cast<std::size_t>(n) - pad);
    return out;
}

std::optional<ClusterKeyMaterial> deriveClusterKey(std::span<const uint8_t> contents) noexcept {
    std::size_t n = contents.size();
    while (n > 0 && (contents[n - 1] == ' ' || contents[n - 1] == '\t' || contents[n - 1] == '\r' || contents[n - 1] == '\n')) --n;
    if (n < 32) return std::nullopt;
    ClusterKeyMaterial m;
    if (!hkdfMode(EVP_KDF_HKDF_MODE_EXTRACT_ONLY, Digest::Sha512, contents.first(n), sv("rune-cluster-key"), {},
                  m.master.data(), m.master.size())) return std::nullopt;
    DigestBytes id;
    if (!hmac(Digest::Sha512, m.master, sv("rune-cluster-key-id"), id)) return std::nullopt;
    for (int k = 7; k >= 0; --k) m.keyId = (m.keyId << 8) | id.bytes[static_cast<std::size_t>(k)];
    return m;
}

} // namespace rune::crypto
