// rune-crypto SCRAM (RFC 5802 §3).  ClientKey = HMAC(SaltedPassword, "Client Key"); StoredKey = H(ClientKey);
// ServerKey = HMAC(SaltedPassword, "Server Key"); ClientSignature = HMAC(StoredKey, AuthMessage);
// ClientProof = ClientKey XOR ClientSignature; ServerSignature = HMAC(ServerKey, AuthMessage).
#include "rune/crypto/Scram.h"
#include <array>
namespace rune::crypto::scram {
namespace {
std::span<const uint8_t> sv(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
bool clientKey(Digest d, const SaltedPassword& sp, DigestBytes& out) { return hmac(d, sp.bytes.view(), sv("Client Key"), out); }
} // namespace

std::optional<SaltedPassword> saltPassword(Digest d, std::string_view pw, std::span<const uint8_t> salt, uint32_t i) noexcept {
    if (i < kMinIterations || i > kMaxIterations || salt.empty()) return std::nullopt;
    SaltedPassword sp;
    if (!pbkdf2(d, pw, salt, i, sp.bytes)) return std::nullopt;
    return sp;
}
std::optional<Verifier> makeVerifier(Digest d, const SaltedPassword& sp, std::span<const uint8_t> salt, uint32_t i) {
    Verifier v; v.digest = d; v.salt.assign(salt.begin(), salt.end()); v.iterations = i;
    DigestBytes ck;
    if (!clientKey(d, sp, ck) || !hash(d, ck.view(), v.storedKey) ||
        !hmac(d, sp.bytes.view(), sv("Server Key"), v.serverKey)) return std::nullopt;
    return v;
}
std::optional<Verifier> makeVerifier(std::string_view pw, uint32_t i) {
    std::array<uint8_t, kSaltBytes> salt{};
    if (!randomBytes(salt)) return std::nullopt;
    auto sp = saltPassword(Digest::Sha512, pw, salt, i);
    if (!sp) return std::nullopt;
    return makeVerifier(Digest::Sha512, *sp, salt, i);
}
std::string makeNonce() {
    std::array<uint8_t, kNonceBytes> r{};
    if (!randomBytes(r)) return {};          // callers treat "" as a local failure (never sent)
    return base64Encode(r);                  // [A-Za-z0-9+/=]: printable, no ','
}
std::string clientFirstBare(std::string_view u, std::string_view c) { return "n=" + std::string(u) + ",r=" + std::string(c); }
std::string serverFirst(std::string_view n, std::span<const uint8_t> salt, uint32_t i) {
    return "r=" + std::string(n) + ",s=" + base64Encode(salt) + ",i=" + std::to_string(i);
}
std::string clientFinalWithoutProof(std::string_view n) { return "c=biws,r=" + std::string(n); }   // biws = base64("n,,")
std::string authMessage(std::string_view a, std::string_view b, std::string_view c) {
    std::string m; m.reserve(a.size() + b.size() + c.size() + 2);
    m.append(a).append(",").append(b).append(",").append(c); return m;
}
std::optional<DigestBytes> clientProof(Digest d, const SaltedPassword& sp, std::string_view am) noexcept {
    DigestBytes ck, sk, sig;
    if (!clientKey(d, sp, ck) || !hash(d, ck.view(), sk) || !hmac(d, sk.view(), sv(am), sig)) return std::nullopt;
    for (std::size_t k = 0; k < ck.size; ++k) ck.bytes[k] ^= sig.bytes[k];
    return ck;
}
std::optional<DigestBytes> serverSignature(Digest d, const DigestBytes& serverKey, std::string_view am) noexcept {
    DigestBytes s; if (!hmac(d, serverKey.view(), sv(am), s)) return std::nullopt; return s;
}
std::optional<DigestBytes> expectedServerSignature(Digest d, const SaltedPassword& sp, std::string_view am) noexcept {
    DigestBytes sk; if (!hmac(d, sp.bytes.view(), sv("Server Key"), sk)) return std::nullopt;
    return serverSignature(d, sk, am);
}
bool verifyClientProof(Digest d, const DigestBytes& storedKey, std::string_view am, std::span<const uint8_t> proof) noexcept {
    if (proof.size() != digestSize(d) || storedKey.size != digestSize(d)) return false;
    DigestBytes sig, ck, h;
    if (!hmac(d, storedKey.view(), sv(am), sig)) return false;
    ck.size = sig.size;
    for (std::size_t k = 0; k < sig.size; ++k) ck.bytes[k] = static_cast<uint8_t>(proof[k] ^ sig.bytes[k]);
    if (!hash(d, ck.view(), h)) return false;
    return constantTimeEqual(h.view(), storedKey.view());
}
} // namespace rune::crypto::scram
