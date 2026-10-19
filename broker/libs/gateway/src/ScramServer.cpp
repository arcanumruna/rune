#include <rune/gateway/ScramServer.h>

#include <rune/crypto/Kdf.h>

#include <algorithm>
#include <cstring>
#include <string>

namespace rune::gateway {

namespace pc = rune::protocol::client;
namespace rs = rune::crypto::scram;
using rune::crypto::Digest;
using rune::crypto::DigestBytes;

namespace {

constexpr uint32_t kFakeIterations = rs::kDefaultIterations;   // 4096 (D7)

DigestBytes keyBytes(const std::array<uint8_t, 64>& k) noexcept {
    DigestBytes d;
    std::memcpy(d.bytes.data(), k.data(), 64);
    d.size = 64;
    return d;
}

rs::Verifier fromStored(const rune::security::ScramVerifier& v) {
    rs::Verifier out;
    out.digest = Digest::Sha512;
    out.salt = v.salt;
    out.iterations = v.iterations;
    out.storedKey = keyBytes(v.storedKey);
    out.serverKey = keyBytes(v.serverKey);
    return out;
}

/// HMAC-SHA-512(seed, tag + name)[0..16] (D8).  An HMAC failure (no OpenSSL) leaves a name-independent constant -
/// still shape-identical, and the fake slot matches nothing either way.
std::vector<uint8_t> fakeSalt(const std::array<uint8_t, rune::security::kFakeSaltSeedBytes>& seed, std::string_view tag,
                              std::string_view name) {
    std::string msg(tag);
    msg.append(name);
    DigestBytes mac;
    std::vector<uint8_t> salt(rs::kSaltBytes, 0);
    if (rune::crypto::hmac(Digest::Sha512, seed,
                           std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(msg.data()), msg.size()), mac))
        std::memcpy(salt.data(), mac.bytes.data(), rs::kSaltBytes);
    return salt;
}

/// A fake slot: a deterministic salt, 4096 iterations, and keys NOTHING can match (random per exchange).
rs::Verifier fakeSlot(const std::array<uint8_t, rune::security::kFakeSaltSeedBytes>& seed, std::string_view tag,
                      std::string_view name) {
    rs::Verifier v;
    v.digest = Digest::Sha512;
    v.salt = fakeSalt(seed, tag, name);
    v.iterations = kFakeIterations;
    v.storedKey.size = 64;
    v.serverKey.size = 64;
    (void)rune::crypto::randomBytes(std::span<uint8_t>(v.storedKey.bytes.data(), 64));
    (void)rune::crypto::randomBytes(std::span<uint8_t>(v.serverKey.bytes.data(), 64));
    return v;
}

std::string authMessageOf(const AuthAwaitingFinal& st, const rs::Verifier& v) {
    return rs::authMessage(st.clientFirstBare, rs::serverFirst(st.combinedNonce, v.salt, v.iterations),
                           rs::clientFinalWithoutProof(st.combinedNonce));
}

}  // namespace

std::optional<pc::AuthClientFirst> decodeClientFirst(const uint8_t* buf, std::size_t len) noexcept
{
    std::size_t pos = 0;
    const auto h = pc::detail::getAuthHeader(buf, len, pc::AuthClientFirst::TEMPLATE_ID, pc::AuthClientFirst::BODY_FIXED, pos);
    if (!h) return std::nullopt;
    pc::AuthClientFirst m;
    m.sessionId = h->sessionId;
    m.correlationId = h->correlationId;
    m.mechanism = buf[pc::MessageHeader::ENCODED_SIZE];
    if (!pc::getVarStr(buf, len, pos, m.username) || !pc::getVarStr(buf, len, pos, m.clientNonce)) return std::nullopt;
    if (m.username.size() > pc::kAuthNameMax) return std::nullopt;                          // empty IS allowed here
    if (m.clientNonce.empty() || m.clientNonce.size() > pc::kAuthNonceMax) return std::nullopt;
    return m;
}

ServerFirstPlan scramServerFirst(const rune::security::SecurityState& state, const pc::AuthClientFirst& cf, int64_t deadlineNs)
{
    ServerFirstPlan plan;
    AuthAwaitingFinal& st = plan.next;
    st.deadlineNs = deadlineNs;
    st.correlationId = cf.correlationId;
    st.user.assign(cf.username);
    st.clientFirstBare = rs::clientFirstBare(cf.username, cf.clientNonce);

    // Before the first Bootstrap there are no users to hide: the seed is all zeros.
    const std::array<uint8_t, rune::security::kFakeSaltSeedBytes> zeroSeed{};
    const auto& seed = state.fakeSaltSeed() ? *state.fakeSaltSeed() : zeroSeed;

    const rune::security::UserObject* user = cf.username.empty() ? nullptr : state.user(cf.username);
    if (user != nullptr && !user->verifiers.empty()) {
        st.userId = user->objectId;
        st.verifiers[0] = fromStored(user->verifiers[0]);
        st.verifiers[1] = user->verifiers.size() > 1 ? fromStored(user->verifiers[1])
                                                     : fakeSlot(seed, "rune-fake-salt2|", cf.username);   // the pad
    } else {
        st.userId = 0;
        st.verifiers[0] = fakeSlot(seed, "rune-fake-salt|", cf.username);
        st.verifiers[1] = fakeSlot(seed, "rune-fake-salt2|", cf.username);
    }
    st.count = AuthAwaitingFinal::kSlots;

    const std::string serverNonce = rs::makeNonce();
    if (serverNonce.empty()) return plan;                   // no randomness: fail closed (frame stays empty)
    st.combinedNonce.assign(cf.clientNonce);
    st.combinedNonce.append(serverNonce);

    pc::AuthServerFirst sf;
    sf.sessionId = cf.sessionId;
    sf.correlationId = cf.correlationId;
    sf.combinedNonce = st.combinedNonce;
    sf.count = st.count;
    for (uint8_t i = 0; i < st.count; ++i)
        sf.salts[i] = pc::ScramSaltWire{
            std::string_view(reinterpret_cast<const char*>(st.verifiers[i].salt.data()), st.verifiers[i].salt.size()),
            st.verifiers[i].iterations};
    plan.frame.resize(sf.encodedSize());
    plan.frame.resize(sf.encode(plan.frame.data()));
    return plan;
}

std::variant<LoginOk, LoginFailure> scramServerFinal(const rune::security::SecurityState& state, const AuthAwaitingFinal& st,
                                                     const pc::AuthClientFinal& fin)
{
    // Verify EVERY slot, whatever the outcome is going to be: the time never depends on which slot matched.
    std::array<std::string, 2> messages;
    bool slotOk[2] = {false, false};
    const bool countOk = fin.count == st.count;
    for (uint8_t i = 0; i < AuthAwaitingFinal::kSlots; ++i) {
        messages[i] = authMessageOf(st, st.verifiers[i]);
        const bool v = rs::verifyClientProof(Digest::Sha512, st.verifiers[i].storedKey, messages[i],
                                             std::span<const uint8_t>(fin.proofs[i].data(), pc::kScramProofBytes));
        slotOk[i] = v && countOk;
    }
    const int matched = slotOk[0] ? 0 : slotOk[1] ? 1 : -1;

    if (st.user.empty()) return LoginFailure::NoCredentials;
    if (st.userId == 0) return LoginFailure::UnknownUser;
    const rune::security::UserObject* user = state.userById(st.userId);
    if (user == nullptr) return LoginFailure::UnknownUser;                       // dropped between the legs
    if (matched < 0) return LoginFailure::BadProof;
    // The matched password must still be one of the account's CURRENT passwords (it may have been changed or dropped
    // between the legs).
    bool current = false;
    for (const auto& v : user->verifiers)
        current |= std::equal(v.storedKey.begin(), v.storedKey.end(), st.verifiers[matched].storedKey.bytes.begin());
    if (!current) return LoginFailure::BadProof;
    if (!user->enabled) return LoginFailure::Disabled;

    const auto sig = rs::serverSignature(Digest::Sha512, st.verifiers[matched].serverKey, messages[matched]);
    if (!sig || sig->size != 64) return LoginFailure::BadProof;                  // local crypto failure: fail closed
    LoginOk ok{user->objectId, user->name, user->mustChangePassword, {}};
    std::memcpy(ok.serverSignature.data(), sig->bytes.data(), 64);
    return ok;
}

std::string_view loginFailureName(LoginFailure f) noexcept
{
    switch (f) {
        case LoginFailure::NoCredentials:  return "no-credentials";
        case LoginFailure::UnknownUser:    return "unknown-user";
        case LoginFailure::BadProof:       return "bad-proof";
        case LoginFailure::Disabled:       return "disabled";
        case LoginFailure::Throttled:      return "throttled";
        case LoginFailure::Timeout:        return "timeout";
        case LoginFailure::SecurityHalted: return "security-halted";
    }
    return "unknown";
}

} // namespace rune::gateway
