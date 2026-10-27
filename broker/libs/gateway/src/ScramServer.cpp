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

static_assert(4 * ((rs::kNonceBytes + 2) / 3) == pc::kAuthServerNonceChars,
              "server nonce length is part of the wire bound");

namespace {

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
                      std::string_view name, uint32_t iterations) {
    rs::Verifier v;
    v.digest = Digest::Sha512;
    v.salt = fakeSalt(seed, tag, name);
    v.iterations = iterations;
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

LoginName resolveLoginName(const rune::catalog::NamespaceRegistry* namespaces, std::string_view username) noexcept
{
    LoginName r;
    const auto q = rune::catalog::parseQualified(username);
    if (!q) return r;                                                    // "" / "a//x" / "/x" / "a/": unknown
    r.local = q->local;
    if (q->ns.empty()) { r.nsId = rune::catalog::kDefaultNsId; r.known = true; return r; }   // bare = `default`
    const rune::catalog::NamespaceRecord* ns = nullptr;
    if (namespaces != nullptr) ns = namespaces->byName(q->ns);
    else if (q->ns == rune::catalog::kDefaultNsName) { r.nsId = rune::catalog::kDefaultNsId; r.known = true; return r; }
    if (ns == nullptr) return r;                                         // unknown namespace (or not yet mirrored)
    r.nsId = ns->nsId;
    r.known = true;
    return r;
}

ServerFirstPlan scramServerFirst(const rune::security::SecurityState& state, const pc::AuthClientFirst& cf, int64_t deadlineNs,
                                 uint32_t fakeIterations, const rune::catalog::NamespaceRegistry* namespaces)
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

    if (fakeIterations < pc::kAuthIterationsMin || fakeIterations > pc::kAuthIterationsMax) fakeIterations = rs::kDefaultIterations;
    // Issue #132: BOTH fake slots are computed for every name - unknown, one password, two passwords - and then
    // selected, so the first leg's work (two HMACs + the random keys) never depends on whether the account exists.
    rs::Verifier fake0 = fakeSlot(seed, "rune-fake-salt|", cf.username, fakeIterations);
    rs::Verifier fake1 = fakeSlot(seed, "rune-fake-salt2|", cf.username, fakeIterations);
    plan.fakeSlotsComputed = 2;
    const LoginName who = resolveLoginName(namespaces, cf.username);
    // Namespaces Task 9: an unknown / malformed namespace still runs the user lookup (against a namespace id no user
    // holds) and takes the SAME fake path as an unknown user - the first leg never reveals which part was wrong.
    const rune::security::UserObject* user = state.userByName(who.nsId, who.local);
    if (!who.known) user = nullptr;
    if (user != nullptr && !user->verifiers.empty()) {
        st.userId = user->objectId;
        st.nsId = user->nsId;
        st.verifiers[0] = fromStored(user->verifiers[0]);
        st.verifiers[1] = user->verifiers.size() > 1 ? fromStored(user->verifiers[1]) : std::move(fake1);   // the pad
    } else {
        st.userId = 0;
        st.verifiers[0] = std::move(fake0);
        st.verifiers[1] = std::move(fake1);
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
    LoginOk ok{user->objectId, user->name, user->mustChangePassword, {}, user->nsId};
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
