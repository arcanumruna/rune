// ScramServerTest.cpp - authentication Task 11: the broker half of SCRAM-SHA-512 as pure functions.  The "client" in
// these tests is the rune-crypto SCRAM API driven exactly as the real client's SessionMachine drives it (one proof
// per listed salt, each over its own AuthMessage).
#include <rune/crypto/Scram.h>
#include <rune/gateway/ScramServer.h>
#include "support/ScramTestClient.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace rs = rune::crypto::scram;
namespace sec = rune::security;

namespace {

using rune::node::test::kTestPasswordA;
using rune::node::test::kTestPasswordB;
using rune::node::test::makeAuthTestState;
using rune::node::test::scramVerifierOf;
constexpr auto kPwA = kTestPasswordA;
constexpr auto kPwB = kTestPasswordB;
sec::SecurityState makeState(uint8_t seed = 0x42) { return makeAuthTestState(seed); }

struct Exchange {
    AuthAwaitingFinal st;
    std::optional<pc::AuthServerFirst> first;
    std::vector<uint8_t> frame;
    std::string clientNonce = "clientNonce0123456789";
    std::string user;
};

Exchange begin(const sec::SecurityState& s, const std::string& user, const std::string& nonce = "clientNonce0123456789") {
    Exchange x;
    x.user = user; x.clientNonce = nonce;
    pc::AuthClientFirst cf;
    cf.sessionId = 7; cf.correlationId = 9; cf.username = x.user; cf.clientNonce = x.clientNonce;
    auto plan = scramServerFirst(s, cf, 123);
    x.st = std::move(plan.next);
    x.frame = std::move(plan.frame);
    x.first = pc::AuthServerFirst::decode(x.frame.data(), x.frame.size());
    return x;
}

/// What the client answers: one proof per listed salt, from `passwords[i]` (an empty string = garbage).
pc::AuthClientFinal answer(const Exchange& x, const std::array<std::string, 2>& passwords, uint8_t count = 2) {
    pc::AuthClientFinal f;
    f.sessionId = 7; f.correlationId = 9; f.count = count;
    const std::string cfb = rs::clientFirstBare(x.user, x.clientNonce);
    const std::string combined(x.first->combinedNonce);
    for (uint8_t i = 0; i < count; ++i) {
        const auto salt = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(x.first->salts[i].salt.data()),
                                                   x.first->salts[i].salt.size());
        if (passwords[i].empty()) { f.proofs[i].fill(0x5A); continue; }
        auto sp = rs::saltPassword(rune::crypto::Digest::Sha512, passwords[i], salt, x.first->salts[i].iterations);
        const std::string am = rs::authMessage(cfb, rs::serverFirst(combined, salt, x.first->salts[i].iterations),
                                               rs::clientFinalWithoutProof(combined));
        auto proof = rs::clientProof(rune::crypto::Digest::Sha512, *sp, am);
        std::memcpy(f.proofs[i].data(), proof->bytes.data(), 64);
    }
    return f;
}

std::array<uint8_t, 64> expectedSignature(const Exchange& x, const std::string& password, uint8_t slot) {
    const std::string cfb = rs::clientFirstBare(x.user, x.clientNonce);
    const std::string combined(x.first->combinedNonce);
    const auto salt = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(x.first->salts[slot].salt.data()),
                                               x.first->salts[slot].salt.size());
    auto sp = rs::saltPassword(rune::crypto::Digest::Sha512, password, salt, x.first->salts[slot].iterations);
    const std::string am = rs::authMessage(cfb, rs::serverFirst(combined, salt, x.first->salts[slot].iterations),
                                           rs::clientFinalWithoutProof(combined));
    auto sig = rs::expectedServerSignature(rune::crypto::Digest::Sha512, *sp, am);
    std::array<uint8_t, 64> out{};
    std::memcpy(out.data(), sig->bytes.data(), 64);
    return out;
}

LoginFailure failureOf(const std::variant<LoginOk, LoginFailure>& r) {
    EXPECT_TRUE(std::holds_alternative<LoginFailure>(r));
    return std::holds_alternative<LoginFailure>(r) ? std::get<LoginFailure>(r) : LoginFailure::SecurityHalted;
}

}  // namespace

TEST(ScramRegression, EveryServerFirstListsTwoSaltsOfTheSameShapeWhoeverTheUserIs) {
    const auto s = makeState();
    std::size_t size = 0;
    for (const char* user : {"ws1", "ws2", "off", "nobody", "admin"}) {
        const Exchange x = begin(s, user);
        ASSERT_TRUE(x.first.has_value()) << user;
        EXPECT_EQ(x.first->count, 2) << user << ": review A - the broker ALWAYS sends 2 salts";
        for (int i = 0; i < 2; ++i) {
            EXPECT_EQ(x.first->salts[i].salt.size(), 16u) << user;
            EXPECT_EQ(x.first->salts[i].iterations, 4096u) << user;
        }
        if (size == 0) size = x.frame.size();
        EXPECT_EQ(x.frame.size(), size) << user << ": the frames are the same length";
        EXPECT_EQ(x.first->combinedNonce.substr(0, x.clientNonce.size()), x.clientNonce);
        EXPECT_GT(x.first->combinedNonce.size(), x.clientNonce.size());
    }
    const Exchange empty = begin(s, "");   // no credentials: the same shape too
    ASSERT_TRUE(empty.first.has_value());
    EXPECT_EQ(empty.first->count, 2);
    EXPECT_EQ(empty.frame.size(), size);
}

// Issue #132: the first leg ran no fake slot for a two-password user, one for a one-password user and two for an
// unknown or empty name (each an HMAC + random keys): the AuthServerFirst latency told real accounts from fake ones.
TEST(ScramRegression, FirstLegWorkIsIndependentOfUserExistenceRegression) {
    const auto s = makeState();
    pc::AuthClientFirst cf;
    cf.sessionId = 7; cf.correlationId = 9; cf.clientNonce = "clientNonce0123456789";
    for (const char* user : {"ws1", "ws2", "off", "nobody", ""}) {
        cf.username = user;
        EXPECT_EQ(scramServerFirst(s, cf, 123).fakeSlotsComputed, 2) << "'" << user << "': the same work for everyone";
    }
    // And the time, with a wide margin: interleaved samples, medians, a two-password user (no fake slot needed) against
    // an unknown name (two needed).  Before the fix the unknown name cost several times more.
    using Clock = std::chrono::steady_clock;
    std::vector<int64_t> known, unknown;
    for (int i = 0; i < 301; ++i)
        for (const bool isKnown : {true, false}) {
            cf.username = isKnown ? "ws1" : "nobody";
            const auto t0 = Clock::now();
            const auto plan = scramServerFirst(s, cf, 123);
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
            ASSERT_FALSE(plan.frame.empty());
            (isKnown ? known : unknown).push_back(ns);
        }
    std::nth_element(known.begin(), known.begin() + 150, known.end());
    std::nth_element(unknown.begin(), unknown.begin() + 150, unknown.end());
    const double ratio = static_cast<double>(unknown[150]) / static_cast<double>(std::max<int64_t>(known[150], 1));
    EXPECT_LT(ratio, 1.6) << "median unknown " << unknown[150] << " ns vs known " << known[150] << " ns";
    EXPECT_GT(ratio, 1 / 1.6) << "median unknown " << unknown[150] << " ns vs known " << known[150] << " ns";
}

TEST(ScramRegression, TheFakeSaltsAreDeterministicPerNameAndPerClusterSeed) {
    const auto a = makeState(0x42), sameSeed = makeState(0x42), otherSeed = makeState(0x77);
    const Exchange x1 = begin(a, "nobody"), x2 = begin(a, "nobody"), onOtherBroker = begin(sameSeed, "nobody");
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(x1.first->salts[i].salt, x2.first->salts[i].salt) << "stable across attempts";
        EXPECT_EQ(x1.first->salts[i].salt, onOtherBroker.first->salts[i].salt) << "stable across brokers (same seed)";
    }
    EXPECT_NE(x1.first->salts[0].salt, x1.first->salts[1].salt);
    EXPECT_NE(x1.first->salts[0].salt, begin(a, "other").first->salts[0].salt) << "keyed on the name";
    EXPECT_NE(x1.first->salts[0].salt, begin(otherSeed, "nobody").first->salts[0].salt) << "keyed on the seed";
    // A user with ONE password gets a stable fake second salt too (its real first salt is its stored one).
    const Exchange w1 = begin(a, "ws2"), w2 = begin(a, "ws2");
    EXPECT_EQ(w1.first->salts[1].salt, w2.first->salts[1].salt);
    EXPECT_EQ(w1.first->salts[0].salt, std::string(16, '\xA3'));
}

TEST(ScramRegression, TwoVerifierExchangeAcceptsOnlyTheRealSlotRegression) {
    const auto s = makeState();
    // ws2 has ONE password: slot 0 is real, slot 1 is the fake pad.
    {   // proof 0 right, proof 1 garbage -> accepted, signed with slot 0's ServerKey
        const Exchange x = begin(s, "ws2");
        const auto r = scramServerFinal(s, x.st, answer(x, {kPwA, ""}));
        ASSERT_TRUE(std::holds_alternative<LoginOk>(r));
        const LoginOk& ok = std::get<LoginOk>(r);
        EXPECT_EQ(ok.user, "ws2");
        EXPECT_FALSE(ok.mustChangePassword);
        EXPECT_EQ(ok.serverSignature, expectedSignature(x, kPwA, 0));
    }
    {   // the right password computed over the FAKE slot's salt (a client that "works" slot 1) -> refused
        const Exchange x = begin(s, "ws2");
        EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {"", kPwA}))), LoginFailure::BadProof);
        EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {"", ""}))), LoginFailure::BadProof);
        EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {kPwB, kPwB}))), LoginFailure::BadProof) << "wrong password";
    }
    {   // ws1 has TWO passwords: either slot logs in; the signature is that slot's
        const Exchange x = begin(s, "ws1");
        const auto first = scramServerFinal(s, x.st, answer(x, {kPwA, ""}));
        ASSERT_TRUE(std::holds_alternative<LoginOk>(first));
        EXPECT_EQ(std::get<LoginOk>(first).serverSignature, expectedSignature(x, kPwA, 0));
        const auto second = scramServerFinal(s, x.st, answer(x, {"", kPwB}));
        ASSERT_TRUE(std::holds_alternative<LoginOk>(second));
        EXPECT_EQ(std::get<LoginOk>(second).serverSignature, expectedSignature(x, kPwB, 1));
        EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {kPwB, kPwA}))), LoginFailure::BadProof) << "swapped slots";
    }
    {   // an answer with the wrong number of proofs never matches (the broker listed two)
        const Exchange x = begin(s, "ws2");
        EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {kPwA, ""}, 1))), LoginFailure::BadProof);
    }
}

TEST(ScramRegression, UnknownAndEmptyUsersNeverAuthenticateAndAreNamedForTheLogsOnly) {
    const auto s = makeState();
    const Exchange x = begin(s, "nobody");
    EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {kPwA, kPwA}))), LoginFailure::UnknownUser);
    const Exchange e = begin(s, "");
    EXPECT_EQ(failureOf(scramServerFinal(s, e.st, answer(e, {kPwA, kPwA}))), LoginFailure::NoCredentials);
    EXPECT_EQ(loginFailureName(LoginFailure::UnknownUser), "unknown-user");
    EXPECT_EQ(loginFailureName(LoginFailure::BadProof), "bad-proof");
    EXPECT_EQ(loginFailureName(LoginFailure::NoCredentials), "no-credentials");
    EXPECT_EQ(loginFailureName(LoginFailure::Disabled), "disabled");
    EXPECT_EQ(loginFailureName(LoginFailure::Throttled), "throttled");
    EXPECT_EQ(loginFailureName(LoginFailure::Timeout), "timeout");
    EXPECT_EQ(loginFailureName(LoginFailure::SecurityHalted), "security-halted");
}

TEST(ScramRegression, ADisabledUserFailsOnlyAtTheFinalLegAndOnlyWithTheRightPassword) {
    const auto s = makeState();
    const Exchange x = begin(s, "off");
    ASSERT_EQ(x.st.userId, s.user("off")->objectId) << "a disabled user still gets its real verifiers (Review Focus 1)";
    EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {kPwA, ""}))), LoginFailure::Disabled);
    EXPECT_EQ(failureOf(scramServerFinal(s, x.st, answer(x, {kPwB, ""}))), LoginFailure::BadProof);
}

TEST(ScramRegression, TheFinalLegReadsTheCurrentState) {
    auto s = makeState();
    const Exchange x = begin(s, "ws2");
    // The user is dropped between the legs: UnknownUser (the copied verifiers do not outlive the account).
    rune::node::test::SecurityEventChain c;   // a separate chain: just a state without ws2
    sec::BootstrapOps b; b.verifier = scramVerifierOf("admin password 123", 9); b.fakeSaltSeed.fill(0x42);
    (void)c.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    EXPECT_EQ(failureOf(scramServerFinal(c.state(), x.st, answer(x, {kPwA, ""}))), LoginFailure::UnknownUser);
    // The bootstrap admin is a one-time-password account: must change.
    const Exchange a = begin(s, "admin");
    const auto r = scramServerFinal(s, a.st, answer(a, {"admin password 123", ""}));
    ASSERT_TRUE(std::holds_alternative<LoginOk>(r));
    EXPECT_TRUE(std::get<LoginOk>(r).mustChangePassword);
}

TEST(ScramRegression, AProofIsBoundToItsOwnExchangeNonce) {
    const auto s = makeState();
    const Exchange x1 = begin(s, "ws2"), x2 = begin(s, "ws2");
    ASSERT_NE(x1.first->combinedNonce, x2.first->combinedNonce) << "a fresh server nonce per exchange";
    EXPECT_EQ(failureOf(scramServerFinal(s, x2.st, answer(x1, {kPwA, ""}))), LoginFailure::BadProof) << "replayed proof";
}

TEST(ScramRegression, AnEmptyUsernameIsDecodedAndAnythingMalformedIsNot) {
    pc::AuthClientFirst cf;
    cf.sessionId = 1; cf.correlationId = 2; cf.username = ""; cf.clientNonce = "n";
    std::vector<uint8_t> b(cf.encodedSize());
    b.resize(cf.encode(b.data()));
    const auto lenient = decodeClientFirst(b.data(), b.size());
    ASSERT_TRUE(lenient.has_value()) << "the generated decoder refuses it; the gateway must not";
    EXPECT_TRUE(lenient->username.empty());
    EXPECT_EQ(lenient->correlationId, 2);
    EXPECT_FALSE(decodeClientFirst(b.data(), 20).has_value());
    cf.clientNonce = "";
    b.resize(cf.encodedSize()); b.resize(cf.encode(b.data()));
    EXPECT_FALSE(decodeClientFirst(b.data(), b.size()).has_value()) << "an empty nonce is malformed";
    const std::string longName(129, 'u');
    cf.username = longName; cf.clientNonce = "n";
    b.resize(cf.encodedSize()); b.resize(cf.encode(b.data()));
    EXPECT_FALSE(decodeClientFirst(b.data(), b.size()).has_value());
}
