#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <gtest/gtest.h>
#include <string>
namespace pc = rune::protocol::client;

TEST(AuthMessagesRegression, ErrorCodesAndTemplatesArePinned) {
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed), 44);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::NotAuthorized), 45);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::SessionRevoked), 46);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::PasswordChangeRequired), 47);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AuthClientFirst), 12);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), 15);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::SecurityDdl), 79);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::SecurityBootstrap), 80);
    EXPECT_EQ(pc::kClientVersion, 10); EXPECT_EQ(pc::kBrokerVersion, 6); EXPECT_EQ(pc::kSecurityTopicId, -7);
    EXPECT_EQ(pc::kClientVersionAuth, 9); EXPECT_EQ(pc::kBrokerVersionAuth, 5);
    EXPECT_TRUE(pc::clientUnderstandsAuth(9)); EXPECT_FALSE(pc::clientUnderstandsAuth(8));
    EXPECT_EQ(static_cast<int>(pc::AuthMode::Required), 0);
    EXPECT_EQ(static_cast<int>(pc::AuthMode::DevDisabled), 1);
    EXPECT_EQ(static_cast<int>(pc::AuthMode::InProcess), 2);
    EXPECT_EQ(static_cast<int>(pc::AuthState::MustAuthenticate), 0);
    EXPECT_EQ(static_cast<int>(pc::AuthState::ResumedAuthenticated), 1);
    EXPECT_EQ(static_cast<int>(pc::AuthState::Anonymous), 2);
    for (uint16_t t : {12, 13, 14, 15, 79, 80}) EXPECT_TRUE(pc::isKnownTemplate(t)) << t;
    EXPECT_FALSE(pc::isKnownTemplate(81));
}

TEST(AuthMessagesRegression, EveryAuthFrameRoundTrips) {
    uint8_t buf[1024];
    pc::AuthClientFirst cf{.sessionId = 7, .correlationId = 9, .username = "ws1", .clientNonce = "abcDEF123+/="};
    auto n = cf.encode(buf); auto d = pc::AuthClientFirst::decode(buf, n);
    ASSERT_TRUE(d); EXPECT_EQ(d->username, "ws1"); EXPECT_EQ(d->clientNonce, "abcDEF123+/=");
    EXPECT_EQ(n, cf.encodedSize());
    EXPECT_EQ(d->sessionId, 7); EXPECT_EQ(d->correlationId, 9); EXPECT_EQ(d->mechanism, pc::kMechanismScramSha512);

    const std::string s1(16, 's'), s2(32, 't');
    pc::AuthServerFirst sf{.sessionId = 7, .correlationId = 9, .combinedNonce = "abc+srv", .count = 2,
                           .salts = {pc::ScramSaltWire{s1, 4096}, pc::ScramSaltWire{s2, 8192}}};
    n = sf.encode(buf); auto ds = pc::AuthServerFirst::decode(buf, n);
    ASSERT_TRUE(ds); EXPECT_EQ(ds->count, 2); EXPECT_EQ(ds->salts[1].iterations, 8192u); EXPECT_EQ(ds->salts[1].salt, s2);
    EXPECT_EQ(ds->salts[0].salt, s1); EXPECT_EQ(ds->combinedNonce, "abc+srv");
    EXPECT_EQ(n, sf.encodedSize());

    pc::AuthClientFinal fin{.sessionId = 7, .correlationId = 9, .count = 2};
    fin.proofs[0].fill(1); fin.proofs[1].fill(2);
    n = fin.encode(buf); auto df = pc::AuthClientFinal::decode(buf, n);
    ASSERT_TRUE(df); EXPECT_EQ(df->proofs[1][63], 2); EXPECT_EQ(df->proofs[0][0], 1);
    EXPECT_EQ(n, fin.encodedSize());

    const std::string sig(64, 'g');
    pc::AuthServerFinal sv{.sessionId = 7, .correlationId = 9, .errorCode = 0,
                           .flags = pc::kAuthFlagMustChangePassword, .retryAfterMs = 0, .serverSignature = sig};
    n = sv.encode(buf); auto dv = pc::AuthServerFinal::decode(buf, n);
    ASSERT_TRUE(dv); EXPECT_EQ(dv->flags, pc::kAuthFlagMustChangePassword); EXPECT_EQ(dv->serverSignature, sig);
    EXPECT_EQ(n, sv.encodedSize());

    pc::AuthServerFinal fail{.sessionId = 7, .correlationId = 9, .errorCode = 44, .retryAfterMs = 750, .detail = "authentication failed"};
    n = fail.encode(buf); dv = pc::AuthServerFinal::decode(buf, n);
    ASSERT_TRUE(dv); EXPECT_EQ(dv->errorCode, 44); EXPECT_EQ(dv->retryAfterMs, 750u);
    EXPECT_TRUE(dv->serverSignature.empty()); EXPECT_EQ(dv->detail, "authentication failed");
}

TEST(AuthMessagesRegression, OutOfBoundsFieldsDoNotDecode) {
    uint8_t buf[1024];
    const std::string shortSalt(15, 's');
    pc::AuthServerFirst sf{.combinedNonce = "n", .count = 1, .salts = {pc::ScramSaltWire{shortSalt, 4096}}};
    EXPECT_FALSE(pc::AuthServerFirst::decode(buf, sf.encode(buf))) << "salt < 16";
    const std::string longSalt(65, 's'), okSalt(16, 's');
    sf.salts[0] = {longSalt, 4096};
    EXPECT_FALSE(pc::AuthServerFirst::decode(buf, sf.encode(buf))) << "salt > 64";
    sf.salts[0] = {okSalt, 4095};
    EXPECT_FALSE(pc::AuthServerFirst::decode(buf, sf.encode(buf))) << "iterations < 4096";
    sf.salts[0] = {okSalt, 1'000'001};
    EXPECT_FALSE(pc::AuthServerFirst::decode(buf, sf.encode(buf))) << "iterations > 1e6";
    sf.salts[0] = {okSalt, 1'000'000};
    EXPECT_TRUE(pc::AuthServerFirst::decode(buf, sf.encode(buf))) << "1e6 is the inclusive bound";
    sf.count = 0;
    EXPECT_EQ(sf.encode(buf), 0u) << "count 0 is not encodable";
    pc::AuthClientFinal fin{.count = 3};
    EXPECT_EQ(fin.encode(buf), 0u) << "count > 2 is not encodable";
    fin.count = 2;
    const std::size_t fn = fin.encode(buf);
    buf[24] = 3;   // count byte forged on the wire
    EXPECT_FALSE(pc::AuthClientFinal::decode(buf, fn)) << "count > 2";
    buf[24] = 0;
    EXPECT_FALSE(pc::AuthClientFinal::decode(buf, fn)) << "count 0";
    pc::AuthClientFirst cf{.username = "", .clientNonce = "n"};
    EXPECT_FALSE(pc::AuthClientFirst::decode(buf, cf.encode(buf))) << "empty username";
    const std::string longName(129, 'u');
    cf = {.username = longName, .clientNonce = "n"};
    EXPECT_FALSE(pc::AuthClientFirst::decode(buf, cf.encode(buf))) << "username > 128";
    cf = {.username = "u", .clientNonce = ""};
    EXPECT_FALSE(pc::AuthClientFirst::decode(buf, cf.encode(buf))) << "empty nonce";
    const std::string sig(64, 'g');
    pc::AuthServerFinal sv{.serverSignature = sig};
    const std::size_t full = sv.encode(buf);
    for (std::size_t cut = 0; cut < full; ++cut) EXPECT_FALSE(pc::AuthServerFinal::decode(buf, cut)) << cut;
    const std::string sig63(63, 'g');
    pc::AuthServerFinal bad{.serverSignature = sig63};
    EXPECT_FALSE(pc::AuthServerFinal::decode(buf, bad.encode(buf))) << "a signature is 0 or 64 bytes";
}

TEST(AuthMessagesRegression, TruncationNeverDecodesAnyAuthFrame) {
    uint8_t buf[1024];
    const std::string s1(16, 's');
    pc::AuthClientFirst cf{.username = "ws1", .clientNonce = "nonce"};
    pc::AuthServerFirst sf{.combinedNonce = "nonce", .count = 1, .salts = {pc::ScramSaltWire{s1, 4096}}};
    pc::AuthClientFinal fin{.count = 2};
    std::size_t n = cf.encode(buf);
    for (std::size_t c = 0; c < n; ++c) EXPECT_FALSE(pc::AuthClientFirst::decode(buf, c)) << "cf " << c;
    n = sf.encode(buf);
    for (std::size_t c = 0; c < n; ++c) EXPECT_FALSE(pc::AuthServerFirst::decode(buf, c)) << "sf " << c;
    n = fin.encode(buf);
    for (std::size_t c = 0; c < n; ++c) EXPECT_FALSE(pc::AuthClientFinal::decode(buf, c)) << "fin " << c;
}

TEST(AuthMessagesRegression, WrongTemplateDoesNotDecode) {
    uint8_t buf[256];
    pc::AuthClientFirst cf{.username = "u", .clientNonce = "n"};
    const std::size_t n = cf.encode(buf);
    EXPECT_FALSE(pc::AuthServerFirst::decode(buf, n));
    EXPECT_FALSE(pc::AuthClientFinal::decode(buf, n));
    EXPECT_FALSE(pc::AuthServerFinal::decode(buf, n));
}
