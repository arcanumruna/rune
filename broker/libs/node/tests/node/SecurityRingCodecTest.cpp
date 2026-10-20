// SecurityRingCodecTest.cpp — authentication Task 10: the gateway → admin ring codecs of the security DDL (79) and the
// bootstrap (80): every field round-trips; a frame the decode cannot read is a MalformedDdl (answered, never dropped).
#include "support/SecurityDdlRig.h"

#include <rune/node/AdminChannels.h>
#include <rune/node/OneToOneByteRing.h>

#include <gtest/gtest.h>

#include <vector>

using namespace rune::node;

namespace {
template <class M>
SecurityRequest roundTrip(const M& m, SecurityRequest (*dec)(const uint8_t*, std::size_t)) {
    OneToOneByteRing ring(kAdminRequestRingBytes);
    EXPECT_TRUE(encodeDdl(ring, m));
    SecurityRequest out = MalformedDdl{};
    ring.drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        EXPECT_EQ(tid, kDdlMsgType<M>);
        out = dec(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
    }, 4);
    return out;
}
}  // namespace

TEST(SecurityRingCodecRegression, EveryFieldOfASecurityDdlRoundTrips) {
    SecurityDdlMsg m;
    m.session = 11; m.corr = 22;
    m.meta.statement = "ALTER USER alice SET PASSWORD '<redacted>' TERMINATE SESSIONS;";
    m.meta.clientName = "rune-cli";
    m.meta.sourceAddress = "aeron:udp?endpoint=10.0.0.1:4000";
    m.meta.authenticated = "root";
    m.meta.sessionId = 11;
    m.meta.requestId = {5, 6};
    m.kind = 2; m.name = "alice"; m.change = 2; m.terminateSessions = 1; m.passwordIndex = 0;
    m.verifier = rune::node::test::SecurityDdlRig::verifierFor("a-long-password-1");
    m.granteeKind = 1; m.op = 6; m.resourceType = 0; m.patternKind = 1; m.pattern = "world."; m.roleName = "world_server";
    const auto got = roundTrip(m, decodeSecurityDdl);
    const auto* d = std::get_if<SecurityDdlMsg>(&got);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(d->session, 11); EXPECT_EQ(d->corr, 22);
    EXPECT_EQ(d->meta, m.meta);
    EXPECT_EQ(d->meta.authenticated, "root");
    EXPECT_EQ(d->kind, 2); EXPECT_EQ(d->name, "alice"); EXPECT_EQ(d->change, 2); EXPECT_EQ(d->terminateSessions, 1);
    EXPECT_EQ(d->verifier, m.verifier);
    EXPECT_EQ(d->granteeKind, 1); EXPECT_EQ(d->op, 6); EXPECT_EQ(d->patternKind, 1);
    EXPECT_EQ(d->pattern, "world."); EXPECT_EQ(d->roleName, "world_server");
}

TEST(SecurityRingCodecRegression, ABootstrapRoundTripsWithItsSeedAndNonce) {
    SecurityBootstrapMsg m;
    m.session = 1; m.corr = 2;
    m.meta.clientName = "rune-cli"; m.meta.sessionId = 1;
    m.adminName = "admin";
    m.verifier = rune::node::test::SecurityDdlRig::verifierFor("initial-one-time-pw");
    for (std::size_t i = 0; i < m.fakeSaltSeed.size(); ++i) m.fakeSaltSeed[i] = static_cast<uint8_t>(i + 1);
    for (std::size_t i = 0; i < m.nonce.size(); ++i) m.nonce[i] = static_cast<uint8_t>(0xA0 + i);
    const auto got = roundTrip(m, decodeSecurityBootstrap);
    const auto* d = std::get_if<SecurityBootstrapMsg>(&got);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(d->adminName, "admin");
    EXPECT_EQ(d->verifier, m.verifier);
    EXPECT_EQ(d->fakeSaltSeed, m.fakeSaltSeed);
    EXPECT_EQ(d->nonce, m.nonce);
}

TEST(SecurityRingCodecRegression, AnUnreadableFrameIsAMalformedRequestNeverACastValue) {
    SecurityDdlMsg m;
    m.session = 3; m.corr = 4; m.kind = 6; m.name = "r"; m.granteeKind = 9;   // an out-of-range grantee byte
    const auto bad = roundTrip(m, decodeSecurityDdl);
    const auto* mal = std::get_if<MalformedDdl>(&bad);
    ASSERT_NE(mal, nullptr);
    EXPECT_EQ(mal->session, 3); EXPECT_EQ(mal->corr, 4);
    EXPECT_EQ(mal->templateId, kTidSecurityDdl);
    const uint8_t junk[3] = {1, 2, 3};
    const auto tiny = decodeSecurityDdl(junk, sizeof junk);
    EXPECT_TRUE(std::holds_alternative<MalformedDdl>(tiny));
}

// Phase 2c Task 9: the kA2cFilterSchema frame (admin -> control) round-trips, and a short frame is refused.
TEST(AdminChannelsRegression, FilterSchemaMessageRoundTripsRegression) {
    const std::vector<uint8_t> blob = {1, 2, 3, 4, 5, 250, 0, 7};
    std::vector<uint8_t> wire(filterSchemaWireLen(blob.size()));
    writeFilterSchema(wire.data(), -1234567, 0xBEEF, blob);
    FilterSchemaMsg m;
    ASSERT_TRUE(decodeFilterSchema(wire.data(), wire.size(), m));
    EXPECT_EQ(m.topicId, -1234567);
    EXPECT_EQ(m.version, 0xBEEF);
    EXPECT_EQ(m.blob, blob);
    FilterSchemaMsg empty;
    ASSERT_TRUE(decodeFilterSchema(wire.data(), 6, empty)) << "an empty blob is a (useless but well-formed) frame";
    EXPECT_TRUE(empty.blob.empty());
    FilterSchemaMsg bad;
    EXPECT_FALSE(decodeFilterSchema(wire.data(), 5, bad)) << "shorter than the 6-byte header";
    EXPECT_NE(kA2cFilterSchema, kA2cSchemaUpdate);
    EXPECT_EQ(kA2cFilterSchema, 24);
}
