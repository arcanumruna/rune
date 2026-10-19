// Authentication spec §3: the frozen formatVer-1 __security event encoding.
#include <rune/security/SecurityEvent.h>
#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/SecurityTypes.h>
#include <gtest/gtest.h>
#include <type_traits>

namespace rs = rune::security;

namespace {
rs::ScramVerifier verifier(uint8_t seed) {
    rs::ScramVerifier v;
    v.salt.assign(16, seed); v.iterations = 4096;
    v.storedKey.fill(static_cast<uint8_t>(seed + 1)); v.serverKey.fill(static_cast<uint8_t>(seed + 2));
    return v;
}
rs::SecurityEvent baseEvent(rs::Ops ops) {
    rs::SecurityEvent e;
    e.kind = rs::kindOf(ops); e.objectId = 9; e.objectName = "ws1"; e.prevVersion = 0; e.newVersion = 1;
    e.statement = "CREATE USER ws1 WITH PASSWORD '<redacted>';";
    e.ops = std::move(ops);
    e.principal = rs::Principal{.sessionId = 5, .clientName = "rune-cli", .sourceAddress = "10.0.0.5",
                                .authenticated = "admin", .requestId = {.hi = 0xA1, .lo = 0xB2},
                                .requestFingerprint = 0x77};
    e.timestampMs = 1'760'000'000'000; e.fingerprint = 0x1122334455667788ull;
    return e;
}
rs::GrantSpec readPrefix() { return {.op = rs::Operation::Read, .resource = {rs::ResourceType::Topic, rs::PatternKind::Prefix, "world."}}; }
std::vector<rs::Ops> everyKind() {
    return {rs::CreateUserOps{verifier(1)},
            rs::AlterUserOps{rs::AddPassword{verifier(2)}}, rs::AlterUserOps{rs::DropPassword{1}},
            rs::AlterUserOps{rs::SetPassword{verifier(3), true}}, rs::AlterUserOps{rs::DisableUser{}},
            rs::AlterUserOps{rs::EnableUser{}}, rs::DropUserOps{}, rs::CreateRoleOps{}, rs::DropRoleOps{},
            rs::GrantOps{rs::GranteeKind::Role, readPrefix()}, rs::RevokeOps{rs::GranteeKind::User, readPrefix()},
            rs::GrantRoleOps{4, "world_server"}, rs::RevokeRoleOps{4, "world_server"}, rs::BootstrapOps{verifier(9)}};
}
} // namespace

TEST(SecurityEventRegression, EveryKindRoundTripsByteExactly) {
    for (const rs::Ops& ops : everyKind()) {
        rs::SecurityEvent e = baseEvent(ops);
        e.replayedFrom = rs::ReplayedFrom{.clusterId = "a", .offset = 3, .principal = "admin@10.0.0.9"};
        const auto bytes = rs::encode(e).value();
        auto d = rs::decode(bytes);
        ASSERT_TRUE(d.has_value()) << rs::kindName(e.kind) << ": " << rs::decodeErrorName(d.error());
        EXPECT_EQ(rs::encode(*d).value(), bytes) << rs::kindName(e.kind);
        EXPECT_EQ(d->ops.index(), e.ops.index());
        EXPECT_EQ(d->principal.authenticated, "admin");
    }
}

TEST(SecurityEventRegression, HeaderLayoutIsFrozenAndEqualsTheCatalogHeader) {
    const auto b = rs::encode(baseEvent(rs::DropUserOps{})).value();
    ASSERT_GE(b.size(), 36u);
    EXPECT_EQ(b[0], 1); EXPECT_EQ(b[1], 0);   // formatVer 1
    EXPECT_EQ(b[2], 3); EXPECT_EQ(b[3], 0);   // DropUser
    EXPECT_EQ(b[4], 9);                        // objectId low byte
    EXPECT_EQ(b[12], 0); EXPECT_EQ(b[16], 1);  // prev 0, new 1
    EXPECT_EQ(b[28], 0x88); EXPECT_EQ(b[35], 0x11);
}

TEST(SecurityEventRegression, EveryTruncationIsRefusedNeverMisread) {
    for (const rs::Ops& ops : everyKind()) {
        const auto b = rs::encode(baseEvent(ops)).value();
        for (std::size_t n = 0; n < b.size(); ++n)
            ASSERT_EQ(rs::decode(std::span<const uint8_t>(b.data(), n)).error(), rs::DecodeError::Truncated) << n;
    }
}

TEST(SecurityEventRegression, TrailingBytesAreRefused) {
    auto b = rs::encode(baseEvent(rs::CreateRoleOps{})).value();
    b.push_back(0);
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::TrailingBytes);
}

TEST(SecurityEventRegression, UnknownFormatVersionAndKindAreErrorsNotSkips) {
    auto b = rs::encode(baseEvent(rs::DropUserOps{})).value();
    b[0] = 2;
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::UnknownFormatVer);
    b[0] = 1; b[2] = 11;
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::UnknownKind);
}

TEST(SecurityEventRegression, KindDisagreeingWithOpsIsRefused) {
    auto b = rs::encode(baseEvent(rs::DropUserOps{})).value();
    b[2] = 5;   // claims DropRole; the ops tag says DropUser
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::KindOpsMismatch);
}

TEST(SecurityEventRegression, BadEnumBytesAreRefused) {
    const rs::Ops g = rs::GrantOps{rs::GranteeKind::User, readPrefix()};
    auto b = rs::encode(baseEvent(g)).value();
    const std::size_t body = b.size() - rs::detail_testing::opsBodySize(g);
    b[body + 1] = 7;   // operation 7 does not exist
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::BadEnum);
    b[body + 1] = 0; b[body + 2] = 5;   // resource type 5 does not exist
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::BadEnum);
}

TEST(SecurityEventRegression, VerifierBoundsAreEnforcedBothWays) {
    rs::ScramVerifier v = verifier(1);
    v.salt.resize(15);
    EXPECT_EQ(rs::encode(baseEvent(rs::CreateUserOps{v})).error(), rs::EncodeError::BadVerifier);
    v = verifier(1); v.iterations = 4095;
    EXPECT_EQ(rs::encode(baseEvent(rs::CreateUserOps{v})).error(), rs::EncodeError::BadVerifier);
    auto b = rs::encode(baseEvent(rs::CreateUserOps{verifier(1)})).value();
    const std::size_t body = b.size() - rs::detail_testing::opsBodySize(rs::CreateUserOps{verifier(1)});
    b[body] = 70;   // salt length above 64
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::BadVerifier);
}

TEST(SecurityEventRegression, EncodeIntoReusesCapacity) {
    std::vector<uint8_t> buf; buf.reserve(4096);
    const auto* data = buf.data();
    ASSERT_TRUE(rs::encodeInto(baseEvent(rs::CreateUserOps{verifier(1)}), buf).has_value());
    EXPECT_EQ(buf.data(), data);
}

TEST(SecurityEventRegression, OpsTagEqualsTheKindLikeCatalogRegression) {
    // __security frames its ops exactly as __catalog: the tag byte IS the kind (index + 1).
    for (const rs::Ops& ops : everyKind()) {
        const rs::SecurityEvent e = baseEvent(ops);
        const auto b = rs::encode(e).value();
        const std::size_t tagAt = b.size() - rs::detail_testing::opsBodySize(ops) - 1;
        EXPECT_EQ(b[tagAt], static_cast<uint8_t>(e.kind)) << rs::kindName(e.kind);
        auto flipped = b; flipped[tagAt] = static_cast<uint8_t>(e.kind) - 1;
        EXPECT_EQ(rs::decode(flipped).error(), rs::DecodeError::KindOpsMismatch) << rs::kindName(e.kind);
    }
    // the catalog's own convention, for comparison: DropTopic (kind 2) with a 4-byte body
    rune::catalog::CatalogEvent c;
    c.kind = rune::catalog::EventKind::DropTopic; c.ops = rune::catalog::DropTopicOps{3};
    const auto cb = rune::catalog::encode(c).value();
    EXPECT_EQ(cb[cb.size() - 4 - 1], static_cast<uint8_t>(c.kind));
}

TEST(SecurityEventRegression, AlterUserChangeTagStaysZeroBasedRegression) {
    const rs::Ops ops = rs::AlterUserOps{rs::SetPassword{verifier(3), true}};
    const auto b = rs::encode(baseEvent(ops)).value();
    EXPECT_EQ(b[b.size() - rs::detail_testing::opsBodySize(ops)], 2) << "SetPassword = UserChangeKind 2";
}

TEST(SecurityEventRegression, BootstrapCarriesTheFakeSaltSeedRegression) {
    rs::BootstrapOps o{verifier(9)};
    for (std::size_t i = 0; i < o.fakeSaltSeed.size(); ++i) o.fakeSaltSeed[i] = static_cast<uint8_t>(0x60 + i);
    const auto b = rs::encode(baseEvent(o)).value();
    auto d = rs::decode(b);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(std::get<rs::BootstrapOps>(d->ops).fakeSaltSeed, o.fakeSaltSeed);
    // the seed is the last 32 bytes of the body (after the verifier)
    EXPECT_EQ(b[b.size() - 32], 0x60); EXPECT_EQ(b[b.size() - 1], 0x60 + 31);
    EXPECT_EQ(rs::detail_testing::opsBodySize(o), 1u + 16 + 4 + 64 + 64 + 32);
    for (std::size_t n = 0; n < b.size(); ++n)
        ASSERT_EQ(rs::decode(std::span<const uint8_t>(b.data(), n)).error(), rs::DecodeError::Truncated) << n;
}

TEST(SecurityEventRegression, EnumsAreTheProtocolEnumsRegression) {
    namespace rp = rune::protocol;
    static_assert(std::is_same_v<rs::Operation, rp::SecurityOperation>);
    static_assert(std::is_same_v<rs::ResourceType, rp::SecurityResourceType>);
    static_assert(std::is_same_v<rs::PatternKind, rp::SecurityPatternKind>);
    static_assert(std::is_same_v<rs::GranteeKind, rp::SecurityGranteeKind>);
    // frozen values (bytes of the wire and of the on-disk event)
    EXPECT_EQ(static_cast<int>(rs::Operation::Admin), 6); EXPECT_EQ(static_cast<int>(rs::Operation::Describe), 5);
    EXPECT_EQ(static_cast<int>(rs::ResourceType::TransactionalId), 4); EXPECT_EQ(static_cast<int>(rs::ResourceType::Cluster), 0);
    EXPECT_EQ(static_cast<int>(rs::PatternKind::Prefix), 1); EXPECT_EQ(static_cast<int>(rs::GranteeKind::Role), 1);
}
